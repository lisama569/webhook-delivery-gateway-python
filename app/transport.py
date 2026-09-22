"""出站传输层：HTTP 投递与本地 mock（无外部依赖、无真实网络）。

分类规则（可解释）：
- 2xx            → SUCCESS；
- 408/429/5xx    → RETRYABLE（临时故障，进入退避重试）；
- 其余 4xx       → PERMANENT（接收方明确拒绝，直接死信）；
- 连接/超时异常  → RETRYABLE（NETWORK_ERROR）。

MockTransport 按 url 注册「响应脚本」：每次调用弹出脚本的下一项；
脚本项可以是 (status_code, text) 或 Exception 实例。用于在本地确定性地
复现「失败 N 次后成功」「持续失败至重试耗尽」等场景。
"""
from __future__ import annotations

import collections

import httpx

from app.logging_utils import get_logger
from app.models import DeliveryKind

logger = get_logger(__name__)

RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504}


class TransportResult:
    def __init__(
        self,
        kind: DeliveryKind,
        http_status: int | None,
        detail: str,
        reason_code: str = "",
    ) -> None:
        self.kind = kind
        self.http_status = http_status
        self.detail = detail
        self.reason_code = reason_code  # HTTP_2XX / HTTP_4XX / HTTP_5XX / NETWORK_ERROR

    def __repr__(self) -> str:
        return (
            f"TransportResult(kind={self.kind.value}, http_status={self.http_status}, "
            f"reason_code={self.reason_code}, detail={self.detail!r})"
        )

    @staticmethod
    def classify_status(status: int) -> tuple[DeliveryKind, str, str]:
        if 200 <= status < 300:
            return DeliveryKind.SUCCESS, "HTTP_2XX", f"receiver acknowledged {status}"
        if status in RETRYABLE_STATUS:
            code = "HTTP_5XX" if status >= 500 else "HTTP_4XX_RETRYABLE"
            return DeliveryKind.RETRYABLE, code, f"retryable status {status}"
        return DeliveryKind.PERMANENT, "HTTP_4XX", f"permanent reject status {status}"


class Transport:
    async def post(self, url: str, headers: dict[str, str], body: bytes) -> TransportResult:
        raise NotImplementedError


class HttpTransport(Transport):
    """基于 httpx 的真实 HTTP 投递（本地联调可选）。"""

    def __init__(self, timeout: float = 5.0) -> None:
        self._timeout = timeout

    async def post(self, url: str, headers: dict[str, str], body: bytes) -> TransportResult:
        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(url, headers=headers, content=body)
        except httpx.HTTPError as exc:
            logger.info("transport basis=NETWORK_ERROR url=%s detail=%s", url, exc)
            return TransportResult(
                DeliveryKind.RETRYABLE, None, f"network error: {exc}", "NETWORK_ERROR"
            )
        kind, code, detail = TransportResult.classify_status(resp.status_code)
        logger.info(
            "transport basis=%s url=%s status=%s", code, url, resp.status_code
        )
        return TransportResult(kind, resp.status_code, detail, code)


class MockTransport(Transport):
    """可编排的内存投递器。

    用法：
        mock.script("https://partner/x", [(503, "busy"), (200, "ok")])
        mock.script("https://partner/y", [ConnectionError("down")])
    未注册 url 的请求默认返回 200。
    """

    def __init__(self) -> None:
        self._scripts: dict[str, collections.deque] = {}
        self.calls: list[dict] = []  # 实际发出的请求，供测试断言

    def script(self, url: str, responses: list) -> None:
        self._scripts[url] = collections.deque(responses)

    async def post(self, url: str, headers: dict[str, str], body: bytes) -> TransportResult:
        self.calls.append({"url": url, "headers": headers, "body": body})
        queue = self._scripts.get(url)
        if not queue:
            return TransportResult(DeliveryKind.SUCCESS, 200, "mock default 200", "HTTP_2XX")
        item = queue.popleft()
        if isinstance(item, Exception):
            logger.info("transport basis=NETWORK_ERROR url=%s detail=%s", url, item)
            return TransportResult(
                DeliveryKind.RETRYABLE, None, f"network error: {item}", "NETWORK_ERROR"
            )
        status, text = item
        kind, code, detail = TransportResult.classify_status(status)
        logger.info("transport basis=%s url=%s status=%s", code, url, status)
        return TransportResult(kind, status, f"{detail}: {text}", code)
