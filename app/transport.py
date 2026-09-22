"""出站传输抽象与本地内置 Mock 传输（不依赖外部服务）。

MockTransport 是一个进程内的"合作方接收端"：
* 按 target_url 配置行为：前 N 次失败（5xx / 网络异常）后成功；
* 可校验 HMAC 签名并声明自己当前接受哪些 key_id（模拟凭据轮换期）；
* 记录每次收到的请求，供测试断言顺序与内容。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Protocol


class Transport(Protocol):
    def post(self, url: str, headers: dict[str, str],
             body: bytes, *, timeout: float = 5.0) -> "TransportResult":
        ...


@dataclass
class TransportResult:
    ok: bool
    http_status: int | None
    fail_reason: str        # TRANSPORT_ERROR / NON_2XX / OK
    detail: str
    accepted_signature: str | None = None  # mock 回显其认可的 key_id


@dataclass
class _Endpoint:
    fail_times: int = 0
    fail_status: int = 500
    transport_error: bool = False
    accept_key_ids: set[str] | None = None    # None = 不验签，全收
    reject_key_ids: set[str] = field(default_factory=set)
    received: list[dict] = field(default_factory=list)
    lock: Lock = field(default_factory=Lock)


class MockTransport:
    def __init__(self) -> None:
        self._endpoints: dict[str, _Endpoint] = {}
        self._lock = Lock()

    def _ep(self, url: str) -> _Endpoint:
        with self._lock:
            ep = self._endpoints.get(url)
            if ep is None:
                ep = _Endpoint()
                self._endpoints[url] = ep
            return ep

    def configure_endpoint(self, url: str, *, fail_times: int = 0,
                           fail_status: int = 500, transport_error: bool = False,
                           accept_key_ids: set[str] | list[str] | None = None,
                           reject_key_ids: set[str] | list[str] | None = None) -> None:
        ep = self._ep(url)
        with ep.lock:
            ep.fail_times = fail_times
            ep.fail_status = fail_status
            ep.transport_error = transport_error
            ep.accept_key_ids = (set(accept_key_ids) if accept_key_ids is not None
                                 else None)
            ep.reject_key_ids = set(reject_key_ids or [])

    def reset_endpoint(self, url: str) -> None:
        with self._lock:
            self._endpoints.pop(url, None)

    def received(self, url: str) -> list[dict]:
        return [dict(r) for r in self._ep(url).received]

    def post(self, url: str, headers: dict[str, str],
             body: bytes, *, timeout: float = 5.0) -> TransportResult:
        ep = self._ep(url)
        with ep.lock:
            attempt_no = len(ep.received) + 1
            key_id = headers.get("X-Signature-Key-Id")
            ep.received.append({
                "attempt": attempt_no,
                "headers": dict(headers),
                "body": body.decode("utf-8", errors="replace"),
            })

            # 1) 合作方按 key_id 做凭据轮换/过期策略
            if key_id and key_id in ep.reject_key_ids:
                return TransportResult(
                    ok=False, http_status=401, fail_reason="NON_2XX",
                    detail=f"receiver rejected credential key_id={key_id} "
                           f"(expired/rotated out)", accepted_signature=None,
                )
            if ep.accept_key_ids is not None and key_id not in ep.accept_key_ids:
                return TransportResult(
                    ok=False, http_status=401, fail_reason="NON_2XX",
                    detail=f"receiver does not recognize key_id={key_id}; "
                           f"accepts {sorted(ep.accept_key_ids)}",
                    accepted_signature=None,
                )

            # 2) 模拟前 N 次失败
            if attempt_no <= ep.fail_times:
                if ep.transport_error:
                    return TransportResult(
                        ok=False, http_status=None, fail_reason="TRANSPORT_ERROR",
                        detail=f"simulated connection error on attempt {attempt_no}",
                    )
                return TransportResult(
                    ok=False, http_status=ep.fail_status, fail_reason="NON_2XX",
                    detail=f"simulated HTTP {ep.fail_status} on attempt {attempt_no}",
                )

            return TransportResult(
                ok=True, http_status=200, fail_reason="NONE",
                detail="accepted", accepted_signature=key_id,
            )


class HttpxTransport:  # pragma: no cover - 仅用于真实部署，本地测试不走这里
    """真实 HTTP 传输（可选）。本地验证无需启动外部服务。"""

    def post(self, url: str, headers: dict[str, str],
             body: bytes, *, timeout: float = 5.0) -> TransportResult:
        import httpx
        try:
            resp = httpx.post(url, headers=headers, content=body, timeout=timeout)
        except httpx.HTTPError as exc:
            return TransportResult(False, None, "TRANSPORT_ERROR", str(exc))
        ok = 200 <= resp.status_code < 300
        return TransportResult(
            ok=ok, http_status=resp.status_code,
            fail_reason="NONE" if ok else "NON_2XX",
            detail="accepted" if ok else f"HTTP {resp.status_code}",
        )
