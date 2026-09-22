"""投递决策：可解释的确定性退避 + 单次尝试分类。"""
from __future__ import annotations

from datetime import timedelta

from .config import Settings
from .models import FailReason
from .transport import Transport


def compute_backoff(attempt: int, settings: Settings,
                    *, rng: float | None = None) -> timedelta:
    """确定性指数退避（无随机抖动，完全可复现/可解释）：

        delay(第 n 次重试) = min(base * factor**(n-1), cap)

    例：base=1, factor=2, cap=300
        重试1/2/3/4 的等待 = 1s, 2s, 4s, 8s ...
    rng 参数仅为保留未来抖动扩展，当前不参与计算（确定性）。
    """
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    raw = settings.backoff_base_seconds * (
        settings.backoff_factor ** (attempt - 1)
    )
    capped = min(raw, settings.backoff_cap_seconds)
    return timedelta(seconds=capped)


def attempt_once(transport: Transport, target_url: str, body: bytes,
                 headers: dict[str, str]
                 ) -> tuple[bool, int | None, FailReason, str, str | None]:
    """执行一次投递并把传输结果分类为领域原因。

    返回 (ok, http_status, fail_reason, detail, accepted_key_id)。
    """
    result = transport.post(target_url, headers, body)
    if result.ok:
        return True, result.http_status, FailReason.NONE, result.detail, \
            result.accepted_signature
    reason = (FailReason(result.fail_reason)
              if result.fail_reason in {r.value for r in FailReason}
              else FailReason.TRANSPORT_ERROR)
    return False, result.http_status, reason, result.detail, \
        result.accepted_signature
