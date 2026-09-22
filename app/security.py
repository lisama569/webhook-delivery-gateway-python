"""HMAC-SHA256 签名、常量时间比较、凭据有效期判定。"""
from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timedelta


def _canonical(timestamp: int, content: bytes) -> bytes:
    """签名内容：时间戳 + '.' + 原始 body。"""
    return f"{timestamp}.".encode("ascii") + content


def sign(secret: str, content: bytes, key_id: str, timestamp: int) -> str:
    """生成形如 't=<ts>,k=<kid>,v=<hexhmac>' 的签名头值。"""
    mac = hmac.new(secret.encode("utf-8"), _canonical(timestamp, content), hashlib.sha256)
    return f"t={timestamp},k={key_id},v={mac.hexdigest()}"


def _parse_signature(signature: str) -> tuple[int | None, str | None, str | None]:
    ts = kid = val = None
    for part in signature.split(","):
        if part.startswith("t="):
            try:
                ts = int(part[2:])
            except ValueError:
                ts = None
        elif part.startswith("k="):
            kid = part[2:] or None
        elif part.startswith("v="):
            val = part[2:] or None
    return ts, kid, val


def verify(secret: str, content: bytes, signature: str) -> bool:
    """按签名中声明的时间戳与密钥常量时间校验 HMAC。"""
    ts, _kid, val = _parse_signature(signature)
    if ts is None or val is None:
        return False
    expected = hmac.new(
        secret.encode("utf-8"), _canonical(ts, content), hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, val)


def signature_key_id(signature: str) -> str | None:
    return _parse_signature(signature)[1]


def constant_time_equals(a: str, b: str) -> bool:
    return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def is_within_validity(now: datetime, valid_from: datetime,
                       valid_to: datetime | None, skew_seconds: int) -> bool:
    skew = timedelta(seconds=skew_seconds)
    if now < valid_from - skew:
        return False
    if valid_to is not None and now > valid_to + skew:
        return False
    return True
