"""HMAC-SHA256 签名/验签与凭据轮换（重叠有效期）。

协议（与 docs 中的投递协议一致）：
- 待签名串： ``<unix时间戳>.<原始请求体字节>``
- 签名： HMAC-SHA256(secret, 待签名串) 的十六进制小写；
- 请求头： X-Key-Id / X-Timestamp / X-Signature。

轮换规则（确定性）：
1. 同一租户可挂多个凭据，凭 not_before/not_after 形成重叠期，重叠期内
   新旧凭据都可验签；
2. now < not_before → 凭据尚未生效（拒绝，INVALID_SIGNATURE）；
3. now > not_after → 凭据已过期（明确拒绝，EXPIRED_CREDENTIAL），即使
   deprecated=False 也拒绝；
4. deprecated=True 但仍在有效期内 → 允许验签，仅作弃用告警（宽限期）；
5. 时间戳偏离服务端超过 SKEW_SECONDS → STALE_TIMESTAMP（防重放）。
"""
from __future__ import annotations

import hashlib
import hmac
from datetime import datetime

from app.logging_utils import get_logger
from app.models import Credential, utcnow

logger = get_logger(__name__)

SKEW_SECONDS = 300


def _canonical(timestamp: str, body: bytes) -> bytes:
    return timestamp.encode("utf-8") + b"." + body


def sign(secret: str, timestamp: str, body: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), _canonical(timestamp, body), hashlib.sha256)
    return digest.hexdigest()


def credential_window_basis(cred: Credential | None, now: datetime | None = None) -> str:
    """出站凭据是否处于可用窗口，返回机器可读判定依据。

    与入站验签遵守完全相同的有效期/轮换规则：
    - cred is None                         -> KEY_NOT_FOUND
    - now < not_before                     -> CREDENTIAL_NOT_YET_VALID
    - now > not_after（含已轮换下线）       -> EXPIRED_CREDENTIAL
    - 窗口内（即使 deprecated=True）        -> OK（弃用宽限，仍可用）
    """
    now = now or utcnow()
    if cred is None:
        return "KEY_NOT_FOUND"
    if now < cred.not_before:
        return "CREDENTIAL_NOT_YET_VALID"
    if now > cred.not_after:
        return "EXPIRED_CREDENTIAL"
    return "OK"


def verify_signature(
    creds: list[Credential],
    key_id: str,
    timestamp: str,
    body: bytes,
    signature: str,
    now: datetime | None = None,
) -> tuple[bool, str, Credential | None]:
    """返回 (是否通过, 判定依据, 命中的凭据)。

    判定依据为机器可读短语（如 EXPIRED_CREDENTIAL / SIGNATURE_MISMATCH），
    供调用方映射到 FailReason 并写入日志。
    """
    now = now or utcnow()

    cred = next((c for c in creds if c.key_id == key_id), None)
    if cred is None:
        logger.info("verify_fail key_id=%s basis=KEY_NOT_FOUND", key_id)
        return False, "KEY_NOT_FOUND", None

    if now < cred.not_before:
        logger.info(
            "verify_fail key_id=%s basis=CREDENTIAL_NOT_YET_VALID not_before=%s",
            key_id, cred.not_before.isoformat(),
        )
        return False, "CREDENTIAL_NOT_YET_VALID", cred

    if now > cred.not_after:
        logger.info(
            "verify_fail key_id=%s basis=EXPIRED_CREDENTIAL not_after=%s now=%s",
            key_id, cred.not_after.isoformat(), now.isoformat(),
        )
        return False, "EXPIRED_CREDENTIAL", cred

    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        logger.info("verify_fail key_id=%s basis=BAD_TIMESTAMP timestamp=%r", key_id, timestamp)
        return False, "BAD_TIMESTAMP", cred
    delta = abs(now.timestamp() - ts)
    if delta > SKEW_SECONDS:
        logger.info(
            "verify_fail key_id=%s basis=STALE_TIMESTAMP skew=%.1f", key_id, delta
        )
        return False, "STALE_TIMESTAMP", cred

    expected = sign(cred.secret, timestamp, body)
    if not hmac.compare_digest(expected, signature or ""):
        logger.info("verify_fail key_id=%s basis=SIGNATURE_MISMATCH", key_id)
        return False, "SIGNATURE_MISMATCH", cred

    logger.info(
        "verify_ok key_id=%s deprecated=%s basis=HMAC_MATCH", key_id, cred.deprecated
    )
    return True, "OK", cred
