"""入站受理服务：认证、幂等、配额、契约校验与扇出（fan-out）。

处理顺序（确定且短路）：
1. 租户存在性 → 2. 验签/凭据时效 → 3. 事件级互斥锁内查幂等
→ 4. 滑动窗口配额 → 5. 契约版本/字段校验 → 6. 落库并按订阅扇出。

幂等语义：
- 键为 (tenant_id, event_id)；锁内检查 + 唯一插入保证并发重复仅受理一次；
- 重复请求返回 200 + duplicate=true 与首次的稳定结论（首次状态可区分）；
- 认证/配额/契约失败不落库、不占用幂等键（客户端修正后可重新提交）。
"""
from __future__ import annotations

from app.contract import check_compatibility, parse_semver
from app.logging_utils import get_logger
from app.models import (
    EventStatus,
    FailReason,
    InboundEvent,
    utcnow,
)
from app.dispatcher import Dispatcher
from app.security import verify_signature
from app.storage import Storage

logger = get_logger(__name__)

# 验签判定依据 -> FailReason
_BASIS_REASON = {
    "KEY_NOT_FOUND": FailReason.INVALID_SIGNATURE,
    "CREDENTIAL_NOT_YET_VALID": FailReason.INVALID_SIGNATURE,
    "SIGNATURE_MISMATCH": FailReason.INVALID_SIGNATURE,
    "BAD_TIMESTAMP": FailReason.STALE_TIMESTAMP,
    "STALE_TIMESTAMP": FailReason.STALE_TIMESTAMP,
    "EXPIRED_CREDENTIAL": FailReason.EXPIRED_CREDENTIAL,
}

_HTTP_STATUS = {
    FailReason.UNKNOWN_TENANT: 401,
    FailReason.INVALID_SIGNATURE: 401,
    FailReason.EXPIRED_CREDENTIAL: 401,
    FailReason.STALE_TIMESTAMP: 401,
    FailReason.QUOTA_EXCEEDED: 429,
    FailReason.UNKNOWN_EVENT_VERSION: 422,
    FailReason.CONTRACT_INCOMPATIBLE: 422,
}


class AcceptResult:
    def __init__(
        self,
        accepted: bool,
        duplicate: bool,
        status_code: int,
        reason: FailReason,
        detail: str,
        event_id: str | None = None,
        schema_version_used: str | None = None,
        unknown_fields: list[str] | None = None,
        deliveries: list[str] | None = None,
    ) -> None:
        self.accepted = accepted
        self.duplicate = duplicate
        self.status_code = status_code
        self.reason = reason
        self.detail = detail
        self.event_id = event_id
        self.schema_version_used = schema_version_used
        self.unknown_fields = unknown_fields or []
        self.deliveries = deliveries or []

    def to_response(self) -> dict:
        return {
            "event_id": self.event_id,
            "accepted": self.accepted,
            "duplicate": self.duplicate,
            "reason": self.reason.value,
            "detail": self.detail,
            "schema_version_used": self.schema_version_used,
            "unknown_fields": self.unknown_fields,
            "deliveries": self.deliveries,
        }


def _reject(reason: FailReason, detail: str, event_id: str | None = None) -> AcceptResult:
    return AcceptResult(False, False, _HTTP_STATUS[reason], reason, detail, event_id)


class InboundService:
    def __init__(self, storage: Storage, dispatcher: Dispatcher) -> None:
        self._storage = storage
        self._dispatcher = dispatcher

    async def accept(
        self,
        tenant_id: str,
        key_id: str,
        timestamp: str,
        signature: str,
        raw_body: bytes,
        parsed: dict,
        now=None,
    ) -> AcceptResult:
        now = now or utcnow()
        event_id = parsed.get("event_id")
        event_type = parsed.get("event_type")
        version = parsed.get("version")
        payload = parsed.get("payload") or {}

        # 1. 租户 ----------------------------------------------------------
        tenant = await self._storage.get_tenant(tenant_id)
        if tenant is None:
            logger.info(
                "inbound_reject event_id=%s basis=UNKNOWN_TENANT tenant_id=%s",
                event_id, tenant_id,
            )
            return _reject(FailReason.UNKNOWN_TENANT, "unknown tenant", event_id)

        # 2. 验签 ----------------------------------------------------------
        creds = self._storage.list_credentials(tenant_id)
        ok, basis, cred = verify_signature(
            creds, key_id, timestamp, raw_body, signature, now=now
        )
        if not ok:
            reason = _BASIS_REASON[basis]
            logger.info(
                "inbound_reject event_id=%s tenant_id=%s basis=%s reason=%s",
                event_id, tenant_id, basis, reason.value,
            )
            return _reject(reason, f"authentication failed: {basis}", event_id)

        # 3. 基本事件字段（幂等键需要） ------------------------------------
        if not event_id or not event_type or not version:
            return _reject(
                FailReason.CONTRACT_INCOMPATIBLE,
                "event envelope requires event_id/event_type/version",
                event_id,
            )

        lock = await self._storage.event_lock(tenant_id, event_id)
        async with lock:
            existing = await self._storage.get_event(tenant_id, event_id)
            if existing is not None:
                # 稳定可区分：返回首次受理的最终结论；duplicate=True
                logger.info(
                    "inbound_duplicate event_id=%s tenant_id=%s basis=IDEMPOTENCY_HIT "
                    "first_status=%s",
                    event_id, tenant_id, existing.status.value,
                )
                return AcceptResult(
                    accepted=True,
                    duplicate=True,
                    status_code=200,
                    reason=FailReason.NONE,
                    detail=f"duplicate request; original status={existing.status.value}",
                    event_id=event_id,
                    schema_version_used=existing.schema_version_used,
                    unknown_fields=existing.unknown_fields,
                )

            # 4. 配额（租户独立） ------------------------------------------
            allowed = await self._storage.quota_gate(
                tenant_id, tenant.rate_limit_per_min, now=now
            )
            if not allowed:
                logger.info(
                    "inbound_reject event_id=%s tenant_id=%s basis=QUOTA_EXCEEDED "
                    "limit=%s",
                    event_id, tenant_id, tenant.rate_limit_per_min,
                )
                return _reject(
                    FailReason.QUOTA_EXCEEDED,
                    f"rate limit {tenant.rate_limit_per_min}/min exceeded",
                    event_id,
                )

            # 5. 契约 ------------------------------------------------------
            try:
                major = parse_semver(version)[0]
            except ValueError:
                return _reject(
                    FailReason.UNKNOWN_EVENT_VERSION,
                    f"invalid event version: {version!r}",
                    event_id,
                )
            schema = await self._storage.get_schema(event_type, major)
            if schema is None:
                logger.info(
                    "inbound_reject event_id=%s tenant_id=%s basis=UNKNOWN_EVENT_VERSION "
                    "event_type=%s version=%s",
                    event_id, tenant_id, event_type, version,
                )
                return _reject(
                    FailReason.UNKNOWN_EVENT_VERSION,
                    f"no registered schema for {event_type} major v{major}",
                    event_id,
                )

            compat = check_compatibility(schema, version, payload)
            if not compat.ok:
                logger.info(
                    "inbound_reject event_id=%s tenant_id=%s reason=%s detail=%s",
                    event_id, tenant_id, compat.reason.value, compat.detail,
                )
                return _reject(compat.reason, compat.detail, event_id)

            # 6. 落库 + 扇出 ------------------------------------------------
            event = InboundEvent(
                event_id=event_id,
                tenant_id=tenant_id,
                event_type=event_type,
                version=version,
                payload=payload,
                received_at=now,
                status=EventStatus.ACCEPTED,
                schema_version_used=compat.effective_version,
                unknown_fields=compat.unknown_fields,
            )
            inserted = await self._storage.save_event_if_absent(event)
            assert inserted  # 锁内不可能重复

            subscriptions = await self._storage.list_subscriptions(tenant_id, event_type)
            delivery_ids: list[str] = []
            for sub in subscriptions:
                # 订阅版本与事件主版本不兼容的具体订阅不扇出，并记录原因
                try:
                    sub_result = check_compatibility(schema, sub.version, payload)
                except ValueError:
                    sub_result = None
                if sub_result is None or not sub_result.ok:
                    logger.info(
                        "fanout_skip event_id=%s subscription_id=%s basis=%s",
                        event_id, sub.subscription_id,
                        sub_result.detail if sub_result else "BAD_SUBSCRIPTION_VERSION",
                    )
                    continue
                record = await self._dispatcher.enqueue(event_id, sub)
                delivery_ids.append(sub.subscription_id)
                logger.info(
                    "fanout_scheduled event_id=%s subscription_id=%s seq=%s",
                    event_id, sub.subscription_id, record.seq,
                )

            event.status = EventStatus.SCHEDULED if delivery_ids else EventStatus.ACCEPTED
            logger.info(
                "inbound_accepted event_id=%s tenant_id=%s basis=FIRST_ACCEPT "
                "deliveries=%s unknown_fields=%s",
                event_id, tenant_id, delivery_ids, compat.unknown_fields,
            )
            return AcceptResult(
                accepted=True,
                duplicate=False,
                status_code=202,
                reason=FailReason.NONE,
                detail=compat.detail,
                event_id=event_id,
                schema_version_used=compat.effective_version,
                unknown_fields=compat.unknown_fields,
                deliveries=delivery_ids,
            )
