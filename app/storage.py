"""内存存储：跨租户的数据隔离与并发原语。本地运行无外部依赖。

并发语义：
- 事件受理以 (tenant_id, event_id) 为幂等键，事件级互斥锁 + 唯一插入保证
  "并发重复到达只处理一次"；
- 投递以 subscription_id 为粒度串行（顺序保证）；
- 配额为每个租户独立的滑动 60 秒窗口计数，互不影响。
"""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import datetime, timedelta

from app.logging_utils import get_logger
from app.models import (
    Credential,
    DeliveryRecord,
    EventSchema,
    EventStatus,
    InboundEvent,
    Subscription,
    SubscriptionState,
    Tenant,
)
from app.models import utcnow

logger = get_logger(__name__)


class _LazyLock:
    """按需在当前运行的事件循环中创建 asyncio.Lock。

    装配（可能在独立循环/线程中）与服务运行使用不同循环时，锁只在真正的
    并发控制点创建，避免 "bound to a different event loop" 错误。
    """

    def __init__(self) -> None:
        self._lock: asyncio.Lock | None = None

    def _get(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def __aenter__(self):
        return await self._get().__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        return await self._get().__aexit__(exc_type, exc, tb)


class Storage:
    def __init__(self) -> None:
        self._tenants: dict[str, Tenant] = {}
        self._credentials: dict[str, Credential] = {}
        self._schemas: dict[tuple[str, int], EventSchema] = {}
        self._subscriptions: dict[str, Subscription] = {}
        self._events: dict[tuple[str, str], InboundEvent] = {}
        self._deliveries: dict[tuple[str, str], DeliveryRecord] = {}
        # 订阅投递开关：subscription_id -> SubscriptionState（默认 ACTIVE）
        self._subscription_states: dict[str, SubscriptionState] = {}
        # 并发原语
        self._event_locks: dict[tuple[str, str], _LazyLock] = {}
        self._subscription_locks: dict[str, _LazyLock] = {}
        self._seq: dict[str, int] = defaultdict(int)
        # 配额：tenant_id -> 接收时间戳队列（滑动窗口）
        self._quota_window: dict[str, deque[datetime]] = defaultdict(deque)

    # tenant / credential -------------------------------------------------
    async def upsert_tenant(self, tenant: Tenant) -> None:
        self._tenants[tenant.tenant_id] = tenant
        logger.info(
            "storage_tenant_upsert",
            extra={"tenant_id": tenant.tenant_id, "rate_limit": tenant.rate_limit_per_min},
        )

    async def get_tenant(self, tenant_id: str) -> Tenant | None:
        return self._tenants.get(tenant_id)

    async def upsert_credential(self, cred: Credential) -> None:
        self._credentials[cred.key_id] = cred
        logger.info(
            "storage_credential_upsert",
            extra={
                "key_id": cred.key_id,
                "tenant_id": cred.tenant_id,
                "not_before": cred.not_before.isoformat(),
                "not_after": cred.not_after.isoformat(),
                "deprecated": cred.deprecated,
            },
        )

    async def get_credential(self, key_id: str) -> Credential:
        return self._credentials.get(key_id)

    def list_credentials(self, tenant_id: str) -> list[Credential]:
        return [c for c in self._credentials.values() if c.tenant_id == tenant_id]

    # schema --------------------------------------------------------------
    async def upsert_schema(self, schema: EventSchema) -> None:
        major = int(schema.version.split(".")[0])
        self._schemas[(schema.event_type, major)] = schema

    async def get_schema(self, event_type: str, major: int) -> EventSchema | None:
        return self._schemas.get((event_type, major))

    # subscription --------------------------------------------------------
    async def upsert_subscription(self, sub: Subscription) -> None:
        self._subscriptions[sub.subscription_id] = sub

    async def get_subscription(self, subscription_id: str) -> Subscription | None:
        return self._subscriptions.get(subscription_id)

    async def list_subscriptions(self, tenant_id: str, event_type: str) -> list[Subscription]:
        return [
            s
            for s in self._subscriptions.values()
            if s.tenant_id == tenant_id and s.event_type == event_type
        ]

    # subscription run-state (pause/resume) -------------------------------
    async def set_subscription_state(
        self, subscription_id: str, state: SubscriptionState
    ) -> None:
        self._subscription_states[subscription_id] = state

    async def get_subscription_state(self, subscription_id: str) -> SubscriptionState:
        return self._subscription_states.get(
            subscription_id, SubscriptionState.ACTIVE
        )

    # event ---------------------------------------------------------------
    async def event_lock(self, tenant_id: str, event_id: str) -> _LazyLock:
        key = (tenant_id, event_id)
        lock = self._event_locks.get(key)
        if lock is None:
            lock = _LazyLock()
            self._event_locks[key] = lock
        return lock

    async def save_event_if_absent(self, event: InboundEvent) -> bool:
        """仅当幂等键不存在时落库。返回 True 表示首次插入。

        调用方必须先持有 event_lock，从而保证"检查-插入"原子。
        """
        key = (event.tenant_id, event.event_id)
        if key in self._events:
            return False
        self._events[key] = event
        return True

    async def get_event(self, tenant_id: str, event_id: str) -> InboundEvent | None:
        return self._events.get((tenant_id, event_id))

    # delivery ------------------------------------------------------------
    async def save_delivery(self, record: DeliveryRecord) -> None:
        self._deliveries[(record.event_id, record.subscription_id)] = record

    async def get_delivery(self, event_id: str, subscription_id: str) -> DeliveryRecord | None:
        return self._deliveries.get((event_id, subscription_id))

    def list_deliveries(
        self, tenant_id: str | None = None, status=None
    ) -> list[DeliveryRecord]:
        records = list(self._deliveries.values())
        if tenant_id is not None:
            records = [r for r in records if self._delivery_tenant(r) == tenant_id]
        if status is not None:
            records = [r for r in records if r.status == status]
        return sorted(records, key=lambda r: (r.subscription_id, r.seq))

    def list_dead_letters(
        self,
        tenant_id: str | None = None,
        subscription_id: str | None = None,
    ) -> list[DeliveryRecord]:
        """失败链路（DEAD_LETTER）查询；按 seq 排序保证重放顺序确定。"""
        records = [
            r for r in self._deliveries.values()
            if r.status is EventStatus.DEAD_LETTER
        ]
        if tenant_id is not None:
            records = [r for r in records if self._delivery_tenant(r) == tenant_id]
        if subscription_id is not None:
            records = [r for r in records if r.subscription_id == subscription_id]
        return sorted(records, key=lambda r: (r.subscription_id, r.seq))

    def _delivery_tenant(self, record: DeliveryRecord) -> str | None:
        sub = self._subscriptions.get(record.subscription_id)
        return sub.tenant_id if sub else None

    # ordering ------------------------------------------------------------
    async def subscription_lock(self, subscription_id: str) -> _LazyLock:
        lock = self._subscription_locks.get(subscription_id)
        if lock is None:
            lock = _LazyLock()
            self._subscription_locks[subscription_id] = lock
        return lock

    async def next_seq(self, subscription_id: str) -> int:
        self._seq[subscription_id] += 1
        return self._seq[subscription_id]

    # quota ---------------------------------------------------------------
    async def quota_gate(self, tenant_id: str, limit: int, now: datetime | None = None) -> bool:
        """滑动 60 秒窗口配额。返回 True 表示放行并占用一个额度。

        每个租户独立计数；不同租户额度互不影响。
        """
        now = now or utcnow()
        window = self._quota_window[tenant_id]
        cutoff = now - timedelta(seconds=60)
        while window and window[0] < cutoff:
            window.popleft()
        if len(window) >= limit:
            logger.info(
                "storage_quota_rejected",
                extra={"tenant_id": tenant_id, "used": len(window), "limit": limit},
            )
            return False
        window.append(now)
        return True

    async def reset_quotas(self) -> None:
        self._quota_window.clear()
