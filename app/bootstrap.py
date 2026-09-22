"""组装与本地演示数据装配；提供可重置的应用状态。"""
from __future__ import annotations

from datetime import timedelta

from app.dispatcher import Dispatcher
from app.logging_utils import get_logger
from app.models import (
    Credential,
    EventSchema,
    FieldSpec,
    Subscription,
    Tenant,
    utcnow,
)
from app.service import InboundService
from app.storage import Storage
from app.transport import MockTransport

logger = get_logger(__name__)

# 演示用回调（默认全部成功；可用 /admin/mock/script 编排故障）
DEMO_CALLBACK_URL = "mock://partner-a/order-hook"


class AppState:
    def __init__(self, seed: bool = True) -> None:
        self.storage = Storage()
        self.transport = MockTransport()
        self.dispatcher = Dispatcher(self.storage, self.transport)
        self.service = InboundService(self.storage, self.dispatcher)
        if seed:
            self.seed_demo_data()

    def seed_demo_data(self) -> None:
        import asyncio

        async def _seed() -> None:
            now = utcnow()
            await self.storage.upsert_tenant(
                Tenant("tenant-a", "合作方A（订单）", rate_limit_per_min=60)
            )
            await self.storage.upsert_tenant(
                Tenant("tenant-b", "合作方B（独立额度）", rate_limit_per_min=5)
            )
            # tenant-a 的新旧凭据处于轮换重叠期
            await self.storage.upsert_credential(
                Credential(
                    key_id="key-a-old",
                    tenant_id="tenant-a",
                    secret="secret-old",
                    not_before=now - timedelta(days=30),
                    not_after=now + timedelta(days=1),
                    deprecated=True,
                )
            )
            await self.storage.upsert_credential(
                Credential(
                    key_id="key-a-new",
                    tenant_id="tenant-a",
                    secret="secret-new",
                    not_before=now - timedelta(days=1),
                    not_after=now + timedelta(days=29),
                )
            )
            # 已过期凭据：必须被明确拒绝
            await self.storage.upsert_credential(
                Credential(
                    key_id="key-a-expired",
                    tenant_id="tenant-a",
                    secret="secret-expired",
                    not_before=now - timedelta(days=60),
                    not_after=now - timedelta(days=1),
                )
            )
            await self.storage.upsert_credential(
                Credential(
                    key_id="key-b-1",
                    tenant_id="tenant-b",
                    secret="secret-b",
                    not_before=now - timedelta(days=1),
                    not_after=now + timedelta(days=30),
                )
            )
            # 契约：order.created v1（v1.2 为注册版本，向前兼容到 1.x）
            await self.storage.upsert_schema(
                EventSchema(
                    event_type="order.created",
                    version="1.2.0",
                    fields={
                        "order_id": FieldSpec("order_id", "string"),
                        "amount": FieldSpec("amount", "int", required=False),
                    },
                )
            )
            await self.storage.upsert_subscription(
                Subscription(
                    subscription_id="sub-a-orders",
                    tenant_id="tenant-a",
                    event_type="order.created",
                    version="1.0.0",
                    callback_url=DEMO_CALLBACK_URL,
                    credential_key_id="key-a-new",
                    max_attempts=4,
                )
            )

        _run_async(_seed())
        logger.info("bootstrap_seed_complete basis=DEMO_DATA_LOADED")


def _run_async(coro):
    """在同步上下文运行协程；已处于事件循环时开一个新循环（用于装配期）。"""
    import asyncio
    import threading

    result = {}

    def _runner():
        loop = asyncio.new_event_loop()
        try:
            result["value"] = loop.run_until_complete(coro)
        finally:
            loop.close()

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    t = threading.Thread(target=_runner)
    t.start()
    t.join()
    if "error" in result:
        raise result["error"]
    return result.get("value")

_state: AppState | None = None


def get_state() -> AppState:
    global _state
    if _state is None:
        _state = AppState(seed=True)
    return _state


def reset_state(seed: bool = True) -> AppState:
    global _state
    _state = AppState(seed=seed)
    return _state
