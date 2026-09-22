"""测试夹具：临时数据库、无后台线程的应用、可控时钟的 TestClient。"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

import main
from app import state
from app.config import get_settings, reset_settings_cache
from app.routers.deps import rate_limiter
from app.worker import signing_key_override

logging.basicConfig(level=logging.INFO)

ADMIN_HEADERS = {"X-Admin-Key": "local-admin-key"}
# 2026-09-22T00:00:00Z —— 与任务给定日期一致的固定时钟
T0 = datetime(2026, 9, 22, 0, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """构造隔离环境：

    * 独立临时 SQLite 文件；
    * 退避基数=0，使后台轮询/手动 tick 时到期立即可重试；
      需要验证退避等待的用例改用 gateway.advance_clock + worker.tick()
      直接操作仓储（通过 admin tick 接口），退避时长仍显式断言；
    * 关闭后台线程，所有投递由测试显式驱动 -> 结果确定可复现；
    * 每用例清空限流窗口与签名 key 覆盖。
    """
    db_file = tmp_path / "test.db"
    monkeypatch.setenv("GATEWAY_DB", str(db_file))
    monkeypatch.setenv("GATEWAY_RUN_WORKER", "false")
    monkeypatch.setenv("GATEWAY_BACKOFF_BASE", "0")
    monkeypatch.setenv("GATEWAY_MAX_ATTEMPTS", "5")
    monkeypatch.setenv("GATEWAY_CLOCK_SKEW", "0")
    reset_settings_cache()
    get_settings()

    rate_limiter.reset()
    signing_key_override.clear()
    state.shutdown()
    state.bootstrap(str(db_file), run_worker=False, backoff_base_seconds=0.0)

    with TestClient(main.app) as c:
        c.gateway = _GatewayHelper(c)  # type: ignore[attr-defined]
        yield c

    state.shutdown()
    reset_settings_cache()
    rate_limiter.reset()
    signing_key_override.clear()


class _GatewayHelper:
    """常用自助流程，降低各用例重复代码。"""

    def __init__(self, client: TestClient) -> None:
        self.c = client
        self.now = T0

    # ---- 时间控制（直接驱动 worker 内部时钟不现实；退避=0 时无需推进）----
    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)

    # ---- 管理面快捷方法 ----
    def create_tenant(self, tenant_id: str, api_key: str, rate_limit: int = 1000):
        r = self.c.post("/admin/tenants", headers=ADMIN_HEADERS, json={
            "tenant_id": tenant_id, "api_key": api_key,
            "rate_limit_per_minute": rate_limit,
            "display_name": tenant_id,
        })
        assert r.status_code == 201, r.text
        return r.json()

    def add_credential(self, tenant_key: str, key_id: str, secret: str,
                       valid_from: datetime | None = None,
                       valid_to: datetime | None = None):
        r = self.c.post("/admin/credentials", headers={"X-API-Key": tenant_key}, json={
            "key_id": key_id, "secret": secret,
            "valid_from": (valid_from or T0).isoformat(),
            "valid_to": valid_to.isoformat() if valid_to else None,
        })
        assert r.status_code == 201, r.text
        return r.json()

    def create_subscription(self, tenant_key: str, event_type: str, url: str,
                            min_version: int = 1, max_version: int | None = None,
                            policy: str = "DELIVER_ANYWAY",
                            known_fields: list[str] | None = None):
        r = self.c.post("/admin/subscriptions", headers={"X-API-Key": tenant_key}, json={
            "event_type": event_type, "target_url": url,
            "min_version": min_version, "max_version": max_version,
            "newer_version_policy": policy,
            "known_fields": known_fields or [],
        })
        assert r.status_code == 201, r.text
        return r.json()

    def configure_mock(self, url: str, **kwargs):
        r = self.c.post(f"/admin/mock/endpoints/{url.removeprefix('http://')}",
                        headers=ADMIN_HEADERS, json=kwargs)
        assert r.status_code == 200, r.text
        return r.json()

    def mock_received(self, url: str):
        r = self.c.get(
            f"/admin/mock/endpoints/{url.removeprefix('http://')}/received",
            headers=ADMIN_HEADERS,
        )
        assert r.status_code == 200, r.text
        return r.json()["received"]

    def tick(self):
        r = self.c.post("/admin/worker/tick", headers=ADMIN_HEADERS)
        assert r.status_code == 200, r.text
        return r.json()["processed"]

    def set_signing_key(self, tenant_key: str, key_id: str | None):
        r = self.c.post("/admin/signing-key", headers={"X-API-Key": tenant_key},
                        json={"key_id": key_id} if key_id else {})
        assert r.status_code == 200, r.text
        return r.json()

    def deliveries(self, tenant_key: str, event_id: str):
        r = self.c.get(f"/admin/events/{event_id}/deliveries",
                       headers={"X-API-Key": tenant_key})
        assert r.status_code == 200, r.text
        return r.json()

    def delivery(self, tenant_key: str, delivery_id: int):
        r = self.c.get(f"/admin/deliveries/{delivery_id}",
                       headers={"X-API-Key": tenant_key})
        assert r.status_code == 200, r.text
        return r.json()

    def dead_letters(self, tenant_key: str):
        r = self.c.get("/admin/dead-letters",
                       headers={"X-API-Key": tenant_key})
        assert r.status_code == 200, r.text
        return r.json()

    def ingest(self, tenant_key: str, event_id: str, event_type: str = "order.created",
               version: int = 1, data=None):
        return self.c.post("/v1/events", headers={"X-API-Key": tenant_key}, json={
            "event_id": event_id, "event_type": event_type,
            "event_version": version, "data": data if data is not None else {},
        })
