"""并发/重复入站事件的幂等受理与租户隔离。

要求：重复请求返回稳定且可区分的结论；并发到达只处理一次；
不同合作方的鉴权/额度互不影响。
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest


def _seed(client, tenant="acme", key="key-acme-123456", rate=1000):
    client.gateway.create_tenant(tenant, key, rate)
    return {"X-API-Key": key}


def test_duplicate_returns_stable_distinct_result(client):
    h = _seed(client)
    body = {"event_id": "evt-1", "event_type": "order.created",
            "event_version": 1, "data": {"order_id": "o-1"}}
    r1 = client.post("/v1/events", headers=h, json=body)
    r2 = client.post("/v1/events", headers=h, json=body)
    # 即使 payload 不同，只要 (tenant, event_id) 相同即为重复
    r3 = client.post("/v1/events", headers=h,
                     json={**body, "data": {"order_id": "tampered"}})

    assert r1.status_code == 201
    assert r1.json()["status"] == "ACCEPTED"
    assert r2.status_code == 200 and r2.json()["status"] == "DUPLICATE"
    assert r3.status_code == 200 and r3.json()["status"] == "DUPLICATE"
    # 重复响应稳定：都指向同一批投递结论
    assert r2.json()["deliveries"] == r3.json()["deliveries"]

    got = client.get("/v1/events/evt-1", headers=h).json()
    assert got["event_id"] == "evt-1"


def test_concurrent_duplicates_processed_once(client):
    h = _seed(client)
    client.gateway.add_credential("key-acme-123456", "k1", "secret-acme-1234")
    client.gateway.create_subscription(
        "key-acme-123456", "order.created", "http://mock/orders")

    body = {"event_id": "evt-conc-1", "event_type": "order.created",
            "event_version": 1, "data": {"v": 1}}
    results: list[tuple[int, str]] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def fire():
        try:
            r = client.post("/v1/events", headers=h, json=body)
            with lock:
                results.append((r.status_code, r.json()["status"]))
        except Exception as exc:  # pragma: no cover
            with lock:
                errors.append(exc)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(lambda _: fire(), range(16)))

    assert not errors, errors
    accepted = [x for x in results if x == (201, "ACCEPTED")]
    duplicates = [x for x in results if x == (200, "DUPLICATE")]
    assert len(accepted) == 1, results
    assert len(duplicates) == 15, results

    # 数据层一致：事件只有一条，只生成一条投递，且 mock 端尚未收到（未 tick）
    dels = client.gateway.deliveries("key-acme-123456", "evt-conc-1")
    assert len(dels) == 1, dels
    assert dels[0]["state"] in {"PENDING", "DEAD_LETTER"}

    client.gateway.configure_mock("http://mock/orders")
    client.gateway.tick()
    received = client.gateway.mock_received("http://mock/orders")
    # 同一订阅内该事件只投递一次（并发没有造成重复副作用），顺序为 seq=1
    hits = [r for r in received if "evt-conc-1" in r["body"]]
    assert len(hits) == 1, hits
    assert hits[0]["headers"]["X-Delivery-Seq"] == "1"


def test_tenant_isolation(client):
    ha = _seed(client, "acme", "key-acme-123456")
    hb = _seed(client, "globex", "key-globex-123456")
    client.gateway.create_subscription(
        "key-acme-123456", "order.created", "http://mock/a")
    client.gateway.create_subscription(
        "key-globex-123456", "order.created", "http://mock/b")

    body_a = {"event_id": "shared-id", "event_type": "order.created",
              "event_version": 1, "data": {"who": "a"}}
    body_b = {"event_id": "shared-id", "event_type": "order.created",
              "event_version": 1, "data": {"who": "b"}}
    ra = client.post("/v1/events", headers=ha, json=body_a)
    rb = client.post("/v1/events", headers=hb, json=body_b)
    assert ra.status_code == 201 and rb.status_code == 201  # 同 event_id 不同租户均可受理

    # 跨租户不可见
    got_a = client.get("/v1/events/shared-id", headers=ha).json()
    got_b = client.get("/v1/events/shared-id", headers=hb).json()
    assert got_a["tenant_id"] == "acme" and got_b["tenant_id"] == "globex"
    assert got_a["deliveries"][0]["subscription_id"] != \
        got_b["deliveries"][0]["subscription_id"]

    # A 拿不到 B 的投递单
    b_delivery = client.gateway.deliveries("key-globex-123456", "shared-id")[0]
    r = client.get(f"/admin/deliveries/{b_delivery['id']}", headers=ha)
    assert r.status_code == 404
    assert client.gateway.dead_letters("key-acme-123456") == []


def test_auth_failures_are_independent(client):
    ha = _seed(client, "acme", "key-acme-123456", rate=3)
    _seed(client, "globex", "key-globex-123456", rate=1000)
    body = {"event_id": "e", "event_type": "t", "event_version": 1, "data": {}}

    # 错误 key 明确拒绝，错误码可区分
    missing = client.post("/v1/events", json=body)
    invalid = client.post("/v1/events", headers={"X-API-Key": "nope"}, json=body)
    assert missing.status_code == 401
    assert missing.json()["detail"]["error"] == "MISSING_API_KEY"
    assert invalid.status_code == 401
    assert invalid.json()["detail"]["error"] == "INVALID_API_KEY"

    # A 额度耗尽（3/min）
    statuses = [client.post("/v1/events", headers=ha,
                            json={**body, "event_id": f"e{i}"}).status_code
                for i in range(5)]
    assert statuses.count(429) == 2, statuses
    # B 不受 A 超限影响
    ok = client.post("/v1/events", headers={"X-API-Key": "key-globex-123456"},
                     json={**body, "event_id": "g1"})
    assert ok.status_code == 201
