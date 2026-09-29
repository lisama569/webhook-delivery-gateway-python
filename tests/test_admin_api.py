"""HTTP 管理接口：订阅暂停/恢复/状态、单条与批量失败重放。"""
import json
import time

import pytest
from fastapi.testclient import TestClient

import main
from app.bootstrap import reset_state
from app.security import sign
from tests.helpers import build_event


@pytest.fixture
def client():
    reset_state(seed=True)
    with TestClient(main.app) as c:  # lifespan：后台 worker 启动
        yield c


def _post_event(client, event_id, responses=None):
    if responses is not None:
        client.post("/admin/mock/script", json={
            "url": "mock://partner-a/order-hook", "responses": responses})
    ts = str(int(time.time()))
    body = build_event(event_id)
    raw = json.dumps(body, separators=(",", ":")).encode()
    h = {"X-Tenant-Id": "tenant-a", "X-Key-Id": "key-a-new",
         "X-Timestamp": ts, "X-Signature": sign("secret-new", ts, raw)}
    return client.post("/v1/events/inbound", content=raw, headers=h)


def _wait_status(client, event_id, status, timeout=6.0):
    import time as _t
    deadline = _t.time() + timeout
    while _t.time() < deadline:
        d = client.get("/admin/deliveries").json()
        rec = next((x for x in d if x["event_id"] == event_id), None)
        if rec and rec["status"] == status:
            return rec
        _t.sleep(0.02)
    raise AssertionError(f"{event_id} not {status}")


PAUSE = "/admin/tenants/tenant-a/subscriptions/sub-a-orders"


def test_pause_resume_state_http(client):
    # 不存在的订阅 -> 404
    r = client.post("/admin/tenants/tenant-a/subscriptions/nope/pause")
    assert r.status_code == 404

    r1 = client.post(f"{PAUSE}/pause")
    assert r1.status_code == 200 and r1.json()["state"] == "PAUSED"
    # 重复暂停幂等
    r1b = client.post(f"{PAUSE}/pause")
    assert r1b.json()["state"] == "PAUSED"

    st = client.get(f"{PAUSE}/state").json()
    assert st["state"] == "PAUSED"

    # 暂停期间入站照常 202
    r = _post_event(client, "evt-http-paused")
    assert r.status_code == 202
    time.sleep(0.15)  # worker 在跑，但订阅暂停不应发出
    d = client.get("/admin/deliveries").json()
    rec = next(x for x in d if x["event_id"] == "evt-http-paused")
    assert rec["status"] == "SCHEDULED"

    r2 = client.post(f"{PAUSE}/resume")
    assert r2.status_code == 200 and r2.json()["state"] == "ACTIVE"
    rec = _wait_status(client, "evt-http-paused", "DELIVERED")
    print("\n[HTTP] 暂停期间受理不投递，恢复后自动投递:", rec["status"])


def test_single_replay_http_idempotent(client):
    # 编排持续 500 -> worker 自动跑成死信
    _post_event(client, "evt-http-rp", responses=[[500, "x"]] * 4)
    rec = _wait_status(client, "evt-http-rp", "DEAD_LETTER")
    assert rec["fail_reason"] == "ATTEMPTS_EXHAUSTED"

    url = (f"{PAUSE}/events/evt-http-rp/replay")
    # 默认回调现在返回 200
    client.post("/admin/mock/script",
                json={"url": "mock://partner-a/order-hook", "responses": []})
    r1 = client.post(url)
    assert r1.status_code == 202 and r1.json()["outcome"] == "REPLAYED"
    assert r1.json()["replayed"] is True
    # 立即重复 -> 200 ALREADY_REPLAYING
    r2 = client.post(url)
    assert r2.status_code == 200 and r2.json()["outcome"] == "ALREADY_REPLAYING"

    rec = _wait_status(client, "evt-http-rp", "DELIVERED")
    # 成功后再重放 -> 409 ALREADY_DELIVERED
    r3 = client.post(url)
    assert r3.status_code == 409 and r3.json()["outcome"] == "ALREADY_DELIVERED"
    print("\n[HTTP] 单条重放 REPLAYED->ALREADY_REPLAYING->ALREADY_DELIVERED")


def test_batch_replay_http(client):
    # 同一回调 FIFO：两个事件各 4 次 500，需一次性编排 8 个 500
    client.post("/admin/mock/script", json={
        "url": "mock://partner-a/order-hook", "responses": [[500, "x"]] * 8})
    _post_event(client, "evt-b1")
    _post_event(client, "evt-b2")
    _wait_status(client, "evt-b1", "DEAD_LETTER")
    _wait_status(client, "evt-b2", "DEAD_LETTER")

    r = client.post(f"{PAUSE}/replay-dead-letters")
    assert r.status_code == 200
    body = r.json()
    assert body["replayed"] == 2
    assert [x["event_id"] for x in body["results"]] == ["evt-b1", "evt-b2"]
    _wait_status(client, "evt-b1", "DELIVERED")
    _wait_status(client, "evt-b2", "DELIVERED")
    print("\n[HTTP] 批量重放:", [x["outcome"] for x in body["results"]])


def test_credential_dead_letter_distinguishable_http(client):
    # 直接把订阅的出站凭据改成已过期的那把，再投事件
    client.put("/admin/tenants/tenant-a/subscriptions", json={
        "subscription_id": "sub-a-orders",
        "event_type": "order.created",
        "version": "1.0.0",
        "callback_url": "mock://partner-a/order-hook",
        "credential_key_id": "key-a-expired",
        "max_attempts": 4,
    })
    _post_event(client, "evt-http-cred")
    rec = _wait_status(client, "evt-http-cred", "DEAD_LETTER")
    assert rec["fail_reason"] == "CREDENTIAL_EXPIRED"
    assert "no request sent" in rec["fail_detail"]
    print("\n[HTTP] 出站凭据过期死信 reason=", rec["fail_reason"])
