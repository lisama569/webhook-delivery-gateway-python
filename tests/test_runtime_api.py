"""运维视角的 HTTP 端到端：自走投递、暂停/恢复、死信重放。

使用 ``with TestClient(app)`` 触发真实 startup（拉起后台投递循环）与
shutdown（停止循环），验证"服务起来后投递自己跑"，全程不调用 /admin/pump。
"""
from __future__ import annotations

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
    with TestClient(main.app) as c:  # startup -> dispatcher worker 自走
        yield c
    # 退出 with 触发 shutdown -> stop_worker


def _post_event(client, event_id, secret="secret-new", key_id="key-a-new",
                tenant="tenant-a", version="1.0.0", payload=None):
    body = build_event(event_id, version=version,
                       payload=payload or {"order_id": event_id})
    raw = json.dumps(body, separators=(",", ":")).encode()
    ts = str(int(time.time()))
    headers = {
        "X-Tenant-Id": tenant, "X-Key-Id": key_id, "X-Timestamp": ts,
        "X-Signature": sign(secret, ts, raw),
    }
    return client.post("/v1/events/inbound", content=raw, headers=headers)


def _wait_delivery_status(client, event_id, status, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        rows = client.get(
            "/admin/deliveries", params={"tenant_id": "tenant-a"}
        ).json()
        mine = [d for d in rows if d["event_id"] == event_id]
        if mine and mine[0]["status"] == status:
            return mine[0]
        time.sleep(0.02)
    raise AssertionError(f"{event_id} never reached {status}")


def test_service_auto_delivers_after_startup_without_pump(client):
    r = _post_event(client, "evt-auto-1")
    assert r.status_code == 202
    # 不调用 pump/drain：后台循环自动投递成功
    rec = _wait_delivery_status(client, "evt-auto-1", "DELIVERED")
    print(f"\n[event=evt-auto-1] 服务自走投递 status={rec['status']} "
          f"attempts={len(rec['attempts'])}")
    assert rec["status"] == "DELIVERED"
    assert len(rec["attempts"]) == 1


def test_pause_and_resume_endpoints_preserve_order(client):
    sub_id = "sub-a-orders"
    url = "mock://partner-a/order-hook"

    # 1) 暂停（重复暂停稳定）
    p1 = client.post(f"/admin/tenants/tenant-a/subscriptions/{sub_id}/pause")
    p2 = client.post(f"/admin/tenants/tenant-a/subscriptions/{sub_id}/pause")
    assert p1.status_code == 200 and p1.json()["paused"] is True
    assert p2.json()["subscription"]["paused"] is True

    # 2) 暂停期间两条事件照常受理、排队但不发出
    r_a = _post_event(client, "evt-paused-a")
    r_b = _post_event(client, "evt-paused-b")
    assert r_a.status_code == 202 and r_b.status_code == 202
    time.sleep(0.3)  # 留足时间让 worker 空转
    rows = client.get("/admin/deliveries", params={"tenant_id": "tenant-a"}).json()
    held = {d["event_id"]: d for d in rows
            if d["event_id"] in ("evt-paused-a", "evt-paused-b")}
    assert all(d["status"] == "SCHEDULED" for d in held.values())

    # 3) 恢复，worker 自动按 a -> b 顺序补发
    rs = client.post(f"/admin/tenants/tenant-a/subscriptions/{sub_id}/resume")
    assert rs.status_code == 200 and rs.json()["paused"] is False
    da = _wait_delivery_status(client, "evt-paused-a", "DELIVERED")
    db = _wait_delivery_status(client, "evt-paused-b", "DELIVERED")
    assert da["seq"] < db["seq"]
    print(f"\n[sub={sub_id}] 暂停恢复后 seq a={da['seq']} < b={db['seq']}，"
          f"均自动投递成功")


def test_dead_letter_replay_single_and_batch_idempotency(client):
    sub_id = "sub-a-orders"
    url = "mock://partner-a/order-hook"

    # 编排：两条事件各 4 次 500 耗尽；之后 evt-dlq-1 重放先吃一次 500（制造
    # 在途/退避窗口，便于断言并发与连续重放的去重结论），随后两次 200 成功。
    client.post("/admin/mock/script", json={
        "url": url,
        "responses": [[500, "x"]] * 9 + [[200, "ok"], [200, "ok"]],
    })
    _post_event(client, "evt-dlq-1")
    _post_event(client, "evt-dlq-2")
    d1 = _wait_delivery_status(client, "evt-dlq-1", "DEAD_LETTER")
    d2 = _wait_delivery_status(client, "evt-dlq-2", "DEAD_LETTER")
    assert d1["fail_reason"] == "ATTEMPTS_EXHAUSTED"

    # 单条重放：第一次 202 REPLAYED
    rp = client.post(
        f"/admin/tenants/tenant-a/subscriptions/{sub_id}"
        f"/dead-letter/evt-dlq-1/replay"
    )
    assert rp.status_code == 202
    assert rp.json()["outcome"] == "REPLAYED"

    # 立刻重复单条重放：200 ALREADY_IN_FLIGHT（稳定可区分）
    rp2 = client.post(
        f"/admin/tenants/tenant-a/subscriptions/{sub_id}"
        f"/dead-letter/evt-dlq-1/replay"
    )
    assert rp2.status_code == 200
    assert rp2.json()["outcome"] == "ALREADY_IN_FLIGHT"

    # 批量重放：evt-dlq-1 在途 -> ALREADY_IN_FLIGHT；evt-dlq-2 -> REPLAYED
    batch = client.post(
        f"/admin/tenants/tenant-a/subscriptions/{sub_id}/dead-letter/replay"
    ).json()
    replayed = [d["event_id"] for d in batch["replayed"]]
    inflight = [d["event_id"] for d in batch["already_in_flight"]]
    print(f"\n[batch] replayed={replayed} already_in_flight={inflight}")
    assert replayed == ["evt-dlq-2"]
    assert inflight == ["evt-dlq-1"]

    _wait_delivery_status(client, "evt-dlq-1", "DELIVERED")
    _wait_delivery_status(client, "evt-dlq-2", "DELIVERED")

    # 已成功事件再重放 -> 409 NOT_DEAD_LETTER，不会再发
    gone = client.post(
        f"/admin/tenants/tenant-a/subscriptions/{sub_id}"
        f"/dead-letter/evt-dlq-1/replay"
    )
    assert gone.status_code == 409
    assert gone.json()["outcome"] == "NOT_DEAD_LETTER"


def test_replay_unknown_targets_404(client):
    # 不存在的订阅
    r = client.post(
        "/admin/tenants/tenant-a/subscriptions/ghost/dead-letter/x/replay"
    )
    assert r.status_code == 404
    # 存在订阅但无该投递记录
    r2 = client.post(
        "/admin/tenants/tenant-a/subscriptions/sub-a-orders/dead-letter/nope/replay"
    )
    assert r2.status_code == 404


def test_pause_other_subscription_does_not_block_unpaused_one(client):
    """暂停一个订阅不影响其他订阅的自走投递。"""
    # 新建第二个订阅并暂停它；种子订阅 sub-a-orders 必须照常自走
    put = client.put("/admin/tenants/tenant-a/subscriptions", json={
        "subscription_id": "sub-a-paused",
        "event_type": "order.created",
        "version": "1.0.0",
        "callback_url": "mock://partner-a/paused-hook",
        "credential_key_id": "key-a-new",
        "max_attempts": 3,
    })
    assert put.status_code == 200
    client.post("/admin/tenants/tenant-a/subscriptions/sub-a-paused/pause")

    _post_event(client, "evt-other-runs")
    rec = _wait_delivery_status(client, "evt-other-runs", "DELIVERED")
    assert rec["subscription_id"] == "sub-a-orders"
    print(f"\n[event=evt-other-runs] 他订阅暂停时本订阅照常投递={rec['status']}")
