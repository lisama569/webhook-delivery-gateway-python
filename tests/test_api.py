"""HTTP 端到端：签名入站 -> mock 故障编排 -> pump 驱动 -> 死信查询。"""
import json

import pytest
from fastapi.testclient import TestClient

import main
from app.bootstrap import reset_state
from app.security import sign
from tests.helpers import build_event


@pytest.fixture
def client():
    reset_state(seed=True)
    return TestClient(main.app)


def _post_event(client, body, secret="secret-new", key_id="key-a-new",
                tenant_id="tenant-a"):
    raw = json.dumps(body, separators=(",", ":")).encode()
    headers = {
        "X-Tenant-Id": tenant_id,
        "X-Key-Id": key_id,
        "X-Timestamp": "1789000000",  # 固定在种子凭据有效期内（仅验签窗口校验）
    }
    headers["X-Signature"] = sign(secret, headers["X-Timestamp"], raw)
    return client.post("/v1/events/inbound", content=raw, headers=headers)


def test_full_flow_duplicate_and_dead_letter(client):
    # 固定时间戳与服务器时间偏差需 <=300s；种子凭据有效期覆盖当前真实时间
    import time
    ts = str(int(time.time()))

    def post(body, secret="secret-new", key_id="key-a-new", tenant_id="tenant-a"):
        raw = json.dumps(body, separators=(",", ":")).encode()
        h = {"X-Tenant-Id": tenant_id, "X-Key-Id": key_id,
             "X-Timestamp": ts,
             "X-Signature": sign(secret, ts, raw)}
        return client.post("/v1/events/inbound", content=raw, headers=h)

    # 1. 编排回调：持续 500 -> 重试耗尽
    r = client.post("/admin/mock/script", json={
        "url": "mock://partner-a/order-hook",
        "responses": [[500, "fail"], [500, "fail"], [500, "fail"], [500, "fail"]],
    })
    assert r.status_code == 200

    body = build_event("evt-http-1")
    r1 = post(body)
    assert r1.status_code == 202
    data1 = r1.json()
    assert data1["duplicate"] is False and data1["deliveries"] == ["sub-a-orders"]

    # 2. 立即重复：稳定可区分结论
    r2 = post(body)
    assert r2.status_code == 200
    data2 = r2.json()
    assert data2["duplicate"] is True
    print(f"\n[event=evt-http-1] 首次={r1.status_code}/duplicate=False "
          f"重复={r2.status_code}/duplicate=True detail={data2['detail']}")

    # 3. 等待真实退避（0.05+0.1+0.2s）后 drain
    rd = client.post("/admin/drain")
    assert rd.status_code == 200

    dlq = client.get("/admin/deliveries/dead-letter").json()
    mine = [d for d in dlq if d["event_id"] == "evt-http-1"]
    assert len(mine) == 1
    assert mine[0]["fail_reason"] == "ATTEMPTS_EXHAUSTED"
    assert len(mine[0]["attempts"]) == 4
    print(f"[event=evt-http-1] 死信可查询 reason={mine[0]['fail_reason']} "
          f"attempts={len(mine[0]['attempts'])} detail={mine[0]['fail_detail']}")


def test_expired_credential_http_401(client):
    import time
    ts = str(int(time.time()))
    body = build_event("evt-http-exp")
    raw = json.dumps(body, separators=(",", ":")).encode()
    h = {"X-Tenant-Id": "tenant-a", "X-Key-Id": "key-a-expired",
         "X-Timestamp": ts,
         "X-Signature": sign("secret-expired", ts, raw)}
    r = client.post("/v1/events/inbound", content=raw, headers=h)
    print(f"\n[event=evt-http-exp] HTTP状态={r.status_code} 错误体={r.json()['detail']}")
    assert r.status_code == 401
    assert r.json()["detail"]["reason"] == "EXPIRED_CREDENTIAL"


def test_unknown_version_http_422(client):
    import time
    ts = str(int(time.time()))
    body = build_event("evt-http-v9", version="9.0.0")
    raw = json.dumps(body, separators=(",", ":")).encode()
    h = {"X-Tenant-Id": "tenant-a", "X-Key-Id": "key-a-new",
         "X-Timestamp": ts,
         "X-Signature": sign("secret-new", ts, raw)}
    r = client.post("/v1/events/inbound", content=raw, headers=h)
    assert r.status_code == 422
    assert r.json()["detail"]["reason"] == "UNKNOWN_EVENT_VERSION"
