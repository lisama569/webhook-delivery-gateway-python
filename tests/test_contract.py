"""事件契约兼容演进：版本区间、未知字段保留、未知版本不崩溃/不静默。"""
from __future__ import annotations


def _seed(client):
    g = client.gateway
    g.create_tenant("acme", "key-acme-123456")
    g.add_credential("key-acme-123456", "k1", "secret-acme-1234")
    g.configure_mock("http://mock/forward")
    g.configure_mock("http://mock/strict")


def test_unknown_fields_are_kept_and_marked(client):
    """未知字段绝不丢弃：随投递送达，并在投递记录中显式列出。"""
    g = client.gateway
    _seed(client)
    g.create_subscription("key-acme-123456", "order.created",
                          "http://mock/forward", min_version=1, max_version=2,
                          known_fields=["order_id", "amount"])
    r = g.ingest("key-acme-123456", "evt-uf", version=2,
                 data={"order_id": "o1", "amount": 100, "future_field": {"x": 1}})
    assert r.status_code == 201
    g.tick()
    d = g.deliveries("key-acme-123456", "evt-uf")[0]
    assert d["state"] == "SUCCEEDED"
    assert d["unknown_fields"] == ["future_field"]
    # 接收端确实收到了未知字段（没有被过滤）
    import json
    body = json.loads(g.mock_received("http://mock/forward")[0]["body"])
    assert body["payload"]["future_field"] == {"x": 1}
    assert body["unknown_fields"] == ["future_field"]


def test_newer_version_delivered_anyway_by_default(client):
    """前向兼容默认策略：超出声明版本仍投递并标注，服务不报错。"""
    g = client.gateway
    _seed(client)
    g.create_subscription("key-acme-123456", "order.created",
                          "http://mock/forward", min_version=1, max_version=1,
                          policy="DELIVER_ANYWAY")
    r = g.ingest("key-acme-123456", "evt-v9", version=9)
    assert r.status_code == 201
    g.tick()
    d = g.deliveries("key-acme-123456", "evt-v9")[0]
    assert d["state"] == "SUCCEEDED"  # 没有被静默丢弃
    assert g.dead_letters("key-acme-123456") == []


def test_newer_version_quarantine(client):
    """严格策略：超出 max_version 的新版本进入可查询死信，原因明确。"""
    g = client.gateway
    _seed(client)
    g.create_subscription("key-acme-123456", "order.created",
                          "http://mock/strict", min_version=1, max_version=1,
                          policy="QUARANTINE")
    r = g.ingest("key-acme-123456", "evt-ahead", version=3)
    assert r.status_code == 201
    # 契约判定在受理时完成：直接 DEAD_LETTER，且根本不会尝试外发
    ref = r.json()["deliveries"][0]
    assert ref["state"] == "DEAD_LETTER"
    assert ref["fail_reason"] == "CONTRACT_VERSION_AHEAD"
    g.tick()
    assert g.mock_received("http://mock/strict") == []
    dead = g.dead_letters("key-acme-123456")
    assert len(dead) == 1
    assert dead[0]["fail_reason"] == "CONTRACT_VERSION_AHEAD"
    assert "exceeds subscriber max_version 1" in dead[0]["detail"]


def test_older_version_rejected_explicitly(client):
    """发送方版本比订阅 min_version 更旧：明确拒绝并入死信，不崩溃、不外发。"""
    g = client.gateway
    _seed(client)
    g.create_subscription("key-acme-123456", "order.created",
                          "http://mock/strict", min_version=3, max_version=5,
                          policy="QUARANTINE")
    r = g.ingest("key-acme-123456", "evt-behind", version=2)
    assert r.status_code == 201
    ref = r.json()["deliveries"][0]
    assert ref["state"] == "DEAD_LETTER"
    assert ref["fail_reason"] == "CONTRACT_VERSION_BEHIND"
    g.tick()
    assert g.mock_received("http://mock/strict") == []
    dead = g.dead_letters("key-acme-123456")
    assert dead[0]["fail_reason"] == "CONTRACT_VERSION_BEHIND"
    assert "older than subscriber min_version 3" in dead[0]["detail"]


def test_unknown_event_type_is_accepted_but_not_routed(client):
    """没有任何订阅的事件类型：正常受理（不报错、不丢事件），只是无投递。"""
    g = client.gateway
    _seed(client)
    r = g.ingest("key-acme-123456", "evt-nobody", event_type="totally.new.type")
    assert r.status_code == 201
    assert r.json()["deliveries"] == []
    g.tick()
    assert g.dead_letters("key-acme-123456") == []


def test_failure_reasons_are_distinguishable(client):
    """两类契约失败 + 重试耗尽在死信链路中始终可用枚举区分。"""
    g = client.gateway
    _seed(client)
    g.create_subscription("key-acme-123456", "behind.type", "http://mock/strict",
                          min_version=5)
    g.create_subscription("key-acme-123456", "ahead.type", "http://mock/strict",
                          min_version=1, max_version=1, policy="QUARANTINE")
    g.ingest("key-acme-123456", "b", event_type="behind.type", version=1)
    g.ingest("key-acme-123456", "a", event_type="ahead.type", version=2)
    dead = g.dead_letters("key-acme-123456")
    reasons = {x["event_id"]: x["fail_reason"] for x in dead}
    assert reasons == {"b": "CONTRACT_VERSION_BEHIND",
                       "a": "CONTRACT_VERSION_AHEAD"}
