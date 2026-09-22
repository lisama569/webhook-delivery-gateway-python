"""可观测性：日志必须打印事件标识与判定依据；失败链路可查询且原因可区分。"""
from __future__ import annotations

import logging


def test_logs_contain_event_id_and_verdict_basis(client, caplog):
    g = client.gateway
    g.create_tenant("acme", "key-acme-123456")
    g.add_credential("key-acme-123456", "k1", "secret-acme-1234")
    g.create_subscription("key-acme-123456", "order.created",
                          "http://mock/flaky")
    g.configure_mock("http://mock/flaky", fail_times=1, fail_status=502)

    with caplog.at_level(logging.INFO):
        g.ingest("key-acme-123456", "evt-log-1")
        g.tick()   # 失败重试
        g.tick()   # 成功

    text = "\n".join(r.getMessage() for r in caplog.records)
    # 入站日志：事件标识 + 判定结论 + 判定依据
    assert "event=evt-log-1" in text
    assert "verdict=ACCEPTED" in text
    assert "basis=unique_constraint_won" in text
    # 投递日志：事件标识 + 每次判定依据
    assert "verdict=RETRY" in text
    assert "reason=NON_2XX" in text
    assert "verdict=SUCCEEDED" in text
    assert "basis=receiver_2xx" in text


def test_duplicate_log_contains_distinct_basis(client, caplog):
    g = client.gateway
    g.create_tenant("acme", "key-acme-123456")
    body = {"event_id": "evt-log-2", "event_type": "t",
            "event_version": 1, "data": {}}
    with caplog.at_level(logging.INFO):
        client.post("/v1/events", headers={"X-API-Key": "key-acme-123456"}, json=body)
        client.post("/v1/events", headers={"X-API-Key": "key-acme-123456"}, json=body)
    text = "\n".join(r.getMessage() for r in caplog.records
                     if r.name.startswith("gateway"))
    assert "verdict=ACCEPTED" in text
    assert "verdict=DUPLICATE" in text
    assert "basis=unique_constraint_lost_or_repeated_request" in text


def test_dead_letter_query_explains_final_and_last_reason(client, caplog):
    g = client.gateway
    g.create_tenant("acme", "key-acme-123456")
    g.add_credential("key-acme-123456", "k1", "secret-acme-1234")
    g.create_subscription("key-acme-123456", "order.created",
                          "http://mock/boom")
    g.configure_mock("http://mock/boom", fail_times=99, fail_status=500,
                     transport_error=False)
    with caplog.at_level(logging.INFO):
        g.ingest("key-acme-123456", "evt-log-3")
        for _ in range(5):
            g.tick()
    dead = g.dead_letters("key-acme-123456")
    assert len(dead) == 1
    assert dead[0]["fail_reason"] == "ATTEMPTS_EXHAUSTED"  # 终因
    assert "NON_2XX" in dead[0]["detail"]                  # 最后一次直接原因
    log_text = "\n".join(r.getMessage() for r in caplog.records)
    assert "event=evt-log-3" in log_text and "verdict=DEAD_LETTER" in log_text
    assert "basis=attempts_exhausted" in log_text
