"""重试退避、耗尽入死信、同订阅顺序保证、失败原因可区分。"""
from __future__ import annotations

from app.config import Settings
from app.delivery import compute_backoff
from app.models import FailReason


def _seed(client, url="http://mock/orders", **sub_kw):
    client.gateway.create_tenant("acme", "key-acme-123456")
    client.gateway.add_credential("key-acme-123456", "k1", "secret-acme-1234")
    client.gateway.create_subscription(
        "key-acme-123456", "order.created", url, **sub_kw)


def test_backoff_schedule_is_deterministic_and_explainable():
    s = Settings(backoff_base_seconds=1.0, backoff_factor=2.0,
                 backoff_cap_seconds=300.0)
    # 第 n 次重试等待 min(base*factor^(n-1), cap)：1,2,4,8...
    assert [compute_backoff(n, s).total_seconds() for n in range(1, 5)] == [1, 2, 4, 8]
    capped = Settings(backoff_base_seconds=10.0, backoff_factor=10.0,
                      backoff_cap_seconds=50.0)
    assert compute_backoff(3, capped).total_seconds() == 50.0


def test_retry_then_success(client):
    _seed(client)
    # 前 3 次 503，之后成功
    client.gateway.configure_mock("http://mock/orders", fail_times=3, fail_status=503)
    r = client.gateway.ingest("key-acme-123456", "evt-retry")
    assert r.status_code == 201
    dels = client.gateway.deliveries("key-acme-123456", "evt-retry")
    did = dels[0]["id"]

    # 退避基数为 0：连续 tick 即可驱动完重试
    client.gateway.tick()  # 1 -> fail
    d1 = client.gateway.delivery("key-acme-123456", did)
    assert d1["state"] == "PENDING" and d1["attempts"] == 1
    assert d1["fail_reason"] == FailReason.NON_2XX.value
    assert d1["history"][0]["http_status"] == 503

    client.gateway.tick()  # 2 -> fail
    client.gateway.tick()  # 3 -> fail
    client.gateway.tick()  # 4 -> success
    d4 = client.gateway.delivery("key-acme-123456", did)
    assert d4["state"] == "SUCCEEDED", d4
    assert d4["attempts"] == 4
    assert [h["http_status"] for h in d4["history"]] == [503, 503, 503, 200]
    assert d4["history"][-1]["signed_with_key_id"] == "k1"

    received = client.gateway.mock_received("http://mock/orders")
    assert [r["attempt"] for r in received] == [1, 2, 3, 4]


def test_transport_error_reason_is_distinct_from_non_2xx(client):
    _seed(client, url="http://mock/down")
    # 网络异常（无 HTTP 状态码）与业务非 2xx 必须可区分
    client.gateway.configure_mock("http://mock/down", fail_times=1,
                                  transport_error=True)
    client.gateway.ingest("key-acme-123456", "evt-net")
    client.gateway.tick()
    d = client.gateway.deliveries("key-acme-123456", "evt-net")[0]
    assert d["fail_reason"] == FailReason.TRANSPORT_ERROR.value
    assert d["history"][0]["http_status"] is None
    assert "connection error" in d["history"][0]["detail"]


def test_retries_exhausted_goes_to_dead_letter(client):
    _seed(client, url="http://mock/always-fail")
    client.gateway.configure_mock("http://mock/always-fail",
                                  fail_times=100, fail_status=500)
    client.gateway.ingest("key-acme-123456", "evt-dead")

    for _ in range(5):  # max_attempts=5
        client.gateway.tick()

    d = client.gateway.deliveries("key-acme-123456", "evt-dead")[0]
    assert d["state"] == "DEAD_LETTER"
    assert d["attempts"] == 5
    assert d["fail_reason"] == FailReason.NON_2XX.value
    # 超过上限后不再投递
    client.gateway.tick()
    assert len(client.gateway.mock_received("http://mock/always-fail")) == 5

    dead = client.gateway.dead_letters("key-acme-123456")
    assert len(dead) == 1
    row = dead[0]
    assert row["event_id"] == "evt-dead"
    assert row["fail_reason"] == FailReason.ATTEMPTS_EXHAUSTED.value
    assert "NON_2XX" in row["detail"]  # 死信记录保留最后一次失败原因


def test_order_preserved_within_subscription(client):
    _seed(client)
    client.gateway.configure_mock("http://mock/orders")
    for i in range(3):
        client.gateway.ingest("key-acme-123456", f"evt-seq-{i}")

    # 每个 tick 每个订阅只放行队头一条：严格 1 -> 2 -> 3
    for expected_seq in range(1, 4):
        processed = client.gateway.tick()
        assert processed == 1
        received = client.gateway.mock_received("http://mock/orders")
        assert received[-1]["headers"]["X-Delivery-Seq"] == str(expected_seq)

    # 数据层 seq 与 event 对应关系一致
    for i in range(3):
        d = client.gateway.deliveries("key-acme-123456", f"evt-seq-{i}")[0]
        assert d["seq"] == i + 1 and d["state"] == "SUCCEEDED"


def test_head_of_line_blocking_when_head_fails(client):
    """队头在失败重试期间，后续事件不得越过它先投递；队头成功后严格按序放行。"""
    _seed(client, url="http://mock/hol")
    client.gateway.configure_mock("http://mock/hol", fail_times=2, fail_status=503)
    client.gateway.ingest("key-acme-123456", "evt-first")
    client.gateway.ingest("key-acme-123456", "evt-second")

    # seq1 需要 3 次尝试才成功；这期间 seq2 一次都不允许发出
    client.gateway.tick()  # seq1 attempt1 fail
    received = client.gateway.mock_received("http://mock/hol")
    assert len(received) == 1 and "evt-first" in received[0]["body"]
    client.gateway.tick()  # seq1 attempt2 fail
    received = client.gateway.mock_received("http://mock/hol")
    assert len(received) == 2 and all("evt-first" in r["body"] for r in received)
    client.gateway.tick()  # seq1 attempt3 success
    received = client.gateway.mock_received("http://mock/hol")
    assert len(received) == 3 and all("evt-first" in r["body"] for r in received)

    # seq1 完成后 seq2 才被放行，绝不乱序
    client.gateway.tick()
    received = client.gateway.mock_received("http://mock/hol")
    assert len(received) == 4
    assert "evt-second" in received[-1]["body"]
    assert received[-1]["headers"]["X-Delivery-Seq"] == "2"

    # 数据状态一致：first=seq1 已成功，second=seq2 已成功
    d1 = client.gateway.deliveries("key-acme-123456", "evt-first")[0]
    d2 = client.gateway.deliveries("key-acme-123456", "evt-second")[0]
    assert (d1["seq"], d1["state"]) == (1, "SUCCEEDED")
    assert (d2["seq"], d2["state"]) == (2, "SUCCEEDED")
