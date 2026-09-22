"""凭据轮换：轮换期间新旧凭据都能通过校验；过期凭据被明确拒绝。"""
from __future__ import annotations

from datetime import timedelta

from tests.conftest import T0


def _seed(client):
    g = client.gateway
    g.create_tenant("acme", "key-acme-123456")
    g.create_subscription("key-acme-123456", "order.created",
                          "http://mock/partner")
    g.configure_mock("http://mock/partner",
                     accept_key_ids=["k-old", "k-new"])


def test_old_and_new_credentials_both_validate(client):
    """轮换窗口：接收方同时接受新旧 key；投递方先后用新/旧签名都成功。"""
    g = client.gateway
    _seed(client)
    # 旧凭据：30 天前生效，1 天后才到期（仍在有效期）
    g.add_credential("key-acme-123456", "k-old", "secret-old-1234",
                     valid_from=T0 - timedelta(days=30),
                     valid_to=T0 + timedelta(days=1))
    # 新凭据：1 天前生效，暂不过期
    g.add_credential("key-acme-123456", "k-new", "secret-new-1234",
                     valid_from=T0 - timedelta(days=1), valid_to=None)

    creds = client.get("/admin/credentials",
                       headers={"X-API-Key": "key-acme-123456"}).json()
    assert {c["key_id"] for c in creds} == {"k-old", "k-new"}
    assert all(c["is_active"] for c in creds)
    assert all(not c["is_expired"] for c in creds)

    # 1) 默认使用最新凭据 k-new 签名 -> 接收方接受
    g.ingest("key-acme-123456", "evt-new-key")
    g.tick()
    d = g.deliveries("key-acme-123456", "evt-new-key")[0]
    assert d["state"] == "SUCCEEDED"
    assert d["history"][0]["signed_with_key_id"] == "k-new"

    # 2) 轮换期投递方仍可用旧凭据 k-old 签名 -> 接收方同样接受
    g.set_signing_key("key-acme-123456", "k-old")
    g.ingest("key-acme-123456", "evt-old-key")
    g.tick()
    d = g.deliveries("key-acme-123456", "evt-old-key")[0]
    assert d["state"] == "SUCCEEDED"
    assert d["history"][0]["signed_with_key_id"] == "k-old"

    # 接收端确实分别收到两把 key 的签名
    received = g.mock_received("http://mock/partner")
    used = {r["headers"]["X-Signature-Key-Id"] for r in received}
    assert used == {"k-new", "k-old"}


def test_expired_credential_rejected(client):
    """旧凭据到期后：管理面标记过期，出站不得再使用，必须给出可解释失败。"""
    g = client.gateway
    _seed(client)
    g.add_credential("key-acme-123456", "k-old", "secret-old-1234",
                     valid_from=T0 - timedelta(days=60),
                     valid_to=T0 - timedelta(days=1))  # 已过期（超过 0 容差）
    g.add_credential("key-acme-123456", "k-new", "secret-new-1234",
                     valid_from=T0 - timedelta(days=1), valid_to=None)

    creds = client.get("/admin/credentials",
                       headers={"X-API-Key": "key-acme-123456"}).json()
    by_id = {c["key_id"]: c for c in creds}
    assert by_id["k-old"]["is_expired"] is True
    assert by_id["k-old"]["is_active"] is False
    assert by_id["k-new"]["is_active"] is True

    # 显式要求用已过期的旧 key 签名 -> 投递被拒，原因明确且进入重试而非静默
    g.set_signing_key("key-acme-123456", "k-old")
    g.ingest("key-acme-123456", "evt-exp-key")
    for _ in range(5):
        g.tick()
    d = g.deliveries("key-acme-123456", "evt-exp-key")[0]
    assert d["state"] == "DEAD_LETTER"  # 持续无有效凭据 -> 不静默丢失，进死信
    assert d["fail_reason"] == "TRANSPORT_ERROR"
    assert all("no active signing credential" in h["detail"]
               for h in d["history"])
    dead = g.dead_letters("key-acme-123456")
    assert len(dead) == 1 and dead[0]["event_id"] == "evt-exp-key"

    # 完成轮换（切回新凭据）后，后续事件立即恢复正常投递
    g.set_signing_key("key-acme-123456", None)
    g.ingest("key-acme-123456", "evt-recovered")
    g.tick()
    d2 = g.deliveries("key-acme-123456", "evt-recovered")[0]
    assert d2["state"] == "SUCCEEDED"
    assert d2["history"][0]["signed_with_key_id"] == "k-new"


def test_receiver_rejects_rotated_out_key(client):
    """即使投递方凭据本地未过期，接收方轮换后拒绝旧 key（401），原因可区分。"""
    g = client.gateway
    g.create_tenant("acme", "key-acme-123456")
    g.add_credential("key-acme-123456", "k-old", "secret-old-1234",
                     valid_from=T0 - timedelta(days=30),
                     valid_to=T0 + timedelta(days=1))
    g.add_credential("key-acme-123456", "k-new", "secret-new-1234",
                     valid_from=T0 - timedelta(days=1))
    g.create_subscription("key-acme-123456", "order.created",
                          "http://mock/partner")
    # 接收方已完成轮换：只接受 k-new，显式拒绝 k-old
    g.configure_mock("http://mock/partner", accept_key_ids=["k-new"],
                     reject_key_ids=["k-old"])

    g.set_signing_key("key-acme-123456", "k-old")
    g.ingest("key-acme-123456", "evt-rotated")
    g.tick()
    d = g.deliveries("key-acme-123456", "evt-rotated")[0]
    assert d["state"] == "PENDING"
    assert d["fail_reason"] == "NON_2XX"
    assert d["history"][0]["http_status"] == 401
    assert "rejected credential key_id=k-old" in d["history"][0]["detail"]
