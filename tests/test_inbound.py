"""入站认证、租户隔离、配额、未知版本/字段的端到端受理判定。"""
import pytest

from app.bootstrap import reset_state
from app.models import (
    Credential,
    EventSchema,
    FieldSpec,
    FailReason,
    Subscription,
    Tenant,
    utcnow,
)
from tests.helpers import build_event, raw_body, signed_headers

# 种子凭据按真实时间装配（见 bootstrap.seed_demo_data），因此判定时钟也锚定
# 当前时刻，避免固定日期随时间漂移导致凭据窗口失配。
T0 = utcnow()


async def _accept(state, body, secret, key_id, tenant_id="tenant-a", now=T0):
    ts = int(now.timestamp())
    headers = signed_headers(secret, body, key_id, ts, tenant_id=tenant_id)
    return await state.service.accept(
        tenant_id, headers["X-Key-Id"], headers["X-Timestamp"],
        headers["X-Signature"], raw_body(body), body, now=now,
    )


@pytest.mark.asyncio
async def test_expired_credential_rejected_at_inbound(state):
    body = build_event("evt-auth-expired")
    result = await _accept(state, body, "secret-expired", "key-a-expired")
    print(f"\n[event=evt-auth-expired] 过期凭据受理判定 accepted={result.accepted} "
          f"http={result.status_code} reason={result.reason.value}")
    assert not result.accepted
    assert result.reason is FailReason.EXPIRED_CREDENTIAL
    assert result.status_code == 401
    # 拒绝不得落库
    assert await state.storage.get_event("tenant-a", "evt-auth-expired") is None


@pytest.mark.asyncio
async def test_rotation_overlap_both_credentials_accepted(state):
    for key_id, secret in (("key-a-old", "secret-old"),
                           ("key-a-new", "secret-new")):
        body = build_event(f"evt-auth-{key_id}")
        result = await _accept(state, body, secret, key_id)
        print(f"\n[event=evt-auth-{key_id}] 轮换重叠期受理 accepted={result.accepted} "
              f"http={result.status_code}")
        assert result.accepted and result.status_code == 202


@pytest.mark.asyncio
async def test_signature_with_other_tenant_secret_fails(state):
    # 跨租户凭据不能互用
    body = build_event("evt-cross-tenant")
    result = await _accept(state, body, "secret-b", "key-b-1",
                           tenant_id="tenant-a")
    print(f"\n[event=evt-cross-tenant] 他租户凭据判定 reason={result.reason.value}")
    assert not result.accepted
    assert result.reason is FailReason.INVALID_SIGNATURE


@pytest.mark.asyncio
async def test_unknown_tenant(state):
    body = build_event("evt-no-tenant")
    result = await _accept(state, body, "x", "x", tenant_id="ghost")
    assert result.reason is FailReason.UNKNOWN_TENANT


@pytest.mark.asyncio
async def test_quota_isolated_per_tenant(state):
    # tenant-b 限额 5/分钟，发送 7 个不同事件；tenant-a 不受影响
    results_b = []
    for i in range(7):
        body = build_event(f"evt-b-{i}", payload={"order_id": f"b{i}"})
        results_b.append(
            await _accept(state, body, "secret-b", "key-b-1",
                          tenant_id="tenant-b")
        )
    accepted = [r for r in results_b if r.accepted]
    rejected = [r for r in results_b if not r.accepted]
    print(f"\n[event=evt-b-*] tenant-b 限额5: 受理={len(accepted)} 拒绝={len(rejected)} "
          f"拒绝原因={rejected[0].reason.value}")
    assert len(accepted) == 5
    assert all(r.reason is FailReason.QUOTA_EXCEEDED and r.status_code == 429
               for r in rejected)

    # tenant-a 仍可受理（额度独立）
    body = build_event("evt-a-after-b-quota")
    result = await _accept(state, body, "secret-new", "key-a-new")
    print(f"\n[event=evt-a-after-b-quota] tenant-a 不受tenant-b限流影响 "
          f"accepted={result.accepted}")
    assert result.accepted


@pytest.mark.asyncio
async def test_unknown_event_version_rejected_not_crashing(state):
    body = build_event("evt-v9", version="9.0.0")
    result = await _accept(state, body, "secret-new", "key-a-new")
    print(f"\n[event=evt-v9] 未知主版本判定 reason={result.reason.value} "
          f"http={result.status_code}")
    assert not result.accepted
    assert result.reason is FailReason.UNKNOWN_EVENT_VERSION
    assert result.status_code == 422


@pytest.mark.asyncio
async def test_minor_version_unknown_fields_retained(state):
    # 发送 1.3.0（主版本在注册表内），契约未知字段保留且事件正常投递
    body = build_event(
        "evt-v13", version="1.3.0",
        payload={"order_id": "o-13", "brand_new": "kept"},
    )
    result = await _accept(state, body, "secret-new", "key-a-new")
    print(f"\n[event=evt-v13] 次版本+未知字段 accepted={result.accepted} "
          f"unknown_fields={result.unknown_fields} "
          f"schema_version_used={result.schema_version_used}")
    assert result.accepted
    assert result.unknown_fields == ["brand_new"]
    event = await state.storage.get_event("tenant-a", "evt-v13")
    assert event.payload["brand_new"] == "kept"  # 不丢弃
