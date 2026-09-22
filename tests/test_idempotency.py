"""并发重复投递：同一 (tenant, event_id) 只受理一次，重复结论稳定可区分。"""
import asyncio
import json

import pytest

from app.models import utcnow
from tests.helpers import build_event, raw_body, signed_headers


async def _post(state, event_body, secret="secret-new", key_id="key-a-new"):
    ts = int(utcnow().timestamp())
    headers = signed_headers(secret, event_body, key_id, ts)
    return await state.service.accept(
        tenant_id=headers["X-Tenant-Id"],
        key_id=headers["X-Key-Id"],
        timestamp=headers["X-Timestamp"],
        signature=headers["X-Signature"],
        raw_body=raw_body(event_body),
        parsed=event_body,
    )


@pytest.mark.asyncio
async def test_concurrent_duplicates_processed_once(state):
    body = build_event("evt-concurrent-1")
    n = 20
    barrier = asyncio.Barrier(n)

    async def call(i):
        async with barrier:  # 尽可能同时涌入
            return await _post(state, body)

    results = await asyncio.gather(*(call(i) for i in range(n)))

    firsts = [r for r in results if not r.duplicate]
    dups = [r for r in results if r.duplicate]
    print(f"\n[event=evt-concurrent-1] 并发 {n} 次: 首次受理={len(firsts)} "
          f"重复={len(dups)} 首次状态码={firsts[0].status_code} "
          f"重复结论={dups[0].detail!r}")

    assert len(firsts) == 1, "必须恰好处理一次"
    assert len(dups) == n - 1
    assert firsts[0].status_code == 202
    # 重复请求返回稳定、可区分的结论
    assert all(d.status_code == 200 and d.duplicate for d in dups)
    assert all("original status=" in d.detail for d in dups)
    # 只产生一条投递任务
    deliveries = state.storage.list_deliveries(tenant_id="tenant-a")
    assert len(deliveries) == 1

    event = await state.storage.get_event("tenant-a", "evt-concurrent-1")
    assert event is not None


@pytest.mark.asyncio
async def test_sequential_duplicate_stable(state):
    body = build_event("evt-seq-1")
    r1 = await _post(state, body)
    r2 = await _post(state, body)
    print(f"\n[event=evt-seq-1] 首次 accepted={r1.accepted} duplicate={r1.duplicate} "
          f"| 重复 duplicate={r2.duplicate} detail={r2.detail!r}")
    assert (r1.accepted, r1.duplicate) == (True, False)
    assert (r2.accepted, r2.duplicate) == (True, True)


@pytest.mark.asyncio
async def test_same_event_id_different_tenants_isolated(state):
    # 相同 event_id 在不同租户下互不影响
    body_a = build_event("evt-shared-id")
    body_b = build_event("evt-shared-id", payload={"order_id": "b-order"})

    async def post_b():
        ts = int(utcnow().timestamp())
        headers = signed_headers("secret-b", body_b, "key-b-1", ts,
                                 tenant_id="tenant-b")
        return await state.service.accept(
            "tenant-b", headers["X-Key-Id"], headers["X-Timestamp"],
            headers["X-Signature"], raw_body(body_b), body_b,
        )

    ra, rb = await asyncio.gather(_post(state, body_a), post_b())
    print(f"\n[event=evt-shared-id] 租户隔离: tenant-a duplicate={ra.duplicate} "
          f"tenant-b duplicate={rb.duplicate}")
    assert ra.duplicate is False and rb.duplicate is False
    assert (await state.storage.get_event("tenant-a", "evt-shared-id")) is not None
    assert (await state.storage.get_event("tenant-b", "evt-shared-id")) is not None
