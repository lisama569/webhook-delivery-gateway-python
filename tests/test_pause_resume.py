"""订阅级暂停/恢复：暂停期间入站照常受理并保序排队、恢复后按原顺序投递；
暂停/恢复幂等；订阅间互不拖累，也不影响入站。"""
import json
from datetime import datetime, timedelta, timezone

import pytest

from app.models import EventStatus, InboundEvent, Subscription, SubscriptionState
from tests.helpers import build_event, raw_body, signed_headers

T0 = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)


def _sub(sub_id, url, max_attempts=4):
    return Subscription(sub_id, "tenant-a", "order.created", "1.0.0",
                        url, "key-a-new", max_attempts=max_attempts)


def _bodies_for(state, event_id, url):
    return [c["body"] for c in state.transport.calls
            if event_id.encode() in c["body"] and c["url"] == url]


def _order(state, url):
    return [json.loads(c["body"])["event_id"]
            for c in state.transport.calls if c["url"] == url]


async def _accept_event(state, eid):
    body = build_event(eid)
    ts = str(int(T0.timestamp()))
    h = signed_headers("secret-new", body, "key-a-new", ts, tenant_id="tenant-a")
    return await state.service.accept(
        "tenant-a", h["X-Key-Id"], h["X-Timestamp"], h["X-Signature"],
        raw_body(body), body, now=T0)


async def _enqueue_direct(state, eid, sub):
    ev = InboundEvent(eid, "tenant-a", "order.created", "1.0.0",
                      {"order_id": eid}, T0)
    await state.storage.save_event_if_absent(ev)
    await state.dispatcher.enqueue(eid, sub)


@pytest.mark.asyncio
async def test_pause_holds_new_events_resume_preserves_order(state):
    sub = _sub("sp1", "mock://pause/1")
    await state.storage.upsert_subscription(sub)
    await state.dispatcher.pause_subscription("sp1")

    for eid in ("p-a", "p-b", "p-c"):
        r = await _accept_event(state, eid)
        assert r.accepted and r.status_code == 202

    await state.dispatcher.run_pending(now=T0)
    assert _bodies_for(state, "p-", "mock://pause/1") == []
    held = state.storage.list_dead_letters()  # noqa: 仅确认无死信
    assert held == []
    print("\n[暂停] 3个事件已受理排队，目标订阅实际发出0次")

    got = await state.dispatcher.resume_subscription("sp1")
    assert got is SubscriptionState.ACTIVE
    now = T0
    for _ in range(20):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)

    order = _order(state, "mock://pause/1")
    print(f"[恢复] 实际投递顺序={order}")
    assert order == ["p-a", "p-b", "p-c"]
    for eid in ("p-a", "p-b", "p-c"):
        rec = await state.storage.get_delivery(eid, "sp1")
        assert rec.status is EventStatus.DELIVERED


@pytest.mark.asyncio
async def test_pause_resume_are_idempotent_and_state_queryable(state):
    sub = _sub("sp2", "mock://pause/2")
    await state.storage.upsert_subscription(sub)
    s1 = await state.dispatcher.pause_subscription("sp2")
    s2 = await state.dispatcher.pause_subscription("sp2")
    assert s1 is s2 is SubscriptionState.PAUSED
    r1 = await state.dispatcher.resume_subscription("sp2")
    r2 = await state.dispatcher.resume_subscription("sp2")
    assert r1 is r2 is SubscriptionState.ACTIVE
    assert await state.dispatcher.subscription_state("sp2") is SubscriptionState.ACTIVE
    print("\n[幂等] 暂停/恢复重复调用结论稳定")


@pytest.mark.asyncio
async def test_pausing_one_subscription_does_not_block_others(state):
    sa = _sub("spA", "mock://pause/A")
    sb = _sub("spB", "mock://pause/B")
    await state.storage.upsert_subscription(sa)
    await state.storage.upsert_subscription(sb)
    await state.dispatcher.pause_subscription("spA")

    await _enqueue_direct(state, "ea", sa)
    await _enqueue_direct(state, "eb", sb)

    await state.dispatcher.run_pending(now=T0)
    assert _bodies_for(state, "ea", "mock://pause/A") == []
    assert len(_bodies_for(state, "eb", "mock://pause/B")) == 1
    rec_b = await state.storage.get_delivery("eb", "spB")
    rec_a = await state.storage.get_delivery("ea", "spA")
    print(f"\n[隔离] 暂停spA: A={rec_a.status.value}（排队中） "
          f"B={rec_b.status.value}（已投递）")
    assert rec_b.status is EventStatus.DELIVERED
    assert rec_a.status is EventStatus.SCHEDULED


@pytest.mark.asyncio
async def test_paused_head_does_not_block_ordering_after_resume_with_failures(state):
    sub = _sub("sp3", "mock://pause/3", max_attempts=2)
    await state.storage.upsert_subscription(sub)
    state.transport.script("mock://pause/3",
                           [(503, "x"), (200, "ok"), (200, "ok")])
    await state.dispatcher.pause_subscription("sp3")

    await _enqueue_direct(state, "h-a", sub)
    await _enqueue_direct(state, "h-b", sub)

    await state.dispatcher.resume_subscription("sp3")
    now = T0
    for _ in range(20):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)

    order = _order(state, "mock://pause/3")
    first_b = order.index("h-b")
    print(f"\n[恢复+重试] 投递顺序={order}；h-b首次出现前全部为h-a="
          f"{all(e == 'h-a' for e in order[:first_b])}")
    assert all(e == "h-a" for e in order[:first_b])
    assert (await state.storage.get_delivery("h-a", "sp3")).status is EventStatus.DELIVERED
    assert (await state.storage.get_delivery("h-b", "sp3")).status is EventStatus.DELIVERED
