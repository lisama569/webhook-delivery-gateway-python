"""出站投递：退避重试、重试耗尽死信（原因可区分）、订阅内顺序保证。"""
from datetime import datetime, timedelta, timezone

import pytest

from app.models import (
    EventStatus,
    FailReason,
    InboundEvent,
    Subscription,
    utcnow,
)

T0 = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)


async def _enqueue_event(state, event_id, sub, payload=None):
    ev = InboundEvent(
        event_id, "tenant-a", "order.created", "1.0.0",
        payload or {"order_id": event_id}, T0,
    )
    await state.storage.save_event_if_absent(ev)
    return await state.dispatcher.enqueue(event_id, sub)


@pytest.mark.asyncio
async def test_retry_then_success(state):
    sub = Subscription("s1", "tenant-a", "order.created", "1.0.0",
                       "https://p/recover", "key-a-new", max_attempts=4)
    await state.storage.upsert_subscription(sub)
    state.transport.script("https://p/recover",
                           [(503, "busy"), (503, "busy"), (200, "ok")])
    await _enqueue_event(state, "e-retry", sub)

    now = T0
    statuses = []
    for _ in range(12):
        await state.dispatcher.run_pending(now=now)
        rec = await state.storage.get_delivery("e-retry", "s1")
        statuses.append(rec.status)
        if rec.status is EventStatus.DELIVERED:
            break
        now += timedelta(milliseconds=60)

    rec = await state.storage.get_delivery("e-retry", "s1")
    print(f"\n[event=e-retry] 最终状态={rec.status.value} 尝试次数={len(rec.attempts)} "
          f"退避序列={[a.backoff_seconds for a in rec.attempts]}")
    assert rec.status is EventStatus.DELIVERED
    assert len(rec.attempts) == 3
    assert [a.http_status for a in rec.attempts] == [503, 503, 200]


@pytest.mark.asyncio
async def test_attempts_exhausted_dead_letter_with_distinct_reasons(state):
    sub = Subscription("s2", "tenant-a", "order.created", "1.0.0",
                       "https://p/down", "key-a-new", max_attempts=3)
    await state.storage.upsert_subscription(sub)

    # 网络故障耗尽
    state.transport.script("https://p/down", [ConnectionError("conn refused")] * 3)
    await _enqueue_event(state, "e-net", sub)
    now = T0
    for _ in range(10):
        await state.dispatcher.run_pending(now=now)
        rec = await state.storage.get_delivery("e-net", "s2")
        if rec.status in (EventStatus.DELIVERED, EventStatus.DEAD_LETTER):
            break
        now += timedelta(milliseconds=60)
    rec = await state.storage.get_delivery("e-net", "s2")
    print(f"\n[event=e-net] 网络重试耗尽 状态={rec.status.value} "
          f"fail_reason={rec.fail_reason.value} detail={rec.fail_detail}")
    assert rec.status is EventStatus.DEAD_LETTER
    assert rec.fail_reason is FailReason.ATTEMPTS_EXHAUSTED
    # 最后一跳底层原因仍可区分
    assert "last_basis=NETWORK_ERROR" in rec.fail_detail

    # 5xx 耗尽：fail_reason 同样为 ATTEMPTS_EXHAUSTED，但 detail 中 basis 不同
    state.transport.script("https://p/down", [(500, "err")] * 3)
    await _enqueue_event(state, "e-5xx", sub)
    for _ in range(10):
        await state.dispatcher.run_pending(now=now)
        rec = await state.storage.get_delivery("e-5xx", "s2")
        if rec.status is EventStatus.DEAD_LETTER:
            break
        now += timedelta(milliseconds=60)
    print(f"\n[event=e-5xx] 5xx重试耗尽 状态={rec.status.value} "
          f"fail_reason={rec.fail_reason.value} detail={rec.fail_detail}")
    assert rec.fail_reason is FailReason.ATTEMPTS_EXHAUSTED
    assert "last_basis=HTTP_5XX" in rec.fail_detail

    # 4xx 永久失败：立即死信，fail_reason=HTTP_4XX，不耗尽重试
    state.transport.script("https://p/down", [(400, "bad request")])
    await _enqueue_event(state, "e-4xx", sub)
    await state.dispatcher.run_pending(now=now)
    rec = await state.storage.get_delivery("e-4xx", "s2")
    print(f"\n[event=e-4xx] 4xx永久失败 状态={rec.status.value} "
          f"fail_reason={rec.fail_reason.value} 尝试次数={len(rec.attempts)}")
    assert rec.status is EventStatus.DEAD_LETTER
    assert rec.fail_reason is FailReason.HTTP_4XX
    assert len(rec.attempts) == 1

    # 死信链路可查询，且原因彼此可区分
    dlq = state.storage.list_deliveries(status=EventStatus.DEAD_LETTER)
    details = {r.event_id: r.fail_detail for r in dlq}
    assert {"e-net", "e-5xx", "e-4xx"} <= set(details)
    assert "last_basis=NETWORK_ERROR" in details["e-net"]
    assert "last_basis=HTTP_5XX" in details["e-5xx"]
    assert "permanent reject" in details["e-4xx"]


@pytest.mark.asyncio
async def test_strict_order_within_subscription(state):
    sub = Subscription("s3", "tenant-a", "order.created", "1.0.0",
                       "https://p/order", "key-a-new", max_attempts=3)
    await state.storage.upsert_subscription(sub)
    # 第一个事件持续失败，第二个事件默认可成功 —— 也不得越过队头
    state.transport.script("https://p/order", [(503, "x"), (503, "x"), (503, "x")])
    await _enqueue_event(state, "ord-1", sub)
    await _enqueue_event(state, "ord-2", sub)

    now = T0
    for _ in range(6):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)

    r1 = await state.storage.get_delivery("ord-1", "s3")
    r2 = await state.storage.get_delivery("ord-2", "s3")
    bodies = [c["body"] for c in state.transport.calls]
    first_ord2 = next(i for i, b in enumerate(bodies) if b"ord-2" in b)
    print(f"\n[event=ord-1/ord-2] 前序状态={r1.status.value}(seq={r1.seq}) "
          f"后序状态={r2.status.value}(seq={r2.seq}) ord-2首次请求下标="
          f"{first_ord2}（此前均为ord-1）")
    assert r1.seq < r2.seq
    # 后序事件的第一次请求之前，全部请求都属于前序事件
    assert all(b"ord-1" in b for b in bodies[:first_ord2])
    assert first_ord2 == 3  # ord-1 恰好尝试 3 次
    assert r1.status is EventStatus.DEAD_LETTER
    assert r2.status is EventStatus.DELIVERED
