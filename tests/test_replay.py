"""失败事件重放：

- 单条/批量重放走订阅 FIFO，不插到未终结的前序事件之前；
- 已成功的绝不重发；非死信/不存在结论稳定可区分；
- 同一事件并发或连续多次重放只生效一次；
- 批量重放按 seq 顺序处理，并发两批只有一批真正重排，且不拖垮服务。
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from app.models import (
    EventStatus,
    FailReason,
    InboundEvent,
    ReplayOutcome,
    Subscription,
)

T0 = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)


def _order(state, url):
    return [json.loads(c["body"])["event_id"]
            for c in state.transport.calls if c["url"] == url]


def _calls_for(state, event_id, url):
    return [c for c in state.transport.calls
            if c["url"] == url and event_id.encode() in c["body"]]


def _post_slice(state, url, start_idx):
    return [json.loads(c["body"])["event_id"]
            for c in state.transport.calls[start_idx:] if c["url"] == url]


async def _make_dead_letter(state, sub, event_id, now, *, failures=None):
    """编排：让某事件重试耗尽成为死信。failures 为脚本（默认 max 次 500）。"""
    if failures is None:
        failures = [(500, "err")] * sub.max_attempts
    state.transport.script(sub.callback_url, failures)
    ev = InboundEvent(event_id, "tenant-a", "order.created", "1.0.0",
                      {"order_id": event_id}, now)
    await state.storage.save_event_if_absent(ev)
    await state.dispatcher.enqueue(event_id, sub)
    t = now
    for _ in range(sub.max_attempts + 4):
        await state.dispatcher.run_pending(now=t)
        rec = await state.storage.get_delivery(event_id, sub.subscription_id)
        if rec.status is EventStatus.DEAD_LETTER:
            return rec
        t += timedelta(milliseconds=60)
    raise AssertionError(f"{event_id} did not become dead letter")


async def _drain(state, now, cycles=40, step_ms=60):
    t = now
    for _ in range(cycles):
        await state.dispatcher.run_pending(now=t)
        t += timedelta(milliseconds=step_ms)
    return t


@pytest.mark.asyncio
async def test_replay_single_then_success(state):
    sub = Subscription("rp1", "tenant-a", "order.created", "1.0.0",
                       "mock://rp/1", "key-a-new", max_attempts=2)
    await state.storage.upsert_subscription(sub)
    rec = await _make_dead_letter(state, sub, "d1", T0)
    assert rec.fail_reason is FailReason.ATTEMPTS_EXHAUSTED
    orig_detail = rec.fail_detail

    # 非死信结论可区分：当前是死信，先重放一次
    state.transport.script("mock://rp/1", [(200, "ok")])
    call_idx = len(state.transport.calls)
    outcome, rec2 = await state.dispatcher.replay_dead_letter("d1", "rp1")
    assert outcome is ReplayOutcome.REPLAYED
    # 归档保留原死信结论；新一轮 attempt 从 1 重新计数
    assert rec2.archived_fail_reason is FailReason.ATTEMPTS_EXHAUSTED
    assert rec2.replayed is True and rec2.replay_count == 1
    assert rec2.attempts == []
    assert rec2.archived_fail_detail == orig_detail
    assert rec2.status is EventStatus.SCHEDULED

    # 立即再次重放（尚未投递完）-> 稳定的 ALREADY_REPLAYING，不重复排队
    outcome2, _ = await state.dispatcher.replay_dead_letter("d1", "rp1")
    assert outcome2 is ReplayOutcome.ALREADY_REPLAYING

    await _drain(state, T0)
    final = await state.storage.get_delivery("d1", "rp1")
    assert final.status is EventStatus.DELIVERED
    assert len(final.attempts) == 1  # 新一轮只发了一次
    post_order = _post_slice(state, "mock://rp/1", call_idx)
    assert post_order == ["d1"]  # 重放后只真正发了一次
    print("\n[单条] 重放成功，原死信结论已归档:",
          final.archived_fail_reason.value)

    # 已成功 -> 拒绝重发
    outcome3, _ = await state.dispatcher.replay_dead_letter("d1", "rp1")
    assert outcome3 is ReplayOutcome.ALREADY_DELIVERED


@pytest.mark.asyncio
async def test_replay_not_found_and_subscription_guard(state):
    sub = Subscription("rpX", "tenant-a", "order.created", "1.0.0",
                       "mock://rp/x", "key-a-new", max_attempts=2)
    await state.storage.upsert_subscription(sub)
    outcome, rec = await state.dispatcher.replay_dead_letter("ghost", "rpX")
    assert outcome is ReplayOutcome.NOT_FOUND and rec is None


@pytest.mark.asyncio
async def test_replay_waits_behind_unfinished_earlier_event(state):
    # A(seq1) 先成死信；B(seq2) 随后成功；重放 A 时没有未终结前序事件。
    # 关键场景：让 A 重放后再次进入退避，此时新入队的 C(seq3) 不得越过 A。
    sub = Subscription("rp2", "tenant-a", "order.created", "1.0.0",
                       "mock://rp/2", "key-a-new", max_attempts=2)
    await state.storage.upsert_subscription(sub)
    # A 耗尽：500,500
    await _make_dead_letter(state, sub, "A", T0,
                            failures=[(500, "a"), (500, "a")])
    # B 入队成功（A 已出队，队列为空）
    state.transport.script("mock://rp/2", [(200, "b")])
    ev_b = InboundEvent("B", "tenant-a", "order.created", "1.0.0",
                        {"order_id": "B"}, T0)
    await state.storage.save_event_if_absent(ev_b)
    await state.dispatcher.enqueue("B", sub)
    await _drain(state, T0, cycles=4)
    assert (await state.storage.get_delivery("B", "rp2")).status is EventStatus.DELIVERED

    # 重放 A：脚本先 503（退避），再 200。同时 C 入队。
    state.transport.script("mock://rp/2", [(503, "busy"), (200, "a-ok"), (200, "c-ok")])
    call_idx = len(state.transport.calls)
    outcome, _ = await state.dispatcher.replay_dead_letter("A", "rp2")
    assert outcome is ReplayOutcome.REPLAYED
    ev_c = InboundEvent("C", "tenant-a", "order.created", "1.0.0",
                        {"order_id": "C"}, T0)
    await state.storage.save_event_if_absent(ev_c)
    await state.dispatcher.enqueue("C", sub)  # seq3，必须排在重放的 A 之后

    t = T0
    for _ in range(30):
        await state.dispatcher.run_pending(now=t)
        t += timedelta(milliseconds=60)

    order = _post_slice(state, "mock://rp/2", call_idx)
    # 重放后 A 的两次请求（503,200）必须都在 C 的请求之前
    first_c = order.index("C")
    before_c = order[:first_c]
    print("\n[保序] 重放A期间入队C，C首次请求前的序列:", before_c)
    assert before_c == ["A", "A"]
    assert all(e == "A" for e in before_c)
    assert (await state.storage.get_delivery("A", "rp2")).status is EventStatus.DELIVERED
    assert (await state.storage.get_delivery("C", "rp2")).status is EventStatus.DELIVERED


@pytest.mark.asyncio
async def test_batch_replay_order_and_summary(state):
    sub = Subscription("rp3", "tenant-a", "order.created", "1.0.0",
                       "mock://rp/3", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    for eid in ("b1", "b2", "b3"):
        await _make_dead_letter(state, sub, eid, T0, failures=[(500, "x")])

    dlq = state.storage.list_dead_letters(subscription_id="rp3")
    assert [r.event_id for r in dlq] == ["b1", "b2", "b3"]

    state.transport.script("mock://rp/3",
                           [(200, "1"), (200, "2"), (200, "3")])
    call_idx = len(state.transport.calls)
    summary = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "rp3")
    assert summary["found"] is True
    assert summary["total"] == 3 and summary["replayed"] == 3
    assert [r["event_id"] for r in summary["results"]] == ["b1", "b2", "b3"]

    # 紧随其后的第二批：冷却窗内识别为重复批量，逐条稳定
    # ALREADY_REPLAYING，不重复认领、不重复发送
    summary2 = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "rp3")
    assert summary2["total"] == 3
    assert summary2["replayed"] == 0
    assert summary2["already_replaying"] == 3
    assert [r["event_id"] for r in summary2["results"]] == ["b1", "b2", "b3"]

    # 冷却窗之外（cooldown=0）的全新调用：确实已无死信，返回空
    summary3 = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "rp3", cooldown_seconds=0)
    assert summary3["total"] == 0 and summary3["replayed"] == 0

    await _drain(state, T0)
    order = _post_slice(state, "mock://rp/3", call_idx)
    print("\n[批量] 重放投递顺序:", order)
    assert order == ["b1", "b2", "b3"]
    for eid in ("b1", "b2", "b3"):
        assert (await state.storage.get_delivery(eid, "rp3")).status is EventStatus.DELIVERED


@pytest.mark.asyncio
async def test_concurrent_batch_replay_only_one_effective(state):
    sub = Subscription("rp4", "tenant-a", "order.created", "1.0.0",
                       "mock://rp/4", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    n = 8
    for i in range(n):
        await _make_dead_letter(state, sub, f"k{i}", T0, failures=[(500, "x")])

    state.transport.script("mock://rp/4", [(200, "ok")] * n)
    call_idx = len(state.transport.calls)
    # 两批批量重放并发；候选集一致，第二批应全部 ALREADY_REPLAYING
    s1, s2 = await asyncio.gather(
        state.dispatcher.replay_subscription_dead_letters("tenant-a", "rp4"),
        state.dispatcher.replay_subscription_dead_letters("tenant-a", "rp4"),
    )
    replayed = s1["replayed"] + s2["replayed"]
    already = s1["already_replaying"] + s2["already_replaying"]
    print(f"\n[并发批量] 真正重排={replayed} 落败(already_replaying)={already}")
    assert replayed == n and already == n

    await _drain(state, T0)
    # 每个事件在重放之后只被真正发送一次（不含重放前的失败尝试）
    post = _post_slice(state, "mock://rp/4", call_idx)
    for i in range(n):
        assert post.count(f"k{i}") == 1
    assert post == [f"k{i}" for i in range(n)]


@pytest.mark.asyncio
async def test_concurrent_single_replay_sends_only_once(state):
    sub = Subscription("rp5", "tenant-a", "order.created", "1.0.0",
                       "mock://rp/5", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    await _make_dead_letter(state, sub, "m1", T0, failures=[(500, "x")])
    state.transport.script("mock://rp/5", [(200, "ok")])
    call_idx = len(state.transport.calls)

    outcomes = await asyncio.gather(*[
        state.dispatcher.replay_dead_letter("m1", "rp5") for _ in range(10)
    ])
    codes = [o.value for o, _ in outcomes]
    assert codes.count("REPLAYED") == 1
    assert codes.count("ALREADY_REPLAYING") == 9
    print("\n[并发单条] 10路并发重放结论:", {c: codes.count(c) for c in set(codes)})

    await _drain(state, T0)
    final = await state.storage.get_delivery("m1", "rp5")
    assert final.status is EventStatus.DELIVERED
    post_order = _post_slice(state, "mock://rp/5", call_idx)
    assert post_order == ["m1"]


@pytest.mark.asyncio
async def test_batch_replay_large_backlog_sequential_and_bounded(state):
    sub = Subscription("rp6", "tenant-a", "order.created", "1.0.0",
                       "mock://rp/6", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    n = 25
    for i in range(n):
        await _make_dead_letter(state, sub, f"q{i}", T0, failures=[(500, "x")])

    state.transport.script("mock://rp/6", [(200, "ok")] * n)
    call_idx = len(state.transport.calls)
    summary = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "rp6")
    assert summary["replayed"] == n
    # 结果按 seq 升序，顺序确定
    assert [r["event_id"] for r in summary["results"]] == [
        f"q{i}" for i in range(n)]

    await _drain(state, T0, cycles=60)
    order = _post_slice(state, "mock://rp/6", call_idx)
    assert order == [f"q{i}" for i in range(n)]
    print(f"\n[大积压] {n}条按序重放完成，无乱序")
