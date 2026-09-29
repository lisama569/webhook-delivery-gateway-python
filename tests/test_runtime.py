"""新运行时行为：自走投递、出站凭据窗口、订阅暂停/恢复、死信重放。

时间说明：
- 凭据按 utcnow() 装配，凡需手动 run_pending(now=...) 的用例也以 utcnow()
  为时钟起点，避免凭据窗口与退避计算漂移；
- 自走循环相关用例使用真实时间并轮询等待（退避基数极小，0.05/0.1/0.2s）。
"""
from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from app.models import (
    EventStatus,
    FailReason,
    InboundEvent,
    Subscription,
    utcnow,
)


async def _enqueue(state, event_id, sub, payload=None, now=None):
    now = now or utcnow()
    ev = InboundEvent(
        event_id, sub.tenant_id, sub.event_type, sub.version,
        payload or {"order_id": event_id}, now,
    )
    await state.storage.save_event_if_absent(ev)
    return await state.dispatcher.enqueue(event_id, sub)


async def _wait_terminal(state, event_id, sub_id, timeout=3.0):
    """轮询等待一条投递进入终态（由后台 worker 自走推进）。"""
    deadline = utcnow() + timedelta(seconds=timeout)
    while utcnow() < deadline:
        rec = await state.storage.get_delivery(event_id, sub_id)
        if rec is not None and rec.status in (
            EventStatus.DELIVERED, EventStatus.DEAD_LETTER
        ):
            return rec
        await asyncio.sleep(0.01)
    raise AssertionError(f"{event_id} did not reach terminal state within {timeout}s")


def _event_bodies(calls, url):
    return [c["body"] for c in state_calls(calls, url)]


def state_calls(calls, url):
    return [c for c in calls if c["url"] == url]


# ---------------------------------------------------------------------------
# 1) 服务正常启动后投递/重试自己往前走
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_worker_auto_delivers_success_without_manual_pump(state):
    state.dispatcher.start_worker()
    try:
        sub = Subscription("w1", "tenant-a", "order.created", "1.0.0",
                           "mock://w/ok", "key-a-new", max_attempts=3)
        await state.storage.upsert_subscription(sub)
        await _enqueue(state, "w-ok", sub)
        # 不调用任何 run_pending/pump：worker 必须自己投递成功
        rec = await _wait_terminal(state, "w-ok", "w1")
        print(f"\n[event=w-ok] 自走投递最终={rec.status.value} "
              f"尝试={len(rec.attempts)}（无手工 pump）")
        assert rec.status is EventStatus.DELIVERED
        assert len(rec.attempts) == 1
    finally:
        await state.dispatcher.stop_worker()


@pytest.mark.asyncio
async def test_worker_auto_retries_then_dead_letters(state):
    state.dispatcher.start_worker()
    try:
        sub = Subscription("w2", "tenant-a", "order.created", "1.0.0",
                           "mock://w/retry", "key-a-new", max_attempts=3)
        await state.storage.upsert_subscription(sub)
        state.transport.script(
            "mock://w/retry", [(500, "x"), (500, "x"), (500, "x")]
        )
        await _enqueue(state, "w-retry", sub)
        rec = await _wait_terminal(state, "w-retry", "w2")
        print(f"\n[event=w-retry] 自走重试耗尽={rec.status.value} "
              f"reason={rec.fail_reason.value} 尝试={len(rec.attempts)}")
        assert rec.status is EventStatus.DEAD_LETTER
        assert rec.fail_reason is FailReason.ATTEMPTS_EXHAUSTED
        assert "last_basis=HTTP_5XX" in rec.fail_detail
        assert len(rec.attempts) == 3
    finally:
        await state.dispatcher.stop_worker()


# ---------------------------------------------------------------------------
# 2) 出站凭据不在可用窗口：请求不发出，结论与接收方失败严格区分
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_expired_outbound_credential_dead_letters_without_sending(state):
    sub = Subscription("c1", "tenant-a", "order.created", "1.0.0",
                       "mock://c/exp", "key-a-expired", max_attempts=4)
    await state.storage.upsert_subscription(sub)
    now = utcnow()
    await _enqueue(state, "c-exp", sub, now=now)
    processed = await state.dispatcher.run_pending(now=now)
    rec = await state.storage.get_delivery("c-exp", "c1")
    sent = [c for c in state.transport.calls if c["url"] == "mock://c/exp"]
    print(f"\n[event=c-exp] 过期出站凭据 processed={processed} "
          f"状态={rec.status.value} reason={rec.fail_reason.value} "
          f"实际发出请求数={len(sent)} detail={rec.fail_detail}")
    assert processed == 1
    assert rec.status is EventStatus.DEAD_LETTER
    assert rec.fail_reason is FailReason.CREDENTIAL_UNAVAILABLE
    assert "EXPIRED_CREDENTIAL" in rec.fail_detail
    assert len(rec.attempts) == 0  # 请求根本没有发出
    assert sent == []


@pytest.mark.asyncio
async def test_missing_outbound_credential_distinct_from_receiver_failure(state):
    sub = Subscription("c2", "tenant-a", "order.created", "1.0.0",
                       "mock://c/missing", "key-does-not-exist", max_attempts=4)
    await state.storage.upsert_subscription(sub)
    now = utcnow()
    await _enqueue(state, "c-missing", sub, now=now)
    await state.dispatcher.run_pending(now=now)
    rec = await state.storage.get_delivery("c-missing", "c2")
    # 同一测试中构造一个接收方 500 耗尽的对照事件，原因必须可区分
    sub2 = Subscription("c3", "tenant-a", "order.created", "1.0.0",
                        "mock://c/500", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub2)
    state.transport.script("mock://c/500", [(500, "down")])
    await _enqueue(state, "c-500", sub2, now=now)
    await state.dispatcher.run_pending(now=now)
    rec500 = await state.storage.get_delivery("c-500", "c3")
    print(f"\n[event=c-missing] 凭据问题={rec.fail_reason.value} vs "
          f"[event=c-500] 对方问题={rec500.fail_reason.value}")
    assert rec.fail_reason is FailReason.CREDENTIAL_UNAVAILABLE
    assert "KEY_NOT_FOUND" in rec.fail_detail
    assert rec500.fail_reason is not FailReason.CREDENTIAL_UNAVAILABLE


# ---------------------------------------------------------------------------
# 3) 订阅暂停/恢复：入站不阻塞、队序不乱、订阅间互不影响、重复调用稳定
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_pause_holds_and_resume_replays_in_order(state):
    url = "mock://p/order"
    sub = Subscription("p1", "tenant-a", "order.created", "1.0.0",
                       url, "key-a-new", max_attempts=3)
    await state.storage.upsert_subscription(sub)
    now = utcnow()

    await state.dispatcher.pause_subscription("p1", now=now)
    await _enqueue(state, "p-a", sub, now=now)
    await _enqueue(state, "p-b", sub, now=now)

    # 暂停期间推进多次：不得发出任何请求
    for _ in range(5):
        n = await state.dispatcher.run_pending(now=now)
        assert n == 0
        now += timedelta(milliseconds=60)
    held = state_calls(state.transport.calls, url)
    assert held == []
    rec_a = await state.storage.get_delivery("p-a", "p1")
    assert rec_a.status is EventStatus.SCHEDULED  # 仍在待投递顺序里

    # 恢复后严格按 a -> b 顺序投递成功（队首阻塞，FIFO）
    state.transport.script(url, [(200, "a"), (200, "b")])
    resumed = await state.dispatcher.resume_subscription("p1", now=now)
    assert resumed.paused is False
    for _ in range(10):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    bodies = [c["body"] for c in state_calls(state.transport.calls, url)]
    order = []
    for b in bodies:
        order.append(b'"event_id":"p-a"' in b and "p-a" or "p-b")
    print(f"\n[sub=p1] 暂停期间发出={len(held)} 恢复后请求顺序={order}")
    assert order == ["p-a", "p-b"]
    ra = await state.storage.get_delivery("p-a", "p1")
    rb = await state.storage.get_delivery("p-b", "p1")
    assert ra.status is EventStatus.DELIVERED and rb.status is EventStatus.DELIVERED
    assert ra.seq < rb.seq


@pytest.mark.asyncio
async def test_pause_resume_idempotent_and_isolated_per_subscription(state):
    s1 = Subscription("p2", "tenant-a", "order.created", "1.0.0",
                      "mock://p2/hook", "key-a-new", max_attempts=2)
    s2 = Subscription("p3", "tenant-a", "order.created", "1.0.0",
                      "mock://p3/hook", "key-a-new", max_attempts=2)
    await state.storage.upsert_subscription(s1)
    await state.storage.upsert_subscription(s2)
    now = utcnow()

    first = await state.dispatcher.pause_subscription("p2", now=now)
    first_paused_at = first.paused_at
    again = await state.dispatcher.pause_subscription("p2", now=now + timedelta(seconds=9))
    print(f"\n[sub=p2] 重复暂停: paused={again.paused} "
          f"paused_at 稳定={again.paused_at == first_paused_at}")
    assert again.paused is True
    assert again.paused_at == first_paused_at  # 重复调用不改变首次结论

    await _enqueue(state, "p2-e", s1, now=now)
    await _enqueue(state, "p3-e", s2, now=now)
    await state.dispatcher.run_pending(now=now)
    rec_p2 = await state.storage.get_delivery("p2-e", "p2")
    rec_p3 = await state.storage.get_delivery("p3-e", "p3")
    assert rec_p2.status is EventStatus.SCHEDULED       # p2 停着
    assert rec_p3.status is EventStatus.DELIVERED      # p3 不受影响

    # 重复恢复结论稳定
    r1 = await state.dispatcher.resume_subscription("p2", now=now)
    r2 = await state.dispatcher.resume_subscription("p2", now=now)
    assert r1.paused is False and r2.paused is False


@pytest.mark.asyncio
async def test_paused_subscription_still_accepts_inbound(state):
    """暂停只影响投递：入站照常 202 受理并入队。"""
    import json
    from tests.helpers import build_event
    from app.security import sign

    sub = Subscription("p4", "tenant-a", "order.created", "1.0.0",
                       "mock://p4/hook", "key-a-new", max_attempts=2)
    await state.storage.upsert_subscription(sub)
    await state.dispatcher.pause_subscription("p4")

    body = build_event("p4-inbound")
    raw = json.dumps(body, separators=(",", ":")).encode()
    ts = str(int(utcnow().timestamp()))
    result = await state.service.accept(
        "tenant-a", "key-a-new", ts, sign("secret-new", ts, raw),
        raw, body,
    )
    print(f"\n[event=p4-inbound] 暂停期间入站 http={result.status_code} "
          f"deliveries={result.deliveries}")
    assert result.status_code == 202
    assert "p4" in result.deliveries  # 种子订阅 sub-a-orders 同样匹配
    rec = await state.storage.get_delivery("p4-inbound", "p4")
    assert rec.status is EventStatus.SCHEDULED
    # p4 尚未恢复，没有任何请求发出
    assert state_calls(state.transport.calls, "mock://p4/hook") == []


# ---------------------------------------------------------------------------
# 4) 死信重放
# ---------------------------------------------------------------------------
async def _make_dead_letter(state, event_id, sub_id, url, *, max_attempts=3,
                            responses=None):
    sub = Subscription(sub_id, "tenant-a", "order.created", "1.0.0",
                       url, "key-a-new", max_attempts=max_attempts)
    await state.storage.upsert_subscription(sub)
    state.transport.script(
        url, responses if responses is not None else [(500, "x")] * max_attempts
    )
    now = utcnow()
    await _enqueue(state, event_id, sub, now=now)
    for _ in range(max_attempts + 4):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    rec = await state.storage.get_delivery(event_id, sub_id)
    assert rec.status is EventStatus.DEAD_LETTER
    return sub, now


@pytest.mark.asyncio
async def test_replay_single_dead_letter_succeeds_and_archives_history(state):
    sub, now = await _make_dead_letter(
        state, "r1-e", "r1", "mock://r1/hook", max_attempts=2,
        responses=[(500, "x"), (500, "x")],
    )
    original_attempts = len((await state.storage.get_delivery("r1-e", "r1")).attempts)

    # 重放后接收方恢复
    state.transport.script("mock://r1/hook", [(200, "ok")])
    result = await state.dispatcher.replay_one("r1", "r1-e", now=now)
    print(f"\n[event=r1-e] 重放结论={result.outcome.value} 代次={result.generation}")
    assert result.outcome.value == "REPLAYED"
    assert result.generation == 1

    for _ in range(8):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    rec = await state.storage.get_delivery("r1-e", "r1")
    assert rec.status is EventStatus.DELIVERED
    assert rec.replay_generation == 1
    # 当代次只有 1 次尝试；旧代次尝试被归档保留可审计
    assert [a.http_status for a in rec.attempts] == [200]
    assert len(rec.previous_attempts) == original_attempts == 2
    assert rec.replay_in_flight is False


@pytest.mark.asyncio
async def test_replay_queues_behind_pending_head_and_preserves_order(state):
    """重放追加到队尾：不得插到前面尚未终结的事件之前。"""
    url = "mock://r2/hook"
    sub = Subscription("r2", "tenant-a", "order.created", "1.0.0",
                       url, "key-a-new", max_attempts=2)
    await state.storage.upsert_subscription(sub)
    now = utcnow()

    # 第一条先耗尽进死信；随后再来一条新事件（队首位置在重放事件之后入队）
    state.transport.script(url, [(500, "x"), (500, "x")])
    await _enqueue(state, "r2-old", sub, now=now)
    for _ in range(6):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    assert (await state.storage.get_delivery("r2-old", "r2")).status is EventStatus.DEAD_LETTER

    # 此时队列已空（死信出队）。先入队一条正常新事件 new-head，再重放 old。
    await _enqueue(state, "r2-new", sub, now=now)
    state.transport.script(url, [(200, "new"), (200, "old")])
    result = await state.dispatcher.replay_one("r2", "r2-old", now=now)
    assert result.outcome.value == "REPLAYED"

    for _ in range(8):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    bodies = [c["body"] for c in state_calls(state.transport.calls, url)]
    seq_after_replay = [
        "r2-new" if b'"event_id":"r2-new"' in b else "r2-old"
        for b in bodies
    ]
    print(f"\n[sub=r2] 重放后实际发送顺序={seq_after_replay[-2:]}")
    # 重放后这一轮：新事件先发，重放事件随后（重放不插队）
    assert seq_after_replay[-2:] == ["r2-new", "r2-old"]
    assert (await state.storage.get_delivery("r2-new", "r2")).status is EventStatus.DELIVERED
    assert (await state.storage.get_delivery("r2-old", "r2")).status is EventStatus.DELIVERED


@pytest.mark.asyncio
async def test_concurrent_replays_only_one_effective(state):
    sub, now = await _make_dead_letter(
        state, "r3-e", "r3", "mock://r3/hook", max_attempts=2,
        responses=[(500, "x"), (500, "x")],
    )
    # 重放后保持故障：让所有重放尝试都慢不下来没关系；关键是并发触发时占用判定
    state.transport.script("mock://r3/hook", [(200, "ok"), (200, "ok"), (200, "ok")])

    barrier = asyncio.Barrier(8)

    async def fire():
        async with barrier:
            return await state.dispatcher.replay_one("r3", "r3-e", now=now)

    results = await asyncio.gather(*(fire() for _ in range(8)))
    outcomes = [r.outcome.value for r in results]
    print(f"\n[event=r3-e] 并发 8 次重放结论分布={outcomes}")
    assert outcomes.count("REPLAYED") == 1
    assert outcomes.count("ALREADY_IN_FLIGHT") == 7
    # 所有请求都拿到稳定、可区分的结论
    assert set(outcomes) == {"REPLAYED", "ALREADY_IN_FLIGHT"}

    # 让唯一一次生效重放跑完
    for _ in range(8):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    rec = await state.storage.get_delivery("r3-e", "r3")
    assert rec.status is EventStatus.DELIVERED

    # 终结后再来的重放请求：事件已成功，绝不再发
    after = await state.dispatcher.replay_one("r3", "r3-e", now=now)
    assert after.outcome.value == "NOT_DEAD_LETTER"
    sent_for_event = [
        c for c in state_calls(state.transport.calls, "mock://r3/hook")
        if b'"event_id":"r3-e"' in c["body"]
    ]
    # 共 3 次真实请求：原代次 2 次 500 + 重放代次恰好 1 次 200；
    # 7 个落败重放请求与终结后的重放请求都没有再产生任何发送。
    assert len(sent_for_event) == 3
    final = await state.storage.get_delivery("r3-e", "r3")
    assert [a.http_status for a in final.attempts] == [200]


@pytest.mark.asyncio
async def test_batch_replay_ordered_idempotent_and_isolates_other_subs(state):
    url = "mock://r4/hook"
    sub = Subscription("r4", "tenant-a", "order.created", "1.0.0",
                       url, "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    now = utcnow()
    # 造 3 条死信（max_attempts=1 -> 每条 1 次 500 即死信，FIFO 逐条进行）
    for eid in ("r4-a", "r4-b", "r4-c"):
        state.transport.script(url, [(500, "x")])
        await _enqueue(state, eid, sub, now=now)
        for _ in range(4):
            await state.dispatcher.run_pending(now=now)
            now += timedelta(milliseconds=60)
    dlq = state.storage.list_deliveries(
        subscription_id="r4", status=EventStatus.DEAD_LETTER
    )
    assert [r.event_id for r in dlq] == ["r4-a", "r4-b", "r4-c"]

    # 批量重放：接收方恢复；逐条有序，积压再多也只是排队顺序处理
    state.transport.script(url, [(200, "a"), (200, "b"), (200, "c")])
    _, results = await state.dispatcher.replay_subscription_dead_letters("r4", now=now)
    assert [r.outcome.value for r in results] == ["REPLAYED"] * 3
    assert [r.event_id for r in results] == ["r4-a", "r4-b", "r4-c"]

    # 同一批立刻再批量重放：全部已在途，没有第二次生效
    _, results2 = await state.dispatcher.replay_subscription_dead_letters("r4", now=now)
    assert [r.outcome.value for r in results2] == ["ALREADY_IN_FLIGHT"] * 3

    for _ in range(10):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    bodies = [
        "r4-a" if b'"event_id":"r4-a"' in b else
        "r4-b" if b'"event_id":"r4-b"' in b else "r4-c"
        for b in [c["body"] for c in state_calls(state.transport.calls, url)]
    ]
    # 重放代次的请求顺序严格 a -> b -> c（此前还有 3 次 500）
    print(f"\n[sub=r4] 全部请求顺序={bodies}")
    assert bodies == ["r4-a", "r4-b", "r4-c", "r4-a", "r4-b", "r4-c"]
    gens = {
        eid: (await state.storage.get_delivery(eid, "r4")).replay_generation
        for eid in ("r4-a", "r4-b", "r4-c")
    }
    assert gens == {"r4-a": 1, "r4-b": 1, "r4-c": 1}


@pytest.mark.asyncio
async def test_replay_unknown_targets_and_delivered_event(state):
    sub, now = await _make_dead_letter(
        state, "r5-e", "r5", "mock://r5/hook", max_attempts=1,
        responses=[(500, "x")],
    )
    missing_sub = await state.dispatcher.replay_one("ghost", "r5-e", now=now)
    missing_evt = await state.dispatcher.replay_one("r5", "ghost", now=now)
    assert missing_sub.outcome.value == "NOT_FOUND"
    assert missing_evt.outcome.value == "NOT_FOUND"

    # 成功投递后的事件不可重放
    state.transport.script("mock://r5/hook", [(200, "ok")])
    await state.dispatcher.replay_one("r5", "r5-e", now=now)
    await state.dispatcher.run_pending(now=now)
    again = await state.dispatcher.replay_one("r5", "r5-e", now=now)
    print(f"\n[event=r5-e] 成功后重放结论={again.outcome.value}")
    assert again.outcome.value == "NOT_DEAD_LETTER"


@pytest.mark.asyncio
async def test_batch_replay_unknown_subscription_returns_not_found(state):
    outcome, results = await state.dispatcher.replay_subscription_dead_letters("nope")
    assert outcome.value == "NOT_FOUND" and results == []


@pytest.mark.asyncio
async def test_replay_while_paused_waits_and_flows_after_resume(state):
    """暂停期间允许发起重放：只排队不发送，恢复后随队列顺序补发。"""
    sub, now = await _make_dead_letter(
        state, "r6-e", "r6", "mock://r6/hook", max_attempts=1,
        responses=[(500, "x")],
    )
    await state.dispatcher.pause_subscription("r6", now=now)
    state.transport.script("mock://r6/hook", [(200, "ok")])
    result = await state.dispatcher.replay_one("r6", "r6-e", now=now)
    assert result.outcome.value == "REPLAYED"

    for _ in range(4):
        n = await state.dispatcher.run_pending(now=now)
        assert n == 0  # 暂停：重放事件也不发出
        now += timedelta(milliseconds=60)
    # 暂停期没有产生任何新投递（仍只有造死信阶段的那 1 次 500）
    assert len(state_calls(state.transport.calls, "mock://r6/hook")) == 1

    await state.dispatcher.resume_subscription("r6", now=now)
    for _ in range(6):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    rec = await state.storage.get_delivery("r6-e", "r6")
    assert rec.status is EventStatus.DELIVERED
    hook_calls = state_calls(state.transport.calls, "mock://r6/hook")
    print(f"\n[event=r6-e] 暂停期重放不发，恢复后总请求数={len(hook_calls)} "
          f"(1 次造死信 500 + 1 次重放 200)")
    assert len(hook_calls) == 2
