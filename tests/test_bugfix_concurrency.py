"""本轮缺陷修复的回归用例（见需求单：并发推进/重放一致性）。

覆盖业务：
1. 同一事件被两条路径并发推进（后台 worker 与手工 pump 同时撞上），
   最终只产生一次有效发送；已终结事件不会被再次拿出来发送；
2. 事件重放后再次失败回到失败链路，无论距上次操作多久都能再次重放；
3. 同一批失败事件并发或连续多次批量重放，只有一次真正生效，其余请求的
   逐条结论稳定、可区分，且与失败链路里查到的真实状态一致；
4. 订阅暂停、重放事件压在待投递队列里、事件已经成功等边界/重复场景下，
   批量重放结果前后一致（逐条结论与 total 不漂移）；
5. 单条重放与批量重放对同一事件的结论彼此说得通。
"""
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from app.models import (
    DeliveryKind,
    EventStatus,
    InboundEvent,
    ReplayOutcome,
    Subscription,
)
from app.transport import MockTransport, TransportResult

T0 = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)


class GateTransport(MockTransport):
    """可阻塞在"请求进行中"的 Mock 传输层。

    enter/leave 两个事件把一次 HTTP 调用卡在发送窗口里，测试借此精确构造
    "两条推进路径同时到达同一事件"的竞态。
    """

    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    def reset_gate(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def post(self, url, headers, body) -> TransportResult:
        # 记录调用后立刻开门通知，再阻塞直到测试放行；放行后按脚本/默认成功
        self.calls.append({"url": url, "headers": headers, "body": body})
        self.entered.set()
        await self.release.wait()
        queue = self._scripts.get(url)
        if not queue:
            return TransportResult(
                DeliveryKind.SUCCESS, 200, "gated 200", "HTTP_2XX")
        item = queue.popleft()
        if isinstance(item, Exception):
            return TransportResult(
                DeliveryKind.RETRYABLE, None, f"network error: {item}",
                "NETWORK_ERROR")
        status, text = item
        from app.transport import TransportResult as _TR
        kind, code, detail = _TR.classify_status(status)
        return TransportResult(kind, status, f"{detail}: {text}", code)


async def _seed_event(state, event_id, sub, now=T0):
    ev = InboundEvent(event_id, "tenant-a", "order.created", "1.0.0",
                      {"order_id": event_id}, now)
    await state.storage.save_event_if_absent(ev)
    await state.dispatcher.enqueue(event_id, sub)


def _call_bodies(state, event_id, url):
    return [c for c in state.transport.calls
            if c["url"] == url and event_id.encode() in c["body"]]


async def _make_dead_letter(state, sub, event_id, now, *, failures=None):
    if failures is None:
        failures = [(500, "err")] * sub.max_attempts
    state.transport.script(sub.callback_url, failures)
    await _seed_event(state, event_id, sub, now=now)
    t = now
    for _ in range(sub.max_attempts + 4):
        await state.dispatcher.run_pending(now=t)
        rec = await state.storage.get_delivery(event_id, sub.subscription_id)
        if rec.status is EventStatus.DEAD_LETTER:
            return rec
        t += timedelta(milliseconds=60)
    raise AssertionError(f"{event_id} did not become dead letter")


@pytest.mark.asyncio
async def test_concurrent_advance_sends_event_only_once(state):
    """后台 worker 与手工 pump 在同一事件上撞车：只允许一次真实发送。"""
    gate = GateTransport()
    state.transport = gate
    state.dispatcher._transport = gate

    sub = Subscription("cf1", "tenant-a", "order.created", "1.0.0",
                       "mock://cf/1", "key-a-new", max_attempts=3)
    await state.storage.upsert_subscription(sub)
    await _seed_event(state, "evt-once", sub)

    # 路径 A（模拟后台 worker）推进，卡在请求进行中
    task_a = asyncio.create_task(state.dispatcher.run_pending(now=T0))
    await asyncio.wait_for(gate.entered.wait(), timeout=2.0)

    # 请求进行中时路径 B（手工 pump）同时推进同一事件
    task_b = asyncio.create_task(state.dispatcher.run_pending(now=T0))
    # 给 B 一点时间跑到锁前；它必须等待，不得自行发出第二次请求
    await asyncio.sleep(0.05)
    assert len(_call_bodies(state, "evt-once", "mock://cf/1")) == 1

    gate.release.set()
    processed = await asyncio.gather(task_a, task_b)

    rec = await state.storage.get_delivery("evt-once", "cf1")
    sent = _call_bodies(state, "evt-once", "mock://cf/1")
    print("\n[并发推进] 两路推进处理数=", processed,
          "真实发送次数=", len(sent), "最终状态=", rec.status.value)
    assert rec.status is EventStatus.DELIVERED
    assert len(sent) == 1  # 任意并发推进下最终只有一次有效发送
    assert len(rec.attempts) == 1

    # 已终结事件再被推进若干次：绝不重发，队头残留也被清理
    for _ in range(3):
        await state.dispatcher.run_pending(now=T0)
    assert len(_call_bodies(state, "evt-once", "mock://cf/1")) == 1
    assert state.dispatcher._queues.get("cf1") is None or not state.dispatcher._queues["cf1"]


@pytest.mark.asyncio
async def test_replay_can_be_replayed_again_after_failing_again(state):
    """重放 -> 投递再次失败回到死信 -> 仍可再次重放，且不受时间/代际卡住。"""
    sub = Subscription("cf2", "tenant-a", "order.created", "1.0.0",
                       "mock://cf/2", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)

    # 首次死信
    rec = await _make_dead_letter(state, sub, "rr1", T0, failures=[(500, "x")])
    assert rec.status is EventStatus.DEAD_LETTER
    first_reason = rec.fail_reason

    # 第一次重放，但投递再次失败（500，max_attempts=1 立即再死信）。
    # 中间插一次批量重放，让该事件进入"上一代批量候选"记忆。
    state.transport.script("mock://cf/2", [(500, "again")])
    batch1 = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "cf2", now=T0)
    assert batch1["replayed"] == 1
    await state.dispatcher.run_pending(now=T0 + timedelta(milliseconds=1))
    rec = await state.storage.get_delivery("rr1", "cf2")
    assert rec.status is EventStatus.DEAD_LETTER
    assert rec.replay_count == 1
    assert rec.archived_fail_reason is first_reason  # 归档保留上一轮结论

    # 距离上次操作"很久以后"（远超历史 5s 冷却窗的量级）再次批量重放：
    # 事件真实在失败链路里，必须再次生效，绝不能被当成"正在重放"卡住。
    much_later = T0 + timedelta(days=30)
    state.transport.script("mock://cf/2", [(200, "ok")])
    call_idx = len(state.transport.calls)
    batch2 = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "cf2", now=much_later)
    print("\n[再失败再重放] 第二次批量结论:",
          [(r["event_id"], r["outcome"]) for r in batch2["results"]])
    assert batch2["total"] == 1
    assert batch2["replayed"] == 1
    assert batch2["already_replaying"] == 0

    await state.dispatcher.run_pending(now=much_later)
    rec = await state.storage.get_delivery("rr1", "cf2")
    assert rec.status is EventStatus.DELIVERED
    assert rec.replay_count == 2
    # 重放后真实发送次数：两次重放各一次（第一轮失败、第二轮成功）
    post = state.transport.calls[call_idx:]
    bodies = [c for c in post if b"rr1" in c["body"] and c["url"] == "mock://cf/2"]
    assert len(bodies) == 1

    # 成功后单条重放必须拒绝重发
    outcome, _ = await state.dispatcher.replay_dead_letter("rr1", "cf2")
    assert outcome is ReplayOutcome.ALREADY_DELIVERED


@pytest.mark.asyncio
async def test_single_replay_again_soon_after_failing_again(state):
    """单条路径：重放后很快又失败，紧接着（任意时间间隔）都能再次重放。"""
    sub = Subscription("cf3", "tenant-a", "order.created", "1.0.0",
                       "mock://cf/3", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    await _make_dead_letter(state, sub, "rr2", T0, failures=[(500, "x")])

    state.transport.script("mock://cf/3", [(500, "again")])
    o1, _ = await state.dispatcher.replay_dead_letter("rr2", "cf3", now=T0)
    assert o1 is ReplayOutcome.REPLAYED
    # 投递中立刻再请求 -> 正在重放
    o_mid, _ = await state.dispatcher.replay_dead_letter("rr2", "cf3", now=T0)
    assert o_mid is ReplayOutcome.ALREADY_REPLAYING
    await state.dispatcher.run_pending(now=T0)
    rec = await state.storage.get_delivery("rr2", "cf3")
    assert rec.status is EventStatus.DEAD_LETTER

    # 同一时刻、无任何时间流逝：已再次落回失败链路 -> 可以再次重放
    state.transport.script("mock://cf/3", [(200, "ok")])
    o2, rec2 = await state.dispatcher.replay_dead_letter("rr2", "cf3", now=T0)
    print("\n[单条再失败再重放] 无间隔再次重放结论:", o2.value,
          "replay_count=", rec2.replay_count)
    assert o2 is ReplayOutcome.REPLAYED
    assert rec2.replay_count == 2


def _summary_map(summary):
    return {r["event_id"]: r["outcome"] for r in summary["results"]}


@pytest.mark.asyncio
async def test_concurrent_batch_replay_outcomes_match_real_state(state):
    """N 路并发批量重放同一批死信：只一批生效，其余逐条 ALREADY_REPLAYING，
    且每批 total 相同、逐条结论与投递记录真实状态一致。"""
    sub = Subscription("cf4", "tenant-a", "order.created", "1.0.0",
                       "mock://cf/4", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    ids = ["c0", "c1", "c2", "c3"]
    for eid in ids:
        await _make_dead_letter(state, sub, eid, T0, failures=[(500, "x")])

    # 重放后事件停在 SCHEDULED（不投递），便于核对"与失败链路真实状态一致"
    call_idx = len(state.transport.calls)
    summaries = await asyncio.gather(*[
        state.dispatcher.replay_subscription_dead_letters(
            "tenant-a", "cf4", now=T0)
        for _ in range(4)
    ])

    replayed_total = sum(s["replayed"] for s in summaries)
    ar_total = sum(s["already_replaying"] for s in summaries)
    print("\n[并发批量] 4 路: 总生效=", replayed_total,
          "落败already_replaying=", ar_total)
    assert replayed_total == len(ids)
    assert ar_total == len(ids) * 3
    # 每一批都覆盖全部候选、total 一致
    for s in summaries:
        assert s["total"] == len(ids)
        assert sorted(r["event_id"] for r in s["results"]) == sorted(ids)

    # 恰好一批 REPLAYED，其余三批对每条事件都给 ALREADY_REPLAYING
    winner = [s for s in summaries if s["replayed"] == len(ids)]
    losers = [s for s in summaries if s["already_replaying"] == len(ids)]
    assert len(winner) == 1 and len(losers) == 3

    # 逐条结论与真实状态对得上：此刻全部 SCHEDULED（已重排、未投递）
    for eid in ids:
        rec = await state.storage.get_delivery(eid, "cf4")
        assert rec.status is EventStatus.SCHEDULED
        assert rec.replay_count == 1
    # 还没有任何真实发送（未推进 run_pending）
    assert len(state.transport.calls) == call_idx

    # 投递成功后，失败链路已无这些事件；再来的批量请求逐条结论必须变成
    # ALREADY_DELIVERED（而不是空结果、也不是仍 ALREADY_REPLAYING）
    state.transport.script("mock://cf/4", [(200, "ok")] * len(ids))
    t = T0
    for _ in range(10):
        await state.dispatcher.run_pending(now=t)
        t += timedelta(milliseconds=60)
    assert state.storage.list_dead_letters(subscription_id="cf4") == []

    after = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "cf4", now=t)
    print("[并发批量-成功后] 逐条结论:", _summary_map(after))
    assert after["total"] == len(ids)
    assert after["already_delivered"] == len(ids)
    assert after["replayed"] == 0
    assert all(v == "ALREADY_DELIVERED" for v in _summary_map(after).values())
    # 失败链路为空与此结论一致
    assert state.storage.list_dead_letters(tenant_id="tenant-a") == []


@pytest.mark.asyncio
async def test_repeated_batch_replay_while_paused_is_consistent(state):
    """订阅暂停、重放事件仍压在待投递队列：重复批量重放（含跨越长间隔）
    逐条结论与 total 必须前后一致，不得这次有结论下次空。"""
    sub = Subscription("cf5", "tenant-a", "order.created", "1.0.0",
                       "mock://cf/5", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    for eid in ("z1", "z2"):
        await _make_dead_letter(state, sub, eid, T0, failures=[(500, "x")])

    # 先暂停，再批量重放：事件被认领重排但无法发出，停在 SCHEDULED
    await state.dispatcher.pause_subscription("cf5")
    call_idx = len(state.transport.calls)  # 只统计重放之后的真实发送
    first = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "cf5", now=T0)
    assert first["replayed"] == 2 and first["total"] == 2
    await state.dispatcher.run_pending(now=T0)  # 暂停：不会发出
    for eid in ("z1", "z2"):
        rec = await state.storage.get_delivery(eid, "cf5")
        assert rec.status is EventStatus.SCHEDULED

    # 连续重复（立刻、稍后、很久以后、再立刻）：结论必须完全一致且稳定
    times = [T0, T0 + timedelta(seconds=1), T0 + timedelta(days=90),
             T0 + timedelta(days=90, seconds=1)]
    for t in times:
        rep = await state.dispatcher.replay_subscription_dead_letters(
            "tenant-a", "cf5", now=t)
        assert rep["total"] == 2
        assert rep["replayed"] == 0
        assert rep["already_replaying"] == 2
        assert _summary_map(rep) == {"z1": "ALREADY_REPLAYING",
                                     "z2": "ALREADY_REPLAYING"}
    # 全程没有任何真实发送（重放后订阅一直暂停）
    post = state.transport.calls[call_idx:]
    assert [c for c in post if c["url"] == "mock://cf/5"] == []
    print("\n[暂停积压重复批量] 4 次重复结论均稳定: total=2 "
          "ALREADY_REPLAYING，真实发送 0 次")

    # 恢复后按序发出一次，随后批量结论应转为 ALREADY_DELIVERED（仍非空）
    await state.dispatcher.resume_subscription("cf5")
    state.transport.script("mock://cf/5", [(200, "a"), (200, "b")])
    t = times[-1]
    for _ in range(10):
        await state.dispatcher.run_pending(now=t)
        t += timedelta(milliseconds=60)
    for eid in ("z1", "z2"):
        assert (await state.storage.get_delivery(eid, "cf5")).status \
            is EventStatus.DELIVERED
    done = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "cf5", now=t)
    assert done["total"] == 2 and done["already_delivered"] == 2
    assert _summary_map(done) == {"z1": "ALREADY_DELIVERED",
                                   "z2": "ALREADY_DELIVERED"}


@pytest.mark.asyncio
async def test_single_and_batch_replay_agree_on_mixed_states(state):
    """同一批事件处于不同真实状态时，单条与批量重放结论必须说得通。

    构造：d 始终 DEAD_LETTER；s/ok 经第一批批量重放后成功 DELIVERED
    （进入代际候选记忆）；w 之后成为死信并被单条重放，停在 SCHEDULED
    （暂停压住）。随后单条与批量对每个事件的结论都应与其真实状态一致。
    """
    sub = Subscription("cf6", "tenant-a", "order.created", "1.0.0",
                       "mock://cf/6", "key-a-new", max_attempts=1)
    await state.storage.upsert_subscription(sub)
    for eid in ("d", "s", "ok"):
        await _make_dead_letter(state, sub, eid, T0, failures=[(500, "x")])

    # 第一批批量：d/s/ok 全部 REPLAYED；投递时让 s、ok 成功，d 再次失败
    state.transport.script(
        "mock://cf/6", [(500, "d-again"), (200, "s-ok"), (200, "ok-ok")])
    b0 = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "cf6", now=T0)
    assert b0["replayed"] == 3
    t = T0
    for _ in range(10):
        await state.dispatcher.run_pending(now=t)
        t += timedelta(milliseconds=60)
    assert (await state.storage.get_delivery("s", "cf6")).status \
        is EventStatus.DELIVERED
    assert (await state.storage.get_delivery("ok", "cf6")).status \
        is EventStatus.DELIVERED
    assert (await state.storage.get_delivery("d", "cf6")).status \
        is EventStatus.DEAD_LETTER

    # w 成为新死信，单条重放后用暂停把它压在待投递队列（SCHEDULED）
    await _make_dead_letter(state, sub, "w", t, failures=[(500, "x")])
    await state.dispatcher.pause_subscription("cf6")
    o_w, _ = await state.dispatcher.replay_dead_letter("w", "cf6", now=t)
    assert o_w is ReplayOutcome.REPLAYED
    await state.dispatcher.run_pending(now=t)
    assert (await state.storage.get_delivery("w", "cf6")).status \
        is EventStatus.SCHEDULED

    # 单条重放：结论逐条对应真实状态
    single = {}
    for eid in ("d", "w", "s", "ok"):
        o, _ = await state.dispatcher.replay_dead_letter(eid, "cf6", now=t)
        single[eid] = o.value
    assert single == {
        "d": "REPLAYED",            # 真实死信 -> 单条本次生效
        "w": "ALREADY_REPLAYING",   # 压在队列中
        "s": "ALREADY_DELIVERED",
        "ok": "ALREADY_DELIVERED",
    }
    await state.dispatcher.run_pending(now=t)  # d 被单条认领后仍暂停，不发出

    # 批量：候选=当代死信(无，d刚被认领) ∪ 上代候选(d/s/ok)；w 是单条认领、
    # 本不在批量候选内，故批量只对 d/s/ok 给出与真实状态一致的结论。
    batch = await state.dispatcher.replay_subscription_dead_letters(
        "tenant-a", "cf6", now=t)
    bmap = _summary_map(batch)
    print("\n[单条vs批量] 单条=", single, " 批量=", bmap)
    assert batch["total"] == 3
    assert batch["replayed"] == 0
    assert batch["already_replaying"] == 1   # d 刚被单条认领
    assert batch["already_delivered"] == 2   # s/ok 已成功
    assert bmap["d"] == "ALREADY_REPLAYING"
    assert bmap["s"] == "ALREADY_DELIVERED"
    assert bmap["ok"] == "ALREADY_DELIVERED"
    # 与失败链路查询一致：失败链路确为空（d/w 都已离开 DEAD_LETTER）
    assert state.storage.list_dead_letters(subscription_id="cf6") == []


@pytest.mark.asyncio
async def test_worker_running_manual_pumps_do_not_resend_terminal(state):
    """后台 worker 运转期间并发手工 pump：成功事件不被二次发送，
    死信事件也不会被 pump 路径再拿出来发。"""
    sub = Subscription("cf7", "tenant-a", "order.created", "1.0.0",
                       "mock://cf/7", "key-a-new", max_attempts=2)
    await state.storage.upsert_subscription(sub)
    state.transport.script("mock://cf/7", [(200, "ok")])
    await _seed_event(state, "win", sub)

    state.start_worker()
    try:
        # 等 worker 自动投递完成，期间并发手工高频 pump
        deadline = 3.0
        waited = 0.0
        pumps = []
        while waited < deadline:
            rec = await state.storage.get_delivery("win", "cf7")
            if rec.status is EventStatus.DELIVERED:
                break
            pumps += await asyncio.gather(*[
                state.dispatcher.run_pending(now=T0) for _ in range(2)
            ])
            await asyncio.sleep(0.01)
            waited += 0.01
        # 终结后再补几路 pump：已终结事件不得被重发
        pumps += await asyncio.gather(*[
            state.dispatcher.run_pending(now=T0) for _ in range(3)
        ])
        await asyncio.sleep(0.03)
    finally:
        await state.stop_worker()

    rec = await state.storage.get_delivery("win", "cf7")
    sent = _call_bodies(state, "win", "mock://cf/7")
    print("\n[worker+手工pump] pump处理数=", pumps,
          "真实发送=", len(sent), "状态=", rec.status.value)
    assert rec.status is EventStatus.DELIVERED
    assert len(sent) == 1
    assert len(rec.attempts) == 1
