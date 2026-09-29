"""本地端到端演示：无需启动服务器、无需外部依赖。

运行： uv run python demo.py
演示链路：
  1) 凭据轮换重叠期：新旧凭据都能验签，过期凭据被明确拒绝；
  2) 幂等：重复（含并发）请求只受理一次，结论稳定可区分；
  3) 契约演进：未知主版本拒绝、次版本新字段保留；
  4) 重试：503 两次后成功（确定性退避）；
  5) 重试耗尽：持续 500 -> 死信，失败原因可查询；
  6) 顺序：同订阅内事件严格有序（队头阻塞）；
  7) 租户隔离：A/B 额度与凭据互不影响；
  8) 暂停/恢复：暂停期间受理但不发出，恢复后按原顺序补发；
  9) 死信重放：单条/批量重放顺序、并发幂等、凭据失窗与对方失败可区分；
  10) 自走投递：服务启动后 worker 自动推进，无需手工 pump。
"""
from __future__ import annotations

import asyncio
import json
from datetime import timedelta

from app.bootstrap import AppState
from app.dispatcher import compute_backoff
from app.logging_utils import get_logger
from app.models import EventStatus, Subscription
from app.security import sign

logger = get_logger("demo")


def _envelope(event_id, version="1.0.0", payload=None, etype="order.created"):
    return {
        "event_id": event_id,
        "event_type": etype,
        "version": version,
        "payload": payload or {"order_id": event_id},
    }


async def post(state, tenant, key_id, secret, body, now):
    raw = json.dumps(body, separators=(",", ":")).encode()
    ts = str(int(now.timestamp()))
    return await state.service.accept(
        tenant, key_id, ts, sign(secret, ts, raw), raw, body, now=now
    )


async def pump_cycles(state, start, cycles=60, step_ms=60):
    now = start
    for _ in range(cycles):
        await state.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=step_ms)
    return now


async def make_sub(state, sub_id, url, max_attempts=4):
    sub = Subscription(sub_id, "tenant-a", "order.created", "1.0.0",
                       url, "key-a-new", max_attempts=max_attempts)
    await state.storage.upsert_subscription(sub)
    return sub



async def _setup_minimal_tenant(state, start):
    from app.models import Credential, EventSchema, FieldSpec, Tenant
    s = state.storage
    await s.upsert_tenant(Tenant("tenant-a", "A", 60))
    await s.upsert_credential(Credential(
        "key-a-new", "tenant-a", "secret-new",
        start - timedelta(days=1), start + timedelta(days=29)))
    await s.upsert_schema(EventSchema(
        "order.created", "1.2.0",
        {"order_id": FieldSpec("order_id", "string")}))


async def main() -> None:
    state = AppState(seed=True)
    s = state.storage
    start = None

    from app.models import utcnow
    start = utcnow()

    print("=" * 70)
    print("1) 凭据轮换：旧凭据(deprecated, 仍在重叠期) / 新凭据 / 过期凭据")
    print("=" * 70)
    for key, secret, eid in (
        ("key-a-old", "secret-old", "demo-auth-old"),
        ("key-a-new", "secret-new", "demo-auth-new"),
        ("key-a-expired", "secret-expired", "demo-auth-exp"),
    ):
        r = await post(state, "tenant-a", key, secret, _envelope(eid), start)
        verdict = f"accepted={r.accepted} http={r.status_code} reason={r.reason.value}"
        print(f"   event={eid:18s} key={key:14s} -> {verdict} {r.detail}")

    print("\n" + "=" * 70)
    print("2) 幂等：同一事件并发 10 次")
    print("=" * 70)
    body = _envelope("demo-idem-1")
    barrier = asyncio.Barrier(10)

    async def one():
        async with barrier:
            return await post(state, "tenant-a", "key-a-new", "secret-new",
                              body, start)

    results = await asyncio.gather(*(one() for _ in range(10)))
    first = sum(not r.duplicate for r in results)
    dup = sum(r.duplicate for r in results)
    print(f"   event=demo-idem-1 首次受理={first} 重复返回={dup} "
          f"重复结论={results[0].detail if results[0].duplicate else [r for r in results if r.duplicate][0].detail}")

    print("\n" + "=" * 70)
    print("3) 契约：未知主版本拒绝；1.3 次版本携带未知字段，保留不丢")
    print("=" * 70)
    r = await post(state, "tenant-a", "key-a-new", "secret-new",
                   _envelope("demo-v9", version="9.0.0"), start)
    print(f"   event=demo-v9      -> accepted={r.accepted} "
          f"http={r.status_code} reason={r.reason.value}")
    r = await post(state, "tenant-a", "key-a-new", "secret-new",
                   _envelope("demo-v13", version="1.3.0",
                             payload={"order_id": "demo-v13", "promo": "KEPT"}),
                   start)
    print(f"   event=demo-v13     -> accepted={r.accepted} "
          f"unknown_fields={r.unknown_fields} schema={r.schema_version_used}")

    print("\n" + "=" * 70)
    print(f"4) 退避重试：503 x2 后 200；退避序列 "
          f"{[round(compute_backoff(i), 3) for i in (1, 2, 3)]}s")
    print("=" * 70)
    url_retry = "mock://demo/retry"
    await make_sub(state, "sub-demo-retry", url_retry)
    state.transport.script(url_retry, [(503, "busy"), (503, "busy"), (200, "ok")])
    r = await post(state, "tenant-a", "key-a-new", "secret-new",
                   _envelope("demo-retry"), start)
    print(f"   event=demo-retry 受理={r.accepted} deliveries={r.deliveries}")
    await pump_cycles(state, start)
    rec = await s.get_delivery("demo-retry", "sub-demo-retry")
    print(f"   event=demo-retry 最终={rec.status.value} "
          f"尝试={[a.http_status for a in rec.attempts]}")

    print("\n" + "=" * 70)
    print("5) 重试耗尽：持续 500 -> 死信（结果不静默，可查询）")
    print("=" * 70)
    url_dlq = "mock://demo/dlq"
    await make_sub(state, "sub-demo-dlq", url_dlq)
    state.transport.script(url_dlq, [(500, "err")] * 4)
    await post(state, "tenant-a", "key-a-new", "secret-new",
               _envelope("demo-dlq"), start)
    await pump_cycles(state, start)
    rec = await s.get_delivery("demo-dlq", "sub-demo-dlq")
    print(f"   event=demo-dlq 最终={rec.status.value} reason={rec.fail_reason.value}")
    print(f"                   detail={rec.fail_detail}")
    dlq = s.list_deliveries(status=EventStatus.DEAD_LETTER)
    print(f"   死信链路查询 /admin/deliveries/dead-letter -> {len(dlq)} 条: "
          f"{[r.event_id for r in dlq]}")

    print("\n" + "=" * 70)
    print("6) 顺序保证：同订阅 ord-A 失败期间 ord-B 不越过队头")
    print("=" * 70)
    # 无种子的隔离状态：恰好一个订阅、一个回调，序列无任何交叉
    state6 = AppState(seed=False)
    await _setup_minimal_tenant(state6, start)
    url_ord = "mock://demo6/order"
    await make_sub(state6, "sub-demo6", url_ord, max_attempts=3)
    state6.transport.script(
        url_ord, [(503, "x"), (503, "x"), (503, "x"), (200, "ok")]
    )
    await post(state6, "tenant-a", "key-a-new", "secret-new",
               _envelope("demo-ord-A"), start)
    await post(state6, "tenant-a", "key-a-new", "secret-new",
               _envelope("demo-ord-B"), start)
    now = start
    for _ in range(60):
        await state6.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    seq = [json.loads(c["body"])["event_id"] for c in state6.transport.calls]
    first_b = seq.index("demo-ord-B")
    print(f"   实际请求顺序={seq}")
    print(f"   ord-B 首次出现在第 {first_b} 次请求，此前全部为 ord-A；"
          f"严格有序={all(e == 'demo-ord-A' for e in seq[:first_b])}")
    ra = await state6.storage.get_delivery("demo-ord-A", "sub-demo6")
    rb = await state6.storage.get_delivery("demo-ord-B", "sub-demo6")
    print(f"   最终: A={ra.status.value}(尝试{len(ra.attempts)}次) "
          f"B={rb.status.value}(尝试{len(rb.attempts)}次)")

    print("\n" + "=" * 70)
    print("7) 租户隔离：tenant-b 额度 5/min，第 6 个事件被拒；tenant-a 不受影响")
    print("=" * 70)
    for i in range(6):
        r = await post(state, "tenant-b", "key-b-1", "secret-b",
                       _envelope(f"demo-b-{i}", payload={"order_id": f"b{i}"}),
                       start)
        if not r.accepted:
            print(f"   event=demo-b-{i} -> http={r.status_code} "
                  f"reason={r.reason.value} ({r.detail})")
    r = await post(state, "tenant-a", "key-a-new", "secret-new",
                   _envelope("demo-a-ok"), start)
    print(f"   event=demo-a-ok tenant-a 仍可受理 -> accepted={r.accepted}")

    print("\n" + "=" * 70)
    print("8) 暂停/恢复：暂停期间受理但不发出，恢复后按原 seq 顺序补发")
    print("=" * 70)
    state8 = AppState(seed=False)
    await _setup_minimal_tenant(state8, start)
    url8 = "mock://demo8/order"
    sub8 = await make_sub(state8, "sub-demo8", url8, max_attempts=2)
    await state8.dispatcher.pause_subscription("sub-demo8", now=start)
    await post(state8, "tenant-a", "key-a-new", "secret-new",
               _envelope("demo8-a"), start)
    await post(state8, "tenant-a", "key-a-new", "secret-new",
               _envelope("demo8-b"), start)
    now = start
    for _ in range(5):
        await state8.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    print(f"   暂停期间实际发出请求数={len(state8.transport.calls)}（应为 0）")
    await state8.dispatcher.resume_subscription("sub-demo8", now=now)
    for _ in range(8):
        await state8.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    order8 = [json.loads(c["body"])["event_id"] for c in state8.transport.calls]
    print(f"   恢复后发送顺序={order8}（严格 a 先 b 后）")

    print("\n" + "=" * 70)
    print("9) 死信重放：耗尽后单条/批量重放，凭据失窗与对方失败原因可区分")
    print("=" * 70)
    state9 = AppState(seed=False)
    await _setup_minimal_tenant(state9, start)
    url9 = "mock://demo9/order"
    await make_sub(state9, "sub-demo9", url9, max_attempts=2)
    state9.transport.script(url9, [(500, "x"), (500, "x"), (500, "x"), (500, "x")])
    await post(state9, "tenant-a", "key-a-new", "secret-new",
               _envelope("demo9-a"), start)
    await post(state9, "tenant-a", "key-a-new", "secret-new",
               _envelope("demo9-b"), start)
    for _ in range(12):
        await state9.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    dlq9 = state9.storage.list_deliveries(
        subscription_id="sub-demo9", status=EventStatus.DEAD_LETTER)
    print(f"   死信 {[r.event_id for r in dlq9]}，原因={dlq9[0].fail_reason.value}")

    # 并发重放同一条：仅一次生效
    barrier9 = asyncio.Barrier(4)

    async def replay9():
        async with barrier9:
            return await state9.dispatcher.replay_one("sub-demo9", "demo9-a", now=now)

    rr = await asyncio.gather(*(replay9() for _ in range(4)))
    print(f"   demo9-a 并发 4 次重放结论={[r.outcome.value for r in rr]}")

    # 批量重放（含上面已在途的 a 与另一条 b），接收方恢复
    state9.transport.script(url9, [(200, "ok"), (200, "ok"), (200, "ok")])
    _, batch = await state9.dispatcher.replay_subscription_dead_letters(
        "sub-demo9", now=now)
    print(f"   批量重放结论={[(r.event_id, r.outcome.value) for r in batch]}")
    for _ in range(10):
        await state9.dispatcher.run_pending(now=now)
        now += timedelta(milliseconds=60)
    finals = {eid: (await state9.storage.get_delivery(eid, "sub-demo9")).status.value
              for eid in ("demo9-a", "demo9-b")}
    print(f"   重放后最终状态={finals}")

    # 凭据已过期：请求不发出，直接死信，原因与接收方失败严格区分
    from app.models import Credential as _Cred, InboundEvent as _IEvt
    await state9.storage.upsert_credential(_Cred(
        "key-9-expired", "tenant-a", "secret-9-exp",
        start - timedelta(days=60), start - timedelta(days=1)))
    sub_bad = Subscription("sub-demo9-bad", "tenant-a", "order.created", "1.0.0",
                           "mock://demo9/bad", "key-9-expired", max_attempts=2)
    await state9.storage.upsert_subscription(sub_bad)
    ev_bad = _IEvt("demo9-cred", "tenant-a", "order.created", "1.0.0",
                   {"order_id": "demo9-cred"}, start)
    await state9.storage.save_event_if_absent(ev_bad)
    await state9.dispatcher.enqueue("demo9-cred", sub_bad)
    await state9.dispatcher.run_pending(now=now)
    rec_bad = await state9.storage.get_delivery("demo9-cred", "sub-demo9-bad")
    sent_bad = [c for c in state9.transport.calls if c["url"] == "mock://demo9/bad"]
    print(f"   demo9-cred 凭据失窗 状态={rec_bad.status.value} "
          f"reason={rec_bad.fail_reason.value} 实际发出={len(sent_bad)} "
          f"attempts={len(rec_bad.attempts)}（请求未发出，可与对方 500 区分）")

    print("\n" + "=" * 70)
    print("10) 自走投递：start_worker 后自动成功/重试/死信，无需 pump")
    print("=" * 70)
    state10 = AppState(seed=False)
    await _setup_minimal_tenant(state10, start)
    url10 = "mock://demo10/order"
    await make_sub(state10, "sub-demo10", url10, max_attempts=2)
    state10.transport.script(url10, [(503, "busy"), (200, "ok")])
    state10.dispatcher.start_worker()
    try:
        await post(state10, "tenant-a", "key-a-new", "secret-new",
                   _envelope("demo10-a"), start)
        rec10 = None
        for _ in range(100):
            rec10 = await state10.storage.get_delivery("demo10-a", "sub-demo10")
            if rec10.status in (EventStatus.DELIVERED, EventStatus.DEAD_LETTER):
                break
            await asyncio.sleep(0.02)
        print(f"   worker 自走最终={rec10.status.value} "
              f"尝试={[a.http_status for a in rec10.attempts]}（全程无 pump）")
    finally:
        await state10.dispatcher.stop_worker()

    print("\n演示完成。")


if __name__ == "__main__":
    asyncio.run(main())
