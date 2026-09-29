"""运行态自运转与出站凭据有效期：

1. 服务启动后后台 worker 自动推进投递/重试/死信，无需手工 pump；
2. 出站凭据与入站验签遵守同一时间窗：不在窗口内绝不发出，结论与
   接收方失败明确区分。
"""
import asyncio
from datetime import timedelta

import pytest

from app.bootstrap import AppState
from app.models import (
    Credential,
    EventStatus,
    FailReason,
    InboundEvent,
    Subscription,
    utcnow,
)
from tests.helpers import build_event, raw_body, signed_headers

T0 = utcnow()


async def _wait_terminal(storage, event_id, sub_id, timeout=3.0):
    deadline = timeout
    step = 0.01
    waited = 0.0
    while waited < deadline:
        rec = await storage.get_delivery(event_id, sub_id)
        if rec and rec.status in (EventStatus.DELIVERED, EventStatus.DEAD_LETTER):
            return rec
        await asyncio.sleep(step)
        waited += step
    raise AssertionError(f"{event_id} not terminal after {timeout}s")


async def _accept(state, body, secret="secret-new", key_id="key-a-new"):
    ts = str(int(utcnow().timestamp()))
    headers = signed_headers(secret, body, key_id, ts, tenant_id="tenant-a")
    return await state.service.accept(
        "tenant-a", headers["X-Key-Id"], headers["X-Timestamp"],
        headers["X-Signature"], raw_body(body), body,
    )


@pytest.mark.asyncio
async def test_background_worker_delivers_and_retries_without_manual_pump():
    state = AppState(seed=True)
    state.start_worker()
    try:
        state.transport.script(
            "mock://partner-a/order-hook",
            [(503, "busy"), (503, "busy"), (200, "ok")],
        )
        r = await _accept(state, build_event("evt-auto-retry"))
        assert r.accepted and r.status_code == 202

        rec = await _wait_terminal(
            state.storage, "evt-auto-retry", "sub-a-orders"
        )
        print(f"\n[event=evt-auto-retry] worker自动投递 状态={rec.status.value} "
              f"尝试={[a.http_status for a in rec.attempts]}（全程无pump）")
        assert rec.status is EventStatus.DELIVERED
        assert [a.http_status for a in rec.attempts] == [503, 503, 200]
    finally:
        await state.stop_worker()


@pytest.mark.asyncio
async def test_background_worker_drives_exhaustion_into_dead_letter():
    state = AppState(seed=True)
    state.start_worker()
    try:
        state.transport.script(
            "mock://partner-a/order-hook", [(500, "x")] * 4
        )
        await _accept(state, build_event("evt-auto-dlq"))
        rec = await _wait_terminal(
            state.storage, "evt-auto-dlq", "sub-a-orders"
        )
        print(f"\n[event=evt-auto-dlq] worker自动跑尽重试 状态={rec.status.value} "
              f"reason={rec.fail_reason.value}")
        assert rec.status is EventStatus.DEAD_LETTER
        assert rec.fail_reason is FailReason.ATTEMPTS_EXHAUSTED
    finally:
        await state.stop_worker()


async def _seed_delivery(state, event_id, sub, *, key_id="key-a-new"):
    ev = InboundEvent(
        event_id, "tenant-a", "order.created", "1.0.0",
        {"order_id": event_id}, T0,
    )
    await state.storage.save_event_if_absent(ev)
    sub.credential_key_id = key_id
    await state.storage.upsert_subscription(sub)
    await state.dispatcher.enqueue(event_id, sub)


@pytest.mark.asyncio
async def test_expired_outbound_credential_dead_letters_without_sending(state):
    sub = Subscription("s-cred", "tenant-a", "order.created", "1.0.0",
                       "mock://cred/expired", "key-a-expired", max_attempts=4)
    await _seed_delivery(state, "e-cred-exp", sub, key_id="key-a-expired")
    await state.dispatcher.run_pending(now=T0)
    rec = await state.storage.get_delivery("e-cred-exp", "s-cred")
    sent = [c for c in state.transport.calls if b"e-cred-exp" in c["body"]]
    print(f"\n[event=e-cred-exp] 过期出站凭据 状态={rec.status.value} "
          f"reason={rec.fail_reason.value} 实际发出请求数={len(sent)}")
    assert rec.status is EventStatus.DEAD_LETTER
    assert rec.fail_reason is FailReason.CREDENTIAL_EXPIRED
    assert sent == []  # 网关侧问题：不发起任何请求
    assert "no request sent" in rec.fail_detail


@pytest.mark.asyncio
async def test_not_yet_valid_and_missing_outbound_credential_distinct(state):
    from datetime import timedelta
    await state.storage.upsert_credential(Credential(
        "key-future", "tenant-a", "secret-future",
        T0 + timedelta(days=1), T0 + timedelta(days=2)))

    sub1 = Subscription("s-future", "tenant-a", "order.created", "1.0.0",
                        "mock://cred/future", "key-future", max_attempts=4)
    await _seed_delivery(state, "e-cred-future", sub1, key_id="key-future")
    await state.dispatcher.run_pending(now=T0)
    rec = await state.storage.get_delivery("e-cred-future", "s-future")
    assert rec.status is EventStatus.DEAD_LETTER
    assert rec.fail_reason is FailReason.CREDENTIAL_NOT_YET_VALID

    sub2 = Subscription("s-missing", "tenant-a", "order.created", "1.0.0",
                        "mock://cred/missing", "key-ghost", max_attempts=4)
    await _seed_delivery(state, "e-cred-missing", sub2, key_id="key-ghost")
    await state.dispatcher.run_pending(now=T0)
    rec2 = await state.storage.get_delivery("e-cred-missing", "s-missing")
    print(f"\n[event=e-cred-missing] 凭据缺失 reason={rec2.fail_reason.value}")
    assert rec2.fail_reason is FailReason.CREDENTIAL_MISSING


@pytest.mark.asyncio
async def test_receiver_failure_is_distinct_from_credential_failure(state):
    # 同样是死信：凭据问题 vs 接收方问题，fail_reason 必须一眼可分
    sub = Subscription("s-vs", "tenant-a", "order.created", "1.0.0",
                       "mock://cred/vs", "key-a-new", max_attempts=2)
    state.transport.script("mock://cred/vs", [(500, "err"), (503, "err")])
    await _seed_delivery(state, "e-recv-fail", sub)
    now = T0
    for _ in range(6):
        await state.dispatcher.run_pending(now=now)
        rec = await state.storage.get_delivery("e-recv-fail", "s-vs")
        if rec.status is EventStatus.DEAD_LETTER:
            break
        now += timedelta(milliseconds=60)

    recv = await state.storage.get_delivery("e-recv-fail", "s-vs")
    assert recv.fail_reason is FailReason.ATTEMPTS_EXHAUSTED
    assert "last_basis=HTTP" in recv.fail_detail
    sent = [c for c in state.transport.calls if b"e-recv-fail" in c["body"]]
    assert len(sent) == 2  # 接收方失败：请求确实发出去了

    sub2 = Subscription("s-vs2", "tenant-a", "order.created", "1.0.0",
                        "mock://cred/vs2", "key-a-expired", max_attempts=2)
    await _seed_delivery(state, "e-cred-fail", sub2, key_id="key-a-expired")
    await state.dispatcher.run_pending(now=T0)
    cred = await state.storage.get_delivery("e-cred-fail", "s-vs2")
    assert cred.fail_reason is FailReason.CREDENTIAL_EXPIRED
    print(f"\n[对比] 接收方失败={recv.fail_reason.value}(发出{len(sent)}次) "
          f"vs 凭据失败={cred.fail_reason.value}(发出0次)")
