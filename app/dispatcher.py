"""出站投递：确定性退避重试、订阅内顺序保证、死信归档。

顺序模型：
- 每个订阅一条 FIFO 队列，事件入队即分配单调递增 seq；
- 队首事件未终结（成功/死信）之前，后续事件不得投递 —— 前序事件退避
  等待期间会"阻塞队头"，从而严格保持同一订阅内事件有序；
- 不同订阅之间完全并发，互不阻塞。

退避策略（可解释、可复现，无随机抖动）：
- 第 n 次尝试后的等待 = min(cap, base * 2**(n-1))；
- 成功或永久性失败（如 4xx）不重试；可重试故障（网络/408/429/5xx）重试；
- 达到 max_attempts 仍失败 → DEAD_LETTER，fail_reason 区分最后一次的
  底层原因（NETWORK_ERROR / HTTP_5XX / HTTP_4XX / ATTEMPTS_EXHAUSTED）。
"""
from __future__ import annotations

import asyncio
import json
from collections import deque
from datetime import timedelta

from app.logging_utils import get_logger
from app.models import (
    DeliveryAttempt,
    DeliveryKind,
    DeliveryRecord,
    EventStatus,
    FailReason,
    ReplayOutcome,
    Subscription,
    SubscriptionState,
    utcnow,
)
from app.security import sign
from app.storage import Storage
from app.transport import Transport

logger = get_logger(__name__)

BASE_BACKOFF_SECONDS = 0.05  # 本地验证用小基数
MAX_BACKOFF_SECONDS = 1.0
WORKER_POLL_SECONDS = 0.02  # 后台 worker 轮询节拍（到期才真正投递）
# 批量重放代际去重冷却窗：该窗口内对同一批死信的重复批量请求，逐条返回
# ALREADY_REPLAYING 而不重复认领（覆盖并发与紧随其后的连续触发）。
BATCH_REPLAY_COOLDOWN_SECONDS = 5.0


def compute_backoff(attempt: int, base: float = BASE_BACKOFF_SECONDS) -> float:
    """第 attempt 次尝试失败后的等待秒数：指数退避，封顶 MAX_BACKOFF_SECONDS。"""
    return min(MAX_BACKOFF_SECONDS, base * (2 ** (attempt - 1)))


_REASON_MAP = {
    "NETWORK_ERROR": FailReason.NETWORK_ERROR,
    "HTTP_5XX": FailReason.HTTP_5XX,
    "HTTP_4XX_RETRYABLE": FailReason.HTTP_5XX,
    "HTTP_4XX": FailReason.HTTP_4XX,
}


def check_outbound_credential(cred, now) -> FailReason | None:
    """出站凭据可用性校验，规则与入站验签的时间窗一致。

    返回 None 表示可用；否则返回网关侧失败原因——此类问题**不发起任何
    HTTP 请求**，结论与接收方返回的失败（NETWORK/4XX/5XX）明确区分。
    """
    if cred is None:
        return FailReason.CREDENTIAL_MISSING
    if now < cred.not_before:
        return FailReason.CREDENTIAL_NOT_YET_VALID
    if now > cred.not_after:
        return FailReason.CREDENTIAL_EXPIRED
    return None


class Dispatcher:
    def __init__(self, storage: Storage, transport: Transport) -> None:
        self._storage = storage
        self._transport = transport
        # subscription_id -> FIFO 队列，元素为 (event_id, subscription_id)
        self._queues: dict[str, deque[tuple[str, str]]] = {}
        self._cond = asyncio.Condition()
        # 并发/连续批量重放去重：subscription_id -> 上一代批量的
        # (候选event_ids, 认领时刻)。冷却窗内重复批量请求识别为落败。
        self._batch_last: dict[str, tuple[list[str], object]] = {}

    # 入队 ----------------------------------------------------------------
    async def enqueue(self, event_id: str, subscription: Subscription) -> DeliveryRecord:
        seq = await self._storage.next_seq(subscription.subscription_id)
        record = DeliveryRecord(
            event_id=event_id,
            subscription_id=subscription.subscription_id,
            status=EventStatus.SCHEDULED,
            seq=seq,
        )
        await self._storage.save_delivery(record)
        async with self._cond:
            self._queues.setdefault(subscription.subscription_id, deque()).append(
                (event_id, subscription.subscription_id)
            )
            self._cond.notify_all()
        logger.info(
            "delivery_enqueued event_id=%s subscription_id=%s seq=%s",
            event_id, subscription.subscription_id, seq,
        )
        return record

    # 暂停 / 恢复（订阅粒度，幂等） ---------------------------------------
    async def pause_subscription(self, subscription_id: str) -> SubscriptionState:
        """暂停某订阅的投递：队列与顺序保留，新事件照常入队，只是不发出。"""
        await self._storage.set_subscription_state(
            subscription_id, SubscriptionState.PAUSED
        )
        logger.info(
            "subscription_paused subscription_id=%s basis=OPERATOR_PAUSE",
            subscription_id,
        )
        return SubscriptionState.PAUSED

    async def resume_subscription(self, subscription_id: str) -> SubscriptionState:
        """恢复投递：暂停期间积压的事件按原 seq 顺序继续。"""
        await self._storage.set_subscription_state(
            subscription_id, SubscriptionState.ACTIVE
        )
        async with self._cond:
            self._cond.notify_all()
        logger.info(
            "subscription_resumed subscription_id=%s basis=OPERATOR_RESUME",
            subscription_id,
        )
        return SubscriptionState.ACTIVE

    async def subscription_state(self, subscription_id: str) -> SubscriptionState:
        return await self._storage.get_subscription_state(subscription_id)

    # 失败重放 ------------------------------------------------------------
    async def replay_dead_letter(
        self, event_id: str, subscription_id: str, now=None
    ) -> tuple[ReplayOutcome, DeliveryRecord | None]:
        """重放单条失败事件。

        规则：
        - 只有 DEAD_LETTER 可重放；已成功的绝不重发；非死信/不存在给出
          稳定且可区分的结论；
        - 在订阅锁内「判定+认领」，因此同一事件并发/连续多次重放只有一次
          生效，落败者得到 ALREADY_REPLAYING；
        - 重新入队到该订阅 FIFO **队尾**（按 seq 仍在所有先前事件之后），
          绝不插到尚未终结的前序事件之前；
        - 原死信结论归档保留，新一轮尝试从 attempt 1 重新计数。
        """
        now = now or utcnow()
        lock = await self._storage.subscription_lock(subscription_id)
        async with lock:
            record = await self._storage.get_delivery(event_id, subscription_id)
            if record is None:
                logger.info(
                    "replay_reject event_id=%s subscription_id=%s basis=NOT_FOUND",
                    event_id, subscription_id,
                )
                return ReplayOutcome.NOT_FOUND, None
            if record.status is EventStatus.DELIVERED:
                return ReplayOutcome.ALREADY_DELIVERED, record
            if record.status is not EventStatus.DEAD_LETTER:
                # SCHEDULED/DELIVERING/RETRYING：已在投递链路中（多半是
                # 先前一次重放已生效）
                return ReplayOutcome.ALREADY_REPLAYING, record

            # 归档原死信结论，重置投递状态
            record.archived_fail_reason = record.fail_reason
            record.archived_fail_detail = record.fail_detail
            record.archived_at = now
            record.replayed = True
            record.replay_count += 1
            record.status = EventStatus.SCHEDULED
            record.fail_reason = FailReason.NONE
            record.fail_detail = ""
            record.next_attempt_at = None
            record.attempts = []
            await self._storage.save_delivery(record)

            async with self._cond:
                queue = self._queues.setdefault(subscription_id, deque())
                marker = (event_id, subscription_id)
                if marker not in queue:
                    queue.append(marker)
                self._cond.notify_all()
            logger.info(
                "replay_accepted event_id=%s subscription_id=%s seq=%s "
                "replay_count=%s archived_reason=%s",
                event_id, subscription_id, record.seq, record.replay_count,
                record.archived_fail_reason.value
                if record.archived_fail_reason else None,
            )
            return ReplayOutcome.REPLAYED, record

    async def replay_subscription_dead_letters(
        self, tenant_id: str, subscription_id: str, now=None,
        cooldown_seconds: float = BATCH_REPLAY_COOLDOWN_SECONDS,
    ) -> dict:
        """批量重放某订阅下所有仍在失败链路中的事件。

        - 按 seq 升序逐条、在各自订阅锁内处理，保证顺序、天然反压（不会
          一次性压垮服务）；
        - 同一批事件并发或连续触发多次：只有一次真正重排，其余请求对每条
          给出稳定可区分的 ALREADY_REPLAYING（代际冷却窗去重，见
          BATCH_REPLAY_COOLDOWN_SECONDS）；
        - 已成功的不重发；冷却窗之后若事件再次落入死信，视为新一轮，可再
          次批量重放。
        """
        now = now or utcnow()
        sub = await self._storage.get_subscription(subscription_id)
        empty = {
            "tenant_id": tenant_id,
            "subscription_id": subscription_id,
            "found": False,
            "total": 0,
            "replayed": 0,
            "already_replaying": 0,
            "already_delivered": 0,
            "not_dead_letter": 0,
            "results": [],
        }
        if sub is None or sub.tenant_id != tenant_id:
            return empty

        results: list[dict] = []
        counts = {
            "replayed": 0,
            "already_replaying": 0,
            "already_delivered": 0,
            "not_dead_letter": 0,
        }

        lock = await self._storage.subscription_lock(subscription_id)
        async with lock:
            current = [
                r.event_id
                for r in self._storage.list_dead_letters(
                    tenant_id=tenant_id, subscription_id=subscription_id
                )
            ]
            last = self._batch_last.get(subscription_id)
            recent_ids: list[str] = []
            if last is not None:
                last_ids, last_at = last
                if (now - last_at).total_seconds() <= cooldown_seconds:
                    recent_ids = list(last_ids)
            recent_set = set(recent_ids)
            # 仍死信且不在上一代冷却候选内 -> 本次真正认领；仍死信但刚被
            # 上一代认领过 -> 稳定落败 ALREADY_REPLAYING（不重复认领）。
            fresh = [eid for eid in current if eid not in recent_set]
            claimed = [eid for eid in current if eid in recent_set]
            if fresh:
                # 登记本代候选与认领时刻，供并发/紧随其后的批次去重
                self._batch_last[subscription_id] = (list(fresh), now)

        for event_id in fresh:
            outcome, _ = await self.replay_dead_letter(
                event_id, subscription_id, now=now
            )
            results.append({"event_id": event_id, "outcome": outcome.value})
            key = outcome.value.lower()
            if key in counts:
                counts[key] += 1

        for event_id in claimed:
            results.append({
                "event_id": event_id,
                "outcome": ReplayOutcome.ALREADY_REPLAYING.value,
            })
            counts["already_replaying"] += 1

        # 纯落败批（当前已无死信，全部在上一代候选内）：把上一代候选逐条
        # 回带为 ALREADY_REPLAYING，使并发两批 total 一致、结论一一对应。
        if not results and recent_ids:
            for event_id in recent_ids:
                results.append({
                    "event_id": event_id,
                    "outcome": ReplayOutcome.ALREADY_REPLAYING.value,
                })
                counts["already_replaying"] += 1

        logger.info(
            "replay_batch subscription_id=%s total=%s replayed=%s "
            "already_replaying=%s",
            subscription_id, len(results), counts["replayed"],
            counts["already_replaying"],
        )
        return {
            "tenant_id": tenant_id,
            "subscription_id": subscription_id,
            "found": True,
            "total": len(results),
            **counts,
            "results": results,
        }

    # 确定性驱动（测试与本地演示） ----------------------------------------
    async def run_pending(self, now=None) -> int:
        """推进所有到期任务：仅投递各订阅队首、且已到 next_attempt_at 的任务。

        每次调用让每个就绪队首尝试一次（不内部循环退避），因此重试节奏
        完全由调用方/测试按 backoff 推进，结果确定可复现。返回处理数。
        """
        now = now or utcnow()
        processed = 0
        # 快照订阅，避免迭代中变更
        for subscription_id in list(self._queues.keys()):
            async with self._cond:
                queue = self._queues.get(subscription_id)
                if not queue:
                    continue
                event_id, sub_id = queue[0]
            # 暂停订阅：保留队首与积压、不发出；也不影响其他订阅继续投递
            if await self._storage.get_subscription_state(sub_id) is SubscriptionState.PAUSED:
                continue
            record = await self._storage.get_delivery(event_id, sub_id)
            if record is None:
                async with self._cond:
                    queue.popleft()
                continue
            if record.next_attempt_at is not None and now < record.next_attempt_at:
                continue  # 队头仍在退避：严格顺序，后续事件不得越过
            subscription = await self._storage.get_subscription(sub_id)
            if subscription is None:
                continue
            await self._attempt_head(record, subscription, now)
            processed += 1
        return processed

    async def run_until_drained(
        self, max_cycles: int = 100, sleep: float = BASE_BACKOFF_SECONDS / 2
    ) -> None:
        """循环推进直到没有可处理任务（退避中也会等待）。仅用于本地演示。"""
        for _ in range(max_cycles):
            if await self.run_pending() == 0 and not self._has_active():
                return
            await asyncio.sleep(sleep)

    # 后台自动运转 --------------------------------------------------------
    async def run_forever(self, poll: float = WORKER_POLL_SECONDS) -> None:
        """服务运行态的投递 worker：周期性推进所有到期任务。

        投递/重试由此自动往前走，无需任何手工 pump；退避节奏仍由
        next_attempt_at 的真实到期决定（确定性退避不变）。暂停的订阅在
        run_pending 内被跳过，不影响其他订阅。应通过 asyncio task 启动并
        在关停时 cancel。
        """
        logger.info("delivery_worker_started basis=AUTO_POLL poll=%.3fs", poll)
        try:
            while True:
                try:
                    await self.run_pending()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 - worker 不能因单次异常退出
                    logger.exception("delivery_worker_tick_failed")
                await asyncio.sleep(poll)
        except asyncio.CancelledError:
            logger.info("delivery_worker_stopped basis=SHUTDOWN")
            raise

    def _has_active(self) -> bool:
        for queue in self._queues.values():
            if queue:
                return True
        return False

    # 单次尝试 ------------------------------------------------------------
    async def _attempt_head(
        self, record: DeliveryRecord, subscription: Subscription, now=None
    ) -> None:
        lock = await self._storage.subscription_lock(subscription.subscription_id)
        async with lock:
            await self._deliver_one(record, subscription, now)
            # 终结则出队，否则保留队首等待退避
            terminal = record.status in (EventStatus.DELIVERED, EventStatus.DEAD_LETTER)
            if terminal:
                async with self._cond:
                    queue = self._queues.get(subscription.subscription_id)
                    if queue and queue[0] == (record.event_id, record.subscription_id):
                        queue.popleft()
                    self._cond.notify_all()

    async def _deliver_one(
        self, record: DeliveryRecord, subscription: Subscription, now=None
    ) -> None:
        event = await self._storage.get_event(subscription.tenant_id, record.event_id)
        attempt_no = len(record.attempts) + 1
        record.status = EventStatus.DELIVERING
        started = now or utcnow()

        # 出站凭据可用性：与入站验签同一套有效期/轮换窗口规则。
        # 不可用则**不发起请求**，直接给出网关侧死信结论（可与对方失败区分）。
        cred = await self._storage.get_credential(subscription.credential_key_id)
        cred_failure = check_outbound_credential(cred, started)
        if cred_failure is not None:
            window = (
                f"{cred.not_before.isoformat()}~{cred.not_after.isoformat()}"
                if cred is not None else "n/a"
            )
            detail = (
                f"outbound credential unusable basis={cred_failure.value} "
                f"key_id={subscription.credential_key_id} window={window} "
                f"(no request sent)"
            )
            record.attempts.append(
                DeliveryAttempt(
                    attempt=attempt_no,
                    started_at=started,
                    finished_at=started,
                    kind=DeliveryKind.PERMANENT,
                    http_status=None,
                    detail=detail,
                )
            )
            record.status = EventStatus.DEAD_LETTER
            record.fail_reason = cred_failure
            record.fail_detail = (
                f"attempts={attempt_no}/{subscription.max_attempts} {detail}"
            )
            record.next_attempt_at = None
            logger.info(
                "delivery_dead_letter event_id=%s subscription_id=%s "
                "reason=%s basis=CREDENTIAL_GATE detail=%s",
                record.event_id, subscription.subscription_id,
                cred_failure.value, detail,
            )
            await self._storage.save_delivery(record)
            return

        body = json.dumps(
            {
                "event_id": event.event_id if event else record.event_id,
                "event_type": event.event_type if event else "",
                "version": event.schema_version_used if event else "",
                "payload": event.payload if event else {},
            },
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")

        headers = {}
        if cred is not None:
            timestamp = str(int(started.timestamp()))
            headers = {
                "Content-Type": "application/json",
                "X-Key-Id": cred.key_id,
                "X-Timestamp": timestamp,
                "X-Signature": sign(cred.secret, timestamp, body),
                "X-Delivery-Id": f"{record.event_id}:{attempt_no}",
            }
        else:
            headers = {"Content-Type": "application/json"}

        result = await self._transport.post(subscription.callback_url, headers, body)
        finished = now or utcnow()
        backoff = 0.0
        record.attempts.append(
            DeliveryAttempt(
                attempt=attempt_no,
                started_at=started,
                finished_at=finished,
                kind=result.kind,
                http_status=result.http_status,
                detail=result.detail,
            )
        )

        if result.kind is DeliveryKind.SUCCESS:
            record.status = EventStatus.DELIVERED
            record.fail_reason = FailReason.NONE
            record.fail_detail = ""
            record.next_attempt_at = None
            logger.info(
                "delivery_success event_id=%s subscription_id=%s attempt=%s basis=%s",
                record.event_id, subscription.subscription_id, attempt_no,
                result.reason_code,
            )
            await self._storage.save_delivery(record)
            return

        underlying = _REASON_MAP.get(result.reason_code, FailReason.ATTEMPTS_EXHAUSTED)
        if result.kind is DeliveryKind.PERMANENT or attempt_no >= subscription.max_attempts:
            # 永久失败立即死信；重试可恢复故障在耗尽后死信。
            exhausted = result.kind is DeliveryKind.RETRYABLE
            record.status = EventStatus.DEAD_LETTER
            record.fail_reason = (
                FailReason.ATTEMPTS_EXHAUSTED if exhausted else underlying
            )
            record.fail_detail = (
                f"attempts={attempt_no}/{subscription.max_attempts} "
                f"last_basis={result.reason_code} last_detail={result.detail}"
            )
            record.next_attempt_at = None
            logger.info(
                "delivery_dead_letter event_id=%s subscription_id=%s attempts=%s "
                "reason=%s basis=%s detail=%s",
                record.event_id, subscription.subscription_id, attempt_no,
                record.fail_reason.value, result.reason_code, result.detail,
            )
            await self._storage.save_delivery(record)
            return

        backoff = compute_backoff(attempt_no)
        record.status = EventStatus.RETRYING
        record.fail_reason = underlying
        record.fail_detail = (
            f"scheduled retry after {backoff:.3f}s; last_basis={result.reason_code} "
            f"last_detail={result.detail}"
        )
        record.next_attempt_at = finished + timedelta(seconds=backoff)
        record.attempts[-1].backoff_seconds = backoff
        logger.info(
            "delivery_retry_scheduled event_id=%s subscription_id=%s attempt=%s "
            "next_in=%.3fs reason=%s basis=%s",
            record.event_id, subscription.subscription_id, attempt_no, backoff,
            underlying.value, result.reason_code,
        )
        await self._storage.save_delivery(record)
