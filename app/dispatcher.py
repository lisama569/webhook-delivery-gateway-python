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
# 批量重放代际去重的默认冷却窗。语义：
# - None（默认）：代际候选不过期——重复/边界场景下批量结论永远与事件真实
#   状态对得上（暂停积压、已成功等场景连续调用结论与总数稳定一致）；
# - 显式传 0：完全不读/不写代际候选（仅测试用，模拟"全新一轮"）；
# - 显式传正数：该秒数后代际候选失效。
# 真正能否重放始终以事件**当前真实状态**为准：只要事件再次落入
# DEAD_LETTER（重放后又失败），无论距上次操作多久都可以再次重放，
# 不会被当成"正在重放"卡住。
BATCH_REPLAY_COOLDOWN_SECONDS: float | None = None


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
        # (候选event_ids, 认领时刻)。候选 = 当代死信 ∪ 上代候选，因此重复
        # 批量调用对每条事件都能给出与真实状态一致、可区分的逐条结论。
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
            return await self._replay_claim_locked(event_id, subscription_id, now)

    async def _replay_claim_locked(
        self, event_id: str, subscription_id: str, now
    ) -> tuple[ReplayOutcome, DeliveryRecord | None]:
        """判定+认领+排队，调用方必须已持有该订阅锁。

        结论完全以事件**当前真实状态**为准——即使它曾被上一代批量认领过，
        只要此刻又是 DEAD_LETTER（重放后再次失败），就可以再次生效重放，
        不会被时间窗或代际标记永久卡住。
        """
        record = await self._storage.get_delivery(event_id, subscription_id)
        if record is None:
            logger.info(
                "replay_reject event_id=%s subscription_id=%s basis=NOT_FOUND",
                event_id, subscription_id,
            )
            return ReplayOutcome.NOT_FOUND, None
        if record.status is EventStatus.DELIVERED:
            logger.info(
                "replay_reject event_id=%s subscription_id=%s "
                "basis=ALREADY_DELIVERED",
                event_id, subscription_id,
            )
            return ReplayOutcome.ALREADY_DELIVERED, record
        if record.status is not EventStatus.DEAD_LETTER:
            # SCHEDULED/DELIVERING/RETRYING：已在投递链路中（多半是
            # 先前一次重放已生效）
            logger.info(
                "replay_reject event_id=%s subscription_id=%s "
                "basis=ALREADY_REPLAYING status=%s",
                event_id, subscription_id, record.status.value,
            )
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
        cooldown_seconds: float | None = BATCH_REPLAY_COOLDOWN_SECONDS,
    ) -> dict:
        """批量重放某订阅下所有仍在失败链路中的事件。

        结论与事件**当前真实状态**严格对齐，重复/并发/连续调用稳定一致：

        - 候选集合 = 当代仍在失败链路（DEAD_LETTER）的事件 ∪ 上一代批量
          候选（未过期时）。因此同一批重复触发时，每条事件都拿到逐条结论，
          不会这次有逐条结论、下次返回空结果或 total 对不上；
        - 逐条在订阅锁内判定：当前 DEAD_LETTER -> 本次真正认领重排
          （REPLAYED）；SCHEDULED/DELIVERING/RETRYING -> ALREADY_REPLAYING；
          DELIVERED -> ALREADY_DELIVERED；记录消失则跳过（NOT_FOUND 不计入）；
        - 整个「判定+认领+排队」在同一把订阅锁内按 seq 升序完成：并发两批
          只有一批真正重排，另一批逐条落败，天然保序且形成反压；
        - 重放后再次失败回到 DEAD_LETTER 的事件，无论距上次操作多久都能
          再次 REPLAYED——代际候选只是「保证重复批次能逐条回带结论」的
          记忆，从不阻止真实死信被重新认领；
        - cooldown_seconds=None 时代际记忆不过期；=0 时完全不读不写
          （"全新一轮"语义，无死信即空结果）；正数为有限冷却秒数。
        """
        now = now or utcnow()
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
        sub = await self._storage.get_subscription(subscription_id)
        if sub is None or sub.tenant_id != tenant_id:
            return empty

        lock = await self._storage.subscription_lock(subscription_id)
        async with lock:
            # 当代死信（按 seq 升序）
            current_records = self._storage.list_dead_letters(
                tenant_id=tenant_id, subscription_id=subscription_id
            )
            current_ids = [r.event_id for r in current_records]

            # 上代候选（受冷却窗约束）：仅用于让重复批次对已离开失败链路的
            # 事件仍能逐条回带稳定结论。
            prior_ids: list[str] = []
            if cooldown_seconds != 0:
                last = self._batch_last.get(subscription_id)
                if last is not None:
                    last_ids, last_at = last
                    if (
                        cooldown_seconds is None
                        or (now - last_at).total_seconds() <= cooldown_seconds
                    ):
                        prior_ids = list(last_ids)

            # 候选 = 当代死信 ∪ 上代候选；按存储中的 seq 顺序稳定排列，
            # 存储中已不存在的上代事件（NOT_FOUND）直接剔除。
            candidate_ids = list(current_ids)
            seen = set(current_ids)
            for eid in prior_ids:
                if eid in seen:
                    continue
                rec = await self._storage.get_delivery(eid, subscription_id)
                if rec is None:
                    continue  # 记录已不存在：无可报告对象，不计入 total
                candidate_ids.append(eid)
                seen.add(eid)
            candidate_ids = await self._sorted_by_seq_locked(
                candidate_ids, subscription_id
            )

            results: list[dict] = []
            counts = {
                "replayed": 0,
                "already_replaying": 0,
                "already_delivered": 0,
                "not_dead_letter": 0,
            }
            for event_id in candidate_ids:
                outcome, _ = await self._replay_claim_locked(
                    event_id, subscription_id, now
                )
                if outcome is ReplayOutcome.NOT_FOUND:
                    # 极端竞态下记录消失：无对应实体，不产生逐条结论
                    continue
                results.append({"event_id": event_id, "outcome": outcome.value})
                key = outcome.value.lower()
                if key in counts:
                    counts[key] += 1

            # 登记本代候选记忆（cooldown=0 表示不记忆）。以全部有结论候选
            # 为准，使随后重复批次对「已成功 / 仍在投递 / 暂停积压」的事件
            # 都能逐条回带，与失败链路查到的状态一致。
            if cooldown_seconds != 0 and candidate_ids:
                remembered = [
                    r["event_id"] for r in results
                ]
                self._batch_last[subscription_id] = (remembered, now)
            elif cooldown_seconds == 0:
                self._batch_last.pop(subscription_id, None)

        logger.info(
            "replay_batch subscription_id=%s total=%s replayed=%s "
            "already_replaying=%s already_delivered=%s not_dead_letter=%s",
            subscription_id, len(results), counts["replayed"],
            counts["already_replaying"], counts["already_delivered"],
            counts["not_dead_letter"],
        )
        return {
            "tenant_id": tenant_id,
            "subscription_id": subscription_id,
            "found": True,
            "total": len(results),
            **counts,
            "results": results,
        }

    async def _sorted_by_seq_locked(
        self, event_ids: list[str], subscription_id: str
    ) -> list[str]:
        """按投递记录 seq 升序排列候选；无记录的事件排到最后（稳定）。"""
        keyed: list[tuple[int, int, str]] = []
        for fallback, eid in enumerate(event_ids):
            rec = await self._storage.get_delivery(eid, subscription_id)
            seq = rec.seq if rec is not None else 10**12
            keyed.append((seq, fallback, eid))
        keyed.sort()
        return [eid for _, _, eid in keyed]

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
            if await self._attempt_head(record, subscription, now):
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
    ) -> bool:
        """推进一次队首投递。返回 True 表示本次真正尝试；并发落败/已终结
        等跳过情形返回 False（不产生任何实际发送）。"""
        lock = await self._storage.subscription_lock(subscription.subscription_id)
        async with lock:
            async with self._cond:
                queue = self._queues.get(subscription.subscription_id)
                head_marker = queue[0] if queue else None
            # 并发推进防护（后台 worker 与手工 pump/另一路径同时到达时）：
            # 1) 必须仍是队首（前序事件未终结前，后来者不得越过）；
            # 2) 记录不得已终结——已成功/已死信的事件绝不允许再次发出；
            # 3) 重新从存储读取当前状态，拒绝处理过期快照（另一路径可能
            #    已经把它推进到 DELIVERING/RETRYING）。
            marker = (record.event_id, record.subscription_id)
            if head_marker != marker:
                logger.info(
                    "delivery_skip event_id=%s subscription_id=%s "
                    "basis=NOT_HEAD head=%s",
                    record.event_id, subscription.subscription_id, head_marker,
                )
                return False
            fresh = await self._storage.get_delivery(
                record.event_id, record.subscription_id
            )
            if fresh is None:
                async with self._cond:
                    if queue and queue[0] == marker:
                        queue.popleft()
                    self._cond.notify_all()
                return False
            if fresh.status in (EventStatus.DELIVERED, EventStatus.DEAD_LETTER):
                # 已终结事件不能被再次拿出来发送：清理残留队头即可。
                logger.info(
                    "delivery_skip event_id=%s subscription_id=%s "
                    "basis=TERMINAL status=%s",
                    record.event_id, subscription.subscription_id, fresh.status.value,
                )
                async with self._cond:
                    if queue and queue[0] == marker:
                        queue.popleft()
                    self._cond.notify_all()
                return False
            if fresh.status is EventStatus.DELIVERING:
                # 另一路径正在发送同一事件：本次推进直接退出，绝不重复 POST。
                logger.info(
                    "delivery_skip event_id=%s subscription_id=%s "
                    "basis=IN_FLIGHT",
                    record.event_id, subscription.subscription_id,
                )
                return False
            # 以存储中的最新记录为准继续（与 fresh 同一对象）
            record = fresh
            await self._deliver_one(record, subscription, now)
            # 终结则出队，否则保留队首等待退避
            terminal = record.status in (EventStatus.DELIVERED, EventStatus.DEAD_LETTER)
            if terminal:
                async with self._cond:
                    queue = self._queues.get(subscription.subscription_id)
                    if queue and queue[0] == (record.event_id, record.subscription_id):
                        queue.popleft()
                    self._cond.notify_all()
            return True

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
