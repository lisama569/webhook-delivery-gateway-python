"""出站投递：自走式后台循环、确定性退避重试、订阅内顺序保证、暂停/恢复、
死信归档与重放。

自走运转：
- 服务启动后由 start_worker() 拉起一个后台循环，按固定 tick 反复调用
  run_pending()：到期的队首事件自动投递、失败自动按退避重试、耗尽自动落入
  可查询的死信链路，全程无需手工推动；
- run_pending() 同时可被测试/管理接口直接调用（确定性驱动），订阅级互斥锁 +
  队首/状态复核保证并发调用不会重复发送同一事件。

顺序模型：
- 每个订阅一条 FIFO 队列，事件入队即分配单调递增 seq；
- 队首事件未终结（成功/死信）之前，后续事件不得投递 —— 前序事件退避
  等待期间会"阻塞队头"，从而严格保持同一订阅内事件有序；
- 不同订阅之间完全并发，互不阻塞；暂停一个订阅也不影响其他订阅。

暂停/恢复（粒度=订阅）：
- 暂停期间新事件照常受理并入队（顺序位置不变），但不会真正发出；
- 恢复后从队首起按原 seq 顺序补发；暂停/恢复可重复调用，结论稳定。

死信重放：
- 仅 DEAD_LETTER 事件可重放；重放追加到该订阅队列尾部，绝不会越过队列中
  尚未终结的事件；批量重放严格按 seq 升序逐条处理；
- 同一条事件并发/连续多次重放：一次生效（REPLAYED），其余得到稳定可区分的
  ALREADY_IN_FLIGHT；已成功的事件不能再发（NOT_DEAD_LETTER）。

退避策略（可解释、可复现，无随机抖动）：
- 第 n 次尝试后的等待 = min(cap, base * 2**(n-1))；
- 成功或永久性失败（如 4xx）不重试；可重试故障（网络/408/429/5xx）重试；
- 达到 max_attempts 仍失败 → DEAD_LETTER，fail_reason 区分最后一次的
  底层原因（NETWORK_ERROR / HTTP_5XX / HTTP_4XX / ATTEMPTS_EXHAUSTED）。
- 出站凭据不在可用窗口（缺失/未生效/已过期）时请求根本不发出，直接死信，
  fail_reason=CREDENTIAL_UNAVAILABLE，与接收方返回的任何失败严格区分。
"""
from __future__ import annotations

import asyncio
import json
from collections import deque
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum

from app.logging_utils import get_logger
from app.models import (
    DeliveryAttempt,
    DeliveryKind,
    DeliveryRecord,
    EventStatus,
    FailReason,
    Subscription,
    utcnow,
)
from app.security import credential_window_basis, sign
from app.storage import Storage
from app.transport import Transport

logger = get_logger(__name__)

BASE_BACKOFF_SECONDS = 0.05  # 本地验证用小基数
MAX_BACKOFF_SECONDS = 1.0
WORKER_TICK_SECONDS = 0.02  # 自走循环节拍


def compute_backoff(attempt: int, base: float = BASE_BACKOFF_SECONDS) -> float:
    """第 attempt 次尝试失败后的等待秒数：指数退避，封顶 MAX_BACKOFF_SECONDS。"""
    return min(MAX_BACKOFF_SECONDS, base * (2 ** (attempt - 1)))


_REASON_MAP = {
    "NETWORK_ERROR": FailReason.NETWORK_ERROR,
    "HTTP_5XX": FailReason.HTTP_5XX,
    "HTTP_4XX_RETRYABLE": FailReason.HTTP_5XX,
    "HTTP_4XX": FailReason.HTTP_4XX,
}


class ReplayOutcome(str, Enum):
    """重放请求的稳定、可区分结论。"""
    REPLAYED = "REPLAYED"                # 本次请求生效：已重新入队
    ALREADY_IN_FLIGHT = "ALREADY_IN_FLIGHT"  # 该事件已有生效重放在途/处理中
    NOT_DEAD_LETTER = "NOT_DEAD_LETTER"  # 事件不在失败链路（含已成功投递）
    NOT_FOUND = "NOT_FOUND"              # 订阅或投递记录不存在


@dataclass
class ReplayResult:
    event_id: str
    subscription_id: str
    outcome: ReplayOutcome
    generation: int
    detail: str = ""


class Dispatcher:
    def __init__(self, storage: Storage, transport: Transport) -> None:
        self._storage = storage
        self._transport = transport
        # subscription_id -> FIFO 队列，元素为 (event_id, subscription_id)
        self._queues: dict[str, deque[tuple[str, str]]] = {}
        self._cond = asyncio.Condition()
        # 自走后台循环
        self._worker_task: asyncio.Task | None = None
        self._worker_stop = asyncio.Event()
        # run_pending 的重入保护：后台循环与手工 pump 并发时，同一队首只允许
        # 一个执行者真正尝试（订阅锁之外再加一层全局在途标记，按队首键去重）。
        self._in_flight: set[tuple[str, str]] = set()

    # 自走后台循环 --------------------------------------------------------
    def start_worker(self) -> bool:
        """拉起后台投递循环。重复调用安全；返回本次是否真正启动。"""
        if self._worker_task is not None and not self._worker_task.done():
            return False
        self._worker_stop.clear()
        self._worker_task = asyncio.create_task(self._worker_loop())
        logger.info("dispatcher_worker_started basis=AUTO_DELIVERY tick=%ss",
                    WORKER_TICK_SECONDS)
        return True

    async def stop_worker(self) -> None:
        """停止后台循环（测试/服务关闭时调用），幂等。"""
        task = self._worker_task
        if task is None:
            return
        self._worker_stop.set()
        async with self._cond:
            self._cond.notify_all()
        try:
            await task
        except asyncio.CancelledError:  # pragma: no cover - 关闭兜底
            pass
        self._worker_task = None
        logger.info("dispatcher_worker_stopped basis=AUTO_DELIVERY")

    async def _worker_loop(self) -> None:
        """正常运行时投递/重试自己往前走的驱动器。"""
        try:
            while not self._worker_stop.is_set():
                try:
                    await self.run_pending()
                except Exception:  # 单轮异常绝不杀死循环
                    logger.exception("dispatcher_worker_tick_failed")
                # 可被暂停恢复/入队/停止立即唤醒，否则按 tick 周期醒来。
                async with self._cond:
                    try:
                        await asyncio.wait_for(
                            self._cond.wait(), timeout=WORKER_TICK_SECONDS
                        )
                    except asyncio.TimeoutError:
                        pass
        except asyncio.CancelledError:  # pragma: no cover - 关闭兜底
            raise

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

    # 确定性驱动（测试与本地演示） ----------------------------------------
    async def run_pending(self, now=None) -> int:
        """推进所有到期任务：仅投递各订阅队首、且已到 next_attempt_at 的任务。

        每次调用让每个就绪队首尝试一次（不内部循环退避），因此重试节奏
        完全由时间流逝/调用方推进，结果确定可复现。返回处理数。

        暂停订阅直接跳过（事件保留在队首，恢复后补发）；同一队首在后台循环
        与手工 pump 并发时只尝试一次（self._in_flight 去重 + 订阅锁内复核）。
        """
        now = now or utcnow()
        processed = 0
        # 快照订阅，避免迭代中变更
        for subscription_id in list(self._queues.keys()):
            async with self._cond:
                queue = self._queues.get(subscription_id)
                if not queue:
                    continue
                head = queue[0]
            if head in self._in_flight:
                continue  # 另一执行者正在投递该队首
            record = await self._storage.get_delivery(head[0], head[1])
            if record is None:
                async with self._cond:
                    if queue and queue[0] == head:
                        queue.popleft()
                continue
            if record.next_attempt_at is not None and now < record.next_attempt_at:
                continue  # 队头仍在退避：严格顺序，后续事件不得越过
            subscription = await self._storage.get_subscription(subscription_id)
            if subscription is None:
                continue
            if subscription.paused:
                continue  # 订阅被运维暂停：保留队序，不发出任何请求
            self._in_flight.add(head)
            try:
                handled = await self._attempt_head(record, subscription, now)
            finally:
                self._in_flight.discard(head)
            processed += handled
        return processed

    async def run_until_drained(
        self, max_cycles: int = 100, sleep: float = BASE_BACKOFF_SECONDS / 2
    ) -> None:
        """循环推进直到没有可处理任务（退避中也会等待）。仅用于本地演示。"""
        for _ in range(max_cycles):
            if await self.run_pending() == 0 and not self._has_active():
                return
            await asyncio.sleep(sleep)

    def _has_active(self) -> bool:
        for queue in self._queues.values():
            if queue:
                return True
        return False

    # 单次尝试 ------------------------------------------------------------
    async def _attempt_head(
        self, record: DeliveryRecord, subscription: Subscription, now=None
    ) -> int:
        """在订阅锁内做最终复核后投递一次队首。返回 1=真正尝试，0=跳过。"""
        lock = await self._storage.subscription_lock(subscription.subscription_id)
        async with lock:
            # 锁内复核：并发/重放等路径可能已经改变状态或队首，严格防重复发送。
            if subscription.paused:
                return 0
            fresh = await self._storage.get_delivery(
                record.event_id, record.subscription_id
            )
            if fresh is None or fresh.status in (
                EventStatus.DELIVERED,
                EventStatus.DEAD_LETTER,
            ):
                return 0
            now = now or utcnow()
            if fresh.next_attempt_at is not None and now < fresh.next_attempt_at:
                return 0
            async with self._cond:
                queue = self._queues.get(subscription.subscription_id)
                is_head = bool(queue) and queue[0] == (
                    record.event_id, record.subscription_id
                )
            if not is_head:
                return 0  # 已不在队首（例如已被重放移动），交由后续轮次处理
            await self._deliver_one(fresh, subscription, now)
            terminal = fresh.status in (EventStatus.DELIVERED, EventStatus.DEAD_LETTER)
            if terminal:
                async with self._cond:
                    queue = self._queues.get(subscription.subscription_id)
                    if queue and queue[0] == (
                        fresh.event_id, fresh.subscription_id
                    ):
                        queue.popleft()
                    self._cond.notify_all()
            return 1

    async def _deliver_one(
        self, record: DeliveryRecord, subscription: Subscription, now=None
    ) -> None:
        event = await self._storage.get_event(subscription.tenant_id, record.event_id)
        started = now or utcnow()

        # 出站凭据窗口预检：与入站验签同一套 not_before/not_after/轮换规则。
        # 凭据缺失/未生效/已过期时请求根本不发出（attempts 不增加网络尝试），
        # 直接进入死信链路，fail_reason 与"接收方返回失败"严格区分。
        cred = await self._storage.get_credential(subscription.credential_key_id)
        cred_basis = credential_window_basis(cred, now=started)
        if cred_basis != "OK":
            record.status = EventStatus.DEAD_LETTER
            record.fail_reason = FailReason.CREDENTIAL_UNAVAILABLE
            record.fail_detail = (
                f"outbound credential not usable; basis={cred_basis} "
                f"key_id={subscription.credential_key_id}; request not sent"
            )
            record.next_attempt_at = None
            record.replay_in_flight = False
            logger.info(
                "delivery_dead_letter event_id=%s subscription_id=%s "
                "reason=CREDENTIAL_UNAVAILABLE basis=%s key_id=%s generation=%s",
                record.event_id, subscription.subscription_id, cred_basis,
                subscription.credential_key_id, record.replay_generation,
            )
            await self._storage.save_delivery(record)
            return

        attempt_no = len(record.attempts) + 1
        record.status = EventStatus.DELIVERING

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

        timestamp = str(int(started.timestamp()))
        headers = {
            "Content-Type": "application/json",
            "X-Key-Id": cred.key_id,
            "X-Timestamp": timestamp,
            "X-Signature": sign(cred.secret, timestamp, body),
            "X-Delivery-Id": f"{record.event_id}:{attempt_no}",
        }

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
            record.replay_in_flight = False  # 成功后该代次结束（成功事件不可再重放）
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
            # 当代次重放终结（死信），允许后续再次重放；并发/连续重复请求在
            # 此之前一律拿到 ALREADY_IN_FLIGHT。
            record.replay_in_flight = False
            logger.info(
                "delivery_dead_letter event_id=%s subscription_id=%s attempts=%s "
                "reason=%s basis=%s detail=%s generation=%s",
                record.event_id, subscription.subscription_id, attempt_no,
                record.fail_reason.value, result.reason_code, result.detail,
                record.replay_generation,
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

    # 暂停 / 恢复（粒度：订阅） -------------------------------------------
    async def pause_subscription(
        self, subscription_id: str, now=None
    ) -> Subscription | None:
        """暂停订阅：队序保留、事件继续入队，但不再发出任何投递。可重复调用。"""
        now = now or utcnow()
        sub = await self._storage.get_subscription(subscription_id)
        if sub is None:
            return None
        if not sub.paused:  # 重复暂停：保留首次 paused_at，结论保持稳定
            sub.paused = True
            sub.paused_at = now
            await self._storage.upsert_subscription(sub)
            logger.info(
                "subscription_paused subscription_id=%s basis=OPS_PAUSE at=%s",
                subscription_id, now.isoformat(),
            )
        return sub

    async def resume_subscription(
        self, subscription_id: str, now=None
    ) -> Subscription | None:
        """恢复订阅：从队首起按原 seq 顺序补发。可重复调用，并立即唤醒循环。"""
        sub = await self._storage.get_subscription(subscription_id)
        if sub is None:
            return None
        if sub.paused:
            sub.paused = False
            sub.paused_at = None
            await self._storage.upsert_subscription(sub)
            logger.info(
                "subscription_resumed subscription_id=%s basis=OPS_RESUME",
                subscription_id,
            )
        # 无论是否本次实际翻转，都唤醒后台循环，使恢复后立刻继续投递。
        async with self._cond:
            self._cond.notify_all()
        return sub

    # 死信重放 ------------------------------------------------------------
    def _requeue(self, record: DeliveryRecord) -> None:
        """把一条（死信）记录追加到所属订阅 FIFO 尾部。"""
        queue = self._queues.setdefault(record.subscription_id, deque())
        queue.append((record.event_id, record.subscription_id))

    async def replay_one(
        self, subscription_id: str, event_id: str, now=None
    ) -> ReplayResult:
        """重放单条失败事件。

        顺序：重放只追加到队列尾部，绝不插到前面尚未终结的事件之前；
        幂等：非死信（含已成功）返回 NOT_DEAD_LETTER；已有生效重放在途返回
        ALREADY_IN_FLIGHT；订阅/记录不存在返回 NOT_FOUND；本次生效返回 REPLAYED。
        """
        now = now or utcnow()
        sub = await self._storage.get_subscription(subscription_id)
        if sub is None:
            return ReplayResult(event_id, subscription_id,
                                ReplayOutcome.NOT_FOUND, 0, "subscription not found")
        lock = await self._storage.subscription_lock(subscription_id)
        async with lock:
            record = await self._storage.get_delivery(event_id, subscription_id)
            if record is None:
                return ReplayResult(
                    event_id, subscription_id, ReplayOutcome.NOT_FOUND, 0,
                    "delivery record not found",
                )
            gen = record.replay_generation
            # 先查重放占用：上一生效重放尚未终结（SCHEDULED/RETRYING 等）时，
            # 并发落败者与连续重复请求一律得到稳定的 ALREADY_IN_FLIGHT。
            if record.replay_in_flight:
                logger.info(
                    "replay_dedup event_id=%s subscription_id=%s "
                    "basis=ALREADY_IN_FLIGHT status=%s generation=%s",
                    event_id, subscription_id, record.status.value, gen + 1,
                )
                return ReplayResult(
                    event_id, subscription_id, ReplayOutcome.ALREADY_IN_FLIGHT,
                    gen + 1, "an accepted replay for this event is already in flight",
                )
            if record.status is not EventStatus.DEAD_LETTER:
                logger.info(
                    "replay_skip event_id=%s subscription_id=%s "
                    "basis=NOT_DEAD_LETTER status=%s generation=%s",
                    event_id, subscription_id, record.status.value, gen,
                )
                return ReplayResult(
                    event_id, subscription_id, ReplayOutcome.NOT_DEAD_LETTER, gen,
                    f"status={record.status.value}; only DEAD_LETTER can replay",
                )

            # 本次请求生效：归档当代次尝试，开启新一代并追加到队列尾部。
            generation = record.replay_generation + 1
            record.previous_attempts.extend(record.attempts)
            record.attempts = []
            record.replay_generation = generation
            record.replay_in_flight = True
            record.status = EventStatus.SCHEDULED
            record.fail_reason = FailReason.NONE
            record.fail_detail = ""
            record.next_attempt_at = None
            # 先落库再入队，保证 worker 唤醒时状态一致。
            await self._storage.save_delivery(record)
            async with self._cond:
                self._requeue(record)
                self._cond.notify_all()
            logger.info(
                "replay_accepted event_id=%s subscription_id=%s seq=%s "
                "basis=REPLAYED generation=%s paused=%s",
                event_id, subscription_id, record.seq, generation, sub.paused,
            )
            return ReplayResult(
                event_id, subscription_id, ReplayOutcome.REPLAYED, generation,
                f"replay accepted; queued at tail of subscription (seq={record.seq})",
            )

    async def replay_subscription_dead_letters(
        self, subscription_id: str, now=None
    ) -> tuple[ReplayOutcome, list[ReplayResult]]:
        """批量重放某订阅下所有尚未处理好的失败事件。

        严格按 seq 升序逐条串行处理（每把每条都走 replay_one 的订阅锁判定），
        因此大批量积压也不会乱序、不会并发冲垮服务；同一批内被前序请求占用的
        条目得到 ALREADY_IN_FLIGHT，已恢复/成功的条目得到 NOT_DEAD_LETTER。
        订阅不存在时返回 (NOT_FOUND, [])。
        """
        if await self._storage.get_subscription(subscription_id) is None:
            return ReplayOutcome.NOT_FOUND, []
        # 候选 = 仍在死信链路 或 已被一次生效重放占用但尚未终结（在途）。
        # 这样同一批重复触发时，在途条目得到 ALREADY_IN_FLIGHT 而不是被漏掉；
        # 已成功/已正常推进的事件 status=DELIVERED 且无占用标记，自然排除。
        candidates = [
            r for r in self._storage.list_deliveries(subscription_id=subscription_id)
            if r.status is EventStatus.DEAD_LETTER or r.replay_in_flight
        ]  # list_deliveries 已按 (subscription, seq) 升序
        results: list[ReplayResult] = []
        for record in candidates:
            results.append(
                await self.replay_one(subscription_id, record.event_id, now=now)
            )
        logger.info(
            "replay_batch subscription_id=%s total=%s replayed=%s in_flight=%s "
            "skipped=%s basis=BATCH_REPLAY",
            subscription_id, len(results),
            sum(r.outcome is ReplayOutcome.REPLAYED for r in results),
            sum(r.outcome is ReplayOutcome.ALREADY_IN_FLIGHT for r in results),
            sum(r.outcome is ReplayOutcome.NOT_DEAD_LETTER for r in results),
        )
        return ReplayOutcome.REPLAYED, results
