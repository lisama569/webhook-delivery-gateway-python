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
    Subscription,
    utcnow,
)
from app.security import sign
from app.storage import Storage
from app.transport import Transport

logger = get_logger(__name__)

BASE_BACKOFF_SECONDS = 0.05  # 本地验证用小基数
MAX_BACKOFF_SECONDS = 1.0


def compute_backoff(attempt: int, base: float = BASE_BACKOFF_SECONDS) -> float:
    """第 attempt 次尝试失败后的等待秒数：指数退避，封顶 MAX_BACKOFF_SECONDS。"""
    return min(MAX_BACKOFF_SECONDS, base * (2 ** (attempt - 1)))


_REASON_MAP = {
    "NETWORK_ERROR": FailReason.NETWORK_ERROR,
    "HTTP_5XX": FailReason.HTTP_5XX,
    "HTTP_4XX_RETRYABLE": FailReason.HTTP_5XX,
    "HTTP_4XX": FailReason.HTTP_4XX,
}


class Dispatcher:
    def __init__(self, storage: Storage, transport: Transport) -> None:
        self._storage = storage
        self._transport = transport
        # subscription_id -> FIFO 队列，元素为 (event_id, subscription_id)
        self._queues: dict[str, deque[tuple[str, str]]] = {}
        self._cond = asyncio.Condition()

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

        cred = await self._storage.get_credential(subscription.credential_key_id)
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
