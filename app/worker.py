"""投递工作循环。

设计要点：
* tick() 同步处理一批到期投递，可在后台线程运行，也可在测试中手动驱动；
* claim_pending 已保证"同订阅严格按 seq、队头阻塞"，因此同订阅绝不乱序；
* 每次尝试的原因与判定依据都写 delivery_attempts 并打 INFO 日志
  （含 tenant/event/seq/attempt）；
* 超过 max_attempts 进入 dead_letters，可通过管理面查询，绝不静默丢失。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone

from .config import Settings
from .db import get_conn
from .delivery import attempt_once, compute_backoff
from .models import FailReason
from .repository import Repository
from .security import sign
from .transport import Transport

log = logging.getLogger("gateway.worker")

# 每个租户可选地强制使用某把签名 key（模拟轮换期投递方行为的测试开关）
# tenant_id -> key_id
signing_key_override: dict[str, str] = {}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DeliveryWorker:
    def __init__(self, repo: Repository, transport: Transport,
                 settings: Settings, clock: Callable[[], datetime] | None = None) -> None:
        self.repo = repo
        self.transport = transport
        self.settings = settings
        self._clock = clock or utcnow
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ---------- 主循环 ----------

    def tick(self) -> int:
        now = self._clock()
        rows = self.repo.claim_pending(now, self.settings.worker_batch_size)
        for row in rows:
            self._deliver_one(row, now)
        return len(rows)

    def _load_event_payload(self, event_pk: int) -> dict:
        with get_conn(self.settings) as conn:
            r = conn.execute(
                "SELECT payload FROM events WHERE id=?", (event_pk,)
            ).fetchone()
        return json.loads(r["payload"]) if r else {}

    def _choose_credential(self, tenant_id: str, now: datetime):
        """选择签名凭据：默认用当前最新有效凭据；测试可强制指定旧/新 key。"""
        active = self.repo.get_active_credentials(tenant_id, now)
        if not active:
            return None
        override = signing_key_override.get(tenant_id)
        if override is not None:
            for key_id, secret in active:
                if key_id == override:
                    return key_id, secret
            return None  # 指定了已过期/不存在的 key
        return active[0]  # 新凭据优先；轮换期间对方仍接受旧凭据时由测试显式指定

    def _build_body(self, row: dict, payload: dict) -> bytes:
        envelope = {
            "event_id": row["event_id"],
            "tenant_id": row["tenant_id"],
            "subscription_id": row["subscription_id"],
            "seq": row["seq"],
            "payload": payload,
            "unknown_fields": row.get("unknown_fields") or [],
        }
        return json.dumps(envelope, ensure_ascii=False, sort_keys=True).encode("utf-8")

    def _deliver_one(self, row: dict, now: datetime) -> None:
        tenant_id = row["tenant_id"]
        event_id = row["event_id"]
        seq = row["seq"]
        delivery_id = row["id"]

        attempt_no = row["attempts"] + 1
        payload = self._load_event_payload(row["event_pk"])
        body = self._build_body(
            {**row, "unknown_fields": _safe_json_list(row.get("unknown_fields"))},
            payload,
        )

        cred = self._choose_credential(tenant_id, now)
        headers = {
            "Content-Type": "application/json",
            "X-Event-Id": event_id,
            "X-Tenant-Id": tenant_id,
            "X-Delivery-Seq": str(seq),
            "X-Attempt": str(attempt_no),
        }
        if cred is not None:
            key_id, secret = cred
            ts = int(now.timestamp())
            signature = sign(secret, body, key_id, ts)
            headers["X-Signature"] = signature
            headers["X-Signature-Key-Id"] = key_id

        target_url = self._target_url(row["subscription_id"])
        log.info(
            "delivery attempt | tenant=%s event=%s sub=%s seq=%s delivery=%s "
            "attempt=%s/%s key=%s reason=due",
            tenant_id, event_id, row["subscription_id"], seq, delivery_id,
            attempt_no, self.settings.max_attempts,
            cred[0] if cred else "<no-active-credential>",
        )

        if cred is None:
            ok, http_status, reason, detail, kid = (
                False, None, FailReason.TRANSPORT_ERROR,
                "no active signing credential for tenant", None,
            )
        else:
            self.repo.mark_in_flight(delivery_id)
            ok, http_status, reason, detail, kid = attempt_once(
                self.transport, target_url, body, headers
            )

        if ok:
            self.repo.record_attempt_success(
                delivery_id, attempt_no, http_status or 200, kid, self._clock()
            )
            log.info(
                "delivery result  | tenant=%s event=%s sub=%s seq=%s "
                "delivery=%s attempt=%s verdict=SUCCEEDED basis=receiver_2xx key=%s",
                tenant_id, event_id, row["subscription_id"], seq, delivery_id,
                attempt_no, kid,
            )
            return

        exhausted = attempt_no >= self.settings.max_attempts
        if exhausted:
            self.repo.record_attempt_dead(
                delivery_id, attempt_no, http_status, reason, detail,
                cred[0] if cred else None, self._clock(),
            )
            log.error(
                "delivery result  | tenant=%s event=%s sub=%s seq=%s "
                "delivery=%s attempt=%s verdict=DEAD_LETTER "
                "basis=attempts_exhausted last_reason=%s detail=%s",
                tenant_id, event_id, row["subscription_id"], seq, delivery_id,
                attempt_no, reason.value, detail,
            )
        else:
            not_before = self._clock() + compute_backoff(attempt_no, self.settings)
            self.repo.record_attempt_retry(
                delivery_id, attempt_no, http_status, reason, detail,
                cred[0] if cred else None, not_before, self._clock(),
            )
            log.warning(
                "delivery result  | tenant=%s event=%s sub=%s seq=%s "
                "delivery=%s attempt=%s verdict=RETRY reason=%s "
                "next_not_before=%s detail=%s",
                tenant_id, event_id, row["subscription_id"], seq, delivery_id,
                attempt_no, reason.value, not_before.isoformat(), detail,
            )

    def _target_url(self, subscription_id: int) -> str:
        with get_conn(self.settings) as conn:
            r = conn.execute(
                "SELECT target_url FROM subscriptions WHERE id=?",
                (subscription_id,),
            ).fetchone()
        return r["target_url"] if r else ""

    # ---------- 后台线程 ----------

    def start_background(self) -> threading.Thread:
        if self._thread is not None:
            return self._thread

        def _run() -> None:
            while not self._stop.is_set():
                try:
                    self.tick()
                except Exception:
                    log.exception("worker tick failed; will retry next interval")
                self._stop.wait(self.settings.worker_interval_seconds)

        self._thread = threading.Thread(
            target=_run, name="delivery-worker", daemon=True
        )
        self._thread.start()
        return self._thread

    def stop_background(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None


def _safe_json_list(value) -> list:
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value:
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []
