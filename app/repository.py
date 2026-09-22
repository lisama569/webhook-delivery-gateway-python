"""仓储层：封装所有 SQL。

关键不变量：
* (tenant_id, event_id) 唯一约束 + BEGIN IMMEDIATE -> 并发/重复只受理一次；
* 所有查询按 tenant_id 过滤 -> 租户隔离；
* (subscription_id, seq) 唯一约束 -> 订阅内事件有确定顺序；
* 失败终态写入 dead_letters -> 结果不静默丢失。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any

from .config import Settings
from .db import get_conn, init_db
from .models import (
    CredentialOut,
    DeliveryState,
    FailReason,
    IngestStatus,
    NewerVersionPolicy,
    SubscriptionOut,
    TenantOut,
)
from .security import constant_time_equals, is_within_validity


class ConflictError(Exception):
    """资源冲突（如重复租户/重复事件）。"""


class NotFoundError(Exception):
    """租户隔离下查不到资源（不存在或属于其他租户）。"""


class DuplicateEventError(Exception):
    """(tenant_id, event_id) 唯一约束冲突 —— 重复/并发到达。"""


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _loads(s: str | None, default: Any) -> Any:
    if not s:
        return default
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return default


class Repository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        init_db(settings)

    # ========== 租户 ==========

    def create_tenant(self, tenant_id: str, display_name: str, api_key: str,
                      rate_limit: int, now: datetime) -> TenantOut:
        with get_conn(self.settings) as conn:
            try:
                conn.execute(
                    "INSERT INTO tenants(tenant_id, display_name, api_key, "
                    "rate_limit_per_minute, created_at) VALUES (?,?,?,?,?)",
                    (tenant_id, display_name, api_key, rate_limit, _iso(now)),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise ConflictError(f"tenant already exists: {tenant_id}") from exc
        return self.get_tenant(tenant_id)  # type: ignore[return-value]

    def get_tenant(self, tenant_id: str) -> TenantOut | None:
        with get_conn(self.settings) as conn:
            row = conn.execute(
                "SELECT * FROM tenants WHERE tenant_id=?", (tenant_id,)
            ).fetchone()
        if row is None:
            return None
        return TenantOut(
            tenant_id=row["tenant_id"],
            display_name=row["display_name"],
            rate_limit_per_minute=row["rate_limit_per_minute"],
            created_at=row["created_at"],
        )

    def authenticate_api_key(self, api_key: str, now: datetime):
        """返回 (tenant_id, rate_limit)；无效/缺失返回 None。

        全表常量时间比较，避免早退造成的跨租户时序侧信道；每个租户的
        凭据体系独立，一方校验失败不影响其他方。
        """
        if not api_key:
            return None
        with get_conn(self.settings) as conn:
            rows = conn.execute(
                "SELECT tenant_id, api_key, rate_limit_per_minute FROM tenants"
            ).fetchall()
        match = None
        for row in rows:
            if constant_time_equals(row["api_key"], api_key):
                match = (row["tenant_id"], row["rate_limit_per_minute"])
        return match

    # ========== 凭据 ==========

    def add_credential(self, tenant_id: str, key_id: str, secret: str,
                       valid_from: datetime, valid_to: datetime | None,
                       now: datetime) -> CredentialOut:
        with get_conn(self.settings) as conn:
            try:
                conn.execute(
                    "INSERT INTO credentials(tenant_id, key_id, secret, valid_from, "
                    "valid_to, created_at) VALUES (?,?,?,?,?,?)",
                    (tenant_id, key_id, secret, _iso(valid_from),
                     _iso(valid_to) if valid_to else None, _iso(now)),
                )
                conn.commit()
            except sqlite3.IntegrityError as exc:
                raise ConflictError(
                    f"credential key_id already exists for tenant: {key_id}"
                ) from exc
        return self.list_credentials(tenant_id, now) and [
            c for c in self.list_credentials(tenant_id, now) if c.key_id == key_id
        ][0]

    def list_credentials(self, tenant_id: str, now: datetime) -> list[CredentialOut]:
        with get_conn(self.settings) as conn:
            rows = conn.execute(
                "SELECT key_id, valid_from, valid_to FROM credentials "
                "WHERE tenant_id=? ORDER BY valid_from DESC, id DESC",
                (tenant_id,),
            ).fetchall()
        out: list[CredentialOut] = []
        for row in rows:
            vf = datetime.fromisoformat(row["valid_from"])
            vt = datetime.fromisoformat(row["valid_to"]) if row["valid_to"] else None
            active = is_within_validity(
                now, vf, vt, self.settings.clock_skew_seconds
            )
            expired = vt is not None and now > vt
            out.append(CredentialOut(
                key_id=row["key_id"], valid_from=vf, valid_to=vt,
                is_expired=expired, is_active=active,
            ))
        return out

    def get_active_credentials(self, tenant_id: str, now: datetime) -> list[tuple[str, str]]:
        """有效期内容忍时钟偏移的 (key_id, secret)，新凭据优先。轮换期含新旧两把。"""
        with get_conn(self.settings) as conn:
            rows = conn.execute(
                "SELECT key_id, secret, valid_from, valid_to FROM credentials "
                "WHERE tenant_id=? ORDER BY valid_from DESC, id DESC",
                (tenant_id,),
            ).fetchall()
        out = []
        for row in rows:
            vf = datetime.fromisoformat(row["valid_from"])
            vt = datetime.fromisoformat(row["valid_to"]) if row["valid_to"] else None
            if is_within_validity(now, vf, vt, self.settings.clock_skew_seconds):
                out.append((row["key_id"], row["secret"]))
        return out

    # ========== 订阅 ==========

    def create_subscription(self, tenant_id: str, event_type: str, min_version: int,
                            max_version: int | None, target_url: str,
                            policy: NewerVersionPolicy, known_fields: list[str],
                            now: datetime) -> SubscriptionOut:
        if max_version is not None and max_version < min_version:
            raise ConflictError("max_version must be >= min_version")
        with get_conn(self.settings) as conn:
            cur = conn.execute(
                "INSERT INTO subscriptions(tenant_id, event_type, min_version, "
                "max_version, target_url, newer_version_policy, known_fields, "
                "next_seq, created_at) VALUES (?,?,?,?,?,?,?,1,?)",
                (tenant_id, event_type, min_version, max_version, target_url,
                 policy.value, json.dumps(sorted(set(known_fields))), _iso(now)),
            )
            sub_id = cur.lastrowid
            conn.commit()
        return SubscriptionOut(
            subscription_id=sub_id, tenant_id=tenant_id, event_type=event_type,
            min_version=min_version, max_version=max_version, target_url=target_url,
            newer_version_policy=policy, created_at=now,
        )

    def list_subscriptions(self, tenant_id: str) -> list[SubscriptionOut]:
        with get_conn(self.settings) as conn:
            rows = conn.execute(
                "SELECT * FROM subscriptions WHERE tenant_id=? ORDER BY id",
                (tenant_id,),
            ).fetchall()
        return [self._sub_out(r) for r in rows]

    @staticmethod
    def _sub_out(row: sqlite3.Row) -> SubscriptionOut:
        return SubscriptionOut(
            subscription_id=row["id"], tenant_id=row["tenant_id"],
            event_type=row["event_type"], min_version=row["min_version"],
            max_version=row["max_version"], target_url=row["target_url"],
            newer_version_policy=NewerVersionPolicy(row["newer_version_policy"]),
            created_at=row["created_at"],
        )

    def find_matching_subscriptions(self, tenant_id: str, event_type: str) -> list[dict]:
        with get_conn(self.settings) as conn:
            rows = conn.execute(
                "SELECT * FROM subscriptions WHERE tenant_id=? AND event_type=?",
                (tenant_id, event_type),
            ).fetchall()
        return [{
            "id": r["id"],
            "min_version": r["min_version"],
            "max_version": r["max_version"],
            "target_url": r["target_url"],
            "policy": NewerVersionPolicy(r["newer_version_policy"]),
            "known_fields": _loads(r["known_fields"], []),
        } for r in rows]

    # ========== 入站事件 + 幂等 ==========

    def try_acquire_event(self, tenant_id: str, event_id: str, event_type: str,
                          event_version: int, payload: dict, occurred_at: str | None,
                          now: datetime):
        """原子受理。返回 (IngestStatus, event_row, deliveries)。

        并发竞争：唯一约束保证只有一个事务能插入成功，其余抛
        DuplicateEventError 由上层读取既有结论，结论稳定一致。
        """
        with get_conn(self.settings) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    "SELECT id FROM events WHERE tenant_id=? AND event_id=?",
                    (tenant_id, event_id),
                ).fetchone()
                if existing is not None:
                    raise DuplicateEventError(event_id)
                cur = conn.execute(
                    "INSERT INTO events(tenant_id, event_id, event_type, "
                    "event_version, payload, occurred_at, accepted_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (tenant_id, event_id, event_type, event_version,
                     json.dumps(payload, ensure_ascii=False), occurred_at, _iso(now)),
                )
                event_pk = cur.lastrowid
                subs = conn.execute(
                    "SELECT * FROM subscriptions WHERE tenant_id=? AND event_type=?",
                    (tenant_id, event_type),
                ).fetchall()

                # 延迟导入避免循环依赖
                from .contract import evaluate

                deliveries: list[dict] = []
                for sub in subs:
                    known_fields = _loads(sub["known_fields"], [])
                    decision = evaluate(
                        event_version, set(payload.keys()), known_fields,
                        sub["min_version"], sub["max_version"],
                        NewerVersionPolicy(sub["newer_version_policy"]),
                    )
                    seq = sub["next_seq"]
                    state = (DeliveryState.PENDING.value if decision.deliver
                             else DeliveryState.DEAD_LETTER.value)
                    conn.execute(
                        "UPDATE subscriptions SET next_seq=next_seq+1 WHERE id=?",
                        (sub["id"],),
                    )
                    dcur = conn.execute(
                        "INSERT INTO deliveries(event_pk, tenant_id, event_id, "
                        "subscription_id, seq, state, attempts, fail_reason, "
                        "not_before, last_error, unknown_fields) "
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                        (event_pk, tenant_id, event_id, sub["id"], seq, state, 0,
                         decision.fail_reason.value, _iso(now),
                         "" if decision.deliver else decision.annotation,
                         json.dumps(decision.unknown_fields)),
                    )
                    delivery_id = dcur.lastrowid
                    if not decision.deliver:
                        conn.execute(
                            "INSERT INTO dead_letters(tenant_id, event_id, "
                            "subscription_id, delivery_id, fail_reason, detail, "
                            "created_at) VALUES (?,?,?,?,?,?,?)",
                            (tenant_id, event_id, sub["id"], delivery_id,
                             decision.fail_reason.value, decision.annotation,
                             _iso(now)),
                        )
                    deliveries.append({
                        "subscription_id": sub["id"],
                        "state": state,
                        "fail_reason": decision.fail_reason.value,
                        "id": delivery_id,
                        "seq": seq,
                    })
                conn.commit()
            except DuplicateEventError:
                conn.rollback()
                raise
            except Exception:
                conn.rollback()
                raise

        event_row = {
            "tenant_id": tenant_id, "event_id": event_id,
            "event_type": event_type, "event_version": event_version,
            "accepted_at": _iso(now),
        }
        return IngestStatus.ACCEPTED, event_row, deliveries

    def get_event_with_deliveries(self, tenant_id: str, event_id: str):
        with get_conn(self.settings) as conn:
            ev = conn.execute(
                "SELECT * FROM events WHERE tenant_id=? AND event_id=?",
                (tenant_id, event_id),
            ).fetchone()
            if ev is None:
                return None
            dels = conn.execute(
                "SELECT id, subscription_id, state, fail_reason FROM deliveries "
                "WHERE tenant_id=? AND event_id=? ORDER BY subscription_id, seq",
                (tenant_id, event_id),
            ).fetchall()
        event_row = {
            "tenant_id": ev["tenant_id"], "event_id": ev["event_id"],
            "event_type": ev["event_type"], "event_version": ev["event_version"],
            "accepted_at": ev["accepted_at"],
        }
        deliveries = [{
            "subscription_id": r["subscription_id"],
            "state": r["state"],
            "fail_reason": r["fail_reason"],
            "id": r["id"],
            "seq": None,
        } for r in dels]
        return event_row, deliveries

    # ========== 投递 ==========

    def claim_pending(self, now: datetime, limit: int) -> list[dict]:
        """领取到期投递。

        顺序保证（同订阅严格按 seq）：若存在更早的 IN_FLIGHT 或尚未到期的
        PENDING 记录，则后面的记录本轮不领取（队头阻塞，绝不乱序）。
        已终态（SUCCEEDED/DEAD_LETTER）记录不阻塞后续。
        """
        sql = (
            "SELECT d.* FROM deliveries d "
            "WHERE d.state IN ('PENDING','IN_FLIGHT') AND d.not_before<=? "
            "AND NOT EXISTS ("
            "  SELECT 1 FROM deliveries e WHERE e.subscription_id=d.subscription_id"
            "    AND e.seq<d.seq"
            "    AND (e.state='IN_FLIGHT' OR (e.state='PENDING' AND e.not_before>?))"
            ") "
            "ORDER BY d.subscription_id, d.seq LIMIT ?"
        )
        with get_conn(self.settings) as conn:
            rows = conn.execute(sql, (_iso(now), _iso(now), limit)).fetchall()
        seen: set[int] = set()
        out: list[dict] = []
        for r in rows:  # 每个订阅本轮只取队头一条，天然串行
            if r["subscription_id"] in seen:
                break
            seen.add(r["subscription_id"])
            out.append(dict(r))
        return out

    def mark_in_flight(self, delivery_id: int) -> None:
        with get_conn(self.settings) as conn:
            conn.execute(
                "UPDATE deliveries SET state='IN_FLIGHT' WHERE id=?",
                (delivery_id,),
            )
            conn.commit()

    def _record(self, conn, delivery_id: int, attempt_no: int, http_status,
                fail_reason: FailReason, detail: str, signed_key_id, at: datetime):
        conn.execute(
            "INSERT INTO delivery_attempts(delivery_id, attempt, http_status, "
            "fail_reason, detail, signed_with_key_id, at) VALUES (?,?,?,?,?,?,?)",
            (delivery_id, attempt_no, http_status, fail_reason.value, detail,
             signed_key_id, _iso(at)),
        )

    def record_attempt_success(self, delivery_id: int, attempt_no: int,
                               http_status: int, signed_key_id: str | None,
                               at: datetime) -> None:
        with get_conn(self.settings) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE deliveries SET state='SUCCEEDED', attempts=?, "
                "fail_reason='NONE', last_error='' WHERE id=?",
                (attempt_no, delivery_id),
            )
            self._record(conn, delivery_id, attempt_no, http_status,
                         FailReason.NONE, "delivered 2xx", signed_key_id, at)
            conn.commit()

    def record_attempt_retry(self, delivery_id: int, attempt_no: int,
                             http_status: int | None, fail_reason: FailReason,
                             detail: str, signed_key_id: str | None,
                             not_before: datetime, at: datetime) -> None:
        with get_conn(self.settings) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE deliveries SET state='PENDING', attempts=?, "
                "fail_reason=?, last_error=?, not_before=? WHERE id=?",
                (attempt_no, fail_reason.value, detail[:2000],
                 _iso(not_before), delivery_id),
            )
            self._record(conn, delivery_id, attempt_no, http_status,
                         fail_reason, detail, signed_key_id, at)
            conn.commit()

    def record_attempt_dead(self, delivery_id: int, attempt_no: int,
                            http_status: int | None, fail_reason: FailReason,
                            detail: str, signed_key_id: str | None,
                            at: datetime) -> None:
        """超过最大尝试次数：置 DEAD_LETTER 并写入死信链路。"""
        with get_conn(self.settings) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT tenant_id, event_id, subscription_id FROM deliveries "
                "WHERE id=?", (delivery_id,),
            ).fetchone()
            conn.execute(
                "UPDATE deliveries SET state='DEAD_LETTER', attempts=?, "
                "fail_reason=?, last_error=? WHERE id=?",
                (attempt_no, fail_reason.value, detail[:2000], delivery_id),
            )
            self._record(conn, delivery_id, attempt_no, http_status,
                         fail_reason, detail, signed_key_id, at)
            conn.execute(
                "INSERT INTO dead_letters(tenant_id, event_id, subscription_id, "
                "delivery_id, fail_reason, detail, created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (row["tenant_id"], row["event_id"], row["subscription_id"],
                 delivery_id, FailReason.ATTEMPTS_EXHAUSTED.value,
                 f"{fail_reason.value}: {detail}", _iso(at)),
            )
            conn.commit()

    # ========== 查询 ==========

    def _delivery_out(self, conn, row: sqlite3.Row) -> dict:
        attempts = conn.execute(
            "SELECT attempt, http_status, fail_reason, detail, "
            "signed_with_key_id, at FROM delivery_attempts "
            "WHERE delivery_id=? ORDER BY attempt", (row["id"],),
        ).fetchall()
        d = dict(row)
        d["unknown_fields"] = _loads(row["unknown_fields"], [])
        d["history"] = [dict(a) for a in attempts]
        return d

    def get_delivery(self, tenant_id: str, delivery_id: int) -> dict | None:
        with get_conn(self.settings) as conn:
            row = conn.execute(
                "SELECT * FROM deliveries WHERE id=? AND tenant_id=?",
                (delivery_id, tenant_id),
            ).fetchone()
            if row is None:
                return None
            return self._delivery_out(conn, row)

    def list_deliveries_for_event(self, tenant_id: str, event_id: str) -> list[dict]:
        with get_conn(self.settings) as conn:
            rows = conn.execute(
                "SELECT * FROM deliveries WHERE tenant_id=? AND event_id=? "
                "ORDER BY subscription_id, seq",
                (tenant_id, event_id),
            ).fetchall()
            return [self._delivery_out(conn, r) for r in rows]

    def list_dead_letters(self, tenant_id: str, limit: int = 100) -> list[dict]:
        with get_conn(self.settings) as conn:
            rows = conn.execute(
                "SELECT id, tenant_id, event_id, subscription_id, fail_reason, "
                "detail, created_at FROM dead_letters WHERE tenant_id=? "
                "ORDER BY id DESC LIMIT ?", (tenant_id, limit),
            ).fetchall()
        return [dict(r) for r in rows]
