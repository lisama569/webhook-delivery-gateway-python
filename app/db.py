"""SQLite 持久化与连接管理（WAL + 唯一约束保证并发幂等）。"""
from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from .config import Settings

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id            TEXT PRIMARY KEY,
    display_name         TEXT NOT NULL DEFAULT '',
    api_key              TEXT NOT NULL,
    rate_limit_per_minute INTEGER NOT NULL DEFAULT 600,
    created_at           TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS credentials (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       TEXT NOT NULL REFERENCES tenants(tenant_id),
    key_id          TEXT NOT NULL,
    secret          TEXT NOT NULL,
    valid_from      TEXT NOT NULL,
    valid_to        TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(tenant_id, key_id)
);
CREATE INDEX IF NOT EXISTS idx_credentials_tenant ON credentials(tenant_id);

CREATE TABLE IF NOT EXISTS subscriptions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id           TEXT NOT NULL REFERENCES tenants(tenant_id),
    event_type          TEXT NOT NULL,
    min_version         INTEGER NOT NULL DEFAULT 1,
    max_version         INTEGER,
    target_url          TEXT NOT NULL,
    newer_version_policy TEXT NOT NULL DEFAULT 'DELIVER_ANYWAY',
    known_fields        TEXT NOT NULL DEFAULT '[]',
    next_seq            INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_subs_match
    ON subscriptions(tenant_id, event_type);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id     TEXT NOT NULL REFERENCES tenants(tenant_id),
    event_id      TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    payload       TEXT NOT NULL,
    occurred_at   TEXT,
    accepted_at   TEXT NOT NULL,
    UNIQUE(tenant_id, event_id)
);

CREATE TABLE IF NOT EXISTS deliveries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_pk        INTEGER NOT NULL REFERENCES events(id),
    tenant_id       TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    subscription_id INTEGER NOT NULL REFERENCES subscriptions(id),
    seq             INTEGER NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING',
    attempts        INTEGER NOT NULL DEFAULT 0,
    fail_reason     TEXT NOT NULL DEFAULT 'NONE',
    not_before      TEXT NOT NULL,
    last_error      TEXT NOT NULL DEFAULT '',
    unknown_fields  TEXT NOT NULL DEFAULT '[]',
    UNIQUE(subscription_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_deliveries_claim
    ON deliveries(state, not_before);
CREATE INDEX IF NOT EXISTS idx_deliveries_tenant_event
    ON deliveries(tenant_id, event_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id        INTEGER NOT NULL REFERENCES deliveries(id),
    attempt            INTEGER NOT NULL,
    http_status        INTEGER,
    fail_reason        TEXT NOT NULL,
    detail             TEXT NOT NULL DEFAULT '',
    signed_with_key_id TEXT,
    at                 TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_delivery ON delivery_attempts(delivery_id);

CREATE TABLE IF NOT EXISTS dead_letters (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id       TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    subscription_id INTEGER,
    delivery_id     INTEGER,
    fail_reason     TEXT NOT NULL,
    detail          TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dead_tenant ON dead_letters(tenant_id);
"""


def init_db(settings: Settings) -> None:
    """创建数据库文件目录与全部表（幂等）。"""
    db_dir = os.path.dirname(settings.database_path)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    conn = sqlite3.connect(settings.database_path, timeout=10)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


@contextmanager
def get_conn(settings: Settings) -> Iterator[sqlite3.Connection]:
    conn = sqlite3.connect(settings.database_path, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        yield conn
    finally:
        conn.close()
