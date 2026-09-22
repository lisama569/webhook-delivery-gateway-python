"""测试辅助：构造带签名的入站请求。"""
from __future__ import annotations

import json

from app.security import sign


def build_event(event_id: str, event_type: str = "order.created",
                version: str = "1.0.0", payload: dict | None = None) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "version": version,
        "payload": payload if payload is not None else {"order_id": "ord-1"},
    }


def signed_headers(secret: str, body: dict, key_id: str, ts: int,
                   tenant_id: str = "tenant-a") -> dict:
    raw = json.dumps(body, separators=(",", ":")).encode()
    return {
        "X-Tenant-Id": tenant_id,
        "X-Key-Id": key_id,
        "X-Timestamp": str(ts),
        "X-Signature": sign(secret, str(ts), raw),
    }


def raw_body(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode()
