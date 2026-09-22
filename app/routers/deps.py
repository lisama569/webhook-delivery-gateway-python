"""FastAPI 依赖：鉴权、租户隔离与每租户独立额度。"""
from __future__ import annotations

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from fastapi import Header, HTTPException, Request

from .. import state
from ..security import constant_time_equals


@dataclass
class _Window:
    hits: deque[float] = field(default_factory=deque)


class RateLimiter:
    """固定 60s 滑动窗口；按 tenant_id 独立计数，互不影响。"""

    def __init__(self) -> None:
        self._windows: dict[str, _Window] = defaultdict(_Window)
        self._lock = threading.Lock()

    def check(self, tenant_id: str, limit_per_minute: int) -> tuple[bool, int]:
        now = time.monotonic()
        cutoff = now - 60.0
        with self._lock:
            win = self._windows[tenant_id]
            while win.hits and win.hits[0] < cutoff:
                win.hits.popleft()
            if len(win.hits) >= limit_per_minute:
                return False, limit_per_minute
            win.hits.append(now)
            return True, limit_per_minute

    def reset(self) -> None:
        with self._lock:
            self._windows.clear()


rate_limiter = RateLimiter()


def require_tenant(request: Request,
                   x_api_key: str = Header(default="", alias="X-API-Key")):
    """租户 API Key 鉴权。

    * 缺失/错误 -> 401，错误码可区分（MISSING_API_KEY / INVALID_API_KEY）；
    * 校验与额度均以 tenant_id 为界，A 方失败/超限不影响 B 方；
    * 认证成功后注入 request.state.tenant_id，所有后续查询强制带租户。
    """
    if not x_api_key:
        raise HTTPException(status_code=401, detail={
            "error": "MISSING_API_KEY",
            "message": "X-API-Key header is required",
        })
    now = datetime.now(timezone.utc)
    auth = state.repo.authenticate_api_key(x_api_key, now)
    if auth is None:
        raise HTTPException(status_code=401, detail={
            "error": "INVALID_API_KEY",
            "message": "api key not recognized or tenant inactive",
        })
    tenant_id, rate_limit = auth
    allowed, limit = rate_limiter.check(tenant_id, rate_limit)
    if not allowed:
        raise HTTPException(status_code=429, detail={
            "error": "RATE_LIMITED",
            "tenant_id": tenant_id,
            "limit_per_minute": limit,
        })
    request.state.tenant_id = tenant_id
    return {"tenant_id": tenant_id, "rate_limit_per_minute": rate_limit}


def require_admin(x_admin_key: str = Header(default="", alias="X-Admin-Key")) -> None:
    expected = state.settings.admin_api_key if state.settings else ""
    if not x_admin_key or not constant_time_equals(x_admin_key, expected):
        raise HTTPException(status_code=401, detail={"error": "INVALID_ADMIN_KEY"})
