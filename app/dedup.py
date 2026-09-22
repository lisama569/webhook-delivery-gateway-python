"""幂等判定辅助。

重复请求（无论串行重试还是并发同时到达）都必须给出稳定且可区分的结论：
首次处理 -> ACCEPTED（HTTP 201）；重复 -> DUPLICATE（HTTP 200）。
并发竞争中只有唯一约束的获胜方得到 ACCEPTED，其余全部得到一致的 DUPLICATE。
"""
from __future__ import annotations

from .models import IngestStatus


def classify(already_exists: bool) -> IngestStatus:
    return IngestStatus.DUPLICATE if already_exists else IngestStatus.ACCEPTED
