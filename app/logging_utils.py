"""统一日志：关键判定打印事件标识与判定依据（reason/detail 等结构化字段）。"""
from __future__ import annotations

import logging

_RESERVED = set(
    logging.LogRecord(
        name="", level=0, pathname="", lineno=0, msg="", args=None, exc_info=None
    ).__dict__
) | {"message", "asctime"}


class _ExtraFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = " ".join(
            f"{k}={record.__dict__[k]}"
            for k in sorted(record.__dict__)
            if k not in _RESERVED
        )
        return f"{base} {extras}".rstrip()


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(_ExtraFormatter("%(asctime)s %(levelname)s %(name)s | %(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
        logger.setLevel(logging.INFO)
    return logger
