"""全局配置（可通过环境变量覆盖，默认值即可本地运行）。"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_path: str = ".data/gateway.db"
    admin_api_key: str = "local-admin-key"
    # 重试策略
    max_attempts: int = 5
    backoff_base_seconds: float = 1.0
    backoff_factor: float = 2.0
    backoff_cap_seconds: float = 300.0
    # 凭据时间容差（秒）：时钟偏移在容差内仍视为有效
    clock_skew_seconds: int = 300
    # 工作线程
    run_worker: bool = True
    worker_interval_seconds: float = 0.1
    worker_batch_size: int = 16

    @classmethod
    def from_env(cls) -> "Settings":
        def env_str(name: str, default: str) -> str:
            return os.environ.get(name, default)

        def env_int(name: str, default: int) -> int:
            raw = os.environ.get(name)
            return int(raw) if raw not in (None, "") else default

        def env_float(name: str, default: float) -> float:
            raw = os.environ.get(name)
            return float(raw) if raw not in (None, "") else default

        def env_bool(name: str, default: bool) -> bool:
            raw = os.environ.get(name)
            if raw is None:
                return default
            return raw.strip().lower() in {"1", "true", "yes", "on"}

        return cls(
            database_path=env_str("GATEWAY_DB", ".data/gateway.db"),
            admin_api_key=env_str("GATEWAY_ADMIN_KEY", "local-admin-key"),
            max_attempts=env_int("GATEWAY_MAX_ATTEMPTS", 5),
            backoff_base_seconds=env_float("GATEWAY_BACKOFF_BASE", 1.0),
            backoff_factor=env_float("GATEWAY_BACKOFF_FACTOR", 2.0),
            backoff_cap_seconds=env_float("GATEWAY_BACKOFF_CAP", 300.0),
            clock_skew_seconds=env_int("GATEWAY_CLOCK_SKEW", 300),
            run_worker=env_bool("GATEWAY_RUN_WORKER", True),
            worker_interval_seconds=env_float("GATEWAY_WORKER_INTERVAL", 0.1),
            worker_batch_size=env_int("GATEWAY_WORKER_BATCH", 16),
        )


_cached: Settings | None = None


def get_settings() -> Settings:
    global _cached
    if _cached is None:
        _cached = Settings.from_env()
    return _cached


def reset_settings_cache() -> None:
    """测试辅助：清除缓存使下次 get_settings() 重新读环境变量。"""
    global _cached
    _cached = None
