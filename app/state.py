"""应用运行期共享状态（单例：settings / repository / transport / worker）。"""
from __future__ import annotations

from dataclasses import replace

from .config import Settings, get_settings
from .repository import Repository
from .transport import MockTransport
from .worker import DeliveryWorker

settings: Settings | None = None
repo: Repository | None = None
transport: MockTransport | None = None
worker: DeliveryWorker | None = None


def bootstrap(db_path: str | None = None, *, run_worker: bool | None = None,
              backoff_base_seconds: float | None = None
              ) -> tuple[Repository, MockTransport, DeliveryWorker, Settings]:
    """初始化（幂等：重复调用返回同一套单例）。"""
    global settings, repo, transport, worker
    if repo is not None:
        assert transport is not None and worker is not None and settings is not None
        return repo, transport, worker, settings

    base = get_settings()
    settings = replace(
        base,
        **({"database_path": db_path} if db_path is not None else {}),
        **({"run_worker": run_worker} if run_worker is not None else {}),
        **({"backoff_base_seconds": backoff_base_seconds}
           if backoff_base_seconds is not None else {}),
    )
    transport = MockTransport()
    repo = Repository(settings)
    worker = DeliveryWorker(repo, transport, settings)
    if settings.run_worker:
        worker.start_background()
    return repo, transport, worker, settings


def shutdown() -> None:
    global settings, repo, transport, worker
    if worker is not None:
        worker.stop_background()
    settings = repo = transport = worker = None  # type: ignore[assignment]


def reset() -> None:
    """测试辅助：彻底重置单例。"""
    shutdown()
