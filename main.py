"""FastAPI 入口。

本地启动： uv run uvicorn main:app --reload
"""
from __future__ import annotations

from fastapi import FastAPI

from app import api
from app.bootstrap import get_state


def create_app() -> FastAPI:
    app = FastAPI(title="Webhook Delivery Gateway")

    @app.on_event("startup")
    async def _startup() -> None:
        state = get_state()  # 预热默认状态（装配演示数据）
        # 正常运行时投递与失败重试自动推进，无需手工 pump。
        state.dispatcher.start_worker()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await get_state().dispatcher.stop_worker()

    @app.get("/")
    def read_root():
        return {"service": "webhook-delivery-gateway", "status": "ok"}

    api.register(app)
    return app


app = create_app()
