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
        get_state()  # 预热默认状态（装配演示数据）

    @app.get("/")
    def read_root():
        return {"service": "webhook-delivery-gateway", "status": "ok"}

    api.register(app)
    return app


app = create_app()
