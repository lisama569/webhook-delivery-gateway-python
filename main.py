"""FastAPI 入口。

本地启动： uv run uvicorn main:app --reload
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import api
from app.bootstrap import get_state


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 预热默认状态（装配演示数据）并启动后台投递 worker：
    # 服务正常运行后，投递与失败重试自动往前走。
    state = get_state()
    state.start_worker()
    try:
        yield
    finally:
        await state.stop_worker()


def create_app() -> FastAPI:
    app = FastAPI(title="Webhook Delivery Gateway", lifespan=lifespan)

    @app.get("/")
    def read_root():
        return {"service": "webhook-delivery-gateway", "status": "ok"}

    api.register(app)
    return app


app = create_app()
