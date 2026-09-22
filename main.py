"""FastAPI 入口：事件通知接入与投递网关（本地可运行）。"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app import state
from app.routers import admin, inbound

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    state.bootstrap()
    yield
    state.shutdown()


app = FastAPI(title="Webhook Delivery Gateway", version="0.1.0", lifespan=lifespan)
app.include_router(inbound.router)
app.include_router(admin.router)


@app.get("/")
def health():
    return {"service": "webhook-delivery-gateway", "status": "ok"}
