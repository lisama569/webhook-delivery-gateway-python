"""HTTP 路由：入站事件、管理接口（租户/凭据/订阅/契约）、可查询的失败链路。"""
from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.bootstrap import get_state, reset_state
from app.models import (
    Credential,
    DeliveryRecord,
    EventSchema,
    EventStatus,
    FieldSpec,
    Subscription,
    Tenant,
    utcnow,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# 请求/响应模型
# ---------------------------------------------------------------------------
class TenantIn(BaseModel):
    name: str
    rate_limit_per_min: int = Field(default=60, ge=1)


class CredentialIn(BaseModel):
    key_id: str
    secret: str
    not_before: str
    not_after: str
    deprecated: bool = False


class FieldSpecIn(BaseModel):
    type: str
    required: bool = True


class SchemaIn(BaseModel):
    event_type: str
    version: str
    fields: dict[str, FieldSpecIn] = Field(default_factory=dict)


class SubscriptionIn(BaseModel):
    subscription_id: str
    event_type: str
    version: str
    callback_url: str
    credential_key_id: str
    max_attempts: int = Field(default=4, ge=1, le=20)


class MockScriptIn(BaseModel):
    url: str
    # 元素为 [http状态码, 文本] 或 {"error": "描述"}（模拟网络故障）
    responses: list = Field(default_factory=list)


def _parse_dt(value: str):
    from datetime import datetime

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"bad datetime: {value!r}")


# ---------------------------------------------------------------------------
# 入站事件
# ---------------------------------------------------------------------------
@router.post("/v1/events/inbound")
async def inbound_event(
    request: Request,
    x_tenant_id: str = Header(..., alias="X-Tenant-Id"),
    x_key_id: str = Header(..., alias="X-Key-Id"),
    x_timestamp: str = Header(..., alias="X-Timestamp"),
    x_signature: str = Header(..., alias="X-Signature"),
):
    state = get_state()
    raw = await request.body()
    try:
        parsed = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="body must be valid JSON")
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="body must be a JSON object")

    result = await state.service.accept(
        tenant_id=x_tenant_id,
        key_id=x_key_id,
        timestamp=x_timestamp,
        signature=x_signature,
        raw_body=raw,
        parsed=parsed,
    )
    if not result.accepted:
        raise HTTPException(
            status_code=result.status_code,
            detail={"reason": result.reason.value, "detail": result.detail},
        )
    return JSONResponse(
        status_code=result.status_code, content=result.to_response()
    )


# ---------------------------------------------------------------------------
# 管理接口：租户 / 凭据 / 契约 / 订阅
# ---------------------------------------------------------------------------
@router.put("/admin/tenants/{tenant_id}")
async def put_tenant(tenant_id: str, body: TenantIn):
    state = get_state()
    await state.storage.upsert_tenant(
        Tenant(tenant_id, body.name, body.rate_limit_per_min)
    )
    return {"tenant_id": tenant_id, "created": True}


@router.put("/admin/tenants/{tenant_id}/credentials")
async def put_credential(tenant_id: str, body: CredentialIn):
    state = get_state()
    tenant = await state.storage.get_tenant(tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    await state.storage.upsert_credential(
        Credential(
            key_id=body.key_id,
            tenant_id=tenant_id,
            secret=body.secret,
            not_before=_parse_dt(body.not_before),
            not_after=_parse_dt(body.not_after),
            deprecated=body.deprecated,
        )
    )
    return {"key_id": body.key_id, "tenant_id": tenant_id}


@router.get("/admin/tenants/{tenant_id}/credentials")
async def list_credentials(tenant_id: str):
    state = get_state()
    return [
        {
            "key_id": c.key_id,
            "tenant_id": c.tenant_id,
            "not_before": c.not_before.isoformat(),
            "not_after": c.not_after.isoformat(),
            "deprecated": c.deprecated,
        }
        for c in state.storage.list_credentials(tenant_id)
    ]


@router.put("/admin/schemas")
async def put_schema(body: SchemaIn):
    state = get_state()
    try:
        major = int(body.version.split(".")[0])
    except (IndexError, ValueError):
        raise HTTPException(status_code=400, detail="version must be like M.m.p")
    await state.storage.upsert_schema(
        EventSchema(
            event_type=body.event_type,
            version=body.version,
            fields={
                name: FieldSpec(name=name, type=spec.type, required=spec.required)
                for name, spec in body.fields.items()
            },
        )
    )
    return {"event_type": body.event_type, "major": major, "version": body.version}


@router.put("/admin/tenants/{tenant_id}/subscriptions")
async def put_subscription(tenant_id: str, body: SubscriptionIn):
    state = get_state()
    tenant = await state.storage.get_tenant(tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    await state.storage.upsert_subscription(
        Subscription(
            subscription_id=body.subscription_id,
            tenant_id=tenant_id,
            event_type=body.event_type,
            version=body.version,
            callback_url=body.callback_url,
            credential_key_id=body.credential_key_id,
            max_attempts=body.max_attempts,
        )
    )
    return {"subscription_id": body.subscription_id, "tenant_id": tenant_id}


# ---------------------------------------------------------------------------
# 可查询的失败链路与投递状态
# ---------------------------------------------------------------------------
def _delivery_dict(record: DeliveryRecord, tenant_id: str | None) -> dict:
    return {
        "event_id": record.event_id,
        "subscription_id": record.subscription_id,
        "tenant_id": tenant_id,
        "seq": record.seq,
        "status": record.status.value,
        "fail_reason": record.fail_reason.value,
        "fail_detail": record.fail_detail,
        "next_attempt_at": record.next_attempt_at.isoformat()
        if record.next_attempt_at
        else None,
        "attempts": [
            {
                "attempt": a.attempt,
                "kind": a.kind.value if a.kind else None,
                "http_status": a.http_status,
                "detail": a.detail,
                "backoff_seconds": a.backoff_seconds,
            }
            for a in record.attempts
        ],
    }


@router.get("/admin/deliveries")
async def list_deliveries(tenant_id: str | None = None, status: str | None = None):
    state = get_state()
    status_enum = EventStatus(status) if status else None
    records = state.storage.list_deliveries(tenant_id=tenant_id, status=status_enum)
    return [
        _delivery_dict(r, state.storage._delivery_tenant(r)) for r in records
    ]


@router.get("/admin/deliveries/dead-letter")
async def list_dead_letters(tenant_id: str | None = None):
    state = get_state()
    records = state.storage.list_deliveries(
        tenant_id=tenant_id, status=EventStatus.DEAD_LETTER
    )
    return [
        _delivery_dict(r, state.storage._delivery_tenant(r)) for r in records
    ]


@router.get("/admin/tenants/{tenant_id}/events/{event_id}")
async def get_event(tenant_id: str, event_id: str):
    state = get_state()
    event = await state.storage.get_event(tenant_id, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return {
        "event_id": event.event_id,
        "tenant_id": event.tenant_id,
        "event_type": event.event_type,
        "version": event.version,
        "schema_version_used": event.schema_version_used,
        "unknown_fields": event.unknown_fields,
        "status": event.status.value,
        "fail_reason": event.fail_reason.value,
        "payload": event.payload,
        "received_at": event.received_at.isoformat(),
    }


# ---------------------------------------------------------------------------
# 本地驱动 / 演示辅助
# ---------------------------------------------------------------------------
@router.post("/admin/pump")
async def pump():
    """推进一次到期的投递任务（确定性退避由时间自然流逝驱动）。"""
    state = get_state()
    processed = await state.dispatcher.run_pending()
    return {"processed": processed, "at": utcnow().isoformat()}


@router.post("/admin/drain")
async def drain():
    """等待所有重试完成（本地演示用）。"""
    state = get_state()
    await state.dispatcher.run_until_drained()
    return {"drained": True}


@router.post("/admin/mock/script")
async def mock_script(body: MockScriptIn):
    """为某个回调 url 编排响应序列，本地复现失败/恢复场景。"""
    state = get_state()
    scripted = []
    for item in body.responses:
        if isinstance(item, dict) and "error" in item:
            scripted.append(ConnectionError(item["error"]))
        elif isinstance(item, list) and len(item) == 2:
            scripted.append((int(item[0]), str(item[1])))
        else:
            raise HTTPException(status_code=400, detail=f"bad script item: {item!r}")
    state.transport.script(body.url, scripted)
    return {"url": body.url, "steps": len(scripted)}


@router.post("/admin/reset")
async def reset():
    reset_state()
    return {"reset": True}


def register(app) -> None:
    app.include_router(router)
