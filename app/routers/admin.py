"""管理面：租户/凭据/订阅/投递查询/死信/Mock 接收端控制。"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request

from .. import state
from ..models import (
    AttemptOut,
    CredentialAdd,
    CredentialOut,
    DeadLetterOut,
    DeliveryOut,
    SubscriptionCreate,
    SubscriptionOut,
    TenantCreate,
    TenantOut,
)
from ..repository import ConflictError
from ..worker import signing_key_override
from .deps import require_admin, require_tenant

log = logging.getLogger("gateway.admin")
router = APIRouter(prefix="/admin", tags=["admin"])


def _tenant_or_404(tenant_id: str) -> TenantOut:
    tenant = state.repo.get_tenant(tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail={"error": "TENANT_NOT_FOUND"})
    return tenant


@router.post("/tenants", response_model=TenantOut, status_code=201,
             dependencies=[Depends(require_admin)])
def create_tenant(body: TenantCreate):
    now = datetime.now(timezone.utc)
    try:
        out = state.repo.create_tenant(
            body.tenant_id, body.display_name, body.api_key,
            body.rate_limit_per_minute, now,
        )
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail={
            "error": "TENANT_ALREADY_EXISTS", "message": str(exc),
        })
    log.info("admin create_tenant tenant=%s", body.tenant_id)
    return out


@router.post("/credentials", response_model=CredentialOut, status_code=201)
def add_credential(body: CredentialAdd, tenant=Depends(require_tenant)):
    tenant_id = tenant["tenant_id"]
    _tenant_or_404(tenant_id)
    now = datetime.now(timezone.utc)
    if body.valid_to is not None and body.valid_to <= body.valid_from:
        raise HTTPException(status_code=422, detail={
            "error": "INVALID_VALIDITY_WINDOW",
            "message": "valid_to must be later than valid_from",
        })
    try:
        out = state.repo.add_credential(
            tenant_id, body.key_id, body.secret, body.valid_from,
            body.valid_to, now,
        )
    except ConflictError as exc:
        raise HTTPException(status_code=409, detail={
            "error": "CREDENTIAL_KEY_EXISTS", "message": str(exc),
        })
    log.info(
        "admin add_credential tenant=%s key=%s valid_from=%s valid_to=%s active=%s",
        tenant_id, body.key_id, body.valid_from, body.valid_to, out.is_active,
    )
    return out


@router.get("/credentials", response_model=list[CredentialOut])
def list_credentials(tenant=Depends(require_tenant)):
    return state.repo.list_credentials(
        tenant["tenant_id"], datetime.now(timezone.utc)
    )


@router.post("/subscriptions", response_model=SubscriptionOut, status_code=201)
def create_subscription(body: SubscriptionCreate, tenant=Depends(require_tenant)):
    tenant_id = tenant["tenant_id"]
    try:
        out = state.repo.create_subscription(
            tenant_id, body.event_type, body.min_version, body.max_version,
            body.target_url, body.newer_version_policy, body.known_fields,
            datetime.now(timezone.utc),
        )
    except ConflictError as exc:
        raise HTTPException(status_code=422, detail={
            "error": "INVALID_SUBSCRIPTION", "message": str(exc),
        })
    log.info(
        "admin create_subscription tenant=%s sub=%s type=%s versions=[%s,%s] "
        "policy=%s url=%s",
        tenant_id, out.subscription_id, body.event_type, body.min_version,
        body.max_version, body.newer_version_policy.value, body.target_url,
    )
    return out


@router.get("/subscriptions", response_model=list[SubscriptionOut])
def list_subscriptions(tenant=Depends(require_tenant)):
    return state.repo.list_subscriptions(tenant["tenant_id"])


def _delivery_to_out(row: dict) -> DeliveryOut:
    return DeliveryOut(
        id=row["id"], tenant_id=row["tenant_id"], event_id=row["event_id"],
        subscription_id=row["subscription_id"], seq=row["seq"],
        state=row["state"], attempts=row["attempts"],
        fail_reason=row["fail_reason"], not_before=row["not_before"],
        last_error=row.get("last_error", ""),
        unknown_fields=row.get("unknown_fields", []),
        history=[
            AttemptOut(
                attempt=h["attempt"], http_status=h["http_status"],
                fail_reason=h["fail_reason"], detail=h["detail"],
                signed_with_key_id=h["signed_with_key_id"], at=h["at"],
            ) for h in row.get("history", [])
        ],
    )


@router.get("/deliveries/{delivery_id}", response_model=DeliveryOut)
def get_delivery(delivery_id: int, tenant=Depends(require_tenant)):
    row = state.repo.get_delivery(tenant["tenant_id"], delivery_id)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "DELIVERY_NOT_FOUND"})
    return _delivery_to_out(row)


@router.get("/events/{event_id}/deliveries", response_model=list[DeliveryOut])
def event_deliveries(event_id: str, tenant=Depends(require_tenant)):
    rows = state.repo.list_deliveries_for_event(tenant["tenant_id"], event_id)
    return [_delivery_to_out(r) for r in rows]


@router.get("/dead-letters", response_model=list[DeadLetterOut])
def list_dead_letters(tenant=Depends(require_tenant)):
    return state.repo.list_dead_letters(tenant["tenant_id"])


# ---------- 本地运维/测试辅助 ----------

@router.post("/signing-key", dependencies=[Depends(require_tenant)])
def set_signing_key(body: dict, tenant=Depends(require_tenant)):
    """强制本租户出站签名使用的 key_id（模拟轮换期投递方切新/用旧）。

    body: {"key_id": "k-new"} 或 {} 恢复"自动选最新有效凭据"。
    指定过期 key 时投递会得到可解释失败：no active signing credential。
    """
    tenant_id = tenant["tenant_id"]
    key_id = body.get("key_id")
    if key_id:
        signing_key_override[tenant_id] = key_id
    else:
        signing_key_override.pop(tenant_id, None)
    log.info("admin signing_key_override tenant=%s key=%s", tenant_id, key_id)
    return {"tenant_id": tenant_id, "signing_key_id": key_id}


@router.post("/worker/tick", dependencies=[Depends(require_admin)])
def worker_tick():
    """同步驱动一轮投递（测试/本地验证用，避免等待后台轮询）。"""
    n = state.worker.tick()
    return {"processed": n}


@router.post("/mock/endpoints/{url:path}", dependencies=[Depends(require_admin)])
def configure_mock(url: str, body: dict):
    accept = body.get("accept_key_ids")
    reject = body.get("reject_key_ids")
    state.transport.configure_endpoint(
        _full_url(url),
        fail_times=int(body.get("fail_times", 0)),
        fail_status=int(body.get("fail_status", 500)),
        transport_error=bool(body.get("transport_error", False)),
        accept_key_ids=set(accept) if accept is not None else None,
        reject_key_ids=set(reject or []),
    )
    return {"url": _full_url(url), "configured": body}


@router.get("/mock/endpoints/{url:path}/received",
            dependencies=[Depends(require_admin)])
def mock_received(url: str):
    return {"url": _full_url(url),
            "received": state.transport.received(_full_url(url))}


def _full_url(url: str) -> str:
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return "http://" + url
