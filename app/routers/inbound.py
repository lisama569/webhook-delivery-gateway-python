"""入站事件：幂等受理 + 租户隔离 + 契约评估后入队。

响应区分：
* 首次受理  -> HTTP 201, status=ACCEPTED
* 重复/并发 -> HTTP 200, status=DUPLICATE（结论稳定可区分）
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from .. import state
from ..models import (
    DeliveryRef,
    DeliveryState,
    FailReason,
    InboundEvent,
    IngestResponse,
    IngestStatus,
)
from ..repository import DuplicateEventError
from .deps import require_tenant

log = logging.getLogger("gateway.inbound")
router = APIRouter(tags=["inbound"])


def _build_response(event_row: dict, deliveries: list[dict],
                    status: IngestStatus = IngestStatus.ACCEPTED) -> IngestResponse:
    return IngestResponse(
        tenant_id=event_row["tenant_id"],
        event_id=event_row["event_id"],
        status=status,
        accepted_at=event_row["accepted_at"],
        deliveries=[
            DeliveryRef(
                subscription_id=d["subscription_id"],
                state=DeliveryState(d["state"]),
                fail_reason=FailReason(d["fail_reason"]),
            )
            for d in (deliveries or [])
        ],
    )


@router.post("/v1/events", response_model=IngestResponse)
def ingest_event(event: InboundEvent, response: Response,
                 tenant=Depends(require_tenant)):
    tenant_id = tenant["tenant_id"]
    now = datetime.now(timezone.utc)
    occurred_at = event.occurred_at.isoformat() if event.occurred_at else None
    try:
        status, event_row, deliveries = state.repo.try_acquire_event(
            tenant_id, event.event_id, event.event_type, event.event_version,
            event.data, occurred_at, now,
        )
        response.status_code = 201
        log.info(
            "ingest verdict | tenant=%s event=%s type=%s v=%s verdict=ACCEPTED "
            "basis=unique_constraint_won deliveries=%s",
            tenant_id, event.event_id, event.event_type, event.event_version,
            [(d["subscription_id"], d["state"], d["fail_reason"]) for d in deliveries],
        )
        return _build_response(event_row, deliveries, IngestStatus.ACCEPTED)
    except DuplicateEventError:
        existing = state.repo.get_event_with_deliveries(tenant_id, event.event_id)
        if existing is None:  # 理论不可达：并发竞争后一定能读到胜者记录
            raise HTTPException(500, detail={"error": "INCONSISTENT_DEDUP_STATE"})
        event_row, deliveries = existing
        response.status_code = 200
        log.info(
            "ingest verdict | tenant=%s event=%s type=%s v=%s verdict=DUPLICATE "
            "basis=unique_constraint_lost_or_repeated_request",
            tenant_id, event.event_id, event.event_type, event.event_version,
        )
        return _build_response(event_row, deliveries, IngestStatus.DUPLICATE)


@router.get("/v1/events/{event_id}", response_model=IngestResponse)
def get_event(event_id: str, tenant=Depends(require_tenant)):
    existing = state.repo.get_event_with_deliveries(tenant["tenant_id"], event_id)
    if existing is None:
        raise HTTPException(status_code=404, detail={
            "error": "EVENT_NOT_FOUND", "event_id": event_id,
        })
    event_row, deliveries = existing
    # 查询接口只报告既有投递，不改变状态
    return IngestResponse(
        tenant_id=event_row["tenant_id"],
        event_id=event_row["event_id"],
        status="ACCEPTED",
        accepted_at=event_row["accepted_at"],
        deliveries=[
            DeliveryRef(
                subscription_id=d["subscription_id"],
                state=DeliveryState(d["state"]),
                fail_reason=FailReason(d["fail_reason"]),
            ) for d in deliveries
        ],
    )
