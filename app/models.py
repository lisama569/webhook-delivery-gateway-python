"""领域模型与 API schema。"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, Field


class IngestStatus(str, Enum):
    ACCEPTED = "ACCEPTED"   # 首次受理
    DUPLICATE = "DUPLICATE"  # 幂等重复


class DeliveryState(str, Enum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    SUCCEEDED = "SUCCEEDED"
    DEAD_LETTER = "DEAD_LETTER"


class FailReason(str, Enum):
    NONE = "NONE"
    TRANSPORT_ERROR = "TRANSPORT_ERROR"        # 网络异常 / 超时
    NON_2XX = "NON_2XX"                        # 接收方返回非 2xx
    ATTEMPTS_EXHAUSTED = "ATTEMPTS_EXHAUSTED"  # 重试上限耗尽
    CONTRACT_VERSION_AHEAD = "CONTRACT_VERSION_AHEAD"
    CONTRACT_VERSION_BEHIND = "CONTRACT_VERSION_BEHIND"
    ORDER_BLOCKED = "ORDER_BLOCKED"            # 仅用于可解释日志，不落失败链路


class NewerVersionPolicy(str, Enum):
    DELIVER_ANYWAY = "DELIVER_ANYWAY"  # 前向兼容：照投并标注未知字段/超范围版本
    QUARANTINE = "QUARANTINE"          # 进入死信链路，可查询


# ---------- 入站 ----------

class InboundEvent(BaseModel):
    event_id: str = Field(min_length=1, max_length=200)
    event_type: str = Field(min_length=1, max_length=100)
    event_version: int = Field(ge=1)
    data: dict[str, Any]
    occurred_at: datetime | None = None


class DeliveryRef(BaseModel):
    subscription_id: int
    state: DeliveryState
    fail_reason: FailReason = FailReason.NONE


class IngestResponse(BaseModel):
    tenant_id: str
    event_id: str
    status: IngestStatus
    accepted_at: datetime
    deliveries: list[DeliveryRef] = []


# ---------- 管理面 ----------

class TenantCreate(BaseModel):
    tenant_id: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9_\-]+$")
    display_name: str = ""
    api_key: str = Field(min_length=8, max_length=200)
    rate_limit_per_minute: int = Field(default=600, ge=1)


class TenantOut(BaseModel):
    tenant_id: str
    display_name: str
    rate_limit_per_minute: int
    created_at: datetime


class CredentialAdd(BaseModel):
    key_id: str = Field(min_length=1, max_length=100)
    secret: str = Field(min_length=8, max_length=200)
    valid_from: datetime
    valid_to: datetime | None = None  # None = 暂不过期


class CredentialOut(BaseModel):
    key_id: str
    valid_from: datetime
    valid_to: datetime | None
    is_expired: bool
    is_active: bool


class SubscriptionCreate(BaseModel):
    event_type: str = Field(min_length=1, max_length=100)
    min_version: int = Field(default=1, ge=1)
    max_version: int | None = Field(default=None, ge=1)
    target_url: str = Field(min_length=1, max_length=500)
    newer_version_policy: NewerVersionPolicy = NewerVersionPolicy.DELIVER_ANYWAY
    known_fields: list[str] = Field(default_factory=list)


class SubscriptionOut(BaseModel):
    subscription_id: int
    tenant_id: str
    event_type: str
    min_version: int
    max_version: int | None
    target_url: str
    newer_version_policy: NewerVersionPolicy
    created_at: datetime


class AttemptOut(BaseModel):
    attempt: int
    http_status: int | None
    fail_reason: FailReason
    detail: str
    signed_with_key_id: str | None
    at: datetime


class DeliveryOut(BaseModel):
    id: int
    tenant_id: str
    event_id: str
    subscription_id: int
    seq: int
    state: DeliveryState
    attempts: int
    fail_reason: FailReason
    not_before: datetime
    last_error: str = ""
    unknown_fields: list[str] = []
    history: list[AttemptOut] = []


class DeadLetterOut(BaseModel):
    id: int
    tenant_id: str
    event_id: str
    subscription_id: int | None
    fail_reason: FailReason
    detail: str
    created_at: datetime
