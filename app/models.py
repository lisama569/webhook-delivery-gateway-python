"""领域模型：枚举与数据结构（仅类型定义，行为在各模块中实现）。"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class EventStatus(str, Enum):
    """入站事件生命周期状态。"""
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    SCHEDULED = "SCHEDULED"
    DELIVERING = "DELIVERING"
    DELIVERED = "DELIVERED"
    RETRYING = "RETRYING"
    DEAD_LETTER = "DEAD_LETTER"


class SubscriptionState(str, Enum):
    """订阅投递开关（运行态，不影响入站受理）。"""
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"


class ReplayOutcome(str, Enum):
    """失败事件重放请求的稳定结论（并发/重复调用可区分）。"""
    REPLAYED = "REPLAYED"                  # 本次请求生效，事件已重新排队
    ALREADY_REPLAYING = "ALREADY_REPLAYING"  # 已被先前请求重放，正在投递中
    NOT_DEAD_LETTER = "NOT_DEAD_LETTER"    # 事件当前不在失败链路
    ALREADY_DELIVERED = "ALREADY_DELIVERED"  # 已成功投递，拒绝重发
    NOT_FOUND = "NOT_FOUND"                # 投递记录不存在


class FailReason(str, Enum):
    """可解释的失败/拒绝原因。"""
    NONE = "NONE"
    UNKNOWN_TENANT = "UNKNOWN_TENANT"
    INVALID_SIGNATURE = "INVALID_SIGNATURE"
    EXPIRED_CREDENTIAL = "EXPIRED_CREDENTIAL"
    STALE_TIMESTAMP = "STALE_TIMESTAMP"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    UNKNOWN_EVENT_VERSION = "UNKNOWN_EVENT_VERSION"
    CONTRACT_INCOMPATIBLE = "CONTRACT_INCOMPATIBLE"
    SUBSCRIPTION_MISMATCH = "SUBSCRIPTION_MISMATCH"
    NETWORK_ERROR = "NETWORK_ERROR"
    HTTP_4XX = "HTTP_4XX"
    HTTP_5XX = "HTTP_5XX"
    ATTEMPTS_EXHAUSTED = "ATTEMPTS_EXHAUSTED"
    # 出站凭据不可用：网关侧问题，与接收方返回的失败明确区分
    CREDENTIAL_MISSING = "CREDENTIAL_MISSING"
    CREDENTIAL_NOT_YET_VALID = "CREDENTIAL_NOT_YET_VALID"
    CREDENTIAL_EXPIRED = "CREDENTIAL_EXPIRED"


class DeliveryKind(str, Enum):
    SUCCESS = "SUCCESS"
    RETRYABLE = "RETRYABLE"
    PERMANENT = "PERMANENT"


@dataclass
class Credential:
    """合作方凭据；not_before/not_after 支持轮换宽限期。"""
    key_id: str
    tenant_id: str
    secret: str
    not_before: datetime
    not_after: datetime
    deprecated: bool = False


@dataclass
class Tenant:
    tenant_id: str
    name: str
    rate_limit_per_min: int
    credential_ids: list[str] = field(default_factory=list)


@dataclass
class FieldSpec:
    name: str
    type: str  # "string" | "int" | "float" | "bool"
    required: bool = True


@dataclass
class EventSchema:
    """某主版本下的事件契约。"""
    event_type: str
    version: str  # 语义化版本，如 "1.0.0"
    fields: dict[str, FieldSpec] = field(default_factory=dict)


@dataclass
class Subscription:
    subscription_id: str
    tenant_id: str
    event_type: str
    version: str  # 订阅声明的契约主版本
    callback_url: str
    credential_key_id: str  # 出站签名使用的凭据
    max_attempts: int = 4


@dataclass
class InboundEvent:
    event_id: str
    tenant_id: str
    event_type: str
    version: str
    payload: dict
    received_at: datetime
    status: EventStatus = EventStatus.ACCEPTED
    fail_reason: FailReason = FailReason.NONE
    schema_version_used: str | None = None
    unknown_fields: list[str] = field(default_factory=list)


@dataclass
class DeliveryAttempt:
    attempt: int
    started_at: datetime
    finished_at: datetime | None
    kind: DeliveryKind | None
    http_status: int | None
    detail: str
    backoff_seconds: float = 0.0


@dataclass
class DeliveryRecord:
    event_id: str
    subscription_id: str
    status: EventStatus
    attempts: list[DeliveryAttempt] = field(default_factory=list)
    fail_reason: FailReason = FailReason.NONE
    fail_detail: str = ""
    next_attempt_at: datetime | None = None
    seq: int = 0
    # 失败重放归档：原死信结论保留可查，与新一轮尝试互不覆盖
    replayed: bool = False
    replay_count: int = 0
    archived_fail_reason: FailReason | None = None
    archived_fail_detail: str | None = None
    archived_at: datetime | None = None
