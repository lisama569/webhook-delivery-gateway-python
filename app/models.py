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
    # 出站侧凭据问题（与接收方返回失败严格区分）：凭据缺失/未生效/已过期，
    # 请求根本没有发出，故不进入退避重试，直接进入死信链路等待处理（换凭据/重放）。
    CREDENTIAL_UNAVAILABLE = "CREDENTIAL_UNAVAILABLE"


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
    # 运维开关：暂停期间新事件照常入队但不投递；恢复后按原 seq 顺序补发。
    paused: bool = False
    paused_at: datetime | None = None


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
    # 重放代次：首次投递为 0；每发起一次有效重放 +1。attempts 仅保留当代次的
    # 尝试历史，历史代次归档到 previous_attempts，全程可审计。
    replay_generation: int = 0
    previous_attempts: list[DeliveryAttempt] = field(default_factory=list)
    # 重放占用标记：对同一条失败事件并发/连续多次重放，只有一次真正生效，
    # 其余请求拿到稳定且可区分的结论。记录最近一次使其进入占用态的重放代次。
    replay_in_flight: bool = False
