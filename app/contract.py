"""事件契约兼容性规则（版本区间 + 未知字段）。

确定的兼容规则（README 同步说明）：
  R1 发送方版本 < 订阅 min_version：发送方更旧，接收方无法理解 ->
      不投递，fail_reason=CONTRACT_VERSION_BEHIND，进入死信链路。
  R2 声明了 max_version 且事件版本 > max_version（发送方更新）：
      - DELIVER_ANYWAY（默认，前向兼容）：照常投递，事件被标注
        "version out of declared range"，绝不静默丢弃；
      - QUARANTINE：不投递，fail_reason=CONTRACT_VERSION_AHEAD，进入死信链路。
  R3 未知字段：不报错、不过滤，随事件一起投递，仅在 unknown_fields
      中显式列出以保证可解释性。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import FailReason, NewerVersionPolicy


@dataclass
class ContractDecision:
    deliver: bool
    fail_reason: FailReason
    unknown_fields: list[str] = field(default_factory=list)
    annotation: str = ""


def evaluate(event_version: int, payload_keys: set[str], known_fields: list[str],
             min_version: int, max_version: int | None,
             policy: NewerVersionPolicy) -> ContractDecision:
    known = set(known_fields)
    unknown_fields = sorted(k for k in payload_keys if k not in known) if known else []

    if event_version < min_version:
        return ContractDecision(
            deliver=False,
            fail_reason=FailReason.CONTRACT_VERSION_BEHIND,
            unknown_fields=unknown_fields,
            annotation=(
                f"event version {event_version} is older than subscriber "
                f"min_version {min_version}; reject explicitly (no silent drop)"
            ),
        )

    if max_version is not None and event_version > max_version:
        annotation = (
            f"event version {event_version} exceeds subscriber max_version "
            f"{max_version}; policy={policy.value}"
        )
        if policy is NewerVersionPolicy.QUARANTINE:
            return ContractDecision(
                deliver=False,
                fail_reason=FailReason.CONTRACT_VERSION_AHEAD,
                unknown_fields=unknown_fields,
                annotation=annotation + "; quarantined to dead-letter chain",
            )
        return ContractDecision(
            deliver=True,
            fail_reason=FailReason.NONE,
            unknown_fields=unknown_fields,
            annotation=annotation + "; delivered anyway (forward compatible)",
        )

    return ContractDecision(
        deliver=True,
        fail_reason=FailReason.NONE,
        unknown_fields=unknown_fields,
        annotation=(
            "within declared version range"
            + (f"; unknown fields retained: {unknown_fields}" if unknown_fields else "")
        ),
    )
