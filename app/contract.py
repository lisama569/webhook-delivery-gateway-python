"""事件契约：语义化版本解析与字段兼容性判定。

兼容规则（确定性，详见 docs）：
1. 契约按 ``事件类型 + 主版本`` 注册。主版本在注册表中不存在 →
   UNKNOWN_EVENT_VERSION（由受理层判定，拒绝受理，返回 422）；
2. 订阅与事件的主版本必须一致，否则 CONTRACT_INCOMPATIBLE；
3. 同一主版本内次版本向前兼容：事件次版本高于注册契约时，多出的字段记为
   unknown_fields —— 不报错、不丢弃（payload 原样投递，字段名落库可查）；
4. 注册契约中 required=True 的字段必须存在且类型匹配，否则
   CONTRACT_INCOMPATIBLE，detail 明确指出字段与期望类型。
"""
from __future__ import annotations

from app.logging_utils import get_logger
from app.models import EventSchema, FailReason

logger = get_logger(__name__)

_TYPE_MAP = {
    "string": ("str", lambda v: isinstance(v, str)),
    "int": ("int", lambda v: isinstance(v, int) and not isinstance(v, bool)),
    "float": ("number", lambda v: isinstance(v, (int, float)) and not isinstance(v, bool)),
    "bool": ("bool", lambda v: isinstance(v, bool)),
}


class CompatibilityResult:
    def __init__(
        self,
        ok: bool,
        reason: FailReason,
        detail: str,
        effective_version: str | None = None,
        unknown_fields: list[str] | None = None,
    ) -> None:
        self.ok = ok
        self.reason = reason
        self.detail = detail
        self.effective_version = effective_version
        self.unknown_fields = unknown_fields or []

    def __repr__(self) -> str:  # 便于测试日志阅读
        return (
            f"CompatibilityResult(ok={self.ok}, reason={self.reason.value}, "
            f"detail={self.detail!r}, effective_version={self.effective_version!r}, "
            f"unknown_fields={self.unknown_fields})"
        )


def parse_semver(version: str) -> tuple[int, int, int]:
    parts = str(version).split(".")
    if len(parts) != 3 or not all(p.isdigit() for p in parts):
        raise ValueError(f"invalid semver: {version!r}")
    return int(parts[0]), int(parts[1]), int(parts[2])


def _type_ok(spec_type: str, value) -> bool:
    checker = _TYPE_MAP.get(spec_type)
    return checker[1](value) if checker else False


def check_compatibility(
    schema: EventSchema,
    subscribed_version: str,
    payload: dict,
) -> CompatibilityResult:
    schema_major = parse_semver(schema.version)[0]
    sub_major, sub_minor, _ = parse_semver(subscribed_version)

    if sub_major != schema_major:
        detail = (
            f"major mismatch: event schema {schema.version} vs subscription "
            f"{subscribed_version}"
        )
        logger.info("contract_check basis=MAJOR_MISMATCH detail=%s", detail)
        return CompatibilityResult(False, FailReason.CONTRACT_INCOMPATIBLE, detail)

    # 未知字段：注册契约之外的字段一律保留并登记，绝不静默丢弃。
    unknown_fields = sorted(k for k in payload if k not in schema.fields)

    # 必填字段存在性与类型。
    problems: list[str] = []
    for name, spec in schema.fields.items():
        if not spec.required:
            continue
        if name not in payload:
            problems.append(f"missing required field {name!r}")
            continue
        if not _type_ok(spec.type, payload[name]):
            expect = _TYPE_MAP.get(spec.type, (spec.type, None))[0]
            problems.append(
                f"field {name!r} expects {expect}, got {type(payload[name]).__name__}"
            )

    if problems:
        detail = "; ".join(problems)
        logger.info(
            "contract_check basis=FIELD_VIOLATION effective_version=%s detail=%s",
            schema.version, detail,
        )
        return CompatibilityResult(
            False, FailReason.CONTRACT_INCOMPATIBLE, detail,
            effective_version=schema.version, unknown_fields=unknown_fields,
        )

    detail = "compatible"
    if unknown_fields:
        detail = f"compatible; {len(unknown_fields)} unknown field(s) retained"
    logger.info(
        "contract_check basis=COMPATIBLE effective_version=%s unknown_fields=%s",
        schema.version, unknown_fields,
    )
    return CompatibilityResult(
        True, FailReason.NONE, detail,
        effective_version=schema.version, unknown_fields=unknown_fields,
    )
