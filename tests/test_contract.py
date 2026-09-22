"""契约版本演进：未知版本、主版本不兼容、次版本前向兼容、未知字段保留。"""
import pytest

from app.contract import check_compatibility, parse_semver
from app.models import EventSchema, FieldSpec, FailReason

SCHEMA = EventSchema(
    "order.created", "1.2.0",
    {
        "order_id": FieldSpec("order_id", "string"),
        "amount": FieldSpec("amount", "int", required=False),
    },
)


def test_minor_forward_compatible_with_unknown_fields_retained():
    # 接收方订阅 1.0，发送方事件升到 1.3，新增字段保留不丢弃
    result = check_compatibility(SCHEMA, "1.0.0",
                                 {"order_id": "o-1", "amount": 10, "new_field": "x"})
    print(f"\n[event=contract-minor] 次版本前向兼容 reason={result.reason.value} "
          f"unknown_fields={result.unknown_fields} detail={result.detail}")
    assert result.ok and result.unknown_fields == ["new_field"]


def test_major_mismatch_incompatible():
    result = check_compatibility(SCHEMA, "2.0.0", {"order_id": "o-1"})
    print(f"\n[event=contract-major] 主版本不一致 reason={result.reason.value} "
          f"detail={result.detail}")
    assert not result.ok and result.reason is FailReason.CONTRACT_INCOMPATIBLE


def test_missing_required_field_incompatible():
    result = check_compatibility(SCHEMA, "1.0.0", {"amount": 10})
    assert not result.ok and "missing required field" in result.detail


def test_wrong_type_incompatible():
    result = check_compatibility(SCHEMA, "1.0.0", {"order_id": 123})
    assert not result.ok and "expects str" in result.detail


def test_semver_parsing():
    assert parse_semver("1.2.3") == (1, 2, 3)
    with pytest.raises(ValueError):
        parse_semver("v1.2")
