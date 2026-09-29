"""验签、凭据轮换重叠期、过期凭据拒绝、时间戳防重放。"""
from datetime import datetime, timedelta, timezone

import pytest

from app.models import Credential
from app.security import SKEW_SECONDS, sign, verify_signature

NOW = datetime(2026, 9, 22, 12, 0, 0, tzinfo=timezone.utc)
TS = str(int(NOW.timestamp()))
BODY = b'{"event_id":"e-sec"}'


def _creds():
    return [
        Credential("old", "t1", "s-old", NOW - timedelta(days=30),
                   NOW + timedelta(days=1), deprecated=True),
        Credential("new", "t1", "s-new", NOW - timedelta(days=1),
                   NOW + timedelta(days=29)),
        Credential("expired", "t1", "s-exp", NOW - timedelta(days=60),
                   NOW - timedelta(days=1)),
        Credential("future", "t1", "s-fut", NOW + timedelta(days=1),
                   NOW + timedelta(days=30)),
    ]


@pytest.mark.parametrize("key_id,secret", [("old", "s-old"), ("new", "s-new")])
def test_rotation_overlap_accepts_old_and_new(key_id, secret):
    ok, basis, cred = verify_signature(_creds(), key_id, TS, BODY,
                                       sign(secret, TS, BODY), now=NOW)
    print(f"\n[event=e-sec] 凭据轮换判定 key_id={key_id} basis={basis} "
          f"deprecated={cred.deprecated}")
    assert ok and basis == "OK"


def test_expired_credential_explicitly_rejected():
    ok, basis, cred = verify_signature(_creds(), "expired", TS, BODY,
                                       sign("s-exp", TS, BODY), now=NOW)
    print(f"\n[event=e-sec] 过期凭据判定 key_id=expired basis={basis} "
          f"not_after={cred.not_after.isoformat()}")
    assert not ok and basis == "EXPIRED_CREDENTIAL"


def test_not_yet_valid_rejected():
    ok, basis, _ = verify_signature(_creds(), "future", TS, BODY,
                                    sign("s-fut", TS, BODY), now=NOW)
    print(f"\n[event=e-sec] 未生效凭据判定 basis={basis}")
    assert not ok and basis == "CREDENTIAL_NOT_YET_VALID"


def test_stale_timestamp_rejected():
    stale = str(int(NOW.timestamp()) - SKEW_SECONDS - 1)
    ok, basis, _ = verify_signature(_creds(), "new", stale, BODY,
                                    sign("s-new", stale, BODY), now=NOW)
    print(f"\n[event=e-sec] 时间戳重放判定 basis={basis} skew={SKEW_SECONDS + 1}s")
    assert not ok and basis == "STALE_TIMESTAMP"


def test_tampered_body_signature_mismatch():
    ok, basis, _ = verify_signature(_creds(), "new", TS, BODY + b"x",
                                    sign("s-new", TS, BODY), now=NOW)
    print(f"\n[event=e-sec] 篡改报文判定 basis={basis}")
    assert not ok and basis == "SIGNATURE_MISMATCH"


def test_unknown_key_rejected():
    ok, basis, _ = verify_signature(_creds(), "ghost", TS, BODY, "ab", now=NOW)
    assert not ok and basis == "KEY_NOT_FOUND"


def test_outbound_credential_window_uses_same_rules():
    """出站凭据窗口判定与入站验签同一套 not_before/not_after/轮换规则。"""
    from app.security import credential_window_basis

    creds = {c.key_id: c for c in _creds()}
    print(f"\n[cred-window] 缺失={credential_window_basis(None, NOW)} "
          f"未生效={credential_window_basis(creds['future'], NOW)} "
          f"已过期={credential_window_basis(creds['expired'], NOW)} "
          f"新={credential_window_basis(creds['new'], NOW)} "
          f"旧(deprecated,窗口内)={credential_window_basis(creds['old'], NOW)}")
    assert credential_window_basis(None, NOW) == "KEY_NOT_FOUND"
    assert credential_window_basis(creds["future"], NOW) == "CREDENTIAL_NOT_YET_VALID"
    assert credential_window_basis(creds["expired"], NOW) == "EXPIRED_CREDENTIAL"
    # 轮换宽限：deprecated 但仍在窗口内仍然可用
    assert credential_window_basis(creds["old"], NOW) == "OK"
    assert credential_window_basis(creds["new"], NOW) == "OK"
