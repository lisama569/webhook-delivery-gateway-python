"""pytest 夹具：隔离的应用状态与异步支持。"""
import pytest

from app.bootstrap import reset_state


@pytest.fixture
def state():
    return reset_state()
