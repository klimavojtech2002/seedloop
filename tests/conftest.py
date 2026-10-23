"""Fail the test that leaves a process-wide function patched, instead of a later, unrelated one."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import Any

import pytest

from seedloop._audit import _CURRENT_TIME_FUNCS, _ENTROPY_SURFACES

# The real functions, captured at collection time, before any run could patch them: every name
# the auditor and the shim replace.
_PATCHED_NAMES: list[tuple[Any, str, Any]] = [
    *((mod, attr, getattr(mod, attr)) for mod, attr, _ in _ENTROPY_SURFACES),
    *((time, name, getattr(time, name)) for name, _ in _CURRENT_TIME_FUNCS if hasattr(time, name)),
    (threading.Thread, "start", threading.Thread.start),
]


@pytest.fixture(autouse=True)
def _no_leaked_process_patches() -> Iterator[None]:
    yield
    leaked = [name for owner, name, real in _PATCHED_NAMES if getattr(owner, name) is not real]
    for owner, name, real in _PATCHED_NAMES:
        setattr(owner, name, real)  # put them back so one leak cannot break every later test
    if leaked:
        pytest.fail(f"the test left process-wide functions patched: {', '.join(leaked)}")
