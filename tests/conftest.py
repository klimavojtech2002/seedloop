"""Shared fixtures: fail the test that leaks something, and make leaks easy to avoid."""

from __future__ import annotations

import threading
import time
import weakref
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from seedloop import World
from seedloop._audit import _CURRENT_TIME_FUNCS, _ENTROPY_SURFACES
from seedloop._loop import DeterministicLoop

# The real functions, captured at collection time, before any run could patch them: every name
# the auditor and the shim replace.
_PATCHED_NAMES: list[tuple[Any, str, Any]] = [
    *((mod, attr, getattr(mod, attr)) for mod, attr, _ in _ENTROPY_SURFACES),
    *((time, name, getattr(time, name)) for name, _ in _CURRENT_TIME_FUNCS if hasattr(time, name)),
    (threading.Thread, "start", threading.Thread.start),
]


@pytest.fixture(autouse=True)
def _no_leaked_process_patches() -> Iterator[None]:
    """Fail the test that leaves a process-wide function patched, not a later, unrelated one."""
    yield
    leaked = [name for owner, name, real in _PATCHED_NAMES if getattr(owner, name) is not real]
    for owner, name, real in _PATCHED_NAMES:
        setattr(owner, name, real)  # put them back so one leak cannot break every later test
    if leaked:
        pytest.fail(f"the test left process-wide functions patched: {', '.join(leaked)}")


@pytest.fixture(autouse=True)
def _no_unclosed_loops(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Fail the test that leaves an event loop open, instead of a random later one.

    A loop is part of a reference cycle, so Python frees it only when the cyclic collector next
    runs, and the ``ResourceWarning`` then lands on whichever test is running. Remembering each loop
    when it is created lets the end of the test that made it say so, without waiting for the
    collector.
    """
    created: list[weakref.ref[DeterministicLoop]] = []
    original_init = DeterministicLoop.__init__

    def remembering_init(self: DeterministicLoop) -> None:
        original_init(self)
        created.append(weakref.ref(self))

    monkeypatch.setattr(DeterministicLoop, "__init__", remembering_init)
    yield
    open_loops = [loop for ref in created if (loop := ref()) is not None and not loop.is_closed()]
    for loop in open_loops:
        loop.close()  # so that it cannot warn again later, in another test
    if open_loops:
        pytest.fail(
            f"the test left {len(open_loops)} event loop(s) open: build worlds with the "
            "make_world fixture, or close the loop"
        )


@pytest.fixture
def make_world() -> Iterator[Callable[[int], World]]:
    """Build ``World(seed)`` objects that are closed when the test ends, whatever its outcome."""
    made: list[World] = []

    def build(seed: int = 0) -> World:
        world = World(seed)
        made.append(world)
        return world

    yield build
    for world in made:
        world._close()


def pytest_terminal_summary(terminalreporter: Any, config: pytest.Config) -> None:
    """Print the random-order seed even under ``-q``, which hides the plugin's own header."""
    seed = getattr(config.option, "randomly_seed", None)
    if seed is not None and config.pluginmanager.hasplugin("randomly"):
        terminalreporter.write_line(
            f"random order seed: {seed} (repeat with -p randomly --randomly-seed={seed})"
        )
