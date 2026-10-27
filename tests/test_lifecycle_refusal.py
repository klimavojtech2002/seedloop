"""A run is refused or closed cleanly: misuse is not a failing seed, and no path leaks a loop."""

from __future__ import annotations

import asyncio
import gc
import warnings

import pytest

import seedloop
from _support import run_isolated
from seedloop._loop import DeterministicLoop
from seedloop._run import _run_one
from seedloop._world import World


def test_check_inside_a_running_loop_is_refused_untagged() -> None:
    code = """
import asyncio, seedloop

async def noop(world):
    pass

async def outer():
    seedloop.check(noop, seeds=3)

try:
    asyncio.run(outer())
except BaseException as exc:
    print(type(exc).__name__, "|", exc, "|", getattr(exc, "__notes__", None))
"""
    out = run_isolated(code, timeout=10).stdout.strip()
    assert "cannot run inside a running event loop" in out, out
    assert "replay with" not in out  # misuse is not a failing seed
    assert "failing seed" not in out


def test_replay_inside_a_running_loop_is_refused() -> None:
    code = """
import asyncio, seedloop

async def noop(world):
    pass

async def outer():
    seedloop.replay(noop, seed=0)

try:
    asyncio.run(outer())
except BaseException as exc:
    print("refused:", exc)
"""
    assert "cannot run inside a running event loop" in run_isolated(code, timeout=10).stdout


def test_a_refused_run_closes_its_coroutine() -> None:
    code = """
import asyncio, gc, warnings, seedloop
from seedloop._world import World

async def noop(world):
    pass

async def outer():
    world = World(0)
    coro = noop(world)
    try:
        world._drive(coro)
    except seedloop.SeedloopError:
        pass
    print("coroutine-closed", coro.cr_frame is None)

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    asyncio.run(outer())
    gc.collect()
print("warnings", [str(w.message) for w in caught])
"""
    out = run_isolated(code, timeout=10).stdout
    assert "coroutine-closed True" in out
    assert "never awaited" not in out


def test_the_loop_is_closed_after_a_synchronous_raise() -> None:
    loops: list[DeterministicLoop] = []

    def scenario(world: World) -> asyncio.Future[None]:
        loops.append(world._loop)
        raise ValueError("sync")

    with pytest.raises(ValueError, match="sync"):
        _run_one(scenario, 0)
    assert loops[0].is_closed()


def test_the_loop_is_closed_for_a_scenario_that_is_not_a_coroutine() -> None:
    loops: list[DeterministicLoop] = []

    def scenario(world: World) -> None:
        loops.append(world._loop)

    with pytest.raises(seedloop.SeedloopError, match="not a coroutine"):
        _run_one(scenario, 0)  # type: ignore[arg-type]
    assert loops[0].is_closed()


def test_no_resource_warning_after_a_failing_scenario() -> None:
    def raises_sync(world: World) -> None:
        raise ValueError("sync")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = seedloop.check(raises_sync, seeds=2, on_failure="return")  # type: ignore[arg-type]
        assert isinstance(result.error, ValueError)
        del result  # its traceback keeps the World alive
        gc.collect()
    assert [w for w in caught if issubclass(w.category, ResourceWarning)] == []


def test_a_refused_run_leaves_no_open_loop() -> None:
    code = """
import asyncio, gc, warnings
from seedloop._run import _run_one

async def noop(world):
    pass

async def outer():
    try:
        _run_one(noop, 0)
    except Exception as exc:
        print("refused", type(exc).__name__)

with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter("always")
    asyncio.run(outer())
    gc.collect()
print("warnings", [str(w.message) for w in caught])
"""
    out = run_isolated(code, timeout=10).stdout
    assert "refused" in out
    assert "warnings []" in out, out


def test_nan_delay_is_rejected() -> None:
    async def scenario(world: World) -> None:
        asyncio.get_running_loop().call_later(float("nan"), lambda: None)

    with pytest.raises(ValueError, match="NaN"):
        _run_one(scenario, 0)


def test_given_seed_does_not_annotate_a_usage_error() -> None:
    pytest.importorskip("hypothesis")
    from seedloop.errors import _UsageError
    from seedloop.hypothesis import given_seed

    @given_seed()
    def scenario_test(*, seed: int) -> None:
        raise _UsageError("misuse")

    with pytest.raises(_UsageError) as caught:
        scenario_test()
    notes = getattr(caught.value, "__notes__", [])
    assert not [n for n in notes if "seedloop.replay" in n], notes  # Hypothesis adds its own


def test_refusal_happens_before_the_seed_loop() -> None:
    code = """
import asyncio, seedloop

async def noop(world):
    pass

async def outer():
    try:
        seedloop.check(noop, seeds=0)
    except Exception as exc:
        print("refused", "running event loop" in str(exc))

asyncio.run(outer())
"""
    assert "refused True" in run_isolated(code, timeout=10).stdout


def test_a_scenario_that_calls_check_is_a_usage_error_not_a_failing_seed() -> None:
    # In a child process: it hangs if this regresses.
    code = """
import seedloop

async def inner(world):
    pass

async def outer(world):
    seedloop.check(inner, seeds=1)

try:
    seedloop.check(outer, seeds=3)
except seedloop.SeedloopError as exc:
    print("usage error:", "running event loop" in str(exc))
"""
    assert "usage error: True" in run_isolated(code, timeout=15).stdout


def test_a_nested_check_is_never_returned_as_a_failing_seed() -> None:
    async def inner(world: World) -> None:
        pass

    async def outer(world: World) -> None:
        seedloop.check(inner, seeds=1)

    with pytest.raises(seedloop.SeedloopError, match="running event loop") as caught:
        seedloop.check(outer, seeds=3, on_failure="return")
    assert not [n for n in getattr(caught.value, "__notes__", []) if "replay" in n]
