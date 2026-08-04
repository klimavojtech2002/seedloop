"""`world.run_for` / `world.run_until`: the time-advancing scenario primitives (0460).

`run_for`'s `faults` parameter is exercised in `test_fault_handles.py` (0470); this file covers the
time-advance mechanics only.
"""

from __future__ import annotations

import asyncio
import contextlib
import time as real_time
from collections.abc import Awaitable, Callable

import pytest

import seedloop
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.errors import DeadlockError, InvariantError, SeedloopError

# --- run_for ---------------------------------------------------------------------------------


def test_run_for_advances_virtual_time_by_exactly_seconds() -> None:
    async def scenario(world: World) -> None:
        await world.run_for(seconds=5)
        world.record(("now", world.now()))

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert result.error is None


def test_run_for_zero_seconds_is_a_no_op_step() -> None:
    async def scenario(world: World) -> None:
        before = world.now()
        await world.run_for(seconds=0)
        assert world.now() == before

    seedloop.check(scenario, seeds=1)  # raises if the assertion inside fails


def test_run_for_negative_seconds_raises() -> None:
    async def scenario(world: World) -> None:
        with pytest.raises(SeedloopError):
            await world.run_for(seconds=-1)

    seedloop.check(scenario, seeds=1)


def test_run_for_rejects_non_finite_seconds() -> None:
    # asyncio.sleep(nan) happens to raise on its own, but relying on that is fragile: it would
    # surface a raw ValueError instead of SeedloopError. Validate explicitly instead.
    async def scenario(world: World) -> None:
        with pytest.raises(SeedloopError, match="finite"):
            await world.run_for(seconds=float("nan"))
        with pytest.raises(SeedloopError, match="finite"):
            await world.run_for(seconds=float("inf"))

    seedloop.check(scenario, seeds=1)


def test_run_for_real_duration_is_virtual() -> None:
    async def scenario(world: World) -> None:
        await world.run_for(seconds=10**6)

    started = real_time.monotonic()
    seedloop.check(scenario, seeds=1)
    assert real_time.monotonic() - started < 2


# --- run_until: basic return-on-predicate ------------------------------------------------------


def test_run_until_returns_once_predicate_flips_true() -> None:
    async def scenario(world: World) -> None:
        state = {"ready": False}

        async def flip() -> None:
            await asyncio.sleep(1)
            state["ready"] = True

        flipper = asyncio.create_task(flip())
        await world.run_until(lambda: state["ready"])
        world.record(("done", world.now()))
        await flipper

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert result.error is None


def test_run_until_already_true_returns_without_extra_step() -> None:
    async def scenario(world: World) -> None:
        before = world.now()
        await world.run_until(lambda: True)
        assert world.now() == before  # no time was spent waiting for an already-true predicate

    seedloop.check(scenario, seeds=1)


# --- run_until: deadline + tie-break -------------------------------------------------------------


def test_run_until_deadline_raises_timeout_when_predicate_never_holds() -> None:
    async def scenario(world: World) -> None:
        with pytest.raises(TimeoutError):
            await world.run_until(lambda: False, deadline=5)
        world.record(("now", world.now()))

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert result.error is None
    assert _run_one(scenario, 0)[-1] == (5.0, ("now", 5.0))


def test_run_until_deadline_is_virtual_not_real() -> None:
    async def scenario(world: World) -> None:
        with pytest.raises(TimeoutError):
            await world.run_until(lambda: False, deadline=10**6)

    started = real_time.monotonic()
    seedloop.check(scenario, seeds=1)
    assert real_time.monotonic() - started < 2


def test_run_until_predicate_wins_the_tie_at_the_deadline() -> None:
    # If the predicate becomes true in the same step the deadline elapses, the predicate wins:
    # run_until returns normally, it does not raise TimeoutError. The predicate reads the clock
    # directly (no Task/Future wakeup hop in between) so it becomes true in the exact same step the
    # deadline's own timer fires — a genuine tie. Flipping the check order in World.run_until
    # (deadline before predicate) must fail this test.
    async def scenario(world: World) -> None:
        await world.run_until(lambda: world.now() >= 3, deadline=3)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert result.error is None


def test_run_until_no_deadline_no_progress_raises_deadlock() -> None:
    # No deadline, the predicate never holds, and nothing else can ever wake the run: the existing
    # DeadlockError guard catches it. No bespoke timeout mechanism is needed for this path.
    async def scenario(world: World) -> None:
        await world.run_until(lambda: False)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, DeadlockError)


def test_run_until_deadlock_while_pending_resets_its_own_state() -> None:
    # DeadlockError fires from _run_once's own quiescent guard, before the after-step hooks even
    # run — a completely different path from run_until's own call stack. World._drive's teardown
    # then clears the whole hook list before cancelling the still-suspended scenario task, whose
    # `run_until` resumes with CancelledError and runs its own `finally`. Without the defensive
    # membership check there, `hooks.remove(check)` raised `ValueError` (not in list) right there,
    # aborting that `finally` before it could reset `_run_until_active` — a state leak invisible to
    # `check()`'s reported error (DeadlockError surfaces either way) but real. White-box: drive the
    # World directly to inspect its post-run state.
    world = World(0)

    async def scenario() -> None:
        await world.run_until(lambda: False)

    with pytest.raises(DeadlockError):
        world._drive(scenario())
    assert world._run_until_active is False
    assert world._loop._sl_after_step_hooks == []


def test_run_until_raising_predicate_propagates() -> None:
    def boom() -> bool:
        raise ValueError("predicate blew up")

    async def scenario(world: World) -> None:
        await world.run_until(boom)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, ValueError)


def test_run_until_predicate_raising_on_a_later_step_is_catchable_by_the_scenario() -> None:
    # A predicate that raises only after the first synchronous check runs from inside the loop's
    # after-step hook, not run_until's own call stack. Before the fix this exception escaped
    # straight through _run_once instead of being routed through `done`, so a scenario's own
    # try/except around `await world.run_until(...)` never saw it, and teardown then crashed trying
    # to remove the already-fired hook (`ValueError`, not in list), masking the real failure.
    async def scenario(world: World) -> None:
        calls = {"n": 0}

        def flaky() -> bool:
            calls["n"] += 1
            if calls["n"] == 1:
                return False  # false on the first, synchronous check inside run_until itself
            raise ValueError("boom on a later step")  # raises from the after-step hook instead

        caught = False
        try:
            await world.run_until(flaky, deadline=10)
        except ValueError as exc:
            caught = True
            assert str(exc) == "boom on a later step"
        assert caught
        world.record(("caught", world.now()))

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert result.error is None


def test_run_until_deadline_nan_is_rejected_not_a_hang() -> None:
    # deadline=nan broke heapq's ordering (NaN compares False against everything), so the virtual
    # clock's autojump never advanced past t=0 and the run spun forever with no exception, no
    # timeout, and no deadlock raised — a genuine unbounded hang reachable from the public API.
    async def scenario(world: World) -> None:
        with pytest.raises(SeedloopError, match="finite"):
            await world.run_until(lambda: False, deadline=float("nan"))

    started = real_time.monotonic()
    seedloop.check(scenario, seeds=1)
    assert real_time.monotonic() - started < 2


def test_concurrent_run_until_is_rejected() -> None:
    async def scenario(world: World) -> None:
        async def first() -> None:
            await world.run_until(lambda: False, deadline=10)

        task = asyncio.create_task(first())
        await asyncio.sleep(0)  # let `first` run up to its own run_until's suspension point
        with pytest.raises(SeedloopError, match="concurrent or nested"):
            await world.run_until(lambda: False, deadline=10)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    seedloop.check(scenario, seeds=1)


# --- run_until + always(): the two after-step hooks must compose ---------------------------------


def test_invariant_still_fires_while_run_until_is_pending() -> None:
    # An always() invariant registered before a run_until call must keep being checked while that
    # call is pending — proof the hook-list generalization doesn't let one hook mask the other.
    async def scenario(world: World) -> None:
        state = {"ok": True}
        world.always(lambda: state["ok"], name="stays-ok")

        async def breach() -> None:
            await asyncio.sleep(1)
            state["ok"] = False

        breacher = asyncio.create_task(breach())
        await world.run_until(lambda: False, deadline=100)  # would time out at t=100 if unmasked
        await breacher

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, InvariantError)
    assert result.error.name == "stays-ok"


def test_sequential_run_until_calls_clean_up_their_hooks() -> None:
    async def scenario(world: World) -> None:
        state = {"a": False, "b": False}

        async def flip(key: str, delay: float) -> None:
            await asyncio.sleep(delay)
            state[key] = True

        before = len(world._loop._sl_after_step_hooks)
        first = asyncio.create_task(flip("a", 1))
        await world.run_until(lambda: state["a"])
        assert len(world._loop._sl_after_step_hooks) == before  # the first call's hook is gone

        second = asyncio.create_task(flip("b", 1))
        await world.run_until(lambda: state["b"])
        assert len(world._loop._sl_after_step_hooks) == before
        await first
        await second

    seedloop.check(scenario, seeds=1)


def test_invariant_and_run_until_predicate_failing_the_same_step_both_surface() -> None:
    # A run_until predicate exception is delivered through its own Future (`done`), one step later
    # than an always() invariant's direct, synchronous raise — so on its own it can never win a
    # same-step race and used to vanish completely if an invariant failed in that exact same step.
    # The invariant still wins (it is the hard failure), but the predicate's exception must now be
    # attached to it as a note rather than silently discarded, whichever hook was registered first.
    async def scenario(world: World) -> None:
        world.always(lambda: world.now() < 1, name="time-bound")

        def flaky() -> bool:
            if world.now() >= 1:
                raise ValueError("predicate bug, independent of the invariant")
            return False

        await world.run_until(flaky, deadline=10)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, InvariantError)
    notes = getattr(result.error, "__notes__", [])
    assert any("predicate bug, independent of the invariant" in note for note in notes)


def test_invariant_and_run_until_predicate_failing_the_same_step_registration_order() -> None:
    # Same coincident failure, but run_until is called before always() this time — the invariant
    # (a hard hook) must still be what the run raises regardless of registration order, since a
    # soft hook (run_until's predicate) never becomes the primary failure on its own.
    async def scenario(world: World) -> None:
        def flaky() -> bool:
            if world.now() >= 1:
                raise ValueError("predicate bug, independent of the invariant")
            return False

        async def register_invariant_late() -> None:
            await asyncio.sleep(0)
            world.always(lambda: world.now() < 1, name="time-bound")

        registrar = asyncio.create_task(register_invariant_late())
        await world.run_until(flaky, deadline=10)
        await registrar

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, InvariantError)


# --- replay equivalence: the determinism proof for this slice -------------------------------------


def test_replay_equivalence_run_for_and_run_until() -> None:
    async def scenario(world: World) -> None:
        a = world.net.bind(1, loss=0.2)
        b = world.net.bind(2, loss=0.2)
        state = {"received": 0}

        async def receiver() -> None:
            while True:
                await b.recv()
                state["received"] += 1

        world.start(_AdHocNode(receiver))
        world.always(lambda: state["received"] >= 0, name="never-negative")

        await world.run_for(seconds=1)
        for i in range(3):
            await a.send(2, i)
        with contextlib.suppress(TimeoutError):
            await world.run_until(lambda: state["received"] >= 3, deadline=5)
        world.record(("received", state["received"]))

    first = _run_one(scenario, 7)
    second = _run_one(scenario, 7)
    assert first == second


class _AdHocNode:
    def __init__(self, run_fn: Callable[[], Awaitable[None]]) -> None:
        self._run_fn = run_fn

    async def run(self) -> None:
        await self._run_fn()
