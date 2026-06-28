"""The invariant API: world.always(...) checked after every step."""

from __future__ import annotations

import asyncio

import seedloop
from seedloop._run import _run_one
from seedloop._world import World


def test_passing_invariant_does_not_fire_or_change_timeline() -> None:
    # An invariant that always holds must not fail the run, and checking it must change nothing: the
    # timeline is identical to the same scenario without the invariant.
    async def base(world: World) -> None:
        for _ in range(3):
            await asyncio.sleep(1)
            world.record(("tick", world.now()))

    async def with_invariant(world: World) -> None:
        world.always(lambda: True, name="always-true")
        await base(world)

    assert _run_one(with_invariant, 1) == _run_one(base, 1)


def test_violated_invariant_raises_with_name() -> None:
    async def scenario(world: World) -> None:
        breached = {"v": False}
        world.always(lambda: not breached["v"], name="must-stay-false")
        await asyncio.sleep(1)
        breached["v"] = True  # next step the invariant is false
        await asyncio.sleep(1)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.InvariantError)
    assert result.error.name == "must-stay-false"
    # The failing seed reproduces the same violation.
    with_replay = result.failing_seed
    assert with_replay is not None
    try:
        seedloop.replay(scenario, seed=with_replay)
    except seedloop.InvariantError as exc:
        assert exc.name == "must-stay-false"
    else:  # pragma: no cover - replay must re-raise
        raise AssertionError("replay did not reproduce the invariant violation")


def test_invariant_error_carries_the_violation_time() -> None:
    # InvariantError.time is a documented public attribute — it must be the virtual time of the
    # violation, not a placeholder.
    async def scenario(world: World) -> None:
        breached = {"v": False}
        world.always(lambda: not breached["v"], name="flips-at-1")
        await asyncio.sleep(1)
        breached["v"] = True  # false from the next step, which runs at t=1.0
        await asyncio.sleep(1)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.InvariantError)
    assert result.error.time == 1.0


def test_invariant_false_at_first_step_fires() -> None:
    async def scenario(world: World) -> None:
        world.always(lambda: False, name="never")
        await asyncio.sleep(1)  # the first step after registration checks and fires

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.InvariantError)
    assert result.error.name == "never"


def test_seed_dependent_invariant_is_found_by_sweep() -> None:
    # The invariant fails iff the seed's draw lands in a band — some seeds pass, some fail.
    async def scenario(world: World) -> None:
        value = world.rng.random()
        world.always(lambda: value < 0.8, name="value-in-range")
        await asyncio.sleep(0)

    result = seedloop.check(scenario, seeds=100, on_failure="return")
    assert isinstance(result.error, seedloop.InvariantError)
    assert result.failing_seed is not None


def test_raising_predicate_propagates() -> None:
    def boom() -> bool:
        raise ValueError("predicate blew up")

    async def scenario(world: World) -> None:
        world.always(boom, name="explodes")
        await asyncio.sleep(1)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, ValueError)


def test_first_failing_invariant_in_registration_order_is_reported() -> None:
    async def scenario(world: World) -> None:
        world.always(lambda: False, name="first")
        world.always(lambda: False, name="second")
        await asyncio.sleep(1)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.InvariantError)
    assert result.error.name == "first"


def test_invariant_is_exported() -> None:
    assert "InvariantError" in seedloop.__all__
    assert issubclass(seedloop.InvariantError, seedloop.SeedloopError)


class _NodeThatCleansUpOnCancel:
    # A node that holds the invariant during the run but breaks it in its cancel handler — the kind
    # of cleanup the Raft demo's nodes do. Checking must stop at teardown, not fire on this.
    def __init__(self) -> None:
        self.healthy = True

    async def run(self) -> None:
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.healthy = False  # mutate observed state during cancellation
            raise


def test_invariants_are_not_checked_during_teardown() -> None:
    # A scenario whose invariant holds for the whole run must pass, even though a started node
    # breaks it while being cancelled at teardown.
    node = _NodeThatCleansUpOnCancel()

    async def scenario(world: World) -> None:
        world.start(node)
        world.always(lambda: node.healthy, name="node-healthy")
        await asyncio.sleep(2)  # the node stays healthy throughout the logical run

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert result.failing_seed is None  # no spurious InvariantError from the cancel handler
    assert result.error is None
