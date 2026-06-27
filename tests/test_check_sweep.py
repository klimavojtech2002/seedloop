"""check() sweeps seeds and reports the first failure (slice 0130)."""

from __future__ import annotations

import pytest

import seedloop
from seedloop._world import World


async def _always_passes(world: World) -> None:
    world.record(world.rng.random())


def test_all_seeds_pass() -> None:
    result = seedloop.check(_always_passes, seeds=20, on_failure="return")
    assert result.checked == 20
    assert result.failing_seed is None
    assert result.error is None


def test_sweep_finds_the_failing_seed() -> None:
    # Fails iff the seed's first user draw lands in a band; check reports the first such seed.
    async def scenario(world: World) -> None:
        assert world.rng.random() < 0.9, "unlucky draw"

    result = seedloop.check(scenario, seeds=200, on_failure="return")
    assert result.failing_seed is not None
    assert isinstance(result.error, AssertionError)

    # The reported seed is the reproduction: replaying it raises the same failure, every time.
    for _ in range(10):
        with pytest.raises(AssertionError, match="unlucky draw"):
            seedloop.replay(scenario, seed=result.failing_seed)


def test_explicit_seed_iterable() -> None:
    seen: list[int] = []

    async def scenario(world: World) -> None:
        seen.append(world.seed)

    result = seedloop.check(scenario, seeds=[3, 9, 27], on_failure="return")
    assert result.checked == 3
    assert seen == [3, 9, 27]


def test_on_failure_raise_tags_the_seed() -> None:
    async def scenario(world: World) -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError) as exc_info:
        seedloop.check(scenario, seeds=5)
    # The raised error carries the failing seed so the user can replay it.
    assert any("failing seed=" in note for note in exc_info.value.__notes__)


def test_deadlock_surfaces_as_failure_not_hang() -> None:
    async def scenario(world: World) -> None:
        await world._loop.create_future()  # never resolved, nothing scheduled -> deadlock

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.DeadlockError)


class _CrashingNode:
    async def run(self) -> None:
        raise ValueError("node crashed")


class _QuietNode:
    def __init__(self) -> None:
        self.ran = False

    async def run(self) -> None:
        self.ran = True


def test_started_node_failure_is_reported() -> None:
    # A node started with world.start that raises must fail the run, not be silently orphaned.
    import asyncio

    async def scenario(world: World) -> None:
        world.start(_CrashingNode())
        await asyncio.sleep(0)  # let the node run and crash

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, ValueError)
    assert result.failing_seed == 0


def test_started_node_runs() -> None:
    import asyncio

    node = _QuietNode()

    async def scenario(world: World) -> None:
        world.start(node)
        await asyncio.sleep(0)

    seedloop.replay(scenario, seed=1)
    assert node.ran
