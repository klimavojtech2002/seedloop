"""The Phase-1 guarantee: same seed -> same timeline."""

from __future__ import annotations

import asyncio

import seedloop
from seedloop._run import _run_one
from seedloop._world import World


async def _mixed_scenario(world: World) -> None:
    # A scenario exercising the full Phase-1 surface: RNG draws, sleeps, and concurrency.
    async def worker(name: str) -> None:
        for _ in range(3):
            delay = world.rng.randint(1, 5)
            await asyncio.sleep(delay)
            world.record((name, world.now(), world.rng.random()))

    await asyncio.gather(worker("a"), worker("b"))


def test_same_seed_same_timeline() -> None:
    first = _run_one(_mixed_scenario, 12345)
    second = _run_one(_mixed_scenario, 12345)
    assert first == second
    assert len(first) == 6  # two workers x three records — the scenario actually ran


def test_replay_reproduces_identically_many_times() -> None:
    baseline = _run_one(_mixed_scenario, 999)
    for _ in range(50):
        assert _run_one(_mixed_scenario, 999) == baseline


def test_record_stamps_the_virtual_time() -> None:
    # The same payload recorded at two different virtual times must land at those times in the
    # timeline, so a timing regression is visible in the trace.
    async def scenario(world: World) -> None:
        world.record("tick")
        await asyncio.sleep(5)
        world.record("tick")

    timeline = _run_one(scenario, 1)
    assert timeline == ((0.0, "tick"), (5.0, "tick"))


def test_different_seeds_can_differ() -> None:
    # The seed has an observable effect (via world.rng); otherwise the guarantee would be vacuous.
    timelines = {tuple(_run_one(_mixed_scenario, s)) for s in range(10)}
    assert len(timelines) > 1


def test_cross_seed_isolation() -> None:
    # Run A, then B, then A again: A's two timelines must match. Catches state bleeding between
    # runs (a hidden global), which would look like nondeterminism.
    a_first = _run_one(_mixed_scenario, 7)
    _run_one(_mixed_scenario, 8)
    a_second = _run_one(_mixed_scenario, 7)
    assert a_first == a_second


def test_secrets_are_seeded_within_a_run() -> None:
    # The CSPRNG shim is installed per run, so secrets/os.urandom are reproducible from the seed.
    import secrets

    async def scenario(world: World) -> None:
        world.record(secrets.token_bytes(8))

    assert _run_one(scenario, 42) == _run_one(scenario, 42)


def test_replay_public_entry_runs() -> None:
    ran: list[int] = []

    async def scenario(world: World) -> None:
        ran.append(world.seed)

    seedloop.replay(scenario, seed=5)
    assert ran == [5]
