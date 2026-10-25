"""Seed-scheduled crashes: `world.crash()` as a `Fault` handle for `run_for`."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import cast

import pytest

import seedloop
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.errors import SeedloopError


def _kinds(timeline: Sequence[object]) -> list[str]:
    return [str(cast("tuple[object, ...]", e)[1]) for e in timeline]  # the event-kind field


async def _spam(
    world: World,
    eps: dict[int, seedloop.Endpoint],
    src: int,
    dst: int,
    *,
    count: int = 40,
    interval: float = 0.05,
) -> None:
    for i in range(count):
        await eps[src].send(dst, i)
        await asyncio.sleep(interval)


def test_crash_unresolved_is_reproducible() -> None:
    # A fully unresolved crash (both node and timing seed-chosen) resolves and takes effect
    # identically across two runs. Both directions being cut is proven elsewhere, on a pinned
    # node: test_crash_leaves_delivery_unaffected_strictly_before_at (the crashed sender) and
    # test_crash_no_restart_across_a_later_run_for_call (the crashed receiver).
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1, 2, 3)}
        tasks = [
            asyncio.ensure_future(_spam(world, eps, 0, 1)),
            asyncio.ensure_future(_spam(world, eps, 2, 3)),
        ]
        await world.run_for(seconds=2, faults=[world.crash()])
        await asyncio.gather(*tasks)

    first = _run_one(scenario, 0)
    second = _run_one(scenario, 0)
    assert first == second
    kinds = _kinds(first)
    assert "fault-crash" in kinds
    assert "drop-crashed" in kinds
    assert "deliver" in kinds  # traffic before the crash, or on the untouched pair, got through


def test_crash_no_restart_across_a_later_run_for_call() -> None:
    delivered: list[object] = []

    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        b = world.net.bind(2)

        async def receiver() -> None:
            while True:
                delivered.append(await b.recv())

        task = asyncio.ensure_future(receiver())
        # Crash node 2 immediately, fully pinned -> deterministic regardless of seed.
        await world.run_for(seconds=1, faults=[world.crash(2, at=0)])
        await a.send(2, "after crash, same run_for")
        await asyncio.sleep(0.5)
        # A second, unrelated run_for call: crash must still hold, with no fault list at all.
        await world.run_for(seconds=1)
        await a.send(2, "after crash, later run_for")
        await asyncio.sleep(0.5)
        task.cancel()

    seedloop.replay(scenario, seed=1)
    assert delivered == []  # nothing ever reached the crashed node
    kinds = _kinds(_run_one(scenario, 1))
    assert kinds.count("drop-crashed") == 2


def test_crash_pinned_node_before_it_is_bound() -> None:
    # A "born dead" node: crashed before it ever binds an endpoint, still cut off once it does.
    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        await world.run_for(seconds=1, faults=[world.crash(99, at=0)])
        world.net.bind(99)
        await a.send(99, "into the void")
        await asyncio.sleep(0.5)

    kinds = _kinds(_run_one(scenario, 1))
    assert "drop-crashed" in kinds


def test_crash_at_outside_seconds_raises() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)
        with pytest.raises(SeedloopError, match="exceeds"):
            await world.run_for(seconds=1, faults=[world.crash(1, at=5)])

    seedloop.check(scenario, seeds=1)


def test_crash_at_just_past_seconds_raises() -> None:
    # test_crash_at_outside_seconds_raises above uses at=5, far past seconds=1, which a loosened
    # bound (e.g. > seconds + 1) would still catch -- this pins the boundary itself.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        with pytest.raises(SeedloopError, match="exceeds"):
            await world.run_for(seconds=1, faults=[world.crash(1, at=1.5)])

    seedloop.check(scenario, seeds=1)


def test_crash_at_exactly_seconds_is_accepted() -> None:
    # The boundary: at == seconds is inside [0, seconds], not "exceeds" it.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        await world.run_for(seconds=1, faults=[world.crash(1, at=1)])

    seedloop.check(scenario, seeds=1)


def test_crash_negative_at_rejected_immediately() -> None:
    async def scenario(world: World) -> None:
        with pytest.raises(SeedloopError, match="finite and >= 0"):
            world.crash(at=-1)

    seedloop.check(scenario, seeds=1)


def test_crash_transport_rejects_a_bypassed_negative_at() -> None:
    # CrashFault is a public dataclass; a caller could construct one directly, bypassing
    # World.crash's own validation. The Transport must re-check when it schedules the fault.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        bad = seedloop.CrashFault(1, -1.0)
        with pytest.raises(SeedloopError, match="finite and >= 0"):
            await world.run_for(seconds=1, faults=[bad])

    seedloop.check(scenario, seeds=1)


def test_crash_transport_rejects_a_bypassed_non_address_node() -> None:
    # Same bypass concern as slow_link's non-address checks: a non-address node would never
    # match a real bound address, so the crash would schedule and record on the timeline
    # without ever cutting anything real.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        bad = seedloop.CrashFault("not-an-address", 0.0)  # type: ignore[arg-type]
        with pytest.raises(SeedloopError, match="must be an address"):
            await world.run_for(seconds=1, faults=[bad])

    seedloop.check(scenario, seeds=1)


def test_crash_fires_at_exactly_the_pinned_at() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        await world.run_for(seconds=1, faults=[world.crash(1, at=0.4)])

    tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, 0)]
    (scheduled,) = [e for e in tl if e[1] == "fault-crash-scheduled"]
    (crashed,) = [e for e in tl if e[1] == "fault-crash"]
    assert scheduled[3] == 1  # the pinned node
    assert scheduled[4] == pytest.approx(0.4)  # now + at, not now - at
    assert crashed[0] == pytest.approx(0.4)  # fires at the resolved time, not immediately
    assert crashed[2] == scheduled[2]  # same fault id on both events


def test_crash_leaves_delivery_unaffected_strictly_before_at() -> None:
    # The acceptance bar is stronger than "some deliveries happen somewhere": a message from the
    # crashed node sent before its own `at` must be delivered normally, and one sent after must be
    # dropped -- proven on the crashed node's own traffic, not inferred from an unrelated pair.
    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        world.net.bind(2)

        async def sender() -> None:
            await a.send(2, "before")
            await asyncio.sleep(0.6)  # crash fires at t=0.4, well before this send
            await a.send(2, "after")

        task = asyncio.ensure_future(sender())
        await world.run_for(seconds=1, faults=[world.crash(1, at=0.4)])
        await task

    tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, 0)]
    kinds_by_mid = {e[2]: e[1] for e in tl if e[1] in ("deliver", "drop-crashed")}
    assert kinds_by_mid == {0: "deliver", 1: "drop-crashed"}


def test_crash_needs_a_bound_address() -> None:
    async def scenario(world: World) -> None:
        with pytest.raises(SeedloopError, match="one bound address"):
            await world.run_for(seconds=1, faults=[world.crash()])

    seedloop.check(scenario, seeds=1)
