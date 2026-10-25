"""Seed-scheduled partitions: `world.partition()` as a `Fault` handle for `run_for`."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
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


def test_partition_unresolved_cuts_and_heals_reproducibly() -> None:
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1, 2, 3)}
        tasks = [
            asyncio.ensure_future(_spam(world, eps, 0, 1)),
            asyncio.ensure_future(_spam(world, eps, 2, 3)),
        ]
        await world.run_for(seconds=2, faults=[world.partition()])
        await asyncio.gather(*tasks)

    first = _run_one(scenario, 0)
    second = _run_one(scenario, 0)
    assert first == second  # same seed -> same split, same window
    kinds = _kinds(first)
    assert "fault-partition-begin" in kinds and "fault-partition-heal" in kinds
    assert "drop-partitioned" in kinds  # some crossed the cut
    assert "deliver" in kinds  # some got through (outside the window, or same-group)

    events = [cast("tuple[object, ...]", e) for e in first]
    (scheduled,) = [e for e in events if e[1] == "fault-partition-scheduled"]
    (begin,) = [e for e in events if e[1] == "fault-partition-begin"]
    (heal,) = [e for e in events if e[1] == "fault-partition-heal"]
    groups = cast("tuple[tuple[int, ...], ...]", scheduled[3])
    start, end = cast(float, scheduled[4]), cast(float, scheduled[5])
    # The window resolves within [0, seconds] (never before the call, never past it, ADR-0022) --
    # and the timers that actually fire land at exactly the recorded, resolved times (a
    # now+start/now-start mix-up in either the recording or the scheduling would show here).
    assert 0.0 <= start <= end <= 2.0
    assert begin[0] == pytest.approx(start)
    assert heal[0] == pytest.approx(end)
    # The resolved split is on the timeline too, not just its timing (ADR-0022) -- a genuine
    # two-way cut of the bound addresses, not an empty side.
    all_addrs = {addr for group in groups for addr in group}
    assert all_addrs == {0, 1, 2, 3}
    assert len(groups) == 2 and all(groups)


def test_partition_leaves_delivery_unaffected_outside_the_window() -> None:
    # The acceptance bar is stronger than "some deliver, some drop somewhere" (the aggregate test
    # above, which the *other, uncut* pair alone can satisfy): on the very same cut pair, a
    # message sent before the window starts or after it heals must deliver normally, not just one
    # sent during it. Also the regression guard for the window's end >= start guarantee -- a
    # mutant that lets end drift below start (so the partition never heals) still passes every
    # other test in this file.
    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        world.net.bind(2)

        async def sender() -> None:
            await a.send(2, "before")
            await asyncio.sleep(1.2)  # the window (seed=0, pinned groups) is [1.098, 2.366)
            await a.send(2, "during")
            await asyncio.sleep(1.3)
            await a.send(2, "after")
            await asyncio.sleep(0.5)

        task = asyncio.ensure_future(sender())
        await world.run_for(seconds=3, faults=[world.partition({1}, {2})])
        await task

    tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, 0)]
    kinds_by_mid = {e[2]: e[1] for e in tl if e[1] in ("deliver", "drop-partitioned")}
    assert kinds_by_mid == {0: "deliver", 1: "drop-partitioned", 2: "deliver"}


def test_bipartition_always_splits_exactly_two_addresses_both_ways(
    make_world: Callable[[int], World],
) -> None:
    # With only two bound addresses, a genuine split has exactly one address per side; the
    # re-draw-until-non-empty loop must never give up early (And instead of the intended check)
    # nor reject the case outright (an off-by-one on the "needs >= 2" guard).
    world = make_world(0)
    world.net.bind(1)
    world.net.bind(2)
    for _ in range(20):
        groups = world.net._draw_bipartition()
        assert len(groups) == 2
        assert groups[0] and groups[1]
        assert groups[0] | groups[1] == {1, 2}
        assert not (groups[0] & groups[1])


def test_two_partition_faults_get_independent_ids() -> None:
    async def scenario(world: World) -> None:
        for addr in (1, 2, 3, 4):
            world.net.bind(addr)
        await world.run_for(
            seconds=2, faults=[world.partition({1}, {2}), world.partition({3}, {4})]
        )

    tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, 0)]
    scheduled = [e for e in tl if e[1] == "fault-partition-scheduled"]
    assert len({e[2] for e in scheduled}) == 2  # two distinct fault ids, not both 0
    begins = {e[2]: e[0] for e in tl if e[1] == "fault-partition-begin"}
    heals = {e[2]: e[0] for e in tl if e[1] == "fault-partition-heal"}
    for _time, _kind, fault_id, _groups, start, end in scheduled:
        assert begins[fault_id] == pytest.approx(start)
        assert heals[fault_id] == pytest.approx(end)


def test_partition_pinned_groups_seed_still_chooses_window() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        await world.run_for(seconds=2, faults=[world.partition({1}, {2})])

    def _window(seed: int) -> tuple[float, float]:
        tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, seed)]
        (ev,) = [e for e in tl if e[1] == "fault-partition-scheduled"]
        return cast(float, ev[4]), cast(float, ev[5])

    assert _window(1) != _window(2)  # groups pinned, timing still seed-chosen


def test_partition_needs_two_bound_addresses() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)  # only one address bound
        with pytest.raises(SeedloopError, match="two bound addresses"):
            await world.run_for(seconds=1, faults=[world.partition()])

    seedloop.check(scenario, seeds=1)


def test_partition_rejects_overlapping_groups() -> None:
    # An address in two listed groups makes the cut ambiguous (_cut matches only the first
    # group containing an address, so "same listed group" would not mean "reachable").
    async def scenario(world: World) -> None:
        for addr in (1, 2, 3):
            world.net.bind(addr)
        with pytest.raises(SeedloopError, match="overlap"):
            await world.run_for(seconds=1, faults=[world.partition({1, 2}, {2, 3})])

    seedloop.check(scenario, seeds=1)


def test_partition_transport_rejects_a_bypassed_malformed_groups() -> None:
    # PartitionFault is a public dataclass; a caller could construct one directly with anything,
    # bypassing World.partition's own tuple(frozenset(g) for g in groups) construction.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        bad = seedloop.PartitionFault(groups=5)  # type: ignore[arg-type]
        with pytest.raises(SeedloopError, match="iterable of groups"):
            await world.run_for(seconds=1, faults=[bad])

    seedloop.check(scenario, seeds=1)


def test_partition_transport_rejects_a_bypassed_non_address_element() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)
        bad = seedloop.PartitionFault(groups=(frozenset({1}), frozenset({"23"})))  # type: ignore[arg-type]
        with pytest.raises(SeedloopError, match="only addresses"):
            await world.run_for(seconds=1, faults=[bad])

    seedloop.check(scenario, seeds=1)
