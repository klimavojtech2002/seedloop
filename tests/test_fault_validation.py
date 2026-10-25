"""Fault handles reject bad arguments before anything is scheduled."""

from __future__ import annotations

import asyncio
import dataclasses
from typing import cast

import pytest

import seedloop
from _sim import event_kinds
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.errors import SeedloopError


@pytest.mark.parametrize(
    "handle",
    [seedloop.PartitionFault(), seedloop.SlowLinkFault(), seedloop.CrashFault()],
    ids=["partition", "slow_link", "crash"],
)
def test_fault_handles_are_frozen(handle: seedloop.Fault) -> None:
    # Immutable value objects (docs/api.md, ADR-0022): a handle records what was pinned and does
    # nothing on its own, which only holds if it can't be mutated after construction.
    field_name = dataclasses.fields(handle)[0].name
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(handle, field_name, None)


def test_unrecognized_fault_type_raises_before_seconds_advances() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)
        before = world.now()
        with pytest.raises(SeedloopError, match="unrecognized fault handle"):
            await world.run_for(seconds=1, faults=[object()])  # type: ignore[list-item]
        assert world.now() == before  # nothing advanced; the invalid fault was rejected first

    seedloop.check(scenario, seeds=1)


def test_a_later_invalid_fault_leaves_an_earlier_valid_one_uncommitted() -> None:
    # run_for validates every fault before committing any of them (ADR-0022): an invalid fault
    # later in the list must not leave an earlier, valid one already resolved and scheduled.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        with pytest.raises(SeedloopError, match="unrecognized fault handle"):
            await world.run_for(seconds=1, faults=[world.partition(), object()])  # type: ignore[list-item]
        world.record("after-raise")

    kinds = event_kinds(_run_one(scenario, 0))
    assert kinds == ["after-raise"]  # no fault-partition-scheduled/begin ever recorded


def test_fault_resolution_is_independent_of_bind_order() -> None:
    # _bound_addresses() sorts before drawing specifically so a seed's resolved target does not
    # depend on the order nodes happened to bind in.
    def scenario(order: tuple[int, ...]) -> seedloop.Scenario:
        async def run(world: World) -> None:
            for addr in order:
                world.net.bind(addr)
            await world.run_for(seconds=1, faults=[world.crash(), world.slow_link()])

        return run

    ascending = [cast("tuple[object, ...]", e) for e in _run_one(scenario((1, 2, 3, 4)), 0)]
    descending = [cast("tuple[object, ...]", e) for e in _run_one(scenario((4, 3, 2, 1)), 0)]
    crash_a = next(e for e in ascending if e[1] == "fault-crash-scheduled")
    crash_d = next(e for e in descending if e[1] == "fault-crash-scheduled")
    slow_a = next(e for e in ascending if e[1] == "fault-slow-link-scheduled")
    slow_d = next(e for e in descending if e[1] == "fault-slow-link-scheduled")
    assert crash_a[3] == crash_d[3]  # same resolved crash node regardless of bind order
    assert (slow_a[3], slow_a[4]) == (slow_d[3], slow_d[4])  # same resolved pair


def test_faults_list_order_is_part_of_the_runs_identity() -> None:
    # Resolution draws sequentially from one shared "faults" stream in list order (ADR-0022): the
    # same seed with the same two unresolved handles in a different order resolves differently.
    def scenario(order: str) -> seedloop.Scenario:
        async def run(world: World) -> None:
            for addr in (1, 2, 3, 4, 5):
                world.net.bind(addr)
            faults = (
                [world.crash(), world.partition()]
                if order == "crash-first"
                else [world.partition(), world.crash()]
            )
            await world.run_for(seconds=1, faults=faults)

        return run

    crash_first = [cast("tuple[object, ...]", e) for e in _run_one(scenario("crash-first"), 0)]
    partition_first = [
        cast("tuple[object, ...]", e) for e in _run_one(scenario("partition-first"), 0)
    ]
    node_a = next(e for e in crash_first if e[1] == "fault-crash-scheduled")[3]
    node_b = next(e for e in partition_first if e[1] == "fault-crash-scheduled")[3]
    assert node_a != node_b


def test_crash_takes_precedence_over_a_concurrent_partition_cut() -> None:
    # A message can be both crossing an active partition and touching a crashed node; the drop
    # reason must be drop-crashed (permanent), not drop-partitioned (heals), so a debugger isn't
    # pointed at the wrong cause (a mutation of this exact precedence must fail a test).
    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        world.net.bind(2)
        world.net.partition({1}, {2})  # scenario partition also cuts this pair
        await world.run_for(seconds=1, faults=[world.crash(1, at=0)])
        await a.send(2, "msg")
        await asyncio.sleep(0.5)

    kinds = event_kinds(_run_one(scenario, 0))
    assert "drop-crashed" in kinds
    assert "drop-partitioned" not in kinds


def test_partition_heal_does_not_resurrect_an_overlapping_crashed_node() -> None:
    # Plan §2's named composition case: fault A (partition, seed=0 window [0.732, 1.578)) and
    # fault B (crash, node 2 at t=0.1) overlap -- A's heal must not restore reachability to B's
    # crashed node. A message sent well after the heal must still show drop-crashed.
    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        world.net.bind(2)
        world.net.bind(3)

        async def sender() -> None:
            await asyncio.sleep(1.6)  # after the partition has healed (~1.578)
            await a.send(2, "post-heal")

        task = asyncio.ensure_future(sender())
        await world.run_for(
            seconds=2, faults=[world.partition({1}, {2, 3}), world.crash(2, at=0.1)]
        )
        await task

    kinds = event_kinds(_run_one(scenario, 0))
    assert "fault-partition-heal" in kinds
    assert kinds[-1] == "drop-crashed"  # the post-heal send, still cut by the unrelated crash


def test_run_for_zero_seconds_with_faults_is_not_an_error() -> None:
    # Every window collapses to the instant of the call; begin/end fire in registration order,
    # so nothing is ever observably active -- not an error.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        await world.run_for(seconds=0, faults=[world.partition(), world.slow_link(), world.crash()])

    kinds = event_kinds(_run_one(scenario, 0))
    assert "fault-crash" in kinds  # ran to completion, no exception


def test_partition_pinned_group_naming_a_never_bound_address_is_allowed() -> None:
    # Symmetric with crash's "born dead" case: a pinned group may name an address that is never
    # bound in this run.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        await world.run_for(seconds=1, faults=[world.partition({99}, {1})])

    seedloop.check(scenario, seeds=1)  # must not raise
