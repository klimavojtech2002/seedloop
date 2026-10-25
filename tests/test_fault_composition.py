"""Faults compose: independent per-fault registries, and the replay of runs with faults."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from _sim import spam
from seedloop._run import _run_one
from seedloop._world import World


def test_fault_partitions_do_not_clobber_each_other_on_heal(
    make_world: Callable[[int], World],
) -> None:
    # Two independently-scheduled fault partitions, overlapping in id-space: one healing must not
    # restore reachability the other one is still cutting.
    world = make_world(0)
    world.net.bind(1)
    world.net.bind(2)
    world.net.bind(3)
    world.net._begin_fault_partition(10, [{1}, {2, 3}])
    world.net._begin_fault_partition(11, [{1, 2}, {3}])
    assert world.net._reachable(1, 2) is False  # cut by fault 10
    assert world.net._reachable(1, 3) is False  # cut by both
    assert world.net._reachable(2, 3) is False  # cut by fault 11

    world.net._end_fault_partition(10)
    assert world.net._reachable(1, 2) is True  # fault 10 gone, fault 11 doesn't cut this pair
    assert world.net._reachable(1, 3) is False  # fault 11 still active

    world.net._end_fault_partition(11)
    assert world.net._reachable(1, 3) is True
    assert world.net._reachable(2, 3) is True


def test_scenario_partition_and_fault_partition_are_independent(
    make_world: Callable[[int], World],
) -> None:
    world = make_world(0)
    for addr in (1, 2, 3, 4):
        world.net.bind(addr)
    world.net.partition({1}, {2})  # scenario-driven: cuts 1<->2 only
    world.net._begin_fault_partition(0, [{3}, {4}])  # fault-scheduled: cuts 3<->4 only
    assert world.net._reachable(1, 2) is False
    assert world.net._reachable(3, 4) is False
    assert world.net._reachable(1, 3) is True  # untouched by either

    world.net.heal()  # clears only the scenario partition
    assert world.net._reachable(1, 2) is True
    assert world.net._reachable(3, 4) is False  # the fault partition is unaffected

    world.net._end_fault_partition(0)
    assert world.net._reachable(3, 4) is True


def test_slow_link_factors_compound_on_the_same_pair_only(
    make_world: Callable[[int], World],
) -> None:
    world = make_world(0)
    world.net.bind(1)
    world.net.bind(2)
    world.net.bind(3)
    world.net._begin_slow_link(20, frozenset((1, 2)), 3.0)
    world.net._begin_slow_link(21, frozenset((1, 2)), 4.0)
    assert world.net._slow_factor(1, 2) == pytest.approx(12.0)  # compounds by multiplication
    assert world.net._slow_factor(1, 3) == pytest.approx(1.0)  # a different pair is untouched

    world.net._end_slow_link(20)
    assert world.net._slow_factor(1, 2) == pytest.approx(4.0)  # only fault 21 remains active

    world.net._end_slow_link(21)
    assert world.net._slow_factor(1, 2) == pytest.approx(1.0)


def test_replay_equivalence_across_all_three_fault_kinds() -> None:
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a, loss=0.1, duplicate=0.1) for a in (0, 1, 2, 3, 4)}
        world.always(lambda: True, name="always-true")
        tasks = [
            asyncio.ensure_future(spam(world, eps, 0, 1)),
            asyncio.ensure_future(spam(world, eps, 2, 3)),
        ]
        await world.run_for(
            seconds=2,
            faults=[world.partition(), world.slow_link(), world.crash(4)],
        )
        await asyncio.gather(*tasks)

    first = _run_one(scenario, 5)
    second = _run_one(scenario, 5)
    assert first == second  # includes the resolved fault parameters, not just the outcomes
