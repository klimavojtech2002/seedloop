"""Seed-scheduled fault handles: `partition`/`slow_link`/`crash` + `run_for(faults=...)` (0470)."""

from __future__ import annotations

import asyncio
import dataclasses
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


def _pair_latencies(timeline: Sequence[object], src: int, dst: int) -> list[float]:
    events = [cast("tuple[object, ...]", e) for e in timeline]
    sent_at = {e[2]: e[0] for e in events if e[1] == "send" and e[3] == src and e[4] == dst}
    return [
        cast(float, e[0]) - cast(float, sent_at[e[2]])
        for e in events
        if e[1] == "deliver" and e[2] in sent_at
    ]


# --- partition ---------------------------------------------------------------------------------


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


def test_bipartition_always_splits_exactly_two_addresses_both_ways() -> None:
    # With only two bound addresses, a genuine split has exactly one address per side; the
    # re-draw-until-non-empty loop must never give up early (And instead of the intended check)
    # nor reject the case outright (an off-by-one on the "needs >= 2" guard).
    world = World(0)
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


# --- slow_link -----------------------------------------------------------------------------


def test_slow_link_unresolved_multiplies_latency_reproducibly() -> None:
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1, 2, 3)}
        tasks = [
            asyncio.ensure_future(_spam(world, eps, 0, 1)),
            asyncio.ensure_future(_spam(world, eps, 2, 3)),
        ]
        await world.run_for(seconds=2, faults=[world.slow_link()])
        await asyncio.gather(*tasks)

    first = _run_one(scenario, 0)
    second = _run_one(scenario, 0)
    assert first == second
    lat = _pair_latencies(first, 0, 1) + _pair_latencies(first, 2, 3)
    assert max(lat) > 0.020  # beyond the ordinary [0.001, 0.020] draw range


def test_slow_link_pinned_pair_scopes_to_that_pair_only() -> None:
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1, 2, 3)}
        tasks = [
            asyncio.ensure_future(_spam(world, eps, 0, 1)),
            asyncio.ensure_future(_spam(world, eps, 2, 3)),
        ]
        await world.run_for(seconds=2, faults=[world.slow_link(0, 1)])
        await asyncio.gather(*tasks)

    tl = _run_one(scenario, 1)
    assert max(_pair_latencies(tl, 0, 1)) > 0.020  # the pinned pair is slowed
    assert max(_pair_latencies(tl, 2, 3)) <= 0.020  # the other pair is untouched

    events = [cast("tuple[object, ...]", e) for e in tl]
    (scheduled,) = [e for e in events if e[1] == "fault-slow-link-scheduled"]
    (begin,) = [e for e in events if e[1] == "fault-slow-link-begin"]
    (end_ev,) = [e for e in events if e[1] == "fault-slow-link-end"]
    a, b = scheduled[3], scheduled[4]
    start, end = cast(float, scheduled[6]), cast(float, scheduled[7])
    assert (a, b) == (0, 1)
    # The window resolves within [0, seconds], and the timers that actually fire land at exactly
    # the recorded, resolved times (a now+start/now-start mix-up in either the recording or the
    # actual scheduling would show here).
    assert 0.0 <= start <= end <= 2.0
    assert begin[0] == pytest.approx(start)
    assert end_ev[0] == pytest.approx(end)


def test_slow_link_leaves_latency_unaffected_outside_the_window() -> None:
    # The acceptance bar is stronger than "off-pair traffic is untouched" (the test above): on the
    # very same pair, a message sent before the window starts or after it ends must see ordinary
    # latency, not just one sent during it.
    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        world.net.bind(2)

        async def sender() -> None:
            await a.send(2, "before")
            await asyncio.sleep(0.19)  # the window (seed=5) is [0.163, 0.212)
            await a.send(2, "during")
            await asyncio.sleep(2.5)
            await a.send(2, "after")
            await asyncio.sleep(0.5)

        task = asyncio.ensure_future(sender())
        await world.run_for(seconds=3, faults=[world.slow_link(1, 2)])
        await task

    tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, 5)]
    sent_at = {cast(int, e[2]): cast(float, e[0]) for e in tl if e[1] == "send"}
    latency: dict[int, float] = {}
    for e in tl:
        if e[1] == "deliver":
            mid = cast(int, e[2])
            latency[mid] = cast(float, e[0]) - sent_at[mid]
    assert len(latency) == 3
    before, during, after = latency[0], latency[1], latency[2]
    assert before <= 0.020
    assert during > 0.020
    assert after <= 0.020


def test_slow_link_pinned_a_resolves_b_from_bound_addresses() -> None:
    # Only `a` pinned: the seed must still resolve `b`, and the effect must land on the resolved
    # pair, not on an unrelated one and not silently no-op with b left unset.
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1, 2)}
        tasks = [
            asyncio.ensure_future(_spam(world, eps, 0, 1)),
            asyncio.ensure_future(_spam(world, eps, 0, 2)),
        ]
        await world.run_for(seconds=2, faults=[world.slow_link(a=0)])
        await asyncio.gather(*tasks)

    tl = _run_one(scenario, 1)
    slowed = max(_pair_latencies(tl, 0, 1)) > 0.020
    untouched = max(_pair_latencies(tl, 0, 2)) > 0.020
    assert slowed != untouched  # exactly one resolved pair is affected, never neither/both


def test_slow_link_pinned_b_resolves_a_from_bound_addresses() -> None:
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1, 2)}
        tasks = [
            asyncio.ensure_future(_spam(world, eps, 1, 0)),
            asyncio.ensure_future(_spam(world, eps, 2, 0)),
        ]
        await world.run_for(seconds=2, faults=[world.slow_link(b=0)])
        await asyncio.gather(*tasks)

    tl = _run_one(scenario, 0)
    slowed = max(_pair_latencies(tl, 1, 0)) > 0.020
    untouched = max(_pair_latencies(tl, 2, 0)) > 0.020
    assert slowed != untouched


def test_slow_link_pinned_b_never_resolves_a_to_the_same_address() -> None:
    # A single seed is not enough to prove this: drawing `a` from every bound address without
    # excluding the pinned `b` collapses the pair on a fraction of seeds (silently inert, since
    # frozenset((a, b)) then never matches a real (src, dst) pair) -- proven wrong for slice 0470
    # by an independent audit. Swept across many seeds, not just one.
    async def scenario(world: World) -> None:
        world.net.bind(0)
        world.net.bind(1)
        world.net.bind(2)
        await world.run_for(seconds=1, faults=[world.slow_link(b=0)])

    for seed in range(40):
        tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, seed)]
        (scheduled,) = [e for e in tl if e[1] == "fault-slow-link-scheduled"]
        assert scheduled[3] != scheduled[4], f"seed={seed}: a == b == {scheduled[3]!r}"


def test_slow_link_works_with_exactly_two_bound_addresses() -> None:
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1)}
        task = asyncio.ensure_future(_spam(world, eps, 0, 1))
        await world.run_for(seconds=2, faults=[world.slow_link()])
        await task

    tl = _run_one(scenario, 0)
    assert max(_pair_latencies(tl, 0, 1)) > 0.020


def test_two_slow_link_faults_get_independent_ids() -> None:
    async def scenario(world: World) -> None:
        for addr in (1, 2, 3, 4):
            world.net.bind(addr)
        await world.run_for(
            seconds=2,
            faults=[world.slow_link(1, 2, factor=5.0), world.slow_link(3, 4, factor=8.0)],
        )

    tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, 0)]
    scheduled = [e for e in tl if e[1] == "fault-slow-link-scheduled"]
    assert len({e[2] for e in scheduled}) == 2  # two distinct fault ids, not both 0
    assert {e[5] for e in scheduled} == {5.0, 8.0}  # each factor kept its own value


def test_two_crash_faults_get_independent_ids() -> None:
    async def scenario(world: World) -> None:
        for addr in (1, 2, 3, 4):
            world.net.bind(addr)
        await world.run_for(seconds=1, faults=[world.crash(1), world.crash(2)])

    tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, 0)]
    scheduled = [e for e in tl if e[1] == "fault-crash-scheduled"]
    assert len({e[2] for e in scheduled}) == 2  # two distinct fault ids, not both 0
    assert {e[3] for e in scheduled} == {1, 2}  # each kept its own pinned node


def test_two_crash_faults_on_the_same_node_are_idempotent() -> None:
    # Plan's own edge case: two handles resolving to the same node is not an error.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        await world.run_for(seconds=1, faults=[world.crash(1), world.crash(1)])

    seedloop.check(scenario, seeds=1)  # must not raise


def test_slow_link_pinned_valid_factor_is_accepted_and_applied() -> None:
    # A pinned, valid factor must not be rejected (guards against the eager check in
    # World.slow_link misfiring on well-formed input) and must actually apply.
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1)}
        task = asyncio.ensure_future(_spam(world, eps, 0, 1))
        await world.run_for(seconds=2, faults=[world.slow_link(0, 1, factor=5.0)])
        await task

    tl = _run_one(scenario, 0)
    assert max(_pair_latencies(tl, 0, 1)) > 0.020


def test_slow_link_multiplies_duplicate_latency_too() -> None:
    # duplicate=1.0 delivers every message twice; both copies of a message sent during the
    # fault's active window must be slowed, not just the first (the "original" latency and the
    # duplicate's are drawn and multiplied separately, ADR-0022).
    async def scenario(world: World) -> None:
        a = world.net.bind(0, duplicate=1.0)
        world.net.bind(1)
        task = asyncio.ensure_future(_spam(world, {0: a}, 0, 1, count=20, interval=0.1))
        await world.run_for(seconds=2, faults=[world.slow_link(0, 1, factor=10.0)])
        await task

    tl = [cast("tuple[object, ...]", e) for e in _run_one(scenario, 0)]
    sent_at = {cast(int, e[2]): cast(float, e[0]) for e in tl if e[1] == "send"}
    # A baseline (unslowed) latency is at most 0.020; a slowed one can still land under that on a
    # small draw, so pairing exact deliveries is noisy. Instead: bucket the two deliveries per mid
    # into "first-seen" (the original) and "second-seen" (the duplicate) and check each population
    # independently — proving the multiplier reaches both call sites (_send's original latency and
    # its separate duplicate latency, ADR-0022), not just one of them.
    seen_mid: set[int] = set()
    originals: list[float] = []
    duplicates: list[float] = []
    for e in tl:
        if e[1] == "deliver":
            mid = cast(int, e[2])
            lat = cast(float, e[0]) - sent_at[mid]
            (duplicates if mid in seen_mid else originals).append(lat)
            seen_mid.add(mid)
    assert originals and duplicates
    assert max(originals) > 0.021
    assert max(duplicates) > 0.021


def test_slow_link_factor_boundary_rejects_exactly_one() -> None:
    async def scenario(world: World) -> None:
        with pytest.raises(SeedloopError, match="factor"):
            world.slow_link(factor=1.0)  # must be strictly greater than 1.0

    seedloop.check(scenario, seeds=1)


def test_slow_link_transport_rejects_a_bypassed_invalid_factor() -> None:
    # SlowLinkFault is a public dataclass; a caller could construct one directly, bypassing
    # World.slow_link's own validation. The Transport must re-check when it schedules the fault.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        bad = seedloop.SlowLinkFault(1, 2, 1.0)  # boundary: exactly 1.0, not > 1.0
        with pytest.raises(SeedloopError, match="factor"):
            await world.run_for(seconds=1, faults=[bad])

    seedloop.check(scenario, seeds=1)


def test_slow_link_transport_rejects_a_bypassed_non_address_a() -> None:
    # Same bypass concern as the factor/a==b checks: a non-address a would never match a real
    # (src, dst) pair, so the fault would schedule and record on the timeline without ever
    # slowing anything real -- the same silently-inert failure mode the a==b check already guards.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        bad = seedloop.SlowLinkFault("not-an-address", 2, 5.0)  # type: ignore[arg-type]
        with pytest.raises(SeedloopError, match="must be an address"):
            await world.run_for(seconds=1, faults=[bad])

    seedloop.check(scenario, seeds=1)


def test_slow_link_transport_rejects_a_bypassed_non_address_b() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        bad = seedloop.SlowLinkFault(1, "not-an-address", 5.0)  # type: ignore[arg-type]
        with pytest.raises(SeedloopError, match="must be an address"):
            await world.run_for(seconds=1, faults=[bad])

    seedloop.check(scenario, seeds=1)


def test_slow_link_transport_rejects_a_bypassed_a_equals_b() -> None:
    # Same bypass concern as the factor check: a == b would otherwise silently no-op (the pair
    # never matches a real (src, dst)) instead of being rejected.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        bad = seedloop.SlowLinkFault(1, 1, 5.0)
        with pytest.raises(SeedloopError, match="distinct"):
            await world.run_for(seconds=1, faults=[bad])

    seedloop.check(scenario, seeds=1)


def test_slow_link_factor_must_be_greater_than_one() -> None:
    async def scenario(world: World) -> None:
        with pytest.raises(SeedloopError, match="factor"):
            world.slow_link(factor=0.5)

    seedloop.check(scenario, seeds=1)


def test_slow_link_a_equals_b_rejected() -> None:
    async def scenario(world: World) -> None:
        with pytest.raises(SeedloopError, match="distinct"):
            world.slow_link(1, 1)

    seedloop.check(scenario, seeds=1)


def test_slow_link_needs_two_bound_addresses() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)
        with pytest.raises(SeedloopError, match="two bound addresses"):
            await world.run_for(seconds=1, faults=[world.slow_link()])

    seedloop.check(scenario, seeds=1)


def test_slow_link_one_pinned_needs_a_distinct_bound_address() -> None:
    # With only one of a/b left for the seed to resolve, only a candidate distinct from the
    # pinned side is required -- not "two bound addresses overall". Here the pinned side (a=1)
    # is itself the only bound address, so no candidate for b exists either way.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        with pytest.raises(SeedloopError, match="distinct from the pinned"):
            await world.run_for(seconds=1, faults=[world.slow_link(a=1)])

    seedloop.check(scenario, seeds=1)


def test_slow_link_pinned_b_one_bound_needs_a_distinct_bound_address() -> None:
    # Mirror of the test above, for the b-pinned/a-unresolved direction.
    async def scenario(world: World) -> None:
        world.net.bind(1)  # only bound address is the pinned b itself
        with pytest.raises(SeedloopError, match="distinct from the pinned"):
            await world.run_for(seconds=1, faults=[world.slow_link(b=1)])

    seedloop.check(scenario, seeds=1)


def test_slow_link_one_pinned_unbound_needs_only_one_other_bound_address() -> None:
    # The pinned side need not be bound at all: a single OTHER bound address is enough for the
    # seed to resolve the unpinned side, since it can never collide with the pinned one.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        await world.run_for(seconds=1, faults=[world.slow_link(a=99)])

    seedloop.check(scenario, seeds=1)


# --- crash -----------------------------------------------------------------------------------


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


# --- composition / registry independence (white-box: ADR-0022) -----------------------------


def test_fault_partitions_do_not_clobber_each_other_on_heal() -> None:
    # Two independently-scheduled fault partitions, overlapping in id-space: one healing must not
    # restore reachability the other one is still cutting.
    world = World(0)
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


def test_scenario_partition_and_fault_partition_are_independent() -> None:
    world = World(0)
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


def test_slow_link_factors_compound_on_the_same_pair_only() -> None:
    world = World(0)
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


# --- validation ------------------------------------------------------------------------------


def test_fault_handles_are_frozen() -> None:
    # Immutable value objects (docs/api.md, ADR-0022): a handle records what was pinned and does
    # nothing on its own, which only holds if it can't be mutated after construction.
    for handle in (
        seedloop.PartitionFault(),
        seedloop.SlowLinkFault(),
        seedloop.CrashFault(),
    ):
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

    kinds = _kinds(_run_one(scenario, 0))
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
    # pointed at the wrong cause (the plan's own devil's-advocate/mutation-target list names this
    # exact precedence as load-bearing).
    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        world.net.bind(2)
        world.net.partition({1}, {2})  # scenario partition also cuts this pair
        await world.run_for(seconds=1, faults=[world.crash(1, at=0)])
        await a.send(2, "msg")
        await asyncio.sleep(0.5)

    kinds = _kinds(_run_one(scenario, 0))
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

    kinds = _kinds(_run_one(scenario, 0))
    assert "fault-partition-heal" in kinds
    assert kinds[-1] == "drop-crashed"  # the post-heal send, still cut by the unrelated crash


def test_run_for_zero_seconds_with_faults_is_not_an_error() -> None:
    # Every window collapses to the instant of the call; begin/end fire in registration order,
    # so nothing is ever observably active -- not an error (plan's own edge case).
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        await world.run_for(seconds=0, faults=[world.partition(), world.slow_link(), world.crash()])

    kinds = _kinds(_run_one(scenario, 0))
    assert "fault-crash" in kinds  # ran to completion, no exception


def test_partition_pinned_group_naming_a_never_bound_address_is_allowed() -> None:
    # Symmetric with crash's "born dead" case: a pinned group may name an address that is never
    # bound in this run.
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(2)
        await world.run_for(seconds=1, faults=[world.partition({99}, {1})])

    seedloop.check(scenario, seeds=1)  # must not raise


# --- replay equivalence ------------------------------------------------------------------------


def test_replay_equivalence_across_all_three_fault_kinds() -> None:
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a, loss=0.1, duplicate=0.1) for a in (0, 1, 2, 3, 4)}
        world.always(lambda: True, name="always-true")
        tasks = [
            asyncio.ensure_future(_spam(world, eps, 0, 1)),
            asyncio.ensure_future(_spam(world, eps, 2, 3)),
        ]
        await world.run_for(
            seconds=2,
            faults=[world.partition(), world.slow_link(), world.crash(4)],
        )
        await asyncio.gather(*tasks)

    first = _run_one(scenario, 5)
    second = _run_one(scenario, 5)
    assert first == second  # includes the resolved fault parameters, not just the outcomes
