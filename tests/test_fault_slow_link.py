"""Seed-scheduled slow links: `world.slow_link()` as a `Fault` handle for `run_for`."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import cast

import pytest

import seedloop
from _sim import spam, window
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.errors import SeedloopError


def _pair_latencies(timeline: Sequence[object], src: int, dst: int) -> list[float]:
    events = [cast("tuple[object, ...]", e) for e in timeline]
    sent_at = {e[2]: e[0] for e in events if e[1] == "send" and e[3] == src and e[4] == dst}
    return [
        cast(float, e[0]) - cast(float, sent_at[e[2]])
        for e in events
        if e[1] == "deliver" and e[2] in sent_at
    ]


def test_slow_link_unresolved_multiplies_latency_reproducibly() -> None:
    async def scenario(world: World) -> None:
        eps = {a: world.net.bind(a) for a in (0, 1, 2, 3)}
        tasks = [
            asyncio.ensure_future(spam(world, eps, 0, 1)),
            asyncio.ensure_future(spam(world, eps, 2, 3)),
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
            asyncio.ensure_future(spam(world, eps, 0, 1)),
            asyncio.ensure_future(spam(world, eps, 2, 3)),
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
    # latency, not just one sent during it. The window is the seed's choice, so a first run without
    # any traffic reads it and the sends are placed relative to it.
    seconds = 3.0

    def run(send_at: Sequence[float]) -> list[object]:
        async def scenario(world: World) -> None:
            a = world.net.bind(1)
            world.net.bind(2)

            async def sender() -> None:
                for moment in send_at:
                    await asyncio.sleep(moment - world.now())
                    await a.send(2, f"at {moment:.3f}")

            task = asyncio.ensure_future(sender())
            await world.run_for(seconds=seconds, faults=[world.slow_link(1, 2)])
            await task

        return list(_run_one(scenario, 5))

    start, end = window(run([]), "fault-slow-link-begin", "fault-slow-link-end")
    assert 0.0 < start < end < seconds, (start, end)
    sends = [start / 2, (start + end) / 2, (end + seconds) / 2]
    timeline = run(sends)
    assert window(timeline, "fault-slow-link-begin", "fault-slow-link-end") == (start, end)
    tl = [cast("tuple[object, ...]", e) for e in timeline]
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
            asyncio.ensure_future(spam(world, eps, 0, 1)),
            asyncio.ensure_future(spam(world, eps, 0, 2)),
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
            asyncio.ensure_future(spam(world, eps, 1, 0)),
            asyncio.ensure_future(spam(world, eps, 2, 0)),
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
    # frozenset((a, b)) then never matches a real (src, dst) pair) -- an easy mistake.
    # Swept across many seeds, not just one.
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
        task = asyncio.ensure_future(spam(world, eps, 0, 1))
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
        task = asyncio.ensure_future(spam(world, eps, 0, 1))
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
        task = asyncio.ensure_future(spam(world, {0: a}, 0, 1, count=20, interval=0.1))
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
