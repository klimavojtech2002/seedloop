"""Network fault injection: loss, duplication, partition, reliable channel."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Sequence
from typing import cast

import seedloop
from seedloop._entropy import csprng_shim, substream
from seedloop._run import _run_one
from seedloop._world import World


def _capture_timeline(scenario: seedloop.Scenario, seed: int) -> tuple[object, ...]:
    # Run a scenario that may deadlock and return the timeline recorded before it failed, so a
    # failing run's reproducibility can be checked (the public _run_one raises before returning it).
    world = World(seed)
    with csprng_shim(substream(seed, "csprng")), contextlib.suppress(seedloop.DeadlockError):
        world._drive(scenario(world))
    return world.timeline


def _kinds(timeline: Sequence[object]) -> list[str]:
    return [str(cast("tuple[object, ...]", e)[1]) for e in timeline]  # the event-kind field


def test_loss_drops_messages_reproducibly() -> None:
    async def scenario(world: World) -> None:
        a = world.net.bind(1, loss=0.5)
        world.net.bind(2)
        for i in range(20):
            await a.send(2, i)
        await asyncio.sleep(1)  # let surviving deliveries fire

    first = _run_one(scenario, 3)
    second = _run_one(scenario, 3)
    assert first == second  # same seed → same drops
    kinds = _kinds(first)
    assert "drop" in kinds and "deliver" in kinds  # some dropped, some delivered
    assert 0 < kinds.count("drop") < 20  # loss=0.5 drops a strict subset


def test_no_loss_drops_nothing() -> None:
    async def scenario(world: World) -> None:
        a = world.net.bind(1)  # loss defaults to 0.0
        world.net.bind(2)
        for i in range(10):
            await a.send(2, i)
        await asyncio.sleep(1)

    assert "drop" not in _kinds(_run_one(scenario, 1))


def test_duplication_delivers_twice() -> None:
    delivered: list[object] = []

    async def scenario(world: World) -> None:
        a = world.net.bind(1, duplicate=1.0)  # every message duplicated
        b = world.net.bind(2)

        async def receiver() -> None:
            for _ in range(2):
                delivered.append(await b.recv())

        task = asyncio.ensure_future(receiver())
        await a.send(2, "x")
        await task

    seedloop.replay(scenario, seed=1)
    assert delivered == [(1, "x"), (1, "x")]  # the same message arrived twice


def test_partition_cuts_then_heal_restores() -> None:
    got: list[object] = []

    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        b = world.net.bind(2)

        async def receiver() -> None:
            got.append(await b.recv())

        task = asyncio.ensure_future(receiver())
        world.net.partition({1}, {2})
        await a.send(2, "cut")  # 1 and 2 are split → dropped at delivery
        await asyncio.sleep(1)
        world.net.heal()
        await a.send(2, "through")  # connectivity restored → delivered
        await task

    seedloop.replay(scenario, seed=1)
    assert got == [(1, "through")]
    # The cut message shows as drop-partitioned in the timeline; the second as deliver.
    kinds = _kinds(_run_one(scenario, 1))
    assert "drop-partitioned" in kinds


def test_reliable_channel_no_loss_in_order() -> None:
    got: list[object] = []

    async def scenario(world: World) -> None:
        # loss=1.0 would drop everything on an unreliable link; reliable ignores it.
        a = world.net.bind(1, reliable=True, loss=1.0, duplicate=1.0)
        b = world.net.bind(2)

        async def receiver() -> None:
            for _ in range(5):
                got.append(await b.recv())

        task = asyncio.ensure_future(receiver())
        for i in range(5):
            await a.send(2, i)
        await task

    seedloop.replay(scenario, seed=1)
    msgs = [cast("tuple[object, object]", g)[1] for g in got]
    assert msgs == [0, 1, 2, 3, 4]  # no loss, no dup, in send order


def test_reliable_deliveries_carry_latency_and_are_non_decreasing() -> None:
    # A reliable link delivers at non-decreasing virtual times that carry the drawn latency — not
    # collapsed to the send instant. Order alone (the test above) does not catch that collapse.
    async def scenario(world: World) -> None:
        a = world.net.bind(1, reliable=True)
        b = world.net.bind(2)

        async def receiver() -> None:
            for _ in range(5):
                await b.recv()

        task = asyncio.ensure_future(receiver())
        for i in range(5):
            await a.send(2, i)
        await task

    timeline = _run_one(scenario, 1)
    deliver_times = [
        cast("float", e[0]) for e in timeline if isinstance(e, tuple) and e[1] == "deliver"
    ]
    assert len(deliver_times) == 5
    assert deliver_times == sorted(deliver_times)  # non-decreasing per (src, dst)
    assert all(t > 0 for t in deliver_times)  # real latency, not min-collapsed to the send instant


def test_replay_equivalence_under_faults() -> None:
    async def scenario(world: World) -> None:
        a = world.net.bind(1, loss=0.3, duplicate=0.3)
        world.net.bind(2)
        world.net.partition({1}, {2})
        for i in range(10):
            await a.send(2, i)
        world.net.heal()
        for i in range(10, 20):
            await a.send(2, i)
        await asyncio.sleep(1)

    first = _run_one(scenario, 999)
    second = _run_one(scenario, 999)
    assert first == second  # the full fault timeline is reproducible


def test_full_loss_recv_deadlocks_not_hang() -> None:
    async def scenario(world: World) -> None:
        a = world.net.bind(1, loss=1.0)
        b = world.net.bind(2)
        await a.send(2, "lost")
        await b.recv()  # nothing survives loss=1.0, nothing wakes recv

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.DeadlockError)


# --- the DST payoff: a partition-dependent bug, found and replayed ---


class _AckServer:
    def __init__(self, world: World) -> None:
        self._endpoint = world.net.bind(2)

    async def run(self) -> None:
        src, _req = await self._endpoint.recv()
        await self._endpoint.send(src, "ack")


async def _naive_request_ack(world: World) -> None:
    # A request/ack with NO retry: if the request is lost (here, by a partition), the client waits
    # forever. Under a partition this deadlocks — a real bug DST surfaces and replays from the seed.
    client = world.net.bind(1)
    world.start(_AckServer(world))  # the server node, cancelled cleanly at teardown
    world.net.partition({1}, {2})  # the request will be dropped
    await client.send(2, "request")
    await client.recv()  # waits for an ack that never comes — the bug


def test_partition_bug_is_found_and_replays() -> None:
    # check surfaces the partition-dependent deadlock; the seed reproduces the same failing run.
    result = seedloop.check(_naive_request_ack, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.DeadlockError)
    first = _capture_timeline(_naive_request_ack, 0)
    second = _capture_timeline(_naive_request_ack, 0)
    assert first == second  # the actual failing timeline replays identically...
    assert _kinds(first).count("drop-partitioned") == 1  # ...and it is the partition that cut it


async def _lossy_request_ack(world: World) -> None:
    # The same no-retry request/ack, but over a lossy link (no partition): on seeds where the
    # request survives it works; on seeds where loss drops it, the client deadlocks. This is the
    # bug a sweep finds — most seeds pass, a few fail — and the failing seed is the reproduction.
    client = world.net.bind(1, loss=0.5)
    world.start(_AckServer(world))
    await client.send(2, "request")
    await client.recv()


def test_sweep_finds_a_loss_triggered_failure() -> None:
    # Across seeds, some deliver the request (pass) and some drop it (deadlock); check reports the
    # first failing seed, and replay reproduces exactly that seed's failure.
    result = seedloop.check(_lossy_request_ack, seeds=50, on_failure="return")
    assert isinstance(result.error, seedloop.DeadlockError)
    assert result.failing_seed is not None
    # The reported seed genuinely fails; a passing seed exists too (the sweep is meaningful).
    bad = _capture_timeline(_lossy_request_ack, result.failing_seed)
    assert "drop" in _kinds(bad)  # the failing seed's request was lost
    assert _capture_timeline(_lossy_request_ack, result.failing_seed) == bad  # replays identically


# --- the slice's own invariants ---


def test_fault_draws_do_not_perturb_net_latencies() -> None:
    # Loss draws come from the "faults" sub-stream, not "net": taking a loss draw must not shift the
    # latencies of surviving messages. loss=0 and a tiny loss>0 (which draws but ~never drops) must
    # produce identical delivery times. (If loss drew from "net", they would diverge.)
    def deliver_times(loss: float) -> list[object]:
        async def scenario(world: World) -> None:
            a = world.net.bind(1, loss=loss)
            world.net.bind(2)
            for i in range(5):
                await a.send(2, i)
            await asyncio.sleep(1)

        return [
            cast("tuple[object, ...]", e)[0]
            for e in _run_one(scenario, 1)
            if cast("tuple[object, ...]", e)[1] == "deliver"
        ]

    assert deliver_times(0.0) == deliver_times(1e-12)


class _ForeverNode:
    def __init__(self) -> None:
        self.cancelled = False

    async def run(self) -> None:
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.cancelled = True
            raise


def test_started_node_is_cancelled_at_teardown() -> None:
    # A node still running when the scenario returns is cancelled cleanly at teardown (no orphaned
    # pending task, no "Task was destroyed" warning).
    node = _ForeverNode()

    async def scenario(world: World) -> None:
        world.start(node)
        await asyncio.sleep(0)  # the scenario returns while the node loops forever

    seedloop.replay(scenario, seed=1)
    assert node.cancelled


def test_loss_and_duplicate_must_be_probabilities() -> None:
    async def bad_loss(world: World) -> None:
        world.net.bind(1, loss=50)  # a "50%" typo, not a probability

    async def bad_duplicate(world: World) -> None:
        world.net.bind(1, duplicate=-1)

    for scenario in (bad_loss, bad_duplicate):
        result = seedloop.check(scenario, seeds=1, on_failure="return")
        assert isinstance(result.error, seedloop.SeedloopError)
