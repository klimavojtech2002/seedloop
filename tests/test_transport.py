"""Simulated datagram transport tests (slice 0200)."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import cast

import pytest

import seedloop
from seedloop._run import _run_one
from seedloop._world import World


def _entries(timeline: Sequence[object]) -> list[tuple[object, ...]]:
    return [cast("tuple[object, ...]", e) for e in timeline]


def test_two_nodes_exchange_a_message() -> None:
    received: list[tuple[int, object]] = []

    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        b = world.net.bind(2)

        async def receiver() -> None:
            received.append(await b.recv())

        task = asyncio.ensure_future(receiver())
        await a.send(2, "hello")
        await task

    seedloop.replay(scenario, seed=1)
    assert received == [(1, "hello")]


async def _six_sends(world: World) -> None:
    a = world.net.bind(1)
    b = world.net.bind(2)

    async def receiver() -> None:
        for _ in range(6):
            await b.recv()

    task = asyncio.ensure_future(receiver())
    for i in range(6):
        await a.send(2, ("m", i))
    await task


def test_replay_equivalence_network() -> None:
    first = _run_one(_six_sends, 12345)
    second = _run_one(_six_sends, 12345)
    assert first == second
    assert any(e[1] == "deliver" for e in _entries(first))  # the network actually ran


def _delivery_order(seed: int) -> list[object]:
    # Two messages sent back-to-back; return the order their ids are delivered in.
    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        b = world.net.bind(2)

        async def receiver() -> None:
            for _ in range(2):
                await b.recv()

        task = asyncio.ensure_future(receiver())
        await a.send(2, "m0")
        await a.send(2, "m1")
        await task

    return [e[2] for e in _entries(_run_one(scenario, seed)) if e[1] == "deliver"]


def test_reordering_is_reproducible() -> None:
    assert _delivery_order(7) == _delivery_order(7)  # same seed, same order
    # Across seeds, at least one delivers message id 1 before id 0 — reordering is real.
    assert any(_delivery_order(s) == [1, 0] for s in range(30))


def test_recv_blocks_then_wakes() -> None:
    log: list[str] = []

    async def scenario(world: World) -> None:
        a = world.net.bind(1)
        b = world.net.bind(2)

        async def receiver() -> None:
            log.append("waiting")
            _src, msg = await b.recv()
            log.append(f"got {msg} at {world.now():.3f}")

        task = asyncio.ensure_future(receiver())
        await asyncio.sleep(1)  # receiver is blocked in recv during this
        await a.send(2, "late")
        await task

    seedloop.replay(scenario, seed=1)
    assert log[0] == "waiting"
    assert log[1].startswith("got late at 1.")  # delivered after the send at t=1 + latency


def test_recv_with_nothing_sent_deadlocks() -> None:
    async def scenario(world: World) -> None:
        b = world.net.bind(2)
        await b.recv()  # nothing is ever sent, no timer to wake it

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.DeadlockError)


def test_bind_twice_raises() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1)
        world.net.bind(1)

    result = seedloop.check(scenario, seeds=1, on_failure="return")
    assert isinstance(result.error, seedloop.SeedloopError)


def test_send_to_unbound_address_is_dropped() -> None:
    # A datagram to an unbound address is dropped cleanly, not an error (at-most-once delivery).
    errors: list[object] = []

    async def scenario(world: World) -> None:
        world._loop.set_exception_handler(lambda _loop, ctx: errors.append(ctx))
        a = world.net.bind(1)
        await a.send(99, "into the void")
        await asyncio.sleep(1)  # let the delivery fire

    seedloop.replay(scenario, seed=1)
    # The delivery callback must handle the unbound address without raising — an exception there
    # would be routed to the loop's handler, not the scenario, and silently swallowed.
    assert errors == []


def test_reliable_channel_not_yet_supported() -> None:
    async def scenario(world: World) -> None:
        world.net.bind(1, reliable=True)

    with pytest.raises(NotImplementedError):
        seedloop.replay(scenario, seed=1)
