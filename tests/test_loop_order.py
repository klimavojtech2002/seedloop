"""Scheduling-order and determinism tests for the deterministic loop (slice 0100)."""

from __future__ import annotations

import asyncio

import pytest

from seedloop._loop import DeterministicLoop
from seedloop._trace import Timeline
from seedloop.errors import DeadlockError


def _run_order() -> list[int]:
    """Schedule ten callbacks and return the order they ran in."""
    loop = DeterministicLoop()
    order: list[int] = []

    async def scenario() -> None:
        for i in range(10):
            loop.call_soon(order.append, i)

    try:
        loop.run_until_complete(scenario())
    finally:
        loop.close()
    return order


def _run_interleaved() -> list[object]:
    """Two coroutines interleaving via sleep(0); return the recorded timeline."""
    loop = DeterministicLoop()
    timeline = Timeline()

    async def worker(name: str, n: int) -> None:
        for k in range(n):
            timeline.record((name, k))
            await asyncio.sleep(0)  # bare yield: reschedules via call_soon, no timer

    async def scenario() -> None:
        await asyncio.gather(worker("a", 3), worker("b", 3))

    try:
        loop.run_until_complete(scenario())
    finally:
        loop.close()
    return list(timeline.events)


def _nested(order: list[str], done: asyncio.Future[None]) -> None:
    order.append("nested")
    done.set_result(None)


def test_call_soon_runs_in_registration_order() -> None:
    assert _run_order() == list(range(10))


def test_scheduling_is_deterministic_across_runs() -> None:
    # Replay equivalence (scheduling): the same scenario, run twice, yields the same order.
    assert _run_order() == _run_order()


def test_replay_equivalence_interleaving() -> None:
    # The determinism proof for this slice: two coroutines interleave identically every run.
    first = _run_interleaved()
    second = _run_interleaved()
    assert first == second
    # And the interleaving is the FIFO one, not an accident.
    assert first == [("a", 0), ("b", 0), ("a", 1), ("b", 1), ("a", 2), ("b", 2)]


def test_dynamically_scheduled_callback_runs_after_queued_callbacks() -> None:
    loop = DeterministicLoop()

    async def scenario() -> list[str]:
        order: list[str] = []
        done: asyncio.Future[None] = loop.create_future()

        def first() -> None:
            order.append("first")
            loop.call_soon(_nested, order, done)

        def second() -> None:
            order.append("second")

        loop.call_soon(first)
        loop.call_soon(second)
        await done
        return order

    try:
        result = loop.run_until_complete(scenario())
    finally:
        loop.close()
    # "nested" is scheduled by `first`, so it goes to the back of the queue and runs after `second`
    # (FIFO). Whether it runs in the same step or the next is not observable until timers exist
    # (slice 0110), so this asserts only the order.
    assert result == ["first", "second", "nested"]


def test_cancelled_callback_is_skipped() -> None:
    loop = DeterministicLoop()
    ran: list[str] = []
    errors: list[object] = []
    loop.set_exception_handler(lambda _loop, ctx: errors.append(ctx))

    async def scenario() -> None:
        handle = loop.call_soon(ran.append, "cancelled")
        handle.cancel()
        loop.call_soon(ran.append, "kept")
        await asyncio.sleep(0)  # let the batch process

    try:
        loop.run_until_complete(scenario())
    finally:
        loop.close()
    assert ran == ["kept"]
    # The cancelled handle is skipped, not run. (cancel() nulls its callback, so running it would
    # call None and route a TypeError to the exception handler — which must not happen.)
    assert errors == []


def test_run_until_complete_returns_result() -> None:
    loop = DeterministicLoop()

    async def scenario() -> int:
        return 42

    try:
        assert loop.run_until_complete(scenario()) == 42
    finally:
        loop.close()


def test_exception_propagates() -> None:
    loop = DeterministicLoop()

    async def scenario() -> None:
        raise ValueError("boom")

    try:
        with pytest.raises(ValueError, match="boom"):
            loop.run_until_complete(scenario())
    finally:
        loop.close()


def test_quiescent_run_raises_deadlock() -> None:
    loop = DeterministicLoop()

    async def scenario() -> None:
        await loop.create_future()  # never resolved; nothing scheduled to resolve it

    try:
        with pytest.raises(DeadlockError):
            loop.run_until_complete(scenario())
    finally:
        loop.close()
