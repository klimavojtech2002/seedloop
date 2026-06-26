"""Timer tie-break and cancellation tests (slice 0110)."""

from __future__ import annotations

import asyncio

from seedloop._loop import DeterministicLoop


def test_equal_deadlines_fire_in_scheduling_order() -> None:
    loop = DeterministicLoop()
    fired: list[str] = []

    async def scenario() -> None:
        loop.call_later(1, fired.append, "first")
        loop.call_later(1, fired.append, "second")
        loop.call_later(1, fired.append, "third")
        await asyncio.sleep(2)  # advance past the shared deadline so all three fire

    try:
        loop.run_until_complete(scenario())
    finally:
        loop.close()
    # CPython's TimerHandle orders by deadline alone; the (when, seq) key makes equal deadlines
    # deterministic — they fire in scheduling order.
    assert fired == ["first", "second", "third"]


def test_cancelled_timer_does_not_fire() -> None:
    loop = DeterministicLoop()
    fired: list[str] = []
    errors: list[object] = []
    loop.set_exception_handler(lambda _loop, ctx: errors.append(ctx))

    async def scenario() -> None:
        # "kept" first so it owns the heap head; the cancelled timer is NOT the head, so it must
        # be skipped by the promote/run guards rather than dropped by the head purge.
        loop.call_later(1, fired.append, "kept")
        handle = loop.call_later(1, fired.append, "cancelled")
        handle.cancel()
        await asyncio.sleep(2)

    try:
        loop.run_until_complete(scenario())
    finally:
        loop.close()
    assert fired == ["kept"]
    # The cancelled timer is skipped, not dispatched: cancel() nulls the callback, so running it
    # would call None and route a TypeError to the exception handler — which must not happen.
    assert errors == []


def test_cancelling_the_earliest_timer_advances_to_the_next() -> None:
    # Cancelling the head timer: it never fires, and the clock jumps to the next live deadline.
    loop = DeterministicLoop()
    fired: list[tuple[float, str]] = []

    async def scenario() -> None:
        early = loop.call_later(1, lambda: fired.append((loop.time(), "early")))
        loop.call_later(3, lambda: fired.append((loop.time(), "late")))
        early.cancel()
        await asyncio.sleep(5)

    try:
        loop.run_until_complete(scenario())
    finally:
        loop.close()
    assert fired == [(3.0, "late")]  # "early" never fired; the clock jumped to 3, not 1


def test_call_soon_runs_before_a_zero_delay_timer() -> None:
    # A 0-delay timer fires after callbacks already queued with call_soon, matching CPython.
    loop = DeterministicLoop()
    order: list[str] = []

    async def scenario() -> None:
        loop.call_later(0, order.append, "timer0")
        loop.call_soon(order.append, "soon")

    try:
        loop.run_until_complete(scenario())
    finally:
        loop.close()
    assert order == ["soon", "timer0"]
