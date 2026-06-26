"""Virtual clock and autojump tests (slice 0110)."""

from __future__ import annotations

import asyncio
import time as real_time

import pytest

from seedloop._loop import DeterministicLoop
from seedloop._trace import Timeline
from seedloop.errors import DeadlockError


def test_sleep_advances_virtual_time_instantly() -> None:
    loop = DeterministicLoop()

    async def scenario() -> float:
        await asyncio.sleep(10)
        return loop.time()

    started = real_time.monotonic()
    try:
        virtual = loop.run_until_complete(scenario())
    finally:
        loop.close()
    assert virtual == 10.0  # the clock jumped to the deadline
    assert real_time.monotonic() - started < 5  # but no real ten-second wait happened


def test_sequential_sleeps_sum_in_virtual_time() -> None:
    loop = DeterministicLoop()

    async def scenario() -> float:
        await asyncio.sleep(3)
        await asyncio.sleep(4)
        return loop.time()

    try:
        assert loop.run_until_complete(scenario()) == 7.0
    finally:
        loop.close()


def _run_timed_timeline() -> list[object]:
    loop = DeterministicLoop()
    timeline = Timeline()

    async def worker(name: str, delays: list[float]) -> None:
        for delay in delays:
            await asyncio.sleep(delay)
            timeline.record((loop.time(), name))

    async def scenario() -> None:
        await asyncio.gather(worker("a", [1, 2]), worker("b", [2, 1]))

    try:
        loop.run_until_complete(scenario())
    finally:
        loop.close()
    return list(timeline.events)


def test_replay_equivalence_timed() -> None:
    # The determinism proof for this slice: the timed timeline is identical across runs, and the
    # two events that land at virtual time 3 are ordered by the (when, seq) tie-break (a's timer
    # was scheduled before b's).
    first = _run_timed_timeline()
    second = _run_timed_timeline()
    assert first == second
    assert first == [(1.0, "a"), (2.0, "b"), (3.0, "a"), (3.0, "b")]


def test_quiescent_run_with_no_timer_raises_deadlock() -> None:
    loop = DeterministicLoop()

    async def scenario() -> None:
        await loop.create_future()  # never resolved, and no timer is scheduled to wake it

    try:
        with pytest.raises(DeadlockError):
            loop.run_until_complete(scenario())
    finally:
        loop.close()
