"""Helpers shared by the tests that run simulated scenarios."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine, Sequence
from typing import Any, cast

import seedloop

_KEPT: list[asyncio.Future[None]] = []


def spawn(coro: Coroutine[Any, Any, None]) -> None:
    """Start a task on the running loop and keep a reference to it.

    A task nobody references can be collected in the middle of a run; this one lives for the
    life of the test process.
    """
    _KEPT.append(asyncio.ensure_future(coro))


def event_kinds(timeline: Sequence[object]) -> list[str]:
    """The event-kind field of every event in a timeline."""
    return [str(cast("tuple[object, ...]", e)[1]) for e in timeline]


async def spam(
    world: seedloop.World,
    eps: dict[int, seedloop.Endpoint],
    src: int,
    dst: int,
    *,
    count: int = 40,
    interval: float = 0.05,
) -> None:
    """Send ``count`` numbered messages from ``src`` to ``dst``, one every ``interval`` seconds."""
    for i in range(count):
        await eps[src].send(dst, i)
        await asyncio.sleep(interval)
