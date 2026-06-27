"""The World: everything for one deterministic run, derived from one seed.

A run is a pure function of its seed. The World assembles the deterministic loop and virtual clock
(slices 0100/0110) and the seeded entropy (slice 0120) into one object, exposes the user's seeded
``rng`` and the virtual clock, and records a timeline so two runs of a seed can be compared. Users
do not construct a World; ``check``/``replay`` build it and pass it to the scenario.

Scheduling stays faithful FIFO (ADR-0012), so in Phase 1 the seed's observable effect is ``rng`` and
timer timing; interleaving exploration arrives with the simulated network in Phase 2.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Protocol, runtime_checkable

from seedloop._entropy import substream
from seedloop._loop import DeterministicLoop
from seedloop._trace import Timeline


@runtime_checkable
class Node(Protocol):
    """User code the World can start: any object with an async ``run``."""

    async def run(self) -> None: ...


class World:
    """One deterministic run, all derived from ``seed``."""

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.rng = substream(seed, "user")  # the user's entropy; never the global random
        self._loop = DeterministicLoop()
        self._timeline = Timeline()
        self._started: list[asyncio.Task[None]] = []

    def now(self) -> float:
        """Current virtual time in seconds (advances by autojump, never by real waiting)."""
        return self._loop.time()

    def record(self, event: object) -> None:
        """Append an event to the run's timeline, stamped with the current virtual time.

        The timeline is the artifact that proves determinism: two runs of a seed must record an
        identical sequence. A scenario records the decisions whose reproducibility it cares about.
        """
        self._timeline.record((self._loop.time(), event))

    def start(self, *nodes: Node) -> None:
        """Schedule each node's ``run()`` coroutine as a task on the loop.

        A started node that raises fails the run (its exception surfaces from the run), rather than
        being orphaned and silently logged — a failure the seed must report.
        """
        for node in nodes:
            self._started.append(self._loop.create_task(node.run()))

    @property
    def timeline(self) -> tuple[object, ...]:
        """The recorded timeline so far (a read-only snapshot)."""
        return tuple(self._timeline.events)

    def _drive(self, main: Awaitable[None]) -> None:
        """Run the scenario to completion, surface any started-node failure, then close the loop."""
        try:
            self._loop.run_until_complete(main)
            # The scenario finished without raising; surface the first started node that failed
            # (a crashed node would otherwise be an orphaned task, only logged).
            for task in self._started:
                exc = task.exception() if task.done() and not task.cancelled() else None
                if exc is not None:
                    raise exc
        finally:
            for task in self._started:
                task.cancel()  # stop nodes still running (e.g. a node loop that never returns)
            self._loop.close()
