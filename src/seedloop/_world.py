"""The World: everything for one deterministic run, derived from one seed.

A run is a pure function of its seed. The World assembles the deterministic loop, the virtual clock,
and the seeded entropy into one object, exposes the user's seeded ``rng`` and the virtual clock, and
records a timeline so two runs of a seed can be compared. Users do not construct a World;
``check``/``replay`` build it and pass it to the scenario.

Scheduling stays faithful FIFO (ADR-0012), so the seed's observable effect is ``rng``, timer timing,
and the simulated network's delivery timing — not callback order.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol, runtime_checkable

from seedloop._entropy import substream
from seedloop._loop import DeterministicLoop
from seedloop._net import Transport
from seedloop._trace import Timeline
from seedloop.errors import InvariantError


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
        self._invariants: list[tuple[str, Callable[[], bool]]] = []
        self.net = Transport(
            self._loop, substream(seed, "net"), substream(seed, "faults"), self._timeline
        )

    def now(self) -> float:
        """Current virtual time in seconds (advances by autojump, never by real waiting)."""
        return self._loop.time()

    def record(self, event: object) -> None:
        """Append an event to the run's timeline, stamped with the current virtual time.

        The timeline is the artifact that proves determinism: two runs of a seed must record an
        identical sequence. A scenario records the decisions whose reproducibility it cares about.
        """
        self._timeline.record((self._loop.time(), event))

    def always(self, predicate: Callable[[], bool], *, name: str) -> None:
        """Register a safety property that must hold throughout the run.

        ``predicate`` is evaluated after every step (not during teardown); the first step where it
        is false raises ``InvariantError(name)``, which ``check`` reports. It must be pure and
        read-only — a predicate that mutates state or draws entropy would break determinism. A
        started node's body runs a step after ``start``, so a predicate over node state sees its
        initial value on the first check.
        """
        self._invariants.append((name, predicate))
        self._loop._sl_after_step = self._check_invariants  # check from the next step on

    def _check_invariants(self) -> None:
        for name, predicate in self._invariants:
            if not predicate():
                raise InvariantError(name, self._loop.time())

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
            # Invariants describe the logical run, not cancellation cleanup — stop checking them
            # before teardown, so a node mutating observed state in its cancel handler cannot raise
            # a spurious InvariantError (and cannot mask the real failure raised above).
            self._loop._sl_after_step = None
            # Cancel every task still pending — started nodes and any the scenario spawned — and let
            # the cancellations process, so the loop closes without "Task was destroyed but it is
            # pending" warnings (a node loop that never returns, or a recv stuck under a fault).
            # all_tasks() is a set in id()-hash order (varies per process); sort by the loop's
            # creation index so cancellation — which a node can observe in its cancel handler — runs
            # in a deterministic, seed-independent order and the timeline stays reproducible.
            pending = [t for t in asyncio.all_tasks(self._loop) if not t.done()]
            pending.sort(key=lambda t: getattr(t, "_sl_seq", -1))
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            self._loop.close()
