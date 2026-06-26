"""The timeline: an append-only record of a run's events.

Determinism is proven by replay — running the same scenario twice must produce an identical
timeline (``docs/testing.md``). This slice records the events a scenario chooses to log; later
slices add scheduled network and fault events with stable identities.
"""

from __future__ import annotations

from collections.abc import Sequence


class Timeline:
    """An ordered, append-only log of events for one run."""

    def __init__(self) -> None:
        self._events: list[object] = []

    def record(self, event: object) -> None:
        """Append one event to the timeline."""
        self._events.append(event)

    @property
    def events(self) -> Sequence[object]:
        """The events recorded so far, in order (a read-only snapshot)."""
        return tuple(self._events)

    def __repr__(self) -> str:
        return f"Timeline({self._events!r})"
