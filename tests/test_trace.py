"""Tests for the Timeline recorder."""

from __future__ import annotations

from seedloop._trace import Timeline


def test_records_in_order() -> None:
    timeline = Timeline()
    for event in (1, "two", 3):
        timeline.record(event)
    assert list(timeline.events) == [1, "two", 3]


def test_events_is_an_independent_snapshot() -> None:
    timeline = Timeline()
    timeline.record(1)
    snapshot = timeline.events
    timeline.record(2)
    # The snapshot taken before the second record must not reflect it: events must not hand out
    # the live backing list (a leak would let a caller corrupt the append-only log).
    assert list(snapshot) == [1]
