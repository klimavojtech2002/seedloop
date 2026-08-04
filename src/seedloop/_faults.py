"""Fault handles consumed by ``World.run_for(faults=[...])``.

A handle records what the caller pinned; any field left unset is resolved by
``Transport._commit_fault`` from the ``"faults"`` sub-stream once ``run_for`` has validated every
fault in its list (``Transport._validate_fault``). A handle itself draws no entropy and does
nothing until passed to ``run_for`` (``docs/network.md``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Deferred: _net.py imports the concrete fault types below, so a runtime import here would be
    # circular. `from __future__ import annotations` already makes every annotation in this module
    # a string, so this import only ever runs for type checkers.
    from seedloop._net import Address


@dataclass(frozen=True)
class PartitionFault:
    """Cut the network between ``groups`` for a seed-chosen window.

    ``groups=()`` lets the seed pick a two-way bipartition of the addresses bound when ``run_for``
    schedules this fault. Timing has no field to pin here — the seed always chooses it.
    """

    groups: tuple[frozenset[Address], ...] = ()


@dataclass(frozen=True)
class SlowLinkFault:
    """Multiply latency on the ``a``↔``b`` link for a seed-chosen window.

    ``a``/``b``/``factor`` left ``None`` are each resolved independently by the seed. Timing has no
    field to pin here — the seed always chooses it.
    """

    a: Address | None = None
    b: Address | None = None
    factor: float | None = None


@dataclass(frozen=True)
class CrashFault:
    """Cut ``node`` off the network, both directions, from ``at`` onward for the rest of the run.

    No restart, no heal — a crash-stop model. ``at`` is a virtual duration from the ``run_for`` call
    that consumes this fault (matching ``run_for``'s own ``seconds``), unlike ``PartitionFault``/
    ``SlowLinkFault`` this is the one fault whose timing can be pinned.
    """

    node: Address | None = None
    at: float | None = None


Fault = PartitionFault | SlowLinkFault | CrashFault
