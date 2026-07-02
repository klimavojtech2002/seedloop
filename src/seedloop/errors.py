"""Exceptions seedloop raises.

One specific exception per failure mode, and seedloop never catches and hides one itself (no bare
``except``). The hierarchy is rooted at ``SeedloopError`` so everything seedloop raises can be
caught with a single class.
"""

from __future__ import annotations


class SeedloopError(Exception):
    """Base class for every error seedloop raises."""


class BoundaryError(SeedloopError):
    """A simulated run reached outside the determinism boundary.

    Real threads, ``run_in_executor``, subprocesses, real sockets, and cross-thread wakeups
    cannot be made deterministic, so they are rejected rather than run silently (``docs/scope.md``).
    """


class DeadlockError(SeedloopError):
    """The run cannot progress and nothing is scheduled to wake it.

    A real ``asyncio`` program would hang here; a simulated run raises instead of spinning, so
    the deadlock is a visible failure tied to the seed that produced it.
    """


class InvariantError(SeedloopError):
    """An ``always(...)`` invariant was violated during a run.

    A continuous safety property (e.g. "at most one leader") that must hold throughout, checked
    after every step; the first step where it is false raises this, which ``check`` reports as the
    failure. Carries the invariant's ``name`` and the virtual ``time`` of the violation.
    """

    def __init__(self, name: str, time: float) -> None:
        super().__init__(f"invariant {name!r} violated at t={time}")
        self.name = name
        self.time = time


class EntropyLeakError(BoundaryError):
    """An uncontrolled entropy source was touched inside a simulated run.

    In audit mode the non-determinism auditor raises this when code reaches for real
    ``os.urandom``/``secrets``, real time, or the unseeded global ``random`` instead of the World's
    seeded source (``docs/decisions.md`` ADR-0008). Carries the offending ``source``.
    """

    def __init__(self, source: str) -> None:
        super().__init__(
            f"uncontrolled entropy source {source!r} used inside a run; route it through the seed "
            f"(world.rng) or the virtual clock — see docs/scope.md"
        )
        self.source = source
