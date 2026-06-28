"""Running scenarios: ``check`` sweeps seeds, ``replay`` reproduces one.

The contract (ADR-0003): a run is a pure function of its seed, so a failing seed *is* the
reproduction. ``check`` runs a scenario once per seed and reports the first failing seed; ``replay``
rebuilds that exact run. A fresh :class:`World` is built per seed with no shared mutable state, so
one run cannot bleed into the next.

``check``/``replay`` do not pin ``PYTHONHASHSEED`` (ADR-0015): the launcher re-runs the whole
interpreter, which is wrong to trigger implicitly from inside a test runner. The guarantee instead
rests on library code never depending on hash order; a user whose own code does can call
``seedloop.ensure_hash_seed`` at their entry point.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

from seedloop._audit import audit_mode
from seedloop._entropy import csprng_shim, substream
from seedloop._world import World

Scenario = Callable[[World], Awaitable[None]]


@dataclass(frozen=True)
class CheckResult:
    """The outcome of a seed sweep."""

    checked: int  # how many seeds ran
    failing_seed: int | None  # first failing seed, or None if all passed
    error: Exception | None  # the exception that seed raised, or None


def _run_one(scenario: Scenario, seed: int, *, audit: bool = False) -> Sequence[object]:
    """Run ``scenario`` for one seed and return its recorded timeline.

    Normally the CSPRNG shim is installed for the run and removed after, so ``os.urandom`` and
    ``secrets`` draw from the seed without leaking the seeded source into later runs. With
    ``audit=True`` the non-determinism auditor runs instead: uncontrolled entropy (real time, the
    unseeded global ``random``, ``os.urandom``/``secrets``, a real thread) raises rather than being
    seeded or run, so a leak fails on this seed (ADR-0008).
    """
    world = World(seed)
    context = audit_mode() if audit else csprng_shim(substream(seed, "csprng"))
    with context:
        world._drive(scenario(world))
    return world.timeline


def check(
    scenario: Scenario,
    *,
    seeds: int | Iterable[int] = 1000,
    on_failure: Literal["raise", "return"] = "raise",
    audit: bool = False,
) -> CheckResult:
    """Run ``scenario`` once per seed; report the first seed that fails.

    ``seeds=N`` runs seeds ``0..N-1``; an iterable runs exactly those seeds. The first run that
    raises — an ``assert``, a ``SeedloopError``, or any exception from the scenario — is the
    failure. With ``on_failure="raise"`` the exception is re-raised tagged with its seed; with
    ``"return"`` the sweep stops and returns the :class:`CheckResult`. With ``audit=True`` the
    non-determinism auditor runs each seed: an uncontrolled entropy source fails it (ADR-0008).
    """
    seed_iter: Iterable[int] = range(seeds) if isinstance(seeds, int) else seeds
    checked = 0
    for seed in seed_iter:
        checked += 1
        try:
            _run_one(scenario, seed, audit=audit)
        except Exception as error:
            # Only a scenario *failure* is caught; KeyboardInterrupt/SystemExit propagate so a
            # long sweep stays abortable (and is never mis-tagged as a failing seed).
            error.add_note(f"seedloop: failing seed={seed} (replay with seedloop.replay)")
            if on_failure == "raise":
                raise
            return CheckResult(checked=checked, failing_seed=seed, error=error)
    return CheckResult(checked=checked, failing_seed=None, error=None)


def replay(scenario: Scenario, *, seed: int, audit: bool = False) -> None:
    """Rebuild the exact run for ``seed`` and run it once, re-raising any failure.

    ``audit=True`` reproduces the run under the non-determinism auditor (ADR-0008).
    """
    _run_one(scenario, seed, audit=audit)
