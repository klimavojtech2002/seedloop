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

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

from seedloop._audit import audit_mode
from seedloop._entropy import csprng_shim, substream
from seedloop._world import _RUNNING_LOOP_MESSAGE, DEFAULT_MAX_STEPS, World
from seedloop.errors import _UsageError

Scenario = Callable[[World], Awaitable[None]]


@dataclass(frozen=True)
class CheckResult:
    """The outcome of a seed sweep."""

    checked: int  # how many seeds ran
    failing_seed: int | None  # first failing seed, or None if all passed
    error: Exception | None  # the exception that seed raised, or None


def _run_one(
    scenario: Scenario,
    seed: int,
    *,
    audit: bool = False,
    max_steps: int | None = DEFAULT_MAX_STEPS,
) -> Sequence[object]:
    """Run ``scenario`` for one seed and return its recorded timeline.

    Normally the CSPRNG shim is installed for the run and removed after, so ``os.urandom`` and
    ``secrets`` draw from the seed without leaking the seeded source into later runs. With
    ``audit=True`` the non-determinism auditor runs instead: uncontrolled entropy (real time, the
    unseeded global ``random``, ``os.urandom``/``secrets``, a real thread) raises rather than being
    seeded or run, so a leak fails on this seed (ADR-0008).
    """
    world = World(seed, max_steps=max_steps)
    try:
        context = audit_mode() if audit else csprng_shim(substream(seed, "csprng"))
        with context:
            world._drive(scenario(world))
    finally:
        world._close()
    return world.timeline


def _preflight(max_steps: int | None) -> None:
    if asyncio.events._get_running_loop() is not None:
        raise _UsageError(_RUNNING_LOOP_MESSAGE)
    # bool is an int, and True would mean "one step": never what was meant.
    if max_steps is not None and (
        isinstance(max_steps, bool) or not isinstance(max_steps, int) or max_steps < 1
    ):
        raise _UsageError(f"max_steps must be a positive int or None, got {max_steps!r}")


def check(
    scenario: Scenario,
    *,
    seeds: int | Iterable[int] = 1000,
    on_failure: Literal["raise", "return"] = "raise",
    audit: bool = False,
    max_steps: int | None = DEFAULT_MAX_STEPS,
) -> CheckResult:
    """Run ``scenario`` once per seed; report the first seed that fails.

    ``seeds=N`` runs seeds ``0..N-1``; an iterable runs exactly those seeds. The first run that
    raises — an ``assert``, a ``SeedloopError``, or any exception from the scenario — is the
    failure. With ``on_failure="raise"`` the exception is re-raised tagged with its seed; with
    ``"return"`` the sweep stops and returns the :class:`CheckResult`. With ``audit=True`` the
    non-determinism auditor runs each seed: an uncontrolled entropy source fails it (ADR-0008).

    A run that takes more than ``max_steps`` loop steps without finishing fails with
    ``LivelockError``; ``None`` removes the limit.
    """
    _preflight(max_steps)
    seed_iter: Iterable[int] = range(seeds) if isinstance(seeds, int) else seeds
    checked = 0
    for seed in seed_iter:
        checked += 1
        try:
            _run_one(scenario, seed, audit=audit, max_steps=max_steps)
        except _UsageError:
            raise  # misuse, not a failing seed: untagged, never a CheckResult
        except Exception as error:
            # Only a scenario *failure* is caught; KeyboardInterrupt/SystemExit propagate so a
            # long sweep stays abortable (and is never mis-tagged as a failing seed).
            with contextlib.suppress(
                Exception
            ):  # an error that takes no notes is still the failure
                error.add_note(f"seedloop: failing seed={seed} (replay with seedloop.replay)")
            if on_failure == "raise":
                raise
            return CheckResult(checked=checked, failing_seed=seed, error=error)
    return CheckResult(checked=checked, failing_seed=None, error=None)


def replay(
    scenario: Scenario,
    *,
    seed: int,
    audit: bool = False,
    max_steps: int | None = DEFAULT_MAX_STEPS,
) -> None:
    """Rebuild the exact run for ``seed`` and run it once, re-raising any failure.

    ``audit=True`` reproduces the run under the non-determinism auditor (ADR-0008). ``max_steps``
    is the same step limit as in ``check``.
    """
    _preflight(max_steps)
    _run_one(scenario, seed, audit=audit, max_steps=max_steps)
