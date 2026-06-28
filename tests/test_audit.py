"""The non-determinism auditor: audit mode trips on uncontrolled entropy."""

from __future__ import annotations

import asyncio
import os
import random
import secrets
import threading
import time

import seedloop
from seedloop._run import _run_one
from seedloop._world import World


async def _clean(world: World) -> None:
    # Only controlled sources: the seeded rng and the virtual clock.
    world.record(("draw", world.rng.random()))
    await asyncio.sleep(1)
    world.record(("tick", world.now()))


def test_clean_run_passes_under_audit_with_identical_timeline() -> None:
    # The auditor is inert when nothing trips it: a clean run is unchanged with audit on vs off.
    assert list(_run_one(_clean, 1, audit=True)) == list(_run_one(_clean, 1, audit=False))


def _scenario_calling(fn: object) -> seedloop.Scenario:
    async def scenario(world: World) -> None:
        fn()  # type: ignore[operator]

    return scenario


def test_real_time_trips() -> None:
    # Real user code calls `time.monotonic()` — an attribute lookup at call time, which the tripwire
    # intercepts. (A reference captured before audit started, e.g. `from time import monotonic`, is
    # early-bound and not caught — the same limit the CSPRNG shim has; documented in _audit.py.)
    result = seedloop.check(
        _scenario_calling(lambda: time.monotonic()), seeds=1, on_failure="return", audit=True
    )
    assert isinstance(result.error, seedloop.EntropyLeakError)
    assert result.error.source == "time.monotonic"


def test_real_wall_clock_trips() -> None:
    result = seedloop.check(
        _scenario_calling(lambda: time.time()), seeds=1, on_failure="return", audit=True
    )
    assert isinstance(result.error, seedloop.EntropyLeakError)


def test_os_urandom_trips_under_audit() -> None:
    result = seedloop.check(
        _scenario_calling(lambda: os.urandom(8)), seeds=1, on_failure="return", audit=True
    )
    assert isinstance(result.error, seedloop.EntropyLeakError)
    assert result.error.source == "os.urandom"


def test_secrets_trips_under_audit() -> None:
    result = seedloop.check(
        _scenario_calling(lambda: secrets.token_bytes(8)), seeds=1, on_failure="return", audit=True
    )
    assert isinstance(result.error, seedloop.EntropyLeakError)


def test_global_random_trips() -> None:
    result = seedloop.check(
        _scenario_calling(lambda: random.random()), seeds=1, on_failure="return", audit=True
    )
    assert isinstance(result.error, seedloop.EntropyLeakError)
    assert result.error.source == "random.random"


def test_audit_covers_every_entropy_drawing_random_function() -> None:
    # A completeness guard, enumerated independently of _audit.py: if the auditor's list drops an
    # entropy-drawing random.* (e.g. expovariate), the audit would pass clean on a real leak.
    from seedloop._audit import _ENTROPY_SURFACES

    covered = {name for _, _, name in _ENTROPY_SURFACES if name.startswith("random.")}
    expected = {
        f"random.{fn}"
        for fn in (
            "random",
            "uniform",
            "triangular",
            "randint",
            "randrange",
            "choice",
            "choices",
            "shuffle",
            "sample",
            "getrandbits",
            "randbytes",
            "betavariate",
            "expovariate",
            "gammavariate",
            "gauss",
            "lognormvariate",
            "normalvariate",
            "vonmisesvariate",
            "paretovariate",
            "weibullvariate",
        )
        if hasattr(random, fn)
    }
    assert not (expected - covered), f"entropy-drawing random.* not audited: {expected - covered}"


def test_every_listed_random_function_actually_trips() -> None:
    # Each listed random.* must raise under audit; the tripwire ignores args, so no-arg calls trip.
    from seedloop._audit import _ENTROPY_SURFACES

    for attr in (n.split(".", 1)[1] for _, _, n in _ENTROPY_SURFACES if n.startswith("random.")):
        result = seedloop.check(
            _scenario_calling(lambda a=attr: getattr(random, a)()),
            seeds=1,
            on_failure="return",
            audit=True,
        )
        assert isinstance(result.error, seedloop.EntropyLeakError), attr
        assert result.error.source == f"random.{attr}", attr


def test_real_thread_trips_as_boundary() -> None:
    def start_thread() -> None:
        threading.Thread(target=lambda: None).start()

    result = seedloop.check(
        _scenario_calling(start_thread), seeds=1, on_failure="return", audit=True
    )
    assert isinstance(result.error, seedloop.BoundaryError)
    assert not isinstance(result.error, seedloop.EntropyLeakError)


def test_seeded_rng_still_works_under_audit() -> None:
    # world.rng is a Random instance; patching the module-level random.* must not break it.
    async def scenario(world: World) -> None:
        world.record(("v", world.rng.randint(0, 100)))

    seedloop.replay(scenario, seed=3, audit=True)  # must not raise


def test_a_leak_replays_on_its_seed() -> None:
    leak = _scenario_calling(lambda: time.monotonic())
    result = seedloop.check(leak, seeds=5, on_failure="return", audit=True)
    assert result.failing_seed is not None
    try:
        seedloop.replay(leak, seed=result.failing_seed, audit=True)
    except seedloop.EntropyLeakError:
        pass
    else:  # pragma: no cover - replay must re-raise
        raise AssertionError("replay did not reproduce the leak")


def test_audit_off_seeds_os_urandom_not_trips() -> None:
    # Without audit, os.urandom is seeded (deterministic), not a failure.
    async def scenario(world: World) -> None:
        world.record(("bytes", os.urandom(4)))

    a = list(_run_one(scenario, 7, audit=False))
    b = list(_run_one(scenario, 7, audit=False))
    assert a == b  # seeded => deterministic, and no EntropyLeakError raised


def test_patches_are_restored_after_audit() -> None:
    before = (time.monotonic, time.time, os.urandom, random.random, threading.Thread.start)
    seedloop.check(_scenario_calling(time.monotonic), seeds=1, on_failure="return", audit=True)
    after = (time.monotonic, time.time, os.urandom, random.random, threading.Thread.start)
    assert before == after


def test_audit_mode_context_manager_is_exported() -> None:
    assert "audit_mode" in seedloop.__all__
    with seedloop.audit_mode():
        try:
            time.monotonic()
        except seedloop.EntropyLeakError:
            pass
        else:  # pragma: no cover
            raise AssertionError("audit_mode did not trip on time.monotonic")
    time.monotonic()  # restored outside the context
