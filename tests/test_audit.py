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
    assert result.error.source == "time.time"


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
    assert result.error.source == "secrets/os.urandom"


def test_global_random_trips() -> None:
    result = seedloop.check(
        _scenario_calling(lambda: random.random()), seeds=1, on_failure="return", audit=True
    )
    assert isinstance(result.error, seedloop.EntropyLeakError)
    assert result.error.source == "random.random"


def test_audit_covers_every_entropy_drawing_random_function() -> None:
    # A completeness guard derived from the stdlib, not a hand-maintained copy: the auditor must
    # cover exactly the entropy-drawing module-level random.* functions — random.__all__ minus the
    # non-drawing seeding/state names and the Random classes. An entropy function added in a future
    # Python (or a quietly dropped one) breaks this, so a real leak cannot pass clean.
    from seedloop._audit import _RANDOM_FUNCS

    drawing = set(random.__all__) - {"Random", "SystemRandom", "seed", "getstate", "setstate"}
    assert set(_RANDOM_FUNCS) == drawing


def test_cpu_clocks_trip_under_audit() -> None:
    # process_time/thread_time and POSIX clock_gettime (and their _ns forms) are real,
    # nondeterministic clocks with no pure form, so they must trip like monotonic/perf_counter. The
    # POSIX-only names are absent on Windows and skipped there; CI exercises them on Linux/macOS.
    for name in (
        "process_time",
        "process_time_ns",
        "thread_time",
        "thread_time_ns",
        "clock_gettime",
        "clock_gettime_ns",
    ):
        if not hasattr(time, name):  # pragma: no cover - platform-dependent
            continue
        result = seedloop.check(
            _scenario_calling(lambda n=name: getattr(time, n)()),
            seeds=1,
            on_failure="return",
            audit=True,
        )
        assert isinstance(result.error, seedloop.EntropyLeakError), name
        assert result.error.source == f"time.{name}", name


def test_current_time_calendar_functions_trip_only_in_their_now_form() -> None:
    # gmtime()/localtime()/ctime()/asctime()/strftime(fmt) read the *current* time and must trip;
    # given an explicit timestamp the same functions are pure conversions and must still work, so
    # the tripwire fires on the now-reading form only (no false positive on a pure convert).
    now_forms = (
        lambda: time.gmtime(),
        lambda: time.localtime(),
        lambda: time.ctime(),
        lambda: time.asctime(),
        lambda: time.strftime("%Y"),
    )
    for fn in now_forms:
        result = seedloop.check(_scenario_calling(fn), seeds=1, on_failure="return", audit=True)
        assert isinstance(result.error, seedloop.EntropyLeakError)

    t0 = time.gmtime(0)

    async def pure_conversions(world: World) -> None:
        # Explicit timestamps: deterministic, must not trip under audit.
        world.record(("gmtime", time.gmtime(0)))
        world.record(("localtime", time.localtime(0)))
        world.record(("ctime", time.ctime(0)))
        world.record(("asctime", time.asctime(t0)))
        world.record(("strftime", time.strftime("%Y", t0)))

    seedloop.replay(pure_conversions, seed=1, audit=True)  # must not raise


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
