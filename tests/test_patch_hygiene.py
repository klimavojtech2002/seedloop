"""Process-wide patches stay correct under overlapping runs, interrupts and other threads."""

from __future__ import annotations

import os
import random
import secrets
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

import seedloop
from _support import run_isolated
from seedloop import _audit, _entropy
from seedloop._entropy import csprng_shim, exclusive_patching, substream
from seedloop._run import _run_one
from seedloop._world import World

_REAL_URANDOM = os.urandom
_REAL_RANDOM_URANDOM = random._urandom  # type: ignore[attr-defined]
_REAL_MONOTONIC = time.monotonic
_REAL_THREAD_START = threading.Thread.start


def _all_real() -> bool:
    return (
        os.urandom is _REAL_URANDOM
        and random._urandom is _REAL_RANDOM_URANDOM  # type: ignore[attr-defined]
        and time.monotonic is _REAL_MONOTONIC
        and threading.Thread.start is _REAL_THREAD_START
    )


def _restore_real() -> None:
    os.urandom = _REAL_URANDOM
    random._urandom = _REAL_RANDOM_URANDOM  # type: ignore[attr-defined]


async def _draws(world: World) -> None:
    world.record(("urandom", os.urandom(8).hex()))
    world.record(("token", secrets.token_hex(4)))


def _timeline(seed: int) -> list[object]:
    return list(_run_one(_draws, seed))


def test_overlapping_runs_equal_sequential_results_and_leave_everything_real() -> None:
    # A stress check: the race is too narrow to hit on demand, the interleaving tests below pin it.
    seeds = list(range(240))
    sequential = [_timeline(seed) for seed in seeds]
    with ThreadPoolExecutor(max_workers=16) as pool:
        concurrent = list(pool.map(_timeline, seeds))
    assert concurrent == sequential
    assert _all_real()


def test_overlapping_audited_runs_leave_everything_real() -> None:
    async def clean(world: World) -> None:
        world.record(("draw", world.rng.random()))

    def run(seed: int) -> list[object]:
        return list(_run_one(clean, seed, audit=True))

    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(run, range(120)))
    assert results == [run(seed) for seed in range(120)]
    assert _all_real()


def test_another_thread_gets_real_bytes_while_a_run_is_seeded() -> None:
    seeded_first = substream(7, "csprng").getrandbits(128).to_bytes(16, "big")
    seen: list[bytes] = []
    with csprng_shim(substream(7, "csprng")):
        worker = threading.Thread(target=lambda: seen.extend(os.urandom(16) for _ in range(2)))
        worker.start()
        worker.join()
        mine = os.urandom(16)
    assert mine == seeded_first
    assert seeded_first not in seen
    assert seen[0] != seen[1]


def test_a_background_thread_is_unaffected_under_audit() -> None:
    failures: list[BaseException] = []
    real_thread = threading.Thread

    def background() -> None:
        try:
            time.monotonic()
            os.urandom(4)
            random.random()
            time.gmtime()
        except BaseException as exc:
            failures.append(exc)

    def start_background() -> None:
        # Thread.start is wrapped for the whole process; only the owner thread is refused.
        try:
            worker = real_thread(target=background)
            worker.start()
            worker.join()
        except BaseException as exc:
            failures.append(exc)

    with seedloop.audit_mode():
        # started through the pre-audit start, as a library thread already running would be
        outer = real_thread(target=start_background)
        _REAL_THREAD_START(outer)
        outer.join()
        with pytest.raises(seedloop.EntropyLeakError):
            time.monotonic()
    assert not failures, failures
    assert _all_real()


def _interrupting_on(call: int, real: object) -> object:
    calls = {"n": 0}

    def wrapper(*args: object) -> None:
        calls["n"] += 1
        if calls["n"] == call:
            raise KeyboardInterrupt
        real(*args)  # type: ignore[operator]

    return wrapper


def _interrupting_after(call: int, real: object) -> object:
    # The interrupt lands right after the call, so that call's own effect still happened.
    calls = {"n": 0}

    def wrapper(*args: object) -> None:
        real(*args)  # type: ignore[operator]
        calls["n"] += 1
        if calls["n"] == call:
            raise KeyboardInterrupt

    return wrapper


def test_an_interrupt_between_installs_restores_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_entropy, "_install", _interrupting_on(2, _entropy._install))
    with pytest.raises(KeyboardInterrupt), csprng_shim(substream(1, "csprng")):
        raise AssertionError("the install should have been interrupted")
    assert _all_real()
    assert len(secrets.token_bytes(8)) == 8


def test_an_interrupt_between_audit_installs_restores_everything(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_audit, "_install", _interrupting_on(5, _entropy._install))
    with pytest.raises(KeyboardInterrupt), seedloop.audit_mode():
        raise AssertionError("the install should have been interrupted")
    assert _all_real()


def test_an_interrupted_restore_still_restores_the_other_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _entropy, "restore_if_ours", _interrupting_after(1, _entropy.restore_if_ours)
    )
    with pytest.raises(KeyboardInterrupt), csprng_shim(substream(1, "csprng")):
        pass
    assert _all_real()


def test_an_interrupted_audit_restore_still_restores_the_other_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_audit, "restore_if_ours", _interrupting_after(3, _entropy.restore_if_ours))
    with pytest.raises(KeyboardInterrupt), seedloop.audit_mode():
        pass
    assert _all_real()


def test_a_name_replaced_by_someone_else_is_not_clobbered_on_exit() -> None:
    def other(n: int) -> bytes:
        return b"x" * n

    try:
        with csprng_shim(substream(1, "csprng")):
            os.urandom = other
        assert os.urandom is other
    finally:
        _restore_real()


def test_audit_mode_does_not_clobber_a_name_replaced_while_it_was_active() -> None:
    def mine() -> float:
        return 1.0

    def my_start(self: threading.Thread) -> None:
        raise AssertionError("unused")

    try:
        with seedloop.audit_mode():
            time.monotonic = mine
            threading.Thread.start = my_start  # type: ignore[method-assign]
        assert time.monotonic is mine
        assert threading.Thread.start is my_start
    finally:
        time.monotonic = _REAL_MONOTONIC
        threading.Thread.start = _REAL_THREAD_START  # type: ignore[method-assign]
    assert _all_real()


def test_the_originals_are_read_after_the_lock_is_taken() -> None:
    def other(n: int) -> bytes:
        return b"y" * n

    started = threading.Event()

    def run() -> None:
        started.set()
        with csprng_shim(substream(1, "csprng")):
            pass

    try:
        with exclusive_patching():
            worker = threading.Thread(target=run)
            worker.start()
            started.wait()
            time.sleep(0.2)  # the worker is now blocked on the lock
            os.urandom = other  # patched by whoever held the lock first
        worker.join()
        assert os.urandom is other  # the worker saw `other` as the original and put it back
    finally:
        _restore_real()


def test_nested_patching_on_one_thread_does_not_deadlock() -> None:
    done = threading.Event()

    def nest() -> None:
        with (
            exclusive_patching(),
            csprng_shim(substream(1, "csprng")),
            csprng_shim(substream(2, "x")),
        ):
            assert len(os.urandom(4)) == 4
        done.set()

    worker = threading.Thread(target=nest, daemon=True)
    worker.start()
    worker.join(10)
    assert done.is_set(), "nesting on one thread blocked on the lock"
    assert _all_real()


def test_another_thread_gets_real_bytes_through_secrets_and_random() -> None:
    seeded_first = substream(11, "csprng").getrandbits(128).to_bytes(16, "big")
    seen: list[bytes] = []
    with csprng_shim(substream(11, "csprng")):
        worker = threading.Thread(target=lambda: seen.append(secrets.token_bytes(16)))
        worker.start()
        worker.join()
        mine = secrets.token_bytes(16)
    assert mine == seeded_first
    assert seen and seen[0] != seeded_first


def test_an_audited_run_started_while_another_is_active_waits_and_restores_in_order() -> None:
    inside = threading.Event()
    release_first = threading.Event()
    release_second = threading.Event()

    def first() -> None:
        with seedloop.audit_mode():
            inside.set()
            release_first.wait()

    def second() -> None:
        with seedloop.audit_mode():
            release_second.wait()

    one = threading.Thread(target=first)
    two = threading.Thread(target=second)
    try:
        one.start()
        inside.wait()
        two.start()
        time.sleep(0.2)  # the second run waits for the lock, or (without it) is already inside
        release_first.set()
        one.join()
        release_second.set()
        two.join()
        assert _all_real()
    finally:
        release_first.set()
        release_second.set()
        _restore_real()


def test_an_interrupt_right_after_an_install_still_restores_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_entropy, "_install", _interrupting_after(1, _entropy._install))
    with pytest.raises(KeyboardInterrupt), csprng_shim(substream(1, "csprng")):
        pass
    assert _all_real()


def test_an_interrupt_right_after_an_audit_install_still_restores_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    surfaces = len(_audit._ENTROPY_SURFACES)
    calendar = sum(1 for name, _ in _audit._CURRENT_TIME_FUNCS if hasattr(time, name))
    # the first entropy surface, the first calendar function, and the thread start (the last)
    for call in (1, surfaces + 1, surfaces + calendar + 1):
        monkeypatch.setattr(_audit, "_install", _interrupting_after(call, _entropy._install))
        with pytest.raises(KeyboardInterrupt), seedloop.audit_mode():
            pass
        assert _all_real(), call


def test_to_thread_check_with_audit_leaves_the_host_loop_working() -> None:
    code = """
import asyncio, time, seedloop

async def scenario(world):
    await asyncio.sleep(1)

async def main():
    task = asyncio.ensure_future(asyncio.to_thread(seedloop.check, scenario, seeds=400, audit=True))
    while not task.done():
        await asyncio.sleep(0)  # the host loop reads the real clock while the check runs
    result = await task
    print("done", result.failing_seed, time.monotonic() > 0)

asyncio.run(main())
"""
    out = run_isolated(code, timeout=60)
    assert "done None True" in out.stdout, out.stderr[-600:]


def test_a_run_started_while_another_is_active_waits_and_restores_in_order() -> None:
    inside = threading.Event()
    release_first = threading.Event()
    release_second = threading.Event()

    def first() -> None:
        with csprng_shim(substream(1, "csprng")):
            inside.set()
            release_first.wait()

    def second() -> None:
        with csprng_shim(substream(2, "csprng")):
            release_second.wait()

    one = threading.Thread(target=first)
    two = threading.Thread(target=second)
    try:
        one.start()
        inside.wait()
        two.start()
        time.sleep(0.2)  # the second run waits for the lock, or (without it) is already inside
        release_first.set()
        one.join()
        release_second.set()
        two.join()
        assert _all_real()
    finally:
        release_first.set()
        release_second.set()
        _restore_real()
