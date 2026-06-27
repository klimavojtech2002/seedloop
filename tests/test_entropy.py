"""Seeded entropy: sub-streams, the CSPRNG shim, and the hash-seed launcher (slice 0120)."""

from __future__ import annotations

import os
import random
import secrets
import subprocess
import sys
import textwrap

import pytest

from seedloop._entropy import (
    csprng_shim,
    ensure_hash_seed,
    hash_seed_for,
    substream,
)

# --- sub-streams (ADR-0009) ---


def test_substream_is_deterministic() -> None:
    first = [substream(7, "net").random() for _ in range(5)]
    second = [substream(7, "net").random() for _ in range(5)]
    assert first == second


def test_substreams_are_distinct() -> None:
    # Different labels give unrelated sequences for the same root seed.
    assert [substream(7, "net").random() for _ in range(5)] != [
        substream(7, "faults").random() for _ in range(5)
    ]


def test_substreams_are_independent() -> None:
    # The ADR-0009 property: drawing extra values from one component's stream does not change
    # another component's sequence for the same root seed. Each stream is derived separately, so
    # adding a draw in "net" cannot shift "faults".
    faults_baseline = [substream(7, "faults").random() for _ in range(5)]
    net = substream(7, "net")
    for _ in range(100):
        net.random()  # exhaust "net" heavily
    faults_after = [substream(7, "faults").random() for _ in range(5)]
    assert faults_after == faults_baseline


def test_substream_changes_with_seed() -> None:
    assert substream(1, "net").random() != substream(2, "net").random()


def test_substream_accepts_any_int_seed() -> None:
    # Canonical text encoding (not fixed-width bytes) accepts negative and >64-bit seeds.
    for seed in (-1, 0, 2**70, -(2**70)):
        assert isinstance(substream(seed, "net").random(), float)


# --- CSPRNG shim (ADR-0010) ---


def test_shim_controls_os_urandom() -> None:
    stream = substream(42, "csprng")
    with csprng_shim(stream):
        a = os.urandom(16)
    with csprng_shim(substream(42, "csprng")):
        b = os.urandom(16)
    assert a == b  # same seed -> same bytes
    assert len(a) == 16


def test_shim_controls_secrets_via_random_alias() -> None:
    # The point of the slice: shimming os.urandom alone would NOT control secrets, because random
    # binds `from os import urandom as _urandom` at import. The shim patches that alias too.
    with csprng_shim(substream(99, "csprng")):
        first = secrets.token_bytes(16)
    with csprng_shim(substream(99, "csprng")):
        second = secrets.token_bytes(16)
    assert first == second


def test_shim_restores_originals_on_normal_exit() -> None:
    orig_os = os.urandom
    orig_random = random._urandom  # type: ignore[attr-defined]
    with csprng_shim(substream(1, "csprng")):
        pass
    # After a clean exit the seeded source must be gone, or post-run code would silently get
    # predictable bytes from os.urandom/secrets.
    assert os.urandom is orig_os
    assert random._urandom is orig_random  # type: ignore[attr-defined]


def test_shim_restores_originals_even_on_error() -> None:
    orig_os = os.urandom
    orig_random = random._urandom  # type: ignore[attr-defined]
    with pytest.raises(RuntimeError), csprng_shim(substream(1, "csprng")):
        raise RuntimeError("boom")
    assert os.urandom is orig_os
    assert random._urandom is orig_random  # type: ignore[attr-defined]


def test_shim_zero_length() -> None:
    with csprng_shim(substream(1, "csprng")):
        assert os.urandom(0) == b""


# --- hash-seed launcher (ADR-0010) ---


def test_hash_seed_for_is_deterministic_and_in_range() -> None:
    assert hash_seed_for(123) == hash_seed_for(123)
    assert hash_seed_for(123) != hash_seed_for(124)
    assert 0 <= hash_seed_for(123) <= 0xFFFFFFFF  # valid PYTHONHASHSEED


def test_ensure_hash_seed_already_pinned_is_noop() -> None:
    # When the guard env var already matches, ensure_hash_seed returns without re-launching.
    target = str(hash_seed_for(5))
    env_key = "_SEEDLOOP_HASHSEED_REEXEC"
    saved = os.environ.get(env_key)
    os.environ[env_key] = target
    try:
        ensure_hash_seed(5)  # returns (no re-exec) because the guard already matches
    finally:
        if saved is None:
            del os.environ[env_key]
        else:
            os.environ[env_key] = saved


def _run_child(seed: int) -> str:
    # Drive the launcher in a fresh interpreter (never in the test process, which would re-exec
    # pytest). The child pins PYTHONHASHSEED via the launcher, then prints hash("seedloop").
    script = textwrap.dedent(f"""
        import sys
        sys.path.insert(0, {os.path.join(os.getcwd(), "src")!r})
        from seedloop._entropy import ensure_hash_seed
        ensure_hash_seed({seed})
        print(hash("seedloop"))
    """)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={k: v for k, v in os.environ.items() if k != "PYTHONHASHSEED"},
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout.strip()


def test_launcher_pins_hash_order_deterministically() -> None:
    # Same seed -> identical hash across processes; different seed -> different hash.
    assert _run_child(123) == _run_child(123)
    assert _run_child(123) != _run_child(456)
