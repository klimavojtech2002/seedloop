# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Invariant API: `world.always(predicate, *, name)` registers a continuous safety property checked after
  every step; the first step it is false raises `InvariantError(name)`, which `check` reports and `replay`
  reproduces. Checking is read-only — a run with a passing invariant has the same timeline as one without.
- The Raft demo (`python -m seedloop.demos.raft`): a small Raft leader election whose deliberate,
  toggled flaw (a missing single-vote rule) lets a seed sweep find a split-brain — two leaders in one
  term — and replay it from the seed; the corrected election passes the same sweep. The worked proof that
  seedloop finds and replays a real class of consensus bug.
- The non-determinism auditor: `check(scenario, ..., audit=True)` (and `seedloop.audit_mode()`) trips on
  uncontrolled entropy inside a run — real time, the unseeded global `random`, `os.urandom`/`secrets`,
  and a bare `threading.Thread` raise `EntropyLeakError`/`BoundaryError` instead of leaking silently, so
  the determinism boundary is enforced, not just stated.

## [0.2.0] — 2026-06-28

Phase 2: the simulated network and fault injection — the deterministic-simulation-testing payoff.

### Added
- Simulated datagram network (`world.net`): nodes `bind` an address and exchange messages through an
  addressed transport — no real socket. Delivery is a seeded timer, so message timing and reordering are
  a reproducible function of the seed, and send/deliver events join the timeline.
- Network faults: per-endpoint `loss` and `duplicate` probabilities (drawn from the seed), network
  `partition`/`heal`, and an opt-in `reliable=True` channel (no-loss, in-order). A partition- or
  loss-dependent bug can now be surfaced by `check` and replayed from the seed.

## [0.1.0] — 2026-06-26

Phase 1: the deterministic core — reproducible, instant `asyncio` runs.

### Added
- Project skeleton: packaging (`pyproject.toml`), the typed `seedloop` package, a test scaffold, and CI
  running the lint, format, type, and test gates across Linux, Windows, and macOS on CPython 3.12 and 3.13.
- Deterministic event loop core: a single-threaded `asyncio` loop (attached via `loop_factory`) that
  drains `call_soon` in faithful FIFO order and rejects real I/O, threads, and subprocesses, with a
  deadlock guard for a quiescent run.
- Virtual clock with autojump: `loop.time()` advances only by jumping to the next scheduled timer, so
  `asyncio.sleep` and timeouts resolve instantly with no real waiting; equal deadlines fire in scheduling
  order via a deterministic `(when, seq)` tie-break.
- Seeded entropy: independent per-component sub-streams derived from the run seed, a CSPRNG shim that
  routes `os.urandom` and `secrets` to the seed, and a launcher that pins `PYTHONHASHSEED` so set/dict
  iteration order is fixed.
- The public API: `World`, `seedloop.check(scenario, seeds=...)`, and `seedloop.replay(scenario,
  seed=...)`. A scenario runs against a seeded `World` (its `rng`, virtual clock, and timeline); `check`
  sweeps seeds and reports the first failing one; `replay` reproduces that seed's run identically.
