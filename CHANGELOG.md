# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.2] — 2026-07-05

### Added
- CI-tested support for CPython 3.14: the full gate matrix (lint, format, types, tests, demo) runs
  on 3.12–3.14 across Linux, Windows, and macOS, and the package declares the 3.14 classifier.

### Fixed
- **The README's leading example could not run.** It called the seed-scheduled fault API
  (`world.run_for`, fault handles) that is still a design target, so pasting it against the
  installed release died with `AttributeError`. The example now uses the implemented API —
  scenario-driven `world.net.partition`/`heal` with virtual-time sleeps — and runs as written;
  `run_for` stays an explicitly deferred item.
- **Teardown left crashed-node exceptions unretrieved.** When the scenario itself raised, or when a
  second started node failed behind the one surfaced, the extra task exceptions were never read and
  asyncio logged "Task exception was never retrieved" at garbage collection. Teardown now retrieves
  every non-cancelled started task's exception after cancellation; which exception a failing run
  raises is unchanged.

## [0.3.1] — 2026-06-30

Determinism and boundary-fidelity fixes found by an independent audit. All three were silent: a
clean run looked correct while a guarantee leaked.

### Fixed
- **Teardown cancellation was nondeterministic.** Tasks still pending when a scenario returned were
  cancelled in `asyncio.all_tasks()` order — a set iterated in `id()`-hash order, which varies per
  process. A node that recorded in its cancel handler then produced a run-varying timeline, breaking
  "same seed → same timeline". The loop now stamps every task with a creation index and the World
  cancels in that deterministic order.
- **A network fault could shift other messages' latencies.** Latency was drawn only when a delivery
  was scheduled, so a dropped message skipped its `net` draw and a duplicate took an extra one,
  shifting every later message's latency. `send` now draws its `net` latency once, before any fault
  decision; a duplicate's extra delivery draws from the `faults` sub-stream. Enabling a fault no
  longer perturbs surviving messages — the independence the docs always claimed.
- **The audit-mode clock tripwires were incomplete.** `time.process_time`/`thread_time` (and their
  `_ns` forms) and current-time calendar reads (`gmtime`/`localtime`/`ctime`/`asctime`/`strftime`
  with no explicit timestamp) passed clean despite the docs claiming real time was closed. They now
  trip; the same functions given an explicit timestamp stay pure conversions and still work.

## [0.3.0] — 2026-06-28

Phase 3: ergonomics and the worked proof — the invariant API, the Raft demo, and the enforced boundary.

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
