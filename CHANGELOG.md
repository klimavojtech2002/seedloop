# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project aims to follow
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Project skeleton: packaging (`pyproject.toml`), the typed `seedloop` package, a test scaffold, and CI
  running the lint, format, type, and test gates across Linux, Windows, and macOS on CPython 3.12 and 3.13.
- Deterministic event loop core: a single-threaded `asyncio` loop (attached via `loop_factory`) that
  drains `call_soon` in faithful FIFO order and rejects real I/O, threads, and subprocesses, with a
  deadlock guard for a quiescent run. Foundational; the public `World`/`check`/`replay` API follows.
- Virtual clock with autojump: `loop.time()` advances only by jumping to the next scheduled timer, so
  `asyncio.sleep` and timeouts resolve instantly with no real waiting; equal deadlines fire in scheduling
  order via a deterministic `(when, seq)` tie-break.
- Seeded entropy: independent per-component sub-streams derived from the run seed, a CSPRNG shim that
  routes `os.urandom` and `secrets` to the seed, and a launcher that pins `PYTHONHASHSEED` so set/dict
  iteration order is fixed.
