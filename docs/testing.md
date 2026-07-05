# Verification strategy

How seedloop proves it works, rather than claiming it. The product *is* a determinism guarantee, so the
central test is not "does the code run" but "does the same seed produce the same timeline" — and that is
proven by replay, never asserted in prose. This document is the test design the build is held to; the
per-slice test matrices live in the implementation plan.

## The central proof: replay equivalence

A run records a **timeline**. In Phase 1 it is user-driven: `world.record(event)` appends a
`(virtual_time, event)` pair, so a scenario logs the decisions whose reproducibility it cares about (see
[internals.md](internals.md)). The determinism guarantee is one assertion over it:

```
run a seed → timeline A
run the same seed again → timeline B
assert A == B            # identical
```

If the two ever differ, an entropy source escaped the World and the test fails loudly with the diverging
event. This single harness is what turns "deterministic" from a promise into a checked property. Because
Phase-1 scheduling is deterministic (faithful FIFO + virtual clock), a scenario that records its `rng`
draws and timed actions captures everything that can vary. Later phases add the simulated network and
faults to what is recorded — and an automatic per-event recorder with stable ids — so the same equality
test covers them. A failing seed found by `check` satisfies it too: `replay(seed)` reproduces the run
exactly.

When the automatic recorder lands (Phase 2), a **completeness meta-test** guards it — inject a known
nondeterminism (an unseeded `random` draw the recorder should reflect) and assert the timeline
*diverges*; if it does not, an event kind is missing. In Phase 1 the equivalent guard is that the
replay-equivalence scenarios record their `rng` draws, so an entropy leak shows up directly.

A companion **independence** check guards ADR-0009: changing how many values one component draws (e.g.
adding a network duplication draw) must not change another component's stream for the same seed — verified by
fixing a seed, recording each sub-stream's draws, and asserting they are unaffected by an unrelated
change.

## Test categories

- **Unit** — the loop's structures in isolation: `deque` FIFO append/popleft, `heapq` tie-break by
  `(when, seq)`, `call_soon`/`call_later`/`call_at` scheduling, autojump (all-blocked → next-timer),
  deadlock detection
  (quiescent with awaiters → raises), cancellation, exception propagation.
- **Replay equivalence** — the harness above, run per phase on representative scenarios. This is the gate
  that proves determinism; a slice claiming a new entropy source is controlled adds a replay test that
  exercises it.
- **Property-based / metamorphic** (Hypothesis, ADR-0004) — generate seeds and scenario inputs, assert
  invariants hold and that `replay` of any generated seed reproduces its outcome. Metamorphic relation:
  reordering independent `send`s in source must not change a timeline the seed already determines.
  Hypothesis also shrinks a failing case to a minimal input. Ships as the optional `seedloop[hypothesis]`
  extra (`seedloop.hypothesis.seeds` / `given_seed`) to keep the core dependency-free (ADR-0017, ADR-0020).
- **Fault injection** — partitions, slow links, drops, duplications, crashes fire on the seed's schedule;
  tests assert the same seed injects the same faults at the same virtual times, and that a known
  partition-dependent bug is surfaced and then replayed identically (the Phase 2 payoff).
- **Boundary** — out-of-boundary use inside a run (`threading`, `run_in_executor`, subprocess, real
  socket, `uvloop`, real `time`/`os.urandom`) raises `BoundaryError`/`EntropyLeakError`, never runs
  silently (ADR-0002, ADR-0008). Each rejection has a test.
- **End-to-end** — the Raft demo: a real concurrency bug found under partition, reported as a seed, and
  replayed in one command. This is both the framework's test and the thing a reviewer runs.

## seedloop's own suite is deterministic

A tool that hunts flaky tests may not be flaky itself. The suite is seeded and order-independent: tests
that involve randomness fix their seed, the timeline assertions are exact, and the run is checked under
randomized test ordering (e.g. `pytest-randomly`) so no test depends on another's side effects. A flaky
result in seedloop's own CI is treated as a defect in seedloop, not retried away.

## CI matrix

CI runs the four gates on every push and PR (ADR/Roadmap Phase 0):

- `ruff check .` and `ruff format --check .` — lint and format.
- `mypy .` — full type check; the public API is explicitly typed.
- `pytest -q` — the suite, including replay-equivalence and boundary tests.

Across a matrix of **OS × Python**: Linux, Windows, macOS × the supported CPython versions (3.12+, since
`loop_factory` is the attach mechanism). The cross-OS run matters specifically for determinism: a
timeline that differs between Linux and Windows for the same seed would expose a hidden platform entropy
source (path hashing, default encodings), so equality must hold across the matrix, not just on one OS.

## What each check guarantees

| Check | What it guarantees |
|-------|--------------------|
| `ruff check` / `ruff format` | lint and formatting are clean |
| `mypy` | the public API is fully and correctly typed |
| `pytest -q` | logic is correct, including edge and adversarial cases |
| Replay-equivalence harness | determinism is proven, not assumed |
| Boundary tests | out-of-boundary use is rejected, never run silently |

Coverage is judged by whether the entropy paths and boundary rejections are exercised, not by a headline
percentage — a high number over trivial tests proves nothing.
