# Verification strategy

How seedloop proves it works, rather than claiming it. The product *is* a determinism guarantee, so the
central test is not "does the code run" but "does the same seed produce the same timeline" — and that is
proven by replay, never asserted in prose. This document is the test design the project is held to.

## The central proof: replay equivalence

A run records a **timeline**. A scenario appends to it with `world.record(event)`, and the simulated
network and the fault scheduler add their own events (`send`, `deliver`, `drop`, `fault-*`) as they
happen (see [internals.md](internals.md)). The determinism guarantee is one assertion over it:

```
run a seed → timeline A
run the same seed again → timeline B
assert A == B            # identical
```

If the two ever differ, an entropy source escaped the World and the test fails loudly with the diverging
event. This single harness is what turns "deterministic" from a promise into a checked property. Task
scheduling is deterministic (faithful FIFO plus the virtual clock) and is not itself recorded, so a
scenario that records its `rng` draws and timed actions captures what can vary; the network and faults
add theirs. A failing seed found by `check` satisfies it too: `replay(seed)` reproduces the run exactly.

The timeline only shows what is recorded, so the replay-equivalence scenarios record their `rng` draws:
an entropy leak then shows up directly as a diverging event. An event kind that nothing records cannot
diverge, which is why the corpus and differential tests below compare whole timelines of many generated
programs.

A companion **independence** check guards ADR-0009: changing how many values one component draws (e.g.
adding a network duplication draw) must not change another component's stream for the same seed — verified by
fixing a seed, recording each sub-stream's draws, and asserting they are unaffected by an unrelated
change.

## Test categories

- **Unit** — the loop's structures in isolation: `deque` FIFO append/popleft, `heapq` tie-break by
  `(when, seq)`, `call_soon`/`call_later`/`call_at` scheduling, autojump (all-blocked → next-timer),
  deadlock detection
  (quiescent with awaiters → raises), cancellation, exception propagation.
- **Replay equivalence** — the harness above, run on representative scenarios. This is the gate
  that proves determinism; a change claiming a new entropy source is controlled adds a replay test that
  exercises it.
- **Replay-stability pin** — a committed golden timeline (`scripts/pin_replay.py` →
  `tests/data/replay_golden.txt`) records one canonical scenario's exact events at a fixed seed, checked
  byte-for-byte on the CI matrix. Where replay-equivalence proves stability *within a run*, the golden
  proves it *across commits*: a timeline-affecting change (ADR-0011) fails CI instead of landing silently.
- **Corpus pin** — `scripts/pin_corpus.py` runs well over a hundred small scenarios, most at one to three seeds (cancellation, timeouts,
  task groups, deadlocks, faults) and compares their timelines with `tests/data/corpus_golden.txt`.
  Where the replay-stability pin covers one scenario, the corpus covers the behaviours a change to the
  loop or the drive is most likely to disturb.
- **Differential test** — generated programs run under the repository's source and under the previous
  release, in separate processes, and their outcomes and timeline digests are compared
  (`tests/test_differential.py`). A difference is a behaviour change and is only accepted when the test
  declares it with its reason.
- **Packaging** — the source distribution is built from a clean copy and must carry every file its own
  test suite reads and none of the working tree's strays (`tests/test_packaging.py`).
- **Property-based** (Hypothesis, ADR-0004) — generate seeds and scenario inputs, assert invariants hold
  and that `replay` of any generated seed reproduces its outcome. Hypothesis also shrinks a failing case
  to a minimal input. Ships as the optional `seedloop[hypothesis]` extra (`seedloop.hypothesis.seeds` / `given_seed`) to keep the core dependency-free (ADR-0017, ADR-0020).
- **Fault injection** — partitions, slow links, drops, duplications, crashes fire on the seed's schedule;
  tests assert the same seed injects the same faults at the same virtual times, and that a known
  partition-dependent bug is surfaced and then replayed identically.
- **Boundary** — out-of-boundary use inside a run (`threading`, `run_in_executor`, subprocess, real
  socket, `uvloop`, real `time`/`os.urandom`) raises `BoundaryError`/`EntropyLeakError`, never runs
  silently (ADR-0002, ADR-0008). Each rejection has a test.
- **End-to-end** — the Raft demo: a real concurrency bug found under partition, reported as a seed, and
  replayed in one command. This is both the framework's test and the thing a reviewer runs.

## seedloop's own suite is deterministic

A tool that hunts flaky tests may not be flaky itself. The suite is seeded and does not depend on test
order: tests that involve randomness fix their seed, the timeline assertions are exact, and no test
relies on another's side effects. It passes with its files run in reverse order; CI does not randomize
the order. A flaky result in seedloop's own CI is treated as a defect in seedloop, not retried away.

## CI matrix

CI runs the four gates on every push and PR:

- `ruff check .` and `ruff format --check .` — lint and format.
- `mypy .` — full type check; the public API is explicitly typed.
- `pytest -q` — the suite, including replay-equivalence and boundary tests.

Across a matrix of **OS × Python**: Linux, Windows, macOS × CPython 3.12, 3.13 and 3.14. The cross-OS
run matters specifically for determinism: a
timeline that differs between Linux and Windows for the same seed would expose a hidden platform entropy
source (path hashing, default encodings), so equality must hold across the matrix, not just on one OS.

Separate CI jobs run the checks that are slow or need the network: `meta` (checks that the checks have
teeth, applying changes to a copy of the source), `differential` (against the previous release's wheel),
and `package` (builds the source distribution and the wheel, runs the suite from the unpacked
source distribution and the demo from the installed wheel).

A separate CI job runs the **mutation gate** (ADR-0019): `scripts/mutation_sweep.py` breaks the library
one change at a time and fails if the test suite does not catch it, so a passing suite means "a
regression would fail a test," not just "the lines ran." A survivor is either a real coverage gap, closed
with a new test, or a genuinely equivalent mutant, recorded with its reason in
`scripts/mutation_baseline.txt`.

## What each check guarantees

| Check | What it guarantees |
|-------|--------------------|
| `ruff check` / `ruff format` | lint and formatting are clean |
| `mypy` | the public API is fully and correctly typed |
| `pytest -q` | logic is correct, including edge and adversarial cases |
| Replay-equivalence harness | determinism is proven, not assumed |
| Corpus pin and replay-stability pin | a change that moves a timeline fails CI instead of landing silently |
| Differential test | behaviour matches the previous release, except for differences declared with a reason |
| Boundary tests | out-of-boundary use is rejected, never run silently |
| Mutation gate | a real code change would be caught by a test, not just that the lines ran |

Coverage is judged by whether the entropy paths and boundary rejections are exercised, not by a headline
percentage — a high number over trivial tests proves nothing.
