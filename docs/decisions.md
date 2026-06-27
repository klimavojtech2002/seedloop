# Decision records

Short records of the non-obvious choices in `seedloop` and the reasoning behind them, so a reviewer
can see *why*, not just *what*. Several of these are scoping decisions that were made by adversarially
trying to disprove the project first — the boundary in [scope.md](scope.md) is their consequence.

Format: **Context → Decision → Consequences.** Status is `Accepted` unless noted.

---

## ADR-0001 — Abstract transport (sans-I/O), not real sockets

**Status:** Accepted

**Context.** A DST tool could try to drive *real* network I/O through its custom event loop, so that
unmodified apps "just work." Investigated honestly, that path is a multi-person-year effort with leaks
even then: it requires implementing enough of CPython's private, unstable `BaseEventLoop` interface to
satisfy real drivers; building a faithful TCP/IP simulation (its own multi-month subsystem — `madsim`
had to fork `tokio` *and* `tokio-postgres`); and it still loses determinism wherever a C extension
reads the clock or OS RNG directly, invisibly to a Python-level shim. Whole-program determinism is why
Antithesis works at the hypervisor level, intercepting every syscall — not something a library can do.

**Decision.** `seedloop` does not drive real I/O. Users write their protocol against an **abstract
transport** (the sans-I/O pattern); `seedloop` supplies a simulated implementation. The controllable
seam is small, pure-Python, and tool-defined.

**Consequences.**
- Dodges the two open-ended hard parts (a correct custom loop over private APIs, and a faithful TCP
  stack), bringing the project into solo, ~2–4-month reach.
- The determinism guarantee is real because the seam is fully controlled.
- Trade-off: code wired directly to real drivers must first be refactored to sans-I/O to be testable.
  That refactor is exactly what makes code testable *and* reusable — DST and sans-I/O are natural
  partners — so the constraint is a feature, stated plainly rather than hidden.

---

## ADR-0002 — Single-threaded `asyncio` only; threads and `multiprocessing` are out

**Status:** Accepted

**Context.** Determinism requires one cooperative thread of control. Real OS threads preempt at
unobservable points and the GIL does not make interleavings reproducible; `multiprocessing` has
separate memory and OS scheduling. This is the same wall that blocks DST in Go.

**Decision.** Target single-threaded `asyncio`. Inside a simulated run, reject or warn on
`run_in_executor`, `threading`, and subprocess use rather than pretending to control them.

**Consequences.**
- The supported model is exactly the one where determinism is achievable; `asyncio`'s single-threaded,
  FIFO event loop is an unusually good substrate (better than Go, comparable to Rust).
- Trade-off: thread-pool-bound code is not covered. Acceptable — that code is not what DST is for, and
  claiming otherwise would break the guarantee.

---

## ADR-0003 — The seed is the reproduction

**Status:** Accepted

**Context.** The entire value of DST is turning an irreproducible failure into a reproducible one.

**Decision.** A run is a pure function of its seed: the loop, clock, RNG, network, and fault schedule
are all derived from it. The contract is **same seed → same timeline → same outcome.** A failing run
reports its seed; `replay(scenario, seed)` reproduces the exact timeline on demand.

**Consequences.**
- Debugging a concurrency bug becomes ordinary: replay the seed under a debugger as many times as
  needed.
- Requires total discipline about entropy — any uncontrolled source (see scope.md) breaks the
  contract, which is why the boundary is drawn so strictly and a non-determinism auditor is planned.

---

## ADR-0004 — Build the runtime; borrow the exploration engine (Hypothesis)

**Status:** Accepted

**Context.** DST has two halves: a deterministic *runtime* (the hard, missing piece in Python) and an
*explorer* that generates many cases and shrinks failures to a minimal one. Python already has a
mature explorer in Hypothesis (seeded generation + shrinking), but no deterministic runtime.

**Decision.** `seedloop` builds the runtime — the genuinely unbuilt part — and integrates with
Hypothesis as the seed-exploration and shrinking driver rather than reinventing it.

**Consequences.**
- Effort concentrates on the novel contribution; case generation and minimization come from a mature
  library.
- Trade-off: a dependency and an integration boundary to keep clean — worth it to avoid rebuilding a
  hard, solved problem.

---

## ADR-0005 — Virtual time with autojump, owned by the loop

**Status:** Accepted

**Context.** Tests of timing-sensitive logic must control time, and must not actually wait. Globally
freezing the clock (e.g. `freezegun`) breaks `asyncio`, which needs a working `monotonic()`.

**Decision.** The loop owns a virtual clock: `loop.time()` returns simulated monotonic time, and when
every task is blocked the clock jumps to the next scheduled timeout — the autojump design from `trio`'s
`MockClock`, ported onto `asyncio`.

**Consequences.**
- A ten-second scenario runs in milliseconds; time is fully deterministic and under the seed's control.
- Sidesteps the known `freezegun`/`asyncio` conflict by owning time inside the loop rather than
  patching it globally.

---

## ADR-0006 — Datagram transport by default; reliable, ordered delivery is opt-in

**Status:** Accepted

**Context.** The simulated transport could model anything from a bare message channel to a faithful
TCP stream (byte sequencing, connection lifecycle, flow control). Full TCP fidelity is a multi-month
subsystem on its own — it is why `madsim` had to fork both `tokio` and `tokio-postgres` — and it
re-opens the open-ended work ADR-0001 was drawn to avoid. The protocols seedloop targets (Raft, gossip,
CRDTs) are specified against an *unreliable* network and provide their own retries and ordering; the
faults DST exists to inject — loss, reordering, duplication, delay — are exactly the ones an unreliable
datagram channel exposes.

**Decision.** The default transport is an unreliable datagram channel: discrete messages, where the
seed decides latency, reordering, duplication, drop, and partitions. A **reliable, ordered** channel is
available per link as an opt-in policy for protocols that legitimately assume per-connection ordering —
implemented as a delivery policy over the same channel, not a TCP stack.

**Consequences.**
- The modeled surface stays small and pure-Python, keeping the determinism seam fully controlled.
- The default matches what consensus/replication code already assumes, so the demo is honest.
- Opt-in ordering covers TCP-assuming protocols without building TCP; the cost is one delivery-policy
  flag, not a stream implementation.
- Trade-off: byte-stream semantics (partial reads, Nagle, backpressure) are not modeled. Out of scope by
  the same reasoning as ADR-0001 — and stated, not hidden.

---

## ADR-0007 — User code talks to an addressed message-passing port, not a stream

**Status:** Accepted

**Context.** ADR-0001 fixes the *sans-I/O* boundary; this ADR fixes its shape. The options were a
byte-stream interface (simulated `StreamReader`/`StreamWriter`), explicit per-link channel objects, or
address-based message passing. A stream interface drags in the TCP fidelity ADR-0006 rejects and couples
the tool to `asyncio`'s stream internals; per-link channel objects add ceremony that node-addressed
protocols (Raft broadcasts to peers) do not want.

**Decision.** User code sends and receives **typed messages through an addressed transport port**: a node
binds an address (`endpoint = world.net.bind(addr)`), then `await endpoint.send(dst, msg)` enqueues a
message and `await endpoint.recv()` receives the next one. The port is a small abstract interface (a
`Protocol`); seedloop supplies the deterministic implementation, and a production user could supply a
real one.

**Consequences.**
- This is the ports-and-adapters boundary in its plainest form, which is exactly what makes the code
  both testable and reusable.
- It is consistent with the discrete-message model of ADR-0006 and with the README example, which the
  API spec (`api.md`) now treats as a commitment rather than an illustration.
- Trade-off: code written against `StreamReader`/`StreamWriter` must adapt to the message port to be
  tested — the same refactor ADR-0001 already requires.

---

## ADR-0008 — The non-determinism auditor uses runtime tripwires, not static scanning

**Status:** Accepted

**Context.** Phase 3 plans an auditor that catches entropy leaking past the boundary (uncontrolled
`os.urandom`, real time, threads, real sockets). It could scan user source statically, trip at runtime,
or both. Static analysis of arbitrary Python is unreliable — dynamic imports and `getattr` defeat it,
and it reports leaks that never execute.

**Decision.** The auditor works by **runtime tripwires**. The World already owns every entropy source;
in audit mode the same interception points raise a specific `EntropyLeakError` (or warn) instead of
silently substituting a seeded source. A leak therefore surfaces as a loud, reproducible failure on the
seed that triggered it.

**Consequences.**
- Consistent with the project's rule that determinism is *proven*, not asserted: a leak fails a run.
- Near-free — it reuses the shims the World installs anyway.
- No false positives from unexecuted code.
- Trade-off: a leak on a path no test exercises is not caught. Accepted; the seed sweep is what drives
  paths, and static scanning is deferred (below) rather than relied upon.

---

## ADR-0009 — Entropy is derived by hierarchical seed-splitting, not one shared stream

**Status:** Accepted

**Context.** Every component that needs randomness (network latency and delivery order, the fault
schedule, the CSPRNG shim, user RNG) draws from the run's seed. If they all pull from one shared PRNG
stream, the sequence each component sees depends on how many values every *other* component happened to
draw — so adding or reordering a draw anywhere silently changes every downstream timeline, and a recorded
seed stops reproducing across versions.

**Decision.** The root seed is **split into independent named sub-streams**, one per component, derived
deterministically from the root (e.g. by hashing the root seed with a stable component label). Each
component draws only from its own stream.

**Consequences.**
- Components are isolated: adding a draw in one does not perturb the others' sequences.
- Replay is far more stable across code changes, which ADR-0011 depends on.
- Trade-off: every entropy consumer must take its stream from the World rather than calling `random`
  directly — enforced by the boundary and the auditor (ADR-0008), so it is a discipline the design wants
  anyway.

---

## ADR-0010 — `PYTHONHASHSEED` is pinned by a re-exec launcher

**Status:** Accepted

**Context.** Hash randomization makes `set`/`dict` iteration order vary between processes. Iteration
order that reaches scheduling or message ordering is an entropy leak (the classic silent back-door noted
in the DST literature). Unlike every other source, this one is fixed by the interpreter *before* any
seedloop code runs, so it cannot be shimmed from inside the run. Verified during design: reassigning
`os.urandom` at runtime does **not** control `secrets`/`random`, because `random` binds `from os import
urandom as _urandom` at import — so the CSPRNG shim must patch `random._urandom` too, and hash order
must be fixed even earlier, at process start.

**Decision.** When a run needs a pinned hash seed, seedloop **re-runs the interpreter** with
`PYTHONHASHSEED` set to a fixed value derived from the run seed (confirmed: two child processes with the
same value hash identically; a different value differs). It re-runs `sys.orig_argv` — the full original
command, so a `-c`/`-m` invocation is reproduced, not just `python script.py`. POSIX replaces the process
in place (`os.execve`); Windows has no in-place `exec`, so it spawns the child and exits with the child's
return code (verified on Windows: in-place `execve` there loses output and the exit code). A guard env var
prevents infinite recursion. The CSPRNG shim separately patches both `os.urandom` and `random._urandom`.

**Consequences.**
- Hash-order entropy is removed at the only point it can be — before interpreter start.
- The primary guarantee is that seedloop's own library code never depends on hash order; the pin is a
  backstop for *user* code that does. A replayed seed re-derives the same `PYTHONHASHSEED` it had when
  found, so reproduction holds regardless of whether a sweep pins once per child or per seed — that
  execution detail is settled in the build, not a correctness question, precisely because library code
  does not rely on the value.
- Cost: a one-time process re-exec at the boundary; invisible to user code.
- Trade-off: a run that pins the hash seed starts a child process. Acceptable and bounded; the
  alternative is an uncontrollable leak.

---

## ADR-0011 — Replay is stable within a major version, not across

**Status:** Accepted

**Context.** "The seed is the reproduction" (ADR-0003) is only useful if a recorded seed still
reproduces later. But any change to scheduling, the fault model, or entropy derivation can change the
timeline a seed produces. Promising cross-version stability would freeze the internals; promising nothing
would make recorded seeds worthless.

**Decision.** Within a **major version**, the same seed yields the same timeline and outcome; this is a
supported contract, covered by replay tests. **Across major versions** it is not guaranteed — a changelog
entry calls out any timeline-affecting change. The hierarchical seed-splitting (ADR-0009) is what makes
intra-version stability hold under ordinary refactors.

**Consequences.**
- Users can trust a recorded seed within the version they found it on, and know exactly when that trust
  resets.
- The internals stay free to improve across majors without a false stability promise.
- Trade-off: a long-lived seed must be re-pinned after a major upgrade. Stated in `api.md`, not implied.

---

## ADR-0012 — Preserve asyncio's `call_soon` FIFO order; explore interleavings through I/O timing

**Status:** Accepted

**Context.** A DST loop could randomize the order in which already-ready callbacks run — as `madsim` does
on `tokio` — to explore more interleavings. But `asyncio` *documents* `call_soon` as running callbacks in
registration order, and correct code is allowed to rely on it. Permuting that order would manufacture
"failures" from code that is in fact correct, and make seedloop an unfaithful `asyncio` loop. `tokio` has
no equivalent documented cross-task ordering contract, so its model does not transfer.

**Decision.** seedloop preserves `call_soon` FIFO order and adds only the `(when, seq)` timer tie-break
for equal deadlines (ADR-0009 covers entropy derivation; this is a deterministic counter, not RNG). It
injects nondeterminism where a real deployment actually has it — the timing and order of I/O readiness,
modelled as the simulated network's seeded delivery (ADR-0006) — not by reordering ready callbacks.
Interleaving exploration is therefore a Phase-2 (network) capability; Phase 1 delivers reproducible,
instant runs, exactly as the roadmap frames the phases.

**Consequences.**
- seedloop stays a faithful drop-in `asyncio` loop. A run that passes then fails across seeds reflects
  real I/O-timing nondeterminism, not a broken runtime contract — so no false positives from legitimate
  `call_soon` reliance.
- The exploration lever is the network model, where production async nondeterminism actually lives — the
  same stance as FoundationDB (simulate the network/disk, run the logic deterministically).
- Trade-off: interleavings reachable *only* by reordering same-instant ready callbacks are not explored.
  Accepted — a correct `asyncio` program is never exposed to that reordering, so exploring it would report
  non-bugs. A future opt-in "aggressive scheduling" mode could add it, carrying this caveat; deferred.

---

## ADR-0013 — Subclass `BaseEventLoop`; replace only the `select()` seam

**Status:** Accepted

**Context.** The deterministic loop could be written from scratch against `AbstractEventLoop`, or built
by subclassing `asyncio.BaseEventLoop` and overriding the parts that touch I/O. asyncio's scheduling
core — the `call_soon` FIFO ready queue, `run_until_complete`, Task/Future integration, and the
running-loop and async-generator bookkeeping — is already deterministic and is the same code every
asyncio program runs; the only nondeterministic seam is the I/O poll inside `_run_once`
(`selector.select()`).

**Decision.** seedloop subclasses `BaseEventLoop` and overrides only `_run_once` to drop the poll (plus
`time()`, and inert `_process_events`/`_write_to_self`). The real-I/O entry points — `run_in_executor`,
`sock_*`, `getaddrinfo`, `add_reader`/`add_writer`, `create_connection`/`create_server`,
`call_soon_threadsafe` — are overridden to raise `BoundaryError`. `BaseEventLoop`, unlike
`BaseSelectorEventLoop`, creates no selector and no self-pipe, so no real socket exists in the loop.

**Consequences.**
- The scheduling semantics are CPython's own, tested semantics — the premise of the whole project
  ("asyncio's scheduling is already deterministic; we replace one seam"). Reimplementing them from
  scratch would risk diverging from asyncio and weakening the faithful-loop claim.
- The effective surface is the scheduling slice; the I/O surface raises — matching `scope.md` by
  construction.
- Trade-off: the loop touches `BaseEventLoop` internals the type stubs do not expose — `_check_closed`,
  `_ready`, and `_stopping` — so a few localized `# type: ignore[attr-defined]` are needed; we are a
  `BaseEventLoop` subclass mirroring its own `_run_once`.

---

## ADR-0014 — `pytest-timeout` for hang-safe tests

**Status:** Accepted

**Context.** A bug in the event loop (a broken autojump, a missing timer promotion) can livelock a
simulated run — a real `asyncio` program would hang there too. Under plain `pytest` a hanging test blocks
the whole suite and CI indefinitely instead of failing. The deterministic-core slices need a real-time
safety net so a hang regression fails fast.

**Decision.** Add `pytest-timeout` as a **dev-only** dependency with a 30-second per-test cap
(`[tool.pytest.ini_options] timeout = 30`). It is test infrastructure, not a runtime dependency; the
shipped package still has zero third-party dependencies.

**Consequences.**
- A hang becomes a fast, visible test failure instead of an indefinite block (verified: a planted
  autojump bug now trips the cap).
- No runtime impact — `pytest-timeout` is in the `dev` extra only.
- Trade-off: a legitimately slow test would trip the cap; a non-issue here, since the whole suite runs in
  virtual time and finishes in milliseconds.

---

## ADR-0015 — `check`/`replay` do not pin `PYTHONHASHSEED`; pinning is opt-in

**Status:** Accepted

**Context.** ADR-0010 gives a launcher that pins `PYTHONHASHSEED` by re-running the interpreter. The
0130 plan leaned toward `check` re-execing once into a pinned child and sweeping there. Building it
surfaced the problem: the launcher re-runs the *whole* process, and `check` typically runs under a test
runner — implicitly re-execing `pytest` from inside a test is hostile (it restarts the entire run).

**Decision.** `check`/`replay` do **not** pin the hash seed. The determinism guarantee rests on
ADR-0010's primary point: seedloop's own library code never depends on hash order. A user whose *own*
code depends on `set`/`dict` iteration order can call `seedloop.ensure_hash_seed(seed)` at their entry
point (before `check`), where re-execing the process is expected and safe.

**Consequences.**
- No surprise process restarts inside a test runner; `check` sweeps in-process, building a fresh `World`
  per seed with no shared state.
- The hash-seed backstop is available but explicit, matching where the cost (a re-exec) is acceptable.
- Trade-off: a user relying on hash-ordered iteration without calling `ensure_hash_seed` could see a
  cross-process replay diverge. Documented; the in-process replay within one run is unaffected.

---

## Planned / deferred decisions

- **Auditor static-scan depth** — whether to add static detection of leak patterns *on top of* the
  runtime tripwires of ADR-0008. Deferred: tripwires carry the guarantee; a static layer is an
  ergonomics add to settle once Phase 3 is in use, not before.

*Resolved since the first draft:* **naming** — `seedloop` is adopted as the name (free on PyPI at the
time of writing; confirm at first release). **Reliable-channel fidelity** (ADR-0006 opt-in) — in-order,
no-loss, whole-message delivery only; no flow control or backpressure, as those are TCP fidelity and out
of scope by ADR-0001.
