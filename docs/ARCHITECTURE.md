# Architecture

How `seedloop` makes Python `asyncio` deterministic, how a run works end to end, and the phased build
that gets there. This is the design target; nothing is built yet. The determinism boundary — what is
controlled and what is deliberately not — is in [scope.md](scope.md), and the reasoning behind the
non-obvious choices is in [decisions.md](decisions.md).

## The key fact that makes this tractable

`asyncio`'s event loop is already single-threaded and cooperative, and its scheduling core is
deterministic *by construction*. In CPython, the ready queue is a FIFO `collections.deque`:
`call_soon` appends, `_run_once` drains with `popleft`, so callbacks run in registration order.
Timers are a `heapq` ordered by deadline — though equal deadlines have no deterministic tie-break in
stock CPython (`TimerHandle` compares the deadline alone), one thing seedloop adds. The single place
nondeterminism enters the loop is `selector.select()` — the OS poll that decides which sockets are
ready and when.

So `seedloop` does not patch CPython or fight the runtime. It supplies a custom loop via
`asyncio`'s `loop_factory` (the supported mechanism; the older event-loop *policy* API is deprecated and
slated for removal in a future release) that replaces the one nondeterministic seam — the I/O poll —
with a virtual scheduler, and drives time from a virtual clock. Because the rest of the loop is already
deterministic, making it *seedable* is a matter of controlling that one seam plus the clock. This is
the direct analogue of why Rust's `madsim` can swap out `tokio`: a single-threaded cooperative
scheduler is replaceable in a way Go's thread-multiplexed scheduler is not.

## How the custom loop attaches

`loop_factory` is a callable that returns the loop a run uses; `asyncio.Runner(loop_factory=...)` and
`asyncio.run(coro, loop_factory=...)` both accept one (confirmed on the target CPython). seedloop's
factory returns its deterministic loop, so user code that calls `asyncio.run` or awaits as usual never
sees the swap — it talks to the standard `asyncio` API, and that API is now backed by the simulator.

The loop implements the slice of the `AbstractEventLoop` surface that `Task` and coroutines actually
drive — `call_soon`, `call_later`, `call_at`, `time`, `create_task`/`create_future`, and exception
handling — not the whole interface (the abstract base declares dozens of methods, most of them real-I/O
entry points). The real-I/O methods (`sock_*`, `getaddrinfo`, `connect_*`, subprocess and signal
hooks) are deliberately *not* implemented as working transports: they are the boundary, and calling one
inside a run is rejected rather than silently run (see [scope.md](scope.md)). Internally the loop keeps
the same two structures CPython's loop does — a `deque` ready-queue drained FIFO (preserved, so
`call_soon` ordering stays faithful to `asyncio`) and a `heapq` of timers — and adds two things: it
replaces the `select()` poll with the virtual scheduler and clock, and it keys timers by `(deadline,
seq)` so equal deadlines fire deterministically. The seed enters through the replaced I/O seam — the
simulated network's delivery timing — not by reordering ready callbacks (ADR-0012). The full
method-by-method surface and the autojump algorithm are in [internals.md](internals.md).

## Components

```
                         ┌──────────────────────────────────────────────┐
                         │                  Controller                   │
                         │   seed sweep · invariants · record / replay   │
                         └───────────────────────┬──────────────────────┘
                                                 │ runs one seed
                                                 ▼
   ┌──────────────────────────────────────────────────────────────────────────────┐
   │                              World  (one deterministic run)                    │
   │                                                                                │
   │   deterministic event loop ── in-order task scheduling (replaces select())     │
   │   virtual clock           ── sleep / timeouts advance simulated time instantly │
   │   seeded RNG              ── one seed drives all randomness                     │
   │   simulated network       ── abstract transport: latency, reorder, drop,       │
   │                              partition — all decided by the seed               │
   │   fault scheduler         ── injects partitions / slow links / crashes by seed │
   └──────────────────────────────────────────────────────────────────────────────┘
                                                 ▲
                                                 │ user code talks to
                         ┌──────────────────────────────────────────────┐
                         │   your protocol / algorithm (sans-I/O style)  │
                         │   sends + receives via world.net, not sockets │
                         └──────────────────────────────────────────────┘
```

- **Controller** — the outer driver. Runs a scenario across many seeds, checks the user's invariants
  after each run, and on a failure reports the seed and supports replaying it. May use
  [Hypothesis](https://hypothesis.readthedocs.io/) as the seed-generation and shrinking engine rather
  than reinventing it.
- **World** — everything for a single deterministic run, all derived from one seed: the loop, the
  clock, the RNG, the network, the fault schedule. The same seed yields the same World, hence the same
  timeline.
- **Deterministic event loop** — the custom loop. Owns task scheduling and keeps its order faithful to
  `asyncio` (`call_soon` FIFO); never touches a real socket or the real clock. The seed drives the I/O
  seam — the simulated network's delivery timing — not the scheduling order (ADR-0012).
- **Virtual clock** — `loop.time()` returns simulated monotonic time. When every task is blocked, the
  clock jumps to the next scheduled timeout (the autojump design from `trio`'s `MockClock`), so a
  ten-second scenario runs in milliseconds and time is fully under control.
- **Simulated network** — the controllable seam. Nodes exchange messages through an in-memory
  transport, not real sockets; the seed decides latency, reordering, drops, and partitions.
- **Fault scheduler** — turns "chaos" into a reproducible function of the seed: which links partition,
  when, for how long, which node pauses.

## How a run works

```
controller.check(scenario, seeds=N):
  for seed in seeds:
     world = World(seed)                      # loop + clock + rng + net + faults, all from seed
     run scenario(world) on the deterministic loop
        - user code sends/receives via world.net
        - the loop advances tasks deterministically (FIFO); message delivery timing is seed-determined
        - the clock autojumps; faults fire on the seed's schedule
     evaluate user invariants
     if an invariant fails:
        report seed S  →  controller.replay(scenario, seed=S) reproduces it exactly, forever
```

The guarantee is the equation **same seed → same timeline → same outcome**. A failure is therefore
not an event you hope to catch again; it is an integer you keep.

## Lineage

The design is a port of proven systems, not speculation:

- **FoundationDB / Flow** — the single-threaded simulation loop and `BUGGIFY` fault injection; FDB ran
  exclusively in simulation for ~18 months before touching real storage.
- **`madsim` / `turmoil` (Rust)** — the runtime-swap model (`madsim` wraps `tokio`) and the virtual
  network topology with seeded faults.
- **`trio`'s `MockClock`** — the virtual-clock-with-autojump design, ported onto `asyncio`.

`seedloop` is the assembly of these into `asyncio` — which, being single-threaded and FIFO, is an
easier substrate than Go and a comparable one to Rust.

## Phased build

Each phase is independently useful, so the project delivers value before it is "finished."

1. **Deterministic core** — the custom loop with deterministic scheduling, the virtual clock, seeded RNG,
   and seed replay. *Already useful on its own:* "make your `asyncio` tests reproducible and instant."
2. **Simulated network + fault injection** — the abstract transport with seeded latency, reordering,
   loss, and partitions. *This is the DST payoff:* hunt for and reproduce partition/timing bugs.
3. **Ergonomics** — the invariant/assertion API, Hypothesis integration for seed exploration and
   shrinking, and a non-determinism auditor that flags entropy leaks (uncontrolled `os.urandom`, real
   threads, real time) so users learn where their code steps outside the supported boundary.

## The demonstration

The repository ships a worked example that is also the proof: a small Raft (or CRDT) implementation in
pure Python, run under `seedloop`, with a real concurrency bug found under partition and replayed from
its seed — the same failure, every time. That is both the test of the framework and the thing a
reviewer can run in one command.
