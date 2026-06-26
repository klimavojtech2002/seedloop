# Internals

How the deterministic core is built, in enough detail to implement it and to defend it. The public
behaviour is in [api.md](api.md); the boundary is in [scope.md](scope.md); the *why* of each
non-obvious choice is in [decisions.md](decisions.md). Vocabulary is fixed in [glossary.md](glossary.md).

Nothing here is built yet. The load-bearing CPython facts below were checked against the target
interpreter during design (CPython 3.13); where a claim depends on a version, it says so.

## The loop and what it implements

seedloop's loop is supplied through `asyncio`'s `loop_factory` — a callable returning the loop a run
uses, accepted by both `asyncio.Runner(loop_factory=...)` and `asyncio.run(coro, loop_factory=...)`
(verified present on 3.13). The user calls `asyncio.run`/`await` as normal; the factory swaps in the
deterministic loop underneath, so user code never sees the change.

In practice seedloop subclasses `asyncio.BaseEventLoop` and overrides only `_run_once` to drop the I/O
poll (ADR-0013), inheriting asyncio's tested scheduling, `run_until_complete`, and Task/Future machinery
unchanged. `BaseEventLoop` declares dozens of methods (≈55 public on 3.13), most of them real-I/O entry
points; the effective surface that works is the slice `Task` and coroutine execution actually drive:

- **Scheduling:** `call_soon`, `call_soon_threadsafe` (rejected — see boundary), `call_later`, `call_at`,
  `time`.
- **Tasks/futures:** `create_task`, `create_future`, `run_until_complete`, `run_forever`, `stop`,
  `is_running`, `close`.
- **Exceptions:** `call_exception_handler`, `set_exception_handler`, `default_exception_handler`,
  `get_debug`.

The real-I/O surface — `run_in_executor`/`call_soon_threadsafe` (threads), `sock_*` and
`add_reader`/`add_writer` (sockets), `getaddrinfo`/`getnameinfo` (DNS),
`create_connection`/`create_server`/`create_datagram_endpoint` (transports), subprocess and signal
hooks — is **not** implemented as working transport. The entry points that could otherwise act are
overridden to raise `BoundaryError` (ADR-0002); the rest inherit `BaseEventLoop`'s `NotImplementedError`.
Either way nothing real runs — which is the whole reason a custom loop is tractable here: we implement
the small cooperative core, not a real reactor.

## Two structures, and what seedloop adds

CPython's loop already uses the two structures seedloop needs; seedloop keeps them and adds a seeded
ordering to each:

- **Ready queue** — a `collections.deque`; `call_soon` appends, the step drains it left-to-right
  (`popleft`), so callbacks run in registration order. seedloop **preserves** this order: `asyncio`
  documents `call_soon` as FIFO and correct code may rely on it, so reordering it would manufacture
  failures from code that is in fact correct (ADR-0012). Interleavings are explored through *when* tasks
  become ready — the network's seeded delivery timing — not by shuffling ready callbacks. O(1)
  append/pop.
- **Timer heap** — a `heapq` of entries keyed by `(when, seq)`, where `when` is the virtual deadline and
  `seq` is a monotonic counter assigned when the timer is scheduled. CPython's `TimerHandle` orders by
  `_when` alone, so two timers with the *same* deadline fire in whatever order the heap happens to pop —
  not deterministic. seedloop adds the `seq` tie-break so equal-deadline timers fire in scheduling order,
  every run. O(log n) push/pop.

The timer tie-break is the one ordering seedloop adds. The one nondeterministic seam it must *replace*
is the I/O poll — and that is where the seed enters: the simulated network's delivery timing decides
when tasks become ready, which is the lever that explores interleavings (Phase 2).

## The step loop (replacing `select()`)

CPython's `_run_once` drains ready callbacks, then blocks in `selector.select(timeout)` until the next
timer or an I/O event. seedloop has no I/O and no real waiting, so the poll becomes a clock move:

```
def _run_once(self):
    # 1. Run every callback currently ready, in FIFO (registration) order.
    for _ in range(len(self._ready)):
        self._ready.popleft()._run()         # may schedule more work, run next step

    # 2. Ready queue drained. If timers remain, autojump the clock to the
    #    earliest deadline and promote every timer now due.
    if not self._ready:
        if self._scheduled:
            self._clock = self._scheduled[0]._when      # virtual time jumps; nobody sleeps
            while self._scheduled and self._scheduled[0]._when <= self._clock:
                self._ready.append(heappop(self._scheduled))
        elif not self._stopping:
            raise DeadlockError(...)          # nothing ready, no timers, run not complete
```

The run ends when the top-level coroutine completes. When the ready queue and the timer heap are both
empty while tasks are still awaiting, nothing can ever wake them — a **deadlock in the simulated
world** — and seedloop raises rather than hanging, because a real `asyncio` program would hang there and
a test must surface it.

This is the autojump of ADR-0005 / `trio`'s `MockClock`: time only moves when all work is blocked, and
then it moves straight to the next scheduled event. A 10-second `sleep` is a timer at `now+10`; when the
loop has nothing else to do it jumps there instantly.

## Virtual clock

`loop.time()` returns `self._clock`, a float of simulated monotonic seconds starting at 0. `call_later(d,
cb)` schedules a timer at `self._clock + d`; `call_at(t, cb)` at absolute virtual `t`. `asyncio.sleep`
is built on `call_later`, so it advances virtual time without waiting. No code in a run reads the OS
clock — `time.monotonic`/`time.time` inside a run are tripwires (ADR-0008), because a real-clock read is
an entropy leak that would make the run depend on wall time.

## Entropy: one seed, independent sub-streams

A run draws randomness from several places — network latency, delivery order, and loss; the fault
schedule; the CSPRNG shim; and the user's own `world.rng`. Drawing them all from one shared PRNG couples
them: the values one component sees would shift whenever another component changes how many values it
draws, breaking replay across refactors (ADR-0009).

Instead the root seed is **split into named sub-streams**, one per component:

```
def substream(root_seed: int, label: str) -> random.Random:
    material = f"{root_seed}:{label}".encode()           # canonical, width-independent
    digest = hashlib.blake2b(material, digest_size=32).digest()
    return random.Random(int.from_bytes(digest, "big"))
```

Each component (`"net"`, `"faults"`, `"user"`, and `"csprng"` for the CSPRNG shim below) gets
its own `random.Random`, independent of the others and stable for a given `(root_seed, label)`. Encoding
the seed canonically as text rather than fixed-width bytes means any `int` seed works — negative, or
larger than 64 bits. The derivation uses `hashlib`, never the builtin `hash()` — `hash()` is randomized
per process (the very thing ADR-0010 pins) and would make the split itself nondeterministic.

## Controlling the CSPRNG

`os.urandom`, `secrets`, and `random.SystemRandom` must yield from the seed during a run. A subtlety
caught during design: shimming `os.urandom` alone is **not enough**. `random` binds the function at
import time (`from os import urandom as _urandom`), and `secrets` draws through `random.SystemRandom`, so
a later reassignment of `os.urandom` does not reach it. Verified: with only `os.urandom` patched,
`secrets.token_bytes` still returned unshimmed bytes; patching `random._urandom` as well brought it under
control.

So the shim, installed for the duration of a run and removed after, patches **both** `os.urandom` and
`random._urandom`, routing each to the run's CSPRNG sub-stream. In audit mode the shim instead raises
`EntropyLeakError`, since CSPRNG use usually means the code wants real unpredictability — exactly what a
deterministic run cannot give.

## Pinning hash order

`PYTHONHASHSEED` randomizes `str`/`bytes` hashing, hence `set`/`dict` iteration order, per process. Any
such order that reaches scheduling or message handling is a silent entropy leak. It is the one source
fixed *before* interpreter start, so it cannot be shimmed from inside a run.

When a run needs it pinned, seedloop **re-execs the interpreter** with `PYTHONHASHSEED` set to a fixed
value derived from the run seed, then runs the scenario in the child. Verified: two child processes
launched with the same `PYTHONHASHSEED` hash identically; a different value differs. The re-exec is a
one-time cost at the boundary and is invisible to user code. (The cleaner design keeps library code from
*depending* on hash order at all; the pin is the backstop for user code that does.)

## Network as scheduled events

The simulated transport (ADR-0006, full model in [network.md](network.md)) needs no new machinery — a
message in flight is a timer. `endpoint.send(dst, msg)` draws a latency from the `"net"` sub-stream and
schedules a delivery callback at `now + latency` that appends `(src, msg)` to `dst`'s receive queue and
wakes any `recv` waiting on it. Reordering falls out for free: two messages sent close together get
independent latencies, so their arrival order can differ from send order. A drop schedules nothing; a
duplicate schedules two deliveries; a partition is a predicate checked when the delivery fires (still
cut → dropped). Because every delivery is an ordinary timer, network timing obeys the same deterministic
heap as everything else.

## Faults as scheduled events

Faults are scheduled the same way. A `Fault` left unparameterized draws its target and timing from the
`"faults"` sub-stream and registers timers that toggle state: a partition flips a link's reachability at
its start and clears it at its end; `slow_link` sets a latency multiplier for a window; `crash` sets a
node's stopped flag at its time. The fault schedule is therefore a deterministic function of the seed,
which is what makes "chaos" reproducible rather than random.

## Proving it: the timeline recorder

Determinism is *proven by replay*, not asserted (the prime rule). The loop records a **timeline**: an
append-only list of `(virtual_time, event_kind, ids…)` for every callback run, message delivered, and
fault fired. Two runs of the same seed must produce byte-identical timelines; a replay-equivalence test
runs a seed twice and asserts equality of the two traces. This is the test that *is* the determinism
guarantee — if it ever differs, an entropy source escaped the World, and the test says so. The harness
and the rest of the verification strategy are in [testing.md](testing.md).
