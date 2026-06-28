# Internals

How the deterministic core is built, in enough detail to implement it and to defend it. The public
behaviour is in [api.md](api.md); the boundary is in [scope.md](scope.md); the *why* of each
non-obvious choice is in [decisions.md](decisions.md). Vocabulary is fixed in [glossary.md](glossary.md).

The Phase-1 core described here — the deterministic loop, the virtual clock and autojump, the seeded
entropy primitives, their assembly into a `World` with `check`/`replay`, and the network with its faults
(loss, duplication, partition, reliable channel), the invariant API (`world.always`), and the
non-determinism auditor (`audit=True`) — is implemented and tested; only the seed-*scheduled* fault API
(`run_for`) is still design. The load-bearing CPython facts below were checked against the
target interpreter (CPython 3.13); where a claim depends on a version, it says so.

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

seedloop reuses CPython's ready queue and runs its own timer heap:

- **Ready queue** — `BaseEventLoop`'s `collections.deque`; `call_soon` appends, the step drains it
  left-to-right (`popleft`), so callbacks run in registration order. seedloop **preserves** this order:
  `asyncio` documents `call_soon` as FIFO and correct code may rely on it, so reordering it would
  manufacture failures from code that is in fact correct (ADR-0012). Interleavings are explored through
  *when* tasks become ready — the network's seeded delivery timing — not by shuffling ready callbacks.
  O(1) append/pop.
- **Timer heap** — seedloop's own `heapq` (`_sl_timers`) of `(when, seq, handle)` tuples ordered by
  `(when, seq)`. CPython's `TimerHandle` orders by `_when` alone, so two timers with the *same* deadline
  fire in whatever order the heap happens to pop — not deterministic. seedloop cannot change that
  ordering, so it keys its own tuples: the monotonic `seq` makes equal-deadline timers fire in scheduling
  order, every run. `call_at`/`call_later` push here, not onto `BaseEventLoop._scheduled`. O(log n)
  push/pop.

The timer tie-break is the one ordering seedloop adds. The one nondeterministic seam it must *replace*
is the I/O poll — and that is where the seed enters: the simulated network's delivery timing decides
when tasks become ready, which is the lever that explores interleavings (Phase 2).

## The step loop (replacing `select()`)

CPython's `_run_once` drains ready callbacks, then blocks in `selector.select(timeout)` until the next
timer or an I/O event. seedloop has no I/O and no real waiting, so the poll becomes a clock move:

```
def _run_once(self):
    # 1. When nothing is ready, jump the clock to the next live timer (autojump).
    if not self._ready:
        self._purge_cancelled_timers()                  # head becomes a live deadline
        if self._sl_timers:
            self._sl_time = max(self._sl_time, self._sl_timers[0][0])   # forward only; nobody sleeps
        elif not self._stopping:
            raise DeadlockError(...)                     # blocked, no timer to wake anyone

    # 2. Promote every timer now due (when <= clock) to ready, then run the batch FIFO.
    self._fire_due_timers()
    for _ in range(len(self._ready)):
        handle = self._ready.popleft()
        if not handle.cancelled():
            handle._run()                    # callbacks scheduled here run next step
```

The run ends when the top-level coroutine completes. When the ready queue and the timer heap are both
empty while tasks are still awaiting, nothing can ever wake them — a **deadlock in the simulated
world** — and seedloop raises rather than hanging, because a real `asyncio` program would hang there and
a test must surface it.

This is the autojump of ADR-0005 / `trio`'s `MockClock`: time only moves when all work is blocked, and
then it moves straight to the next scheduled event. A 10-second `sleep` is a timer at `now+10`; when the
loop has nothing else to do it jumps there instantly.

## Virtual clock

`loop.time()` returns `self._sl_time`, a float of simulated monotonic seconds starting at 0.
`call_later(d, cb)` schedules a timer at `self._sl_time + d`; `call_at(t, cb)` at absolute virtual `t`.
`asyncio.sleep`
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
message in flight is a timer. `endpoint.send(dst, msg)` assigns the message a monotonic id, records a
`(now, "send", mid, src, dst)` event, draws a latency from the `"net"` sub-stream, and schedules a
delivery callback at `now + latency`. The callback records `(now, "deliver", mid, src, dst)`, appends
`(src, msg)` to `dst`'s receive queue, and wakes any `recv` waiting on it; a delivery to an unbound
address is dropped. The monotonic `mid` is the *stable* timeline identity — Python `id()`, `repr`, and
asyncio task names are not (see the timeline note below), so the network's send/deliver events make
replay-equivalence cover the network without leaking object identities. Reordering falls out for free:
two messages sent close together get independent latencies, so their arrival order can differ from send
order. Drop, duplicate, and partition are tweaks to whether and how many delivery timers a send
schedules (drawn from the `"faults"` sub-stream): a drop schedules nothing, a duplicate schedules a
second delivery, and a partition is a reachability check when the delivery fires. The reliable channel
schedules at a non-decreasing per-link delivery time so messages arrive in send order. Because every
delivery is an ordinary timer, network timing obeys the same deterministic heap as everything else.

## Faults as scheduled events

Faults are scheduled the same way. A `Fault` left unparameterized draws its target and timing from the
`"faults"` sub-stream and registers timers that toggle state: a partition flips a link's reachability at
its start and clears it at its end; `slow_link` sets a latency multiplier for a window; `crash` sets a
node's stopped flag at its time. The fault schedule is therefore a deterministic function of the seed,
which is what makes "chaos" reproducible rather than random.

## Proving it: the timeline recorder

Determinism is *proven by replay*, not asserted (the prime rule). In Phase 1 the **timeline** is
user-driven: `world.record(event)` appends a `(virtual_time, event)` pair, so a scenario logs the
decisions whose reproducibility it cares about (its `rng` draws, its timed actions). Two runs of the same
seed must produce byte-identical timelines; a replay-equivalence test runs a seed twice and asserts the
traces are equal. This works because Phase-1 scheduling is deterministic (faithful FIFO + virtual clock),
so a scenario that records its decisions captures everything that can vary. The automatic per-event
recorder — every callback, message, and fault stamped with stable ids — is Phase 2 work, when the
network gives events natural identities; until then the contract is "same seed → same recorded
timeline," and identities outside that (Python `id()`, `repr`, asyncio task names) are not part of it.
The harness and the rest of the verification strategy are in [testing.md](testing.md).
