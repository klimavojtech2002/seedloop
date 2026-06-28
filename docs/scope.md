# Scope and the determinism boundary

A deterministic testing tool is only as honest as the boundary it draws. A tool that claims to make
"any async program" deterministic and then silently leaks nondeterminism is worse than no tool — it
makes failures look reproducible when they are not. `seedloop` therefore states its boundary precisely
and designs to it. This document is that boundary, and the engineering reasons behind it.

The one-line version: **`seedloop` makes your async *logic* deterministic; it does not make your
*infrastructure* deterministic.** It tests your algorithm, not your database driver.

## What "deterministic" means here

A run is a pure function of its seed. For that to hold, every source of nondeterminism a run can touch
must be controlled. `seedloop` controls these:

| Source of nondeterminism | How it is controlled |
|--------------------------|----------------------|
| Task / coroutine scheduling order | The custom loop owns scheduling and runs it deterministically (faithful `call_soon` FIFO + a `(when, seq)` timer tie-break); nondeterminism enters only through I/O timing below, not callback order (ADR-0012). |
| Time (`asyncio.sleep`, timeouts, `loop.time`) | Virtual clock owned by the loop; advances instantly and deterministically. |
| Randomness (`random`) | Seeded RNG injected into the run. |
| CSPRNG (`os.urandom`, `secrets`) | Shimmed to a seeded source during a run — both `os.urandom` and the `random._urandom` alias `secrets` binds at import (ADR-0010). |
| Hash/set ordering (`PYTHONHASHSEED`) | Pinned before interpreter start via the re-exec launcher (ADR-0010). |
| Network: latency, ordering, loss, partitions | The simulated transport; the seed decides all of it (ADR-0006). |

Each source is drawn from its own sub-stream of the seed, not one shared stream, so adding a consumer
does not perturb the others' timelines (ADR-0009). If your code stays inside this set, a failing run
reduces to a seed you can replay forever — within a major version (ADR-0011).

**How the boundary is enforced, not just stated.** The same points where the World substitutes a seeded
source are tripwires: under `check(..., audit=True)` an uncontrolled source — real `os.urandom`, real
time, the unseeded global `random`, a real thread — raises `EntropyLeakError` (or `BoundaryError` for the
thread) instead of running silently (ADR-0008). A leak is therefore a loud, reproducible failure on the
seed that hit it, not a guarantee you have to take on trust. The tripwires intercept Python-level
attribute calls; a reference bound before the audit started (`from time import monotonic`) or a C
extension that reads the clock or the OS RNG directly, below Python, is not caught — the same reason
C-extension drivers are out of scope above.

## What is deliberately out of scope — and why

These are not missing features; they are hard boundaries dictated by how CPython and the OS work.
Pretending otherwise would break the core guarantee.

### Real threads and `multiprocessing` — a wall

Determinism requires a single cooperative thread of control. Real OS threads are preempted by the
kernel at points you cannot observe or reproduce, and the GIL serializes bytecode without making the
interleaving deterministic. `multiprocessing` and subprocesses have separate memory and their own OS
scheduling. This is the same wall that prevents deterministic simulation testing in Go, whose runtime
multiplexes goroutines across OS threads. `seedloop` targets single-threaded `asyncio`, which sits on
the *good* side of that line — and rejects or warns on `run_in_executor`, `threading`, and subprocess
use inside a simulated run rather than pretending to control them.

### `uvloop` — a wall

`uvloop` is a drop-in `asyncio` loop written in Cython on top of libuv, used by most
performance-sensitive deployments. `seedloop` *is itself* a custom event loop, so it is mutually
exclusive with `uvloop` by construction — you run on one or the other. `seedloop`'s loop cannot slot a
simulated network underneath libuv's compiled transports. Code whose behavior depends on `uvloop` is
therefore out of scope.

### Real sockets and C-extension drivers — out of scope by design

This is the decision that makes the project buildable by one person, and it is made openly. A tool
that drove *real* I/O through a custom loop would have to implement enough of the private, unstable
`BaseEventLoop` interface to satisfy real drivers, build a faithful TCP/IP simulation (a multi-month
subsystem on its own — `madsim` had to fork both `tokio` and `tokio-postgres` to get there), and still
lose determinism wherever a C extension reads the clock or the OS RNG directly, invisibly to any
Python-level shim. The teams that pursued whole-program determinism (Antithesis) concluded it requires
intercepting *all* syscalls at the hypervisor level — beyond a library, let alone a solo one.

So `seedloop` does not drive real I/O. Users write their protocol against an **abstract transport**
(the [sans-I/O](https://sans-io.readthedocs.io/) pattern, as in `h11` and `h2`): the logic sends and
receives messages through an interface, and `seedloop` provides a simulated implementation of that
interface. This sidesteps the `BaseEventLoop` instability and the TCP-simulation work entirely,
because the seam is small, pure-Python, and defined by the tool. The cost is honest and stated: code
that is *not* written against an abstract transport — code wired directly to `asyncpg`, `grpcio`, raw
sockets — is not testable by `seedloop` without first being refactored to the sans-I/O style. That
refactor is exactly the design that makes code testable *and* reusable, which is why DST and sans-I/O
are natural partners.

### External oracles (including LLMs) — record/replay, never live

Anything outside the simulated world — a real HTTP call, an LLM — is nondeterministic by nature. Inside
a deterministic run such a call must be **recorded once and replayed** from a fixture, never made live;
a live call voids determinism for that seed. `seedloop` treats these the way it treats the network:
through the controllable seam, replayed.

## What this is good for, concretely

The sweet spot is code where the *logic* holds the concurrency bugs and the I/O is already abstracted:

- Consensus and replication: Raft, Paxos, primary/backup, quorum logic.
- Eventual consistency: CRDTs, gossip, anti-entropy.
- Custom wire protocols and their state machines.
- Coordination logic: schedulers, leader election, distributed locks.
- Resilience logic: retry, backoff, circuit breakers, rate limiters, timeouts.

For this class of code — which is precisely where rare, partition-dependent bugs live and where
reproduction is hardest — `seedloop` is exactly the right tool, and there is no equivalent in Python
today.

## What this is not good for

- I/O-bound applications that are thin glue over real drivers (a web API mostly calling `asyncpg`).
  The bugs there are usually not in the async logic, and the drivers are out of scope.
- Anything depending on `uvloop`, real threads, or `multiprocessing`.
- Performance testing. `seedloop` runs in virtual time; it tells you whether your logic is *correct*
  under adversarial timing, not how *fast* it is.

Drawing this line on purpose — and saying plainly what the tool will and will not do — is what makes
its "deterministic" claim worth trusting.
