# seedloop

Deterministic simulation testing for Python. Run your concurrent async logic through thousands of
controlled, reproducible timelines — varying message timing and delivery order, injecting network
faults, partitions, and delays — to surface the rare concurrency bug that shows up once in a million
runs, and replay it exactly from a seed.

It brings the FoundationDB / TigerBeetle / Antithesis style of reliability testing — until now living
only in Rust, C++, and Java — to Python's `asyncio`, as a `pip`-installable library.

[![CI](https://github.com/klimavojtech2002/seedloop/actions/workflows/ci.yml/badge.svg)](https://github.com/klimavojtech2002/seedloop/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/seedloop)](https://pypi.org/project/seedloop/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## The problem

Concurrency bugs are the worst bugs. A protocol or state machine works in every test, then once a
week in CI a test fails, and nobody can reproduce it — because the failure depended on an exact
interleaving of events, a message arriving late, a partition healing at the wrong moment. You cannot
fix what you cannot reproduce, so these bugs are patched by guesswork and survive for years.

Deterministic simulation testing (DST) inverts this. It takes total control of every source of
nondeterminism — scheduling order, time, randomness, the network — and drives them all from a single
seed. The same seed produces the same timeline, so the same bug, every time. You explore thousands of
seeds to hunt for failures, and when one is found, the seed *is* the reproduction: replay it and the
bug happens again, deterministically, every run.

This is how FoundationDB reached its reliability record. It exists as a polished library in Rust
(`madsim`, `turmoil`). In Python — where a great deal of distributed and protocol code is written — it
does not exist at all. `seedloop` is that library.

## What you do with it

You write your protocol or algorithm against an abstract transport (the
[sans-I/O](https://sans-io.readthedocs.io/) style), and `seedloop` runs it inside a deterministic
world it fully controls. A test looks like this — everything shown runs on the current release; the
seed-*scheduled* fault plan (`world.run_for`) is the next phase, specified in
[docs/api.md](docs/api.md):

```python
import asyncio
import seedloop

async def scenario(world: seedloop.World) -> None:
    # Spin up your nodes; they send messages through the simulated network.
    nodes = [RaftNode(addr, world.net) for addr in range(5)]
    world.start(*nodes)

    # State the invariant that must hold at every step, not just at the end.
    world.always(lambda: at_most_one_leader(nodes), name="at-most-one-leader")

    # Inject chaos: run a while, split the network, heal it, let the cluster recover.
    # The seed decides every message's timing, so each seed is a different timeline.
    await asyncio.sleep(2)
    world.net.partition({0, 1}, {2, 3, 4})
    await asyncio.sleep(2)
    world.net.heal()
    await asyncio.sleep(2)

# Hunt across 10,000 seeded timelines. A failure is re-raised tagged with its seed:
#   seedloop: failing seed=4823 (replay with seedloop.replay)
seedloop.check(scenario, seeds=10_000)
```

`seedloop.replay(scenario, seed=4823)` re-runs that exact timeline, deterministically, as many times
as you need to debug it. The full API is in [docs/api.md](docs/api.md).

## The worked proof: a Raft split-brain, found and replayed

A small Raft leader election ships as a demo. With a deliberate, labelled flaw — a node that omits the
single-vote-per-term rule — a seed sweep finds the timing where two nodes both win an election in the
same term (split-brain), and replays it from the seed. The corrected election passes the same sweep, so
the violation is the toggled flaw, not the harness: in a three-node cluster the shared third voter can
only break the tie once under the single-vote rule, so one candidate gets two votes and the other one —
never two leaders.

```
$ python -m seedloop.demos.raft
seedloop Raft election demo - hunting for split-brain

buggy election: split-brain found at seed=7
  reproduce it:  seedloop.replay(election_scenario(buggy=True), seed=7)
  replay reproduces it: invariant 'at-most-one-leader-per-term' violated at t=0.229...
correct election (single-vote rule enforced): no violation over the same 200 seeds
-> the violation is the toggled flaw, not the harness.
```

The election logic is in [`src/seedloop/demos/raft.py`](src/seedloop/demos/raft.py). It is election only
(terms, `RequestVote`, majority, heartbeats) — log replication, persistence, and membership changes are
out of scope.

## What it does

- A **deterministic event loop** that makes `asyncio` task scheduling reproducible and drives the I/O
  seam — where nondeterminism actually enters — from the seed.
- A **virtual clock** — `sleep` and timeouts advance simulated time instantly; no run is slower for
  testing a 10-second scenario.
- **Seeded randomness** everywhere, so a run is a pure function of its seed.
- A **simulated network** with seeded latency, reordering, message loss, and partitions.
- **Fault injection** driven by the seed, so chaos is reproducible rather than random.
- **Invariants** — `world.always(...)` checks a continuous safety property at every step.
- A **non-determinism auditor** — `audit=True` turns any uncontrolled entropy source into a loud,
  reproducible failure, so the determinism boundary is enforced, not just stated.
- **Seed replay** — the whole point: any failure reduces to a single integer you can replay forever.

## Scope — what it tests, and what it deliberately does not

The honesty in this section is the point. `seedloop` makes your async *logic* deterministic; it does
not make your *infrastructure* deterministic, and it does not pretend to. The full boundary, and the engineering reasons behind it, are in
[docs/scope.md](docs/scope.md). In short:

- **It is for** pure-Python async code that talks to an abstract transport: consensus (Raft/Paxos),
  replication, gossip, CRDTs, custom wire protocols, schedulers, retry/backoff/circuit-breaker logic,
  rate limiters — code where the *logic* holds the concurrency bugs.
- **It is not for** I/O-heavy applications bound to real drivers. Real threads, `multiprocessing`,
  `uvloop`, and C-extension drivers (`asyncpg`, `grpcio`) are explicitly out of scope, because their
  scheduling cannot be controlled from Python — the same wall that stops deterministic testing in Go.
  `seedloop` tests your algorithm, not your database driver.

Choosing this boundary deliberately — rather than promising determinism it cannot deliver — is what
keeps the guarantee real.

## Status

The planned build is **complete**: the deterministic core (custom event loop, virtual
clock with autojump, seeded entropy, the `World` / `check` / `replay` API), the simulated network with
fault injection (loss, duplication, partitions), the `world.always` invariant API, the non-determinism
auditor (`audit=True`), and the worked Raft demo (which runs today) — so `asyncio` runs are reproducible
and instant, a partition- or timing-dependent bug replays identically from its seed, and an uncontrolled
entropy source fails loudly under audit. Deferred: the seed-scheduled `world.run_for` fault schedule and
an optional Hypothesis integration (`seedloop[hypothesis]`). The full API target is in
[docs/api.md](docs/api.md) and the phased build in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Why it exists

There is no `pip`-installable deterministic simulation testing framework for Python `asyncio` — the
capability lives in Rust (`madsim`, `turmoil`), C++ (FoundationDB), Java (OpenDST), and behind a
commercial hypervisor (Antithesis), but not in Python. Meanwhile the discipline is rising fast among
serious engineers (Antithesis raised a $105M round led by Jane Street to standardize DST; AWS has
codified deterministic and formal methods as standing practice). As one of its proponents puts it:
*writing code is no longer the bottleneck — making sure it does the right thing is.* `seedloop` is a
tool for exactly that, in the language that lacked it.

## Documentation

The design is specified before the code:

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — how `asyncio` is made deterministic, and the phased build.
- [docs/api.md](docs/api.md) — the public API: `World`, `check`/`replay`, the transport, faults.
- [docs/internals.md](docs/internals.md) — the loop, virtual clock, entropy control, network and fault scheduling.
- [docs/network.md](docs/network.md) — the simulated transport and fault model.
- [docs/scope.md](docs/scope.md) — the determinism boundary: what is controlled and what is not.
- [docs/testing.md](docs/testing.md) — how determinism is proven by replay.
- [docs/decisions.md](docs/decisions.md) — the decision records (ADRs).
- [docs/glossary.md](docs/glossary.md) — the vocabulary.

## License

MIT — see [LICENSE](LICENSE).
