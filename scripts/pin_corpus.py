"""Characterization corpus: many scenarios x seeds pinned to a committed golden.

``pin_replay.py`` pins one scenario at one seed. This pins many, each chosen for a class of
behaviour that a change to the loop, the network, the faults or the teardown could silently move:
fault scheduling, the reliable channel, ``run_until`` ties, cancellation order, cleanup that sleeps,
work still ready when the scenario returns, a node that fails a few steps after the scenario
returns, and so on.

Per (scenario, seed) the golden holds the outcome (``ok`` or the exception *type name*, never the
message, so a reworded error does not churn the pin) and a digest of the full timeline; a few
scenarios are also stored as full text so a drift can be read, not just detected. It is checked in
the suite and on the CI matrix, so a timeline-affecting change fails there instead of landing
silently; a deliberate change re-pins with ``--update`` and a CHANGELOG note.

Usage:
    python scripts/pin_corpus.py                 # check against the golden
    python scripts/pin_corpus.py --update        # rewrite the golden (review the diff!)
    python scripts/pin_corpus.py --golden PATH   # check a specific file (used by the tests)
"""

from __future__ import annotations

import asyncio
import difflib
import hashlib
import logging
import os
import secrets
import sys
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from seedloop import Endpoint
from seedloop._run import _run_one
from seedloop._world import World
from seedloop.demos.raft import election_scenario

GOLDEN = Path(__file__).resolve().parent.parent / "tests" / "data" / "corpus_golden.txt"
SEEDS = tuple(range(12))
FEW = (0, 1, 2)  # for scenarios that do not draw from the seed: more seeds add nothing

Scenario = Callable[[World], Awaitable[None]]

# Tasks the scenarios spawn without awaiting; held here so they are not garbage collected mid-run.
_KEEP: list[asyncio.Future[Any]] = []


def _spawn(coro: Awaitable[Any]) -> asyncio.Future[Any]:
    future = asyncio.ensure_future(coro)
    _KEEP.append(future)
    return future


@dataclass(frozen=True)
class Entry:
    name: str
    scenario: Scenario
    seeds: tuple[int, ...] = SEEDS
    audit: bool = False
    full_text_for: tuple[int, ...] = ()  # seeds whose complete timeline is stored for human diffs


# --- the harness -------------------------------------------------------------------------------


def capture(
    scenario: Scenario, seed: int, *, audit: bool = False
) -> tuple[str, tuple[object, ...]]:
    """Run one seed through the real ``_run_one``; return (outcome, timeline).

    The timeline is read from the World the scenario was handed, so it is kept even when the run
    fails, and everything ``_run_one`` sets up (the CSPRNG stream, the auditor) is exercised.
    """
    _KEEP.clear()
    worlds: list[World] = []

    async def wrapped(world: World) -> None:
        worlds.append(world)
        await scenario(world)

    outcome = "ok"
    try:
        _run_one(wrapped, seed, audit=audit)
    except Exception as exc:  # the corpus pins *that* a run fails and how, not why
        outcome = type(exc).__name__
    (world,) = worlds
    return outcome, world.timeline


def digest(outcome: str, timeline: Sequence[object]) -> str:
    text = outcome + "\n" + "".join(f"{event!r}\n" for event in timeline)
    return hashlib.sha1(text.encode()).hexdigest()[:12]


def serialize(entries: Sequence[Entry]) -> str:
    lines = ["# seedloop characterization corpus: name seed outcome digest"]
    full: list[str] = []
    for entry in entries:
        for seed in entry.seeds:
            outcome, timeline = capture(entry.scenario, seed, audit=entry.audit)
            lines.append(f"{entry.name} {seed} {outcome} {digest(outcome, timeline)}")
            if seed in entry.full_text_for:
                full.append(f"== {entry.name} seed={seed} outcome={outcome}")
                full.extend(repr(event) for event in timeline)
    return "\n".join([*lines, *full]) + "\n"


# --- the scenarios -----------------------------------------------------------------------------
# Each guards a class of behaviour; the comment says which.


async def _spam(eps: dict[int, Endpoint], src: int, dst: int, count: int = 25) -> None:
    for i in range(count):
        await eps[src].send(dst, i)
        await asyncio.sleep(0.05)


async def faults_unpinned(w: World) -> None:
    # Seed-scheduled partition / slow link / crash with every field left to the seed.
    eps = {a: w.net.bind(a, loss=0.1, duplicate=0.1) for a in (0, 1, 2, 3)}
    tasks = [asyncio.ensure_future(_spam(eps, s, d)) for s, d in ((0, 1), (2, 3), (1, 0))]
    await w.run_for(
        seconds=2,
        faults=[w.partition(), w.slow_link(), w.crash(), w.slow_link(0, 1, factor=3.0)],
    )
    await asyncio.gather(*tasks)


async def faults_pinned(w: World) -> None:
    # The same machinery with every pinnable field pinned.
    eps = {a: w.net.bind(a) for a in (0, 1, 2, 3)}
    tasks = [asyncio.ensure_future(_spam(eps, s, d)) for s, d in ((0, 1), (2, 3), (3, 2))]
    await w.run_for(
        seconds=3,
        faults=[w.partition({0, 1}, {2, 3}), w.crash(2, at=0.5), w.slow_link(1, 2, factor=4.0)],
    )
    await asyncio.gather(*tasks)


async def reliable_with_faults(w: World) -> None:
    # The in-order channel under a slow link: per-link FIFO clock.
    a = w.net.bind(1, reliable=True)
    b = w.net.bind(2, reliable=True)

    async def receiver() -> None:
        for _ in range(20):
            src, msg = await b.recv()
            w.record(("got", src, msg))

    task = asyncio.ensure_future(receiver())
    for i in range(20):
        await a.send(2, i)
    await w.run_for(seconds=1, faults=[w.slow_link(1, 2)])
    await asyncio.wait_for(task, 50)


async def run_until_tie(w: World) -> None:
    # The predicate and the deadline become true in the same step (a timer callback sets the flag at
    # the deadline itself): the predicate wins.
    flag = {"set": False}
    asyncio.get_running_loop().call_later(3.0, lambda: flag.update(set=True))
    await w.run_until(lambda: flag["set"], deadline=3.0)
    w.record(("tie-won-by-predicate", w.now()))


async def run_until_deadline_edge(w: World) -> None:
    # A sleeping task wakes one step after its timer fires, so a predicate that depends on it
    # becomes true one step after a deadline at the same instant: the deadline wins. Pinned as is.
    state = {"n": 0}

    async def tick() -> None:
        for _ in range(10):
            await asyncio.sleep(1.0)
            state["n"] += 1

    _spawn(tick())
    try:
        await w.run_until(lambda: state["n"] >= 3, deadline=3.0)
        w.record(("predicate-won", w.now(), state["n"]))
    except TimeoutError:
        w.record(("deadline-won", w.now(), state["n"]))
    await w.run_until(lambda: state["n"] >= 5, deadline=10.0)
    w.record(("later", w.now(), state["n"]))


async def nodes_record_on_cancel(w: World) -> None:
    # Teardown cancels pending tasks in creation order; a cancel handler can observe it.
    class Node:
        def __init__(self, i: int) -> None:
            self.i = i

        async def run(self) -> None:
            try:
                await asyncio.sleep(100 + self.i)
            except asyncio.CancelledError:
                w.record(("cancelled", self.i))
                raise

    w.start(*[Node(i) for i in range(6)])
    await asyncio.sleep(0.01)


async def cancel_handler_sleeps(w: World) -> None:
    # Graceful shutdown that takes virtual time must still complete (the clock must keep jumping).
    class Slow:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                await asyncio.sleep(0.5)
                w.record(("slow-cleanup", w.now()))
                raise

    class Bounded:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                try:
                    async with asyncio.timeout(0.2):
                        await asyncio.sleep(1)
                except TimeoutError:
                    w.record(("bounded-cleanup", w.now()))
                raise

    w.start(Slow(), Bounded())
    await asyncio.sleep(1)


async def teardown_deadlock(w: World) -> None:
    # A cancel handler that waits for something nobody will provide: the run must report a deadlock.
    class Stuck:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                await asyncio.get_running_loop().create_future()

    w.start(Stuck())
    await asyncio.sleep(0.1)


async def spawn_then_return(w: World) -> None:
    # A task spawned in the last step still gets its first steps before teardown.
    async def child() -> None:
        w.record("child-ran")

    await asyncio.sleep(0)
    _spawn(child())


async def spawn_chain(w: World) -> None:
    # Each link spawns the next; how far the chain runs after the scenario returns is pinned.
    async def link(n: int) -> None:
        w.record(("link", n))
        if n < 4:
            _spawn(link(n + 1))

    _spawn(link(0))


async def cancel_handler_spawns(w: World) -> None:
    class Spawner:
        async def run(self) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:

                async def late() -> None:
                    await asyncio.sleep(0)
                    w.record("spawned-in-cancel-ran")

                _spawn(late())
                raise

    w.start(Spawner())
    await asyncio.sleep(0.1)


async def pending_callbacks_at_return(w: World) -> None:
    # A timer, a done-callback and a call_soon chain that are all still pending when main returns.
    loop = asyncio.get_running_loop()
    loop.call_later(0.5, lambda: w.record("later"))

    async def other() -> None:
        w.record("other")

    task = asyncio.ensure_future(other())
    task.add_done_callback(lambda _t: loop.call_soon(lambda: w.record("soon-after-done")))
    loop.call_soon(lambda: loop.call_soon(lambda: w.record("soon-chain")))


async def busy_node_ticks(w: World) -> None:
    # How many steps a busy node gets around the end of the scenario.
    ticks = {"n": 0}

    class Busy:
        async def run(self) -> None:
            try:
                while True:
                    ticks["n"] += 1
                    await asyncio.sleep(0)
            except asyncio.CancelledError:
                w.record(("ticks", ticks["n"]))
                raise

    w.start(Busy())
    for _ in range(3):
        await asyncio.sleep(0)


def _node_raises_after(steps: int) -> Scenario:
    # A started node fails `steps` loop steps in; whether the seed fails pins how many steps the run
    # still gives started nodes after the scenario returns.
    async def scenario(w: World) -> None:
        class Node:
            async def run(self) -> None:
                for _ in range(steps):
                    await asyncio.sleep(0)
                raise ValueError("late node")

        w.start(Node())
        await asyncio.sleep(0)

    return scenario


async def shield_at_end(w: World) -> None:
    async def inner() -> None:
        await asyncio.sleep(0)
        w.record("inner-done")

    _spawn(asyncio.shield(_spawn(inner())))


async def invariant_fails(w: World) -> None:
    state = {"x": 0}
    w.always(lambda: state["x"] < 3, name="x-below-3")

    async def bump() -> None:
        while True:
            await asyncio.sleep(w.rng.uniform(0.1, 1.0))
            state["x"] += 1
            w.record(("bumped", state["x"], w.now()))

    w.start(type("Bumper", (), {"run": staticmethod(bump)})())
    await asyncio.sleep(10)


async def wait_for_burst(w: World) -> None:
    # Many timeouts, some equal, most cancelled: timer tie-break and cancelled timers.
    queue: asyncio.Queue[int] = asyncio.Queue()
    timed_out = 0
    for i in range(40):
        try:
            await asyncio.wait_for(queue.get(), timeout=round(w.rng.uniform(0.0, 0.3), 2))
        except TimeoutError:
            timed_out += 1
        if i % 7 == 0:
            await queue.put(i)
    w.record(("timed-out", timed_out, w.now()))


async def csprng_values(w: World) -> None:
    w.record((os.urandom(4).hex(), secrets.token_hex(3)))


async def net_basic(w: World) -> None:
    a = w.net.bind(1, loss=0.2, duplicate=0.2)
    b = w.net.bind(2)

    async def receiver() -> None:
        while True:
            src, msg = await b.recv()
            w.record(("got", src, msg, w.rng.randint(1, 100)))

    w.start(type("Receiver", (), {"run": staticmethod(receiver)})())
    for i in range(10):
        await a.send(2, i)
        await asyncio.sleep(w.rng.uniform(0.0, 0.05))
    await asyncio.sleep(0.5)


async def two_nodes_fail(w: World) -> None:
    # Two started nodes fail at different times. The run reports the first node that was *started*
    # to have failed, not the first to fail in time; the timeline records both failures.
    class Failing:
        def __init__(self, after: float, error: Exception) -> None:
            self.after, self.error = after, error

        async def run(self) -> None:
            await asyncio.sleep(self.after)
            w.record(("failing", type(self.error).__name__))
            raise self.error

    w.start(Failing(0.3, LookupError("second")), Failing(0.1, RuntimeError("first")))
    await asyncio.sleep(1)


async def taskgroup_one_child_timeout(w: World) -> None:
    # One child only: with several, cancellation order follows identity hashes (not yet controlled).
    async def worker() -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            w.record("worker-cancelled")
            raise

    try:
        async with asyncio.timeout(1):
            async with asyncio.TaskGroup() as group:
                group.create_task(worker())
    except TimeoutError:
        w.record("timed-out")


def entries() -> list[Entry]:
    return [
        Entry("faults_unpinned", faults_unpinned),
        Entry("faults_pinned", faults_pinned, full_text_for=(0,)),
        Entry("reliable_with_faults", reliable_with_faults),
        Entry("run_until_tie", run_until_tie, seeds=FEW),
        Entry("run_until_deadline_edge", run_until_deadline_edge, seeds=FEW),
        Entry("nodes_record_on_cancel", nodes_record_on_cancel, seeds=FEW),
        Entry("cancel_handler_sleeps", cancel_handler_sleeps, seeds=FEW, full_text_for=(0,)),
        Entry("teardown_deadlock", teardown_deadlock, seeds=FEW),
        Entry("spawn_then_return", spawn_then_return, seeds=FEW, full_text_for=(0,)),
        Entry("spawn_chain", spawn_chain, seeds=FEW, full_text_for=(0,)),
        Entry("cancel_handler_spawns", cancel_handler_spawns, seeds=FEW),
        Entry(
            "pending_callbacks_at_return",
            pending_callbacks_at_return,
            seeds=FEW,
            full_text_for=(0,),
        ),
        Entry("busy_node_ticks", busy_node_ticks, seeds=FEW, full_text_for=(0,)),
        *[
            Entry(f"node_raises_after_{n}", _node_raises_after(n), seeds=(0, 1))
            for n in range(1, 6)
        ],
        Entry("shield_at_end", shield_at_end, seeds=FEW),
        Entry("invariant_fails", invariant_fails),
        Entry("wait_for_burst", wait_for_burst),
        Entry("csprng_values", csprng_values),
        Entry("net_basic", net_basic),
        # Same scenario under the auditor: pins that audit mode does not perturb the timeline.
        Entry("net_basic_audited", net_basic, audit=True),
        Entry("two_nodes_fail", two_nodes_fail, seeds=FEW),
        Entry("taskgroup_one_child_timeout", taskgroup_one_child_timeout, seeds=FEW),
        Entry("raft_buggy", election_scenario(buggy=True), seeds=tuple(range(20))),
        Entry("raft_correct", election_scenario(buggy=False), seeds=tuple(range(20))),
    ]


# --- command line ------------------------------------------------------------------------------


def _read(path: Path) -> str:
    with open(path, encoding="utf-8", newline="") as f:  # newline="" keeps LF, no CRLF on Windows
        return f.read()


def _write(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write(text)


def main(argv: list[str]) -> int:
    # asyncio's own log lines (e.g. a task destroyed while pending, from the deliberate deadlock
    # scenario) are not part of the pinned surface; keep them out of the output.
    logging.getLogger("asyncio").disabled = True
    update = "--update" in argv
    golden_path = GOLDEN
    if "--golden" in argv:
        idx = argv.index("--golden")
        if idx + 1 >= len(argv):
            print("--golden requires a path", file=sys.stderr)
            return 2
        golden_path = Path(argv[idx + 1])

    current = serialize(entries())
    count = current.count("\n")

    if update:
        _write(golden_path, current)
        print(f"corpus written: {count} lines -> {golden_path}")
        return 0

    if not golden_path.exists():
        print(f"no golden at {golden_path}; generate it with --update", file=sys.stderr)
        return 1

    expected = _read(golden_path)
    if current == expected:
        print(f"OK: the corpus matches the golden ({count} lines).")
        return 0

    print(
        "FAIL: the corpus differs from the golden (a timeline-affecting change).", file=sys.stderr
    )
    for line in difflib.unified_diff(
        expected.splitlines(), current.splitlines(), "golden", "current", lineterm="", n=0
    ):
        print(line, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
