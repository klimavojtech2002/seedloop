"""Generated scenario programs for differential testing.

A program is a short list of operations drawn from a seed: sleeping, spawning and cancelling
tasks, gathering, ``wait_for`` / ``asyncio.timeout`` / ``TaskGroup``, cleanup in cancel handlers
(with and without sleeping), done-callbacks, ``call_later``, async generators, invariants,
network sends. The same program is run under two builds of seedloop in separate processes and
the outcomes are compared, so any behavioural drift in the loop, the drive or the teardown shows
up as a difference.

The generator must keep covering the classes of behaviour that earlier, weaker checks missed
(work still ready when the scenario returns, a node failing a step or two after it, cleanup that
sleeps, done-callbacks on the scenario task, ``loop.stop()`` from user code). Extend it whenever
a review finds a class it does not reach.

``TaskGroup`` is used with at most one child here: with several, cancellation order follows
object identity hashes, which a build that does not hash tasks deterministically cannot
reproduce.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from seedloop import World

KINDS = (
    "sleep",
    "sleep",
    "sleep0",
    "record",
    "spawn",
    "spawn",
    "cancel",
    "gather",
    "wait_for",
    "timeout",
    "taskgroup",
    "shield",
    "cleanup_node",
    "invariant",
    "invariant",
    "invariant_steps",
    "done_cb",
    "main_done_cb",
    "loop_stop",
    "call_later",
    "call_soon",
    "agen",
    "raise",
    "net",
    "node_raises",
)


@dataclass(frozen=True)
class Op:
    kind: str
    delay: float
    a: int
    b: int
    r: float
    sub: tuple[Op, ...] = ()


def generate(seed: int) -> tuple[Op, ...]:
    return _gen(random.Random(seed), 0)


def kinds(program: tuple[Op, ...]) -> set[str]:
    """Every operation kind used in ``program``, including nested ones."""
    found: set[str] = set()
    for op in program:
        found.add(op.kind)
        found |= kinds(op.sub)
    return found


def _gen(rng: random.Random, depth: int) -> tuple[Op, ...]:
    ops = []
    for position in range(rng.randint(2, 7)):
        kind = rng.choice(KINDS)
        if depth == 0 and position == 0 and rng.random() < 0.6:
            kind = "spawn"  # most operations need an existing task to do anything
        nested = depth < 2 and kind in ("spawn", "timeout", "taskgroup")
        ops.append(
            Op(
                kind=kind,
                delay=rng.choice([0.0, 0.01, 0.1, 0.5, 1.0, 2.0]),
                a=rng.randint(0, 5),
                b=rng.randint(0, 5),
                r=rng.random(),
                sub=_gen(rng, depth + 1) if nested else (),
            )
        )
    return tuple(ops)


@dataclass
class Ctx:
    world: World
    next_address: int = 100
    keep: list[asyncio.Future[object]] = field(default_factory=list)  # fire-and-forget tasks


async def run_program(ctx: Ctx, prog: tuple[Op, ...], tag: str = "m") -> None:
    w = ctx.world
    tasks: list[asyncio.Future[object]] = []
    for i, op in enumerate(prog):
        try:
            w.record((tag, i, "op", op.kind))  # every executed step is on the timeline
            await _step(ctx, tasks, op, f"{tag}.{i}", i, tag)
        except asyncio.CancelledError:
            w.record((tag, i, "cancelled"))  # the order teardown cancels tasks in is observable
            raise
        except Exception as exc:
            w.record((tag, i, "exc", type(exc).__name__))
            if op.r < 0.3:
                raise
    if len(tag) % 2 == 0:
        for task in tasks:
            task.cancel()


async def _step(
    ctx: Ctx, tasks: list[asyncio.Future[object]], op: Op, sub_tag: str, i: int, tag: str
) -> None:
    w = ctx.world
    kind, d, a, b, r = op.kind, op.delay, op.a, op.b, op.r
    loop = asyncio.get_running_loop()
    if kind == "sleep":
        await asyncio.sleep(d)
    elif kind == "sleep0":
        await asyncio.sleep(0)
    elif kind == "record":
        w.record((tag, i))
    elif kind == "spawn":
        _hold(ctx, tasks, asyncio.ensure_future(run_program(ctx, op.sub, sub_tag)))
    elif kind == "cancel" and tasks:
        tasks[a % len(tasks)].cancel()
    elif kind == "gather" and tasks:
        await asyncio.gather(*tasks[: 1 + a % len(tasks)], return_exceptions=True)
    elif kind == "wait_for" and tasks:
        try:
            await asyncio.wait_for(asyncio.shield(tasks[a % len(tasks)]), timeout=d + 0.05)
        except (asyncio.CancelledError, Exception) as exc:
            w.record((tag, i, type(exc).__name__))
    elif kind == "timeout":
        try:
            async with asyncio.timeout(d + 0.05):
                await run_program(ctx, op.sub, sub_tag)
        except TimeoutError:
            w.record((tag, i, "timeout"))
    elif kind == "taskgroup":
        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(run_program(ctx, op.sub, sub_tag + "a"))
        except* Exception as group_error:
            w.record((tag, i, "taskgroup", len(group_error.exceptions)))
    elif kind == "shield" and tasks:
        try:
            await asyncio.shield(tasks[a % len(tasks)])
        except BaseException as exc:
            w.record((tag, i, "shield", type(exc).__name__))
            if isinstance(exc, asyncio.CancelledError) and r < 0.5:
                raise
    elif kind == "cleanup_node":
        w.start(_CleanupNode(w, tag, i, a % 3, d))
    elif kind == "invariant":
        _add_invariant(ctx, tasks, tag, i, d, r)
    elif kind == "invariant_steps":
        _add_step_invariant(ctx, tasks, tag, i, a % 4)
    elif kind == "done_cb" and tasks:
        tasks[a % len(tasks)].add_done_callback(lambda _t: w.record((tag, i, "done-callback")))
    elif kind == "main_done_cb":
        # A done-callback on the task running this program (the scenario task at depth 0) that
        # schedules more work: it fires while the run is winding down.
        current = asyncio.current_task()
        if current is not None:
            current.add_done_callback(
                lambda _t: loop.call_soon(lambda: w.record((tag, i, "main-done-callback")))
            )
    elif kind == "loop_stop":
        if r < 0.3:  # user code stopping the loop is rare, and ends the run abruptly
            loop.stop()
    elif kind == "call_later":
        loop.call_later(d, lambda: w.record((tag, i, "call-later")))
    elif kind == "call_soon":
        loop.call_soon(lambda: loop.call_soon(lambda: w.record((tag, i, "call-soon-chain"))))
    elif kind == "agen":
        await _consume_generator(w, tag, i, a % 3 + 1, stop_early=bool(b % 2))
    elif kind == "raise":
        if r < 0.5:
            raise ValueError(f"{tag}.{i}")
    elif kind == "net":
        await _network_exchange(ctx, tag, i)
    elif kind == "node_raises":
        w.start(_LateFailure(1 + a % 3))


def _hold(ctx: Ctx, tasks: list[asyncio.Future[object]], task: asyncio.Future[object]) -> None:
    """Keep a spawned task referenced for the whole run so garbage collection cannot end it."""
    tasks.append(task)
    ctx.keep.append(task)


class _CleanupNode:
    """A started node whose cancel handler sleeps, times out, or returns at once."""

    def __init__(self, w: World, tag: str, i: int, mode: int, delay: float) -> None:
        self.w, self.tag, self.i, self.mode, self.delay = w, tag, i, mode, delay

    async def run(self) -> None:
        try:
            await asyncio.sleep(50)
        except asyncio.CancelledError:
            if self.mode == 0:
                await asyncio.sleep(self.delay)
            elif self.mode == 1:
                try:
                    async with asyncio.timeout(0.2):
                        await asyncio.sleep(self.delay)
                except TimeoutError:
                    pass
            self.w.record((self.tag, self.i, "cleaned"))
            raise


class _LateFailure:
    """A started node that fails a few loop steps in (possibly after the scenario returned)."""

    def __init__(self, steps: int) -> None:
        self.steps = steps

    async def run(self) -> None:
        for _ in range(self.steps):
            await asyncio.sleep(0)
        raise ValueError("late node failure")


def _add_invariant(
    ctx: Ctx, tasks: list[asyncio.Future[object]], tag: str, i: int, delay: float, r: float
) -> None:
    state = {"bad": False}
    ctx.world.always(lambda: not state["bad"], name=f"inv{tag}{i}")

    async def flip() -> None:
        await asyncio.sleep(delay + 0.3)
        state["bad"] = True

    if r < 0.4:
        _hold(ctx, tasks, asyncio.ensure_future(flip()))


def _add_step_invariant(
    ctx: Ctx, tasks: list[asyncio.Future[object]], tag: str, i: int, steps: int
) -> None:
    # An invariant that breaks a few loop steps from now, not a few virtual seconds: it can land in
    # the steps right after the scenario returns, where a check ends the run mid-wind-down.
    state = {"bad": False}
    ctx.world.always(lambda: not state["bad"], name=f"inv-steps{tag}{i}")

    async def flip() -> None:
        for _ in range(steps):
            await asyncio.sleep(0)
        state["bad"] = True

    _hold(ctx, tasks, asyncio.ensure_future(flip()))


async def _consume_generator(w: World, tag: str, i: int, count: int, *, stop_early: bool) -> None:
    async def generate_values() -> AsyncIterator[int]:
        try:
            for value in range(count):
                yield value
        finally:
            w.record((tag, i, "agen-finally"))

    async for _ in generate_values():
        if stop_early:
            break


async def _network_exchange(ctx: Ctx, tag: str, i: int) -> None:
    w = ctx.world
    first = w.net.bind(ctx.next_address)
    second = w.net.bind(ctx.next_address + 1)
    ctx.next_address += 2
    await first.send(second.address, i)
    _src, message = await asyncio.wait_for(second.recv(), timeout=1.0)
    w.record((tag, i, "net", message))


async def scenario_for(seed: int, world: World) -> None:
    await run_program(Ctx(world), generate(seed))
