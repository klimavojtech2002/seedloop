"""The World: everything for one deterministic run, derived from one seed.

A run is a pure function of its seed. The World assembles the deterministic loop, the virtual clock,
and the seeded entropy into one object, exposes the user's seeded ``rng`` and the virtual clock, and
records a timeline so two runs of a seed can be compared. Users do not construct a World;
``check``/``replay`` build it and pass it to the scenario.

Scheduling stays faithful FIFO (ADR-0012), so the seed's observable effect is ``rng``, timer timing,
and the simulated network's delivery timing — not callback order.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
from collections.abc import Awaitable, Callable, Collection, Iterator, Sequence
from typing import Protocol, runtime_checkable

from seedloop._entropy import substream
from seedloop._faults import CrashFault, Fault, PartitionFault, SlowLinkFault
from seedloop._loop import DeterministicLoop
from seedloop._net import Address, Transport
from seedloop._trace import Timeline
from seedloop.errors import InvariantError, SeedloopError, _UsageError

_RUNNING_LOOP_MESSAGE = (
    "check/replay cannot run inside a running event loop; call it from synchronous code"
)


@runtime_checkable
class Node(Protocol):
    """User code the World can start: any object with an async ``run``."""

    async def run(self) -> None: ...


class World:
    """One deterministic run, all derived from ``seed``."""

    def __init__(self, seed: int) -> None:
        self.seed = seed
        self.rng = substream(seed, "user")  # the user's entropy; never the global random
        self._loop = DeterministicLoop()
        self._timeline = Timeline()
        self._started: list[asyncio.Task[None]] = []
        self._invariants: list[tuple[str, Callable[[], bool]]] = []
        self._invariants_hook_installed = False
        self._run_until_active = False
        self.net = Transport(
            self._loop, substream(seed, "net"), substream(seed, "faults"), self._timeline
        )

    def now(self) -> float:
        """Current virtual time in seconds (advances by autojump, never by real waiting)."""
        return self._loop.time()

    def record(self, event: object) -> None:
        """Append an event to the run's timeline, stamped with the current virtual time.

        The timeline is the artifact that proves determinism: two runs of a seed must record an
        identical sequence. A scenario records the decisions whose reproducibility it cares about.
        """
        self._timeline.record((self._loop.time(), event))

    def always(self, predicate: Callable[[], bool], *, name: str) -> None:
        """Register a safety property that must hold throughout the run.

        ``predicate`` is evaluated after every step (not during teardown); the first step where it
        is false raises ``InvariantError(name)``, which ``check`` reports. It must be pure and
        read-only — a predicate that mutates state or draws entropy would break determinism. A
        started node's body runs a step after ``start``, so a predicate over node state sees its
        initial value on the first check.
        """
        self._invariants.append((name, predicate))
        if not self._invariants_hook_installed:
            # Installed once no matter how many predicates are registered — _check_invariants loops
            # over all of them itself, so the hook list only ever gets one entry for invariants.
            self._loop._sl_after_step_hooks.append(self._check_invariants)
            self._invariants_hook_installed = True

    def _check_invariants(self) -> None:
        # A hard hook: raises directly, so it always wins the same-step priority in
        # DeterministicLoop._run_once, and its exception carries the invariant's own name/time
        # without going through the generic exception-repr note path (ADR-0021).
        for name, predicate in self._invariants:
            if not predicate():
                raise InvariantError(name, self._loop.time())

    async def run_for(self, *, seconds: float, faults: Sequence[Fault] = ()) -> None:
        """Advance virtual time by ``seconds``, injecting any seed-scheduled ``faults``.

        Each ``Fault`` (from ``world.partition``/``slow_link``/``crash``) is resolved and
        scheduled here — any field its constructor left unset is drawn from the seed's
        ``"faults"`` sub-stream, in list order, so reordering ``faults`` changes what a seed
        resolves (ADR-0022). Every fault is validated *before* any is resolved or scheduled, so
        an invalid fault later in the list cannot leave an earlier one already committed — a
        partial commit despite the call raising. Use ``world.net.partition``/``heal`` instead for
        an immediate, scenario-timed partition rather than a seed-timed one.
        """
        if not math.isfinite(seconds) or seconds < 0:
            raise SeedloopError(f"seconds must be a finite number >= 0, got {seconds!r}")
        for fault in faults:
            self.net._validate_fault(fault, seconds)
        for fault in faults:
            self.net._commit_fault(fault, seconds)
        await asyncio.sleep(seconds)

    def partition(self, *groups: Collection[Address]) -> Fault:
        """Build a seed-timed partition ``Fault`` for ``run_for(faults=[...])``.

        Same group semantics as ``world.net.partition``: nodes in different listed groups cannot
        reach each other while the fault is active; a node in no listed group stays connected to
        everyone. ``groups`` must not overlap (an address in two listed groups is rejected, since
        which one it is cut from would be ambiguous). ``groups=()`` lets the seed pick a genuine
        two-way split of the addresses bound when ``run_for`` schedules it; timing (when, how
        long) is always seed-chosen — there is no parameter to pin it. For an immediate,
        scenario-timed split instead, use ``world.net.partition`` (a different method, on
        ``world.net``, applied right away).
        """
        return PartitionFault(tuple(frozenset(g) for g in groups))

    def slow_link(
        self,
        a: Address | None = None,
        b: Address | None = None,
        *,
        factor: float | None = None,
    ) -> Fault:
        """Build a seed-timed slow-link ``Fault`` for ``run_for(faults=[...])``.

        Any of ``a``, ``b``, ``factor`` left ``None`` is chosen by the seed when ``run_for``
        schedules this fault; timing is always seed-chosen. ``factor``, if given, must be finite and
        greater than ``1.0``.
        """
        if factor is not None and (not math.isfinite(factor) or factor <= 1.0):
            raise SeedloopError(f"slow_link factor must be finite and > 1.0, got {factor!r}")
        if a is not None and a == b:
            raise SeedloopError(f"slow_link requires two distinct addresses, got a == b == {a!r}")
        return SlowLinkFault(a, b, factor)

    def crash(self, node: Address | None = None, *, at: float | None = None) -> Fault:
        """Build a crash ``Fault`` for ``run_for(faults=[...])``.

        Cuts ``node`` off the network, both directions, from virtual time ``at`` onward for the
        rest of the run — no restart, no heal. ``node`` left ``None`` is chosen by the seed from the
        addresses bound when ``run_for`` schedules it. ``at``, unlike ``partition``/``slow_link``,
        can be pinned: a virtual duration from the ``run_for`` call that consumes this fault
        (matching ``run_for``'s own ``seconds``), left ``None`` to let the seed choose it.
        """
        if at is not None and (not math.isfinite(at) or at < 0):
            raise SeedloopError(f"crash(at={at}) must be finite and >= 0")
        return CrashFault(node, at)

    async def run_until(
        self, predicate: Callable[[], bool], *, deadline: float | None = None
    ) -> None:
        """Advance virtual time until ``predicate()`` holds.

        ``deadline``, if given, is a virtual duration from this call (matching ``run_for``'s
        ``seconds``, not an absolute clock reading): if ``predicate`` has not become true within it,
        raise ``TimeoutError``. If both become true in the same step, ``predicate`` wins — decided
        here rather than left to asyncio's internal ordering (ADR-0021). Only one ``run_until`` may
        be active at a time; a nested or concurrent call raises ``SeedloopError``. If ``predicate``
        itself raises, that exception propagates; if an ``always()`` invariant also fails in the
        same step, the invariant is what the run raises regardless of registration order (a hard
        failure always wins), with the predicate's exception attached to it as a note rather than
        discarded (ADR-0021).
        """
        if self._run_until_active:
            raise SeedloopError("run_until does not support concurrent or nested calls")
        if deadline is not None and not math.isfinite(deadline):
            raise SeedloopError(f"deadline must be a finite number or None, got {deadline!r}")
        self._run_until_active = True
        done: asyncio.Future[None] = self._loop.create_future()
        deadline_at = self._loop.time() + deadline if deadline is not None else None

        def check() -> Exception | None:
            if done.done():
                return None
            try:
                holds = predicate()
            except Exception as exc:
                # Route the exception through `done` so `await done` delivers it inside run_until's
                # own frame (catchable by the scenario's own try/except around this call), AND
                # return it so _run_once's hook loop can give it priority if another hook (e.g. an
                # always() invariant) also fails this same step — a deferred-delivery exception can
                # never win a same-step race on its own, since it never raises synchronously.
                done.set_exception(exc)
                return exc
            if holds:
                done.set_result(None)
            elif deadline_at is not None and self._loop.time() >= deadline_at:
                # An elapsed deadline is an expected, often-handled outcome (that's the point of
                # `deadline`), not a bug — unlike a raising predicate, it does not compete for
                # same-step priority against another hook's failure.
                done.set_exception(
                    TimeoutError(f"run_until: predicate did not hold within deadline={deadline}")
                )
            return None

        self._loop._sl_after_step_hooks.append(check)
        # A deadline needs a real timer so the loop has something to autojump to even if nothing
        # else is scheduled — otherwise a scenario with no other pending work would hit the existing
        # DeadlockError guard (which runs before after-step hooks) before the deadline could fire.
        timer = self._loop.call_at(deadline_at, lambda: None) if deadline_at is not None else None
        try:
            check()  # the predicate may already hold; do not wait a full step for that case
            await done
        finally:
            # The hook may already be gone: a DeadlockError (or another hook's exception) raised
            # from _run_once while this call was still pending aborts the run through World._drive,
            # whose teardown clears the whole hook list before this finally ever runs (delivered via
            # the task-cancellation that follows). Removal here is best-effort cleanup, not a
            # correctness requirement — asserting membership first avoids a spurious ValueError that
            # would otherwise mask whatever exception actually ended the run.
            if check in self._loop._sl_after_step_hooks:
                self._loop._sl_after_step_hooks.remove(check)
            if timer is not None:
                timer.cancel()
            self._run_until_active = False

    def start(self, *nodes: Node) -> None:
        """Schedule each node's ``run()`` coroutine as a task on the loop.

        A started node that raises fails the run (its exception surfaces from the run), rather than
        being orphaned and silently logged — a failure the seed must report.
        """
        for node in nodes:
            self._started.append(self._loop.create_task(node.run()))

    @property
    def timeline(self) -> tuple[object, ...]:
        """The recorded timeline so far (a read-only snapshot)."""
        return tuple(self._timeline.events)

    def _close(self) -> None:
        if not self._loop.is_closed():
            self._loop.close()

    def _drive(self, main: Awaitable[None]) -> None:
        """Run the scenario to completion, surface any started-node failure, then close the loop."""
        loop = self._loop
        if asyncio.events._get_running_loop() is not None:
            close = getattr(main, "close", None)
            if close is not None:
                close()  # never started: do not leave a "coroutine was never awaited" warning
            raise _UsageError(_RUNNING_LOOP_MESSAGE)
        main_task = loop.create_task(main)
        failure: BaseException | None = None
        try:
            try:
                # Invariants describe the logical run, not cancellation cleanup: they are cleared
                # between the two settle steps (see run_until_done).
                loop.run_until_done(main_task, settle=loop._sl_after_step_hooks.clear)
                if not main_task.done():
                    raise RuntimeError("Event loop stopped before Future completed.")
            except BaseException as exc:
                failure = exc  # a hook (invariant) or machinery error ended the run mid-scenario
                raise
            finally:
                if failure is not None:  # otherwise run_until_done cleared them between its steps
                    loop._sl_after_step_hooks.clear()
                if not main_task.done():
                    # Settle the scenario task on its own first, so its cancel handler runs before
                    # any other task is cancelled. An error here must not replace the failure
                    # already in flight.
                    main_task.cancel()
                    with _secondary(failure):
                        while not main_task.done():  # a user loop.stop() ends one pass, not the run
                            loop.run_until_done(main_task)
                    if failure is not None and main_task.done() and not main_task.cancelled():
                        unwind_error = main_task.exception()
                        if unwind_error is not None and unwind_error is not failure:
                            _note(failure, _describe(unwind_error))
                elif failure is not None:
                    # A check ended the run in a stop step, after the scenario was done: the hooks
                    # are cleared above and one more step runs, so ready work still progresses.
                    with _secondary(failure):
                        loop.run_one_more_step(main_task)
            main_task.result()  # the scenario's own outcome (raises it)
            for task in self._started:
                node_error = task.exception() if task.done() and not task.cancelled() else None
                if node_error is not None:
                    raise node_error
        except BaseException as exc:
            if failure is not None and exc is not failure:
                _note(exc, _describe(failure))  # an interrupt ended the run: keep what it replaced
            failure = exc  # teardown notes go on what actually propagates, an interrupt included
            raise
        finally:
            try:
                if failure is not None:  # a check added while settling must not fire again here
                    loop._sl_after_step_hooks.clear()
                with _secondary(failure):
                    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
                    pending.sort(key=lambda t: getattr(t, "_sl_seq", -1))
                    for task in pending:
                        task.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            except BaseException as interrupt:
                if failure is not None and interrupt is not failure:
                    _note(interrupt, _describe(failure))  # keep the failure the interrupt replaced
                raise
            finally:
                # Read the failures the run did not surface, so asyncio does not log "Task exception
                # was never retrieved". A task teardown could not finish (it deadlocked) has none.
                for task in (main_task, *self._started):
                    if task.done() and not task.cancelled():
                        task.exception()
                self._close()


def _describe(error: BaseException) -> str:
    try:
        return f"seedloop: {type(error).__name__}: {error}"
    except Exception:  # a hostile __str__ must not replace the failure being noted
        return f"seedloop: {type(error).__name__}"


def _note(failure: BaseException, text: str) -> None:
    # An exception that cannot take a note (frozen, unusable __notes__, custom add_note) keeps its
    # place as the real failure; the note is dropped.
    with contextlib.suppress(Exception):
        failure.add_note(text)


@contextlib.contextmanager
def _secondary(failure: BaseException | None) -> Iterator[None]:
    """A later error must not replace the failure already in flight: it becomes a note on it.

    Only ``Exception`` is absorbed: a ``KeyboardInterrupt``/``SystemExit`` raised while settling
    still propagates, and an interrupt already in flight is not replaced by a settle ``Exception``.
    """
    try:
        yield
    except Exception as exc:
        if failure is None:
            raise
        _note(failure, _describe(exc))
