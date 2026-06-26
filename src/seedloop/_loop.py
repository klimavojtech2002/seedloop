"""The deterministic event loop.

``asyncio``'s loop is already single-threaded and its scheduling is deterministic by
construction; the one nondeterministic seam is the I/O poll (``selector.select()``). seedloop
subclasses :class:`asyncio.BaseEventLoop` and overrides only ``_run_once`` to remove that poll
(ADR-0013): the ready queue is drained in faithful ``call_soon`` FIFO order (ADR-0012), and the
real-I/O surface is rejected rather than run (``docs/scope.md``).

Time is virtual: ``loop.time()`` starts at 0 and never advances by waiting. When every task is
blocked, the loop jumps the clock to the next scheduled timer (the autojump of ADR-0005), so a
ten-second ``sleep`` resolves instantly. Timers live in a heap keyed ``(when, seq)``, so equal
deadlines fire in scheduling order — a deterministic tie-break CPython's ``TimerHandle`` (ordered by
deadline alone) lacks. ``BaseEventLoop`` (unlike ``BaseSelectorEventLoop``) creates no selector and
no self-pipe, so no real socket exists in the loop.
"""

from __future__ import annotations

import asyncio
import heapq
from typing import Any, NoReturn

from seedloop.errors import BoundaryError, DeadlockError


class DeterministicLoop(asyncio.BaseEventLoop):
    """A single-threaded ``asyncio`` loop with no real I/O and a virtual clock."""

    def __init__(self) -> None:
        super().__init__()
        self._sl_time = 0.0  # virtual monotonic time; advanced only by the autojump
        # Timer heap of (when, seq, handle); the monotonic seq is the deterministic tie-break,
        # so equal deadlines fire in scheduling order.
        self._sl_timers: list[tuple[float, int, asyncio.TimerHandle]] = []
        self._sl_timer_seq = 0

    def time(self) -> float:
        return self._sl_time

    def call_at(  # type: ignore[override]
        self, when: float, callback: Any, *args: Any, context: Any = None
    ) -> asyncio.TimerHandle:
        self._check_closed()  # type: ignore[attr-defined]  # BaseEventLoop guard, not in the stubs
        timer = asyncio.TimerHandle(when, callback, args, self, context)
        heapq.heappush(self._sl_timers, (when, self._sl_timer_seq, timer))
        self._sl_timer_seq += 1
        return timer

    def call_later(  # type: ignore[override]
        self, delay: float, callback: Any, *args: Any, context: Any = None
    ) -> asyncio.TimerHandle:
        return self.call_at(self._sl_time + delay, callback, *args, context=context)

    def _timer_handle_cancelled(self, handle: asyncio.TimerHandle) -> None:
        # Cancelled timers are tombstoned and skipped when popped; no count bookkeeping needed.
        pass

    def _run_once(self) -> None:
        # Deterministic replacement for BaseEventLoop._run_once: no select(), no real I/O. When
        # nothing is ready, advance virtual time to the next timer (autojump); then promote every
        # timer now due and run the ready batch in faithful FIFO order (ADR-0012).
        ready: Any = self._ready  # type: ignore[attr-defined]  # BaseEventLoop's ready deque
        if not ready:
            self._purge_cancelled_timers()
            if self._sl_timers:
                self._sl_time = max(self._sl_time, self._sl_timers[0][0])  # jump forward only
            elif not self._stopping:  # type: ignore[attr-defined]  # BaseEventLoop stop flag
                raise DeadlockError(
                    "the run is quiescent: every task is blocked and no timer is scheduled to "
                    "wake one"
                )
        self._fire_due_timers()
        # Run the batch ready at step start in registration order; callbacks scheduled mid-batch
        # run on the next step (the len() bound), matching CPython.
        for _ in range(len(ready)):
            handle = ready.popleft()
            if not handle.cancelled():
                handle._run()

    def _fire_due_timers(self) -> None:
        # Promote every timer whose deadline has arrived (<= the clock) to the ready queue.
        ready = self._ready  # type: ignore[attr-defined]
        while self._sl_timers and self._sl_timers[0][0] <= self._sl_time:
            handle = heapq.heappop(self._sl_timers)[2]
            if not handle.cancelled():
                ready.append(handle)

    def _purge_cancelled_timers(self) -> None:
        # Drop cancelled timers from the heap head so the earliest entry is a live deadline.
        while self._sl_timers and self._sl_timers[0][2].cancelled():
            heapq.heappop(self._sl_timers)

    # --- boundary: operations that cannot be made deterministic are rejected (ADR-0002) ---

    def _reject(self, what: str) -> NoReturn:
        raise BoundaryError(
            f"{what} cannot be made deterministic and is out of scope inside a simulated run "
            f"(see docs/scope.md)"
        )

    def run_in_executor(self, *args: Any, **kwargs: Any) -> NoReturn:  # type: ignore[override]
        self._reject("run_in_executor (real threads)")

    def call_soon_threadsafe(self, *args: Any, **kwargs: Any) -> NoReturn:  # type: ignore[override]
        self._reject("call_soon_threadsafe (another thread)")

    def add_reader(self, *args: Any, **kwargs: Any) -> NoReturn:  # type: ignore[override]
        self._reject("add_reader (real I/O)")

    def add_writer(self, *args: Any, **kwargs: Any) -> NoReturn:  # type: ignore[override]
        self._reject("add_writer (real I/O)")

    async def sock_recv(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("sock_recv (real socket)")

    async def sock_sendall(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("sock_sendall (real socket)")

    async def sock_connect(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("sock_connect (real socket)")

    async def getaddrinfo(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("getaddrinfo (real DNS)")

    async def getnameinfo(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("getnameinfo (real DNS)")

    async def create_connection(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("create_connection (real socket)")

    async def create_server(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("create_server (real socket)")

    async def create_datagram_endpoint(self, *args: Any, **kwargs: Any) -> NoReturn:
        # BaseEventLoop's version opens and binds a real UDP socket before failing; reject first.
        self._reject("create_datagram_endpoint (real socket)")

    async def connect_read_pipe(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("connect_read_pipe (real pipe)")

    async def connect_write_pipe(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("connect_write_pipe (real pipe)")

    async def subprocess_exec(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("subprocess_exec (real subprocess)")

    async def subprocess_shell(self, *args: Any, **kwargs: Any) -> NoReturn:
        self._reject("subprocess_shell (real subprocess)")

    def add_signal_handler(self, *args: Any, **kwargs: Any) -> NoReturn:  # type: ignore[override]
        self._reject("add_signal_handler (real signals)")

    # _process_events and _write_to_self are abstract on BaseEventLoop. We never poll and never
    # need a cross-thread wakeup, so both are inert.
    def _process_events(self, event_list: Any) -> None:
        pass

    def _write_to_self(self) -> None:
        pass
