"""The deterministic event loop.

``asyncio``'s loop is already single-threaded and its scheduling is deterministic by
construction; the one nondeterministic seam is the I/O poll (``selector.select()``). seedloop
subclasses :class:`asyncio.BaseEventLoop` and overrides only ``_run_once`` to remove that poll
(ADR-0013): the ready queue is drained in faithful ``call_soon`` FIFO order (ADR-0012), and the
real-I/O surface is rejected rather than run (``docs/scope.md``).

In this slice time is frozen at 0: timers can be scheduled, but the loop cannot advance time to
fire one (the virtual clock and autojump arrive in the next slice), so a run that would have to is
rejected. ``BaseEventLoop`` (unlike ``BaseSelectorEventLoop``) creates no selector and no self-pipe,
so no real socket exists in the loop.
"""

from __future__ import annotations

import asyncio
from typing import Any, NoReturn

from seedloop.errors import BoundaryError, DeadlockError


class DeterministicLoop(asyncio.BaseEventLoop):
    """A single-threaded ``asyncio`` loop with no real I/O and no real clock."""

    def __init__(self) -> None:
        super().__init__()
        # Virtual time, frozen at 0 in this slice; the clock slice advances it.
        self._sl_time = 0.0

    def time(self) -> float:
        return self._sl_time

    def _run_once(self) -> None:
        # Deterministic replacement for BaseEventLoop._run_once: no select(), no real I/O.
        # call_soon order is preserved (ADR-0012); the seed will drive timing through the
        # simulated network, not through callback order.
        ready: Any = self._ready  # type: ignore[attr-defined]  # BaseEventLoop's ready deque
        if not ready:
            if self._scheduled:  # type: ignore[attr-defined]  # any timer (incl. 0-delay) is deferred
                raise NotImplementedError(
                    "firing timers (call_later / call_at) is added in the clock slice"
                )
            if not self._stopping:  # type: ignore[attr-defined]  # BaseEventLoop stop flag
                raise DeadlockError(
                    "the run is quiescent: every task is blocked and nothing is scheduled to "
                    "wake one"
                )
            return
        # Run every callback ready at the start of this step, in registration order. Callbacks
        # scheduled while the batch runs land in _ready and run on the next step (the len()
        # bound), matching CPython.
        for _ in range(len(ready)):
            handle = ready.popleft()
            if not handle.cancelled():
                handle._run()

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
