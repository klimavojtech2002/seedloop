"""Boundary tests: every out-of-scope operation is rejected, never run."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any

import pytest

from seedloop._loop import DeterministicLoop
from seedloop.errors import BoundaryError

# One call per overridden entry point. Sync methods raise on call; async ones raise when awaited.
# Arguments are throwaway — the override rejects before looking at them.
_BOUNDARY_CALLS: list[tuple[str, Callable[[DeterministicLoop], Any]]] = [
    ("run_in_executor", lambda lp: lp.run_in_executor(None, lambda: 1)),
    ("call_soon_threadsafe", lambda lp: lp.call_soon_threadsafe(lambda: None)),
    ("add_reader", lambda lp: lp.add_reader(0, lambda: None)),
    ("add_writer", lambda lp: lp.add_writer(0, lambda: None)),
    ("add_signal_handler", lambda lp: lp.add_signal_handler(2, lambda: None)),
    ("sock_recv", lambda lp: lp.sock_recv(object(), 1)),
    ("sock_sendall", lambda lp: lp.sock_sendall(object(), b"x")),
    ("sock_connect", lambda lp: lp.sock_connect(object(), None)),
    ("getaddrinfo", lambda lp: lp.getaddrinfo("host", 0)),
    ("getnameinfo", lambda lp: lp.getnameinfo(("host", 0))),
    ("create_connection", lambda lp: lp.create_connection(lambda: None, "host", 0)),
    ("create_server", lambda lp: lp.create_server(lambda: None)),
    ("create_datagram_endpoint", lambda lp: lp.create_datagram_endpoint(lambda: None)),
    ("connect_read_pipe", lambda lp: lp.connect_read_pipe(lambda: None, object())),
    ("connect_write_pipe", lambda lp: lp.connect_write_pipe(lambda: None, object())),
    ("subprocess_exec", lambda lp: lp.subprocess_exec(lambda: None, "echo")),
    ("subprocess_shell", lambda lp: lp.subprocess_shell(lambda: None, "echo hi")),
]


@pytest.mark.parametrize("name,call", _BOUNDARY_CALLS, ids=[n for n, _ in _BOUNDARY_CALLS])
def test_boundary_method_rejected(name: str, call: Callable[[DeterministicLoop], Any]) -> None:
    loop = DeterministicLoop()
    try:
        try:
            result: Any = call(loop)  # sync rejecters raise here
        except BoundaryError:
            return
        # Not a sync raise: must be an async rejecter returning a coroutine that raises on await.
        if inspect.iscoroutine(result):
            with pytest.raises(BoundaryError):
                loop.run_until_complete(result)
        else:
            pytest.fail(f"{name} returned {result!r} instead of raising BoundaryError")
    finally:
        loop.close()
