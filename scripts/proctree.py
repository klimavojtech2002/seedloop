"""Run a command with a time limit and kill everything it started, not just the command itself.

``subprocess.run(timeout=...)`` kills only the direct child. A test run that hangs usually has
children of its own (a test that spawned a subprocess), and those would keep spinning after the
limit. Shared by the test helpers (``tests/_support.py``) and the mutation harness
(``scripts/mutation_sweep.py``).

On POSIX the child leads its own session and the whole process group is killed (a grandchild that
starts yet another session escapes it). On Windows the
child is put in a job object that kills its members when terminated or closed; that also reaches
grandchildren whose parent has already exited, which ``taskkill /T`` cannot find.
"""

from __future__ import annotations

import contextlib
import os
import signal
import subprocess
import sys
import warnings
from collections.abc import Mapping, Sequence
from pathlib import Path


class Job:
    """A set of processes that can be killed together."""

    def __init__(self) -> None:
        self._handle: int | None = None
        if sys.platform == "win32":
            self._handle = _create_kill_on_close_job()

    def adopt(self, process: subprocess.Popen[bytes] | subprocess.Popen[str]) -> None:
        """Put ``process`` (and from now on its children) in the job.

        On Windows the process is already running when it is assigned, so a child it starts in
        that instant could escape the job; Python offers no portable way to start it suspended.
        """
        if sys.platform == "win32" and self._handle is not None:
            if process.poll() is not None:
                return  # it has already exited: nothing to adopt
            try:
                _assign(self._handle, int(getattr(process, "_handle")))  # noqa: B009
            except OSError:
                if process.poll() is None:
                    raise  # a real failure
                # it exited between the check above and the assignment: nothing left to adopt

    def kill(self, process: subprocess.Popen[bytes] | subprocess.Popen[str]) -> None:
        if sys.platform == "win32":
            if self._handle is not None:
                with contextlib.suppress(OSError):  # an empty job (everything already gone)
                    _terminate(self._handle)
        else:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.killpg(process.pid, signal.SIGKILL)  # the child leads its own session
        with contextlib.suppress(OSError):
            process.kill()

    @staticmethod
    def reap(process: subprocess.Popen[bytes] | subprocess.Popen[str]) -> None:
        """Wait for a killed child so that it does not linger as a zombie (POSIX)."""
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=2)

    def close(self) -> None:
        if sys.platform == "win32" and self._handle is not None:
            _close(self._handle)
            self._handle = None


def run(
    argv: Sequence[str],
    *,
    timeout: float,
    env: Mapping[str, str],
    cwd: Path,
    text: bool,
) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
    """Run ``argv`` capturing output; on a timeout or any interruption kill the whole tree first.

    Raises ``subprocess.TimeoutExpired`` (with the output collected so far) after a timeout, like
    ``subprocess.run``; ``KeyboardInterrupt`` or any other exception also kills the tree first.
    """
    job = Job()
    try:
        process = subprocess.Popen(
            list(argv),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=text,
            env=dict(env),
            cwd=cwd,
            start_new_session=sys.platform != "win32",
        )
    except BaseException:
        job.close()  # the command never started: do not leak the job handle
        raise
    try:
        job.adopt(process)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as expired:
            job.kill(process)
            partial_out: object = expired.stdout
            partial_err: object = expired.stderr
            try:
                partial_out, partial_err = process.communicate(timeout=2)
            except subprocess.TimeoutExpired as again:
                # A process in a session of its own can still hold the pipes: keep what we have.
                partial_out = again.stdout or partial_out
                partial_err = again.stderr or partial_err
            job.reap(process)
            raise subprocess.TimeoutExpired(
                list(argv),
                timeout,
                output=partial_out,  # type: ignore[arg-type]
                stderr=partial_err,  # type: ignore[arg-type]
            ) from None
        except BaseException:
            job.kill(process)  # Ctrl-C or any other interruption: leave nothing running
            job.reap(process)
            raise
        return subprocess.CompletedProcess(list(argv), process.returncode, stdout, stderr)
    finally:
        # Also after a normal exit: nothing the command started may outlive it.
        job.kill(process)
        job.reap(process)
        job.close()


# --- Windows job objects ------------------------------------------------------------------------
# The ctypes code lives behind the platform check so the module imports (and type-checks) elsewhere.

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    _kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
    _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000

    class _IoCounters(ctypes.Structure):
        _fields_ = [(name, ctypes.c_ulonglong) for name in ("r", "w", "o", "rb", "wb", "ob")]

    class _BasicLimits(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimits),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    _kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    _kernel32.CreateJobObjectW.argtypes = [wintypes.LPVOID, wintypes.LPCWSTR]
    _kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        wintypes.LPVOID,
        wintypes.DWORD,
    ]
    _kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _kernel32.CloseHandle.argtypes = [wintypes.HANDLE]

    def _warn(what: str) -> None:
        error = ctypes.WinError(ctypes.get_last_error())
        warnings.warn(
            f"{what} failed ({error}); only the direct child will be killed", stacklevel=3
        )

    def _create_kill_on_close_job() -> int | None:
        handle = _kernel32.CreateJobObjectW(None, None)
        if not handle:
            _warn("CreateJobObject")
            return None
        limits = _ExtendedLimits()
        limits.BasicLimitInformation.LimitFlags = _JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        ok = _kernel32.SetInformationJobObject(
            handle,
            _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
            ctypes.byref(limits),
            ctypes.sizeof(limits),
        )
        if not ok:
            _warn("SetInformationJobObject")
            _kernel32.CloseHandle(handle)
            return None
        return int(handle)

    def _assign(job: int, process_handle: int) -> None:
        if not _kernel32.AssignProcessToJobObject(job, process_handle):
            raise ctypes.WinError(ctypes.get_last_error())

    def _terminate(job: int) -> None:
        if not _kernel32.TerminateJobObject(job, 1):
            raise ctypes.WinError(ctypes.get_last_error())

    def _close(job: int) -> None:
        _kernel32.CloseHandle(job)

else:  # pragma: no cover - the Windows helpers are never called elsewhere

    def _create_kill_on_close_job() -> int | None:
        return None

    def _assign(job: int, process_handle: int) -> None:
        return None

    def _terminate(job: int) -> None:
        return None

    def _close(job: int) -> None:
        return None
