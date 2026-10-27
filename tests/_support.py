"""Shared test helpers.

``run_isolated`` is the safe way to test code that may hang or spin: it runs in a fresh interpreter
under a hard time cap, so a regression is a clear test failure instead of a stuck job. In-process
``pytest-timeout`` is not a safe bound for this: on POSIX it raises a ``BaseException`` that the
code under test may swallow, and on Windows it terminates the whole session.
"""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent


def _load_proctree() -> Any:
    # scripts/ is not a package: load the shared process-tree helper by path (untyped for mypy).
    path = ROOT / "scripts" / "proctree.py"
    spec = importlib.util.spec_from_file_location("proctree", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("proctree", module)
    spec.loader.exec_module(module)
    return module


proctree: Any = _load_proctree()


class IsolatedTimeout(AssertionError):
    """The isolated child did not finish within its cap; carries what it had printed so far."""

    def __init__(self, timeout: float, stdout: str, stderr: str) -> None:
        super().__init__(
            f"isolated child still running after {timeout}s (hang or spin)\n"
            f"--- stdout so far ---\n{stdout}\n--- stderr so far ---\n{stderr}"
        )
        self.timeout = timeout
        self.stdout = stdout
        self.stderr = stderr


def _text(data: str | bytes | None) -> str:
    if data is None:
        return ""
    return data if isinstance(data, str) else data.decode("utf-8", errors="replace")


def _env(extra: Mapping[str, str] | None) -> dict[str, str]:
    # Inherit the real environment (Windows needs SystemRoot/PATH to start Python), pin the hash
    # seed even if the caller exported another (the caller can still pass its own through `extra`),
    # and never leak a CI runner's GITHUB_OUTPUT into a child.
    env = dict(os.environ)
    env.pop("GITHUB_OUTPUT", None)
    env["PYTHONHASHSEED"] = "0"
    if extra:
        env.update(extra)
    return env


# Below the 30 s per-test pytest-timeout, so this cap always fires first: on Windows pytest-timeout
# ends the whole session without killing children, which would leave a hung child running.
DEFAULT_CAP = 20.0


def run_isolated(
    code: str,
    *,
    timeout: float = DEFAULT_CAP,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run ``code`` with ``python -c`` in a fresh interpreter; fail clearly past ``timeout``."""
    return run_command([sys.executable, "-c", code], timeout=timeout, env=env, cwd=cwd)


def run_script(
    script: Path,
    *args: str,
    timeout: float = DEFAULT_CAP,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a Python script file in a fresh interpreter under the same cap."""
    return run_command([sys.executable, str(script), *args], timeout=timeout, env=env, cwd=cwd)


_CHECK_SCRIPT = """\
import asyncio, contextlib, json, math, seedloop
{scenario}
r = seedloop.check(scenario, seeds={seeds}, on_failure="return", {options})
e = r.error
print(json.dumps({{
    "type": type(e).__name__ if e else None,
    "message": str(e) if e else "",
    "notes": list(getattr(e, "__notes__", [])) if e else [],
    "seed": r.failing_seed,
    "extra": {report},
}}))
"""


def isolated_check(
    scenario_source: str,
    *,
    seeds: str = "[0]",
    options: str = "",
    report: str = "None",
    timeout: float = DEFAULT_CAP,
) -> dict[str, Any]:
    """Run ``seedloop.check`` on a scenario in a fresh interpreter and report what it returned.

    ``scenario_source`` defines an async function ``scenario(world)``. The result holds the error
    type name, its message and notes, and the failing seed; a hang past ``timeout`` fails this one
    test instead of the whole job. ``report`` is an expression evaluated after the check and
    returned as ``extra`` (it must be JSON-serialisable).
    """
    code = _CHECK_SCRIPT.format(
        scenario=scenario_source, seeds=seeds, options=options, report=report
    )
    done = run_isolated(code, timeout=timeout)
    assert done.returncode == 0, done.stderr
    parsed: dict[str, Any] = json.loads(done.stdout.strip().splitlines()[-1])
    return parsed


def run_command(
    argv: list[str],
    *,
    timeout: float,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = proctree.run(
            argv, timeout=timeout, env=_env(env), cwd=cwd if cwd is not None else ROOT, text=True
        )
    except subprocess.TimeoutExpired as exc:  # the whole process tree has been killed
        raise IsolatedTimeout(timeout, _text(exc.stdout), _text(exc.stderr)) from None
    assert isinstance(result.stdout, str)
    return subprocess.CompletedProcess(argv, result.returncode, result.stdout, result.stderr)


def pid_alive(pid: int) -> bool:
    """Whether a process with this id is still running (used to prove a kill reached it)."""
    if sys.platform == "win32":
        listing = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"], capture_output=True, text=True, check=False
        ).stdout
        return str(pid) in listing.split()
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return _is_running(pid)  # a killed process nobody has waited for still answers signal 0


def _is_running(pid: int) -> bool:
    """Whether the process is running: not a zombie, and not gone since the signal check.

    A killed process is a zombie for a moment before its parent (or init) reaps it, so it can
    vanish between the signal check and the read of its state. That is "not running", not "alive".
    """
    try:  # Linux
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        if Path("/proc/self").exists():
            return False  # procfs is there and the process is not: it has been reaped
    except OSError:
        pass
    else:
        return stat.rsplit(")", 1)[1].split()[0] != "Z"
    listing = subprocess.run(  # macOS and other BSDs
        ["ps", "-o", "stat=", "-p", str(pid)], capture_output=True, text=True, check=False
    ).stdout.strip()
    return bool(listing) and not listing.startswith("Z")  # no output: the process is gone


@dataclass(frozen=True)
class Change:
    """A semantic change: ``old`` becomes ``new`` in ``src/seedloop/<file>``."""

    name: str
    file: str
    old: str
    new: str
    may_hang: bool = False  # the corpus is allowed to hang (not just fail) on this change


def copy_src_with(tmp_path: Path, change: Change) -> Path:
    """Copy ``src/`` into ``tmp_path`` with ``change`` applied; return the copy (for PYTHONPATH)."""
    copy = tmp_path / "src"
    shutil.copytree(ROOT / "src", copy, ignore=shutil.ignore_patterns("__pycache__"))
    target = copy / "seedloop" / change.file
    with open(target, encoding="utf-8", newline="") as f:
        text = f.read()
    # A change whose pattern is gone would silently test nothing: that is a failure, not a skip.
    assert text.count(change.old) == 1, f"pattern for {change.name!r} not found exactly once"
    with open(target, "w", encoding="utf-8", newline="") as f:
        f.write(text.replace(change.old, change.new, 1))
    return copy


REVERSE_CANCEL_ORDER = Change(
    "teardown cancels in reverse creation order",
    "_world.py",
    'pending.sort(key=lambda t: getattr(t, "_sl_seq", -1))',
    'pending.sort(key=lambda t: -getattr(t, "_sl_seq", -1))',
)

# The historical defect that corpus, golden and the unit tests all missed: the drive must run one
# more step after the scenario task is done, so tasks spawned in the last step still progress.
DROPPED_SETTLE_STEP = Change(
    "the extra settle step after the scenario is dropped",
    "_loop.py",
    "                if watch.done():\n                    self._sl_stop_in = 2",
    "                if watch.done():\n                    self._sl_stop_in = 1",
)
