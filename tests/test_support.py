"""The isolation helper must really isolate: output, environment, exit codes and hangs."""

from __future__ import annotations

import contextlib
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import NoReturn

import pytest

from _support import (
    DEFAULT_CAP,
    ROOT,
    Change,
    IsolatedTimeout,
    copy_src_with,
    pid_alive,
    run_isolated,
    run_script,
)


def test_returns_output_and_exit_code() -> None:
    result = run_isolated("import sys; print('out'); print('err', file=sys.stderr); sys.exit(3)")
    assert result.returncode == 3
    assert result.stdout.strip() == "out"
    assert result.stderr.strip() == "err"


def test_runs_in_a_fresh_interpreter() -> None:
    # A module imported here must not be visible there: nothing is shared with the test process.
    sys.modules.setdefault("_support_marker", type(sys)("_support_marker"))
    result = run_isolated("import sys; print('_support_marker' in sys.modules)")
    assert result.stdout.strip() == "False"


def test_pins_the_hash_seed_unless_overridden() -> None:
    code = "import os; print(os.environ.get('PYTHONHASHSEED'))"
    assert run_isolated(code).stdout.strip() == "0"
    assert run_isolated(code, env={"PYTHONHASHSEED": "1234"}).stdout.strip() == "1234"


@pytest.mark.meta
def test_a_hang_fails_fast_with_the_partial_output() -> None:
    code = "import time; print('started', flush=True); time.sleep(60)"
    started = time.monotonic()
    with pytest.raises(IsolatedTimeout) as caught:
        run_isolated(code, timeout=2)
    assert time.monotonic() - started < 30  # the cap, not the child's own 60 s sleep
    assert "started" in caught.value.stdout
    assert caught.value.timeout == 2
    assert isinstance(caught.value, AssertionError)  # shows up as an ordinary test failure


@pytest.mark.meta
def test_a_busy_spin_is_killed_too() -> None:
    with pytest.raises(IsolatedTimeout):
        run_isolated("while True: pass", timeout=2)


def test_run_script_passes_arguments(tmp_path: Path) -> None:
    script = tmp_path / "echo.py"
    script.write_text("import sys; print(' '.join(sys.argv[1:]))", encoding="utf-8")
    assert run_script(script, "a", "b").stdout.strip() == "a b"


# Files that legitimately name the source tree: the helper that copies it, the differential test
# (its candidate), the toy project of the harness tests, and this guard.
_MAY_NAME_THE_SOURCE_TREE = {
    "_support.py",
    "test_differential.py",
    "test_mutation_sweep.py",
    "test_support.py",
    "mutation_sweep.py",  # the harness itself builds the copy of src/
}


def test_no_test_hardcodes_the_source_tree() -> None:
    # The mutation harness runs the suite against a modified copy of src/ (via PYTHONPATH).
    # A test that puts the repository's own src/ on sys.path (or a child that does) would exercise
    # the real tree, ignore the copy, and let mutants of that code "survive".
    offenders = []
    places = [*sorted((ROOT / "tests").glob("*.py")), *sorted((ROOT / "scripts").glob("*.py"))]
    for path in places:
        if path.name in _MAY_NAME_THE_SOURCE_TREE:
            continue
        text = path.read_text(encoding="utf-8")
        path_tweak = re.search(r"sys\.path\.(insert|append)\([^)]*src|addsitedir", text)
        if path_tweak or '"src"' in text or "'src'" in text or "/src" in text:
            offenders.append(path.name)
    assert not offenders, f"tests that name the source tree: {offenders}"


@pytest.mark.meta
def test_a_timeout_also_kills_the_grandchildren() -> None:
    # A hung child can have children of its own (a test run that spawned a subprocess). Killing only
    # the direct child leaves them spinning after the test is over.
    code = (
        "import subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
        "print(child.pid, flush=True)\n"
        "time.sleep(300)\n"
    )
    with pytest.raises(IsolatedTimeout) as caught:
        run_isolated(code, timeout=8)
    printed = caught.value.stdout.split()
    assert printed, "the child never printed the grandchild's pid (slow start?)"
    grandchild = int(printed[0])
    deadline = time.monotonic() + 15
    while pid_alive(grandchild) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not pid_alive(grandchild), "the grandchild outlived the cap"


def test_the_default_cap_is_below_the_per_test_timeout(request: pytest.FixtureRequest) -> None:
    # Otherwise pytest-timeout (which ends the whole session on Windows) would fire first.
    per_test = float(request.config.getini("timeout"))
    assert per_test > 0
    assert per_test > DEFAULT_CAP


def test_the_library_comes_from_the_tree_it_is_meant_to_test() -> None:
    # The mutation harness sets SEEDLOOP_MUTATION_COPY to a modified copy of src/ and puts it first
    # on PYTHONPATH. Imported from anywhere else, the mutants would run against the wrong code.
    import seedloop

    copy = os.environ.get("SEEDLOOP_MUTATION_COPY")
    if not copy:
        return  # a normal run: the library comes from wherever it is installed
    library = Path(seedloop.__file__).resolve()
    assert Path(copy).resolve() in library.parents, f"{library} is not under {copy}"


def test_a_pattern_that_is_not_found_is_a_failure_not_a_skip(tmp_path: Path) -> None:
    missing = Change("nothing to change", "_net.py", "this text is not in the module", "x")
    with pytest.raises(AssertionError, match="not found exactly once"):
        copy_src_with(tmp_path, missing)


@pytest.mark.meta
def test_a_grandchild_that_holds_the_pipes_after_the_child_exited_is_killed() -> None:
    # The direct child exits at once but leaves a grandchild holding stdout/stderr open, so the
    # parent's wait never ends. Killing the child's tree at the cap must still reach the
    # grandchild even though its parent is already gone.
    code = (
        "import subprocess, sys\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
        "print(child.pid, flush=True)\n"
    )
    started = time.monotonic()
    with pytest.raises(IsolatedTimeout) as caught:
        run_isolated(code, timeout=3)
    assert time.monotonic() - started < 25
    grandchild = int(caught.value.stdout.split()[0])
    deadline = time.monotonic() + 15
    while pid_alive(grandchild) and time.monotonic() < deadline:
        time.sleep(0.1)
    if pid_alive(grandchild):
        table = subprocess.run(
            ["ps", "-eo", "pid,ppid,pgid,sid,stat,etimes,cmd"], capture_output=True, text=True
        ).stdout
        status = Path(f"/proc/{grandchild}/status").read_text(encoding="utf-8")
        raise AssertionError(
            "DIAG grandchild=" + str(grandchild) + chr(10) + status + chr(10) + table
        )


@pytest.mark.meta
def test_an_interruption_kills_the_child(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ctrl-C (or any exception) while waiting must not leave the child running.
    started: list[int] = []

    def interrupted(self: subprocess.Popen[str], *args: object, **kwargs: object) -> NoReturn:
        started.append(self.pid)
        raise KeyboardInterrupt

    monkeypatch.setattr(subprocess.Popen, "communicate", interrupted)
    with pytest.raises(KeyboardInterrupt):
        run_isolated("import time; time.sleep(300)", timeout=30)
    monkeypatch.undo()
    (pid,) = started
    deadline = time.monotonic() + 15
    while pid_alive(pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not pid_alive(pid), "the child outlived the interruption"


@pytest.mark.meta
@pytest.mark.skipif(sys.platform == "win32", reason="a new session is a POSIX concept")
def test_output_printed_before_the_cap_survives_a_grandchild_that_escaped_the_group() -> None:
    # A grandchild in a session of its own is outside the killed group and keeps the pipes open.
    # The text the child printed before the cap must still reach the failure message.
    code = (
        "import subprocess, sys, time\n"
        "g = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'],\n"
        "                     start_new_session=True)\n"
        "print('escaped', g.pid, flush=True)\n"
        "time.sleep(300)\n"
    )
    pid = 0
    try:
        with pytest.raises(IsolatedTimeout) as caught:
            run_isolated(code, timeout=4)
        printed = caught.value.stdout.split()
        assert printed[:1] == ["escaped"], caught.value.stdout
        pid = int(printed[1])
    finally:
        if pid:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, getattr(signal, "SIGKILL"))  # noqa: B009


@pytest.mark.meta
def test_orphan_kill_many_times() -> None:
    code = (
        "import subprocess, sys\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)'])\n"
        "print(child.pid, flush=True)\n"
    )
    bad: list[str] = []
    for attempt in range(25):
        with pytest.raises(IsolatedTimeout) as caught:
            run_isolated(code, timeout=2)
        grandchild = int(caught.value.stdout.split()[0])
        deadline = time.monotonic() + 6
        while pid_alive(grandchild) and time.monotonic() < deadline:
            time.sleep(0.1)
        if pid_alive(grandchild):
            table = subprocess.run(
                ["ps", "-eo", "pid,ppid,pgid,sid,stat,etimes,cmd"], capture_output=True, text=True
            ).stdout
            status = Path(f"/proc/{grandchild}/status").read_text(encoding="utf-8")
            bad.append(f"attempt {attempt} pid {grandchild}\n{status}\n{table}")
    assert not bad, "DIAGMANY " + str(len(bad)) + "\n" + "\n".join(bad[:2])
