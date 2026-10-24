"""The suite is strict: warnings fail, unknown markers and options are rejected, leaks are blamed.

Each test runs a child ``pytest`` in a temporary project that carries this project's own pytest
configuration (and, where it matters, its ``conftest.py``), so the checks prove what the real
configuration does, not what it says.
"""

from __future__ import annotations

import shutil
import tomllib
from pathlib import Path

import pytest

from _support import ROOT, run_isolated

_TESTS = Path(__file__).resolve().parent


def _ini_options() -> dict[str, object]:
    with open(ROOT / "pyproject.toml", "rb") as handle:
        options = tomllib.load(handle)["tool"]["pytest"]["ini_options"]
    assert isinstance(options, dict)
    return options


def _write_config(project: Path, extra: str = "") -> None:
    options = _ini_options()
    lines = ["[pytest]"]
    for key, value in options.items():
        if key == "testpaths":
            continue
        if isinstance(value, list):
            lines.append(f"{key} =")
            lines.extend(f"    {item}" for item in value)
        else:
            lines.append(f"{key} = {value}")
    lines.append(extra)
    (project / "pytest.ini").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _run_child(project: Path, args: str = "", *, conftest: bool = False) -> tuple[int, str]:
    if conftest:
        shutil.copy(_TESTS / "conftest.py", project / "conftest.py")
    code = (
        f"import sys, pytest; sys.exit(pytest.main(['-p', 'no:cacheprovider', *{args.split()!r}]))"
    )
    result = run_isolated(code, timeout=120, cwd=project)
    return result.returncode, result.stdout + result.stderr


def test_a_warning_fails_the_test_that_raises_it(tmp_path: Path) -> None:
    _write_config(tmp_path)
    (tmp_path / "test_w.py").write_text(
        "import warnings\n\ndef test_warns():\n    warnings.warn('a deliberate warning')\n",
        encoding="utf-8",
    )
    code, output = _run_child(tmp_path)
    assert code != 0
    assert "a deliberate warning" in output


def test_an_unknown_marker_is_rejected(tmp_path: Path) -> None:
    _write_config(tmp_path)
    (tmp_path / "test_m.py").write_text(
        "import pytest\n\n@pytest.mark.not_a_marker\ndef test_x():\n    pass\n", encoding="utf-8"
    )
    code, output = _run_child(tmp_path, "-W ignore")  # only --strict-markers can reject it now
    assert code != 0
    assert "not_a_marker" in output


def test_an_unknown_configuration_key_is_rejected(tmp_path: Path) -> None:
    _write_config(tmp_path, extra="not_an_option = 1")
    (tmp_path / "test_x.py").write_text("def test_x():\n    pass\n", encoding="utf-8")
    code, output = _run_child(tmp_path, "-W ignore")  # only --strict-config can reject it now
    assert code != 0
    assert "not_an_option" in output


def test_the_random_order_seed_is_printed_even_when_quiet(tmp_path: Path) -> None:
    _write_config(tmp_path)
    (tmp_path / "test_x.py").write_text("def test_x():\n    pass\n", encoding="utf-8")
    code, output = _run_child(tmp_path, "-p randomly --randomly-seed=123", conftest=True)
    assert code == 0, output
    assert "random order seed: 123" in output
    assert "--randomly-seed=123" in output


def test_no_seed_line_is_printed_when_the_order_is_fixed(tmp_path: Path) -> None:
    _write_config(tmp_path)
    (tmp_path / "test_x.py").write_text("def test_x():\n    pass\n", encoding="utf-8")
    code, output = _run_child(tmp_path, "-p no:randomly", conftest=True)
    assert code == 0, output
    assert "random order seed" not in output


def test_a_test_that_leaves_a_loop_open_is_the_one_that_fails(tmp_path: Path) -> None:
    _write_config(tmp_path)
    (tmp_path / "test_leak.py").write_text(
        "from seedloop import World\n\n"
        "def test_leaks():\n    World(1)\n\n"
        "def test_innocent():\n    assert True\n",
        encoding="utf-8",
    )
    code, output = _run_child(tmp_path, "-p no:randomly", conftest=True)
    assert code != 0
    assert "test_leaks" in output
    assert "make_world" in output
    assert "test_innocent" not in output.split("short test summary info")[-1]


def test_a_world_from_the_fixture_is_closed_for_the_test(tmp_path: Path) -> None:
    _write_config(tmp_path)
    (tmp_path / "test_ok.py").write_text(
        "def test_uses_the_fixture(make_world):\n"
        "    world = make_world(1)\n"
        "    assert world.seed == 1\n",
        encoding="utf-8",
    )
    code, output = _run_child(tmp_path, "-p no:randomly", conftest=True)
    assert code == 0, output


def test_the_project_configuration_asks_for_strictness() -> None:
    options = _ini_options()
    assert "error" in list(options["filterwarnings"])  # type: ignore[call-overload]
    addopts = str(options["addopts"])
    assert "--strict-markers" in addopts
    assert "--strict-config" in addopts


@pytest.mark.parametrize(
    "keeps_a_fixed_order",
    [
        "scripts/mutation_sweep.py",
        ".github/workflows/release.yml",
        ".github/workflows/nightly.yml",
        ".github/workflows/ci.yml",
    ],
)
def test_what_must_not_depend_on_the_order_says_so(keeps_a_fixed_order: str) -> None:
    path = ROOT / keeps_a_fixed_order
    if not path.exists():
        pytest.skip("workflow files are not part of the source distribution")
    assert "no:randomly" in path.read_text(encoding="utf-8")


def test_the_tracker_closes_the_loop_it_found_so_it_cannot_warn_later(tmp_path: Path) -> None:
    _write_config(tmp_path)
    (tmp_path / "test_two.py").write_text(
        "import gc\n"
        "from seedloop import World\n\n"
        "def test_leaks():\n    World(1)\n\n"
        "def test_later():\n    gc.collect()\n",
        encoding="utf-8",
    )
    _, output = _run_child(tmp_path, "-p no:randomly", conftest=True)
    summary = output.split("short test summary info")[-1]
    assert "test_leaks" in summary
    assert "test_later" not in summary


def test_a_task_left_pending_on_purpose_does_not_log_in_a_later_audit_window(
    tmp_path: Path,
) -> None:
    _write_config(tmp_path)
    (tmp_path / "test_pending.py").write_text(
        "import asyncio\n"
        "import gc\n"
        "import seedloop\n\n"
        "def test_leaves_a_pending_task(make_world):\n"
        "    loop = make_world(1)._loop\n\n"
        "    async def forever():\n"
        "        await loop.create_future()\n\n"
        "    holder = [loop.create_task(forever())]\n"
        "    loop.run_until_complete(asyncio.sleep(0))  # let it start and wait\n"
        "    holder.append(holder)  # only the cyclic collector can free it, later\n\n"
        "def test_collects_inside_an_audit_window():\n"
        "    with seedloop.audit_mode():\n"
        "        gc.collect()\n",
        encoding="utf-8",
    )
    code, output = _run_child(tmp_path, "-p no:randomly", conftest=True)
    assert code == 0, output
