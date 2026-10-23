"""The public documents agree with the code: links resolve, ADR references exist, examples run."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from _support import run_isolated

ROOT = Path(__file__).resolve().parent.parent
_DOCS = ("README.md", "CONTRIBUTING.md", "CHANGELOG.md")
_LINK = re.compile(r"\]\(([^)\s]+)\)")
_ADR = re.compile(r"\bADR-(\d{4})\b")


def markdown_files(root: Path) -> list[Path]:
    return [root / name for name in _DOCS] + sorted((root / "docs").glob("*.md"))


def _slug(heading: str) -> str:
    """The anchor GitHub gives a heading: lower case, punctuation dropped, spaces to hyphens."""
    return re.sub(r"[^\w\- ]", "", heading.strip().lower().replace("`", "")).replace(" ", "-")


def _anchors(path: Path) -> set[str]:
    headings = re.findall(r"^#{1,6}\s+(.+?)\s*$", path.read_text(encoding="utf-8"), re.MULTILINE)
    return {_slug(h) for h in headings}


def _resolves(destination: Path, anchor: str) -> bool:
    if not destination.exists():
        return False
    return not anchor or destination.suffix != ".md" or anchor in _anchors(destination)


def broken_links(root: Path) -> list[str]:
    """Relative links whose file does not exist, or whose #anchor is not a heading of that file."""
    broken = []
    for path in markdown_files(root):
        for target in _LINK.findall(path.read_text(encoding="utf-8")):
            if re.match(r"^[a-z][a-z0-9+.-]*:", target):
                continue  # an absolute URL
            relative, _, anchor = target.partition("#")
            destination = path.parent / relative if relative else path
            if not _resolves(destination, anchor):
                broken.append(f"{path.relative_to(root).as_posix()} -> {target}")
    return broken


def adr_numbers(root: Path) -> list[int]:
    decisions = (root / "docs" / "decisions.md").read_text(encoding="utf-8")
    return sorted(int(n) for n in re.findall(r"^## ADR-(\d{4})\b", decisions, re.MULTILINE))


def unknown_adrs(root: Path) -> list[str]:
    """ADR numbers cited in the docs or the code that decisions.md does not define."""
    decisions = (root / "docs" / "decisions.md").read_text(encoding="utf-8")
    defined = set(re.findall(r"^## ADR-(\d{4})\b", decisions, re.MULTILINE))
    cited: dict[str, set[str]] = {}
    sources = [*markdown_files(root), *sorted((root / "src").rglob("*.py"))]
    for path in sources:
        for number in _ADR.findall(path.read_text(encoding="utf-8")):
            cited.setdefault(number, set()).add(path.relative_to(root).as_posix())
    return [f"ADR-{n} in {sorted(files)}" for n, files in sorted(cited.items()) if n not in defined]


def test_every_relative_link_resolves() -> None:
    assert not broken_links(ROOT)


def test_the_adr_numbers_have_no_gap() -> None:
    numbers = adr_numbers(ROOT)
    assert numbers == list(range(1, len(numbers) + 1))


def test_every_cited_adr_is_defined() -> None:
    assert not unknown_adrs(ROOT)


def _example_body() -> str:
    text = (ROOT / "examples" / "quickstart.py").read_text(encoding="utf-8")
    return text.split('"""\n\n', 1)[1].rstrip("\n")


def test_the_readme_shows_the_quickstart_as_written() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert _example_body() in readme


@pytest.mark.parametrize("example", sorted((ROOT / "examples").glob("*.py")), ids=lambda p: p.name)
def test_every_example_runs(example: Path) -> None:
    result = run_isolated(
        f"import runpy; runpy.run_path({str(example)!r}, run_name='__main__')", timeout=60
    )
    assert result.returncode == 0, result.stderr[-600:]


def test_the_readme_shows_what_the_quickstart_prints() -> None:
    result = run_isolated(
        f"import runpy; runpy.run_path({str(ROOT / 'examples' / 'quickstart.py')!r}, "
        "run_name='__main__')",
        timeout=60,
    )
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert result.stdout.strip() in readme


def test_the_quickstart_finds_the_bug_replays_it_and_passes_once_fixed() -> None:
    result = run_isolated(
        f"import runpy; runpy.run_path({str(ROOT / 'examples' / 'quickstart.py')!r}, "
        "run_name='__main__')",
        timeout=60,
    )
    lines = result.stdout.splitlines()
    assert lines[0].startswith("server without memory: InvariantError(")
    assert lines[1].endswith("the same failure, every time")
    assert lines[2] == "server that remembers: 200 seeds checked, failing seed: None"


def _tree(tmp_path: Path, *, readme: str, decisions: str, code: str = "") -> Path:
    (tmp_path / "docs").mkdir()
    (tmp_path / "src").mkdir()
    (tmp_path / "README.md").write_text(readme, encoding="utf-8")
    (tmp_path / "CONTRIBUTING.md").write_text("", encoding="utf-8")
    (tmp_path / "CHANGELOG.md").write_text("", encoding="utf-8")
    (tmp_path / "docs" / "decisions.md").write_text(decisions, encoding="utf-8")
    (tmp_path / "src" / "mod.py").write_text(code, encoding="utf-8")
    return tmp_path


def test_a_link_to_a_missing_file_is_found(tmp_path: Path) -> None:
    root = _tree(
        tmp_path,
        readme="[ok](docs/decisions.md) [gone](docs/missing.md) [web](https://example.org/a)",
        decisions="",
    )
    assert broken_links(root) == ["README.md -> docs/missing.md"]


def test_a_cited_adr_that_is_not_defined_is_found(tmp_path: Path) -> None:
    root = _tree(
        tmp_path,
        readme="see ADR-0001 and ADR-0009",
        decisions="## ADR-0001 — one\n",
        code="# ADR-0042\n",
    )
    assert unknown_adrs(root) == [
        "ADR-0009 in ['README.md']",
        "ADR-0042 in ['src/mod.py']",
    ]


def test_a_link_to_a_missing_heading_is_found(tmp_path: Path) -> None:
    root = _tree(
        tmp_path,
        readme=(
            "[good](docs/decisions.md#adr-0001--one) [bad](docs/decisions.md#adr-0002) [own](#nope)"
        ),
        decisions="## ADR-0001 — one\n",
    )
    assert broken_links(root) == [
        "README.md -> docs/decisions.md#adr-0002",
        "README.md -> #nope",
    ]
