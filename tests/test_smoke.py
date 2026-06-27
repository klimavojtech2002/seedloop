import seedloop


def test_version_is_string() -> None:
    assert isinstance(seedloop.__version__, str)


def test_public_api_is_exported() -> None:
    # The Phase-1 entry points and types are importable from the top-level package.
    for name in ("World", "check", "replay", "CheckResult", "SeedloopError"):
        assert name in seedloop.__all__
        assert hasattr(seedloop, name)
