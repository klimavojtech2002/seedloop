import seedloop


def test_version_is_string() -> None:
    assert isinstance(seedloop.__version__, str)


def test_public_api_is_exported() -> None:
    # The Phase-1 entry points and types are importable from the top-level package.
    for name in ("World", "check", "replay", "CheckResult", "SeedloopError"):
        assert name in seedloop.__all__
        assert hasattr(seedloop, name)


def test_check_result_is_an_immutable_value() -> None:
    import dataclasses

    from seedloop import CheckResult

    result = CheckResult(checked=3, failing_seed=None, error=None)
    assert hash(result) == hash(CheckResult(checked=3, failing_seed=None, error=None))
    try:
        result.checked = 5  # type: ignore[misc]
    except dataclasses.FrozenInstanceError:
        return
    raise AssertionError("CheckResult must be frozen")
