import seedloop


def test_version_is_string() -> None:
    assert isinstance(seedloop.__version__, str)


def test_public_api_is_empty_for_now() -> None:
    # Phase 0 ships an empty package; public symbols arrive in Phase 1.
    assert seedloop.__all__ == []
