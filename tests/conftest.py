"""Pytest configuration. Speed-up: zero tenacity sleeps during tests."""

import pytest


@pytest.fixture(autouse=True)
def _fast_retries(monkeypatch):
    """Make tenacity retries instant during tests."""
    import tenacity

    monkeypatch.setattr(tenacity, "nap", lambda *a, **kw: None)
