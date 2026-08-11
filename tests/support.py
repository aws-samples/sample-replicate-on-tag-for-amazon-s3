"""Shared test doubles.

Not a test module (no ``test_`` prefix), in the same vein as ``api_shape.py``.
"""
from unittest.mock import MagicMock

from src.core.models import BucketDisableState


def mock_state_store() -> MagicMock:
    """A ``MagicMock`` ``StateStore`` whose buckets read back as enabled.

    ``run_interval`` consults ``get_disable_state(...).disabled`` for every
    bucket before processing it. On a bare ``MagicMock`` that attribute is
    itself a ``MagicMock``, which is truthy, so every bucket would be skipped
    as disabled and every assertion about processing would fail for a reason
    that has nothing to do with what the test is checking. Returning a real
    :class:`~src.core.models.BucketDisableState` makes "enabled" the default
    and leaves each test free to override it.
    """
    store = MagicMock()
    store.get_disable_state.return_value = BucketDisableState()
    return store


def disabled_state(reason: str = "disabled for test", at: str = "2026-01-01T00:00:00+00:00"):
    """A disabled :class:`~src.core.models.BucketDisableState`."""
    return BucketDisableState(disabled=True, reason=reason, at=at)
