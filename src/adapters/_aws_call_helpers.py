"""Shared helpers for wrapping boto3 calls and reading ClientError reasons.

Consolidates two helpers that were previously copy-pasted across
``batch_operations_adapter.py``, ``sns_report_adapter.py``, and
``source_status_adapter.py`` (code-review-remediation spec Req 5/8.3):

* :func:`call_with_timeout` — a thread-backed wall-clock timeout for a single
  boto3 call.
* :func:`client_error_reason` — extracts a human-readable ``"<Code>: <Message>"``
  string from a :class:`~botocore.exceptions.ClientError`.

Note on the timeout wrapper's real bound (code-review-remediation spec Req 5):
a ``ThreadPoolExecutor``-based timeout cannot, on its own, reliably bound a
call whose underlying socket read never returns — leaving the
``with ThreadPoolExecutor(...)`` block calls ``shutdown(wait=True)``, which
blocks until the hung worker thread finishes. The actual fix for a TCP-level
network stall is a *socket-level* timeout, configured once via
``botocore.config.Config(connect_timeout=..., read_timeout=...)`` on every
client :class:`~src.adapters.client_factory.ClientFactory` constructs. This
wrapper remains as a defense-in-depth bound (and to preserve the
``TimeoutError`` outcome callers already branch on), but the client-level
timeout is what actually returns control within the configured budget.
"""
from __future__ import annotations

import concurrent.futures
from collections.abc import Callable
from typing import TypeVar

from botocore.exceptions import ClientError

_T = TypeVar("_T")

# Default wall-clock seconds to wait for a single AWS API call. Matches the
# budget every adapter using this helper previously hard-coded individually.
DEFAULT_TIMEOUT_SECONDS: float = 60.0


def call_with_timeout(
    fn: Callable[[], _T], *, timeout: float = DEFAULT_TIMEOUT_SECONDS
) -> _T:
    """Run *fn()* in a worker thread; raise :exc:`TimeoutError` if it exceeds *timeout* s.

    Parameters
    ----------
    fn:
        Zero-argument callable whose return value is forwarded to the caller.
    timeout:
        Maximum wall-clock seconds to wait. Defaults to
        :data:`DEFAULT_TIMEOUT_SECONDS`.

    Raises
    ------
    TimeoutError
        If *fn()* does not complete within *timeout* seconds. Note this
        wrapper alone cannot forcibly interrupt a hung underlying call (see
        module docstring) — the socket-level timeout configured on the
        client passed into *fn* is what actually bounds a TCP-level stall.
    Exception
        Any exception raised by *fn()* is re-raised as-is so callers can
        distinguish AWS ``ClientError`` from ``TimeoutError``.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fn)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError as exc:
            raise TimeoutError(
                f"AWS API call did not complete within {timeout:.0f} seconds"
            ) from exc


def client_error_reason(exc: ClientError) -> str:
    """Extract a human-readable rejection reason from a boto3 ``ClientError``.

    Returns ``"<Code>: <Message>"`` when both are present, or whichever is
    non-empty, or the raw string representation as a last resort, so the
    failure a caller logs is never a blank string.
    """
    error = exc.response.get("Error", {})
    code: str = error.get("Code", "")
    message: str = error.get("Message", "")
    if code and message:
        return f"{code}: {message}"
    if code:
        return code
    if message:
        return message
    return str(exc)
