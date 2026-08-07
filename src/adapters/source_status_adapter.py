"""Completion_Tracker adapter — reads the source object's native replication status.

This is the thin I/O shell for Requirement 3's Source_Status_Check
(design.md Decision 4): a ``HeadObject`` call against the **source** object
version, reading the ``x-amz-replication-status`` header (boto3 field name
``ReplicationStatus``) S3 natively sets on a source object subject to
cross-region or same-region replication.

This module takes the *same* source-side ``s3_client`` the orchestrator
already constructs via ``ClientFactory.create_s3_client(region=bucket.region)``
(``src/orchestrator.py::_process_bucket``) — it never constructs a new client
type, and its public function accepts no destination-region or
destination-account parameter. This confirms the architectural note in
requirements.md verbatim: the Solution never constructs a destination-account
or destination-region client for Requirement 3, so
``ClientFactory.check_no_destination_client()`` requires no exception for
this code path (unlike ``destination_presence_adapter.py``, which is the
scoped exception for Requirement 2).

Requirements: 3.1, 3.4, 3.6
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from botocore.exceptions import ClientError

from src.adapters._aws_call_helpers import client_error_reason as _client_error_reason

# ---------------------------------------------------------------------------
# SourceStatusResult
# ---------------------------------------------------------------------------


class SourceStatusCheckKind(Enum):
    """Discriminant for :class:`SourceStatusResult`.

    * ``HEADER_VALUE``  — the ``ReplicationStatus`` field was present on the
      ``HeadObject`` response; ``SourceStatusResult.value`` holds its literal
      string value (``"PENDING"``, ``"COMPLETED"``, or ``"FAILED"``).
    * ``HEADER_ABSENT`` — the call succeeded but the response carried no
      ``ReplicationStatus`` field (Requirement 3.4).
    * ``OBJECT_GONE``   — the object version no longer exists (``HeadObject``
      returned 404). Terminal, not transient: no future check can succeed, so
      retrying forever would pin the Tracked_Object in ``PENDING`` and grow
      the state object without bound (Requirement 3.7).
    * ``CHECK_FAILED``  — the call itself failed with a transient AWS error
      (throttling, a temporary service error, a timeout, etc.) (Requirement 3.6).

    Note on 403 vs 404: S3 masks object existence from a caller lacking
    ``s3:ListBucket`` on the bucket, returning ``403 Forbidden`` where it
    would otherwise return ``404``. ``OBJECT_GONE`` is therefore only
    reachable when the execution role holds ``s3:ListBucket`` on the source
    bucket — which ``deploy/template.yaml`` grants for exactly this reason.
    Without it a deleted object presents as ``CHECK_FAILED`` and is retried
    indefinitely.
    """

    HEADER_VALUE = "HEADER_VALUE"
    HEADER_ABSENT = "HEADER_ABSENT"
    OBJECT_GONE = "OBJECT_GONE"
    CHECK_FAILED = "CHECK_FAILED"


@dataclass(frozen=True)
class SourceStatusResult:
    """Return value of :func:`check_source_replication_status`.

    Exactly one of the three :class:`SourceStatusCheckKind` variants is
    represented by ``kind``:

    Attributes
    ----------
    kind:
        Which of the three outcomes this result represents.
    value:
        The verbatim ``ReplicationStatus`` header value (``"PENDING"``,
        ``"COMPLETED"``, or ``"FAILED"``) when ``kind is HEADER_VALUE``;
        ``None`` otherwise.
    error_reason:
        Human-readable failure reason when ``kind is CHECK_FAILED``; ``None``
        otherwise. Never raised — the caller (the pure core's
        ``reconcile_source_status_check``) branches on ``kind`` rather than
        catching an exception, mirroring the non-raising result pattern used
        by ``batch_operations_adapter.SubmissionResult`` and
        ``sns_report_adapter.PublishResult``.
    """

    kind: SourceStatusCheckKind
    value: str | None = None
    error_reason: str | None = None


def _header_value(value: str) -> SourceStatusResult:
    """Construct a ``HEADER_VALUE`` result carrying the verbatim header value."""
    return SourceStatusResult(kind=SourceStatusCheckKind.HEADER_VALUE, value=value)


def _header_absent() -> SourceStatusResult:
    """Construct a ``HEADER_ABSENT`` result (Requirement 3.4)."""
    return SourceStatusResult(kind=SourceStatusCheckKind.HEADER_ABSENT)


def _check_failed(error_reason: str) -> SourceStatusResult:
    """Construct a ``CHECK_FAILED`` result carrying a human-readable reason (Req 3.6)."""
    return SourceStatusResult(kind=SourceStatusCheckKind.CHECK_FAILED, error_reason=error_reason)


def _object_gone(error_reason: str) -> SourceStatusResult:
    """Construct an ``OBJECT_GONE`` result for a 404 ``HeadObject`` (Req 3.7)."""
    return SourceStatusResult(kind=SourceStatusCheckKind.OBJECT_GONE, error_reason=error_reason)


# HTTP status and error codes that mean "this object version does not exist".
# ``HeadObject`` carries no response body, so botocore often surfaces a bare
# ``404`` with an empty error code rather than ``NoSuchKey``; both shapes are
# matched.
_GONE_ERROR_CODES = frozenset({"404", "NoSuchKey", "NoSuchVersion"})


def _is_object_gone(exc: ClientError) -> bool:
    """True iff *exc* indicates the object version no longer exists.

    Matches on the HTTP status code first (authoritative for ``HeadObject``,
    which returns no body for an error) and falls back to the error code.
    """
    response = exc.response or {}
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    if status == 404:
        return True
    return response.get("Error", {}).get("Code", "") in _GONE_ERROR_CODES


# ---------------------------------------------------------------------------
# check_source_replication_status — public interface
# ---------------------------------------------------------------------------


def check_source_replication_status(
    s3_client,
    source_bucket: str,
    object_key: str,
    version_id: str | None,
) -> SourceStatusResult:
    """Issue a Source_Status_Check against a source object version.

    Implements the ``HeadObject`` half of Requirement 3 (design.md Decision
    4): calls ``HeadObject`` against the **source** object version — passing
    ``VersionId=version_id`` only when *version_id* is not ``None`` — using
    the caller-supplied source-side *s3_client*, and reads the response's
    ``ReplicationStatus`` field (the boto3 name for the
    ``x-amz-replication-status`` header).

    This function constructs no client of any kind; *s3_client* is expected
    to be the same source-side client the orchestrator already built via
    ``ClientFactory.create_s3_client(region=bucket.region)``. The function
    signature accepts no destination-region or destination-account
    parameter, by design — Requirement 3's Source_Status_Check never touches
    the destination account or region.

    Parameters
    ----------
    s3_client:
        A boto3 ``s3`` client scoped to the **source** account and region.
    source_bucket:
        Source bucket name.
    object_key:
        The Replication_Item's object key.
    version_id:
        The Replication_Item's version id, or ``None`` for the null-version
        marker (unversioned bucket) — in which case ``VersionId`` is omitted
        from the ``HeadObject`` call entirely, rather than being passed as a
        literal ``None`` or empty string.

    Returns
    -------
    SourceStatusResult
        One of the three variants documented on :class:`SourceStatusResult`:
        ``HEADER_VALUE`` (the header was present; Requirement 3.2's callers
        branch on ``PENDING``/``FAILED``/``COMPLETED``), ``HEADER_ABSENT``
        (Requirement 3.4), or ``CHECK_FAILED`` (Requirement 3.6).

    Requirements: 3.1, 3.4, 3.6
    """
    kwargs: dict = {"Bucket": source_bucket, "Key": object_key}
    if version_id is not None:
        kwargs["VersionId"] = version_id

    try:
        response = s3_client.head_object(**kwargs)
    except ClientError as exc:
        if _is_object_gone(exc):
            # The object version has been deleted (lifecycle expiry, a
            # version-id DELETE, or test-data cleanup). Terminal: no later
            # check can ever succeed (Requirement 3.7).
            return _object_gone(_client_error_reason(exc))
        # A transient AWS error (throttling, a temporary service error, an
        # access issue, etc.) — leave the caller to retry at the next
        # Completion_Poll_Interval (Requirement 3.6). This adapter never
        # raises past this point; it always returns a SourceStatusResult.
        return _check_failed(_client_error_reason(exc))
    except Exception as exc:  # noqa: BLE001
        # Network error, unexpected SDK error, etc. — also treated as a
        # transient check failure (Requirement 3.6).
        return _check_failed(str(exc) or "unknown error during HeadObject")

    replication_status = response.get("ReplicationStatus")
    if replication_status:
        return _header_value(replication_status)
    return _header_absent()



