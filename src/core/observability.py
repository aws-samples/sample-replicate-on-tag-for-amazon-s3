"""Structured, timestamped observability / logging for the tag-based S3 replication Solution.

All public functions return JSON-serialisable dicts with a ``"timestamp"`` key
in ISO 8601 format.  Every returned dict can be passed directly to ``emit``,
which writes it to the standard Python logging infrastructure.

Requirements: 11.1, 11.2, 11.3, 11.4, 11.5
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, UTC

logger = logging.getLogger(__name__)

# AWS Lambda's Python runtime defaults the root logger to WARNING when the
# function's log format is plain text (the default), so INFO-level records
# — which is what every observability entry in this module is emitted at,
# including `audit` entries such as `journal_read_capped` (Requirement 6.1)
# — are silently dropped unless something raises the level explicitly.
# Setting this module's own logger to INFO fixes that: Python's effective-
# level lookup stops at the first ancestor (including the logger itself)
# whose level is not NOTSET, so this logger's INFO level takes precedence
# over the root logger's WARNING default regardless of whether the root is
# ever configured. Records still propagate to the root logger's handler
# (CloudWatch Logs) normally, since propagation is independent of the
# root's own level. This makes every entry `emit()` produces — `interval_
# summary`, `job_submitted`, `audit` (including `journal_read_capped`),
# `reinvocation_triggered`, `reinvocation_chain_limit_reached` — visible in
# the deployed Lambda's default logging configuration without requiring an
# operator to configure Lambda Advanced Logging Controls.
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    """Return the current UTC datetime (used as the default timestamp)."""
    return datetime.now(tz=UTC)


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def redact_object_key(key: str | None) -> str:
    """Return a non-reversible, log-safe representation of an S3 object key.

    S3 object keys frequently embed sensitive information (PII-laden path
    structures, internal project names, confidential file names). Logging them
    verbatim leaks that information into the log pipeline. This helper replaces
    the raw key with a stable SHA-256 fingerprint (first 12 hex chars) plus the
    key length, so log entries remain correlatable across runs without
    disclosing the key contents.

    Parameters
    ----------
    key
        The raw object key, or ``None``/empty.

    Returns
    -------
    str
        ``"<redacted-key sha256:<12 hex> len=<n>>"`` for a non-empty key, or
        ``"<redacted-key empty>"`` when the key is ``None`` or empty.
    """
    if not key:
        return "<redacted-key empty>"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
    return f"<redacted-key sha256:{digest} len={len(key)}>"


def _iso(ts: datetime) -> str:
    """Render *ts* as an ISO 8601 string with UTC offset.

    If *ts* is naive it is assumed to be UTC and the ``+00:00`` suffix is
    appended explicitly so the serialised value always carries timezone
    information.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.isoformat()


# ---------------------------------------------------------------------------
# Public log-entry constructors
# ---------------------------------------------------------------------------


def log_summary(
    ops_read: int,
    matched_objects: int,
    jobs_submitted: int,
    duplicate_records_discarded: int,
    timestamp: datetime | None = None,
) -> dict:
    """Return a structured summary entry for a completed Processing_Interval.

    Counts
    ------
    ops_read
        Count of *distinct* logical tagging operations forwarded to matching
        (raw records read minus duplicates discarded).  Zero when none were
        read.  (Requirements 11.1, 11.2)
    matched_objects
        Count of Matched_Object entries accumulated across the interval.
        Zero when none matched.  (Requirements 11.1, 11.2)
    jobs_submitted
        Count of Batch_Replication_Job submissions for the interval.  Zero
        when no jobs were submitted.  (Requirements 11.1, 11.2)
    duplicate_records_discarded
        Raw journal records read minus distinct logical operations.  Lets
        operators observe journal duplication without conflating it with the
        primary counts.
    timestamp
        Optional explicit timestamp; defaults to ``datetime.now(tz=timezone.utc)``.
        (Requirement 11.5)

    Returns
    -------
    dict
        JSON-serialisable dict with a ``"timestamp"`` key.

    Example
    -------
    >>> entry = log_summary(10, 3, 1, 2)
    >>> entry["event"]
    'interval_summary'
    >>> entry["Tagging_Operations"]
    10
    """
    ts = timestamp if timestamp is not None else _now_utc()
    return {
        "event": "interval_summary",
        "timestamp": _iso(ts),
        "Tagging_Operations": ops_read,
        "Matched_Objects": matched_objects,
        "Batch_Replication_Job_submissions": jobs_submitted,
        "duplicate_records_discarded": duplicate_records_discarded,
    }


def log_submission(
    job_id: str,
    source_bucket: str,
    timestamp: datetime | None = None,
) -> dict:
    """Return a structured entry for a successful Batch_Replication_Job submission.

    Parameters
    ----------
    job_id
        The job identifier returned by S3 Batch Operations.  (Requirement 11.3)
    source_bucket
        The source bucket name the job was submitted for.  (Requirement 11.3)
    timestamp
        Optional explicit timestamp; defaults to ``datetime.now(tz=timezone.utc)``.
        (Requirement 11.5)

    Returns
    -------
    dict
        JSON-serialisable dict with ``"job_id"``, ``"source_bucket"``, and
        ``"timestamp"`` keys.

    Example
    -------
    >>> entry = log_submission("job-abc-123", "my-bucket")
    >>> entry["event"]
    'job_submitted'
    >>> entry["job_id"]
    'job-abc-123'
    """
    ts = timestamp if timestamp is not None else _now_utc()
    return {
        "event": "job_submitted",
        "timestamp": _iso(ts),
        "job_id": job_id,
        "source_bucket": source_bucket,
    }


def log_error(
    component: str,
    bucket: str,
    cause: str,
    timestamp: datetime | None = None,
) -> dict:
    """Return a structured error entry.

    Parameters
    ----------
    component
        Name of the component where the error occurred
        (e.g., ``"Rule_Deriver"``, ``"Journal_Monitor"``).  (Requirement 11.4)
    bucket
        The affected source bucket name.  (Requirement 11.4)
    cause
        A description identifying the cause of the failure.  (Requirement 11.4)
    timestamp
        Optional explicit timestamp; defaults to ``datetime.now(tz=timezone.utc)``.
        (Requirement 11.5)

    Returns
    -------
    dict
        JSON-serialisable dict with ``"component"``, ``"bucket"``,
        ``"cause"``, and ``"timestamp"`` keys.

    Example
    -------
    >>> entry = log_error("Journal_Monitor", "my-bucket", "Access denied")
    >>> entry["event"]
    'error'
    >>> entry["component"]
    'Journal_Monitor'
    """
    ts = timestamp if timestamp is not None else _now_utc()
    return {
        "event": "error",
        "timestamp": _iso(ts),
        "component": component,
        "bucket": bucket,
        "cause": cause,
    }


def log_reinvocation_triggered(
    chain_position: int,
    timestamp: datetime | None = None,
) -> dict:
    """Return a structured entry for a Self_Reinvocation trigger.

    Emitted when a Capped_Run that progressed issues an async self-invoke to
    continue draining immediately rather than waiting for the next scheduled
    trigger (Requirement 6.2).

    Parameters
    ----------
    chain_position
        The ``reinvocation_depth`` carried by the newly-triggered invocation
        (i.e. the depth the *next* invocation will run at) — its position in
        the Reinvocation_Chain.
    timestamp
        Optional explicit timestamp; defaults to ``datetime.now(tz=timezone.utc)``.
        (Requirement 6.4)

    Returns
    -------
    dict
        JSON-serialisable dict with ``"event": "reinvocation_triggered"``,
        ``"chain_position"``, and ``"timestamp"`` keys.

    Example
    -------
    >>> entry = log_reinvocation_triggered(3)
    >>> entry["event"]
    'reinvocation_triggered'
    >>> entry["chain_position"]
    3
    """
    ts = timestamp if timestamp is not None else _now_utc()
    return {
        "event": "reinvocation_triggered",
        "timestamp": _iso(ts),
        "chain_position": chain_position,
    }


def log_reinvocation_chain_limit_reached(
    chain_limit: int,
    depth: int,
    timestamp: datetime | None = None,
) -> dict:
    """Return a structured entry recording that the Reinvocation_Chain_Limit
    stopped further Self_Reinvocation while backlog remains (Requirement 6.3).

    Emitted when a run was otherwise eligible to reinvoke (it was a
    Capped_Run that progressed) but ``depth >= chain_limit``, so
    ``should_reinvoke`` returned ``False`` for that reason specifically —
    distinct from a run that simply had nothing left to reinvoke for (not
    capped, not progressed, or an inactive bucket), which does not emit this
    entry.

    Parameters
    ----------
    chain_limit
        The configured ``Reinvocation_Chain_Limit``.
    depth
        The ``reinvocation_depth`` at which the limit was hit.
    timestamp
        Optional explicit timestamp; defaults to ``datetime.now(tz=timezone.utc)``.
        (Requirement 6.4)

    Returns
    -------
    dict
        JSON-serialisable dict with
        ``"event": "reinvocation_chain_limit_reached"``, ``"chain_limit"``,
        ``"depth"``, and ``"timestamp"`` keys.

    Example
    -------
    >>> entry = log_reinvocation_chain_limit_reached(20, 20)
    >>> entry["event"]
    'reinvocation_chain_limit_reached'
    >>> entry["chain_limit"]
    20
    """
    ts = timestamp if timestamp is not None else _now_utc()
    return {
        "event": "reinvocation_chain_limit_reached",
        "timestamp": _iso(ts),
        "chain_limit": chain_limit,
        "depth": depth,
    }


def log_audit(
    action: str,
    source_bucket: str,
    details: dict | None = None,
    timestamp: datetime | None = None,
) -> dict:
    """Return a structured audit entry for a security-critical state mutation.

    Audit entries record the consequential operations that determine *what*
    gets replicated and *with what permissions*: checkpoint advancement, lease
    acquisition/release, and the IAM role passed (``iam:PassRole``) to each
    S3 Batch Operations job. Without these, a security investigation cannot
    reconstruct when a checkpoint moved and to what value, when leases changed,
    or which role was passed to a given job.

    Audit entries deliberately carry only non-sensitive identifiers (sequence
    numbers, bucket names, role ARNs, job/lease IDs). Object keys MUST NOT be
    placed in ``details``; use :func:`redact_object_key` if a key-derived value
    is ever required.

    Parameters
    ----------
    action
        Short machine-readable action name, e.g. ``"lease_acquired"``,
        ``"lease_released"``, ``"batch_job_created"``.
    source_bucket
        The source bucket the mutation applies to.
    details
        Optional additional non-sensitive fields merged into the entry
        (e.g. ``checkpoint_from``/``checkpoint_to``, ``lease_id``,
        ``replication_role_arn``, ``job_id``).
    timestamp
        Optional explicit timestamp; defaults to ``datetime.now(tz=timezone.utc)``.

    Returns
    -------
    dict
        JSON-serialisable dict with ``"event": "audit"``, ``"action"``,
        ``"source_bucket"``, and ``"timestamp"`` keys, plus any ``details``.

    Example
    -------
    >>> entry = log_audit("lease_acquired", "my-bucket", {"lease_id": "abc"})
    >>> entry["event"]
    'audit'
    >>> entry["action"]
    'lease_acquired'
    """
    ts = timestamp if timestamp is not None else _now_utc()
    entry: dict = {
        "event": "audit",
        "timestamp": _iso(ts),
        "action": action,
        "source_bucket": source_bucket,
    }
    if details:
        entry.update(details)
    return entry


# ---------------------------------------------------------------------------
# Emitter
# ---------------------------------------------------------------------------


def emit(entry: dict) -> None:
    """Write a structured log entry to the Python logging infrastructure.

    ``error`` events are emitted at ``logging.ERROR``; all others (including
    ``audit``) at ``logging.INFO``.  The entry is serialised as a single-line
    JSON string so it is machine-parseable by log aggregators.

    Parameters
    ----------
    entry
        A dict returned by ``log_summary``, ``log_submission``, ``log_error``,
        or ``log_audit``.
    """
    line = json.dumps(entry, ensure_ascii=False)
    if entry.get("event") == "error":
        logger.error(line)
    else:
        logger.info(line)
