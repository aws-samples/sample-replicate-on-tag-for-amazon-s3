"""Canonical journal watermark helpers.

The Solution checkpoints journal progress on the S3 Metadata journal's
``record_timestamp`` column rather than on ``sequence_number``.  Amazon S3
documents that ``sequence_number`` ordering is only meaningful **for a given
bucket and key** — it is not a globally comparable cursor across different
keys.  ``record_timestamp`` is a globally comparable timestamp, so it is the
correct field for a single per-bucket cross-key watermark.

A *watermark* is the ``record_timestamp`` of the most recent journal record the
Solution has successfully processed for a bucket.  To make the watermark cheap
to compare and persist, it is stored as a **canonical UTC ISO-8601 string** with
fixed microsecond precision and a trailing ``Z``::

    2024-11-15T23:26:44.899000Z

Because the format is fixed-width and zero-padded, lexicographic string
comparison of two canonical watermarks is identical to chronological
comparison.  The rest of the Solution compares watermark strings with
``<`` / ``max`` — lexicographic ordering is sound because the format is
fixed-width and zero-padded, and the underlying ordering is globally
meaningful.

``sequence_number`` is still used — but only for its documented purpose:
breaking ties between duplicate journal deliveries of the *same* logical
operation on the *same* key (see :mod:`src.core.journal_dedup`).
"""
from __future__ import annotations

from datetime import datetime, timedelta, UTC

# Canonical watermark format: fixed-width, UTC, microsecond precision, ``Z``
# suffix.  Fixed width is what makes lexicographic == chronological ordering.
_CANONICAL_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

# The empty string is the "beginning of time" watermark: it sorts below every
# canonical timestamp, so on the first run for a bucket every record is newer.
EPOCH_WATERMARK = ""


# How far beyond "now" a persisted watermark may sit before it is rejected as
# implausible. A watermark is derived from a journal ``record_timestamp``, so a
# value meaningfully in the future did not come from the journal. Clock skew
# between the Lambda and the journal writer is the only legitimate source of a
# small forward offset; 24 hours is far wider than any real skew while still
# rejecting the "year 9999" poisoning case, where a far-future watermark makes
# every subsequent journal query return nothing and silently halts replication
# for the bucket.
MAX_FUTURE_SKEW = timedelta(hours=24)


def is_canonical_watermark(value: str) -> bool:
    """True iff *value* is the epoch watermark or a canonical watermark string.

    Canonical means exactly the fixed-width form :func:`to_watermark` emits.
    Anything else — a bare date, a non-``Z`` offset, extra precision, trailing
    whitespace — is not comparable lexicographically against real watermarks
    and is therefore not a watermark this Solution wrote.
    """
    if value == EPOCH_WATERMARK:
        return True
    try:
        parsed = datetime.strptime(value, _CANONICAL_FORMAT)
    except (ValueError, TypeError):
        return False
    # strptime accepts some inputs that round-trip to a different string
    # (e.g. a single-digit month), so require an exact round-trip.
    return parsed.replace(tzinfo=UTC).strftime(_CANONICAL_FORMAT) == value


def is_plausible_watermark(value: str, now: datetime | None = None) -> bool:
    """True iff *value* is canonical and not more than :data:`MAX_FUTURE_SKEW`
    beyond *now*.

    The state object is a read-write S3 object. A principal able to write it
    could otherwise store a well-formed but far-future watermark, which halts
    all further replication for the bucket without any error. This is the
    bound that makes such a value fail loudly at read time instead.

    Args:
        value: A watermark string, canonical or the epoch.
        now:   Reference time; defaults to the current UTC time.
    """
    if not is_canonical_watermark(value):
        return False
    if value == EPOCH_WATERMARK:
        return True
    reference = now or datetime.now(UTC)
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=UTC)
    return parse_watermark(value) <= reference + MAX_FUTURE_SKEW


def to_watermark(dt: datetime) -> str:
    """Convert a :class:`~datetime.datetime` to its canonical watermark string.

    Naive datetimes are assumed to be UTC; aware datetimes are converted to
    UTC.  The result is fixed-width with microsecond precision and a ``Z``
    suffix, so lexicographic comparison equals chronological comparison.

    Args:
        dt: The timestamp to canonicalize (typically a
            ``TaggingOperation.event_time``).

    Returns:
        The canonical UTC watermark string.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    return dt.strftime(_CANONICAL_FORMAT)


def parse_watermark(value: str) -> datetime:
    """Parse a canonical watermark string back into a UTC datetime.

    Args:
        value: A canonical watermark string produced by :func:`to_watermark`.

    Returns:
        The timezone-aware (UTC) datetime the watermark represents.

    Raises:
        ValueError: If *value* is empty or not in canonical form.
    """
    if not value:
        raise ValueError("cannot parse the empty (epoch) watermark")
    return datetime.strptime(value, _CANONICAL_FORMAT).replace(tzinfo=UTC)


def subtract(watermark: str, lookback: timedelta) -> str:
    """Return the canonical watermark *lookback* earlier than *watermark*.

    Used to compute the **lookback window start**: the journal is queried for
    records whose ``record_timestamp`` is greater than this value, so records
    that arrived in the journal late (with a ``record_timestamp`` at or below
    the current watermark, because the journal is only eventually consistent)
    are re-scanned and given a chance to be processed.  Re-scanned records that
    were already submitted are suppressed by the processed-operation window
    (see :func:`src.core.checkpoint_logic.is_eligible`), so the lookback never
    causes an object to be replicated twice.

    The epoch watermark (empty string) has no earlier point, so it is returned
    unchanged — on the first run the journal is read from the beginning
    regardless of the lookback.

    Args:
        watermark: A canonical watermark string, or ``""`` for the epoch.
        lookback:  How far back to extend the window.  Must be non-negative.

    Returns:
        The canonical watermark *lookback* before *watermark*, or ``""`` when
        *watermark* is the epoch.
    """
    if watermark == EPOCH_WATERMARK:
        return EPOCH_WATERMARK
    if lookback < timedelta(0):
        raise ValueError(f"lookback must be non-negative, got {lookback!r}")
    return to_watermark(parse_watermark(watermark) - lookback)
