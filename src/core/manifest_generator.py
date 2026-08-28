"""Manifest_Generator — accumulates Matched_Objects for a Monitored_Bucket and
produces a single deduplicated S3 Batch Operations manifest per bucket per
interval, spanning every tag-scoped rule that matched (design.md D1).

Requirements: 1.1, 1.2, 6.1, 6.2, 6.3, 6.4, 6.6, 8.3
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

from src.core.models import ManifestEntry, MatchedObject


# ---------------------------------------------------------------------------
# ManifestResult — pure core result; I/O fields (s3_location, etag, created_at)
# are populated by the manifest-write adapter (task 14.1).
# ---------------------------------------------------------------------------


@dataclass
class ManifestResult:
    """Result returned by ManifestGenerator.finalize().

    Represents the single union manifest for one Monitored_Bucket per
    interval (design.md D1) — the entries are deduplicated by
    ``(source_bucket, object_key)`` across every tag-scoped rule that
    matched for the bucket, not scoped to any one
    ``replication_config_id``.

    When ``has_matches`` is ``True``, ``entries`` is non-empty and
    ``object_count`` equals ``len(entries)``.

    When ``has_matches`` is ``False``, ``entries`` is empty and
    ``object_count`` is 0.

    ``all_versioned`` is ``True`` when every entry has a non-None
    ``version_id``; used by the batch adapter to select the correct
    manifest fields (Bucket, Key, VersionId).
    """

    source_bucket: str
    has_matches: bool
    entries: list[ManifestEntry]  # distinct (source_bucket, object_key) pairs only (6.2, 6.3)
    object_count: int             # len(entries); 0 for no-matches; used in failure reports (10.1, 10.3)
    all_versioned: bool = False   # True when every entry carries a version_id


# ---------------------------------------------------------------------------
# Destination ARN rendering
# ---------------------------------------------------------------------------


def _destination_bucket_name(bucket_arn: str) -> str:
    """Render a destination bucket ARN as a plain bucket name.

    An S3 bucket ARN is ``arn:<partition>:s3:::<bucket>`` — it carries no
    account ID or Region — so the trailing segment is the whole identity and
    dropping the prefix loses nothing an operator needs.

    Any value that is not a recognisable S3 bucket ARN is returned unchanged
    rather than parsed optimistically, so an unexpected shape surfaces in the
    report as-is instead of being silently truncated to a misleading name.
    """
    prefix, sep, bucket = bucket_arn.rpartition(":::")
    if sep and prefix.startswith("arn:") and bucket:
        return bucket
    return bucket_arn


# ---------------------------------------------------------------------------
# ManifestGenerator
# ---------------------------------------------------------------------------


class ManifestGenerator:
    """Accumulates Matched_Objects across a Processing_Interval and produces
    one deduplicated union manifest per Monitored_Bucket (design.md D1).

    Entries matched by any of the bucket's tag-scoped rules are accumulated
    into a single per-bucket entry set, deduplicated by
    ``(source_bucket, object_key)`` — the ``replication_config_id`` that
    matched an object no longer partitions the accumulator or the resulting
    manifest; S3 Batch Replication routes each manifest object to every
    destination whose rule it matches from a single job (design.md
    "Why this is correct").

    Typical call sequence for one interval::

        gen = ManifestGenerator()
        gen.accumulate(matched_set_from_rule_matcher)  # zero or more calls
        result = gen.finalize("src-bucket")

    The generator is not reset between ``accumulate`` calls; all accumulated
    entries contribute to the same interval's manifest(s).  Construct a new
    instance for each Processing_Interval.
    """

    def __init__(self) -> None:
        # For each source_bucket, track a dict mapping
        # (source_bucket, object_key) → version_id.  Using a dict provides
        # O(1) deduplication — the first version_id seen for a given key wins
        # (first-write semantics; subsequent duplicate journal records —
        # including ones matched by a different rule/config_id for the same
        # object — are absorbed without overwriting the version_id).
        self._entries: dict[str, dict[tuple[str, str], str | None]] = defaultdict(dict)
        # Per-object timestamps for the completion report email.
        # Keyed identically to _entries; first-write wins (same dedup semantics).
        self._timestamps: dict[str, dict[tuple[str, str], tuple[datetime | None, datetime | None]]] = defaultdict(dict)
        # Per-object matched rule IDs and destination buckets for the
        # completion report email. Keyed identically to _entries, but
        # accumulated as a UNION rather than first-write-wins: an object
        # matched by two rules yields two Matched_Objects that collapse to one
        # manifest entry, and the report must name both rules and both
        # destinations rather than only whichever arrived first.
        self._matched_rules: dict[str, dict[tuple[str, str], set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        self._destinations: dict[str, dict[tuple[str, str], set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )

    # ------------------------------------------------------------------
    # accumulate
    # ------------------------------------------------------------------

    def accumulate(self, matched: set[MatchedObject]) -> None:
        """Add *matched* to the internal per-bucket accumulator.

        Collects every Matched_Object regardless of which rule(s) matched it
        (6.1, 8.3, design.md D1) into one entry set keyed by the object's
        ``source_bucket``. Duplicate entries — the same ``source_bucket`` ×
        ``object_key``, whether from a duplicate journal record or from a
        second rule matching the same object — are silently absorbed into a
        single manifest entry (6.3, design §Manifest_Generator, Req 1.1).

        The first ``version_id`` seen for a given ``(bucket, key)`` pair is
        retained; subsequent duplicates are ignored.

        ``matched_rule_ids`` and ``destination_bucket_arns`` are the exception
        to first-write-wins: they are accumulated as a union across every
        Matched_Object for the same ``(bucket, key)``, so an object matched by
        several rules reports all of them and all their destinations. Those
        unions are therefore updated outside the dedup guard below.

        Parameters
        ----------
        matched:
            Set of Matched_Objects produced by Rule_Matcher for a single
            Tagging_Operation (may be empty; safe to call with an empty set).
        """
        for obj in matched:
            key = (obj.source_bucket, obj.object_key)
            bucket_entries = self._entries[obj.source_bucket]
            if key not in bucket_entries:
                bucket_entries[key] = obj.version_id
                self._timestamps[obj.source_bucket][key] = (obj.tagged_at, obj.last_modified)
            # Union — must run for every Matched_Object, including the ones the
            # guard above absorbs, or a second rule's destination is lost.
            self._matched_rules[obj.source_bucket][key] |= set(obj.matched_rule_ids)
            self._destinations[obj.source_bucket][key] |= set(obj.destination_bucket_arns)

    # ------------------------------------------------------------------
    # finalize
    # ------------------------------------------------------------------

    def finalize(self, source_bucket: str) -> ManifestResult:
        """Produce the finalized union ManifestResult for *source_bucket*.

        Returns a :class:`ManifestResult` with ``has_matches=True`` when one
        or more distinct ``(source_bucket, object_key)`` pairs have been
        accumulated for *source_bucket* (across every rule that matched), or
        ``has_matches=False`` with a "no matches" indication when zero
        matches exist (6.6).

        ``all_versioned`` is set to ``True`` when every entry has a non-None
        ``version_id``, enabling the batch adapter to use the 3-field manifest
        format (Bucket, Key, VersionId).

        Entries are sorted lexicographically by ``(source_bucket, object_key)``
        for deterministic output — S3 Batch Operations does not require a
        particular order, but a stable order makes tests and round-trip
        verification straightforward.

        Parameters
        ----------
        source_bucket:
            The Monitored_Bucket whose accumulated entries (from every
            tag-scoped rule) are to be finalized into a single union
            manifest (design.md D1, Req 1.1).
        """
        pairs: dict[tuple[str, str], str | None] = self._entries.get(source_bucket, {})

        if not pairs:
            return ManifestResult(
                source_bucket=source_bucket,
                has_matches=False,
                entries=[],
                object_count=0,
            )

        # Sort for determinism; each pair → ManifestEntry (6.2, 6.3)
        sorted_items = sorted(pairs.items())
        entries = [
            ManifestEntry(source_bucket=src, object_key=key, version_id=vid)
            for (src, key), vid in sorted_items
        ]
        # Req 13.6, 2.6, 1.2: always use the versioned schema (Bucket, Key, VersionId)
        # regardless of whether any version_id is None; a null-version object carries
        # version_id=None which serialises to an empty CSV field, addressing the null
        # version specifically rather than the current version of the key.
        return ManifestResult(
            source_bucket=source_bucket,
            has_matches=True,
            entries=entries,
            object_count=len(entries),
            all_versioned=True,
        )

    # ------------------------------------------------------------------
    # introspection helpers
    # ------------------------------------------------------------------

    def has_accumulated_entries(self, source_bucket: str) -> bool:
        """Return ``True`` when *source_bucket* has ≥1 accumulated entry.

        Useful for the orchestrator to check whether a bucket will produce
        a manifest without calling ``finalize`` speculatively.
        """
        return bool(self._entries.get(source_bucket))

    def get_timestamps(
        self, source_bucket: str,
    ) -> dict[tuple[str, str], tuple[datetime | None, datetime | None]]:
        """Return accumulated (tagged_at, last_modified) per (object_key, version_id).

        Used by the orchestrator to enrich TrackedObjects with timestamps
        for the completion report email.

        A null version is keyed ``""`` rather than ``None`` because this dict is
        typed ``tuple[str, str]``. That is a transport form, not a second
        identity — real S3 version IDs are never empty — and
        ``StateStore.store_completion_timestamps`` normalizes it back to ``None``
        before building the state-object key, so it matches what the BOPS report
        reader holds on the read side.
        """
        bucket_entries = self._entries.get(source_bucket, {})
        ts_map = self._timestamps.get(source_bucket, {})
        # Re-key from (source_bucket, object_key) to (object_key, version_id)
        result: dict[tuple[str, str], tuple[datetime | None, datetime | None]] = {}
        for (src, key), vid in bucket_entries.items():
            ts = ts_map.get((src, key), (None, None))
            result[(key, vid or "")] = ts
        return result

    def get_routing(
        self, source_bucket: str,
    ) -> dict[tuple[str, str], tuple[list[str], list[str]]]:
        """Return accumulated (matched_rules, destinations) per (object_key, version_id).

        Used by the orchestrator to enrich TrackedObjects for the completion
        report email, keyed identically to :meth:`get_timestamps` so both maps
        can be threaded through the same call path.

        Destinations are returned as bucket *names*, not ARNs: an S3 bucket ARN
        carries no account or Region component, so the name is equally
        unambiguous while being shorter in the state object and more readable in
        the report. Both lists are sorted for deterministic output.
        """
        bucket_entries = self._entries.get(source_bucket, {})
        rules_map = self._matched_rules.get(source_bucket, {})
        dest_map = self._destinations.get(source_bucket, {})
        result: dict[tuple[str, str], tuple[list[str], list[str]]] = {}
        for (src, key), vid in bucket_entries.items():
            rules = sorted(rules_map.get((src, key), set()))
            destinations = sorted(
                _destination_bucket_name(arn)
                for arn in dest_map.get((src, key), set())
            )
            result[(key, vid or "")] = (rules, destinations)
        return result


# ---------------------------------------------------------------------------
# serialize — module-level pure function
# ---------------------------------------------------------------------------


def serialize(entries: list[ManifestEntry]) -> str:
    """Serialize *entries* to the CSV string that S3 Batch Operations accepts.

    Format: one row per entry — ``source-bucket,object-key,version-id`` — with
    no header row, rows separated by newlines. A null ``version_id`` is
    serialized as the literal string ``null`` (the third field is always
    present). A trailing newline is not emitted so that
    ``read(write(S)) == S`` without extra stripping (6.4, 6.5).

    Parameters
    ----------
    entries:
        Ordered list of :class:`~src.core.models.ManifestEntry` objects.
        Pass :attr:`ManifestResult.entries` directly.

    Returns
    -------
    str
        The complete CSV content, or an empty string when *entries* is empty.
    """
    return "\n".join(entry.to_csv_row() for entry in entries)


def deserialize(csv_content: str) -> list[ManifestEntry]:
    """Parse a CSV manifest back into a list of ManifestEntry objects.

    Inverse of :func:`serialize`; enables the round-trip property (6.5).
    Empty strings (empty manifests) return an empty list.

    Retained as the round-trip oracle for ``serialize``'s property test —
    no production caller exists, but the property test in
    ``tests/core/test_manifest_generator.py`` uses this function to verify
    that ``serialize`` output can be losslessly reconstructed.

    Parameters
    ----------
    csv_content:
        The raw CSV string as produced by :func:`serialize`.
    """
    if not csv_content:
        return []
    lines = csv_content.splitlines()
    return [ManifestEntry.from_versioned_csv_row(row) for row in lines]
