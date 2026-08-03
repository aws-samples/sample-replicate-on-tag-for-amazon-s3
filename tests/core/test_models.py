"""Unit tests for src/core/models.py.

Covers:
- MatchedObject identity (source_bucket, object_key, replication_config_id)
- TaggingOperation.logical_operation_id (with and without operation_version)
- ManifestEntry CSV round-trip
- Basic model instantiation and field access
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.models import (
    AppConfig,
    CheckpointState,
    DestinationRef,
    DerivedReplicationRule,
    Lease,
    LeaseStatus,
    ManifestEntry,
    MatchedObject,
    MonitoredBucket,
    S3Location,
    SubmissionRecord,
    SubmissionStatus,
    TaggingOperation,
)

_NOW = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_op(
    source_bucket: str = "example-source-bucket",
    object_key: str = "path/to/object.txt",
    resulting_tag_set: dict | None = None,
    sequence_number: str = "seq-1",
    operation_version: str | None = None,
) -> TaggingOperation:
    return TaggingOperation(
        source_bucket=source_bucket,
        object_key=object_key,
        resulting_tag_set=resulting_tag_set or {"env": "prod"},
        sequence_number=sequence_number,
        operation="PutObjectTagging",
        event_time=_NOW,
        operation_version=operation_version,
    )


def make_matched(
    source_bucket: str = "example-source-bucket",
    object_key: str = "path/to/object.txt",
    replication_config_id: str = "cfg-1",
    matched_rule_ids: frozenset[str] | None = None,
    version_id: str | None = None,
) -> MatchedObject:
    return MatchedObject(
        source_bucket=source_bucket,
        object_key=object_key,
        replication_config_id=replication_config_id,
        matched_rule_ids=matched_rule_ids or frozenset(["rule-1"]),
        version_id=version_id,
    )


# ---------------------------------------------------------------------------
# MonitoredBucket / AppConfig
# ---------------------------------------------------------------------------


class TestMonitoredBucket:
    def test_fields_accessible(self):
        b = MonitoredBucket(name="example-source-bucket", region="us-east-1")
        assert b.name == "example-source-bucket"
        assert b.region == "us-east-1"



# ---------------------------------------------------------------------------
# DerivedReplicationRule / DestinationRef
# ---------------------------------------------------------------------------


class TestDerivedReplicationRule:
    def test_all_fields_set(self):
        dest = DestinationRef(bucket_arn="arn:aws:s3:::dest-bucket")
        rule = DerivedReplicationRule(
            source_bucket="src",
            replication_config_id="cfg-1",
            rule_id="rule-1",
            tag_filter={"env": "prod", "tier": "hot"},
            destination=dest,
            replication_role_arn="arn:aws:iam::123456789012:role/rep-role",
        )
        assert rule.key_prefix is None
        assert rule.tag_filter == {"env": "prod", "tier": "hot"}

    def test_optional_key_prefix(self):
        dest = DestinationRef(bucket_arn="arn:aws:s3:::dest")
        rule = DerivedReplicationRule(
            source_bucket="src",
            replication_config_id="cfg-1",
            rule_id="rule-1",
            tag_filter={"k": "v"},
            destination=dest,
            replication_role_arn="arn:aws:iam::123:role/r",
            key_prefix="logs/",
        )
        assert rule.key_prefix == "logs/"


# ---------------------------------------------------------------------------
# TaggingOperation — logical_operation_id
# ---------------------------------------------------------------------------


class TestTaggingOperationLogicalId:
    def test_with_operation_version(self):
        op = make_op(
            source_bucket="bucket-a",
            object_key="key/obj",
            operation_version="v42",
        )
        lid = op.logical_operation_id
        assert "bucket-a" in lid
        assert "key/obj" in lid
        assert "v42" in lid

    def test_without_operation_version_uses_null_sentinel(self):
        """A missing version renders as the sentinel, and the tag set is hashed in.

        Assertion changed by retag-suppression task 1: the tag set is now a
        truncated SHA-256 digest rather than embedded canonical JSON, so its
        literal keys and values no longer appear in the id.
        """
        op = make_op(
            source_bucket="bucket-a",
            object_key="key/obj",
            resulting_tag_set={"env": "prod"},
            operation_version=None,
        )
        lid = op.logical_operation_id
        assert "bucket-a" in lid
        assert "key/obj" in lid
        assert "\x01null-version" in lid
        # tag set influences the id, via the digest rather than literally
        assert "env" not in lid
        other = make_op(
            source_bucket="bucket-a",
            object_key="key/obj",
            resulting_tag_set={"env": "staging"},
            operation_version=None,
        )
        assert other.logical_operation_id != lid

    def test_null_version_distinct_from_empty_string_version(self):
        op_null = make_op(operation_version=None)
        op_empty = make_op(operation_version="")
        assert op_null.logical_operation_id != op_empty.logical_operation_id

    def test_same_op_version_gives_same_id(self):
        op1 = make_op(source_bucket="b", object_key="k", operation_version="v1")
        op2 = make_op(source_bucket="b", object_key="k", operation_version="v1")
        assert op1.logical_operation_id == op2.logical_operation_id

    def test_different_op_version_gives_different_id(self):
        op1 = make_op(source_bucket="b", object_key="k", operation_version="v1")
        op2 = make_op(source_bucket="b", object_key="k", operation_version="v2")
        assert op1.logical_operation_id != op2.logical_operation_id

    def test_null_version_same_tags_same_id(self):
        """Without operation_version, identical tag sets yield the same id."""
        op1 = make_op(resulting_tag_set={"a": "1", "b": "2"}, operation_version=None)
        op2 = make_op(resulting_tag_set={"b": "2", "a": "1"}, operation_version=None)
        assert op1.logical_operation_id == op2.logical_operation_id

    def test_null_version_different_tags_different_id(self):
        op1 = make_op(resulting_tag_set={"env": "prod"}, operation_version=None)
        op2 = make_op(resulting_tag_set={"env": "staging"}, operation_version=None)
        assert op1.logical_operation_id != op2.logical_operation_id

    def test_tags_change_id_when_version_present(self):
        """Two tagging events on one object version are distinct operations.

        Assertion inverted by retag-suppression task 1: the version no longer
        takes precedence over the tag set, which is why a retag was previously
        suppressed as a duplicate delivery.
        """
        op1 = make_op(resulting_tag_set={"env": "prod"}, operation_version="v1")
        op2 = make_op(resulting_tag_set={"env": "staging"}, operation_version="v1")
        assert op1.logical_operation_id != op2.logical_operation_id

    def test_identical_tag_set_same_version_collapses(self):
        """A duplicate delivery of one event still collapses to one identity."""
        op1 = make_op(resulting_tag_set={"r": "yes"}, operation_version="v1")
        op2 = make_op(resulting_tag_set={"r": "yes"}, operation_version="v1")
        assert op1.logical_operation_id == op2.logical_operation_id


# ---------------------------------------------------------------------------
# TaggingOperation.logical_operation_id — property test (Requirement 4.7)
# ---------------------------------------------------------------------------

_TAG_TEXT = st.text(
    alphabet=st.characters(min_codepoint=32, max_codepoint=126),
    min_size=1,
    max_size=16,
)


class TestLogicalOperationIdProperties:
    @given(
        tags=st.dictionaries(_TAG_TEXT, _TAG_TEXT, min_size=1, max_size=10),
        version=st.one_of(st.none(), st.text(min_size=0, max_size=8)),
    )
    @settings(max_examples=200)
    def test_identity_independent_of_tag_order(
        self, tags: dict[str, str], version: str | None
    ) -> None:
        """The identity does not depend on the insertion order of the tag set.

        Canonical JSON with ``sort_keys=True`` provides this; a switch to a
        different serialisation would break it silently.

        Validates: Requirements 4.7
        """
        shuffled = dict(reversed(list(tags.items())))
        assert dict(shuffled) == dict(tags)

        a = make_op(resulting_tag_set=tags, operation_version=version)
        b = make_op(resulting_tag_set=shuffled, operation_version=version)

        assert a.logical_operation_id == b.logical_operation_id

    @given(tags=st.dictionaries(_TAG_TEXT, _TAG_TEXT, min_size=1, max_size=10))
    @settings(max_examples=200)
    def test_null_version_identity_distinct_from_empty_string_version(
        self, tags: dict[str, str]
    ) -> None:
        """A ``None`` version never renders the same as a literal empty version.

        Validates: Requirements 4.7
        """
        null_version = make_op(resulting_tag_set=tags, operation_version=None)
        empty_version = make_op(resulting_tag_set=tags, operation_version="")
        assert (
            null_version.logical_operation_id
            != empty_version.logical_operation_id
        )


# ---------------------------------------------------------------------------
# MatchedObject — identity-based equality and hashing
# ---------------------------------------------------------------------------


class TestMatchedObjectIdentity:
    def test_same_identity_equal_regardless_of_rule_ids(self):
        a = make_matched(matched_rule_ids=frozenset(["rule-1"]))
        b = make_matched(matched_rule_ids=frozenset(["rule-2", "rule-3"]))
        assert a == b

    def test_different_object_key_not_equal(self):
        a = make_matched(object_key="key/a")
        b = make_matched(object_key="key/b")
        assert a != b

    def test_different_source_bucket_not_equal(self):
        a = make_matched(source_bucket="bucket-1")
        b = make_matched(source_bucket="bucket-2")
        assert a != b

    def test_different_config_id_not_equal(self):
        a = make_matched(replication_config_id="cfg-1")
        b = make_matched(replication_config_id="cfg-2")
        assert a != b

    def test_same_identity_same_hash(self):
        a = make_matched(matched_rule_ids=frozenset(["rule-x"]))
        b = make_matched(matched_rule_ids=frozenset(["rule-y"]))
        assert hash(a) == hash(b)

    def test_usable_in_set_dedup(self):
        """Two MatchedObjects with the same identity collapse to one in a set."""
        a = make_matched(matched_rule_ids=frozenset(["rule-1"]))
        b = make_matched(matched_rule_ids=frozenset(["rule-2"]))
        result = {a, b}
        assert len(result) == 1

    def test_different_identities_preserved_in_set(self):
        a = make_matched(object_key="key/a")
        b = make_matched(object_key="key/b")
        result = {a, b}
        assert len(result) == 2

    def test_not_equal_to_non_matched_object(self):
        a = make_matched()
        assert (a == "not-a-matched-object") is NotImplemented or a != "not-a-matched-object"

    def test_version_id_not_part_of_identity(self):
        """version_id must NOT affect equality or hash (identity is bucket+key+config_id)."""
        a = make_matched(version_id=None)
        b = make_matched(version_id="v1")
        assert a == b
        assert hash(a) == hash(b)

    def test_version_id_field_accessible(self):
        obj = make_matched(version_id="abc123")
        assert obj.version_id == "abc123"

    def test_version_id_defaults_to_none(self):
        obj = make_matched()
        assert obj.version_id is None


# ---------------------------------------------------------------------------
# ManifestEntry — CSV round-trip
# ---------------------------------------------------------------------------


class TestManifestEntry:
    def test_to_csv_row(self):
        entry = ManifestEntry(source_bucket="my-bucket", object_key="path/to/file.txt")
        assert entry.to_csv_row() == "my-bucket,path/to/file.txt,null"

    def test_to_csv_row_with_version_id(self):
        entry = ManifestEntry(source_bucket="my-bucket", object_key="path/to/file.txt", version_id="v1")
        assert entry.to_csv_row() == "my-bucket,path/to/file.txt,v1"

    def test_to_csv_row_none_version_id_emits_literal_null(self):
        entry = ManifestEntry(source_bucket="my-bucket", object_key="path/to/file.txt", version_id=None)
        assert entry.to_csv_row() == "my-bucket,path/to/file.txt,null"

    def test_to_csv_row_always_three_fields(self):
        """to_csv_row() always emits three fields — the literal string ``null``
        as the third field when version_id is None, matching the convention
        S3 Batch Operations requires."""
        entry = ManifestEntry(source_bucket="my-bucket", object_key="path/to/file.txt", version_id=None)
        assert entry.to_csv_row() == "my-bucket,path/to/file.txt,null"

    def test_to_csv_row_with_version_id_unaffected(self):
        """version_id set — the field is emitted as-is."""
        entry = ManifestEntry(source_bucket="my-bucket", object_key="path/to/file.txt", version_id="v1")
        assert entry.to_csv_row() == "my-bucket,path/to/file.txt,v1"

    def test_from_versioned_csv_row_basic(self):
        entry = ManifestEntry.from_versioned_csv_row("my-bucket,path/to/file.txt,v1")
        assert entry.source_bucket == "my-bucket"
        assert entry.object_key == "path/to/file.txt"
        assert entry.version_id == "v1"

    def test_from_versioned_csv_row_key_with_commas(self):
        """rpartition correctly handles commas in the object key."""
        entry = ManifestEntry.from_versioned_csv_row("my-bucket,path,with,commas/file.txt,v99")
        assert entry.source_bucket == "my-bucket"
        assert entry.object_key == "path,with,commas/file.txt"
        assert entry.version_id == "v99"

    def test_from_versioned_csv_row_literal_null_string_parses_as_none(self):
        """The literal string 'null' (the convention to_csv_row() emits for a
        null-version object, and the one S3 Batch Operations itself requires)
        round-trips to version_id=None."""
        entry = ManifestEntry.from_versioned_csv_row("my-bucket,path/to/file.txt,null")
        assert entry.version_id is None

    def test_from_versioned_csv_row_empty_string_still_parses_as_none(self):
        """An empty third field (legacy/alternate representation) also
        round-trips to version_id=None, for tolerance of manifests not
        produced by this codebase's own current to_csv_row."""
        entry = ManifestEntry.from_versioned_csv_row("my-bucket,path/to/file.txt,")
        assert entry.version_id is None

    def test_versioned_round_trip_null_version(self):
        original = ManifestEntry(source_bucket="src", object_key="a/b/c.json", version_id=None)
        restored = ManifestEntry.from_versioned_csv_row(original.to_csv_row())
        assert restored.source_bucket == "src"
        assert restored.object_key == "a/b/c.json"
        assert restored.version_id is None

    def test_key_with_commas_preserved(self):
        """Object_key may contain commas; split via rpartition handles them."""
        entry = ManifestEntry(source_bucket="bucket", object_key="path,with,commas/file.txt")
        restored = ManifestEntry.from_versioned_csv_row(entry.to_csv_row())
        assert restored.source_bucket == "bucket"
        assert restored.object_key == "path,with,commas/file.txt"


# ---------------------------------------------------------------------------
# ManifestEntry — URL-encoding of object keys (CSV injection prevention)
# ---------------------------------------------------------------------------


class TestManifestEntryEncoding:
    """S3 Batch Operations requires manifest keys to be URL-encoded.

    Encoding also neutralizes commas and newlines in object keys — both legal
    in S3 keys — which would otherwise corrupt the CSV or allow an actor who
    controls an object key to inject extra manifest rows.
    """

    def test_comma_in_key_is_encoded(self):
        entry = ManifestEntry(source_bucket="bucket", object_key="a,b/c.txt")
        row = entry.to_csv_row()
        # The comma in the key is URL-encoded; two literal commas are bucket/key and key/version delimiters.
        assert row == "bucket,a%2Cb/c.txt,null"
        assert row.count(",") == 2

    def test_newline_in_key_is_encoded(self):
        entry = ManifestEntry(source_bucket="bucket", object_key="a\nb.txt")
        row = entry.to_csv_row()
        assert "\n" not in row
        assert "%0A" in row

    def test_space_in_key_is_encoded(self):
        entry = ManifestEntry(source_bucket="bucket", object_key="my key.txt")
        assert entry.to_csv_row() == "bucket,my%20key.txt,null"

    def test_slash_in_key_is_preserved(self):
        entry = ManifestEntry(source_bucket="bucket", object_key="a/b/c.txt")
        assert entry.to_csv_row() == "bucket,a/b/c.txt,null"

    def test_percent_in_key_is_encoded_and_round_trips(self):
        entry = ManifestEntry(source_bucket="bucket", object_key="100%done.txt")
        row = entry.to_csv_row()
        assert "%25" in row
        assert ManifestEntry.from_versioned_csv_row(row).object_key == "100%done.txt"

    def test_round_trip_comma_key_unversioned(self):
        original = ManifestEntry(source_bucket="src", object_key="a,b,c/d.txt")
        restored = ManifestEntry.from_versioned_csv_row(original.to_csv_row())
        assert restored == original

    def test_round_trip_newline_key_versioned(self):
        original = ManifestEntry(source_bucket="src", object_key="a\nb.txt", version_id="v1")
        restored = ManifestEntry.from_versioned_csv_row(original.to_csv_row())
        assert restored.object_key == "a\nb.txt"
        assert restored.version_id == "v1"

    def test_row_injection_attempt_stays_single_row(self):
        """A key crafted to inject a second manifest row is neutralized."""
        malicious = "real.txt\nvictim-bucket,secret/object.txt"
        entry = ManifestEntry(source_bucket="src", object_key=malicious)
        row = entry.to_csv_row()
        # No embedded newline => serialization is a single line.
        assert "\n" not in row
        assert len(row.splitlines()) == 1
        # And it round-trips back to the original key, not two entries.
        assert ManifestEntry.from_versioned_csv_row(row).object_key == malicious

    def test_serialize_deserialize_round_trip_with_special_keys(self):
        from src.core.manifest_generator import deserialize, serialize

        entries = [
            ManifestEntry(source_bucket="src", object_key="plain/key.txt"),
            ManifestEntry(source_bucket="src", object_key="comma,key.txt"),
            ManifestEntry(source_bucket="src", object_key="space key.txt"),
        ]
        assert deserialize(serialize(entries)) == entries

    def test_serialize_deserialize_round_trip_versioned_special_keys(self):
        from src.core.manifest_generator import deserialize, serialize

        entries = [
            ManifestEntry(source_bucket="src", object_key="comma,key.txt", version_id="v1"),
            ManifestEntry(source_bucket="src", object_key="a/b/c.txt", version_id="v2"),
        ]
        restored = deserialize(serialize(entries))
        assert [(e.object_key, e.version_id) for e in restored] == [
            ("comma,key.txt", "v1"),
            ("a/b/c.txt", "v2"),
        ]


# ---------------------------------------------------------------------------
# CheckpointState / Lease
# ---------------------------------------------------------------------------


class TestCheckpointState:
    def test_without_lease(self):
        cp = CheckpointState(
            source_bucket="my-bucket",
            last_processed_watermark="2024-01-01T00:00:00.000000Z",
        )
        assert cp.lease is None
        assert cp.processed_window == []

    def test_with_lease(self):
        lease = Lease(
            lease_id="lease-abc",
            candidate_max_watermark="2024-01-02T00:00:00.000000Z",
            acquired_at=_NOW,
        )
        cp = CheckpointState(
            source_bucket="my-bucket",
            last_processed_watermark="2024-01-01T00:00:00.000000Z",
            lease=lease,
        )
        assert cp.lease is not None
        assert cp.lease.status == LeaseStatus.IN_FLIGHT
        assert cp.lease.lease_id == "lease-abc"


# ---------------------------------------------------------------------------
# SubmissionRecord
# ---------------------------------------------------------------------------


class TestSubmissionRecord:
    def test_submitted_status(self):
        rec = SubmissionRecord(
            replication_config_id="cfg-1",
            source_bucket="my-bucket",
            job_id="job-xyz",
            manifest_key="manifests/cfg-1/2024-01-01.csv",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMITTED,
        )
        assert rec.status == SubmissionStatus.SUBMITTED
        assert rec.job_id == "job-xyz"

    def test_create_failed_status(self):
        rec = SubmissionRecord(
            replication_config_id="cfg-1",
            source_bucket="b",
            job_id="",
            manifest_key="k",
            submitted_at=_NOW,
            status=SubmissionStatus.CREATE_FAILED,
        )
        assert rec.status == SubmissionStatus.CREATE_FAILED

    def test_submit_failed_status(self):
        rec = SubmissionRecord(
            replication_config_id="cfg-1",
            source_bucket="b",
            job_id="",
            manifest_key="k",
            submitted_at=_NOW,
            status=SubmissionStatus.SUBMIT_FAILED,
        )
        assert rec.status == SubmissionStatus.SUBMIT_FAILED
