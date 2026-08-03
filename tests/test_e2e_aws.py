#!/usr/bin/env python3
"""End-to-end AWS integration test for s3-replicate-on-tag.

Runs the full pipeline against real AWS resources:
  config → rules → checkpoint → journal → dedup → match → manifest → batch job → checkpoint advance

Exit codes:
  0  SUCCESS  — batch replication job(s) submitted (or pipeline complete with no matches)
  2  NO EVENTS — journal has no new events yet; tag objects in the source bucket and re-run
  1  ERROR    — any unexpected failure

Environment variables (see deploy/README.md for the full reference):
  S3ROT_TEST_ACCOUNT: AWS account ID (e.g., "123456789012")
  S3ROT_TEST_REGION: AWS region (e.g., "us-west-2")
  S3ROT_TEST_SOURCE_BUCKET: Source bucket name
  S3ROT_TEST_DEST_BUCKET: Destination bucket name
  S3ROT_TEST_STATE_BUCKET: State bucket name
  S3ROT_TEST_ROLE_ARN: IAM role ARN for replication
  S3ROT_TEST_WORKGROUP: Athena workgroup name
  S3ROT_TEST_OUTPUT_LOCATION: S3 URI for Athena query results

Optional:
  S3ROT_TEST_KMS_KEY_ARN: symmetric CMK ARN. When set, the state object and the
    manifest are written with SSE-KMS under that key and every write is verified
    with HeadObject, as is the Athena result object the workgroup produced. This
    exercises the KMS-dependent branches that a deployment with KmsKeyArn empty
    never reaches — the branches where a defect can sit unnoticed, as the
    Manifest.Location.ManifestEncryption defect did until 0.1.13. Requires the
    key policy from deploy/README.md "KMS Key Setup", including kms:Decrypt for
    the replication role, or the batch job cannot read the manifest.
    See deploy/README.md "Verifying a KMS-enabled deployment" for the checks
    this script cannot make (SNS topic encryption and the EventBridge publish).

When any required environment variable is unset the module skips: run
standalone it exits 3, and under pytest it is skipped at collection so a
checkout without the variables still collects and passes.
"""

import json
import os
import sys
import time
import logging
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Solution modules
# ---------------------------------------------------------------------------
from src.core.models import (
    TaggingOperation,
    CheckpointState,
    AppConfig,
    MonitoredBucket,
    SubmissionStatus,
)
from src.core.config_loader import load_config, _is_valid_s3_bucket_name
from src.core.journal_dedup import build_submitted_refs, select_eligible_operations
from src.core.rule_matcher import match
from src.core.manifest_generator import ManifestGenerator
from src.core.checkpoint_logic import advance_checkpoint
from src.core.checkpoint_serializer import deserialize, serialize
from src.core.watermark import subtract as watermark_subtract
from src.adapters.client_factory import ClientFactory
from src.adapters.replication_config_adapter import get_replication_rules
from src.adapters.athena_journal_adapter import (
    _parse_event_time,
    _to_athena_timestamp_literal,
)
from src.adapters.state_store import StateStore, ConditionalWriteError
from src.adapters.batch_operations_adapter import submit_batch_job
from src.adapters.inventory_manifest_writer import (
    InventoryManifestWriteError,
    write_in_memory_inventory_manifest,
)
from src.core.manifest_generator import serialize as serialize_manifest
from src.core.manifest_strategy import ManifestFormat
from src.core.delete_filter import filter_deleted_versions
from src.core.models import MatchedObject
from src.adapters import bucket_policy_adapter
from src.adapters.permanent_delete_reader import read_permanent_deletes
from src.orchestrator import _completion_report_prefix

# ---------------------------------------------------------------------------
# Test resource config
# ---------------------------------------------------------------------------
def _get_env_or_skip(name: str, desc: str) -> str:
    """Read an environment variable; skip with an explanation when it is unset.

    Under pytest this skips the module at collection time (Req 6.2), so a
    checkout without the end-to-end variables still collects and passes. Run
    standalone it prints the reason and exits 3.
    """
    value = os.getenv(name)
    if value is not None:
        return value

    reason = (
        f"End-to-end test requires {name} ({desc}). "
        f"Set it and re-run, or skip this test entirely."
    )
    if "pytest" in sys.modules:
        import pytest

        pytest.skip(reason, allow_module_level=True)
    print(f"SKIP: {reason}")
    sys.exit(3)

def _require_valid_bucket_name(name: str, env_var: str) -> str:
    """Reject a bucket name that is not a syntactically valid S3 bucket name.

    ``SOURCE_BUCKET`` is interpolated into the Athena SQL below in two places:
    the ``journal`` table identifier (``JOURNAL_TABLE_SQL``) and the ``bucket``
    predicate. Athena parameter placeholders cannot be used for a table
    identifier, so the identifier cannot be parameterized away — the value has
    to be constrained instead.

    This applies the same validator the Solution uses on bucket names from the
    solution config (``config_loader._is_valid_s3_bucket_name``): the
    underlying character class permits only lowercase letters, digits, hyphens
    and periods, which excludes quotes, backslashes, semicolons, whitespace
    and comment sequences. A name that passes cannot carry a SQL-injection
    payload into either interpolation site.
    """
    if not _is_valid_s3_bucket_name(name):
        reason = (
            f"{env_var} is not a valid S3 bucket name: {name!r}. It is "
            f"interpolated into an Athena table identifier, which cannot be "
            f"parameterized, so only syntactically valid bucket names are "
            f"accepted."
        )
        if "pytest" in sys.modules:
            import pytest

            pytest.skip(reason, allow_module_level=True)
        print(f"ERROR: {reason}")
        sys.exit(1)
    return name


ACCOUNT          = _get_env_or_skip("S3ROT_TEST_ACCOUNT", "AWS account ID")
REGION           = _get_env_or_skip("S3ROT_TEST_REGION", "AWS region")
SOURCE_BUCKET    = _require_valid_bucket_name(
    _get_env_or_skip("S3ROT_TEST_SOURCE_BUCKET", "source bucket name"),
    "S3ROT_TEST_SOURCE_BUCKET",
)
DEST_BUCKET      = _get_env_or_skip("S3ROT_TEST_DEST_BUCKET", "destination bucket name")
STATE_BUCKET     = _get_env_or_skip("S3ROT_TEST_STATE_BUCKET", "state bucket name")
ROLE_ARN         = _get_env_or_skip("S3ROT_TEST_ROLE_ARN", "IAM role ARN for replication")
WORKGROUP        = _get_env_or_skip("S3ROT_TEST_WORKGROUP", "Athena workgroup name")
OUTPUT_LOCATION  = _get_env_or_skip("S3ROT_TEST_OUTPUT_LOCATION", "S3 URI for Athena query results")

# Optional: when set, the KMS-dependent write paths are exercised and verified.
# Not routed through _get_env_or_skip — an unset value must leave the run
# unchanged rather than skip it.
KMS_KEY_ARN = os.getenv("S3ROT_TEST_KMS_KEY_ARN") or None

# Derived values. SOURCE_BUCKET is validated above, so the identifier below
# cannot carry an injection payload.
JOURNAL_TABLE_SQL = f'"s3tablescatalog/aws-s3"."b_{SOURCE_BUCKET.replace(".", "_")}"."journal"'

# Lookback window for re-scanning the journal below the record_timestamp
# watermark (matches the orchestrator default).
LOOKBACK = timedelta(hours=1)


# ---------------------------------------------------------------------------
# Inline journal query — real S3 Metadata journal schema
# ---------------------------------------------------------------------------

def query_journal(athena, since_timestamp=None):
    """Query the S3 Metadata journal using the real schema column names.

    Real column mapping (design vs. actual):
      record_type = 'UPDATE_METADATA'  →  operation = 'PutObjectTagging'
      CAST(object_tags AS JSON)         →  resulting_tags  (valid JSON string)
      record_timestamp                  →  event_time
      bucket, key, version_id, sequence_number  unchanged

    The cross-key cursor is the ``record_timestamp`` watermark (globally
    comparable), not ``sequence_number`` (ordered only per bucket+key).  When
    ``since_timestamp`` (a canonical watermark) is given, only records strictly
    newer than it are returned.

    Returns
    -------
    (ops: list[TaggingOperation], errors: list[str])
    """
    # SOURCE_BUCKET passed _require_valid_bucket_name at module load, so it
    # cannot contain a quote. The doubling below is belt-and-braces, not the
    # control the safety of this query rests on.
    bucket_escaped = SOURCE_BUCKET.replace("'", "''")

    query = (
        "SELECT bucket, key, version_id, "
        "CAST(object_tags AS JSON) AS resulting_tags, "
        "sequence_number, record_timestamp AS event_time "
        f"FROM {JOURNAL_TABLE_SQL} "
        f"WHERE bucket = '{bucket_escaped}' "
        "AND record_type = 'UPDATE_METADATA'"
    )

    if since_timestamp:
        athena_ts = _to_athena_timestamp_literal(since_timestamp)
        query += f" AND record_timestamp > timestamp '{athena_ts}'"

    query += " ORDER BY record_timestamp ASC"

    print(f"[Journal]   SQL: {query[:200]}{'...' if len(query) > 200 else ''}")

    # Submit query — workgroup owns the catalog context; no QueryExecutionContext needed
    resp = athena.start_query_execution(
        QueryString=query,
        WorkGroup=WORKGROUP,
        ResultConfiguration={"OutputLocation": OUTPUT_LOCATION},
    )
    query_id = resp["QueryExecutionId"]
    print(f"[Journal]   QueryExecutionId: {query_id}")

    # Poll until terminal state (2 s interval, 10 min max)
    for attempt in range(300):
        status_resp = athena.get_query_execution(QueryExecutionId=query_id)
        state = status_resp["QueryExecution"]["Status"]["State"]
        if attempt % 5 == 0 or state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            print(f"[Journal]   Poll {attempt}: state={state}")
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            reason = status_resp["QueryExecution"]["Status"].get(
                "StateChangeReason", "unknown"
            )
            return [], [f"Athena query {state}: {reason}"]
        time.sleep(2)
    else:
        return [], ["Athena query timed out after 600 s"]

    # Retrieve and parse result pages
    ops: list[TaggingOperation] = []
    errors: list[str] = []
    next_token = None
    first_page = True
    row_count = 0

    while True:
        kwargs = {"QueryExecutionId": query_id, "MaxResults": 1000}
        if next_token:
            kwargs["NextToken"] = next_token

        page_resp = athena.get_query_results(**kwargs)
        rows = page_resp.get("ResultSet", {}).get("Rows", [])

        # First page always starts with a header row — skip it
        start = 1 if first_page else 0
        first_page = False

        for row in rows[start:]:
            row_count += 1
            values = [col.get("VarCharValue", "") for col in row.get("Data", [])]

            if len(values) < 6:
                errors.append(f"Short row ({len(values)} cols): {values!r}")
                continue

            # Column order: bucket, key, version_id, resulting_tags, sequence_number, event_time
            bucket_val      = values[0] or SOURCE_BUCKET
            key             = values[1]
            version_id      = values[2] if values[2] else None
            resulting_tags_raw = values[3]
            sequence_number = values[4]
            event_time_raw  = values[5]

            if not key:
                errors.append(f"Missing key at seq={sequence_number!r}")
                continue

            if not resulting_tags_raw:
                errors.append(
                    f"Missing or inaccessible object_tags for key={key!r} "
                    f"seq={sequence_number!r}"
                )
                continue

            try:
                tags = json.loads(resulting_tags_raw)
                if not isinstance(tags, dict):
                    errors.append(
                        f"object_tags is not a JSON object for key={key!r}: "
                        f"{resulting_tags_raw!r}"
                    )
                    continue
                if not tags:
                    errors.append(f"Empty object_tags for key={key!r}")
                    continue
            except json.JSONDecodeError as exc:
                errors.append(
                    f"JSON decode error for key={key!r} seq={sequence_number!r}: {exc}"
                )
                continue

            event_time = _parse_event_time(event_time_raw)

            ops.append(
                TaggingOperation(
                    source_bucket=bucket_val,
                    object_key=key,
                    resulting_tag_set=tags,
                    sequence_number=sequence_number,
                    operation="PutObjectTagging",
                    event_time=event_time,
                    operation_version=version_id,
                )
            )

        next_token = page_resp.get("NextToken")
        if not next_token:
            break

    print(
        f"[Journal]   {row_count} raw row(s) → {len(ops)} valid op(s), "
        f"{len(errors)} warning(s)"
    )
    return ops, errors


# ---------------------------------------------------------------------------
# Optional SSE-KMS verification
# ---------------------------------------------------------------------------

def assert_sse_kms(s3_client, bucket: str, key: str, label: str) -> None:
    """Fail loudly when *key* is not SSE-KMS under ``KMS_KEY_ARN``.

    Only called when ``S3ROT_TEST_KMS_KEY_ARN`` is set. Raises rather than
    printing a warning: a write that silently landed under SSE-S3 is precisely
    the encryption downgrade this check exists to catch (it happened — the
    report-missing handler rewrote SSE-KMS state objects under SSE-S3 until
    0.1.13), and a warning in a long console log is easy to miss.
    """
    head = s3_client.head_object(Bucket=bucket, Key=key)
    algorithm = head.get("ServerSideEncryption")
    key_id = head.get("SSEKMSKeyId")
    if algorithm != "aws:kms" or key_id != KMS_KEY_ARN:
        raise AssertionError(
            f"{label} s3://{bucket}/{key} is not encrypted with the expected "
            f"key: ServerSideEncryption={algorithm!r} SSEKMSKeyId={key_id!r}, "
            f"expected 'aws:kms' and {KMS_KEY_ARN!r}"
        )
    print(f"[KMS]      {label}: aws:kms, expected key — OK")


def assert_newest_athena_result_sse_kms(s3_client) -> None:
    """Verify the most recent Athena result object under the output location.

    Result encryption comes from the workgroup's own EncryptionConfiguration,
    not from anything this script passes, so this checks the deployed
    workgroup rather than the client. ``OUTPUT_LOCATION`` is an ``s3://`` URI.
    """
    without_scheme = OUTPUT_LOCATION.removeprefix("s3://")
    bucket, _, prefix = without_scheme.partition("/")
    listing = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = [o for o in listing.get("Contents", []) if not o["Key"].endswith("/")]
    if not objects:
        raise AssertionError(
            f"no Athena result objects under {OUTPUT_LOCATION} to verify; the "
            f"journal query above should have produced one"
        )
    newest = max(objects, key=lambda o: o["LastModified"])
    assert_sse_kms(s3_client, bucket, newest["Key"], "Athena result")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> int:
    logging.basicConfig(level=logging.WARNING)  # suppress verbose SDK output

    # -----------------------------------------------------------------------
    # [Config]  Load and validate AppConfig
    # -----------------------------------------------------------------------
    print("[Config] Loading configuration...")
    try:
        config = load_config(
            {
                "buckets": [{"name": SOURCE_BUCKET, "region": REGION}],
            }
        )
        print(f"[Config] OK: {len(config.buckets)} bucket(s)")
    except Exception as exc:
        print(f"[Config] ERROR: {exc}")
        return 1

    bucket = config.buckets[0]

    # -----------------------------------------------------------------------
    # [Clients]  Create source-side AWS clients
    # -----------------------------------------------------------------------
    print("[Clients] Creating AWS clients...")
    try:
        factory = ClientFactory()
        factory.check_no_destination_client()
        s3        = factory.create_s3_client(region=REGION)
        athena    = factory.create_athena_client(region=REGION)
        s3control = factory.create_s3control_client(region=REGION)
        print(f"[Clients] OK: s3, athena, s3control in {REGION}")
    except Exception as exc:
        print(f"[Clients] ERROR: {exc}")
        return 1

    # -----------------------------------------------------------------------
    # [Rules]  Derive tag-scoped replication rules from bucket config
    # -----------------------------------------------------------------------
    print(f"[Rules] Reading replication config for {SOURCE_BUCKET!r}...")
    try:
        rules, skip_reports = get_replication_rules(s3, bucket)
        for skip in skip_reports:
            print(f"[Rules]   SKIP: {skip.reason}")
        if not rules:
            print("[Rules] ERROR: No tag-scoped replication rules found")
            return 1
        print(f"[Rules] OK: {len(rules)} rule(s)")
        for r in rules:
            print(
                f"[Rules]   id={r.rule_id!r}  tags={r.tag_filter}  "
                f"prefix={r.key_prefix!r}  dest={r.destination.bucket_arn!r}"
            )
    except Exception as exc:
        print(f"[Rules] ERROR: {exc}")
        return 1

    # -----------------------------------------------------------------------
    # [Checkpoint]  Read persisted checkpoint from state bucket
    # -----------------------------------------------------------------------
    print(
        f"[Checkpoint] Reading s3://{STATE_BUCKET}/state/{SOURCE_BUCKET}.json ..."
    )
    if KMS_KEY_ARN:
        print(f"[KMS]      SSE-KMS enabled for this run: {KMS_KEY_ARN}")
    store = StateStore(kms_key_arn=KMS_KEY_ARN)
    try:
        state, checkpoint_etag = store.get_checkpoint(s3, STATE_BUCKET, SOURCE_BUCKET)
        print(
            f"[Checkpoint] watermark={state.last_processed_watermark!r}  "
            f"etag={checkpoint_etag!r}"
        )
        since_timestamp = (
            watermark_subtract(state.last_processed_watermark, LOOKBACK) or None
        )
    except Exception as exc:
        print(f"[Checkpoint] ERROR: {exc}")
        return 1

    # -----------------------------------------------------------------------
    # [Journal]  Query S3 Metadata journal with real schema
    # -----------------------------------------------------------------------
    print(f"[Journal] Querying {JOURNAL_TABLE_SQL} ...")
    try:
        ops, journal_errors = query_journal(athena, since_timestamp=since_timestamp)
        for err in journal_errors:
            print(f"[Journal]   WARNING: {err}")
    except Exception as exc:
        print(f"[Journal] ERROR: {exc}")
        return 1

    if not ops:
        print(
            "[Journal] No new journal events found. "
            "Tag objects in the source bucket and re-run."
        )
        return 2

    print(f"[Journal] {len(ops)} operation(s) found")

    # -----------------------------------------------------------------------
    # [Dedup]  Deduplicate operations and compute candidate high-water mark
    # -----------------------------------------------------------------------
    print("[Dedup] Deduplicating...")
    try:
        deduped, skipped, candidate_hwm = select_eligible_operations(
            ops, state, LOOKBACK
        )
        for s in skipped:
            print(f"[Dedup]   SKIP seq={s.sequence_number!r}: {s.reason}")
        print(
            f"[Dedup] {len(deduped)} unique op(s), "
            f"{len(skipped)} skipped, "
            f"candidate_hwm={candidate_hwm!r}"
        )
    except Exception as exc:
        print(f"[Dedup] ERROR: {exc}")
        return 1

    if not deduped:
        print("[Dedup] No unique operations after deduplication")
        return 2

    # -----------------------------------------------------------------------
    # [Match]  Match operations against derived rules and accumulate
    # -----------------------------------------------------------------------
    print("[Match] Matching operations against rules...")
    try:
        gen = ManifestGenerator()
        total_matched = 0

        # No separate version map is built here. rule_matcher threads
        # ``op.operation_version`` into ``MatchedObject.version_id``
        # (rule_matcher.py:110), so the entries ``gen.finalize()`` returns
        # already carry their version IDs — the same values the orchestrator
        # serializes.
        for op in deduped:
            matched_set, match_errors = match(op, rules)
            for merr in match_errors:
                print(f"[Match]   WARNING: {merr.reason}")
            gen.accumulate(matched_set)
            total_matched += len(matched_set)
        print(
            f"[Match] {total_matched} match(es) across {len(deduped)} op(s)"
        )
    except Exception as exc:
        print(f"[Match] ERROR: {exc}")
        return 1

    # -----------------------------------------------------------------------
    # [Manifest + BatchJob]  One job per bucket: finalize → write → submit
    #
    # design.md D1/D2 (single-batch-job-per-bucket): a bucket produces at most
    # one Batch_Replication_Job per interval over the union of every
    # tag-scoped rule's matches, using the bucket's single replication role
    # (an S3 replication configuration has exactly one top-level Role, so any
    # rule's replication_role_arn is the bucket's role).
    #
    # NOTE: S3ReplicateObject on a versioned bucket requires the manifest to
    # include version IDs (Fields: ['Bucket', 'Key', 'VersionId']).  We write
    # the manifest and create the batch job directly here instead of using the
    # solution's write_manifest / submit_batch_job helpers so we can pass the
    # correct Fields spec.
    # -----------------------------------------------------------------------
    replication_role_arn = rules[0].replication_role_arn
    any_submitted = False

    print("[Manifest] Finalizing per-bucket union manifest...")
    try:
        manifest_result = gen.finalize(SOURCE_BUCKET)
        if not manifest_result.has_matches:
            print("[Manifest]   No matches (object_count=0)")
            return 0
        print(f"[Manifest]   {manifest_result.object_count} object(s) in manifest")
    except Exception as exc:
        print(f"[Manifest] ERROR finalizing: {exc}")
        return 1

    # -----------------------------------------------------------------------
    # [Filter]  Deleted_Version_Filter (Req 13.1, 13.3) — the orchestrator
    # applies this before submitting and this script did not, so it was
    # submitting objects the Solution itself would have excluded. A manifest
    # entry for a permanently deleted version fails its Batch Operations task,
    # so omitting the filter inflates the job's failure rate for reasons that
    # have nothing to do with the Solution.
    # -----------------------------------------------------------------------
    print("[Filter] Applying Deleted_Version_Filter...")
    try:
        perm_deleted = read_permanent_deletes(
            athena_client=athena,
            bucket_name=SOURCE_BUCKET,
            since_window_start=since_timestamp if since_timestamp else None,
            athena_workgroup=WORKGROUP,
            output_location=OUTPUT_LOCATION,
        )
    except Exception as exc:
        print(f"[Filter] WARNING read_permanent_deletes failed (using empty set): {exc}")
        perm_deleted = set()

    matched_for_filter = {
        MatchedObject(
            source_bucket=entry.source_bucket,
            object_key=entry.object_key,
            replication_config_id=SOURCE_BUCKET,
            matched_rule_ids=frozenset(),
            version_id=entry.version_id,
        )
        for entry in manifest_result.entries
    }
    kept_set, excluded_count = filter_deleted_versions(matched_for_filter, perm_deleted)
    print(
        f"[Filter] {excluded_count} excluded as permanently deleted, "
        f"{len(kept_set)} kept"
    )
    if not kept_set:
        print("[Filter] All candidates excluded — no job to submit")
        return 0

    # Narrow the entry list to the survivors, preserving order (mirrors the
    # orchestrator: rebuild from kept_set rather than reusing the pre-filter list).
    kept_keys = {(o.source_bucket, o.object_key) for o in kept_set}
    kept_entries = [
        e for e in manifest_result.entries
        if (e.source_bucket, e.object_key) in kept_keys
    ]
    # Refs are built from the written manifest, not from every eligible
    # operation, so only objects that actually reached a submitted job are
    # recorded as processed (mirrors _leased_manifest_and_submit).
    kept_triples = {
        (e.source_bucket, e.object_key, e.version_id) for e in kept_entries
    }
    submitted_refs = build_submitted_refs(deduped, kept_triples)

    print(f"[Manifest] Writing inventory manifest to s3://{STATE_BUCKET}/manifests/{SOURCE_BUCKET}/...")
    ts_label = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    data_file_key = f"manifests/{SOURCE_BUCKET}/{ts_label}/data/data.csv"
    try:
        # serialize() is the Solution's own serializer: it always emits three
        # fields, writes the literal string "null" for a null version, and
        # percent-encodes the object key. This script previously hand-rolled
        # its own CSV and its own Fields spec, which had none of those three
        # behaviours — so a single object with no version ID collapsed the
        # whole manifest to a 2-field spec that S3ReplicateObject rejects
        # ("Missing job manifest spec fields"). Using the real serializer
        # means this script cannot diverge from what the Solution writes.
        csv_bytes = serialize_manifest(kept_entries).encode("utf-8")
        written = write_in_memory_inventory_manifest(
            s3_client=s3,
            scratch_bucket=STATE_BUCKET,
            config_id=SOURCE_BUCKET,
            source_bucket=SOURCE_BUCKET,
            csv_bytes=csv_bytes,
            data_file_key=data_file_key,
            kms_key_arn=KMS_KEY_ARN,
        )
        written.object_count = len(kept_entries)
        print(
            f"[Manifest] Written: key={written.s3_location.key!r}  "
            f"etag={written.etag!r}  all_versioned={written.all_versioned!r}  "
            f"objects={written.object_count}"
        )
        if KMS_KEY_ARN:
            assert_sse_kms(s3, STATE_BUCKET, written.s3_location.key, "manifest")
            assert_sse_kms(s3, STATE_BUCKET, data_file_key, "manifest data file")
            assert_newest_athena_result_sse_kms(s3)
    except InventoryManifestWriteError as exc:
        print(f"[Manifest] Write ERROR: {exc}")
        return 1
    except Exception as exc:
        print(f"[Manifest] Write ERROR: {exc}")
        return 1

    print(
        f"[BatchJob] Submitting S3 Batch Operations job  "
        f"bucket={SOURCE_BUCKET!r}  role={replication_role_arn!r}..."
    )
    try:
        # Routed through the Solution's own submit_batch_job rather than a
        # hand-rolled create_job. A script that builds its own request cannot
        # catch a defect in the request the Solution builds — the
        # Manifest.Location.ManifestEncryption defect fixed in 0.1.13 lived in
        # exactly that blind spot and this script would not have seen it.
        # The completion report is ENABLED here deliberately. With
        # Report.Enabled=False (this script's previous behaviour, and the
        # default when completion_report_prefix is omitted) a job that fails
        # every task reports only "Job failure rate 100% is above 50%" — S3
        # records no per-task reason anywhere, so a total failure is
        # undiagnosable after the fact. The report is the only place per-task
        # failure reasons exist.
        report_prefix = _completion_report_prefix(
            SOURCE_BUCKET, written.s3_location.key
        )
        # The replication role writes the report itself, so it needs
        # s3:PutObject to that prefix on the State Bucket. The orchestrator
        # primes this before submitting; without it the job runs but the
        # report never appears.
        try:
            bucket_policy_adapter.ensure_completion_report_bucket_policy(
                s3, STATE_BUCKET, SOURCE_BUCKET, replication_role_arn, ACCOUNT,
            )
        except Exception as exc:
            print(f"[BatchJob] WARNING could not prime report bucket policy: {exc}")

        submission = submit_batch_job(
            s3control_client=s3control,
            account_id=ACCOUNT,
            manifest_location=written.s3_location,
            manifest_etag=written.etag,
            replication_role_arn=replication_role_arn,
            config_id=SOURCE_BUCKET,
            object_count=written.object_count,
            source_bucket=SOURCE_BUCKET,
            has_version_ids=written.all_versioned,
            manifest_format=ManifestFormat.INVENTORY_REPORT.value,
            completion_report_prefix=report_prefix,
            state_bucket=STATE_BUCKET,
        )
        if submission.failed:
            # failure_class distinguishes a Solution defect (PERMANENT_CLIENT,
            # rejected by botocore before signing) from an account-side
            # condition (SERVICE). Printed so a failing run says which.
            klass = (
                submission.failure_class.value
                if submission.failure_class
                else "UNKNOWN"
            )
            print(
                f"[BatchJob] ERROR ({submission.status.value}, "
                f"class={klass}): {submission.error_reason}"
            )
            return 1
        if submission.status is SubmissionStatus.SKIPPED:
            print("[BatchJob] Skipped: manifest empty")
            return 0
        job_id = submission.job_id
        print(f"[BatchJob]   created job_id={job_id!r}")

        any_submitted = True
    except Exception as exc:
        print(f"[BatchJob] ERROR: {exc}")
        return 1

    # -----------------------------------------------------------------------
    # Monitoring is informational and OUTSIDE the submission try/except above.
    # The job is already created at this point, so a transient DescribeJob
    # error says nothing about whether submission worked — a single closed
    # connection previously failed the whole run after a successful submit.
    # The orchestrator treats a DescribeJob failure the same way ("a transient
    # DescribeJob error doesn't count as evidence either way", orchestrator.py
    # ~line 732); this mirrors that rather than being stricter than the code
    # under test.
    # -----------------------------------------------------------------------
    print(f"[BatchJob]   status=SUBMITTED  job_id={job_id!r}")
    consecutive_describe_errors = 0
    for attempt in range(60):
        try:
            job_resp = s3control.describe_job(AccountId=ACCOUNT, JobId=job_id)
        except Exception as exc:
            consecutive_describe_errors += 1
            print(
                f"[BatchJob]   poll {attempt}: DescribeJob failed "
                f"(best-effort, {consecutive_describe_errors} in a row): {exc}"
            )
            if consecutive_describe_errors >= 5:
                print(
                    "[BatchJob]   giving up on monitoring after 5 consecutive "
                    "DescribeJob failures; the job was submitted successfully "
                    "and is unaffected"
                )
                break
            time.sleep(5)
            continue
        consecutive_describe_errors = 0
        job_status = job_resp["Job"]["Status"]
        progress = job_resp["Job"].get("ProgressSummary", {})
        print(f"[BatchJob]   poll {attempt}: status={job_status} progress={progress}")
        if job_status == "Complete":
            succeeded = progress.get("NumberOfTasksSucceeded", 0)
            failed = progress.get("NumberOfTasksFailed", 0)
            print(f"[BatchJob]   Complete: {succeeded} succeeded, {failed} failed")
            break
        if job_status in ("Failed", "Cancelled"):
            reasons = job_resp["Job"].get("FailureReasons", [])
            print(f"[BatchJob]   {job_status}: {reasons}")
            print(
                f"[BatchJob]   per-task failure reasons are in the completion "
                f"report under s3://{STATE_BUCKET}/{report_prefix}"
            )
            break
        time.sleep(5)

    # -----------------------------------------------------------------------
    # [Checkpoint]  Advance checkpoint (only when a job was submitted)
    # -----------------------------------------------------------------------
    refs_to_persist = submitted_refs if any_submitted else None
    print(
        f"[Checkpoint] Advancing with {len(refs_to_persist or [])} submitted ref(s)  "
        f"(etag={checkpoint_etag!r})..."
    )
    try:
        # candidate_hwm, not max(refs): the watermark advances over every
        # eligible operation, including those that matched no rule or were
        # excluded by the Deleted_Version_Filter and so carry no ref. Mirrors
        # orchestrator._lease_scope — without it this harness would reproduce
        # exactly the cursor stall that passing it prevents in production.
        new_state = advance_checkpoint(
            state, refs_to_persist, LOOKBACK, candidate_hwm,
        )
        store.put_checkpoint(s3, STATE_BUCKET, new_state, checkpoint_etag)
        print(
            f"[Checkpoint] Advanced to "
            f"{new_state.last_processed_watermark!r}"
        )
        if KMS_KEY_ARN:
            assert_sse_kms(
                s3, STATE_BUCKET, f"state/{SOURCE_BUCKET}.json", "state object"
            )
    except ConditionalWriteError as exc:
        # Another process wrote concurrently — checkpoint not advanced.
        # Jobs already submitted are idempotent; re-run will skip duplicates.
        print(f"[Checkpoint] WARNING (concurrent write, non-fatal): {exc}")
    except Exception as exc:
        print(f"[Checkpoint] WARNING (non-fatal): {exc}")

    if any_submitted:
        print("\n[SUCCESS] Batch replication job(s) submitted.")
        return 0

    print("\n[INFO] Pipeline complete — no matching objects to replicate.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
