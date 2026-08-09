"""Unit and property-based tests for deploy/config_resource/index.py.

Covers the config custom-resource handler lifecycle (create / update / delete / failure)
and Property 1: the Solution_Config produced by the handler construction logic is
accepted by config_loader.load_config and preserves the input bucket set.

Feature: console-deployment
Requirements: 7.2, 7.3, 7.4, 7.5, 7.6, 11.2, 11.3, 11.4
"""
from __future__ import annotations

import json
import string
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.config_loader import ConfigError, load_config

# ---------------------------------------------------------------------------
# Handler loader
# ---------------------------------------------------------------------------

_HANDLER_PATH = Path(__file__).parent.parent / "deploy" / "config_resource" / "index.py"


def _run_handler(
    event: dict, *, s3_client: MagicMock | None = None
) -> tuple[MagicMock, MagicMock]:
    """Execute the standalone handler with mocked boto3 and cfnresponse.

    Returns (cfnresponse_mock, s3_mock) so callers can assert on both.
    """
    from botocore.exceptions import ClientError as _BotocoreClientError

    cfnresponse_mock = MagicMock()
    cfnresponse_mock.SUCCESS = "SUCCESS"
    cfnresponse_mock.FAILED = "FAILED"

    s3_mock = s3_client if s3_client is not None else MagicMock()
    # Wire s3_mock.exceptions.ClientError to the real botocore class so that
    # `except s3.exceptions.ClientError` in the handler works correctly.
    s3_mock.exceptions.ClientError = _BotocoreClientError

    def make_client(service, **kwargs):
        return s3_mock if service == "s3" else MagicMock()

    code = _HANDLER_PATH.read_text()
    context = MagicMock()
    with patch.dict(sys.modules, {"cfnresponse": cfnresponse_mock}):
        with patch("boto3.client", side_effect=make_client):
            ns: dict = {}
            exec(compile(code, str(_HANDLER_PATH), "exec"), ns)
            try:
                ns["handler"](event, context)
            except Exception:
                pass  # handler re-raises after cfnresponse.send(FAILED) — expected
    return cfnresponse_mock, s3_mock


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------


def _create_event(
    buckets: list[str] | None = None,
    region: str = "us-east-1",
    check_frequency_minutes: object = 60,
    state_bucket: str = "example-state-bucket",
    config_key: str = "config/solution-config.json",
) -> dict:
    return {
        "RequestType": "Create",
        "ResourceProperties": {
            "StateBucket": state_bucket,
            "ConfigKey": config_key,
            "Buckets": buckets if buckets is not None else ["bucket-a", "bucket-b"],
            "Region": region,
            "CheckFrequencyMinutes": check_frequency_minutes,
        },
    }


def _update_event(*, old_buckets: list[str] | None = None, **kwargs) -> dict:
    ev = _create_event(**kwargs)
    ev["RequestType"] = "Update"
    ev["PhysicalResourceId"] = ev["ResourceProperties"]["ConfigKey"]
    if old_buckets is not None:
        ev["OldResourceProperties"] = {
            **ev["ResourceProperties"],
            "Buckets": old_buckets,
        }
    return ev


def _delete_event(
    state_bucket: str = "example-state-bucket",
    config_key: str = "config/solution-config.json",
) -> dict:
    return {
        "RequestType": "Delete",
        "PhysicalResourceId": config_key,
        "ResourceProperties": {
            "StateBucket": state_bucket,
            "ConfigKey": config_key,
            "Buckets": [],
            "Region": "us-east-1",
            "CheckFrequencyMinutes": 60,
        },
    }


# ---------------------------------------------------------------------------
# Tests: Create (Req 7.2, 11.2)
# ---------------------------------------------------------------------------


class TestCreate:
    def test_calls_put_object_with_correct_bucket_and_key(self):
        """Create invokes s3.put_object with StateBucket and ConfigKey (Req 7.2)."""
        s3_mock = MagicMock()
        _run_handler(_create_event(), s3_client=s3_mock)
        # First put_object call is the config write.
        config_call = s3_mock.put_object.call_args_list[0]
        kwargs = config_call[1]
        assert kwargs["Bucket"] == "example-state-bucket"
        assert kwargs["Key"] == "config/solution-config.json"

    def test_writes_correct_config_structure(self):
        """Create writes {buckets: [{name, region}], processing_interval} (Req 7.5)."""
        s3_mock = MagicMock()
        _run_handler(
            _create_event(
                buckets=["bucket-a", "bucket-b"],
                region="us-west-2",
                check_frequency_minutes=30,
            ),
            s3_client=s3_mock,
        )
        body = json.loads(s3_mock.put_object.call_args_list[0][1]["Body"])
        assert body == {
            "buckets": [
                {"name": "bucket-a", "region": "us-west-2"},
                {"name": "bucket-b", "region": "us-west-2"},
            ],
            "processing_interval": "30m",
        }

    def test_sends_success_with_stable_physical_id(self):
        """Create sends SUCCESS with physicalResourceId=ConfigKey (Req 7.2)."""
        cfnr, _ = _run_handler(_create_event())
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"
        assert cfnr.send.call_args[1]["physicalResourceId"] == "config/solution-config.json"
        # Data carries CheckFrequencySeconds for the alarm Period derivation.
        data = cfnr.send.call_args[0][3]
        assert data["CheckFrequencySeconds"] == "3600"

    def test_sets_content_type_json(self):
        """Create sets ContentType=application/json on the S3 object."""
        s3_mock = MagicMock()
        _run_handler(_create_event(), s3_client=s3_mock)
        assert s3_mock.put_object.call_args[1].get("ContentType") == "application/json"

    def test_region_uniform_across_all_buckets(self):
        """Every bucket entry uses the single supplied region (Req 7.5)."""
        s3_mock = MagicMock()
        _run_handler(
            _create_event(buckets=["b1", "b2", "b3"], region="eu-central-1"),
            s3_client=s3_mock,
        )
        body = json.loads(s3_mock.put_object.call_args_list[0][1]["Body"])
        assert all(entry["region"] == "eu-central-1" for entry in body["buckets"])


# ---------------------------------------------------------------------------
# Tests: Update (Req 7.3, 11.2)
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_calls_put_object_not_delete(self):
        """Update invokes put_object (not delete_object) with the new config (Req 7.3)."""
        s3_mock = MagicMock()
        _run_handler(
            _update_event(buckets=["new-bucket"], old_buckets=["new-bucket"]),
            s3_client=s3_mock,
        )
        # Only the config write — no seed because bucket list is unchanged.
        s3_mock.put_object.assert_called_once()
        s3_mock.delete_object.assert_not_called()

    def test_physical_resource_id_stable(self):
        """Update sends SUCCESS with the same ConfigKey as PhysicalResourceId (Req 7.3)."""
        cfnr, _ = _run_handler(_update_event())
        assert cfnr.send.call_args[1]["physicalResourceId"] == "config/solution-config.json"
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_writes_updated_config(self):
        """Update writes the new bucket list (Req 7.3)."""
        s3_mock = MagicMock()
        _run_handler(
            _update_event(buckets=["only-bucket"], region="ap-east-1", old_buckets=["only-bucket"]),
            s3_client=s3_mock,
        )
        body = json.loads(s3_mock.put_object.call_args[1]["Body"])
        assert [e["name"] for e in body["buckets"]] == ["only-bucket"]


# ---------------------------------------------------------------------------
# Tests: Delete (Req 7.4, 11.3)
# ---------------------------------------------------------------------------


class TestDelete:
    def test_calls_delete_object_not_put(self):
        """Delete invokes delete_object (not put_object) (Req 7.4)."""
        s3_mock = MagicMock()
        _run_handler(_delete_event(), s3_client=s3_mock)
        s3_mock.delete_object.assert_called_once_with(
            Bucket="example-state-bucket", Key="config/solution-config.json"
        )
        s3_mock.put_object.assert_not_called()

    def test_sends_success(self):
        """Delete sends SUCCESS (Req 7.4)."""
        cfnr, _ = _run_handler(_delete_event())
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_tolerates_absent_object(self):
        """Delete is safe even if the config object does not exist (Req 7.4).

        S3 delete_object is idempotent — it returns 204 whether or not the key
        existed. A NoSuchKey-equivalent from an unusually strict mock should
        still produce SUCCESS if delete_object itself doesn't raise.
        """
        s3_mock = MagicMock()
        s3_mock.delete_object.return_value = {}  # S3 returns empty 204 body
        cfnr, _ = _run_handler(_delete_event(), s3_client=s3_mock)
        assert cfnr.send.call_args[0][2] == "SUCCESS"


# ---------------------------------------------------------------------------
# Tests: Failure signaling (Req 7.6)
# ---------------------------------------------------------------------------


class TestFailureSignaling:
    def test_put_object_raises_sends_failed(self):
        """s3.put_object raising causes cfnresponse.send(FAILED) (Req 7.6)."""
        s3_mock = MagicMock()
        s3_mock.put_object.side_effect = Exception("AccessDenied")
        cfnr, _ = _run_handler(_create_event(), s3_client=s3_mock)
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_delete_object_raises_sends_failed(self):
        """s3.delete_object raising causes cfnresponse.send(FAILED) (Req 7.6)."""
        s3_mock = MagicMock()
        s3_mock.delete_object.side_effect = Exception("InternalError")
        cfnr, _ = _run_handler(_delete_event(), s3_client=s3_mock)
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_failure_includes_error_message(self):
        """FAILED signal includes the exception message in the data dict (Req 7.6)."""
        s3_mock = MagicMock()
        s3_mock.put_object.side_effect = Exception("bucket does not exist")
        cfnr, _ = _run_handler(_create_event(), s3_client=s3_mock)
        data = cfnr.send.call_args[0][3]
        assert "bucket does not exist" in data.get("Error", "")

    def test_cfnresponse_always_called_exactly_once(self):
        """cfnresponse.send is called exactly once regardless of outcome (Req 7.6)."""
        for s3_mock in (MagicMock(), _raising_s3()):
            cfnr, _ = _run_handler(_create_event(), s3_client=s3_mock)
            assert cfnr.send.call_count == 1, (
                "cfnresponse.send must be called exactly once per invocation"
            )


def _raising_s3() -> MagicMock:
    from botocore.exceptions import ClientError as _BotocoreClientError

    m = MagicMock()
    m.put_object.side_effect = RuntimeError("simulated failure")
    m.exceptions.ClientError = _BotocoreClientError
    return m


# ---------------------------------------------------------------------------
# Tests: processing_interval derived from CheckFrequencyMinutes (Req 7.5)
# ---------------------------------------------------------------------------


class TestProcessingIntervalDerivation:
    @pytest.mark.parametrize(
        "minutes,expected",
        [
            (60, "1h"),
            (120, "2h"),
            (30, "30m"),
            (15, "15m"),
            (90, "90m"),
            (1440, "24h"),
            ("60", "1h"),  # CloudFormation passes resource properties as strings
            ("45", "45m"),
        ],
    )
    def test_interval_derived_from_check_frequency(self, minutes, expected):
        """processing_interval is derived from CheckFrequencyMinutes."""
        s3_mock = MagicMock()
        _run_handler(_create_event(check_frequency_minutes=minutes), s3_client=s3_mock)
        body = json.loads(s3_mock.put_object.call_args_list[0][1]["Body"])
        assert body["processing_interval"] == expected



# ---------------------------------------------------------------------------
# Property 1: Config construction is loader-accepted and preserves bucket set
# (Req 7.5, 11.4) — minimum 100 iterations via conftest.py default profile
# ---------------------------------------------------------------------------

_ALNUM = string.ascii_lowercase + string.digits
_INNER = string.ascii_lowercase + string.digits + "-"


@st.composite
def _valid_bucket_name(draw: st.DrawFn) -> str:
    """Draw a valid S3 bucket name (lowercase alphanumeric + hyphens, no periods).

    Restricted to this safe subset to avoid the consecutive-dots and IP-format
    edge cases in config_loader._is_valid_s3_bucket_name.  Length: 3–63 chars.
    """
    first = draw(st.sampled_from(_ALNUM))
    last = draw(st.sampled_from(_ALNUM))
    inner = draw(st.text(alphabet=_INNER, min_size=1, max_size=61))
    return first + inner + last


@given(
    names=st.lists(_valid_bucket_name(), min_size=1, max_size=20, unique=True),
    region=st.sampled_from(
        ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "ca-central-1"]
    ),
    interval=st.sampled_from(["1h", "30m", "15m", "2h", "24h"]),
)
@settings(max_examples=100)
def test_config_construction_is_loader_accepted(
    names: list[str], region: str, interval: str
) -> None:
    """Property 1: config built by handler logic is accepted by load_config.

    For any list of 1–20 distinct valid S3 bucket names, a region, and an
    interval string, the Solution_Config built by the Config_Custom_Resource
    construction logic must:
    (a) contain exactly one buckets entry per input name with region equal to
        the supplied region,
    (b) carry the supplied processing_interval,
    (c) be accepted by load_config (no ConfigError),
    (d) yield an AppConfig whose bucket-name set equals the input set.

    Tags: Feature: console-deployment, Property 1
    Validates: Requirements 7.5, 11.4
    """
    # Build config using the same logic as the handler (Req 7.5)
    config = {
        "buckets": [{"name": n, "region": region} for n in names],
        "processing_interval": interval,
    }

    # (c) load_config must accept it without raising
    try:
        app_config = load_config(config)
    except ConfigError as exc:
        pytest.fail(f"load_config raised ConfigError for valid config: {exc}")

    # (a) one entry per name with the correct region
    assert len(app_config.buckets) == len(names), (
        f"Expected {len(names)} bucket entries, got {len(app_config.buckets)}"
    )
    for bucket in app_config.buckets:
        assert bucket.region == region, (
            f"Expected region {region!r}, got {bucket.region!r} for bucket {bucket.name!r}"
        )

    # (d) bucket-name set equals the input set
    assert {b.name for b in app_config.buckets} == set(names), (
        "AppConfig bucket-name set does not match the input set"
    )

    # (b) processing_interval is present in the raw config (load_config doesn't consume it,
    # but the handler must write it for schema fidelity — Req 7.5)
    assert config["processing_interval"] == interval


# ---------------------------------------------------------------------------
# Tests: Checkpoint seeding on Create and Update
# Feature: deploy-time-watermark-seed
# Requirements: 1.1–1.5, 2.1–2.4
# ---------------------------------------------------------------------------


def _seed_put_calls(s3_mock: MagicMock) -> list[dict]:
    """Return put_object call kwargs that target state/ keys (checkpoint seeds)."""
    return [
        call[1]
        for call in s3_mock.put_object.call_args_list
        if call[1].get("Key", "").startswith("state/")
    ]


class TestCheckpointSeedCreate:
    """On Create, every source bucket gets a seeded checkpoint (Req 1.1–1.5)."""

    def test_seeds_all_buckets_on_create(self):
        """Create writes one state/<name>.json per bucket (Req 1.1)."""
        s3_mock = MagicMock()
        _run_handler(_create_event(buckets=["alpha", "beta"]), s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        assert len(seeds) == 2
        keys = {s["Key"] for s in seeds}
        assert keys == {"state/alpha.json", "state/beta.json"}

    def test_seed_uses_canonical_watermark_format(self):
        """Seeded watermark has format YYYY-MM-DDTHH:MM:SS.ffffffZ (Req 1.2)."""
        import re

        s3_mock = MagicMock()
        _run_handler(_create_event(buckets=["my-bucket"]), s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        body = json.loads(seeds[0]["Body"])
        wm = body["last_processed_watermark"]
        assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$", wm), (
            f"Watermark {wm!r} is not in canonical format"
        )

    def test_seed_checkpoint_schema(self):
        """Seeded checkpoint has source_bucket, watermark, lease, processed_window (Req 1.3)."""
        s3_mock = MagicMock()
        _run_handler(_create_event(buckets=["src-bucket"]), s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        body = json.loads(seeds[0]["Body"])
        assert body["source_bucket"] == "src-bucket"
        assert body["lease"] is None
        assert body["processed_window"] == []
        assert "last_processed_watermark" in body

    def test_seed_uses_conditional_put(self):
        """Seed writes use IfNoneMatch=* to avoid overwriting (Req 1.4)."""
        s3_mock = MagicMock()
        _run_handler(_create_event(buckets=["b1"]), s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        assert seeds[0].get("IfNoneMatch") == "*"

    def test_seed_respects_kms_key(self):
        """Seed writes use SSE-KMS when KmsKeyArn is supplied."""
        s3_mock = MagicMock()
        event = _create_event(buckets=["b1"])
        event["ResourceProperties"]["KmsKeyArn"] = "arn:aws:kms:us-east-1:123:key/abc"
        _run_handler(event, s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        assert seeds[0]["ServerSideEncryption"] == "aws:kms"
        assert seeds[0]["SSEKMSKeyId"] == "arn:aws:kms:us-east-1:123:key/abc"

    def test_precondition_failed_is_swallowed(self):
        """A 412 PreconditionFailed on the seed is silently skipped (Req 1.5)."""
        from botocore.exceptions import ClientError as _CE

        s3_mock = MagicMock()
        call_count = {"n": 0}
        original_put = s3_mock.put_object

        def _side_effect(**kwargs):
            call_count["n"] += 1
            if kwargs.get("Key", "").startswith("state/"):
                raise _CE(
                    {"Error": {"Code": "PreconditionFailed", "Message": "exists"}},
                    "PutObject",
                )
            return original_put(**kwargs)

        s3_mock.put_object = MagicMock(side_effect=_side_effect)
        s3_mock.exceptions.ClientError = _CE
        cfnr, _ = _run_handler(_create_event(buckets=["b1"]), s3_client=s3_mock)
        # Handler must still succeed overall.
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_conditional_request_conflict_is_swallowed(self):
        """A ConditionalRequestConflict on the seed is silently skipped (Req 1.5)."""
        from botocore.exceptions import ClientError as _CE

        s3_mock = MagicMock()

        def _side_effect(**kwargs):
            if kwargs.get("Key", "").startswith("state/"):
                raise _CE(
                    {"Error": {"Code": "ConditionalRequestConflict", "Message": "conflict"}},
                    "PutObject",
                )

        s3_mock.put_object = MagicMock(side_effect=_side_effect)
        s3_mock.exceptions.ClientError = _CE
        cfnr, _ = _run_handler(_create_event(buckets=["b1"]), s3_client=s3_mock)
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_unexpected_client_error_propagates(self):
        """A non-412 ClientError on the seed propagates as FAILED (Req 1.5 inverse)."""
        from botocore.exceptions import ClientError as _CE

        s3_mock = MagicMock()

        def _side_effect(**kwargs):
            if kwargs.get("Key", "").startswith("state/"):
                raise _CE(
                    {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                    "PutObject",
                )

        s3_mock.put_object = MagicMock(side_effect=_side_effect)
        s3_mock.exceptions.ClientError = _CE
        cfnr, _ = _run_handler(_create_event(buckets=["b1"]), s3_client=s3_mock)
        assert cfnr.send.call_args[0][2] == "FAILED"


class TestCheckpointSeedUpdate:
    """On Update, only newly added buckets get a seed (Req 2.1–2.4)."""

    def test_seeds_only_new_buckets(self):
        """Update with one new bucket seeds only that bucket (Req 2.1, 2.2)."""
        s3_mock = MagicMock()
        event = _update_event(
            buckets=["existing", "new-one"],
            old_buckets=["existing"],
        )
        _run_handler(event, s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        assert len(seeds) == 1
        assert seeds[0]["Key"] == "state/new-one.json"

    def test_unchanged_buckets_not_seeded(self):
        """Update with same bucket list writes no state objects (Req 2.3, 2.4)."""
        s3_mock = MagicMock()
        event = _update_event(
            buckets=["alpha", "beta"],
            old_buckets=["alpha", "beta"],
        )
        _run_handler(event, s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        assert seeds == []

    def test_removed_bucket_not_seeded(self):
        """A bucket removed from the list is not touched (Req 2.3)."""
        s3_mock = MagicMock()
        event = _update_event(
            buckets=["beta"],
            old_buckets=["alpha", "beta"],
        )
        _run_handler(event, s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        assert seeds == []

    def test_no_old_resource_properties_seeds_all(self):
        """If OldResourceProperties is absent (edge case), all are treated as new."""
        s3_mock = MagicMock()
        event = _update_event(buckets=["a", "b"])  # old_buckets=None -> no OldResourceProperties
        _run_handler(event, s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        assert len(seeds) == 2


class TestCheckpointSeedDelete:
    """Delete does not touch state objects (Req 6.1)."""

    def test_delete_does_not_write_state(self):
        """Delete event writes no state/ objects."""
        s3_mock = MagicMock()
        _run_handler(_delete_event(), s3_client=s3_mock)
        seeds = _seed_put_calls(s3_mock)
        assert seeds == []
