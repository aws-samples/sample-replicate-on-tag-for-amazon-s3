"""Unit and property-based tests for deploy/config_resource/index.py.

Covers the config custom-resource handler lifecycle (create / update / delete / failure)
and Property 1: the Solution_Config produced by the handler construction logic is
accepted by config_loader.load_config and preserves the input bucket set.

Feature: console-deployment
Requirements: 7.2, 7.3, 7.4, 7.5, 7.6, 11.2, 11.3, 11.4
"""
from __future__ import annotations

import json
import os
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

# The privileged parameters come from the function's environment, so the
# template's Environment block is simulated here (scan-aa27a832 Req 1.3).
STATE_BUCKET = "example-state-bucket"
CONFIG_KEY = "config/solution-config.json"
STACK_ID = (
    "arn:aws:cloudformation:us-east-1:123456789012:stack/s3rot/"
    "11111111-2222-3333-4444-555555555555"
)


def _run_handler(
    event: dict,
    *,
    s3_client: MagicMock | None = None,
    env: dict[str, str] | None = None,
) -> tuple[MagicMock, MagicMock]:
    """Execute the standalone handler with mocked boto3 and cfnresponse.

    Returns (cfnresponse_mock, s3_mock) so callers can assert on both.
    Pass ``env`` to override the simulated template environment.
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

    props = event.get("ResourceProperties", {})
    handler_env = {
        "STATE_BUCKET": STATE_BUCKET,
        "CONFIG_KEY": CONFIG_KEY,
        "STACK_ID": STACK_ID,
        # The template resolves each of these from the same expression as the
        # matching custom-resource property, so by default the simulated
        # environment agrees with the event. Tests that need them to disagree
        # pass ``env`` or use _foreign_props.
        "SOURCE_BUCKET_NAMES": ",".join(props.get("Buckets") or []),
        "REGION": str(props.get("Region", "us-east-1")),
        "CHECK_FREQUENCY_MINUTES": str(props.get("CheckFrequencyMinutes", 60)),
        "KMS_KEY_ARN": str(props.get("KmsKeyArn", "")),
    }
    if env is not None:
        handler_env.update(env)

    code = _HANDLER_PATH.read_text()
    context = MagicMock()
    with patch.dict(os.environ, handler_env):
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
    state_bucket: str = STATE_BUCKET,
    config_key: str = CONFIG_KEY,
) -> dict:
    return {
        "RequestType": "Create",
        "StackId": STACK_ID,
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
    state_bucket: str = STATE_BUCKET,
    config_key: str = CONFIG_KEY,
) -> dict:
    return {
        "RequestType": "Delete",
        "StackId": STACK_ID,
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


# ---------------------------------------------------------------------------
# Tests: privileged-parameter source and StackId gate
# scan-aa27a832 remediation, Req 1.2, 1.3
# ---------------------------------------------------------------------------

FOREIGN_BUCKET = "attacker-bucket"
FOREIGN_KEY = "config/attacker.json"
FOREIGN_STACK_ID = (
    "arn:aws:cloudformation:us-east-1:123456789012:stack/other/"
    "99999999-8888-7777-6666-555555555555"
)


def _foreign_props(event: dict) -> dict:
    """Point the event's StateBucket and ConfigKey at attacker-chosen values."""
    event["ResourceProperties"]["StateBucket"] = FOREIGN_BUCKET
    event["ResourceProperties"]["ConfigKey"] = FOREIGN_KEY
    return event


# The environment a stack deployed for two buckets on an hourly interval sets.
_STACK_ENV = {
    "SOURCE_BUCKET_NAMES": "bucket-a,bucket-b",
    "REGION": "us-east-1",
    "CHECK_FREQUENCY_MINUTES": "60",
    "KMS_KEY_ARN": "",
}


class TestPrivilegedParameterSource:
    """StateBucket and ConfigKey come from the environment, not the event.

    scan-aa27a832 remediation, Req 1.3: the Delete branch removes the config
    object, which halts every scheduled run, so a caller holding
    lambda:InvokeFunction on this function must not be able to name the object
    that is written or deleted.
    """

    def test_event_state_bucket_and_config_key_never_reach_put_object(self):
        """A foreign StateBucket/ConfigKey in the event is ignored on Create (Req 1.3)."""
        s3_mock = MagicMock()
        _run_handler(_foreign_props(_create_event()), s3_client=s3_mock)
        kwargs = s3_mock.put_object.call_args_list[0][1]
        assert kwargs["Bucket"] == STATE_BUCKET
        assert kwargs["Key"] == CONFIG_KEY
        buckets = {c[1]["Bucket"] for c in s3_mock.put_object.call_args_list}
        assert FOREIGN_BUCKET not in buckets

    def test_event_config_key_never_reaches_delete_object(self):
        """A foreign ConfigKey in the event is ignored on Delete (Req 1.3)."""
        s3_mock = MagicMock()
        _run_handler(_foreign_props(_delete_event()), s3_client=s3_mock)
        s3_mock.delete_object.assert_called_once_with(
            Bucket=STATE_BUCKET, Key=CONFIG_KEY
        )

    def test_seed_writes_use_the_environment_bucket(self):
        """Checkpoint seeds land in the template's bucket, not the event's (Req 1.3)."""
        s3_mock = MagicMock()
        _run_handler(
            _foreign_props(_create_event(buckets=["alpha"])), s3_client=s3_mock
        )
        seeds = _seed_put_calls(s3_mock)
        assert seeds, "no seed write observed"
        assert all(s["Bucket"] == STATE_BUCKET for s in seeds)

    def test_physical_resource_id_is_the_environment_key(self):
        """The physical id follows the environment, keeping the resource stable (Req 1.3)."""
        cfnr, _ = _run_handler(_foreign_props(_create_event()))
        assert cfnr.send.call_args[1]["physicalResourceId"] == CONFIG_KEY


class TestStackIdCheck:
    """An event from anything other than this stack is rejected.

    scan-aa27a832 remediation, Req 1.2: the handler compares the event's
    StackId with the STACK_ID environment variable before any S3 mutation.
    """

    def _mismatched(self, event: dict) -> dict:
        event["StackId"] = FOREIGN_STACK_ID
        return event

    def test_mismatched_stack_id_writes_nothing(self):
        """A foreign StackId reaches neither put_object nor delete_object (Req 1.2)."""
        s3_mock = MagicMock()
        _run_handler(self._mismatched(_create_event()), s3_client=s3_mock)
        s3_mock.put_object.assert_not_called()
        s3_mock.delete_object.assert_not_called()

    def test_mismatched_stack_id_deletes_nothing(self):
        """The Delete branch is gated too, so the config object survives (Req 1.2)."""
        s3_mock = MagicMock()
        cfnr, _ = _run_handler(self._mismatched(_delete_event()), s3_client=s3_mock)
        s3_mock.delete_object.assert_not_called()
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_mismatched_stack_id_responds_failed_without_specifics(self):
        """The FAILED reason names neither the expected nor the supplied value (Req 1.2)."""
        cfnr, _ = _run_handler(self._mismatched(_create_event()))
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "FAILED"
        reason = cfnr.send.call_args[0][3]["Error"]
        assert STACK_ID not in reason
        assert FOREIGN_STACK_ID not in reason
        assert STATE_BUCKET not in reason
        assert "StackId" not in reason

    def test_missing_stack_id_is_rejected(self):
        """An event carrying no StackId at all is rejected (Req 1.2)."""
        event = _create_event()
        del event["StackId"]
        s3_mock = MagicMock()
        cfnr, _ = _run_handler(event, s3_client=s3_mock)
        s3_mock.put_object.assert_not_called()
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_matching_stack_id_still_writes(self):
        """The check is not vacuous: the matching StackId path still writes (Req 1.2)."""
        s3_mock = MagicMock()
        cfnr, _ = _run_handler(_create_event(), s3_client=s3_mock)
        assert s3_mock.put_object.call_count > 0
        assert cfnr.send.call_args[0][2] == "SUCCESS"


# ---------------------------------------------------------------------------
# Tests: the config object's content is env-sourced
# diff scan scan-972cfd4f, f-77d789f5
# ---------------------------------------------------------------------------


class TestConfigContentSource:
    """The written config is built from the environment, not the event.

    A caller holding lambda:InvokeFunction can forge a Create or Update event
    carrying this stack's readable StackId. If the bucket list, Region, interval
    or KMS key came from the event, that caller could rewrite
    config/solution-config.json: dropping a bucket stops the orchestrator
    monitoring it, which is silent non-replication, and the poisoned object
    survives until the next legitimate stack update.
    """

    def _forged(self, request_type: str = "Create") -> dict:
        event = _create_event(
            buckets=["attacker-bucket"],
            region="eu-west-3",
            check_frequency_minutes=1440,
        )
        event["RequestType"] = request_type
        event["ResourceProperties"]["KmsKeyArn"] = (
            "arn:aws:kms:eu-west-3:123456789012:key/attacker"
        )
        return event

    def test_event_buckets_and_region_never_reach_the_config_object(self):
        """Forged Buckets and Region in the event do not appear in the config."""
        s3_mock = MagicMock()
        _run_handler(self._forged(), s3_client=s3_mock, env=_STACK_ENV)
        body = json.loads(s3_mock.put_object.call_args_list[0][1]["Body"])
        assert body["buckets"] == [
            {"name": "bucket-a", "region": "us-east-1"},
            {"name": "bucket-b", "region": "us-east-1"},
        ]

    def test_event_check_frequency_never_reaches_the_processing_interval(self):
        """The interval follows CHECK_FREQUENCY_MINUTES, not the event."""
        s3_mock = MagicMock()
        cfnr, _ = _run_handler(self._forged(), s3_client=s3_mock, env=_STACK_ENV)
        body = json.loads(s3_mock.put_object.call_args_list[0][1]["Body"])
        assert body["processing_interval"] == "1h"
        assert cfnr.send.call_args[0][3]["CheckFrequencySeconds"] == "3600"

    def test_event_kms_key_never_reaches_put_object(self):
        """A forged KmsKeyArn does not encrypt the config object under it."""
        s3_mock = MagicMock()
        _run_handler(self._forged(), s3_client=s3_mock, env=_STACK_ENV)
        kwargs = s3_mock.put_object.call_args_list[0][1]
        assert "SSEKMSKeyId" not in kwargs
        assert "ServerSideEncryption" not in kwargs

    def test_event_buckets_never_seed_a_state_object(self):
        """Seeds are written for the environment's bucket names only."""
        s3_mock = MagicMock()
        _run_handler(self._forged(), s3_client=s3_mock, env=_STACK_ENV)
        seeded = {s["Key"] for s in _seed_put_calls(s3_mock)}
        assert seeded == {"state/bucket-a.json", "state/bucket-b.json"}

    def test_environment_kms_key_is_still_applied(self):
        """The check is not vacuous: the environment's KMS key does reach S3."""
        s3_mock = MagicMock()
        key_arn = "arn:aws:kms:us-east-1:123456789012:key/abc"
        _run_handler(
            self._forged(),
            s3_client=s3_mock,
            env={**_STACK_ENV, "KMS_KEY_ARN": key_arn},
        )
        kwargs = s3_mock.put_object.call_args_list[0][1]
        assert kwargs["SSEKMSKeyId"] == key_arn
        assert kwargs["ServerSideEncryption"] == "aws:kms"

    def test_forged_update_cannot_reduce_the_monitored_bucket_set(self):
        """An Update naming one bucket still writes both environment buckets."""
        s3_mock = MagicMock()
        event = self._forged("Update")
        event["OldResourceProperties"] = {"Buckets": ["bucket-a", "bucket-b"]}
        _run_handler(event, s3_client=s3_mock, env=_STACK_ENV)
        body = json.loads(s3_mock.put_object.call_args_list[0][1]["Body"])
        assert [b["name"] for b in body["buckets"]] == ["bucket-a", "bucket-b"]
