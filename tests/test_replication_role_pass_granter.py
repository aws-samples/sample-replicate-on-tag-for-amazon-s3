"""Tests for ReplicationRolePassGranterFunction (inline ZipFile code in
deploy/template.yaml).

The function reads each source bucket's replication role ARN from
GetBucketReplication and writes those ARNs into an iam:PassRole inline policy
on the ExecutionRole. Because a principal holding
s3:PutReplicationConfiguration on a monitored source bucket controls that
value, the ARN is validated for shape and account ownership before it reaches
the policy document (AWS Security Agent finding
f-fdb67b60-9c38-4026-813d-0839e1f525d9).

The function is extracted from the template at test time and executed via
exec() with patched boto3 and cfnresponse so no real AWS calls are made — see
.holmes/accepted-risks.md FP2 for why exec() is the only option here.

Feature: security-scan-remediation
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

_ACCOUNT_ID = "123456789012"
_OTHER_ACCOUNT_ID = "999988887777"
_EXECUTION_ROLE_NAME = "test-execution-role"


# ---------------------------------------------------------------------------
# YAML loader (handles CloudFormation intrinsic function tags)
# ---------------------------------------------------------------------------


class _CfnTag:
    def __init__(self, tag, value):
        self.tag = tag
        self.value = value


def _cfn_constructor(loader, tag_suffix, node):
    tag = "!" + tag_suffix
    if isinstance(node, yaml.ScalarNode):
        value = loader.construct_scalar(node)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    elif isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node, deep=True)
    else:
        value = None
    return _CfnTag(tag, value)


class CfnLoader(yaml.SafeLoader):
    pass


yaml.add_multi_constructor("!", _cfn_constructor, Loader=CfnLoader)

_TEMPLATE_PATH = Path(__file__).parent.parent / "deploy" / "template.yaml"


def _get_zip_code(resource_name: str) -> str:
    with open(_TEMPLATE_PATH) as fh:
        t = yaml.load(fh, Loader=CfnLoader)
    code = t["Resources"][resource_name]["Properties"]["Code"]["ZipFile"]
    assert isinstance(code, str), f"{resource_name}.Code.ZipFile is not a string"
    return code


# ---------------------------------------------------------------------------
# Execution helper
# ---------------------------------------------------------------------------


def _run(
    event: dict,
    *,
    iam_client: MagicMock | None = None,
    s3_client: MagicMock | None = None,
) -> MagicMock:
    """Execute the inline code and call handler(event).

    Returns the cfnresponse mock so callers can assert on send() calls. The
    handler re-raises on failure after signalling FAILED, so callers that
    exercise an error path must wrap this in pytest.raises.
    """
    cfnresponse_mock = MagicMock()
    cfnresponse_mock.SUCCESS = "SUCCESS"
    cfnresponse_mock.FAILED = "FAILED"

    client_map: dict = {}
    if iam_client is not None:
        client_map["iam"] = iam_client
    if s3_client is not None:
        client_map["s3"] = s3_client

    def make_client(service, **kwargs):
        return client_map.get(service, MagicMock())

    code = _get_zip_code("ReplicationRolePassGranterFunction")
    context = MagicMock()
    with patch.dict(sys.modules, {"cfnresponse": cfnresponse_mock}):
        with patch("boto3.client", side_effect=make_client):
            ns: dict = {}
            exec(compile(code, "<ReplicationRolePassGranterFunction>", "exec"), ns)
            ns["handler"](event, context)
    return cfnresponse_mock


def _s3_with_roles(roles_by_bucket: dict[str, str]) -> MagicMock:
    """An s3 client whose get_bucket_replication returns the given Role per
    bucket. A bucket absent from the mapping raises, simulating a bucket with
    no replication configuration."""
    s3 = MagicMock()

    def get_bucket_replication(Bucket):
        if Bucket not in roles_by_bucket:
            raise Exception("ReplicationConfigurationNotFoundError")
        return {"ReplicationConfiguration": {"Role": roles_by_bucket[Bucket]}}

    s3.get_bucket_replication.side_effect = get_bucket_replication
    return s3


def _event(buckets: list[str], account_id: str = _ACCOUNT_ID) -> dict:
    return {
        "RequestType": "Create",
        "ResourceProperties": {
            "ExecutionRoleName": _EXECUTION_ROLE_NAME,
            "SourceBucketNames": buckets,
            "AccountId": account_id,
        },
    }


def _policy_resources(iam_client: MagicMock) -> list[str]:
    """The Resource list from the policy document put_role_policy received."""
    doc = json.loads(iam_client.put_role_policy.call_args.kwargs["PolicyDocument"])
    return doc["Statement"][0]["Resource"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestValidRoleArns:
    def test_same_account_role_is_granted(self):
        arn = f"arn:aws:iam::{_ACCOUNT_ID}:role/replication-role"
        iam = MagicMock()
        resp = _run(
            _event(["bucket-a"]),
            iam_client=iam,
            s3_client=_s3_with_roles({"bucket-a": arn}),
        )
        assert _policy_resources(iam) == [arn]
        assert "SUCCESS" in resp.send.call_args.args

    def test_multiple_buckets_deduplicate_shared_role(self):
        arn = f"arn:aws:iam::{_ACCOUNT_ID}:role/shared-role"
        iam = MagicMock()
        _run(
            _event(["bucket-a", "bucket-b"]),
            iam_client=iam,
            s3_client=_s3_with_roles({"bucket-a": arn, "bucket-b": arn}),
        )
        assert _policy_resources(iam) == [arn]

    @pytest.mark.parametrize("partition", ["aws", "aws-cn", "aws-us-gov"])
    def test_all_partitions_accepted(self, partition):
        arn = f"arn:{partition}:iam::{_ACCOUNT_ID}:role/replication-role"
        iam = MagicMock()
        _run(
            _event(["bucket-a"]),
            iam_client=iam,
            s3_client=_s3_with_roles({"bucket-a": arn}),
        )
        assert _policy_resources(iam) == [arn]

    def test_role_with_path_accepted(self):
        arn = f"arn:aws:iam::{_ACCOUNT_ID}:role/service-role/replication-role"
        iam = MagicMock()
        _run(
            _event(["bucket-a"]),
            iam_client=iam,
            s3_client=_s3_with_roles({"bucket-a": arn}),
        )
        assert _policy_resources(iam) == [arn]


class TestRejectedRoleArns:
    """A role ARN that is malformed or belongs to another account must never
    reach the policy document."""

    @pytest.mark.parametrize(
        "bad_arn",
        [
            f"arn:aws:iam::{_OTHER_ACCOUNT_ID}:role/attacker-role",  # cross-account
            "arn:aws:iam::123:role/short-account",                   # malformed account
            f"arn:aws:iam::{_ACCOUNT_ID}:user/not-a-role",           # not a role
            f"arn:aws:sts::{_ACCOUNT_ID}:assumed-role/x/y",          # wrong service
            f"arn:aws:iam::{_ACCOUNT_ID}:role/",                     # empty role name
            "not-an-arn-at-all",
            "*",
        ],
    )
    def test_invalid_arn_fails_the_custom_resource(self, bad_arn):
        iam = MagicMock()
        with pytest.raises(Exception):
            _run(
                _event(["bucket-a"]),
                iam_client=iam,
                s3_client=_s3_with_roles({"bucket-a": bad_arn}),
            )
        iam.put_role_policy.assert_not_called()

    def test_one_bad_arn_blocks_the_whole_grant(self):
        """A valid ARN alongside an invalid one must not be granted on its
        own — the stack fails so the operator fixes the bad configuration."""
        good = f"arn:aws:iam::{_ACCOUNT_ID}:role/good-role"
        bad = f"arn:aws:iam::{_OTHER_ACCOUNT_ID}:role/attacker-role"
        iam = MagicMock()
        with pytest.raises(Exception):
            _run(
                _event(["bucket-a", "bucket-b"]),
                iam_client=iam,
                s3_client=_s3_with_roles({"bucket-a": good, "bucket-b": bad}),
            )
        iam.put_role_policy.assert_not_called()

    def test_failure_is_signalled_to_cloudformation(self):
        bad = f"arn:aws:iam::{_OTHER_ACCOUNT_ID}:role/attacker-role"
        cfnresponse_mock = MagicMock()
        cfnresponse_mock.SUCCESS = "SUCCESS"
        cfnresponse_mock.FAILED = "FAILED"

        def make_client(service, **kwargs):
            if service == "s3":
                return _s3_with_roles({"bucket-a": bad})
            return MagicMock()

        code = _get_zip_code("ReplicationRolePassGranterFunction")
        with patch.dict(sys.modules, {"cfnresponse": cfnresponse_mock}):
            with patch("boto3.client", side_effect=make_client):
                ns: dict = {}
                exec(compile(code, "<granter>", "exec"), ns)
                with pytest.raises(Exception):
                    ns["handler"](_event(["bucket-a"]), MagicMock())

        assert cfnresponse_mock.send.called
        assert "FAILED" in cfnresponse_mock.send.call_args.args


class TestMissingReplicationConfiguration:
    def test_bucket_without_replication_config_still_fails(self):
        """Pre-existing behaviour must be preserved: a bucket with no
        replication configuration fails the stack."""
        iam = MagicMock()
        with pytest.raises(Exception):
            _run(
                _event(["bucket-a"]),
                iam_client=iam,
                s3_client=_s3_with_roles({}),
            )
        iam.put_role_policy.assert_not_called()


class TestDeleteRequest:
    def test_delete_removes_the_inline_policy(self):
        iam = MagicMock()
        event = {
            "RequestType": "Delete",
            "ResourceProperties": {
                "ExecutionRoleName": _EXECUTION_ROLE_NAME,
                "SourceBucketNames": ["bucket-a"],
                "AccountId": _ACCOUNT_ID,
            },
        }
        _run(event, iam_client=iam, s3_client=MagicMock())
        iam.delete_role_policy.assert_called_once()
        assert iam.delete_role_policy.call_args.kwargs["RoleName"] == _EXECUTION_ROLE_NAME
