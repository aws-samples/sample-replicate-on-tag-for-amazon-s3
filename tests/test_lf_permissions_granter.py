"""Unit tests for the inline Lambda logic in LFPermissionsGranterFunction
and LFAdminGranterFunction (ZipFile code embedded in deploy/template.yaml).

Both functions are extracted from the template at test time and executed via
exec() with patched boto3 and cfnresponse so no real AWS calls are made.

Feature: lf-mode-s3-metadata
Requirements: 1.1, 1.2, 1.3, 2.1, 2.3, 2.4, 3.2, 3.3, 4.1, 4.2
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

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
    """Extract ZipFile inline code from a Lambda resource in the template."""
    with open(_TEMPLATE_PATH) as fh:
        t = yaml.load(fh, Loader=CfnLoader)
    code = t["Resources"][resource_name]["Properties"]["Code"]["ZipFile"]
    assert isinstance(code, str), f"{resource_name}.Code.ZipFile is not a string"
    return code


# ---------------------------------------------------------------------------
# cfnresponse mock factory
# ---------------------------------------------------------------------------


def _make_cfnresponse() -> MagicMock:
    mock = MagicMock()
    mock.SUCCESS = "SUCCESS"
    mock.FAILED = "FAILED"
    return mock


# ---------------------------------------------------------------------------
# Execution helpers — exec() + handler() inside the same patch context
# ---------------------------------------------------------------------------


ADMIN_PRINCIPAL_ARN = "arn:aws:iam::123456789012:role/granter"
ADMIN_STACK_ID = (
    "arn:aws:cloudformation:us-west-2:123456789012:stack/s3rot/11111111-2222-3333-4444-555555555555"
)

# LFPermissionsGranterFunction's privileged parameters come from its
# environment, so the template's Environment block is simulated here
# (scan-aa27a832 Req 1.3).
GRANTS_EXECUTION_ROLE_ARN = "arn:aws:iam::123456789012:role/exec-role"
GRANTS_ACCOUNT_ID = "123456789012"


def _run_lf_permissions_granter(
    event: dict,
    *,
    glue_client: MagicMock | None = None,
    lf_client: MagicMock | None = None,
    sts_client: MagicMock | None = None,
    buckets: str = "bucket-a",
    lf_admin_role_arn: str = "",
    env: dict[str, str] | None = None,
) -> MagicMock:
    """Execute LFPermissionsGranterFunction inline code and call handler(event).

    ``buckets`` and ``lf_admin_role_arn`` populate the simulated environment,
    since the handler reads them from there rather than from the event. Pass
    ``env`` to override any variable outright.

    Returns the cfnresponse mock so callers can assert on send() calls.
    """
    cfnresponse_mock = _make_cfnresponse()
    client_map: dict = {}
    if glue_client is not None:
        client_map["glue"] = glue_client
    if lf_client is not None:
        client_map["lakeformation"] = lf_client
    if sts_client is not None:
        client_map["sts"] = sts_client

    def make_client(service, **kwargs):
        return client_map.get(service, MagicMock())

    handler_env = {
        "PRINCIPAL_ARN": GRANTS_EXECUTION_ROLE_ARN,
        "SOURCE_BUCKET_NAMES": buckets,
        "LF_ADMIN_ROLE_ARN": lf_admin_role_arn,
        "ACCOUNT_ID": GRANTS_ACCOUNT_ID,
        "STACK_ID": ADMIN_STACK_ID,
    }
    if env is not None:
        handler_env.update(env)

    code = _get_zip_code("LFPermissionsGranterFunction")
    context = MagicMock()
    with patch.dict(os.environ, handler_env):
        with patch.dict(sys.modules, {"cfnresponse": cfnresponse_mock}):
            with patch("boto3.client", side_effect=make_client):
                with patch("time.sleep"):  # suppress real sleeps in detect_mode retries
                    ns: dict = {}
                    exec(compile(code, "<LFPermissionsGranterFunction>", "exec"), ns)
                    ns["handler"](event, context)
    return cfnresponse_mock


def _run_lf_admin_granter(
    event: dict,
    *,
    lf_client: MagicMock | None = None,
    glue_client: MagicMock | None = None,
    env: dict[str, str] | None = None,
) -> tuple[MagicMock, MagicMock]:
    """Execute LFAdminGranterFunction inline code and call handler(event).

    The privileged parameters come from the function's environment, so the
    template's environment block is simulated here. Pass ``env`` to override.

    ``glue_client`` drives the catalog mode check and defaults to LF mode, the
    mode in which admin registration is expected to happen.

    Returns (cfnresponse_mock, lf_client) so callers can assert on both.
    """
    cfnresponse_mock = _make_cfnresponse()
    _lf = lf_client if lf_client is not None else MagicMock()
    _glue = glue_client if glue_client is not None else _lf_mode_glue()

    def make_client(service, **kwargs):
        if service == "lakeformation":
            return _lf
        if service == "glue":
            return _glue
        return MagicMock()

    handler_env = {
        "PRINCIPAL_ARN": ADMIN_PRINCIPAL_ARN,
        "STACK_ID": ADMIN_STACK_ID,
        "ACCOUNT_ID": "123456789012",
    }
    if env is not None:
        handler_env.update(env)

    code = _get_zip_code("LFAdminGranterFunction")
    context = MagicMock()
    with patch.dict(os.environ, handler_env):
        with patch.dict(sys.modules, {"cfnresponse": cfnresponse_mock}):
            with patch("boto3.client", side_effect=make_client):
                with patch("time.sleep"):  # suppress real sleeps in retry tests
                    ns: dict = {}
                    exec(compile(code, "<LFAdminGranterFunction>", "exec"), ns)
                    ns["handler"](event, context)
    return cfnresponse_mock, _lf


# ---------------------------------------------------------------------------
# Event builders
# ---------------------------------------------------------------------------


def _create_event(
    buckets: str = "bucket-a",
    lf_admin_role_arn: str = "",
    physical_id: str = "lf-grants",
) -> dict:
    # The properties below are change-detection triggers in the template; the
    # handler reads the same values from its environment. They are kept on the
    # event because CloudFormation still sends them.
    return {
        "RequestType": "Create",
        "StackId": ADMIN_STACK_ID,
        "PhysicalResourceId": physical_id,
        "ResourceProperties": {
            "AccountId": "123456789012",
            "ExecutionRoleArn": "arn:aws:iam::123456789012:role/exec-role",
            "SourceBucketNames": buckets,
            "LFAdminRoleArn": lf_admin_role_arn,
        },
    }


def _delete_event(physical_id: str, lf_admin_role_arn: str = "") -> dict:
    return {
        "RequestType": "Delete",
        "StackId": ADMIN_STACK_ID,
        "PhysicalResourceId": physical_id,
        "ResourceProperties": {
            "AccountId": "123456789012",
            "ExecutionRoleArn": "arn:aws:iam::123456789012:role/exec-role",
            "SourceBucketNames": "bucket-a",
            "LFAdminRoleArn": lf_admin_role_arn,
        },
    }


def _admin_create_event(principal_arn: str = ADMIN_PRINCIPAL_ARN) -> dict:
    return {
        "RequestType": "Create",
        "StackId": ADMIN_STACK_ID,
        "ResourceProperties": {"PrincipalArn": principal_arn},
    }


def _admin_delete_event(principal_arn: str = ADMIN_PRINCIPAL_ARN) -> dict:
    return {
        "RequestType": "Delete",
        "StackId": ADMIN_STACK_ID,
        "ResourceProperties": {"PrincipalArn": principal_arn},
    }


# ---------------------------------------------------------------------------
# Glue mock factories
# ---------------------------------------------------------------------------


def _iam_mode_glue() -> MagicMock:
    """Glue mock that returns IAM_ALLOWED_PRINCIPALS in CreateTableDefaultPermissions."""
    m = MagicMock()
    m.get_catalog.return_value = {
        "Catalog": {
            "CreateTableDefaultPermissions": [
                {
                    "Principal": {"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"},
                    "Permissions": ["ALL"],
                }
            ]
        }
    }
    return m


def _lf_mode_glue() -> MagicMock:
    """Glue mock that returns a non-IAM principal — LF mode."""
    m = MagicMock()
    m.get_catalog.return_value = {
        "Catalog": {
            "CreateTableDefaultPermissions": [
                {
                    "Principal": {
                        "DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/admin"
                    },
                    "Permissions": ["ALL"],
                }
            ]
        }
    }
    return m


# ---------------------------------------------------------------------------
# Tests: mode detection — detect_mode (Req 1.1, 1.2, 1.3)
# ---------------------------------------------------------------------------


class TestDetectMode:
    def test_iam_mode_when_iam_allowed_principals_present(self):
        """Returns IAM mode → no grant_permissions when IAM_ALLOWED_PRINCIPALS present (Req 1.2)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_iam_mode_glue(), lf_client=lf_mock
        )
        lf_mock.grant_permissions.assert_not_called()

    def test_lf_mode_when_iam_allowed_principals_absent(self):
        """Returns LF mode → grant_permissions called when IAM_ALLOWED_PRINCIPALS absent (Req 1.2)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_lf_mode_glue(), lf_client=lf_mock
        )
        assert lf_mock.grant_permissions.call_count > 0

    def test_persistent_get_catalog_error_fails_without_granting(self):
        """Never assumes a mode: a persistent GetCatalog error fails the resource (Req 1.3)."""
        glue_mock = MagicMock()
        glue_mock.get_catalog.side_effect = Exception("AccessDenied")
        lf_mock = MagicMock()
        cfnr = _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=glue_mock, lf_client=lf_mock
        )
        lf_mock.grant_permissions.assert_not_called()
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "FAILED"
        assert "AccessDenied" in cfnr.send.call_args[0][3]["Error"]

    def test_get_catalog_retried_before_failing(self):
        """GetCatalog is retried to absorb IAM propagation delay (Req 1.3)."""
        glue_mock = MagicMock()
        glue_mock.get_catalog.side_effect = Exception("AccessDenied")
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=glue_mock, lf_client=MagicMock()
        )
        assert glue_mock.get_catalog.call_count == 5

    def test_transient_get_catalog_error_then_success(self):
        """A transient GetCatalog failure resolves on retry rather than failing (Req 1.3)."""
        glue_mock = MagicMock()
        glue_mock.get_catalog.side_effect = [
            Exception("AccessDenied"),
            _iam_mode_glue().get_catalog.return_value,
        ]
        lf_mock = MagicMock()
        cfnr = _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=glue_mock, lf_client=lf_mock
        )
        # Second attempt reports IAM mode → no grants, resource succeeds
        lf_mock.grant_permissions.assert_not_called()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_mode_check_uses_assumed_admin_credentials(self):
        """The Glue client for the mode check is built from the LF admin session (Req 1.4)."""
        sts_mock = MagicMock()
        sts_mock.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIAEXAMPLE",
                "SecretAccessKey": "secret",  # noqa: S106
                "SessionToken": "token",
            }
        }
        captured: list[dict] = []

        def make_client(service, **kwargs):
            if service == "sts":
                return sts_mock
            if service == "glue":
                captured.append(kwargs)
                return _iam_mode_glue()
            return MagicMock()

        code = _get_zip_code("LFPermissionsGranterFunction")
        handler_env = {
            "PRINCIPAL_ARN": GRANTS_EXECUTION_ROLE_ARN,
            "SOURCE_BUCKET_NAMES": "bucket-a",
            "LF_ADMIN_ROLE_ARN": "arn:aws:iam::123456789012:role/lf-admin",
            "ACCOUNT_ID": GRANTS_ACCOUNT_ID,
            "STACK_ID": ADMIN_STACK_ID,
        }
        with patch.dict(os.environ, handler_env):
            with patch.dict(sys.modules, {"cfnresponse": _make_cfnresponse()}):
                with patch("boto3.client", side_effect=make_client):
                    ns: dict = {}
                    exec(compile(code, "<LFPermissionsGranterFunction>", "exec"), ns)
                    ns["handler"](
                        _create_event(
                            "bucket-a",
                            lf_admin_role_arn="arn:aws:iam::123456789012:role/lf-admin",
                        ),
                        MagicMock(),
                    )

        assert captured, "glue client was never constructed"
        assert captured[0]["aws_access_key_id"] == "AKIAEXAMPLE"
        assert captured[0]["aws_session_token"] == "token"

    def test_empty_create_table_default_permissions_is_lf_mode(self):
        """Empty CreateTableDefaultPermissions list is treated as LF mode (Req 1.2)."""
        glue_mock = MagicMock()
        glue_mock.get_catalog.return_value = {
            "Catalog": {"CreateTableDefaultPermissions": []}
        }
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=glue_mock, lf_client=lf_mock
        )
        assert lf_mock.grant_permissions.call_count > 0


# ---------------------------------------------------------------------------
# Tests: LF mode grant construction (Req 2.1, 2.2, 2.3)
# ---------------------------------------------------------------------------


class TestLFPermissionsGranter:
    def test_iam_mode_no_grant_calls(self):
        """In IAM mode, grant_permissions is never called (Req 2.3)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_iam_mode_glue(), lf_client=lf_mock
        )
        lf_mock.grant_permissions.assert_not_called()

    def test_iam_mode_cfnresponse_success(self):
        """In IAM mode, cfnresponse.send called with SUCCESS (Req 2.3)."""
        cfnr = _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_iam_mode_glue(), lf_client=MagicMock()
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_lf_mode_grants_twice_per_bucket(self):
        """In LF mode, grant_permissions called exactly twice per bucket (Req 2.1)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_lf_mode_glue(), lf_client=lf_mock
        )
        assert lf_mock.grant_permissions.call_count == 2

    def test_lf_mode_correct_catalog_id(self):
        """Both grants use catalog ID '<AccountId>:s3tablescatalog/aws-s3' (Req 2.1)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_lf_mode_glue(), lf_client=lf_mock
        )
        expected_catalog = "123456789012:s3tablescatalog/aws-s3"
        for c in lf_mock.grant_permissions.call_args_list:
            resource = c.kwargs.get("Resource", {})
            for key in ("Database", "Table"):
                if key in resource:
                    assert resource[key].get("CatalogId") == expected_catalog, (
                        f"CatalogId mismatch in {key} grant: {resource[key]}"
                    )

    def test_lf_mode_grants_correct_namespace(self):
        """Database grant uses namespace 'b_bucket-a' for bucket 'bucket-a' (Req 2.1)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_lf_mode_glue(), lf_client=lf_mock
        )
        db_names = [
            c.kwargs["Resource"]["Database"]["Name"]
            for c in lf_mock.grant_permissions.call_args_list
            if "Database" in c.kwargs.get("Resource", {})
        ]
        assert "b_bucket-a" in db_names

    def test_lf_mode_table_wildcard_grant(self):
        """Table grant uses TableWildcard with SELECT and DESCRIBE (Req 2.1)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_lf_mode_glue(), lf_client=lf_mock
        )
        table_calls = [
            c for c in lf_mock.grant_permissions.call_args_list
            if "Table" in c.kwargs.get("Resource", {})
        ]
        assert table_calls, "No Table grant found"
        table_call = table_calls[0]
        assert "TableWildcard" in table_call.kwargs["Resource"]["Table"]
        perms = table_call.kwargs.get("Permissions", [])
        assert "SELECT" in perms
        assert "DESCRIBE" in perms

    def test_multiple_buckets_namespace_derivation(self):
        """Dots replaced with underscores in namespace; both buckets granted (Req 2.2)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a,bucket.b"),
            glue_client=_lf_mode_glue(),
            lf_client=lf_mock,
            buckets="bucket-a,bucket.b",
        )
        assert lf_mock.grant_permissions.call_count == 4  # 2 grants × 2 buckets
        db_names = {
            c.kwargs["Resource"]["Database"]["Name"]
            for c in lf_mock.grant_permissions.call_args_list
            if "Database" in c.kwargs.get("Resource", {})
        }
        assert "b_bucket-a" in db_names, "Namespace b_bucket-a not found"
        assert "b_bucket_b" in db_names, "Namespace b_bucket_b (dot→underscore) not found"

    def test_lf_mode_grants_to_execution_role(self):
        """Grant Principal is the ExecutionRoleArn (Req 2.1)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_lf_mode_glue(), lf_client=lf_mock
        )
        principals = {
            c.kwargs.get("Principal", {}).get("DataLakePrincipalIdentifier")
            for c in lf_mock.grant_permissions.call_args_list
        }
        assert "arn:aws:iam::123456789012:role/exec-role" in principals

    def test_lf_mode_cfnresponse_success(self):
        """cfnresponse.send called with SUCCESS after grants (Req 2.1)."""
        cfnr = _run_lf_permissions_granter(
            _create_event("bucket-a"), glue_client=_lf_mode_glue(), lf_client=MagicMock()
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_whitespace_stripped_from_bucket_names(self):
        """Leading/trailing whitespace is stripped from bucket names (defensive)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _create_event(" bucket-a , bucket-b "),
            glue_client=_lf_mode_glue(),
            lf_client=lf_mock,
            buckets=" bucket-a , bucket-b ",
        )
        db_names = {
            c.kwargs["Resource"]["Database"]["Name"]
            for c in lf_mock.grant_permissions.call_args_list
            if "Database" in c.kwargs.get("Resource", {})
        }
        assert "b_bucket-a" in db_names
        assert "b_bucket-b" in db_names


# ---------------------------------------------------------------------------
# Tests: Delete / revocation (Req 4.1, 4.2)
# ---------------------------------------------------------------------------


class TestDeleteRevocation:
    def _grants_physical_id(self) -> str:
        return json.dumps({
            "grants": [
                {
                    "principal": "arn:aws:iam::123456789012:role/exec-role",
                    "catalog_id": "123456789012:s3tablescatalog/aws-s3",
                    "namespace": "b_bucket-a",
                }
            ]
        })

    def test_delete_calls_revoke_for_each_grant(self):
        """Delete event calls revoke_permissions for each recorded grant (Req 4.1)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _delete_event(self._grants_physical_id()), lf_client=lf_mock
        )
        # Two revoke calls per grant: database + table wildcard
        assert lf_mock.revoke_permissions.call_count == 2

    def test_delete_cfnresponse_success(self):
        """Delete returns SUCCESS even after revocations (Req 4.1, 4.2)."""
        cfnr = _run_lf_permissions_granter(
            _delete_event(self._grants_physical_id()), lf_client=MagicMock()
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_delete_swallows_revocation_exceptions(self):
        """Revocation failures are swallowed — stack deletion continues (Req 4.2)."""
        lf_mock = MagicMock()
        lf_mock.revoke_permissions.side_effect = Exception("role already deleted")
        cfnr = _run_lf_permissions_granter(
            _delete_event(self._grants_physical_id()), lf_client=lf_mock
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_delete_invalid_physical_id_does_not_crash(self):
        """Delete with non-JSON physicalId (e.g. first-run) is handled gracefully (Req 4.2)."""
        cfnr = _run_lf_permissions_granter(
            _delete_event("lf-grants"), lf_client=MagicMock()
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_delete_revokes_the_environment_grants(self):
        """The revocation list is rebuilt from the environment, so it names the
        same grants Create issued (diff scan scan-f1927e8a, f-756c9994)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _delete_event("lf-grants"), lf_client=lf_mock, buckets="bucket-a,bucket-b"
        )
        principals = {
            c[1]["Principal"]["DataLakePrincipalIdentifier"]
            for c in lf_mock.revoke_permissions.call_args_list
        }
        assert principals == {GRANTS_EXECUTION_ROLE_ARN}
        databases = {
            c[1]["Resource"].get("Database", {}).get("Name")
            for c in lf_mock.revoke_permissions.call_args_list
        } - {None}
        assert databases == {"b_bucket-a", "b_bucket-b"}

    def test_forged_physical_id_never_reaches_revoke_permissions(self):
        """A Delete naming a foreign principal, catalog and namespace revokes
        neither (diff scan scan-f1927e8a, f-756c9994)."""
        forged = json.dumps({
            "grants": [
                {
                    "principal": "arn:aws:iam::123456789012:role/someone-elses-role",
                    "catalog_id": "123456789012:s3tablescatalog/other",
                    "namespace": "b_someone-elses-namespace",
                }
            ]
        })
        lf_mock = MagicMock()
        _run_lf_permissions_granter(_delete_event(forged), lf_client=lf_mock)
        serialized = str(lf_mock.revoke_permissions.call_args_list)
        assert "someone-elses-role" not in serialized
        assert "someone-elses-namespace" not in serialized
        assert "s3tablescatalog/other" not in serialized
        assert lf_mock.revoke_permissions.call_count == 2


# ---------------------------------------------------------------------------
# Tests: External LF admin (Req 3.2)
# ---------------------------------------------------------------------------


class TestExternalLFAdmin:
    def test_sts_assume_role_called_with_lf_admin_role_arn(self):
        """sts:AssumeRole is called with LFAdminRoleArn when it is provided (Req 3.2)."""
        glue_mock = _lf_mode_glue()
        lf_mock = MagicMock()
        sts_mock = MagicMock()
        sts_mock.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIA...",
                "SecretAccessKey": "secret",
                "SessionToken": "token",
            }
        }
        cfnresponse_mock = _make_cfnresponse()
        client_map = {
            "glue": glue_mock,
            "sts": sts_mock,
            "lakeformation": lf_mock,
        }

        def make_client(service, **kwargs):
            return client_map.get(service, MagicMock())

        code = _get_zip_code("LFPermissionsGranterFunction")
        event = _create_event(
            "bucket-a", lf_admin_role_arn="arn:aws:iam::123456789012:role/lf-admin"
        )
        context = MagicMock()
        handler_env = {
            "PRINCIPAL_ARN": GRANTS_EXECUTION_ROLE_ARN,
            "SOURCE_BUCKET_NAMES": "bucket-a",
            "LF_ADMIN_ROLE_ARN": "arn:aws:iam::123456789012:role/lf-admin",
            "ACCOUNT_ID": GRANTS_ACCOUNT_ID,
            "STACK_ID": ADMIN_STACK_ID,
        }
        with patch.dict(os.environ, handler_env):
            with patch.dict(sys.modules, {"cfnresponse": cfnresponse_mock}):
                with patch("boto3.client", side_effect=make_client):
                    ns: dict = {}
                    exec(compile(code, "<test_external_admin>", "exec"), ns)
                    ns["handler"](event, context)

        sts_mock.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/lf-admin",
            RoleSessionName="LFGranter",
        )


# ---------------------------------------------------------------------------
# Tests: LFAdminGranter (Req 3.3, 4.2)
# ---------------------------------------------------------------------------


def _make_lf_admin_mock(existing_admins: list | None = None) -> MagicMock:
    """Create an LF client mock with get_data_lake_settings pre-configured."""
    m = MagicMock()
    m.get_data_lake_settings.return_value = {
        "DataLakeSettings": {"DataLakeAdmins": existing_admins or []}
    }
    # Must define exception class so put_with_retry can catch it
    m.exceptions.ConcurrentModificationException = type(
        "ConcurrentModificationException", (Exception,), {}
    )
    return m


class TestLFAdminGranter:
    def test_create_appends_principal_to_lf_admins(self):
        """Create event appends PrincipalArn to the LF admins list (Req 3.3)."""
        lf_mock = _make_lf_admin_mock()
        cfnr, _ = _run_lf_admin_granter(_admin_create_event(), lf_client=lf_mock)
        lf_mock.put_data_lake_settings.assert_called_once()
        put_kwargs = lf_mock.put_data_lake_settings.call_args[1]
        admins = put_kwargs["DataLakeSettings"]["DataLakeAdmins"]
        assert any(
            a.get("DataLakePrincipalIdentifier") == "arn:aws:iam::123456789012:role/granter"
            for a in admins
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_create_does_not_duplicate_existing_admin(self):
        """Create is idempotent: existing principal not duplicated (Req 3.3)."""
        existing = [{"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/granter"}]
        lf_mock = _make_lf_admin_mock(existing_admins=existing)
        _run_lf_admin_granter(_admin_create_event(), lf_client=lf_mock)
        put_kwargs = lf_mock.put_data_lake_settings.call_args[1]
        admins = put_kwargs["DataLakeSettings"]["DataLakeAdmins"]
        matches = [
            a for a in admins
            if a.get("DataLakePrincipalIdentifier") == "arn:aws:iam::123456789012:role/granter"
        ]
        assert len(matches) == 1, f"Expected exactly 1 entry; got {len(matches)}"

    def test_delete_removes_principal_from_lf_admins(self):
        """Delete event removes PrincipalArn from the LF admins list (Req 3.4)."""
        existing = [{"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/granter"}]
        lf_mock = _make_lf_admin_mock(existing_admins=existing)
        cfnr, _ = _run_lf_admin_granter(_admin_delete_event(), lf_client=lf_mock)
        put_kwargs = lf_mock.put_data_lake_settings.call_args[1]
        remaining = put_kwargs["DataLakeSettings"]["DataLakeAdmins"]
        assert not any(
            a.get("DataLakePrincipalIdentifier") == "arn:aws:iam::123456789012:role/granter"
            for a in remaining
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_delete_logs_removal_failure_at_error_and_still_succeeds(self, caplog):
        """A failed removal is logged at ERROR, naming the principal left behind,
        and the resource still responds SUCCESS.

        scan-aa27a832 remediation, Req 2.2: the failure must not be silently
        swallowed, so an account-wide admin entry cannot outlive the stack
        unnoticed. Responding SUCCESS is deliberate -- FAILED would wedge the
        whole stack in DELETE_FAILED. See the comment at the emit site in
        deploy/template.yaml.
        """
        lf_mock = _make_lf_admin_mock()
        lf_mock.get_data_lake_settings.side_effect = Exception("permission denied")
        with caplog.at_level(logging.ERROR):
            cfnr, _ = _run_lf_admin_granter(_admin_delete_event(), lf_client=lf_mock)

        errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
        assert errors, "Removal failure must be logged at ERROR, not WARNING"
        messages = [r.getMessage() for r in errors]
        assert any(ADMIN_PRINCIPAL_ARN in m for m in messages), (
            f"ERROR log must name the principal left behind; got {messages}"
        )
        assert any("permission denied" in m for m in messages), (
            f"ERROR log must carry the underlying cause; got {messages}"
        )

        # Stack deletion is not blocked.
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_retry_on_concurrent_modification_exception(self):
        """put_data_lake_settings is retried on ConcurrentModificationException (Req 3.3)."""
        lf_mock = _make_lf_admin_mock()
        ConcurrentModEx = lf_mock.exceptions.ConcurrentModificationException

        # Fail the first 2 attempts, succeed on the 3rd
        call_count = {"n": 0}

        def flaky_put(**kwargs):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ConcurrentModEx("concurrent modification")

        lf_mock.put_data_lake_settings.side_effect = flaky_put

        cfnr, _ = _run_lf_admin_granter(_admin_create_event(), lf_client=lf_mock)

        assert call_count["n"] == 3, (
            f"Expected 3 put_data_lake_settings attempts (2 retries + 1 success); "
            f"got {call_count['n']}"
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_cfnresponse_success_returned_on_create(self):
        """cfnresponse.send called with SUCCESS for Create (Req 3.3)."""
        lf_mock = _make_lf_admin_mock()
        cfnr, _ = _run_lf_admin_granter(_admin_create_event(), lf_client=lf_mock)
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"
        # PrincipalArn attribute returned in data dict
        data = cfnr.send.call_args[0][3]
        assert data.get("PrincipalArn") == ADMIN_PRINCIPAL_ARN


class TestLFAdminGranterPrincipalSource:
    """The principal comes from the environment, not the event.

    scan-aa27a832 remediation, Req 1.1: an in-account caller holding
    lambda:InvokeFunction on this function must not be able to name the
    principal that becomes an account-wide Lake Formation administrator.
    """

    FOREIGN = "arn:aws:iam::123456789012:role/attacker"

    def test_event_principal_arn_never_reaches_put_data_lake_settings(self):
        """A foreign PrincipalArn in the event is ignored (Req 1.1)."""
        lf_mock = _make_lf_admin_mock()
        _run_lf_admin_granter(
            _admin_create_event(principal_arn=self.FOREIGN), lf_client=lf_mock
        )
        lf_mock.put_data_lake_settings.assert_called_once()
        admins = lf_mock.put_data_lake_settings.call_args[1]["DataLakeSettings"][
            "DataLakeAdmins"
        ]
        identifiers = [a.get("DataLakePrincipalIdentifier") for a in admins]
        assert self.FOREIGN not in identifiers
        assert identifiers == [ADMIN_PRINCIPAL_ARN]

    def test_delete_removes_the_environment_principal_not_the_event_one(self):
        """Delete revokes the template's principal, ignoring the event (Req 1.1)."""
        existing = [
            {"DataLakePrincipalIdentifier": ADMIN_PRINCIPAL_ARN},
            {"DataLakePrincipalIdentifier": self.FOREIGN},
        ]
        lf_mock = _make_lf_admin_mock(existing_admins=existing)
        _run_lf_admin_granter(
            _admin_delete_event(principal_arn=self.FOREIGN), lf_client=lf_mock
        )
        remaining = lf_mock.put_data_lake_settings.call_args[1]["DataLakeSettings"][
            "DataLakeAdmins"
        ]
        identifiers = [a.get("DataLakePrincipalIdentifier") for a in remaining]
        assert identifiers == [self.FOREIGN]

    def test_template_sets_both_privileged_environment_variables(self):
        """The template supplies PRINCIPAL_ARN and STACK_ID (Req 1.1)."""
        with open(_TEMPLATE_PATH) as fh:
            t = yaml.load(fh, Loader=CfnLoader)
        env = t["Resources"]["LFAdminGranterFunction"]["Properties"]["Environment"][
            "Variables"
        ]
        assert env["PRINCIPAL_ARN"].tag == "!GetAtt"
        assert env["PRINCIPAL_ARN"].value == "LFGranterRole.Arn"
        assert env["STACK_ID"].tag == "!Ref"
        assert env["STACK_ID"].value == "AWS::StackId"


class TestLFAdminGranterStackIdCheck:
    """An event from anything other than this stack is rejected.

    scan-aa27a832 remediation, Req 1.2: the handler compares the event's
    StackId with the STACK_ID environment variable before any mutation.
    """

    FOREIGN_STACK_ID = (
        "arn:aws:cloudformation:us-west-2:123456789012:stack/other/"
        "99999999-8888-7777-6666-555555555555"
    )

    def _mismatched_event(self, request_type: str = "Create") -> dict:
        event = (
            _admin_create_event() if request_type == "Create" else _admin_delete_event()
        )
        event["StackId"] = self.FOREIGN_STACK_ID
        return event

    def test_mismatched_stack_id_performs_no_mutation(self):
        """A foreign StackId reaches neither read nor write of LF settings (Req 1.2)."""
        lf_mock = _make_lf_admin_mock()
        _run_lf_admin_granter(self._mismatched_event(), lf_client=lf_mock)
        lf_mock.put_data_lake_settings.assert_not_called()
        lf_mock.get_data_lake_settings.assert_not_called()

    def test_mismatched_stack_id_responds_failed_without_specifics(self):
        """The FAILED reason names neither the expected nor the supplied value (Req 1.2)."""
        cfnr, _ = _run_lf_admin_granter(
            self._mismatched_event(), lf_client=_make_lf_admin_mock()
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "FAILED"
        reason = cfnr.send.call_args[0][3]["Error"]
        assert ADMIN_STACK_ID not in reason
        assert self.FOREIGN_STACK_ID not in reason
        assert ADMIN_PRINCIPAL_ARN not in reason
        assert "StackId" not in reason

    def test_missing_stack_id_is_rejected(self):
        """An event carrying no StackId at all is rejected (Req 1.2)."""
        event = _admin_create_event()
        del event["StackId"]
        lf_mock = _make_lf_admin_mock()
        cfnr, _ = _run_lf_admin_granter(event, lf_client=lf_mock)
        lf_mock.put_data_lake_settings.assert_not_called()
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_mismatched_stack_id_on_delete_performs_no_mutation(self):
        """The Delete branch is gated too, so an admin entry cannot be removed (Req 1.2)."""
        existing = [{"DataLakePrincipalIdentifier": ADMIN_PRINCIPAL_ARN}]
        lf_mock = _make_lf_admin_mock(existing_admins=existing)
        cfnr, _ = _run_lf_admin_granter(
            self._mismatched_event("Delete"), lf_client=lf_mock
        )
        lf_mock.put_data_lake_settings.assert_not_called()
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_matching_stack_id_still_mutates(self):
        """The check is not vacuous: the matching StackId path still grants (Req 1.2)."""
        lf_mock = _make_lf_admin_mock()
        cfnr, _ = _run_lf_admin_granter(_admin_create_event(), lf_client=lf_mock)
        lf_mock.put_data_lake_settings.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

class TestLFAdminGranterModeGate:
    """Admin registration happens only when the catalog is in LF mode.

    scan-aa27a832 remediation, Req 2.1: an IAM-mode catalog -- the documented
    default -- needs no Lake Formation grants, so the stack must not register
    an account-wide data lake administrator for it.
    """

    def test_iam_mode_registers_no_admin(self):
        """IAM mode: Create writes no DataLakeSettings at all (Req 2.1)."""
        lf_mock = _make_lf_admin_mock()
        cfnr, _ = _run_lf_admin_granter(
            _admin_create_event(), lf_client=lf_mock, glue_client=_iam_mode_glue()
        )
        lf_mock.put_data_lake_settings.assert_not_called()
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "SUCCESS"
        # The PrincipalArn attribute is still returned: ExecuteLFPermissionsGranter
        # reads it via !GetAtt to order itself after this resource.
        assert cfnr.send.call_args[0][3].get("PrincipalArn") == ADMIN_PRINCIPAL_ARN

    def test_lf_mode_still_registers_admin(self):
        """LF mode: the gate is not vacuous, the admin is still added (Req 2.1)."""
        lf_mock = _make_lf_admin_mock()
        cfnr, _ = _run_lf_admin_granter(
            _admin_create_event(), lf_client=lf_mock, glue_client=_lf_mode_glue()
        )
        lf_mock.put_data_lake_settings.assert_called_once()
        admins = lf_mock.put_data_lake_settings.call_args[1]["DataLakeSettings"][
            "DataLakeAdmins"
        ]
        assert [a.get("DataLakePrincipalIdentifier") for a in admins] == [
            ADMIN_PRINCIPAL_ARN
        ]
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_mode_check_uses_the_s3_tables_catalog_id(self):
        """The mode check names '<AccountId>:s3tablescatalog/aws-s3' (Req 2.1)."""
        glue_mock = _iam_mode_glue()
        _run_lf_admin_granter(
            _admin_create_event(), lf_client=_make_lf_admin_mock(), glue_client=glue_mock
        )
        glue_mock.get_catalog.assert_called_once_with(
            CatalogId="123456789012:s3tablescatalog/aws-s3"
        )

    def test_undeterminable_mode_registers_no_admin_and_fails(self):
        """A persistent GetCatalog error never assumes a mode (Req 2.1)."""
        glue_mock = MagicMock()
        glue_mock.get_catalog.side_effect = Exception("AccessDenied")
        lf_mock = _make_lf_admin_mock()
        cfnr, _ = _run_lf_admin_granter(
            _admin_create_event(), lf_client=lf_mock, glue_client=glue_mock
        )
        lf_mock.put_data_lake_settings.assert_not_called()
        assert glue_mock.get_catalog.call_count == 5
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_delete_in_iam_mode_still_removes_a_registered_admin(self):
        """Delete is not mode-gated: an LF-mode registration stays removable (Req 2.1)."""
        existing = [{"DataLakePrincipalIdentifier": ADMIN_PRINCIPAL_ARN}]
        lf_mock = _make_lf_admin_mock(existing_admins=existing)
        glue_mock = _iam_mode_glue()
        cfnr, _ = _run_lf_admin_granter(
            _admin_delete_event(), lf_client=lf_mock, glue_client=glue_mock
        )
        lf_mock.put_data_lake_settings.assert_called_once()
        remaining = lf_mock.put_data_lake_settings.call_args[1]["DataLakeSettings"][
            "DataLakeAdmins"
        ]
        assert remaining == []
        glue_mock.get_catalog.assert_not_called()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_delete_writes_nothing_when_no_admin_was_registered(self):
        """An IAM-mode stack does not rewrite DataLakeSettings on delete (Req 2.1)."""
        lf_mock = _make_lf_admin_mock(
            existing_admins=[{"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:role/other"}]
        )
        cfnr, _ = _run_lf_admin_granter(
            _admin_delete_event(), lf_client=lf_mock, glue_client=_iam_mode_glue()
        )
        lf_mock.put_data_lake_settings.assert_not_called()
        assert cfnr.send.call_args[0][2] == "SUCCESS"

    def test_template_supplies_the_account_id_for_the_mode_check(self):
        """ACCOUNT_ID comes from the template, not the event (Req 2.1)."""
        with open(_TEMPLATE_PATH) as fh:
            t = yaml.load(fh, Loader=CfnLoader)
        env = t["Resources"]["LFAdminGranterFunction"]["Properties"]["Environment"][
            "Variables"
        ]
        assert env["ACCOUNT_ID"].tag == "!Ref"
        assert env["ACCOUNT_ID"].value == "AWS::AccountId"

# ---------------------------------------------------------------------------
# Tests: LFPermissionsGranter privileged-parameter source and StackId gate
# scan-aa27a832 remediation, Req 1.2, 1.3
# ---------------------------------------------------------------------------


FOREIGN_ROLE_ARN = "arn:aws:iam::123456789012:role/attacker"
FOREIGN_STACK_ID = (
    "arn:aws:cloudformation:us-west-2:123456789012:stack/other/"
    "99999999-8888-7777-6666-555555555555"
)


def _foreign_props_create_event() -> dict:
    """A Create event whose every privileged property names an attacker value."""
    event = _create_event()
    event["ResourceProperties"] = {
        "AccountId": "999999999999",
        "ExecutionRoleArn": FOREIGN_ROLE_ARN,
        "SourceBucketNames": "attacker-bucket",
        "LFAdminRoleArn": FOREIGN_ROLE_ARN,
    }
    return event


class TestLFPermissionsGranterPrivilegedParameterSource:
    """Every privileged value comes from the environment, not the event.

    scan-aa27a832 remediation, Req 1.3: an in-account caller holding
    lambda:InvokeFunction on this function must not be able to choose the
    principal that is granted Lake Formation access, the namespaces it is
    granted over, the catalog account, or the role assumed to issue the grant.
    """

    def test_event_execution_role_arn_never_becomes_the_grant_principal(self):
        """A foreign ExecutionRoleArn in the event is ignored (Req 1.3)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _foreign_props_create_event(),
            glue_client=_lf_mode_glue(),
            lf_client=lf_mock,
        )
        principals = {
            c.kwargs["Principal"]["DataLakePrincipalIdentifier"]
            for c in lf_mock.grant_permissions.call_args_list
        }
        assert principals == {GRANTS_EXECUTION_ROLE_ARN}
        assert FOREIGN_ROLE_ARN not in principals

    def test_event_source_bucket_names_never_becomes_the_namespace(self):
        """A foreign SourceBucketNames in the event is ignored (Req 1.3)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _foreign_props_create_event(),
            glue_client=_lf_mode_glue(),
            lf_client=lf_mock,
        )
        namespaces = {
            c.kwargs["Resource"]["Database"]["Name"]
            for c in lf_mock.grant_permissions.call_args_list
            if "Database" in c.kwargs.get("Resource", {})
        }
        assert namespaces == {"b_bucket-a"}
        assert "b_attacker-bucket" not in namespaces

    def test_event_account_id_never_becomes_the_catalog_id(self):
        """A foreign AccountId in the event is ignored (Req 1.3)."""
        lf_mock = MagicMock()
        _run_lf_permissions_granter(
            _foreign_props_create_event(),
            glue_client=_lf_mode_glue(),
            lf_client=lf_mock,
        )
        catalogs = {
            c.kwargs["Resource"][key]["CatalogId"]
            for c in lf_mock.grant_permissions.call_args_list
            for key in ("Database", "Table")
            if key in c.kwargs.get("Resource", {})
        }
        assert catalogs == {f"{GRANTS_ACCOUNT_ID}:s3tablescatalog/aws-s3"}

    def test_event_lf_admin_role_arn_is_never_assumed(self):
        """A foreign LFAdminRoleArn in the event does not drive sts:AssumeRole (Req 1.3)."""
        sts_mock = MagicMock()
        _run_lf_permissions_granter(
            _foreign_props_create_event(),
            glue_client=_lf_mode_glue(),
            lf_client=MagicMock(),
            sts_client=sts_mock,
        )
        sts_mock.assume_role.assert_not_called()

    def test_environment_lf_admin_role_arn_is_still_assumed(self):
        """The gate is not vacuous: the template's value is used (Req 1.3)."""
        sts_mock = MagicMock()
        sts_mock.assume_role.return_value = {
            "Credentials": {
                "AccessKeyId": "AKIAEXAMPLE",
                "SecretAccessKey": "secret",  # noqa: S106
                "SessionToken": "token",
            }
        }
        _run_lf_permissions_granter(
            _foreign_props_create_event(),
            glue_client=_lf_mode_glue(),
            lf_client=MagicMock(),
            sts_client=sts_mock,
            lf_admin_role_arn="arn:aws:iam::123456789012:role/lf-admin",
        )
        sts_mock.assume_role.assert_called_once_with(
            RoleArn="arn:aws:iam::123456789012:role/lf-admin",
            RoleSessionName="LFGranter",
        )

    def test_template_sets_every_privileged_environment_variable(self):
        """The template supplies all five variables the handler reads (Req 1.3)."""
        with open(_TEMPLATE_PATH) as fh:
            t = yaml.load(fh, Loader=CfnLoader)
        env = t["Resources"]["LFPermissionsGranterFunction"]["Properties"][
            "Environment"
        ]["Variables"]
        assert env["PRINCIPAL_ARN"].tag == "!GetAtt"
        assert env["PRINCIPAL_ARN"].value == "ExecutionRole.Arn"
        assert env["SOURCE_BUCKET_NAMES"].tag == "!Join"
        assert env["ACCOUNT_ID"].tag == "!Ref"
        assert env["ACCOUNT_ID"].value == "AWS::AccountId"
        assert env["STACK_ID"].tag == "!Ref"
        assert env["STACK_ID"].value == "AWS::StackId"
        assert env["LF_ADMIN_ROLE_ARN"].tag == "!If"

    def test_custom_resource_retains_its_change_detection_properties(self):
        """CloudFormation sends an Update only when a resource property changes,
        so the properties the handler no longer reads stay listed as triggers.

        Without SourceBucketNames on the custom resource, adding a source bucket
        would leave this function uninvoked and the new namespace ungranted.
        """
        with open(_TEMPLATE_PATH) as fh:
            t = yaml.load(fh, Loader=CfnLoader)
        props = t["Resources"]["ExecuteLFPermissionsGranter"]["Properties"]
        for name in (
            "ExecutionRoleArn",
            "SourceBucketNames",
            "AccountId",
            "LFAdminRoleArn",
        ):
            assert name in props, (
                f"{name} must stay a property of ExecuteLFPermissionsGranter so a "
                "change to it re-invokes the handler"
            )


class TestLFPermissionsGranterStackIdCheck:
    """An event from anything other than this stack is rejected.

    scan-aa27a832 remediation, Req 1.2: the handler compares the event's
    StackId with the STACK_ID environment variable before any grant or
    revocation.
    """

    def _mismatched(self, event: dict) -> dict:
        event["StackId"] = FOREIGN_STACK_ID
        return event

    def test_mismatched_stack_id_grants_nothing(self):
        """A foreign StackId reaches neither the mode check nor a grant (Req 1.2)."""
        lf_mock = MagicMock()
        glue_mock = _lf_mode_glue()
        _run_lf_permissions_granter(
            self._mismatched(_create_event()),
            glue_client=glue_mock,
            lf_client=lf_mock,
        )
        lf_mock.grant_permissions.assert_not_called()
        glue_mock.get_catalog.assert_not_called()

    def test_mismatched_stack_id_responds_failed_without_specifics(self):
        """The FAILED reason names neither the expected nor the supplied value (Req 1.2)."""
        cfnr = _run_lf_permissions_granter(
            self._mismatched(_create_event()),
            glue_client=_lf_mode_glue(),
            lf_client=MagicMock(),
        )
        cfnr.send.assert_called_once()
        assert cfnr.send.call_args[0][2] == "FAILED"
        reason = cfnr.send.call_args[0][3]["Error"]
        assert ADMIN_STACK_ID not in reason
        assert FOREIGN_STACK_ID not in reason
        assert GRANTS_EXECUTION_ROLE_ARN not in reason
        assert "StackId" not in reason

    def test_missing_stack_id_is_rejected(self):
        """An event carrying no StackId at all is rejected (Req 1.2)."""
        event = _create_event()
        del event["StackId"]
        lf_mock = MagicMock()
        cfnr = _run_lf_permissions_granter(
            event, glue_client=_lf_mode_glue(), lf_client=lf_mock
        )
        lf_mock.grant_permissions.assert_not_called()
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_mismatched_stack_id_revokes_nothing_on_delete(self):
        """The Delete branch is gated too, so grants cannot be revoked (Req 1.2)."""
        physical_id = json.dumps({
            "grants": [
                {
                    "principal": GRANTS_EXECUTION_ROLE_ARN,
                    "catalog_id": f"{GRANTS_ACCOUNT_ID}:s3tablescatalog/aws-s3",
                    "namespace": "b_bucket-a",
                }
            ]
        })
        lf_mock = MagicMock()
        cfnr = _run_lf_permissions_granter(
            self._mismatched(_delete_event(physical_id)), lf_client=lf_mock
        )
        lf_mock.revoke_permissions.assert_not_called()
        assert cfnr.send.call_args[0][2] == "FAILED"

    def test_matching_stack_id_still_grants(self):
        """The check is not vacuous: the matching StackId path still grants (Req 1.2)."""
        lf_mock = MagicMock()
        cfnr = _run_lf_permissions_granter(
            _create_event(), glue_client=_lf_mode_glue(), lf_client=lf_mock
        )
        assert lf_mock.grant_permissions.call_count == 2
        assert cfnr.send.call_args[0][2] == "SUCCESS"
