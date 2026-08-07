"""Unit tests for the inline Lambda logic in LFPermissionsGranterFunction
and LFAdminGranterFunction (ZipFile code embedded in deploy/template.yaml).

Both functions are extracted from the template at test time and executed via
exec() with patched boto3 and cfnresponse so no real AWS calls are made.

Feature: lf-mode-s3-metadata
Requirements: 1.1, 1.2, 1.3, 2.1, 2.3, 2.4, 3.2, 3.3, 4.1, 4.2
"""
from __future__ import annotations

import json
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


def _run_lf_permissions_granter(
    event: dict,
    *,
    glue_client: MagicMock | None = None,
    lf_client: MagicMock | None = None,
    sts_client: MagicMock | None = None,
) -> MagicMock:
    """Execute LFPermissionsGranterFunction inline code and call handler(event).

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

    code = _get_zip_code("LFPermissionsGranterFunction")
    context = MagicMock()
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
) -> tuple[MagicMock, MagicMock]:
    """Execute LFAdminGranterFunction inline code and call handler(event).

    Returns (cfnresponse_mock, lf_client) so callers can assert on both.
    """
    cfnresponse_mock = _make_cfnresponse()
    _lf = lf_client if lf_client is not None else MagicMock()

    def make_client(service, **kwargs):
        if service == "lakeformation":
            return _lf
        return MagicMock()

    code = _get_zip_code("LFAdminGranterFunction")
    context = MagicMock()
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
    return {
        "RequestType": "Create",
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
        "PhysicalResourceId": physical_id,
        "ResourceProperties": {
            "AccountId": "123456789012",
            "ExecutionRoleArn": "arn:aws:iam::123456789012:role/exec-role",
            "SourceBucketNames": "bucket-a",
            "LFAdminRoleArn": lf_admin_role_arn,
        },
    }


def _admin_create_event(principal_arn: str = "arn:aws:iam::123456789012:role/granter") -> dict:
    return {
        "RequestType": "Create",
        "ResourceProperties": {"PrincipalArn": principal_arn},
    }


def _admin_delete_event(principal_arn: str = "arn:aws:iam::123456789012:role/granter") -> dict:
    return {
        "RequestType": "Delete",
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

    def test_delete_swallows_exceptions_best_effort(self):
        """Delete swallows all exceptions — best-effort cleanup (Req 4.2)."""
        lf_mock = _make_lf_admin_mock()
        lf_mock.get_data_lake_settings.side_effect = Exception("permission denied")
        cfnr, _ = _run_lf_admin_granter(_admin_delete_event(), lf_client=lf_mock)
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
        assert data.get("PrincipalArn") == "arn:aws:iam::123456789012:role/granter"
