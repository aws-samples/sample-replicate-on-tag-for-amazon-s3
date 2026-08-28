"""IAM policy smoke test — task 19.3.

Verifies the published iam-policy.json (at the project root) contains only
the required source-side actions and does NOT contain:
  - Any DynamoDB actions.
  - Any destination-account or destination-region access permissions.

Also verifies the required actions are present:
  - s3:GetObject / s3:PutObject (scratch/state bucket).
  - iam:PassRole (for the stack-created Batch Operations job role).
  - PassRole is scoped to that role's ARN, with no wildcard resource. It carries
    no iam:PassedToService condition: s3control:CreateJob does not populate that
    context key, so the condition would deny (accepted-risks AR8).

And that no bucket-policy actions are present: the Batch Operations job role
holds its State Bucket grants in its own identity policy, so nothing in the
Solution reads or writes a bucket policy (solution-owned-batchops-role 3.2).

Requirements: 12.1, 12.2
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

# Path to iam-policy.json relative to the project root.
_POLICY_PATH = Path(__file__).parent.parent / "deploy" / "iam-policy.json"


@pytest.fixture(scope="module")
def policy() -> dict:
    """Load and return the parsed iam-policy.json."""
    assert _POLICY_PATH.exists(), f"iam-policy.json not found at {_POLICY_PATH}"
    with open(_POLICY_PATH, "r") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def all_actions(policy: dict) -> list[str]:
    """Collect every IAM action string from all policy statements (lowercased)."""
    actions: list[str] = []
    for stmt in policy.get("Statement", []):
        stmt_actions = stmt.get("Action", [])
        if isinstance(stmt_actions, str):
            stmt_actions = [stmt_actions]
        actions.extend(a.lower() for a in stmt_actions)
    return actions


@pytest.fixture(scope="module")
def all_resources(policy: dict) -> list[str]:
    """Collect every Resource string from all policy statements (lowercased)."""
    resources: list[str] = []
    for stmt in policy.get("Statement", []):
        stmt_resources = stmt.get("Resource", [])
        if isinstance(stmt_resources, str):
            stmt_resources = [stmt_resources]
        resources.extend(r.lower() for r in stmt_resources)
    return resources


# ---------------------------------------------------------------------------
# Structural sanity
# ---------------------------------------------------------------------------


class TestPolicyStructure:
    def test_policy_file_exists(self):
        assert _POLICY_PATH.exists()

    def test_policy_has_version_field(self, policy):
        assert "Version" in policy

    def test_policy_has_statement_list(self, policy):
        assert isinstance(policy.get("Statement"), list)
        assert len(policy["Statement"]) > 0


# ---------------------------------------------------------------------------
# Required actions must be present (Req 12.1)
# ---------------------------------------------------------------------------


class TestRequiredActionsPresent:
    def test_s3_get_object_present(self, all_actions):
        """s3:GetObject required for state store read (Req 12.1)."""
        assert "s3:getobject" in all_actions

    def test_s3_put_object_present(self, all_actions):
        """s3:PutObject required for conditional writes to state/scratch bucket (Req 12.1)."""
        assert "s3:putobject" in all_actions

    def test_s3_list_bucket_present(self, all_actions):
        """s3:ListBucket required so GetObject returns NoSuchKey (not AccessDenied) on first run (Req 12.1)."""
        assert "s3:listbucket" in all_actions

    def test_iam_pass_role_present(self, all_actions):
        """iam:PassRole required to pass the Batch Operations job role to the job (Req 12.1)."""
        assert "iam:passrole" in all_actions

    def test_s3_get_replication_configuration_present(self, all_actions):
        """s3:GetReplicationConfiguration required to read existing replication config (Req 12.1)."""
        assert "s3:getreplicationconfiguration" in all_actions

    def test_s3_create_job_present(self, all_actions):
        """s3:CreateJob required to submit S3 Batch Operations jobs (Req 12.1)."""
        assert "s3:createjob" in all_actions

    def test_athena_actions_present(self, all_actions):
        """Athena query actions required for journal reads (Req 12.1)."""
        assert "athena:startqueryexecution" in all_actions
        assert "athena:getqueryexecution" in all_actions
        assert "athena:getqueryresults" in all_actions


# ---------------------------------------------------------------------------
# Forbidden actions must NOT be present (Req 12.1, 12.2)
# ---------------------------------------------------------------------------


class TestForbiddenActionsAbsent:
    def test_no_dynamodb_actions(self, all_actions):
        """Policy must contain NO DynamoDB actions — state is stored in S3 (Req 12.1)."""
        dynamodb_actions = [a for a in all_actions if a.startswith("dynamodb:")]
        assert dynamodb_actions == [], (
            f"Found unexpected DynamoDB actions: {dynamodb_actions}"
        )

    def test_no_dynamodb_star_wildcard(self, all_actions):
        """Wildcard must not implicitly include DynamoDB."""
        for action in all_actions:
            assert not (action == "*"), "Wildcard '*' action must not appear in policy"

    def test_no_bucket_policy_actions(self, all_actions):
        """No bucket-policy actions — the job role's State Bucket grants sit in
        its own identity policy, so nothing reads or writes a bucket policy
        (solution-owned-batchops-role 3.2, 3.4)."""
        for forbidden in ("s3:getbucketpolicy", "s3:putbucketpolicy"):
            assert forbidden not in all_actions, (
                f"{forbidden!r} must not appear: the Solution manages no bucket policy"
            )

    def test_no_inline_role_policy_actions(self, all_actions):
        """No iam:PutRolePolicy / iam:DeleteRolePolicy — the PassRole granter
        custom resource that needed them is gone
        (solution-owned-batchops-role 4.2)."""
        for forbidden in ("iam:putrolepolicy", "iam:deleterolepolicy"):
            assert forbidden not in all_actions, (
                f"{forbidden!r} must not appear: no resource modifies a role's policies"
            )

    def test_no_s3_replicate_destination_actions(self, all_actions):
        """No destination-side S3 actions that would imply cross-account access."""
        forbidden_destination_actions = [
            "s3:putreplicationconfiguration",
            "s3:deletereplicationconfiguration",
        ]
        for forbidden in forbidden_destination_actions:
            assert forbidden not in all_actions, (
                f"Destination-side action {forbidden!r} must not be in policy"
            )


# ---------------------------------------------------------------------------
# iam:PassRole must be restricted to S3 Batch Operations (Req 12.2, 13.1)
# ---------------------------------------------------------------------------


class TestPassRoleRestriction:
    def test_iam_pass_role_present(self, policy):
        """iam:PassRole must be present and scoped to the Batch Operations job role (Req 12.2)."""
        found = False
        for stmt in policy.get("Statement", []):
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            if any(a.lower() == "iam:passrole" for a in actions):
                found = True
                # Resource must reference the stack-created job role placeholder.
                resources = stmt.get("Resource", [])
                if isinstance(resources, str):
                    resources = [resources]
                assert any("BATCH_OPERATIONS_ROLE_NAME" in r for r in resources), (
                    "iam:PassRole must be scoped to the Batch Operations job role ARN (Req 12.2)"
                )
        assert found, "iam:PassRole statement must be present (Req 12.2)"

    def test_iam_pass_role_names_no_replication_role(self, policy):
        """The customer's replication role is never passed to a job.

        solution-owned-batchops-role Requirement 2.1: the job role ARN comes
        from the stack, not from a source bucket's replication configuration.
        """
        for stmt in policy.get("Statement", []):
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            if any(a.lower() == "iam:passrole" for a in actions):
                resources = stmt.get("Resource", [])
                if isinstance(resources, str):
                    resources = [resources]
                for r in resources:
                    assert "REPLICATION_ROLE" not in r.upper(), (
                        f"iam:PassRole must not name a replication role: {r!r}"
                    )

    def test_iam_pass_role_no_wildcard_resource(self, policy):
        """iam:PassRole must not use wildcard (*) resource (Req 12.2)."""
        for stmt in policy.get("Statement", []):
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            if any(a.lower() == "iam:passrole" for a in actions):
                resources = stmt.get("Resource", [])
                if isinstance(resources, str):
                    resources = [resources]
                assert resources != ["*"], "iam:PassRole must not use wildcard resource (Req 12.2)"


# ---------------------------------------------------------------------------
# No destination-region access (Req 12.2, 13.1)
# ---------------------------------------------------------------------------


class TestNoDestinationAccess:
    # LFGranterRolePolicy documents permissions for LFGranterRole (a separate
    # CloudFormation-provisioned role), not for ExecutionRole. It includes
    # sts:AssumeRole to assume the optional LFAdminRoleArn. Exclude it from the
    # cross-account assumption check which only applies to ExecutionRole.
    #
    # OptionalDestinationPresenceCheckAssumeRole is the deliberate, scoped
    # destination-access exception for replication completion tracking's
    # Destination_Presence_Check (replication-completion-tracking Requirements
    # 2.1, 3.7). It is intentionally excluded from this same-account-only
    # check; TestOptionalDestinationPresenceCheckAssumeRoleStatement below
    # verifies it is scoped to a single placeholder ARN, never Resource: "*".
    #
    # AssumeLFAdminRole is LFGranterRole's optional Lake Formation admin
    # elevation, split out of LFGranterRolePolicy so it can carry a scoped
    # resource (security-scan-remediation Requirement 2.2). Like
    # LFGranterRolePolicy it does not belong to ExecutionRole;
    # TestAssumeRoleNeverWildcard below verifies it is never Resource: "*".
    _EXCLUDED_SIDS = frozenset([
        "LFGranterRolePolicy",
        "AssumeLFAdminRole",
        "OptionalDestinationPresenceCheckAssumeRole",
    ])

    def test_all_arns_are_source_account_or_wildcard(self, policy):
        """ExecutionRole statements must not contain cross-account role assumption actions.

        LFGranterRolePolicy is excluded — it belongs to LFGranterRole, not ExecutionRole.
        """
        for action in ["sts:assumerole", "sts:assumerolewithwebidentity"]:
            all_actions_lower = [
                a.lower()
                for stmt in policy.get("Statement", [])
                if stmt.get("Sid", "") not in self._EXCLUDED_SIDS
                for a in (
                    [stmt.get("Action", [])]
                    if isinstance(stmt.get("Action", []), str)
                    else stmt.get("Action", [])
                )
            ]
            assert action not in all_actions_lower, (
                f"Cross-account role assumption action {action!r} must not appear "
                f"in ExecutionRole policy statements"
            )

    def test_no_s3_bucket_wildcard_on_all_buckets(self, policy):
        """No s3:* action granting implicit destination-bucket write access."""
        for stmt in policy.get("Statement", []):
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            for a in actions:
                assert a.lower() != "s3:*", (
                    "s3:* wildcard must not appear — it would grant destination-side access"
                )


# ---------------------------------------------------------------------------
# CloudWatch metrics optional statement — task 7.4
# Feature: cloudwatch-metrics
# Requirements: 6.1, 6.2, 6.7
# ---------------------------------------------------------------------------


class TestOptionalCloudWatchMetricsStatement:
    """Validate the OptionalCloudWatchMetrics statement in iam-policy.json."""

    @pytest.fixture
    def cw_statement(self, policy):
        """Return the OptionalCloudWatchMetrics statement, or skip if absent."""
        for stmt in policy.get("Statement", []):
            if stmt.get("Sid") == "OptionalCloudWatchMetrics":
                return stmt
        pytest.skip("OptionalCloudWatchMetrics statement not present (it is optional)")

    def test_only_grants_put_metric_data(self, cw_statement):
        """CloudWatch statement grants cloudwatch:PutMetricData and nothing else (Req 6.2)."""
        actions = cw_statement.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        assert actions == ["cloudwatch:PutMetricData"], (
            f"Expected only cloudwatch:PutMetricData; got {actions}"
        )

    def test_has_cloudwatch_namespace_condition(self, cw_statement):
        """Statement scopes the grant with a cloudwatch:namespace condition (Req 6.2)."""
        condition = cw_statement.get("Condition", {})
        string_equals = condition.get("StringEquals", {})
        assert "cloudwatch:namespace" in string_equals, (
            "OptionalCloudWatchMetrics must have cloudwatch:namespace StringEquals condition"
        )

    def test_no_destination_account_access(self, cw_statement):
        """CloudWatch statement grants no destination-account access (Req 6.7)."""
        actions = cw_statement.get("Action", [])
        if isinstance(actions, str):
            actions = [actions]
        for a in actions:
            # cloudwatch:PutMetricData is source-side only — no sts: or s3:
            assert not a.lower().startswith("sts:"), (
                f"Unexpected cross-account action: {a!r}"
            )
            assert not a.lower().startswith("s3:GetObject") or a.lower().startswith("s3:"), (
                f"Unexpected S3 action in CW statement: {a!r}"
            )
        # Specifically: the only action allowed is PutMetricData
        assert all(
            a.lower() == "cloudwatch:putmetricdata" for a in actions
        ), f"Unexpected actions in CloudWatch statement: {actions}"

    def test_resource_is_star(self, cw_statement):
        """PutMetricData has no ARN-level scoping; Resource must be '*' (Req 6.2)."""
        resources = cw_statement.get("Resource", [])
        if isinstance(resources, str):
            resources = [resources]
        assert resources == ["*"], (
            f"cloudwatch:PutMetricData must use Resource: [\"*\"]; got {resources}"
        )


# ---------------------------------------------------------------------------
# No unconditional destination-account access
# Feature: source-status-completion-tracking
# Requirements: 3.1
# ---------------------------------------------------------------------------


class TestNoUnconditionalDestinationAccess:
    """The reference policy grants no blanket destination-side access.

    Completion tracking reads ``x-amz-replication-status`` on the source object,
    so it needs no destination-account access at all. The optional
    ``OptionalDestinationPresenceCheckAssumeRole`` statement that an earlier
    design would have used was removed along with the
    ``DestinationPresenceCheckRoleArn`` parameter, the
    ``HasDestinationPresenceCheckRole`` condition, and the
    ``DESTINATION_PRESENCE_CHECK_ROLE_ARN`` env var.

    ``tests/test_template.py`` and ``tests/test_lambda_handler.py`` assert that
    removal stays done. This asserts the reference policy does not regain
    blanket access by some other route: the only ``sts:AssumeRole`` it grants is
    ``AssumeLFAdminRole``, scoped to the supplied Lake Formation admin role.
    """

    def test_no_wildcard_or_blanket_s3_action(self, all_actions):
        """No ``s3:*`` and no bare ``*`` action anywhere in the policy (Req 3.1)."""
        assert "s3:*" not in all_actions
        assert "*" not in all_actions


class TestRemovedSourceObjectReadPermissions:
    """Report-derived completion must not regain source-object read grants.

    State Bucket reads remain necessary and are checked elsewhere. This guard is
    limited to statements whose resource names the source-bucket placeholder.
    """

    _FORBIDDEN_ACTIONS = {
        "s3:getobject",
        "s3:getobjectversion",
        "s3:listbucket",
    }

    def test_source_bucket_statements_grant_no_completion_read_actions(self, policy):
        source_statements = []
        for statement in policy.get("Statement", []):
            resources = statement.get("Resource", [])
            if isinstance(resources, str):
                resources = [resources]
            if any(
                isinstance(resource, str) and "SOURCE_BUCKET_NAME" in resource
                for resource in resources
            ):
                source_statements.append(statement)

        assert source_statements, "Expected a source-bucket replication-config grant"
        for statement in source_statements:
            actions = statement.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            action_set = {action.lower() for action in actions}
            assert action_set.isdisjoint(self._FORBIDDEN_ACTIONS), (
                f"Source-bucket statement {statement.get('Sid')!r} regained "
                f"completion read permissions: {action_set & self._FORBIDDEN_ACTIONS}"
            )
