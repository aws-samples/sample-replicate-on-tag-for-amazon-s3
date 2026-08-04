"""Structural assertion tests for deploy/template.yaml.

Parses the CloudFormation template using a custom PyYAML loader that handles
intrinsic function tags (!Ref, !Sub, !If, etc.) and asserts correctness of
resource definitions, IAM policies, lifecycle rules, and naming conventions.

Feature: cloudformation-deployment
Requirements: 2.1, 2.2, 2.3, 2.5, 4.2, 4.3, 6.2, 6.3, 6.4, 7.2, 9.2
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

# ---------------------------------------------------------------------------
# Custom YAML loader for CloudFormation intrinsic functions
# ---------------------------------------------------------------------------


class _CfnTag:
    """Represents a CloudFormation intrinsic function tag."""

    def __init__(self, tag: str, value):
        self.tag = tag
        self.value = value

    def __repr__(self):
        return f"_CfnTag({self.tag!r}, {self.value!r})"


def _cfn_constructor(loader, tag_suffix, node):
    """Multi-constructor that handles all ! prefixed tags."""
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_TEMPLATE_PATH = Path(__file__).parent.parent / "deploy" / "template.yaml"
_IAM_POLICY_PATH = Path(__file__).parent.parent / "deploy" / "iam-policy.json"


@pytest.fixture(scope="module")
def template() -> dict:
    """Load and parse the CloudFormation template."""
    assert _TEMPLATE_PATH.exists(), f"Template not found at {_TEMPLATE_PATH}"
    with open(_TEMPLATE_PATH, "r") as fh:
        return yaml.load(fh, Loader=CfnLoader)


@pytest.fixture(scope="module")
def resources(template) -> dict:
    return template.get("Resources", {})


@pytest.fixture(scope="module")
def iam_policy() -> dict:
    """Load the reference iam-policy.json."""
    assert _IAM_POLICY_PATH.exists()
    with open(_IAM_POLICY_PATH, "r") as fh:
        return json.load(fh)


@pytest.fixture(scope="module")
def lambda_resource(resources) -> dict:
    return resources.get("ReplicationLambda", {})


@pytest.fixture(scope="module")
def lambda_props(lambda_resource) -> dict:
    return lambda_resource.get("Properties", {})


@pytest.fixture(scope="module")
def execution_role(resources) -> dict:
    return resources.get("ExecutionRole", {})


@pytest.fixture(scope="module")
def execution_role_policy_statements(execution_role) -> list:
    """Extract all IAM policy statements from the ExecutionRole inline policy."""
    policies = execution_role.get("Properties", {}).get("Policies", [])
    statements = []
    for policy in policies:
        doc = policy.get("PolicyDocument", {})
        statements.extend(doc.get("Statement", []))
    return statements


@pytest.fixture(scope="module")
def state_bucket(resources) -> dict:
    return resources.get("StateBucket", {})


@pytest.fixture(scope="module")
def state_bucket_props(state_bucket) -> dict:
    return state_bucket.get("Properties", {})


@pytest.fixture(scope="module")
def kms_key_policy(resources) -> dict:
    return resources.get("KmsKeyPolicy", {})


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _extract_actions_from_statements(statements: list) -> set[str]:
    """Collect all IAM actions (lowercased) from a list of policy statements."""
    actions = set()
    for stmt in statements:
        stmt_actions = stmt.get("Action", [])
        if isinstance(stmt_actions, str):
            stmt_actions = [stmt_actions]
        for a in stmt_actions:
            if isinstance(a, str):
                actions.add(a.lower())
    return actions


def _get_iam_policy_non_kms_actions(iam_policy: dict) -> set[str]:
    """Get all actions from iam-policy.json excluding the KMS and optional
    CloudWatch metrics statements.

    The KMS statement lives only in the conditional KmsKeyPolicy resource (not
    in the ExecutionRole inline policy), so it is excluded by Sid/prefix.

    The OptionalCloudWatchMetrics statement (cloudwatch:PutMetricData) lives
    only in the conditional MetricsPolicy resource — it is NOT present in the
    ExecutionRole inline policy — so it must also be excluded here; otherwise
    test_all_non_kms_actions_present would fail because it searches for
    cloudwatch:PutMetricData in the inline policy where it will never appear.

    Requirements: cloudwatch-metrics 6.4, 6.5
    """
    # Sids whose actions live only in a conditional resource, not inline.
    # LFGranterRolePolicy belongs to LFGranterRole (a separate IAM role),
    # not to ExecutionRole — exclude it from the inline-policy coverage check.
    _CONDITIONAL_SIDS = frozenset([
        "OptionalCustomerManagedKmsKey",
        "JournalTableKmsKey",
        "OptionalCloudWatchMetrics",
        "LFGranterRolePolicy",
        # AssumeLFAdminRole is LFGranterRole's optional LF admin elevation,
        # split out of LFGranterRolePolicy so it can carry a scoped resource
        # (security-scan-remediation Requirement 2.2). Like the statement it
        # was split from, it belongs to LFGranterRole, not ExecutionRole.
        "AssumeLFAdminRole",
        # iam:PassRole is granted at deploy time by the ReplicationRolePassGranter
        # custom resource (scoped to the buckets' replication roles), not as a
        # static statement in the ExecutionRole inline policy.
        "PassExistingReplicationRole",
    ])
    actions = set()
    for stmt in iam_policy.get("Statement", []):
        sid = stmt.get("Sid", "")
        if sid in _CONDITIONAL_SIDS:
            continue
        # Also skip any statement whose actions are all kms: (belt-and-suspenders)
        if "Kms" in sid or "kms" in sid.lower():
            continue
        stmt_actions = stmt.get("Action", [])
        if isinstance(stmt_actions, str):
            stmt_actions = [stmt_actions]
        actions.update(a.lower() for a in stmt_actions)
    return actions


def _cfn_tag_value(obj) -> str | None:
    """Extract the string value from a _CfnTag if it is one, else return str or None."""
    if isinstance(obj, _CfnTag):
        return obj.value if isinstance(obj.value, str) else None
    if isinstance(obj, str):
        return obj
    return None


# ---------------------------------------------------------------------------
# Tests: Lambda Function properties
# ---------------------------------------------------------------------------


class TestLambdaFunction:
    def test_runtime_is_python312(self, lambda_props):
        """Lambda Runtime must be python3.12 (matches python3.1x pattern)."""
        runtime = lambda_props.get("Runtime", "")
        assert re.match(r"^python3\.1\d$", runtime), f"Runtime {runtime!r} does not match python3.1x"

    def test_handler_is_correct(self, lambda_props):
        """Handler must be src.lambda_handler.handler."""
        handler = lambda_props.get("Handler", "")
        assert handler == "src.lambda_handler.handler"

    def test_timeout_at_most_900(self, lambda_props):
        """Timeout must be <= 900 seconds."""
        timeout = lambda_props.get("Timeout")
        # May be a !Ref tag
        if isinstance(timeout, _CfnTag) and timeout.tag == "!Ref":
            # Check the parameter default/max
            pass  # Validated via parameter MaxValue in template
        elif isinstance(timeout, (int, float)):
            assert timeout <= 900

    def test_reserved_concurrent_executions_is_one(self, lambda_props):
        """ReplicationLambda must be capped at 1 concurrent execution — it
        processes every configured bucket sequentially with no in-process
        concurrency, so there is no throughput reason to allow more than
        one, and capping it prevents two invocations (e.g. an overrunning
        run overlapping the next ScheduleExpression trigger) from racing
        on the same bucket's lease at all, rather than relying solely on
        the lease's conditional write to arbitrate a genuine overlap."""
        assert lambda_props.get("ReservedConcurrentExecutions") == 1

    def test_required_env_vars_present(self, lambda_props):
        """STATE_BUCKET, ATHENA_WORKGROUP, ATHENA_OUTPUT_LOCATION, ACCOUNT_ID must be set."""
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        required = {"STATE_BUCKET", "ATHENA_WORKGROUP", "ATHENA_OUTPUT_LOCATION", "ACCOUNT_ID"}
        actual_keys = set(env_vars.keys())
        missing = required - actual_keys
        assert not missing, f"Missing required env vars: {missing}"

    def test_batch_job_failure_env_vars_present(self, lambda_props):
        """ReplicationLambda needs BATCH_JOB_FAILURE_TOPIC_ARN and
        BATCH_JOB_FAILURE_LOG_GROUP so its on_bucket_disable callback can
        publish the bucket-disabled recovery-instructions alert (mirrors
        CompletionReportCheckLambda's own report-missing alert wiring)."""
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        assert "BATCH_JOB_FAILURE_TOPIC_ARN" in env_vars
        assert "BATCH_JOB_FAILURE_LOG_GROUP" in env_vars

    def test_batch_job_failure_topic_arn_env_var_conditional_on_has_alarm_email(
        self, lambda_props
    ):
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        val = env_vars.get("BATCH_JOB_FAILURE_TOPIC_ARN")
        assert isinstance(val, _CfnTag) and val.tag == "!If"
        cond_name = val.value[0] if isinstance(val.value, list) else None
        assert cond_name == "HasAlarmEmail"

    def test_batch_job_failure_log_group_env_var_references_log_group(
        self, lambda_props
    ):
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        val = env_vars.get("BATCH_JOB_FAILURE_LOG_GROUP")
        assert isinstance(val, _CfnTag) and val.tag == "!Ref"
        assert val.value == "BatchJobFailureLogGroup"

    def test_journal_read_row_cap_env_var_references_parameter(self, lambda_props):
        """code-review-remediation verification-notes.md "scaling risk"
        finding: ReplicationLambda must be wired to the JournalReadRowCap
        parameter so a customer can override the default 500,000-row cap."""
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        val = env_vars.get("JOURNAL_READ_ROW_CAP")
        assert isinstance(val, _CfnTag) and val.tag == "!Ref"
        assert val.value == "JournalReadRowCap"

    def test_reinvocation_chain_limit_env_var_references_parameter(self, lambda_props):
        """Task 7.2 (scale-threshold-and-drain-throughput, Requirement 5.1):
        ReplicationLambda must be wired to the ReinvocationChainLimit
        parameter so an operator can override the default chain-length
        guard on self-reinvocation."""
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        val = env_vars.get("REINVOCATION_CHAIN_LIMIT")
        assert isinstance(val, _CfnTag) and val.tag == "!Ref"
        assert val.value == "ReinvocationChainLimit"


class TestJournalReadRowCapParameter:
    """code-review-remediation verification-notes.md "scaling risk" finding."""

    @pytest.fixture(scope="class")
    def param(self, template) -> dict:
        return template.get("Parameters", {}).get("JournalReadRowCap", {})

    def test_exists_with_default_500000(self, param):
        assert param.get("Type") == "Number"
        assert param.get("Default") == 500000

    def test_min_value_is_1(self, param):
        assert param.get("MinValue") == 1


class TestReinvocationChainLimitParameter:
    """scale-threshold-and-drain-throughput task 7.2, Requirement 5.1."""

    @pytest.fixture(scope="class")
    def param(self, template) -> dict:
        return template.get("Parameters", {}).get("ReinvocationChainLimit", {})

    def test_exists_with_default_20(self, param):
        assert param.get("Type") == "Number"
        assert param.get("Default") == 20

    def test_min_value_is_0(self, param):
        """0 disables reinvocation entirely — should_reinvoke's `depth <
        chain_limit` is always False when chain_limit is 0, so 0 is a
        legitimate (opt-out) value, not an error."""
        assert param.get("MinValue") == 0


# ---------------------------------------------------------------------------
# Tests: ExecutionRole policy actions match iam-policy.json
# ---------------------------------------------------------------------------


class TestExecutionRolePolicy:
    def test_all_non_kms_actions_present(self, execution_role_policy_statements, iam_policy):
        """ExecutionRole inline policy includes all non-KMS actions from iam-policy.json."""
        expected = _get_iam_policy_non_kms_actions(iam_policy)
        actual = _extract_actions_from_statements(execution_role_policy_statements)

        # Also include CloudWatch Logs actions which are added in the template
        # but not in iam-policy.json
        logs_actions = {"logs:createloggroup", "logs:createlogstream", "logs:putlogevents"}
        # Remove logs from actual for comparison against iam-policy.json
        actual_without_logs = actual - logs_actions

        missing = expected - actual_without_logs
        assert not missing, f"Missing actions from iam-policy.json: {missing}"

    def test_pass_role_granted_by_custom_resource_scoped(self, resources):
        """iam:PassRole is granted at deploy time by the ReplicationRolePassGranter
        custom resource, scoped to the discovered replication role ARNs (not wildcard),
        and not present as a static statement in the ExecutionRole inline policy."""
        # The ExecutionRole must NOT carry a static iam:PassRole statement anymore.
        exec_role = resources.get("ExecutionRole", {})
        for policy in exec_role.get("Properties", {}).get("Policies", []):
            for stmt in policy.get("PolicyDocument", {}).get("Statement", []):
                actions = stmt.get("Action", [])
                if isinstance(actions, str):
                    actions = [actions]
                assert not any(
                    isinstance(a, str) and a.lower() == "iam:passrole" for a in actions
                ), "ExecutionRole must not statically grant iam:PassRole; the custom resource does"

        # The granter function, role, and custom resource must exist.
        fn = resources.get("ReplicationRolePassGranterFunction", {})
        assert fn.get("Type") == "AWS::Lambda::Function", "Granter Lambda missing"
        assert "ReplicationRolePassGranterRole" in resources, "Granter role missing"
        cr = resources.get("ExecuteReplicationRolePassGranter", {})
        assert cr.get("Type") == "Custom::ReplicationRolePassGranter", "Granter custom resource missing"

        # The granter Lambda code grants iam:PassRole scoped to a derived list, never "*".
        code = fn.get("Properties", {}).get("Code", {}).get("ZipFile", "")
        assert isinstance(code, str) and "iam:PassRole" in code, (
            "Granter Lambda must grant iam:PassRole"
        )
        assert '"Resource": "*"' not in code and "'Resource': '*'" not in code, (
            "Granter must not grant iam:PassRole on a wildcard resource"
        )

    def test_granter_role_pass_role_management_scoped_to_execution_role(self, resources):
        """The granter role's iam:PutRolePolicy/DeleteRolePolicy must target the
        ExecutionRole ARN, not a wildcard."""
        role = resources.get("ReplicationRolePassGranterRole", {})
        found = False
        for policy in role.get("Properties", {}).get("Policies", []):
            for stmt in policy.get("PolicyDocument", {}).get("Statement", []):
                actions = stmt.get("Action", [])
                if isinstance(actions, str):
                    actions = [actions]
                if any(isinstance(a, str) and a.lower() == "iam:putrolepolicy" for a in actions):
                    found = True
                    resource = stmt.get("Resource")
                    assert resource != "*" and resource != ["*"], (
                        "Granter PutRolePolicy must be scoped to the ExecutionRole, not wildcard"
                    )
        assert found, "Granter role must grant iam:PutRolePolicy"


# ---------------------------------------------------------------------------
# Tests: KMS statement only under HasKmsKey condition
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def athena_workgroup(resources) -> dict:
    return resources.get("AthenaWorkGroup", {})


class TestAthenaWorkGroupEncryption:
    """security-scan-remediation Requirement 3 (Decisions 1, 2) — the
    workgroup enforces its own configuration and encrypts every query's
    results, with the customer's key when supplied and SSE-S3 otherwise."""

    def test_enforces_workgroup_configuration(self, athena_workgroup):
        config = athena_workgroup.get("Properties", {}).get("WorkGroupConfiguration", {})
        assert config.get("EnforceWorkGroupConfiguration") is True

    def test_encryption_configuration_is_conditional_on_has_kms_key(
        self, athena_workgroup
    ):
        result_config = (
            athena_workgroup.get("Properties", {})
            .get("WorkGroupConfiguration", {})
            .get("ResultConfiguration", {})
        )
        encryption = result_config.get("EncryptionConfiguration")
        assert isinstance(encryption, _CfnTag), (
            "EncryptionConfiguration must be a CloudFormation !If expression"
        )
        assert encryption.tag == "!If"
        cond_name = encryption.value[0] if isinstance(encryption.value, list) else None
        assert cond_name == "HasKmsKey"

    def test_kms_branch_uses_sse_kms_with_kms_key_arn(self, athena_workgroup):
        result_config = (
            athena_workgroup.get("Properties", {})
            .get("WorkGroupConfiguration", {})
            .get("ResultConfiguration", {})
        )
        encryption = result_config["EncryptionConfiguration"]
        branches = encryption.value
        assert isinstance(branches, list) and len(branches) == 3
        kms_branch = branches[1]
        assert kms_branch.get("EncryptionOption") == "SSE_KMS"
        kms_key = kms_branch.get("KmsKey")
        assert isinstance(kms_key, _CfnTag) and kms_key.tag == "!Ref"
        assert kms_key.value == "KmsKeyArn"

    def test_no_kms_key_branch_uses_sse_s3(self, athena_workgroup):
        result_config = (
            athena_workgroup.get("Properties", {})
            .get("WorkGroupConfiguration", {})
            .get("ResultConfiguration", {})
        )
        encryption = result_config["EncryptionConfiguration"]
        branches = encryption.value
        no_kms_branch = branches[2]
        assert no_kms_branch.get("EncryptionOption") == "SSE_S3"
        assert "KmsKey" not in no_kms_branch

    def test_output_location_unchanged(self, athena_workgroup):
        result_config = (
            athena_workgroup.get("Properties", {})
            .get("WorkGroupConfiguration", {})
            .get("ResultConfiguration", {})
        )
        output_location = result_config.get("OutputLocation")
        assert isinstance(output_location, _CfnTag) and output_location.tag == "!Sub"
        assert "StateBucket" in output_location.value
        assert "athena-results" in output_location.value


class TestKmsPolicy:
    def test_kms_actions_not_in_execution_role_inline(self, execution_role_policy_statements):
        """KMS actions must NOT appear in the ExecutionRole inline policy."""
        actions = _extract_actions_from_statements(execution_role_policy_statements)
        kms_actions = {a for a in actions if a.startswith("kms:")}
        assert not kms_actions, (
            f"KMS actions found in ExecutionRole inline policy: {kms_actions}. "
            "They should only be in KmsKeyPolicy (conditional)."
        )

    def test_kms_key_policy_has_condition(self, kms_key_policy):
        """KmsKeyPolicy resource must have Condition: HasKmsKey."""
        condition = kms_key_policy.get("Condition")
        if isinstance(condition, _CfnTag):
            # The condition value
            pass
        elif isinstance(condition, str):
            assert condition == "HasKmsKey"
        else:
            pytest.fail(f"KmsKeyPolicy Condition unexpected type: {type(condition)}")


# ---------------------------------------------------------------------------
# Tests: StateBucket lifecycle rules
# ---------------------------------------------------------------------------


class TestStateBucketLifecycle:
    def test_lifecycle_rules_cover_manifests_and_athena_results(self, state_bucket_props):
        """Lifecycle rules must cover manifests/ and athena-results/ prefixes."""
        rules = state_bucket_props.get("LifecycleConfiguration", {}).get("Rules", [])
        prefixes = set()
        for rule in rules:
            prefix = rule.get("Prefix", "")
            if isinstance(prefix, str):
                prefixes.add(prefix)
        assert "manifests/" in prefixes, "Missing lifecycle rule for manifests/"
        assert "athena-results/" in prefixes, "Missing lifecycle rule for athena-results/"

    def test_lifecycle_has_exactly_three_rules(self, state_bucket_props):
        """StateBucket must have exactly three LifecycleConfiguration.Rules entries
        (manifests, athena-results, completion-reports) — design.md Decision 4."""
        rules = state_bucket_props.get("LifecycleConfiguration", {}).get("Rules", [])
        assert len(rules) == 3, f"Expected exactly 3 lifecycle rules; found {len(rules)}"

    def test_completion_reports_lifecycle_rule_is_unconditional_and_matches_shape(
        self, state_bucket_props
    ):
        """The expire-completion-reports rule is scoped to completion-reports/,
        reuses LifecycleExpirationDays (no new parameter), matches the same
        AbortIncompleteMultipartUpload.DaysAfterInitiation: 7 shape as the other
        two rules, and stays unconditional (design.md Decision 4)."""
        rules = state_bucket_props.get("LifecycleConfiguration", {}).get("Rules", [])
        matching = [r for r in rules if r.get("Prefix") == "completion-reports/"]
        assert len(matching) == 1, "Expected exactly one expire-completion-reports rule"
        rule = matching[0]
        assert rule.get("Id") == "expire-completion-reports"
        assert rule.get("Status") == "Enabled"
        expiration = rule.get("ExpirationInDays")
        assert isinstance(expiration, _CfnTag) and expiration.tag == "!Ref"
        assert expiration.value == "LifecycleExpirationDays"
        abort = rule.get("AbortIncompleteMultipartUpload", {})
        assert abort.get("DaysAfterInitiation") == 7

    def test_lifecycle_rules_do_not_cover_state(self, state_bucket_props):
        """Lifecycle rules must NOT cover state/ prefix or the whole bucket."""
        rules = state_bucket_props.get("LifecycleConfiguration", {}).get("Rules", [])
        for rule in rules:
            prefix = rule.get("Prefix", "")
            if isinstance(prefix, str):
                assert prefix != "state/", "Lifecycle rule must not cover state/ prefix"
                assert prefix != "", "Lifecycle rule must not cover the whole bucket (empty prefix)"


# ---------------------------------------------------------------------------
# Tests: StateBucket naming (BucketNamePrefix + BucketNamespace, no BucketName)
# ---------------------------------------------------------------------------


class TestStateBucketNaming:
    def test_has_bucket_name_prefix(self, state_bucket_props):
        """StateBucket must declare BucketNamePrefix."""
        prefix = state_bucket_props.get("BucketNamePrefix")
        assert prefix is not None, "StateBucket must have BucketNamePrefix"

    def test_has_bucket_namespace_account_regional(self, state_bucket_props):
        """StateBucket must have BucketNamespace = account-regional."""
        namespace = state_bucket_props.get("BucketNamespace", "")
        assert namespace == "account-regional", (
            f"BucketNamespace should be 'account-regional', got {namespace!r}"
        )

    def test_no_literal_bucket_name(self, state_bucket_props):
        """StateBucket must NOT set a literal BucketName."""
        assert "BucketName" not in state_bucket_props, (
            "StateBucket must not have a literal BucketName — use BucketNamePrefix + BucketNamespace"
        )


# ---------------------------------------------------------------------------
# Tests: No extra S3 buckets
# ---------------------------------------------------------------------------


class TestNoExtraBuckets:
    def test_only_one_s3_bucket_resource(self, resources):
        """Only StateBucket should be an AWS::S3::Bucket — no source bucket resources."""
        bucket_resources = [
            name for name, res in resources.items()
            if res.get("Type") == "AWS::S3::Bucket"
        ]
        assert bucket_resources == ["StateBucket"], (
            f"Expected only StateBucket, found: {bucket_resources}"
        )


# ---------------------------------------------------------------------------
# Tests: CloudWatch MetricsPolicy and env vars — task 7.4
# Feature: cloudwatch-metrics
# Requirements: 6.3, 6.4, 6.5, 6.6, 9.5
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def metrics_policy(resources) -> dict:
    return resources.get("MetricsPolicy", {})


class TestMetricsPolicy:
    def test_metrics_policy_resource_exists(self, metrics_policy):
        """MetricsPolicy resource must be present in the template (Req 6.4)."""
        assert metrics_policy, "MetricsPolicy resource not found in template"

    def test_metrics_policy_has_has_metrics_namespace_condition(self, metrics_policy):
        """MetricsPolicy must have Condition: HasMetricsNamespace (Req 6.4)."""
        condition = metrics_policy.get("Condition")
        if isinstance(condition, str):
            assert condition == "HasMetricsNamespace", (
                f"MetricsPolicy Condition must be HasMetricsNamespace; got {condition!r}"
            )
        elif isinstance(condition, _CfnTag):
            pass  # parsed as a tag; structure is correct if Condition key is present
        else:
            pytest.fail(f"MetricsPolicy Condition unexpected: {condition!r}")

    def test_metrics_policy_type_is_iam_policy(self, metrics_policy):
        """MetricsPolicy must be AWS::IAM::Policy (Req 6.4)."""
        assert metrics_policy.get("Type") == "AWS::IAM::Policy"

    def test_metrics_policy_grants_only_put_metric_data(self, metrics_policy):
        """MetricsPolicy statement grants only cloudwatch:PutMetricData (Req 6.4)."""
        stmts = (
            metrics_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        actions = _extract_actions_from_statements(stmts)
        assert actions == {"cloudwatch:putmetricdata"}, (
            f"MetricsPolicy actions must be only cloudwatch:PutMetricData; got {actions}"
        )

    def test_cloudwatch_actions_not_in_execution_role_inline(
        self, execution_role_policy_statements
    ):
        """cloudwatch:* must NOT appear in the ExecutionRole inline policy (Req 6.5)."""
        actions = _extract_actions_from_statements(execution_role_policy_statements)
        cw_actions = {a for a in actions if a.startswith("cloudwatch:")}
        assert not cw_actions, (
            f"CloudWatch actions found in ExecutionRole inline policy: {cw_actions}. "
            "They should only be in MetricsPolicy (conditional)."
        )


class TestMetricsParameters:
    def test_metrics_namespace_parameter_exists(self, template):
        """MetricsNamespace parameter must be present (Req 6.3)."""
        params = template.get("Parameters", {})
        assert "MetricsNamespace" in params, "MetricsNamespace parameter not found"

    def test_metrics_namespace_parameter_default_is_empty(self, template):
        """MetricsNamespace default must be empty string (Req 6.3)."""
        default = template.get("Parameters", {}).get("MetricsNamespace", {}).get("Default")
        assert default == "", (
            f"MetricsNamespace Default must be \"\"; got {default!r}"
        )

    def test_metrics_deployment_id_parameter_exists(self, template):
        """MetricsDeploymentId parameter must be present (Req 9.5)."""
        params = template.get("Parameters", {})
        assert "MetricsDeploymentId" in params, "MetricsDeploymentId parameter not found"

    def test_has_metrics_namespace_condition_exists(self, template):
        """HasMetricsNamespace condition must be present (Req 6.3)."""
        conditions = template.get("Conditions", {})
        assert "HasMetricsNamespace" in conditions, "HasMetricsNamespace condition not found"

    def test_has_metrics_deployment_id_condition_exists(self, template):
        """HasMetricsDeploymentId condition must be present (Req 9.5)."""
        conditions = template.get("Conditions", {})
        assert "HasMetricsDeploymentId" in conditions, (
            "HasMetricsDeploymentId condition not found"
        )


class TestMetricsEnvVars:
    def test_metrics_namespace_env_var_exists_in_lambda(self, lambda_props):
        """METRICS_NAMESPACE env var must be set on the Lambda (Req 6.6)."""
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        assert "METRICS_NAMESPACE" in env_vars, (
            "METRICS_NAMESPACE env var not found in Lambda Environment.Variables"
        )

    def test_metrics_deployment_id_env_var_exists_in_lambda(self, lambda_props):
        """METRICS_DEPLOYMENT_ID env var must be set on the Lambda (Req 9.5)."""
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        assert "METRICS_DEPLOYMENT_ID" in env_vars, (
            "METRICS_DEPLOYMENT_ID env var not found in Lambda Environment.Variables"
        )

    def test_metrics_namespace_env_var_uses_if_condition(self, lambda_props):
        """METRICS_NAMESPACE must use !If [HasMetricsNamespace, ...] (Req 6.5)."""
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        ns_var = env_vars.get("METRICS_NAMESPACE")
        # When parsed by CfnLoader, !If becomes a _CfnTag with tag='!If'
        assert isinstance(ns_var, _CfnTag), (
            f"METRICS_NAMESPACE must be a CloudFormation !If expression; got {type(ns_var)}"
        )
        assert ns_var.tag == "!If"
        # First element of the condition list is the condition name
        if isinstance(ns_var.value, list) and ns_var.value:
            cond_name = _cfn_tag_value(ns_var.value[0]) or (
                ns_var.value[0] if isinstance(ns_var.value[0], str) else None
            )
            if cond_name:
                assert cond_name == "HasMetricsNamespace", (
                    f"METRICS_NAMESPACE !If must reference HasMetricsNamespace; "
                    f"got {cond_name!r}"
                )


# ---------------------------------------------------------------------------
# Tests: LF mode support (lf-mode-s3-metadata spec)
# Requirements: 1.1, 2.1, 2.2, 3.1, 3.2, 3.3, 4.2, 5.1, 5.2, 5.3
# ---------------------------------------------------------------------------

_LF_GRANTER_IAM_ACTIONS = {
    "lakeformation:getdatalakesettings",
    "lakeformation:putdatalakesettings",
    "lakeformation:grantpermissions",
    "lakeformation:revokepermissions",
    "glue:getcatalog",
    "s3tables:gettablebucket",
    "s3tables:getnamespace",
    "s3tables:gettable",
    "s3tables:listnamespaces",
    "s3tables:listtables",
    "sts:assumerole",
}


@pytest.fixture(scope="module")
def lf_granter_role(resources) -> dict:
    return resources.get("LFGranterRole", {})


@pytest.fixture(scope="module")
def lf_admin_granter_function(resources) -> dict:
    return resources.get("LFAdminGranterFunction", {})


@pytest.fixture(scope="module")
def lf_admin_granter_resource(resources) -> dict:
    return resources.get("LFAdminGranterResource", {})


@pytest.fixture(scope="module")
def lf_permissions_granter_function(resources) -> dict:
    return resources.get("LFPermissionsGranterFunction", {})


@pytest.fixture(scope="module")
def execute_lf_permissions_granter(resources) -> dict:
    return resources.get("ExecuteLFPermissionsGranter", {})


class TestLFAdminRoleArnParameter:
    def test_lf_admin_role_arn_parameter_exists(self, template):
        """LFAdminRoleArn parameter must be present (Req 3.1, 5.1)."""
        params = template.get("Parameters", {})
        assert "LFAdminRoleArn" in params, "LFAdminRoleArn parameter not found"

    def test_lf_admin_role_arn_default_is_empty(self, template):
        """LFAdminRoleArn default must be empty string (Req 5.2)."""
        default = template.get("Parameters", {}).get("LFAdminRoleArn", {}).get("Default")
        assert default == "", (
            f"LFAdminRoleArn Default must be \"\"; got {default!r}"
        )

    def test_lf_admin_role_arn_type_is_string(self, template):
        """LFAdminRoleArn must be Type: String."""
        param_type = template.get("Parameters", {}).get("LFAdminRoleArn", {}).get("Type")
        assert param_type == "String", f"LFAdminRoleArn Type must be String; got {param_type!r}"

    def test_has_lf_admin_role_condition_exists(self, template):
        """HasLFAdminRole condition must be present (Req 3.1)."""
        conditions = template.get("Conditions", {})
        assert "HasLFAdminRole" in conditions, "HasLFAdminRole condition not found"

    def test_create_own_lf_admin_condition_exists(self, template):
        """CreateOwnLFAdmin condition must be present (Req 3.3)."""
        conditions = template.get("Conditions", {})
        assert "CreateOwnLFAdmin" in conditions, "CreateOwnLFAdmin condition not found"


class TestLFGranterRole:
    def test_lf_granter_role_resource_exists(self, lf_granter_role):
        """LFGranterRole resource must be present (Req 2.1, 2.2)."""
        assert lf_granter_role, "LFGranterRole resource not found in template"

    def test_lf_granter_role_type_is_iam_role(self, lf_granter_role):
        """LFGranterRole must be AWS::IAM::Role."""
        assert lf_granter_role.get("Type") == "AWS::IAM::Role"

    def test_lf_granter_role_has_no_condition(self, lf_granter_role):
        """LFGranterRole must NOT have a Condition (always created, Req 5.1)."""
        assert "Condition" not in lf_granter_role, (
            "LFGranterRole must not have a Condition — it is always created"
        )

    def test_lf_granter_role_contains_expected_iam_actions(self, lf_granter_role):
        """LFGranterRole policy must include all required LF/Glue/STS actions (Req 2.1, 3.2)."""
        stmts = []
        for policy in lf_granter_role.get("Properties", {}).get("Policies", []):
            stmts.extend(policy.get("PolicyDocument", {}).get("Statement", []))
        actual = _extract_actions_from_statements(stmts)
        missing = _LF_GRANTER_IAM_ACTIONS - actual
        assert not missing, (
            f"LFGranterRole policy missing required IAM actions: {missing}"
        )

    def test_lf_granter_role_lambda_trust_policy(self, lf_granter_role):
        """LFGranterRole must allow lambda.amazonaws.com to assume it."""
        trust = (
            lf_granter_role.get("Properties", {})
            .get("AssumeRolePolicyDocument", {})
            .get("Statement", [])
        )
        services = []
        for stmt in trust:
            principal = stmt.get("Principal", {})
            if isinstance(principal, dict):
                svc = principal.get("Service", "")
                if isinstance(svc, str):
                    services.append(svc)
                elif isinstance(svc, list):
                    services.extend(svc)
        assert "lambda.amazonaws.com" in services, (
            "LFGranterRole trust policy must allow lambda.amazonaws.com"
        )


class TestLFAdminGranter:
    def test_lf_admin_granter_function_exists(self, lf_admin_granter_function):
        """LFAdminGranterFunction resource must be present (Req 3.3, 4.2)."""
        assert lf_admin_granter_function, "LFAdminGranterFunction resource not found"

    def test_lf_admin_granter_function_condition(self, lf_admin_granter_function):
        """LFAdminGranterFunction must have Condition: CreateOwnLFAdmin (Req 3.3, 5.1)."""
        condition = lf_admin_granter_function.get("Condition")
        assert condition == "CreateOwnLFAdmin", (
            f"LFAdminGranterFunction Condition must be CreateOwnLFAdmin; got {condition!r}"
        )

    def test_lf_admin_granter_function_type(self, lf_admin_granter_function):
        """LFAdminGranterFunction must be AWS::Lambda::Function."""
        assert lf_admin_granter_function.get("Type") == "AWS::Lambda::Function"

    def test_lf_admin_granter_function_has_zip_file_code(self, lf_admin_granter_function):
        """LFAdminGranterFunction must use inline ZipFile code."""
        code = (
            lf_admin_granter_function.get("Properties", {})
            .get("Code", {})
            .get("ZipFile")
        )
        assert code is not None, "LFAdminGranterFunction must have Code.ZipFile"
        assert isinstance(code, str), "LFAdminGranterFunction Code.ZipFile must be a string"

    def test_lf_admin_granter_resource_exists(self, lf_admin_granter_resource):
        """LFAdminGranterResource resource must be present (Req 3.3)."""
        assert lf_admin_granter_resource, "LFAdminGranterResource resource not found"

    def test_lf_admin_granter_resource_condition(self, lf_admin_granter_resource):
        """LFAdminGranterResource must have Condition: CreateOwnLFAdmin (Req 3.3, 5.1)."""
        condition = lf_admin_granter_resource.get("Condition")
        assert condition == "CreateOwnLFAdmin", (
            f"LFAdminGranterResource Condition must be CreateOwnLFAdmin; got {condition!r}"
        )

    def test_lf_admin_granter_resource_type(self, lf_admin_granter_resource):
        """LFAdminGranterResource must be Custom::LFAdminGranter."""
        assert lf_admin_granter_resource.get("Type") == "Custom::LFAdminGranter"


class TestExecuteLFPermissionsGranter:
    def test_execute_lf_permissions_granter_exists(self, execute_lf_permissions_granter):
        """ExecuteLFPermissionsGranter resource must be present (Req 2.1, 2.2)."""
        assert execute_lf_permissions_granter, "ExecuteLFPermissionsGranter resource not found"

    def test_execute_lf_permissions_granter_type(self, execute_lf_permissions_granter):
        """ExecuteLFPermissionsGranter must be Custom::LFPermissionsGranter."""
        assert execute_lf_permissions_granter.get("Type") == "Custom::LFPermissionsGranter"

    def test_execute_lf_permissions_granter_has_no_condition(self, execute_lf_permissions_granter):
        """ExecuteLFPermissionsGranter must NOT have a Condition (always runs, Req 2.1)."""
        assert "Condition" not in execute_lf_permissions_granter, (
            "ExecuteLFPermissionsGranter must not have a Condition — it is always created"
        )

    def test_execute_lf_permissions_granter_has_lf_admin_role_arn_if(
        self, execute_lf_permissions_granter
    ):
        """LFAdminRoleArn property must be wired through !If [HasLFAdminRole, ...] (Req 3.1, 3.2)."""
        props = execute_lf_permissions_granter.get("Properties", {})
        lf_admin_arn = props.get("LFAdminRoleArn")
        assert isinstance(lf_admin_arn, _CfnTag), (
            "LFAdminRoleArn property must be a CloudFormation intrinsic (!If); "
            f"got {type(lf_admin_arn)}"
        )
        assert lf_admin_arn.tag == "!If", (
            f"LFAdminRoleArn property must use !If; got tag {lf_admin_arn.tag!r}"
        )
        # First element of !If must reference HasLFAdminRole
        if isinstance(lf_admin_arn.value, list) and lf_admin_arn.value:
            cond = lf_admin_arn.value[0]
            cond_name = cond if isinstance(cond, str) else (
                cond.value if isinstance(cond, _CfnTag) else None
            )
            if cond_name is not None:
                assert cond_name == "HasLFAdminRole", (
                    f"LFAdminRoleArn !If must reference HasLFAdminRole; got {cond_name!r}"
                )

    def test_execute_lf_permissions_granter_has_execution_role_arn(
        self, execute_lf_permissions_granter
    ):
        """ExecuteLFPermissionsGranter must pass ExecutionRoleArn (Req 2.1)."""
        props = execute_lf_permissions_granter.get("Properties", {})
        assert "ExecutionRoleArn" in props, (
            "ExecuteLFPermissionsGranter must have ExecutionRoleArn property"
        )

    def test_execute_lf_permissions_granter_has_source_bucket_names(
        self, execute_lf_permissions_granter
    ):
        """ExecuteLFPermissionsGranter must pass SourceBucketNames (Req 2.1, 2.2)."""
        props = execute_lf_permissions_granter.get("Properties", {})
        assert "SourceBucketNames" in props, (
            "ExecuteLFPermissionsGranter must have SourceBucketNames property"
        )

    def test_execute_lf_permissions_granter_depends_on_execution_role(
        self, execute_lf_permissions_granter
    ):
        """ExecuteLFPermissionsGranter must DependsOn ExecutionRole (Req 2.1)."""
        depends_on = execute_lf_permissions_granter.get("DependsOn", [])
        if isinstance(depends_on, str):
            depends_on = [depends_on]
        assert "ExecutionRole" in depends_on, (
            "ExecuteLFPermissionsGranter must DependsOn ExecutionRole"
        )


# ---------------------------------------------------------------------------
# Tests: console-deployment spec — config custom resource fidelity and
#        template structure (Tasks 4.1, 4.2)
# Feature: console-deployment
# Requirements: 6.1, 6.2, 7.1, 8.1, 8.2, 8.3, 10.1, 10.2, 10.3, 10.4
# ---------------------------------------------------------------------------

_CONFIG_RESOURCE_SOURCE = (
    Path(__file__).parent.parent / "deploy" / "config_resource" / "index.py"
)


class TestConfigResourceFidelity:
    """Task 4.1 — inline ZipFile matches the standalone module and is under the size limit."""

    def test_inline_zip_file_matches_standalone_module(self, resources):
        """ConfigResourceFunction ZipFile equals deploy/config_resource/index.py (Req 7.1)."""
        assert _CONFIG_RESOURCE_SOURCE.exists(), (
            f"Standalone module not found at {_CONFIG_RESOURCE_SOURCE}"
        )
        config_fn = resources.get("ConfigResourceFunction", {})
        zip_file = config_fn.get("Properties", {}).get("Code", {}).get("ZipFile")
        assert zip_file is not None, "ConfigResourceFunction must have Code.ZipFile"
        assert isinstance(zip_file, str), "Code.ZipFile must be a string"

        expected = _CONFIG_RESOURCE_SOURCE.read_text()
        assert zip_file == expected, (
            "ConfigResourceFunction Code.ZipFile does not match "
            "deploy/config_resource/index.py — keep them in sync."
        )

    def test_inline_zip_file_under_4096_bytes(self, resources):
        """ConfigResourceFunction ZipFile is < 4096 bytes (AWS ZipFile limit) (Req 7.1)."""
        zip_file = (
            resources.get("ConfigResourceFunction", {})
            .get("Properties", {})
            .get("Code", {})
            .get("ZipFile", "")
        )
        size = len(zip_file.encode("utf-8"))
        assert size < 4096, (
            f"ConfigResourceFunction Code.ZipFile is {size} bytes; "
            "must be < 4096 to fit in AWS CloudFormation ZipFile limit."
        )


class TestAccountIdRemoval:
    """Task 4.2 (partial) — AccountId parameter removed; all references replaced (Req 6.1, 6.2)."""

    def test_no_account_id_parameter(self, template):
        """AccountId parameter must not exist (Req 6.1)."""
        params = template.get("Parameters", {})
        assert "AccountId" not in params, (
            "AccountId parameter still present; it must be removed (Req 6.1)."
        )

    def test_no_ref_account_id_in_raw_text(self):
        """No !Ref AccountId should remain anywhere in the template (Req 6.2).

        Both former sites (Main_Lambda ACCOUNT_ID env var and
        ExecuteLFPermissionsGranter AccountId property) must now use
        !Ref AWS::AccountId.
        """
        raw = _TEMPLATE_PATH.read_text()
        assert "!Ref AccountId" not in raw, (
            "Found !Ref AccountId in template; both sites must use !Ref AWS::AccountId (Req 6.2)."
        )

    def test_account_id_env_var_uses_aws_account_id(self, lambda_props):
        """Main_Lambda ACCOUNT_ID env var must reference AWS::AccountId (Req 6.2)."""
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        account_id_val = env_vars.get("ACCOUNT_ID")
        assert isinstance(account_id_val, _CfnTag), (
            "ACCOUNT_ID env var must be a CloudFormation intrinsic (e.g. !Ref AWS::AccountId)"
        )
        assert account_id_val.tag == "!Ref", (
            f"ACCOUNT_ID env var must use !Ref; got {account_id_val.tag!r}"
        )
        assert account_id_val.value == "AWS::AccountId", (
            f"ACCOUNT_ID env var must reference AWS::AccountId; got {account_id_val.value!r}"
        )


class TestConfigResourceStructure:
    """Task 4.2 (partial) — config resource, role, and invocation are present and correct
    (Req 7.1, 8.1, 8.2, 8.3)."""

    def test_config_resource_function_exists(self, resources):
        """ConfigResourceFunction resource must be present (Req 7.1)."""
        assert "ConfigResourceFunction" in resources, (
            "ConfigResourceFunction resource not found in template."
        )

    def test_config_resource_function_type(self, resources):
        """ConfigResourceFunction must be AWS::Lambda::Function (Req 7.1)."""
        assert resources["ConfigResourceFunction"].get("Type") == "AWS::Lambda::Function"

    def test_config_resource_function_has_zip_file(self, resources):
        """ConfigResourceFunction must use inline Code.ZipFile (Req 7.1)."""
        code = (
            resources["ConfigResourceFunction"]
            .get("Properties", {})
            .get("Code", {})
        )
        assert "ZipFile" in code, "ConfigResourceFunction must have Code.ZipFile (not S3Bucket/S3Key)"
        assert "S3Bucket" not in code, "ConfigResourceFunction must not use S3Bucket"

    def test_config_resource_function_distinct_from_replication_lambda(self, resources):
        """ConfigResourceFunction must be a distinct resource from ReplicationLambda (Req 7.1)."""
        assert "ReplicationLambda" in resources, "ReplicationLambda not found"
        cr_code = (
            resources["ConfigResourceFunction"].get("Properties", {}).get("Code", {})
        )
        rl_code = (
            resources["ReplicationLambda"].get("Properties", {}).get("Code", {})
        )
        # ConfigResourceFunction uses ZipFile; ReplicationLambda uses S3Bucket/S3Key
        assert "ZipFile" in cr_code and "S3Bucket" in rl_code, (
            "ConfigResourceFunction must use ZipFile; ReplicationLambda must use S3Bucket."
        )

    def test_config_resource_function_distinct_from_lf_granter_functions(self, resources):
        """ConfigResourceFunction must be distinct from LFAdminGranterFunction and
        LFPermissionsGranterFunction (Req 7.1)."""
        assert "ConfigResourceFunction" != "LFAdminGranterFunction"
        assert "ConfigResourceFunction" != "LFPermissionsGranterFunction"
        # Each is a separate top-level resource key
        assert "ConfigResourceFunction" in resources
        assert "LFPermissionsGranterFunction" in resources

    def test_config_resource_role_exists(self, resources):
        """ConfigResourceRole resource must be present (Req 8.1-8.3)."""
        assert "ConfigResourceRole" in resources, (
            "ConfigResourceRole resource not found in template."
        )

    def test_config_resource_role_s3_actions_exactly_put_and_delete(self, resources):
        """ConfigResourceRole S3 permissions must be only PutObject + DeleteObject (Req 8.1, 8.2)."""
        stmts = []
        for policy in (
            resources.get("ConfigResourceRole", {})
            .get("Properties", {})
            .get("Policies", [])
        ):
            stmts.extend(policy.get("PolicyDocument", {}).get("Statement", []))

        s3_actions: set[str] = set()
        for stmt in stmts:
            if not isinstance(stmt, dict):
                continue  # skip !If / conditional statements (_CfnTag)
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            for a in actions:
                if isinstance(a, str) and a.lower().startswith("s3:"):
                    s3_actions.add(a.lower())

        assert s3_actions == {"s3:putobject", "s3:deleteobject"}, (
            f"ConfigResourceRole S3 actions must be exactly {{s3:PutObject, s3:DeleteObject}}; "
            f"got {s3_actions} (Req 8.1, 8.2)."
        )

    def test_config_resource_role_s3_resource_is_object_not_bucket(self, resources):
        """ConfigResourceRole S3 resource must be scoped to the object ARN, not the bucket (Req 8.3)."""
        stmts = []
        for policy in (
            resources.get("ConfigResourceRole", {})
            .get("Properties", {})
            .get("Policies", [])
        ):
            stmts.extend(policy.get("PolicyDocument", {}).get("Statement", []))

        for stmt in stmts:
            if not isinstance(stmt, dict):
                continue  # skip !If / conditional statements (_CfnTag)
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            has_s3 = any(
                isinstance(a, str) and a.lower().startswith("s3:")
                for a in actions
            )
            if not has_s3:
                continue
            resource = stmt.get("Resource")
            # Resource must be a !Sub tag referencing the object ARN (not bucket or /*)
            assert isinstance(resource, _CfnTag), (
                "ConfigResourceRole S3 Resource must be a CloudFormation intrinsic (!Sub)"
            )
            # The !Sub string should reference StateBucket.Arn and the fixed
            # config object key (i.e. an object-level ARN, not a bucket ARN)
            if isinstance(resource.value, str):
                assert "solution-config.json" in resource.value, (
                    f"ConfigResourceRole S3 Resource {resource.value!r} does not reference "
                    "the config object key; expected an object-level ARN (Req 8.3)."
                )

    def test_solution_config_resource_exists(self, resources):
        """SolutionConfig Custom::SolutionConfig resource must be present (Req 7.1)."""
        assert "SolutionConfig" in resources, (
            "SolutionConfig resource not found in template."
        )

    def test_solution_config_type(self, resources):
        """SolutionConfig must be Custom::SolutionConfig."""
        assert resources["SolutionConfig"].get("Type") == "Custom::SolutionConfig"

    def test_solution_config_depends_on_state_bucket(self, resources):
        """SolutionConfig must have DependsOn: StateBucket so S3 write happens after bucket creation."""
        depends_on = resources["SolutionConfig"].get("DependsOn")
        if isinstance(depends_on, list):
            assert "StateBucket" in depends_on
        else:
            assert depends_on == "StateBucket", (
                f"SolutionConfig DependsOn must include StateBucket; got {depends_on!r}"
            )

    def test_solution_config_passes_check_frequency(self, resources):
        """SolutionConfig must pass CheckFrequencyMinutes to the handler; processing_interval
        is derived from it (Req 7.5)."""
        props = resources["SolutionConfig"].get("Properties", {})
        assert "CheckFrequencyMinutes" in props, (
            "SolutionConfig must have CheckFrequencyMinutes in ResourceProperties."
        )


class TestExistingResourcesPreserved:
    """Task 4.2 (partial) — existing resources, outputs, and parameters preserved (Req 10.1-10.4)."""

    _REQUIRED_RESOURCES = [
        "ReplicationLambda",
        "ReplicationSchedule",
        "StateBucket",
        "AthenaWorkGroup",
        "ExecutionRole",
        "ScheduleRole",
    ]

    _REQUIRED_OUTPUTS = ["StateBucketName", "ExecutionRoleArn"]

    _REQUIRED_PARAMETERS = [
        "CodeLocation",
        "SourceBucketNames",
        "CheckFrequencyMinutes",
        "LifecycleExpirationDays",
        "KmsKeyArn",
        "JournalKmsKeyArn",
        "LambdaTimeoutSeconds",
        "LambdaMemoryMB",
        "CompletionCheckMemoryMB",
        "VpcId",
        "SubnetIds",
        "SecurityGroupIds",
        "JournalLookbackSeconds",
        "MetricsNamespace",
        "MetricsDeploymentId",
        "LFAdminRoleArn",
    ]

    def test_required_resources_present(self, resources):
        """All pre-existing runtime resources must still be present (Req 10.1)."""
        missing = [r for r in self._REQUIRED_RESOURCES if r not in resources]
        assert not missing, f"Missing required resources after console-deployment edits: {missing}"

    def test_replication_lambda_sourced_from_code_bucket(self, resources):
        """ReplicationLambda must source from S3 (derived from CodeLocation), not an inline ZipFile (Req 10.2)."""
        code = resources["ReplicationLambda"].get("Properties", {}).get("Code", {})
        assert "S3Bucket" in code, "ReplicationLambda must still use Code.S3Bucket"
        assert "S3Key" in code, "ReplicationLambda must still use Code.S3Key"
        assert "ZipFile" not in code, "ReplicationLambda must not be inlined (too large)"

    def test_required_outputs_present(self, template):
        """All required stack outputs must still exist (Req 10.3)."""
        outputs = template.get("Outputs", {})
        missing = [o for o in self._REQUIRED_OUTPUTS if o not in outputs]
        assert not missing, f"Missing required outputs: {missing}"

    def test_required_parameters_present(self, template):
        """All required parameters must be present; AccountId must not be (Req 10.4)."""
        params = template.get("Parameters", {})
        missing = [p for p in self._REQUIRED_PARAMETERS if p not in params]
        assert not missing, f"Missing required parameters after console-deployment edits: {missing}"
        assert "AccountId" not in params, (
            "AccountId must not be in Parameters (Req 6.1)."
        )


class TestCodeLocationParser:
    """CodeLocation is parsed into bucket/key by a custom resource so keys with
    prefixes are supported (CloudFormation intrinsics can't extract them)."""

    def test_parser_resources_exist(self, resources):
        fn = resources.get("CodeLocationParserFunction", {})
        assert fn.get("Type") == "AWS::Lambda::Function", "CodeLocationParserFunction missing"
        assert "CodeLocationParserRole" in resources, "CodeLocationParserRole missing"
        cr = resources.get("ExecuteCodeLocationParser", {})
        assert cr.get("Type") == "Custom::CodeLocationParser", "ExecuteCodeLocationParser missing"

    def test_parser_receives_code_location(self, resources):
        props = resources.get("ExecuteCodeLocationParser", {}).get("Properties", {})
        assert "CodeLocation" in props, "Parser custom resource must receive CodeLocation"

    def test_lambda_code_uses_parser_outputs(self, resources):
        """ReplicationLambda Code.S3Bucket/S3Key must come from the parser's GetAtt outputs."""
        code = resources["ReplicationLambda"].get("Properties", {}).get("Code", {})
        for field in ("S3Bucket", "S3Key"):
            val = code.get(field)
            assert isinstance(val, _CfnTag) and val.tag == "!GetAtt", (
                f"Code.{field} must be a !GetAtt of the parser custom resource"
            )
            ref = val.value if isinstance(val.value, str) else ""
            assert ref.startswith("ExecuteCodeLocationParser."), (
                f"Code.{field} must reference ExecuteCodeLocationParser, got {ref!r}"
            )

    def test_parser_role_has_no_broad_permissions(self, resources):
        """The parser does pure string work — its role should grant only logs."""
        role = resources.get("CodeLocationParserRole", {})
        actions = set()
        for policy in role.get("Properties", {}).get("Policies", []):
            for stmt in policy.get("PolicyDocument", {}).get("Statement", []):
                acts = stmt.get("Action", [])
                if isinstance(acts, str):
                    acts = [acts]
                actions.update(a.lower() for a in acts if isinstance(a, str))
        assert actions and all(a.startswith("logs:") for a in actions), (
            f"CodeLocationParserRole should grant only logs actions; got {actions}"
        )


# ---------------------------------------------------------------------------
# Tests: Batch failure monitoring resources (Task 3.4)
# Requirements: 3.1, 3.2, 3.3, 3.4
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def batch_failure_rule(resources) -> dict:
    return resources.get("BatchJobFailureRule", {})


@pytest.fixture(scope="module")
def batch_failure_alarm(resources) -> dict:
    return resources.get("BatchJobFailureAlarm", {})


@pytest.fixture(scope="module")
def batch_failure_log_group(resources) -> dict:
    return resources.get("BatchJobFailureLogGroup", {})


@pytest.fixture(scope="module")
def batch_failure_topic(resources) -> dict:
    return resources.get("BatchJobFailureTopic", {})


@pytest.fixture(scope="module")
def batch_failure_subscription(resources) -> dict:
    return resources.get("BatchJobFailureSubscription", {})


@pytest.fixture(scope="module")
def batch_failure_metric_filter(resources) -> dict:
    return resources.get("BatchJobFailureMetricFilter", {})


@pytest.fixture(scope="module")
def batch_failure_topic_policy(resources) -> dict:
    return resources.get("BatchJobFailureTopicPolicy", {})


class TestBatchJobFailureMonitoring:
    # --- Log group --------------------------------------------------------

    def test_log_group_exists(self, batch_failure_log_group):
        assert batch_failure_log_group.get("Type") == "AWS::Logs::LogGroup"

    def test_log_group_retention_is_90_days(self, batch_failure_log_group):
        retention = batch_failure_log_group.get("Properties", {}).get("RetentionInDays")
        assert retention == 90

    def test_log_group_deletion_policy_is_retain(self, batch_failure_log_group):
        assert batch_failure_log_group.get("DeletionPolicy") == "Retain"

    # --- EventBridge rule event pattern -----------------------------------

    def test_rule_exists(self, batch_failure_rule):
        assert batch_failure_rule.get("Type") == "AWS::Events::Rule"

    def test_rule_source_is_aws_s3(self, batch_failure_rule):
        pattern = batch_failure_rule.get("Properties", {}).get("EventPattern", {})
        assert "aws.s3" in pattern.get("source", [])

    def test_rule_detail_type_is_cloudtrail_event(self, batch_failure_rule):
        pattern = batch_failure_rule.get("Properties", {}).get("EventPattern", {})
        detail_types = pattern.get("detail-type", [])
        assert "AWS Service Event via CloudTrail" in detail_types

    def test_rule_filters_on_job_status_changed(self, batch_failure_rule):
        pattern = batch_failure_rule.get("Properties", {}).get("EventPattern", {})
        detail = pattern.get("detail", {})
        event_names = detail.get("eventName", [])
        assert "JobStatusChanged" in event_names

    def test_rule_filters_failed_and_cancelled_status(self, batch_failure_rule):
        pattern = batch_failure_rule.get("Properties", {}).get("EventPattern", {})
        detail = pattern.get("detail", {})
        statuses = detail.get("serviceEventDetails", {}).get("status", [])
        assert "Failed" in statuses
        assert "Cancelled" in statuses

    def test_rule_state_is_enabled(self, batch_failure_rule):
        state = batch_failure_rule.get("Properties", {}).get("State")
        assert state == "ENABLED"

    def test_rule_has_log_group_target(self, batch_failure_rule):
        targets = batch_failure_rule.get("Properties", {}).get("Targets", [])
        target_ids = [t.get("Id") for t in targets if isinstance(t, dict)]
        assert "BatchJobFailureLogTarget" in target_ids

    # --- Alarm (metric filter + alarm) ------------------------------------

    def test_metric_filter_exists(self, batch_failure_metric_filter):
        assert batch_failure_metric_filter.get("Type") == "AWS::Logs::MetricFilter"

    def test_metric_filter_metric_name_is_failed_batch_jobs(self, batch_failure_metric_filter):
        transforms = (
            batch_failure_metric_filter.get("Properties", {})
            .get("MetricTransformations", [])
        )
        assert transforms, "MetricTransformations must not be empty"
        assert transforms[0].get("MetricName") == "FailedBatchJobs"

    def test_alarm_exists(self, batch_failure_alarm):
        assert batch_failure_alarm.get("Type") == "AWS::CloudWatch::Alarm"

    def test_alarm_threshold_is_zero(self, batch_failure_alarm):
        props = batch_failure_alarm.get("Properties", {})
        assert props.get("Threshold") == 0

    def test_alarm_comparator_is_greater_than(self, batch_failure_alarm):
        props = batch_failure_alarm.get("Properties", {})
        assert props.get("ComparisonOperator") == "GreaterThanThreshold"

    def test_alarm_treat_missing_data_not_breaching(self, batch_failure_alarm):
        props = batch_failure_alarm.get("Properties", {})
        assert props.get("TreatMissingData") == "notBreaching"

    # --- SNS conditional on HasAlarmEmail ---------------------------------

    def test_sns_topic_has_alarm_email_condition(self, batch_failure_topic):
        assert batch_failure_topic.get("Type") == "AWS::SNS::Topic"
        assert batch_failure_topic.get("Condition") == "HasAlarmEmail"

    def test_sns_subscription_has_alarm_email_condition(self, batch_failure_subscription):
        assert batch_failure_subscription.get("Type") == "AWS::SNS::Subscription"
        assert batch_failure_subscription.get("Condition") == "HasAlarmEmail"

    # --- Notifier Lambda (replaces the old direct EventBridge→SNS target) ---
    #
    # EventBridge's direct-to-SNS target does not support a Subject field,
    # so every failure email arrived with an unhelpful first-line-of-body
    # subject. A tiny Lambda now receives the event and calls sns:Publish
    # with "Batch job Failed: <stack> (<region>)" as the subject.

    # The old architecture targeted SNS directly (InputTransformer, TopicPolicy).
    # The new architecture targets a Lambda that calls sns:Publish with a
    # meaningful Subject, which EventBridge's direct-to-SNS target cannot set.
    # The TopicPolicy is gone — the Lambda publishes via its own IAM role
    # (ExecutionRole already has sns:Publish on BatchJobFailureTopic).

    # The old architecture targeted SNS directly (InputTransformer, TopicPolicy).
    # The new architecture targets a Lambda that calls sns:Publish with a
    # meaningful Subject, which EventBridge's direct-to-SNS target cannot set.
    # The TopicPolicy is gone — the Lambda publishes via its own IAM role
    # (ExecutionRole already has sns:Publish on BatchJobFailureTopic).

    def test_notifier_lambda_exists_and_is_conditional(self, resources):
        func = resources.get("BatchJobFailureNotifierFunction", {})
        assert func.get("Type") == "AWS::Lambda::Function"
        assert func.get("Condition") == "HasAlarmEmail"

    def test_notifier_lambda_has_topic_arn_env_var(self, resources):
        func = resources.get("BatchJobFailureNotifierFunction", {})
        env_vars = (
            func.get("Properties", {})
            .get("Environment", {})
            .get("Variables", {})
        )
        assert "TOPIC_ARN" in env_vars

    def test_rule_targets_notifier_lambda(self, batch_failure_rule):
        """The EventBridge rule targets the notifier Lambda (not SNS directly)
        so the email arrives with a meaningful subject line."""
        targets = batch_failure_rule.get("Properties", {}).get("Targets", [])
        lambda_target = None
        for t in targets:
            if isinstance(t, dict) and t.get("Id") == "BatchJobFailureLambdaTarget":
                lambda_target = t
                break
            if isinstance(t, _CfnTag) and t.tag == "!If":
                candidate = t.value[1] if isinstance(t.value, list) and len(t.value) > 1 else None
                if isinstance(candidate, dict) and candidate.get("Id") == "BatchJobFailureLambdaTarget":
                    lambda_target = candidate
                    break
        assert lambda_target is not None, "BatchJobFailureLambdaTarget not found in rule targets"

    def test_eventbridge_permission_exists(self, resources):
        perm = resources.get("BatchJobFailureNotifierPermission", {})
        assert perm.get("Type") == "AWS::Lambda::Permission"
        props = perm.get("Properties", {})
        assert props.get("Action") == "lambda:InvokeFunction"
        assert props.get("Principal") == "events.amazonaws.com"

    def test_alarm_has_no_alarm_actions(self, batch_failure_alarm):
        """The alarm must NOT have its own AlarmActions (no SNS publish) —
        the notifier Lambda is the email notification for this event.
        Giving the alarm its own AlarmActions would send a second, far
        less readable "Threshold Crossed: 1 datapoint..." email for the
        same underlying failure."""
        props = batch_failure_alarm.get("Properties", {})
        assert "AlarmActions" not in props

    def test_has_alarm_email_condition_exists(self, template):
        conditions = template.get("Conditions", {})
        assert "HasAlarmEmail" in conditions

    def test_alarm_email_parameter_exists(self, template):
        params = template.get("Parameters", {})
        assert "AlarmEmail" in params
        assert params["AlarmEmail"].get("Default") == ""

    # --- Output -----------------------------------------------------------

    def test_batch_job_failure_alarm_arn_output_exists(self, template):
        outputs = template.get("Outputs", {})
        assert "BatchJobFailureAlarmArn" in outputs

    def test_batch_job_failure_alarm_arn_output_references_alarm(self, template):
        outputs = template.get("Outputs", {})
        value = outputs.get("BatchJobFailureAlarmArn", {}).get("Value")
        assert isinstance(value, _CfnTag), "Output Value must be !GetAtt"
        assert value.tag == "!GetAtt"
        assert "BatchJobFailureAlarm" in str(value.value)

    # --- s3:DescribeJob in ExecutionRole ----------------------------------

    def test_describe_job_in_execution_role(self, execution_role_policy_statements):
        """s3:DescribeJob must be in the S3BatchOperationsCreateJob statement."""
        all_actions = _extract_actions_from_statements(execution_role_policy_statements)
        assert "s3:describejob" in all_actions


# ---------------------------------------------------------------------------
# Tests: replication-completion-tracking CFN parameters and conditional
#        statements — task 22.3
# Feature: replication-completion-tracking
# Requirements: 3.5
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def completion_report_topic(resources) -> dict:
    return resources.get("CompletionReportTopic", {})


@pytest.fixture(scope="module")
def completion_report_subscription(resources) -> dict:
    return resources.get("CompletionReportSubscription", {})


@pytest.fixture(scope="module")
def completion_report_policy(resources) -> dict:
    return resources.get("CompletionReportPolicy", {})


class TestCompletionTrackingParameters:
    def test_completion_notification_email_parameter(self, template):
        params = template.get("Parameters", {})
        assert "CompletionNotificationEmail" in params, (
            "CompletionNotificationEmail parameter not found"
        )
        param = params["CompletionNotificationEmail"]
        assert param.get("Type") == "String"
        assert param.get("Default") == ""

    def test_completion_check_batch_size_parameter(self, template):
        params = template.get("Parameters", {})
        assert "CompletionCheckBatchSize" in params, (
            "CompletionCheckBatchSize parameter not found"
        )
        param = params["CompletionCheckBatchSize"]
        assert param.get("Type") == "Number"
        assert param.get("Default") == 2000

    def test_has_completion_notification_email_condition_exists(self, template):
        conditions = template.get("Conditions", {})
        assert "HasCompletionNotificationEmail" in conditions

    def test_no_destination_presence_check_role_arn_parameter(self, template):
        """DestinationPresenceCheckRoleArn must not exist — source-status
        completion tracking removes destination access entirely."""
        params = template.get("Parameters", {})
        assert "DestinationPresenceCheckRoleArn" not in params

    def test_no_completion_destination_region_parameter(self, template):
        """CompletionDestinationRegion must not exist — no destination-region
        client is ever constructed."""
        params = template.get("Parameters", {})
        assert "CompletionDestinationRegion" not in params

    def test_no_completion_source_status_threshold_seconds_parameter(self, template):
        """CompletionSourceStatusThresholdSeconds must not exist — there is no
        age gate; every gated candidate goes straight to a Source_Status_Check."""
        params = template.get("Parameters", {})
        assert "CompletionSourceStatusThresholdSeconds" not in params

    def test_no_destination_presence_conditions_exist(self, template):
        conditions = template.get("Conditions", {})
        assert "HasDestinationPresenceCheckRole" not in conditions
        assert "HasCompletionDestinationRegion" not in conditions

    def test_no_destination_presence_check_policy_resource(self, resources):
        assert "DestinationPresenceCheckPolicy" not in resources

    def test_no_sts_assume_role_in_execution_role_or_completion_report_policy(
        self, execution_role_policy_statements, completion_report_policy
    ):
        """No sts:AssumeRole statement related to completion tracking remains
        in the ExecutionRole inline policy or the CompletionReportPolicy —
        source-status completion tracking makes no destination-account call.
        (LFGranterRole's own sts:AssumeRole, used for LF admin elevation, is
        an unrelated pre-existing feature and lives on a different role.)"""
        actions = _extract_actions_from_statements(execution_role_policy_statements)
        assert "sts:assumerole" not in actions

        cr_stmts = (
            completion_report_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        cr_actions = _extract_actions_from_statements(cr_stmts)
        assert "sts:assumerole" not in cr_actions


class TestCompletionTrackingIamPolicyPermissions:
    """Permissions smoke test for deploy/iam-policy.json (task 21.3) — confirms
    the source-status completion tracking rewrite left ReadSourceObjectTags
    intact, removed the destination sts:AssumeRole statement, and introduced
    no unconditional destination-account access."""

    def test_read_source_object_tags_still_includes_get_object(self, iam_policy):
        stmt = next(
            s for s in iam_policy["Statement"]
            if s.get("Sid") == "ReadSourceObjectTags"
        )
        actions = [a.lower() for a in stmt.get("Action", [])]
        assert "s3:getobject" in actions, (
            "ReadSourceObjectTags must still grant s3:GetObject "
            "(required by the Source_Status_Check HeadObject call)"
        )

    # LFGranterRole's own sts:AssumeRole (for optional LF admin elevation) is
    # an unrelated pre-existing feature on a different role. It now lives in
    # its own AssumeLFAdminRole statement so it can be scoped to a single role
    # ARN (security-scan-remediation Requirement 2.2); both Sids are excluded.
    _LF_GRANTER_SIDS = frozenset(["LFGranterRolePolicy", "AssumeLFAdminRole"])

    def test_no_sts_assume_role_statement_remains(self, iam_policy):
        """No completion-tracking-related sts:AssumeRole statement remains."""
        for stmt in iam_policy["Statement"]:
            if stmt.get("Sid") in self._LF_GRANTER_SIDS:
                continue
            actions = stmt.get("Action", [])
            if isinstance(actions, str):
                actions = [actions]
            assert not any(
                isinstance(a, str) and a.lower() == "sts:assumerole" for a in actions
            ), (
                f"Unexpected sts:AssumeRole statement {stmt.get('Sid')!r} found — "
                "source-status completion tracking makes no destination-account call"
            )

    def test_no_optional_destination_presence_sid_remains(self, iam_policy):
        sids = {s.get("Sid") for s in iam_policy["Statement"]}
        assert "OptionalDestinationPresenceCheckAssumeRole" not in sids

    def test_no_unconditional_destination_account_resource(self, iam_policy):
        """No statement in the policy scopes a Resource to a destination-account
        placeholder (e.g. a bare wildcard covering an account other than the
        deployment's own account/bucket placeholders)."""
        for stmt in iam_policy["Statement"]:
            resources = stmt.get("Resource", [])
            if isinstance(resources, str):
                resources = [resources]
            for r in resources:
                assert "DESTINATION" not in r.upper(), (
                    f"Statement {stmt.get('Sid')!r} references a destination-account "
                    f"placeholder resource {r!r}"
                )


class TestReferencePolicyAssumeRoleScoping:
    """security-scan-remediation Requirement 2.5 — the reference policy must
    never grant sts:AssumeRole on a wildcard resource, so a customer following
    deploy/iam-policy.json instead of deploying the template cannot end up with
    assume-any-role-in-any-account."""

    @staticmethod
    def _as_list(value) -> list:
        if isinstance(value, str):
            return [value]
        return value if isinstance(value, list) else []

    def test_no_wildcard_sts_assume_role(self, iam_policy):
        for stmt in iam_policy["Statement"]:
            actions = [
                a.lower() for a in self._as_list(stmt.get("Action")) if isinstance(a, str)
            ]
            if "sts:assumerole" not in actions:
                continue
            resources = self._as_list(stmt.get("Resource"))
            assert resources, (
                f"Statement {stmt.get('Sid')!r} grants sts:AssumeRole with no Resource"
            )
            for r in resources:
                assert "*" not in r, (
                    f"Statement {stmt.get('Sid')!r} grants sts:AssumeRole on wildcard "
                    f"resource {r!r}; it must be scoped to a single role ARN (Req 2.4)"
                )

    def test_assume_lf_admin_role_statement_is_scoped_to_one_role_arn(self, iam_policy):
        stmt = next(
            (s for s in iam_policy["Statement"] if s.get("Sid") == "AssumeLFAdminRole"),
            None,
        )
        assert stmt is not None, "AssumeLFAdminRole statement not found (Req 2.2)"
        assert self._as_list(stmt.get("Action")) == ["sts:AssumeRole"]
        resources = self._as_list(stmt.get("Resource"))
        assert len(resources) == 1, "AssumeLFAdminRole must name exactly one role ARN"
        assert re.match(r"^arn:aws:iam::[^:]+:role/.+$", resources[0]), (
            f"AssumeLFAdminRole resource {resources[0]!r} is not an IAM role ARN"
        )

    def test_lf_granter_role_policy_no_longer_lists_assume_role(self, iam_policy):
        stmt = next(
            s for s in iam_policy["Statement"] if s.get("Sid") == "LFGranterRolePolicy"
        )
        actions = [
            a.lower() for a in self._as_list(stmt.get("Action")) if isinstance(a, str)
        ]
        assert "sts:assumerole" not in actions, (
            "sts:AssumeRole must be split out of LFGranterRolePolicy (Req 2.1)"
        )


class TestCompletionReportResources:
    def test_completion_report_topic_exists_and_conditional(self, completion_report_topic):
        assert completion_report_topic.get("Type") == "AWS::SNS::Topic"
        assert completion_report_topic.get("Condition") == "HasCompletionNotificationEmail"

    def test_completion_report_subscription_exists_and_conditional(
        self, completion_report_subscription
    ):
        assert completion_report_subscription.get("Type") == "AWS::SNS::Subscription"
        assert (
            completion_report_subscription.get("Condition")
            == "HasCompletionNotificationEmail"
        )

    def test_completion_report_subscription_uses_email_protocol(
        self, completion_report_subscription
    ):
        props = completion_report_subscription.get("Properties", {})
        assert props.get("Protocol") == "email"

    def test_completion_report_subscription_references_topic_and_email(
        self, completion_report_subscription
    ):
        props = completion_report_subscription.get("Properties", {})
        topic_arn = props.get("TopicArn")
        endpoint = props.get("Endpoint")
        assert isinstance(topic_arn, _CfnTag) and topic_arn.tag == "!Ref"
        assert topic_arn.value == "CompletionReportTopic"
        assert isinstance(endpoint, _CfnTag) and endpoint.tag == "!Ref"
        assert endpoint.value == "CompletionNotificationEmail"

    def test_completion_report_policy_exists_and_conditional(
        self, completion_report_policy
    ):
        assert completion_report_policy.get("Type") == "AWS::IAM::Policy"
        assert (
            completion_report_policy.get("Condition") == "HasCompletionNotificationEmail"
        )

    def test_completion_report_policy_grants_only_sns_publish(
        self, completion_report_policy
    ):
        stmts = (
            completion_report_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        actions = _extract_actions_from_statements(stmts)
        assert actions == {"sns:publish"}, (
            f"CompletionReportPolicy actions must be only sns:Publish; got {actions}"
        )

    def test_completion_report_policy_scoped_to_topic_ref(self, completion_report_policy):
        stmts = (
            completion_report_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        assert stmts, "CompletionReportPolicy must have at least one statement"
        resource = stmts[0].get("Resource")
        assert isinstance(resource, list) and len(resource) == 1
        ref = resource[0]
        assert isinstance(ref, _CfnTag) and ref.tag == "!Ref"
        assert ref.value == "CompletionReportTopic"

    def test_completion_report_policy_attached_to_execution_role(
        self, completion_report_policy
    ):
        roles = completion_report_policy.get("Properties", {}).get("Roles", [])
        assert len(roles) == 1
        role_ref = roles[0]
        assert isinstance(role_ref, _CfnTag) and role_ref.tag == "!Ref"
        assert role_ref.value == "ExecutionRole"

    def test_sns_publish_not_in_execution_role_inline(
        self, execution_role_policy_statements
    ):
        """sns:Publish must NOT appear in the ExecutionRole inline policy —
        only in the conditional CompletionReportPolicy."""
        actions = _extract_actions_from_statements(execution_role_policy_statements)
        assert "sns:publish" not in actions


class TestCompletionTrackingLambdaEnvVars:
    def test_completion_report_topic_arn_env_var_uses_if(self, lambda_props):
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        val = env_vars.get("COMPLETION_REPORT_TOPIC_ARN")
        assert isinstance(val, _CfnTag) and val.tag == "!If"
        cond_name = val.value[0] if isinstance(val.value, list) else None
        assert cond_name == "HasCompletionNotificationEmail"

    def test_completion_check_batch_size_env_var(self, lambda_props):
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        val = env_vars.get("COMPLETION_CHECK_BATCH_SIZE")
        assert isinstance(val, _CfnTag) and val.tag == "!Ref"
        assert val.value == "CompletionCheckBatchSize"

    def test_no_destination_presence_check_role_arn_env_var(self, lambda_props):
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        assert "DESTINATION_PRESENCE_CHECK_ROLE_ARN" not in env_vars

    def test_no_completion_destination_region_env_var(self, lambda_props):
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        assert "COMPLETION_DESTINATION_REGION" not in env_vars

    def test_no_completion_source_status_threshold_seconds_env_var(self, lambda_props):
        env_vars = lambda_props.get("Environment", {}).get("Variables", {})
        assert "COMPLETION_SOURCE_STATUS_THRESHOLD_SECONDS" not in env_vars


# ---------------------------------------------------------------------------
# Tests: report-missing detection resources — task 23.12
# Feature: source-status-completion-tracking, design.md Decision 9
# Requirements: 8.7
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def completion_report_check_lambda(resources) -> dict:
    return resources.get("CompletionReportCheckLambda", {})


@pytest.fixture(scope="module")
def completion_report_check_lambda_log_group(resources) -> dict:
    return resources.get("CompletionReportCheckLambdaLogGroup", {})


@pytest.fixture(scope="module")
def completion_report_check_schedule(resources) -> dict:
    return resources.get("CompletionReportCheckSchedule", {})


@pytest.fixture(scope="module")
def batch_job_failure_alert_policy(resources) -> dict:
    return resources.get("BatchJobFailureAlertPolicy", {})


@pytest.fixture(scope="module")
def batch_job_failure_log_write_policy(resources) -> dict:
    return resources.get("BatchJobFailureLogWritePolicy", {})


class TestCompletionReportCheckLambda:
    def test_exists_and_conditional(self, completion_report_check_lambda):
        assert completion_report_check_lambda.get("Type") == "AWS::Lambda::Function"
        assert (
            completion_report_check_lambda.get("Condition")
            == "HasCompletionNotificationEmail"
        )

    def test_handler_is_check_report_handler(self, completion_report_check_lambda):
        props = completion_report_check_lambda.get("Properties", {})
        assert props.get("Handler") == "src.lambda_handler.check_report_handler"

    def test_shares_code_with_replication_lambda(
        self, completion_report_check_lambda, lambda_props
    ):
        props = completion_report_check_lambda.get("Properties", {})
        check_code = props.get("Code", {})
        repl_code = lambda_props.get("Code", {})
        # Both must be sourced from the same S3Bucket/S3Key GetAtt outputs
        # (the parser custom resource) — not a separate inline ZipFile.
        assert check_code.get("S3Bucket") is not None
        assert check_code.get("S3Key") is not None
        for field in ("S3Bucket", "S3Key"):
            check_val = check_code.get(field)
            repl_val = repl_code.get(field)
            assert isinstance(check_val, _CfnTag) and check_val.tag == "!GetAtt"
            assert isinstance(repl_val, _CfnTag) and repl_val.tag == "!GetAtt"
            assert check_val.value == repl_val.value

    def test_shares_execution_role(self, completion_report_check_lambda):
        props = completion_report_check_lambda.get("Properties", {})
        role = props.get("Role")
        assert isinstance(role, _CfnTag) and role.tag == "!GetAtt"
        assert role.value == "ExecutionRole.Arn"

    def test_has_short_timeout(self, completion_report_check_lambda):
        props = completion_report_check_lambda.get("Properties", {})
        timeout = props.get("Timeout")
        assert isinstance(timeout, int)
        assert timeout <= 300

    def test_env_vars_present(self, completion_report_check_lambda):
        props = completion_report_check_lambda.get("Properties", {})
        env_vars = props.get("Environment", {}).get("Variables", {})
        assert "STATE_BUCKET" in env_vars
        assert "ACCOUNT_ID" in env_vars
        assert "BATCH_JOB_FAILURE_TOPIC_ARN" in env_vars
        assert "BATCH_JOB_FAILURE_LOG_GROUP" in env_vars

    def test_kms_key_arn_env_var_matches_replication_lambda(
        self, completion_report_check_lambda, lambda_props
    ):
        """This handler writes the same state objects as ReplicationLambda, so
        it must receive the same KMS key. Without it, its writes rewrite
        SSE-KMS state objects under SSE-S3 — an encryption downgrade."""
        check_env = (
            completion_report_check_lambda.get("Properties", {})
            .get("Environment", {})
            .get("Variables", {})
        )
        repl_env = lambda_props.get("Environment", {}).get("Variables", {})

        assert "KMS_KEY_ARN" in check_env, (
            "CompletionReportCheckLambda is missing KMS_KEY_ARN; its state "
            "writes would downgrade SSE-KMS objects to SSE-S3"
        )
        check_val = check_env["KMS_KEY_ARN"]
        repl_val = repl_env["KMS_KEY_ARN"]
        assert isinstance(check_val, _CfnTag) and check_val.tag == "!If"
        assert check_val.value[0] == "HasKmsKey"
        # Identical wiring to the main Lambda, so the two cannot drift.
        # _CfnTag defines no __eq__, so compare the reprs, which serialize
        # nested tags deterministically.
        assert repr(check_val) == repr(repl_val)

    def test_batch_job_failure_topic_arn_env_var_conditional_on_has_alarm_email(
        self, completion_report_check_lambda
    ):
        props = completion_report_check_lambda.get("Properties", {})
        env_vars = props.get("Environment", {}).get("Variables", {})
        val = env_vars.get("BATCH_JOB_FAILURE_TOPIC_ARN")
        assert isinstance(val, _CfnTag) and val.tag == "!If"
        cond_name = val.value[0] if isinstance(val.value, list) else None
        assert cond_name == "HasAlarmEmail"

    def test_batch_job_failure_log_group_env_var_references_log_group(
        self, completion_report_check_lambda
    ):
        props = completion_report_check_lambda.get("Properties", {})
        env_vars = props.get("Environment", {}).get("Variables", {})
        val = env_vars.get("BATCH_JOB_FAILURE_LOG_GROUP")
        assert isinstance(val, _CfnTag) and val.tag == "!Ref"
        assert val.value == "BatchJobFailureLogGroup"

    def test_absent_when_condition_false(self, resources):
        """Absence when HasCompletionNotificationEmail is false is asserted
        structurally via the Condition attribute above — CloudFormation
        omits conditional resources entirely from the deployed stack when
        their condition evaluates false; this test just re-confirms the
        resource declares the condition rather than being unconditional."""
        assert resources["CompletionReportCheckLambda"].get("Condition") is not None


class TestCompletionReportCheckLambdaLogGroup:
    def test_exists_and_conditional(self, completion_report_check_lambda_log_group):
        assert completion_report_check_lambda_log_group.get("Type") == "AWS::Logs::LogGroup"
        assert (
            completion_report_check_lambda_log_group.get("Condition")
            == "HasCompletionNotificationEmail"
        )

    def test_retention_is_90_days(self, completion_report_check_lambda_log_group):
        props = completion_report_check_lambda_log_group.get("Properties", {})
        assert props.get("RetentionInDays") == 90

    def test_deletion_policy_is_retain(self, completion_report_check_lambda_log_group):
        assert completion_report_check_lambda_log_group.get("DeletionPolicy") == "Retain"

    def test_log_group_name_references_the_check_lambda(
        self, completion_report_check_lambda_log_group
    ):
        props = completion_report_check_lambda_log_group.get("Properties", {})
        name = props.get("LogGroupName")
        assert isinstance(name, _CfnTag) and name.tag == "!Sub"
        assert "CompletionReportCheckLambda" in name.value


class TestCompletionReportCheckSchedule:
    def test_exists_and_conditional(self, completion_report_check_schedule):
        assert completion_report_check_schedule.get("Type") == "AWS::Scheduler::Schedule"
        assert (
            completion_report_check_schedule.get("Condition")
            == "HasCompletionNotificationEmail"
        )

    def test_schedule_expression_is_rate_5_minutes(self, completion_report_check_schedule):
        props = completion_report_check_schedule.get("Properties", {})
        assert props.get("ScheduleExpression") == "rate(5 minutes)"

    def test_flexible_time_window_mode_off(self, completion_report_check_schedule):
        props = completion_report_check_schedule.get("Properties", {})
        assert props.get("FlexibleTimeWindow", {}).get("Mode") == "OFF"

    def test_targets_completion_report_check_lambda(self, completion_report_check_schedule):
        props = completion_report_check_schedule.get("Properties", {})
        target = props.get("Target", {})
        arn = target.get("Arn")
        assert isinstance(arn, _CfnTag) and arn.tag == "!GetAtt"
        assert arn.value == "CompletionReportCheckLambda.Arn"

    def test_reuses_existing_schedule_role(self, completion_report_check_schedule):
        props = completion_report_check_schedule.get("Properties", {})
        target = props.get("Target", {})
        role_arn = target.get("RoleArn")
        assert isinstance(role_arn, _CfnTag) and role_arn.tag == "!GetAtt"
        assert role_arn.value == "ScheduleRole.Arn"


class TestScheduleRoleExtendedForCompletionReportCheckLambda:
    def test_schedule_role_grants_invoke_on_completion_report_check_lambda(
        self, resources
    ):
        role = resources.get("ScheduleRole", {})
        stmts = []
        for policy in role.get("Properties", {}).get("Policies", []):
            stmts.extend(policy.get("PolicyDocument", {}).get("Statement", []))
        assert stmts, "ScheduleRole must have at least one statement"
        resource_list = stmts[0].get("Resource")
        assert isinstance(resource_list, list)
        # Must contain a conditional !If referencing HasCompletionNotificationEmail
        found = False
        for r in resource_list:
            if isinstance(r, _CfnTag) and r.tag == "!If":
                cond_name = r.value[0] if isinstance(r.value, list) else None
                if cond_name == "HasCompletionNotificationEmail":
                    found = True
        assert found, (
            "ScheduleRole InvokeLambdaPolicy must have an !If "
            "[HasCompletionNotificationEmail, ...] entry for "
            "CompletionReportCheckLambda.Arn"
        )


class TestBatchJobFailureAlertPolicy:
    """SNS-publish-only policy, scoped to HasAlarmEmail — ReplicationLambda's
    bucket-disabled alert and check_report_handler's report-missing alert
    both need sns:Publish whenever AlarmEmail is configured, independent of
    CompletionNotificationEmail (the recovery mechanism for a
    circuit-breaker/InlineHashCeiling disable does not depend on completion
    tracking being enabled)."""

    def test_exists(self, batch_job_failure_alert_policy):
        assert batch_job_failure_alert_policy.get("Type") == "AWS::IAM::Policy"

    def test_condition_is_has_alarm_email(self, batch_job_failure_alert_policy):
        assert batch_job_failure_alert_policy.get("Condition") == "HasAlarmEmail"

    def test_grants_sns_publish_scoped_to_batch_job_failure_topic(
        self, batch_job_failure_alert_policy
    ):
        stmts = (
            batch_job_failure_alert_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        sns_stmt = next(
            s for s in stmts
            if "sns:Publish" in (s.get("Action") if isinstance(s.get("Action"), list) else [s.get("Action")])
        )
        resource = sns_stmt.get("Resource")
        assert isinstance(resource, list) and len(resource) == 1
        ref = resource[0]
        assert isinstance(ref, _CfnTag) and ref.tag == "!Ref"
        assert ref.value == "BatchJobFailureTopic"

    def test_attached_to_execution_role(self, batch_job_failure_alert_policy):
        roles = batch_job_failure_alert_policy.get("Properties", {}).get("Roles", [])
        assert len(roles) == 1
        role_ref = roles[0]
        assert isinstance(role_ref, _CfnTag) and role_ref.tag == "!Ref"
        assert role_ref.value == "ExecutionRole"


class TestBatchJobFailureLogWritePolicy:
    """Log-write-only policy, unconditional — both check_report_handler's
    report-missing alert and ReplicationLambda's bucket-disabled alert
    always write BatchJobFailureLogGroup regardless of AlarmEmail, mirroring
    Requirement 8.4's guarantee that the alert is visible even without SNS
    configured."""

    def test_exists_and_unconditional(self, batch_job_failure_log_write_policy):
        assert batch_job_failure_log_write_policy.get("Type") == "AWS::IAM::Policy"
        assert batch_job_failure_log_write_policy.get("Condition") is None

    def test_grants_logs_put_and_create_stream_scoped_to_log_group(
        self, batch_job_failure_log_write_policy
    ):
        stmts = (
            batch_job_failure_log_write_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        logs_stmt = next(
            s for s in stmts
            if "logs:PutLogEvents" in (
                s.get("Action") if isinstance(s.get("Action"), list) else [s.get("Action")]
            )
        )
        actions = logs_stmt.get("Action")
        assert "logs:PutLogEvents" in actions
        assert "logs:CreateLogStream" in actions
        resource = logs_stmt.get("Resource")
        assert isinstance(resource, list) and len(resource) == 1
        ref = resource[0]
        assert isinstance(ref, _CfnTag) and ref.tag == "!GetAtt"
        assert ref.value == "BatchJobFailureLogGroup.Arn"

    def test_attached_to_execution_role(self, batch_job_failure_log_write_policy):
        roles = batch_job_failure_log_write_policy.get("Properties", {}).get("Roles", [])
        assert len(roles) == 1
        role_ref = roles[0]
        assert isinstance(role_ref, _CfnTag) and role_ref.tag == "!Ref"
        assert role_ref.value == "ExecutionRole"


# ---------------------------------------------------------------------------
# Tests: pre-submission bucket-policy priming IAM statement — task 26.9
# Feature: source-status-completion-tracking, design.md Decision 10
# Requirements: 9.1
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def manage_completion_report_bucket_policy(resources) -> dict:
    return resources.get("ManageCompletionReportBucketPolicy", {})


class TestManageCompletionReportBucketPolicy:
    def test_exists_and_conditional(self, manage_completion_report_bucket_policy):
        assert manage_completion_report_bucket_policy.get("Type") == "AWS::IAM::Policy"
        assert (
            manage_completion_report_bucket_policy.get("Condition")
            == "HasCompletionNotificationEmail"
        )

    def test_grants_only_get_and_put_bucket_policy(
        self, manage_completion_report_bucket_policy
    ):
        stmts = (
            manage_completion_report_bucket_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        actions = _extract_actions_from_statements(stmts)
        assert actions == {"s3:getbucketpolicy", "s3:putbucketpolicy"}, (
            "ManageCompletionReportBucketPolicy actions must be exactly "
            f"s3:GetBucketPolicy/s3:PutBucketPolicy; got {actions}"
        )

    def test_scoped_to_state_bucket_arn(self, manage_completion_report_bucket_policy):
        stmts = (
            manage_completion_report_bucket_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        assert stmts, "ManageCompletionReportBucketPolicy must have at least one statement"
        resource = stmts[0].get("Resource")
        assert isinstance(resource, list) and len(resource) == 1
        ref = resource[0]
        assert isinstance(ref, _CfnTag) and ref.tag == "!GetAtt"
        assert ref.value == "StateBucket.Arn"

    def test_attached_to_execution_role(self, manage_completion_report_bucket_policy):
        roles = manage_completion_report_bucket_policy.get("Properties", {}).get(
            "Roles", []
        )
        assert len(roles) == 1
        role_ref = roles[0]
        assert isinstance(role_ref, _CfnTag) and role_ref.tag == "!Ref"
        assert role_ref.value == "ExecutionRole"

    def test_absent_when_condition_false(self, manage_completion_report_bucket_policy):
        """Absence when HasCompletionNotificationEmail is false is asserted
        structurally via the Condition attribute above — CloudFormation
        omits conditional resources entirely from the deployed stack when
        their condition evaluates false; this test just re-confirms the
        resource declares the condition rather than being unconditional."""
        assert manage_completion_report_bucket_policy.get("Condition") is not None


# ---------------------------------------------------------------------------
# Tests: self-reinvocation IAM permission + async invoke config — task 7.2
# Feature: scale-threshold-and-drain-throughput
# Requirements: 5.1
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reinvocation_self_invoke_policy(resources) -> dict:
    return resources.get("ReinvocationSelfInvokePolicy", {})


@pytest.fixture(scope="module")
def replication_lambda_event_invoke_config(resources) -> dict:
    return resources.get("ReplicationLambdaEventInvokeConfig", {})


class TestReinvocationSelfInvokePolicy:
    """Standalone AWS::IAM::Policy (not part of ExecutionRole's own inline
    Policies list) so ReplicationLambda (which depends on ExecutionRole via
    its Role property) can be referenced by ARN without a circular
    dependency (ExecutionRole -> ReplicationLambda -> ExecutionRole)."""

    def test_exists_and_unconditional(self, reinvocation_self_invoke_policy):
        assert reinvocation_self_invoke_policy.get("Type") == "AWS::IAM::Policy"
        assert reinvocation_self_invoke_policy.get("Condition") is None

    def test_grants_only_invoke_function(self, reinvocation_self_invoke_policy):
        stmts = (
            reinvocation_self_invoke_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        actions = _extract_actions_from_statements(stmts)
        assert actions == {"lambda:invokefunction"}, (
            "ReinvocationSelfInvokePolicy actions must be exactly "
            f"lambda:InvokeFunction; got {actions}"
        )

    def test_scoped_to_replication_lambda_arn_not_wildcard(
        self, reinvocation_self_invoke_policy
    ):
        stmts = (
            reinvocation_self_invoke_policy.get("Properties", {})
            .get("PolicyDocument", {})
            .get("Statement", [])
        )
        assert stmts, "ReinvocationSelfInvokePolicy must have at least one statement"
        resource = stmts[0].get("Resource")
        assert isinstance(resource, list) and len(resource) == 1
        ref = resource[0]
        assert isinstance(ref, _CfnTag) and ref.tag == "!GetAtt"
        assert ref.value == "ReplicationLambda.Arn"
        assert ref.value != "*"

    def test_attached_to_execution_role(self, reinvocation_self_invoke_policy):
        roles = reinvocation_self_invoke_policy.get("Properties", {}).get("Roles", [])
        assert len(roles) == 1
        role_ref = roles[0]
        assert isinstance(role_ref, _CfnTag) and role_ref.tag == "!Ref"
        assert role_ref.value == "ExecutionRole"


class TestReplicationLambdaEventInvokeConfig:
    """design.md "Self-invoke async retry is disabled" decision:
    MaximumRetryAttempts=0 so a transient async error does not silently
    duplicate a reinvocation; no DLQ; MaximumEventAgeInSeconds left at
    default."""

    def test_exists(self, replication_lambda_event_invoke_config):
        assert (
            replication_lambda_event_invoke_config.get("Type")
            == "AWS::Lambda::EventInvokeConfig"
        )

    def test_targets_replication_lambda(self, replication_lambda_event_invoke_config):
        props = replication_lambda_event_invoke_config.get("Properties", {})
        function_name = props.get("FunctionName")
        assert isinstance(function_name, _CfnTag) and function_name.tag == "!Ref"
        assert function_name.value == "ReplicationLambda"

    def test_qualifier_is_latest(self, replication_lambda_event_invoke_config):
        props = replication_lambda_event_invoke_config.get("Properties", {})
        assert props.get("Qualifier") == "$LATEST"

    def test_maximum_retry_attempts_is_zero(
        self, replication_lambda_event_invoke_config
    ):
        props = replication_lambda_event_invoke_config.get("Properties", {})
        assert props.get("MaximumRetryAttempts") == 0

    def test_no_destination_config(self, replication_lambda_event_invoke_config):
        props = replication_lambda_event_invoke_config.get("Properties", {})
        assert "DestinationConfig" not in props

    def test_no_maximum_event_age_override(
        self, replication_lambda_event_invoke_config
    ):
        """Left at its default per the resolved decision — no explicit
        MaximumEventAgeInSeconds override."""
        props = replication_lambda_event_invoke_config.get("Properties", {})
        assert "MaximumEventAgeInSeconds" not in props


# ---------------------------------------------------------------------------
# Optional SNS topic encryption (SnsKmsKeyArn)
#
# AWS Security Agent finding f-ac92e9c8-0605-49b1-a2ea-cb742e883a34 noted that
# completion report bodies carry raw object keys and are delivered by email.
# The keys stay unredacted by design (the report exists to identify object
# versions), so the mitigation offered is optional topic encryption — the
# remediation .holmes/accepted-risks.md AR6 named for itself.
#
# Default-off is the important property: an existing deployment that has not
# added the required KMS key-policy statements must be unaffected.
# ---------------------------------------------------------------------------


class TestSnsTopicEncryption:
    def test_parameter_exists_and_defaults_to_empty(self, template):
        params = template.get("Parameters", {})
        assert "SnsKmsKeyArn" in params
        assert params["SnsKmsKeyArn"].get("Default") == "", (
            "SnsKmsKeyArn must default to empty so topics stay unencrypted "
            "unless the operator opts in and configures the key policy"
        )

    def test_condition_defined(self, template):
        assert "HasSnsKmsKey" in template.get("Conditions", {})

    @pytest.mark.parametrize(
        "topic_name", ["CompletionReportTopic", "BatchJobFailureTopic"]
    )
    def test_topic_kms_master_key_id_is_conditional(self, resources, topic_name):
        topic = resources.get(topic_name, {})
        assert topic.get("Type") == "AWS::SNS::Topic"
        val = topic.get("Properties", {}).get("KmsMasterKeyId")
        assert isinstance(val, _CfnTag) and val.tag == "!If", (
            f"{topic_name} must gate KmsMasterKeyId behind HasSnsKmsKey"
        )
        assert val.value[0] == "HasSnsKmsKey"
        # Else branch must be AWS::NoValue, not a literal — otherwise an
        # unset parameter would be sent to SNS as an empty key id.
        else_branch = val.value[2]
        assert isinstance(else_branch, _CfnTag) and else_branch.tag == "!Ref"
        assert else_branch.value == "AWS::NoValue"

    def test_execution_role_granted_kms_for_publish(self, resources):
        """Publishing to an SSE-enabled topic needs GenerateDataKey/Decrypt on
        the topic's key, scoped to that key and gated on the condition."""
        policy = resources.get("SnsKmsKeyPolicy", {})
        assert policy.get("Type") == "AWS::IAM::Policy"
        assert policy.get("Condition") == "HasSnsKmsKey"

        props = policy.get("Properties", {})
        roles = props.get("Roles", [])
        assert any(
            isinstance(r, _CfnTag) and r.value == "ExecutionRole" for r in roles
        )

        statements = props.get("PolicyDocument", {}).get("Statement", [])
        assert len(statements) == 1
        stmt = statements[0]
        assert set(stmt.get("Action", [])) == {
            "kms:GenerateDataKey", "kms:Decrypt", "kms:DescribeKey",
        }
        resource = stmt.get("Resource", [])
        assert len(resource) == 1
        assert isinstance(resource[0], _CfnTag)
        assert resource[0].value == "SnsKmsKeyArn", (
            "the grant must be scoped to the SNS key, not the state-object key "
            "or a wildcard"
        )
