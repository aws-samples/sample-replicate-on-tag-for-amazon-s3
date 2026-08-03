"""Unit and property tests for the pre-submission bucket-policy priming pure
functions (task 26.1/26.2).

Feature: source-status-completion-tracking.

Covers ``build_completion_report_bucket_policy_statement`` and
``ensure_completion_report_bucket_policy_statement`` in
``src/core/completion_tracker.py`` (design.md Decision 10). Both functions
are pure — no boto3, no I/O.
"""
from __future__ import annotations

import copy

from hypothesis import given, settings
from hypothesis import strategies as st

from src.core.completion_tracker import (
    build_completion_report_bucket_policy_statement,
    ensure_completion_report_bucket_policy_statement,
)

_STATE_BUCKET = "example-state-bucket"
_ACCOUNT_ID = "123456789012"


def _desired_statement(config_id: str, role_arn: str, state_bucket: str = _STATE_BUCKET) -> dict:
    return {
        "Sid": f"AllowCompletionReportWrite-{config_id}",
        "Effect": "Allow",
        "Principal": {"AWS": role_arn},
        "Action": "s3:PutObject",
        "Resource": f"arn:aws:s3:::{state_bucket}/completion-reports/{config_id}/*",
    }


def _unrelated_statement(sid: str = "SomeOtherStatement") -> dict:
    return {
        "Sid": sid,
        "Effect": "Allow",
        "Principal": {"AWS": "arn:aws:iam::123456789012:role/SomeOtherRole"},
        "Action": "s3:GetObject",
        "Resource": "arn:aws:s3:::example-state-bucket/other/*",
    }


class TestBuildCompletionReportBucketPolicyStatement:
    def test_shape_matches_design(self):
        statement = build_completion_report_bucket_policy_statement(
            "cfg-1", "arn:aws:iam::123456789012:role/ReplRole", _STATE_BUCKET, _ACCOUNT_ID
        )
        assert statement == {
            "Sid": "AllowCompletionReportWrite-cfg-1",
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:role/ReplRole"},
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::example-state-bucket/completion-reports/cfg-1/*",
        }

    def test_sid_is_derived_from_config_id(self):
        statement = build_completion_report_bucket_policy_statement(
            "my-config-id", "arn:aws:iam::123456789012:role/R", _STATE_BUCKET, _ACCOUNT_ID
        )
        assert statement["Sid"] == "AllowCompletionReportWrite-my-config-id"

    def test_resource_scoped_to_config_prefix(self):
        statement = build_completion_report_bucket_policy_statement(
            "cfg-a", "arn:aws:iam::123456789012:role/R", "bucket-x", _ACCOUNT_ID
        )
        assert statement["Resource"] == "arn:aws:s3:::bucket-x/completion-reports/cfg-a/*"

    def test_rejects_role_arn_in_different_account(self):
        """security-scan-remediation Requirement 11.2: an ARN belonging to a
        different account is rejected, skipping this config's statement."""
        statement = build_completion_report_bucket_policy_statement(
            "cfg-1", "arn:aws:iam::999999999999:role/ReplRole", _STATE_BUCKET, _ACCOUNT_ID
        )
        assert statement is None

    def test_rejects_malformed_role_arn(self):
        """security-scan-remediation Requirement 11.1: a non-role ARN (or
        garbage) is rejected."""
        for bad_arn in ("not-an-arn", "arn:aws:iam::123456789012:user/NotARole", ""):
            assert build_completion_report_bucket_policy_statement(
                "cfg-1", bad_arn, _STATE_BUCKET, _ACCOUNT_ID
            ) is None


class TestEnsureCompletionReportBucketPolicyStatementUnit:
    def test_no_current_policy_starts_from_empty_document(self):
        merged, write_needed = ensure_completion_report_bucket_policy_statement(
            None, "cfg-1", "arn:aws:iam::123456789012:role/R", _STATE_BUCKET, _ACCOUNT_ID
        )
        assert write_needed is True
        assert merged["Version"] == "2012-10-17"
        assert merged["Statement"] == [
            _desired_statement("cfg-1", "arn:aws:iam::123456789012:role/R")
        ]

    def test_empty_statement_list_adds_the_statement(self):
        current = {"Version": "2012-10-17", "Statement": []}
        merged, write_needed = ensure_completion_report_bucket_policy_statement(
            current, "cfg-1", "arn:aws:iam::123456789012:role/R", _STATE_BUCKET, _ACCOUNT_ID
        )
        assert write_needed is True
        assert merged["Statement"] == [
            _desired_statement("cfg-1", "arn:aws:iam::123456789012:role/R")
        ]

    def test_exact_desired_statement_already_present_needs_no_write(self):
        role_arn = "arn:aws:iam::123456789012:role/R"
        current = {
            "Version": "2012-10-17",
            "Statement": [_desired_statement("cfg-1", role_arn)],
        }
        merged, write_needed = ensure_completion_report_bucket_policy_statement(
            current, "cfg-1", role_arn, _STATE_BUCKET, _ACCOUNT_ID
        )
        assert write_needed is False
        assert merged is None

    def test_same_sid_different_content_triggers_write_and_replaces_it(self):
        current = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowCompletionReportWrite-cfg-1",
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::123456789012:role/StaleRole"},
                    "Action": "s3:PutObject",
                    "Resource": "arn:aws:s3:::example-state-bucket/completion-reports/cfg-1/*",
                }
            ],
        }
        role_arn = "arn:aws:iam::123456789012:role/NewRole"
        merged, write_needed = ensure_completion_report_bucket_policy_statement(
            current, "cfg-1", role_arn, _STATE_BUCKET, _ACCOUNT_ID
        )
        assert write_needed is True
        assert merged["Statement"] == [_desired_statement("cfg-1", role_arn)]

    def test_unrelated_statements_are_preserved(self):
        unrelated = _unrelated_statement()
        current = {"Version": "2012-10-17", "Statement": [unrelated]}
        role_arn = "arn:aws:iam::123456789012:role/R"
        merged, write_needed = ensure_completion_report_bucket_policy_statement(
            current, "cfg-1", role_arn, _STATE_BUCKET, _ACCOUNT_ID
        )
        assert write_needed is True
        assert unrelated in merged["Statement"]
        assert _desired_statement("cfg-1", role_arn) in merged["Statement"]
        assert len(merged["Statement"]) == 2

    def test_other_config_ids_own_statement_preserved_and_not_overwritten(self):
        role_arn_a = "arn:aws:iam::123456789012:role/RoleA"
        role_arn_b = "arn:aws:iam::123456789012:role/RoleB"
        other_statement = _desired_statement("cfg-b", role_arn_b)
        current = {"Version": "2012-10-17", "Statement": [other_statement]}
        merged, write_needed = ensure_completion_report_bucket_policy_statement(
            current, "cfg-a", role_arn_a, _STATE_BUCKET, _ACCOUNT_ID
        )
        assert write_needed is True
        assert other_statement in merged["Statement"]
        assert _desired_statement("cfg-a", role_arn_a) in merged["Statement"]
        assert len(merged["Statement"]) == 2

    def test_input_document_not_mutated(self):
        role_arn = "arn:aws:iam::123456789012:role/R"
        current = {"Version": "2012-10-17", "Statement": [_unrelated_statement()]}
        original = copy.deepcopy(current)
        ensure_completion_report_bucket_policy_statement(
            current, "cfg-1", role_arn, _STATE_BUCKET, _ACCOUNT_ID
        )
        assert current == original


# ---------------------------------------------------------------------------
# Property 21: The bucket-policy statement is written if and only if it is
# missing or different, and the write always merges rather than overwrites
# Feature: source-status-completion-tracking, Property 21: The bucket-policy statement is written if and only if it is missing or different, and the write always merges rather than overwrites
# Validates: Requirements 9.3, 9.4, 9.5, 9.6
# ---------------------------------------------------------------------------

_config_ids = st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=8)
_role_arns = st.builds(
    lambda suffix: f"arn:aws:iam::123456789012:role/ReplRole-{suffix}",
    st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=6),
)
_state_buckets = st.builds(
    lambda suffix: f"state-bucket-{suffix}",
    st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=6),
)


def _other_config_desired_statement(other_config_id: str) -> dict:
    """A statement matching the exact desired shape, but for a different
    config_id — used to assert other configs' own statements are never
    disturbed."""
    return _desired_statement(
        other_config_id, f"arn:aws:iam::123456789012:role/OtherRole-{other_config_id}"
    )


@st.composite
def _policy_scenarios(draw):
    """Generate (current_policy, config_id, role_arn, state_bucket, case) tuples
    covering: current_policy=None, empty statement list, a document already
    containing the exact desired statement, a document with a same-Sid-but-
    different-content statement, and a document with unrelated statements
    including other config_ids' own Completion_Report_Bucket_Policy_Statements.
    """
    config_id = draw(_config_ids)
    role_arn = draw(_role_arns)
    state_bucket = draw(_state_buckets)
    other_config_id = draw(_config_ids.filter(lambda c: c != config_id))

    case = draw(
        st.sampled_from(
            [
                "none",
                "empty",
                "exact_present",
                "same_sid_different_content",
                "unrelated_only",
                "other_config_own_statement",
                "mixed",
            ]
        )
    )

    desired = _desired_statement(config_id, role_arn, state_bucket)
    other_own_statement = _other_config_desired_statement(other_config_id)
    unrelated = _unrelated_statement(sid=f"Unrelated-{other_config_id}")

    if case == "none":
        current_policy = None
    elif case == "empty":
        current_policy = {"Version": "2012-10-17", "Statement": []}
    elif case == "exact_present":
        current_policy = {"Version": "2012-10-17", "Statement": [desired]}
    elif case == "same_sid_different_content":
        stale = dict(desired)
        stale["Principal"] = {"AWS": "arn:aws:iam::123456789012:role/StaleDifferentRole"}
        current_policy = {"Version": "2012-10-17", "Statement": [stale]}
    elif case == "unrelated_only":
        current_policy = {"Version": "2012-10-17", "Statement": [unrelated]}
    elif case == "other_config_own_statement":
        current_policy = {"Version": "2012-10-17", "Statement": [other_own_statement]}
    else:  # mixed
        current_policy = {
            "Version": "2012-10-17",
            "Statement": [unrelated, other_own_statement],
        }

    return current_policy, config_id, role_arn, state_bucket, case


class TestProperty21BucketPolicyWriteNeeded:
    """# Feature: source-status-completion-tracking, Property 21: The bucket-policy statement is written if and only if it is missing or different, and the write always merges rather than overwrites

    Validates: Requirements 9.3, 9.4, 9.5, 9.6
    """

    @given(scenario=_policy_scenarios())
    @settings(max_examples=100)
    def test_write_needed_correct_and_merge_preserves_other_statements(self, scenario) -> None:
        """# Feature: source-status-completion-tracking, Property 21: The bucket-policy statement is written if and only if it is missing or different, and the write always merges rather than overwrites"""
        current_policy, config_id, role_arn, state_bucket, case = scenario

        pre_existing_statements = []
        if current_policy is not None:
            pre_existing_statements = list(current_policy.get("Statement", []))

        merged, write_needed = ensure_completion_report_bucket_policy_statement(
            current_policy, config_id, role_arn, state_bucket, _ACCOUNT_ID
        )

        expected_write_needed = case != "exact_present"
        assert write_needed is expected_write_needed

        if not write_needed:
            # No write: exact desired statement already existed under this Sid.
            assert merged is None
            return

        # A write happened: the merged document must exist and be well-formed.
        assert merged is not None
        assert merged["Version"] == "2012-10-17"
        statements = merged["Statement"]

        sid = f"AllowCompletionReportWrite-{config_id}"
        matching = [s for s in statements if s.get("Sid") == sid]
        assert len(matching) == 1
        assert matching[0] == build_completion_report_bucket_policy_statement(
            config_id, role_arn, state_bucket, _ACCOUNT_ID
        )

        # Every pre-existing statement NOT under this config's Sid is preserved
        # unchanged, including other config_ids' own statements.
        other_pre_existing = [s for s in pre_existing_statements if s.get("Sid") != sid]
        for statement in other_pre_existing:
            assert statement in statements

        # No statements were invented beyond "others preserved + this one".
        assert len(statements) == len(other_pre_existing) + 1

    @given(scenario=_policy_scenarios())
    @settings(max_examples=100)
    def test_input_document_never_mutated(self, scenario) -> None:
        """# Feature: source-status-completion-tracking, Property 21: The bucket-policy statement is written if and only if it is missing or different, and the write always merges rather than overwrites"""
        current_policy, config_id, role_arn, state_bucket, _case = scenario
        original = copy.deepcopy(current_policy)
        ensure_completion_report_bucket_policy_statement(
            current_policy, config_id, role_arn, state_bucket, _ACCOUNT_ID
        )
        assert current_policy == original
