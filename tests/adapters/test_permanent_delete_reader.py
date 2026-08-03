"""Unit tests for src/adapters/permanent_delete_reader.py.

Covers the code-review-remediation spec Req 2: the null-version supersession
leg of the Athena query must not include ``UPDATE_METADATA`` — tagging never
creates or supersedes a version on a versioning-enabled bucket (which the
solution requires), so including it caused a tagged, live null-version object
to filter itself out of the manifest.
"""
from __future__ import annotations

from unittest.mock import MagicMock

from src.adapters.permanent_delete_reader import read_permanent_deletes

_BUCKET = "source-bucket"
_WORKGROUP = "test-workgroup"
_OUTPUT = "s3://scratch/athena-results/"
_EXEC_ID = "query-exec-id-abc123"


def _make_athena_client() -> MagicMock:
    """Mock Athena client that succeeds with an empty result set."""
    client = MagicMock()
    client.start_query_execution.return_value = {"QueryExecutionId": _EXEC_ID}
    client.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    client.get_query_results.return_value = {
        "ResultSet": {
            "Rows": [
                {"Data": [{"VarCharValue": "key"}, {"VarCharValue": "version_id"}]},
            ]
        }
    }
    return client


class TestNullVersionSupersessionQuery:
    def test_query_does_not_treat_tagging_as_supersession(self):
        """UPDATE_METADATA must not appear in the record_type IN (...) list
        used for null-version supersession — tagging does not create or
        supersede a version, so including it would cause a tagged, live
        null-version object to filter itself out of the manifest."""
        client = _make_athena_client()
        read_permanent_deletes(
            client,
            _BUCKET,
            since_window_start=None,
            athena_workgroup=_WORKGROUP,
            output_location=_OUTPUT,
        )
        query = client.start_query_execution.call_args.kwargs["QueryString"]
        assert "UPDATE_METADATA" not in query

    def test_query_still_covers_versioned_supersession_sources(self):
        """PUT/COPY/RESTORE remain in the supersession leg (they are the
        only record types that can write a new version)."""
        client = _make_athena_client()
        read_permanent_deletes(
            client,
            _BUCKET,
            since_window_start=None,
            athena_workgroup=_WORKGROUP,
            output_location=_OUTPUT,
        )
        query = client.start_query_execution.call_args.kwargs["QueryString"]
        assert "'PUT'" in query
        assert "'COPY'" in query
        assert "'RESTORE'" in query

    def test_direct_delete_predicate_still_present(self):
        """The direct-delete leg (unaffected by this fix) is unchanged."""
        client = _make_athena_client()
        read_permanent_deletes(
            client,
            _BUCKET,
            since_window_start=None,
            athena_workgroup=_WORKGROUP,
            output_location=_OUTPUT,
        )
        query = client.start_query_execution.call_args.kwargs["QueryString"]
        assert "record_type = 'DELETE'" in query
