"""Structural guards for removal of source-object completion polling.

**Validates: Requirements 3.4, 8.4**

These checks deliberately distinguish the completion runtime from legitimate
metadata checks. In particular, the report-missing handler may HEAD the State
Bucket's report manifest; no completion runtime path may HEAD a source object.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode


_ROOT = Path(__file__).parent.parent
_SOURCE_ROOT = _ROOT / "src"
_REFERENCE_POLICY_PATH = _ROOT / "deploy" / "iam-policy.json"
_TEMPLATE_PATH = _ROOT / "deploy" / "template.yaml"

# These are the modules that implement runtime completion tracking and
# publication. bops_report_reader is deliberately excluded: its exact
# report-manifest existence probe HEADs the State Bucket, not a source object.
_COMPLETION_RUNTIME_MODULES = (
    _SOURCE_ROOT / "orchestrator.py",
    _SOURCE_ROOT / "lambda_handler.py",
    _SOURCE_ROOT / "adapters" / "state_store.py",
    _SOURCE_ROOT / "adapters" / "sns_report_adapter.py",
    _SOURCE_ROOT / "core" / "completion_tracker.py",
    _SOURCE_ROOT / "core" / "completion_serializer.py",
)

_OBSOLETE_SYMBOLS = frozenset(
    {
        "reconcile_source_status_check",
        "select_check_candidates",
        "CheckCandidate",
        "_TrackingCandidate",
        "_check_batch",
        "get_check_eligible_items",
        "apply_completion_resolutions",
        # Deleted in remediation: the legacy-state migration could never
        # succeed, and its failure branch skipped the affected bucket's whole
        # publish phase every interval. Upgrading from 1.0.1 is a reinstall
        # (design.md Decision 5), so no 1.0.1 state reaches a running stack.
        "migrate_legacy_completion_items",
    }
)
# The resolution_method string the deleted migration wrote. Guarded as a literal
# because a reintroduced migration could use a differently named function.
# Deliberately still spelled with the old version number: this is the literal
# string the deleted migration wrote, and the guard exists to stop that exact
# string returning. The migration never shipped in a release, so no state object
# anywhere contains it.
_OBSOLETE_RESOLUTION_METHOD = "migrated_1_0_2"
_FORBIDDEN_SOURCE_READ_ACTIONS = frozenset(
    {"s3:getobject", "s3:getobjectversion", "s3:listbucket"}
)


def _module_tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _head_object_calls(path: Path) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(_module_tree(path))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "head_object"
    ]


def _functions_calling_head_object(path: Path) -> set[str]:
    """Names of the functions in *path* that call ``head_object``.

    Identifies the call site by enclosing function rather than by line number:
    a line number changes whenever anything above it in the file changes, so it
    makes the guard fail for edits that have nothing to do with what is being
    guarded.
    """
    names: set[str] = set()
    for node in ast.walk(_module_tree(path)):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if any(
            isinstance(inner, ast.Call)
            and isinstance(inner.func, ast.Attribute)
            and inner.func.attr == "head_object"
            for inner in ast.walk(node)
        ):
            names.add(node.name)
    return names


def _statement_actions(statement: dict) -> set[str]:
    actions = statement.get("Action", [])
    if isinstance(actions, str):
        actions = [actions]
    return {action.lower() for action in actions if isinstance(action, str)}


def _statement_resources(statement: dict) -> list[str]:
    resources = statement.get("Resource", [])
    if isinstance(resources, str):
        resources = [resources]
    return [resource for resource in resources if isinstance(resource, str)]


def _scalar_values(node: Node) -> list[str]:
    if isinstance(node, ScalarNode):
        return [node.value]
    if isinstance(node, (MappingNode, SequenceNode)):
        return [value for child in node.value for value in _scalar_values(child if isinstance(node, SequenceNode) else child[1])]
    return []


def _template_statements(node: Node):
    if isinstance(node, MappingNode):
        fields = {
            key.value: value
            for key, value in node.value
            if isinstance(key, ScalarNode)
        }
        if "Action" in fields and "Resource" in fields:
            yield fields
        for _, value in node.value:
            yield from _template_statements(value)
    elif isinstance(node, SequenceNode):
        for value in node.value:
            yield from _template_statements(value)


class TestReportDerivedCompletionRemovalGuards:
    def test_source_status_adapter_and_obsolete_symbols_are_absent(self):
        assert not (_SOURCE_ROOT / "adapters" / "source_status_adapter.py").exists()

        found: dict[str, list[str]] = {}
        for path in _SOURCE_ROOT.rglob("*.py"):
            names = {
                node.name
                for node in ast.walk(_module_tree(path))
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            }
            imported_names = {
                alias.name.rsplit(".", 1)[-1]
                for node in ast.walk(_module_tree(path))
                if isinstance(node, ast.ImportFrom)
                for alias in node.names
            }
            unexpected = (names | imported_names) & _OBSOLETE_SYMBOLS
            if unexpected:
                found[str(path.relative_to(_ROOT))] = sorted(unexpected)

        assert not found, f"Obsolete completion symbols or imports remain: {found}"

    def test_no_legacy_migration_resolution_method_remains(self):
        """No code path writes the deleted migration's resolution_method."""
        found = {
            str(path.relative_to(_ROOT)): [
                node.value
                for node in ast.walk(_module_tree(path))
                if isinstance(node, ast.Constant)
                and node.value == _OBSOLETE_RESOLUTION_METHOD
            ]
            for path in _SOURCE_ROOT.rglob("*.py")
        }
        found = {path: values for path, values in found.items() if values}
        assert not found, (
            f"{_OBSOLETE_RESOLUTION_METHOD!r} must not be produced by 1.1.0: {found}"
        )

    def test_completion_runtime_modules_do_not_head_objects(self):
        calls = {
            str(path.relative_to(_ROOT)): [call.lineno for call in _head_object_calls(path)]
            for path in _COMPLETION_RUNTIME_MODULES
        }
        calls = {path: lines for path, lines in calls.items() if lines}
        assert not calls, f"Completion runtime must not call head_object: {calls}"

    def test_only_state_report_manifest_probe_uses_head_object(self):
        callers = {
            str(path.relative_to(_ROOT)): sorted(_functions_calling_head_object(path))
            for path in _SOURCE_ROOT.rglob("*.py")
        }
        callers = {path: names for path, names in callers.items() if names}
        assert callers == {
            "src/adapters/bops_report_reader.py": ["report_manifest_written_at"]
        }, (
            "Only report_manifest_written_at may HEAD the State Bucket manifest; "
            f"found {callers}"
        )

    def test_reference_policy_has_no_source_object_read_or_list_grant(self):
        policy = json.loads(_REFERENCE_POLICY_PATH.read_text(encoding="utf-8"))
        source_statements = [
            statement
            for statement in policy["Statement"]
            if any("SOURCE_BUCKET_NAME" in resource for resource in _statement_resources(statement))
        ]

        assert source_statements, "Expected a source-bucket replication-config statement"
        for statement in source_statements:
            forbidden = _statement_actions(statement) & _FORBIDDEN_SOURCE_READ_ACTIONS
            assert not forbidden, (
                f"Reference policy source statement {statement.get('Sid')!r} "
                f"regained read/list permissions: {sorted(forbidden)}"
            )

    def test_template_has_no_source_object_read_or_list_grant(self):
        root = yaml.compose(_TEMPLATE_PATH.read_text(encoding="utf-8"))
        assert root is not None

        source_statements = [
            statement
            for statement in _template_statements(root)
            if "SourceBucketNames" in _scalar_values(statement["Resource"])
        ]
        assert source_statements, "Expected a source-bucket replication-config statement"

        for statement in source_statements:
            actions = {action.lower() for action in _scalar_values(statement["Action"])}
            forbidden = actions & _FORBIDDEN_SOURCE_READ_ACTIONS
            assert not forbidden, (
                "Template source-bucket statement regained read/list permissions: "
                f"{sorted(forbidden)}"
            )
