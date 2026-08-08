"""Unit tests for the inline Lambda logic in CodeLocationParserFunction
(ZipFile code embedded in deploy/template.yaml).

The function is extracted from the template at test time and executed via
exec() with patched boto3 and cfnresponse so no real AWS calls are made.

The behavior under test is the deploy-time guard against a CodeLocation that
points at a zip this Solution cannot import: notably GitHub's auto-generated
"Source code (zip)" asset, which nests the tree under <repo>-<version>/ so
that src.lambda_handler is unimportable. Without the guard the stack reaches
CREATE_COMPLETE and the Lambda raises ImportModuleError on every run.

The central distinction is between a zip that is readable and demonstrably
wrong (fail the stack) and a zip that cannot be read at all (warn and
proceed), since the parser role holds no kms:Decrypt and must not turn an
SSE-KMS or cross-account code bucket into a deploy failure.
"""
from __future__ import annotations

import io
import sys
import zipfile
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
_RESOURCE = "CodeLocationParserFunction"


@pytest.fixture(scope="module")
def template() -> dict:
    with open(_TEMPLATE_PATH) as fh:
        return yaml.load(fh, Loader=CfnLoader)


def _get_zip_code() -> str:
    with open(_TEMPLATE_PATH) as fh:
        t = yaml.load(fh, Loader=CfnLoader)
    code = t["Resources"][_RESOURCE]["Properties"]["Code"]["ZipFile"]
    assert isinstance(code, str), f"{_RESOURCE}.Code.ZipFile is not a string"
    return code


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfnresponse() -> MagicMock:
    mock = MagicMock()
    mock.SUCCESS = "SUCCESS"
    mock.FAILED = "FAILED"
    return mock


def _zip_bytes(names: list[str]) -> bytes:
    """Build an in-memory zip containing an entry per name."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name in names:
            zf.writestr(name, "x")
    return buf.getvalue()


def _s3_mock(
    body: bytes | None = None,
    *,
    content_length: int | None = None,
    head_error: Exception | None = None,
    get_error: Exception | None = None,
) -> MagicMock:
    s3 = MagicMock()
    if head_error is not None:
        s3.head_object.side_effect = head_error
    else:
        size = content_length if content_length is not None else len(body or b"")
        s3.head_object.return_value = {"ContentLength": size}
    if get_error is not None:
        s3.get_object.side_effect = get_error
    else:
        s3.get_object.return_value = {"Body": io.BytesIO(body or b"")}
    return s3


def _event(request_type: str = "Create", loc: str = "s3://code-bkt/package-1.0.0.zip") -> dict:
    return {"RequestType": request_type, "ResourceProperties": {"CodeLocation": loc}}


def _run(event: dict, s3: MagicMock | None = None) -> tuple[MagicMock, MagicMock]:
    """Execute the inline code and call handler(event).

    Returns (cfnresponse_mock, s3_mock). Exceptions raised by the handler after
    it has sent a FAILED response are swallowed, matching how Lambda treats
    them: the response has already reached CloudFormation.
    """
    cfnresponse_mock = _make_cfnresponse()
    _s3 = s3 if s3 is not None else _s3_mock(_zip_bytes(["src/lambda_handler.py"]))

    def make_client(service, **_kwargs):
        return _s3 if service == "s3" else MagicMock()

    code = _get_zip_code()
    context = MagicMock()
    with patch.dict(sys.modules, {"cfnresponse": cfnresponse_mock}):
        with patch("boto3.client", side_effect=make_client):
            ns: dict = {}
            exec(compile(code, f"<{_RESOURCE}>", "exec"), ns)
            try:
                ns["handler"](event, context)
            except Exception:
                pass  # handler re-raises after responding; assert on the response
    return cfnresponse_mock, _s3


def _sent(cfnresponse_mock: MagicMock):
    assert cfnresponse_mock.send.call_count == 1, "handler must send exactly one response"
    return cfnresponse_mock.send.call_args


def _status(cfnresponse_mock: MagicMock) -> str:
    return _sent(cfnresponse_mock).args[2]


def _reason(cfnresponse_mock: MagicMock) -> str:
    return _sent(cfnresponse_mock).kwargs.get("reason", "")


# ---------------------------------------------------------------------------
# The valid case
# ---------------------------------------------------------------------------


class TestValidPackage:
    def test_correct_layout_succeeds(self):
        cfn, _ = _run(_event(), _s3_mock(_zip_bytes(["src/__init__.py", "src/lambda_handler.py"])))
        assert _status(cfn) == "SUCCESS"

    def test_returns_parsed_bucket_and_key(self):
        cfn, _ = _run(_event(loc="s3://my-bkt/nested/prefix/package-1.0.0.zip"))
        data = _sent(cfn).args[3]
        assert data == {"Bucket": "my-bkt", "Key": "nested/prefix/package-1.0.0.zip"}

    def test_delete_succeeds_without_reading_s3(self):
        """Stack deletion must never block on validating a code package."""
        cfn, s3 = _run(_event("Delete"))
        assert _status(cfn) == "SUCCESS"
        s3.head_object.assert_not_called()
        s3.get_object.assert_not_called()


# ---------------------------------------------------------------------------
# Readable and demonstrably wrong: fail the stack
# ---------------------------------------------------------------------------


class TestRejectsUnusablePackage:
    def test_github_source_archive_is_rejected(self):
        """The actual mistake this guard exists for."""
        names = [
            "sample-replicate-on-tag-for-amazon-s3-1.0.0/README.md",
            "sample-replicate-on-tag-for-amazon-s3-1.0.0/src/__init__.py",
            "sample-replicate-on-tag-for-amazon-s3-1.0.0/src/lambda_handler.py",
            "sample-replicate-on-tag-for-amazon-s3-1.0.0/deploy/template.yaml",
        ]
        cfn, _ = _run(_event(), _s3_mock(_zip_bytes(names)))
        assert _status(cfn) == "FAILED"

    def test_source_archive_reason_names_the_cause_and_the_fix(self):
        names = [
            "sample-replicate-on-tag-for-amazon-s3-1.0.0/src/lambda_handler.py",
            "sample-replicate-on-tag-for-amazon-s3-1.0.0/pyproject.toml",
        ]
        cfn, _ = _run(_event(), _s3_mock(_zip_bytes(names)))
        reason = _reason(cfn)
        assert "sample-replicate-on-tag-for-amazon-s3-1.0.0/" in reason
        assert "Source code" in reason
        assert "package-<version>.zip" in reason

    def test_missing_marker_with_several_top_levels_is_rejected(self):
        cfn, _ = _run(_event(), _s3_mock(_zip_bytes(["docs/a.md", "deploy/b.yaml", "c.txt"])))
        assert _status(cfn) == "FAILED"
        assert "src/lambda_handler.py" in _reason(cfn)

    def test_marker_nested_one_level_is_not_accepted(self):
        """A substring match on the path would wrongly accept this."""
        cfn, _ = _run(_event(), _s3_mock(_zip_bytes(["wrapper/src/lambda_handler.py"])))
        assert _status(cfn) == "FAILED"

    def test_non_zip_object_is_rejected(self):
        """Unreadable-as-a-zip is proof of a bad package, not an access problem."""
        cfn, _ = _run(_event(), _s3_mock(b"this is not a zip file"))
        assert _status(cfn) == "FAILED"
        assert "not a valid zip archive" in _reason(cfn)

    def test_malformed_code_location_is_rejected(self):
        cfn, _ = _run(_event(loc="https://example.com/package.zip"))
        assert _status(cfn) == "FAILED"


# ---------------------------------------------------------------------------
# Cannot determine: warn and proceed
# ---------------------------------------------------------------------------


class TestFailsOpenWhenUnreadable:
    """The role has no kms:Decrypt and the code bucket may be cross-account.

    Those configurations deploy successfully today and must keep doing so.
    """

    def test_access_denied_on_head_still_succeeds(self):
        cfn, _ = _run(_event(), _s3_mock(head_error=Exception("AccessDenied")))
        assert _status(cfn) == "SUCCESS"

    def test_access_denied_on_get_still_succeeds(self):
        body = _zip_bytes(["src/lambda_handler.py"])
        s3 = _s3_mock(body, get_error=Exception("AccessDenied"))
        cfn, _ = _run(_event(), s3)
        assert _status(cfn) == "SUCCESS"

    def test_kms_decrypt_denied_still_succeeds(self):
        err = Exception("KMS.AccessDeniedException: not authorized to perform kms:Decrypt")
        cfn, _ = _run(_event(), _s3_mock(head_error=err))
        assert _status(cfn) == "SUCCESS"

    def test_oversized_object_skips_validation(self):
        """Bounds the in-memory read; an object this large is not a package anyway."""
        s3 = _s3_mock(b"", content_length=64 * 1024 * 1024 + 1)
        cfn, _ = _run(_event(), s3)
        assert _status(cfn) == "SUCCESS"
        s3.get_object.assert_not_called()

    def test_object_at_the_size_ceiling_is_still_validated(self):
        body = _zip_bytes(["src/lambda_handler.py"])
        s3 = _s3_mock(body, content_length=64 * 1024 * 1024)
        cfn, _ = _run(_event(), s3)
        assert _status(cfn) == "SUCCESS"
        s3.get_object.assert_called_once()


# ---------------------------------------------------------------------------
# Disclosure — responses and Data are visible in stack events
# ---------------------------------------------------------------------------


class TestDoesNotLeakObjectContents:
    def test_failure_response_data_carries_no_file_listing(self):
        names = ["wrapper/secret-internal-name.py", "wrapper/another-private-file.py"]
        cfn, _ = _run(_event(), _s3_mock(_zip_bytes(names)))
        data = _sent(cfn).args[3]
        assert data == {}, "failure response Data must stay empty"
        assert "secret-internal-name" not in _reason(cfn)
        assert "another-private-file" not in _reason(cfn)

    def test_reason_is_bounded(self):
        """CloudFormation truncates long reasons; keep well inside the limit."""
        deep = "/".join(f"lvl{i}" for i in range(200))
        cfn, _ = _run(_event(), _s3_mock(_zip_bytes([f"{deep}/x.py"])))
        assert len(_reason(cfn)) <= 900


# ---------------------------------------------------------------------------
# Template wiring
# ---------------------------------------------------------------------------


class TestTemplateWiring:
    def test_read_grant_is_scoped_to_one_bucket(self, template):
        """s3:GetObject must not be granted on *."""
        role = template["Resources"]["CodeLocationParserRole"]
        policies = role["Properties"]["Policies"]
        reads = [p for p in policies if p["PolicyName"] == "CodeLocationParserReadPackage"]
        assert len(reads) == 1, "expected exactly one package-read policy"

        statements = reads[0]["PolicyDocument"]["Statement"]
        assert len(statements) == 1
        stmt = statements[0]
        assert stmt["Effect"] == "Allow"
        assert stmt["Action"] == "s3:GetObject", "read-only: no Put, Delete, or List"

        resource = stmt["Resource"]
        assert isinstance(resource, _CfnTag) and resource.tag == "!Sub"
        arn_template, substitutions = resource.value
        assert arn_template == "arn:aws:s3:::${CodeBucket}/*"
        assert arn_template != "arn:aws:s3:::*"

        bucket = substitutions["CodeBucket"]
        assert isinstance(bucket, _CfnTag) and bucket.tag == "!Select"
        index, split = bucket.value
        assert str(index) == "2"
        assert isinstance(split, _CfnTag) and split.tag == "!Split"
        assert split.value[0] == "/"

    def test_grant_derives_bucket_from_code_location(self, template):
        role = template["Resources"]["CodeLocationParserRole"]
        policies = role["Properties"]["Policies"]
        read = next(p for p in policies if p["PolicyName"] == "CodeLocationParserReadPackage")
        split = read["PolicyDocument"]["Statement"][0]["Resource"].value[1]["CodeBucket"].value[1]
        ref = split.value[1]
        assert isinstance(ref, _CfnTag) and ref.tag == "!Ref"
        assert ref.value == "CodeLocation"

    def test_code_location_pattern_guarantees_the_split_is_safe(self, template):
        """!Select [2, ...] is only safe because the parameter shape is enforced."""
        param = template["Parameters"]["CodeLocation"]
        assert param["AllowedPattern"] == "^s3://[a-z0-9.-]{3,63}/.+\\.zip$"

    def test_memory_covers_the_read_ceiling(self, template):
        props = template["Resources"][_RESOURCE]["Properties"]
        assert props["MemorySize"] >= 512, "must exceed the 64 MiB in-memory read"

    def test_inline_code_is_within_the_cloudformation_limit(self, template):
        """Code.ZipFile zips to at most 4 MB."""
        code = template["Resources"][_RESOURCE]["Properties"]["Code"]["ZipFile"]
        assert len(code.encode()) < 4 * 1024 * 1024
