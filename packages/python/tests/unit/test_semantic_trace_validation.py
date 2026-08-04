from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from semantic_layer import initialize, reset_capture_for_tests
from semantic_layer.validation import validate_artifact


@pytest.fixture(autouse=True)
def reset_capture() -> None:
    reset_capture_for_tests()
    yield
    reset_capture_for_tests()


def _writer_bundle(tmp_path: Path) -> Path:
    capture = initialize(output=tmp_path, service_name="validator-fixture")
    with capture.observe("coding-agent", input={"task": "inspect"}) as run:
        assert run.tool(
            "read",
            {"path": "README.md"},
            lambda _input: b"validated blob",
        ) == b"validated blob"
    return Path(capture.shutdown().artifact_path)


def _account(manifest: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    trace_bytes = "".join(
        json.dumps(row, separators=(",", ":")) + "\n" for row in rows
    ).encode()
    manifest["trace"].update(
        records=len(rows),
        last_seq=len(rows),
        bytes=len(trace_bytes),
        losses=sum(row["kind"] == "loss" for row in rows),
        sha256=hashlib.sha256(trace_bytes).hexdigest(),
    )


def _rich_injected() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    time = "2026-07-26T12:00:00.000Z"
    source = "src_fixture"
    rows = [
        {
            "id": "rec_root",
            "seq": 1,
            "time": time,
            "kind": "run.start",
            "origin": "observed",
            "source": source,
            "data": {"name": "agent"},
        },
        {
            "id": "rec_request",
            "seq": 2,
            "time": time,
            "kind": "model.request",
            "origin": "observed",
            "source": source,
            "parent": "rec_root",
            "data": {"context_refs": []},
        },
        {
            "id": "rec_response",
            "seq": 3,
            "time": time,
            "kind": "model.response",
            "origin": "observed",
            "source": source,
            "parent": "rec_root",
            "links": [{"type": "result_of", "record": "rec_request"}],
            "data": {"status": "completed", "content": "done"},
        },
        {
            "id": "rec_tool_call",
            "seq": 4,
            "time": time,
            "kind": "tool.call",
            "origin": "observed",
            "source": source,
            "parent": "rec_root",
            "data": {"call_id": "call_fixture", "name": "read", "input": {}},
        },
        {
            "id": "rec_tool_result",
            "seq": 5,
            "time": time,
            "kind": "tool.result",
            "origin": "observed",
            "source": source,
            "parent": "rec_root",
            "links": [{"type": "result_of", "record": "rec_tool_call"}],
            "data": {"call_id": "call_fixture", "status": "succeeded"},
        },
        {
            "id": "rec_state",
            "seq": 6,
            "time": time,
            "kind": "state",
            "origin": "observed",
            "source": source,
            "parent": "rec_root",
            "data": {"type": "state.complete", "value": True},
        },
        {
            "id": "rec_run_outcome",
            "seq": 7,
            "time": time,
            "kind": "run.outcome",
            "origin": "observed",
            "source": source,
            "parent": "rec_root",
            "data": {"status": "completed"},
        },
    ]
    manifest = {
        "schema": "semantic_trace_manifest_v1",
        "record_schema": "semantic_trace_record_v1",
        "bundle_id": "bundle_fixture",
        "state": "open",
        "sdk": {"language": "python", "version": "test"},
        "privacy_mode": "local-rich",
        "started_at": time,
        "updated_at": time,
        "sources": [{"id": source, "name": "fixture"}],
        "trace": {
            "path": "trace.jsonl",
            "records": 0,
            "last_seq": 0,
            "bytes": 0,
            "losses": 0,
            "sha256": hashlib.sha256(b"").hexdigest(),
        },
        "blobs": {"path": "blobs", "count": 0, "bytes": 0},
    }
    _account(manifest, rows)
    return manifest, rows


def test_real_writer_bundle_passes_structural_validation(tmp_path: Path) -> None:
    artifact = _writer_bundle(tmp_path)

    report = validate_artifact(artifact)

    assert report.valid
    assert report.issues == ()
    assert report.rows == 4
    assert report.secret_matches == 0


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink assertion")
@pytest.mark.parametrize("symlink_location", ["root", "intermediate"])
def test_validation_rejects_symlink_artifact_paths_before_reading(
    tmp_path: Path,
    symlink_location: str,
) -> None:
    artifact = _writer_bundle(tmp_path)
    if symlink_location == "root":
        submitted = tmp_path / "artifact-link"
        submitted.symlink_to(artifact, target_is_directory=True)
    else:
        linked_parent = tmp_path / "parent-link"
        linked_parent.symlink_to(artifact.parent, target_is_directory=True)
        submitted = linked_parent / artifact.name

    report = validate_artifact(submitted)

    assert not report.valid
    assert report.issues == ("ARTIFACT_UNREADABLE",)
    assert report.rows == 0


def test_validation_detects_minimum_length_secret_embedded_in_json_value() -> None:
    manifest, rows = _rich_injected()
    rows[0]["data"]["name"] = "prefixxabcdefghxsuffix"
    _account(manifest, rows)

    report = validate_artifact(
        ".",
        injected={"manifest": manifest, "rows": rows},
        secret_values=["abcdefgh"],
    )

    assert report.secret_matches == 1
    assert "SECRET_MATCH" in report.issues


def test_rich_agent_profile_accepts_compact_semantic_kinds_under_one_root() -> None:
    manifest, rows = _rich_injected()
    rows = [
        row
        for row in rows
        if row["kind"] not in {"state", "error", "verification"}
    ]
    for sequence, row in enumerate(rows, 1):
        row["seq"] = sequence
    _account(manifest, rows)

    assert sum(row["kind"] == "run.start" for row in rows) == 1
    report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": rows},
        profile="rich-agent",
    )

    assert report.valid
    assert report.issues == ()


def test_validation_accepts_bounded_context_base_chain() -> None:
    manifest, rows = _rich_injected()
    request = rows[1]
    response = rows[2]
    message_a = {
        "id": "rec_message_a",
        "seq": 0,
        "time": request["time"],
        "kind": "message",
        "origin": "observed",
        "source": request["source"],
        "parent": "rec_root",
        "data": {"role": "user", "content": "A"},
    }
    message_b = {
        **message_a,
        "id": "rec_message_b",
        "data": {"role": "tool", "content": "B"},
    }
    second_request = {
        **request,
        "id": "rec_request_b",
        "data": {
            "context_base_ref": "rec_request",
            "context_refs": ["rec_message_b"],
        },
    }
    second_response = {
        **response,
        "id": "rec_response_b",
        "links": [{"type": "result_of", "record": "rec_request_b"}],
    }
    request["data"] = {"context_refs": ["rec_message_a"]}
    rows = [
        rows[0],
        message_a,
        request,
        response,
        message_b,
        second_request,
        second_response,
        *rows[3:],
    ]
    for sequence, row in enumerate(rows, 1):
        row["seq"] = sequence
    _account(manifest, rows)

    report = validate_artifact(
        ".",
        injected={"manifest": manifest, "rows": rows},
    )

    assert report.valid
    assert report.issues == ()


@pytest.mark.parametrize(
    ("base_ref", "expected_issue"),
    [
        ("rec_message_a", "CONTEXT_BASE_REFERENCE_INVALID"),
        ("rec_request_b", "CONTEXT_BASE_REFERENCE_INVALID"),
    ],
)
def test_validation_rejects_non_request_or_forward_context_base(
    base_ref: str,
    expected_issue: str,
) -> None:
    manifest, rows = _rich_injected()
    message = {
        "id": "rec_message_a",
        "seq": 2,
        "time": rows[0]["time"],
        "kind": "message",
        "origin": "observed",
        "source": rows[0]["source"],
        "parent": "rec_root",
        "data": {"role": "user", "content": "A"},
    }
    rows.insert(1, message)
    rows[2]["data"] = {
        "context_base_ref": base_ref,
        "context_refs": [],
    }
    rows.append(
        {
            **rows[2],
            "id": "rec_request_b",
            "parent": "rec_root",
            "data": {"context_refs": ["rec_message_a"]},
        }
    )
    for sequence, row in enumerate(rows, 1):
        row["seq"] = sequence
    _account(manifest, rows)

    report = validate_artifact(
        ".",
        injected={"manifest": manifest, "rows": rows},
    )

    assert not report.valid
    assert expected_issue in report.issues


def test_validation_rejects_context_base_with_unobserved_context() -> None:
    manifest, rows = _rich_injected()
    rows[1]["data"] = {}
    rows.insert(
        3,
        {
            **rows[1],
            "id": "rec_request_b",
            "data": {
                "context_base_ref": "rec_request",
                "context_refs": [],
            },
        },
    )
    for sequence, row in enumerate(rows, 1):
        row["seq"] = sequence
    _account(manifest, rows)

    report = validate_artifact(
        ".",
        injected={"manifest": manifest, "rows": rows},
    )

    assert not report.valid
    assert "CONTEXT_BASE_REFERENCE_INVALID" in report.issues


def test_validation_does_not_expand_a_request_with_invalid_context_refs() -> None:
    manifest, rows = _rich_injected()
    rows[1]["data"] = {"context_refs": ["missing"]}
    rows.insert(
        3,
        {
            **rows[1],
            "id": "rec_request_b",
            "data": {
                "context_base_ref": "rec_request",
                "context_refs": [],
            },
        },
    )
    for sequence, row in enumerate(rows, 1):
        row["seq"] = sequence
    _account(manifest, rows)

    report = validate_artifact(
        ".",
        injected={"manifest": manifest, "rows": rows},
    )

    assert not report.valid
    assert "CONTEXT_REFERENCE_INVALID" in report.issues
    assert "CONTEXT_BASE_REFERENCE_INVALID" in report.issues


def test_validation_reports_non_array_context_refs_explicitly() -> None:
    manifest, rows = _rich_injected()
    rows[1]["data"] = {"context_refs": "not-an-array"}
    _account(manifest, rows)

    report = validate_artifact(
        ".",
        injected={"manifest": manifest, "rows": rows},
    )

    assert not report.valid
    assert "RECORD_SCHEMA_INVALID" in report.issues
    assert "CONTEXT_REFERENCE_INVALID" in report.issues


def test_model_evidence_requires_a_completed_response() -> None:
    manifest, fixture_rows = _rich_injected()

    for status in ("failed", "incomplete", "cancelled"):
        rows = deepcopy(fixture_rows)
        response = next(row for row in rows if row["kind"] == "model.response")
        response["data"]["status"] = status
        _account(manifest, rows)

        report = validate_artifact(
            "",
            injected={"manifest": manifest, "rows": rows},
            required_evidence=("model",),
        )

        assert "REQUIRED_EVIDENCE_MISSING:model" in report.issues

        rich = validate_artifact(
            "",
            injected={"manifest": manifest, "rows": rows},
            profile="rich-agent",
        )
        assert "PROFILE_PAIR_MISSING:model" in rich.issues


def test_tool_evidence_requires_a_succeeded_result() -> None:
    manifest, fixture_rows = _rich_injected()

    for status in ("failed", "cancelled"):
        rows = deepcopy(fixture_rows)
        result = next(row for row in rows if row["kind"] == "tool.result")
        result["data"]["status"] = status
        _account(manifest, rows)

        report = validate_artifact(
            "",
            injected={"manifest": manifest, "rows": rows},
            required_evidence=("tool",),
        )

        assert "REQUIRED_EVIDENCE_MISSING:tool" in report.issues

        rich = validate_artifact(
            "",
            injected={"manifest": manifest, "rows": rows},
            profile="rich-agent",
        )
        assert "PROFILE_PAIR_MISSING:tool" in rich.issues


def test_rich_agent_profile_rejects_evidence_split_across_roots() -> None:
    manifest, rows = _rich_injected()
    second_root = deepcopy(rows[0])
    second_root.update(id="rec_root_second", data={"name": "second-agent"})
    rows.insert(3, second_root)
    for row in rows:
        if row["kind"] in {"tool.call", "tool.result"}:
            row["parent"] = second_root["id"]
    second_outcome = deepcopy(rows[-1])
    second_outcome.update(id="rec_outcome_second", parent=second_root["id"])
    rows.append(second_outcome)
    for sequence, row in enumerate(rows, 1):
        row["seq"] = sequence
    _account(manifest, rows)

    report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": rows},
        profile="rich-agent",
    )

    assert not report.valid
    assert "PROFILE_PAIR_ROOT_MISMATCH" in report.issues


def test_rich_agent_profile_requires_exact_pairs() -> None:
    manifest, rows = _rich_injected()
    rows[4]["links"] = [{"type": "derived_from", "record": "rec_tool_call"}]
    rows.pop(5)
    for sequence, row in enumerate(rows, 1):
        row["seq"] = sequence
    _account(manifest, rows)

    report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": rows},
        profile="rich-agent",
    )

    assert "PROFILE_PAIR_MISSING:tool" in report.issues


def test_checks_only_explicitly_required_source_activity_and_evidence() -> None:
    manifest, rows = _rich_injected()

    without_delivery = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": rows},
        required_evidence=("root", "model", "tool", "delivery"),
        required_source_activity=("fixture", "missing-source"),
    )

    assert not without_delivery.valid
    assert "REQUIRED_EVIDENCE_MISSING:delivery" in without_delivery.issues
    assert (
        "REQUIRED_SOURCE_ACTIVITY_MISSING:missing-source"
        in without_delivery.issues
    )
    assert any(
        activity.name == "fixture" and activity.records > 0
        for activity in without_delivery.source_activity
    )

    delivered = deepcopy(rows)
    delivered[-1]["links"] = [
        {"type": "derived_from", "record": "rec_tool_result"}
    ]
    _account(manifest, delivered)
    complete = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": delivered},
        required_evidence=("root", "model", "tool", "delivery"),
        required_source_activity=("fixture",),
    )

    assert complete.valid
    assert complete.issues == ()

    failed_outcome = deepcopy(rows)
    failed_outcome[-1]["data"] = {"status": "failed", "output": "partial"}
    _account(manifest, failed_outcome)
    failed_outcome_report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": failed_outcome},
        required_evidence=("delivery",),
    )
    assert "REQUIRED_EVIDENCE_MISSING:delivery" in failed_outcome_report.issues

    failed_response = deepcopy(rows)
    failed_response[2]["data"]["status"] = "failed"
    failed_response[-1]["links"] = [
        {"type": "derived_from", "record": "rec_response"}
    ]
    _account(manifest, failed_response)
    failed_response_report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": failed_response},
        required_evidence=("delivery",),
    )
    assert "REQUIRED_EVIDENCE_MISSING:delivery" in failed_response_report.issues

    wrong_delivery = deepcopy(rows)
    wrong_delivery[-1]["links"] = [
        {"type": "derived_from", "record": "rec_tool_call"}
    ]
    _account(manifest, wrong_delivery)
    wrong_delivery_report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": wrong_delivery},
        required_evidence=("delivery",),
    )

    assert "REQUIRED_EVIDENCE_MISSING:delivery" in wrong_delivery_report.issues


def test_idle_bundle_remains_valid_unless_activity_is_required() -> None:
    manifest, _rows = _rich_injected()
    _account(manifest, [])

    structural = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": []},
    )

    assert structural.valid
    assert structural.issues == ()
    assert any(
        activity.name == "fixture" and activity.records == 0
        for activity in structural.source_activity
    )

    required = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": []},
        required_evidence=("root",),
        required_source_activity=("fixture",),
    )

    assert not required.valid
    assert "REQUIRED_EVIDENCE_MISSING:root" in required.issues
    assert (
        "REQUIRED_SOURCE_ACTIVITY_MISSING:fixture" in required.issues
    )


def test_malformed_manifest_reports_empty_source_activity() -> None:
    report = validate_artifact(
        "",
        injected={"manifest": [], "rows": []},
    )

    assert not report.valid
    assert "MANIFEST_SCHEMA_INVALID" in report.issues
    assert report.source_activity == ()


def test_rejects_unknown_required_semantic_evidence() -> None:
    manifest, rows = _rich_injected()

    with pytest.raises(ValueError, match="required_evidence contains unknown value"):
        validate_artifact(
            "",
            injected={"manifest": manifest, "rows": rows},
            required_evidence=("unsupported",),  # type: ignore[arg-type]
        )


def test_manifest_accounts_loss_rows_not_semantic_count() -> None:
    manifest, rows = _rich_injected()
    root = rows[0]
    rows.insert(
        -1,
        {
            "id": "loss_counted_gap",
            "seq": 0,
            "time": root["time"],
            "kind": "loss",
            "origin": "observed",
            "source": root["source"],
            "parent": root["id"],
            "data": {
                "reason": "fixture_missing_evidence",
                "stage": "source",
                "count": 3,
                "recoverable": False,
            },
        },
    )
    for sequence, row in enumerate(rows, 1):
        row["seq"] = sequence
    _account(manifest, rows)

    report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": rows},
    )
    assert report.valid
    assert report.issues == ()

    manifest["trace"]["losses"] = 3
    mismatch = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": rows},
    )
    assert not mismatch.valid
    assert "LOSS_COUNT_MISMATCH" in mismatch.issues


@pytest.mark.parametrize(
    ("mutate", "issue"),
    [
        (
            lambda _manifest, rows: rows[1].update(seq=7),
            "SEQUENCE_INVALID",
        ),
        (
            lambda _manifest, rows: rows[1].update(id="rec_root"),
            "RECORD_ID_DUPLICATE",
        ),
        (
            lambda _manifest, rows: rows[1].update(source="src_missing"),
            "SOURCE_UNDECLARED",
        ),
        (
            lambda _manifest, rows: rows[1].update(parent="rec_missing"),
            "PARENT_REFERENCE_INVALID",
        ),
        (
            lambda _manifest, rows: rows[2].update(
                links=[{"type": "result_of", "record": "rec_missing"}]
            ),
            "LINK_REFERENCE_INVALID",
        ),
        (
            lambda manifest, _rows: manifest["trace"].update(records=99),
            "MANIFEST_COUNT_MISMATCH",
        ),
    ],
)
def test_injected_records_reject_broken_trace_invariants(
    mutate: Any,
    issue: str,
) -> None:
    manifest, rows = _rich_injected()
    mutate(manifest, rows)

    report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": rows},
    )

    assert not report.valid
    assert issue in report.issues


def test_injected_records_reject_more_than_one_outcome_for_a_root() -> None:
    manifest, rows = _rich_injected()
    duplicate = deepcopy(rows[-1])
    duplicate.update(id="rec_outcome_duplicate", seq=8)
    rows.append(duplicate)
    _account(manifest, rows)

    report = validate_artifact(
        "",
        injected={"manifest": manifest, "rows": rows},
    )

    assert "ROOT_OUTCOME_DUPLICATE" in report.issues


def test_disk_validation_checks_exact_files_blobs_permissions_and_seal(
    tmp_path: Path,
) -> None:
    artifact = _writer_bundle(tmp_path)
    blob = next((artifact / "blobs").iterdir())
    blob.write_bytes(b"tampered blob!")
    os.chmod(blob, 0o644)
    extra = artifact / "debug.log"
    extra.write_text("do not persist this")
    os.chmod(extra, 0o600)
    manifest = json.loads((artifact / "manifest.json").read_text())
    manifest["state"] = "open"
    (artifact / "manifest.json").write_text(json.dumps(manifest))
    os.chmod(artifact / "manifest.json", 0o600)

    report = validate_artifact(artifact)

    assert {
        "ARTIFACT_NOT_SEALED",
        "BLOB_DIGEST_MISMATCH",
        "FILE_PERMISSION_INVALID",
        "UNDECLARED_ARTIFACT_FILE",
    }.issubset(report.issues)


def test_final_scan_covers_undeclared_files(tmp_path: Path) -> None:
    artifact = _writer_bundle(tmp_path)
    secret = "fixture-secret-value-123"
    leaked = artifact / "debug.log"
    leaked.write_text(secret)
    os.chmod(leaked, 0o600)

    report = validate_artifact(artifact, secret_values=[secret])

    assert report.secret_matches == 1
    assert "SECRET_MATCH" in report.issues


@pytest.mark.parametrize("filename", ["manifest.json", "trace.jsonl"])
def test_validation_rejects_symlinked_core_files_before_reading(
    tmp_path: Path,
    filename: str,
) -> None:
    artifact = _writer_bundle(tmp_path)
    original = artifact / filename
    external = tmp_path / f"external-{filename}"
    external.write_bytes(original.read_bytes())
    original.unlink()
    original.symlink_to(external)

    report = validate_artifact(artifact)

    assert report.rows == 0
    assert report.issues == ("ARTIFACT_UNREADABLE",)


def test_validation_does_not_follow_symlinked_declared_blob(
    tmp_path: Path,
) -> None:
    artifact = _writer_bundle(tmp_path)
    blob = next((artifact / "blobs").iterdir())
    external = tmp_path / "external-blob"
    external.write_bytes(blob.read_bytes())
    blob.unlink()
    blob.symlink_to(external)

    report = validate_artifact(artifact)

    assert "FILE_TYPE_INVALID" in report.issues
    assert "BLOB_MISSING" in report.issues
