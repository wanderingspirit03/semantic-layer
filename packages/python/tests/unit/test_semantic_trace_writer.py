from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from semantic_layer import CaptureSource, initialize, reset_capture_for_tests
from semantic_layer import capture_v1 as capture_module
from semantic_layer._adapter_native import native_snapshot
from semantic_layer.validation import validate_artifact


@pytest.fixture(autouse=True)
def reset() -> Iterator[None]:
    reset_capture_for_tests()
    yield
    reset_capture_for_tests()


def _rows(artifact: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in (artifact / "trace.jsonl").read_text().splitlines()
        if line
    ]


def _schemas() -> tuple[dict[str, Any], dict[str, Any]]:
    root = Path(__file__).resolve().parents[4]
    contract = root / "contracts" / "trace" / "v1"
    return (
        json.loads((contract / "semantic-trace-manifest.schema.json").read_text()),
        json.loads((contract / "semantic-trace-record.schema.json").read_text()),
    )


def _assert_contract_bundle(artifact: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = json.loads((artifact / "manifest.json").read_text())
    rows = _rows(artifact)
    manifest_schema, record_schema = _schemas()
    Draft202012Validator(
        manifest_schema, format_checker=FormatChecker()
    ).validate(manifest)
    validator = Draft202012Validator(record_schema, format_checker=FormatChecker())
    for row in rows:
        validator.validate(row)
    assert [row["seq"] for row in rows] == list(range(1, len(rows) + 1))
    assert manifest["trace"]["records"] == len(rows)
    assert manifest["trace"]["last_seq"] == len(rows)
    assert manifest["trace"]["bytes"] == (artifact / "trace.jsonl").stat().st_size
    assert manifest["trace"]["sha256"] == hashlib.sha256(
        (artifact / "trace.jsonl").read_bytes()
    ).hexdigest()
    return manifest, rows


def test_managed_installation_emits_valid_manifest_v2(tmp_path: Path) -> None:
    installation_id = "install_abcdefghijklmnopqrstuv"
    capture = initialize(
        output=tmp_path,
        service_name="managed-writer",
        installation_id=installation_id,
    )
    with capture.observe("managed-run"):
        pass

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    manifest = json.loads((artifact / "manifest.json").read_text())

    assert manifest["schema"] == "semantic_trace_manifest_v2"
    assert manifest["record_schema"] == "semantic_trace_record_v1"
    assert manifest["installation_id"] == installation_id
    assert manifest["capture_policy"] == "rich-credential-scrubbed"
    assert {
        "status": "exact_qualified"
    } in [source["qualification"] for source in manifest["sources"]]
    report = validate_artifact(artifact)
    assert report.valid, report.issues


def test_managed_installation_rejects_host_derived_identity(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="installation_id"):
        initialize(
            output=tmp_path,
            service_name="managed-writer",
            installation_id="customer-hostname",
        )


def test_manual_root_tool_and_blob_persist_as_compact_bundle(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="writer-fixture")
    payload = b"semantic-trace-binary"
    with capture.observe("coding-agent", input={"task": "inspect"}) as root:
        assert root.tool("read", {"path": "README.md"}, lambda _value: payload) == payload

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    manifest, rows = _assert_contract_bundle(artifact)

    assert not (artifact / "capture.jsonl").exists()
    assert [row["kind"] for row in rows] == [
        "run.start",
        "tool.call",
        "tool.result",
        "run.outcome",
    ]
    tool_call = rows[1]
    tool_result = rows[2]
    assert tool_result["parent"] == rows[0]["id"]
    assert tool_result["links"] == [{"type": "result_of", "record": tool_call["id"]}]
    blob = artifact / tool_result["blob_refs"][0]["path"]
    assert blob.read_bytes() == payload
    assert manifest["blobs"] == {
        "path": "blobs",
        "count": 1,
        "bytes": len(payload),
    }
    manual_source = next(source for source in manifest["sources"] if source["name"] == "manual")
    assert manual_source["id"] == (
        "src_" + hashlib.sha256(b"builtin/manual").hexdigest()[:24]
    )
    assert manual_source["id"] == rows[0]["source"]
    assert stat.S_IMODE(artifact.stat().st_mode) == 0o700
    assert stat.S_IMODE((artifact / "trace.jsonl").stat().st_mode) == 0o600
    assert stat.S_IMODE(blob.stat().st_mode) == 0o600


def test_staged_blob_bytes_count_toward_queue_high_water(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="blob-queue-accounting")
    payload = b"b" * (128 * 1024)

    with capture.observe("blob.root") as root:
        assert root.tool("blob", None, lambda _value: payload) == payload

    closed = capture.shutdown()
    assert closed.high_water_bytes >= len(payload)
    assert closed.high_water_bytes <= closed.queue_capacity_bytes


def test_native_snapshot_resource_limit_records_one_affected_loss(
    tmp_path: Path,
) -> None:
    snapshot = native_snapshot("x" * (8 * 1024 * 1024 + 1))
    assert snapshot == {"native_type": "str", "omitted": "resource_limit"}
    capture = initialize(output=tmp_path, service_name="native-resource-limit")

    with capture.observe("native-resource-limit", input=snapshot):
        pass

    artifact = Path(capture.shutdown().artifact_path)
    rows = _rows(artifact)
    root = next(row for row in rows if row["kind"] == "run.start")
    losses = [
        row
        for row in rows
        if row["kind"] == "loss"
        and row["data"]["reason"] == "serialization_failure"
    ]
    assert len(losses) == 1
    assert losses[0]["links"] == [{"type": "affects", "record": root["id"]}]
    assert validate_artifact(artifact).valid


def test_staged_blob_is_rejected_when_it_exceeds_remaining_queue_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = initialize(output=tmp_path, service_name="blob-queue-rejection")
    monkeypatch.setattr(capture_module, "QUEUE_CAPACITY", 64 * 1024)
    payload = b"b" * (128 * 1024)

    with capture.observe("blob.root") as root:
        assert root.tool("blob", None, lambda _value: payload) == payload

    closed = capture.shutdown()
    assert closed.losses["queue_backpressure_drop"] >= 1
    assert not list((Path(closed.artifact_path) / "blobs").glob("*.blob"))


def test_open_source_trace_capacity_rejects_new_roots_without_evicting_active_ones(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(capture_module, "MAX_OPEN_SOURCE_TRACES", 3)
    receipts: list[Any] = []

    class ManyOpenTraces(CaptureSource):
        metadata = {
            "name": "open-trace-capacity",
            "seam": "fixture.callback",
            "identity_domain": "fixture.operation",
            "coverage": [],
        }

        def install(self, sink: Any) -> Any:
            for index in range(4):
                receipts.append(sink.open_trace({"name": f"root-{index}"}))

            class Lifecycle:
                def deactivate(self) -> None:
                    return None

                def drain(self) -> None:
                    return None

            return Lifecycle()

    capture = initialize(output=tmp_path, service_name="open-trace-capacity")
    capture.install_source(ManyOpenTraces())

    assert [receipt.accepted for receipt in receipts] == [True, True, True, False]
    assert receipts[-1].reason == "correlation_capacity"
    assert len(capture._runtime.open_source_traces) == 3
    artifact = Path(capture.shutdown().artifact_path)
    assert validate_artifact(artifact).valid


def test_shutdown_reports_persisted_source_loss_counts(
    tmp_path: Path,
) -> None:
    class CountedGapSource(CaptureSource):
        metadata = {
            "name": "counted-gap",
            "seam": "fixture.callback",
            "identity_domain": "fixture.operation",
            "coverage": [],
        }

        def install(self, sink: Any) -> Any:
            opened = sink.open_trace(
                {
                    "name": "counted-gap-run",
                    "semantic": {
                        "type": "workflow.run",
                        "name": "counted-gap-run",
                    },
                }
            )
            assert opened.accepted and opened.identity is not None
            sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "counted-gap",
                    "trace": opened.identity,
                    "native": None,
                    "semantic": {
                        "type": "capture.gap",
                        "reason": "fixture_missing_evidence",
                        "count": 3,
                    },
                }
            )
            sink.record(
                {
                    "kind": "lifecycle",
                    "phase": "end",
                    "name": "counted-gap-run",
                    "trace": opened.identity,
                    "parent_record_id": opened.record_id,
                    "native": None,
                    "semantic": {
                        "type": "workflow.run",
                        "status": "succeeded",
                    },
                }
            )

            class Lifecycle:
                def deactivate(self) -> None:
                    return None

                def drain(self) -> None:
                    return None

            return Lifecycle()

    capture = initialize(output=tmp_path, service_name="counted-gap")
    capture.install_source(CountedGapSource())
    closed = capture.shutdown()
    rows = _rows(Path(closed.artifact_path))

    losses = [row for row in rows if row["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["count"] == 3
    assert closed.losses == {"fixture_missing_evidence": 3}


class _NestedSource(CaptureSource):
    metadata = {
        "name": "nested-fixture",
        "seam": "fixture.callback",
        "identity_domain": "fixture",
        "coverage": [{"operation": "framework.run", "domain": "fixture"}],
    }

    def __init__(self) -> None:
        self.receipts: dict[str, Any] = {}

    def install(self, sink: Any) -> Any:
        opened = sink.open_trace(
            {
                "name": "workflow",
                "native_identity": "workflow-1",
                "semantic": {"type": "workflow.run", "name": "workflow"},
            }
        )
        assert opened.accepted and opened.identity is not None
        scope = sink.record(
            {
                "kind": "lifecycle",
                "phase": "start",
                "name": "step",
                "trace": opened.identity,
                "semantic": {
                    "type": "workflow.step",
                    "scope_type": "step",
                    "name": "step",
                },
                "parent_record_id": opened.record_id,
                "native": None,
            }
        )
        redundant = sink.record(
            {
                "kind": "log",
                "phase": "event",
                "name": "duplicate",
                "trace": opened.identity,
                "semantic": {"type": "capture.redundant"},
                "parent_record_id": scope.record_id,
                "native": None,
            }
        )
        terminal = sink.record(
            {
                "kind": "lifecycle",
                "phase": "error",
                "name": "step",
                "trace": opened.identity,
                "semantic": {
                    "type": "workflow.step",
                    "status": "failed",
                    "error": {
                        "type": "error.fixture",
                        "message": "step failed",
                        "recoverable": False,
                    },
                },
                "parent_record_id": scope.record_id,
                "native": None,
            }
        )
        after_nested = sink.record(
            {
                "kind": "state",
                "phase": "event",
                "name": "state.recovered",
                "trace": opened.identity,
                "semantic": {
                    "type": "state.transition",
                    "state_type": "state.recovered",
                    "value": True,
                },
                "parent_record_id": opened.record_id,
                "native": None,
            }
        )
        root_terminal = sink.record(
            {
                "kind": "lifecycle",
                "phase": "end",
                "name": "workflow",
                "trace": opened.identity,
                "semantic": {"type": "workflow.run", "status": "succeeded"},
                "parent_record_id": opened.record_id,
                "native": None,
            }
        )
        self.receipts = {
            "opened": opened,
            "scope": scope,
            "redundant": redundant,
            "terminal": terminal,
            "after_nested": after_nested,
            "root_terminal": root_terminal,
        }

        class Lifecycle:
            def deactivate(self) -> None:
                return None

            def drain(self) -> None:
                return None

        return Lifecycle()


class _ErrorIdentitySource(CaptureSource):
    metadata = {
        "name": "error-identity-fixture",
        "seam": "fixture.error",
        "identity_domain": "fixture.error",
        "coverage": [],
    }

    def __init__(self) -> None:
        self.sink: Any = None

    def install(self, sink: Any) -> Any:
        self.sink = sink

        class Lifecycle:
            def deactivate(self) -> None:
                return None

            def drain(self) -> None:
                return None

        return Lifecycle()

    def emit(self, error: BaseException) -> Any:
        opened = self.sink.open_trace(
            {
                "name": "source-operation",
                "semantic": {"type": "agent.run", "name": "source-operation"},
            }
        )
        assert opened.accepted and opened.identity is not None
        receipt = self.sink.record(
            {
                "kind": "error",
                "phase": "error",
                "name": "source.error",
                "trace": opened.identity,
                "parent_record_id": opened.record_id,
                "native": {"observed": True},
                "semantic": {
                    "type": "agent.error",
                    "error": {
                        "type": "error.fixture",
                        "message": str(error),
                        "recoverable": False,
                    },
                },
                "error_identity": error,
            }
        )
        self.sink.record(
            {
                "kind": "lifecycle",
                "phase": "error",
                "name": "source-operation",
                "trace": opened.identity,
                "parent_record_id": opened.record_id,
                "native": None,
                "semantic": {"type": "agent.run", "status": "failed"},
            }
        )
        return receipt

    def reject(self, error: BaseException) -> Any:
        opened = self.sink.open_trace(
            {
                "name": "closed-source-operation",
                "semantic": {
                    "type": "agent.run",
                    "name": "closed-source-operation",
                },
            }
        )
        assert opened.accepted and opened.identity is not None
        self.sink.record(
            {
                "kind": "lifecycle",
                "phase": "end",
                "name": "closed-source-operation",
                "trace": opened.identity,
                "parent_record_id": opened.record_id,
                "native": None,
                "semantic": {"type": "agent.run", "status": "completed"},
            }
        )
        return self.sink.record(
            {
                "kind": "error",
                "phase": "error",
                "name": "rejected.error",
                "trace": opened.identity,
                "parent_record_id": opened.record_id,
                "native": None,
                "semantic": {
                    "type": "agent.error",
                    "error": {
                        "type": "error.fixture",
                        "message": str(error),
                        "recoverable": False,
                    },
                },
                "error_identity": error,
            }
        )


def test_exact_admitted_source_error_identity_suppresses_only_manual_duplicate(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="error-identity")
    source = _ErrorIdentitySource()
    capture.install_source(source)
    expected = RuntimeError("source failure")

    with pytest.raises(RuntimeError) as caught:
        with capture.observe("manual-root"):
            assert source.emit(expected).accepted
            raise expected
    assert caught.value is expected
    assert capture._runtime.source_error_identities == {}

    artifact = Path(capture.shutdown().artifact_path)
    rows = _rows(artifact)
    errors = [row for row in rows if row["kind"] == "error"]
    outcomes = [row for row in rows if row["kind"] == "run.outcome"]
    assert len(errors) == 1
    assert errors[0]["data"]["message"] == "source failure"
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "failed"
    assert "error_identity" not in (artifact / "trace.jsonl").read_text()


def test_exact_admitted_source_error_identity_suppresses_turn_duplicate(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="turn-error-identity")
    source = _ErrorIdentitySource()
    capture.install_source(source)
    expected = RuntimeError("turn failure")

    with capture.observe("manual-root") as root:
        with pytest.raises(RuntimeError) as caught:
            with root.turn("manual-turn"):
                assert source.emit(expected).accepted
                raise expected
        assert caught.value is expected

    rows = _rows(Path(capture.shutdown().artifact_path))
    errors = [row for row in rows if row["kind"] == "error"]
    assert len(errors) == 1
    assert errors[0]["data"]["message"] == "turn failure"


@pytest.mark.parametrize("same_message", [False, True])
def test_distinct_application_error_is_not_deduped_by_source_error(
    tmp_path: Path,
    same_message: bool,
) -> None:
    capture = initialize(output=tmp_path, service_name="distinct-error-identity")
    source = _ErrorIdentitySource()
    capture.install_source(source)
    source_error = RuntimeError("same failure" if same_message else "source failure")
    application_error = RuntimeError(
        "same failure" if same_message else "application failure"
    )

    with pytest.raises(RuntimeError) as caught:
        with capture.observe("manual-root"):
            assert source.emit(source_error).accepted
            raise application_error
    assert caught.value is application_error

    rows = _rows(Path(capture.shutdown().artifact_path))
    errors = [row for row in rows if row["kind"] == "error"]
    assert len(errors) == 2
    assert [row["data"]["message"] for row in errors] == [
        str(source_error),
        str(application_error),
    ]


def test_rejected_source_error_identity_never_suppresses_manual_error(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="rejected-error-identity")
    source = _ErrorIdentitySource()
    capture.install_source(source)
    expected = RuntimeError("application failure")

    with pytest.raises(RuntimeError):
        with capture.observe("manual-root"):
            assert not source.reject(expected).accepted
            raise expected

    rows = _rows(Path(capture.shutdown().artifact_path))
    errors = [row for row in rows if row["kind"] == "error"]
    assert len(errors) == 1
    assert errors[0]["data"]["message"] == "application failure"


def test_zero_row_redundant_multi_record_terminal_and_nested_root_lifecycle(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="nested-writer-fixture")
    source = _NestedSource()
    capture.install_source(source)
    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    manifest, rows = _assert_contract_bundle(artifact)

    assert all(receipt.accepted for receipt in source.receipts.values())
    assert manifest["blobs"] == {"path": "blobs", "count": 0, "bytes": 0}
    assert not (artifact / "blobs").exists()
    redundant_id = source.receipts["redundant"].record_id
    assert redundant_id not in {row["id"] for row in rows}
    kinds = [row["kind"] for row in rows]
    assert kinds == [
        "run.start",
        "scope",
        "scope",
        "error",
        "state",
        "run.outcome",
    ]
    scope_terminal = rows[2]
    assert rows[3]["parent"] == scope_terminal["id"]
    assert rows[4]["parent"] == rows[0]["id"]
    assert rows[5]["parent"] == rows[0]["id"]


@pytest.mark.asyncio
async def test_long_multi_step_run_stays_ordered_correlated_and_bounded(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="long-run-writer-fixture")
    application_results: list[str] = []
    flush_statuses = []
    step_count = 10

    async def inspect(value: dict[str, Any]) -> str:
        await asyncio.sleep(0)
        return f"{value['step']}:{value['lane']}"

    async with capture.observe(
        "repair-workspace", input={"task": "fix failing checks"}
    ) as root:
        for step in range(step_count):
            async with root.turn(f"inspect-step-{step}", input={"step": step}) as turn:
                application_results.extend(
                    await asyncio.gather(
                        turn.tool(
                            "inspect_workspace",
                            {"step": step, "lane": "source"},
                            inspect,
                        ),
                        turn.tool(
                            "inspect_workspace",
                            {"step": step, "lane": "tests"},
                            inspect,
                        ),
                    )
                )
            if (step + 1) % 3 == 0:
                flush_statuses.append(capture.flush())

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    manifest, rows = _assert_contract_bundle(artifact)

    assert application_results == [
        result
        for step in range(step_count)
        for result in (f"{step}:source", f"{step}:tests")
    ]
    assert len(flush_statuses) == 3
    for status_value in [*flush_statuses, closed]:
        assert status_value.pending_bytes == 0
        assert status_value.pending_control_bytes == 0
        assert 0 < status_value.high_water_bytes <= status_value.queue_capacity_bytes

    assert [row["seq"] for row in rows] == list(range(1, len(rows) + 1))
    assert len([row for row in rows if row["kind"] == "scope"]) == step_count * 2
    calls = [row for row in rows if row["kind"] == "tool.call"]
    results = [row for row in rows if row["kind"] == "tool.result"]
    assert len(calls) == step_count * 2
    assert len(results) == step_count * 2
    assert {row["data"]["name"] for row in calls} == {"inspect_workspace"}
    calls_by_record = {row["id"]: row for row in calls}
    for result in results:
        result_link = next(
            link for link in result["links"] if link["type"] == "result_of"
        )
        call = calls_by_record[result_link["record"]]
        assert result["data"]["call_id"] == call["data"]["call_id"]
        assert result["parent"] == call["parent"]
        assert result["data"]["output"] == (
            f"{call['data']['input']['step']}:{call['data']['input']['lane']}"
        )

    assert closed.rejected == 0
    assert closed.losses == {}
    assert not any(row["kind"] == "loss" for row in rows)
    assert manifest["state"] == "sealed"
    assert manifest["trace"]["losses"] == 0
    assert manifest["blobs"] == {"path": "blobs", "count": 0, "bytes": 0}
    assert {path.name for path in artifact.iterdir()} == {
        "manifest.json",
        "trace.jsonl",
    }


def test_persistence_recovery_keeps_durable_prefix_blobs_and_records_one_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture = initialize(output=tmp_path, service_name="recovery-writer-fixture")
    payload = b"durable-before-append-failure"
    with capture.observe("before-failure") as root:
        assert root.tool("read", {"path": "README.md"}, lambda _value: payload) == payload
    capture.flush()
    artifact = Path(capture.status().artifact_path)
    trace_path = artifact / "trace.jsonl"
    durable_rows = _rows(artifact)
    durable_blob = artifact / durable_rows[2]["blob_refs"][0]["path"]
    original_open = Path.open
    append_failed = False

    def fail_first_append(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        nonlocal append_failed
        if path == trace_path and mode == "ab" and not append_failed:
            append_failed = True
            raise OSError("injected append failure")
        return original_open(path, mode, buffering, encoding, errors, newline)

    monkeypatch.setattr(Path, "open", fail_first_append)

    with capture.observe("will-be-discarded"):
        pass
    closed = capture.shutdown()
    manifest, rows = _assert_contract_bundle(Path(closed.artifact_path))

    assert append_failed
    assert manifest["state"] == "sealed"
    assert rows[:-1] == durable_rows
    assert rows[-1]["kind"] == "loss"
    assert rows[-1]["data"] == {
        "reason": "persistence_failure",
        "stage": "persist",
        "count": closed.rejected,
        "recoverable": False,
        "path": "/trace.jsonl",
    }
    assert closed.losses == {"persistence_failure": closed.rejected}
    assert durable_blob.read_bytes() == payload
    assert manifest["blobs"] == {
        "path": "blobs",
        "count": 1,
        "bytes": len(payload),
    }
    validation = validate_artifact(Path(closed.artifact_path))
    assert validation.valid
    assert validation.issues == ()
    assert "trace persistence failed" in (closed.last_error or "")


def test_sealing_hashes_trace_without_reading_the_whole_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = initialize(output=tmp_path, service_name="streaming-seal-digest")
    with capture.observe("streaming-seal"):
        pass
    trace_path = Path(capture.status().artifact_path) / "trace.jsonl"
    original_read_bytes = Path.read_bytes

    def reject_whole_trace_read(path: Path) -> bytes:
        if path == trace_path:
            raise AssertionError("seal must hash the trace incrementally")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", reject_whole_trace_read)
    closed = capture.shutdown()

    assert closed.state == "closed"
    assert json.loads((trace_path.parent / "manifest.json").read_text())["state"] == "sealed"


def test_failed_seal_can_be_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = initialize(output=tmp_path, service_name="retry-seal")
    with capture.observe("retry-seal"):
        pass
    artifact = Path(capture.status().artifact_path)
    manifest_path = artifact / "manifest.json"
    original_replace = os.replace
    failed = False

    def fail_first_sealed_manifest(source: Any, destination: Any) -> None:
        nonlocal failed
        if (
            Path(destination) == manifest_path
            and not failed
            and '"state": "sealed"' in Path(source).read_text()
        ):
            failed = True
            raise OSError("injected seal failure")
        original_replace(source, destination)

    monkeypatch.setattr(os, "replace", fail_first_sealed_manifest)

    first = capture.shutdown()
    second = capture.shutdown()

    assert failed
    assert first.state == "closing"
    assert second.state == "closed"
    assert validate_artifact(artifact).valid


def test_persistence_recovery_refuses_to_seal_when_durable_prefix_is_truncated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    capture = initialize(output=tmp_path, service_name="broken-prefix-fixture")
    with capture.observe("durable-prefix"):
        pass
    capture.flush()
    artifact = Path(capture.status().artifact_path)
    trace_path = artifact / "trace.jsonl"
    durable_trace = trace_path.read_bytes()
    original_open = Path.open
    append_failed = False

    def fail_first_append(
        path: Path,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        nonlocal append_failed
        if path == trace_path and mode == "ab" and not append_failed:
            append_failed = True
            raise OSError("injected append failure")
        return original_open(path, mode, buffering, encoding, errors, newline)

    monkeypatch.setattr(Path, "open", fail_first_append)
    with capture.observe("will-be-discarded"):
        pass
    capture.flush()
    trace_path.write_bytes(b"")

    try:
        with pytest.raises(
            RuntimeError,
            match="durable prefix could not be recovered",
        ):
            capture.shutdown()

        assert append_failed
        assert capture.status().state == "closing"
        manifest = json.loads((artifact / "manifest.json").read_text())
        assert manifest["state"] != "sealed"
    finally:
        trace_path.write_bytes(durable_trace)
