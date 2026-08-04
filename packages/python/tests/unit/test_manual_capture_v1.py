from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest

from semantic_layer import capture_v1 as capture_module
from semantic_layer import initialize, reset_capture_for_tests
from semantic_layer.validation import validate_artifact


def test_continuation_identities_join_two_sealed_bundles(tmp_path: Path) -> None:
    identity_key = "fixture-continuation-key-32-bytes"
    first = initialize(
        output=tmp_path,
        service_name="manual-continuation",
        identity_key=identity_key,
    )
    with first.observe(
        "turn-one",
        conversation_id="conversation-a",
        turn_id="turn-one",
        turn_index=0,
    ):
        pass
    first_path = Path(first.shutdown().artifact_path)
    reset_capture_for_tests()

    second = initialize(
        output=tmp_path,
        service_name="manual-continuation",
        identity_key=identity_key,
    )
    with second.observe(
        "turn-two",
        conversation_id="conversation-a",
        turn_id="turn-two",
        turn_index=1,
        previous_turn_id="turn-one",
    ):
        pass
    second_path = Path(second.shutdown().artifact_path)

    first_start = next(
        row
        for row in (
            json.loads(line) for line in (first_path / "trace.jsonl").read_text().splitlines()
        )
        if row["kind"] == "run.start"
    )
    second_start = next(
        row
        for row in (
            json.loads(line)
            for line in (second_path / "trace.jsonl").read_text().splitlines()
        )
        if row["kind"] == "run.start"
    )
    assert second_start["data"]["conversation_id"] == first_start["data"]["conversation_id"]
    assert second_start["data"]["previous_turn_id"] == first_start["data"]["turn_id"]
    assert second_start["data"]["turn_index"] == 1
    assert second_start["data"]["turn_id"] != first_start["data"]["turn_id"]


def test_raw_identities_are_always_valid_trace_ids(tmp_path: Path) -> None:
    capture = initialize(
        output=tmp_path,
        service_name="manual-raw-identity",
        identity_mode="raw",
    )
    with capture.observe(
        "raw-identity",
        conversation_id="UPPER CASE/" * 20,
        turn_id="TURN/" * 40,
        turn_index=0,
    ):
        pass
    for name, turn_id in (
        ("case-upper", "A"),
        ("case-lower", "a"),
        ("punctuation-slash", "same/punctuation"),
        ("punctuation-question", "same?punctuation"),
        ("long-suffix-a", f"{'x' * 300}A"),
        ("long-suffix-b", f"{'x' * 300}B"),
    ):
        with capture.observe(name, turn_id=turn_id, turn_index=0):
            pass

    artifact = Path(capture.shutdown().artifact_path)
    starts = {
        row["data"]["name"]: row
        for row in (
            json.loads(line)
            for line in (artifact / "trace.jsonl").read_text().splitlines()
        )
        if row["kind"] == "run.start"
    }
    for start in starts.values():
        for field in ("conversation_id", "turn_id", "previous_turn_id"):
            identity = start["data"].get(field)
            if identity is not None:
                assert re.fullmatch(r"[a-z][a-z0-9._:-]{7,127}", identity)
    for left, right in (
        ("case-upper", "case-lower"),
        ("punctuation-slash", "punctuation-question"),
        ("long-suffix-a", "long-suffix-b"),
    ):
        assert starts[left]["data"]["turn_id"] != starts[right]["data"]["turn_id"]
    assert starts["case-upper"]["data"]["turn_id"].endswith(
        f"_{hashlib.sha256(b'A').hexdigest()[:16]}"
    )
    assert validate_artifact(artifact).valid


def test_completed_turn_correlation_history_is_bounded_across_flushes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(capture_module, "MAX_RUNTIME_CORRELATION_ENTRIES", 3)
    capture = initialize(output=tmp_path, service_name="manual-bounded-correlation")

    for index in range(6):
        with capture.observe(
            f"turn-{index}",
            conversation_id=f"conversation-{index}",
            turn_id=f"turn-{index}",
            turn_index=index,
        ):
            pass
        capture.flush()

    assert len(capture._runtime.conversation_order) == 3
    assert len(capture._runtime.turn_indexes) == 3
    artifact = Path(capture.shutdown().artifact_path)
    assert capture._runtime.conversation_order == {}
    assert capture._runtime.turn_indexes == {}
    assert validate_artifact(artifact).valid


def test_manual_capture_preserves_exact_sync_async_and_error_behavior(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="manual-behavior")
    sentinel = object()
    expected = RuntimeError("tool failed")

    async def async_tool(value: dict[str, int]) -> dict[str, int]:
        return value

    with capture.observe("manual.root", input={"task": "inspect"}) as root:
        assert root.tool("identity", {"value": 1}, lambda _value: sentinel) is sentinel
        with pytest.raises(RuntimeError) as caught:
            root.tool("failure", {}, lambda _value: (_ for _ in ()).throw(expected))
        assert caught.value is expected

    async def exercise_async() -> Any:
        async with capture.observe("manual.async") as root:
            return await root.tool("async", {"value": 2}, async_tool)

    assert asyncio.run(exercise_async()) == {"value": 2}
    status = capture.shutdown()
    artifact = Path(status.artifact_path)
    assert status.state == "closed"
    assert (artifact / "trace.jsonl").is_file()
    assert not (artifact / "capture.jsonl").exists()
    assert validate_artifact(artifact).valid


def test_manual_observation_set_output_preserves_exact_falsey_json_values(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="manual-output")
    expected = [False, 0, "", [], {}, None, {"nested": [0, False, None]}]

    for index, value in enumerate(expected):
        with capture.observe(f"manual-output-{index}") as scope:
            assert scope.set_output(value) is value

    artifact = Path(capture.shutdown().artifact_path)
    outcomes = [
        record
        for record in (
            json.loads(line)
            for line in (artifact / "trace.jsonl").read_text().splitlines()
        )
        if record["kind"] == "run.outcome"
    ]
    assert [record["data"]["output"] for record in outcomes] == expected
    assert validate_artifact(artifact).valid


def test_manual_observation_failure_omits_previously_set_output(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="manual-failed-output")
    expected = RuntimeError("application failed after producing a candidate")

    with pytest.raises(RuntimeError) as caught:
        with capture.observe("manual-failed-output") as scope:
            scope.set_output({"candidate": True})
            raise expected
    assert caught.value is expected

    artifact = Path(capture.shutdown().artifact_path)
    outcome = next(
        record
        for record in (
            json.loads(line)
            for line in (artifact / "trace.jsonl").read_text().splitlines()
        )
        if record["kind"] == "run.outcome"
    )
    assert outcome["data"] == {"status": "failed"}
    assert validate_artifact(artifact).valid


def test_falsey_exception_is_still_captured_as_a_failed_observation(
    tmp_path: Path,
) -> None:
    class FalseyError(RuntimeError):
        def __bool__(self) -> bool:
            return False

    capture = initialize(output=tmp_path, service_name="manual-falsey-error")
    expected = FalseyError("falsey failure")

    with pytest.raises(FalseyError) as caught:
        with capture.observe("manual-falsey-error"):
            raise expected
    assert caught.value is expected

    artifact = Path(capture.shutdown().artifact_path)
    records = [
        json.loads(line)
        for line in (artifact / "trace.jsonl").read_text().splitlines()
    ]
    errors = [record for record in records if record["kind"] == "error"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(errors) == 1
    assert errors[0]["data"]["message"] == "falsey failure"
    assert [record["data"] for record in outcomes] == [{"status": "failed"}]


def test_configured_secret_is_removed_and_reported_as_loss(tmp_path: Path) -> None:
    secret = "sk-compact-suite-private-value"
    capture = initialize(
        output=tmp_path,
        service_name="manual-privacy",
        secret_values=[secret],
    )
    with capture.observe("privacy.root") as root:
        assert root.tool("echo", {"token": secret}, lambda value: value) == {
            "token": secret
        }
    artifact = Path(capture.shutdown().artifact_path)
    trace_text = (artifact / "trace.jsonl").read_text()
    records = [json.loads(line) for line in trace_text.splitlines()]
    assert secret not in trace_text
    assert any(
        record["kind"] == "loss"
        and record["data"]["reason"] == "credential_redaction"
        for record in records
    )
    assert validate_artifact(artifact, secret_values=[secret]).valid


def test_record_redactions_are_one_counted_loss(tmp_path: Path) -> None:
    secret = "sk-compact-suite-private-value"
    capture = initialize(
        output=tmp_path,
        service_name="manual-redaction-count",
        secret_values=[secret],
    )
    with capture.observe("privacy.root") as root:
        root.emit("privacy.state", {"first": secret, "second": secret})
    artifact = Path(capture.shutdown().artifact_path)
    records = [
        json.loads(line) for line in (artifact / "trace.jsonl").read_text().splitlines()
    ]
    losses = [
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"] == "credential_redaction"
    ]
    assert len(losses) == 1
    assert losses[0]["data"]["count"] == 2


@pytest.mark.parametrize("secret", ["a", "1234567"])
def test_short_configured_secret_is_rejected(
    tmp_path: Path,
    secret: str,
) -> None:
    with pytest.raises(
        ValueError,
        match="configured secret values must contain at least 8 bytes",
    ):
        initialize(
            output=tmp_path,
            service_name="manual-short-secret",
            secret_values=[secret],
        )


def test_minimum_length_configured_secret_is_removed_when_embedded(
    tmp_path: Path,
) -> None:
    secret = "12345678"
    leaked_value = f"prefixx{secret}xsuffix"
    capture = initialize(
        output=tmp_path,
        service_name="manual-minimum-secret",
        secret_values=[secret],
    )
    with capture.observe("privacy.root", input={"value": leaked_value}):
        pass

    artifact = Path(capture.shutdown().artifact_path)
    trace_text = (artifact / "trace.jsonl").read_text()
    assert leaked_value not in trace_text
    assert "[REDACTED_CREDENTIAL]" in trace_text
    assert validate_artifact(artifact, secret_values=[secret]).valid


def test_signed_query_redaction_before_json_quote_escape_is_clean() -> None:
    scanner = capture_module._Scanner([])  # noqa: SLF001 - scanner parity regression
    scrubbed, redactions = scanner.scrub(
        {"output": 'command "https://example.test/callback?api_key=sensitive-query-value"'}
    )
    encoded = json.dumps(scrubbed, separators=(",", ":")).encode()

    assert redactions == 1
    assert b"sensitive-query-value" not in encoded
    assert scanner.clean_json(encoded)


@pytest.mark.parametrize("secret", ["CREDENTI", "[REDACTED_CREDENTIAL]"])
def test_configured_secret_cannot_collide_with_redaction_marker(
    tmp_path: Path,
    secret: str,
) -> None:
    capture = initialize(
        output=tmp_path,
        service_name="manual-marker-collision",
        secret_values=[secret],
    )
    with capture.observe("privacy.root", input={"value": f"prefix-{secret}-suffix"}):
        pass

    artifact = Path(capture.shutdown().artifact_path)
    for path in artifact.rglob("*"):
        if path.is_file():
            assert secret.encode() not in path.read_bytes()
    assert validate_artifact(artifact, secret_values=[secret]).valid


def test_stale_run_symlink_is_quarantined_without_touching_target(
    tmp_path: Path,
) -> None:
    external = tmp_path / "external-run"
    external.mkdir()
    lock = external / ".writer.lock"
    lock.write_text(json.dumps({"pid": 0}))
    (external / "manifest.json").write_text(json.dumps({"state": "open"}))
    (external / "trace.jsonl").write_text("")
    output = tmp_path / "capture"
    output.mkdir()
    linked_run = output / "run-linked"
    linked_run.symlink_to(external, target_is_directory=True)

    capture = initialize(output=output, service_name="manual-symlink-recovery")
    capture.shutdown()

    assert lock.exists()
    quarantined = output / "quarantine-run-linked"
    assert quarantined.is_symlink()
    assert quarantined.resolve() == external


def test_hostile_and_cyclic_values_never_escape_the_public_api(
    tmp_path: Path,
) -> None:
    class HostileMapping(Mapping[str, object]):
        def __getitem__(self, _key: str) -> object:
            raise RuntimeError("hostile getter")

        def __iter__(self) -> Iterator[str]:
            return iter(("dangerous",))

        def __len__(self) -> int:
            return 1

    cyclic: dict[str, object] = {"safe": True}
    cyclic["cycle"] = cyclic
    hostile = HostileMapping()
    capture = initialize(output=tmp_path, service_name="manual-hostile")
    with capture.observe("hostile.root") as root:
        assert root.tool("hostile", hostile, lambda value: value) is hostile
        assert root.tool("cyclic", cyclic, lambda value: value) is cyclic

    artifact = Path(capture.shutdown().artifact_path)
    records = [
        json.loads(line)
        for line in (artifact / "trace.jsonl").read_text().splitlines()
    ]
    assert any(record["kind"] == "loss" for record in records)
    assert validate_artifact(artifact).valid
