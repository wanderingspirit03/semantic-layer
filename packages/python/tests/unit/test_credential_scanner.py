from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from semantic_layer import capture_v1 as capture_module
from semantic_layer import initialize
from semantic_layer.validation import validate_artifact

_ROOT = Path(__file__).parents[4]
_FIXTURE_HEX = "0123456789abcdef"
_FIXTURE_VALUES = {
    "{{OPENROUTER_KEY_64}}": "-".join(("sk", "or", "v1", _FIXTURE_HEX * 4)),
    "{{OPENROUTER_KEY_32}}": "-".join(("sk", "or", "v1", _FIXTURE_HEX * 2)),
}


def _expand_fixture(value: Any) -> Any:
    if isinstance(value, str):
        return _FIXTURE_VALUES.get(value, value)
    if isinstance(value, list):
        return [_expand_fixture(child) for child in value]
    if isinstance(value, dict):
        return {key: _expand_fixture(child) for key, child in value.items()}
    return value


_CORPUS: dict[str, Any] = _expand_fixture(
    json.loads((_ROOT / "contracts/capture/v1/credential-safety-cases.json").read_text())
)


@pytest.mark.parametrize(
    "fixture",
    _CORPUS["scrub_cases"],
    ids=lambda fixture: str(fixture["id"]),
)
def test_scrub_conformance(fixture: dict[str, Any]) -> None:
    scanner = capture_module._Scanner(fixture["secret_values"])  # noqa: SLF001
    value, redactions = scanner.scrub(fixture["input"])

    assert value == fixture["expected"]
    assert redactions == fixture["redactions"]
    assert scanner.clean_json(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode())


@pytest.mark.parametrize(
    "fixture",
    _CORPUS["scan_cases"],
    ids=lambda fixture: str(fixture["id"]),
)
def test_scan_conformance(fixture: dict[str, Any]) -> None:
    scanner = capture_module._Scanner(fixture["secret_values"])  # noqa: SLF001
    encoded = fixture["text"].encode()
    assert scanner.clean_json(encoded) is fixture["clean"]
    assert scanner.clean(encoded) is fixture["clean"]


def test_uses_language_neutral_detector_specification() -> None:
    assert _CORPUS["detector_spec"] == "semantic-layer-credential-detectors-v4"
    assert (
        capture_module._Scanner.detector_digest
        == hashlib.sha256(  # noqa: SLF001
            _CORPUS["detector_spec"].encode()
        ).hexdigest()
    )


def test_raw_scans_invalid_utf8_blobs_for_exact_known_secret_bytes() -> None:
    scanner = capture_module._Scanner(["fixture-secret-value"])  # noqa: SLF001
    assert not scanner.clean(b"\xff\xfefixture-secret-value\xff")
    assert scanner.clean(b"\xff\xfe\xfd")


def test_one_policy_covers_reasoning_tools_and_blobs_with_safe_partial_trace(
    tmp_path: Path,
) -> None:
    secret = "fixture-secret-ÿÿ"
    percent_encoded = "fixture-secret-%C3%BF%C3%BF"
    base64_encoded = b"Zml4dHVyZS1zZWNyZXQtw7/Dvw=="
    capture = initialize(
        output=tmp_path,
        service_name="credential-policy-paths",
        secret_values=[secret],
    )

    with capture.observe(
        "credential-policy-run",
        input={"reasoning": [{"type": "text", "text": f"consider {secret}"}]},
    ) as root:
        assert root.tool(
            "fixture-tool",
            {"authorization": {"scheme": "custom", "value": secret}},
            lambda _value: {"summary": f"result {percent_encoded}"},
        ) == {"summary": f"result {percent_encoded}"}
        receipt = root.emit("state.binary-evidence", {"content": base64_encoded})
        receipt.settled.result()

    artifact = Path(capture.shutdown().artifact_path)
    trace = (artifact / "trace.jsonl").read_text()
    assert secret not in trace
    assert percent_encoded not in trace
    assert base64_encoded.decode() not in trace
    assert "consider [REDACTED_CREDENTIAL]" in trace
    assert "credential_redaction" in trace
    assert "blob_scan_blocked" in trace
    assert not list((artifact / "blobs").glob("*.blob"))
    report = validate_artifact(artifact, secret_values=[secret])
    assert report.valid
    assert report.secret_matches == 0
