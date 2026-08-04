from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from pathlib import Path

from semantic_layer import initialize
from semantic_layer.parent_context import is_valid_traceparent
from semantic_layer.validation import validate_artifact

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def test_shared_w3c_traceparent_corpus() -> None:
    root = Path(__file__).resolve().parents[4]
    corpus = json.loads((root / "contracts/capture/v1/traceparent-cases.json").read_text())
    assert all(is_valid_traceparent(value) for value in corpus["valid"])
    assert not any(is_valid_traceparent(value) for value in corpus["invalid"])


def test_hostile_parent_context_cannot_change_application_behavior(
    tmp_path: Path,
) -> None:
    class HostileMapping(Mapping[str, object]):
        reads = 0

        def __getitem__(self, _key: str) -> object:
            type(self).reads += 1
            raise RuntimeError("hostile getter")

        def __iter__(self) -> Iterator[str]:
            return iter(("traceparent",))

        def __len__(self) -> int:
            return 1

    capture = initialize(output=tmp_path, service_name="hostile-context")
    sentinel = object()
    with capture.observe("hostile.root", parent_context=HostileMapping()) as root:
        assert root.tool("identity", {}, lambda _value: sentinel) is sentinel
    assert HostileMapping.reads > 0
    assert validate_artifact(capture.shutdown().artifact_path).valid


def test_inherited_context_and_required_parent_gaps_are_explicit(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="context-e2e")
    with capture.observe(
        "valid.root",
        parent_context={"traceparent": TRACEPARENT, "required": True},
    ) as root:
        with root.turn("valid.child") as child:
            child.emit("state.context", {"inherited": True})
    with capture.observe("missing.root", parent_context={"required": True}):
        pass
    with capture.observe(
        "invalid.root",
        parent_context={"traceparent": "00-invalid", "required": True},
    ):
        pass

    artifact = Path(capture.shutdown().artifact_path)
    records = [
        json.loads(line)
        for line in (artifact / "trace.jsonl").read_text().splitlines()
    ]
    parent_losses = [
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"] == "missing_parent_context"
    ]
    assert len(parent_losses) == 2
    assert not [
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"] == "unsupported_semantic_projection"
    ]
    valid_root = next(
        record
        for record in records
        if record["kind"] == "run.start"
        and record["data"]["name"] == "valid.root"
    )
    valid_child = next(
        record
        for record in records
        if record["kind"] == "scope"
        and record["data"]["phase"] == "start"
    )
    assert valid_child["parent"] == valid_root["id"]
    assert validate_artifact(artifact).valid
