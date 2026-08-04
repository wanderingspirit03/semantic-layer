from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CONTRACT = ROOT / "contracts" / "capture" / "v1"
TRACE_CONTRACT = ROOT / "contracts" / "trace" / "v1"
TRACE_V2_CONTRACT = ROOT / "contracts" / "trace" / "v2"
OUTPUT = ROOT / "packages" / "python" / "semantic_layer" / "_generated" / "capture_v1.py"
SCHEMA_OUTPUT = ROOT / "packages" / "python" / "semantic_layer" / "schemas"
SCHEMA_SOURCES = {
    "semantic-capture-event.schema.json": CONTRACT / "semantic-capture-event.schema.json",
    "semantic-trace-manifest.schema.json": TRACE_CONTRACT
    / "semantic-trace-manifest.schema.json",
    "semantic-trace-manifest-v2.schema.json": TRACE_V2_CONTRACT
    / "semantic-trace-manifest.schema.json",
    "semantic-trace-record.schema.json": TRACE_CONTRACT
    / "semantic-trace-record.schema.json",
}


def render() -> str:
    event_text = (CONTRACT / "semantic-capture-event.schema.json").read_text()
    event = json.loads(event_text)
    reasons = event["$defs"]["loss_reason"]["enum"]
    digest = hashlib.sha256(event_text.encode()).hexdigest()
    reason_literal = ", ".join(repr(item) for item in reasons)
    return f"""# Generated from semantic_capture_event_v1. Schema digest: {digest}.
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Any, Literal

from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

_SCHEMAS = Path(__file__).resolve().parents[1] / "schemas"

@lru_cache(maxsize=1)
def _schema_validator(name: str) -> Draft202012Validator:
    schema = json.loads((_SCHEMAS / name).read_text(encoding="utf-8"))
    return Draft202012Validator(schema, format_checker=FormatChecker())

LossReason = Literal[{reason_literal}]
Bounded = Annotated[str, StringConstraints(min_length=1, max_length=512)]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{{64}}$")]
CaptureId = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_:-]{{7,127}}$")]
SourceId = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=512,
        pattern=r"^[^\\s/]+(?:/[^\\s/]+)+$",
    ),
]

class _Generated(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True, strict=True)

class _ExactGenerated(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, strict=True)

class Source(_Generated):
    source_id: SourceId
    name: Bounded
    seam: Bounded
    identity_domain: Bounded
    official: bool

class CoverageIdentity(_ExactGenerated):
    operation: Bounded
    domain: Bounded
    identity_token: Sha256

class OwnedDecision(_ExactGenerated):
    type: Literal["coverage.ownership.v1"]
    status: Literal["owned"]
    primary_source_id: SourceId
    participant_source_ids: list[SourceId] = Field(min_length=2, max_length=256)
    secondary_source_ids: list[SourceId] = Field(min_length=1, max_length=255)
    final: Literal[True]

class UnownedDecision(_ExactGenerated):
    type: Literal["coverage.ownership.v1"]
    status: Literal["ambiguous", "evidence_only"]
    participant_source_ids: list[SourceId] = Field(min_length=2, max_length=256)
    secondary_source_ids: list[SourceId] = Field(max_length=0)
    final: Literal[True]

OwnershipDecision = OwnedDecision | UnownedDecision

class OwnershipAmbiguity(_ExactGenerated):
    type: Literal["coverage.ownership.ambiguity.v1"]
    decision_record_id: CaptureId

class Loss(_Generated):
    reason: LossReason
    stage: Literal["source", "snapshot", "serialize", "scrub", "scan", "queue", "persist", "recover"]
    recoverable: bool

class SemanticCaptureEventV1(_Generated):
    schema_: Literal["semantic_capture_event_v1"] = Field(alias="schema")
    run_id: CaptureId
    record_id: CaptureId
    seq: int = Field(ge=1)
    observed_at: str
    monotonic_ns: int = Field(ge=0)
    trace_id: CaptureId
    source: Source
    coverage: CoverageIdentity | None = None
    event_kind: Literal["lifecycle", "model", "tool", "state", "log", "error", "stream", "correlation", "unknown", "loss"]
    phase: Literal["start", "event", "end", "error", "cancelled", "gap"]
    name: Bounded
    native: Any
    semantic: dict[str, Any]
    correlation: dict[str, Any]
    loss: Loss | None = None
    loss_refs: list[CaptureId]
    blob_refs: list[dict[str, Any]]
    provenance: dict[str, Any]

def validate_capture_event_v1(value: Any) -> SemanticCaptureEventV1:
    _schema_validator("semantic-capture-event.schema.json").validate(value)
    return SemanticCaptureEventV1.model_validate(value)
"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = render()
    if args.check:
        if not OUTPUT.exists() or OUTPUT.read_text() != expected:
            raise SystemExit("generated v1 Python capture models are stale")
        for name, source in SCHEMA_SOURCES.items():
            if (
                not (SCHEMA_OUTPUT / name).exists()
                or (SCHEMA_OUTPUT / name).read_text() != source.read_text()
            ):
                raise SystemExit("packaged Python semantic schemas are stale")
    else:
        OUTPUT.write_text(expected)
        SCHEMA_OUTPUT.mkdir(parents=True, exist_ok=True)
        for name, source in SCHEMA_SOURCES.items():
            (SCHEMA_OUTPUT / name).write_text(source.read_text())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
