# Generated from semantic_capture_event_v1. Schema digest: 4026f827aa6f5009f66deae75f8659e214b3a54fa4d38d128a215a381ea8db91.
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

LossReason = Literal['credential_redaction', 'configured_redaction', 'scrubber_failure_payload_omitted', 'serialization_failure', 'unsafe_getter_avoided', 'unsafe_helper_avoided', 'size_overflow_blobbed', 'size_overflow_discarded', 'blob_scan_blocked', 'queue_backpressure_drop', 'persistence_failure', 'unsupported_native_value', 'source_rejection', 'filter_limit_exclusion', 'missing_parent_context', 'parser_error_malformed_bytes', 'crash_recovery', 'uncertain_tail', 'shutdown_timeout', 'turn_order_ambiguous']
Bounded = Annotated[str, StringConstraints(min_length=1, max_length=512)]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CaptureId = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_:-]{7,127}$")]
SourceId = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=512,
        pattern=r"^[^\s/]+(?:/[^\s/]+)+$",
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
