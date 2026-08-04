from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from semantic_layer.trace import SemanticProjector

_ROOT = Path(__file__).parents[4]


def test_projects_shared_semantic_corpus_exactly() -> None:
    corpus = json.loads(
        (_ROOT / "contracts/capture/v1/semantic-projection-cases.json").read_text()
    )
    capture_validator = Draft202012Validator(
        json.loads(
            (_ROOT / "contracts/capture/v1/semantic-capture-event.schema.json").read_text()
        ),
        format_checker=FormatChecker(),
    )
    record_validator = Draft202012Validator(
        json.loads(
            (_ROOT / "contracts/trace/v1/semantic-trace-record.schema.json").read_text()
        ),
        format_checker=FormatChecker(),
    )

    for fixture in corpus["cases"]:
        projector = SemanticProjector()
        projected: list[dict[str, Any]] = []
        for index, event in enumerate(fixture["events"], 1):
            row = {**corpus["defaults"], **event, "seq": index, "monotonic_ns": index}
            capture_validator.validate(row)
            projected.extend(projector.project(row))
        for record in projected:
            record_validator.validate(record)
        assert (
            _normalize_generated_identifiers(projected, fixture["events"])
            == fixture["expected"]
        ), fixture["name"]


def test_normalizes_generated_ids_without_rewriting_callers_or_relationships() -> None:
    normalized = _normalize_generated_identifiers(
        [
            {
                "id": "loss_internal_hash",
                "source": "src_internal_hash",
                "kind": "loss",
                "data": {},
            },
            {
                "id": "rec_caller",
                "source": "src_internal_hash",
                "kind": "state",
                "parent": "loss_internal_hash",
                "links": [
                    {"type": "derived_from", "record": "loss_internal_hash"}
                ],
                "data": {"type": "state.done"},
            },
        ],
        [{"record_id": "rec_caller", "semantic": {}}],
    )

    assert normalized == [
        {
            "id": "__generated_record_1__",
            "source": "__generated_source_1__",
            "kind": "loss",
            "data": {},
        },
        {
            "id": "rec_caller",
            "source": "__generated_source_1__",
            "kind": "state",
            "parent": "__generated_record_1__",
            "links": [
                {"type": "derived_from", "record": "__generated_record_1__"}
            ],
            "data": {"type": "state.done"},
        },
    ]


def _normalize_generated_identifiers(
    records: list[dict[str, Any]], events: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    caller_record_ids = {event["record_id"] for event in events}
    caller_scope_ids = {
        semantic["scope_id"]
        for event in events
        if isinstance((semantic := event.get("semantic")), dict)
        and isinstance(semantic.get("scope_id"), str)
    }
    record_ids: dict[str, str] = {}
    source_ids: dict[str, str] = {}
    call_ids: dict[str, str] = {}
    scope_ids: dict[str, str] = {}

    for record in records:
        record_id = record.get("id")
        if isinstance(record_id, str) and record_id not in caller_record_ids:
            _placeholder(record_ids, record_id, "record")
        source = record.get("source")
        if isinstance(source, str):
            _placeholder(source_ids, source, "source")
        data = record.get("data")
        if not isinstance(data, dict):
            continue
        call_id = data.get("call_id")
        if (
            record.get("kind") in {"tool.proposal", "tool.call", "tool.result"}
            and isinstance(call_id, str)
        ):
            _placeholder(call_ids, call_id, "call")
        scope_id = data.get("scope_id")
        if (
            record.get("kind") == "scope"
            and isinstance(scope_id, str)
            and scope_id not in caller_scope_ids
        ):
            _placeholder(scope_ids, scope_id, "scope")

    normalized = deepcopy(records)
    for record in normalized:
        record["id"] = record_ids.get(record["id"], record["id"])
        record["source"] = source_ids.get(record["source"], record["source"])
        parent = record.get("parent")
        if isinstance(parent, str):
            record["parent"] = record_ids.get(parent, parent)
        links = record.get("links")
        if isinstance(links, list):
            for link in links:
                if isinstance(link, dict) and isinstance(link.get("record"), str):
                    link["record"] = record_ids.get(link["record"], link["record"])
        data = record.get("data")
        if not isinstance(data, dict):
            continue
        call_id = data.get("call_id")
        if isinstance(call_id, str) and call_id in call_ids:
            data["call_id"] = call_ids[call_id]
        scope_id = data.get("scope_id")
        if isinstance(scope_id, str) and scope_id in scope_ids:
            data["scope_id"] = scope_ids[scope_id]
    return normalized


def _placeholder(values: dict[str, str], value: str, kind: str) -> str:
    return values.setdefault(value, f"__generated_{kind}_{len(values) + 1}__")
