from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from semantic_layer.trace import SemanticProjector

_SCHEMA_PATH = (
    Path(__file__).parents[4] / "contracts/trace/v1/semantic-trace-record.schema.json"
)
_VALIDATOR = Draft202012Validator(
    json.loads(_SCHEMA_PATH.read_text()),
    format_checker=FormatChecker(),
)


def _assert_valid(records: list[dict[str, Any]]) -> None:
    for record in records:
        _VALIDATOR.validate(record)


def _row(
    *,
    trace_id: str = "trace-a",
    record_id: str,
    event_kind: str,
    phase: str,
    name: str,
    semantic: dict[str, Any],
    parent_record_id: str | None = None,
    native: Any = None,
    native_identity: str | None = None,
    conversation_id: str | None = None,
    turn_id: str | None = None,
    turn_index: int | None = None,
    previous_turn_id: str | None = None,
    run_correlation: dict[str, Any] | None = None,
    source_id: str = "official/openai",
    identity_domain: str = "openai.operation",
) -> dict[str, Any]:
    correlation: dict[str, Any] = {}
    if parent_record_id is not None:
        correlation["parent_record_id"] = parent_record_id
    row = {
        "trace_id": trace_id,
        "record_id": record_id,
        "seq": 1,
        "observed_at": "2026-07-25T12:00:00Z",
        "source": {
            "source_id": source_id,
            "identity_domain": identity_domain,
        },
        "event_kind": event_kind,
        "phase": phase,
        "name": name,
        "native": native,
        "semantic": semantic,
        "correlation": correlation,
    }
    if native_identity is not None:
        row["native_identity"] = native_identity
    if conversation_id is not None:
        row["conversation_id"] = conversation_id
    if turn_id is not None:
        row["turn_id"] = turn_id
    if turn_index is not None:
        row["turn_index"] = turn_index
    if previous_turn_id is not None:
        row["previous_turn_id"] = previous_turn_id
    if run_correlation is not None:
        row["run_correlation"] = run_correlation
    return row


def test_projects_protected_run_correlation_on_root_start() -> None:
    correlation = {
        "task_id": f"task_{'a' * 64}",
        "execution": {
            "system": "job-runner",
            "run_id": f"exec_{'b' * 64}",
            "parent_run_id": f"exec_{'c' * 64}",
            "root_run_id": f"exec_{'d' * 64}",
            "attempt": 0,
        },
    }

    records = SemanticProjector().project(
        _row(
            record_id="source-correlation-start",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            semantic={"type": "agent.run", "name": "research"},
            run_correlation=correlation,
        )
    )

    assert records[0]["kind"] == "run.start"
    assert records[0]["data"]["correlation"] == correlation
    _assert_valid(records)


def test_retires_model_tool_and_lifecycle_correlation_after_omission() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="root-start",
            event_kind="lifecycle",
            phase="start",
            name="run",
            semantic={"type": "agent.run", "name": "run"},
        )
    )
    projector.project(
        _row(
            record_id="model-start-1",
            event_kind="model",
            phase="start",
            name="model",
            native_identity="model-identity",
            parent_record_id="root-start",
            semantic={"type": "model.request", "model": "fixture"},
        )
    )
    projector.retire_omitted(
        _row(
            record_id="model-end-omitted",
            event_kind="model",
            phase="end",
            name="model",
            native_identity="model-identity",
            parent_record_id="model-start-1",
            semantic={"type": "model.response", "status": "completed"},
        )
    )
    reused_model = projector.project(
        _row(
            record_id="model-start-2",
            event_kind="model",
            phase="start",
            name="model",
            native_identity="model-identity",
            parent_record_id="root-start",
            semantic={"type": "model.request", "model": "fixture"},
        )
    )
    assert [row["kind"] for row in reused_model] == ["model.request"]

    projector.project(
        _row(
            record_id="tool-start-1",
            event_kind="tool",
            phase="start",
            name="lookup",
            native_identity="tool-identity",
            parent_record_id="root-start",
            semantic={"type": "tool.execution", "name": "lookup", "input": {}},
        )
    )
    projector.retire_omitted(
        _row(
            record_id="tool-end-omitted",
            event_kind="tool",
            phase="end",
            name="lookup",
            native_identity="tool-identity",
            parent_record_id="tool-start-1",
            semantic={"type": "tool.result", "status": "succeeded", "output": None},
        )
    )
    reused_tool = projector.project(
        _row(
            record_id="tool-start-2",
            event_kind="tool",
            phase="start",
            name="lookup",
            native_identity="tool-identity",
            parent_record_id="root-start",
            semantic={"type": "tool.execution", "name": "lookup", "input": {}},
        )
    )
    assert [row["kind"] for row in reused_tool] == ["tool.call"]

    projector.retire_omitted(
        _row(
            record_id="root-end-omitted",
            event_kind="lifecycle",
            phase="end",
            name="run",
            parent_record_id="root-start",
            semantic={"type": "agent.run", "status": "succeeded"},
        )
    )
    reused_root = projector.project(
        _row(
            record_id="root-start-2",
            event_kind="lifecycle",
            phase="start",
            name="run two",
            semantic={"type": "agent.run", "name": "run two"},
        )
    )
    assert [row["kind"] for row in reused_root] == ["run.start"]


def test_omission_keeps_ambiguous_correlations_and_retires_proposal() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="root-start",
            event_kind="lifecycle",
            phase="start",
            name="run",
            semantic={"type": "agent.run", "name": "run"},
        )
    )
    for identity in ("model-a", "model-b"):
        projector.project(
            _row(
                record_id=f"{identity}-start",
                event_kind="model",
                phase="start",
                name="model",
                native_identity=identity,
                parent_record_id="root-start",
                semantic={"type": "model.request", "model": "fixture"},
            )
        )
    projector.retire_omitted(
        _row(
            record_id="ambiguous-model-end",
            event_kind="model",
            phase="end",
            name="model",
            native_identity="model-b",
            parent_record_id="model-a-start",
            semantic={"type": "model.response", "status": "completed"},
        )
    )
    for identity in ("model-a", "model-b"):
        duplicate = projector.project(
            _row(
                record_id=f"{identity}-duplicate",
                event_kind="model",
                phase="start",
                name="model",
                native_identity=identity,
                parent_record_id="root-start",
                semantic={"type": "model.request", "model": "fixture"},
            )
        )
        assert duplicate[0]["data"]["reason"] == "duplicate_active_model_identity"

    for identity in ("tool-a", "tool-b"):
        projector.project(
            _row(
                record_id=f"{identity}-start",
                event_kind="tool",
                phase="start",
                name="lookup",
                native_identity=identity,
                parent_record_id="root-start",
                semantic={"type": "tool.execution", "name": "lookup", "input": {}},
            )
        )
    projector.retire_omitted(
        _row(
            record_id="ambiguous-tool-end",
            event_kind="tool",
            phase="end",
            name="lookup",
            native_identity="tool-b",
            parent_record_id="tool-a-start",
            semantic={"type": "tool.result", "status": "succeeded", "output": None},
        )
    )
    for identity in ("tool-a", "tool-b"):
        duplicate = projector.project(
            _row(
                record_id=f"{identity}-duplicate",
                event_kind="tool",
                phase="start",
                name="lookup",
                native_identity=identity,
                parent_record_id="root-start",
                semantic={"type": "tool.execution", "name": "lookup", "input": {}},
            )
        )
        assert duplicate[0]["data"]["reason"] == "duplicate_active_tool_identity"

    projector.project(
        _row(
            record_id="proposal-start",
            event_kind="tool",
            phase="start",
            name="proposed",
            native_identity="proposal-identity",
            parent_record_id="root-start",
            semantic={"type": "tool.proposal", "name": "proposed", "input": {}},
        )
    )
    projector.retire_omitted(
        _row(
            record_id="proposal-execution-omitted",
            event_kind="tool",
            phase="start",
            name="proposed",
            native_identity="proposal-identity",
            parent_record_id="root-start",
            semantic={"type": "tool.execution", "name": "proposed", "input": {}},
        )
    )
    reused = projector.project(
        _row(
            record_id="proposal-reused",
            event_kind="tool",
            phase="start",
            name="proposed",
            native_identity="proposal-identity",
            parent_record_id="root-start",
            semantic={"type": "tool.proposal", "name": "proposed", "input": {}},
        )
    )
    assert [row["kind"] for row in reused] == ["tool.proposal"]


def test_projects_multiple_root_run_pairs_in_one_capture_session() -> None:
    projector = SemanticProjector()

    first_start = projector.project(
        _row(
            trace_id="trace-a",
            record_id="source-a-start",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            turn_id="turn-a",
            semantic={"type": "agent.run", "name": "research", "input": {"topic": "traces"}},
        )
    )
    first_end = projector.project(
        _row(
            trace_id="trace-a",
            record_id="source-a-end",
            event_kind="lifecycle",
            phase="end",
            name="agent.run",
            semantic={
                "type": "agent.run",
                "output": {"answer": "done"},
                "summary": "Research completed",
            },
            parent_record_id="source-a-start",
        )
    )
    second_start = projector.project(
        _row(
            trace_id="trace-b",
            record_id="source-b-start",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            turn_id="turn-b",
            previous_turn_id="turn-a",
            semantic={"type": "workflow.run", "name": "publish"},
        )
    )
    second_end = projector.project(
        _row(
            trace_id="trace-b",
            record_id="source-b-end",
            event_kind="lifecycle",
            phase="cancelled",
            name="workflow.run",
            semantic={"type": "workflow.run"},
            parent_record_id="source-b-start",
        )
    )

    records = first_start + first_end + second_start + second_end
    _assert_valid(records)
    assert [record["kind"] for record in records] == [
        "run.start",
        "run.outcome",
        "run.start",
        "run.outcome",
    ]
    assert first_start[0]["data"] == {
        "name": "research",
        "input": {"topic": "traces"},
        "turn_id": "turn-a",
    }
    assert first_end[0]["parent"] == first_start[0]["id"]
    assert first_end[0]["data"] == {
        "status": "completed",
        "summary": "Research completed",
        "output": {"answer": "done"},
    }
    assert second_end[0]["data"] == {"status": "cancelled"}
    assert second_start[0]["links"] == [
        {"type": "continues_from", "record": first_start[0]["id"]}
    ]
    assert records[0]["seq"] == 1
    assert records[-1]["seq"] == 4
    assert all(record["source"].startswith("src_") for record in records)
    assert first_start[0]["id"] == "source-a-start"


def test_keeps_external_continuation_identity_without_cross_bundle_loss() -> None:
    projector = SemanticProjector()
    records = projector.project(
        _row(
            trace_id="trace-resumed",
            record_id="source-resumed-start",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            conversation_id="conversation-shared",
            turn_id="turn-b",
            turn_index=1,
            previous_turn_id="turn-a",
            semantic={"type": "agent.run", "name": "resumed agent run"},
        )
    )

    assert len(records) == 1
    assert records[0]["data"] == {
        "name": "resumed agent run",
        "conversation_id": "conversation-shared",
        "turn_id": "turn-b",
        "turn_index": 1,
        "previous_turn_id": "turn-a",
    }
    assert "links" not in records[0]
    _assert_valid(records)


def test_projects_nested_scope_start_and_end_with_one_scope_identity() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run", "name": "workflow"},
        )
    )[0]

    scope_start = projector.project(
        _row(
            record_id="worker-start",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            semantic={"type": "agent.run", "scope_type": "agent", "name": "writer"},
            parent_record_id="root",
        )
    )[0]
    scope_terminal = projector.project(
        _row(
            record_id="worker-end",
            event_kind="lifecycle",
            phase="error",
            name="agent.run",
            semantic={
                "type": "agent.run",
                "status": "succeeded",
                "error": {
                    "type": "tool_error",
                    "message": "The worker tool failed.",
                    "recoverable": True,
                },
            },
            parent_record_id="worker-start",
        )
    )
    scope_end, scope_error = scope_terminal
    duplicate_end = projector.project(
        _row(
            record_id="worker-end-again",
            event_kind="lifecycle",
            phase="error",
            name="agent.run",
            semantic={"type": "agent.run"},
            parent_record_id="worker-start",
        )
    )

    _assert_valid([root, scope_start, scope_end, scope_error, *duplicate_end])
    assert scope_start["kind"] == "scope"
    assert scope_start["parent"] == root["id"]
    assert scope_start["data"] == {
        "scope_id": scope_start["data"]["scope_id"],
        "type": "agent",
        "phase": "start",
        "name": "writer",
    }
    assert scope_end["kind"] == "scope"
    assert scope_end["parent"] == scope_start["id"]
    assert scope_end["data"] == {
        "scope_id": scope_start["data"]["scope_id"],
        "type": "agent",
        "phase": "end",
        "status": "failed",
    }
    assert scope_error["kind"] == "error"
    assert scope_error["parent"] == scope_end["id"]
    assert scope_error["data"] == {
        "type": "tool_error",
        "message": "The worker tool failed.",
        "recoverable": True,
    }
    assert scope_error["id"].startswith("error_")
    assert duplicate_end[0]["kind"] == "loss"
    assert duplicate_end[0]["data"]["reason"] == "unsupported_semantic_projection"


def test_composed_source_outcome_does_not_override_non_null_root_output() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            semantic={"type": "agent.run", "name": "outer"},
            source_id="builtin/manual",
            identity_domain="manual.operation",
        )
    )[0]
    projector.project(
        _row(
            record_id="source-start",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            semantic={"type": "agent.run", "name": "inner"},
            parent_record_id="root",
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )
    projector.project(
        _row(
            record_id="source-end",
            event_kind="lifecycle",
            phase="end",
            name="agent.run",
            semantic={
                "type": "agent.run",
                "status": "completed",
                "output": "source output",
            },
            parent_record_id="source-start",
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )
    outcome = projector.project(
        _row(
            record_id="root-end",
            event_kind="lifecycle",
            phase="end",
            name="agent.run",
            semantic={
                "type": "agent.run",
                "status": "completed",
                "output": "manual output",
            },
            parent_record_id="root",
            source_id="builtin/manual",
            identity_domain="manual.operation",
        )
    )[0]

    assert outcome["kind"] == "run.outcome"
    assert outcome["parent"] == root["id"]
    assert outcome["data"] == {
        "status": "completed",
        "output": "manual output",
    }
    _assert_valid([root, outcome])


def _start_composed_run(
    projector: SemanticProjector,
    trace_id: str,
) -> dict[str, Any]:
    return projector.project(
        _row(
            trace_id=trace_id,
            record_id=f"{trace_id}-root",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            semantic={"type": "agent.run", "name": "outer"},
            source_id="builtin/manual",
            identity_domain="manual.operation",
        )
    )[0]


def _finish_composed_child(
    projector: SemanticProjector,
    trace_id: str,
    index: int,
    output: Any,
) -> None:
    start_id = f"{trace_id}-child-{index}-start"
    projector.project(
        _row(
            trace_id=trace_id,
            record_id=start_id,
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            semantic={"type": "agent.run", "name": f"child {index}"},
            parent_record_id=f"{trace_id}-root",
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )
    projector.project(
        _row(
            trace_id=trace_id,
            record_id=f"{trace_id}-child-{index}-end",
            event_kind="lifecycle",
            phase="end",
            name="agent.run",
            semantic={
                "type": "agent.run",
                "status": "completed",
                "output": output,
            },
            parent_record_id=start_id,
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )


def _finish_composed_run(
    projector: SemanticProjector,
    trace_id: str,
    semantic: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    return projector.project(
        _row(
            trace_id=trace_id,
            record_id=f"{trace_id}-root-end",
            event_kind="lifecycle",
            phase="end",
            name="agent.run",
            semantic=semantic or {"type": "agent.run", "status": "completed"},
            parent_record_id=f"{trace_id}-root",
            source_id="builtin/manual",
            identity_domain="manual.operation",
        )
    )


def test_root_without_child_output_keeps_output_absent() -> None:
    projector = SemanticProjector()
    _start_composed_run(projector, "trace-zero")

    records = _finish_composed_run(projector, "trace-zero")

    _assert_valid(records)
    assert records[0]["kind"] == "run.outcome"
    assert records[0]["data"] == {"status": "completed"}


def test_missing_manual_root_adopts_exactly_one_direct_child_output() -> None:
    projector = SemanticProjector()
    _start_composed_run(projector, "trace-one")
    _finish_composed_child(projector, "trace-one", 1, {"answer": "child"})

    records = _finish_composed_run(projector, "trace-one")

    _assert_valid(records)
    assert records[0]["data"] == {
        "status": "completed",
        "output": {"answer": "child"},
    }


def test_explicit_null_root_output_wins_over_child_output() -> None:
    projector = SemanticProjector()
    _start_composed_run(projector, "trace-null")
    _finish_composed_child(projector, "trace-null", 1, {"answer": "child"})

    records = _finish_composed_run(
        projector,
        "trace-null",
        {"type": "agent.run", "status": "completed", "output": None},
    )

    _assert_valid(records)
    assert records[0]["data"] == {"status": "completed", "output": None}


def test_multiple_direct_child_outputs_are_not_guessed() -> None:
    projector = SemanticProjector()
    _start_composed_run(projector, "trace-many")
    _finish_composed_child(projector, "trace-many", 1, "first")
    _finish_composed_child(projector, "trace-many", 2, "second")

    records = _finish_composed_run(projector, "trace-many")

    _assert_valid(records)
    outcome, loss = records
    assert outcome["data"] == {"status": "completed"}
    assert loss["kind"] == "loss"
    assert loss["data"]["reason"] == "ambiguous_root_output"
    assert loss["links"] == [{"type": "affects", "record": outcome["id"]}]


def test_explicit_root_output_reference_wins_over_child_output() -> None:
    projector = SemanticProjector()
    root = _start_composed_run(projector, "trace-ref")
    message = projector.project(
        _row(
            trace_id="trace-ref",
            record_id="trace-ref-message",
            event_kind="log",
            phase="event",
            name="message",
            semantic={"type": "message", "role": "assistant", "content": "explicit"},
            parent_record_id="trace-ref-root",
        )
    )[0]
    _finish_composed_child(projector, "trace-ref", 1, "child")

    records = _finish_composed_run(
        projector,
        "trace-ref",
        {
            "type": "agent.run",
            "status": "completed",
            "output": None,
            "output_ref": "trace-ref-message",
        },
    )

    _assert_valid([root, message, *records])
    assert records[0]["data"] == {"status": "completed", "output": None}
    assert records[0]["links"] == [
        {"type": "derived_from", "record": message["id"]}
    ]


def test_falsey_direct_child_outputs_are_preserved() -> None:
    projector = SemanticProjector()
    falsey_outputs: list[Any] = [False, 0, "", [], {}]

    for index, output in enumerate(falsey_outputs):
        trace_id = f"trace-falsey-{index}"
        _start_composed_run(projector, trace_id)
        _finish_composed_child(projector, trace_id, 1, output)
        records = _finish_composed_run(projector, trace_id)

        assert type(records[0]["data"]["output"]) is type(output)
        assert records[0]["data"]["output"] == output


def test_composed_output_state_does_not_leak_between_or_reused_traces() -> None:
    projector = SemanticProjector()
    _start_composed_run(projector, "trace-a")
    _finish_composed_child(projector, "trace-a", 1, "trace A output")
    _start_composed_run(projector, "trace-b")

    trace_b_outcome = _finish_composed_run(projector, "trace-b")[0]
    trace_a_outcome = _finish_composed_run(projector, "trace-a")[0]
    _start_composed_run(projector, "trace-a")
    reused_trace_outcome = _finish_composed_run(projector, "trace-a")[0]

    assert trace_b_outcome["data"] == {"status": "completed"}
    assert trace_a_outcome["data"] == {
        "status": "completed",
        "output": "trace A output",
    }
    assert reused_trace_outcome["data"] == {"status": "completed"}


def test_unresolved_explicit_parent_becomes_loss_not_guessed_containment() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    orphan_terminal = projector.project(
        _row(
            record_id="orphan-end",
            event_kind="lifecycle",
            phase="error",
            name="agent.run",
            semantic={"type": "agent.run"},
            parent_record_id="missing-scope",
        )
    )[0]
    unparented_scope_terminal = projector.project(
        _row(
            record_id="unparented-scope-end",
            event_kind="lifecycle",
            phase="end",
            name="scope",
            semantic={"type": "scope", "status": "succeeded"},
        )
    )[0]
    unparented_run_terminal = projector.project(
        _row(
            record_id="unparented-run-end",
            event_kind="lifecycle",
            phase="end",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    failed_root = projector.project(
        _row(
            record_id="root-end",
            event_kind="lifecycle",
            phase="error",
            name="workflow.run",
            semantic={"type": "workflow.run", "status": "succeeded"},
            parent_record_id="root",
        )
    )[0]
    replacement_root = projector.project(
        _row(
            record_id="replacement-root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]

    _assert_valid(
        [
            root,
            orphan_terminal,
            unparented_scope_terminal,
            unparented_run_terminal,
            failed_root,
            replacement_root,
        ]
    )
    assert orphan_terminal["kind"] == "loss"
    assert orphan_terminal["data"]["reason"] == "unresolved_parent"
    assert "parent" not in orphan_terminal
    assert unparented_scope_terminal["kind"] == "loss"
    assert unparented_scope_terminal["data"]["reason"] == (
        "unsupported_semantic_projection"
    )
    assert unparented_run_terminal["kind"] == "loss"
    assert failed_root["kind"] == "run.outcome"
    assert failed_root["parent"] == root["id"]
    assert failed_root["data"] == {"status": "failed"}
    assert replacement_root["kind"] == "run.start"


def test_pairs_out_of_order_same_name_tool_results_by_exact_call_identity() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    proposal = projector.project(
        _row(
            record_id="proposal-a",
            event_kind="tool",
            phase="event",
            name="search",
            semantic={
                "type": "tool.proposal",
                "call_id": "call_a",
                "native_call_id": "provider/call A",
                "name": "search",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
        )
    )[0]
    call_a = projector.project(
        _row(
            record_id="call-a-start",
            event_kind="tool",
            phase="start",
            name="search",
            semantic={
                "type": "tool.execution",
                "call_id": "call_a",
                "native_call_id": "provider/call A",
                "name": "search",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
            native={"call_id": "wrong-native-id"},
        )
    )[0]
    call_b = projector.project(
        _row(
            record_id="call-b-start",
            event_kind="tool",
            phase="event",
            name="search",
            semantic={
                "type": "tool.execution",
                "call_id": "call_b",
                "native_call_id": "provider/call B",
                "name": "search",
                "input": {"query": "beta"},
            },
            parent_record_id="root",
        )
    )[0]
    result_b = projector.project(
        _row(
            record_id="call-b-end",
            event_kind="tool",
            phase="end",
            name="search",
            semantic={
                "type": "tool.result",
                "call_id": "call_b",
                "native_call_id": "provider/call B",
                "output": {"hits": ["b"]},
            },
            parent_record_id="call-b-start",
        )
    )[0]
    result_a = projector.project(
        _row(
            record_id="call-a-end",
            event_kind="tool",
            phase="error",
            name="search",
            semantic={
                "type": "tool.error",
                "call_id": "call_a",
                "native_call_id": "provider/call A",
                "error": {
                    "type": "tool_timeout",
                    "message": "timed out",
                    "recoverable": True,
                },
            },
            parent_record_id="call-a-start",
        )
    )[0]

    records = [root, proposal, call_a, call_b, result_b, result_a]
    _assert_valid(records)
    assert proposal["kind"] == "tool.proposal"
    assert proposal["data"]["native_call_id"] == "provider/call A"
    assert call_a["kind"] == call_b["kind"] == "tool.call"
    assert call_a["data"]["call_id"] != call_b["data"]["call_id"]
    assert call_a["data"]["call_id"] == proposal["data"]["call_id"]
    assert call_a["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert result_b["data"] == {
        "call_id": call_b["data"]["call_id"],
        "native_call_id": "provider/call B",
        "status": "succeeded",
        "output": {"hits": ["b"]},
    }
    assert result_b["links"] == [{"type": "result_of", "record": call_b["id"]}]
    assert result_b["parent"] == root["id"]
    assert result_a["data"]["call_id"] == call_a["data"]["call_id"]
    assert result_a["data"]["status"] == "failed"
    assert result_a["links"] == [{"type": "result_of", "record": call_a["id"]}]
    reused_call = projector.project(
        _row(
            record_id="call-a-reused-start",
            event_kind="tool",
            phase="event",
            name="search",
            semantic={
                "type": "tool.execution",
                "native_call_id": "provider/call A",
                "name": "search",
                "input": {"query": "reused"},
            },
            parent_record_id="root",
        )
    )[0]
    stale_result = projector.project(
        _row(
            record_id="call-a-stale-end",
            event_kind="tool",
            phase="end",
            name="search",
            semantic={
                "type": "tool.result",
                "native_call_id": "provider/call A",
                "output": {"hits": ["stale"]},
            },
            parent_record_id="call-a-start",
        )
    )[0]
    reused_result = projector.project(
        _row(
            record_id="call-a-reused-end",
            event_kind="tool",
            phase="end",
            name="search",
            semantic={
                "type": "tool.result",
                "native_call_id": "provider/call A",
                "output": {"hits": ["new"]},
            },
            parent_record_id="call-a-reused-start",
        )
    )[0]
    _assert_valid([stale_result, reused_call, reused_result])
    assert stale_result["kind"] == "loss"
    assert stale_result["data"]["reason"] == "unmatched_tool_result"
    assert stale_result["parent"] == root["id"]
    assert "links" not in reused_call
    assert reused_result["links"] == [
        {"type": "result_of", "record": reused_call["id"]}
    ]
    alternate_projector = SemanticProjector()
    alternate_projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    same_native_with_other_semantic_id = alternate_projector.project(
        _row(
            record_id="proposal-alternate",
            event_kind="tool",
            phase="event",
            name="search",
            semantic={
                "type": "tool.proposal",
                "call_id": "different_sdk_value",
                "native_call_id": "provider/call A",
                "name": "search",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
        )
    )[0]
    assert (
        same_native_with_other_semantic_id["data"]["call_id"]
        == proposal["data"]["call_id"]
    )
    assert (
        same_native_with_other_semantic_id["data"]["native_call_id"]
        == "provider/call A"
    )
    same_identity_in_other_domain = projector.project(
        _row(
            record_id="other-domain-proposal",
            event_kind="tool",
            phase="event",
            name="search",
            semantic={
                "type": "tool.proposal",
                "native_call_id": "provider/call A",
                "name": "search",
                "input": {"query": "other domain"},
            },
            parent_record_id="root",
            identity_domain="other.operation",
        )
    )[0]
    assert (
        same_identity_in_other_domain["data"]["call_id"]
        != proposal["data"]["call_id"]
    )
    projector.project(
        _row(
            trace_id="trace-b",
            record_id="other-root",
            event_kind="lifecycle",
            phase="start",
            name="other run",
            semantic={"type": "agent.run"},
        )
    )
    same_native_id_in_other_root = projector.project(
        _row(
            trace_id="trace-b",
            record_id="other-proposal",
            event_kind="tool",
            phase="event",
            name="search",
            semantic={
                "type": "tool.proposal",
                "call_id": "call_a",
                "native_call_id": "provider/call A",
                "name": "search",
                "input": {"query": "other"},
            },
            parent_record_id="other-root",
        )
    )[0]
    assert (
        same_native_id_in_other_root["data"]["call_id"]
        != proposal["data"]["call_id"]
    )


def test_links_provider_proposal_to_custom_execution_by_exact_native_identity() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    proposal = projector.project(
        _row(
            record_id="provider-proposal",
            event_kind="tool",
            phase="event",
            name="lookup",
            semantic={
                "type": "tool.proposal",
                "native_call_id": "shared-native-call",
                "name": "lookup",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
            source_id="provider/openrouter",
            identity_domain="openai.operation",
        )
    )[0]
    call = projector.project(
        _row(
            record_id="custom-call",
            event_kind="tool",
            phase="start",
            name="lookup",
            semantic={
                "type": "tool.execution",
                "native_call_id": "shared-native-call",
                "name": "lookup",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )[0]
    result = projector.project(
        _row(
            record_id="custom-result",
            event_kind="tool",
            phase="end",
            name="lookup",
            semantic={
                "type": "tool.result",
                "native_call_id": "shared-native-call",
                "output": {"answer": 42},
            },
            parent_record_id="custom-call",
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )[0]

    _assert_valid([proposal, call, result])
    assert call["data"]["call_id"] == proposal["data"]["call_id"]
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert result["data"]["call_id"] == proposal["data"]["call_id"]
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]


def test_different_cross_source_tool_identity_does_not_link() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    proposal = projector.project(
        _row(
            record_id="provider-proposal",
            event_kind="tool",
            phase="event",
            name="lookup",
            semantic={
                "type": "tool.proposal",
                "native_call_id": "provider-call",
                "name": "lookup",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
            source_id="provider/openrouter",
            identity_domain="openai.operation",
        )
    )[0]
    call = projector.project(
        _row(
            record_id="custom-call",
            event_kind="tool",
            phase="start",
            name="lookup",
            semantic={
                "type": "tool.execution",
                "native_call_id": "custom-call",
                "name": "lookup",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )[0]

    _assert_valid([proposal, call])
    assert call["data"]["call_id"] != proposal["data"]["call_id"]
    assert "links" not in call


def test_duplicate_cross_source_proposal_identity_is_an_explicit_loss() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    first = projector.project(
        _row(
            record_id="provider-proposal",
            event_kind="tool",
            phase="event",
            name="lookup",
            semantic={
                "type": "tool.proposal",
                "native_call_id": "ambiguous-call",
                "name": "lookup",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
            source_id="provider/openrouter",
            identity_domain="openai.operation",
        )
    )[0]
    duplicate = projector.project(
        _row(
            record_id="custom-proposal",
            event_kind="tool",
            phase="event",
            name="lookup",
            semantic={
                "type": "tool.proposal",
                "native_call_id": "ambiguous-call",
                "name": "lookup",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )[0]
    call = projector.project(
        _row(
            record_id="custom-call",
            event_kind="tool",
            phase="start",
            name="lookup",
            semantic={
                "type": "tool.execution",
                "native_call_id": "ambiguous-call",
                "name": "lookup",
                "input": {"query": "alpha"},
            },
            parent_record_id="root",
            source_id="custom/agent",
            identity_domain="custom.operation",
        )
    )[0]

    _assert_valid([first, duplicate, call])
    assert duplicate["kind"] == "loss"
    assert duplicate["data"]["reason"] == "duplicate_active_tool_identity"
    assert call["data"]["call_id"] == first["data"]["call_id"]
    assert call["links"] == [{"type": "derived_from", "record": first["id"]}]


def test_only_explicit_run_semantics_create_roots_and_ids_are_stable() -> None:
    projector = SemanticProjector()
    generic = projector.project(
        _row(
            record_id="generic-start",
            event_kind="lifecycle",
            phase="start",
            name="generic lifecycle",
            semantic={"type": "capture.envelope"},
        )
    )
    material_generic = projector.project(
        _row(
            record_id="generic-material",
            event_kind="lifecycle",
            phase="start",
            name="generic lifecycle",
            semantic={"type": "capture.envelope"},
            native={"opaque": True},
        )
    )[0]
    explicit = projector.project(
        _row(
            trace_id="trace-root",
            record_id="record_root",
            event_kind="lifecycle",
            phase="start",
            name="agent generation",
            semantic={"type": "agent.generation", "name": "agent generation"},
        )
    )[0]
    unsafe_row = _row(
        trace_id="trace-unsafe",
        record_id="UPPER/unsafe",
        event_kind="lifecycle",
        phase="start",
        name="unsafe id run",
        semantic={"type": "agent.run"},
    )
    unsafe_a = projector.project(unsafe_row)[0]
    unsafe_b = SemanticProjector().project(unsafe_row)[0]

    _assert_valid([material_generic, explicit, unsafe_a, unsafe_b])
    assert generic == []
    assert material_generic["kind"] == "loss"
    assert explicit["kind"] == "run.start"
    assert explicit["id"] == "record_root"
    assert unsafe_a["id"] == unsafe_b["id"]
    assert unsafe_a["id"].startswith("rec_")


def test_projects_concurrent_models_by_exact_identity_not_request_order() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    scope = projector.project(
        _row(
            record_id="scope",
            event_kind="lifecycle",
            phase="start",
            name="agent",
            semantic={"type": "agent.scope", "scope_type": "agent"},
            parent_record_id="root",
        )
    )[0]
    message = projector.project(
        _row(
            record_id="message",
            event_kind="log",
            phase="event",
            name="context message",
            semantic={
                "type": "message",
                "origin": "context",
                "role": "user",
                "content": "Work concurrently",
            },
            parent_record_id="scope",
        )
    )[0]
    request_a = projector.project(
        _row(
            record_id="request-a",
            event_kind="model",
            phase="event",
            name="model request",
            native_identity="native-a",
            semantic={
                "type": "model.request",
                "model": "small-model",
                "context_refs": ["message"],
                "tools": ["search", "search"],
            },
            parent_record_id="scope",
        )
    )[0]
    request_b = projector.project(
        _row(
            record_id="request-b",
            event_kind="model",
            phase="event",
            name="model request",
            native_identity="native-b",
            semantic={"type": "model.request", "model": "small-model"},
            parent_record_id="scope",
        )
    )[0]
    conflicting_parent_request = projector.project(
        _row(
            record_id="request-conflicting-parent",
            event_kind="model",
            phase="event",
            name="model request",
            native_identity="native-conflicting-parent",
            semantic={"type": "model.request"},
            parent_record_id="root",
        )
    )[0]
    conflicting_response = projector.project(
        _row(
            record_id="response-conflicting",
            event_kind="model",
            phase="end",
            name="model response",
            native_identity="native-a",
            semantic={"type": "model.response", "content": "ambiguous"},
            parent_record_id="request-conflicting-parent",
        )
    )[0]
    response_b = projector.project(
        _row(
            record_id="response-b",
            event_kind="model",
            phase="end",
            name="model response",
            native_identity="native-b",
            semantic={
                "type": "model.response",
                "status": "completed",
                "content": "B",
                "usage": {"input_tokens": 7, "output_tokens": 1},
            },
            parent_record_id="root",
        )
    )[0]
    response_a = projector.project(
        _row(
            record_id="response-a",
            event_kind="model",
            phase="end",
            name="model response",
            native_identity="native-a",
            semantic={"type": "model.response", "status": "completed", "content": "A"},
            parent_record_id="root",
        )
    )[0]
    stale_response = projector.project(
        _row(
            record_id="response-a-stale",
            event_kind="model",
            phase="end",
            name="model response",
            native_identity="native-a",
            semantic={"type": "model.response", "content": "stale"},
            parent_record_id="request-a",
        )
    )[0]
    reused_request = projector.project(
        _row(
            record_id="request-a-reused",
            event_kind="model",
            phase="event",
            name="model request",
            native_identity="native-a",
            semantic={"type": "model.request"},
            parent_record_id="scope",
        )
    )[0]
    reused_response = projector.project(
        _row(
            record_id="response-a-reused",
            event_kind="model",
            phase="end",
            name="model response",
            native_identity="native-a",
            semantic={"type": "model.response", "content": "new"},
            parent_record_id="root",
        )
    )[0]
    unpaired = projector.project(
        _row(
            record_id="response-unpaired",
            event_kind="model",
            phase="end",
            name="model response",
            semantic={"type": "model.response", "content": "unknown"},
            parent_record_id="root",
        )
    )[0]

    records = [
        root,
        message,
        request_a,
        request_b,
        conflicting_parent_request,
        conflicting_response,
        response_b,
        response_a,
        stale_response,
        reused_request,
        reused_response,
        unpaired,
    ]
    records.insert(1, scope)
    _assert_valid(records)
    assert message["origin"] == "context"
    assert request_a["data"] == {
        "model": "small-model",
        "context_refs": [message["id"]],
        "tools": ["search"],
    }
    assert response_b["links"] == [{"type": "result_of", "record": request_b["id"]}]
    assert response_a["links"] == [{"type": "result_of", "record": request_a["id"]}]
    assert conflicting_response["kind"] == "loss"
    assert conflicting_response["data"]["reason"] == "ambiguous_model_response"
    assert conflicting_response["parent"] == root["id"]
    assert response_b["parent"] == scope["id"]
    assert response_a["parent"] == scope["id"]
    assert stale_response["kind"] == "loss"
    assert stale_response["data"]["reason"] == "unmatched_model_response"
    assert stale_response["parent"] == scope["id"]
    assert reused_response["links"] == [
        {"type": "result_of", "record": reused_request["id"]}
    ]
    assert reused_response["parent"] == scope["id"]
    assert "links" not in unpaired


def test_preserves_repeated_exposed_reasoning_in_source_order() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            semantic={"type": "agent.run", "name": "reasoning run"},
        )
    )
    response = projector.project(
        _row(
            record_id="reasoning-response",
            event_kind="model",
            phase="end",
            name="model response",
            semantic={
                "type": "model.response",
                "status": "completed",
                "content": "The answer is 42.",
                "reasoning": [
                    {"type": "summary", "text": "Checked the available evidence."},
                    {"type": "summary", "text": "Checked the available evidence."},
                    {"type": "text", "text": "The provider exposed this reasoning."},
                ],
            },
            parent_record_id="root",
        )
    )[0]

    assert response["data"] == {
        "status": "completed",
        "content": "The answer is 42.",
        "reasoning": [
            {"type": "summary", "text": "Checked the available evidence."},
            {"type": "summary", "text": "Checked the available evidence."},
            {"type": "text", "text": "The provider exposed this reasoning."},
        ],
    }
    _assert_valid([response])


def test_model_context_only_resolves_allowed_semantic_evidence() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    message = projector.project(
        _row(
            record_id="message",
            event_kind="log",
            phase="event",
            name="message",
            semantic={"type": "message", "role": "user", "content": "hello"},
            parent_record_id="root",
        )
    )[0]
    state = projector.project(
        _row(
            record_id="state",
            event_kind="state",
            phase="event",
            name="state",
            semantic={"type": "state.ready"},
            parent_record_id="root",
        )
    )[0]
    projection = projector.project(
        _row(
            record_id="request",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_refs": [
                    "message",
                    "message",
                    "state",
                    "root",
                    "missing",
                    7,
                ],
            },
            parent_record_id="root",
        )
    )

    _assert_valid([root, message, state, *projection])
    request, loss = projection
    assert request["data"]["context_refs"] == [message["id"], message["id"]]
    assert loss["kind"] == "loss"
    assert loss["data"]["reason"] == "unresolved_context_ref"
    assert loss["data"]["count"] == 4
    assert loss["links"] == [{"type": "affects", "record": request["id"]}]


def test_model_context_base_resolves_one_earlier_same_trace_request() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    first_message = projector.project(
        _row(
            record_id="message-a",
            event_kind="log",
            phase="event",
            name="message",
            semantic={"type": "message", "role": "user", "content": "A"},
            parent_record_id="root",
        )
    )[0]
    first_request = projector.project(
        _row(
            record_id="request-a",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_refs": ["message-a"],
            },
            parent_record_id="root",
        )
    )[0]
    projector.project(
        _row(
            record_id="response-a",
            event_kind="model",
            phase="end",
            name="response",
            semantic={"type": "model.response", "status": "completed"},
            parent_record_id="request-a",
        )
    )
    second_message = projector.project(
        _row(
            record_id="message-b",
            event_kind="log",
            phase="event",
            name="message",
            semantic={"type": "message", "role": "tool", "content": "B"},
            parent_record_id="root",
        )
    )[0]
    second_projection = projector.project(
        _row(
            record_id="request-b",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_base_ref": "request-a",
                "context_refs": ["message-b"],
            },
            parent_record_id="root",
        )
    )

    assert len(second_projection) == 1
    second_request = second_projection[0]
    assert second_request["data"] == {
        "context_base_ref": first_request["id"],
        "context_refs": [second_message["id"]],
    }
    _assert_valid([root, first_message, first_request, second_message, second_request])


def test_model_context_preserves_ordered_duplicate_refs_across_a_base() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    message = projector.project(
        _row(
            record_id="message",
            event_kind="log",
            phase="event",
            name="message",
            semantic={"type": "message", "role": "user", "content": "repeat"},
            parent_record_id="root",
        )
    )[0]
    first = projector.project(
        _row(
            record_id="request-a",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_refs": ["message", "message"],
            },
            parent_record_id="root",
        )
    )[0]
    second = projector.project(
        _row(
            record_id="request-b",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_base_ref": "request-a",
                "context_refs": ["message"],
            },
            parent_record_id="root",
        )
    )[0]

    assert first["data"]["context_refs"] == [message["id"], message["id"]]
    assert second["data"] == {
        "context_base_ref": first["id"],
        "context_refs": [message["id"]],
    }
    _assert_valid([root, message, first, second])


def test_model_context_base_accepts_different_source_traces_under_one_root() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            trace_id="root-trace",
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    first = projector.project(
        _row(
            trace_id="model-trace-a",
            record_id="request-a",
            event_kind="model",
            phase="event",
            name="request",
            semantic={"type": "model.request", "context_refs": []},
            parent_record_id="root",
        )
    )[0]
    projection = projector.project(
        _row(
            trace_id="model-trace-b",
            record_id="request-b",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_base_ref": "request-a",
                "context_refs": [],
            },
            parent_record_id="root",
        )
    )

    assert projection[0]["data"] == {
        "context_base_ref": first["id"],
        "context_refs": [],
    }
    _assert_valid([root, first, projection[0]])


def test_model_context_base_rejects_cross_trace_or_non_request_sources() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            trace_id="trace-a",
            record_id="root-a",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    message = projector.project(
        _row(
            trace_id="trace-a",
            record_id="message-a",
            event_kind="log",
            phase="event",
            name="message",
            semantic={"type": "message", "role": "user", "content": "A"},
            parent_record_id="root-a",
        )
    )[0]
    request = projector.project(
        _row(
            trace_id="trace-a",
            record_id="request-a",
            event_kind="model",
            phase="event",
            name="request",
            semantic={"type": "model.request", "context_refs": ["message-a"]},
            parent_record_id="root-a",
        )
    )[0]
    projector.project(
        _row(
            trace_id="trace-b",
            record_id="root-b",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )

    for index, invalid_base in enumerate(("message-a", "request-a")):
        projection = projector.project(
            _row(
                trace_id="trace-b",
                record_id=f"request-b-{index}",
                event_kind="model",
                phase="event",
                name="request",
                semantic={
                    "type": "model.request",
                    "context_base_ref": invalid_base,
                    "context_refs": [],
                },
                parent_record_id="root-b",
            )
        )
        assert "context_base_ref" not in projection[0]["data"]
        assert projection[1]["data"]["reason"] == "unresolved_context_base_ref"
        assert projection[1]["data"]["count"] == 1

    assert message["kind"] == "message"
    assert request["kind"] == "model.request"


def test_model_context_base_uses_effective_containment_root() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            trace_id="trace-a",
            record_id="root-a",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    projector.project(
        _row(
            trace_id="trace-b",
            record_id="root-b",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    crossed = projector.project(
        _row(
            trace_id="trace-b",
            record_id="request-crossed",
            event_kind="model",
            phase="event",
            name="request",
            semantic={"type": "model.request", "context_refs": []},
            parent_record_id="root-a",
        )
    )[0]
    projection = projector.project(
        _row(
            trace_id="trace-b",
            record_id="request-b",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_base_ref": "request-crossed",
                "context_refs": [],
            },
            parent_record_id="root-b",
        )
    )

    assert crossed["parent"] != projection[0]["parent"]
    assert projection[0]["data"] == {}
    assert projection[1]["data"]["reason"] == "unresolved_context_base_ref"


def test_model_context_base_rejects_two_rootless_requests() -> None:
    projector = SemanticProjector()
    first = projector.project(
        _row(
            record_id="rootless-a",
            event_kind="model",
            phase="event",
            name="request",
            semantic={"type": "model.request", "context_refs": []},
        )
    )[0]
    projection = projector.project(
        _row(
            record_id="rootless-b",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_base_ref": "rootless-a",
                "context_refs": [],
            },
        )
    )

    assert first["kind"] == "model.request"
    assert projection[0]["data"] == {}
    assert projection[1]["data"]["reason"] == "unresolved_context_base_ref"


def test_model_context_base_rejects_an_evicted_request() -> None:
    projector = SemanticProjector(max_completed_records=0)
    projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    projector.project(
        _row(
            record_id="request-a",
            event_kind="model",
            phase="event",
            name="request",
            semantic={"type": "model.request", "context_refs": []},
            parent_record_id="root",
        )
    )
    projector.project(
        _row(
            record_id="response-a",
            event_kind="model",
            phase="end",
            name="response",
            semantic={"type": "model.response", "status": "completed"},
            parent_record_id="request-a",
        )
    )

    projection = projector.project(
        _row(
            record_id="request-b",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_base_ref": "request-a",
                "context_refs": [],
            },
            parent_record_id="root",
        )
    )

    assert projection[0]["data"] == {}
    assert projection[1]["data"]["reason"] == "unresolved_context_base_ref"


def test_model_context_base_rejects_a_request_with_unresolved_context() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    first_projection = projector.project(
        _row(
            record_id="request-a",
            event_kind="model",
            phase="event",
            name="request",
            semantic={"type": "model.request", "context_refs": ["missing"]},
            parent_record_id="root",
        )
    )
    second_projection = projector.project(
        _row(
            record_id="request-b",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_base_ref": "request-a",
                "context_refs": [],
            },
            parent_record_id="root",
        )
    )

    assert first_projection[0]["data"] == {"context_refs": []}
    assert first_projection[1]["data"]["reason"] == "unresolved_context_ref"
    assert second_projection[0]["data"] == {}
    assert second_projection[1]["data"]["reason"] == "unresolved_context_base_ref"


def test_model_context_base_rejects_a_request_with_unobserved_context() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )
    projector.project(
        _row(
            record_id="request-a",
            event_kind="model",
            phase="event",
            name="request",
            semantic={"type": "model.request"},
            parent_record_id="root",
        )
    )
    projection = projector.project(
        _row(
            record_id="request-b",
            event_kind="model",
            phase="event",
            name="request",
            semantic={
                "type": "model.request",
                "context_base_ref": "request-a",
                "context_refs": [],
            },
            parent_record_id="root",
        )
    )

    assert projection[0]["data"] == {}
    assert projection[1]["data"]["reason"] == "unresolved_context_base_ref"


def test_projects_normalized_state_error_and_runtime_loss() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="root",
            event_kind="lifecycle",
            phase="start",
            name="workflow.run",
            semantic={"type": "workflow.run"},
        )
    )[0]
    state_row = _row(
        record_id="state",
        event_kind="state",
        phase="event",
        name="state transition",
        semantic={
            "type": "state.transition",
            "state_type": "state.delta",
            "version": 2,
            "value": {"ready": True},
        },
        parent_record_id="root",
    )
    state_row["blob_refs"] = [
        {
            "digest": "a" * 64,
            "byte_length": 12,
            "mime_type": "application/json",
            "scan": "clean",
        }
    ]
    state = projector.project(state_row)[0]
    interrupt = projector.project(
        _row(
            record_id="interrupt",
            event_kind="state",
            phase="event",
            name="interrupted",
            semantic={"type": "state.interrupt"},
            parent_record_id="root",
        )
    )[0]
    verification = projector.project(
        _row(
            record_id="verification",
            event_kind="state",
            phase="event",
            name="state check",
            semantic={
                "type": "verification",
                "subject": "action",
                "status": "passed",
                "records": ["state"],
            },
            parent_record_id="root",
        )
    )[0]
    error = projector.project(
        _row(
            record_id="error",
            event_kind="error",
            phase="error",
            name="agent error",
            semantic={
                "type": "agent.error",
                "error": {
                    "type": "tool_error",
                    "message": "The tool failed.",
                    "recoverable": True,
                    "code": "E_TOOL",
                },
            },
            parent_record_id="root",
        )
    )[0]
    loss_row = _row(
        record_id="loss",
        event_kind="loss",
        phase="gap",
        name="serialization loss",
        semantic={},
        parent_record_id="missing-loss-parent",
    )
    loss_row["loss"] = {
        "reason": "serialization_failure",
        "stage": "snapshot",
        "affected_record_id": "state",
        "affected_path": "/value",
        "count": 2,
        "recoverable": False,
        "bytes": 42,
        "detail": "Two values could not be represented.",
    }
    loss, loss_parent_gap = projector.project(loss_row)
    redundant = projector.project(
        _row(
            record_id="control",
            event_kind="correlation",
            phase="event",
            name="trace marker",
            semantic={"type": "agent.trace"},
            parent_record_id="root",
        )
    )
    duplicate = projector.project(
        _row(
            record_id="duplicate-model",
            event_kind="model",
            phase="end",
            name="duplicate model callback",
            native={"output": "already retained elsewhere"},
            semantic={"type": "capture.redundant"},
            parent_record_id="root",
        )
    )

    _assert_valid([root, state, interrupt, verification, error, loss, loss_parent_gap])
    assert state["data"] == {
        "type": "state.delta",
        "version": 2,
        "value": {"ready": True},
    }
    assert state["blob_refs"] == [
        {
            "path": f"blobs/{'a' * 64}.blob",
            "sha256": "a" * 64,
            "bytes": 12,
            "media_type": "application/json",
            "scan": "clean",
        }
    ]
    assert interrupt["data"] == {"type": "state.interrupt"}
    assert verification["data"] == {"subject": "action", "status": "passed"}
    assert verification["links"] == [
        {"type": "verifies", "record": state["id"]}
    ]
    assert error["data"] == {
        "type": "tool_error",
        "message": "The tool failed.",
        "recoverable": True,
        "code": "E_TOOL",
    }
    assert loss["data"] == {
        "reason": "serialization_failure",
        "stage": "serialize",
        "count": 2,
        "recoverable": False,
        "path": "/value",
        "bytes": 42,
        "detail": "Two values could not be represented.",
    }
    assert loss["links"] == [{"type": "affects", "record": state["id"]}]
    assert "parent" not in loss
    assert loss_parent_gap["kind"] == "loss"
    assert loss_parent_gap["data"]["reason"] == "unresolved_parent"
    assert loss_parent_gap["links"] == [{"type": "affects", "record": loss["id"]}]
    assert redundant == []
    assert duplicate == []


def test_projects_exact_state_and_outcome_evidence_references() -> None:
    projector = SemanticProjector()
    root = projector.project(
        _row(
            record_id="run-start",
            event_kind="lifecycle",
            phase="start",
            name="agent.run",
            semantic={"type": "agent.run"},
        )
    )[0]
    response = projector.project(
        _row(
            record_id="model-response",
            event_kind="model",
            phase="end",
            name="model",
            native_identity="model-call",
            semantic={
                "type": "model.response",
                "status": "completed",
                "content": {"text": "done"},
            },
            parent_record_id="run-start",
        )
    )[0]
    tool_call = projector.project(
        _row(
            record_id="tool-call",
            event_kind="tool",
            phase="start",
            name="lookup",
            native_identity="call-1",
            semantic={
                "type": "tool.execution",
                "native_call_id": "call-1",
                "name": "lookup",
                "input": {"query": "weather"},
            },
            parent_record_id="run-start",
        )
    )[0]
    tool_result = projector.project(
        _row(
            record_id="tool-result",
            event_kind="tool",
            phase="end",
            name="lookup",
            native_identity="call-1",
            semantic={
                "type": "tool.result",
                "native_call_id": "call-1",
                "status": "succeeded",
                "output": {"ok": True},
            },
            parent_record_id="tool-call",
        )
    )[0]
    state = projector.project(
        _row(
            record_id="state",
            event_kind="state",
            phase="event",
            name="state",
            semantic={
                "type": "state.transition",
                "state_type": "session.state_delta",
                "value": {"seen": True},
                "result_ref": "tool-result",
            },
            parent_record_id="run-start",
        )
    )[0]
    outcome = projector.project(
        _row(
            record_id="run-end",
            event_kind="lifecycle",
            phase="end",
            name="agent.run",
            semantic={
                "type": "agent.run",
                "status": "succeeded",
                "output_ref": "model-response",
            },
            parent_record_id="run-start",
        )
    )[0]

    records = [root, response, tool_call, tool_result, state, outcome]
    _assert_valid(records)
    assert state["links"] == [
        {"type": "derived_from", "record": tool_result["id"]}
    ]
    assert outcome["data"] == {"status": "completed"}
    assert outcome["links"] == [
        {"type": "derived_from", "record": response["id"]}
    ]


def test_bounds_completed_history_while_preserving_active_correlations() -> None:
    projector = SemanticProjector(max_completed_records=1)
    root = projector.project(
        _row(
            record_id="bounded-root",
            event_kind="lifecycle",
            phase="start",
            name="bounded run",
            turn_id="bounded-turn-old",
            semantic={"type": "agent.run", "name": "bounded run"},
        )
    )[0]
    scope = projector.project(
        _row(
            record_id="bounded-scope",
            event_kind="lifecycle",
            phase="start",
            name="bounded scope",
            parent_record_id="bounded-root",
            semantic={"type": "scope", "scope_type": "step", "name": "bounded scope"},
        )
    )[0]
    call = projector.project(
        _row(
            record_id="bounded-call",
            event_kind="tool",
            phase="start",
            name="lookup",
            native_identity="bounded-call",
            parent_record_id="bounded-scope",
            semantic={
                "type": "tool.execution",
                "name": "lookup",
                "input": {"query": "bounded"},
            },
        )
    )[0]
    request = projector.project(
        _row(
            record_id="bounded-request",
            event_kind="model",
            phase="start",
            name="bounded model request",
            native_identity="bounded-request",
            parent_record_id="bounded-scope",
            semantic={"type": "model.request", "model": "fixture-model"},
        )
    )[0]
    for record_id, content in (
        ("bounded-old-message", "old"),
        ("bounded-new-message", "new"),
    ):
        projector.project(
            _row(
                record_id=record_id,
                event_kind="log",
                phase="event",
                name=content,
                parent_record_id="bounded-scope",
                semantic={"type": "message", "role": "user", "content": content},
            )
        )

    evicted_parent = projector.project(
        _row(
            record_id="bounded-orphan",
            event_kind="log",
            phase="event",
            name="orphaned message",
            parent_record_id="bounded-old-message",
            semantic={
                "type": "message",
                "role": "assistant",
                "content": "orphaned",
            },
        )
    )[0]
    result = projector.project(
        _row(
            record_id="bounded-result",
            event_kind="tool",
            phase="end",
            name="lookup result",
            native_identity="bounded-call",
            parent_record_id="bounded-call",
            semantic={
                "type": "tool.result",
                "status": "succeeded",
                "output": {"ok": True},
            },
        )
    )[0]
    response = projector.project(
        _row(
            record_id="bounded-response",
            event_kind="model",
            phase="end",
            name="bounded model response",
            native_identity="bounded-request",
            parent_record_id="bounded-request",
            semantic={
                "type": "model.response",
                "status": "completed",
                "content": "done",
            },
        )
    )[0]
    scope_end = projector.project(
        _row(
            record_id="bounded-scope-end",
            event_kind="lifecycle",
            phase="end",
            name="bounded scope",
            parent_record_id="bounded-scope",
            semantic={"type": "scope", "status": "succeeded"},
        )
    )[0]
    outcome = projector.project(
        _row(
            record_id="bounded-outcome",
            event_kind="lifecycle",
            phase="end",
            name="bounded run",
            parent_record_id="bounded-root",
            semantic={"type": "agent.run", "status": "succeeded"},
        )
    )[0]
    projector.project(
        _row(
            trace_id="bounded-second-trace",
            record_id="bounded-second-root",
            event_kind="lifecycle",
            phase="start",
            name="second bounded run",
            turn_id="bounded-turn-new",
            semantic={"type": "agent.run", "name": "second bounded run"},
        )
    )
    projector.project(
        _row(
            trace_id="bounded-second-trace",
            record_id="bounded-second-outcome",
            event_kind="lifecycle",
            phase="end",
            name="second bounded run",
            parent_record_id="bounded-second-root",
            semantic={"type": "agent.run", "status": "succeeded"},
        )
    )
    unresolved_continuation = projector.project(
        _row(
            trace_id="bounded-resumed-trace",
            record_id="bounded-resumed-root",
            event_kind="lifecycle",
            phase="start",
            name="resumed bounded run",
            turn_id="bounded-turn-resumed",
            previous_turn_id="bounded-turn-old",
            semantic={"type": "agent.run", "name": "resumed bounded run"},
        )
    )
    for record_id, content, role in (
        ("bounded-reference-old", "old reference", "user"),
        ("bounded-reference-new", "new reference", "assistant"),
    ):
        projector.project(
            _row(
                trace_id="bounded-resumed-trace",
                record_id=record_id,
                event_kind="log",
                phase="event",
                name=content,
                parent_record_id="bounded-resumed-root",
                semantic={"type": "message", "role": role, "content": content},
            )
        )
    partial_verification = projector.project(
        _row(
            trace_id="bounded-resumed-trace",
            record_id="bounded-verification",
            event_kind="state",
            phase="event",
            name="bounded verification",
            parent_record_id="bounded-resumed-root",
            semantic={
                "type": "verification",
                "subject": "delivery",
                "status": "passed",
                "records": ["bounded-reference-new", "bounded-reference-old"],
            },
        )
    )
    runtime_loss = _row(
        trace_id="bounded-resumed-trace",
        record_id="bounded-runtime-loss",
        event_kind="loss",
        phase="gap",
        name="bounded runtime loss",
        parent_record_id="bounded-resumed-root",
        semantic={},
    )
    runtime_loss["loss"] = {
        "reason": "serialization_failure",
        "stage": "serialize",
        "affected_record_id": "bounded-reference-old",
        "count": 1,
        "recoverable": False,
    }
    unresolved_affected = projector.project(runtime_loss)

    assert evicted_parent["kind"] == "loss"
    assert evicted_parent["data"]["reason"] == "unresolved_parent"
    assert "parent" not in evicted_parent
    assert result["kind"] == "tool.result"
    assert result["parent"] == scope["id"]
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert response["kind"] == "model.response"
    assert response["parent"] == scope["id"]
    assert response["links"] == [{"type": "result_of", "record": request["id"]}]
    assert scope_end["kind"] == "scope"
    assert scope_end["parent"] == scope["id"]
    assert outcome["kind"] == "run.outcome"
    assert outcome["parent"] == root["id"]
    assert len(unresolved_continuation) == 2
    assert unresolved_continuation[0]["kind"] == "run.start"
    assert "links" not in unresolved_continuation[0]
    assert unresolved_continuation[1]["kind"] == "loss"
    assert (
        unresolved_continuation[1]["data"]["reason"]
        == "unresolved_previous_turn"
    )
    assert unresolved_continuation[1]["links"] == [
        {"type": "affects", "record": "bounded-resumed-root"}
    ]
    assert len(partial_verification) == 2
    assert partial_verification[0]["kind"] == "verification"
    assert partial_verification[0]["links"] == [
        {"type": "verifies", "record": "bounded-reference-new"}
    ]
    assert partial_verification[1]["kind"] == "loss"
    assert (
        partial_verification[1]["data"]["reason"]
        == "unresolved_verification_ref"
    )
    assert partial_verification[1]["data"]["count"] == 1
    assert partial_verification[1]["links"] == [
        {"type": "affects", "record": "bounded-verification"}
    ]
    assert len(unresolved_affected) == 2
    assert unresolved_affected[0]["kind"] == "loss"
    assert unresolved_affected[0]["data"]["reason"] == "serialization_failure"
    assert "links" not in unresolved_affected[0]
    assert unresolved_affected[1]["kind"] == "loss"
    assert unresolved_affected[1]["data"]["reason"] == "unresolved_affected_ref"
    assert unresolved_affected[1]["links"] == [
        {"type": "affects", "record": "bounded-runtime-loss"}
    ]
    _assert_valid(
        [
            evicted_parent,
            result,
            response,
            scope_end,
            outcome,
            *unresolved_continuation,
            *partial_verification,
            *unresolved_affected,
        ]
    )
    assert len(projector._evicted_turns) <= 1
    assert "bounded-turn-old" in projector._evicted_turns
    projector.project(
        _row(
            trace_id="bounded-reintroduced-trace",
            record_id="bounded-reintroduced-root",
            event_kind="lifecycle",
            phase="start",
            name="reintroduced bounded run",
            turn_id="bounded-turn-old",
            semantic={"type": "agent.run", "name": "reintroduced bounded run"},
        )
    )
    assert "bounded-turn-old" not in projector._evicted_turns


def test_rejects_duplicate_active_identities_without_overwriting_correlations() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="duplicate-root",
            event_kind="lifecycle",
            phase="start",
            name="duplicate run",
            semantic={"type": "agent.run", "name": "duplicate run"},
        )
    )
    proposal = projector.project(
        _row(
            record_id="duplicate-proposal-original",
            event_kind="tool",
            phase="event",
            name="lookup proposal",
            native_identity="duplicate-tool",
            parent_record_id="duplicate-root",
            semantic={"type": "tool.proposal", "name": "lookup", "input": {"value": 1}},
        )
    )[0]
    reused_proposal_record = projector.project(
        _row(
            record_id="duplicate-proposal-original",
            event_kind="tool",
            phase="event",
            name="reused proposal record",
            native_identity="different-tool",
            parent_record_id="duplicate-root",
            semantic={"type": "tool.proposal", "name": "lookup", "input": {"value": 3}},
        )
    )[0]
    duplicate_proposal = projector.project(
        _row(
            record_id="duplicate-proposal-rejected",
            event_kind="tool",
            phase="event",
            name="duplicate lookup proposal",
            native_identity="duplicate-tool",
            parent_record_id="duplicate-root",
            semantic={"type": "tool.proposal", "name": "lookup", "input": {"value": 2}},
        )
    )[0]
    call = projector.project(
        _row(
            record_id="duplicate-call-original",
            event_kind="tool",
            phase="start",
            name="lookup call",
            native_identity="duplicate-tool",
            parent_record_id="duplicate-root",
            semantic={"type": "tool.execution", "name": "lookup", "input": {"value": 1}},
        )
    )[0]
    duplicate_call = projector.project(
        _row(
            record_id="duplicate-call-rejected",
            event_kind="tool",
            phase="start",
            name="duplicate lookup call",
            native_identity="duplicate-tool",
            parent_record_id="duplicate-root",
            semantic={"type": "tool.execution", "name": "lookup", "input": {"value": 2}},
        )
    )[0]
    result = projector.project(
        _row(
            record_id="duplicate-result",
            event_kind="tool",
            phase="end",
            name="lookup result",
            native_identity="duplicate-tool",
            parent_record_id="duplicate-call-original",
            semantic={"type": "tool.result", "status": "succeeded", "output": {"value": 1}},
        )
    )[0]
    request = projector.project(
        _row(
            record_id="duplicate-request-original",
            event_kind="model",
            phase="start",
            name="model request",
            native_identity="duplicate-model",
            parent_record_id="duplicate-root",
            semantic={"type": "model.request", "model": "fixture-model"},
        )
    )[0]
    duplicate_request = projector.project(
        _row(
            record_id="duplicate-request-rejected",
            event_kind="model",
            phase="start",
            name="duplicate model request",
            native_identity="duplicate-model",
            parent_record_id="duplicate-root",
            semantic={"type": "model.request", "model": "other-model"},
        )
    )[0]
    response = projector.project(
        _row(
            record_id="duplicate-response",
            event_kind="model",
            phase="end",
            name="model response",
            native_identity="duplicate-model",
            parent_record_id="duplicate-request-original",
            semantic={"type": "model.response", "status": "completed", "content": "original"},
        )
    )[0]

    assert duplicate_proposal["data"]["reason"] == "duplicate_active_tool_identity"
    assert (
        reused_proposal_record["data"]["reason"]
        == "duplicate_active_tool_identity"
    )
    assert reused_proposal_record["id"] != proposal["id"]
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert duplicate_call["data"]["reason"] == "duplicate_active_tool_identity"
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert duplicate_request["data"]["reason"] == "duplicate_active_model_identity"
    assert response["links"] == [{"type": "result_of", "record": request["id"]}]
    _assert_valid(
        [
            proposal,
            reused_proposal_record,
            duplicate_proposal,
            call,
            duplicate_call,
            result,
            request,
            duplicate_request,
            response,
        ]
    )


def test_keeps_sibling_tool_executions_with_shared_semantic_call_id_distinct() -> None:
    projector = SemanticProjector()
    projector.project(
        _row(
            record_id="sibling-root",
            event_kind="lifecycle",
            phase="start",
            name="sibling run",
            semantic={"type": "agent.run", "name": "sibling run"},
        )
    )
    first_call = projector.project(
        _row(
            record_id="sibling-call-first",
            event_kind="tool",
            phase="start",
            name="lookup call",
            native_identity="execution-first",
            parent_record_id="sibling-root",
            semantic={
                "type": "tool.execution",
                "call_id": "shared-semantic-call",
                "name": "lookup",
                "input": {"branch": "first"},
            },
        )
    )[0]
    second_call = projector.project(
        _row(
            record_id="sibling-call-second",
            event_kind="tool",
            phase="start",
            name="lookup call",
            native_identity="execution-second",
            parent_record_id="sibling-root",
            semantic={
                "type": "tool.execution",
                "call_id": "shared-semantic-call",
                "name": "lookup",
                "input": {"branch": "second"},
            },
        )
    )[0]
    first_result = projector.project(
        _row(
            record_id="sibling-result-first",
            event_kind="tool",
            phase="end",
            name="lookup result",
            parent_record_id="sibling-call-first",
            semantic={
                "type": "tool.result",
                "call_id": "shared-semantic-call",
                "status": "succeeded",
                "output": {"branch": "first"},
            },
        )
    )[0]
    second_result = projector.project(
        _row(
            record_id="sibling-result-second",
            event_kind="tool",
            phase="end",
            name="lookup result",
            native_identity="execution-second",
            parent_record_id="sibling-call-second",
            semantic={
                "type": "tool.result",
                "status": "succeeded",
                "output": {"branch": "second"},
            },
        )
    )[0]

    assert first_call["kind"] == "tool.call"
    assert second_call["kind"] == "tool.call"
    assert first_call["data"]["call_id"] == second_call["data"]["call_id"]
    assert first_result["links"] == [
        {"type": "result_of", "record": first_call["id"]}
    ]
    assert second_result["links"] == [
        {"type": "result_of", "record": second_call["id"]}
    ]
    _assert_valid([first_call, second_call, first_result, second_result])


def test_caps_active_correlations_under_sustained_unterminated_and_reused_ids() -> None:
    max_active = 8
    projector = SemanticProjector(
        max_completed_records=8,
        max_active_correlations=max_active,
    )
    rejected_roots = 0
    for index in range(10_000):
        record = projector.project(
            _row(
                trace_id=f"stress-trace-{index}",
                record_id=f"stress-root-{index}",
                event_kind="lifecycle",
                phase="start",
                name="stress run",
                semantic={"type": "agent.run", "name": "stress run"},
            )
        )[0]
        if record["kind"] == "loss":
            assert record["data"]["reason"] == "active_correlation_limit"
            rejected_roots += 1

    assert rejected_roots == 10_000 - max_active
    assert len(projector._roots) == max_active
    assert len(projector._correlation_history) <= 16

    reused = SemanticProjector(
        max_completed_records=8,
        max_active_correlations=max_active,
    )
    reused.project(
        _row(
            record_id="stress-tool-root",
            event_kind="lifecycle",
            phase="start",
            name="tool stress run",
            semantic={"type": "agent.run", "name": "tool stress run"},
        )
    )
    reused.project(
        _row(
            record_id="stress-tool-call-0",
            event_kind="tool",
            phase="start",
            name="lookup",
            native_identity="stress-tool",
            parent_record_id="stress-tool-root",
            semantic={"type": "tool.execution", "name": "lookup", "input": {"value": 0}},
        )
    )
    duplicate_losses = 0
    for index in range(1, 10_000):
        record = reused.project(
            _row(
                record_id=f"stress-tool-call-{index}",
                event_kind="tool",
                phase="start",
                name="lookup",
                native_identity="stress-tool",
                parent_record_id="stress-tool-root",
                semantic={
                    "type": "tool.execution",
                    "name": "lookup",
                    "input": {"value": index},
                },
            )
        )[0]
        if record["data"].get("reason") == "duplicate_active_tool_identity":
            duplicate_losses += 1

    assert duplicate_losses == 9_999
    assert len(reused._tool_calls) == 1
    assert len(reused._tool_calls_by_start) == 1
    assert len(reused._correlation_history) <= 10
