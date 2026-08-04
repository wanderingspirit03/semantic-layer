from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker

pytest.importorskip("crewai")

from crewai.events import crewai_event_bus  # noqa: E402
from crewai.events.types.a2a_events import (  # noqa: E402
    A2ADelegationCompletedEvent,
    A2ADelegationStartedEvent,
)
from crewai.events.types.crew_events import (  # noqa: E402
    CrewKickoffCompletedEvent,
    CrewKickoffFailedEvent,
    CrewKickoffStartedEvent,
)
from crewai.events.types.llm_events import (  # noqa: E402
    LLMCallCompletedEvent,
    LLMCallFailedEvent,
    LLMCallStartedEvent,
    LLMCallType,
    LLMThinkingChunkEvent,
)
from crewai.events.types.tool_usage_events import (  # noqa: E402
    ToolUsageErrorEvent,
    ToolUsageFinishedEvent,
    ToolUsageStartedEvent,
)

from semantic_layer import initialize, reset_capture_for_tests  # noqa: E402
from semantic_layer.capture_v1 import AdmissionReceipt  # noqa: E402
from semantic_layer.crewai_adapter import (  # noqa: E402
    _CrewAISource,
    _OpenTurn,
    crewai_adapter,
)

_SCHEMA_PATH = Path(__file__).parents[4] / "contracts/trace/v1/semantic-trace-record.schema.json"
_VALIDATOR = Draft202012Validator(
    json.loads(_SCHEMA_PATH.read_text()),
    format_checker=FormatChecker(),
)


def _semantic_records(path: str) -> list[dict[str, Any]]:
    trace_path = Path(path) / "trace.jsonl"
    records = [json.loads(line) for line in trace_path.read_text().splitlines()]
    for record in records:
        _VALIDATOR.validate(record)
    return records


def _emit_crewai(event: Any) -> None:
    crewai_event_bus.emit(object(), event)
    crewai_event_bus.flush()


def _assert_compact_tool_relationships(records: list[dict[str, Any]]) -> None:
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    call = next(record for record in records if record["kind"] == "tool.call")
    result = next(record for record in records if record["kind"] == "tool.result")

    assert call["data"]["call_id"] == proposal["data"]["call_id"]
    assert result["data"]["call_id"] == call["data"]["call_id"]
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]


def test_crewai_capture_projects_one_compact_semantic_trajectory(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-semantic-projection")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)

    _emit_crewai(
        CrewKickoffStartedEvent(
            crew_name="support",
            inputs={"question": "Where is order 7?"},
            event_id="crew-turn-1",
        )
    )
    _emit_crewai(
        LLMCallStartedEvent(
            call_id="crew-model-1",
            model="fixture-model",
            messages=[{"role": "user", "content": "Where is order 7?"}],
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "lookup_order",
                        "description": "Find an order by ID.",
                        "parameters": {
                            "type": "object",
                            "properties": {"order_id": {"type": "integer"}},
                            "required": ["order_id"],
                        },
                    },
                    "provider_metadata": None,
                }
            ],
            temperature=0.0,
            top_p=0.9,
            max_tokens=512,
            stream=False,
            seed=0,
            stop_sequences=["END"],
            frequency_penalty=0.0,
            presence_penalty=0.0,
            n=1,
        )
    )
    _emit_crewai(
        LLMCallCompletedEvent(
            call_id="crew-model-1",
            model="fixture-model",
            response="I will look it up.",
            call_type=LLMCallType.LLM_CALL,
            finish_reason="stop",
            usage={"prompt_tokens": 6, "completion_tokens": 5},
        )
    )
    tool_start = ToolUsageStartedEvent(
        tool_name="lookup_order",
        tool_args={"order_id": 7},
        event_id="crew-tool-1",
    )
    _emit_crewai(tool_start)
    now = datetime.now(timezone.utc)
    _emit_crewai(
        ToolUsageFinishedEvent(
            tool_name="lookup_order",
            tool_args={"order_id": 7},
            output={"status": "shipped"},
            started_at=now,
            finished_at=now,
            event_id="crew-tool-1-end",
            started_event_id=tool_start.event_id,
        )
    )
    _emit_crewai(
        CrewKickoffCompletedEvent(
            crew_name="support",
            output="Order 7 shipped.",
            event_id="crew-turn-1-end",
        )
    )

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    assert [record["kind"] for record in records].count("model.response") == 1
    assert not [record for record in records if record["kind"] == "loss"]
    assert records[0]["kind"] == "run.start"
    assert records[-1]["kind"] == "run.outcome"
    assert records[-1]["parent"] == records[0]["id"]
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"] == {
        "status": "completed",
        "model": "fixture-model",
        "content": "I will look it up.",
        "finish_reason": "stop",
        "usage": {"input_tokens": 6, "output_tokens": 5},
    }
    message = next(record for record in records if record["kind"] == "message")
    request = next(record for record in records if record["kind"] == "model.request")
    assert request["data"]["context_refs"] == [message["id"]]
    assert request["data"]["tools"] == ["lookup_order"]
    assert request["data"]["tool_definitions"] == [
        {
            "type": "function",
            "function": {
                "name": "lookup_order",
                "description": "Find an order by ID.",
                "parameters": {
                    "type": "object",
                    "properties": {"order_id": {"type": "integer"}},
                    "required": ["order_id"],
                },
            },
        }
    ]
    assert request["data"]["settings"] == {
        "temperature": 0.0,
        "top_p": 0.9,
        "max_tokens": 512,
        "stream": False,
        "seed": 0,
        "stop_sequences": ["END"],
        "frequency_penalty": 0.0,
        "presence_penalty": 0.0,
        "n": 1,
    }
    _assert_compact_tool_relationships(records)


def test_crewai_aggregates_exposed_thinking_chunks_on_the_model_response(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-reasoning-projection")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)

    _emit_crewai(
        CrewKickoffStartedEvent(
            crew_name="research",
            inputs={"question": "Choose the current record."},
            event_id="crew-reasoning-turn",
        )
    )
    _emit_crewai(
        LLMCallStartedEvent(
            call_id="crew-reasoning-model",
            model="fixture-reasoning-model",
            messages=[{"role": "user", "content": "Choose the current record."}],
            stream=True,
        )
    )
    for chunk in ("Inspect both ", "records."):
        _emit_crewai(
            LLMThinkingChunkEvent(
                call_id="crew-reasoning-model",
                model="fixture-reasoning-model",
                chunk=chunk,
            )
        )
    _emit_crewai(
        LLMCallCompletedEvent(
            call_id="crew-reasoning-model",
            model="fixture-reasoning-model",
            response="Use record two.",
            call_type=LLMCallType.LLM_CALL,
            finish_reason="stop",
        )
    )
    _emit_crewai(
        CrewKickoffCompletedEvent(
            crew_name="research",
            output="Use record two.",
            event_id="crew-reasoning-turn-end",
        )
    )

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect both records."}
    ]


def test_crewai_a2a_handoff_uses_only_official_started_event_identity(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-handoff-projection")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)

    _emit_crewai(
        CrewKickoffStartedEvent(
            crew_name="support",
            inputs={"question": "Escalate order 7"},
            event_id="crew-handoff-turn",
        )
    )
    started = A2ADelegationStartedEvent(
        endpoint="https://specialist.example/a2a",
        task_description="Investigate order 7",
        from_agent=SimpleNamespace(id="support-agent", role="support-lead"),
        task_id="support-task-7",
        context_id="a2a-context-7",
        a2a_agent_name="Order specialist",
        event_id="handoff-start-7",
    )
    _emit_crewai(started)
    _emit_crewai(
        A2ADelegationCompletedEvent(
            status="completed",
            result="Order 7 was found.",
            context_id="a2a-context-7",
            endpoint="https://specialist.example/a2a",
            a2a_agent_name="Order specialist",
            event_id="handoff-end-7",
            started_event_id=started.event_id,
        )
    )
    _emit_crewai(
        CrewKickoffCompletedEvent(
            crew_name="support",
            output="Order 7 was found.",
            event_id="crew-handoff-turn-end",
        )
    )

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    handoffs = [
        record
        for record in records
        if record["kind"] == "state" and record["data"]["type"] == "agent.handoff"
    ]
    assert len(handoffs) == 2
    assert handoffs[0]["data"]["value"] == {
        "status": "started",
        "source_agent_id": "support-agent",
        "source_agent_role": "support-lead",
        "source_task_id": "support-task-7",
        "task": "Investigate order 7",
        "context_id": "a2a-context-7",
        "endpoint": "https://specialist.example/a2a",
        "target_agent_name": "Order specialist",
        "turn_number": 1,
        "is_multiturn": False,
    }
    assert handoffs[1]["data"]["value"] == {
        "status": "completed",
        "context_id": "a2a-context-7",
        "endpoint": "https://specialist.example/a2a",
        "target_agent_name": "Order specialist",
        "is_multiturn": False,
        "result": "Order 7 was found.",
    }
    assert handoffs[1]["parent"] == handoffs[0]["id"]
    assert not [record for record in records if record["kind"] == "loss"]


def test_crewai_handoff_completion_without_event_id_is_retained_without_fake_identity() -> None:
    class Sink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            self.records.append(record)
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    source = _CrewAISource(object(), "1.15.2")
    sink = Sink()
    source._sink = sink
    turn = _OpenTurn(
        {
            "trace_id": "trace",
            "operation_id": "operation",
            "session_id": "session",
        },
        "turn",
        AdmissionReceipt(True, record_id="root"),
    )
    source._event_turn["handoff-start-7"] = turn
    source._handoffs["handoff-start-7"] = AdmissionReceipt(
        True,
        record_id="handoff-start-record",
    )
    completion = A2ADelegationCompletedEvent(
        status="completed",
        result="Order 7 was found.",
        started_event_id="handoff-start-7",
        event_id="temporary-event-id",
    )
    completion.event_id = None  # type: ignore[assignment]

    source._finish_handoff(completion)

    ended = [record for record in sink.records if record["phase"] == "end"]
    assert len(ended) == 1
    assert "native_identity" not in ended[0]
    assert ended[0]["parent_record_id"] == "handoff-start-record"
    gaps = [record for record in sink.records if record["phase"] == "gap"]
    assert len(gaps) == 1
    assert gaps[0]["semantic"]["reason"] == "handoff_completion_identity_not_observed"


def test_crewai_unpaired_handoff_completion_emits_exactly_one_loss(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-unpaired-handoff")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)

    _emit_crewai(
        CrewKickoffStartedEvent(
            crew_name="support",
            inputs={"question": "Escalate order 8"},
            event_id="crew-unpaired-turn",
        )
    )
    _emit_crewai(
        A2ADelegationCompletedEvent(
            status="completed",
            result="Order 8 was found.",
            event_id="handoff-end-8",
            started_event_id="handoff-start-not-observed",
        )
    )
    _emit_crewai(
        CrewKickoffCompletedEvent(
            crew_name="support",
            output="Order 8 was found.",
            event_id="crew-unpaired-turn-end",
        )
    )

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "handoff_correlation_not_observed"


def test_crewai_failures_project_as_observed_outcomes_not_losses(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-failure-projection")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)

    _emit_crewai(
        CrewKickoffStartedEvent(
            crew_name="support",
            inputs={"question": "Find order 8"},
            event_id="crew-failed-turn",
        )
    )
    _emit_crewai(
        LLMCallStartedEvent(
            call_id="crew-failed-model",
            model="fixture-model",
            messages=[{"role": "user", "content": "Find order 8"}],
        )
    )
    _emit_crewai(
        LLMCallFailedEvent(
            call_id="crew-failed-model",
            model="fixture-model",
            error="model unavailable",
        )
    )
    tool_start = ToolUsageStartedEvent(
        tool_name="lookup_order",
        tool_args={"order_id": 8},
        event_id="crew-failed-tool",
    )
    _emit_crewai(tool_start)
    _emit_crewai(
        ToolUsageErrorEvent(
            tool_name="lookup_order",
            tool_args={"order_id": 8},
            error="tool unavailable",
            event_id="crew-failed-tool-end",
            started_event_id=tool_start.event_id,
        )
    )
    _emit_crewai(
        CrewKickoffFailedEvent(
            crew_name="support",
            error="workflow failed",
            event_id="crew-failed-turn-end",
        )
    )

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    assert not [record for record in records if record["kind"] == "loss"]
    assert (
        next(record for record in records if record["kind"] == "model.response")["data"]["status"]
        == "failed"
    )
    failed_tool = next(record for record in records if record["kind"] == "tool.result")
    assert failed_tool["data"]["status"] == "failed"
    assert failed_tool["data"]["error"]["message"] == "tool unavailable"
    assert records[-1]["kind"] == "run.outcome"
    assert records[-1]["data"]["status"] == "failed"
    assert records[-1]["data"]["error"]["message"] == "workflow failed"
