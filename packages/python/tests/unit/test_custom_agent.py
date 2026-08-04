from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from semantic_layer import create_custom_agent_source, initialize


def _records(path: str) -> list[dict[str, Any]]:
    trace = Path(path, "trace.jsonl").read_text()
    return [json.loads(line) for line in trace.splitlines()]


def _runtime_event(value: dict[str, Any]) -> Any:
    return value


def test_custom_agent_keeps_exact_model_and_tool_chain(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="fixture-agent", version="1")
    capture.install_source(bridge.source)

    assert bridge.record(
        {
            "type": "run.start",
            "run_id": "run-a",
            "name": "coding-agent",
            "input": {"task": "read README"},
        }
    ).accepted
    bridge.record(
        {
            "type": "message",
            "run_id": "run-a",
            "message_id": "message-1",
            "role": "user",
            "content": [{"type": "text", "text": "read README"}],
        }
    )
    bridge.record(
        {
            "type": "model.request",
            "run_id": "run-a",
            "call_id": "model-1",
            "model": "fixture-model",
            "message_ids": ["message-1"],
        }
    )
    bridge.record(
        {
            "type": "model.response",
            "run_id": "run-a",
            "call_id": "model-1",
            "status": "completed",
            "content": [{"type": "text", "text": "I will read it."}],
            "usage": {"input_tokens": 3, "output_tokens": 5},
        }
    )
    bridge.record(
        {
            "type": "tool.proposal",
            "run_id": "run-a",
            "call_id": "tool-1",
            "name": "read_file",
            "input": {"path": "README.md"},
        }
    )
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "run-a",
            "call_id": "tool-1",
            "name": "read_file",
            "input": {"path": "README.md"},
        }
    )
    bridge.record(
        {
            "type": "tool.result",
            "run_id": "run-a",
            "call_id": "tool-1",
            "status": "succeeded",
            "output": {"text": "contents"},
        }
    )
    bridge.record(
        {
            "type": "run.outcome",
            "run_id": "run-a",
            "status": "completed",
            "output": "done",
        }
    )

    records = _records(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records] == [
        "run.start",
        "message",
        "model.request",
        "model.response",
        "tool.proposal",
        "tool.call",
        "tool.result",
        "run.outcome",
    ]
    call = next(record for record in records if record["kind"] == "tool.call")
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    result = next(record for record in records if record["kind"] == "tool.result")
    message = next(record for record in records if record["kind"] == "message")
    request = next(record for record in records if record["kind"] == "model.request")
    assert request["data"]["context_refs"] == [message["id"]]
    assert proposal["data"]["native_call_id"] == "tool-1"
    assert call["data"]["native_call_id"] == "tool-1"
    assert result["data"]["native_call_id"] == "tool-1"
    assert proposal["data"]["call_id"] == call["data"]["call_id"] == result["data"]["call_id"]
    assert {"type": "derived_from", "record": proposal["id"]} in call["links"]
    assert {"type": "result_of", "record": call["id"]} in result["links"]


def test_custom_agent_model_context_presence_is_exact_and_never_partial(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-context-presence")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)
    bridge.record({"type": "run.start", "run_id": "run-a", "name": "agent"})
    for message_id, content in (
        ("message-1", "first"),
        ("message-2", "second"),
    ):
        bridge.record(
            {
                "type": "message",
                "run_id": "run-a",
                "message_id": message_id,
                "role": "user",
                "content": content,
            }
        )

    requests: list[dict[str, Any]] = [
        {"type": "model.request", "run_id": "run-a", "call_id": "omitted"},
        {
            "type": "model.request",
            "run_id": "run-a",
            "call_id": "empty",
            "message_ids": [],
        },
        {
            "type": "model.request",
            "run_id": "run-a",
            "call_id": "full",
            "message_ids": ["message-1", "message-2"],
        },
        {
            "type": "model.request",
            "run_id": "run-a",
            "call_id": "partial-invalid",
            "message_ids": ["message-1", "missing", ""],
        },
        {
            "type": "model.request",
            "run_id": "run-a",
            "call_id": "malformed",
            "message_ids": 42,
        },
    ]
    for request in requests:
        bridge.record(_runtime_event(request))
        bridge.record(
            {
                "type": "model.response",
                "run_id": "run-a",
                "call_id": request["call_id"],
                "status": "completed",
                "content": None,
            }
        )
    bridge.record({"type": "run.outcome", "run_id": "run-a", "status": "completed"})

    records = _records(capture.shutdown().artifact_path)
    messages = [record for record in records if record["kind"] == "message"]
    model_requests = [record for record in records if record["kind"] == "model.request"]
    assert len(model_requests) == 5
    assert "context_refs" not in model_requests[0]["data"]
    assert model_requests[1]["data"]["context_refs"] == []
    assert model_requests[2]["data"]["context_refs"] == [
        messages[0]["id"],
        messages[1]["id"],
    ]
    assert "context_refs" not in model_requests[3]["data"]
    assert "context_refs" not in model_requests[4]["data"]
    assert [
        record["data"]["reason"] for record in records if record["kind"] == "loss"
    ] == [
        "invalid_message_id",
        "unknown_message_id",
        "invalid_message_ids",
    ]


def test_custom_agent_output_survives_empty_manual_observe_outcome(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-composed-output")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)
    expected_output = "The agent delivered its complete terminal answer."

    with capture.observe("fixture-agent", input={"task": "answer"}):
        assert bridge.record(
            {
                "type": "run.start",
                "run_id": "run-a",
                "name": "fixture-agent",
                "input": {"task": "answer"},
            }
        ).accepted
        assert bridge.record(
            {
                "type": "run.outcome",
                "run_id": "run-a",
                "status": "completed",
                "output": expected_output,
            }
        ).accepted

    records = _records(capture.shutdown().artifact_path)
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(outcomes) == 1
    assert outcomes[0]["data"] == {
        "status": "completed",
        "output": expected_output,
    }


@pytest.mark.parametrize("manual_output", [None, False, {"manual": "delivery"}])
def test_explicit_manual_output_precedes_composed_custom_output(
    tmp_path: Path,
    manual_output: Any,
) -> None:
    capture = initialize(output=tmp_path, service_name="manual-output-precedence")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)

    with capture.observe("fixture-agent") as scope:
        scope.set_output(manual_output)
        bridge.record(
            {
                "type": "run.start",
                "run_id": "run-a",
                "name": "fixture-agent",
            }
        )
        bridge.record(
            {
                "type": "run.outcome",
                "run_id": "run-a",
                "status": "completed",
                "output": "custom output",
            }
        )

    records = _records(capture.shutdown().artifact_path)
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(outcomes) == 1
    assert outcomes[0]["data"] == {
        "status": "completed",
        "output": manual_output,
    }


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_custom_agent_terminal_status_stays_on_scope_when_manual_root_completes(
    tmp_path: Path,
    status: str,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-composed-status")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)

    with capture.observe("fixture-agent"):
        assert bridge.record(
            {
                "type": "run.start",
                "run_id": "run-a",
                "name": "fixture-agent",
            }
        ).accepted
        assert bridge.record(
            {
                "type": "run.outcome",
                "run_id": "run-a",
                "status": status,
            }
        ).accepted

    records = _records(capture.shutdown().artifact_path)
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    scope_ends = [
        record
        for record in records
        if record["kind"] == "scope" and record["data"]["phase"] == "end"
    ]
    assert len(outcomes) == 1
    assert outcomes[0]["data"] == {"status": "completed"}
    assert len(scope_ends) == 1
    assert scope_ends[0]["data"]["status"] == status


def test_custom_agent_keeps_pi_shaped_responses_without_request_callbacks(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="pi-shaped-agent")
    capture.install_source(bridge.source)
    bridge.record({"type": "run.start", "run_id": "run-a", "name": "agent"})
    bridge.record(
        {
            "type": "message",
            "run_id": "run-a",
            "message_id": "message-1",
            "role": "user",
            "content": "Inspect the repository.",
        }
    )
    bridge.record(
        {
            "type": "model.response",
            "run_id": "run-a",
            "call_id": "model-response-1",
            "status": "completed",
            "model": "fixture-model",
            "content": [{"type": "text", "text": "I will inspect it."}],
            "reasoning": [{"type": "summary", "text": "Need repository context."}],
            "finish_reason": "tool_use",
            "usage": {"input_tokens": 8, "output_tokens": 5},
        }
    )
    bridge.record(
        {
            "type": "model.response",
            "run_id": "run-a",
            "call_id": "model-response-2",
            "status": "failed",
            "error": {
                "type": "provider_error",
                "message": "provider unavailable",
                "recoverable": True,
                "code": "503",
                "details": {"retry_after": 1},
            },
        }
    )
    bridge.record({"type": "run.outcome", "run_id": "run-a", "status": "failed"})

    records = _records(capture.shutdown().artifact_path)
    run = next(record for record in records if record["kind"] == "run.start")
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 2
    assert all(response["parent"] == run["id"] for response in responses)
    assert all(
        not any(link["type"] == "result_of" for link in response.get("links", []))
        for response in responses
    )
    assert responses[0]["data"] == {
        "status": "completed",
        "model": "fixture-model",
        "finish_reason": "tool_use",
        "content": [{"type": "text", "text": "I will inspect it."}],
        "reasoning": [{"type": "summary", "text": "Need repository context."}],
        "usage": {"input_tokens": 8, "output_tokens": 5},
    }
    error = next(record for record in records if record["kind"] == "error")
    assert error["data"] == {
        "type": "provider_error",
        "message": "provider unavailable",
        "recoverable": True,
        "code": "503",
        "details": {"retry_after": 1},
    }
    assert [
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"] == "model_request_not_observed"
    ] == ["model_request_not_observed"]


def test_custom_agent_keeps_response_only_callbacks_without_native_ids(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="response-only-agent", version="1.0.0")
    capture.install_source(bridge.source)
    bridge.record(
        {"type": "run.start", "run_id": "conversation-1", "name": "response-only-agent"}
    )

    first = bridge.record(
        _runtime_event(
            {
                "type": "model.response",
                "run_id": "conversation-1",
                "status": "completed",
                "model": "fixture-model",
                "content": [
                    {"type": "text", "text": "I will inspect the file."},
                ],
                "reasoning": [{"type": "summary", "text": "Need repository evidence."}],
                "finish_reason": "tool_use",
                "usage": {"input_tokens": 12, "output_tokens": 9},
            }
        )
    )
    assert first.accepted
    bridge.record(
        {
            "type": "tool.proposal",
            "run_id": "conversation-1",
            "call_id": "tool-call-1",
            "name": "read_file",
            "input": {"path": "README.md"},
        }
    )
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "conversation-1",
            "call_id": "tool-call-1",
            "name": "read_file",
            "input": {"path": "README.md"},
        }
    )
    bridge.record(
        {
            "type": "tool.result",
            "run_id": "conversation-1",
            "call_id": "tool-call-1",
            "status": "succeeded",
            "output": {"text": "# Example"},
        }
    )
    second = bridge.record(
        _runtime_event(
            {
                "type": "model.response",
                "run_id": "conversation-1",
                "status": "failed",
                "model": "fixture-model",
                "error": {
                    "type": "provider_error",
                    "message": "upstream unavailable",
                    "recoverable": True,
                    "code": "503",
                    "details": {"retry_after": 1},
                },
            }
        )
    )
    assert second.accepted
    bridge.record(
        {
            "type": "run.outcome",
            "run_id": "conversation-1",
            "status": "failed",
        }
    )

    records = _records(capture.shutdown().artifact_path)
    run = next(record for record in records if record["kind"] == "run.start")
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 2
    assert all(response["parent"] == run["id"] for response in responses)
    assert all(
        not any(link["type"] == "result_of" for link in response.get("links", []))
        for response in responses
    )
    assert responses[0]["data"] == {
        "status": "completed",
        "model": "fixture-model",
        "finish_reason": "tool_use",
        "content": [
            {"type": "text", "text": "I will inspect the file."},
        ],
        "reasoning": [{"type": "summary", "text": "Need repository evidence."}],
        "usage": {"input_tokens": 12, "output_tokens": 9},
    }
    error = next(record for record in records if record["kind"] == "error")
    assert error["parent"] == responses[1]["id"]
    assert error["data"] == {
        "type": "provider_error",
        "message": "upstream unavailable",
        "recoverable": True,
        "code": "503",
        "details": {"retry_after": 1},
    }
    losses = [record["data"]["reason"] for record in records if record["kind"] == "loss"]
    assert losses.count("model_request_not_observed") == 1
    assert losses.count("model_response_identity_not_observed") == 1

    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    call = next(record for record in records if record["kind"] == "tool.call")
    result = next(record for record in records if record["kind"] == "tool.result")
    assert proposal["data"]["native_call_id"] == "tool-call-1"
    assert call["data"]["native_call_id"] == "tool-call-1"
    assert result["data"]["native_call_id"] == "tool-call-1"
    assert proposal["data"]["call_id"] == call["data"]["call_id"] == result["data"]["call_id"]
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]


def test_custom_agent_separates_concurrent_runs_without_proposals(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)

    for run_id in ("run-a", "run-b"):
        bridge.record({"type": "run.start", "run_id": run_id, "name": "agent"})
        bridge.record(
            {
                "type": "tool.call",
                "run_id": run_id,
                "call_id": "shared-native-id",
                "name": "lookup",
                "input": {"run_id": run_id},
            }
        )
    for run_id in ("run-b", "run-a"):
        bridge.record(
            {
                "type": "tool.result",
                "run_id": run_id,
                "call_id": "shared-native-id",
                "status": "succeeded",
                "output": {"run_id": run_id},
            }
        )
        bridge.record({"type": "run.outcome", "run_id": run_id, "status": "completed"})

    records = _records(capture.shutdown().artifact_path)
    calls = [record for record in records if record["kind"] == "tool.call"]
    results = [record for record in records if record["kind"] == "tool.result"]
    assert len(calls) == len(results) == 2
    assert len({record["data"]["call_id"] for record in calls}) == 2
    for result in results:
        linked = next(link["record"] for link in result["links"] if link["type"] == "result_of")
        assert any(call["id"] == linked and call["parent"] == result["parent"] for call in calls)


def test_manual_tool_uses_supplied_call_id_without_changing_identity(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    returned = {"exact": True}
    with capture.observe("manual-agent") as run:
        result = run.tool(
            "read_file",
            {"path": "README.md"},
            lambda _value: returned,
            call_id="native-call-7",
        )
    assert result is returned

    with capture.observe("manual-agent-invalid-id") as run:
        run.tool(
            "read_file",
            {"path": "README.md"},
            lambda _value: returned,
            call_id="\0",
        )
    records = _records(capture.shutdown().artifact_path)
    call = next(record for record in records if record["kind"] == "tool.call")
    tool_result = next(record for record in records if record["kind"] == "tool.result")
    assert call["data"]["native_call_id"] == "native-call-7"
    assert tool_result["data"]["call_id"] == call["data"]["call_id"]
    assert any(
        record["kind"] == "loss" and record["data"]["reason"] == "invalid_call_id"
        for record in records
    )


@pytest.mark.asyncio
async def test_manual_duplicate_call_id_uses_local_correlation_without_behavior_change(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    first_ready: asyncio.Future[str] = asyncio.Future()
    second_ready: asyncio.Future[str] = asyncio.Future()
    executions = 0

    async def execute(future: asyncio.Future[str]) -> str:
        nonlocal executions
        executions += 1
        return await future

    with capture.observe("manual-agent") as run:
        first = run.tool(
            "first",
            first_ready,
            execute,
            call_id="reused-call",
        )
        second = run.tool(
            "second",
            second_ready,
            execute,
            call_id="reused-call",
        )
        second_ready.set_result("second-result")
        assert await second == "second-result"
        first_ready.set_result("first-result")
        assert await first == "first-result"

    records = _records(capture.shutdown().artifact_path)
    calls = [record for record in records if record["kind"] == "tool.call"]
    results = [record for record in records if record["kind"] == "tool.result"]
    assert executions == 2
    assert len(calls) == len(results) == 2
    assert sum(record["data"].get("native_call_id") == "reused-call" for record in calls) == 1
    assert len({record["data"]["call_id"] for record in calls}) == 2
    for result in results:
        linked = next(link["record"] for link in result["links"] if link["type"] == "result_of")
        assert any(call["id"] == linked for call in calls)
    assert (
        sum(
            record["kind"] == "loss" and record["data"]["reason"] == "duplicate_call_id"
            for record in records
        )
        == 1
    )


def test_custom_agent_never_guesses_after_collision_or_replay(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)
    bridge.record({"type": "run.start", "run_id": "run-a", "name": "agent"})

    bridge.record({"type": "model.request", "run_id": "run-a", "call_id": "model-collision"})
    bridge.record({"type": "model.request", "run_id": "run-a", "call_id": "model-collision"})
    bridge.record(
        {
            "type": "model.response",
            "run_id": "run-a",
            "call_id": "model-collision",
            "status": "completed",
            "content": None,
        }
    )
    for index in range(2):
        bridge.record(
            {
                "type": "tool.call",
                "run_id": "run-a",
                "call_id": "tool-collision",
                "name": f"lookup-{index}",
                "input": {"index": index},
            }
        )
    bridge.record(
        {
            "type": "tool.result",
            "run_id": "run-a",
            "call_id": "tool-collision",
            "status": "succeeded",
            "output": "ambiguous",
        }
    )
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "run-a",
            "call_id": "tool-replay",
            "name": "lookup",
            "input": None,
        }
    )
    bridge.record(
        {
            "type": "tool.result",
            "run_id": "run-a",
            "call_id": "tool-replay",
            "status": "succeeded",
            "output": "first",
        }
    )
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "run-a",
            "call_id": "tool-replay",
            "name": "lookup",
            "input": None,
        }
    )
    bridge.record(
        {
            "type": "tool.result",
            "run_id": "run-a",
            "call_id": "tool-replay",
            "status": "succeeded",
            "output": "replayed",
        }
    )
    bridge.record({"type": "run.outcome", "run_id": "run-a", "status": "completed"})

    records = _records(capture.shutdown().artifact_path)
    assert sum(record["kind"] == "model.request" for record in records) == 1
    assert sum(record["kind"] == "model.response" for record in records) == 0
    assert sum(record["kind"] == "tool.call" for record in records) == 2
    assert sum(record["kind"] == "tool.result" for record in records) == 1
    assert [record["data"]["reason"] for record in records if record["kind"] == "loss"] == [
        "duplicate_model_request",
        "ambiguous_model_response",
        "duplicate_tool_call",
        "ambiguous_tool_result",
        "duplicate_tool_call",
        "duplicate_tool_result",
    ]


def test_custom_agent_malformed_tool_callbacks_do_not_poison_corrections(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)
    bridge.record({"type": "run.start", "run_id": "run-a", "name": "agent"})
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "run-a",
            "call_id": "bad-name",
            "name": "",
            "input": None,
        }
    )
    bridge.record(
        _runtime_event(
            {
                "type": "tool.call",
                "run_id": "run-a",
                "call_id": "corrected",
                "name": "lookup",
            }
        )
    )
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "run-a",
            "call_id": "corrected",
            "name": "lookup",
            "input": None,
        }
    )
    bridge.record(
        {
            "type": "tool.result",
            "run_id": "run-a",
            "call_id": "corrected",
            "status": "succeeded",
            "output": None,
        }
    )
    bridge.record(
        _runtime_event(
            {
                "type": "message",
                "run_id": "run-a",
                "role": "user",
                "content": "missing ID",
            }
        )
    )
    bridge.record(
        _runtime_event(
            {
                "type": "model.request",
                "run_id": "run-a",
                "call_id": "malformed-model",
                "message_ids": 42,
                "tools": 42,
            }
        )
    )
    bridge.record(
        {
            "type": "model.response",
            "run_id": "run-a",
            "call_id": "malformed-model",
            "status": "completed",
            "content": None,
        }
    )
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "run-a",
            "call_id": "x" * 257,
            "name": "lookup",
            "input": None,
        }
    )
    bridge.record({"type": "run.outcome", "run_id": "run-a", "status": "completed"})

    records = _records(capture.shutdown().artifact_path)
    assert sum(record["kind"] == "tool.call" for record in records) == 1
    assert sum(record["kind"] == "tool.result" for record in records) == 1
    assert [record["data"]["reason"] for record in records if record["kind"] == "loss"] == [
        "invalid_tool_name",
        "tool_input_not_captured",
        "invalid_message_id",
        "invalid_message_ids",
        "invalid_tools",
        "invalid_call_id",
    ]


def test_custom_agent_keeps_model_errors_and_marks_missing_terminals(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)
    bridge.record({"type": "run.start", "run_id": "run-a", "name": "agent"})
    bridge.record({"type": "model.request", "run_id": "run-a", "call_id": "model-1"})
    bridge.record(
        {
            "type": "model.response",
            "run_id": "run-a",
            "call_id": "model-1",
            "status": "failed",
            "error": {
                "type": "provider_error",
                "message": "provider rejected the request",
                "recoverable": True,
            },
        }
    )
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "run-a",
            "call_id": "tool-1",
            "name": "read_file",
            "input": {"path": "README.md"},
        }
    )
    bridge.record({"type": "run.outcome", "run_id": "run-a", "status": "failed"})

    closed = capture.shutdown()
    records = _records(closed.artifact_path)
    error = next(record for record in records if record["kind"] == "error")
    assert error["data"] == {
        "type": "provider_error",
        "message": "provider rejected the request",
        "recoverable": True,
    }
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "tool_call_without_result"
    assert closed.losses == {"tool_call_without_result": 1}


def test_custom_agent_rejects_unknown_types_and_terminal_statuses_as_gaps(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)
    bridge.record({"type": "run.start", "run_id": "run-a", "name": "agent"})
    bridge.record({"type": "model.request", "run_id": "run-a", "call_id": "model-1"})
    assert bridge.record(
        _runtime_event(
            {
                "type": "model.response",
                "run_id": "run-a",
                "call_id": "model-1",
                "status": "surprising",
            }
        )
    ).accepted
    bridge.record(
        {
            "type": "model.response",
            "run_id": "run-a",
            "call_id": "model-1",
            "status": "completed",
            "content": None,
        }
    )
    bridge.record(
        {
            "type": "tool.call",
            "run_id": "run-a",
            "call_id": "tool-1",
            "name": "read_file",
            "input": {"path": "README.md"},
        }
    )
    assert bridge.record(
        _runtime_event(
            {
                "type": "future.tool.event",
                "run_id": "run-a",
                "call_id": "tool-1",
                "status": "succeeded",
            }
        )
    ).accepted
    assert bridge.record(
        _runtime_event(
            {
                "type": "tool.result",
                "run_id": "run-a",
                "call_id": "tool-1",
                "status": "surprising",
                "output": "not admitted",
            }
        )
    ).accepted
    assert bridge.record(
        {
            "type": "tool.result",
            "run_id": "run-a",
            "call_id": "tool-1",
            "status": "cancelled",
            "error": {
                "type": "cancelled",
                "message": "not a failure",
                "recoverable": False,
            },
        }
    ).accepted
    bridge.record(
        {
            "type": "tool.result",
            "run_id": "run-a",
            "call_id": "tool-1",
            "status": "cancelled",
        }
    )
    assert bridge.record(
        _runtime_event(
            {
                "type": "run.outcome",
                "run_id": "run-a",
                "status": "surprising",
            }
        )
    ).accepted
    assert bridge.record(
        {
            "type": "run.outcome",
            "run_id": "run-a",
            "status": "cancelled",
            "error": {
                "type": "cancelled",
                "message": "not a failure",
                "recoverable": False,
            },
        }
    ).accepted
    bridge.record({"type": "run.outcome", "run_id": "run-a", "status": "completed"})

    records = _records(capture.shutdown().artifact_path)
    losses = [record["data"]["reason"] for record in records if record["kind"] == "loss"]
    assert losses.count("invalid_status") == 3
    assert losses.count("contradictory_terminal_error") == 2
    assert losses.count("unknown_event_type") == 1
    assert len([record for record in records if record["kind"] == "tool.result"]) == 1


def test_custom_agent_records_missing_evidence_and_omits_invalid_reasoning(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)
    bridge.record({"type": "run.start", "run_id": "run-a", "name": "agent"})
    bridge.record(
        {
            "type": "message",
            "run_id": "run-a",
            "message_id": "message-1",
            "role": "tool",
            "content": "result",
            "call_id": "",
        }
    )
    bridge.record(
        {
            "type": "model.request",
            "run_id": "run-a",
            "call_id": "model-1",
            "message_ids": ["message-1"],
        }
    )
    bridge.record(
        _runtime_event(
            {
                "type": "model.response",
                "run_id": "run-a",
                "call_id": "model-1",
                "status": "completed",
                "reasoning": [{"type": "private", "text": "do not project"}],
            }
        )
    )
    bridge.record(
        {
            "type": "model.request",
            "run_id": "run-a",
            "call_id": "model-2",
        }
    )
    bridge.record(
        {
            "type": "model.response",
            "run_id": "run-a",
            "call_id": "model-2",
            "status": "incomplete",
            "content": None,
            "reasoning": [{"type": "summary", "text": "Observed summary"}],
        }
    )
    for call_id, include_output in (
        ("tool-missing-output", False),
        ("tool-null-output", True),
    ):
        bridge.record(
            {
                "type": "tool.call",
                "run_id": "run-a",
                "call_id": call_id,
                "name": "read_file",
                "input": {"path": "README.md"},
            }
        )
        result: dict[str, Any] = {
            "type": "tool.result",
            "run_id": "run-a",
            "call_id": call_id,
            "status": "succeeded",
        }
        if include_output:
            result["output"] = None
        bridge.record(result)  # type: ignore[arg-type]
    bridge.record({"type": "run.outcome", "run_id": "run-a", "status": "completed"})

    records = _records(capture.shutdown().artifact_path)
    losses = [record["data"]["reason"] for record in records if record["kind"] == "loss"]
    assert losses.count("invalid_call_id") == 1
    assert losses.count("model_content_not_captured") == 1
    assert losses.count("tool_output_not_captured") == 1
    assert losses.count("invalid_reasoning") == 1
    responses = [record for record in records if record["kind"] == "model.response"]
    assert "reasoning" not in responses[0]["data"]
    assert responses[1]["data"]["reasoning"] == [{"type": "summary", "text": "Observed summary"}]


def test_custom_agent_deactivation_marks_unobserved_run_terminal(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="custom-test")
    bridge = create_custom_agent_source(name="fixture-agent")
    capture.install_source(bridge.source)
    bridge.record({"type": "run.start", "run_id": "run-a", "name": "agent"})

    records = _records(capture.shutdown().artifact_path)
    outcome = next(record for record in records if record["kind"] == "run.outcome")
    assert outcome["data"]["status"] == "unknown"
    assert any(
        record["kind"] == "loss" and record["data"]["reason"] == "run_terminal_not_observed"
        for record in records
    )
