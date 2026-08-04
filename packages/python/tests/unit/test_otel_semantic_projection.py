from __future__ import annotations

import json
from pathlib import Path

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import Status, StatusCode

from semantic_layer import create_otel_source, initialize, reset_capture_for_tests
from semantic_layer.validation import validate_artifact


@pytest.fixture(autouse=True)
def reset_capture() -> None:
    reset_capture_for_tests()
    yield
    reset_capture_for_tests()


def test_current_genai_invoke_agent_tree_is_one_rich_run(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="otel-agent-fixture")
    source = create_otel_source(version="1.42.1")
    capture.install_source(source)
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(source.span_processor)
    tracer = provider.get_tracer(
        "genai-fixture",
        "1",
        "https://opentelemetry.io/schemas/gen-ai/1.42.0",
    )
    agent = tracer.start_span(
        "invoke_agent coding-agent",
        attributes={
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.agent.name": "coding-agent",
            "gen_ai.input.messages": json.dumps(
                [
                    {
                        "role": "user",
                        "parts": [{"type": "text", "content": "read README"}],
                    }
                ]
            ),
        },
    )
    agent_context = trace.set_span_in_context(agent)
    model = tracer.start_span(
        "chat fixture-model",
        context=agent_context,
        attributes={
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "fixture-model",
            "gen_ai.input.messages": json.dumps(
                [
                    {
                        "role": "user",
                        "parts": [{"type": "text", "content": "read README"}],
                    }
                ]
            ),
        },
    )
    model.set_attribute(
        "gen_ai.output.messages",
        json.dumps(
            [
                {
                    "role": "assistant",
                    "parts": [{"type": "text", "content": "using read_file"}],
                }
            ]
        ),
    )
    model.end()
    tool = tracer.start_span(
        "execute_tool read_file",
        context=agent_context,
        attributes={
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "read_file",
            "gen_ai.tool.call.id": "call-readme",
            "gen_ai.tool.call.arguments": '{"path":"README.md"}',
        },
    )
    tool.set_attribute("gen_ai.tool.call.result", '{"text":"contents"}')
    tool.end()
    child_agent = tracer.start_span(
        "invoke_agent reviewer",
        context=agent_context,
        attributes={
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.agent.name": "reviewer",
            "gen_ai.input.messages": json.dumps(
                [
                    {
                        "role": "user",
                        "parts": [{"type": "text", "content": "review contents"}],
                    }
                ]
            ),
        },
    )
    child_agent.set_attribute(
        "gen_ai.output.messages",
        json.dumps(
            [
                {
                    "role": "assistant",
                    "parts": [{"type": "text", "content": "looks good"}],
                }
            ]
        ),
    )
    child_agent.end()
    agent.set_attribute(
        "gen_ai.output.messages",
        json.dumps(
            [
                {
                    "role": "assistant",
                    "parts": [{"type": "text", "content": "done"}],
                }
            ]
        ),
    )
    agent.end()
    provider.force_flush()

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    rows = [json.loads(line) for line in (artifact / "trace.jsonl").read_text().splitlines()]
    assert [row["kind"] for row in rows] == [
        "run.start",
        "message",
        "model.request",
        "model.response",
        "tool.call",
        "tool.result",
        "scope",
        "message",
        "scope",
        "message",
        "run.outcome",
    ]
    root = rows[0]
    assert all(
        row["kind"] in {"message", "scope"} or row.get("parent") == root["id"] for row in rows[1:]
    )
    scopes = [row for row in rows if row["kind"] == "scope"]
    assert [row["data"]["phase"] for row in scopes] == ["start", "end"]
    assert scopes[1]["parent"] == scopes[0]["id"]
    report = validate_artifact(artifact, profile="rich-agent")
    assert report.valid, report.issues
    provider.shutdown()


def test_invoke_agent_input_added_before_end_has_no_false_gap(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="otel-late-input-fixture")
    source = create_otel_source(version="1.42.1")
    capture.install_source(source)
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(source.span_processor)
    tracer = provider.get_tracer(
        "genai-fixture",
        "1",
        "https://opentelemetry.io/schemas/gen-ai/1.42.0",
    )
    agent = tracer.start_span(
        "invoke_agent late-input",
        attributes={
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.agent.name": "late-input",
        },
    )
    agent.set_attribute(
        "gen_ai.input.messages",
        json.dumps(
            [
                {
                    "role": "user",
                    "parts": [{"type": "text", "content": "late but valid"}],
                }
            ]
        ),
    )
    agent.set_attribute(
        "gen_ai.output.messages",
        json.dumps(
            [
                {
                    "role": "assistant",
                    "parts": [{"type": "text", "content": "done"}],
                }
            ]
        ),
    )
    agent.end()
    provider.force_flush()

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    rows = [json.loads(line) for line in (artifact / "trace.jsonl").read_text().splitlines()]
    assert [row["kind"] for row in rows] == [
        "run.start",
        "message",
        "run.outcome",
    ]
    assert rows[1]["data"] == {
        "role": "user",
        "content": [{"type": "text", "content": "late but valid"}],
    }
    report = validate_artifact(artifact)
    assert report.valid, report.issues
    provider.shutdown()


def test_unrelated_root_does_not_steal_rich_agent_and_malformed_arrays_are_gaps(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="otel-owner-fixture")
    source = create_otel_source(version="1.42.1")
    capture.install_source(source)
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(source.span_processor)
    tracer = provider.get_tracer(
        "genai-fixture",
        "1",
        "https://opentelemetry.io/schemas/gen-ai/1.42.0",
    )
    ordinary = tracer.start_span("ordinary application root")
    agent = tracer.start_span(
        "invoke_agent worker",
        context=trace.set_span_in_context(ordinary),
        attributes={
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.agent.name": "worker",
            "gen_ai.input.messages": '"not an array"',
        },
    )
    agent.set_attribute("gen_ai.output.messages", '"also not an array"')
    agent.record_exception(ValueError("agent failed"))
    agent.set_status(Status(StatusCode.ERROR, "agent failed"))
    agent.end()
    ordinary.end()
    provider.force_flush()

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    rows = [json.loads(line) for line in (artifact / "trace.jsonl").read_text().splitlines()]
    assert [row["kind"] for row in rows] == [
        "run.start",
        "loss",
        "loss",
        "run.outcome",
    ]
    assert rows[0]["data"]["name"] == "worker"
    assert "input" not in rows[0]["data"]
    assert [row["data"]["reason"] for row in rows[1:3]] == [
        "agent_input_malformed",
        "agent_output_malformed",
    ]
    assert rows[-1]["data"]["status"] == "failed"
    assert rows[-1]["data"]["error"] == {
        "type": "value_error",
        "message": "agent failed",
        "recoverable": False,
    }
    assert "output" not in rows[-1]["data"]
    report = validate_artifact(artifact)
    assert report.valid, report.issues
    provider.shutdown()


def test_invoke_agent_without_exact_schema_stays_control_with_explicit_gap(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="otel-schema-fixture")
    source = create_otel_source(version="1.42.1")
    capture.install_source(source)
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(source.span_processor)
    for tracer in (
        provider.get_tracer("missing-schema"),
        provider.get_tracer(
            "other-schema",
            "1",
            "https://opentelemetry.io/schemas/1.27.0",
        ),
    ):
        span = tracer.start_span(
            "invoke_agent unsupported",
            attributes={
                "gen_ai.operation.name": "invoke_agent",
                "gen_ai.agent.name": "unsupported",
                "gen_ai.input.messages": "[]",
            },
        )
        span.set_attribute("gen_ai.output.messages", "[]")
        span.end()
    provider.force_flush()

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    rows = [json.loads(line) for line in (artifact / "trace.jsonl").read_text().splitlines()]
    assert [row["kind"] for row in rows] == ["loss", "loss"]
    assert [row["data"]["reason"] for row in rows] == [
        "missing_genai_schema",
        "unsupported_genai_schema",
    ]
    assert "https://opentelemetry.io/schemas/1.27.0" in rows[1]["data"]["detail"]
    assert not {
        "run.start",
        "run.outcome",
    }.intersection(row["kind"] for row in rows)
    report = validate_artifact(artifact)
    assert report.valid, report.issues
    provider.shutdown()


def test_late_model_and_tool_input_are_explicit_and_errors_are_structured(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="otel-terminal-fixture")
    source = create_otel_source(version="1.42.1")
    capture.install_source(source)
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(source.span_processor)
    tracer = provider.get_tracer("genai-fixture")

    model = tracer.start_span(
        "chat model",
        attributes={
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "fixture-model",
        },
    )
    model.set_attribute(
        "gen_ai.input.messages",
        json.dumps([{"role": "user", "parts": [{"type": "text", "content": "late"}]}]),
    )
    model.record_exception(RuntimeError("model failed"))
    model.set_status(Status(StatusCode.ERROR, "model failed"))
    model.end()

    failed_tool = tracer.start_span(
        "execute_tool failing",
        attributes={
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "failing",
            "gen_ai.tool.call.id": "call-failing",
            "gen_ai.tool.call.arguments": '{"value":1}',
        },
    )
    failed_tool.record_exception(ValueError("tool failed"))
    failed_tool.set_status(Status(StatusCode.ERROR, "tool failed"))
    failed_tool.end()

    late_tool = tracer.start_span(
        "execute_tool late",
        attributes={
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "late",
            "gen_ai.tool.call.id": "call-late",
        },
    )
    late_tool.set_attribute("gen_ai.tool.call.arguments", '{"value":2}')
    late_tool.set_attribute("gen_ai.tool.call.result", '{"ok":true}')
    late_tool.end()
    provider.force_flush()

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    rows = [json.loads(line) for line in (artifact / "trace.jsonl").read_text().splitlines()]
    losses = [row["data"]["reason"] for row in rows if row["kind"] == "loss"]
    assert "model_input_late_unlinked" in losses
    assert "tool_input_late_unprojected" not in losses
    assert "unmatched_tool_result" not in losses
    assert any(
        row["kind"] == "message" and row["data"]["content"] == [{"type": "text", "content": "late"}]
        for row in rows
    )
    model_response = next(row for row in rows if row["kind"] == "model.response")
    assert model_response["data"]["status"] == "failed"
    model_error = next(
        row for row in rows if row["kind"] == "error" and row["data"]["type"] == "runtime_error"
    )
    assert model_error["data"]["message"] == "model failed"
    assert model_error["parent"] == model_response["id"]
    tool_result = next(
        row
        for row in rows
        if row["kind"] == "tool.result" and row["data"]["native_call_id"] == "call-failing"
    )
    assert tool_result["data"]["status"] == "failed"
    assert tool_result["data"]["error"] == {
        "type": "value_error",
        "message": "tool failed",
        "recoverable": False,
    }
    late_call = next(
        row
        for row in rows
        if row["kind"] == "tool.call" and row["data"]["native_call_id"] == "call-late"
    )
    late_result = next(
        row
        for row in rows
        if row["kind"] == "tool.result" and row["data"]["native_call_id"] == "call-late"
    )
    assert late_call["data"]["input"] == {"value": 2}
    assert late_result["data"]["output"] == {"ok": True}
    assert {"type": "result_of", "record": late_call["id"]} in late_result["links"]
    report = validate_artifact(artifact)
    assert report.valid, report.issues
    provider.shutdown()


def test_unknown_genai_and_malformed_model_output_are_named_losses(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="otel-malformed-output-fixture")
    source = create_otel_source(version="1.42.1")
    capture.install_source(source)
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(source.span_processor)
    tracer = provider.get_tracer("genai-fixture")

    model = tracer.start_span(
        "chat malformed output",
        attributes={
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "fixture-model",
            "gen_ai.input.messages": json.dumps(
                [{"role": "user", "parts": [{"type": "text", "content": "hello"}]}]
            ),
        },
    )
    model.set_attribute("gen_ai.output.messages", '"not a message array"')
    model.end()
    tracer.start_span(
        "embeddings future operation",
        attributes={"gen_ai.operation.name": "embeddings"},
    ).end()
    malformed_tool_output = tracer.start_span(
        "execute_tool malformed output",
        attributes={
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "lookup",
            "gen_ai.tool.call.id": "call-malformed-output",
            "gen_ai.tool.call.arguments": '{"query":"hello"}',
        },
    )
    malformed_tool_output.set_attribute("gen_ai.tool.call.result", "not-json")
    malformed_tool_output.end()
    tracer.start_span(
        "execute_tool malformed input",
        attributes={
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "lookup",
            "gen_ai.tool.call.id": "call-malformed-input",
            "gen_ai.tool.call.arguments": "not-json",
            "gen_ai.tool.call.result": '{"ok":true}',
        },
    ).end()
    tracer.start_span(
        "execute_tool oversized ID",
        attributes={
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "lookup",
            "gen_ai.tool.call.id": "x" * 257,
            "gen_ai.tool.call.arguments": '{"query":"hello"}',
            "gen_ai.tool.call.result": '{"ok":true}',
        },
    ).end()
    typed_error = tracer.start_span(
        "chat typed error",
        attributes={
            "gen_ai.operation.name": "chat",
            "gen_ai.input.messages": json.dumps(
                [{"role": "user", "parts": [{"type": "text", "content": "fail"}]}]
            ),
            "error.type": "ProviderThrottle",
        },
    )
    typed_error.set_status(Status(StatusCode.ERROR, "rate limited"))
    typed_error.end()
    provider.force_flush()

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    rows = [json.loads(line) for line in (artifact / "trace.jsonl").read_text().splitlines()]
    response = next(row for row in rows if row["kind"] == "model.response")
    assert "content" not in response["data"]
    malformed_result = next(
        row
        for row in rows
        if row["kind"] == "tool.result"
        and row["data"].get("native_call_id") == "call-malformed-output"
    )
    assert "output" not in malformed_result["data"]
    assert any(
        row["kind"] == "error" and row["data"]["type"] == "provider_throttle" for row in rows
    )
    assert not any(row["data"].get("native_call_id") == "x" * 256 for row in rows)
    assert sorted(row["data"]["reason"] for row in rows if row["kind"] == "loss") == [
        "invalid_tool_call_id",
        "model_output_malformed",
        "tool_input_malformed",
        "tool_output_malformed",
        "unsupported_genai_operation",
    ]
    assert validate_artifact(artifact).valid
    provider.shutdown()


def test_generic_otel_is_additive_projects_exact_genai_and_bounds_losses(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="otel-bundle-fixture")
    source = create_otel_source(version="1.42.1")
    capture.install_source(source)

    application_exporter = InMemorySpanExporter()
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(SimpleSpanProcessor(application_exporter))
    provider.add_span_processor(source.span_processor)
    tracer = provider.get_tracer("fixture")
    root = tracer.start_span("application workflow")
    root_context = trace.set_span_in_context(root)

    user_parts = [{"type": "text", "content": "question"}]
    participant_name = "requester-" * 40
    bounded_participant = participant_name[:256]
    assistant_messages = [
        {
            "role": "assistant",
            "name": "fixture-agent",
            "parts": [{"type": "text", "content": "answer"}],
        },
        {
            "role": "assistant",
            "name": "fixture-agent-alternative",
            "parts": [{"type": "text", "content": "alternative answer"}],
        },
    ]
    model_span = tracer.start_span(
        "chat",
        context=root_context,
        attributes={
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "fixture-model",
            "gen_ai.input.messages": json.dumps(
                [
                    {
                        "role": "user",
                        "name": participant_name,
                        "parts": user_parts,
                    },
                    {
                        "role": "invalid",
                        "parts": [{"type": "text", "content": "discarded one"}],
                    },
                    {
                        "role": "invalid",
                        "parts": [{"type": "text", "content": "discarded two"}],
                    },
                    {"role": "user", "parts": "not structured"},
                ]
            ),
            "gen_ai.usage.input_tokens": 3,
            "gen_ai.usage.output_tokens": 2,
        },
    )
    model_span.set_attribute("gen_ai.output.messages", json.dumps(assistant_messages))
    # The terminal remains a model response even if mutable attributes later drift.
    model_span.set_attribute("gen_ai.operation.name", "invoke_agent")
    model_span.end()
    followup_parts = [{"type": "text", "content": "follow-up"}]
    cumulative_model_span = tracer.start_span(
        "chat cumulative",
        context=root_context,
        attributes={
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "fixture-model",
            "gen_ai.input.messages": json.dumps(
                [
                    {
                        "role": "user",
                        "name": participant_name,
                        "parts": user_parts,
                    },
                    *assistant_messages,
                    {
                        "role": "user",
                        "name": participant_name,
                        "parts": followup_parts,
                    },
                ]
            ),
        },
    )
    cumulative_model_span.set_attribute(
        "gen_ai.output.messages",
        json.dumps(
            [
                {
                    "role": "assistant",
                    "parts": [{"type": "text", "content": "follow-up answer"}],
                }
            ]
        ),
    )
    cumulative_model_span.end()
    tool_span = tracer.start_span(
        "execute_tool read_file",
        context=root_context,
        attributes={
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "read_file",
            "gen_ai.tool.call.id": "call-readme",
            "gen_ai.tool.call.arguments": '{"path":"README.md"}',
        },
    )
    tool_span.set_attribute("gen_ai.tool.call.result", '{"text":"contents"}')
    tool_span.end()
    tracer.start_span(
        "execute_tool without input",
        context=root_context,
        attributes={
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.tool.name": "lookup",
            "gen_ai.tool.call.id": "call-missing-input",
            "gen_ai.tool.call.result": '"not projected as a result"',
        },
    ).end()
    tracer.start_span(
        "agent invocation",
        context=root_context,
        attributes={
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.input.messages": '"agent input"',
            "gen_ai.output.messages": '"agent output"',
        },
    ).end()
    tracer.start_span("database query", context=root_context).end()
    routine_span_names = [
        f"{'database' if index % 2 == 0 else 'container'} {index}" for index in range(20)
    ]
    for name in routine_span_names:
        tracer.start_span(
            name,
            context=root_context,
            attributes={"service.component": "fixture"},
        ).end()
    root.end()

    class InvalidContext:
        trace_id = 0
        span_id = 0
        is_valid = False

    class InvalidOrdinarySpan:
        name = "invalid ordinary span"
        attributes: dict[str, str] = {}

        def get_span_context(self) -> InvalidContext:
            return InvalidContext()

    class InvalidGenAISpan:
        name = "invalid GenAI span"
        attributes = {"gen_ai.operation.name": "invoke_agent"}

        def get_span_context(self) -> InvalidContext:
            return InvalidContext()

    source.span_processor.on_start(InvalidOrdinarySpan())
    source.span_processor.on_start(InvalidGenAISpan())
    provider.force_flush()

    closed = capture.shutdown()
    artifact = Path(closed.artifact_path)
    before_late_span = (artifact / "trace.jsonl").read_bytes()
    tracer.start_span("application still owns provider").end()
    provider.force_flush()

    assert [span.name for span in application_exporter.get_finished_spans()] == [
        "chat",
        "chat cumulative",
        "execute_tool read_file",
        "execute_tool without input",
        "agent invocation",
        "database query",
        *routine_span_names,
        "application workflow",
        "application still owns provider",
    ]
    assert (artifact / "trace.jsonl").read_bytes() == before_late_span

    rows = [json.loads(line) for line in before_late_span.decode().splitlines() if line]
    manifest = json.loads((artifact / "manifest.json").read_text())
    messages = [row for row in rows if row["kind"] == "message"]
    requests = [row for row in rows if row["kind"] == "model.request"]
    responses = [row for row in rows if row["kind"] == "model.response"]
    message = messages[0]
    request = requests[0]
    response = responses[0]
    call = next(row for row in rows if row["kind"] == "tool.call")
    result = next(row for row in rows if row["kind"] == "tool.result")
    losses = [row for row in rows if row["kind"] == "loss"]

    assert len(rows) == 15
    assert len(messages) == 5
    assert message["origin"] == "context"
    assert message["data"] == {
        "role": "user",
        "name": bounded_participant,
        "content": user_parts,
    }
    assert message["seq"] < request["seq"]
    assert request["data"]["model"] == "fixture-model"
    assert request["data"]["context_refs"] == [message["id"]]
    assert requests[1]["data"]["context_refs"] == [
        messages[1]["id"],
        messages[2]["id"],
        messages[3]["id"],
        messages[4]["id"],
    ]
    assert len(set(requests[1]["data"]["context_refs"])) == 4
    assert messages[4]["data"] == {
        "role": "user",
        "name": bounded_participant,
        "content": followup_parts,
    }
    assert responses[1]["links"] == [{"type": "result_of", "record": requests[1]["id"]}]
    assert response["origin"] == "inferred"
    assert response["links"] == [{"type": "result_of", "record": request["id"]}]
    assert response["data"] == {
        "status": "completed",
        "model": "fixture-model",
        "content": assistant_messages,
        "usage": {"input_tokens": 3, "output_tokens": 2},
    }
    assert call["data"] == {
        "call_id": call["data"]["call_id"],
        "native_call_id": "call-readme",
        "name": "read_file",
        "input": {"path": "README.md"},
    }
    assert result["origin"] == "inferred"
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert result["data"] == {
        "call_id": call["data"]["call_id"],
        "native_call_id": "call-readme",
        "status": "succeeded",
        "output": {"text": "contents"},
    }
    assert len([row for row in rows if row["kind"] == "tool.call"]) == 1
    assert len([row for row in rows if row["kind"] == "tool.result"]) == 1
    assert sorted(row["data"]["reason"] for row in losses) == [
        "invalid_span_context",
        "missing_genai_schema",
        "model_input_messages_discarded",
        "tool_input_not_captured",
    ]
    assert len([row for row in losses if row["data"]["reason"] == "tool_input_not_captured"]) == 1
    model_input_losses = [
        row for row in losses if row["data"]["reason"] == "model_input_messages_discarded"
    ]
    assert len(model_input_losses) == 1
    assert model_input_losses[0]["data"]["count"] == 3
    assert all(row["data"]["count"] == 1 for row in losses if row is not model_input_losses[0])
    assert not [row for row in losses if "semantic.type=scope" in row["data"]["detail"]]
    assert len([row for row in rows if row["kind"] == "scope"]) == 0
    assert not {
        "run.start",
        "run.outcome",
        "state",
        "tool.proposal",
    }.intersection(row["kind"] for row in rows)
    assert manifest["state"] == "sealed"
    assert any(source["name"] == "generic:otel" for source in manifest["sources"])
    assert manifest["trace"]["path"] == "trace.jsonl"
    assert manifest["trace"]["records"] == len(rows)
    assert manifest["trace"]["losses"] == len(losses)
    report = validate_artifact(artifact)
    assert report.valid, report.issues
    provider.shutdown()
