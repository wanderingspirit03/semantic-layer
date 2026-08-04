from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker

os.environ["HAYSTACK_TELEMETRY_ENABLED"] = "false"
pytest.importorskip("haystack")

from haystack import Pipeline, component
from haystack import tracing as haystack_tracing
from haystack.components.generators.chat import OpenAIResponsesChatGenerator
from haystack.core.errors import PipelineRuntimeError
from haystack.dataclasses import ChatMessage
from haystack.tracing import OpenTelemetryTracer
from haystack.utils import Secret
from openai.types.responses import (
    Response,
    ResponseCompletedEvent,
    ResponseOutputItemAddedEvent,
    ResponseOutputItemDoneEvent,
    ResponseOutputMessage,
    ResponseOutputText,
    ResponseReasoningItem,
    ResponseReasoningSummaryTextDeltaEvent,
    ResponseTextDeltaEvent,
)
from openai.types.responses.response_reasoning_item import Summary
from opentelemetry.sdk.trace import TracerProvider

from semantic_layer import (
    create_otel_source,
    haystack_otel_adapter,
    initialize,
    reset_capture_for_tests,
)

_TRACE_SCHEMA_PATH = (
    Path(__file__).parents[4] / "contracts/trace/v1/semantic-trace-record.schema.json"
)
_TRACE_VALIDATOR = Draft202012Validator(
    json.loads(_TRACE_SCHEMA_PATH.read_text()),
    format_checker=FormatChecker(),
)


@pytest.fixture(autouse=True)
def _reset_capture() -> Any:
    yield
    reset_capture_for_tests()


def _trace_records(artifact: str) -> list[dict[str, Any]]:
    trace_path = Path(artifact) / "trace.jsonl"
    records = [json.loads(line) for line in trace_path.read_text().splitlines()]
    for record in records:
        _TRACE_VALIDATOR.validate(record)
    return records


def _one(records: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    return next(record for record in records if record["kind"] == kind)


def _openai_reasoning_response() -> Response:
    return Response.model_construct(
        id="response-haystack",
        created_at=0,
        model="fixture-reasoning-model",
        object="response",
        output=[
            ResponseReasoningItem(
                id="reasoning-haystack",
                type="reasoning",
                summary=[
                    Summary(type="summary_text", text="Inspect both records."),
                    Summary(type="summary_text", text="Inspect both records."),
                ],
                encrypted_content="opaque-reasoning-state",
                status="completed",
            ),
            ResponseOutputMessage(
                id="message-haystack",
                type="message",
                role="assistant",
                status="completed",
                content=[
                    ResponseOutputText(
                        type="output_text",
                        text="Use record two.",
                        annotations=[],
                        logprobs=[],
                    )
                ],
            ),
        ],
        status="completed",
        error=None,
        incomplete_details=None,
        instructions=None,
        metadata={},
        parallel_tool_calls=True,
        temperature=None,
        tool_choice="auto",
        tools=[],
        top_p=None,
    )


def _openai_reasoning_stream() -> list[Any]:
    response = _openai_reasoning_response()
    completed_reasoning = response.output[0]
    assert isinstance(completed_reasoning, ResponseReasoningItem)
    return [
        ResponseOutputItemAddedEvent(
            item=ResponseReasoningItem(
                id="reasoning-haystack",
                type="reasoning",
                summary=[],
                encrypted_content=None,
                status="in_progress",
            ),
            output_index=0,
            sequence_number=0,
            type="response.output_item.added",
        ),
        ResponseReasoningSummaryTextDeltaEvent(
            delta="Inspect both records.\n",
            item_id="reasoning-haystack",
            output_index=0,
            sequence_number=1,
            summary_index=0,
            type="response.reasoning_summary_text.delta",
        ),
        ResponseReasoningSummaryTextDeltaEvent(
            delta="Inspect both records.",
            item_id="reasoning-haystack",
            output_index=0,
            sequence_number=2,
            summary_index=1,
            type="response.reasoning_summary_text.delta",
        ),
        ResponseOutputItemDoneEvent(
            item=completed_reasoning,
            output_index=0,
            sequence_number=3,
            type="response.output_item.done",
        ),
        ResponseTextDeltaEvent(
            content_index=0,
            delta="Use record two.",
            item_id="message-haystack",
            logprobs=[],
            output_index=1,
            sequence_number=4,
            type="response.output_text.delta",
        ),
        ResponseCompletedEvent(
            response=response,
            sequence_number=5,
            type="response.completed",
        ),
    ]


@component
class _HaystackSemanticFixture:
    def __init__(self, tracer: Any) -> None:
        self._tracer = tracer

    @component.output_types(answer=str)
    def run(self, query: str) -> dict[str, str]:
        with self._tracer.start_as_current_span(
            "chat fixture",
            attributes={
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "fixture-model",
                "gen_ai.input.messages": json.dumps(
                    [
                        {
                            "role": "user",
                            "parts": [{"type": "text", "content": query}],
                        }
                    ]
                ),
            },
        ) as model:
            with self._tracer.start_as_current_span(
                "lookup fixture",
                attributes={
                    "gen_ai.operation.name": "execute_tool",
                    "gen_ai.tool.name": "lookup",
                    "gen_ai.tool.call.id": "call-haystack",
                    "gen_ai.tool.call.arguments": json.dumps(query),
                },
            ) as tool:
                tool.set_attribute("gen_ai.tool.call.result", json.dumps("found"))
            model.set_attribute("gen_ai.output.messages", json.dumps("done"))
            model.set_attribute("gen_ai.usage.input_tokens", 2)
            model.set_attribute("gen_ai.usage.output_tokens", 3)
        return {"answer": "done"}


@component
class _HaystackFailingFixture:
    def __init__(self, error: RuntimeError) -> None:
        self._error = error

    @component.output_types(answer=str)
    def run(self, query: str) -> dict[str, str]:
        del query
        raise self._error


@component
class _HaystackReasoningFixture:
    def __init__(self, tracer: Any) -> None:
        self._tracer = tracer

    @component.output_types(answer=str)
    def run(self, query: str) -> dict[str, str]:
        with self._tracer.start_as_current_span(
            "reasoning fixture",
            attributes={
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "fixture-reasoning-model",
                "gen_ai.input.messages": json.dumps(
                    [{"role": "user", "parts": [{"type": "text", "content": query}]}]
                ),
            },
        ) as model:
            model.set_attribute(
                "gen_ai.output.messages",
                json.dumps(
                    [
                        {
                            "role": "assistant",
                            "parts": [
                                {"type": "reasoning", "content": "Inspect both records."},
                                {"type": "reasoning", "content": "Inspect both records."},
                                {
                                    "type": "reasoning",
                                    "encrypted_content": "opaque-reasoning-state",
                                },
                                {"type": "text", "content": "Use record two."},
                            ],
                        }
                    ]
                ),
            )
        return {"answer": "Use record two."}


def test_haystack_otel_route_projects_pipeline_component_model_and_tool(
    tmp_path: Path,
) -> None:
    provider = TracerProvider(shutdown_on_exit=False)
    source = haystack_otel_adapter(version="2.31.0")
    provider.add_span_processor(source.span_processor)
    pipeline = Pipeline()
    pipeline.add_component(
        "agent",
        _HaystackSemanticFixture(provider.get_tracer("haystack-semantic")),
    )
    prior_tracer = haystack_tracing.tracer.actual_tracer

    capture = initialize(output=tmp_path, service_name="haystack-semantic")
    capture.install_source(source)
    try:
        haystack_tracing.enable_tracing(
            OpenTelemetryTracer(provider.get_tracer("haystack-semantic"))
        )
        assert pipeline.run({"agent": {"query": "lookup"}}) == {"agent": {"answer": "done"}}
        provider.force_flush()
    finally:
        haystack_tracing.enable_tracing(prior_tracer)

    artifact_path = capture.shutdown().artifact_path
    records = _trace_records(artifact_path)
    provider.shutdown()
    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == ["pipeline_output_not_captured"]
    assert [record["kind"] for record in records].count("scope") == 2

    model_request = _one(records, "model.request")
    model_response = _one(records, "model.response")
    message = _one(records, "message")
    assert message["origin"] == "context"
    assert message["data"] == {
        "role": "user",
        "content": [{"type": "text", "content": "lookup"}],
    }
    assert model_request["data"] == {
        "context_refs": [message["id"]],
        "model": "fixture-model",
    }
    assert model_response["links"] == [{"type": "result_of", "record": model_request["id"]}]
    assert model_response["data"]["content"] == "done"
    assert model_response["data"]["usage"] == {
        "input_tokens": 2,
        "output_tokens": 3,
    }

    call = _one(records, "tool.call")
    result = _one(records, "tool.result")
    assert call["data"]["native_call_id"] == "call-haystack"
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert result["data"]["output"] == "found"

    pipeline_state = next(
        record
        for record in records
        if record["kind"] == "state" and record["data"]["type"] == "state.pipeline_io"
    )
    assert pipeline_state["data"]["value"]["input"] == {"agent": {"query": "lookup"}}
    assert "output" not in pipeline_state["data"]["value"]
    assert _one(records, "run.outcome")["data"]["status"] == "completed"
    manifest = json.loads((Path(artifact_path) / "manifest.json").read_text())
    source_metadata = next(
        source
        for source in manifest["sources"]
        if source["name"] not in {"manual", "semantic-layer-runtime"}
    )
    assert source_metadata == {
        "id": source_metadata["id"],
        "name": "official:haystack-otel",
        "seam": "OpenTelemetry TracerProvider/span processor",
        "version": "haystack-ai=2.31.0;opentelemetry-sdk=1.42.1",
    }


def test_haystack_failed_pipeline_preserves_error_without_output_loss(
    tmp_path: Path,
) -> None:
    expected = RuntimeError("fixture exploded")
    provider = TracerProvider(shutdown_on_exit=False)
    source = haystack_otel_adapter(version="2.31.0")
    provider.add_span_processor(source.span_processor)
    pipeline = Pipeline()
    pipeline.add_component("agent", _HaystackFailingFixture(expected))
    prior_tracer = haystack_tracing.tracer.actual_tracer

    capture = initialize(output=tmp_path, service_name="haystack-failure-semantic")
    capture.install_source(source)
    try:
        haystack_tracing.enable_tracing(
            OpenTelemetryTracer(provider.get_tracer("haystack-failure-semantic"))
        )
        with pytest.raises(PipelineRuntimeError) as caught:
            pipeline.run({"agent": {"query": "fail"}})
        assert caught.value.__cause__ is expected
        provider.force_flush()
    finally:
        haystack_tracing.enable_tracing(prior_tracer)

    records = _trace_records(capture.shutdown().artifact_path)
    provider.shutdown()
    outcome = _one(records, "run.outcome")
    assert outcome["data"]["status"] == "failed"
    assert outcome["data"]["error"] == {
        "type": "haystack.core.errors.pipeline_runtime_error",
        "message": str(caught.value),
        "recoverable": False,
    }
    assert "pipeline_output_not_captured" not in {
        record["data"]["reason"] for record in records if record["kind"] == "loss"
    }


def test_haystack_content_tracing_retains_uncorrelated_generator_reasoning_as_state(
    tmp_path: Path,
) -> None:
    provider = TracerProvider(shutdown_on_exit=False)
    source = haystack_otel_adapter(version="2.31.0")
    provider.add_span_processor(source.span_processor)
    observed_chunks: list[Any] = []
    generator = OpenAIResponsesChatGenerator(
        api_key=Secret.from_token("fixture-api-key"),
        model="fixture-reasoning-model",
        streaming_callback=observed_chunks.append,
    )
    generator.warm_up()
    generator.client = SimpleNamespace(
        responses=SimpleNamespace(create=lambda **_kwargs: _openai_reasoning_stream())
    )
    pipeline = Pipeline()
    pipeline.add_component("generator", generator)
    prior_tracer = haystack_tracing.tracer.actual_tracer
    prior_content_tracing = haystack_tracing.tracer.is_content_tracing_enabled
    capture = initialize(output=tmp_path, service_name="haystack-native-reasoning")
    capture.install_source(source)
    try:
        haystack_tracing.enable_tracing(
            OpenTelemetryTracer(provider.get_tracer("haystack-native-reasoning"))
        )
        haystack_tracing.tracer.is_content_tracing_enabled = True
        result = pipeline.run(
            {"generator": {"messages": [ChatMessage.from_user("Choose a record.")]}}
        )
        assert result["generator"]["replies"][0].text == "Use record two."
        assert [
            chunk.reasoning.reasoning_text
            for chunk in observed_chunks
            if chunk.reasoning and chunk.reasoning.reasoning_text
        ] == ["Inspect both records.\n", "Inspect both records."]
        provider.force_flush()
    finally:
        haystack_tracing.tracer.is_content_tracing_enabled = prior_content_tracing
        haystack_tracing.enable_tracing(prior_tracer)

    artifact_path = capture.shutdown().artifact_path
    records = _trace_records(artifact_path)
    provider.shutdown()
    assert not [record for record in records if record["kind"] == "model.response"]
    generator_state = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"]["type"] == "state.haystack_generator_output"
    )
    assert generator_state["data"]["value"]["replies"][0]["content"][0]["reasoning"][
        "reasoning_text"
    ] == "Inspect both records.\nInspect both records."
    assert (
        generator_state["data"]["value"]["replies"][0]["content"][0]["reasoning"][
            "extra"
        ]["encrypted_content"]
        == "opaque-reasoning-state"
    )
    correlation_losses = [
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"]
        == "haystack_generator_reasoning_model_correlation_unavailable"
    ]
    assert len(correlation_losses) == 1
    assert correlation_losses[0]["parent"] == generator_state["id"]
    availability_losses = [
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"] == "unsupported_native_value"
    ]
    assert len(availability_losses) == 1
    assert availability_losses[0]["parent"] == generator_state["id"]
    assert "remains native evidence" in availability_losses[0]["data"]["detail"]


def test_haystack_prefers_exact_genai_model_identity_over_generator_state(
    tmp_path: Path,
) -> None:
    provider = TracerProvider(shutdown_on_exit=False)
    source = haystack_otel_adapter(version="2.31.0")
    provider.add_span_processor(source.span_processor)
    model_tracer = provider.get_tracer("haystack-exact-model")

    def model_response(**_kwargs: Any) -> list[Any]:
        with model_tracer.start_as_current_span(
            "chat fixture",
            attributes={
                "gen_ai.operation.name": "chat",
                "gen_ai.request.model": "fixture-reasoning-model",
                "gen_ai.input.messages": json.dumps(
                    [{"role": "user", "parts": [{"type": "text", "content": "Choose."}]}]
                ),
            },
        ) as span:
            span.set_attribute(
                "gen_ai.output.messages",
                json.dumps(
                    [
                        {
                            "role": "assistant",
                            "parts": [
                                {"type": "reasoning", "content": "Inspect records."},
                                {"type": "text", "content": "Use record two."},
                            ],
                        }
                    ]
                ),
            )
        return _openai_reasoning_stream()

    generator = OpenAIResponsesChatGenerator(
        api_key=Secret.from_token("fixture-api-key"),
        model="fixture-reasoning-model",
        streaming_callback=lambda _chunk: None,
    )
    generator.warm_up()
    generator.client = SimpleNamespace(
        responses=SimpleNamespace(create=model_response)
    )
    pipeline = Pipeline()
    pipeline.add_component("generator", generator)
    prior_tracer = haystack_tracing.tracer.actual_tracer
    prior_content_tracing = haystack_tracing.tracer.is_content_tracing_enabled
    capture = initialize(output=tmp_path, service_name="haystack-exact-model")
    capture.install_source(source)
    try:
        haystack_tracing.enable_tracing(
            OpenTelemetryTracer(provider.get_tracer("haystack-exact-model"))
        )
        haystack_tracing.tracer.is_content_tracing_enabled = True
        pipeline.run(
            {"generator": {"messages": [ChatMessage.from_user("Choose a record.")]}}
        )
        provider.force_flush()
    finally:
        haystack_tracing.tracer.is_content_tracing_enabled = prior_content_tracing
        haystack_tracing.enable_tracing(prior_tracer)

    records = _trace_records(capture.shutdown().artifact_path)
    provider.shutdown()
    response = _one(records, "model.response")
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect records."}
    ]
    assert not [
        record
        for record in records
        if record["kind"] == "state"
        and record["data"]["type"] == "state.haystack_generator_output"
    ]
    assert "haystack_generator_reasoning_model_correlation_unavailable" not in {
        record["data"]["reason"] for record in records if record["kind"] == "loss"
    }


def test_haystack_otel_preserves_explicit_reasoning_parts_in_output_order(
    tmp_path: Path,
) -> None:
    provider = TracerProvider(shutdown_on_exit=False)
    source = haystack_otel_adapter(version="2.31.0")
    provider.add_span_processor(source.span_processor)
    pipeline = Pipeline()
    pipeline.add_component(
        "agent",
        _HaystackReasoningFixture(provider.get_tracer("haystack-reasoning")),
    )
    prior_tracer = haystack_tracing.tracer.actual_tracer
    capture = initialize(output=tmp_path, service_name="haystack-reasoning")
    capture.install_source(source)
    try:
        haystack_tracing.enable_tracing(OpenTelemetryTracer(provider.get_tracer("haystack")))
        assert pipeline.run({"agent": {"query": "choose"}}) == {
            "agent": {"answer": "Use record two."}
        }
        provider.force_flush()
    finally:
        haystack_tracing.enable_tracing(prior_tracer)

    records = _trace_records(capture.shutdown().artifact_path)
    provider.shutdown()
    response = _one(records, "model.response")
    assert response["data"]["content"] == [
        {
            "role": "assistant",
            "parts": [{"type": "text", "content": "Use record two."}],
        }
    ]
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect both records."},
        {"type": "text", "text": "Inspect both records."},
    ]
    losses = [
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"] == "unsupported_native_value"
    ]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "unsupported_native_value"
    assert losses[0]["parent"] == response["id"]
    assert "opaque-reasoning-state" not in json.dumps(losses[0])
    assert "native snapshot" in losses[0]["data"]["detail"]


def test_generic_otel_keeps_reasoning_out_of_visible_output_messages(
    tmp_path: Path,
) -> None:
    provider = TracerProvider(shutdown_on_exit=False)
    source = create_otel_source(version="1.42.1")
    provider.add_span_processor(source.span_processor)
    capture = initialize(output=tmp_path, service_name="generic-otel-reasoning")
    capture.install_source(source)
    tracer = provider.get_tracer("generic-otel-reasoning")
    with tracer.start_as_current_span(
        "chat fixture",
        attributes={
            "gen_ai.operation.name": "chat",
            "gen_ai.request.model": "fixture-model",
            "gen_ai.input.messages": json.dumps(
                [{"role": "user", "parts": [{"type": "text", "content": "Choose."}]}]
            ),
        },
    ) as span:
        span.set_attribute(
            "gen_ai.output.messages",
            json.dumps(
                [
                    {
                        "role": "assistant",
                        "parts": [
                            {"type": "reasoning", "content": "Inspect."},
                            {"type": "reasoning", "content": "Inspect."},
                            {
                                "type": "reasoning",
                                "encrypted_content": "opaque-provider-state",
                            },
                            {"type": "text", "content": "Use record two."},
                        ],
                    }
                ]
            ),
        )
    provider.force_flush()

    records = _trace_records(capture.shutdown().artifact_path)
    provider.shutdown()
    response = _one(records, "model.response")
    assert response["data"]["content"] == [
        {
            "role": "assistant",
            "parts": [{"type": "text", "content": "Use record two."}],
        }
    ]
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect."},
        {"type": "text", "text": "Inspect."},
    ]
    assert "opaque-provider-state" not in json.dumps(response["data"])
