from __future__ import annotations

import asyncio
import gc
import json
import weakref
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from google.adk.agents import LlmAgent
from google.adk.models.llm_response import LlmResponse
from google.adk.plugins import BasePlugin
from google.adk.runners import InMemoryRunner
from google.adk.tools import ToolContext
from google.genai import types as genai_types
from google.genai.errors import ServerError
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel
from pydantic_ai import Agent as PydanticAgent
from pydantic_ai import ModelRetry, RunContext
from pydantic_ai.capabilities.abstract import AbstractCapability
from pydantic_ai.messages import (
    CompactionPart,
    ModelResponse,
    RetryPromptPart,
    TextPart,
    ThinkingPart,
    ToolCallPart,
)
from pydantic_ai.models.function import DeltaThinkingPart, FunctionModel
from pydantic_ai.models.test import TestModel

from semantic_layer import (
    google_adk_adapter,
    initialize,
    pydantic_ai_adapter,
    reset_capture_for_tests,
)
from semantic_layer.capture_v1 import AdmissionReceipt, OpenTraceReceipt
from tests.unit.test_framework_adapters import (
    GOOGLE_ADK_VERSION,
    PYDANTIC_AI_VERSION,
    _FixtureADKModel,
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


def _project(artifact_path: str) -> list[dict[str, Any]]:
    records = [
        json.loads(line) for line in (Path(artifact_path) / "trace.jsonl").read_text().splitlines()
    ]
    for record in records:
        _TRACE_VALIDATOR.validate(record)
    return records


def _assert_exact_pairs(records: list[dict[str, Any]], kind: str) -> None:
    starts = [record for record in records if record["kind"] == kind]
    terminal_kind = "model.response" if kind == "model.request" else "tool.result"
    terminals = [record for record in records if record["kind"] == terminal_kind]
    starts_by_id = {
        record["data"]["call_id"]: record["id"] for record in starts if kind == "tool.call"
    }
    if kind == "model.request":
        start_ids = {record["id"] for record in starts}
        assert all(
            terminal["links"]
            == [
                {
                    "type": "result_of",
                    "record": terminal["links"][0]["record"],
                }
            ]
            and terminal["links"][0]["record"] in start_ids
            for terminal in terminals
        )
    else:
        assert all(
            terminal["links"]
            == [
                {
                    "type": "result_of",
                    "record": starts_by_id[terminal["data"]["call_id"]],
                }
            ]
            for terminal in terminals
        )
    assert len(starts) == len(terminals)


def _contains_none(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, dict):
        return any(_contains_none(child) for child in value.values())
    if isinstance(value, list):
        return any(_contains_none(child) for child in value)
    return False


async def test_pydantic_ai_capture_projects_one_clear_semantic_trace(
    tmp_path: Path,
) -> None:
    agent = PydanticAgent(TestModel(call_tools="all"))

    @agent.tool_plain
    def lookup(value: str) -> str:
        return f"ok:{value}"

    capture = initialize(output=tmp_path, service_name="pydantic-semantic-app")
    capture.instrument(
        adapter=pydantic_ai_adapter(version=PYDANTIC_AI_VERSION),
        client=agent,
    )
    result = await agent.run(
        "lookup",
        conversation_id="conversation-1",
    )
    assert "ok:a" in result.output

    records = _project(capture.shutdown().artifact_path)
    assert not [record for record in records if record["kind"] == "loss"]
    assert [record["kind"] for record in records].count("run.start") == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    assert [record["kind"] for record in records].count("tool.proposal") == 1
    assert records[-1]["kind"] == "run.outcome"
    assert records[-1]["data"] == {"status": "completed"}
    _assert_exact_pairs(records, "model.request")
    _assert_exact_pairs(records, "tool.call")
    requests = [record for record in records if record["kind"] == "model.request"]
    context = {
        record["id"]: record
        for record in records
        if record["kind"] in {"message", "model.response", "tool.result"}
    }
    assert [
        [context[record_id]["data"]["role"] for record_id in request["data"]["context_refs"]]
        for request in requests
    ] == [["user"], ["user", "assistant", "tool"]]
    assert [
        context[record_id]["data"]["content"]["content"]
        for record_id in requests[0]["data"]["context_refs"]
    ] == ["lookup"]
    assert requests[0]["data"]["tool_definitions"] == [
        {
            "description": None,
            "kind": "function",
            "name": "lookup",
            "parameters_json_schema": {
                "additionalProperties": False,
                "properties": {"value": {"type": "string"}},
                "required": ["value"],
                "type": "object",
            },
        }
    ]
    assert requests[0]["data"]["settings"] == {
        "allow_image_output": False,
        "allow_text_output": True,
        "model_settings": {},
        "output_mode": "text",
    }
    responses = [record for record in records if record["kind"] == "model.response"]
    assert all(response["data"]["usage"] for response in responses)
    assert not [
        record
        for record in records
        if record["kind"] == "state" and record["data"]["type"] == "run.result"
    ]


async def test_pydantic_ai_preserves_exposed_thinking_and_readable_compaction_order(
    tmp_path: Path,
) -> None:
    def respond(_messages: list[Any], _info: Any) -> ModelResponse:
        return ModelResponse(
            parts=[
                ThinkingPart("Inspect both records."),
                ThinkingPart("Inspect both records."),
                ThinkingPart("", signature="opaque-thinking-signature"),
                CompactionPart("Earlier research favored record one."),
                TextPart("Use record two."),
            ],
            model_name="fixture-reasoning-model",
        )

    agent = PydanticAgent(FunctionModel(respond))
    capture = initialize(output=tmp_path, service_name="pydantic-reasoning-app")
    capture.instrument(
        adapter=pydantic_ai_adapter(version="2.9.0"),
        client=agent,
    )

    result = await agent.run("choose the current record")
    assert result.output == "Use record two."

    records = _project(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert [part["part_kind"] for part in response["data"]["content"]] == ["text"]
    assert [part["content"] for part in response["data"]["content"]] == ["Use record two."]
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect both records."},
        {"type": "text", "text": "Inspect both records."},
        {"type": "summary", "text": "Earlier research favored record one."},
    ]
    assert not [record for record in records if record["kind"] == "loss"]


async def test_pydantic_ai_run_stream_keeps_root_open_and_projects_reasoning(
    tmp_path: Path,
) -> None:
    class FinalOmitsThinkingModel(FunctionModel):
        @asynccontextmanager
        async def request_stream(self, *args: Any, **kwargs: Any) -> Any:
            async with super().request_stream(*args, **kwargs) as streamed:
                original_get = streamed.get

                def get_without_thinking() -> ModelResponse:
                    response = original_get()
                    response.parts = [
                        part for part in response.parts if part.part_kind != "thinking"
                    ]
                    return response

                streamed.get = get_without_thinking
                yield streamed

    async def respond(_messages: list[Any], _info: Any) -> Any:
        yield {0: DeltaThinkingPart(content="Inspect ")}
        yield {0: DeltaThinkingPart(content="records.")}
        yield "Use record two."

    agent = PydanticAgent(FinalOmitsThinkingModel(stream_function=respond))
    capture = initialize(output=tmp_path, service_name="pydantic-stream-reasoning-app")
    capture.instrument(adapter=pydantic_ai_adapter(version="2.9.0"), client=agent)

    async with agent.run_stream("choose the current record") as result:
        assert await result.get_output() == "Use record two."

    records = _project(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records].count("run.start") == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    response = next(record for record in records if record["kind"] == "model.response")
    assert [part["part_kind"] for part in response["data"]["content"]] == ["text"]
    assert response["data"]["reasoning"] == [{"type": "text", "text": "Inspect records."}]


def test_pydantic_ai_run_stream_sync_keeps_root_open_through_completion(
    tmp_path: Path,
) -> None:
    async def respond(_messages: list[Any], _info: Any) -> Any:
        yield {0: DeltaThinkingPart(content="Check evidence.")}
        yield "Done."

    agent = PydanticAgent(FunctionModel(stream_function=respond))
    capture = initialize(output=tmp_path, service_name="pydantic-sync-stream-app")
    capture.instrument(adapter=pydantic_ai_adapter(version="2.9.0"), client=agent)

    with agent.run_stream_sync("check") as result:
        assert result.get_output() == "Done."

    records = _project(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records].count("run.start") == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["reasoning"] == [{"type": "text", "text": "Check evidence."}]


def test_pydantic_ai_run_stream_sync_retains_thinking_when_consumer_is_cancelled(
    tmp_path: Path,
) -> None:
    async def respond(_messages: list[Any], _info: Any) -> Any:
        yield {0: DeltaThinkingPart(content="Check before cancellation.")}
        yield "Done."
        await asyncio.Event().wait()

    agent = PydanticAgent(FunctionModel(stream_function=respond))
    capture = initialize(output=tmp_path, service_name="pydantic-sync-cancelled-stream-app")
    capture.instrument(adapter=pydantic_ai_adapter(version="2.9.0"), client=agent)

    with pytest.raises(asyncio.CancelledError):
        with agent.run_stream_sync("check"):
            raise asyncio.CancelledError

    records = _project(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records].count("run.start") == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 1
    assert responses[0]["data"]["status"] == "cancelled"
    assert responses[0]["data"]["reasoning"] == [
        {"type": "text", "text": "Check before cancellation."}
    ]


async def test_pydantic_ai_direct_iter_keeps_root_open_through_completion(
    tmp_path: Path,
) -> None:
    def respond(_messages: list[Any], _info: Any) -> ModelResponse:
        return ModelResponse(parts=[ThinkingPart("Follow the evidence."), TextPart("Done.")])

    agent = PydanticAgent(FunctionModel(respond))
    capture = initialize(output=tmp_path, service_name="pydantic-direct-iter-app")
    capture.instrument(adapter=pydantic_ai_adapter(version="2.9.0"), client=agent)

    async with agent.iter("check") as run:
        async for _node in run:
            pass
        assert run.result is not None
        assert run.result.output == "Done."

    records = _project(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records].count("run.start") == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["reasoning"] == [{"type": "text", "text": "Follow the evidence."}]


async def test_pydantic_ai_run_stream_closes_root_when_consumer_is_cancelled(
    tmp_path: Path,
) -> None:
    async def respond(_messages: list[Any], _info: Any) -> Any:
        yield {0: DeltaThinkingPart(content="Check before cancellation.")}
        yield "Done."
        await asyncio.Event().wait()

    agent = PydanticAgent(FunctionModel(stream_function=respond))
    capture = initialize(output=tmp_path, service_name="pydantic-cancelled-stream-app")
    capture.instrument(adapter=pydantic_ai_adapter(version="2.9.0"), client=agent)
    entered = asyncio.Event()

    async def consume() -> None:
        async with agent.run_stream("check"):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(consume())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    records = _project(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records].count("run.start") == 1
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "failed"
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 1
    assert responses[0]["data"]["status"] == "cancelled"
    assert responses[0]["data"]["reasoning"] == [
        {"type": "text", "text": "Check before cancellation."}
    ]


async def test_pydantic_ai_run_stream_prepare_failure_does_not_poison_next_run(
    tmp_path: Path,
) -> None:
    async def respond(_messages: list[Any], _info: Any) -> Any:
        yield "Done."

    agent = PydanticAgent(FunctionModel(stream_function=respond))
    capture = initialize(output=tmp_path, service_name="pydantic-prepare-failure-app")
    capture.instrument(adapter=pydantic_ai_adapter(version="2.9.0"), client=agent)

    with pytest.raises(TypeError):
        async with agent.run_stream("invalid", capabilities=object()):  # type: ignore[arg-type]
            pass

    async with agent.run_stream("valid") as result:
        assert await result.get_output() == "Done."

    records = _project(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records].count("run.start") == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    assert [record["kind"] for record in records].count("model.response") == 1


async def test_pydantic_ai_mixed_role_request_preserves_each_context_part(
    tmp_path: Path,
) -> None:
    agent = PydanticAgent(TestModel(), system_prompt="system-rule")
    capture = initialize(output=tmp_path, service_name="pydantic-mixed-context-app")
    capture.instrument(
        adapter=pydantic_ai_adapter(version=PYDANTIC_AI_VERSION),
        client=agent,
    )

    result = await agent.run("user-task", conversation_id="conversation-1")
    assert result.output == "success (no tool calls)"

    records = _project(capture.shutdown().artifact_path)
    request = next(record for record in records if record["kind"] == "model.request")
    context = {record["id"]: record for record in records if record["kind"] == "message"}
    messages = [context[record_id] for record_id in request["data"]["context_refs"]]
    assert [message["data"]["role"] for message in messages] == ["system", "user"]
    assert [message["data"]["content"]["part_kind"] for message in messages] == [
        "system-prompt",
        "user-prompt",
    ]
    assert [message["data"]["content"]["content"] for message in messages] == [
        "system-rule",
        "user-task",
    ]
    assert not [record for record in records if record["kind"] == "loss"]


async def test_pydantic_ai_unknown_context_part_is_omitted_with_one_named_loss(
    tmp_path: Path,
) -> None:
    class FuturePartCapability(AbstractCapability[Any]):
        async def before_model_request(self, ctx: Any, request_context: Any) -> Any:
            del ctx
            for index, part in enumerate(request_context.messages[0].parts):
                object.__setattr__(part, "part_kind", f"future-prompt-{index}")
            return request_context

    agent = PydanticAgent(TestModel(), system_prompt="system-rule")
    capture = initialize(output=tmp_path, service_name="pydantic-future-context-app")
    capture.instrument(
        adapter=pydantic_ai_adapter(version=PYDANTIC_AI_VERSION),
        client=agent,
    )

    result = await agent.run(
        "user-task",
        conversation_id="conversation-1",
        capabilities=[FuturePartCapability()],
    )
    assert result.output == "success (no tool calls)"

    records = _project(capture.shutdown().artifact_path)
    request = next(record for record in records if record["kind"] == "model.request")
    losses = [record for record in records if record["kind"] == "loss"]
    assert "context_refs" not in request["data"]
    assert not [record for record in records if record["kind"] == "message"]
    assert [loss["data"]["reason"] for loss in losses] == ["model_context_part_not_captured"]
    assert losses[0]["data"]["count"] == 2
    assert len(losses[0]["data"]["detail"]) <= 4096


async def test_pydantic_ai_retry_remains_visible_without_duplicate_noise(
    tmp_path: Path,
) -> None:
    agent = PydanticAgent(TestModel(call_tools="all"))
    attempts = 0

    @agent.tool_plain
    def unstable(value: str) -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ModelRetry("retry fixture")
        return f"recovered:{value}"

    capture = initialize(output=tmp_path, service_name="pydantic-retry-semantic-app")
    capture.instrument(
        adapter=pydantic_ai_adapter(version=PYDANTIC_AI_VERSION),
        client=agent,
    )
    result = await agent.run("recover", conversation_id="conversation-1")
    assert "recovered:a" in result.output

    records = _project(capture.shutdown().artifact_path)
    assert not [record for record in records if record["kind"] == "loss"]
    tool_results = [record for record in records if record["kind"] == "tool.result"]
    assert [record["data"]["status"] for record in tool_results] == [
        "failed",
        "succeeded",
    ]
    assert tool_results[0]["data"]["error"]["recoverable"] is True
    assert any(
        record["kind"] == "state" and record["data"]["type"] == "recovery.retry"
        for record in records
    )
    assert records[-1]["kind"] == "run.outcome"
    assert records[-1]["data"]["status"] == "completed"


async def test_pydantic_ai_tool_validation_retry_preserves_model_visible_content(
    tmp_path: Path,
) -> None:
    agent = PydanticAgent(TestModel(call_tools="all"))
    attempts: list[int] = []

    def reject_once(ctx: RunContext[None], value: str) -> None:
        del value
        attempts.append(ctx.retry)
        if len(attempts) == 1:
            raise ModelRetry("retry fixture")

    @agent.tool_plain(args_validator=reject_once, retries=2)
    def unstable(value: str) -> str:
        return f"recovered:{value}"

    capture = initialize(output=tmp_path, service_name="pydantic-validator-retry-app")
    capture.instrument(
        adapter=pydantic_ai_adapter(version=PYDANTIC_AI_VERSION),
        client=agent,
    )

    result = await agent.run("recover", conversation_id="conversation-1")
    assert "recovered:a" in result.output
    assert attempts == [0, 1]

    records = _project(capture.shutdown().artifact_path)
    retry = next(
        record
        for record in records
        if record["kind"] == "message"
        and record["data"]["role"] == "tool"
        and record["data"].get("name") == "unstable"
    )
    expected_content = RetryPromptPart(
        "retry fixture",
        tool_name="unstable",
        tool_call_id=retry["data"]["call_id"],
    ).model_response()
    assert retry["data"]["content"] == expected_content
    assert retry["data"]["content"] == "retry fixture\n\nFix the errors and try again."
    assert [record["kind"] for record in records].count("tool.proposal") == 2
    assert [record["kind"] for record in records].count("tool.call") == 1
    assert [record["kind"] for record in records].count("tool.result") == 1


async def test_pydantic_ai_structured_retry_preserves_model_visible_content(
    tmp_path: Path,
) -> None:
    class Answer(BaseModel):
        city: str
        country: str

    attempts = 0
    model_visible_retry: RetryPromptPart | None = None

    def respond(messages: list[Any], info: Any) -> ModelResponse:
        nonlocal attempts, model_visible_retry
        attempts += 1
        output_tool = info.output_tools[0].name
        if attempts == 1:
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        output_tool,
                        '{"city": "Chicago", "country": "United States"}, "type": "name_only"',
                        tool_call_id="typed-output-1",
                    )
                ]
            )
        model_visible_retry = next(
            part for message in messages for part in message.parts if type(part) is RetryPromptPart
        )
        return ModelResponse(
            parts=[
                ToolCallPart(
                    output_tool,
                    {"city": "Chicago", "country": "United States"},
                    tool_call_id="typed-output-2",
                )
            ]
        )

    agent = PydanticAgent(FunctionModel(respond), output_type=Answer)
    capture = initialize(output=tmp_path, service_name="pydantic-structured-retry-app")
    capture.instrument(
        adapter=pydantic_ai_adapter(version=PYDANTIC_AI_VERSION),
        client=agent,
    )

    result = await agent.run("The windy city in the US of A.")
    assert result.output == Answer(city="Chicago", country="United States")
    assert attempts == 2
    assert model_visible_retry is not None

    records = _project(capture.shutdown().artifact_path)
    retry = next(
        record
        for record in records
        if record["kind"] == "message" and record["data"].get("call_id") == "typed-output-1"
    )
    assert retry["data"] == {
        "role": "tool",
        "content": model_visible_retry.model_response(),
        "name": model_visible_retry.tool_name,
        "call_id": model_visible_retry.tool_call_id,
    }
    assert retry["data"]["content"].endswith("\n\nFix the errors and try again.")
    assert not [record for record in records if record["kind"] == "loss"]


async def test_pydantic_ai_links_framework_validated_typed_result_to_outcome(
    tmp_path: Path,
) -> None:
    class Answer(BaseModel):
        answer: str

    agent = PydanticAgent(TestModel(), output_type=Answer)
    capture = initialize(output=tmp_path, service_name="pydantic-typed-result-app")
    capture.instrument(
        adapter=pydantic_ai_adapter(version=PYDANTIC_AI_VERSION),
        client=agent,
    )

    result = await agent.run("answer", conversation_id="conversation-1")
    assert result.output == Answer(answer="a")

    records = _project(capture.shutdown().artifact_path)
    model_response = next(record for record in records if record["kind"] == "model.response")
    model_request = next(record for record in records if record["kind"] == "model.request")
    validated = next(
        record
        for record in records
        if record["kind"] == "state" and record["data"]["type"] == "state.validated_result"
    )
    outcome = next(record for record in records if record["kind"] == "run.outcome")
    assert validated["data"]["value"] == {"answer": "a"}
    assert validated["links"] == [{"type": "derived_from", "record": model_response["id"]}]
    assert outcome["data"] == {"status": "completed"}
    assert outcome["links"] == [{"type": "derived_from", "record": validated["id"]}]
    output_schema = {
        "properties": {"answer": {"type": "string"}},
        "required": ["answer"],
        "title": "Answer",
        "type": "object",
    }
    assert model_request["data"]["tool_definitions"][0] == {
        "description": "The final response which ends this conversation",
        "kind": "output",
        "name": "final_result",
        "parameters_json_schema": output_schema,
    }
    assert model_request["data"]["settings"]["output_object"]["json_schema"] == (output_schema)
    assert not [
        record
        for record in records
        if record["kind"] in {"tool.proposal", "tool.call", "tool.result"}
    ]


async def test_google_adk_capture_projects_one_clear_semantic_trace(
    tmp_path: Path,
) -> None:
    class FixtureOutput(BaseModel):
        result: str

    def fixture_lookup(value: str, tool_context: ToolContext) -> dict[str, str]:
        """Look up a fixture value."""
        tool_context.state["tool_state"] = "applied"
        return {"result": f"ok:{value}"}

    agent = LlmAgent(
        name="fixture",
        model=_FixtureADKModel(model="fixture"),
        instruction="Use fixture_lookup when a lookup is requested.",
        output_schema=FixtureOutput,
        tools=[fixture_lookup],
        generate_content_config=genai_types.GenerateContentConfig(
            temperature=0.25,
            top_p=0.8,
            max_output_tokens=256,
            stop_sequences=["END"],
            seed=7,
        ),
    )
    runner = InMemoryRunner(agent=agent, app_name="fixture")
    await runner.session_service.create_session(
        app_name="fixture",
        user_id="fixture-user",
        session_id="conversation-1",
    )
    capture = initialize(output=tmp_path, service_name="adk-semantic-app")
    capture.instrument(
        adapter=google_adk_adapter(version=GOOGLE_ADK_VERSION),
        client=runner,
    )
    events = [
        event
        async for event in runner.run_async(
            user_id="fixture-user",
            session_id="conversation-1",
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="use tool")],
            ),
            state_delta={
                "run_state": "applied",
                "enabled": False,
                "count": 0,
                "text": "",
                "items": [],
                "details": {},
            },
        )
    ]
    assert events[-1].content.parts[0].text == "done"

    records = _project(capture.shutdown().artifact_path)
    requests = [record for record in records if record["kind"] == "model.request"]
    assert not [record for record in records if record["kind"] == "loss"]
    system = next(
        record
        for record in records
        if record["kind"] == "message" and record["data"]["role"] == "system"
    )
    assert "Use fixture_lookup when a lookup is requested." in system["data"]["content"]
    assert [request["data"]["context_refs"] for request in requests] == [
        [system["id"]],
        [system["id"]],
    ]
    output_schema = {
        "properties": {"result": {"title": "Result", "type": "string"}},
        "required": ["result"],
        "title": "FixtureOutput",
        "type": "object",
    }
    assert all(
        request["data"]["tools"] == ["set_model_response", "fixture_lookup"] for request in requests
    )
    for request in requests:
        declarations = request["data"]["tool_definitions"][0]["function_declarations"]
        assert declarations[0]["name"] == "set_model_response"
        assert declarations[0]["parameters_json_schema"] == {
            **output_schema,
            "title": "set_model_responseParams",
        }
        assert declarations[1] == {
            "description": "Look up a fixture value.",
            "name": "fixture_lookup",
            "parameters_json_schema": {
                "properties": {"value": {"title": "Value", "type": "string"}},
                "required": ["value"],
                "title": "fixture_lookupParams",
                "type": "object",
            },
        }
    assert all(
        request["data"]["settings"]
        == {
            "temperature": 0.25,
            "top_p": 0.8,
            "max_output_tokens": 256,
            "stop_sequences": ["END"],
            "seed": 7,
            "response_schema": output_schema,
        }
        for request in requests
    )
    assert [record["kind"] for record in records].count("run.start") == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    assert [record["kind"] for record in records].count("tool.proposal") == 1
    assert records[-1]["kind"] == "run.outcome"
    assert records[-1]["data"]["status"] == "completed"
    assert records[-1]["data"]["output"]["parts"][0]["text"] == "done"
    _assert_exact_pairs(records, "model.request")
    _assert_exact_pairs(records, "tool.call")
    responses = [record for record in records if record["kind"] == "model.response"]
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    run_start = next(record for record in records if record["kind"] == "run.start")
    assert proposal["parent"] == run_start["id"]
    assert "links" not in records[-1]
    assert all(
        response["data"]["usage"] == {"input_tokens": 1, "output_tokens": 1}
        for response in responses
    )
    assert not any(_contains_none(record["data"]) for record in records)
    states = [record for record in records if record["kind"] == "state"]
    assert any(
        state["data"]
        == {
            "type": "session.state_delta",
            "value": {
                "run_state": "applied",
                "enabled": False,
                "count": 0,
                "text": "",
                "items": [],
                "details": {},
            },
        }
        for state in states
    )
    result = next(record for record in records if record["kind"] == "tool.result")
    tool_state = next(
        state for state in states if state["data"]["value"] == {"tool_state": "applied"}
    )
    assert tool_state["links"] == [{"type": "derived_from", "record": result["id"]}]


async def test_google_adk_preserves_exposed_thought_parts_in_native_order(
    tmp_path: Path,
) -> None:
    class ReasoningModel(_FixtureADKModel):
        async def generate_content_async(self, llm_request: Any, stream: bool = False) -> Any:
            del llm_request, stream
            yield LlmResponse(
                content=genai_types.Content(
                    role="model",
                    parts=[
                        genai_types.Part(
                            thought=True,
                            thought_signature=b"opaque-thought-signature",
                        )
                    ],
                ),
                partial=True,
            )
            for text in ("Inspect both ", "records."):
                yield LlmResponse(
                    content=genai_types.Content(
                        role="model",
                        parts=[genai_types.Part(text=text, thought=True)],
                    ),
                    partial=True,
                )
            yield LlmResponse(
                content=genai_types.Content(
                    role="model",
                    parts=[
                        genai_types.Part(text="Inspect both ", thought=True),
                        genai_types.Part(text="records.", thought=True),
                        genai_types.Part.from_text(text="Use record two."),
                    ],
                ),
                partial=False,
            )

    runner = InMemoryRunner(
        agent=LlmAgent(name="reasoning_fixture", model=ReasoningModel(model="fixture")),
        app_name="reasoning-fixture",
    )
    await runner.session_service.create_session(
        app_name="reasoning-fixture",
        user_id="fixture-user",
        session_id="reasoning-session",
    )
    capture = initialize(output=tmp_path, service_name="adk-reasoning-app")
    capture.instrument(
        adapter=google_adk_adapter(version="2.4.0"),
        client=runner,
    )

    events = [
        event
        async for event in runner.run_async(
            user_id="fixture-user",
            session_id="reasoning-session",
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="choose the current record")],
            ),
        )
    ]
    assert events[-1].content.parts[-1].text == "Use record two."

    records = _project(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == {
        "parts": [{"text": "Use record two."}],
        "role": "model",
    }
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect both "},
        {"type": "text", "text": "records."},
    ]
    assert not [record for record in records if record["kind"] == "loss"]


async def test_google_adk_request_refs_use_exact_provider_call_ids(
    tmp_path: Path,
) -> None:
    class ProviderIdModel(_FixtureADKModel):
        async def generate_content_async(self, llm_request: Any, stream: bool = False) -> Any:
            async for response in super().generate_content_async(llm_request, stream):
                content = response.content
                for part in getattr(content, "parts", None) or []:
                    if part.function_call is not None:
                        part.function_call.id = "provider-call-1"
                yield response

    def fixture_lookup(value: str) -> dict[str, str]:
        return {"result": f"ok:{value}"}

    runner = InMemoryRunner(
        agent=LlmAgent(
            name="fixture",
            model=ProviderIdModel(model="fixture"),
            tools=[fixture_lookup],
        ),
        app_name="fixture",
    )
    await runner.session_service.create_session(
        app_name="fixture",
        user_id="fixture-user",
        session_id="conversation-1",
    )
    capture = initialize(output=tmp_path, service_name="adk-context-semantic-app")
    capture.instrument(
        adapter=google_adk_adapter(version=GOOGLE_ADK_VERSION),
        client=runner,
    )
    events = [
        event
        async for event in runner.run_async(
            user_id="fixture-user",
            session_id="conversation-1",
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="use tool")],
            ),
        )
    ]
    assert events[-1].content.parts[0].text == "done"

    records = _project(capture.shutdown().artifact_path)
    requests = [record for record in records if record["kind"] == "model.request"]
    first_response = next(record for record in records if record["kind"] == "model.response")
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    result = next(record for record in records if record["kind"] == "tool.result")
    system = next(
        record
        for record in records
        if record["kind"] == "message" and record["data"]["role"] == "system"
    )
    assert requests[0]["data"]["context_refs"] == [system["id"]]
    assert requests[1]["data"]["context_refs"] == [
        system["id"],
        first_response["id"],
        result["id"],
    ]
    assert proposal["parent"] == first_response["id"]
    assert not [record for record in records if record["kind"] == "loss"]


async def test_google_adk_failed_model_keeps_compact_error_without_exception_graph_losses(
    tmp_path: Path,
) -> None:
    class Unsupported:
        __slots__ = ()

    failure = ServerError(
        503,
        {"error": {"status": "UNAVAILABLE", "message": "fixture unavailable"}},
        SimpleNamespace(headers=Unsupported(), stream=Unsupported()),
    )
    runner = InMemoryRunner(
        agent=LlmAgent(
            name="fixture",
            model=_FixtureADKModel(
                model="fixture",
                fail_first=True,
                failure=failure,
            ),
        ),
        app_name="fixture",
    )
    await runner.session_service.create_session(
        app_name="fixture",
        user_id="fixture-user",
        session_id="conversation-1",
    )
    capture = initialize(output=tmp_path, service_name="adk-error-semantic-app")
    capture.instrument(
        adapter=google_adk_adapter(version=GOOGLE_ADK_VERSION),
        client=runner,
    )

    with pytest.raises(ServerError) as caught:
        async for _event in runner.run_async(
            user_id="fixture-user",
            session_id="conversation-1",
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="fail once")],
            ),
        ):
            pass
    assert caught.value is failure

    records = _project(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records] == [
        "run.start",
        "message",
        "model.request",
        "model.response",
        "error",
        "run.outcome",
    ]
    assert not [record for record in records if record["kind"] == "loss"]
    errors = [record for record in records if record["kind"] == "error"]
    assert len(errors) == 1
    assert errors[0]["data"]["message"] == "fixture unavailable"
    assert errors[0]["data"]["details"] == {
        "native_type": "ServerError",
        "code": 503,
        "status": "UNAVAILABLE",
    }
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "failed"


async def test_google_adk_model_error_recovery_keeps_completed_run(
    tmp_path: Path,
) -> None:
    failure = RuntimeError("recoverable model failure")

    def recover_model(**_kwargs: Any) -> LlmResponse:
        return LlmResponse(
            content=genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="recovered")],
            )
        )

    runner = InMemoryRunner(
        agent=LlmAgent(
            name="fixture",
            model=_FixtureADKModel(
                model="fixture",
                fail_first=True,
                failure=failure,
            ),
            on_model_error_callback=recover_model,
        ),
        app_name="fixture",
    )
    await runner.session_service.create_session(
        app_name="fixture",
        user_id="fixture-user",
        session_id="conversation-1",
    )
    capture = initialize(output=tmp_path, service_name="adk-model-recovery")
    capture.instrument(
        adapter=google_adk_adapter(version=GOOGLE_ADK_VERSION),
        client=runner,
    )

    events = [
        event
        async for event in runner.run_async(
            user_id="fixture-user",
            session_id="conversation-1",
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="recover")],
            ),
        )
    ]
    assert events[-1].content.parts[0].text == "recovered"

    records = _project(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(responses) == 1
    assert responses[0]["data"]["status"] == "completed"
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "completed"
    assert any(
        record["kind"] == "error"
        and record["data"]["message"] == "recoverable model failure"
        and record["data"]["recoverable"] is True
        for record in records
    )
    assert not [record for record in records if record["kind"] == "loss"]


async def test_google_adk_recovery_survives_earlier_after_model_short_circuit(
    tmp_path: Path,
) -> None:
    class EarlierPlugin(BasePlugin):
        def __init__(self) -> None:
            super().__init__("earlier")

        async def after_model_callback(
            self,
            *,
            callback_context: Any,
            llm_response: LlmResponse,
        ) -> LlmResponse:
            del callback_context, llm_response
            return LlmResponse(
                content=genai_types.Content(
                    role="model",
                    parts=[genai_types.Part.from_text(text="altered recovery")],
                )
            )

    failure = RuntimeError("recoverable model failure")

    def recover_model(**_kwargs: Any) -> LlmResponse:
        return LlmResponse(
            content=genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="recovered")],
            )
        )

    runner = InMemoryRunner(
        agent=LlmAgent(
            name="fixture",
            model=_FixtureADKModel(
                model="fixture",
                fail_first=True,
                failure=failure,
            ),
            on_model_error_callback=recover_model,
        ),
        app_name="fixture",
        plugins=[EarlierPlugin()],
    )
    await runner.session_service.create_session(
        app_name="fixture",
        user_id="fixture-user",
        session_id="conversation-1",
    )
    capture = initialize(output=tmp_path, service_name="adk-short-circuit-recovery")
    capture.instrument(
        adapter=google_adk_adapter(version=GOOGLE_ADK_VERSION),
        client=runner,
    )

    events = [
        event
        async for event in runner.run_async(
            user_id="fixture-user",
            session_id="conversation-1",
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="recover")],
            ),
        )
    ]
    assert events[-1].content.parts[0].text == "altered recovery"

    records = _project(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(responses) == 1
    assert responses[0]["data"]["status"] == "completed"
    assert responses[0]["data"]["content"]["parts"][0]["text"] == "altered recovery"
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "completed"
    assert any(
        record["kind"] == "error"
        and record["data"]["message"] == "recoverable model failure"
        and record["data"]["recoverable"] is True
        for record in records
    )
    assert not [record for record in records if record["kind"] == "loss"]


async def test_google_adk_tool_error_recovery_keeps_completed_run(
    tmp_path: Path,
) -> None:
    def fixture_lookup(value: str) -> dict[str, str]:
        del value
        raise RuntimeError("recoverable tool failure")

    def recover_tool(**_kwargs: Any) -> dict[str, str]:
        return {"result": "recovered"}

    runner = InMemoryRunner(
        agent=LlmAgent(
            name="fixture",
            model=_FixtureADKModel(model="fixture"),
            tools=[fixture_lookup],
            on_tool_error_callback=recover_tool,
        ),
        app_name="fixture",
    )
    await runner.session_service.create_session(
        app_name="fixture",
        user_id="fixture-user",
        session_id="conversation-1",
    )
    capture = initialize(output=tmp_path, service_name="adk-tool-recovery")
    capture.instrument(
        adapter=google_adk_adapter(version=GOOGLE_ADK_VERSION),
        client=runner,
    )

    events = [
        event
        async for event in runner.run_async(
            user_id="fixture-user",
            session_id="conversation-1",
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="use tool")],
            ),
        )
    ]
    assert events[-1].content.parts[0].text == "done"

    records = _project(capture.shutdown().artifact_path)
    results = [record for record in records if record["kind"] == "tool.result"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(results) == 1
    assert results[0]["data"]["status"] == "succeeded"
    assert results[0]["data"]["output"] == {"result": "recovered"}
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "completed"
    assert any(
        record["kind"] == "error"
        and record["data"]["message"] == "recoverable tool failure"
        and record["data"]["recoverable"] is True
        for record in records
    )
    assert not [record for record in records if record["kind"] == "loss"]


async def test_google_adk_tool_recovery_survives_earlier_after_tool_short_circuit(
    tmp_path: Path,
) -> None:
    class EarlierPlugin(BasePlugin):
        def __init__(self) -> None:
            super().__init__("earlier")

        async def after_tool_callback(
            self,
            *,
            tool: Any,
            tool_args: dict[str, Any],
            tool_context: Any,
            result: Any,
        ) -> dict[str, str]:
            del tool, tool_args, tool_context, result
            return {"result": "altered recovery"}

    def fixture_lookup(value: str) -> dict[str, str]:
        del value
        raise RuntimeError("recoverable tool failure")

    def recover_tool(**_kwargs: Any) -> dict[str, str]:
        return {"result": "recovered"}

    runner = InMemoryRunner(
        agent=LlmAgent(
            name="fixture",
            model=_FixtureADKModel(model="fixture"),
            tools=[fixture_lookup],
            on_tool_error_callback=recover_tool,
        ),
        app_name="fixture",
        plugins=[EarlierPlugin()],
    )
    await runner.session_service.create_session(
        app_name="fixture",
        user_id="fixture-user",
        session_id="conversation-1",
    )
    capture = initialize(output=tmp_path, service_name="adk-tool-short-circuit-recovery")
    capture.instrument(
        adapter=google_adk_adapter(version=GOOGLE_ADK_VERSION),
        client=runner,
    )

    events = [
        event
        async for event in runner.run_async(
            user_id="fixture-user",
            session_id="conversation-1",
            new_message=genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="use tool")],
            ),
        )
    ]
    assert events[-1].content.parts[0].text == "done"

    records = _project(capture.shutdown().artifact_path)
    results = [record for record in records if record["kind"] == "tool.result"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(results) == 1
    assert results[0]["data"]["status"] == "succeeded"
    assert results[0]["data"]["output"] == {"result": "altered recovery"}
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "completed"
    assert any(
        record["kind"] == "error"
        and record["data"]["message"] == "recoverable tool failure"
        and record["data"]["recoverable"] is True
        for record in records
    )
    assert not [record for record in records if record["kind"] == "loss"]


async def test_google_adk_finished_invocation_releases_unmatched_pair_receipts() -> None:
    class PluginManager:
        plugin: Any = None

        def register_plugin(self, plugin: Any) -> None:
            self.plugin = plugin

    class Sink:
        start_receipt: weakref.ReferenceType[AdmissionReceipt] | None = None
        values: list[dict[str, Any]]

        def __init__(self) -> None:
            self.values = []

        def open_trace(self, _value: Any) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                accepted=True,
                record_id="root",
                identity={
                    "run_id": "run",
                    "trace_id": "trace",
                    "operation_id": "operation",
                },
            )

        def record(self, value: Any) -> AdmissionReceipt:
            self.values.append(value)
            receipt = AdmissionReceipt(accepted=True, record_id="record")
            if value["kind"] == "model" and value["phase"] == "start":
                self.start_receipt = weakref.ref(receipt)
            return receipt

    manager = PluginManager()
    sink = Sink()
    runner = SimpleNamespace(plugin_manager=manager)
    source = google_adk_adapter(version=GOOGLE_ADK_VERSION).create_source(runner)
    source.install(sink)
    invocation = SimpleNamespace(
        invocation_id="invocation-1",
        session=SimpleNamespace(id="session-1", events=[]),
    )
    callback = SimpleNamespace(
        _invocation_context=invocation,
        actions=SimpleNamespace(),
    )

    await manager.plugin.before_run_callback(invocation_context=invocation)
    await manager.plugin.before_model_callback(
        callback_context=callback,
        llm_request=SimpleNamespace(config=None, contents=[]),
    )
    assert sink.start_receipt is not None
    assert sink.start_receipt() is not None

    await manager.plugin.after_run_callback(invocation_context=invocation)
    gc.collect()

    gaps = [
        value for value in sink.values if value.get("semantic", {}).get("type") == "capture.gap"
    ]
    assert len(gaps) == 1
    assert gaps[0]["semantic"]["reason"] == "pair_completion_not_observed"
    assert sink.start_receipt() is None


def test_google_adk_deactivate_releases_retained_sink() -> None:
    class PluginManager:
        plugin: Any = None

        def register_plugin(self, plugin: Any) -> None:
            self.plugin = plugin

    class Sink:
        pass

    manager = PluginManager()
    runner = SimpleNamespace(plugin_manager=manager)
    source = google_adk_adapter(version=GOOGLE_ADK_VERSION).create_source(runner)
    sink = Sink()
    sink_reference = weakref.ref(sink)
    lifecycle = source.install(sink)

    lifecycle.deactivate()
    del sink
    gc.collect()

    assert manager.plugin is not None
    assert sink_reference() is None
