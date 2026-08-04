from __future__ import annotations

import json
import threading
import tracemalloc
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import Field

pytest.importorskip("llama_index")

from llama_index.core.agent.workflow import (
    AgentInput,
    AgentOutput,
    AgentStream,
    FunctionAgent,
    ToolCall,
    ToolCallResult,
)
from llama_index.core.base.llms.types import (
    ChatMessage,
    ChatResponse,
    ImageBlock,
    MessageRole,
    TextBlock,
    ThinkingBlock,
)
from llama_index.core.callbacks import CallbackManager
from llama_index.core.callbacks.base_handler import BaseCallbackHandler
from llama_index.core.callbacks.schema import CBEventType, EventPayload
from llama_index.core.instrumentation import root_dispatcher
from llama_index.core.llms.mock import MockLLM
from llama_index.core.tools import FunctionTool, ToolOutput
from llama_index.core.workflow import Context, StartEvent, StopEvent, Workflow, step

from semantic_layer import (
    initialize,
    llamaindex_adapter,
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("blocks", "delta", "expected_content"),
    [
        (
            [ThinkingBlock(content="The result is 1,386,528.")],
            None,
            None,
        ),
        (
            [
                ThinkingBlock(content="Multiply the exposed operands."),
                TextBlock(text="The result is 1,386,528."),
            ],
            "The result is 1,386,528.",
            "The result is 1,386,528.",
        ),
    ],
)
async def test_llamaindex_projects_exposed_thinking_blocks_without_mixing_visible_output(
    tmp_path: Path,
    blocks: list[Any],
    delta: str | None,
    expected_content: str | None,
) -> None:
    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-thinking-block")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-thinking",
        turn_id="turn-0",
        turn_index=0,
    ):
        callback = manager.handlers[-1]
        model_id = callback.on_event_start(
            CBEventType.LLM,
            payload={
                EventPayload.MESSAGES: [
                    ChatMessage(role=MessageRole.USER, content="calculate")
                ],
                EventPayload.SERIALIZED: {"model_name": "fixture-model"},
            },
            event_id="model-thinking",
        )
        callback.on_event_end(
            CBEventType.LLM,
            payload={
                EventPayload.RESPONSE: ChatResponse(
                    message=ChatMessage(
                        role=MessageRole.ASSISTANT,
                        blocks=blocks,
                        additional_kwargs={
                            "reasoning_content": "private provider payload"
                        },
                    ),
                    delta=delta,
                )
            },
            event_id=model_id,
        )

    records = _trace_records(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 1
    response = responses[0]
    assert response["data"]["reasoning"] == [
        {
            "type": "text",
            "text": native.content,
        }
        for native in blocks
        if isinstance(native, ThinkingBlock)
    ]
    if expected_content is None:
        assert "content" not in response["data"]
    else:
        assert response["data"]["content"] == expected_content
    assert "private provider payload" not in json.dumps(response["data"])
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.asyncio
@pytest.mark.parametrize("final_thinking", [False, True])
async def test_llamaindex_projects_agent_stream_thinking_on_active_model_response(
    tmp_path: Path,
    final_thinking: bool,
) -> None:
    message = ChatMessage(
        role=MessageRole.ASSISTANT,
        blocks=[
            *(
                [ThinkingBlock(content="Check the units.")]
                if final_thinking
                else []
            ),
            TextBlock(text="The answer is 42."),
        ],
    )
    provider_raw = object()
    chat_response = ChatResponse(message=message, raw=provider_raw)
    result = AgentOutput(response=message, current_agent_name="fixture-agent")
    manager = CallbackManager([])
    callback: Any = None

    class Handler:
        async def stream_events(self) -> Any:
            model_id = callback.on_event_start(
                CBEventType.LLM,
                payload={
                    EventPayload.MESSAGES: [
                        ChatMessage(role=MessageRole.USER, content="answer")
                    ],
                    EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                },
                event_id="stream-thinking-model",
            )
            yield AgentStream(
                delta="",
                response="",
                current_agent_name="fixture-agent",
                thinking_delta="Check ",
                raw=provider_raw,
            )
            yield AgentStream(
                delta="The answer is 42.",
                response="The answer is 42.",
                current_agent_name="fixture-agent",
                thinking_delta="the units.",
                raw=provider_raw,
            )
            callback.on_event_end(
                CBEventType.LLM,
                payload={EventPayload.RESPONSE: chat_response},
                event_id=model_id,
            )

        def __await__(self) -> Any:
            async def complete() -> AgentOutput:
                return result

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    capture = initialize(output=tmp_path, service_name="llamaindex-stream-thinking")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-stream-thinking",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        handler = turn.run(Agent(), user_msg="answer")
        assert len([event async for event in handler.stream_events()]) == 2
        assert await handler is result

    records = _trace_records(capture.shutdown().artifact_path)
    response = _one(records, "model.response")
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Check the units."}
    ]
    assert response["data"]["content"] == "The answer is 42."
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.asyncio
async def test_llamaindex_interrupted_thinking_stream_settles_exact_model_response(
    tmp_path: Path,
) -> None:
    manager = CallbackManager([])
    callback: Any = None

    class Handler:
        async def stream_events(self) -> Any:
            callback.on_event_start(
                CBEventType.LLM,
                payload={
                    EventPayload.MESSAGES: [
                        ChatMessage(role=MessageRole.USER, content="answer")
                    ],
                    EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                },
                event_id="interrupted-thinking-model",
            )
            yield AgentStream(
                delta="",
                response="",
                current_agent_name="fixture-agent",
                thinking_delta="Still checking.",
            )

        def __await__(self) -> Any:
            raise AssertionError("interrupted caller must not await the handler")

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    capture = initialize(output=tmp_path, service_name="llamaindex-interrupted-thinking")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-interrupted-thinking",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        stream = turn.run(Agent(), user_msg="answer").stream_events()
        event = await anext(stream)
        assert event.thinking_delta == "Still checking."
        await stream.aclose()

    records = _trace_records(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 1
    assert responses[0]["data"] == {
        "status": "cancelled",
    }
    reasoning_state = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"].get("value", {}).get("reasoning") == "Still checking."
    )
    assert reasoning_state["data"]["value"] == {
        "reasoning": "Still checking.",
        "status": "cancelled",
    }
    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == [
        "workflow_stream_reasoning_model_response_correlation_unavailable"
    ]


@pytest.mark.asyncio
async def test_llamaindex_natural_stream_exhaustion_settles_omitted_model_incomplete(
    tmp_path: Path,
) -> None:
    manager = CallbackManager([])
    callback: Any = None

    class Handler:
        async def stream_events(self) -> Any:
            callback.on_event_start(
                CBEventType.LLM,
                payload={
                    EventPayload.MESSAGES: [
                        ChatMessage(role=MessageRole.USER, content="answer")
                    ],
                    EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                },
                event_id="exhausted-thinking-model",
            )
            yield AgentStream(
                delta="",
                response="",
                current_agent_name="fixture-agent",
                thinking_delta="Still checking.",
            )

        def __await__(self) -> Any:
            raise AssertionError("stream-only caller must not await the handler")

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    capture = initialize(output=tmp_path, service_name="llamaindex-exhausted-thinking")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-exhausted-thinking",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        streamed = [
            event
            async for event in turn.run(Agent(), user_msg="answer").stream_events()
        ]
        assert len(streamed) == 1

    records = _trace_records(capture.shutdown().artifact_path)
    response = _one(records, "model.response")
    assert response["data"] == {"status": "incomplete"}
    assert _one(records, "run.outcome")
    reasoning_state = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"].get("value", {}).get("reasoning") == "Still checking."
    )
    assert reasoning_state["data"]["value"] == {
        "reasoning": "Still checking.",
        "status": "completed",
    }
    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == [
        "workflow_stream_reasoning_model_response_correlation_unavailable"
    ]


@pytest.mark.asyncio
async def test_llamaindex_callback_error_retains_streamed_thinking(
    tmp_path: Path,
) -> None:
    manager = CallbackManager([])
    callback: Any = None
    model_error = RuntimeError("provider failed")
    provider_raw = object()
    failed_response = ChatResponse(
        message=ChatMessage(role=MessageRole.ASSISTANT),
        raw=provider_raw,
    )

    class Handler:
        async def stream_events(self) -> Any:
            model_id = callback.on_event_start(
                CBEventType.LLM,
                payload={
                    EventPayload.MESSAGES: [
                        ChatMessage(role=MessageRole.USER, content="answer")
                    ],
                    EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                },
                event_id="failed-thinking-model",
            )
            yield AgentStream(
                delta="",
                response="",
                current_agent_name="fixture-agent",
                thinking_delta="Check ",
                raw=provider_raw,
            )
            yield AgentStream(
                delta="",
                response="",
                current_agent_name="fixture-agent",
                thinking_delta="the units.",
                raw=provider_raw,
            )
            callback.on_event_end(
                CBEventType.LLM,
                payload={
                    EventPayload.EXCEPTION: model_error,
                    EventPayload.RESPONSE: failed_response,
                },
                event_id=model_id,
            )

        def __await__(self) -> Any:
            async def complete() -> None:
                return None

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    capture = initialize(output=tmp_path, service_name="llamaindex-failed-thinking")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-failed-thinking",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        handler = turn.run(Agent(), user_msg="answer")
        assert len([event async for event in handler.stream_events()]) == 2

    records = _trace_records(capture.shutdown().artifact_path)
    response = _one(records, "model.response")
    assert response["data"] == {
        "status": "failed",
        "reasoning": [{"type": "text", "text": "Check the units."}],
    }


@pytest.mark.asyncio
async def test_llamaindex_does_not_guess_between_concurrent_model_responses(
    tmp_path: Path,
) -> None:
    manager = CallbackManager([])
    callback: Any = None

    class Handler:
        async def stream_events(self) -> Any:
            model_ids = [
                callback.on_event_start(
                    CBEventType.LLM,
                    payload={
                        EventPayload.MESSAGES: [
                            ChatMessage(role=MessageRole.USER, content="answer")
                        ],
                        EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                    },
                    event_id=f"concurrent-model-{index}",
                )
                for index in range(2)
            ]
            yield AgentStream(
                delta="",
                response="",
                current_agent_name="fixture-agent",
                thinking_delta="Unowned thinking.",
            )
            for index, model_id in enumerate(model_ids):
                callback.on_event_end(
                    CBEventType.LLM,
                    payload={
                        EventPayload.RESPONSE: ChatResponse(
                            message=ChatMessage(
                                role=MessageRole.ASSISTANT,
                                content=f"response-{index}",
                            )
                        )
                    },
                    event_id=model_id,
                )

        def __await__(self) -> Any:
            async def complete() -> None:
                return None

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    capture = initialize(output=tmp_path, service_name="llamaindex-concurrent-thinking")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-concurrent-thinking",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        streamed = [
            event
            async for event in turn.run(Agent(), user_msg="answer").stream_events()
        ]
        assert len(streamed) == 1

    records = _trace_records(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 2
    assert all("reasoning" not in record["data"] for record in responses)
    reasoning_state = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"].get("value", {}).get("reasoning")
        == "Unowned thinking."
    )
    assert reasoning_state["data"]["value"]["status"] == "completed"
    assert [
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
    ] == ["workflow_stream_reasoning_model_response_correlation_unavailable"]


@pytest.mark.asyncio
async def test_llamaindex_reports_only_proven_contentless_thinking_blocks(
    tmp_path: Path,
) -> None:
    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-thinking-token-gap")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-thinking-token-gap",
        turn_id="turn-0",
        turn_index=0,
    ):
        callback = manager.handlers[-1]
        model_id = callback.on_event_start(
            CBEventType.LLM,
            payload={
                EventPayload.MESSAGES: [
                    ChatMessage(role=MessageRole.USER, content="answer")
                ],
                EventPayload.SERIALIZED: {"model_name": "fixture-model"},
            },
            event_id="thinking-token-gap-model",
        )
        callback.on_event_end(
            CBEventType.LLM,
            payload={
                EventPayload.RESPONSE: ChatResponse(
                    message=ChatMessage(
                        role=MessageRole.ASSISTANT,
                        blocks=[
                            ThinkingBlock(num_tokens=3),
                            ThinkingBlock(num_tokens=0),
                            ThinkingBlock(),
                        ],
                    )
                )
            },
            event_id=model_id,
        )

    records = _trace_records(capture.shutdown().artifact_path)
    response = _one(records, "model.response")
    assert "reasoning" not in response["data"]
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "unsupported_native_value"
    assert losses[0]["data"]["count"] == 1


class _ExistingHandler(BaseCallbackHandler):
    def __init__(self) -> None:
        super().__init__([], [])

    def on_event_start(
        self,
        event_type: Any,
        payload: Any = None,
        event_id: str = "",
        parent_id: str = "",
        **kwargs: Any,
    ) -> str:
        del event_type, payload, parent_id, kwargs
        return event_id

    def on_event_end(
        self,
        event_type: Any,
        payload: Any = None,
        event_id: str = "",
        **kwargs: Any,
    ) -> None:
        del event_type, payload, event_id, kwargs

    def start_trace(self, trace_id: str | None = None) -> None:
        del trace_id

    def end_trace(self, trace_id: str | None = None, trace_map: Any = None) -> None:
        del trace_id, trace_map


def test_llamaindex_01423_accepts_function_agent_and_restores_handlers(
    tmp_path: Path,
) -> None:
    existing = _ExistingHandler()
    manager = CallbackManager([existing])
    handlers = manager.handlers
    prior_dispatcher_handlers = list(root_dispatcher.event_handlers)
    prior_dispatcher_span_handlers = list(root_dispatcher.span_handlers)
    agent = FunctionAgent(
        llm=MockLLM(max_tokens=8, callback_manager=manager),
        tools=[],
    )

    capture = initialize(output=tmp_path, service_name="llamaindex-01423-attach")
    capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=agent,
    )

    assert manager.handlers is handlers
    assert manager.handlers[0] is existing
    assert len(manager.handlers) == 2
    assert root_dispatcher.event_handlers[:-1] == prior_dispatcher_handlers
    assert root_dispatcher.span_handlers[:-1] == prior_dispatcher_span_handlers

    capture.shutdown()

    assert manager.handlers is handlers
    assert manager.handlers == [existing]
    assert root_dispatcher.event_handlers == prior_dispatcher_handlers
    assert root_dispatcher.span_handlers == prior_dispatcher_span_handlers


@pytest.mark.asyncio
async def test_llamaindex_callback_manager_preserves_result_and_stream_identity(
    tmp_path: Path,
) -> None:
    final_message = ChatMessage(
        role=MessageRole.ASSISTANT,
        content="I will look it up.",
        additional_kwargs={
            "semantic_tool_calls": [
                {
                    "tool_id": "call-llama",
                    "tool_name": "lookup",
                    "tool_kwargs": {"city": "London"},
                }
            ]
        },
    )
    result = AgentOutput(
        response=final_message,
        current_agent_name="fixture-agent",
    )
    tool_call = ToolCall(
        tool_name="lookup",
        tool_kwargs={"city": "London"},
        tool_id="call-llama",
    )
    tool_result = ToolCallResult(
        tool_name="lookup",
        tool_kwargs={"city": "London"},
        tool_id="call-llama",
        tool_output=ToolOutput(
            content="21 C",
            tool_name="lookup",
            raw_input={"city": "London"},
            raw_output={"temperature": 21},
        ),
        return_direct=False,
    )

    class Handler:
        async def stream_events(self) -> Any:
            response = ""
            for index in range(32):
                delta = str(index % 10)
                response += delta
                yield AgentStream(
                    delta=delta,
                    response=response,
                    current_agent_name="fixture-agent",
                )
            yield tool_call
            yield tool_result

        def __await__(self) -> Any:
            async def complete() -> object:
                return result

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "weather?"}
            return Handler()

    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-semantic")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-llama",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        model_id = callback.on_event_start(
            CBEventType.LLM,
            payload={
                EventPayload.MESSAGES: [ChatMessage(role=MessageRole.USER, content="weather?")],
                EventPayload.SERIALIZED: {
                    "class_name": "OpenAILike",
                    "model_name": "fixture-model",
                    "provider": "openrouter",
                    "temperature": 0.9,
                    "max_tokens": 64,
                    "context_window": 100_000,
                },
                EventPayload.ADDITIONAL_KWARGS: {
                    "temperature": 0.2,
                    "max_tokens": 512,
                    "tool_choice": "auto",
                    "tools": [
                        {
                            "type": "function",
                            "function": {
                                "name": "lookup",
                                "description": "Look up the weather.",
                                "parameters": {
                                    "type": "object",
                                    "properties": {"city": {"type": "string"}},
                                    "required": ["city"],
                                },
                            },
                        }
                    ],
                    "client": object(),
                },
            },
            event_id="model-llama",
        )
        callback.on_event_end(
            CBEventType.LLM,
            payload={EventPayload.RESPONSE: ChatResponse(message=final_message)},
            event_id=model_id,
        )
        handler = turn.run(Agent(), user_msg="weather?")
        events = [event async for event in handler.stream_events()]
        assert len(events) == 34
        assert events[-2] is tool_call
        assert events[-1] is tool_result
        assert await handler is result

    records = _trace_records(capture.shutdown().artifact_path)
    assert not [record for record in records if record["kind"] == "loss"]
    kinds = [record["kind"] for record in records]
    assert kinds.count("run.start") == 1
    assert kinds.count("run.outcome") == 1
    assert kinds.count("model.request") == 1
    assert kinds.count("model.response") == 1
    assert kinds.count("tool.proposal") == 1
    assert kinds.count("tool.call") == 1
    assert kinds.count("tool.result") == 1

    model_request = _one(records, "model.request")
    model_response = _one(records, "model.response")
    assert model_request["data"]["context_refs"]
    assert model_request["data"]["model"] == "fixture-model"
    assert model_request["data"]["tools"] == ["lookup"]
    assert model_request["data"]["tool_definitions"] == [
        {
            "type": "function",
            "function": {
                "name": "lookup",
                "description": "Look up the weather.",
                "parameters": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"],
                },
            },
        }
    ]
    assert model_request["data"]["settings"] == {
        "provider": "openrouter",
        "class_name": "OpenAILike",
        "temperature": 0.2,
        "max_tokens": 512,
        "context_window": 100_000,
        "tool_choice": "auto",
    }
    assert "client" not in json.dumps(model_request["data"])
    assert model_response["data"]["content"] == "I will look it up."
    assert model_response["links"] == [{"type": "result_of", "record": model_request["id"]}]
    proposal = _one(records, "tool.proposal")
    call = _one(records, "tool.call")
    tool_result_record = _one(records, "tool.result")
    assert proposal["data"]["native_call_id"] == "call-llama"
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert tool_result_record["links"] == [{"type": "result_of", "record": call["id"]}]
    assert tool_result_record["data"]["output"] == {"temperature": 21}
    outcome = _one(records, "run.outcome")
    assert outcome["data"] == {"status": "completed"}
    assert outcome["links"] == [{"type": "derived_from", "record": model_response["id"]}]


@pytest.mark.asyncio
async def test_llamaindex_cumulative_history_is_retained_once_with_exact_refs(
    tmp_path: Path,
) -> None:
    turns = 40
    manager = CallbackManager([])
    callback: Any = None
    final_output: AgentOutput | None = None

    class Handler:
        async def stream_events(self) -> Any:
            nonlocal final_output
            history = [
                ChatMessage(
                    role=MessageRole.USER,
                    content="inspect " + ("repository " * 16),
                )
            ]
            for index in range(turns):
                call_id = f"call-{index}"
                response = ChatMessage(
                    role=MessageRole.ASSISTANT,
                    content=(
                        "same tool proposal"
                        if index < turns - 1
                        else "completed " + ("summary " * 16)
                    ),
                    additional_kwargs=(
                        {
                            "semantic_tool_calls": [
                                {
                                    "tool_id": call_id,
                                    "tool_name": "lookup",
                                    "tool_kwargs": {"step": index},
                                }
                            ]
                        }
                        if index < turns - 1
                        else {}
                    ),
                )
                event_id = callback.on_event_start(
                    CBEventType.LLM,
                    payload={
                        EventPayload.MESSAGES: list(history),
                        EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                    },
                    event_id=f"model-{index}",
                )
                callback.on_event_end(
                    CBEventType.LLM,
                    payload={EventPayload.RESPONSE: ChatResponse(message=response)},
                    event_id=event_id,
                )
                yield AgentInput(
                    input=list(history),
                    current_agent_name="fixture-agent",
                )
                output = AgentOutput(
                    response=response,
                    current_agent_name="fixture-agent",
                )
                yield output
                final_output = output
                if index == turns - 1:
                    continue
                tool_call = ToolCall(
                    tool_name="lookup",
                    tool_kwargs={"step": index},
                    tool_id=call_id,
                )
                tool_result = ToolCallResult(
                    tool_name="lookup",
                    tool_kwargs={"step": index},
                    tool_id=call_id,
                    tool_output=ToolOutput(
                        content=f"result-{index}",
                        tool_name="lookup",
                        raw_input={"step": index},
                        raw_output={"value": index},
                    ),
                    return_direct=False,
                )
                yield tool_call
                yield tool_result
                history.extend(
                    [
                        response,
                        ChatMessage(
                            role=MessageRole.TOOL,
                            content=f"result-{index}",
                        ),
                    ]
                )

        def __await__(self) -> Any:
            async def complete() -> AgentOutput:
                assert final_output is not None
                return final_output

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs["user_msg"].startswith("inspect repository")
            return Handler()

    capture = initialize(output=tmp_path, service_name="llamaindex-cumulative")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-cumulative",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        handler = turn.run(
            Agent(),
            user_msg="inspect " + ("repository " * 16),
        )
        streamed = [event async for event in handler.stream_events()]
        assert len(streamed) == (turns * 2) + ((turns - 1) * 2)
        assert await handler is final_output

    status = capture.shutdown()
    records = _trace_records(status.artifact_path)
    kinds = [record["kind"] for record in records]
    message_records = [record for record in records if record["kind"] == "message"]
    model_requests = [record for record in records if record["kind"] == "model.request"]
    state_types = {
        record["data"].get("type")
        for record in records
        if record["kind"] == "state"
    }

    assert not [record for record in records if record["kind"] == "loss"]
    assert len(message_records) == 1 + (2 * (turns - 1))
    assert len(model_requests) == turns
    assert kinds.count("model.response") == turns
    assert kinds.count("tool.proposal") == turns - 1
    assert kinds.count("tool.call") == turns - 1
    assert kinds.count("tool.result") == turns - 1
    assert "state.agent_input" not in state_types
    assert "state.agent_output" not in state_types

    assert [len(request["data"]["context_refs"]) for request in model_requests] == [
        1,
        *([2] * (turns - 1)),
    ]
    assert [
        request["data"].get("context_base_ref") for request in model_requests
    ] == [
        None,
        *[request["id"] for request in model_requests[:-1]],
    ]
    expanded_context: dict[str, list[str]] = {}
    for request in model_requests:
        base = request["data"].get("context_base_ref")
        expanded_context[request["id"]] = [
            *(expanded_context[base] if isinstance(base, str) else []),
            *request["data"]["context_refs"],
        ]
    assert expanded_context[model_requests[-1]["id"]] == [
        record["id"] for record in message_records
    ]
    assert sum(
        len(request["data"]["context_refs"]) for request in model_requests
    ) == len(message_records)
    assert len({record["id"] for record in message_records}) == len(message_records)
    assert sum(
        record["data"]["content"] == "same tool proposal"
        for record in message_records
    ) == turns - 1

    trace_bytes = (Path(status.artifact_path) / "trace.jsonl").stat().st_size
    unique_message_bytes = sum(
        len(record["data"]["content"].encode("utf-8")) for record in message_records
    )
    assert trace_bytes < (4_096 * len(records)) + (unique_message_bytes * 4)


@pytest.mark.asyncio
async def test_llamaindex_stream_only_agent_output_retains_delivery(
    tmp_path: Path,
) -> None:
    message = ChatMessage(
        role=MessageRole.ASSISTANT,
        content="delivered without awaiting the handler",
    )
    output = AgentOutput(
        response=message,
        current_agent_name="fixture-agent",
    )
    manager = CallbackManager([])
    callback: Any = None

    class Handler:
        async def stream_events(self) -> Any:
            event_id = callback.on_event_start(
                CBEventType.LLM,
                payload={
                    EventPayload.MESSAGES: [
                        ChatMessage(role=MessageRole.USER, content="stream only")
                    ],
                    EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                },
                event_id="stream-only-model",
            )
            callback.on_event_end(
                CBEventType.LLM,
                payload={EventPayload.RESPONSE: ChatResponse(message=message)},
                event_id=event_id,
            )
            yield output

        def __await__(self) -> Any:
            raise AssertionError("stream-only caller must not await the handler")

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "stream only"}
            return Handler()

    capture = initialize(output=tmp_path, service_name="llamaindex-stream-only")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-stream-only",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        streamed = [
            event
            async for event in turn.run(
                Agent(),
                user_msg="stream only",
            ).stream_events()
        ]
        assert streamed == [output]

    records = _trace_records(capture.shutdown().artifact_path)
    response = _one(records, "model.response")
    outcome = _one(records, "run.outcome")
    assert response["data"]["content"] == message.content
    assert outcome["data"] == {"status": "completed"}
    assert outcome["links"] == [{"type": "derived_from", "record": response["id"]}]
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.asyncio
async def test_llamaindex_request_materialization_is_bounded_before_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_module("semantic_layer.llamaindex_adapter")
    monkeypatch.setattr(module, "_MAX_PLAIN_JSON_BYTES", 32)
    monkeypatch.setattr(module, "_MAX_PLAIN_JSON_NODES", 4)
    monkeypatch.setattr(module, "_MAX_PLAIN_JSON_WIDTH", 4)
    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-request-bounds")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )
    wide_tools = [
        {
            "type": "function",
            "function": {
                "name": f"tool-{index}",
                "parameters": {"type": "object"},
            },
        }
        for index in range(100)
    ]

    async with installed.turn(
        conversation_id="conversation-bounds",
        turn_id="turn-0",
        turn_index=0,
    ):
        callback = manager.handlers[-1]
        event_id = callback.on_event_start(
            CBEventType.LLM,
            payload={
                EventPayload.MESSAGES: [
                    ChatMessage(role=MessageRole.USER, content="Bound the request")
                ],
                EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                EventPayload.ADDITIONAL_KWARGS: {
                    "tools": wide_tools,
                    "stop": "x" * 1_000,
                    "response_format": {"a": {"b": {"c": {"d": "value"}}}},
                },
            },
            event_id="bounded-model",
        )
        callback.on_event_end(
            CBEventType.LLM,
            payload={
                EventPayload.RESPONSE: ChatResponse(
                    message=ChatMessage(role=MessageRole.ASSISTANT, content="done")
                )
            },
            event_id=event_id,
        )

    records = _trace_records(capture.shutdown().artifact_path)
    request = _one(records, "model.request")
    assert "tools" not in request["data"]
    assert "tool_definitions" not in request["data"]
    settings = request["data"].get("settings", {})
    assert "stop" not in settings
    assert "response_format" not in settings


def test_llamaindex_plain_json_rejects_oversized_text_without_copying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = import_module("semantic_layer.llamaindex_adapter")
    monkeypatch.setattr(module, "_MAX_PLAIN_JSON_BYTES", 64)
    oversized = "x" * (1024 * 1024)

    tracemalloc.start()
    try:
        result = module._plain_json(oversized)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert result is module._UNAVAILABLE
    assert peak < 128 * 1024


def test_llamaindex_context_commit_requires_all_message_admissions() -> None:
    module = import_module("semantic_layer.llamaindex_adapter")

    class RejectSecondMessageSink:
        def __init__(self) -> None:
            self.messages = 0
            self.next_id = 0
            self.records: list[dict[str, Any]] = []

        def record(self, record: dict[str, Any]) -> Any:
            self.records.append(record)
            self.next_id += 1
            if record["name"] == "llamaindex.message":
                self.messages += 1
                if self.messages == 2:
                    return module.AdmissionReceipt(False, "injected_rejection")
            return module.AdmissionReceipt(
                True,
                record_id=f"record-{self.next_id}",
            )

    source = module._Source(CallbackManager([]), "0.14.23")
    source._sink = RejectSecondMessageSink()
    trace = module._Trace(
        identity={"run_id": "run", "trace_id": "trace", "operation_id": "operation"},
        turn_id="turn",
        conversation="conversation",
        trace_id="trace",
        root=module.AdmissionReceipt(True, record_id="root"),
    )
    source._event_traces["root"] = trace
    source._event_start(
        CBEventType.LLM,
        {
            EventPayload.MESSAGES: [
                ChatMessage(role=MessageRole.USER, content="A"),
                ChatMessage(role=MessageRole.TOOL, content="B"),
            ]
        },
        "request-a",
        "root",
        {},
    )
    assert trace.model_context_length == 0
    assert trace.model_context_digest is None
    assert trace.model_context_request_ref is None
    first_request = next(
        record
        for record in source._sink.records
        if record["native_identity"] == "request-a"
    )
    assert "context_refs" not in first_request["semantic"]
    assert "context_base_ref" not in first_request["semantic"]

    source._event_start(
        CBEventType.LLM,
        {
            EventPayload.MESSAGES: [
                ChatMessage(role=MessageRole.USER, content="A"),
                ChatMessage(role=MessageRole.TOOL, content="B"),
                ChatMessage(role=MessageRole.USER, content="C"),
            ]
        },
        "request-b",
        "root",
        {},
    )
    second_request = next(
        record
        for record in source._sink.records
        if record["native_identity"] == "request-b"
    )
    assert len(second_request["semantic"]["context_refs"]) == 3
    assert "context_base_ref" not in second_request["semantic"]
    assert trace.model_context_length == 3
    assert trace.model_context_digest is not None
    assert trace.model_context_request_ref is not None


def test_llamaindex_request_rejection_preserves_fifo_pending_provenance() -> None:
    module = import_module("semantic_layer.llamaindex_adapter")

    class RejectFirstRequestSink:
        def __init__(self) -> None:
            self.next_id = 0
            self.rejected = False
            self.records: list[tuple[dict[str, Any], Any]] = []

        def record(self, record: dict[str, Any]) -> Any:
            self.next_id += 1
            if record["name"] == "llamaindex.llm" and not self.rejected:
                self.rejected = True
                receipt = module.AdmissionReceipt(False, "injected_rejection")
            else:
                receipt = module.AdmissionReceipt(
                    True,
                    record_id=f"record-{self.next_id}",
                )
            self.records.append((record, receipt))
            return receipt

    source = module._Source(CallbackManager([]), "0.14.23")
    source._sink = RejectFirstRequestSink()
    source._active = True
    trace = module._Trace(
        identity={"run_id": "run", "trace_id": "trace", "operation_id": "operation"},
        turn_id="turn",
        conversation="conversation",
        trace_id="trace",
        root=module.AdmissionReceipt(True, record_id="root"),
    )
    source._traces[trace.trace_id] = trace
    source._event_traces["root"] = trace
    source._workflow_user_input(trace, {"user_msg": "identical"})
    source._workflow_user_input(trace, {"user_msg": "identical"})
    pending_ids = [pending[1] for pending in trace.pending_context_messages]
    assert [pending[2] for pending in trace.pending_context_messages] == [1, 2]

    source._event_start(
        CBEventType.LLM,
        {
            EventPayload.MESSAGES: [
                ChatMessage(role=MessageRole.USER, content="identical")
            ]
        },
        "request-rejected",
        "root",
        {},
    )
    assert [pending[1] for pending in trace.pending_context_messages] == pending_ids
    assert trace.model_context_length == 0
    assert trace.model_context_request_ref is None

    source._event_start(
        CBEventType.LLM,
        {
            EventPayload.MESSAGES: [
                ChatMessage(role=MessageRole.USER, content="identical"),
                ChatMessage(role=MessageRole.USER, content="identical"),
            ]
        },
        "request-accepted",
        "root",
        {},
    )
    accepted_request = next(
        record
        for record, _receipt in source._sink.records
        if record["native_identity"] == "request-accepted"
    )
    assert accepted_request["semantic"]["context_refs"] == pending_ids
    assert trace.pending_context_messages == []
    assert trace.model_context_length == 2


def test_llamaindex_context_digest_detects_tool_metadata_change_and_native_is_compact() -> None:
    module = import_module("semantic_layer.llamaindex_adapter")

    class Sink:
        def __init__(self) -> None:
            self.next_id = 0
            self.records: list[dict[str, Any]] = []

        def record(self, record: dict[str, Any]) -> Any:
            self.next_id += 1
            self.records.append(record)
            return module.AdmissionReceipt(True, record_id=f"record-{self.next_id}")

    source = module._Source(CallbackManager([]), "0.14.23")
    source._sink = Sink()
    trace = module._Trace(
        identity={"run_id": "run", "trace_id": "trace", "operation_id": "operation"},
        turn_id="turn",
        conversation="conversation",
        trace_id="trace",
        root=module.AdmissionReceipt(True, record_id="root"),
    )
    source._event_traces["root"] = trace

    def history(call_id: str, argument: int) -> list[ChatMessage]:
        return [
            ChatMessage(role=MessageRole.USER, content="same question"),
            ChatMessage(
                role=MessageRole.ASSISTANT,
                content="same visible text",
                additional_kwargs={
                    "tool_calls": [
                        {
                            "id": call_id,
                            "function": {
                                "name": "lookup",
                                "arguments": json.dumps({"value": argument}),
                            },
                        }
                    ]
                },
            ),
        ]

    source._event_start(
        CBEventType.LLM,
        {EventPayload.MESSAGES: history("call-a", 1)},
        "request-a",
        "root",
        {},
    )
    first_digest = trace.model_context_digest
    source._event_start(
        CBEventType.LLM,
        {EventPayload.MESSAGES: history("call-b", 2)},
        "request-b",
        "root",
        {},
    )
    requests = [
        record for record in source._sink.records if record["name"] == "llamaindex.llm"
    ]
    assert first_digest != trace.model_context_digest
    assert "context_base_ref" not in requests[1]["semantic"]
    assert len(requests[1]["semantic"]["context_refs"]) == 2
    assert requests[1]["native"] == {
        "event_type": "llm",
        "message_count": 2,
        "context_snapshot": "complete",
    }
    assert all(
        forbidden not in json.dumps(requests[1]["native"])
        for forbidden in ('"messages"', '"message_history"', '"payload"')
    )
    changed_message = next(
        record
        for record in source._sink.records
        if record["native_identity"] == "request-b::message::1"
    )
    assert changed_message["native"]["additional_kwargs"]["tool_calls"][0]["id"] == (
        "call-b"
    )


def test_llamaindex_non_text_context_is_exact_or_explicitly_unavailable() -> None:
    module = import_module("semantic_layer.llamaindex_adapter")
    source = module._Source(CallbackManager([]), "0.14.23")
    trace = module._Trace(
        identity={"run_id": "run", "trace_id": "trace", "operation_id": "operation"},
        turn_id="turn",
        conversation="conversation",
        trace_id="trace",
        root=module.AdmissionReceipt(True, record_id="root"),
    )
    url_plan = source._plan_messages(
        trace,
        [
            ChatMessage(
                role=MessageRole.USER,
                blocks=[ImageBlock(url="https://x.example/image.png")],
            )
        ],
    )
    assert url_plan.complete is False
    assert url_plan.context_base_ref is None

    binary_a = source._plan_messages(
        trace,
        [
            ChatMessage(
                role=MessageRole.USER,
                blocks=[ImageBlock(image=b"image-a")],
            )
        ],
    )
    assert binary_a.complete is True
    assert binary_a.digest is not None
    trace.model_context_length = 1
    trace.model_context_digest = binary_a.digest
    trace.model_context_request_ref = "request-a"
    binary_b = source._plan_messages(
        trace,
        [
            ChatMessage(
                role=MessageRole.USER,
                blocks=[ImageBlock(image=b"image-b")],
            )
        ],
    )
    assert binary_b.complete is True
    assert binary_b.digest != binary_a.digest
    assert binary_b.context_base_ref is None
    assert binary_b.selected_from == 0


def test_llamaindex_context_digest_is_total_for_surrogate_text() -> None:
    module = import_module("semantic_layer.llamaindex_adapter")
    source = module._Source(CallbackManager([]), "0.14.23")
    trace = module._Trace(
        identity={"run_id": "run", "trace_id": "trace", "operation_id": "operation"},
        turn_id="turn",
        conversation="conversation",
        trace_id="trace",
        root=module.AdmissionReceipt(True, record_id="root"),
    )
    plan = source._plan_messages(
        trace,
        [ChatMessage(role=MessageRole.USER, content="bad\ud800")],
    )
    assert plan.complete is True
    assert type(plan.digest) is str


def test_llamaindex_agent_input_native_does_not_copy_cumulative_history() -> None:
    module = import_module("semantic_layer.llamaindex_adapter")

    class Sink:
        def __init__(self) -> None:
            self.recorded: dict[str, Any] | None = None

        def record(self, record: dict[str, Any]) -> Any:
            self.recorded = record
            return module.AdmissionReceipt(True, record_id="record-1")

    source = module._Source(CallbackManager([]), "0.14.23")
    source._sink = Sink()
    source._active = True
    trace = module._Trace(
        identity={"run_id": "run", "trace_id": "trace", "operation_id": "operation"},
        turn_id="turn",
        conversation="conversation",
        trace_id="trace",
        root=module.AdmissionReceipt(True, record_id="root"),
    )
    source._traces[trace.trace_id] = trace
    source._workflow_event(
        trace,
        AgentInput(
            input=[
                ChatMessage(role=MessageRole.USER, content=f"message-{index}")
                for index in range(128)
            ],
            current_agent_name="fixture-agent",
        ),
    )
    assert source._sink.recorded is not None
    assert source._sink.recorded["native"] == {
        "event_type": "AgentInput",
        "message_count": 128,
        "current_agent_name": "fixture-agent",
    }
    assert "input" not in json.dumps(source._sink.recorded["native"])


@pytest.mark.parametrize(
    "limited_snapshot",
    [
        {"native_type": "ChatMessage", "omitted": "resource_limit"},
        {"native_type": "ChatMessage", "reference": "$/0"},
    ],
    ids=["resource-limit", "cycle-reference"],
)
def test_llamaindex_incomplete_snapshot_reports_gap_and_disables_context_base(
    monkeypatch: pytest.MonkeyPatch,
    limited_snapshot: dict[str, str],
) -> None:
    module = import_module("semantic_layer.llamaindex_adapter")
    original_snapshot = module.native_snapshot

    class Sink:
        def __init__(self) -> None:
            self.next_id = 0
            self.records: list[dict[str, Any]] = []

        def record(self, record: dict[str, Any]) -> Any:
            self.next_id += 1
            self.records.append(record)
            return module.AdmissionReceipt(True, record_id=f"record-{self.next_id}")

    def resource_limited(value: Any) -> Any:
        if type(value) in {list, tuple}:
            return [limited_snapshot]
        return original_snapshot(value)

    source = module._Source(CallbackManager([]), "0.14.23")
    source._sink = Sink()
    trace = module._Trace(
        identity={"run_id": "run", "trace_id": "trace", "operation_id": "operation"},
        turn_id="turn",
        conversation="conversation",
        trace_id="trace",
        root=module.AdmissionReceipt(True, record_id="root"),
        model_context_length=1,
        model_context_digest="prior",
        model_context_request_ref="request-prior",
    )
    source._event_traces["root"] = trace
    monkeypatch.setattr(module, "native_snapshot", resource_limited)
    source._event_start(
        CBEventType.LLM,
        {
            EventPayload.MESSAGES: [
                ChatMessage(role=MessageRole.USER, content="oversized")
            ]
        },
        "request-limited",
        "root",
        {},
    )
    request = next(
        record
        for record in source._sink.records
        if record["name"] == "llamaindex.llm"
    )
    gaps = [
        record
        for record in source._sink.records
        if record["semantic"]["type"] == "capture.gap"
    ]
    assert "context_refs" not in request["semantic"]
    assert "context_base_ref" not in request["semantic"]
    assert len(gaps) == 1
    assert gaps[0]["semantic"]["reason"] == "model_context_snapshot_unavailable"
    assert gaps[0]["semantic"]["count"] == 1
    assert gaps[0]["parent_record_id"] == "record-1"
    assert trace.model_context_length == 1
    assert trace.model_context_digest == "prior"
    assert trace.model_context_request_ref == "request-prior"


def test_llamaindex_committed_context_state_is_constant_size() -> None:
    module = import_module("semantic_layer.llamaindex_adapter")

    class Sink:
        def __init__(self) -> None:
            self.next_id = 0

        def record(self, _record: dict[str, Any]) -> Any:
            self.next_id += 1
            return module.AdmissionReceipt(True, record_id=f"record-{self.next_id}")

    source = module._Source(CallbackManager([]), "0.14.23")
    source._sink = Sink()
    trace = module._Trace(
        identity={"run_id": "run", "trace_id": "trace", "operation_id": "operation"},
        turn_id="turn",
        conversation="conversation",
        trace_id="trace",
        root=module.AdmissionReceipt(True, record_id="root"),
    )
    source._event_traces["root"] = trace
    history: list[ChatMessage] = []
    for index in range(256):
        history.append(ChatMessage(role=MessageRole.USER, content=f"turn-{index}"))
        source._event_start(
            CBEventType.LLM,
            {EventPayload.MESSAGES: history},
            f"request-{index}",
            "root",
            {},
        )

    assert trace.model_context_length == len(history)
    assert type(trace.model_context_digest) is str
    assert type(trace.model_context_request_ref) is str
    assert not hasattr(trace, "model_context_messages")
    assert not hasattr(trace, "model_context_refs")
    assert len(trace.pending_context_messages) <= module._MAX_PENDING_CONTEXT_MESSAGES


def test_llamaindex_workflow_prefix_is_bounded_without_copying_full_text() -> None:
    module = import_module("semantic_layer.llamaindex_adapter")
    oversized = "x" * (1024 * 1024)

    tracemalloc.start()
    try:
        prefix, byte_count, truncated = module._bounded_utf8_prefix(oversized, 64)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert prefix == "x" * 64
    assert byte_count == 64
    assert truncated is True
    assert peak < 128 * 1024


@pytest.mark.asyncio
async def test_llamaindex_interrupted_stream_retains_one_bounded_partial_state(
    tmp_path: Path,
) -> None:
    class Handler:
        async def stream_events(self) -> Any:
            yield AgentStream(
                delta="partial",
                response="partial",
                current_agent_name="fixture-agent",
            )
            yield AgentStream(
                delta=" response",
                response="partial response",
                current_agent_name="fixture-agent",
            )

        def __await__(self) -> Any:
            async def complete() -> object:
                return object()

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "stream"}
            return Handler()

    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-partial-stream")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-partial",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        unrelated_id = callback.on_event_start(
            CBEventType.LLM,
            payload={
                EventPayload.MESSAGES: [
                    ChatMessage(role=MessageRole.USER, content="Earlier unrelated call")
                ],
                EventPayload.SERIALIZED: {"model_name": "fixture-model"},
            },
            event_id="unrelated-model",
        )
        callback.on_event_end(
            CBEventType.LLM,
            payload={
                EventPayload.RESPONSE: ChatResponse(
                    message=ChatMessage(
                        role=MessageRole.ASSISTANT,
                        content="Earlier unrelated response",
                    )
                )
            },
            event_id=unrelated_id,
        )
        stream = turn.run(Agent(), user_msg="stream").stream_events()
        first = await anext(stream)
        assert first.response == "partial"
        await stream.aclose()

    records = _trace_records(capture.shutdown().artifact_path)
    partial_states = [
        record
        for record in records
        if record["kind"] == "state"
        and record["data"]["type"] == "state.stream_partial"
    ]
    assert len(partial_states) == 1
    assert partial_states[0]["data"]["value"] == {
        "content": "partial",
        "status": "cancelled",
    }
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.asyncio
async def test_llamaindex_interrupted_stream_reports_one_partial_truncation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        import_module("semantic_layer.llamaindex_adapter"),
        "_MAX_WORKFLOW_STREAM_PARTIAL_BYTES",
        8,
    )

    class Handler:
        async def stream_events(self) -> Any:
            yield AgentStream(
                delta="more than eight bytes",
                response="more than eight bytes",
                current_agent_name="fixture-agent",
            )

        def __await__(self) -> Any:
            async def complete() -> object:
                return object()

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            del kwargs
            return Handler()

    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-truncated-stream")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-truncated",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        stream = turn.run(Agent(), user_msg="stream").stream_events()
        await anext(stream)
        await stream.aclose()

    records = _trace_records(capture.shutdown().artifact_path)
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "workflow_stream_partial_truncated"
    partial_states = [
        record
        for record in records
        if record["kind"] == "state"
        and record["data"]["type"] == "state.stream_partial"
    ]
    assert len(partial_states) == 1
    assert partial_states[0]["data"]["value"] == {
        "content": "more tha",
        "status": "cancelled",
        "truncated": True,
    }
    assert partial_states[0]["seq"] < losses[0]["seq"]


@pytest.mark.asyncio
async def test_llamaindex_retains_unmatched_workflow_output_without_guessing(
    tmp_path: Path,
) -> None:
    callback_message = ChatMessage(
        role=MessageRole.ASSISTANT,
        content="Same visible content.",
    )
    returned_message = ChatMessage(
        role=MessageRole.ASSISTANT,
        content="Same visible content.",
    )
    result = AgentOutput(
        response=returned_message,
        current_agent_name="fixture-agent",
    )

    class Handler:
        def __await__(self) -> Any:
            async def complete() -> AgentOutput:
                return result

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-output-gap")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-output-gap",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        model_id = callback.on_event_start(
            CBEventType.LLM,
            payload={
                EventPayload.MESSAGES: [ChatMessage(role=MessageRole.USER, content="answer")],
                EventPayload.SERIALIZED: {"model_name": "fixture-model"},
            },
            event_id="model-output-gap",
        )
        callback.on_event_end(
            CBEventType.LLM,
            payload={
                EventPayload.RESPONSE: ChatResponse(message=callback_message),
            },
            event_id=model_id,
        )
        handler = turn.run(Agent(), user_msg="answer")
        assert await handler is result

    records = _trace_records(capture.shutdown().artifact_path)
    outcome = _one(records, "run.outcome")
    assert outcome["data"]["output"]["response"]["blocks"][0]["text"] == ("Same visible content.")
    assert "links" not in outcome
    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == [
        "workflow_result_model_response_correlation_unavailable"
    ]


@pytest.mark.asyncio
async def test_llamaindex_uses_agent_output_response_as_reasoning_fallback(
    tmp_path: Path,
) -> None:
    message = ChatMessage(
        role=MessageRole.ASSISTANT,
        blocks=[
            ThinkingBlock(content="Check the units."),
            TextBlock(text="The answer is 42."),
        ],
    )
    result = AgentOutput(
        response=message,
        current_agent_name="fixture-agent",
    )

    class Handler:
        def __await__(self) -> Any:
            async def complete() -> AgentOutput:
                return result

            return complete().__await__()

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-output-fallback")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-output-fallback",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        assert await turn.run(Agent(), user_msg="answer") is result

    records = _trace_records(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    assert responses == []
    output_state = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"].get("type") == "state.agent_output"
    )
    assert output_state["data"]["value"]["content"] == "The answer is 42."
    assert output_state["data"]["value"]["reasoning"] == [
        {"type": "text", "text": "Check the units."}
    ]
    outcome = _one(records, "run.outcome")
    assert outcome["links"] == [
        {"type": "derived_from", "record": output_state["id"]}
    ]
    assert [
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
    ] == ["workflow_result_model_response_correlation_unavailable"]


@pytest.mark.asyncio
async def test_llamaindex_streamed_agent_output_falls_back_when_terminal_is_omitted(
    tmp_path: Path,
) -> None:
    message = ChatMessage(role=MessageRole.ASSISTANT, content="The answer is 42.")
    output = AgentOutput(response=message, current_agent_name="fixture-agent")

    class Handler:
        async def stream_events(self) -> Any:
            yield AgentStream(
                delta="",
                response="",
                current_agent_name="fixture-agent",
                thinking_delta="Check ",
            )
            yield AgentStream(
                delta="The answer is 42.",
                response="The answer is 42.",
                current_agent_name="fixture-agent",
                thinking_delta="the units.",
            )
            yield output

        def __await__(self) -> Any:
            raise AssertionError("stream-only caller must not await the handler")

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-terminal-fallback")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-terminal-fallback",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        streamed = [
            event
            async for event in turn.run(Agent(), user_msg="answer").stream_events()
        ]
        assert streamed[-1] is output

    records = _trace_records(capture.shutdown().artifact_path)
    assert not [record for record in records if record["kind"] == "model.response"]
    output_state = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"].get("type") == "state.agent_output"
    )
    assert output_state["data"]["value"]["content"] == "The answer is 42."
    assert [
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
    ] == [
        "workflow_result_model_response_correlation_unavailable",
        "workflow_stream_reasoning_model_response_correlation_unavailable",
    ]


@pytest.mark.asyncio
async def test_llamaindex_terminal_omission_does_not_infer_active_model_identity(
    tmp_path: Path,
) -> None:
    message = ChatMessage(role=MessageRole.ASSISTANT, content="The answer is 42.")
    output = AgentOutput(response=message, current_agent_name="fixture-agent")
    manager = CallbackManager([])
    callback: Any = None

    class Handler:
        async def stream_events(self) -> Any:
            callback.on_event_start(
                CBEventType.LLM,
                payload={
                    EventPayload.MESSAGES: [
                        ChatMessage(role=MessageRole.USER, content="answer")
                    ],
                    EventPayload.SERIALIZED: {"model_name": "fixture-model"},
                },
                event_id="terminal-omitted-model",
            )
            yield AgentStream(
                delta="The answer is 42.",
                response="The answer is 42.",
                current_agent_name="fixture-agent",
                thinking_delta="Check the units.",
            )
            yield output

        def __await__(self) -> Any:
            raise AssertionError("stream-only caller must not await the handler")

    class Agent:
        def run(self, **kwargs: Any) -> Handler:
            assert kwargs == {"user_msg": "answer"}
            return Handler()

    capture = initialize(output=tmp_path, service_name="llamaindex-exact-terminal")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-exact-terminal",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        streamed = [
            event
            async for event in turn.run(Agent(), user_msg="answer").stream_events()
        ]
        assert streamed[-1] is output

    records = _trace_records(capture.shutdown().artifact_path)
    request = _one(records, "model.request")
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 1
    omitted = responses[0]
    output_state = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"].get("type") == "state.agent_output"
    )
    assert output_state["data"]["value"]["content"] == "The answer is 42."
    assert omitted["links"] == [
        {"type": "result_of", "record": request["id"]}
    ]
    assert [
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
    ] == [
        "workflow_result_model_response_correlation_unavailable",
        "workflow_stream_reasoning_model_response_correlation_unavailable",
    ]


@pytest.mark.asyncio
async def test_llamaindex_custom_workflow_captures_direct_function_tool_execution(
    tmp_path: Path,
) -> None:
    observed_outputs: list[ToolOutput] = []

    def add(a: int, b: int) -> int:
        return a + b

    tool = FunctionTool.from_defaults(fn=add, name="add")

    class DirectToolWorkflow(Workflow):
        @step
        async def execute(self, event: StartEvent) -> StopEvent:
            observed_outputs.append(tool(a=21, b=21))
            observed_outputs.append(tool(a=observed_outputs[-1].raw_output, b=8))
            return StopEvent(result=observed_outputs[-1])

    manager = CallbackManager([])
    workflow = DirectToolWorkflow(timeout=10)
    capture = initialize(output=tmp_path, service_name="llamaindex-direct-tool")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-direct-tool",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        model_id = callback.on_event_start(
            CBEventType.LLM,
            payload={
                EventPayload.MESSAGES: [
                    ChatMessage(role=MessageRole.USER, content="Add 21 and 21")
                ],
            },
            event_id="model-direct-tool",
        )
        callback.on_event_end(
            CBEventType.LLM,
            payload={
                EventPayload.RESPONSE: ChatResponse(
                    message=ChatMessage(
                        role=MessageRole.ASSISTANT,
                        additional_kwargs={
                            "semantic_tool_calls": [
                                {
                                    "tool_id": "provider-call-add",
                                    "tool_name": "add",
                                    "tool_kwargs": {"a": 21, "b": 21},
                                },
                                {
                                    "tool_id": "provider-call-add-again",
                                    "tool_name": "add",
                                    "tool_kwargs": {"a": 42, "b": 8},
                                },
                            ]
                        },
                    )
                )
            },
            event_id=model_id,
        )
        result = await turn.run(workflow)

    assert result is observed_outputs[-1]
    records = _trace_records(capture.shutdown().artifact_path)
    calls = [record for record in records if record["kind"] == "tool.call"]
    tool_results = [record for record in records if record["kind"] == "tool.result"]
    assert [call["data"]["name"] for call in calls] == ["add", "add"]
    assert [call["data"]["input"] for call in calls] == [
        {"a": 21, "b": 21},
        {"a": 42, "b": 8},
    ]
    assert all(call["data"]["native_call_id"].startswith("FunctionTool.call-") for call in calls)
    assert all("links" not in call for call in calls)
    assert [tool_result["data"]["output"] for tool_result in tool_results] == [42, 50]
    assert [tool_result["links"] for tool_result in tool_results] == [
        [{"type": "result_of", "record": call["id"]}] for call in calls
    ]

    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == [
        "tool_proposal_correlation_unavailable"
    ]


@pytest.mark.asyncio
async def test_llamaindex_direct_function_tool_preserves_exception_identity(
    tmp_path: Path,
) -> None:
    expected = RuntimeError("tool exploded")

    def fail(
        a: int = Field(default=2),
        b: int = Field(default=3),
    ) -> int:
        del a, b
        raise expected

    tool = FunctionTool.from_defaults(
        fn=fail,
        name="fail",
        partial_params={"b": 40},
    )
    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-tool-error")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )

    async with installed.turn(
        conversation_id="conversation-tool-error",
        turn_id="turn-0",
        turn_index=0,
    ):
        with pytest.raises(RuntimeError) as raised:
            tool()
        assert raised.value is expected

    records = _trace_records(capture.shutdown().artifact_path)
    call = _one(records, "tool.call")
    result = _one(records, "tool.result")
    assert call["data"]["input"] == {"a": 2, "b": 40}
    assert result["data"]["status"] == "failed"
    assert result["data"]["error"]["message"] == "tool exploded"
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.asyncio
async def test_llamaindex_function_tool_does_not_attach_from_an_unbound_thread(
    tmp_path: Path,
) -> None:
    tool = FunctionTool.from_defaults(fn=lambda value: value + 1, name="increment")
    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-thread-ownership")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )
    results: list[ToolOutput] = []

    async with installed.turn(
        conversation_id="conversation-thread-ownership",
        turn_id="turn-0",
        turn_index=0,
    ):
        thread = threading.Thread(target=lambda: results.append(tool(value=41)))
        thread.start()
        thread.join()

    assert results[0].raw_output == 42
    records = _trace_records(capture.shutdown().artifact_path)
    assert not [
        record
        for record in records
        if record["kind"] in {"tool.call", "tool.result"}
    ]


@pytest.mark.asyncio
async def test_llamaindex_custom_workflow_captures_direct_async_function_tool(
    tmp_path: Path,
) -> None:
    def add(
        a: int = Field(default=2),
        b: int = Field(default=3),
    ) -> int:
        return a + b

    tool = FunctionTool.from_defaults(
        fn=add,
        name="add",
        partial_params={"b": 40},
    )

    class DirectAsyncToolWorkflow(Workflow):
        @step
        async def execute(self, event: StartEvent) -> StopEvent:
            return StopEvent(result=await tool.acall())

    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-direct-async-tool")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )
    async with installed.turn(
        conversation_id="conversation-direct-async-tool",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        result = await turn.run(DirectAsyncToolWorkflow(timeout=10))

    assert result.raw_output == 42
    records = _trace_records(capture.shutdown().artifact_path)
    call = _one(records, "tool.call")
    tool_result = _one(records, "tool.result")
    assert call["data"]["input"] == {"a": 2, "b": 40}
    assert call["data"]["native_call_id"].startswith("FunctionTool.acall-")
    assert tool_result["data"]["output"] == 42
    assert tool_result["links"] == [{"type": "result_of", "record": call["id"]}]


@pytest.mark.asyncio
async def test_llamaindex_async_tool_span_and_workflow_events_emit_one_operation(
    tmp_path: Path,
) -> None:
    def add(
        a: int = Field(default=2),
        b: int = Field(default=3),
    ) -> int:
        return a + b

    tool = FunctionTool.from_defaults(
        fn=add,
        name="add",
        partial_params={"b": 40},
    )
    native_call = ToolCall(
        tool_name="add",
        tool_kwargs={},
        tool_id="call-overlap",
    )

    class ExactToolEventWorkflow(Workflow):
        @step
        async def begin(self, event: StartEvent) -> ToolCall:
            return native_call

        @step
        async def call_tool(self, context: Context, event: ToolCall) -> StopEvent:
            context.write_event_to_stream(event)
            output = await tool.acall(**event.tool_kwargs)
            context.write_event_to_stream(
                ToolCallResult(
                    tool_name=event.tool_name,
                    tool_kwargs=event.tool_kwargs,
                    tool_id=event.tool_id,
                    tool_output=output,
                    return_direct=False,
                )
            )
            return StopEvent(result=output)

    manager = CallbackManager([])
    capture = initialize(output=tmp_path, service_name="llamaindex-async-overlap")
    installed = capture.instrument(
        adapter=llamaindex_adapter(version="0.14.23"),
        client=manager,
    )
    async with installed.turn(
        conversation_id="conversation-async-overlap",
        turn_id="turn-0",
        turn_index=0,
    ) as turn:
        callback = manager.handlers[-1]
        model_id = callback.on_event_start(
            CBEventType.LLM,
            payload={EventPayload.MESSAGES: []},
            event_id="model-async-overlap",
        )
        callback.on_event_end(
            CBEventType.LLM,
            payload={
                EventPayload.RESPONSE: ChatResponse(
                    message=ChatMessage(
                        role=MessageRole.ASSISTANT,
                        additional_kwargs={
                            "semantic_tool_calls": [
                                {
                                    "tool_id": "call-overlap",
                                    "tool_name": "add",
                                    "tool_kwargs": {},
                                }
                            ]
                        },
                    )
                )
            },
            event_id=model_id,
        )
        handler = turn.run(ExactToolEventWorkflow(timeout=10))
        streamed = [event async for event in handler.stream_events()]
        result = await handler

    assert streamed[0] is native_call
    assert result.raw_output == 42
    records = _trace_records(capture.shutdown().artifact_path)
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    calls = [record for record in records if record["kind"] == "tool.call"]
    tool_results = [record for record in records if record["kind"] == "tool.result"]
    assert len(proposals) == len(calls) == len(tool_results) == 1
    assert calls[0]["data"]["native_call_id"] == "call-overlap"
    assert calls[0]["data"]["input"] == {"a": 2, "b": 40}
    assert calls[0]["links"] == [
        {"type": "derived_from", "record": proposals[0]["id"]}
    ]
    assert tool_results[0]["data"]["output"] == 42
    assert tool_results[0]["links"] == [
        {"type": "result_of", "record": calls[0]["id"]}
    ]
    assert not [record for record in records if record["kind"] == "loss"]
