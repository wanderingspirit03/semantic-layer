from __future__ import annotations

import asyncio
import json
import operator
import subprocess
import sys
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import Annotated, Any, Literal, TypedDict
from unittest.mock import AsyncMock, MagicMock

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from langchain_core.language_models.fake_chat_models import (
    FakeListChatModel,
    FakeMessagesListChatModel,
)
from langchain_core.messages import (
    AIMessage,
    AIMessageChunk,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)
from langchain_core.outputs import ChatGenerationChunk
from langchain_core.runnables import RunnableConfig, RunnableLambda
from langchain_core.tools import tool as langchain_tool
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.prebuilt import ToolNode
from langgraph.types import Command, interrupt
from mcp.types import Tool as MCPTool
from pydantic import BaseModel
from strands import Agent as StrandsAgent
from strands import tool as strands_tool
from strands.agent.conversation_manager import NullConversationManager
from strands.hooks import BeforeInvocationEvent, BeforeToolCallEvent
from strands.models import Model as StrandsModel
from strands.tools.mcp import MCPAgentTool
from strands.tools.mcp.mcp_types import MCPToolResult
from strands.types.agent import ConcurrentInvocationMode
from strands.types.exceptions import ContextWindowOverflowException
from typing_extensions import TypedDict as ExtendedTypedDict

from semantic_layer import (
    initialize,
    langgraph_adapter,
    reset_capture_for_tests,
    strands_adapter,
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


def _semantic_records(artifact: str) -> list[dict[str, Any]]:
    records = [
        json.loads(line)
        for line in (Path(artifact) / "trace.jsonl").read_text().splitlines()
    ]
    for record in records:
        _TRACE_VALIDATOR.validate(record)
    return records


def _record(records: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    return next(record for record in records if record["kind"] == kind)


def test_semantic_layer_import_does_not_require_langgraph() -> None:
    script = """
import builtins

original_import = builtins.__import__


def reject_langgraph(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "langgraph" or name.startswith("langgraph."):
        raise ModuleNotFoundError("langgraph blocked for base-package smoke")
    return original_import(name, globals, locals, fromlist, level)


builtins.__import__ = reject_langgraph

import semantic_layer

assert callable(semantic_layer.langgraph_adapter)
"""
    subprocess.run([sys.executable, "-c", script], check=True)


class _GraphState(TypedDict):
    values: Annotated[list[int], operator.add]


class _UsageFakeChatModel(FakeListChatModel):
    def _stream(self, messages: Any, **kwargs: Any) -> Any:
        for index, content in enumerate("ok"):
            yield ChatGenerationChunk(
                message=AIMessageChunk(
                    content=content,
                    additional_kwargs=(
                        {"reasoning_content": "Checked the exposed graph context."}
                        if index == 1
                        else {}
                    ),
                    chunk_position="last" if index == 1 else None,
                    usage_metadata=(
                        {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3}
                        if index == 1
                        else None
                    ),
                )
            )


def test_langgraph_capture_projects_compact_exact_semantics(tmp_path: Path) -> None:
    model = _UsageFakeChatModel(responses=["unused"])

    @langchain_tool
    def lookup() -> str:
        """Return exact fixture output."""

        return "tool-output"

    def exercise(_state: _GraphState, config: RunnableConfig) -> _GraphState:
        assert "".join(chunk.content for chunk in model.stream("hello", config=config)) == "ok"
        assert lookup.invoke({}, config=config) == "tool-output"
        return {"values": [1]}

    builder = StateGraph(_GraphState)
    builder.add_node("exercise", exercise)
    builder.add_edge(START, "exercise")
    builder.add_edge("exercise", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-semantic-fixture")
    capture.instrument(
        adapter=langgraph_adapter(
            version=distribution_version("langgraph"),
        ),
        client=graph,
    )

    result = graph.invoke(
        {"values": []},
        {"configurable": {"thread_id": "conversation-langgraph"}},
    )
    assert result == {"values": [1]}

    records = _semantic_records(capture.shutdown().artifact_path)
    kinds = [record["kind"] for record in records]
    assert kinds.count("run.start") == kinds.count("run.outcome") == 1
    assert "loss" not in kinds
    states = [record for record in records if record["kind"] == "state"]
    assert [record["data"]["type"] for record in states] == [
        "langgraph.step.output",
        "langgraph.final_state",
    ]
    assert [record["data"]["value"] for record in states] == [
        {"values": [1]},
        {"values": [1]},
    ]
    assert states[0]["data"]["version"].startswith("exercise:")
    assert states[1]["data"]["version"] != states[0]["data"]["version"]
    scopes = [record for record in records if record["kind"] == "scope"]
    assert scopes
    assert all(
        "messages" not in json.dumps(record.get("data", {})) for record in records
        if record["kind"] in {"state", "run.outcome"}
    )

    request = _record(records, "model.request")
    response = _record(records, "model.response")
    context = _record(records, "message")
    assert request["data"]["context_refs"] == [context["id"]]
    assert context["data"]["role"] == "user"
    assert response["links"] == [{"type": "result_of", "record": request["id"]}]
    assert response["data"]["usage"] == {"input_tokens": 2, "output_tokens": 1}
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Checked the exposed graph context."}
    ]

    call = _record(records, "tool.call")
    tool_result = _record(records, "tool.result")
    assert "links" not in call
    assert tool_result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert call["data"]["name"] == "lookup"
    assert call["data"]["input"] == {}
    assert tool_result["data"]["output"] == "tool-output"
    assert tool_result["data"]["native_call_id"] == call["data"]["native_call_id"]


def test_langgraph_model_tool_proposal_is_not_duplicated_by_execution(
    tmp_path: Path,
) -> None:
    model = FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {"query": "exact"},
                        "id": "langgraph-call-1",
                        "type": "tool_call",
                    }
                ],
            )
        ]
    )

    @langchain_tool
    def lookup(query: str) -> str:
        """Return exact fixture output."""

        assert query == "exact"
        return "tool-output"

    def exercise(_state: _GraphState, config: RunnableConfig) -> _GraphState:
        response = model.invoke("use lookup", config=config)
        tool_message = lookup.invoke(
            {**response.tool_calls[0], "type": "tool_call"},
            config=config,
        )
        assert tool_message.content == "tool-output"
        return {"values": [1]}

    builder = StateGraph(_GraphState)
    builder.add_node("exercise", exercise)
    builder.add_edge(START, "exercise")
    builder.add_edge("exercise", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-proposal-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )
    graph.invoke({"values": []})

    records = _semantic_records(capture.shutdown().artifact_path)
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    calls = [record for record in records if record["kind"] == "tool.call"]
    results = [record for record in records if record["kind"] == "tool.result"]
    assert len(proposals) == len(calls) == len(results) == 1
    assert calls[0]["links"] == [
        {"type": "derived_from", "record": proposals[0]["id"]}
    ]
    assert results[0]["links"] == [{"type": "result_of", "record": calls[0]["id"]}]
    assert not [record for record in records if record["kind"] == "loss"]


def test_langgraph_reuses_exact_context_and_keeps_terminal_state_minimal(
    tmp_path: Path,
) -> None:
    model = FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {"query": "exact"},
                        "id": "langgraph-call-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="finished"),
        ]
    )

    @langchain_tool
    def lookup(query: str) -> str:
        """Return exact fixture output."""

        assert query == "exact"
        return "tool-output"

    class MessageState(TypedDict):
        messages: Annotated[list[Any], operator.add]

    def first_model(state: MessageState, config: RunnableConfig) -> MessageState:
        return {"messages": [model.invoke(state["messages"], config=config)]}

    def execute_tool(state: MessageState, config: RunnableConfig) -> MessageState:
        proposal = state["messages"][-1].tool_calls[0]
        return {
            "messages": [
                lookup.invoke(
                    {**proposal, "type": "tool_call"},
                    config=config,
                )
            ]
        }

    def final_model(state: MessageState, config: RunnableConfig) -> MessageState:
        return {"messages": [model.invoke(state["messages"], config=config)]}

    builder = StateGraph(MessageState)
    builder.add_node("first_model", first_model)
    builder.add_node("execute_tool", execute_tool)
    builder.add_node("final_model", final_model)
    builder.add_edge(START, "first_model")
    builder.add_edge("first_model", "execute_tool")
    builder.add_edge("execute_tool", "final_model")
    builder.add_edge("final_model", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-context-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    system = SystemMessage("Follow the task.")
    user = HumanMessage("Use lookup.")
    result = graph.invoke({"messages": [system, user]})
    assert result["messages"][-1].content == "finished"

    records = _semantic_records(capture.shutdown().artifact_path)
    messages = [record for record in records if record["kind"] == "message"]
    requests = [record for record in records if record["kind"] == "model.request"]
    responses = [record for record in records if record["kind"] == "model.response"]
    tool_result = _record(records, "tool.result")
    states = [record for record in records if record["kind"] == "state"]

    assert [record["data"]["role"] for record in messages] == ["system", "user"]
    assert len(requests) == len(responses) == 2
    assert requests[0]["data"]["context_refs"] == [
        messages[0]["id"],
        messages[1]["id"],
    ]
    assert requests[1]["data"]["context_refs"] == [
        messages[0]["id"],
        messages[1]["id"],
        responses[0]["id"],
        tool_result["id"],
    ]
    assert len(states) == 1
    assert states[0]["data"]["type"] == "langgraph.final_state"
    assert states[0]["data"]["value"] == {"messages": {"count": 5}}
    assert isinstance(states[0]["data"]["version"], str)
    assert not [record for record in records if record["kind"] == "loss"]


def test_langgraph_toolnode_preserves_exact_proposal_execution_and_result(
    tmp_path: Path,
) -> None:
    model = FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "lookup",
                        "args": {"query": "exact"},
                        "id": "toolnode-call-1",
                        "type": "tool_call",
                    }
                ],
            )
        ]
    )

    @langchain_tool
    def lookup(query: str) -> str:
        """Return exact fixture output."""

        assert query == "exact"
        return "toolnode-output"

    def call_model(state: MessagesState, config: RunnableConfig) -> dict[str, Any]:
        return {"messages": [model.invoke(state["messages"], config=config)]}

    builder = StateGraph(MessagesState)
    builder.add_node("model", call_model)
    builder.add_node("tools", ToolNode([lookup]))
    builder.add_edge(START, "model")
    builder.add_edge("model", "tools")
    builder.add_edge("tools", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-toolnode-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    result = graph.invoke({"messages": [HumanMessage("Use lookup.")]})
    assert result["messages"][-1].content == "toolnode-output"

    records = _semantic_records(capture.shutdown().artifact_path)
    proposal = _record(records, "tool.proposal")
    call = _record(records, "tool.call")
    tool_result = _record(records, "tool.result")
    assert proposal["data"]["native_call_id"] == "toolnode-call-1"
    assert call["data"]["native_call_id"] == "toolnode-call-1"
    assert tool_result["data"]["native_call_id"] == "toolnode-call-1"
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert tool_result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert tool_result["data"]["output"]["content"] == "toolnode-output"
    assert not [record for record in records if record["kind"] == "loss"]


def test_langgraph_manual_tool_result_declares_exact_execution_gap(
    tmp_path: Path,
) -> None:
    model = FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "think_tool",
                        "args": {"reflection": "exact reflection"},
                        "id": "manual-call-1",
                        "type": "tool_call",
                    }
                ],
            )
        ]
    )

    def call_model(state: MessagesState, config: RunnableConfig) -> dict[str, Any]:
        return {"messages": [model.invoke(state["messages"], config=config)]}

    def execute_without_tool_callback(state: MessagesState) -> dict[str, Any]:
        proposal = state["messages"][-1].tool_calls[0]
        return {
            "messages": [
                ToolMessage(
                    content=f"Reflection recorded: {proposal['args']['reflection']}",
                    name=proposal["name"],
                    tool_call_id=proposal["id"],
                )
            ]
        }

    builder = StateGraph(MessagesState)
    builder.add_node("model", call_model)
    builder.add_node("manual_tool", execute_without_tool_callback)
    builder.add_edge(START, "model")
    builder.add_edge("model", "manual_tool")
    builder.add_edge("manual_tool", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-manual-tool-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    result = graph.invoke({"messages": [HumanMessage("Reflect once.")]})
    assert result["messages"][-1].content == "Reflection recorded: exact reflection"

    records = _semantic_records(capture.shutdown().artifact_path)
    proposal = _record(records, "tool.proposal")
    tool_message = next(
        record
        for record in records
        if record["kind"] == "message" and record["data"]["role"] == "tool"
    )
    losses = [record for record in records if record["kind"] == "loss"]

    assert proposal["data"]["native_call_id"] == "manual-call-1"
    assert tool_message["data"] == {
        "role": "tool",
        "content": "Reflection recorded: exact reflection",
        "name": "think_tool",
        "call_id": "manual-call-1",
    }
    assert not [record for record in records if record["kind"] == "tool.call"]
    assert not [record for record in records if record["kind"] == "tool.result"]
    assert [record["data"]["reason"] for record in losses] == ["tool_execution_not_observed"]
    assert losses[0]["parent"] == proposal["id"]


def test_langgraph_reports_pydantic_response_format_without_building_schema(
    tmp_path: Path,
) -> None:
    model = FakeMessagesListChatModel(responses=[AIMessage(content="structured")])

    def call_model(_state: _GraphState, config: RunnableConfig) -> _GraphState:
        response = model.invoke(
            "return a structured answer",
            config=config,
            response_format=_StructuredAnswer,
        )
        assert response.content == "structured"
        return {"values": [1]}

    builder = StateGraph(_GraphState)
    builder.add_node("model", call_model)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-response-format")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    assert graph.invoke({"values": []}) == {"values": [1]}

    records = _semantic_records(capture.shutdown().artifact_path)
    request = _record(records, "model.request")
    assert "response_format" not in request["data"].get("settings", {})
    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == [
        "response_format_not_observed"
    ]
    assert losses[0]["parent"] == request["id"]


def test_langgraph_reports_opaque_response_format_as_precise_gap(
    tmp_path: Path,
) -> None:
    model = FakeMessagesListChatModel(responses=[AIMessage(content="fallback")])

    def call_model(_state: _GraphState, config: RunnableConfig) -> _GraphState:
        response = model.invoke(
            "return an answer",
            config=config,
            response_format=object(),
        )
        assert response.content == "fallback"
        return {"values": [1]}

    builder = StateGraph(_GraphState)
    builder.add_node("model", call_model)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-opaque-response-format")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    assert graph.invoke({"values": []}) == {"values": [1]}

    records = _semantic_records(capture.shutdown().artifact_path)
    request = _record(records, "model.request")
    assert "response_format" not in request["data"].get("settings", {})
    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == [
        "response_format_not_observed"
    ]
    assert losses[0]["parent"] == request["id"]


def test_langgraph_retains_already_materialized_response_schema(
    tmp_path: Path,
) -> None:
    schema = {
        "type": "json_schema",
        "json_schema": {
            "name": "answer",
            "schema": {
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
            },
        },
    }
    model = FakeMessagesListChatModel(responses=[AIMessage(content="materialized")])

    def call_model(_state: _GraphState, config: RunnableConfig) -> _GraphState:
        response = model.invoke(
            "return an answer",
            config=config,
            response_format=schema,
        )
        assert response.content == "materialized"
        return {"values": [1]}

    builder = StateGraph(_GraphState)
    builder.add_node("model", call_model)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-materialized-schema")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    assert graph.invoke({"values": []}) == {"values": [1]}

    records = _semantic_records(capture.shutdown().artifact_path)
    request = _record(records, "model.request")
    assert request["data"]["settings"]["response_format"] == schema
    assert not [record for record in records if record["kind"] == "loss"]


def test_langgraph_never_invokes_pydantic_schema_hook(
    tmp_path: Path,
) -> None:
    hook_calls: list[str] = []

    class HostileSchema(BaseModel):
        answer: str

        @classmethod
        def __get_pydantic_json_schema__(cls, core_schema: Any, handler: Any) -> Any:
            hook_calls.append("called")
            raise AssertionError("capture invoked application schema code")

    def build_graph() -> Any:
        model = FakeMessagesListChatModel(responses=[AIMessage(content="same")])

        def call_model(_state: _GraphState, config: RunnableConfig) -> _GraphState:
            response = model.invoke(
                "return an answer",
                config=config,
                response_format=HostileSchema,
            )
            assert response.content == "same"
            return {"values": [1]}

        builder = StateGraph(_GraphState)
        builder.add_node("model", call_model)
        builder.add_edge(START, "model")
        builder.add_edge("model", END)
        return builder.compile()

    baseline = build_graph()
    assert baseline.invoke({"values": []}) == {"values": [1]}
    assert hook_calls == []

    instrumented = build_graph()
    capture = initialize(output=tmp_path, service_name="langgraph-hostile-schema")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=instrumented,
    )
    assert instrumented.invoke({"values": []}) == {"values": [1]}
    assert hook_calls == []

    records = _semantic_records(capture.shutdown().artifact_path)
    assert [record["data"]["reason"] for record in records if record["kind"] == "loss"] == [
        "response_format_not_observed"
    ]


class _StructuredState(TypedDict):
    structured_response: Any


class _StructuredAnswer(BaseModel):
    answer: str
    confidence: float


class _TypedResponse(ExtendedTypedDict):
    answer: str
    confidence: float


def test_langgraph_reports_typed_dict_response_format_without_building_schema(
    tmp_path: Path,
) -> None:
    model = FakeMessagesListChatModel(responses=[AIMessage(content="typed")])

    def call_model(_state: _GraphState, config: RunnableConfig) -> _GraphState:
        response = model.invoke(
            "return a typed answer",
            config=config,
            response_format=_TypedResponse,
        )
        assert response.content == "typed"
        return {"values": [1]}

    builder = StateGraph(_GraphState)
    builder.add_node("model", call_model)
    builder.add_edge(START, "model")
    builder.add_edge("model", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-typed-response-format")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    assert graph.invoke({"values": []}) == {"values": [1]}

    records = _semantic_records(capture.shutdown().artifact_path)
    request = _record(records, "model.request")
    assert "response_format" not in request["data"].get("settings", {})
    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == [
        "response_format_not_observed"
    ]
    assert losses[0]["parent"] == request["id"]


def test_langgraph_records_only_official_node_state_and_normalizes_structured_response(
    tmp_path: Path,
) -> None:
    nested = RunnableLambda(lambda value: value)

    def answer(_state: _StructuredState, config: RunnableConfig) -> _StructuredState:
        assert nested.invoke("nested", config=config) == "nested"
        return {
            "structured_response": _StructuredAnswer(
                answer="complete",
                confidence=0.9,
            )
        }

    builder = StateGraph(_StructuredState)
    builder.add_node("answer", answer)
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-structured-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    result = graph.invoke({"structured_response": None})
    assert isinstance(result["structured_response"], _StructuredAnswer)

    records = _semantic_records(capture.shutdown().artifact_path)
    states = [record for record in records if record["kind"] == "state"]
    assert [record["data"]["value"] for record in states] == [
        {
            "structured_response": {
                "answer": "complete",
                "confidence": 0.9,
            }
        },
        {
            "structured_response": {
                "answer": "complete",
                "confidence": 0.9,
            }
        },
    ]
    assert states[0]["data"]["version"].startswith("answer:")
    assert states[1]["data"]["version"] != states[0]["data"]["version"]
    assert not [record for record in records if record["kind"] == "loss"]


class _TransitionState(TypedDict):
    phase: str


def test_langgraph_retains_equal_values_from_distinct_state_transitions(
    tmp_path: Path,
) -> None:
    builder = StateGraph(_TransitionState)
    builder.add_node("first_a", lambda _state: {"phase": "A"})
    builder.add_node("middle_b", lambda _state: {"phase": "B"})
    builder.add_node("second_a", lambda _state: {"phase": "A"})
    builder.add_edge(START, "first_a")
    builder.add_edge("first_a", "middle_b")
    builder.add_edge("middle_b", "second_a")
    builder.add_edge("second_a", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-transition-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    assert graph.invoke({"phase": "initial"}) == {"phase": "A"}

    records = _semantic_records(capture.shutdown().artifact_path)
    node_states = [
        record
        for record in records
        if record["kind"] == "state"
        and record["data"]["type"] == "langgraph.step.output"
    ]
    assert [record["data"]["value"] for record in node_states] == [
        {"phase": "A"},
        {"phase": "B"},
        {"phase": "A"},
    ]
    assert len({record["data"]["version"] for record in node_states}) == 3
    assert not [record for record in records if record["kind"] == "loss"]


def test_langgraph_compacts_nested_cumulative_message_state(
    tmp_path: Path,
) -> None:
    class SupervisorState(TypedDict):
        supervisor_messages: Annotated[list[Any], operator.add]
        final_report: str

    first_message = AIMessage(content="plan-" * 1000, id="plan-message")
    final_message = AIMessage(content="finish-" * 1000, id="finish-message")

    def plan(_state: SupervisorState) -> Command[Literal["finish"]]:
        return Command(
            update={"supervisor_messages": [first_message]},
            goto="finish",
        )

    def finish(_state: SupervisorState) -> SupervisorState:
        return {
            "supervisor_messages": [final_message],
            "final_report": "done",
        }

    builder = StateGraph(SupervisorState)
    builder.add_node("plan", plan)
    builder.add_node("finish", finish)
    builder.add_edge(START, "plan")
    builder.add_edge("finish", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-state-compaction")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    result = graph.invoke({"supervisor_messages": [], "final_report": ""})
    assert result == {
        "supervisor_messages": [first_message, final_message],
        "final_report": "done",
    }

    records = _semantic_records(capture.shutdown().artifact_path)
    messages = [record for record in records if record["kind"] == "message"]
    states = [record for record in records if record["kind"] == "state"]
    serialized_states = json.dumps(states)
    state_bytes = sum(len(json.dumps(record)) for record in states)

    assert {record["data"]["content"] for record in messages} == {
        first_message.content,
        final_message.content,
    }
    assert first_message.content not in serialized_states
    assert final_message.content not in serialized_states
    assert state_bytes < 2_500
    assert states[-1]["data"]["value"] == {
        "supervisor_messages": {"count": 2},
        "final_report": "done",
    }
    assert not [record for record in records if record["kind"] == "loss"]


def test_langgraph_projects_supported_dict_messages_key(
    tmp_path: Path,
) -> None:
    class DictMessageState(TypedDict):
        messages: list[dict[str, str]]
        result: str

    def answer(_state: DictMessageState) -> DictMessageState:
        return {
            "messages": [{"role": "assistant", "content": "done"}],
            "result": "complete",
        }

    builder = StateGraph(DictMessageState)
    builder.add_node("answer", answer)
    builder.add_edge(START, "answer")
    builder.add_edge("answer", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-dict-messages")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    result = graph.invoke(
        {
            "messages": [{"role": "user", "content": "finish"}],
            "result": "",
        }
    )
    assert result["result"] == "complete"

    records = _semantic_records(capture.shutdown().artifact_path)
    messages = [record for record in records if record["kind"] == "message"]
    states = [record for record in records if record["kind"] == "state"]
    assert [record["data"]["content"] for record in messages] == ["done"]
    assert states[-1]["data"]["value"] == {
        "messages": {"count": 1},
        "result": "complete",
    }
    assert not [record for record in records if record["kind"] == "loss"]


def test_langgraph_preserves_non_message_suffix_state(
    tmp_path: Path,
) -> None:
    class MetadataState(TypedDict):
        email_messages: dict[str, str]
        error_messages: str
        audit_messages: list[dict[str, str]]

    expected: MetadataState = {
        "email_messages": {"subject": "Quarterly update"},
        "error_messages": "No error",
        "audit_messages": [{"event": "opened"}],
    }
    builder = StateGraph(MetadataState)
    builder.add_node("keep", lambda _state: expected)
    builder.add_edge(START, "keep")
    builder.add_edge("keep", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-suffix-state")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    assert graph.invoke(expected) == expected

    records = _semantic_records(capture.shutdown().artifact_path)
    states = [record for record in records if record["kind"] == "state"]
    assert states[-1]["data"]["value"] == expected
    assert not [record for record in records if record["kind"] == "message"]
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.parametrize("unsafe_shape", ["cycle", "deep", "wide"])
def test_langgraph_state_compaction_limit_keeps_completed_run(
    tmp_path: Path,
    unsafe_shape: str,
) -> None:
    class PayloadState(TypedDict):
        payload: Any

    if unsafe_shape == "cycle":
        payload: Any = {}
        payload["self"] = payload
    elif unsafe_shape == "deep":
        payload = "leaf"
        for _ in range(52):
            payload = {"child": payload}
    else:
        payload = list(range(20_001))

    builder = StateGraph(PayloadState)
    builder.add_node("keep", lambda state: {"payload": state["payload"]})
    builder.add_edge(START, "keep")
    builder.add_edge("keep", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name=f"langgraph-{unsafe_shape}-state")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    result = graph.invoke({"payload": payload})
    assert result["payload"] is payload

    records = _semantic_records(capture.shutdown().artifact_path)
    losses = [record for record in records if record["kind"] == "loss"]
    scopes = [record for record in records if record["kind"] == "scope"]
    assert [record["data"]["reason"] for record in losses] == [
        "state_compaction_unavailable"
    ]
    assert any(record["data"].get("status") == "completed" for record in scopes)
    assert _record(records, "run.outcome")["data"]["status"] == "completed"


@pytest.mark.parametrize("container_kind", ["dict", "list", "tuple"])
def test_langgraph_opaque_container_subclass_has_baseline_behavior(
    tmp_path: Path,
    container_kind: str,
) -> None:
    class PayloadState(TypedDict):
        payload: Any

    calls: dict[int, int] = {}

    def called(value: Any) -> None:
        calls[id(value)] = calls.get(id(value), 0) + 1

    if container_kind == "dict":

        class HostileDict(dict[str, Any]):
            def items(self) -> Any:
                called(self)
                dict.__setitem__(self, "unexpected_mutation", True)
                return dict.items(self)

            def __iter__(self) -> Any:
                called(self)
                return dict.__iter__(self)

            def __getitem__(self, key: str) -> Any:
                called(self)
                return dict.__getitem__(self, key)

        def make_payload() -> Any:
            return HostileDict({"value": 1})

    elif container_kind == "list":

        class HostileList(list[int]):
            def __iter__(self) -> Any:
                called(self)
                return list.__iter__(self)

            def __getitem__(self, key: Any) -> Any:
                called(self)
                return list.__getitem__(self, key)

        def make_payload() -> Any:
            return HostileList([1, 2])

    else:

        class HostileTuple(tuple[int, ...]):
            def __iter__(self) -> Any:
                called(self)
                return tuple.__iter__(self)

            def __getitem__(self, key: Any) -> Any:
                called(self)
                return tuple.__getitem__(self, key)

        def make_payload() -> Any:
            return HostileTuple((1, 2))

    builder = StateGraph(PayloadState)
    builder.add_node("keep", lambda state: {"payload": state["payload"]})
    builder.add_edge(START, "keep")
    builder.add_edge("keep", END)
    graph = builder.compile()

    baseline_payload = make_payload()
    baseline = graph.invoke({"payload": baseline_payload})
    assert baseline["payload"] is baseline_payload
    baseline_calls = calls.get(id(baseline_payload), 0)

    captured_payload = make_payload()
    capture = initialize(output=tmp_path, service_name=f"langgraph-hostile-{container_kind}")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )
    instrumented = graph.invoke({"payload": captured_payload})
    assert instrumented["payload"] is captured_payload
    assert calls.get(id(captured_payload), 0) == baseline_calls

    records = _semantic_records(capture.shutdown().artifact_path)
    assert [record["data"]["reason"] for record in records if record["kind"] == "loss"] == [
        "state_compaction_unavailable"
    ]
    assert _record(records, "run.outcome")["data"]["status"] == "completed"


def test_langgraph_graph_interrupt_is_a_pause_not_a_failure(tmp_path: Path) -> None:
    def pause(_state: _TransitionState) -> _TransitionState:
        interrupt({"question": "continue?"})
        return {"phase": "unreachable"}

    builder = StateGraph(_TransitionState)
    builder.add_node("pause", pause)
    builder.add_edge(START, "pause")
    builder.add_edge("pause", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-pause-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    result = graph.invoke({"phase": "initial"})
    assert result["__interrupt__"][0].value == {"question": "continue?"}

    records = _semantic_records(capture.shutdown().artifact_path)
    pause_record = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"]["type"] == "langgraph.pause"
    )
    assert pause_record["data"]["value"]["status"] == "paused"
    assert _record(records, "run.outcome")["data"]["status"] == "unknown"
    assert not [record for record in records if record["kind"] == "error"]


def test_langgraph_checkpointer_interrupt_resume_links_completed_turn(
    tmp_path: Path,
) -> None:
    def pause(state: _TransitionState) -> _TransitionState:
        answer = interrupt({"question": "continue?", "phase": state["phase"]})
        return {"phase": f"resumed:{answer}"}

    builder = StateGraph(_TransitionState)
    builder.add_node("pause", pause)
    builder.add_edge(START, "pause")
    builder.add_edge("pause", END)
    graph = builder.compile(checkpointer=MemorySaver())
    capture = initialize(output=tmp_path, service_name="langgraph-resume-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )
    config: RunnableConfig = {"configurable": {"thread_id": "resume-thread"}}

    paused = graph.invoke({"phase": "initial"}, config=config)
    assert paused["__interrupt__"][0].value == {
        "question": "continue?",
        "phase": "initial",
    }
    resumed = graph.invoke(Command(resume="yes"), config=config)
    assert resumed["phase"] == "resumed:yes"

    records = _semantic_records(capture.shutdown().artifact_path)
    starts = [record for record in records if record["kind"] == "run.start"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(starts) == len(outcomes) == 2
    assert outcomes[0]["data"]["status"] == "unknown"
    assert outcomes[1]["data"]["status"] == "completed"
    continuation = starts[1]["links"]
    assert continuation[0]["type"] == "continues_from"
    linked = next(
        record for record in records if record["id"] == continuation[0]["record"]
    )
    assert linked["id"] == starts[0]["id"] or linked.get("parent") == starts[0]["id"]
    assert "unsafe_helper_avoided" not in {
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
    }


def test_langgraph_early_stream_close_is_cancelled(tmp_path: Path) -> None:
    builder = StateGraph(_TransitionState)
    builder.add_node("first", lambda _state: {"phase": "first"})
    builder.add_node("second", lambda _state: {"phase": "second"})
    builder.add_edge(START, "first")
    builder.add_edge("first", "second")
    builder.add_edge("second", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-cancel-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    stream = graph.stream({"phase": "initial"})
    assert next(stream) == {"first": {"phase": "first"}}
    stream.close()

    records = _semantic_records(capture.shutdown().artifact_path)
    assert _record(records, "run.outcome")["data"]["status"] == "cancelled"
    assert not [record for record in records if record["kind"] == "error"]


def test_langgraph_stream_events_uses_the_same_callback_capture_seam(
    tmp_path: Path,
) -> None:
    builder = StateGraph(_TransitionState)
    builder.add_node("finish", lambda _state: {"phase": "finished"})
    builder.add_edge(START, "finish")
    builder.add_edge("finish", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-events-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    events = list(graph.stream_events({"phase": "initial"}, version="v3"))
    assert [event["params"]["data"] for event in events] == [
        {"phase": "initial"},
        {"phase": "finished"},
    ]

    records = _semantic_records(capture.shutdown().artifact_path)
    assert _record(records, "run.outcome")["data"]["status"] == "completed"
    assert _record(records, "state")["data"]["value"] == {"phase": "finished"}


@pytest.mark.asyncio
async def test_langgraph_astream_events_uses_the_same_callback_capture_seam(
    tmp_path: Path,
) -> None:
    builder = StateGraph(_TransitionState)
    builder.add_node("finish", lambda _state: {"phase": "finished"})
    builder.add_edge(START, "finish")
    builder.add_edge("finish", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-aevents-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    stream = graph.astream_events({"phase": "initial"}, version="v3")
    events = [event async for event in await stream]
    assert [event["params"]["data"] for event in events] == [
        {"phase": "initial"},
        {"phase": "finished"},
    ]

    records = _semantic_records((await capture.shutdown_async()).artifact_path)
    assert _record(records, "run.outcome")["data"]["status"] == "completed"


def test_langgraph_propagated_failure_emits_one_error_record(tmp_path: Path) -> None:
    expected = RuntimeError("node failed")

    def fail(_state: _TransitionState) -> _TransitionState:
        raise expected

    builder = StateGraph(_TransitionState)
    builder.add_node("fail", fail)
    builder.add_edge(START, "fail")
    builder.add_edge("fail", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-error-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    with pytest.raises(RuntimeError) as caught:
        graph.invoke({"phase": "initial"})
    assert caught.value is expected

    records = _semantic_records(capture.shutdown().artifact_path)
    assert len([record for record in records if record["kind"] == "error"]) == 1
    assert _record(records, "run.outcome")["data"]["status"] == "failed"


class _ToolStrandsModel(StrandsModel):
    def update_config(self, **model_config: Any) -> None:
        return None

    def get_config(self) -> dict[str, str]:
        return {"model_id": "fixture"}

    async def structured_output(self, *args: Any, **kwargs: Any) -> Any:
        if False:
            yield None

    async def stream(self, messages: Any, *args: Any, **kwargs: Any) -> Any:
        has_result = any(
            any("toolResult" in block for block in message.get("content", []))
            for message in messages
        )
        if not has_result:
            events = [
                {"messageStart": {"role": "assistant"}},
                {
                    "contentBlockStart": {
                        "start": {
                            "toolUse": {
                                "name": "lookup",
                                "toolUseId": "strands-call-1",
                            }
                        }
                    }
                },
                {"contentBlockDelta": {"delta": {"toolUse": {"input": "{}"}}}},
                {"contentBlockStop": {}},
                {"messageStop": {"stopReason": "tool_use"}},
            ]
        else:
            events = [
                {"messageStart": {"role": "assistant"}},
                {"contentBlockStart": {"start": {}}},
                {"contentBlockDelta": {"delta": {"text": "done"}}},
                {"contentBlockStop": {}},
                {"messageStop": {"stopReason": "end_turn"}},
            ]
        events.append(
            {
                "metadata": {
                    "usage": {
                        "inputTokens": 4,
                        "outputTokens": 2,
                        "totalTokens": 6,
                    },
                    "metrics": {"latencyMs": 1},
                }
            }
        )
        for event in events:
            yield event


class _FailFirstStrandsModel(_ToolStrandsModel):
    def __init__(self, failure: Exception) -> None:
        self.failure = failure
        self.calls = 0

    async def stream(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        if self.calls == 1:
            raise self.failure
        async for event in super().stream(*args, **kwargs):
            yield event


class _ReasoningStrandsModel(_ToolStrandsModel):
    async def stream(self, messages: Any, *args: Any, **kwargs: Any) -> Any:
        del messages, args, kwargs
        events = [
            {"messageStart": {"role": "assistant"}},
            {"contentBlockStart": {"start": {}}},
            {
                "contentBlockDelta": {
                    "delta": {"reasoningContent": {"text": "Inspect both records."}}
                }
            },
            {"contentBlockStop": {}},
            {"contentBlockStart": {"start": {}}},
            {
                "contentBlockDelta": {
                    "delta": {"reasoningContent": {"text": "Inspect both records."}}
                }
            },
            {"contentBlockStop": {}},
            {"contentBlockStart": {"start": {}}},
            {"contentBlockDelta": {"delta": {"text": "Use record two."}}},
            {"contentBlockStop": {}},
            {"messageStop": {"stopReason": "end_turn"}},
            {
                "metadata": {
                    "usage": {"inputTokens": 3, "outputTokens": 6, "totalTokens": 9},
                    "metrics": {"latencyMs": 1},
                }
            },
        ]
        for event in events:
            yield event


class _FailedReasoningStrandsModel(_ToolStrandsModel):
    def __init__(self, failure: BaseException) -> None:
        self.failure = failure

    async def stream(self, messages: Any, *args: Any, **kwargs: Any) -> Any:
        del messages, args, kwargs
        yield {"messageStart": {"role": "assistant"}}
        yield {"contentBlockStart": {"start": {}}}
        yield {
            "contentBlockDelta": {
                "delta": {"reasoningContent": {"text": "first"}}
            }
        }
        yield {
            "contentBlockDelta": {
                "delta": {"reasoningContent": {"text": "first"}}
            }
        }
        raise self.failure


class _ConcurrentFailedReasoningStrandsModel(_ToolStrandsModel):
    def __init__(self) -> None:
        self.started = 0
        self.both_started = asyncio.Event()

    async def stream(self, messages: Any, *args: Any, **kwargs: Any) -> Any:
        del messages, args, kwargs
        self.started += 1
        call = self.started
        yield {"messageStart": {"role": "assistant"}}
        yield {
            "contentBlockDelta": {
                "delta": {"reasoningContent": {"text": f"reasoning-{call}"}}
            }
        }
        if self.started == 2:
            self.both_started.set()
        await self.both_started.wait()
        raise RuntimeError(f"failure-{call}")


class _ArgumentToolStrandsModel(_ToolStrandsModel):
    async def stream(self, messages: Any, *args: Any, **kwargs: Any) -> Any:
        has_result = any(
            any("toolResult" in block for block in message.get("content", []))
            for message in messages
        )
        events = (
            [
                {"messageStart": {"role": "assistant"}},
                {
                    "contentBlockStart": {
                        "start": {
                            "toolUse": {
                                "name": "lookup",
                                "toolUseId": "strands-effective-call-1",
                            }
                        }
                    }
                },
                {
                    "contentBlockDelta": {
                        "delta": {"toolUse": {"input": '{"query":"proposed"}'}}
                    }
                },
                {"contentBlockStop": {}},
                {"messageStop": {"stopReason": "tool_use"}},
            ]
            if not has_result
            else [
                {"messageStart": {"role": "assistant"}},
                {"contentBlockStart": {"start": {}}},
                {"contentBlockDelta": {"delta": {"text": "done"}}},
                {"contentBlockStop": {}},
                {"messageStop": {"stopReason": "end_turn"}},
            ]
        )
        for event in events:
            yield event


class _StructuredStrandsAnswer(BaseModel):
    answer: str


class _StructuredStrandsModel(_ToolStrandsModel):
    returned: _StructuredStrandsAnswer | None = None

    async def structured_output(
        self,
        output_model: type[_StructuredStrandsAnswer],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        self.returned = output_model(answer="structured")
        yield {"output": self.returned}


def test_strands_capture_projects_compact_exact_semantics(tmp_path: Path) -> None:
    @strands_tool
    def lookup() -> str:
        """Return exact fixture output."""

        return "tool-output"

    agent = StrandsAgent(
        model=_ToolStrandsModel(),
        tools=[lookup],
        system_prompt="Use only the available tool when needed.",
        callback_handler=None,
    )
    capture = initialize(output=tmp_path, service_name="strands-semantic-fixture")
    capture.instrument(
        adapter=strands_adapter(version=distribution_version("strands-agents")),
        client=agent,
    )

    result = agent(
        "use the tool",
        invocation_state={
            "session_id": "conversation-strands",
            "turn_id": "turn-strands-1",
            "turn_index": 0,
        },
    )
    assert result.message["content"][0]["text"] == "done"

    records = _semantic_records(capture.shutdown().artifact_path)
    kinds = [record["kind"] for record in records]
    assert kinds.count("run.start") == kinds.count("run.outcome") == 1
    assert kinds.count("model.request") == kinds.count("model.response") == 2
    losses = [record for record in records if record["kind"] == "loss"]
    assert [record["data"]["reason"] for record in losses] == [
        "strands_post_middleware_context_unavailable"
    ]

    requests = [record for record in records if record["kind"] == "model.request"]
    responses = [record for record in records if record["kind"] == "model.response"]
    messages = [record for record in records if record["kind"] == "message"]
    messages_by_role = {record["data"]["role"]: record for record in messages}
    assert set(messages_by_role) == {"system", "user"}
    assert all(request["data"]["model"] == "fixture" for request in requests)
    assert all(request["data"]["tools"] == ["lookup"] for request in requests)
    assert all(
        request["data"]["tool_definitions"][0]["name"] == "lookup"
        for request in requests
    )
    assert all(
        request["data"]["settings"] == {"model_id": "fixture"}
        for request in requests
    )
    records_by_id = {record["id"]: record for record in records}
    assert [
        records_by_id[record_id]["data"]["role"]
        for record_id in requests[0]["data"]["context_refs"]
    ] == ["system", "user"]
    assert {
        response["links"][0]["record"]
        for response in responses
        if response.get("links")
    } == {request["id"] for request in requests}
    assert all(
        response["data"]["usage"] == {"input_tokens": 4, "output_tokens": 2}
        for response in responses
    )
    assert [response["data"]["finish_reason"] for response in responses] == [
        "tool_use",
        "end_turn",
    ]

    proposal = _record(records, "tool.proposal")
    call = _record(records, "tool.call")
    tool_result = _record(records, "tool.result")
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert tool_result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert call["data"]["name"] == "lookup"
    assert call["data"]["input"] == {}
    assert tool_result["data"]["status"] == "succeeded"
    assert tool_result["data"]["output"] == {
        "toolUseId": "strands-call-1",
        "status": "success",
        "content": [{"text": "tool-output"}],
    }
    assert tool_result["data"]["native_call_id"] == call["data"]["native_call_id"]
    outcome = _record(records, "run.outcome")
    assert "output" not in outcome["data"]
    assert outcome["links"] == [
        {"type": "derived_from", "record": responses[-1]["id"]}
    ]
    assert "metrics" not in json.dumps(outcome)
    assert "state" not in json.dumps(outcome)


def test_langgraph_separates_structured_reasoning_from_visible_content(
    tmp_path: Path,
) -> None:
    model = FakeMessagesListChatModel(
        responses=[
            AIMessage(
                content=[
                    {"type": "reasoning", "reasoning": "first"},
                    {"type": "thinking", "thinking": "second"},
                    {"type": "reasoning_summary", "summary": "third"},
                    {"type": "text", "text": "visible"},
                    {"type": "reasoning", "reasoning": "first"},
                ]
            )
        ]
    )

    def exercise(_state: _GraphState, config: RunnableConfig) -> _GraphState:
        response = model.invoke("inspect", config=config)
        assert response.content[-2] == {"type": "text", "text": "visible"}
        return {"values": [1]}

    builder = StateGraph(_GraphState)
    builder.add_node("exercise", exercise)
    builder.add_edge(START, "exercise")
    builder.add_edge("exercise", END)
    graph = builder.compile()
    capture = initialize(output=tmp_path, service_name="langgraph-blocks-fixture")
    capture.instrument(
        adapter=langgraph_adapter(version=distribution_version("langgraph")),
        client=graph,
    )

    assert graph.invoke({"values": []}) == {"values": [1]}
    records = _semantic_records(capture.shutdown().artifact_path)
    response = _record(records, "model.response")
    assert response["data"]["content"] == [{"type": "text", "text": "visible"}]
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "first"},
        {"type": "text", "text": "second"},
        {"type": "summary", "text": "third"},
        {"type": "text", "text": "first"},
    ]


def test_strands_preserves_exposed_reasoning_blocks_in_native_order(tmp_path: Path) -> None:
    agent = StrandsAgent(
        model=_ReasoningStrandsModel(),
        callback_handler=None,
    )
    capture = initialize(output=tmp_path, service_name="strands-reasoning-fixture")
    capture.instrument(
        adapter=strands_adapter(version="1.47.0"),
        client=agent,
    )

    result = agent("choose the current record")
    assert result.message["content"][-1]["text"] == "Use record two."

    records = _semantic_records(capture.shutdown().artifact_path)
    response = _record(records, "model.response")
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect both records."},
        {"type": "text", "text": "Inspect both records."},
    ]
    assert response["data"]["content"] == [{"text": "Use record two."}]


def test_strands_preserves_reasoning_streamed_before_model_failure(
    tmp_path: Path,
) -> None:
    expected = RuntimeError("reasoning stream failed")
    agent = StrandsAgent(
        model=_FailedReasoningStrandsModel(expected),
        callback_handler=None,
    )
    capture = initialize(output=tmp_path, service_name="strands-failed-reasoning")
    capture.instrument(
        adapter=strands_adapter(version="1.47.0"),
        client=agent,
    )

    with pytest.raises(RuntimeError) as caught:
        agent("inspect")
    assert caught.value is expected

    records = _semantic_records(capture.shutdown().artifact_path)
    response = _record(records, "model.response")
    assert response["data"]["status"] == "failed"
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "first"},
        {"type": "text", "text": "first"},
    ]


def test_strands_reports_unobservable_context_reduction_after_overflow(
    tmp_path: Path,
) -> None:
    expected = ContextWindowOverflowException("context window exceeded")
    agent = StrandsAgent(
        model=_FailedReasoningStrandsModel(expected),
        callback_handler=None,
        conversation_manager=NullConversationManager(),
    )
    capture = initialize(output=tmp_path, service_name="strands-context-reduction")
    capture.instrument(adapter=strands_adapter(version="1.47.0"), client=agent)

    with pytest.raises(ContextWindowOverflowException) as caught:
        agent("inspect")
    assert caught.value is expected

    records = _semantic_records(capture.shutdown().artifact_path)
    loss = next(
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"] == "strands_context_reduction_unobserved"
    )
    assert loss["data"]["count"] == 1
    assert loss["data"]["recoverable"] is False


@pytest.mark.asyncio
async def test_strands_preserves_reasoning_streamed_before_model_cancellation(
    tmp_path: Path,
) -> None:
    expected = asyncio.CancelledError("reasoning stream cancelled")
    agent = StrandsAgent(
        model=_FailedReasoningStrandsModel(expected),
        callback_handler=None,
    )
    capture = initialize(output=tmp_path, service_name="strands-cancelled-reasoning")
    capture.instrument(
        adapter=strands_adapter(version="1.47.0"),
        client=agent,
    )

    with pytest.raises(asyncio.CancelledError) as caught:
        async for _event in agent.stream_async(
            "inspect",
            invocation_state={"turn_id": "cancelled-reasoning"},
        ):
            pass
    assert caught.value is expected

    records = _semantic_records((await capture.shutdown_async()).artifact_path)
    response = _record(records, "model.response")
    assert response["data"]["status"] == "cancelled"
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "first"},
        {"type": "text", "text": "first"},
    ]


@pytest.mark.asyncio
async def test_strands_concurrent_streams_keep_reasoning_with_exact_invocation(
    tmp_path: Path,
) -> None:
    model = _ConcurrentFailedReasoningStrandsModel()
    agent = StrandsAgent(
        model=model,
        callback_handler=None,
        concurrent_invocation_mode=ConcurrentInvocationMode.UNSAFE_REENTRANT,
    )
    capture = initialize(output=tmp_path, service_name="strands-concurrent-reasoning")
    capture.instrument(adapter=strands_adapter(version="1.47.0"), client=agent)

    async def consume(turn_id: str) -> None:
        with pytest.raises(RuntimeError):
            async for _event in agent.stream_async(
                turn_id,
                invocation_state={"turn_id": turn_id},
            ):
                pass

    await asyncio.gather(consume("one"), consume("two"))

    records = _semantic_records((await capture.shutdown_async()).artifact_path)
    responses = [
        record for record in records if record["kind"] == "model.response"
    ]
    assert {
        tuple(block["text"] for block in response["data"]["reasoning"])
        for response in responses
    } == {("reasoning-1",), ("reasoning-2",)}
    assert not [
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"]["reason"]
        == "strands_stream_reasoning_invocation_unavailable"
    ]


@pytest.mark.parametrize(
    ("native_result", "expected_status", "expected_error"),
    [
        (
            MCPToolResult(
                status="error",
                toolUseId="strands-call-1",
                content=[{"text": "ValueError: Cannot divide by zero"}],
                isError=True,
            ),
            "failed",
            {
                "type": "strands.tool_error",
                "message": "Strands reported a failed tool result.",
                "recoverable": False,
            },
        ),
        (
            MCPToolResult(
                status="success",
                toolUseId="strands-call-1",
                content=[{"text": "tool-output"}],
                isError=False,
            ),
            "succeeded",
            None,
        ),
        (
            MCPToolResult(
                status="success",
                toolUseId="strands-call-1",
                content=[{"text": "contradictory failure"}],
                isError=True,
            ),
            "failed",
            {
                "type": "strands.tool_error",
                "message": "Strands reported a failed tool result.",
                "recoverable": False,
            },
        ),
    ],
    ids=["mcp-error", "mcp-success", "contradictory-flags"],
)
def test_strands_mcp_result_flags_determine_tool_status(
    tmp_path: Path,
    native_result: MCPToolResult,
    expected_status: str,
    expected_error: dict[str, Any] | None,
) -> None:
    mcp_client = MagicMock()
    mcp_client.call_tool_async = AsyncMock(return_value=native_result)
    mcp_tool = MCPTool(
        name="lookup",
        description="Return the fixture result.",
        inputSchema={"type": "object", "properties": {}},
    )
    agent = StrandsAgent(
        model=_ToolStrandsModel(),
        tools=[MCPAgentTool(mcp_tool, mcp_client)],
        callback_handler=None,
    )
    capture = initialize(output=tmp_path, service_name="strands-mcp-result-fixture")
    capture.instrument(
        adapter=strands_adapter(version=distribution_version("strands-agents")),
        client=agent,
    )

    result = agent("use the tool")
    assert result.message["content"][0]["text"] == "done"

    records = _semantic_records(capture.shutdown().artifact_path)
    tool_result = _record(records, "tool.result")
    assert tool_result["data"]["status"] == expected_status
    assert tool_result["data"]["output"] == native_result
    if expected_error is None:
        assert "error" not in tool_result["data"]
    else:
        assert tool_result["data"]["error"] == expected_error
    assert [
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
    ] == ["strands_post_middleware_context_unavailable"]


def test_strands_tool_call_uses_post_middleware_execution_input(
    tmp_path: Path,
) -> None:
    @strands_tool
    def lookup(query: str) -> str:
        """Return the effective query."""

        assert query == "effective"
        return query

    agent = StrandsAgent(
        model=_ArgumentToolStrandsModel(),
        tools=[lookup],
        callback_handler=None,
    )
    capture = initialize(output=tmp_path, service_name="strands-effective-tool")
    capture.instrument(
        adapter=strands_adapter(version=distribution_version("strands-agents")),
        client=agent,
    )

    def rewrite_after_adapter(event: Any) -> None:
        if isinstance(event, BeforeToolCallEvent):
            event.tool_use = {
                **event.tool_use,
                "input": {"query": "effective"},
            }

    agent.add_hook(rewrite_after_adapter, BeforeToolCallEvent)
    result = agent("use lookup")
    assert result.message["content"][0]["text"] == "done"

    records = _semantic_records(capture.shutdown().artifact_path)
    proposal = _record(records, "tool.proposal")
    call = _record(records, "tool.call")
    tool_result = _record(records, "tool.result")
    assert proposal["data"]["input"] == {"query": "proposed"}
    assert call["data"]["input"] == {"query": "effective"}
    assert tool_result["links"] == [{"type": "result_of", "record": call["id"]}]


def test_strands_result_none_from_cancelled_invocation_is_not_success(
    tmp_path: Path,
) -> None:
    agent = StrandsAgent(
        model=_ToolStrandsModel(),
        callback_handler=None,
    )

    def cancel(event: Any) -> None:
        if isinstance(event, BeforeInvocationEvent):
            event.cancel = "cancelled by fixture"

    agent.add_hook(cancel, BeforeInvocationEvent)
    capture = initialize(output=tmp_path, service_name="strands-cancel-fixture")
    capture.instrument(
        adapter=strands_adapter(version=distribution_version("strands-agents")),
        client=agent,
    )

    result = agent("do not run")
    assert result.message["content"][0]["text"] == "cancelled by fixture"

    records = _semantic_records(capture.shutdown().artifact_path)
    assert _record(records, "run.outcome")["data"]["status"] == "cancelled"
    assert not [record for record in records if record["kind"] == "model.request"]


def test_strands_structured_output_result_none_is_finalized_by_public_return(
    tmp_path: Path,
) -> None:
    model = _StructuredStrandsModel()
    agent = StrandsAgent(model=model, callback_handler=None)
    capture = initialize(output=tmp_path, service_name="strands-structured-fixture")
    capture.instrument(
        adapter=strands_adapter(version=distribution_version("strands-agents")),
        client=agent,
    )

    with pytest.deprecated_call():
        result = agent.structured_output(
            _StructuredStrandsAnswer,
            "return structured output",
        )
    assert result is model.returned

    records = _semantic_records(capture.shutdown().artifact_path)
    outcome = _record(records, "run.outcome")
    assert outcome["data"] == {
        "status": "completed",
        "output": {"answer": "structured"},
    }


def test_strands_failure_and_next_turn_project_as_outcome_retry_and_continuation(
    tmp_path: Path,
) -> None:
    expected = RuntimeError("model unavailable")
    agent = StrandsAgent(
        model=_FailFirstStrandsModel(expected),
        callback_handler=None,
    )
    capture = initialize(output=tmp_path, service_name="strands-retry-fixture")
    capture.instrument(
        adapter=strands_adapter(version=distribution_version("strands-agents")),
        client=agent,
    )

    with pytest.raises(RuntimeError) as caught:
        agent(
            "fail",
            invocation_state={
                "session_id": "conversation-retry",
                "turn_id": "turn-retry-1",
                "turn_index": 0,
            },
        )
    assert caught.value is expected
    recovered = agent(
        "recover",
        invocation_state={
            "session_id": "conversation-retry",
            "turn_id": "turn-retry-2",
            "turn_index": 1,
            "previous_turn_id": "turn-retry-1",
        },
    )
    assert recovered.message["content"][0]["text"] == "done"

    records = _semantic_records(capture.shutdown().artifact_path)
    assert [
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
    ] == [
        "strands_post_middleware_context_unavailable",
        "strands_post_middleware_context_unavailable",
    ]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert [record["data"]["status"] for record in outcomes] == ["failed", "completed"]
    assert any(record["kind"] == "error" for record in records)
    retry = next(
        record
        for record in records
        if record["kind"] == "state"
        and record["data"]["type"] == "recovery.retry"
    )
    assert retry["data"]["value"] == {"previous_turn_id": "turn-retry-1"}
    roots = [record for record in records if record["kind"] == "run.start"]
    assert roots[1]["links"] == [{"type": "continues_from", "record": roots[0]["id"]}]
