from __future__ import annotations

import asyncio
import json
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker

pytest.importorskip("agent_framework")

from agent_framework import (  # noqa: E402
    Agent,
    AgentContext,
    AgentMiddleware,
    AgentResponse,
    AgentResponseUpdate,
    BaseChatClient,
    ChatContext,
    ChatMiddleware,
    ChatMiddlewareLayer,
    ChatResponse,
    ChatResponseUpdate,
    Content,
    FunctionInvocationContext,
    FunctionInvocationLayer,
    FunctionMiddleware,
    FunctionTool,
    Message,
)

from semantic_layer import (  # noqa: E402
    _framework_adapter_shared,
    initialize,
    reset_capture_for_tests,
)
from semantic_layer.capture_v1 import AdmissionReceipt, OpenTraceReceipt  # noqa: E402
from semantic_layer.microsoft_agent_framework_adapter import (  # noqa: E402
    microsoft_agent_framework_adapter,
)

_SCHEMA_PATH = Path(__file__).parents[4] / "contracts/trace/v1/semantic-trace-record.schema.json"
_VALIDATOR = Draft202012Validator(
    json.loads(_SCHEMA_PATH.read_text()),
    format_checker=FormatChecker(),
)


def _semantic_records(path: str) -> list[dict[str, Any]]:
    records = [json.loads(line) for line in (Path(path) / "trace.jsonl").read_text().splitlines()]
    for record in records:
        _VALIDATOR.validate(record)
    return records


class _FixtureAgent:
    def __init__(self) -> None:
        self.id = "agent-1"
        self.name = "fixture"
        self.description = "fixture"
        self.client = None
        self.default_options = {"model": "fixture-model"}
        self.agent_middleware: list[Any] = []
        self.context_providers: list[Any] = []
        self.mcp_tools: list[Any] = []
        self.middleware: list[Any] = []


def test_factory_resolves_repo_style_core_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[str] = []

    def distribution_version(distribution: str) -> str:
        requested.append(distribution)
        if distribution == "agent-framework-core":
            return "1.11.0"
        raise AssertionError(f"unexpected distribution lookup: {distribution}")

    monkeypatch.setattr(
        _framework_adapter_shared,
        "distribution_version",
        distribution_version,
    )

    assert microsoft_agent_framework_adapter(version="1.11.0").version == "1.11.0"
    assert requested == ["agent-framework-core"]


def test_factory_rejects_core_version_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        _framework_adapter_shared,
        "distribution_version",
        lambda distribution: (
            "1.11.0"
            if distribution == "agent-framework-core"
            else pytest.fail(f"unexpected distribution lookup: {distribution}")
        ),
    )

    with pytest.raises(
        ValueError,
        match=(
            "requested version 1.10.0 does not match installed "
            "agent-framework-core distribution 1.11.0"
        ),
    ):
        microsoft_agent_framework_adapter(version="1.10.0")


@pytest.mark.asyncio
async def test_stream_fragments_project_one_exact_tool_chain(tmp_path: Path) -> None:
    assert version("agent-framework-core") == "1.11.0"
    reset_capture_for_tests()
    agent = _FixtureAgent()
    capture = initialize(output=tmp_path, service_name="maf-semantic-projection")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    agent_middleware, _, function_middleware = agent.middleware
    context = AgentContext(
        agent=agent,
        messages=[Message("user", ["Find order 7"])],
        stream=True,
        metadata={
            "conversation_id": "support",
            "turn_id": "maf-turn-1",
            "turn_index": 0,
        },
    )

    async def start_stream() -> None:
        context.result = None

    await agent_middleware.process(context, start_stream)
    proposal_updates = [
        AgentResponseUpdate(
            contents=[
                Content(
                    type="function_call",
                    call_id="maf-tool-1",
                    name="lookup_order",
                    arguments="",
                )
            ],
            response_id="maf-response-1",
            message_id="maf-message-1",
        ),
        AgentResponseUpdate(
            contents=[Content(type="function_call", arguments='{"order')],
            response_id="maf-response-1",
            message_id="maf-message-1",
        ),
        AgentResponseUpdate(
            contents=[Content(type="function_call", arguments='_id": 7}')],
            response_id="maf-response-1",
            message_id="maf-message-1",
        ),
    ]
    for update in proposal_updates:
        assert await context.stream_transform_hooks[0](update) is update

    invocation = FunctionInvocationContext(
        function=FunctionTool(
            name="lookup_order",
            description="Find an order",
            func=lambda order_id: {"order_id": order_id},
        ),
        arguments={"order_id": 7},
        metadata={"call_id": "maf-tool-1"},
    )
    tool_result = [Content(type="text", text="Order 7 shipped.")]

    async def execute_tool() -> None:
        invocation.result = tool_result

    await function_middleware.process(invocation, execute_tool)
    assert invocation.result is tool_result

    final = AgentResponse(
        messages=[Message("assistant", ["Order 7 shipped."])],
        response_id="maf-response-1",
    )
    final.usage_details = {"input_tokens": 7, "output_tokens": 4}
    assert await context.stream_result_hooks[0](final) is final

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    assert not [record for record in records if record["kind"] == "loss"]
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    assert len(proposals) == 1
    proposal = proposals[0]
    call = next(record for record in records if record["kind"] == "tool.call")
    result = next(record for record in records if record["kind"] == "tool.result")
    assert proposal["data"]["name"] == "lookup_order"
    assert proposal["data"]["input"] == {"order_id": 7}
    assert call["data"]["input"] == {"order_id": 7}
    assert result["data"]["output"] == "Order 7 shipped."
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]


@pytest.mark.asyncio
async def test_complete_mapping_proposal_and_tool_error_keep_prior_behavior(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    agent = _FixtureAgent()
    capture = initialize(output=tmp_path, service_name="maf-prior-projection")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    agent_middleware, _, function_middleware = agent.middleware
    context = AgentContext(agent=agent, messages=[], stream=True)

    async def start_stream() -> None:
        context.result = None

    await agent_middleware.process(context, start_stream)
    proposal = AgentResponseUpdate(
        contents=[
            Content(
                type="function_call",
                call_id="maf-tool-2",
                name="lookup_order",
                arguments={"order_id": 8},
            )
        ]
    )
    assert await context.stream_transform_hooks[0](proposal) is proposal
    invocation = FunctionInvocationContext(
        function=FunctionTool(
            name="lookup_order",
            description="Find an order",
            func=lambda order_id: {"order_id": order_id},
        ),
        arguments={"order_id": 8},
        metadata={"call_id": "maf-tool-2"},
    )
    failure = RuntimeError("tool unavailable")

    async def fail_tool() -> None:
        raise failure

    with pytest.raises(RuntimeError) as raised:
        await function_middleware.process(invocation, fail_tool)
    assert raised.value is failure
    final = AgentResponse(messages=[Message("assistant", ["Unable to look it up."])])
    assert await context.stream_result_hooks[0](final) is final

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()
    assert len([record for record in records if record["kind"] == "tool.proposal"]) == 1
    result = next(record for record in records if record["kind"] == "tool.result")
    assert result["data"]["status"] == "failed"
    assert result["data"]["error"]["message"] == "tool unavailable"


@pytest.mark.asyncio
async def test_unexecuted_stream_proposal_uses_final_framework_value(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    agent = _FixtureAgent()
    capture = initialize(output=tmp_path, service_name="maf-final-proposal")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    agent_middleware, _, _ = agent.middleware
    context = AgentContext(agent=agent, messages=[], stream=True)

    async def start_stream() -> None:
        context.result = None

    await agent_middleware.process(context, start_stream)
    fragments = [
        AgentResponseUpdate(
            contents=[
                Content(
                    type="function_call",
                    call_id="maf-tool-3",
                    name="lookup_order",
                    arguments="",
                )
            ]
        ),
        AgentResponseUpdate(contents=[Content(type="function_call", arguments='{"order_id": 9}')]),
    ]
    for update in fragments:
        assert await context.stream_transform_hooks[0](update) is update
    final = AgentResponse(
        messages=[
            Message(
                "assistant",
                [
                    Content(
                        type="function_call",
                        call_id="maf-tool-3",
                        name="lookup_order",
                        arguments='{"order_id": 9}',
                    )
                ],
            )
        ]
    )
    assert await context.stream_result_hooks[0](final) is final

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    assert len(proposals) == 1
    assert proposals[0]["data"]["input"] == '{"order_id": 9}'
    assert not [record for record in records if record["kind"] == "tool.call"]


@pytest.mark.asyncio
async def test_chat_middleware_projects_two_exact_provider_phases_around_tool_result(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    agent = _FixtureAgent()
    capture = initialize(output=tmp_path, service_name="maf-chat-phases")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    agent_middleware, chat_middleware, function_middleware = agent.middleware
    user = Message("user", ["Find order 10"])
    proposal_content = Content(
        type="function_call",
        call_id="maf-tool-10",
        name="lookup_order",
        arguments={"order_id": 10},
    )
    proposal_message = Message("assistant", [proposal_content])
    result_content = Content(
        type="function_result",
        call_id="maf-tool-10",
        result="Order 10 shipped.",
    )
    result_message = Message("tool", [result_content])
    first_chat = ChatContext(
        client=agent,
        messages=[user],
        options={"model": "fixture-model", "tools": []},
    )
    second_chat = ChatContext(
        client=agent,
        messages=[user, proposal_message, result_message],
        options={"model": "fixture-model", "tools": []},
    )
    agent_context = AgentContext(
        agent=agent,
        messages=[user],
        metadata={"conversation_id": "support", "turn_id": "maf-turn-10"},
    )
    tool_context = FunctionInvocationContext(
        function=FunctionTool(
            name="lookup_order",
            description="Find an order",
            func=lambda order_id: {"order_id": order_id},
        ),
        arguments={"order_id": 10},
        metadata={"call_id": "maf-tool-10"},
    )
    tool_result = [Content(type="text", text="Order 10 shipped.")]

    async def execute_agent() -> None:
        async def first_provider_call() -> None:
            first_chat.result = ChatResponse(
                messages=[proposal_message],
                model="fixture-model",
                finish_reason="tool_calls",
            )

        await chat_middleware.process(first_chat, first_provider_call)

        async def execute_tool() -> None:
            tool_context.result = tool_result

        await function_middleware.process(tool_context, execute_tool)

        async def second_provider_call() -> None:
            second_chat.result = ChatResponse(
                messages=[Message("assistant", ["Order 10 shipped."])],
                model="fixture-model",
                finish_reason="stop",
            )

        await chat_middleware.process(second_chat, second_provider_call)
        agent_context.result = AgentResponse(messages=[Message("assistant", ["Order 10 shipped."])])

    await agent_middleware.process(agent_context, execute_agent)

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    assert not [record for record in records if record["kind"] == "loss"]
    requests = [record for record in records if record["kind"] == "model.request"]
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(requests) == 2
    assert len(responses) == 2
    messages = [record for record in records if record["kind"] == "message"]
    assert [record["data"] for record in messages] == [{"role": "user", "content": "Find order 10"}]
    tool_result_record = next(record for record in records if record["kind"] == "tool.result")
    assert requests[0]["data"]["context_refs"] == [messages[0]["id"]]
    assert requests[1]["data"]["context_refs"] == [
        messages[0]["id"],
        responses[0]["id"],
        tool_result_record["id"],
    ]
    assert responses[0]["links"] == [{"type": "result_of", "record": requests[0]["id"]}]
    assert responses[1]["links"] == [{"type": "result_of", "record": requests[1]["id"]}]
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    call = next(record for record in records if record["kind"] == "tool.call")
    result = next(record for record in records if record["kind"] == "tool.result")
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert proposal["parent"] == responses[0]["id"]


@pytest.mark.asyncio
async def test_agent_framework_preserves_exposed_text_reasoning_in_message_order(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    agent = _FixtureAgent()
    capture = initialize(output=tmp_path, service_name="maf-reasoning")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    agent_middleware, chat_middleware, _ = agent.middleware
    user = Message("user", ["Choose the current record."])
    agent_context = AgentContext(agent=agent, messages=[user])
    chat_context = ChatContext(
        client=agent,
        messages=[user],
        options={"model": "fixture-reasoning-model"},
    )

    async def execute_agent() -> None:
        async def execute_model() -> None:
            chat_context.result = ChatResponse(
                messages=[
                    Message(
                        "assistant",
                        [
                            Content.from_text_reasoning(text="Inspect both records."),
                            Content(
                                type="text_reasoning",
                                protected_data="opaque-protected-reasoning",
                            ),
                            Content.from_text(text="Use record two."),
                            Content.from_text_reasoning(text="Inspect both records."),
                        ],
                    )
                ],
                model="fixture-reasoning-model",
                finish_reason="stop",
            )

        await chat_middleware.process(chat_context, execute_model)
        agent_context.result = AgentResponse(
            messages=[Message("assistant", ["Use record two."])]
        )

    await agent_middleware.process(agent_context, execute_agent)

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect both records."},
        {"type": "text", "text": "Inspect both records."},
    ]
    assert response["data"]["content"] == [
        {"role": "assistant", "content": "Use record two."}
    ]
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "unsupported_native_value"
    assert losses[0]["parent"] == response["id"]
    assert "opaque-protected-reasoning" not in json.dumps(losses[0])


@pytest.mark.asyncio
async def test_agent_framework_aggregates_streamed_reasoning_when_final_omits_it(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    agent = _FixtureAgent()
    capture = initialize(output=tmp_path, service_name="maf-streamed-reasoning")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    agent_middleware, chat_middleware, _ = agent.middleware
    user = Message("user", ["Choose the current record."])
    agent_context = AgentContext(agent=agent, messages=[user])
    chat_context = ChatContext(
        client=agent,
        messages=[user],
        options={"model": "fixture-reasoning-model"},
        stream=True,
    )

    async def execute_agent() -> None:
        async def start_model_stream() -> None:
            chat_context.result = None

        await chat_middleware.process(chat_context, start_model_stream)
        for text in ("Inspect both ", "records."):
            update = AgentResponseUpdate(
                contents=[Content.from_text_reasoning(text=text)]
            )
            assert await chat_context.stream_transform_hooks[0](update) is update
        final = ChatResponse(
            messages=[Message("assistant", ["Use record two."])],
            model="fixture-reasoning-model",
            finish_reason="stop",
        )
        assert await chat_context.stream_result_hooks[0](final) is final
        agent_context.result = AgentResponse(
            messages=[Message("assistant", ["Use record two."])]
        )

    await agent_middleware.process(agent_context, execute_agent)

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["reasoning"] == [
        {"type": "text", "text": "Inspect both records."}
    ]
    assert response["data"]["content"] == [
        {"role": "assistant", "content": "Use record two."}
    ]


@pytest.mark.asyncio
async def test_agent_framework_keeps_exact_stream_reasoning_when_final_is_partial(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    agent = _FixtureAgent()
    capture = initialize(output=tmp_path, service_name="maf-partial-final-reasoning")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    agent_middleware, chat_middleware, _ = agent.middleware
    user = Message("user", ["Choose the current record."])
    agent_context = AgentContext(agent=agent, messages=[user])
    chat_context = ChatContext(
        client=agent,
        messages=[user],
        options={"model": "fixture-reasoning-model"},
        stream=True,
    )

    async def execute_agent() -> None:
        async def start_model_stream() -> None:
            chat_context.result = None

        await chat_middleware.process(chat_context, start_model_stream)
        for text in ("Inspect both ", "records."):
            update = ChatResponseUpdate(
                contents=[Content.from_text_reasoning(text=text)],
                response_id="maf-reasoning-response",
                message_id="maf-reasoning-message",
            )
            assert await chat_context.stream_transform_hooks[0](update) is update
        final = ChatResponse(
            messages=[
                Message(
                    "assistant",
                    [Content.from_text_reasoning(text="records.")],
                    message_id="maf-reasoning-message",
                )
            ],
            response_id="maf-reasoning-response",
            model="fixture-reasoning-model",
            finish_reason="stop",
        )
        assert await chat_context.stream_result_hooks[0](final) is final
        agent_context.result = AgentResponse(messages=[Message("assistant", ["Use record two."])])

    await agent_middleware.process(agent_context, execute_agent)

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["reasoning"] == [{"type": "text", "text": "Inspect both records."}]
    assert "content" not in response["data"]


@pytest.mark.asyncio
async def test_concurrent_model_streams_keep_exact_response_and_message_correlation() -> None:
    class Sink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []

        def open_trace(self, record: dict[str, Any]) -> OpenTraceReceipt:
            turn_id = record["native_identity"]
            return OpenTraceReceipt(
                True,
                record_id=f"root-{turn_id}",
                identity={
                    "run_id": f"run-{turn_id}",
                    "trace_id": f"trace-{turn_id}",
                    "operation_id": turn_id,
                },
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            self.records.append(record)
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    agent = _FixtureAgent()
    sink = Sink()
    lifecycle = (
        microsoft_agent_framework_adapter(version="1.11.0").create_source(agent).install(sink)
    )
    agent_middleware, chat_middleware, _ = agent.middleware
    both_started = asyncio.Event()
    started = 0

    async def run(label: str) -> None:
        nonlocal started
        user = Message("user", [f"Question {label}"])
        agent_context = AgentContext(
            agent=agent,
            messages=[user],
            metadata={"conversation_id": label, "turn_id": f"turn-{label}"},
        )
        chat_context = ChatContext(
            client=agent,
            messages=[user],
            options={"model": "fixture-reasoning-model"},
            stream=True,
        )

        async def execute_agent() -> None:
            nonlocal started

            async def start_model_stream() -> None:
                chat_context.result = None

            await chat_middleware.process(chat_context, start_model_stream)
            started += 1
            if started == 2:
                both_started.set()
            await both_started.wait()
            response_id = f"response-{label}"
            message_id = f"message-{label}"
            update = ChatResponseUpdate(
                contents=[Content.from_text_reasoning(text=f"Reason {label}.")],
                response_id=response_id,
                message_id=message_id,
            )
            assert await chat_context.stream_transform_hooks[0](update) is update
            final = ChatResponse(
                messages=[
                    Message(
                        "assistant",
                        [Content.from_text_reasoning(text=f"Reason {label}.")],
                        message_id=message_id,
                    )
                ],
                response_id=response_id,
                model="fixture-reasoning-model",
                finish_reason="stop",
            )
            assert await chat_context.stream_result_hooks[0](final) is final
            agent_context.result = AgentResponse(
                messages=[Message("assistant", [f"Answer {label}."])]
            )

        await agent_middleware.process(agent_context, execute_agent)

    await asyncio.gather(run("a"), run("b"))
    lifecycle.deactivate()
    lifecycle.drain()

    for label in ("a", "b"):
        operation_id = f"turn-{label}"
        records = [
            record for record in sink.records if record["trace"]["operation_id"] == operation_id
        ]
        request = next(
            record
            for record in records
            if record["name"] == "agent_framework.model" and record["phase"] == "start"
        )
        update = next(
            record
            for record in records
            if record["name"] == "agent_framework.response.update" and record["phase"] == "event"
        )
        response = next(
            record
            for record in records
            if record["name"] == "agent_framework.model" and record["phase"] == "end"
        )
        assert update["native"]["update"]["response_id"] == f"response-{label}"
        assert update["native"]["update"]["message_id"] == f"message-{label}"
        assert response["native"]["output"]["response_id"] == f"response-{label}"
        assert response["native"]["output"]["messages"][0]["message_id"] == (f"message-{label}")
        assert response["semantic"]["reasoning"] == [{"type": "text", "text": f"Reason {label}."}]
        assert "content" not in response["semantic"]
        request_record_id = f"record-{sink.records.index(request) + 1}"
        assert response["parent_record_id"] == request_record_id


@pytest.mark.asyncio
async def test_provider_failure_is_one_finite_error_and_preserves_identity(
    tmp_path: Path,
) -> None:
    class ExplosiveProviderError(RuntimeError):
        @property
        def structured_error(self) -> Any:
            raise AssertionError("unsafe provider error accessor was evaluated")

    reset_capture_for_tests()
    agent = _FixtureAgent()
    capture = initialize(output=tmp_path, service_name="maf-provider-failure")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    agent_middleware, chat_middleware, _ = agent.middleware
    user = Message("user", ["Find order 11"])
    agent_context = AgentContext(agent=agent, messages=[user])
    chat_context = ChatContext(
        client=agent,
        messages=[user],
        options={"model": "fixture-model"},
    )
    failure = ExplosiveProviderError("provider unavailable")

    async def execute_agent() -> None:
        async def fail_provider() -> None:
            raise failure

        await chat_middleware.process(chat_context, fail_provider)

    with pytest.raises(ExplosiveProviderError) as raised:
        await agent_middleware.process(agent_context, execute_agent)
    assert raised.value is failure

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    assert not [record for record in records if record["kind"] == "loss"]
    failed_responses = [
        record
        for record in records
        if record["kind"] == "model.response" and record["data"]["status"] == "failed"
    ]
    errors = [record for record in records if record["kind"] == "error"]
    assert len(failed_responses) == 1
    assert len(errors) == 1
    assert errors[0]["data"] == {
        "type": "explosive_provider_error",
        "message": "provider unavailable",
        "recoverable": False,
        "details": {"native_type": "ExplosiveProviderError"},
    }
    assert errors[0]["parent"] == failed_responses[0]["id"]
    assert records[-1]["kind"] == "run.outcome"
    assert records[-1]["data"]["status"] == "failed"


@pytest.mark.asyncio
async def test_real_agent_loop_dispatches_all_three_middleware_roles(
    tmp_path: Path,
) -> None:
    class FixtureClient(
        FunctionInvocationLayer,
        ChatMiddlewareLayer,
        BaseChatClient,
    ):
        def __init__(self) -> None:
            super().__init__()
            self.call_count = 0

        async def _inner_get_response(
            self,
            *,
            messages: Any,
            stream: bool,
            options: Any,
            **kwargs: Any,
        ) -> ChatResponse:
            del messages, stream, options, kwargs
            self.call_count += 1
            if self.call_count == 1:
                return ChatResponse(
                    messages=[
                        Message(
                            "assistant",
                            [
                                Content(
                                    type="function_call",
                                    call_id="maf-tool-12",
                                    name="lookup_order",
                                    arguments={"order_id": 12},
                                )
                            ],
                        )
                    ],
                    model="fixture-model",
                    finish_reason="tool_calls",
                )
            return ChatResponse(
                messages=[Message("assistant", ["Order 12 shipped."])],
                model="fixture-model",
                finish_reason="stop",
            )

    reset_capture_for_tests()
    client = FixtureClient()

    def lookup_order(order_id: int) -> str:
        return f"Order {order_id} shipped."

    tool = FunctionTool(
        name="lookup_order",
        description="Find an order",
        func=lookup_order,
    )
    agent = Agent(
        client=client,
        tools=[tool],
        default_options={"model": "fixture-model"},
    )
    capture = initialize(output=tmp_path, service_name="maf-real-loop")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )

    result = await agent.run([Message("user", ["Find order 12"])])
    assert result.text == "Order 12 shipped."
    assert client.call_count == 2

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    assert not [record for record in records if record["kind"] == "loss"]
    messages = [record for record in records if record["kind"] == "message"]
    requests = [record for record in records if record["kind"] == "model.request"]
    responses = [record for record in records if record["kind"] == "model.response"]
    tool_result = next(record for record in records if record["kind"] == "tool.result")
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    assert len(messages) == 1
    assert len(requests) == 2
    assert len(responses) == 2
    assert requests[0]["data"]["context_refs"] == [messages[0]["id"]]
    assert requests[1]["data"]["context_refs"] == [
        messages[0]["id"],
        responses[0]["id"],
        tool_result["id"],
    ]
    assert proposal["parent"] == responses[0]["id"]
    assert [record["kind"] for record in records].count("tool.proposal") == 1
    assert [record["kind"] for record in records].count("tool.call") == 1
    assert [record["kind"] for record in records].count("tool.result") == 1
    outcome = records[-1]
    assert outcome["kind"] == "run.outcome"
    assert outcome["data"]["status"] == "completed"
    assert outcome["links"] == [
        {
            "type": "derived_from",
            "record": responses[-1]["id"],
        }
    ]


@pytest.mark.asyncio
async def test_real_agent_capture_observes_existing_outer_postprocessing(
    tmp_path: Path,
) -> None:
    class FixtureClient(
        FunctionInvocationLayer,
        ChatMiddlewareLayer,
        BaseChatClient,
    ):
        def __init__(self) -> None:
            super().__init__()
            self.call_count = 0
            self.seen_tool_result: Any = None

        async def _inner_get_response(
            self,
            *,
            messages: Any,
            stream: bool,
            options: Any,
            **kwargs: Any,
        ) -> ChatResponse:
            del stream, options, kwargs
            self.call_count += 1
            if self.call_count == 1:
                return ChatResponse(
                    messages=[
                        Message(
                            "assistant",
                            [
                                Content(
                                    type="function_call",
                                    call_id="maf-tool-post",
                                    name="lookup_order",
                                    arguments={"order_id": 13},
                                )
                            ],
                        )
                    ],
                    model="fixture-model",
                    finish_reason="tool_calls",
                )
            self.seen_tool_result = messages[-1].contents[0].result
            return ChatResponse(
                messages=[Message("assistant", ["provider inner"])],
                model="fixture-model",
                finish_reason="stop",
            )

    outer_chat = ChatResponse(
        messages=[Message("assistant", ["provider outer"])],
        model="fixture-model",
        finish_reason="stop",
    )
    outer_agent = AgentResponse(messages=[Message("assistant", ["agent outer"])])
    outer_tool = [Content(type="text", text="tool outer")]

    class ExistingAgentMiddleware(AgentMiddleware):
        async def process(self, context: Any, call_next: Any) -> None:
            await call_next()
            context.result = outer_agent

    class ExistingChatMiddleware(ChatMiddleware):
        async def process(self, context: Any, call_next: Any) -> None:
            await call_next()
            if context.result is not None and context.result.finish_reason == "stop":
                context.result = outer_chat

    class ExistingFunctionMiddleware(FunctionMiddleware):
        async def process(self, context: Any, call_next: Any) -> None:
            await call_next()
            context.result = outer_tool

    def lookup_order(order_id: int) -> str:
        return f"tool inner {order_id}"

    existing = [
        ExistingAgentMiddleware(),
        ExistingChatMiddleware(),
        ExistingFunctionMiddleware(),
    ]
    client = FixtureClient()
    agent = Agent(
        client=client,
        tools=[
            FunctionTool(
                name="lookup_order",
                description="Find an order",
                func=lookup_order,
            )
        ],
        default_options={"model": "fixture-model"},
        middleware=existing,
    )
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="maf-outer-postprocessing")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )
    assert agent.middleware[-len(existing) :] == existing

    result = await agent.run([Message("user", ["Find order 13"])])
    assert result is outer_agent
    assert client.seen_tool_result == "tool outer"

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()
    assert agent.middleware == existing

    responses = [record for record in records if record["kind"] == "model.response"]
    tool_result = next(record for record in records if record["kind"] == "tool.result")
    outcome = records[-1]
    assert responses[-1]["data"]["content"] == [{"role": "assistant", "content": "provider outer"}]
    assert tool_result["data"]["output"] == "tool outer"
    assert outcome["kind"] == "run.outcome"
    assert outcome["data"] == {
        "status": "completed",
        "output": "agent outer",
    }
    assert "messages" not in outcome["data"]
    assert "raw_representation" not in outcome["data"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delivered", "expected_output", "expected_gap", "adapter_version"),
    [
        pytest.param(
            AgentResponse(messages=[], value={"answer": 42}),
            {"answer": 42},
            None,
            "1.11.0",
            id="structured-value",
        ),
        pytest.param(
            AgentResponse(
                messages=[Message("assistant", ['{"answer": 42}'])],
                response_format={
                    "type": "object",
                    "properties": {"answer": {"type": "integer"}},
                    "required": ["answer"],
                },
            ),
            '{"answer": 42}',
            "structured_output_not_materialized",
            "1.11.0",
            id="lazy-structured-value",
        ),
        pytest.param(
            AgentResponse(
                messages=[Message("assistant", ['{"answer": 42}'])],
                value={"answer": 42},
            ),
            '{"answer": 42}',
            "structured_output_projection_unverified_version",
            "1.12.0",
            id="unverified-version-structured-value",
        ),
        pytest.param(
            AgentResponse(messages=[Message("assistant", ["hello", "world"])]),
            "hello world",
            None,
            "1.11.0",
            id="multipart-message-text",
        ),
        pytest.param(
            AgentResponse(
                messages=[
                    Message("assistant", ["first"]),
                    Message("assistant", ["second"]),
                ]
            ),
            "firstsecond",
            None,
            "1.11.0",
            id="multi-message-text",
        ),
        pytest.param(
            AgentResponse(
                messages=[
                    Message(
                        "assistant",
                        [
                            Content(
                                type="function_call",
                                call_id="large-call",
                                name="large_tool",
                                arguments={"payload": "large-argument-marker-" * 1_000},
                            ),
                            Content(
                                type="function_result",
                                call_id="large-call",
                                result="large-result-marker-" * 1_000,
                            ),
                            "final visible answer",
                        ],
                    )
                ]
            ),
            "final visible answer",
            None,
            "1.11.0",
            id="large-tool-history",
        ),
    ],
)
async def test_real_agent_outcome_keeps_only_exact_delivered_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    delivered: AgentResponse[Any],
    expected_output: Any,
    expected_gap: str | None,
    adapter_version: str,
) -> None:
    class FixtureClient(
        FunctionInvocationLayer,
        ChatMiddlewareLayer,
        BaseChatClient,
    ):
        async def _inner_get_response(
            self,
            *,
            messages: Any,
            stream: bool,
            options: Any,
            **kwargs: Any,
        ) -> ChatResponse:
            del messages, stream, options, kwargs
            return ChatResponse(
                messages=[Message("assistant", ["provider response"])],
                model="fixture-model",
                finish_reason="stop",
            )

    class ExistingAgentMiddleware(AgentMiddleware):
        async def process(self, context: Any, call_next: Any) -> None:
            await call_next()
            context.result = delivered

    agent = Agent(
        client=FixtureClient(),
        default_options={"model": "fixture-model"},
        middleware=[ExistingAgentMiddleware()],
    )
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="maf-compact-outcome")
    if adapter_version != "1.11.0":
        monkeypatch.setattr(
            _framework_adapter_shared,
            "distribution_version",
            lambda _distribution: adapter_version,
        )
        adapter = microsoft_agent_framework_adapter()
    else:
        adapter = microsoft_agent_framework_adapter(version="1.11.0")
    capture.instrument(
        adapter=adapter,
        client=agent,
    )

    value_parsed_before = vars(delivered).get("_value_parsed")
    value_before = vars(delivered).get("_value")
    result = await agent.run([Message("user", ["Return the fixture response"])])
    assert result is delivered
    assert vars(delivered).get("_value_parsed") is value_parsed_before
    assert vars(delivered).get("_value") is value_before

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()
    outcome = records[-1]
    assert outcome["kind"] == "run.outcome"
    assert outcome["data"] == {
        "status": "completed",
        "output": expected_output,
    }
    serialized = json.dumps(outcome["data"])
    assert "large-argument-marker" not in serialized
    assert "large-result-marker" not in serialized
    assert len(serialized) < 1_024
    structured_gaps = [
        record
        for record in records
        if record["kind"] == "loss"
        and record["data"].get("reason", "").startswith("structured_output")
    ]
    assert [record["data"]["reason"] for record in structured_gaps] == (
        [expected_gap] if expected_gap is not None else []
    )
    if structured_gaps:
        serialized_gap = json.dumps(structured_gaps[0]["data"])
        assert '"properties"' not in serialized_gap
        assert len(serialized_gap) < 1_024


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_accepted", [True, False])
async def test_streamed_large_native_response_is_retained_once(
    terminal_accepted: bool,
) -> None:
    class Sink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id="root",
                identity={
                    "run_id": "run",
                    "trace_id": "trace",
                    "operation_id": "operation",
                },
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            self.records.append(record)
            if (
                not terminal_accepted
                and record["name"] == "agent_framework.response.update"
                and record["phase"] == "end"
            ):
                return AdmissionReceipt(False, reason="backpressure")
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    agent = _FixtureAgent()
    sink = Sink()
    lifecycle = (
        microsoft_agent_framework_adapter(version="1.11.0").create_source(agent).install(sink)
    )
    agent_middleware = agent.middleware[0]
    context = AgentContext(agent=agent, messages=[], stream=True)

    async def start_stream() -> None:
        context.result = None

    await agent_middleware.process(context, start_stream)
    marker = "large-stream-result-marker-" * 1_000
    final = AgentResponse(
        messages=[
            Message(
                "assistant",
                [
                    Content(
                        type="function_result",
                        call_id="large-call",
                        result=marker,
                    ),
                    "final visible answer",
                ],
            )
        ]
    )
    assert await context.stream_result_hooks[0](final) is final
    lifecycle.deactivate()

    terminal = next(
        record
        for record in sink.records
        if record["name"] == "agent_framework.response.update" and record["phase"] == "end"
    )
    outcome = next(
        record
        for record in sink.records
        if record["name"] == "agent_framework.run" and record["phase"] == "end"
    )
    assert marker in json.dumps(terminal["native"])
    if terminal_accepted:
        assert outcome["native"] == {"terminal_ref": "record-1"}
        assert marker not in json.dumps(outcome["native"])
    else:
        assert marker in json.dumps(outcome["native"])


@pytest.mark.asyncio
async def test_real_agent_provider_failure_has_no_unobserved_output_reference(
    tmp_path: Path,
) -> None:
    class FailingClient(
        FunctionInvocationLayer,
        ChatMiddlewareLayer,
        BaseChatClient,
    ):
        async def _inner_get_response(
            self,
            *,
            messages: Any,
            stream: bool,
            options: Any,
            **kwargs: Any,
        ) -> ChatResponse:
            del messages, stream, options, kwargs
            raise failure

    failure = RuntimeError("provider failed before output")
    agent = Agent(
        client=FailingClient(),
        default_options={"model": "fixture-model"},
    )
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="maf-failure-delivery")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )

    with pytest.raises(RuntimeError) as raised:
        await agent.run([Message("user", ["Fail before output"])])
    assert raised.value is failure

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    assert not [record for record in records if record["kind"] == "loss"]
    outcome = records[-1]
    assert outcome["kind"] == "run.outcome"
    assert outcome["data"]["status"] == "failed"
    assert "output" not in outcome["data"]
    assert "links" not in outcome


@pytest.mark.asyncio
async def test_real_agent_late_failure_links_only_observed_partial_output(
    tmp_path: Path,
) -> None:
    class PartiallyFailingClient(
        FunctionInvocationLayer,
        ChatMiddlewareLayer,
        BaseChatClient,
    ):
        def __init__(self) -> None:
            super().__init__()
            self.call_count = 0

        async def _inner_get_response(
            self,
            *,
            messages: Any,
            stream: bool,
            options: Any,
            **kwargs: Any,
        ) -> ChatResponse:
            del messages, stream, options, kwargs
            self.call_count += 1
            if self.call_count == 1:
                return ChatResponse(
                    messages=[
                        Message(
                            "assistant",
                            [
                                Content(
                                    type="function_call",
                                    call_id="maf-tool-partial",
                                    name="lookup_order",
                                    arguments={"order_id": 15},
                                )
                            ],
                        )
                    ],
                    model="fixture-model",
                    finish_reason="tool_calls",
                )
            raise failure

    def lookup_order(order_id: int) -> str:
        return f"Order {order_id} shipped."

    failure = RuntimeError("provider failed after partial output")
    agent = Agent(
        client=PartiallyFailingClient(),
        tools=[
            FunctionTool(
                name="lookup_order",
                description="Find an order",
                func=lookup_order,
            )
        ],
        default_options={"model": "fixture-model"},
    )
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="maf-partial-failure")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )

    with pytest.raises(RuntimeError) as raised:
        await agent.run([Message("user", ["Find order 15"])])
    assert raised.value is failure

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    responses = [record for record in records if record["kind"] == "model.response"]
    assert [record["data"]["status"] for record in responses] == ["completed", "failed"]
    outcome = records[-1]
    assert outcome["kind"] == "run.outcome"
    assert outcome["data"]["status"] == "failed"
    assert outcome["links"] == [{"type": "derived_from", "record": responses[0]["id"]}]


@pytest.mark.asyncio
async def test_real_agent_same_message_identity_is_reused_only_while_unchanged(
    tmp_path: Path,
) -> None:
    class FixtureClient(
        FunctionInvocationLayer,
        ChatMiddlewareLayer,
        BaseChatClient,
    ):
        def __init__(self) -> None:
            super().__init__()
            self.call_count = 0

        async def _inner_get_response(
            self,
            *,
            messages: Any,
            stream: bool,
            options: Any,
            **kwargs: Any,
        ) -> ChatResponse:
            del messages, stream, options, kwargs
            self.call_count += 1
            if self.call_count == 1:
                return ChatResponse(
                    messages=[
                        Message(
                            "assistant",
                            [
                                Content(
                                    type="function_call",
                                    call_id="maf-tool-mutated-context",
                                    name="lookup_order",
                                    arguments={"order_id": 14},
                                )
                            ],
                        )
                    ],
                    model="fixture-model",
                    finish_reason="tool_calls",
                )
            return ChatResponse(
                messages=[Message("assistant", ["done"])],
                model="fixture-model",
                finish_reason="stop",
            )

    class MutateSameMessage(ChatMiddleware):
        def __init__(self) -> None:
            self.call_count = 0

        async def process(self, context: Any, call_next: Any) -> None:
            await call_next()
            self.call_count += 1
            if self.call_count == 1:
                context.messages[0].contents = [Content(type="text", text="Find mutated order 14")]

    def lookup_order(order_id: int) -> str:
        return f"Order {order_id} shipped."

    original = Message("user", ["Find order 14"])
    agent = Agent(
        client=FixtureClient(),
        tools=[
            FunctionTool(
                name="lookup_order",
                description="Find an order",
                func=lookup_order,
            )
        ],
        default_options={"model": "fixture-model"},
        middleware=[MutateSameMessage()],
    )
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="maf-mutated-context")
    capture.instrument(
        adapter=microsoft_agent_framework_adapter(version="1.11.0"),
        client=agent,
    )

    result = await agent.run([original])
    assert result.text == "done"
    assert original.text == "Find mutated order 14"

    records = _semantic_records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    messages = [record for record in records if record["kind"] == "message"]
    requests = [record for record in records if record["kind"] == "model.request"]
    responses = [record for record in records if record["kind"] == "model.response"]
    tool_result = next(record for record in records if record["kind"] == "tool.result")
    assert [record["data"]["content"] for record in messages] == [
        "Find order 14",
        "Find mutated order 14",
    ]
    assert requests[0]["data"]["context_refs"] == [messages[0]["id"]]
    assert requests[1]["data"]["context_refs"] == [
        messages[1]["id"],
        responses[0]["id"],
        tool_result["id"],
    ]
