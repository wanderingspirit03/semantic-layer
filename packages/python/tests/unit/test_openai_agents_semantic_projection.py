from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from importlib.metadata import version as distribution_version
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import agents
import pytest
from jsonschema import Draft202012Validator, FormatChecker
from openai.types.chat import ChatCompletionMessage
from openai.types.chat.chat_completion_message_function_tool_call import (
    ChatCompletionMessageFunctionToolCall,
    Function,
)
from openai.types.responses import (
    ResponseCompletedEvent,
    ResponseCreatedEvent,
    ResponseErrorEvent,
    ResponseFailedEvent,
    ResponseIncompleteEvent,
    ResponseReasoningSummaryTextDeltaEvent,
    ResponseTextDeltaEvent,
)
from openai.types.responses.response_reasoning_item import (
    Content as ResponseReasoningContent,
)
from openai.types.responses.response_reasoning_item import ResponseReasoningItem
from openai.types.responses.response_reasoning_item import Summary as ResponseReasoningSummary

from semantic_layer import initialize, openai_agents_adapter, reset_capture_for_tests
from semantic_layer.openai_agents_adapter import (
    _is_exact_official_stream_bookkeeping,
    _is_official_terminal_response_failure,
    _record_openai_stream_event,
)
from semantic_layer.validation import validate_artifact

_TRACE_SCHEMA = Path(__file__).parents[4] / "contracts/trace/v1/semantic-trace-record.schema.json"
_TRACE_VALIDATOR = Draft202012Validator(
    json.loads(_TRACE_SCHEMA.read_text()),
    format_checker=FormatChecker(),
)


@pytest.fixture(autouse=True)
def _reset_capture() -> Any:
    yield
    reset_capture_for_tests()


def test_openai_agents_projects_exact_context_model_tool_and_error_evidence(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="openai-agents-semantic-fixture")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=agents,
    )
    agents.set_tracing_disabled(False)

    try:
        call_id = "span_0123456789abcdef01234567"
        tool_input = '{"path":"src/app.py"}'
        with agents.trace(
            "coding-agent",
            group_id="conversation-python",
            metadata={"turn_id": "turn-1", "turn_index": 0},
        ) as trace:
            with agents.generation_span(
                input=[
                    {"role": "user", "content": "Inspect src/app.py."},
                    {
                        "role": "tool",
                        "tool_call_id": "historical-call",
                        "content": "Earlier run output.",
                    },
                ],
                output=[
                    {
                        "type": "function_call",
                        "call_id": call_id,
                        "name": "read_file",
                        "arguments": tool_input,
                    }
                ],
                model="fixture-model",
                usage={"input_tokens": 4, "output_tokens": 2},
                parent=trace,
            ):
                pass
            with agents.function_span(
                "read_file",
                input=tool_input,
                output="ANSWER = 42",
                span_id=call_id,
                parent=trace,
            ):
                pass
            with agents.generation_span(
                input=[{"role": "user", "content": "Now compile it."}],
                output=[],
                model="fixture-model",
                parent=trace,
            ) as failed_generation:
                failed_generation.set_error(
                    {
                        "message": "fixture model failure",
                        "data": {
                            "code": "MODEL_FIXTURE_FAILURE",
                            "retryable": True,
                        },
                    }
                )
        agents.flush_traces()

        closed = capture.shutdown()
        report = validate_artifact(closed.artifact_path)
        assert report.valid
        assert report.issues == ()
        records = _semantic_records(closed.artifact_path)

        losses = [record for record in records if record["kind"] == "loss"]
        assert [(loss["data"]["reason"], loss["data"]["count"]) for loss in losses] == [
            ("tool_chronology_not_captured", 1),
            ("tool_execution_not_captured", 1),
        ]
        assert [record["kind"] for record in records].count("run.start") == 1
        assert [record["kind"] for record in records].count("run.outcome") == 1

        context = [record for record in records if record["kind"] == "message"]
        assert [
            (record["origin"], record["data"]["role"], record["data"]["content"])
            for record in context
        ] == [
            ("context", "user", "Inspect src/app.py."),
            ("context", "tool", "Earlier run output."),
            ("context", "user", "Now compile it."),
        ]
        assert context[1]["data"]["call_id"] == "historical-call"
        assert all(
            record["origin"] == "observed" for record in records if record["kind"] != "message"
        )

        requests = [record for record in records if record["kind"] == "model.request"]
        responses = [record for record in records if record["kind"] == "model.response"]
        assert len(requests) == len(responses) == 2
        assert [request["data"]["context_refs"] for request in requests] == [
            [record["id"] for record in context[:2]],
            [context[2]["id"]],
        ]
        assert all(
            response["links"] == [{"type": "result_of", "record": requests[index]["id"]}]
            for index, response in enumerate(responses)
        )
        assert [response["data"]["status"] for response in responses] == [
            "completed",
            "failed",
        ]
        assert responses[0]["data"]["usage"] == {
            "input_tokens": 4,
            "output_tokens": 2,
        }

        proposal = _record(records, "tool.proposal")
        assert proposal["data"] == {
            "call_id": proposal["data"]["call_id"],
            "native_call_id": call_id,
            "name": "read_file",
            "input": tool_input,
        }
        assert not [record for record in records if record["kind"] in {"tool.call", "tool.result"}]

        errors = [record for record in records if record["kind"] == "error"]
        assert len(errors) == 1
        assert errors[0]["data"] == {
            "type": "openai_agents_span_error",
            "message": "fixture model failure",
            "recoverable": True,
            "details": {
                "code": "MODEL_FIXTURE_FAILURE",
                "retryable": True,
            },
        }
        assert [record["seq"] for record in records] == list(range(1, len(records) + 1))
    finally:
        agents.set_tracing_disabled(True)


def test_openai_agents_preserves_exposed_reasoning_summary_and_text_in_native_order(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="openai-agents-reasoning")
    capture.instrument(
        adapter=openai_agents_adapter(version="0.18.2"),
        client=agents,
    )
    agents.set_tracing_disabled(False)

    try:
        reasoning = ResponseReasoningItem(
            id="reasoning-1",
            type="reasoning",
            summary=[
                ResponseReasoningSummary(type="summary_text", text="Checked the evidence."),
                ResponseReasoningSummary(type="summary_text", text="Checked the evidence."),
            ],
            content=[
                ResponseReasoningContent(type="reasoning_text", text="Compare both records."),
                ResponseReasoningContent(type="reasoning_text", text="The second record wins."),
            ],
            encrypted_content="opaque-provider-state",
        )
        with agents.trace("reasoning-agent") as trace:
            with agents.generation_span(
                input=[{"role": "user", "content": "Choose the current record."}],
                output=[reasoning, {"role": "assistant", "content": "Use record two."}],
                model="fixture-reasoning-model",
                parent=trace,
            ):
                pass
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        response = _record(records, "model.response")
        assert response["data"]["reasoning"] == [
            {"type": "summary", "text": "Checked the evidence."},
            {"type": "summary", "text": "Checked the evidence."},
            {"type": "text", "text": "Compare both records."},
            {"type": "text", "text": "The second record wins."},
        ]
        assert "opaque-provider-state" not in json.dumps(response["data"]["reasoning"])
        losses = [record for record in records if record["kind"] == "loss"]
        assert len(losses) == 1
        assert losses[0]["data"] == {
            "reason": "unsupported_native_value",
            "stage": "source",
            "count": 1,
            "recoverable": False,
            "detail": (
                "OpenAI Agents exposed encrypted reasoning bytes; the bytes "
                "were omitted from canonical reasoning."
            ),
        }
        assert losses[0]["parent"] == response["id"]
    finally:
        agents.set_tracing_disabled(True)


def test_openai_agents_uses_authoritative_end_snapshots_for_context_and_tool_input(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="openai-agents-end-snapshot")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=agents,
    )
    agents.set_tracing_disabled(False)

    try:
        call_id = "span_abcdef0123456789abcdef01"
        tool_input = '{"path":"src/end_snapshot.py"}'
        with agents.trace("end-snapshot-agent") as trace:
            with agents.generation_span(
                input=None,
                output=None,
                model=None,
                parent=trace,
            ) as generation:
                generation.span_data.input = [
                    {"role": "user", "content": "Inspect the end snapshot."}
                ]
                generation.span_data.output = [
                    {
                        "type": "function_call",
                        "call_id": call_id,
                        "name": "read_file",
                        "arguments": tool_input,
                    }
                ]
                generation.span_data.model = "fixture-model"
            with agents.function_span(
                "read_file",
                input=None,
                output=None,
                span_id=call_id,
                parent=trace,
            ) as function:
                function.span_data.input = tool_input
                function.span_data.output = "END_SNAPSHOT = True"
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        request = _record(records, "model.request")
        context = _record(records, "message")
        proposal = _record(records, "tool.proposal")

        assert request["data"]["context_refs"] == [context["id"]]
        assert request["data"]["model"] == "fixture-model"
        assert context["data"] == {
            "role": "user",
            "content": "Inspect the end snapshot.",
        }
        assert proposal["data"]["input"] == tool_input
        assert proposal["data"]["native_call_id"] == call_id
        assert not [record for record in records if record["kind"] in {"tool.call", "tool.result"}]
        losses = [record for record in records if record["kind"] == "loss"]
        assert [(loss["data"]["reason"], loss["data"]["count"]) for loss in losses] == [
            ("tool_execution_not_captured", 1)
        ]
    finally:
        agents.set_tracing_disabled(True)


def test_openai_agents_never_uses_function_span_id_as_model_tool_call_id(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="openai-agents-tool-identity")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=agents,
    )
    agents.set_tracing_disabled(False)

    try:
        model_call_id = "call_from_generation_output"
        function_span_id = "span_eeeeeeeeeeeeeeeeeeeeeeee"
        tool_input = '{"path":"src/identity.py"}'
        with agents.trace("tool-identity-agent") as trace:
            with agents.generation_span(
                input=[{"role": "user", "content": "Inspect the file."}],
                output=[
                    {
                        "type": "function_call",
                        "call_id": model_call_id,
                        "name": "read_file",
                        "arguments": tool_input,
                    }
                ],
                model="fixture-model",
                parent=trace,
            ):
                pass
            with agents.function_span(
                "read_file",
                input=tool_input,
                output="IDENTITY = True",
                span_id=function_span_id,
                parent=trace,
            ):
                pass
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        proposal = _record(records, "tool.proposal")
        assert proposal["data"] == {
            "call_id": proposal["data"]["call_id"],
            "native_call_id": model_call_id,
            "name": "read_file",
            "input": tool_input,
        }
        assert not [record for record in records if record["kind"] in {"tool.call", "tool.result"}]
        losses = [record for record in records if record["kind"] == "loss"]
        assert [(loss["data"]["reason"], loss["data"]["count"]) for loss in losses] == [
            ("tool_execution_not_captured", 1)
        ]
        assert function_span_id not in {
            record["data"].get("native_call_id")
            for record in records
            if record["kind"].startswith("tool.")
        }
    finally:
        agents.set_tracing_disabled(True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reuse_call_id",
    [False, True],
)
async def test_openai_agents_run_result_keeps_late_tool_evidence_and_reports_chronology(
    tmp_path: Path,
    reuse_call_id: bool,
) -> None:
    class Runner:
        subject: Any
        received_hooks: Any = None

        @classmethod
        async def run(
            cls,
            result: Any,
            native_trace: Any,
            model_call_id: str,
            tool_input: str,
            *,
            hooks: Any = None,
        ) -> Any:
            cls.received_hooks = hooks
            # An application can create an identical official function span. It is
            # tracing evidence, not authoritative framework tool-call identity.
            with agents.function_span(
                "read_file",
                input=tool_input,
                output="UNRELATED = True",
                parent=native_trace,
            ) as unrelated:
                cls.subject.processor.on_span_start(unrelated)
            cls.subject.processor.on_span_end(unrelated)
            # RunResult becomes available only after these later model turns. The
            # adapter keeps that real order and reports the missing chronology once.
            for turn in range(3):
                with agents.generation_span(
                    input=[
                        {
                            "role": "tool",
                            "tool_call_id": model_call_id,
                            "content": "CURRENT = True",
                        }
                    ],
                    output=[{"role": "assistant", "content": f"turn-{turn}"}],
                    model="fixture-model",
                    parent=native_trace,
                ) as generation:
                    cls.subject.processor.on_span_start(generation)
                cls.subject.processor.on_span_end(generation)
            cls.subject.processor.on_trace_end(agents.get_current_trace())
            return result

    class Subject:
        Runner: Any

        def __init__(self) -> None:
            self.processor: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            assert self.processor is processor
            self.processor = None

        def get_current_trace(self) -> Any:
            return agents.get_current_trace()

    subject = Subject()
    subject.Runner = Runner
    Runner.subject = subject
    capture = initialize(output=tmp_path, service_name="openai-agents-current-result")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=subject,
    )
    source_agent = agents.Agent(name="fixture-agent")
    model_call_id = "call_current_run_result"
    tool_input = '{"path":"src/current.py"}'
    call_item = agents.ToolCallItem(
        agent=source_agent,
        raw_item={
            "type": "function_call",
            "call_id": model_call_id,
            "name": "read_file",
            "arguments": tool_input,
        },
    )
    output_item = agents.ToolCallOutputItem(
        agent=source_agent,
        raw_item={
            "type": "function_call_output",
            "call_id": model_call_id,
            "output": "CURRENT = True",
        },
        output="CURRENT = True",
    )
    new_items: list[Any] = [call_item, output_item]
    if reuse_call_id:
        new_items.extend([call_item, output_item])
    run_result = agents.RunResult(
        input="Inspect src/current.py.",
        new_items=new_items,
        raw_responses=[],
        final_output={"answer": "done"},
        input_guardrail_results=[],
        output_guardrail_results=[],
        tool_input_guardrail_results=[],
        tool_output_guardrail_results=[],
        context_wrapper=agents.RunContextWrapper(context=None),
        _last_agent=source_agent,
    )
    agents.set_tracing_disabled(False)
    try:
        with agents.trace("existing-current-trace") as native_trace:
            subject.processor.on_trace_start(native_trace)
            with agents.generation_span(
                input=[{"role": "user", "content": "Inspect src/current.py."}],
                output=[
                    ChatCompletionMessage(
                        role="assistant",
                        content=None,
                        tool_calls=[
                            ChatCompletionMessageFunctionToolCall(
                                id=model_call_id,
                                type="function",
                                function=Function(
                                    name="read_file",
                                    arguments=tool_input,
                                ),
                            )
                        ],
                    )
                ],
                model="fixture-model",
                parent=native_trace,
            ) as generation:
                subject.processor.on_span_start(generation)
            subject.processor.on_span_end(generation)

            application_hooks = object()
            assert (
                await Runner.run(
                    run_result,
                    native_trace,
                    model_call_id,
                    tool_input,
                    hooks=application_hooks,
                )
                is run_result
            )
            assert Runner.received_hooks is application_hooks
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        proposal = _record(records, "tool.proposal")
        call = _record(records, "tool.call")
        result = _record(records, "tool.result")
        state = _record(records, "state")
        outcome = _record(records, "run.outcome")

        assert proposal["data"] == {
            "call_id": proposal["data"]["call_id"],
            "native_call_id": model_call_id,
            "name": "read_file",
            "input": tool_input,
        }
        assert len([record for record in records if record["kind"] == "tool.proposal"]) == 1
        assert call["data"] == {
            "call_id": proposal["data"]["call_id"],
            "native_call_id": model_call_id,
            "name": "read_file",
            "input": tool_input,
        }
        assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
        assert result["data"] == {
            "call_id": call["data"]["call_id"],
            "native_call_id": model_call_id,
            "status": "succeeded",
            "output": "CURRENT = True",
        }
        assert result["links"] == [{"type": "result_of", "record": call["id"]}]
        assert len([record for record in records if record["kind"] == "tool.call"]) == 1
        assert len([record for record in records if record["kind"] == "tool.result"]) == 1
        causal_kinds = [
            record["kind"]
            for record in records
            if record["kind"]
            in {
                "tool.proposal",
                "tool.call",
                "tool.result",
                "model.request",
                "model.response",
                "run.outcome",
            }
        ]
        assert causal_kinds == [
            "model.request",
            "model.response",
            "tool.proposal",
            "model.request",
            "model.response",
            "model.request",
            "model.response",
            "model.request",
            "model.response",
            "tool.call",
            "tool.result",
            "run.outcome",
        ]
        assert state["data"] == {
            "type": "openai_agents.final_output",
            "value": {"answer": "done"},
        }
        assert outcome["data"]["status"] == "completed"
        assert outcome["links"] == [{"type": "derived_from", "record": state["id"]}]
        losses = [record for record in records if record["kind"] == "loss"]
        if reuse_call_id:
            assert [
                (loss["data"]["reason"], loss["data"]["count"]) for loss in losses
            ] == [
                ("tool_call_id_reused", 2),
                ("tool_chronology_not_captured", 1),
            ]
        else:
            assert [
                (loss["data"]["reason"], loss["data"]["count"]) for loss in losses
            ] == [("tool_chronology_not_captured", 1)]
    finally:
        agents.set_tracing_disabled(True)


@pytest.mark.asyncio
async def test_openai_agents_null_generation_before_runner_error_is_not_completed_response(
    tmp_path: Path,
) -> None:
    class Runner:
        subject: Any
        error: BaseException

        @classmethod
        async def run(cls) -> None:
            with agents.trace("provider-error-trace") as native_trace:
                cls.subject.processor.on_trace_start(native_trace)
                with agents.generation_span(
                    input=[{"role": "user", "content": "Reach the provider."}],
                    output=None,
                    model="fixture-model",
                    parent=native_trace,
                ) as generation:
                    cls.subject.processor.on_span_start(generation)
                cls.subject.processor.on_span_end(generation)
                cls.subject.processor.on_trace_end(native_trace)
            raise cls.error

    class Subject:
        Runner: Any

        def __init__(self) -> None:
            self.processor: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            assert self.processor is processor
            self.processor = None

        def get_current_trace(self) -> Any:
            return agents.get_current_trace()

    subject = Subject()
    subject.Runner = Runner
    Runner.subject = subject
    marker = RuntimeError("provider connection failed")
    Runner.error = marker
    capture = initialize(output=tmp_path, service_name="openai-agents-provider-error")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=subject,
    )

    agents.set_tracing_disabled(False)
    try:
        with pytest.raises(RuntimeError) as raised:
            await Runner.run()
        assert raised.value is marker
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        assert _record(records, "model.request")["data"]["model"] == "fixture-model"
        assert not [record for record in records if record["kind"] == "model.response"]
        assert any(
            record["kind"] == "error"
            and record["data"]["type"] == "openai_agents.runner_error"
            for record in records
        )
        assert _record(records, "run.outcome")["data"]["status"] == "failed"
    finally:
        agents.set_tracing_disabled(True)


@pytest.mark.asyncio
async def test_openai_agents_nested_runs_do_not_own_outer_trace_outcome(
    tmp_path: Path,
) -> None:
    class Result:
        def __init__(self, answer: str) -> None:
            self.final_output = {"answer": answer}
            self.new_items: list[Any] = []

    class Runner:
        subject: Any

        @classmethod
        async def run(
            cls,
            native_trace: Any,
            result: Result,
            mode: str,
            *,
            hooks: Any = None,
        ) -> Result:
            def record_task(*, failed: bool) -> None:
                with agents.task_span(mode, parent=native_trace) as task:
                    cls.subject.processor.on_span_start(task)
                    if failed:
                        task.set_error(
                            agents.SpanError(
                                message="caught inner failure",
                                data={"mode": mode},
                            )
                        )
                cls.subject.processor.on_span_end(task)

            if mode == "inner":
                record_task(failed=False)
                return result
            if mode == "inner-failure":
                record_task(failed=True)
                raise RuntimeError("caught inner failure")

            cls.subject.current_trace = native_trace
            cls.subject.processor.on_trace_start(native_trace)
            try:
                with agents.task_span(mode, parent=native_trace) as task:
                    cls.subject.processor.on_span_start(task)
                    await cls.run(native_trace, Result("inner"), "inner")
                    try:
                        await cls.run(
                            native_trace,
                            Result("ignored"),
                            "inner-failure",
                        )
                    except RuntimeError:
                        pass
                cls.subject.processor.on_span_end(task)
                return result
            finally:
                cls.subject.processor.on_trace_end(native_trace)
                cls.subject.current_trace = None

    class Subject:
        Runner: Any

        def __init__(self) -> None:
            self.processor: Any = None
            self.current_trace: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            assert self.processor is processor
            self.processor = None

        def get_current_trace(self) -> Any:
            return self.current_trace

    subject = Subject()
    subject.Runner = Runner
    Runner.subject = subject
    capture = initialize(output=tmp_path, service_name="openai-agents-nested-runner")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=subject,
    )
    agents.set_tracing_disabled(False)
    try:
        with agents.trace("nested-runner") as native_trace:
            result = Result("outer")
            assert await Runner.run(native_trace, result, "outer") is result
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        assert [record["data"] for record in records if record["kind"] == "state"] == [
            {
                "type": "openai_agents.final_output",
                "value": {"answer": "outer"},
            }
        ]
        assert any(
            record["kind"] == "error"
            and record["data"]["message"] == "caught inner failure"
            for record in records
        )
        assert _record(records, "run.outcome")["data"]["status"] == "completed"
    finally:
        agents.set_tracing_disabled(True)


@pytest.mark.asyncio
async def test_openai_agents_nested_stream_keeps_tools_but_not_inner_output(
    tmp_path: Path,
) -> None:
    class Result:
        def __init__(self, final_output: Any, new_items: list[Any]) -> None:
            self.final_output = final_output
            self.new_items = new_items

    class StreamResult(Result):
        def __init__(self, trace: Any, final_output: Any, new_items: list[Any]) -> None:
            super().__init__(final_output, new_items)
            self.trace = trace
            self.run_loop_task = None
            self.run_loop_exception = None

        async def stream_events(self) -> Any:
            if False:
                yield None

    class Runner:
        subject: Any

        @classmethod
        async def run(
            cls,
            native_trace: Any,
            outer_result: Result,
            inner_result: StreamResult,
        ) -> Result:
            cls.subject.current_trace = native_trace
            cls.subject.processor.on_trace_start(native_trace)
            try:
                with agents.generation_span(
                    input=[{"role": "user", "content": "Inspect the file."}],
                    output=[
                        {
                            "type": "function_call",
                            "call_id": "call_nested_stream",
                            "name": "read_file",
                            "arguments": '{"path":"src/nested.py"}',
                        }
                    ],
                    model="fixture-model",
                    parent=native_trace,
                ) as generation:
                    cls.subject.processor.on_span_start(generation)
                cls.subject.processor.on_span_end(generation)
                nested = cls.run_streamed(inner_result)
                assert [event async for event in nested.stream_events()] == []
                return outer_result
            finally:
                cls.subject.processor.on_trace_end(native_trace)
                cls.subject.current_trace = None

        @classmethod
        def run_streamed(cls, result: StreamResult) -> StreamResult:
            return result

    class Subject:
        Runner: Any

        def __init__(self) -> None:
            self.processor: Any = None
            self.current_trace: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            assert self.processor is processor
            self.processor = None

        def get_current_trace(self) -> Any:
            return self.current_trace

    subject = Subject()
    subject.Runner = Runner
    Runner.subject = subject
    capture = initialize(output=tmp_path, service_name="openai-agents-nested-stream")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=subject,
    )
    source_agent = agents.Agent(name="fixture-agent")
    call_item = agents.ToolCallItem(
        agent=source_agent,
        raw_item={
            "type": "function_call",
            "call_id": "call_nested_stream",
            "name": "read_file",
            "arguments": '{"path":"src/nested.py"}',
        },
    )
    output_item = agents.ToolCallOutputItem(
        agent=source_agent,
        raw_item={
            "type": "function_call_output",
            "call_id": "call_nested_stream",
            "output": "NESTED = True",
        },
        output="NESTED = True",
    )

    agents.set_tracing_disabled(False)
    try:
        with agents.trace("nested-stream") as native_trace:
            outer_result = Result({"answer": "outer"}, [])
            inner_result = StreamResult(
                native_trace,
                {"answer": "inner"},
                [call_item, output_item],
            )
            assert (
                await Runner.run(native_trace, outer_result, inner_result)
                is outer_result
            )
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        assert len([record for record in records if record["kind"] == "tool.call"]) == 1
        assert len([record for record in records if record["kind"] == "tool.result"]) == 1
        assert [record["data"] for record in records if record["kind"] == "state"] == [
            {
                "type": "openai_agents.final_output",
                "value": {"answer": "outer"},
            }
        ]
        assert _record(records, "run.outcome")["data"]["status"] == "completed"
    finally:
        agents.set_tracing_disabled(True)


@pytest.mark.asyncio
async def test_openai_agents_ignores_unrelated_function_spans_and_bounds_missing_input_loss(
    tmp_path: Path,
) -> None:
    class Runner:
        subject: Any

        @classmethod
        async def run(cls, result: Any) -> Any:
            cls.subject.processor.on_trace_end(agents.get_current_trace())
            return result

    class Subject:
        Runner: Any

        def __init__(self) -> None:
            self.processor: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            assert self.processor is processor
            self.processor = None

        def get_current_trace(self) -> Any:
            return agents.get_current_trace()

    subject = Subject()
    subject.Runner = Runner
    Runner.subject = subject
    capture = initialize(output=tmp_path, service_name="openai-agents-tool-loss")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=subject,
    )
    source_agent = agents.Agent(name="fixture-agent")
    missing_input_item = agents.ToolCallItem(
        agent=source_agent,
        raw_item={
            "type": "function_call",
            "call_id": "call_missing_arguments",
            "name": "read_file",
        },
    )
    run_result = agents.RunResult(
        input="Inspect files.",
        new_items=[missing_input_item],
        raw_responses=[],
        final_output="done",
        input_guardrail_results=[],
        output_guardrail_results=[],
        tool_input_guardrail_results=[],
        tool_output_guardrail_results=[],
        context_wrapper=agents.RunContextWrapper(context=None),
        _last_agent=source_agent,
    )

    agents.set_tracing_disabled(False)
    try:
        with agents.trace("tool-loss-trace") as native_trace:
            subject.processor.on_trace_start(native_trace)
            with agents.generation_span(
                input=[{"role": "user", "content": "Inspect files."}],
                output=[
                    ChatCompletionMessage(
                        role="assistant",
                        content=None,
                        tool_calls=[
                            ChatCompletionMessageFunctionToolCall.model_construct(
                                id=None,
                                type="function",
                                function=Function(
                                    name="read_file",
                                    arguments='{"path":"src/missing-id.py"}',
                                ),
                            )
                        ],
                    )
                ],
                model="fixture-model",
                parent=native_trace,
            ) as generation:
                subject.processor.on_span_start(generation)
            subject.processor.on_span_end(generation)
            for index in range(2):
                with agents.function_span(
                    "read_file",
                    input=f'{{"index":{index}}}',
                    output=f"result-{index}",
                    parent=native_trace,
                ) as function:
                    subject.processor.on_span_start(function)
                subject.processor.on_span_end(function)
            assert await Runner.run(run_result) is run_result
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        assert not [
            record
            for record in records
            if record["kind"] in {"tool.proposal", "tool.call", "tool.result"}
        ]
        losses = [record["data"] for record in records if record["kind"] == "loss"]
        assert losses == [
            {
                "reason": "tool_correlation_not_captured",
                "stage": "source",
                "count": 1,
                "recoverable": False,
                "detail": (
                    "Tool evidence could not be correlated through authoritative "
                    "generation and RunResult call identifiers."
                ),
            },
            {
                "reason": "tool_input_not_captured",
                "stage": "source",
                "count": 1,
                "recoverable": False,
                "detail": ("RunResult tool execution items omitted authoritative tool input."),
            },
        ]
    finally:
        agents.set_tracing_disabled(True)


def test_openai_agents_0_18_2_projects_task_agent_and_turn_without_noise(
    tmp_path: Path,
) -> None:
    assert distribution_version("openai-agents") == "0.18.2"
    capture = initialize(output=tmp_path, service_name="openai-agents-scope-fixture")
    capture.instrument(
        adapter=openai_agents_adapter(version="0.18.2"),
        client=agents,
    )
    agents.set_tracing_disabled(False)

    try:
        with agents.trace("runner-task") as trace:
            with agents.task_span("runner-task", parent=trace) as task:
                with agents.agent_span("fixture-agent", parent=task) as agent:
                    with agents.turn_span(1, "fixture-agent", parent=agent) as turn:
                        with agents.generation_span(
                            input=[{"role": "user", "content": "Finish the task."}],
                            output=[{"role": "assistant", "content": "done"}],
                            model="fixture-model",
                            usage={"input_tokens": 3, "output_tokens": 1},
                            parent=turn,
                        ):
                            pass
        agents.flush_traces()

        closed = capture.shutdown()
        assert validate_artifact(closed.artifact_path).valid
        records = _semantic_records(closed.artifact_path)

        assert not [record for record in records if record["kind"] == "loss"]
        scopes = [record for record in records if record["kind"] == "scope"]
        assert [
            (
                record["data"]["type"],
                record["data"]["phase"],
                record["data"].get("status"),
            )
            for record in scopes
        ] == [
            ("agent", "start", None),
            ("turn", "start", None),
            ("turn", "end", "completed"),
            ("agent", "end", "completed"),
        ]
        assert scopes[1]["parent"] == scopes[0]["id"]
        request = _record(records, "model.request")
        response = _record(records, "model.response")
        assert request["parent"] == scopes[1]["id"]
        assert response["links"] == [{"type": "result_of", "record": request["id"]}]
        assert _record(records, "run.outcome")["data"]["status"] == "completed"
    finally:
        agents.set_tracing_disabled(True)


def test_openai_agents_lookalike_bookkeeping_spans_remain_visible_as_loss(
    tmp_path: Path,
) -> None:
    class LookalikeSpanData(agents.tracing.SpanData):
        def __init__(self, span_type: str) -> None:
            self._span_type = span_type

        @property
        def type(self) -> str:
            return self._span_type

        def export(self) -> dict[str, Any]:
            return {"type": self._span_type}

    capture = initialize(output=tmp_path, service_name="openai-agents-lookalike-fixture")
    capture.instrument(
        adapter=openai_agents_adapter(version="0.18.2"),
        client=agents,
    )
    agents.set_tracing_disabled(False)

    try:
        with agents.trace("lookalike-spans") as trace:
            for span_type in ("task", "agent", "turn"):
                span = agents.tracing.get_trace_provider().create_span(
                    LookalikeSpanData(span_type),
                    parent=trace,
                )
                with span:
                    pass
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        losses = [record for record in records if record["kind"] == "loss"]
        assert len(losses) == 6
        assert all(
            record["data"]["reason"] == "unsupported_semantic_projection" for record in losses
        )
        assert _record(records, "run.outcome")["data"]["status"] == "unknown"
    finally:
        agents.set_tracing_disabled(True)


def test_openai_agents_task_error_remains_visible_and_fails_run(
    tmp_path: Path,
) -> None:
    capture = initialize(output=tmp_path, service_name="openai-agents-task-error")
    capture.instrument(
        adapter=openai_agents_adapter(version="0.18.2"),
        client=agents,
    )
    agents.set_tracing_disabled(False)

    try:
        with agents.trace("failed-runner-task") as trace:
            with agents.task_span("failed-runner-task", parent=trace) as task:
                task.set_error(
                    {
                        "message": "runner failed",
                        "data": {"code": "RUNNER_FAILURE", "retryable": False},
                    }
                )
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        assert not [record for record in records if record["kind"] == "loss"]
        error = _record(records, "error")
        assert error["data"]["message"] == "runner failed"
        assert _record(records, "run.outcome")["data"]["status"] == "failed"
    finally:
        agents.set_tracing_disabled(True)


@pytest.mark.asyncio
async def test_openai_agents_run_task_cancellation_is_the_run_outcome(
    tmp_path: Path,
) -> None:
    class Result:
        def __init__(self, trace: Any, task: asyncio.Task[None]) -> None:
            self.trace = trace
            self.run_loop_task = task
            self.run_loop_exception = None

        async def stream_events(self) -> Any:
            if False:
                yield None

    class Runner:
        result: Result

        @classmethod
        def run_streamed(cls, *args: Any, **kwargs: Any) -> Result:
            return cls.result

    class Subject:
        Runner: Any

        def __init__(self) -> None:
            self.processor: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            assert self.processor is processor
            self.processor = None

    subject = Subject()
    subject.Runner = Runner
    capture = initialize(output=tmp_path, service_name="openai-agents-cancel")
    capture.instrument(
        adapter=openai_agents_adapter(version="0.18.2"),
        client=subject,
    )
    native_trace = SimpleNamespace(
        trace_id="trace-cancel",
        name="cancelled-runner-task",
        group_id=None,
        metadata={},
    )
    subject.processor.on_trace_start(native_trace)
    task = asyncio.create_task(asyncio.sleep(60))
    Runner.result = Result(native_trace, task)
    result = Runner.run_streamed()
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    await asyncio.sleep(0)
    assert [event async for event in result.stream_events()] == []
    await asyncio.sleep(0)
    subject.processor.on_trace_end(native_trace)

    records = _semantic_records(capture.shutdown().artifact_path)
    assert not [record for record in records if record["kind"] == "loss"]
    assert _record(records, "run.outcome")["data"]["status"] == "cancelled"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("termination", "expected_status", "overlapping_response", "late_generation"),
    [
        ("cancel", "cancelled", False, False),
        ("failure", "incomplete", False, False),
        ("cancel", "cancelled", True, False),
        ("cancel", "cancelled", False, True),
    ],
)
async def test_openai_agents_keeps_consumed_reasoning_when_stream_ends_early(
    tmp_path: Path,
    termination: str,
    expected_status: str,
    overlapping_response: bool,
    late_generation: bool,
) -> None:
    from agents.stream_events import RawResponsesStreamEvent

    created = ResponseCreatedEvent.model_construct(
        type="response.created",
        sequence_number=1,
        response=SimpleNamespace(id="resp-partial-reasoning"),
    )
    deltas = [
        ResponseReasoningSummaryTextDeltaEvent.model_construct(
            type="response.reasoning_summary_text.delta",
            sequence_number=index + 2,
            item_id="reasoning-partial-1",
            output_index=0,
            summary_index=0,
            delta=text,
        )
        for index, text in enumerate(("Keep this ", "summary."))
    ]

    class Result:
        def __init__(self, trace: Any) -> None:
            self.trace = trace
            self.run_loop_task = None
            self.run_loop_exception = None

        async def stream_events(self) -> Any:
            yield RawResponsesStreamEvent(data=created)
            if overlapping_response:
                yield RawResponsesStreamEvent(
                    data=ResponseCreatedEvent.model_construct(
                        type="response.created",
                        sequence_number=2,
                        response=SimpleNamespace(id="resp-overlapping-reasoning"),
                    )
                )
            for delta in deltas:
                yield RawResponsesStreamEvent(data=delta)
            if termination == "failure":
                raise RuntimeError("stream failed after reasoning")

    class Runner:
        result: Result

        @classmethod
        def run_streamed(cls, *args: Any, **kwargs: Any) -> Result:
            return cls.result

    class Subject:
        def __init__(self) -> None:
            self.processor: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            self.processor = None

    subject = Subject()
    subject.Runner = Runner
    capture = initialize(output=tmp_path, service_name="openai-agents-partial-reasoning")
    capture.instrument(adapter=openai_agents_adapter(version="0.18.2"), client=subject)
    native_trace = SimpleNamespace(
        trace_id="trace-partial-reasoning",
        name="partial-reasoning",
        group_id=None,
        metadata={},
    )
    subject.processor.on_trace_start(native_trace)
    generation = SimpleNamespace(
        trace_id=native_trace.trace_id,
        span_id="span-late-generation",
        parent_id=None,
        span_data=SimpleNamespace(
            type="generation",
            input=[{"role": "user", "content": "Think briefly."}],
            output=[
                {
                    "id": "reasoning-partial-1",
                    "type": "reasoning",
                    "summary": [],
                    "content": [],
                }
            ],
            model="fixture-model",
            model_config={},
            usage=None,
        ),
        error=None,
        started_at=None,
        ended_at=None,
    )
    if late_generation:
        subject.processor.on_span_start(generation)
    Runner.result = Result(native_trace)
    result = Runner.run_streamed()
    events = result.stream_events()
    assert (await events.__anext__()).data is created
    if overlapping_response:
        overlapping = await events.__anext__()
        assert overlapping.data.response.id == "resp-overlapping-reasoning"
    assert (await events.__anext__()).data is deltas[0]
    assert (await events.__anext__()).data is deltas[1]
    if termination == "cancel":
        await events.aclose()
    else:
        with pytest.raises(RuntimeError, match="stream failed after reasoning"):
            await events.__anext__()
    if late_generation:
        subject.processor.on_span_end(generation)
    subject.processor.on_trace_end(native_trace)

    records = _semantic_records(capture.shutdown().artifact_path)
    if late_generation:
        responses = [record for record in records if record["kind"] == "model.response"]
        assert [response["data"]["reasoning"] for response in responses] == [
            [{"type": "summary", "text": "Keep this summary."}]
        ]
        assert not [
            record
            for record in records
            if record["kind"] == "state"
            and record["data"]["type"] == "openai_agents.stream.reasoning_partial"
        ]
        return
    assert not [record for record in records if record["kind"] == "model.response"]
    if overlapping_response:
        assert not [
            record
            for record in records
            if record["kind"] == "state"
            and record["data"]["type"] == "openai_agents.stream.reasoning_partial"
        ]
        assert _record(records, "loss")["data"]["detail"] == (
            "openai_agents_reasoning_response_correlation_not_captured"
        )
        return
    state = _record(records, "state")
    assert state["data"] == {
        "type": "openai_agents.stream.reasoning_partial",
        "value": {
            "status": expected_status,
            "response_id": "resp-partial-reasoning",
            "reasoning": [{"type": "summary", "text": "Keep this summary."}],
        },
    }
    assert _record(records, "loss")["data"] == {
        "reason": "unsupported_native_value",
        "stage": "source",
        "count": 1,
        "recoverable": False,
        "detail": "openai_agents_reasoning_model_request_correlation_not_captured",
    }


@pytest.mark.asyncio
async def test_openai_agents_stream_retains_events_and_links_final_output(
    tmp_path: Path,
) -> None:
    event = {"type": "fixture.event"}

    class Result:
        def __init__(self, trace: Any) -> None:
            self.trace = trace
            self.run_loop_task = None
            self.run_loop_exception = None
            self.final_output = {"answer": "streamed"}
            self.stream_calls = 0

        async def stream_events(self) -> Any:
            self.stream_calls += 1
            yield event

    class Runner:
        subject: Any
        result: Result

        @classmethod
        def run_streamed(cls, *args: Any, **kwargs: Any) -> Result:
            cls.subject.processor.on_trace_end(agents.get_current_trace())
            return cls.result

    class Subject:
        Runner: Any

        def __init__(self) -> None:
            self.processor: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            assert self.processor is processor
            self.processor = None

        def get_current_trace(self) -> Any:
            return agents.get_current_trace()

    subject = Subject()
    subject.Runner = Runner
    Runner.subject = subject
    capture = initialize(output=tmp_path, service_name="openai-agents-stream-output")
    capture.instrument(
        adapter=openai_agents_adapter(version=distribution_version("openai-agents")),
        client=subject,
    )
    agents.set_tracing_disabled(False)
    try:
        with agents.trace("existing-stream-trace") as native_trace:
            subject.processor.on_trace_start(native_trace)
            expected_result = Result(native_trace)
            Runner.result = expected_result

            result = Runner.run_streamed()
            assert result is expected_result
            yielded = [item async for item in result.stream_events()]
            assert len(yielded) == 1
            assert yielded[0] is event
            assert expected_result.stream_calls == 1
        agents.flush_traces()

        records = _semantic_records(capture.shutdown().artifact_path)
        state = _record(records, "state")
        outcome = _record(records, "run.outcome")
        assert state["data"] == {
            "type": "openai_agents.final_output",
            "value": {"answer": "streamed"},
        }
        assert outcome["links"] == [{"type": "derived_from", "record": state["id"]}]
    finally:
        agents.set_tracing_disabled(True)


@pytest.mark.asyncio
async def test_openai_agents_runner_retains_result_and_links_final_output(
    tmp_path: Path,
) -> None:
    class Result:
        def __init__(self, final_output: Any) -> None:
            self.final_output = final_output

    class Runner:
        subject: Any

        @classmethod
        async def run(
            cls,
            native_trace: Any,
            result: Result,
            error: BaseException | None = None,
        ) -> Result:
            cls.subject.processor.on_trace_start(native_trace)
            try:
                if error is not None:
                    raise error
                return result
            finally:
                cls.subject.processor.on_trace_end(native_trace)

        @classmethod
        def run_sync(cls, native_trace: Any, result: Result) -> Result:
            cls.subject.processor.on_trace_start(native_trace)
            try:
                return result
            finally:
                cls.subject.processor.on_trace_end(native_trace)

    class Subject:
        Runner: Any

        def __init__(self) -> None:
            self.processor: Any = None

        def add_trace_processor(self, processor: Any) -> None:
            self.processor = processor

        def remove_trace_processor(self, processor: Any) -> None:
            assert self.processor is processor
            self.processor = None

    subject = Subject()
    subject.Runner = Runner
    Runner.subject = subject
    capture = initialize(output=tmp_path, service_name="openai-agents-final-output")
    capture.instrument(
        adapter=openai_agents_adapter(version="0.18.2"),
        client=subject,
    )

    async_result = Result({"answer": 42})
    async_trace = SimpleNamespace(
        trace_id="trace-async-result",
        name="async-result",
        group_id=None,
        metadata={},
    )
    assert await Runner.run(async_trace, async_result) is async_result

    sync_result = Result({"answer": 84})
    sync_trace = SimpleNamespace(
        trace_id="trace-sync-result",
        name="sync-result",
        group_id=None,
        metadata={},
    )
    assert Runner.run_sync(sync_trace, sync_result) is sync_result

    expected_error = RuntimeError("exact runner failure")
    error_trace = SimpleNamespace(
        trace_id="trace-async-error",
        name="async-error",
        group_id=None,
        metadata={},
    )
    with pytest.raises(RuntimeError) as raised:
        await Runner.run(error_trace, Result(None), expected_error)
    assert raised.value is expected_error

    records = _semantic_records(capture.shutdown().artifact_path)
    states = [record for record in records if record["kind"] == "state"]
    assert [state["data"] for state in states] == [
        {"type": "openai_agents.final_output", "value": {"answer": 42}},
        {"type": "openai_agents.final_output", "value": {"answer": 84}},
    ]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert [outcome["data"]["status"] for outcome in outcomes] == [
        "completed",
        "completed",
        "failed",
    ]
    assert [outcome["links"] for outcome in outcomes[:2]] == [
        [{"type": "derived_from", "record": states[0]["id"]}],
        [{"type": "derived_from", "record": states[1]["id"]}],
    ]
    errors = [record for record in records if record["kind"] == "error"]
    assert [error["data"] for error in errors] == [
        {
            "type": "openai_agents.runner_error",
            "message": "exact runner failure",
            "recoverable": False,
        }
    ]
    assert outcomes[2]["data"]["error"] == errors[0]["data"]


def test_openai_agents_stream_compaction_keeps_failure_and_unknown_events() -> None:
    from agents.stream_events import RawResponsesStreamEvent

    ordinary_events: list[Any] = [
        ResponseTextDeltaEvent.model_construct(
            type="response.output_text.delta",
            sequence_number=1,
            item_id="item-1",
            output_index=0,
            content_index=0,
            delta="d",
            logprobs=[],
        ),
        ResponseCompletedEvent.model_construct(
            type="response.completed",
            sequence_number=2,
            response=None,
        ),
    ]
    failure_events: list[Any] = [
        ResponseErrorEvent.model_construct(
            type="error",
            sequence_number=3,
            code="provider_error",
            message="failed",
            param=None,
        ),
        ResponseFailedEvent.model_construct(
            type="response.failed",
            sequence_number=4,
            response=None,
        ),
        ResponseIncompleteEvent.model_construct(
            type="response.incomplete",
            sequence_number=5,
            response=None,
        ),
    ]

    for event in ordinary_events:
        assert _is_exact_official_stream_bookkeeping(
            RawResponsesStreamEvent(data=event),
            "0.18.2",
        )
    for event in failure_events:
        wrapped = RawResponsesStreamEvent(data=event)
        assert not _is_exact_official_stream_bookkeeping(
            wrapped,
            "0.18.2",
        )
    assert all(
        _is_official_terminal_response_failure(
            RawResponsesStreamEvent(data=event),
            "0.18.2",
        )
        for event in failure_events
    )
    assert not _is_exact_official_stream_bookkeeping(
        RawResponsesStreamEvent(data=ordinary_events[0]),
        "0.18.3",
    )

    class Sink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []

        def record(self, value: dict[str, Any]) -> None:
            self.records.append(value)

    sink = Sink()
    trace = {"run_id": "run", "trace_id": "trace", "operation_id": "operation"}
    assert not _record_openai_stream_event(
        sink,
        trace,
        RawResponsesStreamEvent(data=failure_events[0]),
        version="0.18.2",
    )
    assert not _record_openai_stream_event(
        sink,
        trace,
        object(),
        version="0.18.2",
    )
    assert len(sink.records) == 2
    assert [record["semantic"]["type"] for record in sink.records] == [
        "error",
        "stream.event",
    ]
    assert sink.records[0]["semantic"]["error"] == {
        "type": "openai_agents_response_stream_error",
        "message": "failed",
        "recoverable": False,
    }


def _semantic_records(path: str) -> list[dict[str, Any]]:
    records = [json.loads(line) for line in (Path(path) / "trace.jsonl").read_text().splitlines()]
    for record in records:
        _TRACE_VALIDATOR.validate(record)
    return records


def _record(records: list[dict[str, Any]], kind: str) -> dict[str, Any]:
    return next(record for record in records if record["kind"] == kind)
