"""Official openai agents adapter capture adapter."""

from __future__ import annotations

from contextvars import ContextVar
from functools import lru_cache
from importlib import import_module
from threading import RLock
from types import UnionType
from typing import Annotated, Any, Union, get_args, get_origin

from ._adapter_native import native_field, native_snapshot
from ._framework_adapter_shared import (
    _explicit_turn_identity,
    _installed_version,
    _Lifecycle,
    _OpenSpan,
    _OpenTrace,
    _owned_field,
    _raw_native,
    _record_unavailable_reasoning_gap,
    _source_qualification,
)
from .capture_v1 import CaptureSource, _trust_official_source

_AGENT_RUN_COVERAGE = {"operation": "agent-run", "domain": "openai-agents.trace"}


_OPENAI_RESPONSE_COVERAGE = {
    "operation": "model-response",
    "domain": "openai.response",
}

class _ObservedAsyncIterator:
    """Protocol-transparent observation of an application-consumed async iterator."""

    def __init__(
        self,
        iterator: Any,
        on_event: Any,
        on_error: Any,
        on_complete: Any = None,
        on_cancel: Any = None,
        on_settle: Any = None,
    ) -> None:
        self._iterator = iterator
        self._on_event = on_event
        self._on_error = on_error
        self._on_complete = on_complete
        self._on_cancel = on_cancel
        self._on_settle = on_settle
        self._completed = False
        self._closed = False
        self._settled = False

    def __aiter__(self) -> _ObservedAsyncIterator:
        return self

    async def __anext__(self) -> Any:
        return await self._forward(self._iterator.__anext__)

    async def asend(self, value: Any) -> Any:
        return await self._forward(self._iterator.asend, value)

    async def athrow(self, *args: Any) -> Any:
        return await self._forward(self._iterator.athrow, *args)

    async def aclose(self) -> Any:
        result = await self._iterator.aclose()
        if not self._closed and not self._completed:
            self._closed = True
            try:
                if self._on_cancel is not None:
                    self._on_cancel()
            finally:
                self._settle()
        return result

    async def _forward(self, operation: Any, *args: Any) -> Any:
        if self._closed:
            raise StopAsyncIteration
        try:
            event = await operation(*args)
        except StopAsyncIteration:
            if not self._completed and self._on_complete is not None:
                self._completed = True
                try:
                    self._on_complete()
                finally:
                    self._settle()
            raise
        except GeneratorExit:
            raise
        except BaseException as error:
            try:
                self._on_error(error)
            finally:
                self._settle()
            raise
        self._on_event(event)
        return event

    def _settle(self) -> None:
        if self._settled:
            return
        self._settled = True
        if self._on_settle is not None:
            self._on_settle()


def _is_exact_official_agents_registration(subject: object, register: Any) -> bool:
    """Recognize the one no-remover registration API whose mutation is atomic."""

    try:
        official_agents = import_module("agents")
    except ImportError:
        return False
    # The official API currently exposes no remover. The atomic-registration assumption
    # is deliberately limited to this exact module/function identity, and registration
    # remains the final install mutation. Every custom or forged subject must instead
    # provide a remover so retain-then-raise can roll back.
    return subject is official_agents and register is official_agents.add_trace_processor


class _AgentsAdapter:
    def __init__(self, version: str) -> None:
        self.version = version

    def create_source(self, client: object) -> _AgentsSource:
        return _trust_official_source(_AgentsSource(client, self.version), "deep")  # type: ignore[return-value]


class _AgentsSource(CaptureSource):
    def __init__(self, subject: object, version: str) -> None:
        self.subject = subject
        self.version = version
        self.metadata = {
            "name": "official:openai-agents-python",
            "seam": (
                "add_trace_processor/TracingProcessor + "
                "Runner.run_streamed/stream_events proxy"
            ),
            "identity_domain": "openai-agents.trace",
            "version": version,
            "qualification": _source_qualification(
                version,
                exact_versions=frozenset({"0.18.2"}),
                profile="openai-agents-python-adapter-v1",
            ),
            "official": True,
            "coverage": [
                {**_AGENT_RUN_COVERAGE, "role": "owner"},
                {**_OPENAI_RESPONSE_COVERAGE, "role": "owner"},
            ],
        }

    def install(self, sink: Any) -> _Lifecycle:
        register = getattr(self.subject, "add_trace_processor", None)
        if not callable(register):
            raise TypeError("OpenAI Agents subject must expose add_trace_processor")
        unregister = getattr(self.subject, "remove_trace_processor", None)
        traces: dict[str, _OpenTrace] = {}
        spans: dict[str, _OpenSpan] = {}
        trace_outcomes: dict[str, str] = {}
        trace_errors: dict[str, dict[str, Any]] = {}
        trace_output_refs: dict[str, str] = {}
        trace_tool_proposals: dict[str, set[str]] = {}
        trace_tool_executions: dict[str, set[str]] = {}
        trace_tool_results: dict[str, set[str]] = {}
        trace_tool_chronology_lost: set[str] = set()
        trace_tool_gaps: dict[str, dict[str, int]] = {}
        pending_stream_holds: dict[str, int] = {}
        deferred_trace_ends: dict[str, dict[str, Any]] = {}
        runner_invocation_traces: dict[object, str] = {}
        trace_owner_invocations: dict[str, object] = {}
        runner_invocation: ContextVar[object | None] = ContextVar(
            "semantic_layer_openai_agents_runner_invocation",
            default=None,
        )
        consumer_holds: dict[object, tuple[str, dict[str, str] | None, bool]] = {}
        task_callbacks: dict[object, tuple[Any, Any]] = {}
        stream_reasoning: dict[str, dict[str, dict[str, Any]]] = {}
        active_stream_responses: dict[str, set[str]] = {}
        recorded_partial_responses: set[tuple[str, str]] = set()
        reasoning_correlation_gaps: set[str] = set()
        open_generation_spans: dict[str, int] = {}
        pending_partial_status: dict[str, str] = {}
        lock = RLock()
        active = True
        adapter_version = self.version

        def accumulate_stream_reasoning(trace_id: str, event: Any) -> None:
            data = getattr(event, "data", None)
            event_type = str(getattr(data, "type", ""))
            if event_type == "response.created":
                response_id = getattr(getattr(data, "response", None), "id", None)
                if isinstance(response_id, str) and response_id:
                    active_stream_responses.setdefault(trace_id, set()).add(response_id)
                    stream_reasoning.setdefault(trace_id, {}).setdefault(
                        response_id,
                        {"blocks": {}, "native_events": []},
                    )
                return
            if event_type in {
                "response.completed",
                "response.failed",
                "response.incomplete",
            }:
                response_id = getattr(getattr(data, "response", None), "id", None)
                if isinstance(response_id, str):
                    active_stream_responses.get(trace_id, set()).discard(response_id)
                return
            kind = (
                "text"
                if event_type in {
                    "response.reasoning_text.delta",
                    "response.reasoning_text.done",
                }
                else "summary"
                if event_type in {
                    "response.reasoning_summary_text.delta",
                    "response.reasoning_summary_text.done",
                }
                else None
            )
            if kind is None:
                return
            active_responses = active_stream_responses.get(trace_id, set())
            response_id = next(iter(active_responses)) if len(active_responses) == 1 else None
            item_id = getattr(data, "item_id", None)
            index_name = "content_index" if kind == "text" else "summary_index"
            part_index = getattr(data, index_name, None)
            if (
                not isinstance(response_id, str)
                or not isinstance(item_id, str)
                or not item_id
                or type(part_index) is not int
                or part_index < 0
            ):
                if trace_id not in reasoning_correlation_gaps:
                    opened = traces.get(trace_id)
                    if opened is not None:
                        reasoning_correlation_gaps.add(trace_id)
                        sink.record(
                            {
                                "kind": "unknown",
                                "phase": "gap",
                                "name": "openai_agents.stream.reasoning_uncorrelated",
                                "trace": opened.identity,
                                "native": {"event": _raw_native(event)},
                                "semantic": {
                                    "type": "capture.gap",
                                    "framework": "openai-agents",
                                    "reason": "unsupported_native_value",
                                    "detail": (
                                        "openai_agents_reasoning_response_"
                                        "correlation_not_captured"
                                    ),
                                },
                            }
                        )
                return
            accumulated = stream_reasoning.get(trace_id, {}).get(response_id)
            if accumulated is None:
                return
            key = (item_id, kind, part_index)
            blocks = accumulated["blocks"]
            existing = blocks.get(key)
            delta = getattr(data, "delta", None)
            text = getattr(data, "text", None)
            if isinstance(delta, str) and delta:
                blocks[key] = {
                    "type": kind,
                    "text": f"{existing['text'] if existing else ''}{delta}",
                    "delta_observed": True,
                }
            elif isinstance(text, str) and text and not (
                existing and existing["delta_observed"]
            ):
                blocks[key] = {
                    "type": kind,
                    "text": text,
                    "delta_observed": False,
                }
            accumulated["native_events"].append(_raw_native(event))

        def exact_stream_reasoning(trace_id: str, output: Any) -> tuple[str, dict[str, Any]] | None:
            if not isinstance(output, (list, tuple)):
                return None
            item_ids = {
                item.get("id") if isinstance(item, dict) else _owned_field(item, "id")
                for item in output
                if (
                    item.get("type") if isinstance(item, dict)
                    else _owned_field(item, "type")
                ) == "reasoning"
            }
            matches = [
                value
                for response_id, value in stream_reasoning.get(trace_id, {}).items()
                if any(key[0] in item_ids for key in value["blocks"])
            ]
            if len(matches) != 1:
                return None
            value = matches[0]
            response_id = next(
                key for key, candidate in stream_reasoning[trace_id].items() if candidate is value
            )
            return response_id, value

        def record_partial_reasoning(trace_id: str, status: str) -> None:
            pending_partial_status[trace_id] = status
            active_responses = active_stream_responses.get(trace_id, set())
            response_id = next(iter(active_responses)) if len(active_responses) == 1 else None
            if not response_id or (trace_id, response_id) in recorded_partial_responses:
                return
            if open_generation_spans.get(trace_id, 0) > 0:
                return
            accumulated = stream_reasoning.get(trace_id, {}).get(response_id)
            if not accumulated or not accumulated["blocks"]:
                return
            opened = traces.get(trace_id)
            if opened is None:
                return
            recorded_partial_responses.add((trace_id, response_id))
            pending_partial_status.pop(trace_id, None)
            reasoning = [
                {"type": block["type"], "text": block["text"]}
                for block in accumulated["blocks"].values()
            ]
            state = sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "openai_agents.stream.reasoning.partial",
                    "trace": opened.identity,
                    "native_identity": response_id,
                    "native": {
                        "response_id": response_id,
                        "consumed_reasoning_events": accumulated["native_events"],
                    },
                    "semantic": {
                        "type": "state.transition",
                        "framework": "openai-agents",
                        "state_type": "openai_agents.stream.reasoning_partial",
                        "value": {
                            "status": status,
                            "response_id": response_id,
                            "reasoning": reasoning,
                        },
                    },
                }
            )
            loss: dict[str, Any] = {
                "kind": "unknown",
                "phase": "gap",
                "name": "openai_agents.stream.reasoning_request_unresolved",
                "trace": opened.identity,
                "native": {"response_id": response_id, "status": status},
                "semantic": {
                    "type": "capture.gap",
                    "framework": "openai-agents",
                    "reason": "unsupported_native_value",
                    "detail": (
                        "openai_agents_reasoning_model_request_"
                        "correlation_not_captured"
                    ),
                },
            }
            _parent_from_receipt(loss, state)
            sink.record(loss)

        def set_trace_outcome(trace_id: str, status: str) -> None:
            if not trace_id:
                return
            with lock:
                current = trace_outcomes.get(trace_id)
                if status == "failed" or current is None:
                    trace_outcomes[trace_id] = status
                elif status == "cancelled" and current == "completed":
                    trace_outcomes[trace_id] = status

        def open_trace(trace: Any) -> _OpenTrace | None:
            name = getattr(trace, "name", None) or "openai_agents.trace"
            trace_id = str(getattr(trace, "trace_id"))
            metadata = getattr(trace, "metadata", None)
            opened = sink.open_trace(
                {
                    "name": name,
                    "coverage": _AGENT_RUN_COVERAGE,
                    "native_identity": trace_id,
                    "conversation_id": getattr(trace, "group_id", None),
                    **_explicit_turn_identity(metadata),
                    "native": _trace_snapshot(trace),
                    "semantic": {
                        "type": "agent.run",
                        "framework": "openai-agents",
                        "name": name,
                    },
                }
            )
            if not opened.accepted or opened.identity is None:
                return None
            result = _OpenTrace(opened.identity, name)
            traces[trace_id] = result
            sink.record(
                {
                    "kind": "correlation",
                    "phase": "event",
                    "name": "openai_agents.trace.native",
                    "trace": result.identity,
                    "native_identity": trace_id,
                    "native": _trace_snapshot(trace),
                    "semantic": {
                        "type": "capture.redundant",
                        "framework": "openai-agents",
                    },
                }
            )
            if isinstance(metadata, dict) and metadata.get("recovery_of"):
                sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": "openai_agents.recovery",
                        "trace": result.identity,
                        "native_identity": trace_id,
                        "native": {
                            "recovery_of": metadata["recovery_of"],
                            "trace": _trace_snapshot(trace),
                        },
                        "semantic": {
                            "type": "recovery.retry",
                            "framework": "openai-agents",
                        },
                    }
                )
            return result

        def add_tool_gap(trace_id: str, reason: str, count: int = 1) -> None:
            if not trace_id or count <= 0:
                return
            gaps = trace_tool_gaps.setdefault(trace_id, {})
            gaps[reason] = min(gaps.get(reason, 0) + count, 1_000_000)

        def close_trace(trace_id: str, native: dict[str, Any]) -> None:
            response_ids = set(stream_reasoning.pop(trace_id, {}))
            active_stream_responses.pop(trace_id, None)
            reasoning_correlation_gaps.discard(trace_id)
            open_generation_spans.pop(trace_id, None)
            pending_partial_status.pop(trace_id, None)
            recorded_partial_responses.difference_update(
                (trace_id, response_id) for response_id in response_ids
            )
            opened = traces.pop(trace_id, None)
            output_ref = trace_output_refs.pop(trace_id, None)
            trace_owner_invocations.pop(trace_id, None)
            trace_tool_chronology_lost.discard(trace_id)
            trace_tool_executions.pop(trace_id, None)
            if opened is not None:
                unmatched_proposals = trace_tool_proposals.pop(trace_id, set())
                trace_tool_results.pop(trace_id, None)
                if unmatched_proposals:
                    add_tool_gap(
                        trace_id,
                        "tool_execution_not_captured",
                        len(unmatched_proposals),
                    )
                gap_details = {
                    "tool_correlation_not_captured": (
                        "Tool evidence could not be correlated through authoritative "
                        "generation and RunResult call identifiers."
                    ),
                    "tool_execution_not_captured": (
                        "Generation output proposed tool calls, but no authoritative "
                        "RunResult execution items were captured."
                    ),
                    "tool_input_not_captured": (
                        "RunResult tool execution items omitted authoritative tool input."
                    ),
                    "tool_chronology_not_captured": (
                        "A later model turn was observed before the framework exposed "
                        "authoritative tool execution evidence."
                    ),
                    "tool_call_id_reused": (
                        "The framework reused a tool call identifier within one run, "
                        "so repeated execution evidence was omitted."
                    ),
                }
                for reason, count in sorted(trace_tool_gaps.pop(trace_id, {}).items()):
                    sink.record(
                        {
                            "kind": "unknown",
                            "phase": "gap",
                            "name": f"openai_agents.{reason}.gap",
                            "trace": opened.identity,
                            "native_identity": f"{trace_id}:{reason}",
                            "native": {
                                "reason": reason,
                                "count": count,
                            },
                            "semantic": {
                                "type": "capture.gap",
                                "reason": reason,
                                "count": count,
                                "detail": gap_details[reason],
                            },
                        }
                    )
                status = trace_outcomes.pop(trace_id, "unknown")
                semantic: dict[str, Any] = {
                    "type": "agent.run",
                    "framework": "openai-agents",
                    "status": status,
                }
                error = trace_errors.pop(trace_id, None)
                if error is not None:
                    semantic["error"] = error
                if output_ref is not None:
                    semantic["output_ref"] = output_ref
                sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": "end",
                        "name": opened.name,
                        "trace": opened.identity,
                        "native_identity": trace_id,
                        "native": native,
                        "semantic": semantic,
                    }
                )

        def acquire_stream_hold(trace_id: str) -> None:
            if trace_id:
                pending_stream_holds[trace_id] = pending_stream_holds.get(trace_id, 0) + 1

        def release_stream_hold(trace_id: str) -> None:
            if not trace_id:
                return
            with lock:
                pending = pending_stream_holds.get(trace_id, 0)
                if pending <= 1:
                    pending_stream_holds.pop(trace_id, None)
                    deferred = deferred_trace_ends.pop(trace_id, None)
                    if deferred is not None and active:
                        close_trace(trace_id, deferred)
                else:
                    pending_stream_holds[trace_id] = pending - 1

        def record_final_output(trace_id: str, result: Any) -> None:
            if not trace_id or trace_id in trace_output_refs:
                return
            opened = traces.get(trace_id)
            if opened is None:
                return
            missing = object()
            output = _owned_field(result, "final_output", missing)
            if output is missing:
                return
            receipt = sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "openai_agents.final_output",
                    "trace": opened.identity,
                    "native_identity": f"{trace_id}:final_output",
                    "native": {"final_output": _raw_native(output)},
                    "semantic": {
                        "type": "state.transition",
                        "framework": "openai-agents",
                        "state_type": "openai_agents.final_output",
                        "value": _raw_native(output),
                    },
                }
            )
            if receipt.accepted and receipt.record_id is not None:
                trace_output_refs[trace_id] = receipt.record_id

        def record_run_result(
            trace_id: str,
            result: Any,
            *,
            include_final_output: bool = True,
        ) -> None:
            opened = traces.get(trace_id)
            if opened is None:
                return
            executions = trace_tool_executions.setdefault(trace_id, set())
            results = trace_tool_results.setdefault(trace_id, set())
            proposals = trace_tool_proposals.setdefault(trace_id, set())
            seen_calls: set[str] = set()
            seen_results: set[str] = set()
            for item in _official_run_tool_items(result, adapter_version):
                if item["kind"] == "gap":
                    add_tool_gap(trace_id, item["reason"])
                    continue
                call_id = item.get("call_id")
                if not isinstance(call_id, str) or not call_id:
                    continue
                if item["kind"] == "call":
                    if call_id in seen_calls:
                        add_tool_gap(trace_id, "tool_call_id_reused")
                        continue
                    seen_calls.add(call_id)
                    if call_id in executions:
                        continue
                    executions.add(call_id)
                    if call_id not in proposals:
                        add_tool_gap(trace_id, "tool_correlation_not_captured")
                    proposals.discard(call_id)
                    sink.record(
                        {
                            "kind": "tool",
                            "phase": "event",
                            "name": "openai_agents.tool.execution",
                            "trace": opened.identity,
                            "native_identity": call_id,
                            "native": {"item": item["native"]},
                            "semantic": {
                                "type": "tool.execution",
                                "framework": "openai-agents",
                                "name": item["name"],
                                "input": item["input"],
                                "native_call_id": call_id,
                            },
                        }
                    )
                elif item["kind"] == "result":
                    if call_id in seen_results:
                        add_tool_gap(trace_id, "tool_call_id_reused")
                        continue
                    seen_results.add(call_id)
                    if call_id in results:
                        continue
                    if call_id not in executions:
                        add_tool_gap(trace_id, "tool_correlation_not_captured")
                        continue
                    results.add(call_id)
                    sink.record(
                        {
                            "kind": "tool",
                            "phase": "event",
                            "name": "openai_agents.tool.result",
                            "trace": opened.identity,
                            "native_identity": call_id,
                            "native": {"item": item["native"]},
                            "semantic": {
                                "type": "tool.result",
                                "framework": "openai-agents",
                                "native_call_id": call_id,
                                "status": "completed",
                                "output": item["output"],
                            },
                        }
                    )
            if include_final_output:
                record_final_output(trace_id, result)

        def record_runner_error(trace_id: str, error: BaseException) -> None:
            if not trace_id:
                return
            opened = traces.get(trace_id)
            if opened is None:
                return
            semantic_error = _runner_semantic_error(error)
            trace_errors[trace_id] = semantic_error
            set_trace_outcome(trace_id, "failed")
            sink.record(
                {
                    "kind": "error",
                    "phase": "event",
                    "name": "openai_agents.runner.error",
                    "trace": opened.identity,
                    "native_identity": f"{trace_id}:runner_error",
                    "error_identity": error,
                    "native": _openai_stream_error_native(error),
                    "semantic": {
                        "type": "error",
                        "framework": "openai-agents",
                        "error": semantic_error,
                    },
                }
            )

        def begin_runner_invocation(marker: object) -> None:
            get_current_trace = getattr(self.subject, "get_current_trace", None)
            if not callable(get_current_trace):
                return
            try:
                current = get_current_trace()
            except BaseException:
                return
            trace_id = str(getattr(current, "trace_id", ""))
            with lock:
                if trace_id in traces and marker not in runner_invocation_traces:
                    runner_invocation_traces[marker] = trace_id
                    trace_owner_invocations.setdefault(trace_id, marker)
                    acquire_stream_hold(trace_id)

        def settle_runner_invocation(
            marker: object,
            *,
            result: Any = None,
            completed: bool,
            error: BaseException | None = None,
        ) -> None:
            with lock:
                trace_id = runner_invocation_traces.pop(marker, "")
                owns_trace = trace_owner_invocations.get(trace_id) is marker
                if error is not None and owns_trace:
                    record_runner_error(trace_id, error)
                elif completed and trace_id:
                    record_run_result(
                        trace_id,
                        result,
                        include_final_output=owns_trace,
                    )
                    if owns_trace:
                        trace_outcomes[trace_id] = "completed"
            release_stream_hold(trace_id)

        class Processor:
            def on_trace_start(self, trace: Any) -> None:
                if not active:
                    return
                with lock:
                    opened = open_trace(trace)
                    marker = runner_invocation.get()
                    if (
                        opened is not None
                        and marker is not None
                        and marker not in runner_invocation_traces
                    ):
                        trace_id = str(getattr(trace, "trace_id"))
                        runner_invocation_traces[marker] = trace_id
                        trace_owner_invocations.setdefault(trace_id, marker)
                        acquire_stream_hold(trace_id)

            def on_trace_end(self, trace: Any) -> None:
                if not active:
                    return
                with lock:
                    trace_id = str(getattr(trace, "trace_id"))
                    native = _trace_snapshot(trace)
                    if pending_stream_holds.get(trace_id, 0) > 0:
                        deferred_trace_ends[trace_id] = native
                    else:
                        close_trace(trace_id, native)

            def on_span_start(self, span: Any) -> None:
                if not active:
                    return
                with lock:
                    trace_id = str(getattr(span, "trace_id"))
                    opened = traces.get(trace_id)
                    if opened is None:
                        opened = open_trace(_ImplicitTrace(trace_id))
                    if opened is None:
                        return
                    span_data = getattr(span, "span_data")
                    official_span_type = _official_agents_span_type(
                        span_data,
                        adapter_version,
                    )
                    data = _span_data_snapshot(span_data)
                    span_type = official_span_type or data.get("type")
                    if span_type == "generation":
                        open_generation_spans[trace_id] = open_generation_spans.get(trace_id, 0) + 1
                    name = f"openai_agents.{span_type or 'unknown'}"
                    span_id = str(getattr(span, "span_id"))
                    parent = _span_parent(spans, span)
                    semantic = _span_start_semantic(
                        span_type,
                        span_id,
                        data,
                        official_span_type=official_span_type,
                    )
                    start_value: dict[str, Any] = {
                        "kind": _span_kind(
                            span_type,
                            official_span_type=official_span_type,
                        ),
                        "phase": "start",
                        "name": name,
                        "trace": opened.identity,
                        "native_identity": span_id,
                        "native": _span_snapshot(span, data),
                        "semantic": semantic,
                    }
                    _parent_from_receipt(start_value, parent)
                    start = sink.record(start_value)
                    spans[span_id] = _OpenSpan(opened.identity, name, start)

            def on_span_end(self, span: Any) -> None:
                if not active:
                    return
                with lock:
                    span_id = str(getattr(span, "span_id"))
                    opened = spans.pop(span_id, None)
                    if opened is None:
                        return
                    span_data = getattr(span, "span_data")
                    official_span_type = _official_agents_span_type(
                        span_data,
                        adapter_version,
                    )
                    data = _span_data_snapshot(span_data)
                    span_type = official_span_type or data.get("type")
                    error = getattr(span, "error", None)
                    kind = _span_kind(
                        span_type,
                        official_span_type=official_span_type,
                    )
                    phase = "error" if error is not None else "end"
                    semantic = _span_end_semantic(
                        span_type,
                        span_id,
                        data,
                        phase,
                        official_span_type=official_span_type,
                    )
                    value: dict[str, Any] = {
                        "kind": kind,
                        "phase": phase,
                        "name": opened.name,
                        "trace": opened.trace,
                        **(
                            {"error_identity": error}
                            if isinstance(error, BaseException)
                            else {}
                        ),
                        # Lifecycle identity belongs to the framework span.  A provider
                        # response id is separate overlap evidence and must not replace
                        # the identity used by the matching span start.
                        "native_identity": span_id,
                        "native": _span_snapshot(span, data),
                        "semantic": semantic,
                    }
                    if opened.start.accepted and opened.start.record_id is not None:
                        value["parent_record_id"] = opened.start.record_id
                    terminal = sink.record(value)
                    trace_id = str(getattr(span, "trace_id"))
                    if span_type == "generation":
                        remaining = max(0, open_generation_spans.get(trace_id, 0) - 1)
                        if remaining:
                            open_generation_spans[trace_id] = remaining
                        else:
                            open_generation_spans.pop(trace_id, None)
                    if official_span_type == "task":
                        set_trace_outcome(
                            str(getattr(span, "trace_id")),
                            "failed" if error is not None else "completed",
                        )
                    response_id = data.get("response_id")
                    if span_type == "response" and response_id:
                        response_value: dict[str, Any] = {
                            "kind": "error" if error is not None else "model",
                            "phase": "event",
                            "name": opened.name,
                            "trace": opened.trace,
                            **(
                                {"error_identity": error}
                                if isinstance(error, BaseException)
                                else {}
                            ),
                            "native_identity": response_id,
                            "coverage": _OPENAI_RESPONSE_COVERAGE,
                            "native": _span_snapshot(span, data),
                            "semantic": {
                                "type": "model.error" if error is not None else "model.response",
                                "framework": "openai-agents",
                            },
                        }
                        if terminal.accepted and terminal.record_id is not None:
                            response_value["parent_record_id"] = terminal.record_id
                        sink.record(response_value)
                    if error is not None:
                        error_value: dict[str, Any] = {
                            "kind": "error",
                            "phase": "event",
                            "name": "openai_agents.span.error",
                            "trace": opened.trace,
                            **(
                                {"error_identity": error}
                                if isinstance(error, BaseException)
                                else {}
                            ),
                            "native_identity": span_id,
                            "native": {
                                **_openai_stream_error_native(error),
                                "span_data": data,
                            },
                            "semantic": {
                                "type": "error",
                                "framework": "openai-agents",
                                **_semantic_span_error(error),
                            },
                        }
                        if terminal.accepted and terminal.record_id is not None:
                            error_value["parent_record_id"] = terminal.record_id
                        sink.record(error_value)
                    if span_type == "generation":
                        streamed_match = exact_stream_reasoning(trace_id, data.get("output"))
                        streamed = streamed_match[1] if streamed_match is not None else None
                        if streamed_match is not None:
                            recorded_partial_responses.add((trace_id, streamed_match[0]))
                            pending_partial_status.pop(trace_id, None)
                        pending_proposals = trace_tool_proposals.get(trace_id, set())
                        if (
                            pending_proposals
                            and trace_id not in trace_tool_chronology_lost
                        ):
                            add_tool_gap(
                                trace_id,
                                "tool_chronology_not_captured",
                                len(pending_proposals),
                            )
                            trace_tool_chronology_lost.add(trace_id)
                        parent = _span_parent(spans, span)
                        context_refs = _record_model_context(
                            sink,
                            opened.trace,
                            span_id,
                            data.get("input"),
                            parent=parent,
                        )
                        request_value: dict[str, Any] = {
                            "kind": "model",
                            "phase": "event",
                            "name": "openai_agents.model.request",
                            "trace": opened.trace,
                            "native_identity": span_id,
                            "native": {
                                "input": data.get("input"),
                                "model": data.get("model"),
                                "model_config": data.get("model_config"),
                            },
                            "semantic": {
                                "type": "model.request",
                                "framework": "openai-agents",
                                "model": data.get("model"),
                                "context_refs": context_refs,
                            },
                        }
                        _parent_from_receipt(request_value, parent)
                        request = sink.record(request_value)
                        if error is not None or data.get("output") is not None:
                            generation_response_value: dict[str, Any] = {
                                "kind": "model",
                                "phase": "event",
                                "name": "openai_agents.model.response",
                                "trace": opened.trace,
                                "native_identity": span_id,
                                "native": {
                                    "output": data.get("output"),
                                    "model": data.get("model"),
                                    "usage": data.get("usage"),
                                    **(
                                        {"consumed_reasoning_events": streamed["native_events"]}
                                        if streamed
                                        else {}
                                    ),
                                },
                                "semantic": {
                                    "type": "model.response",
                                    "framework": "openai-agents",
                                    "model": data.get("model"),
                                    "content": data.get("output"),
                                    **(
                                        {"reasoning": reasoning}
                                        if (
                                            reasoning := _openai_agents_reasoning(
                                                data.get("output")
                                            )
                                        )
                                        else {}
                                    ),
                                    **(
                                        {
                                            "reasoning": [
                                                {"type": block["type"], "text": block["text"]}
                                                for block in streamed["blocks"].values()
                                            ]
                                        }
                                        if streamed
                                        and not _openai_agents_reasoning(data.get("output"))
                                        else {}
                                    ),
                                    "status": "failed" if error is not None else "completed",
                                    "usage": data.get("usage"),
                                },
                            }
                            if request.accepted and request.record_id is not None:
                                generation_response_value["parent_record_id"] = request.record_id
                            response = sink.record(generation_response_value)
                            _record_unavailable_reasoning_gap(
                                sink,
                                opened.trace,
                                framework="openai-agents",
                                affected=response,
                                count=_openai_agents_unavailable_reasoning(
                                    data.get("output")
                                ),
                                detail=(
                                    "OpenAI Agents exposed encrypted reasoning bytes; "
                                    "the bytes were omitted from canonical reasoning."
                                ),
                            )
                        if streamed_match is None and trace_id in pending_partial_status:
                            record_partial_reasoning(
                                trace_id,
                                pending_partial_status[trace_id],
                            )
                        proposals = trace_tool_proposals.setdefault(trace_id, set())
                        for proposal in _generation_tool_proposals(data.get("output")):
                            if proposal["kind"] == "gap":
                                add_tool_gap(trace_id, proposal["reason"])
                                continue
                            call_id = proposal["call_id"]
                            if call_id in proposals:
                                continue
                            proposals.add(call_id)
                            proposal_value: dict[str, Any] = {
                                "kind": "tool",
                                "phase": "event",
                                "name": "openai_agents.tool.proposal",
                                "trace": opened.trace,
                                "native_identity": call_id,
                                "native": {"item": proposal["native"]},
                                "semantic": {
                                    "type": "tool.proposal",
                                    "framework": "openai-agents",
                                    "name": proposal["name"],
                                    "input": proposal["input"],
                                    "native_call_id": call_id,
                                },
                            }
                            _parent_from_receipt(proposal_value, parent)
                            sink.record(proposal_value)
                        if data.get("usage") is not None:
                            sink.record(
                                {
                                    "kind": "model",
                                    "phase": "event",
                                    "name": "openai_agents.usage",
                                    "trace": opened.trace,
                                    "native_identity": span_id,
                                    "native": {"usage": data.get("usage")},
                                    "semantic": {
                                        "type": "capture.redundant",
                                        "framework": "openai-agents",
                                    },
                                }
                            )

            def force_flush(self) -> None:
                return None

            def shutdown(self) -> None:
                return None

        processor = Processor()
        runner_restores: list[Any] = []

        def restore_runner() -> None:
            for restore in reversed(runner_restores):
                restore()

        runner = getattr(self.subject, "Runner", None)
        run_descriptor = vars(runner).get("run") if runner is not None else None
        original_run = getattr(runner, "run", None)
        if runner is not None and run_descriptor is not None and callable(original_run):

            async def run(cls: Any, *args: Any, **kwargs: Any) -> Any:
                marker = object()
                token = runner_invocation.set(marker)
                begin_runner_invocation(marker)
                try:
                    result = await original_run(*args, **kwargs)
                except BaseException as error:
                    settle_runner_invocation(marker, completed=False, error=error)
                    raise
                else:
                    settle_runner_invocation(marker, result=result, completed=True)
                    return result
                finally:
                    runner_invocation.reset(token)

            run_replacement = classmethod(run)
            try:
                setattr(runner, "run", run_replacement)
            except BaseException:
                active = False
                raise

            def restore_run() -> None:
                if vars(runner).get("run") is run_replacement:
                    setattr(runner, "run", run_descriptor)

            runner_restores.append(restore_run)

        run_sync_descriptor = vars(runner).get("run_sync") if runner is not None else None
        original_run_sync = getattr(runner, "run_sync", None)
        if runner is not None and run_sync_descriptor is not None and callable(original_run_sync):

            def run_sync(cls: Any, *args: Any, **kwargs: Any) -> Any:
                marker = object()
                token = runner_invocation.set(marker)
                begin_runner_invocation(marker)
                try:
                    result = original_run_sync(*args, **kwargs)
                except BaseException as error:
                    settle_runner_invocation(marker, completed=False, error=error)
                    raise
                else:
                    settle_runner_invocation(marker, result=result, completed=True)
                    return result
                finally:
                    runner_invocation.reset(token)

            run_sync_replacement = classmethod(run_sync)
            try:
                setattr(runner, "run_sync", run_sync_replacement)
            except BaseException:
                active = False
                restore_runner()
                raise

            def restore_run_sync() -> None:
                if vars(runner).get("run_sync") is run_sync_replacement:
                    setattr(runner, "run_sync", run_sync_descriptor)

            runner_restores.append(restore_run_sync)

        descriptor = vars(runner).get("run_streamed") if runner is not None else None
        original_run_streamed = getattr(runner, "run_streamed", None)
        if runner is not None and descriptor is not None and callable(original_run_streamed):

            def run_streamed(cls: Any, *args: Any, **kwargs: Any) -> Any:
                marker = object()
                token = runner_invocation.set(marker)
                begin_runner_invocation(marker)
                try:
                    result = original_run_streamed(*args, **kwargs)
                except BaseException as error:
                    settle_runner_invocation(marker, completed=False, error=error)
                    raise
                finally:
                    runner_invocation.reset(token)
                if getattr(result, "_semantic_layer_stream_capture", False):
                    settle_runner_invocation(marker, completed=False)
                    return result
                original_stream_events = getattr(result, "stream_events", None)
                if not callable(original_stream_events):
                    settle_runner_invocation(marker, completed=False)
                    return result
                terminal_observed = False
                initial_trace_id = str(getattr(getattr(result, "trace", None), "trace_id", ""))
                with lock:
                    if (
                        initial_trace_id in traces
                        and marker not in runner_invocation_traces
                    ):
                        runner_invocation_traces[marker] = initial_trace_id
                        trace_owner_invocations.setdefault(initial_trace_id, marker)
                        acquire_stream_hold(initial_trace_id)
                initial_trace = traces.get(initial_trace_id)
                stream_trace: dict[str, str] | None = (
                    initial_trace.identity if initial_trace is not None else None
                )
                consumer_completed = False
                completion_recorded = False
                cancellation_recorded = False
                observed_error_ids: set[int] = set()
                first_consumer_claimed = False

                def observe_event(event: Any) -> None:
                    nonlocal terminal_observed, stream_trace
                    if not active:
                        return
                    trace_id = str(getattr(getattr(result, "trace", None), "trace_id", ""))
                    opened = traces.get(trace_id)
                    if opened is not None:
                        accumulate_stream_reasoning(trace_id, event)
                        stream_trace = opened.identity
                        if _is_official_terminal_response_failure(
                            event,
                            adapter_version,
                        ) and trace_owner_invocations.get(trace_id) is marker:
                            set_trace_outcome(trace_id, "failed")
                        terminal_observed = (
                            _record_openai_stream_event(
                                sink,
                                opened.identity,
                                event,
                                version=adapter_version,
                            )
                            or terminal_observed
                        )

                def observe_error(error: BaseException) -> None:
                    error_id = id(error)
                    if error_id in observed_error_ids:
                        return
                    observed_error_ids.add(error_id)
                    if not active:
                        return
                    trace_id = str(getattr(getattr(result, "trace", None), "trace_id", ""))
                    opened = traces.get(trace_id)
                    record_partial_reasoning(trace_id, "incomplete")
                    trace_identity = opened.identity if opened is not None else stream_trace
                    if trace_identity is not None:
                        if (
                            trace_id
                            and opened is not None
                            and trace_owner_invocations.get(trace_id) is marker
                        ):
                            record_runner_error(trace_id, error)
                        else:
                            sink.record(
                                {
                                    "kind": "error",
                                    "phase": "event",
                                    "name": "openai_agents.stream.error",
                                    "trace": trace_identity,
                                    "error_identity": error,
                                    "native": _openai_stream_error_native(error),
                                    "semantic": {
                                        "type": "error",
                                        "framework": "openai-agents",
                                        "error": _runner_semantic_error(error),
                                    },
                                }
                            )

                def record_complete() -> None:
                    nonlocal terminal_observed, completion_recorded
                    if completion_recorded:
                        return
                    if not active:
                        return
                    trace_id = str(getattr(getattr(result, "trace", None), "trace_id", ""))
                    opened = traces.get(trace_id)
                    trace_identity = opened.identity if opened is not None else stream_trace
                    if trace_identity is not None:
                        completion_recorded = True
                        terminal_observed = True
                        owns_trace = trace_owner_invocations.get(trace_id) is marker
                        record_run_result(
                            trace_id,
                            result,
                            include_final_output=owns_trace,
                        )
                        if owns_trace:
                            trace_outcomes[trace_id] = "completed"

                def observe_complete() -> None:
                    nonlocal consumer_completed
                    if not active:
                        return
                    consumer_completed = True
                    run_loop_error = _openai_run_loop_exception(result)
                    if run_loop_error is not None:
                        observe_error(run_loop_error)
                        return
                    task = _openai_run_loop_task(result)
                    if task is not None:
                        try:
                            if not task.done():
                                return
                            if task.cancelled():
                                record_cancel(
                                    "openai_agents.stream.run_cancelled",
                                    {"run_task_cancelled": True},
                                )
                                return
                        except BaseException:
                            return
                    record_complete()

                def observe_run_loop_done(task: Any) -> None:
                    try:
                        if not active:
                            return
                        if task.cancelled():
                            record_cancel(
                                "openai_agents.stream.run_cancelled",
                                {"run_task_cancelled": True},
                            )
                            return
                        error = task.exception()
                        if isinstance(error, BaseException):
                            observe_error(error)
                        elif consumer_completed:
                            record_complete()
                    except BaseException:
                        pass
                    finally:
                        with lock:
                            task_callbacks.pop(task_token, None)
                        release_stream_hold(initial_trace_id)

                def record_cancel(name: str, native: dict[str, Any]) -> None:
                    nonlocal cancellation_recorded, terminal_observed
                    if cancellation_recorded:
                        return
                    if not active:
                        return
                    trace_id = str(getattr(getattr(result, "trace", None), "trace_id", ""))
                    opened = traces.get(trace_id)
                    record_partial_reasoning(trace_id, "cancelled")
                    trace_identity = opened.identity if opened is not None else stream_trace
                    if trace_identity is not None:
                        cancellation_recorded = True
                        terminal_observed = True
                        if native.get("run_task_cancelled") is True:
                            if trace_owner_invocations.get(trace_id) is marker:
                                set_trace_outcome(trace_id, "cancelled")
                            return
                        sink.record(
                            {
                                "kind": "state",
                                "phase": "event",
                                "name": name,
                                "trace": trace_identity,
                                "native": native,
                                "semantic": {
                                    "type": "state.stream.consumer_cancelled",
                                    "framework": "openai-agents",
                                    "value": native,
                                },
                            }
                        )

                def observe_cancel() -> None:
                    record_cancel(
                        "openai_agents.stream.consumer_cancelled",
                        {"consumer_action": "aclose"},
                    )

                def release_consumer_hold(token: object) -> None:
                    with lock:
                        held = consumer_holds.pop(token, None)
                    if held is not None:
                        release_stream_hold(held[0])

                def stream_events() -> _ObservedAsyncIterator:
                    nonlocal first_consumer_claimed
                    with lock:
                        if not first_consumer_claimed:
                            first_consumer_claimed = True
                            token = initial_consumer_token
                            held = consumer_holds.get(token)
                            if held is not None:
                                consumer_holds[token] = (held[0], held[1], True)
                        else:
                            token = object()
                            acquire_stream_hold(initial_trace_id)
                            consumer_holds[token] = (initial_trace_id, stream_trace, True)
                    try:
                        iterator = original_stream_events()
                    except BaseException as error:
                        try:
                            observe_error(error)
                        finally:
                            release_consumer_hold(token)
                        raise
                    return _ObservedAsyncIterator(
                        iterator,
                        observe_event,
                        observe_error,
                        observe_complete,
                        observe_cancel,
                        lambda: release_consumer_hold(token),
                    )

                had_stream = "stream_events" in vars(result)
                own_stream = vars(result).get("stream_events")
                try:
                    setattr(result, "stream_events", stream_events)
                    setattr(result, "_semantic_layer_stream_capture", True)
                except BaseException:
                    if vars(result).get("stream_events") is stream_events:
                        if had_stream:
                            setattr(result, "stream_events", own_stream)
                        else:
                            delattr(result, "stream_events")
                    settle_runner_invocation(marker, completed=False)
                    return result
                initial_consumer_token = object()
                with lock:
                    acquire_stream_hold(initial_trace_id)
                    consumer_holds[initial_consumer_token] = (
                        initial_trace_id,
                        stream_trace,
                        False,
                    )
                task = _openai_run_loop_task(result)
                if task is not None and initial_trace_id:
                    task_token = object()
                    with lock:
                        acquire_stream_hold(initial_trace_id)
                        task_callbacks[task_token] = (task, observe_run_loop_done)
                    try:
                        task.add_done_callback(observe_run_loop_done)
                    except BaseException:
                        with lock:
                            task_callbacks.pop(task_token, None)
                        release_stream_hold(initial_trace_id)
                settle_runner_invocation(marker, completed=False)
                return result

            replacement = classmethod(run_streamed)
            try:
                setattr(runner, "run_streamed", replacement)
            except BaseException:
                active = False
                restore_runner()
                raise

            def restore_run_streamed() -> None:
                if vars(runner).get("run_streamed") is replacement:
                    setattr(runner, "run_streamed", descriptor)

            runner_restores.append(restore_run_streamed)

        trusted_official_registration = _is_exact_official_agents_registration(
            self.subject, register
        )
        if not trusted_official_registration and not callable(unregister):
            active = False
            restore_runner()
            raise TypeError(
                "non-official OpenAI Agents subjects must expose remove_trace_processor"
            )
        try:
            register(processor)
        except BaseException:
            active = False
            restore_runner()
            if callable(unregister):
                unregister(processor)
            raise

        def deactivate() -> None:
            nonlocal active
            with lock:
                unsettled_by_trace: dict[str, tuple[dict[str, str] | None, int, int]] = {}
                for trace_id, trace_identity, started in consumer_holds.values():
                    identity, total, unstarted = unsettled_by_trace.get(
                        trace_id, (trace_identity, 0, 0)
                    )
                    unsettled_by_trace[trace_id] = (
                        identity or trace_identity,
                        total + 1,
                        unstarted + (0 if started else 1),
                    )
                for trace_id, (trace_identity, total, unstarted) in unsettled_by_trace.items():
                    opened = traces.get(trace_id)
                    identity = opened.identity if opened is not None else trace_identity
                    if identity is not None:
                        sink.record(
                            {
                                "kind": "stream",
                                "phase": "gap",
                                "name": "openai_agents.stream.consumer_unsettled",
                                "trace": identity,
                                "native": {
                                    "unsettled_consumers": total,
                                    "unstarted_consumers": unstarted,
                                    "shutdown": True,
                                },
                                "semantic": {
                                    "type": "stream.gap",
                                    "framework": "openai-agents",
                                },
                            }
                        )
                active = False
                callbacks = list(task_callbacks.values())
                task_callbacks.clear()
                consumer_holds.clear()
                pending_stream_holds.clear()
                deferred_trace_ends.clear()
                trace_outcomes.clear()
                trace_errors.clear()
                trace_output_refs.clear()
                trace_tool_proposals.clear()
                trace_tool_executions.clear()
                trace_tool_results.clear()
                trace_tool_chronology_lost.clear()
                trace_tool_gaps.clear()
                runner_invocation_traces.clear()
                trace_owner_invocations.clear()
                stream_reasoning.clear()
                active_stream_responses.clear()
                recorded_partial_responses.clear()
                reasoning_correlation_gaps.clear()
                open_generation_spans.clear()
                pending_partial_status.clear()
                spans.clear()
                traces.clear()
            for task, callback in callbacks:
                try:
                    task.remove_done_callback(callback)
                except BaseException:
                    pass
            restore_runner()
            if callable(unregister):
                try:
                    unregister(processor)
                except (KeyError, ValueError):
                    pass

        return _Lifecycle(deactivate)


class _ImplicitTrace:
    def __init__(self, trace_id: str) -> None:
        self.trace_id = trace_id
        self.name = "openai_agents.trace"
        self.group_id = None
        self.metadata: dict[str, Any] = {}


def openai_agents_adapter(*, version: str | None = None) -> _AgentsAdapter:
    return _AgentsAdapter(_installed_version("openai-agents", version))


def _trace_snapshot(trace: Any) -> dict[str, Any]:
    return {
        "trace_id": getattr(trace, "trace_id", None),
        "name": getattr(trace, "name", None),
        "group_id": getattr(trace, "group_id", None),
        "metadata": _raw_native(getattr(trace, "metadata", None)),
    }


def _generation_tool_proposals(output: Any) -> list[dict[str, Any]]:
    """Read exact function-call proposals from the authoritative generation output."""

    if not isinstance(output, (list, tuple)):
        return []
    proposals: list[dict[str, Any]] = []
    missing = object()

    def append_proposal(
        *,
        call_id: Any,
        name: Any,
        arguments: Any,
        native: Any,
    ) -> None:
        if not isinstance(call_id, str) or not call_id or not isinstance(name, str) or not name:
            proposals.append(
                {
                    "kind": "gap",
                    "reason": "tool_correlation_not_captured",
                }
            )
        elif arguments is missing:
            proposals.append(
                {
                    "kind": "gap",
                    "reason": "tool_input_not_captured",
                }
            )
        else:
            proposals.append(
                {
                    "kind": "proposal",
                    "call_id": call_id,
                    "name": name,
                    "input": arguments,
                    "native": native,
                }
            )

    for item in output:
        if native_field(item, "type") == "function_call":
            append_proposal(
                call_id=native_field(item, "call_id"),
                name=native_field(item, "name"),
                arguments=native_field(item, "arguments", missing),
                native=item,
            )
            continue

        tool_calls = native_field(item, "tool_calls")
        if not isinstance(tool_calls, (list, tuple)):
            continue
        for tool_call in tool_calls:
            if native_field(tool_call, "type") != "function":
                continue
            function = native_field(tool_call, "function")
            append_proposal(
                call_id=native_field(tool_call, "id"),
                name=native_field(function, "name"),
                arguments=native_field(function, "arguments", missing),
                native=tool_call,
            )
    return proposals


def _official_run_tool_items(result: Any, version: str) -> list[dict[str, Any]]:
    """Read tool executions/results only from exact current RunResult item classes."""

    if version != "0.18.2":
        return []
    try:
        from agents.items import ToolCallItem, ToolCallOutputItem
    except ImportError:  # pragma: no cover - guarded by the adapter dependency
        return []
    new_items = _owned_field(result, "new_items")
    if not isinstance(new_items, list):
        return []
    captured: list[dict[str, Any]] = []
    for item in new_items:
        if type(item) is ToolCallItem:
            raw = _owned_field(item, "raw_item")
            native = raw if isinstance(raw, dict) else native_snapshot(raw)
            if not isinstance(native, dict):
                continue
            call_id = native.get("call_id") or native.get("id")
            name = native.get("name")
            if not isinstance(call_id, str) or not call_id or not isinstance(name, str) or not name:
                captured.append(
                    {
                        "kind": "gap",
                        "reason": "tool_correlation_not_captured",
                    }
                )
                continue
            if "arguments" not in native:
                captured.append(
                    {
                        "kind": "gap",
                        "reason": "tool_input_not_captured",
                    }
                )
                continue
            captured.append(
                {
                    "kind": "call",
                    "call_id": call_id,
                    "name": name,
                    "input": native["arguments"],
                    "native": native,
                }
            )
        elif type(item) is ToolCallOutputItem:
            raw = _owned_field(item, "raw_item")
            native = raw if isinstance(raw, dict) else native_snapshot(raw)
            if not isinstance(native, dict):
                continue
            call_id = native.get("call_id") or native.get("id")
            if not isinstance(call_id, str) or not call_id:
                captured.append(
                    {
                        "kind": "gap",
                        "reason": "tool_correlation_not_captured",
                    }
                )
                continue
            captured.append(
                {
                    "kind": "result",
                    "call_id": call_id,
                    "output": _owned_field(item, "output"),
                    "native": native,
                }
            )
    return captured


def _record_model_context(
    sink: Any,
    trace: dict[str, str],
    span_id: str,
    model_input: Any,
    *,
    parent: Any = None,
) -> list[str]:
    """Record only explicit role/content messages from the owned generation snapshot."""

    if not isinstance(model_input, list):
        return []
    refs: list[str] = []
    for index, message in enumerate(model_input):
        if not isinstance(message, dict):
            continue
        role = message.get("role")
        if role not in {"system", "developer", "user", "assistant", "tool"}:
            continue
        if "content" not in message:
            continue
        call_id = _context_call_id(message)
        value: dict[str, Any] = {
            "kind": "model",
            "phase": "event",
            "name": "openai_agents.model.context",
            "trace": trace,
            "native_identity": f"{span_id}:context:{index}",
            "native": {"message": message},
            "semantic": {
                "type": "message",
                "framework": "openai-agents",
                "origin": "context",
                "role": role,
                "content": message["content"],
                **({"call_id": call_id} if call_id is not None else {}),
            },
        }
        name = message.get("name")
        if isinstance(name, str) and name:
            value["semantic"]["name"] = name
        _parent_from_receipt(value, parent)
        admitted = sink.record(value)
        if admitted.accepted and admitted.record_id is not None:
            refs.append(admitted.record_id)
    return refs


def _context_call_id(message: dict[str, Any]) -> str | None:
    values = [
        message[key]
        for key in ("call_id", "tool_call_id", "callId", "toolCallId")
        if key in message
    ]
    if not values or any(not isinstance(value, str) or not value.strip() for value in values):
        return None
    return values[0] if len(set(values)) == 1 else None


def _semantic_span_error(error: Any) -> dict[str, Any]:
    """Expose the exact structured error fields the official span callback owns."""

    if not isinstance(error, dict):
        return {}
    message = error.get("message")
    if not isinstance(message, str):
        return {}
    details = error.get("data")
    recoverable = (
        details.get("retryable")
        if isinstance(details, dict) and isinstance(details.get("retryable"), bool)
        else False
    )
    normalized: dict[str, Any] = {
        "type": "openai_agents_span_error",
        "message": message,
        "recoverable": recoverable,
    }
    if details is not None:
        normalized["details"] = details
    return {"error": normalized}


def _runner_semantic_error(error: BaseException) -> dict[str, Any]:
    args = object.__getattribute__(error, "args")
    message = (
        args[0] if args and isinstance(args[0], str) and args[0] else "OpenAI Agents Runner failed"
    )
    semantic: dict[str, Any] = {
        "type": "openai_agents.runner_error",
        "message": message,
        "recoverable": False,
    }
    structured = _owned_field(error, "structured_error")
    if structured is not None:
        semantic["details"] = structured
    return semantic


def _span_snapshot(span: Any, data: dict[str, Any]) -> dict[str, Any]:
    return {
        "trace_id": getattr(span, "trace_id", None),
        "span_id": getattr(span, "span_id", None),
        "parent_id": getattr(span, "parent_id", None),
        "started_at": _raw_native(getattr(span, "started_at", None)),
        "ended_at": _raw_native(getattr(span, "ended_at", None)),
        "error": _raw_native(getattr(span, "error", None)),
        "span_data": data,
    }


def _span_data_snapshot(data: Any) -> dict[str, Any]:
    fields = (
        "type",
        "name",
        "turn",
        "agent_name",
        "input",
        "output",
        "model",
        "model_config",
        "usage",
        "from_agent",
        "to_agent",
        "triggered",
    )
    captured = native_snapshot(data)
    result = dict(captured) if isinstance(captured, dict) else {}
    result.update(
        {field: _raw_native(getattr(data, field)) for field in fields if hasattr(data, field)}
    )
    if result.get("type") == "response":
        response = getattr(data, "response", None)
        response_id = getattr(response, "id", None)
        if isinstance(response_id, str) and response_id:
            result["response_id"] = response_id
    return result


def _official_agents_span_type(data: Any, version: str) -> str | None:
    """Recognize only the three 0.18.2 Runner bookkeeping span classes."""

    if version != "0.18.2":
        return None
    try:
        from agents.tracing.span_data import AgentSpanData, TaskSpanData, TurnSpanData
    except ImportError:  # pragma: no cover - guarded by the adapter dependency
        return None
    for expected, span_type in (
        (AgentSpanData, "agent"),
        (TaskSpanData, "task"),
        (TurnSpanData, "turn"),
    ):
        if type(data) is expected:
            return span_type
    return None


def _span_parent(spans: dict[str, _OpenSpan], span: Any) -> Any:
    parent_id = getattr(span, "parent_id", None)
    if not isinstance(parent_id, str) or not parent_id:
        return None
    parent = spans.get(parent_id)
    return parent.start if parent is not None else None


def _parent_from_receipt(value: dict[str, Any], receipt: Any) -> None:
    if receipt is not None and receipt.accepted and receipt.record_id is not None:
        value["parent_record_id"] = receipt.record_id


def _span_start_semantic(
    span_type: Any,
    span_id: str,
    data: dict[str, Any],
    *,
    official_span_type: str | None,
) -> dict[str, Any]:
    if span_type in {"generation", "function"} or official_span_type == "task":
        semantic_type = "capture.redundant"
    elif official_span_type == "agent":
        return {
            "type": "agent.scope",
            "framework": "openai-agents",
            "scope_type": "agent",
            "scope_id": span_id,
            "name": data.get("name"),
        }
    elif official_span_type == "turn":
        return {
            "type": "scope",
            "framework": "openai-agents",
            "scope_type": "turn",
            "scope_id": span_id,
            "name": _turn_scope_name(data),
        }
    else:
        semantic_type = _span_semantic_type(span_type, "start")
    return {"type": semantic_type, "framework": "openai-agents"}


def _span_end_semantic(
    span_type: Any,
    span_id: str,
    data: dict[str, Any],
    phase: str,
    *,
    official_span_type: str | None,
) -> dict[str, Any]:
    if span_type in {"generation", "function"} or official_span_type == "task":
        semantic_type = "capture.redundant"
    elif official_span_type == "agent":
        return {
            "type": "agent.scope",
            "framework": "openai-agents",
            "scope_type": "agent",
            "scope_id": span_id,
            "status": "failed" if phase == "error" else "completed",
        }
    elif official_span_type == "turn":
        return {
            "type": "scope",
            "framework": "openai-agents",
            "scope_type": "turn",
            "scope_id": span_id,
            "status": "failed" if phase == "error" else "completed",
        }
    else:
        semantic_type = _span_semantic_type(span_type, phase)
    return {"type": semantic_type, "framework": "openai-agents"}


def _turn_scope_name(data: dict[str, Any]) -> str:
    turn = data.get("turn")
    agent_name = data.get("agent_name")
    if isinstance(turn, int) and not isinstance(turn, bool) and turn >= 0:
        if isinstance(agent_name, str) and agent_name:
            return f"{agent_name} turn {turn}"
        return f"turn {turn}"
    return "turn"


def _span_kind(value: Any, *, official_span_type: str | None = None) -> str:
    if official_span_type in {"agent", "turn"}:
        return "lifecycle"
    if value in {"generation", "response", "transcription", "speech"}:
        return "model"
    if value in {"function", "mcp_tools"}:
        return "tool"
    if value in {"agent", "handoff", "guardrail", "turn", "task"}:
        return "state"
    return "unknown"


def _span_semantic_type(value: Any, phase: str) -> str:
    if value == "generation":
        return {
            "start": "model.request",
            "end": "model.response",
            "error": "model.error",
        }[phase]
    if value == "function":
        return {
            "start": "tool.execution",
            "end": "tool.result",
            "error": "tool.error",
        }[phase]
    if value == "response":
        return "openai-agents.response"
    return "native.event"


def _openai_agents_reasoning(output: Any) -> list[dict[str, str]]:
    """Project only reasoning fields exposed by Responses reasoning items."""

    if not isinstance(output, (list, tuple)):
        return []
    blocks: list[dict[str, str]] = []

    def field(value: Any, name: str) -> Any:
        return value.get(name) if isinstance(value, dict) else _owned_field(value, name)

    def append(values: Any, kind: str) -> None:
        if not isinstance(values, (list, tuple)):
            return
        for part in values:
            text = field(part, "text")
            if isinstance(text, str) and text:
                blocks.append({"type": kind, "text": text})

    for item in output:
        if field(item, "type") != "reasoning":
            continue
        append(field(item, "summary"), "summary")
        append(field(item, "content"), "text")
    return blocks


def _openai_agents_unavailable_reasoning(output: Any) -> int:
    if not isinstance(output, (list, tuple)):
        return 0
    count = 0
    for item in output:
        if isinstance(item, dict):
            item_type = item.get("type")
            encrypted = item.get("encrypted_content")
        else:
            item_type = _owned_field(item, "type")
            encrypted = _owned_field(item, "encrypted_content")
        if (
            item_type == "reasoning"
            and isinstance(encrypted, (str, bytes))
            and encrypted
        ):
            count += 1
    return count


def _record_openai_stream_event(
    sink: Any,
    trace: dict[str, str],
    event: Any,
    *,
    version: str,
) -> bool:
    if _is_exact_official_stream_bookkeeping(event, version):
        return False
    response_completed_type: Any = ()
    try:
        from openai.types.responses import ResponseCompletedEvent

        response_completed_type = ResponseCompletedEvent
    except ImportError:  # pragma: no cover
        pass
    data = getattr(event, "data", None)
    native_type = str(getattr(data, "type", None) or getattr(event, "type", "unknown"))
    failure = _official_response_stream_failure(data, version)
    if failure is not None:
        event_kind = "error"
        semantic_type = "error"
    elif "delta" in native_type:
        event_kind = "stream"
        semantic_type = "stream.delta"
    elif isinstance(data, response_completed_type):
        event_kind = "stream"
        semantic_type = "stream.terminal"
    else:
        event_kind = "stream"
        semantic_type = "stream.event"
    sink.record(
        {
            "kind": event_kind,
            "phase": "event",
            "name": f"openai_agents.{native_type}",
            "trace": trace,
            "native_identity": _openai_stream_identity(event),
            "native": _raw_native(event),
            "semantic": {
                "type": semantic_type,
                "framework": "openai-agents",
                **({"error": failure} if failure is not None else {}),
            },
        }
    )
    return semantic_type == "stream.terminal"


_RUN_ITEM_EVENT_NAMES = {
    "message_output_created",
    "handoff_requested",
    "handoff_occured",
    "tool_called",
    "tool_search_called",
    "tool_search_output_created",
    "tool_output",
    "reasoning_item_created",
    "mcp_approval_requested",
    "mcp_approval_response",
    "mcp_list_tools",
}


def _is_exact_official_stream_bookkeeping(event: Any, version: str) -> bool:
    """Drop exact 0.18.2 stream mirrors already represented by spans and outcome."""

    if version != "0.18.2":
        return False
    try:
        from agents.stream_events import (
            AgentUpdatedStreamEvent,
            RawResponsesStreamEvent,
            RunItemStreamEvent,
        )
    except ImportError:  # pragma: no cover - guarded by the adapter dependency
        return False
    event_type = getattr(event, "type", None)
    if type(event) is AgentUpdatedStreamEvent:
        return event_type == "agent_updated_stream_event"
    if type(event) is RunItemStreamEvent:
        return (
            event_type == "run_item_stream_event"
            and getattr(event, "name", None) in _RUN_ITEM_EVENT_NAMES
        )
    if type(event) is RawResponsesStreamEvent:
        data = getattr(event, "data", None)
        return (
            event_type == "raw_response_event"
            and type(data) in _official_response_stream_event_types()
            and type(data) not in _official_response_failure_event_types()
        )
    return False


@lru_cache(maxsize=1)
def _official_response_stream_event_types() -> tuple[type[Any], ...]:
    """Resolve the exact response event classes in the installed Agents contract."""

    try:
        from agents.items import TResponseStreamEvent
    except ImportError:  # pragma: no cover - guarded by the adapter dependency
        return ()
    annotation = TResponseStreamEvent
    if get_origin(annotation) is Annotated:
        annotation = get_args(annotation)[0]
    if get_origin(annotation) not in {Union, UnionType}:
        return ()
    return tuple(value for value in get_args(annotation) if isinstance(value, type))


@lru_cache(maxsize=1)
def _official_response_failure_event_types() -> tuple[type[Any], ...]:
    try:
        from openai.types.responses import (
            ResponseErrorEvent,
            ResponseFailedEvent,
            ResponseIncompleteEvent,
            ResponseMcpCallFailedEvent,
            ResponseMcpListToolsFailedEvent,
        )
    except ImportError:  # pragma: no cover - guarded by the adapter dependency
        return ()
    return (
        ResponseErrorEvent,
        ResponseFailedEvent,
        ResponseIncompleteEvent,
        ResponseMcpCallFailedEvent,
        ResponseMcpListToolsFailedEvent,
    )


def _is_official_terminal_response_failure(event: Any, version: str) -> bool:
    if version != "0.18.2":
        return False
    try:
        from agents.stream_events import RawResponsesStreamEvent
        from openai.types.responses import (
            ResponseErrorEvent,
            ResponseFailedEvent,
            ResponseIncompleteEvent,
        )
    except ImportError:  # pragma: no cover - guarded by the adapter dependency
        return False
    if type(event) is not RawResponsesStreamEvent:
        return False
    return type(getattr(event, "data", None)) in {
        ResponseErrorEvent,
        ResponseFailedEvent,
        ResponseIncompleteEvent,
    }


def _official_response_stream_failure(
    data: Any,
    version: str,
) -> dict[str, Any] | None:
    if version != "0.18.2" or type(data) not in _official_response_failure_event_types():
        return None
    response = getattr(data, "response", None)
    response_error = getattr(response, "error", None)
    message = getattr(data, "message", None)
    if not isinstance(message, str) or not message:
        message = getattr(response_error, "message", None)
    if not isinstance(message, str) or not message:
        message = {
            "response.failed": "OpenAI response failed.",
            "response.incomplete": "OpenAI response was incomplete.",
        }.get(str(getattr(data, "type", "")), "OpenAI response stream failed.")
    return {
        "type": "openai_agents_response_stream_error",
        "message": message,
        "recoverable": False,
    }


def _openai_stream_identity(event: Any) -> str:
    data = getattr(event, "data", None)
    response = getattr(data, "response", None)
    for value in (
        getattr(data, "item_id", None),
        getattr(response, "id", None),
        getattr(getattr(data, "item", None), "id", None),
    ):
        if value:
            return f"model:{value}"
    event_type = str(getattr(event, "type", type(event).__name__))
    name = str(getattr(event, "name", "event"))
    return f"runner:{event_type}:{name}"


def _openai_run_loop_exception(result: Any) -> BaseException | None:
    """Read the official post-stream failure slot without affecting the consumer."""

    try:
        error = getattr(result, "run_loop_exception", None)
    except BaseException:
        return None
    return error if isinstance(error, BaseException) else None


def _openai_run_loop_task(result: Any) -> Any:
    try:
        task = getattr(result, "run_loop_task", None)
    except BaseException:
        return None
    return task if callable(getattr(task, "add_done_callback", None)) else None


def _openai_stream_error_native(error: BaseException) -> dict[str, Any]:
    """Keep the exact exception and expose its reviewed rich envelope at a stable pointer."""

    native: dict[str, Any] = {"error": _raw_native(error)}
    structured = _owned_field(error, "structured_error")
    if structured is not None:
        native["structured_error"] = structured
    return native
