"""Official strands adapter capture adapter."""

from __future__ import annotations

import asyncio
import contextvars
from concurrent.futures import CancelledError as FutureCancelledError
from typing import Any

from ._framework_adapter_shared import (
    _explicit_turn_identity,
    _framework_metadata,
    _FrameworkAdapter,
    _installed_version,
    _Lifecycle,
    _OpenTrace,
    _owned_field,
    _provider_error_native,
    _raw_native,
    _record_unavailable_reasoning_gap,
    _source_qualification,
)
from .capture_v1 import AdmissionReceipt, CaptureSource

_STRANDS_AGENT_COVERAGE = {"operation": "agent-run", "domain": "strands.agent"}


_TOOL_EXECUTION_COVERAGE = {
    "operation": "tool-execution",
    "domain": "gen-ai.tool-call",
}


def strands_adapter(*, version: str | None = None) -> _FrameworkAdapter:
    """Capture a Strands Agent through its official lifecycle hook registry."""

    return _FrameworkAdapter(_installed_version("strands-agents", version), _StrandsSource)


class _StrandsSource(CaptureSource):
    def __init__(self, agent: object, version: str) -> None:
        self.agent = agent
        self.metadata = _framework_metadata(
            "strands-python",
            "Agent.add_hook/HookRegistry + Agent.callback_handler wrapper",
            "strands.agent",
            version,
        )
        self.metadata["qualification"] = _source_qualification(
            version,
            exact_versions=frozenset({"1.47.0"}),
            profile="strands-python-adapter-v1",
        )
        self.metadata["coverage"] = [
            {**_STRANDS_AGENT_COVERAGE, "role": "owner"},
            {**_TOOL_EXECUTION_COVERAGE, "role": "owner"},
        ]

    def install(self, sink: Any) -> _Lifecycle:
        add_hook = getattr(self.agent, "add_hook", None)
        if not callable(add_hook):
            raise TypeError("Strands subject must expose add_hook")
        try:
            from strands.hooks import (
                AfterInvocationEvent,
                AfterModelCallEvent,
                AfterToolCallEvent,
                BeforeInvocationEvent,
                BeforeModelCallEvent,
                BeforeToolCallEvent,
                MessageAddedEvent,
            )
        except ImportError as error:  # pragma: no cover - dependency error is environment-specific
            raise RuntimeError("strands-agents is required for the Strands adapter") from error

        active = True
        traces: dict[int, _OpenTrace] = {}
        starts: dict[tuple[int, str, str | None], AdmissionReceipt] = {}
        start_identities: dict[tuple[int, str, str | None], str] = {}
        model_call_sequences: dict[int, int] = {}
        failed_turns: set[tuple[str, str]] = set()
        message_evidence: dict[
            int, dict[int, tuple[object, str, object, list[str]]]
        ] = {}
        tool_result_evidence: dict[
            int, dict[int, tuple[object, str, object, str]]
        ] = {}
        tool_proposal_evidence: dict[
            int, dict[int, tuple[object, str, object, str]]
        ] = {}
        system_context_evidence: dict[int, dict[object, str]] = {}
        request_gap_emitted: set[int] = set()
        cancelled_invocations: set[int] = set()
        streamed_reasoning: dict[int, list[dict[str, str]]] = {}
        structured_invocations: set[int] = set()
        streaming_invocations: set[int] = set()
        pending_structured: dict[int, tuple[int, _OpenTrace, dict[str, Any]]] = {}
        pending_streams: dict[int, tuple[int, _OpenTrace, dict[str, Any]]] = {}
        model_cycle_keys: dict[str, int] = {}
        invocation_turn_keys: dict[tuple[str, str], int] = {}
        active_stream_turns: set[tuple[str, str]] = set()
        structured_invocation_token: contextvars.ContextVar[object | None] = (
            contextvars.ContextVar("semantic_layer_strands_structured_token", default=None)
        )
        structured_token_keys: dict[object, int] = {}

        def cleanup_trace(key: int, agent: Any) -> None:
            traces.pop(key, None)
            model_call_sequences.pop(key, None)
            message_evidence.pop(key, None)
            tool_result_evidence.pop(key, None)
            tool_proposal_evidence.pop(key, None)
            system_context_evidence.pop(key, None)
            request_gap_emitted.discard(key)
            cancelled_invocations.discard(key)
            streamed_reasoning.pop(key, None)
            for cycle_id, owner in list(model_cycle_keys.items()):
                if owner == key:
                    model_cycle_keys.pop(cycle_id, None)
            for turn_key, owner in list(invocation_turn_keys.items()):
                if owner == key:
                    invocation_turn_keys.pop(turn_key, None)

        def callback(event: Any) -> None:
            if not active:
                return
            raw_state = getattr(event, "invocation_state", None)
            state = _strands_invocation_identity_state(
                raw_state
            )
            key = id(raw_state) if isinstance(raw_state, dict) else id(event)
            structured_token = structured_invocation_token.get()
            mapped_structured_key = (
                structured_token_keys.get(structured_token)
                if structured_token is not None
                else None
            )
            if mapped_structured_key is not None:
                key = mapped_structured_key
            event_name = type(event).__name__
            native = _strands_event_evidence(event)
            if isinstance(event, BeforeInvocationEvent):
                if structured_token is not None:
                    structured_token_keys[structured_token] = key
                    structured_invocations.add(key)
                turn_key = _strands_explicit_turn_key(state, self.agent)
                if turn_key is not None:
                    invocation_turn_keys[turn_key] = key
                if turn_key in active_stream_turns:
                    streaming_invocations.add(key)
                opened = sink.open_trace(
                    {
                        "name": "strands.invocation",
                        "coverage": _STRANDS_AGENT_COVERAGE,
                        "native_identity": str(key),
                        "conversation_id": _conversation_id(state, self.agent),
                        **_explicit_turn_identity(state),
                        "native": native,
                        "semantic": {
                            "type": "agent.run",
                            "framework": "strands",
                            "name": "strands.invocation",
                        },
                    }
                )
                if opened.accepted and opened.identity is not None:
                    traces[key] = _OpenTrace(dict(opened.identity), "strands.invocation")
                    if bool(getattr(event, "cancel", False)):
                        cancelled_invocations.add(key)
                    previous_turn_id = (
                        state.get("previous_turn_id") if isinstance(state, dict) else None
                    )
                    failure_key = (
                        _conversation_id(state, self.agent),
                        str(previous_turn_id),
                    )
                    if previous_turn_id is not None and failure_key in failed_turns:
                        sink.record(
                            {
                                "kind": "state",
                                "phase": "event",
                                "name": "strands.recovery.retry",
                                "trace": dict(opened.identity),
                                "native": native,
                                "semantic": {
                                    "type": "state.retry",
                                    "framework": "strands",
                                    "state_type": "recovery.retry",
                                    "value": {
                                        "previous_turn_id": str(previous_turn_id),
                                    },
                                },
                            }
                        )
                        failed_turns.remove(failure_key)
                return
            opened = traces.get(key)
            if opened is None:
                return
            if isinstance(event, AfterInvocationEvent):
                result = getattr(event, "result", None)
                agent = getattr(event, "agent", self.agent)
                if result is None and key in streaming_invocations:
                    pending_streams[key] = (key, opened, native)
                    return
                if result is None and key in structured_invocations:
                    pending_structured[key] = (key, opened, native)
                    return
                result_message = _strands_field(result, "message")
                result_semantic = _strands_message_semantic(result_message)
                result_refs = (
                    _strands_exact_message_refs(
                        result_message,
                        result_semantic,
                        message_evidence.get(key, {}),
                    )
                    if result_semantic is not None
                    else None
                )
                stop_reason = _strands_field(result, "stop_reason")
                status = (
                    "cancelled"
                    if key in cancelled_invocations or stop_reason == "cancelled"
                    else "unknown"
                    if result is None
                    else "succeeded"
                )
                semantic: dict[str, Any] = {
                    "type": "agent.run",
                    "framework": "strands",
                    "status": status,
                }
                if result is not None:
                    result_output = _strands_agent_result_output(
                        result,
                        exact_message_link=bool(result_refs),
                    )
                    if result_output:
                        semantic["output"] = result_output
                if result_refs:
                    semantic["output_ref"] = result_refs[-1]
                sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": "cancelled" if status == "cancelled" else "end",
                        "name": opened.name,
                        "trace": opened.identity,
                        "native_identity": str(key),
                        "native": native,
                        "semantic": semantic,
                    }
                )
                cleanup_trace(key, agent)
                return
            if isinstance(event, (BeforeModelCallEvent, AfterModelCallEvent)):
                family, name = "model", "strands.model.call"
                if isinstance(event, BeforeModelCallEvent):
                    cycle_id = _strands_field(raw_state, "event_loop_cycle_id")
                    if cycle_id is not None:
                        model_cycle_keys[str(cycle_id)] = key
            elif isinstance(event, (BeforeToolCallEvent, AfterToolCallEvent)):
                family, name = "tool", "strands.tool.execution"
                if isinstance(event, BeforeToolCallEvent):
                    tool_call_id = _strands_tool_call_id(event)
                    proposal = {
                        "kind": "tool",
                        "phase": "event",
                        "name": "strands.tool.proposal",
                        "trace": opened.identity,
                        "native_identity": tool_call_id,
                        "coverage": (
                            _TOOL_EXECUTION_COVERAGE
                            if tool_call_id is not None
                            else _STRANDS_AGENT_COVERAGE
                        ),
                        "native": native,
                        "semantic": {
                            "type": "tool.proposal",
                            "framework": "strands",
                            **_strands_tool_semantic(event),
                        },
                    }
                    tool_use = getattr(event, "tool_use", None)
                    proposal_parent = _strands_exact_tool_proposal_parent(
                        tool_use,
                        tool_call_id,
                        tool_proposal_evidence.get(key, {}),
                    )
                    if proposal_parent is not None:
                        proposal["parent_record_id"] = proposal_parent
                    sink.record(proposal)
            elif isinstance(event, MessageAddedEvent):
                native_message = getattr(event, "message", None)
                message = _strands_message_semantic(native_message)
                if message is None:
                    return
                evidence = message_evidence.setdefault(key, {})
                known = _strands_exact_message_refs(native_message, message, evidence)
                if known is not None:
                    return
                result_refs = _strands_exact_tool_result_refs(
                    native_message,
                    tool_result_evidence.get(key, {}),
                )
                if result_refs is not None:
                    _strands_remember_message(
                        evidence,
                        native_message,
                        message,
                        result_refs,
                    )
                    return
                receipt = sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": "strands.message.added",
                        "trace": opened.identity,
                        "native": native,
                        "semantic": {
                            "type": "message",
                            "framework": "strands",
                            **message,
                        },
                    }
                )
                if receipt.accepted and receipt.record_id is not None:
                    _strands_remember_message(
                        evidence,
                        native_message,
                        message,
                        [receipt.record_id],
                    )
                return
            else:
                return
            phase = "start" if event_name.startswith("Before") else "end"
            event_error = getattr(event, "exception", None)
            if event_error is not None:
                phase = "error"
            tool_call_id = _strands_tool_call_id(event) if family == "tool" else None
            pair = (
                key,
                name,
                tool_call_id if family == "tool" else None,
            )
            if family == "tool":
                lifecycle_identity = tool_call_id
            elif phase == "start":
                sequence = model_call_sequences.get(key, 0) + 1
                model_call_sequences[key] = sequence
                lifecycle_identity = f"{key}:{name}:{sequence}"
            else:
                lifecycle_identity = start_identities.get(pair)
            model_request_semantic = (
                _strands_model_request_semantic(
                    sink,
                    opened.identity,
                    getattr(event, "agent", self.agent),
                    message_evidence.setdefault(key, {}),
                    tool_result_evidence.setdefault(key, {}),
                    system_context_evidence.setdefault(key, {}),
                )
                if family == "model" and phase == "start"
                else None
            )
            value: dict[str, Any] = {
                "kind": family,
                "phase": phase,
                "name": name,
                "trace": opened.identity,
                **(
                    {"error_identity": event_error}
                    if isinstance(event_error, BaseException)
                    else {}
                ),
                **(
                    {"native_identity": lifecycle_identity}
                    if lifecycle_identity is not None
                    else {}
                ),
                "coverage": (
                    _TOOL_EXECUTION_COVERAGE
                    if tool_call_id is not None
                    else _STRANDS_AGENT_COVERAGE
                ),
                "native": native,
                "semantic": {
                    "type": (
                        "model.request"
                        if family == "model" and phase == "start"
                        else "model.response"
                        if family == "model" and phase == "end"
                        else "model.response"
                        if family == "model" and phase == "error"
                        else "tool.result"
                        if family == "tool" and phase == "end"
                        else "tool.error"
                        if family == "tool" and phase == "error"
                        else "capture.redundant"
                        if family == "tool" and phase == "start"
                        else name.removeprefix("strands.")
                    ),
                    "framework": "strands",
                    **(
                        model_request_semantic
                        if model_request_semantic is not None
                        else _strands_model_response_semantic(native, phase)
                        if family == "model"
                        else _strands_tool_semantic(
                            event,
                            terminal=phase != "start",
                        )
                    ),
                },
            }
            effective_call: AdmissionReceipt | None = None
            if family == "tool" and phase != "start":
                effective_call = sink.record(
                    {
                        "kind": "tool",
                        "phase": "event",
                        "name": "strands.tool.execution.effective",
                        "trace": opened.identity,
                        **(
                            {"native_identity": tool_call_id}
                            if tool_call_id is not None
                            else {}
                        ),
                        "coverage": (
                            _TOOL_EXECUTION_COVERAGE
                            if tool_call_id is not None
                            else _STRANDS_AGENT_COVERAGE
                        ),
                        "native": native,
                        "semantic": {
                            "type": "tool.execution",
                            "framework": "strands",
                            **_strands_effective_tool_semantic(event),
                        },
                    }
                )
            if phase == "start":
                start_receipt = sink.record(value)
                starts[pair] = start_receipt
                if family == "model":
                    streamed_reasoning[key] = []
                if lifecycle_identity is not None:
                    start_identities[pair] = lifecycle_identity
                if (
                    family == "model"
                    and key not in request_gap_emitted
                    and start_receipt.accepted
                    and start_receipt.record_id is not None
                ):
                    sink.record(
                        {
                            "kind": "state",
                            "phase": "event",
                            "name": "strands.model.request.context.gap",
                            "trace": opened.identity,
                            "parent_record_id": start_receipt.record_id,
                            "native": {
                                "limitation": (
                                    "BeforeModelCallEvent does not expose the final "
                                    "provider request after all middleware."
                                )
                            },
                            "semantic": {
                                "type": "capture.gap",
                                "framework": "strands",
                                "reason": (
                                    "strands_post_middleware_context_unavailable"
                                ),
                                "count": 1,
                                "detail": (
                                    "Strands exposes the agent's model, system prompt, "
                                    "tool definitions, settings, and current messages "
                                    "at BeforeModelCallEvent, but not the exact provider "
                                    "request after all later middleware."
                                ),
                            },
                        }
                    )
                    request_gap_emitted.add(key)
            else:
                start = starts.pop(pair, None)
                start_identities.pop(pair, None)
                if (
                    family == "model"
                    and streamed_reasoning.get(key)
                    and "reasoning" not in value["semantic"]
                ):
                    value["semantic"]["reasoning"] = list(streamed_reasoning[key])
                if (
                    effective_call is not None
                    and effective_call.accepted
                    and effective_call.record_id is not None
                ):
                    value["parent_record_id"] = effective_call.record_id
                elif start is not None and start.accepted and start.record_id is not None:
                    value["parent_record_id"] = start.record_id
                ended = sink.record(value)
                if family == "model":
                    if phase == "error" and _is_strands_context_overflow(event_error):
                        gap: dict[str, Any] = {
                            "kind": "unknown",
                            "phase": "gap",
                            "name": "strands.context.reduction.unobserved",
                            "trace": opened.identity,
                            "native": (
                                _provider_error_native(event_error)
                                if isinstance(event_error, BaseException)
                                else {"error": _raw_native(event_error)}
                            ),
                            "semantic": {
                                "type": "capture.gap",
                                "framework": "strands",
                                "reason": "strands_context_reduction_unobserved",
                                "count": 1,
                                "detail": (
                                    "Strands may reduce context after a context-window "
                                    "overflow; built-in summarization calls its model "
                                    "outside the public hook stream."
                                ),
                            },
                        }
                        if ended.accepted and ended.record_id is not None:
                            gap["parent_record_id"] = ended.record_id
                        sink.record(gap)
                    response_message = _strands_field(
                        getattr(event, "stop_response", None),
                        "message",
                    )
                    _record_unavailable_reasoning_gap(
                        sink,
                        opened.identity,
                        framework="strands",
                        affected=ended,
                        count=_strands_unavailable_reasoning(response_message),
                        detail=(
                            "Strands exposed redacted reasoning or a reasoning "
                            "signature without readable text; opaque bytes were omitted."
                        ),
                    )
                if (
                    family == "model"
                    and phase == "end"
                    and ended.accepted
                    and ended.record_id is not None
                ):
                    response = _strands_field(
                        getattr(event, "stop_response", None),
                        "message",
                    )
                    message = _strands_message_semantic(response)
                    if message is not None:
                        _strands_remember_message(
                            message_evidence.setdefault(key, {}),
                            response,
                            message,
                            [ended.record_id],
                        )
                        _strands_remember_tool_proposals(
                            tool_proposal_evidence.setdefault(key, {}),
                            response,
                            ended.record_id,
                        )
                if (
                    family == "tool"
                    and phase == "end"
                    and ended.accepted
                    and ended.record_id is not None
                ):
                    result = getattr(event, "result", None)
                    _strands_remember_tool_result(
                        tool_result_evidence.setdefault(key, {}),
                        result,
                        tool_call_id,
                        ended.record_id,
                    )
                if phase == "error":
                    sink.record(
                        {
                            "kind": "error",
                            "phase": "event",
                            "name": f"{name}.error",
                            "trace": opened.identity,
                            "native_identity": tool_call_id,
                            **(
                                {"error_identity": event_error}
                                if isinstance(event_error, BaseException)
                                else {}
                            ),
                            "coverage": (
                                _TOOL_EXECUTION_COVERAGE
                                if tool_call_id is not None
                                else _STRANDS_AGENT_COVERAGE
                            ),
                            "native": native,
                            "semantic": {
                                "type": "error",
                                "framework": "strands",
                                "error": _semantic_error(
                                    event_error
                                ),
                            },
                        }
                    )
                    if isinstance(event, AfterModelCallEvent) and not bool(
                        getattr(event, "retry", False)
                    ):
                        error = event_error
                        explicit_turn = _explicit_turn_identity(state)
                        if explicit_turn.get("turn_id") is not None:
                            failed_turns.add(
                                (
                                    _conversation_id(state, self.agent),
                                    str(explicit_turn["turn_id"]),
                                )
                            )
                        sink.record(
                            {
                                "kind": "lifecycle",
                                "phase": "error",
                                "name": opened.name,
                                "trace": opened.identity,
                                "native_identity": str(key),
                                **(
                                    {"error_identity": error}
                                    if isinstance(error, BaseException)
                                    else {}
                                ),
                                "native": {
                                    **(
                                        _provider_error_native(error)
                                        if isinstance(error, BaseException)
                                        else {"error": _raw_native(error)}
                                    ),
                                    "event": {
                                        name: value
                                        for name, value in native.items()
                                        if name != "exception"
                                    },
                                },
                                "semantic": {
                                    "type": "agent.run",
                                    "framework": "strands",
                                    "status": "failed",
                                    "error": _semantic_error(error),
                                },
                            }
                        )
                        cleanup_trace(
                            key,
                            getattr(event, "agent", self.agent),
                        )
                if isinstance(event, AfterToolCallEvent) and phase == "end":
                    tool_call_id = _strands_tool_call_id(event)
                    sink.record(
                        {
                            "kind": "tool",
                            "phase": "event",
                            "name": "strands.tool.result",
                            "trace": opened.identity,
                            "native_identity": tool_call_id,
                            "coverage": (
                                _TOOL_EXECUTION_COVERAGE
                                if tool_call_id is not None
                                else _STRANDS_AGENT_COVERAGE
                            ),
                            "native": native,
                            "semantic": {
                                "type": "capture.redundant",
                                "framework": "strands",
                            },
                        }
                    )

        def finish_structured_invocation(
            agent: Any,
            exact_key: int | None,
            *,
            result: Any = None,
            error: BaseException | None = None,
        ) -> None:
            pending = pending_structured.pop(exact_key, None) if exact_key is not None else None
            if pending is None:
                return
            key, opened, native = pending
            cancelled = error is not None and _is_strands_cancellation(error)
            status = (
                "cancelled"
                if cancelled
                else "failed"
                if error is not None
                else "succeeded"
            )
            semantic: dict[str, Any] = {
                "type": "agent.run",
                "framework": "strands",
                "status": status,
            }
            if error is None:
                semantic["output"] = _raw_native(result)
            else:
                semantic["error"] = _semantic_error(error)
            sink.record(
                {
                    "kind": "lifecycle",
                    "phase": (
                        "cancelled"
                        if cancelled
                        else "error"
                        if error is not None
                        else "end"
                    ),
                    "name": opened.name,
                    "trace": opened.identity,
                    "native_identity": str(key),
                    **(
                        {"error_identity": error}
                        if isinstance(error, BaseException)
                        else {}
                    ),
                    "native": {
                        **native,
                        "structured_output": _raw_native(result),
                        **(
                            _provider_error_native(error)
                            if error is not None
                            else {}
                        ),
                    },
                    "semantic": semantic,
                }
            )
            cleanup_trace(key, agent)

        def finish_cancelled_stream(
            agent: Any,
            error: BaseException,
            exact_key: int | None,
        ) -> None:
            if not _is_strands_cancellation(error):
                return
            key = exact_key
            opened = traces.get(key) if key is not None else None
            if key is None or opened is None:
                return
            pending_streams.pop(key, None)
            pair = (key, "strands.model.call", None)
            start = starts.pop(pair, None)
            lifecycle_identity = start_identities.pop(pair, None)
            if start is not None:
                response: dict[str, Any] = {
                    "kind": "model",
                    "phase": "cancelled",
                    "name": "strands.model.call",
                    "trace": opened.identity,
                    **(
                        {"native_identity": lifecycle_identity}
                        if lifecycle_identity is not None
                        else {}
                    ),
                    "error_identity": error,
                    "native": _provider_error_native(error),
                    "semantic": {
                        "type": "model.response",
                        "framework": "strands",
                        "status": "cancelled",
                        **(
                            {"reasoning": list(streamed_reasoning[key])}
                            if streamed_reasoning.get(key)
                            else {}
                        ),
                        "error": _semantic_error(error),
                    },
                }
                if start.accepted and start.record_id is not None:
                    response["parent_record_id"] = start.record_id
                sink.record(response)
            sink.record(
                {
                    "kind": "lifecycle",
                    "phase": "cancelled",
                    "name": opened.name,
                    "trace": opened.identity,
                    "native_identity": str(key),
                    "error_identity": error,
                    "native": _provider_error_native(error),
                    "semantic": {
                        "type": "agent.run",
                        "framework": "strands",
                        "status": "cancelled",
                        "error": _semantic_error(error),
                    },
                }
            )
            cleanup_trace(key, agent)

        watched = [
            BeforeInvocationEvent,
            AfterInvocationEvent,
            BeforeModelCallEvent,
            AfterModelCallEvent,
            BeforeToolCallEvent,
            AfterToolCallEvent,
            MessageAddedEvent,
        ]
        try:
            for event_type in watched:
                add_hook(callback, event_type)
        except BaseException:
            active = False
            raise

        def record_callback_event(
            kwargs: dict[str, Any],
        ) -> None:
            if not active:
                return
            cycle_id = kwargs.get("event_loop_cycle_id")
            exact_key = model_cycle_keys.get(str(cycle_id)) if cycle_id is not None else None
            reasoning_text = _strands_callback_reasoning_text(kwargs)
            opened = traces.get(exact_key) if exact_key is not None else None
            if exact_key is not None and opened is not None and reasoning_text is not None:
                streamed_reasoning.setdefault(exact_key, []).append(
                    {"type": "text", "text": reasoning_text}
                )
            elif reasoning_text is not None:
                seen: set[str] = set()
                for candidate in traces.values():
                    trace_id = candidate.identity["trace_id"]
                    if trace_id in seen:
                        continue
                    seen.add(trace_id)
                    sink.record(
                        {
                            "kind": "unknown",
                            "phase": "gap",
                            "name": "strands.model.stream.reasoning.uncorrelated",
                            "trace": candidate.identity,
                            "native": {
                                "reasoning": {"type": "text", "text": reasoning_text},
                                "correlation": "invocation_identity_unavailable",
                            },
                            "semantic": {
                                "type": "capture.gap",
                                "framework": "strands",
                                "reason": (
                                    "strands_stream_reasoning_invocation_unavailable"
                                ),
                                "count": 1,
                                "detail": (
                                    "Strands callback_handler exposed readable reasoning "
                                    "without an invocation identity; the block was retained "
                                    "as uncorrelated native evidence and was not assigned "
                                    "to a model call."
                                ),
                            },
                        }
                    )
                return
            if opened is None:
                return
            event = kwargs.get("event")
            if event is None:
                return
            sink.record(
                {
                    "kind": "stream",
                    "phase": "event",
                    "name": "strands.model.stream",
                    "trace": opened.identity,
                    "native": _raw_native(kwargs),
                    "semantic": {
                        "type": "capture.redundant",
                        "framework": "strands",
                    },
                }
            )

        had_own_callback = "callback_handler" in vars(self.agent)
        own_callback = vars(self.agent).get("callback_handler")
        original_callback = getattr(self.agent, "callback_handler", None)
        callback_wrapper: Any = None
        if original_callback is None or callable(original_callback):

            def callback_wrapper(*args: Any, **kwargs: Any) -> Any:
                record_callback_event(kwargs)
                if callable(original_callback):
                    return original_callback(*args, **kwargs)
                return None

            try:
                setattr(self.agent, "callback_handler", callback_wrapper)
            except BaseException:
                active = False
                raise

        had_own_stream_async = "stream_async" in vars(self.agent)
        own_stream_async = vars(self.agent).get("stream_async")
        original_stream_async = getattr(self.agent, "stream_async", None)
        stream_async_wrapper: Any = None
        if callable(original_stream_async):

            async def stream_async_wrapper(*args: Any, **kwargs: Any) -> Any:
                invocation_state = _strands_invocation_identity_state(
                    kwargs.get("invocation_state")
                )
                turn_key = _strands_explicit_turn_key(invocation_state, self.agent)
                if turn_key is not None:
                    active_stream_turns.add(turn_key)
                try:
                    async for event in original_stream_async(*args, **kwargs):
                        yield event
                except BaseException as error:
                    exact_key = (
                        invocation_turn_keys.get(turn_key)
                        if turn_key is not None
                        else None
                    )
                    finish_cancelled_stream(self.agent, error, exact_key)
                    raise
                finally:
                    if turn_key is not None:
                        active_stream_turns.discard(turn_key)
                    exact_key = (
                        invocation_turn_keys.get(turn_key)
                        if turn_key is not None
                        else None
                    )
                    if exact_key is not None:
                        streaming_invocations.discard(exact_key)
                        pending_streams.pop(exact_key, None)

            try:
                setattr(self.agent, "stream_async", stream_async_wrapper)
            except BaseException:
                if (
                    callback_wrapper is not None
                    and getattr(self.agent, "callback_handler", None) is callback_wrapper
                ):
                    if had_own_callback:
                        setattr(self.agent, "callback_handler", own_callback)
                    else:
                        delattr(self.agent, "callback_handler")
                active = False
                raise

        had_own_structured_async = "structured_output_async" in vars(self.agent)
        own_structured_async = vars(self.agent).get("structured_output_async")
        original_structured_async = getattr(self.agent, "structured_output_async", None)
        structured_async_wrapper: Any = None
        if callable(original_structured_async):

            async def structured_async_wrapper(*args: Any, **kwargs: Any) -> Any:
                structured_token = object()
                context_token = structured_invocation_token.set(structured_token)
                try:
                    result = await original_structured_async(*args, **kwargs)
                except BaseException as error:
                    finish_structured_invocation(
                        self.agent,
                        structured_token_keys.get(structured_token),
                        error=error,
                    )
                    raise
                else:
                    finish_structured_invocation(
                        self.agent,
                        structured_token_keys.get(structured_token),
                        result=result,
                    )
                    return result
                finally:
                    structured_invocation_token.reset(context_token)
                    key = structured_token_keys.pop(structured_token, None)
                    if key is not None:
                        structured_invocations.discard(key)

            try:
                setattr(
                    self.agent,
                    "structured_output_async",
                    structured_async_wrapper,
                )
            except BaseException:
                if (
                    stream_async_wrapper is not None
                    and getattr(self.agent, "stream_async", None)
                    is stream_async_wrapper
                ):
                    if had_own_stream_async:
                        setattr(self.agent, "stream_async", own_stream_async)
                    else:
                        delattr(self.agent, "stream_async")
                if (
                    callback_wrapper is not None
                    and getattr(self.agent, "callback_handler", None)
                    is callback_wrapper
                ):
                    if had_own_callback:
                        setattr(self.agent, "callback_handler", own_callback)
                    else:
                        delattr(self.agent, "callback_handler")
                active = False
                raise

        def deactivate() -> None:
            nonlocal active
            active = False
            if (
                structured_async_wrapper is not None
                and getattr(self.agent, "structured_output_async", None)
                is structured_async_wrapper
            ):
                if had_own_structured_async:
                    setattr(self.agent, "structured_output_async", own_structured_async)
                else:
                    delattr(self.agent, "structured_output_async")
            if (
                stream_async_wrapper is not None
                and getattr(self.agent, "stream_async", None) is stream_async_wrapper
            ):
                if had_own_stream_async:
                    setattr(self.agent, "stream_async", own_stream_async)
                else:
                    delattr(self.agent, "stream_async")
            if (
                callback_wrapper is not None
                and getattr(self.agent, "callback_handler", None) is callback_wrapper
            ):
                if had_own_callback:
                    setattr(self.agent, "callback_handler", own_callback)
                else:
                    delattr(self.agent, "callback_handler")

        return _Lifecycle(deactivate)


def _strands_event_evidence(event: Any) -> dict[str, Any]:
    state = _strands_invocation_identity_state(
        _owned_field(event, "invocation_state", {})
    )
    selected_state = (
        {
            name: dict.get(state, name)
            for name in (
                "session_id",
                "conversation_id",
                "thread_id",
                "turn_id",
                "turn_index",
                "previous_turn_id",
            )
            if name in state
        }
        if isinstance(state, dict)
        else {}
    )
    event_type = type(event).__name__
    evidence: dict[str, Any] = {
        "event_type": event_type,
        "invocation_state": selected_state,
    }
    if event_type == "BeforeModelCallEvent":
        messages = _owned_field(_owned_field(event, "agent"), "messages")
        if messages is not None:
            evidence["messages"] = messages
    for name in (
        "tool_use",
        "exception",
        "message",
        "stop_response",
        "result",
        "cancel",
        "cancel_message",
        "retry",
    ):
        value = _owned_field(event, name)
        if value is not None:
            evidence[name] = value
    if event_type == "AfterToolCallEvent":
        result = _owned_field(event, "result")
        if result is not None:
            evidence["result"] = result
    exception = _owned_field(event, "exception")
    if isinstance(exception, BaseException):
        structured = _provider_error_native(exception).get("structured_error")
        if structured is not None:
            evidence["structured_error"] = structured
    return evidence


def _strands_callback_reasoning_text(kwargs: dict[str, Any]) -> str | None:
    if kwargs.get("reasoning") is not True:
        return None
    text = kwargs.get("reasoningText")
    return text if isinstance(text, str) and text else None


def _strands_explicit_turn_key(
    state: dict[str, Any] | None,
    agent: Any,
) -> tuple[str, str] | None:
    explicit = _explicit_turn_identity(state)
    turn_id = explicit.get("turn_id")
    if turn_id is None:
        return None
    return (_conversation_id(state, agent), str(turn_id))


def _is_strands_context_overflow(error: Any) -> bool:
    return isinstance(error, BaseException) and type(error).__name__ in {
        "ContextWindowOverflowException",
        "ContextWindowOverflowError",
    }


_INVOCATION_IDENTITY_FIELDS = (
    "session_id",
    "conversation_id",
    "thread_id",
    "turn_id",
    "turn_index",
    "previous_turn_id",
)


def _strands_invocation_identity_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    direct = {
        name: dict.get(value, name)
        for name in _INVOCATION_IDENTITY_FIELDS
        if name in value
    }
    nested_value = dict.get(value, "invocation_state")
    nested = (
        {
            name: dict.get(nested_value, name)
            for name in _INVOCATION_IDENTITY_FIELDS
            if name in nested_value
        }
        if isinstance(nested_value, dict)
        else {}
    )
    if any(
        name in direct and name in nested and direct[name] != nested[name]
        for name in _INVOCATION_IDENTITY_FIELDS
    ):
        return {}
    return {**nested, **direct}


def _conversation_id(state: Any, agent: object) -> str:
    if isinstance(state, dict):
        for key in ("session_id", "conversation_id", "thread_id"):
            if state.get(key) is not None:
                return str(state[key])
    return str(getattr(agent, "agent_id", "strands-default"))


def _strands_tool_call_id(event: Any) -> str | None:
    tool_use = getattr(event, "tool_use", None)
    if isinstance(tool_use, dict):
        value = tool_use.get("toolUseId")
        return value if isinstance(value, str) and value.strip() else None
    return None


def _strands_tool_semantic(event: Any, *, terminal: bool = False) -> dict[str, Any]:
    tool_use = getattr(event, "tool_use", None)
    if not isinstance(tool_use, dict):
        return {}
    name = tool_use.get("name")
    call_id = _strands_tool_call_id(event)
    if not isinstance(name, str) or not name.strip() or call_id is None:
        return {}
    semantic: dict[str, Any] = {
        "name": name,
        "native_call_id": call_id,
    }
    if not terminal:
        semantic["input"] = _raw_native(tool_use.get("input"))
        return semantic
    error = getattr(event, "exception", None)
    if error is not None:
        semantic.update(
            {
                "status": "failed",
                "error": _semantic_error(error),
            }
        )
        return semantic
    if getattr(event, "cancel_message", None) is not None:
        semantic.update(
            {
                "status": "cancelled",
                "output": _raw_native(getattr(event, "result", None)),
            }
        )
        return semantic
    result = getattr(event, "result", None)
    if _strands_tool_result_failed(result):
        semantic.update(
            {
                "status": "failed",
                "output": _raw_native(result),
                "error": {
                    "type": "strands.tool_error",
                    "message": "Strands reported a failed tool result.",
                    "recoverable": False,
                },
            }
        )
        return semantic
    semantic.update(
        {
            "status": "succeeded",
            "output": _raw_native(result),
        }
    )
    return semantic


def _strands_tool_result_failed(result: Any) -> bool:
    if not isinstance(result, dict):
        return False
    return result.get("status") == "error" or result.get("isError") is True


def _strands_effective_tool_semantic(event: Any) -> dict[str, Any]:
    """Project the tool and arguments Strands reports after actual execution."""

    semantic = _strands_tool_semantic(event)
    selected_tool = getattr(event, "selected_tool", None)
    try:
        selected_name = getattr(selected_tool, "tool_name", None)
    except BaseException:
        selected_name = None
    if isinstance(selected_name, str) and selected_name.strip():
        semantic["name"] = selected_name
    return semantic


def _strands_message_semantic(message: Any) -> dict[str, Any] | None:
    if not isinstance(message, dict):
        return None
    role = message.get("role")
    if role not in {"system", "developer", "user", "assistant", "tool"}:
        return None
    if "content" not in message:
        return None
    return {
        "role": role,
        "content": _raw_native(message["content"]),
    }


_STRANDS_UNSAFE_IDENTITY_SNAPSHOT = object()


def _strands_identity_snapshot(value: Any) -> object:
    if value is None or type(value) in {bool, int, float, str}:
        return ("value", value)
    if type(value) is list:
        children = [_strands_identity_snapshot(item) for item in list.__iter__(value)]
        if any(child is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT for child in children):
            return _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
        return ("list", tuple(children))
    if type(value) is dict:
        items: list[tuple[str, object]] = []
        for key, item in dict.items(value):
            if type(key) is not str:
                return _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
            child = _strands_identity_snapshot(item)
            if child is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT:
                return _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
            items.append((key, child))
        return ("dict", tuple(items))
    return _STRANDS_UNSAFE_IDENTITY_SNAPSHOT


def _strands_message_snapshot(message: dict[str, Any]) -> object:
    return _strands_identity_snapshot(
        {
            "role": message["role"],
            "content": message["content"],
        }
    )


def _strands_remember_message(
    evidence: dict[int, tuple[object, str, object, list[str]]],
    native_message: Any,
    message: dict[str, Any],
    record_ids: list[str],
) -> None:
    snapshot = _strands_message_snapshot(message)
    if (
        type(native_message) is not dict
        or snapshot is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
        or not record_ids
    ):
        return
    evidence[id(native_message)] = (
        native_message,
        str(message["role"]),
        snapshot,
        list(record_ids),
    )


def _strands_exact_message_refs(
    native_message: Any,
    message: dict[str, Any],
    evidence: dict[int, tuple[object, str, object, list[str]]],
) -> list[str] | None:
    known = evidence.get(id(native_message))
    if known is None or known[0] is not native_message or known[1] != message["role"]:
        return None
    snapshot = _strands_message_snapshot(message)
    if (
        snapshot is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
        or snapshot != known[2]
    ):
        return None
    return list(known[3])


def _strands_model_context_refs(
    sink: Any,
    trace: dict[str, str],
    agent: Any,
    evidence: dict[int, tuple[object, str, object, list[str]]],
    tool_results: dict[int, tuple[object, str, object, str]],
    system_evidence: dict[object, str] | None = None,
) -> list[str]:
    refs = _strands_system_context_refs(
        sink,
        trace,
        agent,
        system_evidence if system_evidence is not None else {},
    )
    messages = _owned_field(agent, "messages")
    if type(messages) is not list:
        return refs
    for native_message in list.__iter__(messages):
        message = _strands_message_semantic(native_message)
        if message is None:
            continue
        known = _strands_exact_message_refs(native_message, message, evidence)
        if known is not None:
            _strands_append_unique(refs, known)
            continue
        result_refs = _strands_exact_tool_result_refs(native_message, tool_results)
        if result_refs is not None:
            _strands_append_unique(refs, result_refs)
            _strands_remember_message(
                evidence,
                native_message,
                message,
                result_refs,
            )
            continue
        tracking_id = (
            dict.get(native_message, "trackingId")
            if type(native_message) is dict
            else None
        )
        receipt = sink.record(
            {
                "kind": "state",
                "phase": "event",
                "name": "strands.model.context.message",
                "trace": trace,
                **(
                    {"native_identity": tracking_id}
                    if isinstance(tracking_id, str) and tracking_id.strip()
                    else {}
                ),
                "native": {"message": _raw_native(native_message)},
                "semantic": {
                    "type": "message",
                    "framework": "strands",
                    "origin": "context",
                    **message,
                },
            }
        )
        if receipt.accepted and receipt.record_id is not None:
            refs.append(receipt.record_id)
            _strands_remember_message(
                evidence,
                native_message,
                message,
                [receipt.record_id],
            )
    return refs


def _strands_model_request_semantic(
    sink: Any,
    trace: dict[str, str],
    agent: Any,
    evidence: dict[int, tuple[object, str, object, list[str]]],
    tool_results: dict[int, tuple[object, str, object, str]],
    system_evidence: dict[object, str],
) -> dict[str, Any]:
    model = _owned_field(agent, "model")
    raw_settings = _strands_snapshot_call(model, "get_config")
    settings = raw_settings if type(raw_settings) is dict else {}
    registry = _owned_field(agent, "tool_registry")
    raw_tool_definitions = _strands_snapshot_call(
        registry,
        "get_all_tool_specs",
    )
    tool_definitions = (
        raw_tool_definitions
        if type(raw_tool_definitions) is list
        and all(type(item) is dict for item in raw_tool_definitions)
        else []
    )
    model_name = next(
        (
            settings[name]
            for name in ("model_id", "model", "model_name")
            if isinstance(settings.get(name), str) and settings[name].strip()
        ),
        None,
    )
    tool_names = [
        item["name"]
        for item in tool_definitions
        if isinstance(item.get("name"), str) and item["name"].strip()
    ]
    return {
        "context_refs": _strands_model_context_refs(
            sink,
            trace,
            agent,
            evidence,
            tool_results,
            system_evidence,
        ),
        **({"model": model_name} if model_name is not None else {}),
        **({"tools": tool_names} if tool_names else {}),
        **(
            {"tool_definitions": _raw_native(tool_definitions)}
            if tool_definitions
            else {}
        ),
        **({"settings": _raw_native(settings)} if settings else {}),
    }


def _strands_system_context_refs(
    sink: Any,
    trace: dict[str, str],
    agent: Any,
    evidence: dict[object, str],
) -> list[str]:
    try:
        content = getattr(agent, "system_prompt_content")
    except BaseException:
        content = None
    if content is None:
        return []
    snapshot = _strands_identity_snapshot(content)
    if snapshot is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT:
        return []
    known = evidence.get(snapshot)
    if known is not None:
        return [known]
    receipt = sink.record(
        {
            "kind": "state",
            "phase": "event",
            "name": "strands.model.context.system",
            "trace": trace,
            "native": {"system_prompt": _raw_native(content)},
            "semantic": {
                "type": "message",
                "framework": "strands",
                "origin": "context",
                "role": "system",
                "content": _raw_native(content),
            },
        }
    )
    if not receipt.accepted or receipt.record_id is None:
        return []
    evidence[snapshot] = receipt.record_id
    return [receipt.record_id]


def _strands_snapshot_call(value: Any, name: str) -> Any:
    """Call a public snapshot method without letting observation alter the run."""

    try:
        method = getattr(value, name, None)
        return method() if callable(method) else None
    except BaseException:
        return None


def _strands_remember_tool_result(
    evidence: dict[int, tuple[object, str, object, str]],
    result: Any,
    call_id: str | None,
    record_id: str,
) -> None:
    snapshot = _strands_identity_snapshot(result)
    if (
        type(result) is not dict
        or call_id is None
        or snapshot is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
    ):
        return
    evidence[id(result)] = (result, call_id, snapshot, record_id)


def _strands_exact_tool_result_refs(
    message: Any,
    evidence: dict[int, tuple[object, str, object, str]],
) -> list[str] | None:
    if type(message) is not dict:
        return None
    content = dict.get(message, "content")
    if type(content) is not list or not content:
        return None
    refs: list[str] = []
    for block in list.__iter__(content):
        if type(block) is not dict:
            return None
        result = dict.get(block, "toolResult")
        if type(result) is not dict:
            return None
        known = evidence.get(id(result))
        call_id = dict.get(result, "toolUseId")
        snapshot = _strands_identity_snapshot(result)
        if (
            known is None
            or known[0] is not result
            or call_id != known[1]
            or snapshot is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
            or snapshot != known[2]
        ):
            return None
        _strands_append_unique(refs, [known[3]])
    return refs


def _strands_remember_tool_proposals(
    evidence: dict[int, tuple[object, str, object, str]],
    message: Any,
    response_record_id: str,
) -> None:
    if type(message) is not dict:
        return
    content = dict.get(message, "content")
    if type(content) is not list:
        return
    for block in list.__iter__(content):
        if type(block) is not dict:
            continue
        tool_use = dict.get(block, "toolUse")
        if type(tool_use) is not dict:
            continue
        call_id = dict.get(tool_use, "toolUseId")
        snapshot = _strands_identity_snapshot(tool_use)
        if (
            not isinstance(call_id, str)
            or not call_id.strip()
            or snapshot is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
        ):
            continue
        evidence[id(tool_use)] = (
            tool_use,
            call_id,
            snapshot,
            response_record_id,
        )


def _strands_exact_tool_proposal_parent(
    tool_use: Any,
    call_id: str | None,
    evidence: dict[int, tuple[object, str, object, str]],
) -> str | None:
    known = evidence.get(id(tool_use))
    snapshot = _strands_identity_snapshot(tool_use)
    if (
        known is None
        or known[0] is not tool_use
        or call_id != known[1]
        or snapshot is _STRANDS_UNSAFE_IDENTITY_SNAPSHOT
        or snapshot != known[2]
    ):
        return None
    return known[3]


def _strands_append_unique(target: list[str], values: list[str]) -> None:
    for value in values:
        if value not in target:
            target.append(value)


def _strands_message_is_final(message: Any) -> bool:
    if not isinstance(message, dict) or message.get("role") != "assistant":
        return False
    content = message.get("content")
    return (
        isinstance(content, list)
        and bool(content)
        and not any(
            isinstance(block, dict) and "toolUse" in block
            for block in content
        )
    )


def _strands_model_response_semantic(
    native: dict[str, Any], phase: str
) -> dict[str, Any]:
    response = native.get("stop_response")
    stop_reason = _strands_field(response, "stop_reason")
    message = _strands_field(response, "message")
    metadata = _strands_field(message, "metadata")
    raw_usage = _strands_field(metadata, "usage")
    usage = (
        {
            "input_tokens": _strands_field(raw_usage, "inputTokens"),
            "output_tokens": _strands_field(raw_usage, "outputTokens"),
        }
        if raw_usage is not None
        else None
    )
    semantic: dict[str, Any] = {
        "status": "failed" if phase == "error" else "completed",
    }
    content = _strands_field(message, "content")
    if content is not None:
        semantic["content"] = _raw_native(_strands_visible_content(content))
    reasoning = _strands_reasoning(message)
    if reasoning:
        semantic["reasoning"] = reasoning
    if isinstance(stop_reason, str) and stop_reason:
        semantic["finish_reason"] = stop_reason
    if usage is not None:
        semantic["usage"] = usage
    error = native.get("exception")
    if phase == "error" and isinstance(error, BaseException):
        semantic["error"] = _semantic_error(error)
    return semantic


def _strands_visible_content(content: Any) -> Any:
    if not isinstance(content, (list, tuple)):
        return content
    return [
        block
        for block in content
        if _strands_field(block, "reasoningContent") is None
    ]


def _strands_reasoning(message: Any) -> list[dict[str, str]]:
    """Project readable Strands reasoning blocks without retaining signatures."""

    content = _strands_field(message, "content")
    if not isinstance(content, (list, tuple)):
        return []
    blocks: list[dict[str, str]] = []
    for block in content:
        reasoning = _strands_field(block, "reasoningContent")
        reasoning_text = _strands_field(reasoning, "reasoningText")
        text = _strands_field(reasoning_text, "text")
        if isinstance(text, str) and text:
            blocks.append({"type": "text", "text": text})
    return blocks


def _strands_unavailable_reasoning(message: Any) -> int:
    content = _strands_field(message, "content")
    if not isinstance(content, (list, tuple)):
        return 0
    count = 0
    for block in content:
        reasoning = _strands_field(block, "reasoningContent")
        redacted = _strands_field(reasoning, "redactedContent")
        if isinstance(redacted, (str, bytes)) and redacted:
            count += 1
            continue
        reasoning_text = _strands_field(reasoning, "reasoningText")
        text = _strands_field(reasoning_text, "text")
        signature = _strands_field(reasoning_text, "signature")
        if (
            not (isinstance(text, str) and text)
            and isinstance(signature, (str, bytes))
            and signature
        ):
            count += 1
    return count


def _strands_agent_result_output(
    result: Any,
    *,
    exact_message_link: bool = False,
) -> dict[str, Any]:
    """Retain only AgentResult evidence not already represented by an exact link."""

    output: dict[str, Any] = {}
    names = (
        ("structured_output", "interrupts", "checkpoint")
        if exact_message_link
        else (
            "stop_reason",
            "message",
            "metrics",
            "state",
            "interrupts",
            "structured_output",
            "checkpoint",
        )
    )
    for name in names:
        value = _strands_field(result, name)
        if value is not None or (
            not exact_message_link and name in {"message", "state"}
        ):
            output[name] = _raw_native(value)
    return output


def _strands_field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return _owned_field(value, name)


def _semantic_error(error: Any) -> dict[str, Any]:
    if not isinstance(error, BaseException):
        return {
            "type": "strands.error",
            "message": "framework error details unavailable",
            "recoverable": False,
        }
    args = object.__getattribute__(error, "args")
    message = (
        args[0]
        if args and isinstance(args[0], str) and args[0]
        else "Strands callback failed"
    )
    return {
        "type": "strands.error",
        "message": message,
        "recoverable": False,
    }


def _is_strands_cancellation(error: BaseException) -> bool:
    return isinstance(
        error,
        (GeneratorExit, asyncio.CancelledError, FutureCancelledError),
    )
