"""Official LlamaIndex callback and instrumentation-event capture adapter."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import secrets
import threading
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any

from ._adapter_native import native_field, native_own_data, native_snapshot
from ._framework_adapter_shared import (
    _installed_version,
    _record_unavailable_reasoning_gap,
    _source_qualification,
)
from .capture_v1 import (
    AdmissionReceipt,
    CaptureSource,
    _trust_official_source,
    is_unsafe_accessor_omission,
)

_MAX_WORKFLOW_STREAM_PARTIAL_BYTES = 256 * 1024
_MAX_PLAIN_JSON_BYTES = 256 * 1024
_MAX_PLAIN_JSON_NODES = 4096
_MAX_PLAIN_JSON_WIDTH = 128
_MAX_PENDING_CONTEXT_MESSAGES = 128
_CONTEXT_DIGEST_DOMAIN = b"semantic-layer.llamaindex.context.v1"


@dataclass
class _Trace:
    identity: dict[str, str]
    turn_id: str
    conversation: str
    trace_id: str
    root: AdmissionReceipt
    failed: bool = False
    failed_error: Any = None
    failed_error_identity: BaseException | None = None
    terminal_status: str | None = None
    end_requested: bool = False
    trace_map: Any = None
    events: dict[str, AdmissionReceipt] = field(default_factory=dict)
    tool_proposals: dict[str, AdmissionReceipt] = field(default_factory=dict)
    workflow_tools: dict[str, AdmissionReceipt] = field(default_factory=dict)
    pending_workflow_tools: dict[str, dict[str, Any]] = field(default_factory=dict)
    completed_workflow_tools: set[str] = field(default_factory=set)
    workflow_events: int = 0
    user_inputs: int = 0
    workflow_started: bool = False
    workflow_stream_terminal: bool = False
    tool_correlation_loss: bool = False
    model_responses_observed: int = 0
    latest_model_output: _ModelOutput | None = None
    workflow_output: Any = None
    workflow_output_observed: bool = False
    workflow_output_ref: AdmissionReceipt | None = None
    workflow_output_correlation_loss: bool = False
    workflow_output_provisional: bool = False
    streamed_agent_output: Any = None
    workflow_stream_content: str | None = None
    workflow_stream_bytes: int = 0
    workflow_stream_truncated: bool = False
    model_context_length: int = 0
    model_context_digest: str | None = None
    model_context_request_ref: str | None = None
    active_model_requests: set[str] = field(default_factory=set)
    workflow_reasoning: list[_WorkflowReasoning] = field(default_factory=list)
    pending_context_messages: list[tuple[tuple[str, str], str, int]] = field(
        default_factory=list
    )


@dataclass
class _ModelOutput:
    response: Any
    message: Any
    receipt: AdmissionReceipt


@dataclass
class _WorkflowReasoning:
    native: Any
    parts: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _ContextMessage:
    native: Any
    semantic: dict[str, Any]
    pending_key: tuple[str, str] | None


@dataclass(frozen=True)
class _ContextPlan:
    messages: list[_ContextMessage]
    digest: str | None
    selected_from: int
    context_base_ref: str | None
    complete: bool


@dataclass
class _PlainJsonBudget:
    bytes_left: int
    nodes_left: int
    exhausted: bool = False

    def take(self, *, byte_count: int = 0, node_count: int = 0) -> bool:
        if byte_count > self.bytes_left or node_count > self.nodes_left:
            self.exhausted = True
            return False
        self.bytes_left -= byte_count
        self.nodes_left -= node_count
        return True


def _utf8_character_width(character: str) -> int | None:
    codepoint = ord(character)
    if codepoint < 0x80:
        return 1
    if codepoint < 0x800:
        return 2
    if 0xD800 <= codepoint <= 0xDFFF:
        return None
    if codepoint < 0x10000:
        return 3
    return 4


def _bounded_utf8_size(value: str, byte_limit: int) -> int | None:
    character_count = len(value)
    if character_count > byte_limit:
        return None
    byte_count = 0
    for character in value:
        width = _utf8_character_width(character)
        if width is None:
            return None
        byte_count += width
        if byte_count > byte_limit:
            return None
    return byte_count


def _bounded_utf8_prefix(value: str, byte_limit: int) -> tuple[str, int, bool]:
    exact_size = _bounded_utf8_size(value, byte_limit)
    if exact_size is not None:
        return value, exact_size, False
    byte_count = 0
    end = 0
    for index, character in enumerate(value):
        width = _utf8_character_width(character)
        if width is None or byte_count + width > byte_limit:
            break
        byte_count += width
        end = index + 1
    return value[:end], byte_count, True


@dataclass
class _DispatcherSpan:
    trace: _Trace
    span_id: str
    model_event_id: str | None = None
    model: AdmissionReceipt | None = None
    saw_delta: bool = False


@dataclass
class _FunctionToolSpan:
    trace: _Trace
    span_id: str
    call_id: str
    name: str
    input: Any
    workflow_identity: bool


def _user_turn_native_identity(key: bytes, conversation: str, turn_id: str, turn_index: int) -> str:
    """Return a per-runtime unlinkable identity without embedding caller-owned IDs."""

    digest = hmac.new(key, digestmod=hashlib.sha256)
    for value in ("llamaindex-user-turn-v1", conversation, turn_id, str(turn_index)):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return f"user-turn::{digest.hexdigest()}"


class _Lifecycle:
    def __init__(self, source: _Source, deactivate: Any) -> None:
        self._source = source
        self._deactivate = deactivate

    def deactivate(self) -> None:
        self._deactivate()

    def drain(self) -> None:
        return None

    def turn(
        self,
        *,
        conversation_id: str,
        turn_id: str,
        turn_index: int,
        previous_turn_id: str | None = None,
    ) -> _UserTurnContext:
        """Bind one official ``agent.run`` call to one explicit user turn."""

        return _UserTurnContext(
            self._source,
            conversation_id=conversation_id,
            turn_id=turn_id,
            turn_index=turn_index,
            previous_turn_id=previous_turn_id,
        )


class _ObservedEventStream:
    def __init__(self, source: _Source, trace: _Trace, stream: Any) -> None:
        self._source = source
        self._trace = trace
        self._stream = stream.__aiter__()
        self._terminal = False

    def __aiter__(self) -> _ObservedEventStream:
        return self

    async def __anext__(self) -> Any:
        try:
            event = await self._stream.__anext__()
        except StopAsyncIteration:
            self._finish("completed")
            raise
        except BaseException as error:
            self._finish("cancelled" if _is_cancellation(error) else "error", error)
            raise
        self._source._workflow_event(self._trace, event)
        return event

    async def aclose(self) -> Any:
        close = getattr(self._stream, "aclose", None)
        try:
            result = await close() if callable(close) else None
        except BaseException as error:
            self._finish("cancelled" if _is_cancellation(error) else "error", error)
            raise
        else:
            self._finish("cancelled")
            return result

    async def athrow(self, *args: Any) -> Any:
        throw = getattr(self._stream, "athrow", None)
        if not callable(throw):
            raise TypeError("underlying workflow event stream does not support athrow")
        try:
            event = await throw(*args)
        except StopAsyncIteration:
            self._finish("completed")
            raise
        except BaseException as error:
            self._finish("cancelled" if _is_cancellation(error) else "error", error)
            raise
        self._source._workflow_event(self._trace, event)
        return event

    def _finish(self, status: str, error: BaseException | None = None) -> None:
        if self._terminal:
            return
        self._terminal = True
        self._source._workflow_stream_terminal(self._trace, status, error)


class _ObservedWorkflowHandler:
    def __init__(self, source: _Source, trace: _Trace, handler: Any) -> None:
        self._source = source
        self._trace = trace
        self._handler = handler

    def stream_events(self, *args: Any, **kwargs: Any) -> _ObservedEventStream:
        stream = self._handler.stream_events(*args, **kwargs)
        return _ObservedEventStream(self._source, self._trace, stream)

    def __await__(self) -> Any:
        return self._observe_await().__await__()

    async def _observe_await(self) -> Any:
        try:
            result = await self._handler
        except BaseException as error:
            status = "cancelled" if _is_cancellation(error) else "error"
            self._source._workflow_stream_terminal(self._trace, status, error)
            raise
        self._source._workflow_result(self._trace, result)
        self._source._workflow_stream_terminal(self._trace, "completed", None)
        return result

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handler, name)


class _UserTurn:
    def __init__(self, source: _Source, trace: _Trace) -> None:
        self._source = source
        self._trace = trace

    def run(self, agent: Any, /, **kwargs: Any) -> _ObservedWorkflowHandler:
        run = getattr(agent, "run", None)
        if not callable(run):
            raise TypeError("LlamaIndex user-turn subject requires agent.run")
        self._source._workflow_user_input(self._trace, kwargs)
        handler = run(**kwargs)
        self._source._workflow_handler_started(self._trace)
        return _ObservedWorkflowHandler(self._source, self._trace, handler)

    def record_state(self, state: Any) -> AdmissionReceipt:
        return self._source._workflow_state(self._trace, state)


class _UserTurnContext:
    def __init__(
        self,
        source: _Source,
        *,
        conversation_id: str,
        turn_id: str,
        turn_index: int,
        previous_turn_id: str | None,
    ) -> None:
        self._source = source
        self._values = (conversation_id, turn_id, turn_index, previous_turn_id)
        self._trace: _Trace | None = None
        self._token: Token[_Trace | None] | None = None

    async def __aenter__(self) -> _UserTurn:
        self._trace, self._token = self._source._begin_user_turn(*self._values)
        return _UserTurn(self._source, self._trace)

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: Any,
    ) -> bool:
        del kind, traceback
        if self._trace is not None and self._token is not None:
            self._source._finish_user_turn(self._trace, self._token, error)
        return False


class _Adapter:
    def __init__(self, version: str) -> None:
        self.version = version

    def create_source(self, client: object) -> _Source:
        return _trust_official_source(
            _Source(_callback_manager(client), self.version), "deep"
        )  # type: ignore[return-value]


class _Source(CaptureSource):
    def __init__(self, manager: object, version: str) -> None:
        self.manager = manager
        self.metadata = {
            "name": "official:llamaindex",
            "seam": (
                "CallbackManager/BaseCallbackHandler + "
                "root_dispatcher/BaseEventHandler/BaseSpanHandler + "
                "WorkflowHandler.stream_events proxy"
            ),
            "identity_domain": "llamaindex.event",
            "version": version,
            "qualification": _source_qualification(
                version,
                exact_versions=frozenset({"0.14.23"}),
                profile="llamaindex-python-adapter-v1",
            ),
            "official": True,
            "coverage": [
                {"operation": "callback-event", "domain": "llamaindex.event", "role": "owner"}
            ],
        }
        self._sink: Any = None
        self._active = False
        self._guard = threading.RLock()
        self._traces: dict[str, _Trace] = {}
        self._event_traces: dict[str, _Trace] = {}
        self._trace_aliases: dict[str, _Trace] = {}
        self._dispatcher_spans: dict[str, _DispatcherSpan] = {}
        self._function_tool_spans: dict[str, _FunctionToolSpan] = {}
        self._workflow_tool_steps: dict[str, tuple[_Trace, str]] = {}
        self._current_trace: ContextVar[str | None] = ContextVar(
            f"semantic_layer_llama_trace_{id(self)}", default=None
        )
        self._bound_turn: ContextVar[_Trace | None] = ContextVar(
            f"semantic_layer_llama_user_turn_{id(self)}", default=None
        )
        self._last_turn: dict[str, str] = {}
        self._failed_turns: set[str] = set()
        self._turn_order: dict[str, int] = {}
        self._identity_key = secrets.token_bytes(32)

    def install(self, sink: Any) -> _Lifecycle:
        if self._active:
            raise RuntimeError("LlamaIndex source is already installed")
        from llama_index.core.agent.workflow import (  # type: ignore[import-not-found,unused-ignore]
            ToolCall as WorkflowToolCall,
        )
        from llama_index.core.callbacks.base_handler import (  # type: ignore[import-not-found,unused-ignore]
            BaseCallbackHandler,
        )
        from llama_index.core.instrumentation import (  # type: ignore[attr-defined,import-not-found,unused-ignore]
            root_dispatcher,
        )
        from llama_index.core.instrumentation.event_handlers import (  # type: ignore[import-not-found,unused-ignore]
            BaseEventHandler,
        )
        from llama_index.core.instrumentation.span import (  # type: ignore[import-not-found,unused-ignore]
            SimpleSpan,
        )
        from llama_index.core.instrumentation.span_handlers import (  # type: ignore[import-not-found,unused-ignore]
            BaseSpanHandler,
        )
        from llama_index.core.tools import (  # type: ignore[import-not-found,unused-ignore]
            FunctionTool,
        )

        source = self

        class Handler(BaseCallbackHandler):  # type: ignore[misc,unused-ignore]
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
                source._event_start(event_type, payload, event_id, parent_id, kwargs)
                return event_id

            def on_event_end(
                self,
                event_type: Any,
                payload: Any = None,
                event_id: str = "",
                **kwargs: Any,
            ) -> None:
                source._event_end(event_type, payload, event_id, kwargs)

            def start_trace(self, trace_id: str | None = None) -> None:
                source._start_trace(trace_id or "llama-index")

            def end_trace(self, trace_id: str | None = None, trace_map: Any = None) -> None:
                source._end_trace(trace_id or "llama-index", trace_map)

        class InstrumentationHandler(BaseEventHandler):  # type: ignore[misc,unused-ignore]
            def handle(self, event: Any, **kwargs: Any) -> Any:
                source._dispatcher_event(event, kwargs)
                return event

        class InstrumentationSpanHandler(
            BaseSpanHandler[SimpleSpan]  # type: ignore[misc,unused-ignore]
        ):
            def new_span(
                self,
                id_: str,
                bound_args: Any,
                instance: Any = None,
                parent_span_id: str | None = None,
                tags: Any = None,
                **kwargs: Any,
            ) -> SimpleSpan | None:
                del tags, kwargs
                source._workflow_tool_step_start(
                    id_,
                    bound_args,
                    WorkflowToolCall,
                )
                if not source._function_tool_span_start(
                    id_,
                    bound_args,
                    instance,
                    parent_span_id,
                    FunctionTool,
                ):
                    return None
                return SimpleSpan(id_=id_, parent_id=parent_span_id)

            def prepare_to_exit_span(
                self,
                id_: str,
                bound_args: Any,
                instance: Any = None,
                result: Any = None,
                **kwargs: Any,
            ) -> SimpleSpan | None:
                del bound_args, instance, kwargs
                source._function_tool_span_end(id_, result)
                source._workflow_tool_step_end(id_)
                return self.open_spans.get(id_)

            def prepare_to_drop_span(
                self,
                id_: str,
                bound_args: Any,
                instance: Any = None,
                err: BaseException | None = None,
                **kwargs: Any,
            ) -> SimpleSpan | None:
                del bound_args, instance, kwargs
                source._function_tool_span_error(id_, err)
                source._workflow_tool_step_end(id_)
                return self.open_spans.get(id_)

        handler = Handler()
        instrumentation_handler = InstrumentationHandler()
        instrumentation_span_handler = InstrumentationSpanHandler()
        add = getattr(self.manager, "add_handler", None)
        remove = getattr(self.manager, "remove_handler", None)
        if not callable(add) or not callable(remove):
            raise TypeError("LlamaIndex CallbackManager requires add_handler/remove_handler")
        prior_handlers = list(getattr(self.manager, "handlers", []))
        self._sink = sink
        self._active = True
        try:
            add(handler)
            root_dispatcher.add_event_handler(instrumentation_handler)
            root_dispatcher.add_span_handler(instrumentation_span_handler)
        except BaseException:
            self._active = False
            set_handlers = getattr(self.manager, "set_handlers", None)
            if callable(set_handlers):
                set_handlers(prior_handlers)
            else:
                handlers = getattr(self.manager, "handlers", None)
                if isinstance(handlers, list):
                    handlers[:] = prior_handlers
            _discard_identity(root_dispatcher.event_handlers, instrumentation_handler)
            _discard_identity(root_dispatcher.span_handlers, instrumentation_span_handler)
            raise

        def deactivate() -> None:
            with self._guard:
                self._active = False
                try:
                    handlers = getattr(self.manager, "handlers", [])
                    if any(item is handler for item in handlers):
                        remove(handler)
                finally:
                    _discard_identity(root_dispatcher.event_handlers, instrumentation_handler)
                    _discard_identity(root_dispatcher.span_handlers, instrumentation_span_handler)

        return _Lifecycle(self, deactivate)

    def _begin_user_turn(
        self,
        conversation: str,
        turn_id: str,
        turn_index: int,
        previous_turn_id: str | None,
    ) -> tuple[_Trace, Token[_Trace | None]]:
        with self._guard:
            if not self._active:
                raise RuntimeError("LlamaIndex source is not active")
            if self._bound_turn.get() is not None:
                raise RuntimeError("LlamaIndex user turns cannot be nested")
            if (
                type(conversation) is not str
                or not conversation
                or type(turn_id) is not str
                or not turn_id
                or type(turn_index) is not int
                or turn_index < 0
                or (previous_turn_id is not None and type(previous_turn_id) is not str)
            ):
                raise ValueError("LlamaIndex user turn identity is invalid")
            expected_index = self._turn_order.get(conversation, 0)
            expected_previous = self._last_turn.get(conversation)
            if turn_index != expected_index or previous_turn_id != expected_previous:
                raise ValueError("LlamaIndex user turn continuity does not match prior turns")
            native_identity = _user_turn_native_identity(
                self._identity_key, conversation, turn_id, turn_index
            )
            opened = self._sink.open_trace(
                {
                    "name": "llamaindex.trace",
                    "native_identity": native_identity,
                    "native": {"user_turn": True, "turn_index": turn_index},
                    "semantic": {
                        "type": "workflow.run",
                        "framework": "llamaindex",
                        "name": "llamaindex.user_turn",
                    },
                    "conversation_id": conversation,
                    "turn_id": turn_id,
                    "turn_index": turn_index,
                    **(
                        {"previous_turn_id": previous_turn_id}
                        if previous_turn_id is not None
                        else {}
                    ),
                }
            )
            if not opened.accepted or opened.identity is None:
                raise RuntimeError("LlamaIndex user turn was rejected by capture")
            trace = _Trace(opened.identity, turn_id, conversation, native_identity, opened)
            self._traces[native_identity] = trace
            self._turn_order[conversation] = turn_index + 1
            self._current_trace.set(native_identity)
            token = self._bound_turn.set(trace)
            self._sink.record(
                {
                    "kind": "state",
                    "phase": "start",
                    "name": "llamaindex.user_turn",
                    "trace": trace.identity,
                    "native_identity": native_identity,
                    "native": {"state": "running", "user_turn": True},
                    "semantic": {
                        "type": "state.transition",
                        "framework": "llamaindex",
                        "state_type": "state.turn_started",
                        "value": {"turn_index": turn_index},
                    },
                }
            )
            if expected_previous in self._failed_turns:
                self._sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": "llamaindex.recovery",
                        "trace": trace.identity,
                        "native_identity": native_identity,
                        "native": {
                            "attempt": 2,
                            "previous_turn_failed": True,
                            "previous_turn_index": turn_index - 1,
                        },
                        "semantic": {
                            "type": "state.transition",
                            "framework": "llamaindex",
                            "state_type": "recovery.retry",
                            "value": {
                                "previous_turn_failed": True,
                                "previous_turn_index": turn_index - 1,
                            },
                        },
                    }
                )
            return trace, token

    def _finish_user_turn(
        self,
        trace: _Trace,
        token: Token[_Trace | None],
        error: BaseException | None,
    ) -> None:
        with self._guard:
            self._bound_turn.reset(token)
            if not trace.workflow_stream_terminal:
                status = (
                    "cancelled"
                    if _is_cancellation(error)
                    else "error"
                    if error is not None
                    else "unobserved"
                    if trace.workflow_started
                    else "completed"
                )
                self._workflow_stream_terminal(trace, status, error)
            if error is not None:
                trace.failed = True
                trace.failed_error = error
                trace.failed_error_identity = error
            trace.end_requested = True
            if self._current_trace.get() == trace.trace_id:
                self._current_trace.set(None)
            self._maybe_close_trace(trace)

    def _start_trace(self, trace_id: str) -> None:
        with self._guard:
            if not self._active:
                return
            bound = self._bound_turn.get()
            if bound is not None:
                self._trace_aliases[trace_id] = bound
                self._current_trace.set(bound.trace_id)
                return
            conversation, turn_id = _trace_parts(trace_id)
            previous = self._last_turn.get(conversation)
            turn_index = self._turn_order.get(conversation, 0)
            opened = self._sink.open_trace(
                {
                    "name": "llamaindex.trace",
                    "native_identity": trace_id,
                    "native": {"trace_id": trace_id},
                    "semantic": {
                        "type": "workflow.run",
                        "framework": "llamaindex",
                        "name": "llamaindex.trace",
                    },
                    "conversation_id": conversation,
                    "turn_id": turn_id,
                    "turn_index": turn_index,
                    **({"previous_turn_id": previous} if previous else {}),
                }
            )
            if not opened.accepted or opened.identity is None:
                return
            self._turn_order[conversation] = turn_index + 1
            trace = _Trace(opened.identity, turn_id, conversation, trace_id, opened)
            self._traces[trace_id] = trace
            self._current_trace.set(trace_id)
            self._sink.record(
                {
                    "kind": "state",
                    "phase": "start",
                    "name": "llamaindex.state",
                    "trace": trace.identity,
                    "native_identity": trace_id,
                    "native": {"state": "running", "trace_id": trace_id},
                    "semantic": {
                        "type": "state.transition",
                        "framework": "llamaindex",
                        "state_type": "state.run_started",
                        "value": {"trace_id": trace_id},
                    },
                }
            )
            if previous in self._failed_turns:
                self._sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": "llamaindex.recovery",
                        "trace": trace.identity,
                        "native_identity": trace_id,
                        "native": {"attempt": 2, "previous_turn_id": previous},
                        "semantic": {
                            "type": "state.transition",
                            "framework": "llamaindex",
                            "state_type": "recovery.retry",
                            "value": {"previous_turn_id": previous},
                        },
                    }
                )

    def _end_trace(self, trace_id: str, trace_map: Any) -> None:
        with self._guard:
            alias = self._trace_aliases.pop(trace_id, None)
            if alias is not None:
                alias.trace_map = trace_map
                self._maybe_close_trace(alias)
                return
            trace = self._traces.get(trace_id)
            if trace is None:
                return
            trace.end_requested = True
            trace.trace_map = trace_map
            if self._current_trace.get() == trace_id:
                self._current_trace.set(None)
            self._maybe_close_trace(trace)

    def _maybe_close_trace(self, trace: _Trace) -> None:
        if (
            not trace.end_requested
            or trace.events
            or trace.workflow_tools
            or trace.pending_workflow_tools
            or any(span.trace is trace for span in self._dispatcher_spans.values())
            or any(span.trace is trace for span in self._function_tool_spans.values())
        ):
            return
        self._traces.pop(trace.trace_id, None)
        for alias, candidate in list(self._trace_aliases.items()):
            if candidate is trace:
                self._trace_aliases.pop(alias, None)
        self._record_workflow_partial(trace)
        failure = native_snapshot(
            trace.failed_error if trace.failed_error is not None else "trace failed"
        )
        terminal_output = (
            trace.workflow_output if trace.workflow_output_observed else trace.trace_map
        )
        output_ref = (
            trace.workflow_output_ref.record_id
            if trace.workflow_output_ref is not None
            and trace.workflow_output_ref.accepted
            and trace.workflow_output_ref.record_id is not None
            else None
        )
        self._sink.record(
            {
                "kind": "state",
                "phase": "error" if trace.failed else "end",
                "name": "llamaindex.state",
                "trace": trace.identity,
                "native_identity": trace.trace_id,
                "native": {"error": failure}
                if trace.failed
                else {"state": "completed", "trace_map": native_snapshot(trace.trace_map)},
                "semantic": {
                    "type": "state.transition",
                    "framework": "llamaindex",
                    "state_type": "state.run_terminal",
                    "value": (
                        {"status": _run_status(trace), "error": failure}
                        if trace.failed
                        else {"status": _run_status(trace)}
                    ),
                },
                **_parent(trace.root),
            }
        )
        self._sink.record(
            {
                "kind": "lifecycle",
                "phase": "error" if trace.failed else "end",
                "name": "llamaindex.trace",
                "trace": trace.identity,
                "native_identity": trace.trace_id,
                **(
                    {"error_identity": trace.failed_error_identity}
                    if trace.failed
                    and isinstance(trace.failed_error_identity, BaseException)
                    else {}
                ),
                "native": {"error": failure}
                if trace.failed
                else {"output": native_snapshot(terminal_output)},
                "semantic": {
                    "type": "workflow.run",
                    "framework": "llamaindex",
                    "status": _run_status(trace),
                    **(
                        {"error": _semantic_error(trace.failed_error)}
                        if trace.failed_error is not None
                        else {}
                    ),
                    **(
                        {"output": native_snapshot(terminal_output)}
                        if output_ref is None
                        and (trace.workflow_output_observed or trace.trace_map is not None)
                        else {}
                    ),
                    **({"output_ref": output_ref} if output_ref is not None else {}),
                },
                **_parent(trace.root),
            }
        )
        if trace.failed:
            self._failed_turns.add(trace.turn_id)
        self._last_turn[trace.conversation] = trace.turn_id
        for event_id, event_trace in list(self._event_traces.items()):
            if event_trace is trace:
                self._event_traces.pop(event_id, None)

    def _dispatcher_event(self, event: Any, kwargs: Any) -> None:
        del kwargs
        with self._guard:
            if not self._active:
                return
            event_name = _native_type_name(event)
            span_id = native_field(event, "span_id", None)
            if type(span_id) is not str or not span_id:
                return
            if event_name in {"LLMCompletionStartEvent", "LLMChatStartEvent"}:
                trace = self._current()
                if trace is not None:
                    self._dispatcher_spans[span_id] = _DispatcherSpan(trace, span_id)
                return
            span = self._dispatcher_spans.get(span_id)
            if span is None:
                return
            if event_name in {
                "LLMCompletionInProgressEvent",
                "LLMChatInProgressEvent",
            }:
                response = native_field(event, "response", None)
                delta = native_field(response, "delta", None)
                if type(delta) is str:
                    span.saw_delta = True
                    self._sink.record(
                        {
                            "kind": "stream",
                            "phase": "event",
                            "name": "llamaindex.llm.stream",
                            "trace": span.trace.identity,
                            "native_identity": span_id,
                            "native": {"delta": delta, "event": native_snapshot(event)},
                            "semantic": {
                                "type": "capture.redundant",
                                "framework": "llamaindex",
                            },
                            **_parent(span.model),
                        }
                    )
                return
            if event_name in {"LLMCompletionEndEvent", "LLMChatEndEvent"}:
                response = native_field(event, "response", None)
                if span.saw_delta:
                    terminal_native: dict[str, Any] = {
                        "output": native_snapshot(response),
                        "event": native_snapshot(event),
                    }
                    usage = _find_usage(response)
                    if usage is not None:
                        terminal_native["usage"] = native_snapshot(usage)
                    self._sink.record(
                        {
                            "kind": "stream",
                            "phase": "end",
                            "name": "llamaindex.llm.stream",
                            "trace": span.trace.identity,
                            "native_identity": span_id,
                            "native": terminal_native,
                            "semantic": {
                                "type": "capture.redundant",
                                "framework": "llamaindex",
                            },
                            **_parent(span.model),
                        }
                    )
                self._dispatcher_spans.pop(span_id, None)
                self._maybe_close_trace(span.trace)
                return
            if event_name == "ExceptionEvent":
                error = native_field(event, "exception", None)
                self._record_model_error(
                    span.trace, span.model_event_id or span_id, span.model, error
                )
                self._dispatcher_spans.pop(span_id, None)
                self._maybe_close_trace(span.trace)

    def _workflow_tool_step_start(
        self,
        span_id: str,
        bound_args: Any,
        workflow_tool_call_type: type[Any],
    ) -> None:
        with self._guard:
            trace = self._bound_trace()
            if trace is None:
                return
            arguments = bound_args.arguments
            if type(arguments) is not dict:
                return
            event = next(
                (
                    value
                    for value in dict.values(arguments)
                    if isinstance(value, workflow_tool_call_type)
                ),
                None,
            )
            tool_id = native_field(event, "tool_id", None)
            if type(tool_id) is str and tool_id:
                self._workflow_tool_steps[span_id] = (trace, tool_id)

    def _workflow_tool_step_end(self, span_id: str) -> None:
        with self._guard:
            self._workflow_tool_steps.pop(span_id, None)

    def _function_tool_span_start(
        self,
        span_id: str,
        bound_args: Any,
        instance: Any,
        parent_span_id: str | None,
        function_tool_type: type[Any],
    ) -> bool:
        with self._guard:
            if (
                not self._active
                or not isinstance(instance, function_tool_type)
                or not _is_function_tool_call_span(span_id, instance)
            ):
                return False
            trace = self._bound_trace()
            metadata = instance.metadata
            name = native_field(metadata, "name", None)
            if trace is None or name is None:
                return False
            workflow_step = self._workflow_tool_steps.get(parent_span_id or "")
            if workflow_step is not None and workflow_step[0] is trace:
                workflow_identity = True
                call_id = workflow_step[1]
            else:
                workflow_identity = False
                call_id = span_id
            tool_input = _function_tool_input(bound_args, instance)
            self._function_tool_spans[span_id] = _FunctionToolSpan(
                trace=trace,
                span_id=span_id,
                call_id=call_id,
                name=name,
                input=tool_input,
                workflow_identity=workflow_identity,
            )
            return True

    def _function_tool_span_end(self, span_id: str, result: Any) -> None:
        with self._guard:
            span = self._function_tool_spans.pop(span_id, None)
            if span is None:
                return
            executed_input = _function_tool_result_input(result, span.input)
            call = self._record_function_tool_call(
                span.trace,
                span_id=span.span_id,
                call_id=span.call_id,
                name=span.name,
                tool_input=executed_input,
                correlation_available=span.workflow_identity,
            )
            raw_output = native_field(result, "raw_output", result)
            failed = native_field(result, "is_error", False) is True
            self._sink.record(
                {
                    "kind": "tool",
                    "phase": "end",
                    "name": "llamaindex.function_tool",
                    "trace": span.trace.identity,
                    "native_identity": span.span_id,
                    "native": {
                        "name": span.name,
                        "input": native_snapshot(executed_input),
                        "output": native_snapshot(result),
                        "raw_output": native_snapshot(raw_output),
                        "is_error": failed,
                    },
                    "semantic": {
                        "type": "tool.result",
                        "framework": "llamaindex",
                        "call_id": span.call_id,
                        "output": native_snapshot(raw_output),
                        "status": "failed" if failed else "succeeded",
                    },
                    **_parent(call),
                }
            )
            self._finish_function_tool(span)
            self._maybe_close_trace(span.trace)

    def _function_tool_span_error(self, span_id: str, error: BaseException | None) -> None:
        with self._guard:
            span = self._function_tool_spans.pop(span_id, None)
            if span is None:
                return
            call = self._record_function_tool_call(
                span.trace,
                span_id=span.span_id,
                call_id=span.call_id,
                name=span.name,
                tool_input=span.input,
                correlation_available=span.workflow_identity,
            )
            actual_error = error or RuntimeError("LlamaIndex FunctionTool failed")
            self._sink.record(
                {
                    "kind": "tool",
                    "phase": "error",
                    "name": "llamaindex.function_tool",
                    "trace": span.trace.identity,
                    "native_identity": span.span_id,
                    **(
                        {"error_identity": error}
                        if isinstance(error, BaseException)
                        else {}
                    ),
                    "native": {
                        "name": span.name,
                        "input": native_snapshot(span.input),
                        **_error_native(actual_error),
                    },
                    "semantic": {
                        "type": "tool.error",
                        "framework": "llamaindex",
                        "call_id": span.call_id,
                        "status": ("cancelled" if _is_cancellation(actual_error) else "failed"),
                        "error": _semantic_error(actual_error),
                    },
                    **_parent(call),
                }
            )
            self._finish_function_tool(span)
            self._maybe_close_trace(span.trace)

    def _record_function_tool_call(
        self,
        trace: _Trace,
        *,
        span_id: str,
        call_id: str,
        name: str,
        tool_input: Any,
        correlation_available: bool,
    ) -> AdmissionReceipt:
        receipt: AdmissionReceipt = self._sink.record(
            {
                "kind": "tool",
                "phase": "start",
                "name": "llamaindex.function_tool",
                "trace": trace.identity,
                "native_identity": span_id,
                "native": {
                    "name": name,
                    "input": native_snapshot(tool_input),
                    "dispatcher_span_id": span_id,
                },
                "semantic": {
                    "type": "tool.execution",
                    "framework": "llamaindex",
                    "call_id": call_id,
                    "name": name,
                    "input": native_snapshot(tool_input),
                },
                **_parent(trace.tool_proposals.get(call_id)),
            }
        )
        if (
            not correlation_available
            and trace.tool_proposals
            and not trace.tool_correlation_loss
        ):
            trace.tool_correlation_loss = True
            self._sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "llamaindex.function_tool.correlation_gap",
                    "trace": trace.identity,
                    "native_identity": f"{trace.trace_id}::tool-correlation-gap",
                    "native": {
                        "dispatcher_span_id": span_id,
                        "provider_call_ids": list(trace.tool_proposals),
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "framework": "llamaindex",
                        "reason": "tool_proposal_correlation_unavailable",
                        "count": 1,
                        "detail": (
                            "The FunctionTool dispatcher span proves execution but "
                            "does not expose the provider proposal call ID."
                        ),
                    },
                    **_parent(receipt),
                }
            )
        return receipt

    def _record_pending_workflow_tool(
        self,
        trace: _Trace,
        identity: str,
        pending: dict[str, Any],
    ) -> AdmissionReceipt:
        tool_id = dict.get(pending, "tool_id")
        tool_name = dict.get(pending, "tool_name")
        tool_input = dict.get(pending, "tool_input")
        receipt: AdmissionReceipt = self._sink.record(
            {
                "kind": "tool",
                "phase": "start",
                "name": "llamaindex.workflow.tool",
                "trace": trace.identity,
                "native_identity": identity,
                "native": dict.get(pending, "native"),
                "semantic": (
                    {
                        "type": "tool.execution",
                        "framework": "llamaindex",
                        "call_id": tool_id,
                        "name": tool_name,
                        "input": tool_input,
                    }
                    if (
                        type(tool_id) is str
                        and tool_id
                        and type(tool_name) is str
                        and tool_name
                    )
                    else {"framework": "llamaindex"}
                ),
                **_parent(trace.tool_proposals.get(identity)),
            }
        )
        return receipt

    @staticmethod
    def _finish_function_tool(span: _FunctionToolSpan) -> None:
        if not span.workflow_identity:
            return
        span.trace.pending_workflow_tools.pop(span.call_id, None)
        span.trace.completed_workflow_tools.add(span.call_id)

    def _bound_trace(self) -> _Trace | None:
        trace_id = self._current_trace.get()
        if trace_id is None:
            return None
        return self._traces.get(trace_id) or self._trace_aliases.get(trace_id)

    def _event_start(
        self, event_type: Any, payload: Any, event_id: str, parent_id: str, kwargs: Any
    ) -> None:
        with self._guard:
            trace = self._event_traces.get(parent_id) or self._current()
            if trace is None:
                return
            kind = _event_name(event_type)
            self._event_traces[event_id] = trace
            if kind == "llm":
                messages = _payload(payload, "messages")
                context_plan = self._plan_messages(trace, messages)
                context_refs, consumed_pending, context_admitted = (
                    self._admit_planned_messages(
                        trace,
                        event_id,
                        parent_id,
                        context_plan,
                    )
                )
                serialized = _payload(payload, "serialized")
                invocation = _payload(payload, "additional_kwargs")
                request_semantic = _model_request_semantic(
                    serialized,
                    invocation,
                    context_refs if context_admitted else None,
                    (
                        context_plan.context_base_ref
                        if context_admitted
                        else None
                    ),
                )
                receipt = self._sink.record(
                    {
                        "kind": "model",
                        "phase": "start",
                        "name": "llamaindex.llm",
                        "trace": trace.identity,
                        "native_identity": event_id,
                        "native": {
                            "event_type": kind,
                            "message_count": (
                                len(messages)
                                if type(messages) in {list, tuple}
                                else None
                            ),
                            "context_snapshot": (
                                "complete" if context_plan.complete else "unavailable"
                            ),
                        },
                        "semantic": request_semantic,
                        **self._event_parent(trace, parent_id),
                    }
                )
                trace.events[event_id] = receipt
                trace.active_model_requests.add(event_id)
                if not context_plan.complete:
                    self._sink.record(
                        {
                            "kind": "state",
                            "phase": "event",
                            "name": "llamaindex.model.context_snapshot_gap",
                            "trace": trace.identity,
                            "native_identity": f"{event_id}::context-snapshot-gap",
                            "native": {
                                "message_count": (
                                    len(messages)
                                    if type(messages) in {list, tuple}
                                    else None
                                ),
                                "context_snapshot": "unavailable",
                            },
                            "semantic": {
                                "type": "capture.gap",
                                "framework": "llamaindex",
                                "reason": "model_context_snapshot_unavailable",
                                "count": 1,
                                "detail": (
                                    "The model-visible context could not be retained "
                                    "exactly within the snapshot safety bounds, so "
                                    "context references were omitted."
                                ),
                            },
                            **_parent(receipt),
                        }
                    )
                if (
                    context_admitted
                    and receipt.accepted
                    and receipt.record_id is not None
                    and context_plan.digest is not None
                ):
                    trace.model_context_length = len(context_plan.messages)
                    trace.model_context_digest = context_plan.digest
                    trace.model_context_request_ref = receipt.record_id
                    if consumed_pending:
                        trace.pending_context_messages = [
                            pending
                            for index, pending in enumerate(
                                trace.pending_context_messages
                            )
                            if index not in consumed_pending
                        ]
                for span in reversed(self._dispatcher_spans.values()):
                    if span.trace is trace and span.model is None:
                        span.model_event_id = event_id
                        span.model = receipt
                        break
                return
            snapshot = {
                "event_type": kind,
                "payload": native_snapshot(payload),
                "kwargs": native_snapshot(kwargs),
            }
            if kind == "function_call":
                tool = _payload(payload, "tool")
                tool_name = _tool_name(tool)
                tool_input = native_snapshot(_payload(payload, "function_call"))
                native = {
                    "name": native_snapshot(tool),
                    "input": tool_input,
                    "call_id": event_id,
                    "event": snapshot,
                }
                trace.events[event_id] = self._sink.record(
                    {
                        "kind": "tool",
                        "phase": "start",
                        "name": "llamaindex.tool",
                        "trace": trace.identity,
                        "native_identity": event_id,
                        "native": native,
                        "semantic": {
                            "type": "tool.execution",
                            "framework": "llamaindex",
                            "call_id": event_id,
                            "name": tool_name,
                            "input": tool_input,
                        }
                        if tool_name is not None
                        else {"framework": "llamaindex"},
                        **_parent(trace.tool_proposals.get(event_id)),
                        **(
                            {}
                            if event_id in trace.tool_proposals
                            else self._event_parent(trace, parent_id)
                        ),
                    }
                )
            elif kind == "exception":
                error = _payload(payload, "exception")
                if error is None:
                    error = "LlamaIndex callback exception"
                self._sink.record(
                    {
                        "kind": "error",
                        "phase": "event",
                        "name": "llamaindex.error",
                        "trace": trace.identity,
                        "native_identity": event_id,
                        **(
                            {"error_identity": error}
                            if isinstance(error, BaseException)
                            else {}
                        ),
                        "native": {
                            **_error_native(error),
                            "event_type": kind,
                            "kwargs": native_snapshot(kwargs),
                        },
                        "semantic": {
                            "type": "error",
                            "framework": "llamaindex",
                            "error": _semantic_error(error),
                        },
                        **self._event_parent(trace, parent_id),
                    }
                )
                trace.failed = True
                trace.failed_error = error
                trace.failed_error_identity = (
                    error if isinstance(error, BaseException) else None
                )
            else:
                trace.events[event_id] = self._sink.record(
                    {
                        "kind": "state",
                        "phase": "start",
                        "name": "llamaindex.state.event",
                        "trace": trace.identity,
                        "native_identity": event_id,
                        "native": snapshot,
                        "semantic": {
                            "type": "state.transition",
                            "framework": "llamaindex",
                            "state_type": "state.callback_started",
                            "value": snapshot,
                        },
                        **self._event_parent(trace, parent_id),
                    }
                )

    def _event_end(self, event_type: Any, payload: Any, event_id: str, kwargs: Any) -> None:
        with self._guard:
            trace = self._event_traces.get(event_id) or self._current()
            if trace is None:
                return
            kind = _event_name(event_type)
            self._event_traces.pop(event_id, None)
            receipt = trace.events.pop(event_id, None)
            snapshot = {
                "event_type": kind,
                "payload": native_snapshot(payload),
                "kwargs": native_snapshot(kwargs),
            }
            if kind == "llm":
                trace.active_model_requests.discard(event_id)
                dispatcher_span = next(
                    (
                        span
                        for span in self._dispatcher_spans.values()
                        if span.trace is trace and span.model_event_id == event_id
                    ),
                    None,
                )
                response = _payload(payload, "response")
                if response is None:
                    response = _payload(payload, "completion")
                streamed_reasoning = self._take_workflow_reasoning(trace, response)
                error = _payload(payload, "exception")
                if error is not None:
                    self._record_model_error(
                        trace,
                        event_id,
                        receipt,
                        error,
                        streamed_reasoning,
                    )
                    if dispatcher_span is not None:
                        self._dispatcher_spans.pop(dispatcher_span.span_id, None)
                else:
                    native = native_snapshot(response)
                    model_native: dict[str, Any] = {"output": native}
                    usage = _find_usage(response)
                    if usage is not None:
                        model_native["usage"] = native_snapshot(usage)
                    response_receipt = self._sink.record(
                        {
                            "kind": "model",
                            "phase": "end",
                            "name": "llamaindex.llm",
                            "trace": trace.identity,
                            "native_identity": event_id,
                            "native": model_native,
                            "semantic": {
                                "type": "model.response",
                                "framework": "llamaindex",
                                **_model_response_semantic(
                                    response,
                                    usage,
                                    streamed_reasoning,
                                ),
                            },
                            **_parent(receipt),
                        }
                    )
                    _record_unavailable_reasoning_gap(
                        self._sink,
                        trace.identity,
                        framework="llamaindex",
                        affected=response_receipt,
                        count=_message_unavailable_reasoning(
                            native_field(response, "message", None)
                        ),
                        detail=(
                            "LlamaIndex exposed encrypted, redacted, or "
                            "signature-only thinking metadata without readable "
                            "content; opaque bytes were omitted."
                        ),
                    )
                    trace.model_responses_observed += 1
                    trace.latest_model_output = _ModelOutput(
                        response=response,
                        message=native_field(response, "message", None),
                        receipt=response_receipt,
                    )
                    self._record_tool_proposals(trace, event_id, response_receipt, response)
                    if usage is not None:
                        self._sink.record(
                            {
                                "kind": "model",
                                "phase": "event",
                                "name": "llamaindex.llm.usage",
                                "trace": trace.identity,
                                "native_identity": f"{event_id}::usage",
                                "native": {"usage": native_snapshot(usage)},
                                "semantic": {
                                    "type": "capture.redundant",
                                    "framework": "llamaindex",
                                },
                                **_parent(receipt),
                            }
                        )
                    if dispatcher_span is None:
                        self._record_callback_stream(trace, event_id, receipt, response, snapshot)
            elif kind == "function_call":
                output = native_snapshot(_payload(payload, "function_call_response"))
                self._sink.record(
                    {
                        "kind": "tool",
                        "phase": "end",
                        "name": "llamaindex.tool",
                        "trace": trace.identity,
                        "native_identity": event_id,
                        "native": {
                            "output": output,
                            "call_id": event_id,
                            "event": snapshot,
                        },
                        "semantic": {
                            "type": "tool.result",
                            "framework": "llamaindex",
                            "call_id": event_id,
                            "output": output,
                            "status": "succeeded",
                        },
                        **_parent(receipt),
                    }
                )
            elif kind != "exception":
                self._sink.record(
                    {
                        "kind": "state",
                        "phase": "end",
                        "name": "llamaindex.state.event",
                        "trace": trace.identity,
                        "native_identity": event_id,
                        "native": {"state": "completed", "event": snapshot},
                        "semantic": {
                            "type": "state.transition",
                            "framework": "llamaindex",
                            "state_type": "state.callback_completed",
                            "value": snapshot,
                        },
                        **_parent(receipt),
                    }
                )
            self._maybe_close_trace(trace)

    def _record_callback_stream(
        self,
        trace: _Trace,
        event_id: str,
        receipt: AdmissionReceipt | None,
        response: Any,
        snapshot: dict[str, Any],
    ) -> None:
        delta = native_field(response, "delta", None)
        streamed = (
            delta is not None
            and not is_unsafe_accessor_omission(delta)
            and type(response) is not dict
        )
        if not streamed:
            return
        native = native_snapshot(response)
        self._sink.record(
            {
                "kind": "stream",
                "phase": "event",
                "name": "llamaindex.llm.stream",
                "trace": trace.identity,
                "native_identity": event_id,
                "native": {"delta": delta, "event": snapshot},
                "semantic": {"type": "capture.redundant", "framework": "llamaindex"},
                **_parent(receipt),
            }
        )
        terminal_native: dict[str, Any] = {
            "output": native,
            "finish_reason": native_field(response, "finish_reason", None),
        }
        usage = _find_usage(response)
        if usage is not None:
            terminal_native["usage"] = native_snapshot(usage)
        self._sink.record(
            {
                "kind": "stream",
                "phase": "end",
                "name": "llamaindex.llm.stream",
                "trace": trace.identity,
                "native_identity": event_id,
                "native": terminal_native,
                "semantic": {"type": "capture.redundant", "framework": "llamaindex"},
                **_parent(receipt),
            }
        )

    def _record_model_error(
        self,
        trace: _Trace,
        event_id: str,
        receipt: AdmissionReceipt | None,
        error: Any,
        streamed_reasoning: list[str] | None = None,
    ) -> None:
        trace.failed = True
        trace.failed_error = error
        trace.failed_error_identity = (
            error if isinstance(error, BaseException) else None
        )
        reasoning_text = "".join(streamed_reasoning or [])
        self._sink.record(
            {
                "kind": "model",
                "phase": "error",
                "name": "llamaindex.llm",
                "trace": trace.identity,
                "native_identity": event_id,
                **(
                    {"error_identity": error}
                    if isinstance(error, BaseException)
                    else {}
                ),
                "native": {"error": native_snapshot(error)},
                "semantic": {
                    "type": "model.response",
                    "framework": "llamaindex",
                    "status": "failed",
                    **(
                        {
                            "reasoning": [
                                {"type": "text", "text": reasoning_text}
                            ]
                        }
                        if reasoning_text
                        else {}
                    ),
                },
                **_parent(receipt),
            }
        )
        self._sink.record(
            {
                "kind": "error",
                "phase": "event",
                "name": "llamaindex.error",
                "trace": trace.identity,
                "native_identity": event_id,
                **(
                    {"error_identity": error}
                    if isinstance(error, BaseException)
                    else {}
                ),
                "native": _error_native(error),
                "semantic": {
                    "type": "error",
                    "framework": "llamaindex",
                    "error": _semantic_error(error),
                },
                **_parent(receipt),
            }
        )

    def _record_tool_proposals(
        self,
        trace: _Trace,
        event_id: str,
        receipt: AdmissionReceipt | None,
        response: Any,
    ) -> None:
        for index, proposal in enumerate(_tool_proposals(response), start=1):
            call_id = proposal.get("call_id")
            identity = (
                call_id if type(call_id) is str and call_id else f"{event_id}::proposal::{index}"
            )
            if identity in trace.tool_proposals:
                continue
            tool_name = proposal.get("name")
            semantic = (
                {
                    "type": "tool.proposal",
                    "framework": "llamaindex",
                    "call_id": call_id,
                    "name": tool_name,
                    "input": proposal.get("input"),
                }
                if (
                    type(call_id) is str
                    and call_id
                    and type(tool_name) is str
                    and tool_name
                )
                else {"framework": "llamaindex"}
            )
            trace.tool_proposals[identity] = self._sink.record(
                {
                    "kind": "tool",
                    "phase": "event",
                    "name": "llamaindex.tool.proposal",
                    "trace": trace.identity,
                    "native_identity": identity,
                    "native": proposal,
                    "semantic": semantic,
                    **_parent(receipt),
                }
            )

    def _workflow_event(self, trace: _Trace, event: Any) -> None:
        with self._guard:
            if not self._active or self._traces.get(trace.trace_id) is not trace:
                return
            event_name = _native_type_name(event)
            trace.workflow_events += 1
            sequence = trace.workflow_events
            native_identity = f"{trace.trace_id}::workflow::{sequence}"
            if event_name == "AgentStream":
                self._observe_workflow_stream(trace, event)
                self._sink.record(
                    {
                        "kind": "stream",
                        "phase": "event",
                        "name": "llamaindex.workflow.stream",
                        "trace": trace.identity,
                        "native_identity": native_identity,
                        "native": {
                            "delta": native_snapshot(native_field(event, "delta", None)),
                            "event": native_snapshot(event),
                        },
                        "semantic": {
                            "type": "capture.redundant",
                            "framework": "llamaindex",
                        },
                    }
                )
                return
            if event_name == "ToolCall":
                tool_id = native_field(event, "tool_id", None)
                identity = tool_id if type(tool_id) is str and tool_id else native_identity
                existing = trace.workflow_tools.get(identity)
                if (
                    existing is not None
                    or identity in trace.pending_workflow_tools
                    or identity in trace.completed_workflow_tools
                ):
                    self._sink.record(
                        {
                            "kind": "tool",
                            "phase": "event",
                            "name": "llamaindex.workflow.tool",
                            "trace": trace.identity,
                            "native_identity": identity,
                            "native": {"event": native_snapshot(event)},
                            "semantic": {
                                "type": "capture.redundant",
                                "framework": "llamaindex",
                            },
                            **_parent(existing),
                        }
                    )
                    return
                tool_name = native_field(event, "tool_name", None)
                tool_input = native_snapshot(native_field(event, "tool_kwargs", None))
                native = {
                    "tool_name": native_snapshot(tool_name),
                    "tool_kwargs": tool_input,
                    "tool_id": native_snapshot(tool_id),
                    "event": native_snapshot(event),
                }
                trace.pending_workflow_tools[identity] = {
                    "tool_id": tool_id,
                    "tool_name": tool_name,
                    "tool_input": tool_input,
                    "native": native,
                }
                return
            if event_name == "ToolCallResult":
                tool_id = native_field(event, "tool_id", None)
                identity = tool_id if type(tool_id) is str and tool_id else native_identity
                if identity in trace.completed_workflow_tools:
                    trace.pending_workflow_tools.pop(identity, None)
                    trace.completed_workflow_tools.discard(identity)
                    self._sink.record(
                        {
                            "kind": "tool",
                            "phase": "event",
                            "name": "llamaindex.workflow.tool",
                            "trace": trace.identity,
                            "native_identity": identity,
                            "native": {"event": native_snapshot(event)},
                            "semantic": {
                                "type": "capture.redundant",
                                "framework": "llamaindex",
                            },
                        }
                    )
                    return
                tool_output = native_field(event, "tool_output", None)
                raw_result = native_field(tool_output, "raw_output", tool_output)
                receipt = trace.workflow_tools.pop(identity, None)
                pending = trace.pending_workflow_tools.pop(identity, None)
                if receipt is None and pending is not None:
                    pending = {
                        **pending,
                        "tool_input": _function_tool_result_input(
                            tool_output, dict.get(pending, "tool_input")
                        ),
                    }
                    receipt = self._record_pending_workflow_tool(
                        trace, identity, pending
                    )
                self._sink.record(
                    {
                        "kind": "tool",
                        "phase": "end",
                        "name": "llamaindex.workflow.tool",
                        "trace": trace.identity,
                        "native_identity": identity,
                        "native": {
                            "tool_name": native_snapshot(native_field(event, "tool_name", None)),
                            "tool_kwargs": native_snapshot(
                                native_field(event, "tool_kwargs", None)
                            ),
                            "tool_id": native_snapshot(tool_id),
                            "call_id": identity,
                            "tool_output": native_snapshot(tool_output),
                            "result": native_snapshot(raw_result),
                            "event": native_snapshot(event),
                        },
                        "semantic": (
                            {
                                "type": "tool.result",
                                "framework": "llamaindex",
                                "call_id": tool_id,
                                "output": native_snapshot(raw_result),
                                "status": "succeeded",
                            }
                            if type(tool_id) is str and tool_id
                            else {"framework": "llamaindex"}
                        ),
                        **_parent(receipt),
                    }
                )
                return
            if event_name in {"AgentInput", "AgentOutput"}:
                if event_name == "AgentOutput":
                    trace.streamed_agent_output = event
                workflow_input = native_field(event, "input", None)
                event_native = (
                    {
                        "event_type": event_name,
                        "message_count": (
                            len(workflow_input)
                            if type(workflow_input) in {list, tuple}
                            else None
                        ),
                        "current_agent_name": native_snapshot(
                            native_field(event, "current_agent_name", None)
                        ),
                    }
                    if event_name == "AgentInput"
                    else {"event": native_snapshot(event)}
                )
                self._sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": "llamaindex.workflow.event",
                        "trace": trace.identity,
                        "native_identity": native_identity,
                        "native": event_native,
                        "semantic": {
                            "type": "capture.redundant",
                            "framework": "llamaindex",
                        },
                    }
                )

    def _workflow_user_input(self, trace: _Trace, kwargs: dict[str, Any]) -> None:
        """Capture the exact official ``agent.run(user_msg=...)`` input before formatting."""

        if "user_msg" not in kwargs:
            return
        with self._guard:
            if not self._active or self._traces.get(trace.trace_id) is not trace:
                return
            trace.user_inputs += 1
            state_receipt = self._sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "llamaindex.user.input",
                    "trace": trace.identity,
                    "native_identity": (
                        f"{trace.trace_id}::user-input::{trace.user_inputs}"
                    ),
                    "native": {
                        "role": "user",
                        "user_msg": native_snapshot(dict.get(kwargs, "user_msg")),
                    },
                    "semantic": {
                        "type": "state.transition",
                        "framework": "llamaindex",
                    },
                }
            )
            message = dict.get(kwargs, "user_msg")
            message_receipt = self._sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "llamaindex.user.message",
                    "trace": trace.identity,
                    "native_identity": (
                        f"{trace.trace_id}::user-message::{trace.user_inputs}"
                    ),
                    "native": {"role": "user", "content": native_snapshot(message)},
                    "semantic": {
                        "type": "message",
                        "framework": "llamaindex",
                        "role": "user",
                        "content": native_snapshot(message),
                    },
                    **_parent(state_receipt),
                }
            )
            if (
                type(message) is str
                and message_receipt.accepted
                and message_receipt.record_id is not None
            ):
                trace.pending_context_messages.append(
                    (
                        ("user", message),
                        message_receipt.record_id,
                        trace.user_inputs,
                    )
                )
                if len(trace.pending_context_messages) > _MAX_PENDING_CONTEXT_MESSAGES:
                    trace.pending_context_messages.pop(0)

    def _observe_workflow_stream(self, trace: _Trace, event: Any) -> None:
        thinking_delta = native_field(event, "thinking_delta", None)
        if type(thinking_delta) is str and thinking_delta:
            raw = native_field(event, "raw", None)
            if raw is not None:
                trace.workflow_reasoning.append(
                    _WorkflowReasoning(raw, [thinking_delta])
                )
            else:
                trace.workflow_reasoning.append(
                    _WorkflowReasoning(None, [thinking_delta])
                )
        response = native_field(event, "response", None)
        delta = native_field(event, "delta", None)
        if type(response) is str:
            content, byte_count, truncated = _bounded_utf8_prefix(
                response,
                _MAX_WORKFLOW_STREAM_PARTIAL_BYTES,
            )
            trace.workflow_stream_content = content
            trace.workflow_stream_bytes = byte_count
        elif type(delta) is str:
            retained_delta, byte_count, truncated = _bounded_utf8_prefix(
                delta,
                _MAX_WORKFLOW_STREAM_PARTIAL_BYTES - trace.workflow_stream_bytes,
            )
            if retained_delta:
                existing = trace.workflow_stream_content
                trace.workflow_stream_content = (
                    retained_delta if existing is None else f"{existing}{retained_delta}"
                )
            trace.workflow_stream_bytes += byte_count
        else:
            return
        trace.workflow_stream_truncated = trace.workflow_stream_truncated or truncated

    def _take_workflow_reasoning(self, trace: _Trace, response: Any) -> list[str]:
        message = native_field(response, "message", None)
        candidates = (
            response,
            message,
            native_field(response, "raw", None),
            native_field(message, "raw", None),
        )
        matched: list[str] = []
        retained: list[_WorkflowReasoning] = []
        for reasoning in trace.workflow_reasoning:
            if any(
                candidate is reasoning.native
                for candidate in candidates
                if candidate is not None
            ):
                matched.extend(reasoning.parts)
            else:
                retained.append(reasoning)
        trace.workflow_reasoning = retained
        return matched

    def _record_workflow_partial(self, trace: _Trace) -> None:
        content = trace.workflow_stream_content
        if content is None or trace.workflow_output_observed:
            return
        status = trace.terminal_status or "unobserved"
        value = {
            "content": content,
            "status": status,
            **({"truncated": True} if trace.workflow_stream_truncated else {}),
        }
        partial_receipt = self._sink.record(
            {
                "kind": "state",
                "phase": "event",
                "name": "llamaindex.workflow.stream.partial",
                "trace": trace.identity,
                "native_identity": f"{trace.trace_id}::workflow::partial",
                "native": value,
                "semantic": {
                    "type": "state.transition",
                    "framework": "llamaindex",
                    "state_type": "state.stream_partial",
                    "value": value,
                },
            }
        )
        if trace.workflow_stream_truncated:
            self._sink.record(
                {
                    "kind": "stream",
                    "phase": "gap",
                    "name": "llamaindex.workflow.stream.partial_truncated",
                    "trace": trace.identity,
                    "native_identity": f"{trace.trace_id}::workflow::partial",
                    "native": {
                        "content": content,
                        "status": status,
                        "truncated": True,
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "framework": "llamaindex",
                        "reason": "workflow_stream_partial_truncated",
                        "count": 1,
                        "detail": (
                            "The workflow ended without authoritative terminal "
                            "output and its partial streamed content exceeded "
                            "the retention budget."
                        ),
                    },
                    **_parent(partial_receipt),
                }
            )

    def _workflow_handler_started(self, trace: _Trace) -> None:
        with self._guard:
            if self._active and self._traces.get(trace.trace_id) is trace:
                trace.workflow_started = True

    def _workflow_result(
        self, trace: _Trace, result: Any, *, provisional: bool = False
    ) -> None:
        with self._guard:
            if not self._active or self._traces.get(trace.trace_id) is not trace:
                return
            if trace.workflow_output_observed and not (
                trace.workflow_output_provisional and not provisional
            ):
                return
            trace.workflow_output = result
            trace.workflow_output_observed = True
            trace.workflow_output_provisional = provisional
            trace.workflow_output_ref = None
            final = trace.latest_model_output
            response = native_field(result, "response", None)
            message = native_field(result, "message", None)
            raw = native_field(result, "raw", None)
            if final is None:
                if response is None:
                    return
                streamed_reasoning = self._take_workflow_reasoning(trace, response)
                response_semantic = _message_response_semantic(
                    response,
                    streamed_reasoning,
                )
                output_value = {
                    key: value
                    for key, value in response_semantic.items()
                    if key != "status"
                }
                output_receipt = self._sink.record(
                    {
                        "kind": "state",
                        "phase": "event",
                        "name": "llamaindex.workflow.agent_output",
                        "trace": trace.identity,
                        "native_identity": (
                            f"{trace.trace_id}::agent-output-response-fallback"
                        ),
                        "native": {"output": native_snapshot(response)},
                        "semantic": {
                            "type": "state.transition",
                            "framework": "llamaindex",
                            "state_type": "state.agent_output",
                            "value": output_value,
                        },
                    }
                )
                _record_unavailable_reasoning_gap(
                    self._sink,
                    trace.identity,
                    framework="llamaindex",
                    affected=output_receipt,
                    count=_message_unavailable_reasoning(response),
                    detail=(
                        "LlamaIndex exposed encrypted, redacted, or "
                        "signature-only thinking metadata without readable "
                        "content; opaque bytes were omitted."
                    ),
                )
                trace.workflow_output_ref = output_receipt
                self._record_workflow_output_correlation_loss(trace, result)
                return
            candidates = [
                result,
                response,
                message,
                raw,
            ]
            if any(
                candidate is final.response or candidate is final.message
                for candidate in candidates
                if candidate is not None
            ):
                trace.workflow_output_ref = final.receipt
                return
            if provisional or (response is None and message is None):
                return
            self._record_workflow_output_correlation_loss(trace, result)

    def _record_workflow_output_correlation_loss(
        self, trace: _Trace, result: Any
    ) -> None:
        if trace.workflow_output_correlation_loss:
            return
        trace.workflow_output_correlation_loss = True
        self._sink.record(
            {
                "kind": "state",
                "phase": "event",
                "name": "llamaindex.workflow.output_correlation_gap",
                "trace": trace.identity,
                "native_identity": f"{trace.trace_id}::workflow-output-correlation-gap",
                "native": {
                    "model_responses_observed": trace.model_responses_observed,
                    "workflow_result_type": _native_type_name(result),
                },
                "semantic": {
                    "type": "capture.gap",
                    "framework": "llamaindex",
                    "reason": "workflow_result_model_response_correlation_unavailable",
                    "count": 1,
                    "detail": (
                        "The awaited workflow result was retained, but exact "
                        "framework object identity for its model response was "
                        "unavailable."
                    ),
                },
            }
        )

    def _workflow_stream_terminal(
        self, trace: _Trace, status: str, error: BaseException | None
    ) -> None:
        with self._guard:
            if not self._active or self._traces.get(trace.trace_id) is not trace:
                return
            if trace.workflow_stream_terminal:
                return
            trace.workflow_stream_terminal = True
            trace.terminal_status = status
            if (
                status == "completed"
                and not trace.workflow_output_observed
                and trace.streamed_agent_output is not None
            ):
                self._workflow_result(
                    trace,
                    trace.streamed_agent_output,
                    provisional=True,
                )
            self._terminalize_omitted_models(trace, status)
            self._record_uncorrelated_workflow_reasoning(trace, status)
            missing_tools = self._terminalize_workflow_tools(trace, status)
            failed = error is not None or status != "completed" or missing_tools > 0
            if failed:
                trace.failed = True
                trace.failed_error = error or RuntimeError(
                    f"LlamaIndex workflow stream {status} with {missing_tools} "
                    "pending tool result(s)"
                )
                trace.failed_error_identity = error
            self._sink.record(
                {
                    "kind": "stream",
                    "phase": "error" if failed else "end",
                    "name": "llamaindex.workflow.stream",
                    "trace": trace.identity,
                    "native_identity": f"{trace.trace_id}::workflow::terminal",
                    "native": {
                        "status": status,
                        "events_observed": trace.workflow_events,
                        "pending_tools_terminalized": missing_tools,
                        **({"error": native_snapshot(error)} if error is not None else {}),
                    },
                    "semantic": {
                        "type": "capture.redundant",
                        "framework": "llamaindex",
                    },
                }
            )
            self._maybe_close_trace(trace)

    def _terminalize_omitted_models(self, trace: _Trace, status: str) -> None:
        for event_id in list(trace.active_model_requests):
            receipt = trace.events.pop(event_id, None)
            trace.active_model_requests.discard(event_id)
            self._event_traces.pop(event_id, None)
            if receipt is None:
                continue
            response_receipt = self._sink.record(
                {
                    "kind": "model",
                    "phase": (
                        "cancelled"
                        if status == "cancelled"
                        else "end"
                        if status == "completed"
                        else "error"
                    ),
                    "name": "llamaindex.llm",
                    "trace": trace.identity,
                    "native_identity": event_id,
                    "native": {"status": status, "terminal_event_omitted": True},
                    "semantic": {
                        "type": "model.response",
                        "framework": "llamaindex",
                        "status": (
                            "cancelled"
                            if status == "cancelled"
                            else "incomplete"
                            if status == "completed"
                            else "failed"
                        ),
                    },
                    **_parent(receipt),
                }
            )
            trace.model_responses_observed += 1
            trace.latest_model_output = _ModelOutput(
                response=None,
                message=None,
                receipt=response_receipt,
            )
            for span_id, span in list(self._dispatcher_spans.items()):
                if span.trace is trace and span.model_event_id == event_id:
                    self._dispatcher_spans.pop(span_id, None)

    def _record_uncorrelated_workflow_reasoning(
        self, trace: _Trace, status: str
    ) -> None:
        parts = [
            part
            for reasoning in trace.workflow_reasoning
            for part in reasoning.parts
        ]
        trace.workflow_reasoning = []
        text = "".join(parts)
        if not text:
            return
        state = self._sink.record(
            {
                "kind": "state",
                "phase": "event",
                "name": "llamaindex.workflow.reasoning.uncorrelated",
                "trace": trace.identity,
                "native_identity": f"{trace.trace_id}::workflow-reasoning-uncorrelated",
                "native": {"reasoning": text, "status": status},
                "semantic": {
                    "type": "state.transition",
                    "framework": "llamaindex",
                    "state_type": "state.stream_reasoning",
                    "value": {"reasoning": text, "status": status},
                },
            }
        )
        self._sink.record(
            {
                "kind": "stream",
                "phase": "gap",
                "name": "llamaindex.workflow.reasoning.correlation_gap",
                "trace": trace.identity,
                "native_identity": f"{trace.trace_id}::workflow-reasoning-correlation-gap",
                "native": {"reasoning": text, "status": status},
                "semantic": {
                    "type": "capture.gap",
                    "framework": "llamaindex",
                    "reason": "workflow_stream_reasoning_model_response_correlation_unavailable",
                    "count": 1,
                    "detail": (
                        "LlamaIndex exposed readable workflow stream reasoning "
                        "without an exact model response identity."
                    ),
                },
                **_parent(state),
            }
        )

    def _terminalize_workflow_tools(self, trace: _Trace, status: str) -> int:
        pending = list(trace.workflow_tools.items())
        trace.workflow_tools.clear()
        for identity, event in list(trace.pending_workflow_tools.items()):
            pending.append(
                (identity, self._record_pending_workflow_tool(trace, identity, event))
            )
        trace.pending_workflow_tools.clear()
        for identity, receipt in pending:
            self._sink.record(
                {
                    "kind": "tool",
                    "phase": "error",
                    "name": "llamaindex.workflow.tool",
                    "trace": trace.identity,
                    "native_identity": identity,
                    "native": {
                        "status": status,
                        "missing_result": True,
                        "error": {
                            "type": "MissingToolCallResult",
                            "message": "Workflow stream ended before ToolCallResult",
                        },
                    },
                    "semantic": {
                        "type": "tool.error",
                        "framework": "llamaindex",
                        "call_id": identity,
                        "status": (
                            "cancelled" if status == "cancelled" else "failed"
                        ),
                        "error": {
                            "type": "MissingToolCallResult",
                            "message": "Workflow stream ended before ToolCallResult",
                            "recoverable": False,
                        },
                    },
                    **_parent(receipt),
                }
            )
        return len(pending)

    def _workflow_state(self, trace: _Trace, state: Any) -> AdmissionReceipt:
        with self._guard:
            if not self._active or self._traces.get(trace.trace_id) is not trace:
                return AdmissionReceipt(False, "source_inactive")
            receipt: AdmissionReceipt = self._sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "llamaindex.workflow.state",
                    "trace": trace.identity,
                    "native_identity": f"{trace.trace_id}::workflow::state",
                    "native": {"state": native_snapshot(state)},
                    "semantic": {
                        "type": "state.transition",
                        "framework": "llamaindex",
                        "state_type": "state.workflow",
                        "value": native_snapshot(state),
                    },
                }
            )
            return receipt

    def _plan_messages(
        self,
        trace: _Trace,
        messages: Any,
    ) -> _ContextPlan:
        materialized = _context_messages(messages)
        if materialized is None:
            return _ContextPlan([], None, 0, None, False)
        exact_prefix = (
            trace.model_context_request_ref is not None
            and trace.model_context_digest is not None
            and len(materialized) >= trace.model_context_length
            and _context_digest(materialized[: trace.model_context_length])
            == trace.model_context_digest
        )
        digest = _context_digest(materialized)
        return _ContextPlan(
            messages=materialized,
            digest=digest,
            selected_from=trace.model_context_length if exact_prefix else 0,
            context_base_ref=(
                trace.model_context_request_ref if exact_prefix else None
            ),
            complete=True,
        )

    def _admit_planned_messages(
        self,
        trace: _Trace,
        event_id: str,
        parent_id: str,
        plan: _ContextPlan,
    ) -> tuple[list[str] | None, set[int], bool]:
        if not plan.complete:
            return None, set(), False

        refs: list[str] = []
        consumed_pending: set[int] = set()
        all_accepted = True
        for index in range(plan.selected_from, len(plan.messages)):
            message = plan.messages[index]
            pending_index = next(
                (
                    candidate
                    for candidate, pending in enumerate(
                        trace.pending_context_messages
                    )
                    if candidate not in consumed_pending
                    and message.pending_key is not None
                    and pending[0] == message.pending_key
                ),
                None,
            )
            if pending_index is not None:
                refs.append(trace.pending_context_messages[pending_index][1])
                consumed_pending.add(pending_index)
                continue
            receipt = self._sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "llamaindex.message",
                    "trace": trace.identity,
                    "native_identity": f"{event_id}::message::{index}",
                    "native": message.native,
                    "semantic": {
                        "type": "message",
                        "framework": "llamaindex",
                        **message.semantic,
                    },
                    **self._event_parent(trace, parent_id),
                }
            )
            if receipt.accepted and receipt.record_id is not None:
                refs.append(receipt.record_id)
            else:
                all_accepted = False

        if not all_accepted:
            return None, set(), False
        return refs, consumed_pending, True

    def _current(self) -> _Trace | None:
        if not self._active:
            return None
        trace_id = self._current_trace.get()
        if trace_id is not None:
            return self._traces.get(trace_id) or self._trace_aliases.get(trace_id)
        unique = {id(trace): trace for trace in self._traces.values()}
        return next(iter(unique.values())) if len(unique) == 1 else None

    @staticmethod
    def _event_parent(trace: _Trace, parent_id: str) -> dict[str, str]:
        return _parent(trace.events.get(parent_id))


def _event_name(value: Any) -> str:
    enum_value = native_field(value, "_value_", None)
    return enum_value.lower() if type(enum_value) is str else _native_type_name(value).lower()


def _payload(payload: Any, name: str) -> Any:
    if type(payload) is not dict:
        return None
    for key, value in dict.items(payload):
        enum_value = native_field(key, "_value_", None)
        if enum_value == name or key == name:
            return value
    return None


def _native_type_name(value: Any) -> str:
    try:
        return str(type.__getattribute__(type(value), "__name__"))
    except BaseException:
        return "unknown"


def _discard_identity(values: list[Any], target: Any) -> None:
    for index, value in enumerate(values):
        if value is target:
            del values[index]
            return


def _trace_parts(trace_id: str) -> tuple[str, str]:
    if "::" in trace_id:
        conversation, turn = trace_id.split("::", 1)
        return conversation, turn
    return trace_id, trace_id


def _find_usage(value: Any, seen: set[int] | None = None) -> Any:
    if seen is None:
        seen = set()
    identity = id(value)
    if identity in seen:
        return None
    seen.add(identity)
    data = native_own_data(value)
    if data:
        for key in ("usage", "usage_details", "token_usage"):
            candidate = dict.get(data, key)
            if type(candidate) is dict and candidate:
                return candidate
        for child in dict.values(data):
            found = _find_usage(child, seen)
            if found is not None:
                return found
    elif type(value) is list:
        for child in value:
            found = _find_usage(child, seen)
            if found is not None:
                return found
    return None


def _error_native(error: Any) -> dict[str, Any]:
    """Retain the exact failure and expose an owned structured envelope for navigation."""

    native = {"error": native_snapshot(error)}
    structured = native_field(error, "structured_error", None)
    if type(structured) is dict:
        native["structured_error"] = native_snapshot(structured)
    return native


def _context_messages(messages: Any) -> list[_ContextMessage] | None:
    """Materialize one bounded, exact view of the messages visible to the model."""

    if type(messages) not in {list, tuple}:
        return None
    snapshot = native_snapshot(messages)
    if (
        type(snapshot) is not list
        or len(snapshot) != len(messages)
        or not _snapshot_is_complete(snapshot)
    ):
        return None
    result: list[_ContextMessage] = []
    for message, message_snapshot in zip(messages, snapshot, strict=True):
        semantic = _context_message_semantic(message, message_snapshot)
        if semantic is None:
            return None
        pending_key = _plain_user_pending_key(message_snapshot, semantic)
        result.append(
            _ContextMessage(
                native=message_snapshot,
                semantic=semantic,
                pending_key=pending_key,
            )
        )
    return result


def _snapshot_is_complete(value: Any, *, depth: int = 0) -> bool:
    if depth > 64:
        return False
    if type(value) is dict:
        if "native_type" in value and (
            "omitted" in value or "reference" in value
        ):
            return False
        if set(value) == {"native_type"}:
            return False
        return all(
            _snapshot_is_complete(child, depth=depth + 1)
            for child in dict.values(value)
        )
    if type(value) is list:
        return all(
            _snapshot_is_complete(child, depth=depth + 1) for child in value
        )
    return value is None or type(value) in {str, bool, int, float, bytes}


def _plain_user_pending_key(
    snapshot: Any,
    semantic: dict[str, Any],
) -> tuple[str, str] | None:
    content = semantic["content"]
    if semantic["role"] != "user" or type(content) is not str:
        return None
    if type(snapshot) is dict and "content" in snapshot:
        if set(snapshot) <= {"role", "content"} and snapshot["content"] == content:
            return ("user", content)
        return None
    if type(snapshot) is not dict:
        return None
    if set(snapshot) - {"native_type", "role", "additional_kwargs", "blocks"}:
        return None
    additional = dict.get(snapshot, "additional_kwargs")
    if additional is not None and additional != {}:
        return None
    blocks = dict.get(snapshot, "blocks")
    if type(blocks) is not list or len(blocks) != 1:
        return None
    block = blocks[0]
    if (
        type(block) is dict
        and set(block) <= {"native_type", "block_type", "text"}
        and dict.get(block, "block_type") == "text"
        and dict.get(block, "text") == content
    ):
        return ("user", content)
    return None


def _context_message_semantic(
    message: Any,
    snapshot: Any,
) -> dict[str, Any] | None:
    role_value = native_field(message, "role", None)
    role = native_field(role_value, "_value_", role_value)
    if role not in {"system", "developer", "user", "assistant", "tool"}:
        return None

    content: Any = native_field(message, "content", _UNAVAILABLE)
    if content is _UNAVAILABLE or is_unsafe_accessor_omission(content):
        content = _message_content(message)
    if content is None and type(snapshot) is dict:
        content = dict.get(snapshot, "blocks")
    elif type(content) in {dict, list} and type(snapshot) is dict:
        content = dict.get(snapshot, "content", content)
    if content is _UNAVAILABLE or content is None:
        return None
    if type(content) not in {str, bool, int, float, dict, list}:
        content = native_snapshot(content)
        if not _snapshot_is_complete(content):
            return None

    semantic: dict[str, Any] = {"role": role, "content": content}
    name = native_field(message, "name", None)
    if type(name) is str and name:
        semantic["name"] = name
    additional = native_field(message, "additional_kwargs", None)
    for candidate in (
        native_field(message, "tool_call_id", None),
        native_field(message, "call_id", None),
        native_field(additional, "tool_call_id", None),
        native_field(additional, "call_id", None),
    ):
        if type(candidate) is str and candidate:
            semantic["call_id"] = candidate
            break
    return semantic


def _context_digest(messages: list[_ContextMessage]) -> str:
    digest = hashlib.sha256()
    digest.update(_CONTEXT_DIGEST_DOMAIN)
    for message in messages:
        _update_context_digest(digest, message.native)
    return digest.hexdigest()


def _update_context_digest(digest: Any, value: Any) -> None:
    value_type = type(value)
    if value is None:
        digest.update(b"N")
        return
    if value_type is bool:
        digest.update(b"B1" if value else b"B0")
        return
    if value_type is int:
        _digest_bytes(digest, b"I", str(value).encode("ascii"))
        return
    if value_type is float:
        _digest_bytes(digest, b"F", value.hex().encode("ascii"))
        return
    if value_type is str:
        _digest_bytes(digest, b"S", value.encode("utf-8", errors="surrogatepass"))
        return
    if value_type is bytes:
        _digest_bytes(digest, b"Y", value)
        return
    if value_type is list:
        digest.update(b"L")
        digest.update(len(value).to_bytes(8, "big"))
        for child in value:
            _update_context_digest(digest, child)
        return
    if value_type is dict:
        digest.update(b"D")
        digest.update(len(value).to_bytes(8, "big"))
        for key in sorted(value):
            _digest_bytes(
                digest,
                b"K",
                key.encode("utf-8", errors="surrogatepass"),
            )
            _update_context_digest(digest, value[key])
        return
    raise TypeError("unsupported exact context value")


def _digest_bytes(digest: Any, tag: bytes, value: bytes) -> None:
    digest.update(tag)
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _model_request_semantic(
    serialized: Any,
    invocation: Any,
    context_refs: list[str] | None,
    context_base_ref: str | None,
) -> dict[str, Any]:
    """Project only exposed request configuration into canonical fields."""

    model_data = native_own_data(serialized)
    invocation_data = native_own_data(invocation)
    semantic: dict[str, Any] = {
        "type": "model.request",
        "framework": "llamaindex",
        **({"context_refs": context_refs} if context_refs is not None else {}),
        **({"context_base_ref": context_base_ref} if context_base_ref is not None else {}),
    }
    for name in ("model", "model_name", "model_id"):
        model = dict.get(model_data, name)
        if type(model) is str and model:
            semantic["model"] = model
            break

    raw_tools = next(
        (
            dict.get(invocation_data, name)
            for name in ("tools", "tool_definitions", "functions")
            if name in invocation_data
        ),
        next(
            (
                dict.get(model_data, name)
                for name in ("tools", "tool_definitions", "functions")
                if name in model_data
            ),
            None,
        ),
    )
    normalized_tools = _plain_json(raw_tools)
    if type(normalized_tools) is list:
        definitions: list[dict[str, Any]] = []
        names: list[str] = []
        for definition in normalized_tools:
            if type(definition) is not dict:
                continue
            function = dict.get(definition, "function")
            target = function if type(function) is dict else definition
            tool_name = dict.get(target, "name")
            if type(tool_name) is not str or not tool_name:
                continue
            definitions.append(definition)
            if tool_name not in names:
                names.append(tool_name)
        if names:
            semantic["tools"] = names
        if definitions:
            semantic["tool_definitions"] = definitions

    settings: dict[str, Any] = {}
    for name in ("provider", "provider_name", "api_type"):
        provider = (
            dict.get(invocation_data, name)
            if name in invocation_data
            else dict.get(model_data, name)
        )
        if type(provider) is str and provider:
            settings["provider"] = provider
            break
    for name in (
        "class_name",
        "temperature",
        "top_p",
        "max_tokens",
        "max_completion_tokens",
        "num_output",
        "context_window",
        "tool_choice",
        "stop",
        "seed",
        "frequency_penalty",
        "presence_penalty",
        "response_format",
        "reasoning_effort",
        "is_chat_model",
        "is_function_calling_model",
        "system_role",
    ):
        source = invocation_data if name in invocation_data else model_data
        if name not in source:
            continue
        value = _plain_json(dict.get(source, name))
        if value is not _UNAVAILABLE:
            settings[name] = value
    if settings:
        semantic["settings"] = settings
    return semantic


_UNAVAILABLE = object()


def _plain_json(value: Any) -> Any:
    """Return bounded JSON data without traversing framework object internals."""

    budget = _PlainJsonBudget(
        bytes_left=_MAX_PLAIN_JSON_BYTES,
        nodes_left=_MAX_PLAIN_JSON_NODES,
    )
    result = _plain_json_value(value, budget, depth=0)
    return _UNAVAILABLE if budget.exhausted else result


def _plain_json_value(value: Any, budget: _PlainJsonBudget, *, depth: int) -> Any:
    if depth > 12:
        return _UNAVAILABLE
    if not budget.take(node_count=1):
        return _UNAVAILABLE
    if value is None:
        return value
    if type(value) is str:
        byte_count = _bounded_utf8_size(value, budget.bytes_left)
        if byte_count is None:
            budget.exhausted = True
            return _UNAVAILABLE
        return value if budget.take(byte_count=byte_count) else _UNAVAILABLE
    if type(value) in {bool, int}:
        return value if budget.take(byte_count=len(str(value))) else _UNAVAILABLE
    if type(value) is float:
        if not math.isfinite(value):
            return _UNAVAILABLE
        return value if budget.take(byte_count=len(str(value))) else _UNAVAILABLE
    enum_value = native_field(value, "_value_", _UNAVAILABLE)
    if enum_value is not _UNAVAILABLE and enum_value is not value:
        return _plain_json_value(enum_value, budget, depth=depth + 1)
    if type(value) is dict:
        if len(value) > _MAX_PLAIN_JSON_WIDTH:
            budget.exhausted = True
            return _UNAVAILABLE
        result: dict[str, Any] = {}
        for key, child in dict.items(value):
            if type(key) is not str:
                continue
            key_bytes = _bounded_utf8_size(key, budget.bytes_left)
            if key_bytes is None or not budget.take(byte_count=key_bytes):
                budget.exhausted = True
                return _UNAVAILABLE
            normalized = _plain_json_value(child, budget, depth=depth + 1)
            if budget.exhausted:
                return _UNAVAILABLE
            if normalized is not _UNAVAILABLE:
                result[key] = normalized
        return result
    if type(value) in {list, tuple}:
        if len(value) > _MAX_PLAIN_JSON_WIDTH:
            budget.exhausted = True
            return _UNAVAILABLE
        result_list = []
        for child in value:
            normalized = _plain_json_value(child, budget, depth=depth + 1)
            if budget.exhausted:
                return _UNAVAILABLE
            if normalized is _UNAVAILABLE:
                continue
            result_list.append(normalized)
        return result_list
    return _UNAVAILABLE


def _model_response_semantic(
    response: Any,
    usage: Any,
    streamed_reasoning: list[str] | None = None,
) -> dict[str, Any]:
    message = native_field(response, "message", None)
    result = _message_response_semantic(message, streamed_reasoning)
    content = _message_content(message)
    if content is None:
        text = native_field(response, "text", None)
        if type(text) is str:
            content = text
    if content is not None:
        result["content"] = content
    finish_reason = native_field(response, "finish_reason", None)
    if type(finish_reason) is str and finish_reason:
        result["finish_reason"] = finish_reason
    normalized_usage = _semantic_usage(usage)
    if normalized_usage:
        result["usage"] = normalized_usage
    return result


def _message_response_semantic(
    message: Any,
    streamed_reasoning: list[str] | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"status": "completed"}
    content = _message_content(message)
    if content is not None:
        result["content"] = content
    reasoning = _message_reasoning(message)
    if not reasoning and streamed_reasoning:
        reasoning_text = "".join(streamed_reasoning)
        if reasoning_text:
            reasoning = [{"type": "text", "text": reasoning_text}]
    if reasoning:
        result["reasoning"] = reasoning
    return result


def _message_reasoning(message: Any) -> list[dict[str, str]]:
    blocks = native_field(message, "blocks", None)
    if type(blocks) not in {list, tuple}:
        return []
    reasoning: list[dict[str, str]] = []
    for block in blocks:
        if native_field(block, "block_type", None) != "thinking":
            continue
        content = native_field(block, "content", None)
        if type(content) is str and content:
            reasoning.append({"type": "text", "text": content})
    return reasoning


def _message_unavailable_reasoning(message: Any) -> int:
    blocks = native_field(message, "blocks", None)
    if type(blocks) not in {list, tuple}:
        return 0
    count = 0
    for block in blocks:
        if native_field(block, "block_type", None) != "thinking":
            continue
        content = native_field(block, "content", None)
        if type(content) is str and content:
            continue
        num_tokens = native_field(block, "num_tokens", None)
        if type(num_tokens) is int and num_tokens > 0:
            count += 1
            continue
        additional = native_field(block, "additional_information", None)
        if type(additional) is not dict:
            continue
        if any(
            additional.get(name) not in (None, "", b"")
            for name in ("encrypted_content", "redacted_content", "signature")
        ):
            count += 1
    return count


def _message_content(message: Any) -> str | None:
    content = native_field(message, "content", None)
    if type(content) is str:
        return content
    blocks = native_field(message, "blocks", None)
    if type(blocks) not in {list, tuple}:
        return None
    text = [
        value
        for block in blocks
        if type(value := native_field(block, "text", None)) is str
    ]
    return "\n".join(text) if text else None


def _semantic_usage(value: Any) -> dict[str, int]:
    if type(value) is not dict:
        return {}
    result: dict[str, int] = {}
    for names, target in (
        (("input_tokens", "prompt_tokens"), "input_tokens"),
        (("output_tokens", "completion_tokens"), "output_tokens"),
    ):
        for name in names:
            candidate = dict.get(value, name)
            if type(candidate) is int and candidate >= 0:
                result[target] = candidate
                break
    return result


def _tool_name(value: Any) -> str | None:
    if type(value) is str and value:
        return value
    for field_name in ("name", "tool_name"):
        candidate = native_field(value, field_name, None)
        if type(candidate) is str and candidate:
            return candidate
    metadata = native_field(value, "metadata", None)
    candidate = native_field(metadata, "name", None)
    return candidate if type(candidate) is str and candidate else None


def _is_function_tool_call_span(span_id: str, instance: Any) -> bool:
    prefix = type(instance).__name__
    return span_id.startswith(f"{prefix}.call-") or span_id.startswith(
        f"{prefix}.acall-"
    )


def _function_tool_input(bound_args: Any, instance: Any) -> Any:
    arguments = bound_args.arguments
    if type(arguments) is not dict:
        return None
    positional = dict.get(arguments, "args", ())
    keyword = dict.get(arguments, "kwargs", {})
    defaults = native_field(instance, "_field_defaults", {})
    partial = instance.partial_params
    merged_keyword = (
        {**defaults, **partial, **keyword}
        if type(defaults) is dict
        and type(partial) is dict
        and type(keyword) is dict
        else keyword
    )
    context_name = instance.ctx_param_name
    if type(merged_keyword) is dict and type(context_name) is str:
        merged_keyword = {
            key: value for key, value in dict.items(merged_keyword) if key != context_name
        }
    if type(positional) in {list, tuple} and not positional:
        return native_snapshot(merged_keyword)
    return native_snapshot({"args": positional, "kwargs": merged_keyword})


def _function_tool_result_input(result: Any, fallback: Any) -> Any:
    raw_input = native_field(result, "raw_input", None)
    if type(raw_input) is not dict:
        return fallback
    positional = dict.get(raw_input, "args", ())
    keyword = dict.get(raw_input, "kwargs", {})
    if type(positional) in {list, tuple} and not positional and type(keyword) is dict:
        return native_snapshot(keyword)
    return native_snapshot(raw_input)


def _semantic_error(error: Any) -> dict[str, Any]:
    native_type: Any
    message: Any
    if isinstance(error, BaseException):
        native_type = type(error).__name__
        args = object.__getattribute__(error, "args")
        message = args[0] if args and type(args[0]) is str else None
    else:
        snapshot = native_snapshot(error)
        if type(snapshot) is dict:
            native_type = dict.get(snapshot, "type")
            message = dict.get(snapshot, "message")
        else:
            native_type = None
            message = snapshot if type(snapshot) is str else None
    if not (type(native_type) is str and native_type):
        native_type = type(error).__name__
    normalized_type = "".join(
        character.lower() if character.isalnum() or character in "._-" else "_"
        for character in native_type
    ).strip("._-")
    if len(normalized_type) < 3 or not normalized_type[0].isalpha():
        normalized_type = "unknown_error"
    if not (type(message) is str and message):
        message = normalized_type
    return {
        "type": normalized_type[:127],
        "message": message[:4096],
        "recoverable": False,
    }


def _run_status(trace: _Trace) -> str:
    if trace.terminal_status == "cancelled":
        return "cancelled"
    if trace.terminal_status == "unobserved":
        return "unknown"
    if trace.failed:
        return "failed"
    return "succeeded"


def _tool_proposals(response: Any) -> list[dict[str, Any]]:
    """Extract provider-proposed calls without invoking framework accessors."""

    candidates: Any = None
    message = native_field(response, "message", None)
    for container in (message, response):
        additional = native_field(container, "additional_kwargs", None)
        if type(additional) is not dict:
            continue
        for name in ("semantic_tool_calls", "tool_calls"):
            value = dict.get(additional, name)
            if type(value) in {list, tuple} and value:
                candidates = value
                break
        if candidates is not None:
            break
    if candidates is None:
        found: list[Any] = []
        _collect_tool_calls(native_field(response, "raw", None), found, set())
        candidates = found

    proposals = []
    for candidate in candidates or []:
        normalized = _normalize_tool_proposal(candidate)
        if normalized is not None:
            proposals.append(normalized)
    return proposals


def _collect_tool_calls(value: Any, result: list[Any], seen: set[int], depth: int = 0) -> None:
    if depth > 24 or len(seen) > 5_000:
        return
    if type(value) not in {dict, list, tuple}:
        return
    identity = id(value)
    if identity in seen:
        return
    seen.add(identity)
    if type(value) is dict:
        for key, child in dict.items(value):
            if key in {"semantic_tool_calls", "tool_calls"} and type(child) in {
                list,
                tuple,
            }:
                result.extend(child)
            else:
                _collect_tool_calls(child, result, seen, depth + 1)
        return
    for child in value:
        _collect_tool_calls(child, result, seen, depth + 1)


def _normalize_tool_proposal(value: Any) -> dict[str, Any] | None:
    call_id = native_field(value, "tool_id", None)
    if type(call_id) is not str or not call_id:
        call_id = native_field(value, "id", None)
    if type(call_id) is not str or not call_id:
        call_id = native_field(value, "call_id", None)

    name = native_field(value, "tool_name", None)
    if type(name) is not str or not name:
        name = native_field(value, "name", None)
    arguments = native_field(value, "tool_kwargs", None)
    if arguments is None:
        arguments = native_field(value, "arguments", None)

    function = native_field(value, "function", None)
    if function is not None and not is_unsafe_accessor_omission(function):
        function_name = native_field(function, "name", None)
        function_arguments = native_field(function, "arguments", None)
        if type(function_name) is str and function_name:
            name = function_name
        if function_arguments is not None:
            arguments = function_arguments

    if type(arguments) is str:
        try:
            parsed = json.loads(arguments)
        except (json.JSONDecodeError, TypeError):
            pass
        else:
            arguments = parsed
    if not (
        (type(call_id) is str and call_id) or (type(name) is str and name) or arguments is not None
    ):
        return None
    return {
        "call_id": call_id if type(call_id) is str else None,
        "name": name if type(name) is str else None,
        "input": native_snapshot(arguments),
        "proposal": native_snapshot(value),
    }


def _is_cancellation(error: BaseException | None) -> bool:
    if isinstance(error, (asyncio.CancelledError, GeneratorExit)):
        return True
    try:
        from workflows.errors import (  # type: ignore[import-not-found,unused-ignore]
            WorkflowCancelledByUser,
        )
    except ImportError:
        return False
    return isinstance(error, WorkflowCancelledByUser)


def _parent(receipt: AdmissionReceipt | None) -> dict[str, str]:
    return (
        {"parent_record_id": receipt.record_id}
        if receipt and receipt.accepted and receipt.record_id
        else {}
    )


def _callback_manager(client: object) -> object:
    """Resolve LlamaIndex's documented callback manager attachment shapes."""

    from llama_index.core.callbacks import (  # type: ignore[import-not-found,unused-ignore]
        CallbackManager,
    )

    if isinstance(client, CallbackManager):
        return client
    llm = native_field(client, "llm", None)
    manager = native_field(llm, "callback_manager", None)
    if isinstance(manager, CallbackManager):
        return manager
    raise TypeError(
        "LlamaIndex instrumentation requires a CallbackManager or an agent "
        "with llm.callback_manager"
    )


def llamaindex_adapter(*, version: str | None = None) -> _Adapter:
    """Create the official LlamaIndex callback adapter for the installed version."""

    return _Adapter(_installed_version("llama-index-core", version))
