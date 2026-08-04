"""Official Microsoft Agent Framework middleware capture adapter."""

from __future__ import annotations

import re
import threading
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, cast

from ._adapter_native import native_field, native_snapshot
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


@dataclass
class _ModelCall:
    identity: str
    request: AdmissionReceipt
    text_parts: list[str] = field(default_factory=list)
    reasoning_parts: list[str] = field(default_factory=list)
    reasoning_contexts: list[tuple[str | None, str | None, str]] = field(default_factory=list)
    reasoning_gap_count: int = 0
    usage: dict[str, int] = field(default_factory=dict)
    streamed: bool = False
    closed: bool = False


@dataclass
class _Invocation:
    trace: dict[str, str]
    turn_id: str
    root: AdmissionReceipt
    conversation: str = ""
    session_key: int = 0
    model: AdmissionReceipt | None = None
    model_response: AdmissionReceipt | None = None
    observed_output: AdmissionReceipt | None = None
    error_record: AdmissionReceipt | None = None
    model_index: int = 0
    active_models: dict[str, _ModelCall] = field(default_factory=dict)
    context_records: dict[int, tuple[Any, Any, AdmissionReceipt]] = field(default_factory=dict)
    model_context_records: dict[int, tuple[Any, Any, AdmissionReceipt]] = field(
        default_factory=dict
    )
    proposals: dict[str, AdmissionReceipt] = field(default_factory=dict)
    tools: dict[str, AdmissionReceipt] = field(default_factory=dict)
    tool_results: dict[str, AdmissionReceipt] = field(default_factory=dict)
    tool_result_outputs: dict[str, Any] = field(default_factory=dict)
    observed_call_ids: set[str] = field(default_factory=set)
    recorded_proposal_ids: set[str] = field(default_factory=set)
    text_parts: list[str] = field(default_factory=list)
    usage: dict[str, int] = field(default_factory=dict)
    streamed: bool = False
    closed: bool = False


_CURRENT_INVOCATION: ContextVar[_Invocation | None] = ContextVar(
    "semantic_layer_microsoft_agent_framework_invocation", default=None
)
_OWNED_STRUCTURED_OUTPUT_VERSION = "1.11.0"


class _Lifecycle:
    def __init__(self, deactivate: Any, drain: Any) -> None:
        self._deactivate = deactivate
        self._drain = drain

    def deactivate(self) -> None:
        self._deactivate()

    def drain(self) -> None:
        self._drain()


class _PullContext:
    """Bind one invocation only while its ResponseStream advances."""

    def __init__(self, invocation: _Invocation) -> None:
        self._invocation = invocation
        self._token: Any = None

    def __enter__(self) -> None:
        self._token = _CURRENT_INVOCATION.set(self._invocation)

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        del exc_type, exc, traceback
        _CURRENT_INVOCATION.reset(self._token)


class _Adapter:
    def __init__(self, version: str) -> None:
        self.version = version

    def create_source(self, client: object) -> _Source:
        return _trust_official_source(_Source(client, self.version), "deep")  # type: ignore[return-value]


class _Source(CaptureSource):
    def __init__(self, agent: object, version: str) -> None:
        self.agent = agent
        self._version = version
        self.metadata = {
            "name": "official:microsoft-agent-framework",
            "seam": (
                "AgentMiddleware + ChatMiddleware + FunctionMiddleware"
                "/AgentResponseUpdate"
                "+ResponseStream.with_pull_context_manager"
                "+exact ResponseStream._stream_error"
            ),
            "identity_domain": "agent-framework.run",
            "version": version,
            "qualification": _source_qualification(
                version,
                exact_versions=frozenset({"1.11.0"}),
                profile="microsoft-agent-framework-python-adapter-v1",
            ),
            "official": True,
            "coverage": [
                {"operation": "agent-run", "domain": "agent-framework.run", "role": "owner"}
            ],
        }
        self._sink: Any = None
        self._accepting = False
        self._guard = threading.RLock()
        self._settled = threading.Condition(self._guard)
        self._inflight = 0
        self._hook_inflight = 0
        self._last_turn: dict[str, str] = {}
        self._turn_order: dict[str, int] = {}
        self._failed_turns: set[str] = set()
        self._invocations: dict[str, _Invocation] = {}

    def install(self, sink: Any) -> _Lifecycle:
        if self._accepting:
            raise RuntimeError("Microsoft Agent Framework source is already installed")
        from agent_framework import (  # type: ignore[import-not-found,unused-ignore]
            AgentMiddleware,
            ChatMiddleware,
            FunctionMiddleware,
        )

        source = self

        class Middleware(AgentMiddleware):  # type: ignore[misc,unused-ignore]
            async def process(self, context: Any, call_next: Any) -> None:
                await source._process(context, call_next)

        class ToolMiddleware(FunctionMiddleware):  # type: ignore[misc,unused-ignore]
            async def process(self, context: Any, call_next: Any) -> None:
                await source._process_function(context, call_next)

        class ProviderMiddleware(ChatMiddleware):  # type: ignore[misc,unused-ignore]
            async def process(self, context: Any, call_next: Any) -> None:
                await source._process_chat(context, call_next)

        middleware = Middleware()
        provider_middleware = ProviderMiddleware()
        tool_middleware = ToolMiddleware()
        prior = _field(self.agent, "middleware", None)
        if prior is None:
            replacement: list[Any] = []
            setattr(self.agent, "middleware", replacement)
            target = replacement
        elif isinstance(prior, list):
            target = prior
        else:
            raise TypeError("agent middleware must be a mutable list")
        self._sink = sink
        self._accepting = True
        try:
            target[0:0] = [middleware, provider_middleware, tool_middleware]
        except BaseException:
            self._accepting = False
            if tool_middleware in target:
                target.remove(tool_middleware)
            if provider_middleware in target:
                target.remove(provider_middleware)
            if middleware in target:
                target.remove(middleware)
            if prior is None and _field(self.agent, "middleware", None) is target:
                setattr(self.agent, "middleware", None)
            raise

        def deactivate() -> None:
            with self._guard:
                self._accepting = False
                if middleware in target:
                    target.remove(middleware)
                if provider_middleware in target:
                    target.remove(provider_middleware)
                if tool_middleware in target:
                    target.remove(tool_middleware)
                if (
                    prior is None
                    and _field(self.agent, "middleware", None) is target
                    and not target
                ):
                    setattr(self.agent, "middleware", None)
                for invocation in list(self._invocations.values()):
                    self._cancel(invocation, "capture shutdown")

        def drain() -> None:
            with self._settled:
                while self._inflight or self._hook_inflight:
                    self._settled.wait(timeout=0.05)

        return _Lifecycle(deactivate, drain)

    async def _process(self, context: Any, call_next: Any) -> None:
        metadata = dict(_field(context, "metadata", {}) or {})
        conversation = str(
            metadata.get("conversation_id") or _session_id(context) or "agent-framework"
        )
        with self._guard:
            if not self._accepting:
                admitted = False
                turn_index = 0
                turn_id = ""
                previous = None
            else:
                admitted = True
                allocated = self._turn_order.get(conversation, 0)
                explicit_index = metadata.get("turn_index")
                turn_index = explicit_index if type(explicit_index) is int else allocated
                explicit_turn = metadata.get("turn_id")
                turn_id = (
                    explicit_turn
                    if type(explicit_turn) is str and explicit_turn
                    else f"{conversation}:turn:{allocated}"
                )
                previous = self._last_turn.get(conversation)
                self._turn_order[conversation] = max(allocated + 1, turn_index + 1)
                self._last_turn[conversation] = turn_id
                self._inflight += 1
        if not admitted:
            await call_next()
            return
        agent = _agent_projection(_field(context, "agent", None))
        options = _options_projection(native_field(context, "options", _MISSING))
        tools = _projection_items(native_field(context, "tools", ()), _tool_projection)
        session_snapshot = native_snapshot(_field(context, "session", None))
        session = {
            "session_id": _session_id(context),
            "snapshot": session_snapshot,
            "history": _session_history(session_snapshot),
        }
        opened = self._sink.open_trace(
            {
                "name": "agent_framework.run",
                "native_identity": turn_id,
                "native": {
                    "agent": agent,
                    "messages": native_snapshot(_field(context, "messages", [])),
                    "options": options,
                    "session": session,
                    "stream": bool(_field(context, "stream", False)),
                    "metadata": native_snapshot(metadata),
                    "tools": tools,
                },
                "semantic": {
                    "type": "agent.run",
                    "framework": "agent-framework",
                    "name": "agent_framework.run",
                },
                "conversation_id": conversation,
                "turn_id": turn_id,
                "turn_index": turn_index,
                **({"previous_turn_id": previous} if previous and previous != turn_id else {}),
            }
        )
        if not opened.accepted or opened.identity is None:
            try:
                await call_next()
            finally:
                self._settle()
            return
        if previous in self._failed_turns:
            self._sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "agent_framework.recovery",
                    "trace": opened.identity,
                    "native_identity": turn_id,
                    "native": {"attempt": 2, "previous_turn_id": previous},
                    "semantic": {
                        "type": "state.transition",
                        "framework": "agent-framework",
                        "state_type": "agent.recovery",
                        "value": {"previous_turn_id": previous},
                    },
                }
            )
        streamed = bool(_field(context, "stream", False))
        invocation = _Invocation(
            opened.identity,
            turn_id,
            opened,
            conversation=conversation,
            session_key=_object_identity(_field(context, "session", None)),
            streamed=streamed,
        )
        with self._guard:
            self._invocations[turn_id] = invocation

        async def transform(update: Any) -> Any:
            if not self._begin_hook(invocation):
                return update
            try:
                self._on_update(invocation, update)
            finally:
                self._end_hook()
            return update

        async def final(result: Any) -> Any:
            self._finish(invocation, result, conversation)
            return result

        async def cleanup() -> None:
            stream = _field(context, "result", None)
            # Agent Framework exposes no public stream-error callback. Its own
            # observability layer reads this exact field. Both supported package
            # trees are authority-frozen and exact-version tests exercise it.
            error = _field(stream, "_stream_error", None)
            if isinstance(error, BaseException):
                self._fail(invocation, error, conversation)

        if streamed:
            _field(context, "stream_transform_hooks", []).append(transform)
            _field(context, "stream_result_hooks", []).append(final)
            _field(context, "stream_cleanup_hooks", []).append(cleanup)
        if streamed:
            try:
                await call_next()
            except BaseException as error:
                self._fail(invocation, error, conversation)
                raise
            stream = _field(context, "result", None)
            bind_pull = _field(stream, "with_pull_context_manager", None)
            if callable(bind_pull):
                bind_pull(lambda: _PullContext(invocation))
        else:
            token = _CURRENT_INVOCATION.set(invocation)
            try:
                await call_next()
            except BaseException as error:
                self._fail(invocation, error, conversation)
                raise
            finally:
                _CURRENT_INVOCATION.reset(token)
            self._finish(invocation, _field(context, "result", None), conversation)

    def _record_model_context(
        self,
        invocation: _Invocation,
        identity: str,
        messages: Any,
    ) -> list[str]:
        snapshot = native_snapshot(messages)
        if not isinstance(snapshot, list) or type(messages) not in {list, tuple}:
            return []
        refs: list[str] = []
        for index, (raw_message, message) in enumerate(zip(messages, snapshot, strict=False)):
            if not isinstance(message, dict):
                continue
            role = message.get("role")
            if role not in {"system", "developer", "user", "assistant", "tool"}:
                continue
            prior = self._prior_context_records(invocation, role, raw_message, message)
            prior_ids = [
                receipt.record_id
                for receipt in prior
                if receipt.accepted and receipt.record_id is not None
            ]
            if prior_ids:
                refs.extend(record_id for record_id in prior_ids if record_id not in refs)
                continue
            cached = invocation.context_records.get(id(raw_message))
            if (
                cached is not None
                and cached[0] is raw_message
                and cached[1] == message
                and cached[2].accepted
                and cached[2].record_id is not None
            ):
                refs.append(cached[2].record_id)
                continue
            if "contents" in message:
                content = _compact_message_content(message["contents"])
            elif "content" in message:
                content = message["content"]
            else:
                continue
            receipt = self._sink.record(
                {
                    "kind": "log",
                    "phase": "event",
                    "name": "agent_framework.message",
                    "trace": invocation.trace,
                    "native_identity": f"{identity}:message:{index}",
                    "native": message,
                    "semantic": {
                        "type": "message",
                        "framework": "agent-framework",
                        "role": role,
                        "content": content,
                    },
                }
            )
            if receipt.accepted and receipt.record_id is not None:
                invocation.context_records[id(raw_message)] = (
                    raw_message,
                    message,
                    receipt,
                )
                refs.append(receipt.record_id)
        return refs

    @staticmethod
    def _prior_context_records(
        invocation: _Invocation,
        role: str,
        message: Any,
        message_snapshot: dict[str, Any],
    ) -> list[AdmissionReceipt]:
        raw_contents = native_field(message, "contents", None)
        if is_unsafe_accessor_omission(raw_contents) or type(raw_contents) not in {
            list,
            tuple,
        }:
            return []
        records: list[AdmissionReceipt] = []
        for content in raw_contents:
            content_type = native_field(content, "type", None)
            if role == "assistant" and content_type == "function_call":
                prior = invocation.model_context_records.get(id(message))
                if prior is not None and prior[0] is message and prior[1] == message_snapshot:
                    return [prior[2]]
            if role == "tool" and content_type == "function_result":
                call_id = native_field(content, "call_id", None)
                if isinstance(call_id, str) and call_id:
                    result = invocation.tool_results.get(call_id)
                    observed_output = invocation.tool_result_outputs.get(call_id, _MISSING)
                    visible_output = native_snapshot(native_field(content, "result", None))
                    if (
                        result is not None
                        and observed_output is not _MISSING
                        and visible_output == observed_output
                        and result not in records
                    ):
                        records.append(result)
        return records

    async def _process_chat(self, context: Any, call_next: Any) -> None:
        invocation = self._context_invocation(context)
        if invocation is None or not self._begin_hook(invocation):
            await call_next()
            return

        with self._guard:
            model_index = invocation.model_index
            invocation.model_index += 1
        identity = f"{invocation.turn_id}:model:{model_index}"
        messages = _field(context, "messages", [])
        options_value = native_field(context, "options", _MISSING)
        options = _options_projection(options_value)
        tools = _projection_items(
            dict.get(options_value, "tools", ()) if type(options_value) is dict else (),
            _tool_projection,
        )
        context_refs = self._record_model_context(invocation, identity, messages)
        model_name = _chat_model(context, options_value)
        request = self._sink.record(
            {
                "kind": "model",
                "phase": "start",
                "name": "agent_framework.model",
                "trace": invocation.trace,
                "native_identity": identity,
                "native": {
                    "messages": native_snapshot(messages),
                    "options": options,
                    "model": model_name,
                    "stream": bool(_field(context, "stream", False)),
                    "tools": tools,
                },
                "semantic": {
                    "type": "model.request",
                    "framework": "agent-framework",
                    "model": model_name,
                    "context_refs": context_refs,
                    "tools": _semantic_tool_names(tools),
                },
            }
        )
        model_call = _ModelCall(
            identity=identity,
            request=request,
            streamed=bool(_field(context, "stream", False)),
        )
        with self._guard:
            invocation.model = request
            invocation.active_models[identity] = model_call

        async def transform(update: Any) -> Any:
            if not self._begin_hook(invocation):
                return update
            try:
                self._on_update(invocation, update, model_call)
            finally:
                self._end_hook()
            return update

        async def final(result: Any) -> Any:
            self._finish_model(invocation, model_call, result)
            return result

        async def cleanup() -> None:
            stream = _field(context, "result", None)
            error = _field(stream, "_stream_error", None)
            if isinstance(error, BaseException):
                self._fail_model(invocation, model_call, error)

        if model_call.streamed:
            _field(context, "stream_transform_hooks", []).append(transform)
            _field(context, "stream_result_hooks", []).append(final)
            _field(context, "stream_cleanup_hooks", []).append(cleanup)

        try:
            await call_next()
        except BaseException as error:
            self._fail_model(invocation, model_call, error)
            raise
        else:
            if not model_call.streamed:
                self._finish_model(invocation, model_call, _field(context, "result", None))
        finally:
            self._end_hook()

    def _context_invocation(self, context: Any) -> _Invocation | None:
        current = _CURRENT_INVOCATION.get()
        if current is not None and not current.closed:
            return current
        session_key = _object_identity(_field(context, "session", None))
        with self._guard:
            matching = [
                item
                for item in self._invocations.values()
                if not item.closed and item.session_key == session_key
            ]
        return matching[0] if len(matching) == 1 else None

    def _finish_model(self, invocation: _Invocation, model_call: _ModelCall, result: Any) -> None:
        with self._guard:
            if invocation.closed or model_call.closed:
                return
            model_call.closed = True
            invocation.active_models.pop(model_call.identity, None)
            invocation.model = model_call.request
            snapshot = native_snapshot(result)
            raw_usage = _field(result, "usage_details", None)
            semantic_usage = _semantic_usage(raw_usage)
            if not semantic_usage:
                semantic_usage = dict(model_call.usage)
            semantic_response: dict[str, Any] = {
                "type": "model.response",
                "framework": "agent-framework",
                "status": "completed",
            }
            model_name = _field(result, "model", None)
            if isinstance(model_name, str) and model_name:
                semantic_response["model"] = model_name
            finish_reason = _field(result, "finish_reason", None)
            if finish_reason is not None:
                semantic_response["finish_reason"] = str(finish_reason)
            content = _semantic_response_content(result, model_call.text_parts)
            if content is not None:
                semantic_response["content"] = content
            reasoning = _semantic_response_reasoning(
                result,
                model_call.reasoning_contexts,
            )
            if not reasoning and model_call.reasoning_parts:
                reasoning_text = "".join(model_call.reasoning_parts)
                if reasoning_text:
                    reasoning = [{"type": "text", "text": reasoning_text}]
            if reasoning:
                semantic_response["reasoning"] = reasoning
            if semantic_usage:
                semantic_response["usage"] = semantic_usage
            response = self._sink.record(
                {
                    "kind": "model",
                    "phase": "end",
                    "name": "agent_framework.model",
                    "trace": invocation.trace,
                    "native_identity": model_call.identity,
                    "native": {
                        "output": snapshot,
                        **({"usage": native_snapshot(raw_usage)} if raw_usage is not None else {}),
                    },
                    "semantic": semantic_response,
                    **_parent(model_call.request),
                }
            )
            _record_unavailable_reasoning_gap(
                self._sink,
                invocation.trace,
                framework="agent-framework",
                affected=response,
                count=max(
                    model_call.reasoning_gap_count,
                    _semantic_response_unavailable_reasoning(result),
                ),
                detail=(
                    "Agent Framework exposed protected reasoning data without "
                    "readable reasoning text; protected bytes were omitted."
                ),
            )
            invocation.model_response = response
            if content is not None:
                invocation.observed_output = response
            invocation.model = response
            response_messages = native_field(result, "messages", None)
            response_snapshots = native_snapshot(response_messages)
            if type(response_messages) in {list, tuple} and isinstance(response_snapshots, list):
                for raw_message, message_snapshot in zip(
                    response_messages, response_snapshots, strict=False
                ):
                    if isinstance(message_snapshot, dict):
                        invocation.model_context_records[id(raw_message)] = (
                            raw_message,
                            message_snapshot,
                            response,
                        )
            self._record_final_proposals(invocation, result, parent=response)

    def _fail_model(
        self,
        invocation: _Invocation,
        model_call: _ModelCall,
        error: BaseException,
    ) -> None:
        with self._guard:
            if invocation.closed or model_call.closed:
                return
            model_call.closed = True
            invocation.active_models.pop(model_call.identity, None)
            error_data = _finite_error(error, "Agent Framework provider request failed")
            response = self._sink.record(
                {
                    "kind": "model",
                    "phase": "error",
                    "name": "agent_framework.model",
                    "trace": invocation.trace,
                    "native_identity": model_call.identity,
                    "error_identity": error,
                    "native": {"error_ref": f"{model_call.identity}:error"},
                    "semantic": {
                        "type": "model.response",
                        "framework": "agent-framework",
                        "status": "failed",
                        **_streamed_reasoning_semantic(model_call.reasoning_parts),
                    },
                    **_parent(model_call.request),
                }
            )
            _record_unavailable_reasoning_gap(
                self._sink,
                invocation.trace,
                framework="agent-framework",
                affected=response,
                count=model_call.reasoning_gap_count,
                detail=(
                    "Agent Framework exposed protected reasoning data without "
                    "readable reasoning text; protected bytes were omitted."
                ),
            )
            invocation.model_response = response
            invocation.error_record = self._sink.record(
                {
                    "kind": "error",
                    "phase": "event",
                    "name": "agent_framework.provider.error",
                    "trace": invocation.trace,
                    "native_identity": f"{model_call.identity}:error",
                    "error_identity": error,
                    "native": {"error": error_data},
                    "semantic": {
                        "type": "error",
                        "framework": "agent-framework",
                        "error": error_data,
                    },
                    **_parent(response),
                }
            )

    async def _process_function(self, context: Any, call_next: Any) -> None:
        raw_call_id = _field(_field(context, "metadata", {}), "get", None)
        call_id_value = raw_call_id("call_id") if callable(raw_call_id) else None
        call_id = call_id_value if isinstance(call_id_value, str) and call_id_value else "tool"
        with self._guard:
            invocation = self._function_invocation(context, call_id)
            entered = invocation is not None and self._begin_hook(invocation)
        if not entered or invocation is None:
            await call_next()
            return

        function = _field(context, "function", None)
        raw_name = _field(function, "name", None)
        native = {
            "name": raw_name if isinstance(raw_name, str) and raw_name else "tool",
            "input": native_snapshot(_field(context, "arguments", None)),
            "call_id": call_id,
            "function": native_snapshot(function),
        }
        with self._guard:
            proposal = invocation.proposals.get(call_id)
            if proposal is None:
                proposal = self._record_tool_proposal(
                    invocation,
                    call_id=call_id,
                    name=native["name"],
                    input_value=native["input"],
                    native={"function": native["function"], "source": "function_middleware"},
                )
            execution = self._sink.record(
                {
                    "kind": "tool",
                    "phase": "start",
                    "name": "agent_framework.tool",
                    "trace": invocation.trace,
                    "native_identity": call_id,
                    "native": native,
                    "semantic": {
                        "type": "tool.execution",
                        "framework": "agent-framework",
                        "name": native["name"],
                        "input": native["input"],
                        "native_call_id": call_id,
                    },
                    **_parent(proposal),
                }
            )
            invocation.tools[call_id] = execution
        try:
            await call_next()
        except BaseException as error:
            with self._guard:
                if not invocation.closed:
                    self._record_function_error(invocation, call_id, error)
            raise
        else:
            with self._guard:
                if not invocation.closed:
                    self._record_function_result(
                        invocation, call_id, _field(context, "result", None)
                    )
        finally:
            self._end_hook()

    def _function_invocation(self, context: Any, call_id: str) -> _Invocation | None:
        current = self._context_invocation(context)
        if current is not None:
            return current
        active = [item for item in self._invocations.values() if not item.closed]
        proposed = [
            item
            for item in active
            if call_id in item.proposals or call_id in item.observed_call_ids
        ]
        if len(proposed) == 1:
            return proposed[0]
        session_key = _object_identity(_field(context, "session", None))
        matching = [item for item in active if item.session_key == session_key]
        return matching[0] if len(matching) == 1 else None

    def _record_function_result(self, invocation: _Invocation, call_id: str, result: Any) -> None:
        execution = invocation.tools.pop(call_id, None)
        invocation.proposals.pop(call_id, None)
        output = _tool_output_projection(result)
        result_receipt = self._sink.record(
            {
                "kind": "tool",
                "phase": "end",
                "name": "agent_framework.tool",
                "trace": invocation.trace,
                "native_identity": call_id,
                "native": {"output": output, "call_id": call_id},
                "semantic": {
                    "type": "tool.result",
                    "framework": "agent-framework",
                    "native_call_id": call_id,
                    "status": "succeeded",
                    "output": output,
                },
                **_parent(execution),
            }
        )
        invocation.tool_results[call_id] = result_receipt
        invocation.tool_result_outputs[call_id] = output

    def _record_function_error(
        self, invocation: _Invocation, call_id: str, error: BaseException
    ) -> None:
        execution = invocation.tools.pop(call_id, None)
        invocation.proposals.pop(call_id, None)
        error_data = _finite_error(error, "Agent Framework tool execution failed")
        result_receipt = self._sink.record(
            {
                "kind": "tool",
                "phase": "error",
                "name": "agent_framework.tool",
                "trace": invocation.trace,
                "native_identity": call_id,
                "error_identity": error,
                "native": {"error": error_data, "call_id": call_id},
                "semantic": {
                    "type": "tool.error",
                    "framework": "agent-framework",
                    "native_call_id": call_id,
                    "status": "failed",
                    "error": error_data,
                },
                **_parent(execution),
            }
        )
        invocation.tool_results[call_id] = result_receipt
        invocation.tool_result_outputs[call_id] = error_data

    def _on_update(
        self,
        invocation: _Invocation,
        update: Any,
        model_call: _ModelCall | None = None,
    ) -> None:
        if invocation.closed:
            return
        snapshot = native_snapshot(update)
        raw_contents = native_field(update, "contents", [])
        if is_unsafe_accessor_omission(raw_contents):
            with self._guard:
                if not invocation.closed:
                    self._record_update_omissions(invocation, snapshot, [raw_contents])
            return
        contents = list(raw_contents) if type(raw_contents) in {list, tuple} else []
        omissions: list[Any] = []
        observed_contents: list[tuple[Any, str]] = []
        text_parts: list[str] = []
        reasoning_parts: list[str] = []
        reasoning_contexts: list[tuple[str | None, str | None, str]] = []
        reasoning_gap_count = 0
        raw_response_id = _capture_field(update, "response_id", None, omissions)
        response_id = raw_response_id if type(raw_response_id) is str and raw_response_id else None
        raw_message_id = _capture_field(update, "message_id", None, omissions)
        message_id = raw_message_id if type(raw_message_id) is str and raw_message_id else None
        for content in contents:
            content_type = _capture_field(content, "type", "", omissions)
            safe_type = content_type if type(content_type) is str else ""
            observed_contents.append((content, safe_type))
            if safe_type == "text":
                content_text = _capture_field(content, "text", "", omissions)
                if type(content_text) is str:
                    text_parts.append(content_text)
            elif safe_type == "text_reasoning":
                reasoning_text = _capture_field(content, "text", "", omissions)
                if type(reasoning_text) is str:
                    reasoning_parts.append(reasoning_text)
                    reasoning_contexts.append((response_id, message_id, reasoning_text))
                protected_data = native_field(content, "protected_data", None)
                if isinstance(protected_data, (str, bytes)) and protected_data:
                    reasoning_gap_count += 1
        with self._guard:
            if invocation.closed:
                return
            if model_call is not None:
                model_call.reasoning_parts.extend(reasoning_parts)
                model_call.reasoning_contexts.extend(reasoning_contexts)
                model_call.reasoning_gap_count = min(
                    model_call.reasoning_gap_count + reasoning_gap_count,
                    1_000_000,
                )
            self._record_update(
                invocation,
                observed_contents,
                "".join(text_parts),
                snapshot,
                omissions,
                model_call,
            )

    def _record_update(
        self,
        invocation: _Invocation,
        contents: list[tuple[Any, str]],
        text: str,
        snapshot: Any,
        omissions: list[Any],
        model_call: _ModelCall | None,
    ) -> None:
        if text:
            (model_call.text_parts if model_call is not None else invocation.text_parts).append(
                text
            )
        parent = model_call.request if model_call is not None else invocation.model
        stream_native: dict[str, Any] = {"update": snapshot}
        if text:
            stream_native["delta"] = text
        self._sink.record(
            {
                "kind": "stream",
                "phase": "event",
                "name": "agent_framework.response.update",
                "trace": invocation.trace,
                "native_identity": invocation.turn_id,
                "native": stream_native,
                "semantic": {
                    "type": "capture.redundant",
                    "framework": "agent-framework",
                },
                **_parent(parent),
            }
        )
        for content_index, (content, content_type) in enumerate(contents):
            raw_call_id = _capture_field(content, "call_id", None, omissions)
            call_id = raw_call_id if type(raw_call_id) is str and raw_call_id else None
            if content_type == "function_call":
                if call_id is not None:
                    invocation.observed_call_ids.add(call_id)
                # Agent Framework streams function arguments as adjacent Content
                # fragments. Continuation fragments intentionally omit call_id and
                # name; AgentResponse.from_updates coalesces them before tool
                # execution. Recording each update would invent extra proposals.
                # Proposal capture is deferred until FunctionMiddleware exposes
                # exact parsed arguments or the final AgentResponse exposes the
                # framework-coalesced Content.
            elif content_type == "function_result":
                if call_id is None:
                    continue
                execution = invocation.tools.pop(call_id, None)
                proposal = invocation.proposals.pop(call_id, None)
                if execution is None and proposal is None:
                    continue
                raw_result = _capture_field(content, "result", None, omissions)
                output = native_snapshot(raw_result)
                result_receipt = self._sink.record(
                    {
                        "kind": "tool",
                        "phase": "end" if execution is not None else "event",
                        "name": "agent_framework.tool",
                        "trace": invocation.trace,
                        "native_identity": call_id,
                        "native": {
                            "output": output,
                            "call_id": call_id,
                        },
                        "semantic": {
                            "type": "tool.result",
                            "framework": "agent-framework",
                            "native_call_id": call_id,
                            "status": "succeeded",
                            "output": output,
                        },
                        **_parent(execution or proposal),
                    }
                )
                invocation.tool_results[call_id] = result_receipt
                invocation.tool_result_outputs[call_id] = output
            elif content_type == "usage":
                raw_usage = _capture_field(content, "usage_details", None, omissions)
                (model_call.usage if model_call is not None else invocation.usage).update(
                    _semantic_usage(raw_usage)
                )
                self._sink.record(
                    {
                        "kind": "model",
                        "phase": "event",
                        "name": "agent_framework.model.usage",
                        "trace": invocation.trace,
                        "native_identity": invocation.turn_id,
                        "native": {
                            "usage": native_snapshot(raw_usage),
                            "update": snapshot,
                        },
                        "semantic": {
                            "type": "capture.redundant",
                            "framework": "agent-framework",
                        },
                        **_parent(parent),
                    }
                )
        self._record_update_omissions(invocation, snapshot, omissions, parent)

    def _record_tool_proposal(
        self,
        invocation: _Invocation,
        *,
        call_id: str,
        name: str,
        input_value: Any,
        native: dict[str, Any],
        parent: AdmissionReceipt | None = None,
    ) -> AdmissionReceipt:
        existing = invocation.proposals.get(call_id)
        if existing is not None:
            return existing
        proposal = cast(
            AdmissionReceipt,
            self._sink.record(
                {
                    "kind": "tool",
                    "phase": "event",
                    "name": "agent_framework.tool.proposal",
                    "trace": invocation.trace,
                    "native_identity": call_id,
                    "native": {
                        "name": name,
                        "input": input_value,
                        "call_id": call_id,
                        **native,
                    },
                    "semantic": {
                        "type": "tool.proposal",
                        "framework": "agent-framework",
                        "name": name,
                        "input": input_value,
                        "native_call_id": call_id,
                    },
                    **_parent(parent or invocation.model),
                }
            ),
        )
        invocation.proposals[call_id] = proposal
        invocation.recorded_proposal_ids.add(call_id)
        return proposal

    def _record_update_omissions(
        self,
        invocation: _Invocation,
        snapshot: Any,
        omissions: list[Any],
        parent: AdmissionReceipt | None = None,
    ) -> None:
        if not omissions:
            return
        self._sink.record(
            {
                "kind": "unknown",
                "phase": "event",
                "name": "agent_framework.response.update.omission",
                "trace": invocation.trace,
                "native_identity": invocation.turn_id,
                "native": {"fields": omissions, "update": snapshot},
                **_parent(parent or invocation.model),
            }
        )

    def _finish(self, invocation: _Invocation, result: Any, conversation: str) -> None:
        if invocation.closed:
            return
        invocation.closed = True
        self._record_final_proposals(invocation, result, parent=invocation.model_response)
        snapshot = native_snapshot(result)
        output, output_gap = _semantic_run_output(result, self._version)
        if output_gap is not None:
            gap_details = {
                "structured_output_not_materialized": (
                    "AgentResponse.value was still lazy; exact public response text "
                    "was retained without running application validators."
                ),
                "structured_output_projection_unverified_version": (
                    "Structured AgentResponse internals are not qualified for this "
                    "Agent Framework version; exact public response text was retained."
                ),
                "structured_output_owned_value_unavailable": (
                    "Agent Framework marked structured output as materialized but did "
                    "not expose the qualified owned value."
                ),
            }
            self._sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "agent_framework.structured_output.gap",
                    "trace": invocation.trace,
                    "native_identity": f"{invocation.turn_id}:structured-output-gap",
                    "native": {
                        "reason": output_gap,
                        "framework_version": self._version,
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "framework": "agent-framework",
                        "reason": output_gap,
                        "count": 1,
                        "detail": gap_details[output_gap],
                    },
                    **_parent(invocation.model_response or invocation.root),
                }
            )
        terminal: AdmissionReceipt | None = None
        if invocation.streamed:
            terminal_native: dict[str, Any] = {"output": snapshot}
            finish_reason = _field(result, "finish_reason", None)
            if finish_reason is not None:
                terminal_native["finish_reason"] = native_snapshot(finish_reason)
            raw_usage = _field(result, "usage_details", None)
            usage = native_snapshot(raw_usage) if raw_usage is not None else None
            if usage is not None:
                terminal_native["usage"] = usage
            terminal = self._sink.record(
                {
                    "kind": "stream",
                    "phase": "end",
                    "name": "agent_framework.response.update",
                    "trace": invocation.trace,
                    "native_identity": invocation.turn_id,
                    "native": terminal_native,
                    "semantic": {
                        "type": "capture.redundant",
                        "framework": "agent-framework",
                    },
                    **_parent(invocation.model),
                }
            )
        lifecycle_native = {"output": snapshot}
        if terminal is not None and terminal.accepted and terminal.record_id is not None:
            lifecycle_native = {"terminal_ref": terminal.record_id}
        self._sink.record(
            {
                "kind": "lifecycle",
                "phase": "end",
                "name": "agent_framework.run",
                "trace": invocation.trace,
                "native_identity": invocation.turn_id,
                "native": lifecycle_native,
                "semantic": {
                    "type": "agent.run",
                    "framework": "agent-framework",
                    "status": "succeeded",
                    **({"output": output} if output is not _MISSING else {}),
                    **(
                        {"output_ref": invocation.observed_output.record_id}
                        if invocation.observed_output is not None
                        and invocation.observed_output.accepted
                        and invocation.observed_output.record_id is not None
                        else {}
                    ),
                },
                **_parent(invocation.root),
            }
        )
        self._settle()

    def _record_final_proposals(
        self,
        invocation: _Invocation,
        result: Any,
        *,
        parent: AdmissionReceipt | None = None,
    ) -> None:
        messages = native_field(result, "messages", None)
        if is_unsafe_accessor_omission(messages) or type(messages) not in {list, tuple}:
            return
        for message_index, message in enumerate(messages):
            contents = native_field(message, "contents", None)
            if is_unsafe_accessor_omission(contents) or type(contents) not in {
                list,
                tuple,
            }:
                continue
            for content_index, content in enumerate(contents):
                if native_field(content, "type", None) != "function_call":
                    continue
                call_id = native_field(content, "call_id", None)
                name = native_field(content, "name", None)
                if (
                    not isinstance(call_id, str)
                    or not call_id
                    or call_id in invocation.recorded_proposal_ids
                    or not isinstance(name, str)
                    or not name
                ):
                    continue
                arguments = native_snapshot(native_field(content, "arguments", None))
                self._record_tool_proposal(
                    invocation,
                    call_id=call_id,
                    name=name,
                    input_value=arguments,
                    native={
                        "message_index": message_index,
                        "content_index": content_index,
                        "source": "finalized_response",
                    },
                    parent=parent,
                )

    def _fail(self, invocation: _Invocation, error: BaseException, conversation: str) -> None:
        if invocation.closed:
            return
        invocation.closed = True
        if invocation.error_record is None:
            error_data = _finite_error(error, "Agent Framework execution failed")
            invocation.error_record = self._sink.record(
                {
                    "kind": "error",
                    "phase": "event",
                    "name": "agent_framework.error",
                    "trace": invocation.trace,
                    "native_identity": f"{invocation.turn_id}:error",
                    "error_identity": error,
                    "native": {"error": error_data},
                    "semantic": {
                        "type": "error",
                        "framework": "agent-framework",
                        "error": error_data,
                    },
                    **_parent(invocation.model_response or invocation.model),
                }
            )
        self._sink.record(
            {
                "kind": "state",
                "phase": "error",
                "name": "agent_framework.state",
                "trace": invocation.trace,
                "native_identity": invocation.turn_id,
                "native": {
                    "state": "failed",
                    **(
                        {"error_ref": invocation.error_record.record_id}
                        if invocation.error_record.accepted
                        and invocation.error_record.record_id is not None
                        else {}
                    ),
                },
                "semantic": {
                    "type": "state.transition",
                    "framework": "agent-framework",
                    "state_type": "agent.status",
                    "value": "failed",
                },
            }
        )
        self._sink.record(
            {
                "kind": "lifecycle",
                "phase": "error",
                "name": "agent_framework.run",
                "trace": invocation.trace,
                "native_identity": invocation.turn_id,
                "error_identity": error,
                "native": {
                    **(
                        {"error_ref": invocation.error_record.record_id}
                        if invocation.error_record.accepted
                        and invocation.error_record.record_id is not None
                        else {}
                    )
                },
                "semantic": {
                    "type": "agent.run",
                    "framework": "agent-framework",
                    "status": "failed",
                    **(
                        {"output_ref": invocation.observed_output.record_id}
                        if invocation.observed_output is not None
                        and invocation.observed_output.accepted
                        and invocation.observed_output.record_id is not None
                        else {}
                    ),
                },
                **_parent(invocation.root),
            }
        )
        self._failed_turns.add(invocation.turn_id)
        self._settle()

    def _cancel(self, invocation: _Invocation, reason: str) -> None:
        if invocation.closed:
            return
        invocation.closed = True
        native = {"reason": reason}
        for call_id, execution in list(invocation.tools.items()):
            self._sink.record(
                {
                    "kind": "tool",
                    "phase": "cancelled",
                    "name": "agent_framework.tool",
                    "trace": invocation.trace,
                    "native_identity": call_id,
                    "native": {"call_id": call_id, **native},
                    "semantic": {
                        "type": "tool.result",
                        "framework": "agent-framework",
                        "native_call_id": call_id,
                        "status": "cancelled",
                    },
                    **_parent(execution),
                }
            )
        invocation.tools.clear()
        invocation.proposals.clear()
        for model_call in list(invocation.active_models.values()):
            model_call.closed = True
            self._sink.record(
                {
                    "kind": "model",
                    "phase": "cancelled",
                    "name": "agent_framework.model",
                    "trace": invocation.trace,
                    "native_identity": model_call.identity,
                    "native": native,
                    "semantic": {
                        "type": "model.response",
                        "framework": "agent-framework",
                        "status": "cancelled",
                        **_streamed_reasoning_semantic(model_call.reasoning_parts),
                    },
                    **_parent(model_call.request),
                }
            )
        invocation.active_models.clear()
        self._sink.record(
            {
                "kind": "state",
                "phase": "cancelled",
                "name": "agent_framework.state",
                "trace": invocation.trace,
                "native_identity": invocation.turn_id,
                "native": {"state": "cancelled", **native},
                "semantic": {
                    "type": "state.transition",
                    "framework": "agent-framework",
                    "state_type": "agent.status",
                    "value": "cancelled",
                },
            }
        )
        self._sink.record(
            {
                "kind": "lifecycle",
                "phase": "cancelled",
                "name": "agent_framework.run",
                "trace": invocation.trace,
                "native_identity": invocation.turn_id,
                "native": native,
                "semantic": {
                    "type": "agent.run",
                    "framework": "agent-framework",
                    "status": "cancelled",
                },
                **_parent(invocation.root),
            }
        )
        self._last_turn[invocation.conversation] = invocation.turn_id
        self._settle()

    def _settle(self) -> None:
        with self._settled:
            for turn_id, invocation in list(self._invocations.items()):
                if invocation.closed:
                    self._invocations.pop(turn_id, None)
            if self._inflight:
                self._inflight -= 1
            self._settled.notify_all()

    def _begin_hook(self, invocation: _Invocation) -> bool:
        with self._settled:
            if invocation.closed:
                return False
            self._hook_inflight += 1
            return True

    def _end_hook(self) -> None:
        with self._settled:
            self._hook_inflight -= 1
            self._settled.notify_all()


def _capture_field(value: Any, name: str, default: Any, omissions: list[Any]) -> Any:
    observed = native_field(value, name, default)
    if is_unsafe_accessor_omission(observed):
        omissions.append(observed)
        return default
    return observed


def _update_context(snapshot: Any) -> dict[str, Any]:
    """Expose exact finite response context used to group partial stream contents."""

    if type(snapshot) is not dict:
        return {}
    return {
        field_name: dict.get(snapshot, field_name)
        for field_name in ("response_id", "message_id", "role", "author_name", "agent_id")
        if field_name in snapshot
    }


_MISSING = object()
_AGENT_TEXT_FIELDS = ("id", "name", "description")
_CLIENT_TEXT_FIELDS = (
    "base_url",
    "azure_endpoint",
    "api_version",
    "instruction_role",
    "org_id",
)
_OPTION_FIELDS = (
    "instructions",
    "temperature",
    "top_p",
    "max_tokens",
    "max_completion_tokens",
    "max_output_tokens",
    "seed",
    "stop",
    "store",
    "tool_choice",
    "allow_multiple_tool_calls",
    "parallel_tool_calls",
    "frequency_penalty",
    "presence_penalty",
    "conversation_id",
    "user",
)
_TOOL_FIELDS = ("name", "description", "approval_mode", "declaration_only")


def _agent_projection(value: Any) -> dict[str, Any]:
    """Project the finite official Agent surface without walking client internals."""

    projected: dict[str, Any] = {"native_type": _native_type(value)}
    for name in _AGENT_TEXT_FIELDS:
        observed = _owned_scalar(value, name)
        if observed is not _MISSING:
            projected[name] = observed

    options = native_field(value, "default_options", _MISSING)
    if is_unsafe_accessor_omission(options):
        options = _MISSING
    client = native_field(value, "client", _MISSING)
    if is_unsafe_accessor_omission(client):
        client = _MISSING

    model = _owned_scalar(client, "model") if client is not _MISSING else _MISSING
    if model is _MISSING and type(options) is dict:
        model = _finite_value(dict.get(options, "model", _MISSING))
    if model is not _MISSING:
        projected["model"] = model

    if client is not _MISSING and client is not None:
        client_projection: dict[str, Any] = {"native_type": _native_type(client)}
        for name in _CLIENT_TEXT_FIELDS:
            observed = _owned_scalar(client, name)
            if observed is not _MISSING:
                client_projection[name] = observed
        projected["client"] = client_projection

    projected_options = _options_projection(options)
    if projected_options:
        projected["options"] = projected_options

    tools = _projection_items(
        dict.get(options, "tools", ()) if type(options) is dict else (),
        _tool_projection,
    )
    tools.extend(_projection_items(native_field(value, "mcp_tools", ()), _tool_projection))
    if tools:
        projected["tools"] = tools

    middleware: list[dict[str, Any]] = []
    seen: set[int] = set()
    for field_name in ("middleware", "agent_middleware"):
        observed = native_field(value, field_name, ())
        if is_unsafe_accessor_omission(observed) or type(observed) not in {list, tuple}:
            continue
        for item in observed:
            identity = id(item)
            if identity in seen:
                continue
            seen.add(identity)
            middleware.append({"native_type": _native_type(item)})
    if middleware:
        projected["middleware"] = middleware

    context_providers = _projection_items(
        native_field(value, "context_providers", ()), _context_provider_projection
    )
    if context_providers:
        projected["context_providers"] = context_providers

    persistence = _owned_scalar(value, "require_per_service_call_history_persistence")
    if persistence is not _MISSING:
        projected["require_per_service_call_history_persistence"] = persistence
    return projected


def _options_projection(value: Any) -> dict[str, Any]:
    if type(value) is not dict:
        return {}
    projected: dict[str, Any] = {}
    for name in _OPTION_FIELDS:
        observed = _finite_value(dict.get(value, name, _MISSING))
        if observed is not _MISSING:
            projected[name] = observed
    return projected


def _tool_projection(value: Any) -> dict[str, Any]:
    projected: dict[str, Any] = {"native_type": _native_type(value)}
    for name in _TOOL_FIELDS:
        observed = _owned_scalar(value, name)
        if observed is not _MISSING:
            projected[name] = observed
    return projected


def _context_provider_projection(value: Any) -> dict[str, Any]:
    projected: dict[str, Any] = {"native_type": _native_type(value)}
    for name in ("name", "source_id"):
        observed = _owned_scalar(value, name)
        if observed is not _MISSING:
            projected[name] = observed
    return projected


def _projection_items(value: Any, projector: Any) -> list[dict[str, Any]]:
    if type(value) not in {list, tuple}:
        return []
    return [projector(item) for item in value]


def _owned_scalar(value: Any, name: str) -> Any:
    observed = native_field(value, name, _MISSING)
    if is_unsafe_accessor_omission(observed):
        return _MISSING
    return _finite_value(observed)


def _finite_value(value: Any) -> Any:
    if value is _MISSING:
        return _MISSING
    if value is None or type(value) in {bool, int, float, str}:
        return native_snapshot(value)
    if type(value) in {list, tuple}:
        projected = [_finite_value(item) for item in value]
        return projected if all(item is not _MISSING for item in projected) else _MISSING
    return _MISSING


def _native_type(value: Any) -> str:
    try:
        return str(type.__getattribute__(type(value), "__name__"))
    except BaseException:
        return "unknown"


def _field(value: Any, name: str, default: Any) -> Any:
    try:
        return object.__getattribute__(value, name)
    except (AttributeError, TypeError):
        return default


def _object_identity(value: Any) -> int:
    return id(value) if value is not None else 0


def _session_id(context: Any) -> str | None:
    session = _field(context, "session", None)
    value = _field(session, "session_id", None)
    return str(value) if value else None


def _session_history(snapshot: Any) -> list[dict[str, str]]:
    if not isinstance(snapshot, dict):
        return []
    state = snapshot.get("state")
    memory = state.get("in_memory") if isinstance(state, dict) else None
    messages = memory.get("messages") if isinstance(memory, dict) else None
    if not isinstance(messages, list):
        return []
    history: list[dict[str, str]] = []
    for message in messages:
        if not isinstance(message, dict) or not isinstance(message.get("role"), str):
            continue
        contents = message.get("contents")
        text = (
            "".join(
                content.get("text", "")
                for content in contents
                if isinstance(content, dict) and isinstance(content.get("text"), str)
            )
            if isinstance(contents, list)
            else ""
        )
        if text:
            history.append({"role": message["role"], "content": text})
    return history


def _semantic_tool_names(tools: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    for tool in tools:
        name = tool.get("name")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


def _chat_model(context: Any, options: Any) -> str | None:
    if type(options) is dict:
        model = dict.get(options, "model")
        if isinstance(model, str) and model:
            return model
    client = native_field(context, "client", None)
    model = native_field(client, "model", None)
    if not is_unsafe_accessor_omission(model) and isinstance(model, str) and model:
        return model
    defaults = native_field(client, "default_options", None)
    if type(defaults) is dict:
        model = dict.get(defaults, "model")
        if isinstance(model, str) and model:
            return model
    return None


def _semantic_usage(value: Any) -> dict[str, int]:
    snapshot = native_snapshot(value)
    if not isinstance(snapshot, dict):
        return {}
    usage: dict[str, int] = {}
    for key in ("input_tokens", "output_tokens"):
        count = snapshot.get(key)
        if isinstance(count, int) and not isinstance(count, bool) and count >= 0:
            usage[key] = count
    return usage


def _semantic_response_content(result: Any, text_parts: list[str]) -> Any:
    messages = native_field(result, "messages", None)
    if not is_unsafe_accessor_omission(messages) and type(messages) in {list, tuple}:
        snapshot = native_snapshot(messages)
        if isinstance(snapshot, list) and snapshot:
            compact: list[dict[str, Any]] = []
            for message in snapshot:
                if not isinstance(message, dict):
                    continue
                role = message.get("role")
                contents = message.get("contents")
                if isinstance(role, str) and contents is not None:
                    compact_content = _compact_message_content(contents)
                    if compact_content in ("", []):
                        continue
                    compact.append(
                        {
                            "role": role,
                            "content": compact_content,
                        }
                    )
            if compact:
                return compact
    text = "".join(text_parts)
    return text if text else None


def _semantic_response_reasoning(
    result: Any,
    streamed: list[tuple[str | None, str | None, str]] | None = None,
) -> list[dict[str, str]]:
    """Project explicit Agent Framework text_reasoning content in message order."""

    messages = native_field(result, "messages", None)
    if is_unsafe_accessor_omission(messages) or type(messages) not in {list, tuple}:
        return []
    response_id = native_field(result, "response_id", None)
    exact_response_id = response_id if isinstance(response_id, str) and response_id else None
    reasoning: list[tuple[str | None, str | None, str]] = []
    for message in messages:
        contents = native_field(message, "contents", None)
        if is_unsafe_accessor_omission(contents) or type(contents) not in {list, tuple}:
            continue
        message_id = native_field(message, "message_id", None)
        exact_message_id = message_id if isinstance(message_id, str) and message_id else None
        for content in contents:
            if native_field(content, "type", None) != "text_reasoning":
                continue
            text = native_field(content, "text", None)
            if isinstance(text, str) and text:
                reasoning.append((exact_response_id, exact_message_id, text))
    if not streamed or not reasoning:
        return [{"type": "text", "text": text} for _, _, text in reasoning]

    streamed_by_identity: dict[tuple[str, str], str] = {}
    for streamed_response_id, streamed_message_id, text in streamed:
        if streamed_response_id is None or streamed_message_id is None:
            continue
        key = (streamed_response_id, streamed_message_id)
        streamed_by_identity[key] = streamed_by_identity.get(key, "") + text

    final_by_identity: dict[tuple[str, str], str] = {}
    for final_response_id, final_message_id, text in reasoning:
        if final_response_id is None or final_message_id is None:
            continue
        key = (final_response_id, final_message_id)
        final_by_identity[key] = final_by_identity.get(key, "") + text

    replacements = {
        key: streamed_text
        for key, streamed_text in streamed_by_identity.items()
        if key in final_by_identity and final_by_identity[key] in streamed_text
    }
    projected: list[dict[str, str]] = []
    replaced: set[tuple[str, str]] = set()
    for final_response_id, final_message_id, text in reasoning:
        final_key = (
            (final_response_id, final_message_id)
            if final_response_id is not None and final_message_id is not None
            else None
        )
        if final_key is not None and final_key in replacements:
            if final_key not in replaced:
                projected.append({"type": "text", "text": replacements[final_key]})
                replaced.add(final_key)
            continue
        projected.append({"type": "text", "text": text})
    return projected


def _semantic_response_unavailable_reasoning(result: Any) -> int:
    messages = native_field(result, "messages", None)
    if is_unsafe_accessor_omission(messages) or type(messages) not in {list, tuple}:
        return 0
    count = 0
    for message in messages:
        contents = native_field(message, "contents", None)
        if is_unsafe_accessor_omission(contents) or type(contents) not in {list, tuple}:
            continue
        for content in contents:
            if native_field(content, "type", None) != "text_reasoning":
                continue
            protected = native_field(content, "protected_data", None)
            if isinstance(protected, (str, bytes)) and protected:
                count += 1
    return count


def _streamed_reasoning_semantic(parts: list[str]) -> dict[str, Any]:
    text = "".join(parts)
    return {"reasoning": [{"type": "text", "text": text}]} if text else {}


def _semantic_run_output(result: Any, framework_version: str) -> tuple[Any, str | None]:
    """Keep the delivered response without repeating its model/tool history."""

    output_gap: str | None = None
    if framework_version == _OWNED_STRUCTURED_OUTPUT_VERSION:
        # AgentResponse.value parses lazily. Read only its already-owned delivered
        # value so capture cannot change application behavior.
        value_parsed = native_field(result, "_value_parsed", _MISSING)
        response_format = native_field(result, "_response_format", _MISSING)
        if value_parsed is True:
            value = native_field(result, "_value", _MISSING)
            if not is_unsafe_accessor_omission(value) and value is not _MISSING:
                return native_snapshot(value), None
            output_gap = "structured_output_owned_value_unavailable"
        elif (
            value_parsed is False
            and not is_unsafe_accessor_omission(response_format)
            and response_format is not _MISSING
            and response_format is not None
        ):
            output_gap = "structured_output_not_materialized"
    else:
        output_gap = "structured_output_projection_unverified_version"

    text = native_field(result, "text", _MISSING)
    if not is_unsafe_accessor_omission(text) and isinstance(text, str):
        return text, output_gap

    messages = native_field(result, "messages", None)
    if is_unsafe_accessor_omission(messages) or type(messages) not in {list, tuple}:
        return _MISSING, output_gap
    message_texts: list[str] = []
    for message in messages:
        contents = native_field(message, "contents", None)
        if is_unsafe_accessor_omission(contents) or type(contents) not in {list, tuple}:
            continue
        content_texts: list[str] = []
        for content in contents:
            if native_field(content, "type", None) != "text":
                continue
            content_text = native_field(content, "text", _MISSING)
            if not is_unsafe_accessor_omission(content_text) and isinstance(content_text, str):
                content_texts.append(content_text)
        message_texts.append(" ".join(content_texts))
    return "".join(message_texts), output_gap


def _compact_message_content(value: Any) -> Any:
    if not isinstance(value, list):
        return value
    parts: list[Any] = []
    all_text = True
    for content in value:
        if not isinstance(content, dict):
            all_text = False
            parts.append(content)
            continue
        content_type = content.get("type")
        if content_type == "text_reasoning":
            continue
        if content_type == "text" and isinstance(content.get("text"), str):
            parts.append(content["text"])
            continue
        all_text = False
        if content_type == "function_call":
            parts.append(
                {
                    "type": "function_call",
                    "call_id": content.get("call_id"),
                    "name": content.get("name"),
                    "arguments": content.get("arguments"),
                }
            )
        elif content_type == "function_result":
            parts.append(
                {
                    "type": "function_result",
                    "call_id": content.get("call_id"),
                    "result": content.get("result"),
                }
            )
        else:
            parts.append({"type": content_type})
    return "".join(parts) if all_text and all(isinstance(part, str) for part in parts) else parts


def _tool_output_projection(value: Any) -> Any:
    """Keep material fields from the framework's common list[Content] result."""

    snapshot = native_snapshot(value)
    if (
        isinstance(snapshot, list)
        and snapshot
        and all(
            isinstance(item, dict) and item.get("native_type") == "Content" for item in snapshot
        )
    ):
        compact = [
            {
                key: field_value
                for key, field_value in item.items()
                if key != "native_type"
                and field_value is not None
                and field_value != {}
                and field_value != []
                and not (key == "informational_only" and field_value is False)
            }
            for item in snapshot
        ]
        if all(
            set(item) == {"type", "text"}
            and item["type"] == "text"
            and isinstance(item["text"], str)
            for item in compact
        ):
            return "".join(item["text"] for item in compact)
        return compact
    return snapshot


def _finite_error(error: BaseException, fallback: str) -> dict[str, Any]:
    native_type = _native_type(error)
    try:
        args = object.__getattribute__(error, "args")
    except (AttributeError, TypeError):
        args = ()
    message = args[0] if args and isinstance(args[0], str) and args[0] else fallback
    normalized_type = re.sub(r"(?<!^)(?=[A-Z])", "_", native_type).lower()
    if re.fullmatch(r"[a-z][a-z0-9._-]{2,127}", normalized_type) is None:
        normalized_type = "agent_framework_error"
    return {
        "type": normalized_type,
        "message": message,
        "recoverable": False,
        "details": {"native_type": native_type},
    }


def _parent(receipt: AdmissionReceipt | None) -> dict[str, str]:
    return (
        {"parent_record_id": receipt.record_id}
        if receipt and receipt.accepted and receipt.record_id
        else {}
    )


def microsoft_agent_framework_adapter(*, version: str | None = None) -> _Adapter:
    """Create the official Agent Framework adapter for the installed version."""

    return _Adapter(_installed_version("agent-framework-core", version))
