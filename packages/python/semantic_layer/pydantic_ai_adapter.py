"""Official pydantic ai adapter capture adapter."""

from __future__ import annotations

import traceback
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any

from ._framework_adapter_shared import (
    _framework_metadata,
    _FrameworkAdapter,
    _installed_version,
    _Lifecycle,
    _owned_field,
    _raw_native,
    _source_qualification,
)
from .capture_v1 import AdmissionReceipt, CaptureSource


def pydantic_ai_adapter(*, version: str | None = None) -> _FrameworkAdapter:
    """Capture PydanticAI without forcing ordinary runs into streaming mode."""

    return _FrameworkAdapter(_installed_version("pydantic-ai", version), _PydanticAISource)


class _PydanticAISource(CaptureSource):
    def __init__(self, agent: object, version: str) -> None:
        self.agent = agent
        self.metadata = _framework_metadata(
            "pydanticai",
            "Agent.run/run_sync wrappers + AbstractCapability model/tool hooks",
            "pydanticai.run",
            version,
        )
        self.metadata["qualification"] = _source_qualification(
            version,
            exact_versions=frozenset({"2.9.0"}),
            profile="pydantic-ai-python-adapter-v1",
        )

    def install(self, sink: Any) -> _Lifecycle:
        try:
            from pydantic_ai.capabilities.abstract import AbstractCapability
        except ImportError as error:  # pragma: no cover
            raise RuntimeError("pydantic-ai capabilities are required") from error

        restores: list[Any] = []
        active = True
        indexes: dict[str, int] = {}
        failed_turns: set[str] = set()
        implicit_runs = 0
        sync_label: ContextVar[str | None] = ContextVar(
            "semantic_layer_pydantic_method", default=None
        )
        streaming_owner: ContextVar[bool] = ContextVar(
            "semantic_layer_pydantic_streaming_owner", default=False
        )

        class CaptureCapability(AbstractCapability[Any]):
            def __init__(self, trace: dict[str, str], native_id: str) -> None:
                self.trace = trace
                self.native_id = native_id
                self.model_starts: list[tuple[AdmissionReceipt, str]] = []
                self.model_requests = 0
                self.recovery_markers: list[AdmissionReceipt] = []
                self.context_messages: list[tuple[Any, list[AdmissionReceipt], bool]] = []
                self.last_model_response: AdmissionReceipt | None = None
                self.consumed_thinking: list[str] = []
                self.cancellation_requested = False

            async def before_model_request(self, ctx: Any, request_context: Any) -> Any:
                if active:
                    self.model_requests += 1
                    self.consumed_thinking = []
                    model_identity = f"{self.native_id}:model:{self.model_requests}"
                    request_evidence = _pydantic_model_request_evidence(ctx, request_context)
                    context_refs, context_complete = self._record_model_context(
                        _pydantic_context_messages(_owned_field(request_context, "messages"))
                    )
                    request_semantic: dict[str, Any] = {
                        "type": "model.request",
                        "framework": "pydantic-ai",
                        "model": request_evidence["model"],
                        "tools": [
                            tool["name"]
                            for tool in request_evidence["tool_definitions"]
                            if isinstance(tool.get("name"), str) and tool["name"]
                        ],
                        "tool_definitions": request_evidence["tool_definitions"],
                        "settings": request_evidence["settings"],
                    }
                    if context_complete:
                        request_semantic["context_refs"] = context_refs
                    self.model_starts.append(
                        (
                            sink.record(
                                {
                                    "kind": "model",
                                    "phase": "start",
                                    "name": "pydantic_ai.model.call",
                                    "trace": self.trace,
                                    "native_identity": model_identity,
                                    "native": request_evidence,
                                    "semantic": request_semantic,
                                }
                            ),
                            model_identity,
                        )
                    )
                return request_context

            def _record_model_context(self, messages: Any) -> tuple[list[str], bool]:
                if not isinstance(messages, list):
                    return [], False
                common = 0
                shared_length = min(len(messages), len(self.context_messages))
                while (
                    common < shared_length and messages[common] == self.context_messages[common][0]
                ):
                    common += 1

                retained = self.context_messages[:common]
                unknown_context_parts: list[tuple[int, str]] = []
                for index, message in enumerate(messages[common:], start=common):
                    receipts: list[AdmissionReceipt] = []
                    semantics, unknown_part_kinds = _pydantic_context_message_semantics(message)
                    for part_index, semantic in semantics:
                        receipt = sink.record(
                            {
                                "kind": "model",
                                "phase": "event",
                                "name": "pydantic_ai.model.context",
                                "trace": self.trace,
                                "native_identity": (
                                    f"{self.native_id}:context:{index}:"
                                    f"{part_index if part_index is not None else 'message'}"
                                ),
                                "native": {
                                    "message_index": index,
                                    **(
                                        {
                                            "part_index": part_index,
                                            "part": semantic["content"],
                                        }
                                        if part_index is not None
                                        else {"message": message}
                                    ),
                                },
                                "semantic": {
                                    "type": "message",
                                    "framework": "pydantic-ai",
                                    "origin": "context",
                                    **semantic,
                                },
                            }
                        )
                        receipts.append(receipt)
                    unknown_context_parts.extend(
                        (index, part_kind) for part_kind in unknown_part_kinds
                    )
                    retained.append((message, receipts, not unknown_part_kinds))
                if unknown_context_parts:
                    _record_pydantic_context_part_gap(
                        sink,
                        self.trace,
                        unknown_context_parts,
                    )
                self.context_messages = retained
                return (
                    [
                        receipt.record_id
                        for _message, receipts, _complete in retained
                        for receipt in receipts
                        if receipt.accepted and receipt.record_id is not None
                    ],
                    all(complete for _message, _receipts, complete in retained),
                )

            async def after_model_request(
                self, ctx: Any, *, request_context: Any, response: Any
            ) -> Any:
                if active:
                    self._finish_model(request_context, response=response)
                return response

            async def wrap_model_request(
                self, ctx: Any, *, request_context: Any, handler: Any
            ) -> Any:
                try:
                    return await handler(request_context)
                except BaseException as error:
                    if active:
                        self._finish_model(request_context, error=error)
                    raise

            def _finish_model(
                self,
                request_context: Any,
                *,
                response: Any = None,
                error: BaseException | None = None,
            ) -> None:
                opened = self.model_starts.pop(0) if self.model_starts else None
                # PydanticAI can run both the wrapping error hook and a later completion hook
                # for one failed request. Only the hook that owns the matched start may emit
                # the terminal; a duplicate terminal after the run closes is not trace evidence.
                if opened is None:
                    return
                start, model_identity = opened
                response_evidence = _pydantic_model_response_evidence(response)
                response_semantic = _pydantic_model_response_semantic(response_evidence)
                if self.consumed_thinking and not response_semantic.get("reasoning"):
                    response_semantic["reasoning"] = [
                        {"type": "text", "text": "".join(self.consumed_thinking)}
                    ]
                self.consumed_thinking = []
                cancelled = self.cancellation_requested or (
                    error is not None
                    and type(error).__name__ in {"CancelledError", "GeneratorExit"}
                )
                value: dict[str, Any] = {
                    "kind": "model",
                    "phase": (
                        "cancelled" if cancelled else "error" if error is not None else "end"
                    ),
                    "name": "pydantic_ai.model.call",
                    "trace": self.trace,
                    "native_identity": model_identity,
                    "native": {
                        "request": _pydantic_model_request_evidence(None, request_context),
                        "response": response_evidence,
                        **(_pydantic_error_native(error) if error is not None else {"error": None}),
                    },
                    "semantic": {
                        "type": "model.response",
                        "framework": "pydantic-ai",
                        "status": (
                            "cancelled"
                            if cancelled
                            else "failed"
                            if error is not None
                            else "completed"
                        ),
                        **response_semantic,
                    },
                }
                if error is not None:
                    value["error_identity"] = error
                if start is not None and start.accepted and start.record_id is not None:
                    value["parent_record_id"] = start.record_id
                terminal = sink.record(value)
                if error is None and terminal.accepted:
                    self.last_model_response = terminal
                if error is not None:
                    sink.record(
                        {
                            "kind": "error",
                            "phase": "event",
                            "name": "pydantic_ai.model.error",
                            "trace": self.trace,
                            "error_identity": error,
                            "native": {
                                **_pydantic_error_native(error),
                                "request": _pydantic_model_request_evidence(None, request_context),
                            },
                            "semantic": {
                                "type": "error",
                                "framework": "pydantic-ai",
                                "error": _pydantic_semantic_error(error),
                            },
                        }
                    )

            async def before_tool_validate(
                self, ctx: Any, *, call: Any, tool_def: Any, args: Any
            ) -> Any:
                if active:
                    call_id = _pydantic_capability_call_id(call)
                    tool_name = _owned_field(tool_def, "name")
                    sink.record(
                        {
                            "kind": "tool",
                            "phase": "event",
                            "name": "pydantic_ai.tool.proposal",
                            "trace": self.trace,
                            "native_identity": call_id,
                            "native": {
                                "call": call,
                                "tool_name": tool_name,
                            },
                            "semantic": {
                                "type": "tool.proposal",
                                "framework": "pydantic-ai",
                                "name": tool_name,
                                "input": args,
                                "native_call_id": call_id,
                            },
                        }
                    )
                return args

            async def wrap_tool_execute(
                self, ctx: Any, *, call: Any, tool_def: Any, args: Any, handler: Any
            ) -> Any:
                call_id = _pydantic_capability_call_id(call)
                tool_name = _owned_field(tool_def, "name")
                start_value: dict[str, Any] = {
                    "kind": "tool",
                    "phase": "start",
                    "name": "pydantic_ai.tool.execution",
                    "trace": self.trace,
                    "native_identity": call_id,
                    "native": {
                        "tool_call_id": call_id,
                        "tool_name": tool_name,
                        "args": args,
                    },
                    "semantic": {
                        "type": "tool.execution",
                        "framework": "pydantic-ai",
                        "name": tool_name,
                        "input": args,
                        "native_call_id": call_id,
                    },
                }
                if self.recovery_markers:
                    marker = self.recovery_markers.pop(0)
                    if marker.accepted and marker.record_id is not None:
                        start_value["parent_record_id"] = marker.record_id
                start = sink.record(start_value) if active else None
                try:
                    result = await handler(args)
                except BaseException as error:
                    if active:
                        self._finish_tool(
                            start,
                            call_id,
                            call,
                            tool_def,
                            args,
                            error=error,
                            retry_attempt=_owned_nonnegative_int(ctx, "retry") + 1,
                        )
                    raise
                if active:
                    self._finish_tool(start, call_id, call, tool_def, args, result=result)
                return result

            def _finish_tool(
                self,
                start: AdmissionReceipt | None,
                call_id: str,
                call: Any,
                tool_def: Any,
                args: Any,
                *,
                result: Any = None,
                error: BaseException | None = None,
                retry_attempt: int | None = None,
            ) -> None:
                is_retry = error is not None and type(error).__name__ in {
                    "ModelRetry",
                    "ToolRetryError",
                }
                native_error = _pydantic_retry_error(error) if is_retry else error
                value: dict[str, Any] = {
                    "kind": "tool",
                    "phase": "error" if error is not None else "end",
                    "name": "pydantic_ai.tool.execution",
                    "trace": self.trace,
                    "native_identity": call_id,
                    "native": {
                        "tool_call_id": call_id,
                        "tool_name": _owned_field(tool_def, "name"),
                        "args": args,
                        "result": result,
                        "error": native_error,
                    },
                    "semantic": {
                        "type": "tool.error" if error is not None else "tool.result",
                        "framework": "pydantic-ai",
                        "native_call_id": call_id,
                        "status": "failed" if error is not None else "succeeded",
                        **({"output": result} if error is None else {}),
                        **(
                            {"error": _pydantic_semantic_error(native_error)}
                            if error is not None and native_error is not None
                            else {}
                        ),
                    },
                }
                if error is not None:
                    value["error_identity"] = error
                if start is not None and start.accepted and start.record_id is not None:
                    value["parent_record_id"] = start.record_id
                sink.record(value)
                if error is None:
                    sink.record(
                        {
                            "kind": "tool",
                            "phase": "event",
                            "name": "pydantic_ai.tool.result",
                            "trace": self.trace,
                            "native_identity": call_id,
                            "native": {"call": call, "result": result},
                            "semantic": {
                                "type": "capture.redundant",
                                "framework": "pydantic-ai",
                            },
                        }
                    )
                    return
                if not is_retry:
                    sink.record(
                        {
                            "kind": "error",
                            "phase": "event",
                            "name": "pydantic_ai.tool.error",
                            "trace": self.trace,
                            "native_identity": call_id,
                            "error_identity": error,
                            "native": {"error": error, "tool_call_id": call_id},
                            "semantic": {
                                "type": "capture.redundant",
                                "framework": "pydantic-ai",
                            },
                        }
                    )
                    return
                error_receipt = sink.record(
                    {
                        "kind": "error",
                        "phase": "event",
                        "name": "pydantic_ai.tool.retry",
                        "trace": self.trace,
                        "native_identity": call_id,
                        "error_identity": error,
                        "native": {"error": native_error, "call": call},
                        "semantic": {
                            "type": "capture.redundant",
                            "framework": "pydantic-ai",
                        },
                    }
                )
                assert native_error is not None
                marker_value: dict[str, Any] = {
                    "kind": "state",
                    "phase": "event",
                    "name": "pydantic_ai.recovery.retry",
                    "trace": self.trace,
                    "native_identity": call_id,
                    "native": {
                        "error": _pydantic_retry_error(error),
                        "attempt": retry_attempt,
                    },
                    "semantic": {
                        "type": "state.transition",
                        "state_type": "recovery.retry",
                        "framework": "pydantic-ai",
                        "value": {
                            "attempt": retry_attempt,
                            "error": _pydantic_semantic_error(native_error),
                        },
                    },
                }
                if error_receipt.accepted and error_receipt.record_id is not None:
                    marker_value["parent_record_id"] = error_receipt.record_id
                self.recovery_markers.append(sink.record(marker_value))

        class StreamingCaptureCapability(CaptureCapability):
            async def wrap_run_event_stream(self, ctx: Any, *, stream: Any) -> Any:
                del ctx
                async for event in stream:
                    if active:
                        event_name = type(event).__name__
                        part = _owned_field(event, "part")
                        delta = _owned_field(event, "delta")
                        text = None
                        if (
                            event_name == "PartStartEvent"
                            and _owned_field(part, "part_kind") == "thinking"
                        ):
                            text = _owned_field(part, "content")
                        elif (
                            event_name == "PartDeltaEvent"
                            and _owned_field(delta, "part_delta_kind") == "thinking"
                        ):
                            text = _owned_field(delta, "content_delta")
                        if isinstance(text, str) and text:
                            self.consumed_thinking.append(text)
                    yield event

        for method_name in ("run", "run_sync", "run_stream", "iter"):
            had_own_method = method_name in vars(self.agent)
            own_method = vars(self.agent).get(method_name)
            original = getattr(self.agent, method_name, None)
            if not callable(original):
                continue

            def prepare(
                args: tuple[Any, ...],
                kwargs: dict[str, Any],
                __method_name: str = method_name,
            ) -> tuple[dict[str, str] | None, str, CaptureCapability | None]:
                nonlocal implicit_runs
                requested_capabilities = list(kwargs.get("capabilities") or [])
                requested_conversation = kwargs.get("conversation_id")
                explicit_conversation = (
                    str(requested_conversation)
                    if requested_conversation not in (None, "", "new")
                    else None
                )
                if explicit_conversation is None:
                    conversation = f"pydantic-ai-implicit:{implicit_runs}"
                    implicit_runs += 1
                    index = 0
                else:
                    conversation = explicit_conversation
                    index = indexes.get(conversation, -1) + 1
                    indexes[conversation] = index
                native_id = f"{conversation}:{index}"
                prompt = args[0] if args else kwargs.get("user_prompt")
                root_semantic: dict[str, Any] = {
                    "type": "agent.run",
                    "framework": "pydantic-ai",
                    "name": "pydantic_ai.run",
                }
                if prompt is not None:
                    root_semantic["input"] = prompt
                opened = sink.open_trace(
                    {
                        "name": "pydantic_ai.run",
                        "native_identity": native_id,
                        "conversation_id": conversation,
                        "turn_id": f"{conversation}:{index}",
                        "turn_index": index,
                        **({"previous_turn_id": f"{conversation}:{index - 1}"} if index else {}),
                        "native": {"method": sync_label.get() or __method_name},
                        "semantic": root_semantic,
                    }
                )
                trace = dict(opened.identity) if opened.accepted and opened.identity else None
                previous_native_id = f"{conversation}:{index - 1}" if index else None
                if trace is not None and previous_native_id in failed_turns:
                    sink.record(
                        {
                            "kind": "state",
                            "phase": "event",
                            "name": "pydantic_ai.recovery.retry",
                            "trace": trace,
                            "native": {"attempt": 1, "previous_turn_id": previous_native_id},
                            "semantic": {
                                "type": "state.transition",
                                "state_type": "recovery.retry",
                                "framework": "pydantic-ai",
                                "value": {
                                    "attempt": 1,
                                    "previous_turn_id": previous_native_id,
                                },
                            },
                        }
                    )
                    failed_turns.remove(previous_native_id)
                prior = kwargs.get("event_stream_handler")
                capability: CaptureCapability | None = None
                if trace is not None:
                    capability_type = (
                        StreamingCaptureCapability
                        if __method_name == "run_stream"
                        else CaptureCapability
                    )
                    capability = capability_type(trace, native_id)
                    kwargs["capabilities"] = [
                        *requested_capabilities,
                        capability,
                    ]
                if prior is not None and trace is not None:

                    async def observed_handler(context: Any, events: Any) -> None:
                        async def observed_events() -> Any:
                            last_event: Any = None
                            async for event in events:
                                last_event = event
                                if active:
                                    _record_pydantic_stream_event(sink, trace, event)
                                yield event
                            if active and last_event is not None:
                                _record_pydantic_stream_completion(sink, trace, last_event)

                        await prior(context, observed_events())

                    kwargs["event_stream_handler"] = observed_handler
                return trace, native_id, capability

            wrapper: Any
            if method_name == "run":

                async def async_wrapper(
                    *args: Any,
                    __original: Any = original,
                    __prepare: Any = prepare,
                    **kwargs: Any,
                ) -> Any:
                    trace, native_id, capability = __prepare(args, kwargs)
                    validated_result: AdmissionReceipt | None = None
                    owner_token = streaming_owner.set(True)
                    try:
                        try:
                            result = await __original(*args, **kwargs)
                        finally:
                            streaming_owner.reset(owner_token)
                    except BaseException as error:
                        if active:
                            failed_turns.add(native_id)
                            _close_framework_trace(
                                sink, trace, "pydantic_ai.run", native_id, error=error
                            )
                        raise
                    if active and trace is not None:
                        result_snapshot = _pydantic_result_snapshot(result)
                        semantic: dict[str, Any] = {
                            "type": "state.transition",
                            "framework": "pydantic-ai",
                            "state_type": "state.validated_result",
                            "value": result_snapshot["output"],
                        }
                        if (
                            capability is not None
                            and capability.last_model_response is not None
                            and capability.last_model_response.accepted
                            and capability.last_model_response.record_id is not None
                        ):
                            semantic["result_ref"] = capability.last_model_response.record_id
                        validated_result = sink.record(
                            {
                                "kind": "state",
                                "phase": "event",
                                "name": "pydantic_ai.validated_result",
                                "trace": trace,
                                "native": {
                                    "output": result_snapshot["output"],
                                    "output_type": type(result.output).__qualname__,
                                },
                                "semantic": semantic,
                            }
                        )
                        sink.record(
                            {
                                "kind": "model",
                                "phase": "event",
                                "name": "pydantic_ai.usage",
                                "trace": trace,
                                "native": {"usage": result_snapshot["usage"]},
                                "semantic": {
                                    "type": "capture.redundant",
                                    "framework": "pydantic-ai",
                                },
                            }
                        )
                    if active:
                        _close_framework_trace(
                            sink,
                            trace,
                            "pydantic_ai.run",
                            native_id,
                            output=validated_result,
                        )
                    return result

                wrapper = async_wrapper
            elif method_name == "run_sync":

                def sync_wrapper(
                    *args: Any,
                    __original: Any = original,
                    **kwargs: Any,
                ) -> Any:
                    token = sync_label.set("run_sync")
                    try:
                        return __original(*args, **kwargs)
                    finally:
                        sync_label.reset(token)

                wrapper = sync_wrapper
            else:

                @asynccontextmanager  # type: ignore[arg-type]
                async def streaming_wrapper(
                    *args: Any,
                    __original: Any = original,
                    __prepare: Any = prepare,
                    **kwargs: Any,
                ) -> AsyncIterator[Any]:
                    if streaming_owner.get():
                        async with __original(*args, **kwargs) as value:
                            yield value
                        return
                    owner_token = streaming_owner.set(True)
                    try:
                        trace, native_id, capability = __prepare(args, kwargs)
                        try:
                            async with __original(*args, **kwargs) as value:
                                try:
                                    yield value
                                except BaseException as error:
                                    if capability is not None and type(error).__name__ in {
                                        "CancelledError",
                                        "GeneratorExit",
                                    }:
                                        capability.cancellation_requested = True
                                    raise
                        except BaseException as error:
                            if active:
                                failed_turns.add(native_id)
                                _close_framework_trace(
                                    sink,
                                    trace,
                                    "pydantic_ai.run",
                                    native_id,
                                    error=error,
                                )
                            raise
                        else:
                            if active:
                                _close_framework_trace(
                                    sink,
                                    trace,
                                    "pydantic_ai.run",
                                    native_id,
                                )
                    finally:
                        streaming_owner.reset(owner_token)

                wrapper = streaming_wrapper
            try:
                setattr(self.agent, method_name, wrapper)
            except BaseException:
                active = False
                for restore in reversed(restores):
                    restore()
                raise

            def restore(
                name: str = method_name,
                replacement: Any = wrapper,
                prior_was_own: bool = had_own_method,
                prior_own: Any = own_method,
            ) -> None:
                if getattr(self.agent, name, None) is replacement:
                    if prior_was_own:
                        setattr(self.agent, name, prior_own)
                    else:
                        delattr(self.agent, name)

            restores.append(restore)

        if not restores:
            raise TypeError("PydanticAI subject must expose run or run_sync")

        def deactivate() -> None:
            nonlocal active
            active = False
            for restore in reversed(restores):
                restore()

        return _Lifecycle(deactivate)


def _owned_nonnegative_int(value: Any, name: str) -> int:
    candidate = _owned_field(value, name, 0)
    return candidate if isinstance(candidate, int) and not isinstance(candidate, bool) else 0


def _pydantic_capability_call_id(call: Any) -> str:
    value = _owned_field(call, "tool_call_id")
    return str(value) if value else f"pydantic-call:{id(call)}"


def _pydantic_retry_error(error: BaseException | None) -> BaseException | None:
    if error is None:
        return None
    descriptor = vars(BaseException)["__cause__"]
    try:
        cause = descriptor.__get__(error, type(error))
    except BaseException:
        return error
    return cause if isinstance(cause, BaseException) else error


def _pydantic_error_native(error: BaseException) -> dict[str, Any]:
    cause = _pydantic_retry_error(error)
    native_error: dict[str, Any] = {
        "type": type(error).__name__,
        "message": str(error),
        "traceback": "".join(traceback.format_tb(error.__traceback__)),
    }
    if cause is not error and cause is not None:
        native_error["cause"] = {
            "type": type(cause).__name__,
            "message": str(cause),
            "traceback": "".join(traceback.format_tb(cause.__traceback__)),
        }
    context = error.__context__
    context_exceptions = getattr(context, "exceptions", None)
    if isinstance(context_exceptions, tuple):
        native_error["context"] = {
            "type": type(context).__name__,
            "message": str(context),
            "exceptions": [
                {"$semantic_layer_ref": "error"}
                if child is error
                else {"type": type(child).__name__, "message": str(child)}
                for child in context_exceptions
            ],
        }
    native = {"error": native_error}
    structured = _owned_field(error, "structured_error")
    if type(structured) is dict:
        native["structured_error"] = _pydantic_jsonable(structured)
    return native


def _pydantic_model_request_evidence(ctx: Any, request_context: Any) -> dict[str, Any]:
    parameters = _owned_field(request_context, "model_request_parameters")
    function_tools = _pydantic_tool_definitions(parameters, "function_tools")
    output_tools = _pydantic_tool_definitions(parameters, "output_tools")
    return {
        "prompt": _owned_field(ctx, "prompt") if ctx is not None else None,
        "model": _pydantic_model_name(ctx, request_context),
        "messages": _pydantic_jsonable(_owned_field(request_context, "messages")),
        "tool_definitions": [*function_tools, *output_tools],
        "settings": _pydantic_request_settings(request_context, parameters),
    }


def _pydantic_tool_definitions(parameters: Any, name: str) -> list[dict[str, Any]]:
    tools = _owned_field(parameters, name, [])
    if not isinstance(tools, (list, tuple)):
        return []
    return [
        {
            "name": _owned_field(tool, "name"),
            "description": _owned_field(tool, "description"),
            "parameters_json_schema": _pydantic_jsonable(
                _owned_field(tool, "parameters_json_schema")
            ),
            "kind": _owned_field(tool, "kind")
            or ("output" if name == "output_tools" else "function"),
        }
        for tool in tools
    ]


def _pydantic_request_settings(request_context: Any, parameters: Any) -> dict[str, Any]:
    settings: dict[str, Any] = {
        "model_settings": _pydantic_jsonable(_owned_field(request_context, "model_settings", {}))
    }
    for name in (
        "output_mode",
        "output_object",
        "prompted_output_template",
        "allow_text_output",
        "allow_image_output",
        "instruction_parts",
        "thinking",
    ):
        value = _owned_field(parameters, name)
        if value is not None:
            settings[name] = _pydantic_jsonable(value)
    return settings


def _pydantic_context_message_semantics(
    message: Any,
) -> tuple[list[tuple[int | None, dict[str, Any]]], list[str]]:
    if not isinstance(message, dict):
        return [], ["non_object_message"]
    if message.get("kind") == "response":
        return [(None, {"role": "assistant", "content": message})], []
    if message.get("kind") != "request":
        return [], [str(message.get("kind") or "missing_message_kind")[:128]]
    roles = {
        "system-prompt": "system",
        "user-prompt": "user",
        "tool-search-return": "tool",
        "capability-load-return": "tool",
        "tool-return": "tool",
        "retry-prompt": "tool",
    }
    parts = message.get("parts")
    if not isinstance(parts, list):
        return [], ["non_array_parts"]
    semantics: list[tuple[int | None, dict[str, Any]]] = []
    unknown_part_kinds: list[str] = []
    for index, part in enumerate(parts):
        part_kind = part.get("part_kind") if isinstance(part, dict) else None
        role = roles.get(part_kind) if isinstance(part_kind, str) else None
        if isinstance(part, dict) and isinstance(role, str):
            if part_kind == "retry-prompt":
                semantic: dict[str, Any] = {
                    "role": role,
                    "content": part.get("content"),
                }
                tool_name = part.get("tool_name")
                if isinstance(tool_name, str) and tool_name:
                    semantic["name"] = tool_name
                tool_call_id = part.get("tool_call_id")
                if isinstance(tool_call_id, str) and tool_call_id:
                    semantic["call_id"] = tool_call_id
                semantics.append((index, semantic))
            else:
                semantics.append((index, {"role": role, "content": part}))
        else:
            unknown_part_kinds.append(str(part_kind or "missing_part_kind")[:128])
    return semantics, unknown_part_kinds


def _pydantic_context_messages(messages: Any) -> Any:
    projected = _pydantic_jsonable(messages)
    if not isinstance(messages, (list, tuple)) or not isinstance(projected, list):
        return projected
    try:
        from pydantic_ai.messages import RetryPromptPart
    except ImportError:  # pragma: no cover
        return projected
    for message, projected_message in zip(messages, projected, strict=False):
        parts = _owned_field(message, "parts")
        projected_parts = (
            projected_message.get("parts") if isinstance(projected_message, dict) else None
        )
        if not isinstance(parts, (list, tuple)) or not isinstance(projected_parts, list):
            continue
        for part, projected_part in zip(parts, projected_parts, strict=False):
            if type(part) is not RetryPromptPart or not isinstance(projected_part, dict):
                continue
            projected_part["content"] = part.model_response()
    return projected


def _record_pydantic_context_part_gap(
    sink: Any,
    trace: dict[str, str],
    context_parts: list[tuple[int, str]],
) -> None:
    count = len(context_parts)
    samples = context_parts[:16]
    kinds = ", ".join(part_kind for _message_index, part_kind in samples)
    sink.record(
        {
            "kind": "unknown",
            "phase": "gap",
            "name": "pydantic_ai.model.context.gap",
            "trace": trace,
            "native_identity": "pydantic-context-gap",
            "native": {
                "parts": [
                    {
                        "message_index": message_index,
                        "part_kind": part_kind,
                    }
                    for message_index, part_kind in samples
                ],
                "count": count,
            },
            "semantic": {
                "type": "capture.gap",
                "reason": "model_context_part_not_captured",
                "count": count,
                "detail": (
                    "PydanticAI model request context omitted material parts "
                    f"with unsupported kinds: {kinds}."
                )[:4096],
            },
        }
    )


def _pydantic_model_name(ctx: Any, request_context: Any) -> Any:
    model = _owned_field(ctx, "model")
    if model is None:
        model = _owned_field(request_context, "model")
    return _owned_field(model, "model_name") or _owned_field(model, "system")


def _pydantic_jsonable(value: Any) -> Any:
    from pydantic_core import to_jsonable_python

    return to_jsonable_python(value)


def _pydantic_model_response_evidence(response: Any) -> Any:
    if response is None:
        return None
    usage = _owned_field(response, "usage")
    return {
        "parts": _owned_field(response, "parts"),
        "model_name": _owned_field(response, "model_name"),
        "finish_reason": _owned_field(response, "finish_reason"),
        "usage": {
            name: _owned_field(usage, name)
            for name in (
                "input_tokens",
                "cache_write_tokens",
                "cache_read_tokens",
                "output_tokens",
                "input_audio_tokens",
                "cache_audio_read_tokens",
                "output_audio_tokens",
                "details",
            )
        },
    }


def _pydantic_model_response_semantic(response: Any) -> dict[str, Any]:
    if not isinstance(response, dict):
        return {}
    semantic: dict[str, Any] = {
        "content": _pydantic_visible_parts(response.get("parts")),
        "usage": response.get("usage"),
    }
    reasoning = _pydantic_reasoning(response.get("parts"))
    if reasoning:
        semantic["reasoning"] = reasoning
    for name in ("model_name", "finish_reason"):
        value = response.get(name)
        if isinstance(value, str) and value:
            semantic["model" if name == "model_name" else name] = value
    return semantic


def _pydantic_reasoning(parts: Any) -> list[dict[str, str]]:
    """Project explicit PydanticAI reasoning parts in response order."""

    if not isinstance(parts, (list, tuple)):
        return []
    reasoning: list[dict[str, str]] = []
    for part in parts:
        part_kind = _owned_field(part, "part_kind")
        if part_kind not in {"thinking", "compaction"}:
            continue
        content = _owned_field(part, "content")
        if isinstance(content, str) and content:
            reasoning.append(
                {
                    "type": "summary" if part_kind == "compaction" else "text",
                    "text": content,
                }
            )
    return reasoning


def _pydantic_visible_parts(parts: Any) -> Any:
    """Project response parts without thinking or compaction content."""

    if not isinstance(parts, (list, tuple)):
        return parts
    return [
        part for part in parts if _owned_field(part, "part_kind") not in {"thinking", "compaction"}
    ]


def _pydantic_semantic_error(error: BaseException) -> dict[str, Any]:
    return {
        "type": "pydantic_ai_error",
        "message": str(error),
        "recoverable": type(error).__name__ in {"ModelRetry", "ToolRetryError"},
        "details": {"native_type": type(error).__name__},
    }


def _record_pydantic_stream_event(sink: Any, trace: dict[str, str], event: Any) -> None:
    event_name = type(event).__name__
    if event_name not in {"PartStartEvent", "PartDeltaEvent", "AgentRunResultEvent"}:
        return
    part = _owned_field(event, "part")
    event_delta = _owned_field(event, "delta")
    part_kind = _owned_field(part, "part_kind")
    delta_kind = _owned_field(event_delta, "part_delta_kind")
    visible_text = (
        _owned_field(part, "content")
        if part_kind == "text"
        else _owned_field(event_delta, "content_delta")
        if delta_kind == "text"
        else None
    )
    delta = visible_text
    if delta is None:
        delta = (
            _owned_field(part, "args")
            or _owned_field(event_delta, "args_delta")
            or _owned_field(event_delta, "tool_name_delta")
        )
    native = {"delta": delta, "event": event} if event_name == "PartStartEvent" else event
    sink.record(
        {
            "kind": "state" if event_name == "AgentRunResultEvent" else "stream",
            "phase": "event",
            "name": f"pydantic_ai.{event_name}",
            "trace": trace,
            "native": native,
            "semantic": {
                "type": "capture.redundant",
                "framework": "pydantic-ai",
            },
        }
    )


def _record_pydantic_stream_completion(sink: Any, trace: dict[str, str], last_event: Any) -> None:
    result = _owned_field(last_event, "result")
    part = _owned_field(last_event, "part")
    output = _owned_field(result, "output")
    if output is None:
        output = _owned_field(part, "content") or _owned_field(part, "args")
    sink.record(
        {
            "kind": "stream",
            "phase": "end",
            "name": "pydantic_ai.stream.consumer_completed",
            "trace": trace,
            "native": {
                "result": {
                    "output": output,
                    "last_event_type": type(last_event).__name__,
                }
            },
            "semantic": {
                "type": "capture.redundant",
                "framework": "pydantic-ai",
            },
        }
    )


def _pydantic_result_snapshot(result: Any) -> dict[str, Any]:
    return {
        "output": _raw_native(getattr(result, "output", None)),
        "usage": _raw_native(_call_or_value(getattr(result, "usage", None))),
    }


def _call_or_value(value: Any) -> Any:
    return value() if callable(value) else value


def _close_framework_trace(
    sink: Any,
    trace: dict[str, str] | None,
    name: str,
    native_identity: str,
    error: BaseException | None = None,
    output: AdmissionReceipt | None = None,
) -> None:
    if trace is None:
        return
    phase = "error" if error is not None else "end"
    if error is not None:
        sink.record(
            {
                "kind": "error",
                "phase": "event",
                "name": f"{name}.error",
                "trace": trace,
                "error_identity": error,
                "native": _pydantic_error_native(error),
                "semantic": {
                    "type": "error",
                    "framework": "pydantic-ai",
                    "error": _pydantic_semantic_error(error),
                },
            }
        )
    semantic: dict[str, Any] = {
        "type": "agent.run",
        "framework": "pydantic-ai",
        "status": "failed" if error is not None else "succeeded",
    }
    if error is not None:
        semantic["error"] = _pydantic_semantic_error(error)
    elif output is not None and output.accepted and output.record_id is not None:
        semantic["output_ref"] = output.record_id
    sink.record(
        {
            "kind": "lifecycle",
            "phase": phase,
            "name": name,
            "trace": trace,
            "native_identity": native_identity,
            **({"error_identity": error} if error is not None else {}),
            "native": (
                _pydantic_error_native(error) if error is not None else {"status": "completed"}
            ),
            "semantic": semantic,
        }
    )
