"""Explicit-client provider adapters using the public capture source seam."""

from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import AsyncIterator, Iterable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from importlib.metadata import version as distribution_version
from itertools import chain, islice
from threading import Lock
from typing import Any, Generic, TypeVar

from ._adapter_native import native_field, native_own_data, native_snapshot
from .capture_v1 import CaptureSource, _trust_official_source, is_unsafe_accessor_omission

_PROVIDER_SEAMS = {
    "openai": "responses.create/chat.completions.create sync+async",
    "anthropic": "messages._post /v1/messages sync+async",
    "gemini": "models/aio.models per-request generate_content seams",
    "openrouter": "OpenAI-compatible responses.create/chat.completions.create sync+async",
}
_PROVIDER_REQUEST_COVERAGE = {"operation": "model-call", "domain": "provider.request"}
_OPENAI_RESPONSE_COVERAGE = {
    "operation": "model-response",
    "domain": "openai.response",
}
_EXACT_QUALIFIED_PROVIDER_VERSIONS = {
    "openai": frozenset({"2.45.0"}),
    "openrouter": frozenset({"2.45.0"}),
    "anthropic": frozenset({"0.116.0"}),
    "gemini": frozenset({"2.11.0"}),
}
_PROVIDER_CAPABILITY_PROFILE = "provider-reasoning-v1"
_MAX_GEMINI_PENDING_TOOL_TURNS = 1024
_MAX_PROVIDER_EVIDENCE_BYTES = 256 * 1024
_MAX_PROVIDER_EVIDENCE_NODES = 4096
_MAX_PROVIDER_STREAM_RETAINED_BYTES = 256 * 1024
_MAX_PROVIDER_STREAM_RETAINED_NODES = 4096
_MAX_PROVIDER_STREAM_SEMANTIC_BYTES = 256 * 1024
_MAX_PROVIDER_STREAM_SEMANTIC_NODES = 4096
_MAX_PROVIDER_MATERIALIZATION_WIDTH = 128
_MAX_PROVIDER_CONTEXT_TRACES = 4096
_MAX_PROVIDER_EXCEPTION_SCAN_BYTES = 64 * 1024
_MAX_PROVIDER_EXCEPTION_SCAN_NODES = 1024
_MAX_PROVIDER_EXCEPTION_SCAN_WIDTH = 128

T = TypeVar("T")
_provider_context: ContextVar[dict[str, Any] | None] = ContextVar(
    "semantic_layer_provider_context", default=None
)


@contextmanager
def provider_capture_context(
    *,
    conversation_id: str | None = None,
    turn_id: str | None = None,
    turn_index: int | None = None,
    previous_turn_id: str | None = None,
) -> Any:
    """Attach causal turn identity without mutating provider request values."""
    value = {
        key: child
        for key, child in {
            "conversation_id": conversation_id,
            "turn_id": turn_id,
            "turn_index": turn_index,
            "previous_turn_id": previous_turn_id,
        }.items()
        if child is not None
    }
    token = _provider_context.set(value)
    try:
        yield
    finally:
        _provider_context.reset(token)


class _Lifecycle:
    def __init__(self, restores: list[Any]) -> None:
        self._restores = restores

    def deactivate(self) -> None:
        for restore in reversed(self._restores):
            restore()
        self._restores.clear()

    def drain(self) -> None:
        return None


@dataclass
class _Trace:
    identity: dict[str, str]
    name: str
    operation: str
    context: dict[str, Any]
    request_record_id: str | None = None
    closed: bool = False
    last_part: Any = None
    response_id: str | None = None
    gemini_call_ids: list[str] = field(default_factory=list)
    gemini_tool_candidate_indexes: set[int] = field(default_factory=set)
    gemini_request_contents: Any = None
    stream_text: list[str] = field(default_factory=list)
    stream_reasoning: list[dict[str, str]] = field(default_factory=list)
    stream_reasoning_keys: dict[str, int] = field(default_factory=dict)
    stream_event_index: int = 0
    stream_model: str | None = None
    stream_finish_reason: str | None = None
    stream_usage: dict[str, int | float] = field(default_factory=dict)
    stream_tools: dict[str, dict[str, Any]] = field(default_factory=dict)
    observation_loss_recorded: bool = False
    evidence_retention_loss_recorded: bool = False
    stream_semantic_loss_recorded: bool = False
    stream_retained_bytes: int = 0
    stream_retained_nodes: int = 0
    stream_text_bytes: int = 0
    stream_text_nodes: int = 0
    stream_reasoning_bytes: int = 0
    stream_reasoning_nodes: int = 0
    stream_tool_bytes: int = 0
    stream_tool_nodes: int = 0


@dataclass
class _ContextHistory:
    message_count: int = 0
    digest: str | None = None
    request_ref: str | None = None


@dataclass
class _ContextPlan:
    message_count: int
    digest: str | None
    context_refs: list[str]
    context_base_ref: str | None
    exact: bool


class _ObservedIterator(Generic[T]):
    def __init__(
        self,
        target: Iterator[T],
        source: _ProviderSource,
        trace: _Trace,
    ) -> None:
        self._target = target
        self._source = source
        self._trace = trace

    def __iter__(self) -> _ObservedIterator[T]:
        return self

    def __next__(self) -> T:
        try:
            value = next(self._target)
        except StopIteration:
            self._source._safe_finish_stream(self._trace, "end", {"consumed": True})
            raise
        except BaseException as error:
            self._source._safe_finish_stream(self._trace, "error", {"error": error})
            raise
        self._source._safe_stream_part(self._trace, value)
        return value

    def close(self) -> Any:
        close = getattr(self._target, "close", None)
        try:
            if callable(close):
                result = close()
            else:
                result = None
        except BaseException as error:
            self._source._safe_finish_stream(self._trace, "error", {"error": error})
            raise
        self._source._safe_finish_stream(self._trace, "cancelled", {"consumer_cancelled": True})
        return result

    def send(self, value: Any) -> Any:
        send = getattr(self._target, "send")
        try:
            result = send(value)
        except StopIteration:
            self._source._safe_finish_stream(self._trace, "end", {"consumed": True})
            raise
        except BaseException as error:
            self._source._safe_finish_stream(self._trace, "error", {"error": error})
            raise
        self._source._safe_stream_part(self._trace, result)
        return result

    def throw(self, *args: Any) -> Any:
        throw = getattr(self._target, "throw")
        try:
            result = throw(*args)
        except StopIteration:
            self._source._safe_finish_stream(self._trace, "end", {"consumed": True})
            raise
        except BaseException as error:
            self._source._safe_finish_stream(self._trace, "error", {"error": error})
            raise
        self._source._safe_stream_part(self._trace, result)
        return result


class _ObservedAsyncIterator(Generic[T]):
    def __init__(
        self,
        target: AsyncIterator[T],
        source: _ProviderSource,
        trace: _Trace,
    ) -> None:
        self._target = target
        self._source = source
        self._trace = trace

    def __aiter__(self) -> _ObservedAsyncIterator[T]:
        return self

    async def __anext__(self) -> T:
        try:
            value = await self._target.__anext__()
        except StopAsyncIteration:
            self._source._safe_finish_stream(self._trace, "end", {"consumed": True})
            raise
        except BaseException as error:
            self._source._safe_finish_stream(self._trace, "error", {"error": error})
            raise
        self._source._safe_stream_part(self._trace, value)
        return value

    async def aclose(self) -> Any:
        close = getattr(self._target, "aclose", None)
        try:
            if callable(close):
                result = await close()
            else:
                result = None
        except BaseException as error:
            self._source._safe_finish_stream(self._trace, "error", {"error": error})
            raise
        self._source._safe_finish_stream(self._trace, "cancelled", {"consumer_cancelled": True})
        return result

    async def asend(self, value: Any) -> Any:
        send = getattr(self._target, "asend")
        try:
            result = await send(value)
        except StopAsyncIteration:
            self._source._safe_finish_stream(self._trace, "end", {"consumed": True})
            raise
        except BaseException as error:
            self._source._safe_finish_stream(self._trace, "error", {"error": error})
            raise
        self._source._safe_stream_part(self._trace, result)
        return result

    async def athrow(self, *args: Any) -> Any:
        throw = getattr(self._target, "athrow")
        try:
            result = await throw(*args)
        except StopAsyncIteration:
            self._source._safe_finish_stream(self._trace, "end", {"consumed": True})
            raise
        except BaseException as error:
            self._source._safe_finish_stream(self._trace, "error", {"error": error})
            raise
        self._source._safe_stream_part(self._trace, result)
        return result


class _ProviderAdapter:
    def __init__(self, provider: str, version: str, paths: tuple[tuple[str, ...], ...]) -> None:
        self.provider = provider
        self.version = version
        self.paths = paths

    def create_source(self, client: object) -> _ProviderSource:
        return _trust_official_source(
            _ProviderSource(self.provider, self.version, self.paths, client), "provider"
        )  # type: ignore[return-value]


class _ProviderSource(CaptureSource):
    def __init__(
        self,
        provider: str,
        version: str,
        paths: tuple[tuple[str, ...], ...],
        client: object,
    ) -> None:
        self.provider = provider
        self.paths = paths
        self.client = client
        self.sink: Any = None
        self._gemini_pending: dict[tuple[str, str], tuple[str, ...] | None] = {}
        self._gemini_pending_lock = Lock()
        self._context_histories: dict[str, _ContextHistory] = {}
        self._context_histories_lock = Lock()
        self.metadata = {
            "name": f"provider:{provider}",
            "seam": _PROVIDER_SEAMS[provider],
            "identity_domain": "provider.request",
            "version": version,
            "qualification": (
                {"status": "exact_qualified"}
                if version in _EXACT_QUALIFIED_PROVIDER_VERSIONS[provider]
                else {
                    "status": "capability_checked_unqualified",
                    "profile": _PROVIDER_CAPABILITY_PROFILE,
                }
            ),
            "official": True,
            "coverage": [
                {**_PROVIDER_REQUEST_COVERAGE, "role": "owner"},
                *(
                    [{**_OPENAI_RESPONSE_COVERAGE, "role": "owner"}]
                    if provider in {"openai", "openrouter"}
                    else []
                ),
            ],
        }

    def install(self, sink: Any) -> _Lifecycle:
        self.sink = sink
        restores: list[Any] = []
        try:
            paths = self._install_paths()
            for path in paths:
                restore = self._patch(path)
                if restore is None:
                    raise TypeError(f"frozen {self.provider} seam is missing: {'.'.join(path)}")
                restores.append(restore)
            restores.insert(0, self._clear_context_histories)
        except BaseException:
            for restore in reversed(restores):
                restore()
            self.sink = None
            raise
        return _Lifecycle(restores)

    def _clear_context_histories(self) -> None:
        with self._context_histories_lock:
            self._context_histories.clear()

    def _install_paths(self) -> tuple[tuple[str, ...], ...]:
        if self.provider == "anthropic":
            per_request_path = ("messages", "_post")
            if self._has_callable(per_request_path):
                return (per_request_path,)
            return self.paths
        if self.provider != "gemini":
            return self.paths
        per_request_paths = tuple((*path[:-1], f"_{path[-1]}") for path in self.paths)
        if all(self._has_callable(path) for path in per_request_paths):
            return per_request_paths
        return self.paths

    def _has_callable(self, path: tuple[str, ...]) -> bool:
        target: Any = self.client
        for segment in path[:-1]:
            target = getattr(target, segment, None)
            if target is None:
                return False
        return callable(getattr(target, path[-1], None))

    def _patch(self, path: tuple[str, ...]) -> Any:
        target: Any = self.client
        for segment in path[:-1]:
            target = getattr(target, segment, None)
            if target is None:
                return None
        key = path[-1]
        original = getattr(target, key, None)
        if not callable(original):
            return None
        namespace = vars(target) if hasattr(target, "__dict__") else {}
        had_own = key in namespace
        own_value = namespace.get(key)
        operation = ".".join(path[:-1])
        if inspect.iscoroutinefunction(original):

            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                if not self._observes_call(path, args, kwargs):
                    return await original(*args, **kwargs)
                try:
                    result = await original(*args, **kwargs)
                except BaseException as error:
                    trace = self._safe_start(operation, args, kwargs)
                    self._safe_error(trace, operation, error)
                    raise
                if inspect.isgenerator(result):
                    return self._observed_generator(result, operation, args, kwargs)
                if inspect.isasyncgen(result):
                    return self._observed_async_generator(result, operation, args, kwargs)
                trace = self._safe_start(operation, args, kwargs)
                return self._safe_settle(trace, operation, result)

            wrapper = async_wrapper
        else:

            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                if not self._observes_call(path, args, kwargs):
                    return original(*args, **kwargs)
                try:
                    result = original(*args, **kwargs)
                except BaseException as error:
                    trace = self._safe_start(operation, args, kwargs)
                    self._safe_error(trace, operation, error)
                    raise
                if inspect.isawaitable(result):

                    async def await_result() -> Any:
                        try:
                            resolved = await result
                        except BaseException as error:
                            trace = self._safe_start(operation, args, kwargs)
                            self._safe_error(trace, operation, error)
                            raise
                        if inspect.isgenerator(resolved):
                            return self._observed_generator(resolved, operation, args, kwargs)
                        if inspect.isasyncgen(resolved):
                            return self._observed_async_generator(resolved, operation, args, kwargs)
                        trace = self._safe_start(operation, args, kwargs)
                        return self._safe_settle(trace, operation, resolved)

                    return await_result()
                if inspect.isgenerator(result):
                    return self._observed_generator(result, operation, args, kwargs)
                if inspect.isasyncgen(result):
                    return self._observed_async_generator(result, operation, args, kwargs)
                trace = self._safe_start(operation, args, kwargs)
                return self._safe_settle(trace, operation, result)

            wrapper = sync_wrapper
        try:
            setattr(target, key, wrapper)
        except BaseException:
            _restore_attribute(target, key, wrapper, had_own, own_value)
            raise

        def restore() -> None:
            _restore_attribute(target, key, wrapper, had_own, own_value)

        return restore

    def _observes_call(
        self,
        path: tuple[str, ...],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> bool:
        if self.provider != "anthropic" or path != ("messages", "_post"):
            return True
        request_path = args[0] if args else kwargs.get("path")
        return request_path == "/v1/messages"

    def _safe_start(
        self, operation: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> _Trace | None:
        try:
            return self._start(operation, args, kwargs)
        except BaseException:
            return None

    def _safe_settle(self, trace: _Trace | None, operation: str, result: Any) -> Any:
        try:
            return self._settle(trace, operation, result)
        except BaseException as error:
            self._record_observation_loss(trace, "settle", error)
            return result

    def _safe_error(self, trace: _Trace | None, operation: str, error: BaseException) -> None:
        try:
            self._error(trace, operation, error)
        except BaseException as observation_error:
            self._record_observation_loss(trace, "error", observation_error)
            return

    def _safe_stream_part(self, trace: _Trace | None, part: Any) -> None:
        if trace is None:
            return
        try:
            self._stream_part(trace, part)
        except BaseException as error:
            self._record_observation_loss(trace, "stream_part", error)
            return

    def _safe_finish_stream(self, trace: _Trace | None, phase: str, native: Any) -> None:
        if trace is None:
            return
        try:
            self._finish_stream(trace, phase, native)
        except BaseException as error:
            self._record_observation_loss(trace, "stream_finish", error)
            return

    def _record_observation_loss(
        self, trace: _Trace | None, stage: str, error: BaseException
    ) -> None:
        if trace is None or trace.observation_loss_recorded:
            return
        trace.observation_loss_recorded = True
        try:
            self.sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": f"{self.provider}.observation.failed",
                    "trace": trace.identity,
                    "native": {
                        "provider": self.provider,
                        "stage": stage,
                        "error_type": type(error).__name__,
                    },
                    "semantic": {
                        "type": "provider.observation.failed",
                        "count": 1,
                    },
                }
            )
        except BaseException:
            return

    def _retain_stream_part(self, trace: _Trace, part: Any) -> bool:
        remaining_bytes = _MAX_PROVIDER_STREAM_RETAINED_BYTES - trace.stream_retained_bytes
        remaining_nodes = _MAX_PROVIDER_STREAM_RETAINED_NODES - trace.stream_retained_nodes
        cost = _retained_value_cost(part, remaining_bytes, remaining_nodes)
        if cost is None:
            return False
        byte_count, node_count = cost
        trace.stream_retained_bytes += byte_count
        trace.stream_retained_nodes += node_count
        return True

    def _record_stream_semantic_loss(self, trace: _Trace) -> None:
        if trace.stream_semantic_loss_recorded:
            return
        trace.stream_semantic_loss_recorded = True
        try:
            self.sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": f"{self.provider}.stream.semantic_truncated",
                    "trace": trace.identity,
                    "native": {
                        "provider": self.provider,
                        "byte_limit": _MAX_PROVIDER_STREAM_SEMANTIC_BYTES,
                        "node_limit": _MAX_PROVIDER_STREAM_SEMANTIC_NODES,
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "reason": "provider_stream_semantic_truncated",
                        "count": 1,
                        "detail": (
                            "The model-visible provider stream exceeded the "
                            "bounded semantic accumulation budget."
                        ),
                    },
                }
            )
        except BaseException as error:
            self._record_observation_loss(trace, "stream_semantic_loss", error)

    def _record_evidence_retention_loss(self, trace: _Trace, area: str) -> None:
        if trace.evidence_retention_loss_recorded:
            return
        trace.evidence_retention_loss_recorded = True
        try:
            self.sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": f"{self.provider}.evidence.retention_truncated",
                    "trace": trace.identity,
                    "native": {
                        "provider": self.provider,
                        "area": area,
                        "byte_limit": _MAX_PROVIDER_EVIDENCE_BYTES,
                        "node_limit": _MAX_PROVIDER_EVIDENCE_NODES,
                    },
                    "semantic": {
                        "type": "provider.evidence.retention_truncated",
                        "count": 1,
                    },
                }
            )
        except BaseException as error:
            self._record_observation_loss(trace, "evidence_retention_loss", error)

    def _start(
        self, operation: str, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> _Trace | None:
        request = (
            kwargs["body"]
            if self.provider == "anthropic"
            and operation == "messages"
            and type(kwargs.get("body")) is dict
            else kwargs
            if kwargs
            else args[0]
            if args
            else None
        )
        context = _provider_context.get() or {}
        opened = self.sink.open_trace(
            {
                "name": f"{self.provider}.{operation}",
                "coverage": _PROVIDER_REQUEST_COVERAGE,
                **context,
                "native": {"provider": self.provider, "operation": operation},
                "semantic": {
                    "type": "agent.run",
                    "name": f"{self.provider}.{operation}",
                    "input": {"provider": self.provider, "operation": operation},
                },
            }
        )
        if not opened.accepted or opened.identity is None:
            return None
        trace = _Trace(
            opened.identity,
            f"{self.provider}.{operation}",
            operation,
            dict(context),
        )
        try:
            self._populate_start(trace, operation, request)
        except BaseException as error:
            self._record_observation_loss(trace, "start", error)
        return trace

    def _populate_start(self, trace: _Trace, operation: str, request: Any) -> None:
        gemini_request_retained = _retained_value_cost(
            request,
            _MAX_PROVIDER_EVIDENCE_BYTES,
            _MAX_PROVIDER_EVIDENCE_NODES,
        ) if self.provider == "gemini" else None
        if self.provider == "gemini" and gemini_request_retained is not None:
            trace.gemini_request_contents = _gemini_request_contents(request)
        tool_results = (
            _gemini_terminal_function_responses(request)
            if self.provider == "gemini" and gemini_request_retained is not None
            else []
        )
        semantic_call_ids = self._take_gemini_call_ids(trace, tool_results)
        if tool_results and semantic_call_ids is None:
            self.sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "gemini.function_responses.unpaired",
                    "trace": trace.identity,
                    "native": {
                        "provider": "gemini",
                        "function_responses": tool_results,
                    },
                    "semantic": {
                        "type": "gemini.function_responses.unpaired",
                        "count": len(tool_results),
                    },
                }
            )
        trace_id = trace.identity["trace_id"]
        with self._context_histories_lock:
            history = self._context_histories.get(trace_id)
            if history is None:
                while len(self._context_histories) >= _MAX_PROVIDER_CONTEXT_TRACES:
                    self._context_histories.pop(next(iter(self._context_histories)))
                history = _ContextHistory()
                self._context_histories[trace_id] = history
            plan = self._plan_request_messages(trace, request, history)
            native_request = _provider_request_native(
                self.provider,
                request,
                message_count=plan.message_count,
            )
            retained = _retained_value_cost(
                native_request,
                _MAX_PROVIDER_EVIDENCE_BYTES,
                _MAX_PROVIDER_EVIDENCE_NODES,
            )
            if retained is None:
                self._record_evidence_retention_loss(trace, "request")
            context_complete = plan.exact
            request_fields = (
                _model_request_fields(
                    self.provider,
                    request,
                    plan.context_refs,
                    plan.context_base_ref,
                )
                if retained is not None
                else {
                    "context_refs": plan.context_refs,
                    **(
                        {"context_base_ref": plan.context_base_ref}
                        if plan.context_base_ref is not None
                        else {}
                    ),
                }
            )
            if not context_complete:
                request_fields.pop("context_refs", None)
                request_fields.pop("context_base_ref", None)
            request_semantic: dict[str, Any] = {
                "type": "model.request",
                "provider": self.provider,
                **request_fields,
            }
            if semantic_call_ids is not None:
                request_semantic["call_ids"] = list(semantic_call_ids)
            request_receipt = self.sink.record(
                {
                    "kind": "model",
                    "phase": "event",
                    "name": f"{self.provider}.request",
                    "trace": trace.identity,
                    "native": {
                        "provider": self.provider,
                        "operation": operation,
                        "request": native_request
                        if retained is not None
                        else {"retained": False},
                    },
                    "semantic": request_semantic,
                }
            )
            if (
                context_complete
                and request_receipt.accepted
                and request_receipt.record_id is not None
            ):
                trace.request_record_id = request_receipt.record_id
                history.message_count = plan.message_count
                history.digest = plan.digest
                history.request_ref = request_receipt.record_id
            elif request_receipt.accepted and request_receipt.record_id is not None:
                trace.request_record_id = request_receipt.record_id

    def _plan_request_messages(
        self,
        trace: _Trace,
        request: Any,
        history: _ContextHistory,
    ) -> _ContextPlan:
        messages = [
            _provider_context_message(self.provider, message)
            for message in _request_messages(self.provider, request)
        ]
        mapped_complete = _request_context_complete(
            self.provider,
            request,
            len(messages),
        )
        snapshot_complete = not any(
            _has_inexact_snapshot(message) for message in messages
        )
        exact = mapped_complete and snapshot_complete
        if not exact:
            self.sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": f"{self.provider}.context.unavailable",
                    "trace": trace.identity,
                    "native": {
                        "provider": self.provider,
                        "mapped_complete": mapped_complete,
                        "snapshot_complete": snapshot_complete,
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "reason": "unsupported_native_value",
                        "count": 1,
                        "detail": (
                            "The provider request context contained an unmapped "
                            "or incomplete native value, so authoritative context "
                            "references were omitted."
                        ),
                    },
                }
            )
        current_digest = _context_digest(messages) if exact else None
        exact = exact and current_digest is not None
        exact_prefix = (
            exact
            and history.request_ref is not None
            and history.digest is not None
            and len(messages) >= history.message_count
            and _context_digest(messages, limit=history.message_count)
            == history.digest
        )
        start = history.message_count if exact_prefix else 0
        context_refs: list[str] = []
        for message in islice(messages, start, None):
            receipt = self.sink.record(
                {
                    "kind": "model",
                    "phase": "event",
                    "name": f"{self.provider}.context.message",
                    "trace": trace.identity,
                    "native": {
                        "provider": self.provider,
                        "message": message["native"],
                    },
                    "semantic": {
                        "type": "message",
                        "origin": "context",
                        **message["semantic"],
                    },
                }
            )
            if receipt.accepted and receipt.record_id is not None:
                context_refs.append(receipt.record_id)
            else:
                exact = False
        return _ContextPlan(
            len(messages),
            current_digest if exact else None,
            context_refs,
            history.request_ref if exact_prefix else None,
            exact,
        )

    def _settle(self, trace: _Trace | None, operation: str, result: Any) -> Any:
        if trace is None:
            return result
        owned = {} if type(result) is dict else native_own_data(result)
        iterator = dict.get(owned, "_iterator")
        if iterator is not None:
            is_async = inspect.getattr_static(iterator, "__anext__", None) is not None
            wrapper: Any = (
                _ObservedAsyncIterator(iterator, self, trace)
                if is_async
                else _ObservedIterator(iterator, self, trace)
            )
            try:
                dict.__setitem__(owned, "_iterator", wrapper)
            except BaseException:
                if dict.get(owned, "_iterator") is not iterator:
                    dict.__setitem__(owned, "_iterator", iterator)
                raise
            self._observe_stream_close(result, trace)
            return result
        retained = _retained_value_cost(
            result,
            _MAX_PROVIDER_EVIDENCE_BYTES,
            _MAX_PROVIDER_EVIDENCE_NODES,
        )
        if retained is None:
            self._record_evidence_retention_loss(trace, "response")
            truncated_response_record: dict[str, Any] = {
                "kind": "model",
                "phase": "event",
                "name": f"{self.provider}.response",
                "trace": trace.identity,
                "native": {
                    "provider": self.provider,
                    "operation": operation,
                    "response": result,
                },
                "semantic": {
                    "type": "model.response",
                    "provider": self.provider,
                    "status": "completed",
                },
            }
            if trace.request_record_id is not None:
                truncated_response_record["parent_record_id"] = trace.request_record_id
            self.sink.record(truncated_response_record)
            self._record_state(trace, "completed")
            self._close(trace, "end", {"provider": self.provider, "operation": operation})
            return result
        response_id = _exact_openai_response_id(self.provider, operation, result)
        response_record: dict[str, Any] = {
            "kind": "model",
            "phase": "event",
            "name": f"{self.provider}.response",
            "trace": trace.identity,
            "native": {
                "provider": self.provider,
                "operation": operation,
                "response": result,
            },
            "semantic": {"type": "model.response", "provider": self.provider},
        }
        if trace.request_record_id is not None:
            response_record["parent_record_id"] = trace.request_record_id
        response_record["semantic"].update(_model_response_fields(self.provider, result))
        if response_id is not None:
            response_record["native_identity"] = response_id
            response_record["coverage"] = _OPENAI_RESPONSE_COVERAGE
        gemini_afc_history = (
            self._record_gemini_afc_history(trace, result) if self.provider == "gemini" else False
        )
        self.sink.record(response_record)
        usage = _field(result, "usage", "usage_metadata", "usageMetadata")
        if usage is not None:
            self.sink.record(
                {
                    "kind": "model",
                    "phase": "event",
                    "name": f"{self.provider}.usage",
                    "trace": trace.identity,
                    "native": {"provider": self.provider, "usage": usage},
                    "semantic": {"type": "capture.redundant", "provider": self.provider},
                }
            )
        if self.provider == "gemini":
            trace.gemini_tool_candidate_indexes.update(_gemini_tool_candidate_indexes(result))
        for tool in [] if gemini_afc_history else _tool_calls(result):
            proposal = _tool_proposal_fields(self.provider, tool)
            if proposal is None:
                continue
            semantic = {
                "type": "tool.proposal",
                "provider": self.provider,
                **proposal,
            }
            if self.provider == "gemini":
                native_call_id = _exact_string(proposal.get("native_call_id"))
                if native_call_id is not None:
                    trace.gemini_call_ids.append(native_call_id)
            self.sink.record(
                {
                    "kind": "tool",
                    "phase": "event",
                    "name": f"{self.provider}.tool.proposed",
                    "trace": trace.identity,
                    "native": {"provider": self.provider, "tool": tool},
                    "semantic": semantic,
                }
            )
        self._record_state(trace, "completed")
        self._close(trace, "end", {"provider": self.provider, "operation": operation})
        return result

    def _observe_stream_close(self, target: Any, trace: _Trace) -> None:
        owned = native_own_data(target)
        close = dict.get(owned, "close")
        if not callable(close):
            static_close = inspect.getattr_static(target, "close", None)
            if not inspect.isfunction(static_close):
                return
            close = static_close.__get__(target, type(target))
        if not callable(close):
            return
        if inspect.iscoroutinefunction(close):

            async def observed_async_close(*args: Any, **kwargs: Any) -> Any:
                try:
                    result = await close(*args, **kwargs)
                except BaseException as error:
                    self._safe_finish_stream(trace, "error", {"error": error})
                    raise
                self._safe_finish_stream(trace, "cancelled", {"consumer_cancelled": True})
                return result

            setattr(target, "close", observed_async_close)
            return

        def observed_sync_close(*args: Any, **kwargs: Any) -> Any:
            try:
                result = close(*args, **kwargs)
            except BaseException as error:
                self._safe_finish_stream(trace, "error", {"error": error})
                raise
            self._safe_finish_stream(trace, "cancelled", {"consumer_cancelled": True})
            return result

        setattr(target, "close", observed_sync_close)

    def _observed_generator(
        self,
        target: Any,
        operation: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        trace: _Trace | None = None
        trace_started = False

        def ensure_trace() -> _Trace | None:
            nonlocal trace, trace_started
            if not trace_started:
                trace_started = True
                trace = self._safe_start(operation, args, kwargs)
            return trace

        def observed() -> Any:
            try:
                try:
                    value = next(target)
                except StopIteration as stopped:
                    self._safe_finish_stream(ensure_trace(), "end", {"consumed": True})
                    return stopped.value
                except BaseException as error:
                    self._safe_finish_stream(ensure_trace(), "error", {"error": error})
                    raise
                while True:
                    self._safe_stream_part(ensure_trace(), value)
                    try:
                        sent = yield value
                    except GeneratorExit:
                        try:
                            target.close()
                        except BaseException as error:
                            self._safe_finish_stream(ensure_trace(), "error", {"error": error})
                            raise
                        self._safe_finish_stream(
                            ensure_trace(),
                            "cancelled",
                            {"consumer_cancelled": True},
                        )
                        raise
                    except BaseException as injected:
                        try:
                            value = target.throw(injected)
                        except StopIteration as stopped:
                            self._safe_finish_stream(ensure_trace(), "end", {"consumed": True})
                            return stopped.value
                        except BaseException as error:
                            self._safe_finish_stream(ensure_trace(), "error", {"error": error})
                            raise
                    else:
                        try:
                            value = target.send(sent)
                        except StopIteration as stopped:
                            self._safe_finish_stream(ensure_trace(), "end", {"consumed": True})
                            return stopped.value
                        except BaseException as error:
                            self._safe_finish_stream(ensure_trace(), "error", {"error": error})
                            raise
            except BaseException:
                raise

        return observed()

    def _observed_async_generator(
        self,
        target: Any,
        operation: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        trace: _Trace | None = None
        trace_started = False

        def ensure_trace() -> _Trace | None:
            nonlocal trace, trace_started
            if not trace_started:
                trace_started = True
                trace = self._safe_start(operation, args, kwargs)
            return trace

        async def observed() -> Any:
            try:
                value = await target.__anext__()
            except StopAsyncIteration:
                self._safe_finish_stream(ensure_trace(), "end", {"consumed": True})
                return
            except BaseException as error:
                self._safe_finish_stream(ensure_trace(), "error", {"error": error})
                raise
            while True:
                self._safe_stream_part(ensure_trace(), value)
                try:
                    sent = yield value
                except GeneratorExit:
                    try:
                        await target.aclose()
                    except BaseException as error:
                        self._safe_finish_stream(ensure_trace(), "error", {"error": error})
                        raise
                    self._safe_finish_stream(
                        ensure_trace(),
                        "cancelled",
                        {"consumer_cancelled": True},
                    )
                    raise
                except BaseException as injected:
                    try:
                        value = await target.athrow(injected)
                    except StopAsyncIteration:
                        self._safe_finish_stream(ensure_trace(), "end", {"consumed": True})
                        return
                    except BaseException as error:
                        self._safe_finish_stream(ensure_trace(), "error", {"error": error})
                        raise
                else:
                    try:
                        value = await target.asend(sent)
                    except StopAsyncIteration:
                        self._safe_finish_stream(ensure_trace(), "end", {"consumed": True})
                        return
                    except BaseException as error:
                        self._safe_finish_stream(ensure_trace(), "error", {"error": error})
                        raise

        return observed()

    def _record_gemini_afc_history(self, trace: _Trace, result: Any) -> bool:
        history, pairs = _gemini_afc_history_pairs(
            result,
            trace.gemini_request_contents,
        )
        if history is None or (isinstance(history, list) and not history):
            return False
        if pairs is None:
            self.sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "gemini.afc.history.unpaired",
                    "trace": trace.identity,
                    "native": {
                        "provider": "gemini",
                        "automatic_function_calling_history": history,
                    },
                    "semantic": {
                        "type": "gemini.afc.history.unpaired",
                        "count": 1,
                    },
                }
            )
            return True
        for content_index, part_index, function_call, function_response in pairs:
            name = _field(function_call, "name")
            arguments = _field(function_call, "args")
            payload = _field(function_response, "response")
            call_id = _exact_string(_field(function_call, "id", "call_id", "callId"))
            if call_id is None:
                return True
            trace.gemini_call_ids.append(call_id)
            self.sink.record(
                {
                    "kind": "tool",
                    "phase": "event",
                    "name": "gemini.tool.proposed",
                    "trace": trace.identity,
                    "native": {
                        "provider": "gemini",
                        "function_call": function_call,
                    },
                    "semantic": {
                        "type": "tool.proposal",
                        "provider": "gemini",
                        "call_id": call_id,
                        "name": name,
                        "input": arguments,
                    },
                }
            )
            call_receipt = self.sink.record(
                {
                    "kind": "tool",
                    "phase": "event",
                    "name": "gemini.tool.call",
                    "trace": trace.identity,
                    "native": {
                        "provider": "gemini",
                        "function_call": function_call,
                        "function_response": function_response,
                    },
                    "semantic": {
                        "type": "tool.execution",
                        "provider": "gemini",
                        "call_id": call_id,
                        "name": name,
                        "input": arguments,
                    },
                }
            )
            failed = isinstance(payload, dict) and "error" in payload
            semantic_result: dict[str, Any] = {
                "type": "tool.error" if failed else "tool.result",
                "provider": "gemini",
                "call_id": call_id,
                "status": "failed" if failed else "succeeded",
            }
            if failed:
                semantic_result["error"] = {
                    "type": "tool_error",
                    "message": _safe_text(payload["error"], "tool_error"),
                    "recoverable": False,
                }
            else:
                semantic_result["output"] = payload["result"]
            self.sink.record(
                {
                    "kind": "tool",
                    "phase": "error" if failed else "end",
                    "name": "gemini.tool.result",
                    "trace": trace.identity,
                    **(
                        {"parent_record_id": call_receipt.record_id}
                        if call_receipt.record_id is not None
                        else {}
                    ),
                    "native": {
                        "provider": "gemini",
                        "function_response": function_response,
                    },
                    "semantic": semantic_result,
                }
            )
        return True

    def _stream_part(self, trace: _Trace, part: Any) -> None:
        if trace.closed:
            return
        trace.response_id = (
            _exact_openai_stream_response_id(self.provider, trace.operation, part)
            or trace.response_id
        )
        native_retained = self._retain_stream_part(trace, part)
        if native_retained and _field(part, "type") in {
            "response.completed",
            "response.failed",
            "response.incomplete",
        }:
            trace.last_part = part
        if not _accumulate_stream(trace, self.provider, part):
            self._record_stream_semantic_loss(trace)
        self.sink.record(
            {
                "kind": "stream",
                "phase": "event",
                "name": f"{self.provider}.stream.delta",
                "trace": trace.identity,
                "native": {
                    "provider": self.provider,
                    "delta": {"observed": True},
                    "part": (
                        part
                        if native_retained
                        else {"observed": True, "retained": False}
                    ),
                },
                "semantic": {"type": "capture.redundant", "provider": self.provider},
            }
        )
        # The terminal response is the authoritative semantic record. Per-delta
        # rows retain native evidence but do not become hot trace records.

    def _finish_stream(self, trace: _Trace, phase: str, native: Any) -> None:
        if trace.closed:
            return
        if phase == "error":
            stream_error = _field(native, "error")
            if isinstance(stream_error, BaseException):
                failed_partial_record_id = self._record_failed_stream_partial(
                    trace,
                    stream_error,
                )
                if failed_partial_record_id is not None:
                    self.sink.record(
                        {
                            "kind": "unknown",
                            "phase": "gap",
                            "name": f"{self.provider}.stream.terminal_not_observed",
                            "trace": trace.identity,
                            "parent_record_id": failed_partial_record_id,
                            "native": {
                                "provider": self.provider,
                                "stream_failed": True,
                            },
                            "semantic": {
                                "type": "capture.gap",
                                "reason": "stream_terminal_not_observed",
                                "detail": (
                                    "The provider stream failed after partial "
                                    "content was delivered and before a terminal "
                                    "response was observed."
                                ),
                            },
                        }
                    )
                self._error(trace, trace.operation, stream_error)
                return
        partial_response_record_id: str | None = None
        terminal = (
            {**native, "terminal": trace.last_part}
            if phase == "end" and isinstance(native, dict)
            else native
        )
        self.sink.record(
            {
                "kind": "stream",
                "phase": phase,
                "name": f"{self.provider}.stream.{'terminal' if phase == 'end' else phase}",
                "trace": trace.identity,
                "native": {
                    "provider": self.provider,
                    "terminal": trace.last_part,
                    "finish_reason": trace.stream_finish_reason or "consumed",
                }
                if phase == "end"
                else terminal,
                "semantic": {
                    "type": "capture.redundant",
                    "provider": self.provider,
                },
            }
        )
        if phase in {"end", "cancelled"} and (
            phase == "end" or _has_stream_response_evidence(trace)
        ):
            semantic_terminal = _stream_terminal(trace, self.provider)
            response_record: dict[str, Any] = {
                "kind": "model",
                "phase": "event",
                "name": f"{self.provider}.response",
                "trace": trace.identity,
                "native": {
                    "provider": self.provider,
                    **(
                        {"response": trace.last_part}
                        if trace.last_part is not None
                        else {
                            "partial_response": semantic_terminal,
                            "consumer_cancelled": True,
                        }
                    ),
                },
                "semantic": {"type": "model.response", "provider": self.provider},
            }
            if trace.request_record_id is not None:
                response_record["parent_record_id"] = trace.request_record_id
            response_record["semantic"].update(
                _model_response_fields(self.provider, semantic_terminal)
            )
            if phase == "cancelled" and trace.last_part is None:
                response_record["semantic"]["status"] = "cancelled"
            if trace.response_id is not None:
                response_record["native_identity"] = trace.response_id
                response_record["coverage"] = _OPENAI_RESPONSE_COVERAGE
            response_receipt = self.sink.record(response_record)
            partial_response_record_id = response_receipt.record_id
            if phase == "end" or trace.last_part is not None:
                self._record_usage_and_tools(trace, semantic_terminal)
        if phase == "cancelled" and trace.last_part is None:
            self.sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": f"{self.provider}.stream.terminal_not_observed",
                    "trace": trace.identity,
                    **(
                        {
                            "parent_record_id": (
                                partial_response_record_id or trace.request_record_id
                            )
                        }
                        if partial_response_record_id is not None
                        or trace.request_record_id is not None
                        else {}
                    ),
                    "native": {
                        "provider": self.provider,
                        "consumer_cancelled": True,
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "reason": "stream_terminal_not_observed",
                        "detail": (
                            "The consumer closed the provider stream before a "
                            "terminal event was observed"
                            + (
                                "; the response contains only observed partial evidence."
                                if partial_response_record_id is not None
                                else "."
                            )
                        ),
                    },
                }
            )
        self._record_state(trace, "completed" if phase == "end" else phase)
        self._close(trace, phase, terminal)

    def _record_failed_stream_partial(
        self,
        trace: _Trace,
        error: BaseException,
    ) -> str | None:
        if not _has_stream_response_evidence(trace):
            return None
        semantic_terminal = _stream_terminal(trace, self.provider)
        response_record: dict[str, Any] = {
            "kind": "model",
            "phase": "error",
            "name": f"{self.provider}.response",
            "trace": trace.identity,
            "error_identity": error,
            "native": {
                "provider": self.provider,
                "partial_response": semantic_terminal,
                "stream_failed": True,
            },
            "semantic": {
                "type": "model.response",
                "provider": self.provider,
                **_model_response_fields(self.provider, semantic_terminal),
                "status": "failed",
            },
        }
        if trace.request_record_id is not None:
            response_record["parent_record_id"] = trace.request_record_id
        if trace.response_id is not None:
            response_record["native_identity"] = trace.response_id
            response_record["coverage"] = _OPENAI_RESPONSE_COVERAGE
        record_id = self.sink.record(response_record).record_id
        return record_id if isinstance(record_id, str) else None

    def _record_state(self, trace: _Trace, state: str) -> None:
        self.sink.record(
            {
                "kind": "state",
                "phase": "event",
                "name": f"{self.provider}.operation.state",
                "trace": trace.identity,
                "native": {
                    "provider": self.provider,
                    "operation": trace.operation,
                    "state": state,
                },
                "semantic": {"type": "capture.redundant", "provider": self.provider},
            }
        )

    def _record_usage_and_tools(self, trace: _Trace, value: Any) -> None:
        event_type = _field(value, "type")
        nested = _field(value, "response")
        observed = (
            nested
            if isinstance(event_type, str)
            and event_type in {"response.completed", "response.failed", "response.incomplete"}
            and nested is not None
            else value
        )
        usage = _field(observed, "usage", "usage_metadata", "usageMetadata")
        if usage is not None:
            self.sink.record(
                {
                    "kind": "model",
                    "phase": "event",
                    "name": f"{self.provider}.usage",
                    "trace": trace.identity,
                    "native": {"provider": self.provider, "usage": usage},
                    "semantic": {"type": "capture.redundant", "provider": self.provider},
                }
            )
        if self.provider == "gemini":
            trace.gemini_tool_candidate_indexes.update(_gemini_tool_candidate_indexes(observed))
        for tool in _tool_calls(observed):
            proposal = _tool_proposal_fields(self.provider, tool)
            if proposal is None:
                continue
            semantic = {
                "type": "tool.proposal",
                "provider": self.provider,
                **proposal,
            }
            if self.provider == "gemini":
                native_call_id = _exact_string(proposal.get("native_call_id"))
                if native_call_id is not None:
                    trace.gemini_call_ids.append(native_call_id)
            self.sink.record(
                {
                    "kind": "tool",
                    "phase": "event",
                    "name": f"{self.provider}.tool.proposed",
                    "trace": trace.identity,
                    "native": {"provider": self.provider, "tool": tool},
                    "semantic": semantic,
                }
            )

    def _remember_gemini_call_ids(self, trace: _Trace) -> None:
        conversation_id = trace.context.get("conversation_id")
        turn_id = trace.context.get("turn_id")
        if (
            not isinstance(conversation_id, str)
            or not conversation_id
            or not isinstance(turn_id, str)
            or not turn_id
            or not trace.gemini_call_ids
        ):
            return
        key = (conversation_id, turn_id)
        with self._gemini_pending_lock:
            # Alternative candidates and repeated producer keys are ambiguous,
            # so poison the turn instead of inventing one ordered batch.
            if len(trace.gemini_tool_candidate_indexes) != 1 or key in self._gemini_pending:
                self._gemini_pending[key] = None
            elif len(set(trace.gemini_call_ids)) != len(trace.gemini_call_ids):
                self._gemini_pending[key] = None
            else:
                self._gemini_pending[key] = tuple(trace.gemini_call_ids)
            while len(self._gemini_pending) > _MAX_GEMINI_PENDING_TOOL_TURNS:
                self._gemini_pending.pop(next(iter(self._gemini_pending)))

    def _take_gemini_call_ids(self, trace: _Trace, results: list[Any]) -> tuple[str, ...] | None:
        if self.provider != "gemini":
            return None
        conversation_id = trace.context.get("conversation_id")
        turn_id = trace.context.get("turn_id")
        previous_turn_id = trace.context.get("previous_turn_id")
        if not all(
            isinstance(value, str) and value
            for value in (conversation_id, turn_id, previous_turn_id)
        ):
            return None
        with self._gemini_pending_lock:
            # Consume before validating so a mismatch cannot later be guessed into a match.
            pending = self._gemini_pending.pop(
                (conversation_id, previous_turn_id),  # type: ignore[arg-type]
                None,
            )
        result_ids = tuple(
            _exact_string(_field(result, "id", "call_id", "callId")) for result in results
        )
        if (
            pending is None
            or any(result_id is None for result_id in result_ids)
            or len(set(result_ids)) != len(result_ids)
            or set(result_ids) != set(pending)
        ):
            return None
        return tuple(result_id for result_id in result_ids if result_id is not None)

    def _error(self, trace: _Trace | None, operation: str, error: BaseException) -> None:
        if trace is None:
            return
        structured_error = _structured_error_evidence(error)
        error_evidence = _provider_error_evidence(error)
        error_record: dict[str, Any] = {
            "kind": "error",
            "name": f"{self.provider}.error",
            "trace": trace.identity,
            "phase": "error",
            "error_identity": error,
            "native": {
                "provider": self.provider,
                "operation": operation,
                "error": error_evidence,
                **({"structured_error": structured_error} if structured_error is not None else {}),
            },
            "semantic": {
                "type": "model.error",
                "provider": self.provider,
                "error": _normalized_error(error),
            },
        }
        if trace.request_record_id is not None:
            error_record["parent_record_id"] = trace.request_record_id
        self.sink.record(error_record)
        self._close(
            trace,
            "error",
            {"error": error_evidence},
            error_identity=error,
        )

    def _close(
        self,
        trace: _Trace,
        phase: str,
        native: Any,
        *,
        error_identity: BaseException | None = None,
    ) -> None:
        if trace.closed:
            return
        if self.provider == "gemini" and phase == "end":
            self._remember_gemini_call_ids(trace)
        trace.closed = True
        self.sink.record(
            {
                "kind": "lifecycle",
                "phase": phase,
                "name": trace.name,
                "trace": trace.identity,
                **(
                    {"error_identity": error_identity}
                    if isinstance(error_identity, BaseException)
                    else {}
                ),
                "native": native,
                "semantic": {
                    "type": "agent.run",
                    "status": (
                        "succeeded"
                        if phase == "end"
                        else "cancelled"
                        if phase == "cancelled"
                        else "failed"
                    ),
                },
            }
        )


def _installed_version(distribution: str, expected: str | None) -> str:
    installed = distribution_version(distribution)
    if expected is not None and expected != installed:
        raise ValueError(
            f"requested version {expected} does not match installed "
            f"{distribution} distribution {installed}"
        )
    return installed


def openai_provider_adapter(
    *, version: str | None = None, provider: str = "openai"
) -> _ProviderAdapter:
    """Create an OpenAI or OpenAI-compatible/OpenRouter explicit-client adapter."""
    if provider not in {"openai", "openrouter"}:
        raise ValueError("provider must be openai or openrouter")
    return _ProviderAdapter(
        provider,
        _installed_version("openai", version),
        (("responses", "create"), ("chat", "completions", "create")),
    )


def anthropic_provider_adapter(*, version: str | None = None) -> _ProviderAdapter:
    return _ProviderAdapter(
        "anthropic", _installed_version("anthropic", version), (("messages", "create"),)
    )


def gemini_provider_adapter(*, version: str | None = None) -> _ProviderAdapter:
    return _ProviderAdapter(
        "gemini",
        _installed_version("google-genai", version),
        (
            ("models", "generate_content"),
            ("models", "generate_content_stream"),
            ("aio", "models", "generate_content"),
            ("aio", "models", "generate_content_stream"),
        ),
    )


def _request_messages(provider: str, request: Any) -> list[dict[str, Any]]:
    if not isinstance(request, dict):
        return []
    if provider == "gemini":
        return _gemini_request_messages(request)
    messages: list[dict[str, Any]] = []
    if provider == "anthropic" and "system" in request:
        messages.append(
            {
                "role": "system",
                "content": request["system"],
                "native": native_snapshot(request["system"]),
            }
        )
    if (
        provider in {"openai", "openrouter"}
        and request.get("instructions") is not None
    ):
        messages.append(
            {
                "role": "system",
                "content": request["instructions"],
                "native": native_snapshot(request["instructions"]),
            }
        )
    candidates = request.get("messages")
    if provider in {"openai", "openrouter"} and not isinstance(candidates, list):
        response_input = request.get("input")
        if isinstance(response_input, str):
            messages.append(
                {
                    "role": "user",
                    "content": response_input,
                    "native": response_input,
                }
            )
            return messages
        candidates = response_input
    if not isinstance(candidates, list):
        return messages
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if (
            provider in {"openai", "openrouter"}
            and candidate.get("type") == "function_call_output"
            and "output" in candidate
        ):
            call_id = _exact_string(candidate.get("call_id"))
            messages.append(
                {
                    "role": "tool",
                    "content": candidate["output"],
                    **({"call_id": call_id} if call_id else {}),
                    "native": native_snapshot(candidate),
                }
            )
            continue
        if (
            provider in {"openai", "openrouter"}
            and candidate.get("type") == "function_call"
        ):
            call_id = _exact_string(candidate.get("call_id"))
            name = _exact_string(candidate.get("name"))
            messages.append(
                {
                    "role": "assistant",
                    "content": native_snapshot(candidate),
                    **({"name": name} if name else {}),
                    **({"call_id": call_id} if call_id else {}),
                    "native": native_snapshot(candidate),
                }
            )
            continue
        role = candidate.get("role")
        if role not in {"system", "developer", "user", "assistant", "tool"}:
            continue
        if "content" not in candidate and "tool_calls" not in candidate:
            continue
        content = candidate.get("content")
        if "tool_calls" in candidate:
            content = {
                "content": native_snapshot(content),
                "tool_calls": native_snapshot(candidate["tool_calls"]),
            }
        name = _exact_string(candidate.get("name"))
        call_id = _exact_string(candidate.get("tool_call_id"))
        messages.append(
            {
                "role": role,
                "content": content,
                **({"name": name} if name else {}),
                **({"call_id": call_id} if call_id else {}),
                "native": native_snapshot(candidate),
            }
        )
    return messages


def _gemini_request_messages(request: dict[str, Any]) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    config = request.get("config")
    system_instruction = _field(
        config,
        "system_instruction",
        "systemInstruction",
    )
    if system_instruction is not None:
        messages.append(
            {
                "role": "system",
                "content": system_instruction,
                "native": native_snapshot(system_instruction),
            }
        )
    contents = request.get("contents")
    if isinstance(contents, str):
        messages.append(
            {
                "role": "user",
                "content": contents,
                "native": contents,
            }
        )
        return messages
    if not isinstance(contents, list):
        return messages
    for content in contents:
        role = _field(content, "role")
        normalized_role = "assistant" if role == "model" else "user" if role == "user" else None
        parts = _field(content, "parts")
        if normalized_role is not None and isinstance(parts, list):
            messages.append(
                {
                    "role": normalized_role,
                    "content": parts,
                    "native": native_snapshot(content),
                }
            )
    return messages


def _request_context_complete(
    provider: str,
    request: Any,
    mapped_count: int,
) -> bool:
    if type(request) is not dict:
        return False
    if provider == "gemini":
        contents = request.get("contents")
        system_count = int(
            _field(
                request.get("config"),
                "system_instruction",
                "systemInstruction",
            )
            is not None
        )
        return (
            isinstance(contents, str)
            and mapped_count == 1 + system_count
            or type(contents) is list
            and mapped_count == len(contents) + system_count
            or contents is None
            and mapped_count == system_count
        )
    if provider in {"openai", "openrouter"} and (
        request.get("previous_response_id") is not None
        or request.get("conversation") is not None
        or request.get("prompt") is not None
    ):
        return False
    candidates = request.get("messages")
    system_count = (
        1
        if provider == "anthropic" and "system" in request
        else 1
        if provider in {"openai", "openrouter"}
        and request.get("instructions") is not None
        else 0
    )
    if type(candidates) is list:
        return mapped_count == len(candidates) + system_count
    if provider in {"openai", "openrouter"} and "input" in request:
        response_input = request.get("input")
        if isinstance(response_input, str):
            return mapped_count == 1 + system_count
        if type(response_input) is list:
            return mapped_count == len(response_input) + system_count
        return False
    return candidates is None and mapped_count == system_count


def _provider_context_message(provider: str, message: dict[str, Any]) -> dict[str, Any]:
    semantic = {
        "role": message["role"],
        "content": _canonical_provider_message_content(
            provider,
            message["content"],
        ),
        **(
            {"name": message["name"]}
            if isinstance(message.get("name"), str)
            else {}
        ),
        **(
            {"call_id": message["call_id"]}
            if isinstance(message.get("call_id"), str)
            else {}
        ),
    }
    return {
        "native": message.get("native", native_snapshot(message)),
        "semantic": semantic,
    }


def _canonical_provider_message_content(provider: str, content: Any) -> Any:
    copied = native_snapshot(content)
    if not isinstance(copied, list):
        return copied
    if provider not in {"anthropic", "gemini"}:
        return copied
    if provider == "gemini":
        return [
            {
                key: value
                for key, value in part.items()
                if key not in {"thought_signature", "thoughtSignature"}
            }
            if isinstance(part, dict)
            else part
            for part in copied
        ]
    canonical: list[Any] = []
    for block in copied:
        if not isinstance(block, dict):
            canonical.append(block)
        elif block.get("type") == "thinking":
            canonical.append({key: value for key, value in block.items() if key != "signature"})
        elif block.get("type") == "redacted_thinking":
            canonical.append({"type": "redacted_thinking"})
        else:
            canonical.append(block)
    return canonical


def _has_inexact_snapshot(value: Any) -> bool:
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if (
                current.get("omitted") == "resource_limit"
                or current.get("$semantic_layer_omitted") is not None
                or (
                    isinstance(current.get("native_type"), str)
                    and isinstance(current.get("reference"), str)
                )
            ):
                return True
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return False


def _context_digest(
    messages: Iterable[dict[str, Any]],
    *,
    limit: int | None = None,
) -> str | None:
    digest = hashlib.sha256()

    def write(value: Any) -> bool:
        value_type = type(value)
        if value is None:
            digest.update(b"N")
            return True
        if value_type is bool:
            digest.update(b"B1" if value else b"B0")
            return True
        if value_type is int:
            encoded = str(value).encode()
            digest.update(b"I" + len(encoded).to_bytes(8, "big") + encoded)
            return True
        if value_type is float:
            encoded = repr(value).encode()
            digest.update(b"F" + len(encoded).to_bytes(8, "big") + encoded)
            return True
        if value_type is str:
            encoded = value.encode()
            digest.update(b"S" + len(encoded).to_bytes(8, "big") + encoded)
            return True
        if value_type in {bytes, bytearray}:
            encoded = bytes(value)
            digest.update(b"Y" + len(encoded).to_bytes(8, "big") + encoded)
            return True
        if value_type is memoryview:
            encoded = value.tobytes()
            digest.update(b"V" + len(encoded).to_bytes(8, "big") + encoded)
            return True
        if value_type is list:
            digest.update(b"L" + len(value).to_bytes(8, "big"))
            return all(write(item) for item in value)
        if value_type is dict and all(type(key) is str for key in value):
            keys = sorted(value)
            digest.update(b"D" + len(keys).to_bytes(8, "big"))
            return all(write(key) and write(value[key]) for key in keys)
        return False

    for index, message in enumerate(messages):
        if limit is not None and index >= limit:
            break
        if not write(message):
            return None
    return digest.hexdigest()


def _model_request_fields(
    provider: str,
    request: Any,
    context_refs: list[str],
    context_base_ref: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "context_refs": context_refs,
        **(
            {"context_base_ref": context_base_ref}
            if context_base_ref is not None
            else {}
        ),
    }
    model = _exact_string(_field(request, "model"))
    if model:
        result["model"] = model
    tools = _request_tool_names(provider, request)
    if tools:
        result["tools"] = tools
    return result


def _request_tool_names(provider: str, request: Any) -> list[str]:
    names: list[str] = []

    def add(value: Any) -> None:
        name = _exact_string(value)
        if name and name not in names:
            names.append(name)

    tools = _field(request, "tools")
    if isinstance(tools, list):
        for tool in tools:
            if provider in {"openai", "openrouter"}:
                add(_field(_field(tool, "function"), "name") or _field(tool, "name"))
            elif provider == "anthropic":
                add(_field(tool, "name"))
            elif provider == "gemini":
                if (callable_name := _gemini_callable_name(tool)) is not None:
                    add(callable_name)
                    continue
                declarations = _field(tool, "function_declarations", "functionDeclarations")
                if isinstance(declarations, list):
                    for declaration in declarations:
                        add(_field(declaration, "name"))
    if provider == "gemini":
        config_tools = _field(_field(request, "config"), "tools")
        if isinstance(config_tools, list):
            for tool in config_tools:
                if (callable_name := _gemini_callable_name(tool)) is not None:
                    add(callable_name)
                    continue
                declarations = _field(tool, "function_declarations", "functionDeclarations")
                if isinstance(declarations, list):
                    for declaration in declarations:
                        add(_field(declaration, "name"))
    return names


def _provider_request_native(
    provider: str,
    request: Any,
    *,
    message_count: int | None = None,
) -> Any:
    if type(request) is not dict:
        return native_snapshot(request)
    context_fields = (
        {"contents"}
        if provider == "gemini"
        else {"messages", "system"}
        if provider == "anthropic"
        else {"input", "instructions", "messages"}
    )
    metadata = {
        key: native_snapshot(value)
        for key, value in request.items()
        if key not in context_fields
    }
    if provider == "gemini" and isinstance(metadata.get("config"), dict):
        metadata["config"] = {
            key: value
            for key, value in metadata["config"].items()
            if key not in {"system_instruction", "systemInstruction"}
        }
    return {
        "message_count": (
            message_count
            if message_count is not None
            else len(_request_messages(provider, request))
        ),
        "metadata": metadata,
    }


def _gemini_callable_name(tool: Any) -> str | None:
    if not (inspect.isfunction(tool) or inspect.ismethod(tool)):
        return None
    return tool.__name__


def _model_response_fields(provider: str, value: Any) -> dict[str, Any]:
    event_type = _field(value, "type")
    nested = _field(value, "response")
    response = (
        nested
        if isinstance(event_type, str)
        and event_type in {"response.completed", "response.failed", "response.incomplete"}
        and nested is not None
        else value
    )
    fields: dict[str, Any] = {"status": "completed"}
    native_status = _exact_string(_field(response, "status"))
    if native_status in {"failed", "incomplete", "cancelled"}:
        fields["status"] = native_status
    model = _exact_string(_field(response, "model", "model_version", "modelVersion"))
    if model:
        fields["model"] = model
    content = _response_content(provider, response)
    if content is not None:
        fields["content"] = content
    reasoning = _response_reasoning(provider, response)
    if reasoning:
        fields["reasoning"] = reasoning
    finish_reason = _finish_reason(response)
    if finish_reason:
        fields["finish_reason"] = finish_reason
    usage = _normalized_usage(_field(response, "usage", "usage_metadata", "usageMetadata"))
    if usage:
        fields["usage"] = usage
    return fields


def _accumulate_stream(trace: _Trace, provider: str, value: Any) -> bool:
    retained = True
    event_index = trace.stream_event_index
    trace.stream_event_index += 1

    def append_text(parts: list[str], counter: str, text: str) -> None:
        nonlocal retained
        byte_count = len(text.encode("utf-8"))
        retained_bytes = getattr(trace, counter)
        node_counter = f"{counter.removesuffix('_bytes')}_nodes"
        retained_nodes = getattr(trace, node_counter)
        if (
            byte_count > _MAX_PROVIDER_STREAM_SEMANTIC_BYTES - retained_bytes
            or retained_nodes >= _MAX_PROVIDER_STREAM_SEMANTIC_NODES
        ):
            retained = False
            return
        if retained_nodes >= _MAX_PROVIDER_MATERIALIZATION_WIDTH:
            parts[-1] = f"{parts[-1]}{text}"
        else:
            parts.append(text)
            retained_nodes += 1
        setattr(trace, counter, retained_bytes + byte_count)
        setattr(trace, node_counter, retained_nodes)

    def append_reasoning(kind: str, text: Any, key: str | None = None) -> None:
        nonlocal retained
        if kind not in {"text", "summary"} or not isinstance(text, str) or not text:
            return
        byte_count = len(text.encode("utf-8"))
        block_index = trace.stream_reasoning_keys.get(key) if key is not None else None
        if block_index is not None:
            if (
                byte_count > _MAX_PROVIDER_STREAM_SEMANTIC_BYTES - trace.stream_reasoning_bytes
                or trace.stream_reasoning_nodes >= _MAX_PROVIDER_STREAM_SEMANTIC_NODES
            ):
                retained = False
                return
            block = trace.stream_reasoning[block_index]
            if block["type"] != kind:
                retained = False
                return
            block["text"] = f"{block['text']}{text}"
            trace.stream_reasoning_bytes += byte_count
            trace.stream_reasoning_nodes += 1
            return
        if (
            byte_count > _MAX_PROVIDER_STREAM_SEMANTIC_BYTES - trace.stream_reasoning_bytes
            or trace.stream_reasoning_nodes >= _MAX_PROVIDER_STREAM_SEMANTIC_NODES
            or len(trace.stream_reasoning) >= _MAX_PROVIDER_MATERIALIZATION_WIDTH
        ):
            retained = False
            return
        if key is not None:
            trace.stream_reasoning_keys[key] = len(trace.stream_reasoning)
        trace.stream_reasoning.append({"type": kind, "text": text})
        trace.stream_reasoning_bytes += byte_count
        trace.stream_reasoning_nodes += 1

    def observe_tool(
        key: str,
        *,
        call_id: Any = None,
        name: Any = None,
        input_value: Any = None,
        fragment: bool = False,
    ) -> None:
        nonlocal retained
        tool = trace.stream_tools.get(key)
        if tool is None:
            if (
                trace.stream_tool_nodes >= _MAX_PROVIDER_STREAM_SEMANTIC_NODES
                or len(trace.stream_tools) >= _MAX_PROVIDER_MATERIALIZATION_WIDTH
            ):
                retained = False
                return
            tool = {"fragments": []}
            trace.stream_tools[key] = tool
            trace.stream_tool_nodes += 1

        def retain_tool_text(field: str, text: str) -> None:
            nonlocal retained
            previous = tool.get(field)
            previous_bytes = len(previous.encode("utf-8")) if isinstance(previous, str) else 0
            byte_count = len(text.encode("utf-8"))
            added = max(0, byte_count - previous_bytes)
            added_nodes = 0 if isinstance(previous, str) else 1
            if (
                added > _MAX_PROVIDER_STREAM_SEMANTIC_BYTES - trace.stream_tool_bytes
                or added_nodes
                > _MAX_PROVIDER_STREAM_SEMANTIC_NODES - trace.stream_tool_nodes
            ):
                retained = False
                return
            tool[field] = text
            trace.stream_tool_bytes += byte_count - previous_bytes
            trace.stream_tool_nodes += added_nodes

        exact_id = _exact_string(call_id)
        exact_name = _exact_string(name)
        if exact_id:
            retain_tool_text("id", exact_id)
        if exact_name:
            retain_tool_text("name", exact_name)
        if fragment and isinstance(input_value, str):
            byte_count = len(input_value.encode("utf-8"))
            if (
                byte_count > _MAX_PROVIDER_STREAM_SEMANTIC_BYTES - trace.stream_tool_bytes
                or trace.stream_tool_nodes >= _MAX_PROVIDER_STREAM_SEMANTIC_NODES
                or len(tool["fragments"]) >= _MAX_PROVIDER_MATERIALIZATION_WIDTH
            ):
                retained = False
            else:
                tool["fragments"].append(input_value)
                trace.stream_tool_bytes += byte_count
                trace.stream_tool_nodes += 1
        elif input_value is not None:
            cost = _retained_value_cost(
                input_value,
                _MAX_PROVIDER_STREAM_SEMANTIC_BYTES - trace.stream_tool_bytes,
                _MAX_PROVIDER_STREAM_SEMANTIC_NODES - trace.stream_tool_nodes,
            )
            if cost is None:
                retained = False
            else:
                tool["input"] = input_value
                trace.stream_tool_bytes += cost[0]
                trace.stream_tool_nodes += cost[1]

    event_type = _field(value, "type")
    terminal_response = (
        _field(value, "response")
        if event_type in {"response.completed", "response.failed", "response.incomplete"}
        else None
    )
    if terminal_response is not None:
        if (
            _retained_value_cost(
                terminal_response,
                _MAX_PROVIDER_STREAM_SEMANTIC_BYTES,
                _MAX_PROVIDER_STREAM_SEMANTIC_NODES,
                _MAX_PROVIDER_MATERIALIZATION_WIDTH,
            )
            is None
        ):
            return False
        if trace.last_part is value:
            return retained
        trace.stream_model = (
            _exact_string(
                _field(terminal_response, "model", "model_version", "modelVersion")
            )
            or trace.stream_model
        )
        trace.stream_usage.update(
            _normalized_usage(
                _field(terminal_response, "usage", "usage_metadata", "usageMetadata")
            )
        )
        content = _response_content(provider, terminal_response)
        if content is None:
            trace.stream_text = []
            trace.stream_text_bytes = 0
            trace.stream_text_nodes = 0
        elif isinstance(content, str):
            content_bytes = len(content.encode("utf-8"))
            if (
                content_bytes <= _MAX_PROVIDER_STREAM_SEMANTIC_BYTES
                and _MAX_PROVIDER_STREAM_SEMANTIC_NODES >= 1
            ):
                trace.stream_text = [content]
                trace.stream_text_bytes = content_bytes
                trace.stream_text_nodes = 1
            else:
                retained = False
        reasoning = _response_reasoning(provider, terminal_response)
        reasoning_bytes = sum(len(block["text"].encode("utf-8")) for block in reasoning)
        if (
            reasoning_bytes <= _MAX_PROVIDER_STREAM_SEMANTIC_BYTES
            and len(reasoning) <= _MAX_PROVIDER_STREAM_SEMANTIC_NODES
            and len(reasoning) <= _MAX_PROVIDER_MATERIALIZATION_WIDTH
        ):
            trace.stream_reasoning = [dict(block) for block in reasoning]
            trace.stream_reasoning_keys.clear()
            trace.stream_reasoning_bytes = reasoning_bytes
            trace.stream_reasoning_nodes = len(reasoning)
        else:
            retained = False
        finish = _finish_reason(terminal_response)
        if finish is not None:
            trace.stream_finish_reason = finish
        terminal_tools: dict[str, dict[str, Any]] = {}
        terminal_tool_bytes = 0
        terminal_tool_nodes = 0
        terminal_tools_retained = True
        for tool_position, tool_call in enumerate(_tool_calls(terminal_response)):
            proposal = _tool_proposal_fields(provider, tool_call)
            if proposal is None:
                continue
            call_id = _exact_string(proposal.get("call_id"))
            name = proposal["name"]
            input_value = proposal["input"]
            input_cost = _retained_value_cost(
                input_value,
                _MAX_PROVIDER_STREAM_SEMANTIC_BYTES - terminal_tool_bytes,
                _MAX_PROVIDER_STREAM_SEMANTIC_NODES - terminal_tool_nodes - 3,
            )
            string_bytes = len(name.encode("utf-8")) + (
                len(call_id.encode("utf-8")) if call_id is not None else 0
            )
            string_nodes = 1 + (1 if call_id is not None else 0)
            if (
                input_cost is None
                or string_bytes
                > _MAX_PROVIDER_STREAM_SEMANTIC_BYTES
                - terminal_tool_bytes
                - input_cost[0]
                or terminal_tool_nodes + input_cost[1] + string_nodes + 1
                > _MAX_PROVIDER_STREAM_SEMANTIC_NODES
            ):
                terminal_tools_retained = False
                break
            terminal_tools[f"terminal:tool:{tool_position}"] = {
                "fragments": [],
                **({"id": call_id} if call_id is not None else {}),
                "name": name,
                "input": input_value,
            }
            terminal_tool_bytes += string_bytes + input_cost[0]
            terminal_tool_nodes += input_cost[1] + string_nodes + 1
        if terminal_tools_retained:
            trace.stream_tools = terminal_tools
            trace.stream_tool_bytes = terminal_tool_bytes
            trace.stream_tool_nodes = terminal_tool_nodes
        else:
            retained = False
        return retained

    trace.stream_model = (
        _exact_string(_field(value, "model", "model_version", "modelVersion")) or trace.stream_model
    )
    trace.stream_usage.update(
        _normalized_usage(_field(value, "usage", "usage_metadata", "usageMetadata"))
    )

    if provider in {"openai", "openrouter"}:
        delta_value = _field(value, "delta")
        if isinstance(delta_value, str) and event_type == "response.output_text.delta":
            append_text(trace.stream_text, "stream_text_bytes", delta_value)
            return retained
        if isinstance(delta_value, str) and isinstance(event_type, str):
            opaque_event = any(
                marker in event_type for marker in ("encrypted", "signature", "redacted")
            )
            item_key = _exact_string(_field(value, "item_id")) or str(_field(value, "output_index"))
            if not opaque_event and "reasoning_summary_text" in event_type:
                append_reasoning(
                    "summary",
                    delta_value,
                    f"responses:{item_key}:summary:{_field(value, 'summary_index')}",
                )
                return retained
            if not opaque_event and "reasoning" in event_type:
                append_reasoning(
                    "text",
                    delta_value,
                    f"responses:{item_key}:text:{_field(value, 'content_index')}",
                )
                return retained
        if isinstance(event_type, str) and any(
            marker in event_type for marker in ("encrypted", "signature", "redacted")
        ):
            return retained
        choices = _field(value, "choices")
        if not isinstance(choices, list):
            return retained
        for choice_position, choice in enumerate(choices):
            if choice_position >= _MAX_PROVIDER_MATERIALIZATION_WIDTH:
                retained = False
                break
            finish = _field(choice, "finish_reason")
            if finish is not None:
                trace.stream_finish_reason = _enum_string(finish)
            delta = _field(choice, "delta")
            content = _field(delta, "content")
            if isinstance(content, str):
                append_text(trace.stream_text, "stream_text_bytes", content)
            reasoning_details = _field(delta, "reasoning_details", "reasoningDetails")
            if isinstance(reasoning_details, list) and reasoning_details:
                for detail_position, detail in enumerate(reasoning_details):
                    detail_kind, detail_text = _reasoning_detail(detail)
                    detail_index = _field(detail, "index")
                    stable_detail_index = (
                        f"native:{detail_index}"
                        if isinstance(detail_index, int)
                        else f"event:{event_index}:{detail_position}"
                    )
                    append_reasoning(
                        detail_kind,
                        detail_text,
                        f"chat:{choice_position}:detail:{stable_detail_index}",
                    )
            else:
                exposed_reasoning = _field(delta, "reasoning_content", "reasoning")
                if isinstance(exposed_reasoning, str):
                    append_reasoning(
                        "text",
                        exposed_reasoning,
                        f"chat:{choice_position}:reasoning",
                    )
            tool_calls = _field(delta, "tool_calls", "toolCalls")
            if not isinstance(tool_calls, list):
                continue
            for tool_position, tool_call in enumerate(tool_calls):
                if tool_position >= _MAX_PROVIDER_MATERIALIZATION_WIDTH:
                    retained = False
                    break
                index = _field(tool_call, "index")
                function = _field(tool_call, "function")
                tool_index = index if isinstance(index, int) else tool_position
                observe_tool(
                    f"choice:{choice_position}:tool:{tool_index}",
                    call_id=_field(tool_call, "id"),
                    name=_field(function, "name"),
                    input_value=_field(function, "arguments"),
                    fragment=True,
                )
        return retained

    if provider == "anthropic":
        if event_type == "message_start":
            message = _field(value, "message")
            trace.stream_model = _exact_string(_field(message, "model")) or trace.stream_model
            trace.stream_usage.update(_normalized_usage(_field(message, "usage")))
        if event_type == "content_block_start":
            block = _field(value, "content_block")
            if _field(block, "type") == "text":
                block_text = _field(block, "text")
                if isinstance(block_text, str):
                    append_text(trace.stream_text, "stream_text_bytes", block_text)
            elif _field(block, "type") == "thinking":
                thinking = _field(block, "thinking")
                append_reasoning("summary", thinking, f"anthropic:{_field(value, 'index')}")
            elif _field(block, "type") in {"summary", "thinking_summary"}:
                append_reasoning(
                    "summary",
                    _field(block, "text", "summary"),
                    f"anthropic:{_field(value, 'index')}",
                )
            elif _field(block, "type") == "tool_use":
                observe_tool(
                    f"content:{_field(value, 'index')}",
                    call_id=_field(block, "id"),
                    name=_field(block, "name"),
                    input_value=_field(block, "input"),
                )
        if event_type == "content_block_delta":
            delta = _field(value, "delta")
            delta_type = _field(delta, "type")
            delta_text = _field(delta, "text")
            if isinstance(delta_text, str) and delta_type not in {
                "summary_delta",
                "thinking_summary_delta",
            }:
                append_text(trace.stream_text, "stream_text_bytes", delta_text)
            thinking = _field(delta, "thinking")
            append_reasoning("summary", thinking, f"anthropic:{_field(value, 'index')}")
            if delta_type in {"summary_delta", "thinking_summary_delta"}:
                append_reasoning(
                    "summary",
                    _field(delta, "text", "summary"),
                    f"anthropic:{_field(value, 'index')}",
                )
            partial_json = _field(delta, "partial_json")
            if isinstance(partial_json, str):
                observe_tool(
                    f"content:{_field(value, 'index')}",
                    input_value=partial_json,
                    fragment=True,
                )
        if event_type == "message_delta":
            delta = _field(value, "delta")
            finish = _field(delta, "stop_reason")
            if finish is not None:
                trace.stream_finish_reason = _enum_string(finish)
        return retained

    candidates = _field(value, "candidates")
    if not isinstance(candidates, list):
        return retained
    for candidate_index, candidate in enumerate(candidates):
        if candidate_index >= _MAX_PROVIDER_MATERIALIZATION_WIDTH:
            retained = False
            break
        finish = _field(candidate, "finish_reason", "finishReason")
        if finish is not None:
            trace.stream_finish_reason = _enum_string(finish)
        parts = _field(_field(candidate, "content"), "parts")
        if not isinstance(parts, list):
            continue
        for part_index, part in enumerate(parts):
            if part_index >= _MAX_PROVIDER_MATERIALIZATION_WIDTH:
                retained = False
                break
            part_text = _field(part, "text")
            if isinstance(part_text, str):
                if _field(part, "thought") is True:
                    append_reasoning(
                        "summary",
                        part_text,
                        f"gemini:{candidate_index}:part:{part_index}",
                    )
                else:
                    append_text(trace.stream_text, "stream_text_bytes", part_text)
            function_call = _field(part, "function_call", "functionCall")
            if function_call is not None:
                observe_tool(
                    f"candidate:{candidate_index}:part:{part_index}",
                    call_id=_field(function_call, "id", "call_id", "callId"),
                    name=_field(function_call, "name"),
                    input_value=_field(function_call, "args"),
                )
    return retained


def _has_stream_response_evidence(trace: _Trace) -> bool:
    return bool(
        trace.stream_text
        or trace.stream_reasoning
        or trace.stream_model
        or trace.stream_finish_reason
        or trace.stream_usage
        or trace.stream_tools
    )


def _stream_terminal(trace: _Trace, provider: str) -> Any:
    if (
        _field(trace.last_part, "type")
        in {"response.completed", "response.failed", "response.incomplete"}
        and _field(trace.last_part, "response") is not None
    ):
        return trace.last_part
    tools: list[dict[str, Any]] = []
    for tool in trace.stream_tools.values():
        name = _exact_string(tool.get("name"))
        if not name:
            continue
        fragments = tool.get("fragments")
        input_value: Any
        if isinstance(fragments, list) and fragments:
            input_value = "".join(fragment for fragment in fragments if isinstance(fragment, str))
        else:
            input_value = tool.get("input")
        if input_value is None:
            continue
        tools.append(
            {
                **({"id": tool["id"]} if isinstance(tool.get("id"), str) else {}),
                "name": name,
                "arguments": input_value,
            }
        )
    if provider == "anthropic":
        return {
            **({"model": trace.stream_model} if trace.stream_model else {}),
            **({"reasoning": _stream_reasoning(trace)} if _stream_reasoning(trace) else {}),
            "content": [
                *(
                    [{"type": "text", "text": "".join(trace.stream_text)}]
                    if trace.stream_text
                    else []
                ),
                *[
                    {
                        "type": "tool_use",
                        **({"id": tool["id"]} if "id" in tool else {}),
                        "name": tool["name"],
                        "input": _parse_exact_json(tool["arguments"]),
                    }
                    for tool in tools
                ],
            ],
            **({"stop_reason": trace.stream_finish_reason} if trace.stream_finish_reason else {}),
            **({"usage": trace.stream_usage} if trace.stream_usage else {}),
        }
    if provider == "gemini":
        return {
            **({"modelVersion": trace.stream_model} if trace.stream_model else {}),
            **({"reasoning": _stream_reasoning(trace)} if _stream_reasoning(trace) else {}),
            "candidates": [
                {
                    "content": {
                        "role": "model",
                        "parts": [
                            *([{"text": "".join(trace.stream_text)}] if trace.stream_text else []),
                            *[
                                {
                                    "functionCall": {
                                        **({"id": tool["id"]} if "id" in tool else {}),
                                        "name": tool["name"],
                                        "args": _parse_exact_json(tool["arguments"]),
                                    }
                                }
                                for tool in tools
                            ],
                        ],
                    },
                    **(
                        {"finishReason": trace.stream_finish_reason}
                        if trace.stream_finish_reason
                        else {}
                    ),
                }
            ],
            **({"usageMetadata": trace.stream_usage} if trace.stream_usage else {}),
        }
    return {
        **({"model": trace.stream_model} if trace.stream_model else {}),
        **({"reasoning": _stream_reasoning(trace)} if _stream_reasoning(trace) else {}),
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    **({"content": "".join(trace.stream_text)} if trace.stream_text else {}),
                    **(
                        {
                            "tool_calls": [
                                {
                                    **({"id": tool["id"]} if "id" in tool else {}),
                                    "type": "function",
                                    "function": {
                                        "name": tool["name"],
                                        "arguments": tool["arguments"],
                                    },
                                }
                                for tool in tools
                            ]
                        }
                        if tools
                        else {}
                    ),
                },
                **(
                    {"finish_reason": trace.stream_finish_reason}
                    if trace.stream_finish_reason
                    else {}
                ),
            }
        ],
        **({"usage": trace.stream_usage} if trace.stream_usage else {}),
    }


def _response_content(provider: str, value: Any) -> Any:
    output_text = _field(value, "output_text")
    if isinstance(output_text, str):
        return output_text
    if provider == "anthropic":
        content = _field(value, "content")
        if isinstance(content, list):
            text = "".join(
                part_text
                for part in content
                if _field(part, "type") == "text"
                and isinstance((part_text := _field(part, "text")), str)
            )
            return text or None
    if provider == "gemini":
        text_parts: list[str] = []
        candidates = _field(value, "candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                parts = _field(_field(candidate, "content"), "parts")
                if not isinstance(parts, list):
                    continue
                text_parts.extend(
                    text
                    for part in parts
                    if _field(part, "thought") is not True
                    and isinstance((text := _field(part, "text")), str)
                )
        return "".join(text_parts) or None
    choices = _field(value, "choices")
    if isinstance(choices, list) and choices:
        message = _field(choices[0], "message") or _field(choices[0], "delta")
        content = _field(message, "content")
        if content is not None:
            return content
    output = _field(value, "output")
    if isinstance(output, list):
        text_parts = []
        for item in output:
            if _field(item, "type") != "message":
                continue
            content = _field(item, "content")
            if not isinstance(content, list):
                continue
            text_parts.extend(
                text for part in content if isinstance((text := _field(part, "text")), str)
            )
        return "".join(text_parts) or None
    return None


def _stream_reasoning(trace: _Trace) -> list[dict[str, str]]:
    return [dict(block) for block in trace.stream_reasoning]


def _response_reasoning(provider: str, value: Any) -> list[dict[str, str]]:
    blocks: list[dict[str, str]] = []

    def add(kind: str, text: Any) -> None:
        if kind not in {"text", "summary"} or not isinstance(text, str) or not text:
            return
        blocks.append({"type": kind, "text": text})

    canonical = _field(value, "reasoning")
    if isinstance(canonical, list):
        for block in canonical:
            add(_field(block, "type"), _field(block, "text"))
        return blocks
    if provider == "anthropic":
        content = _field(value, "content")
        if isinstance(content, list):
            for part in content:
                part_type = _field(part, "type")
                if part_type == "thinking":
                    add("summary", _field(part, "thinking"))
                elif part_type in {"summary", "thinking_summary"}:
                    add("summary", _field(part, "text", "summary"))
    if provider == "gemini":
        candidates = _field(value, "candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                parts = _field(_field(candidate, "content"), "parts")
                if not isinstance(parts, list):
                    continue
                for part in parts:
                    if _field(part, "thought") is True:
                        add("summary", _field(part, "text"))
    choices = _field(value, "choices")
    if isinstance(choices, list) and choices:
        message = _field(choices[0], "message") or _field(choices[0], "delta")
        reasoning_details = _field(message, "reasoning_details", "reasoningDetails")
        if isinstance(reasoning_details, list) and reasoning_details:
            for detail in reasoning_details:
                kind, text = _reasoning_detail(detail)
                add(kind, text)
        else:
            add("text", _field(message, "reasoning_content", "reasoning"))
    output = _field(value, "output")
    if isinstance(output, list):
        for item in output:
            if _field(item, "type") != "reasoning":
                continue
            summary = _field(item, "summary")
            if isinstance(summary, list):
                for part in summary:
                    add("summary", _field(part, "text"))
            content = _field(item, "content")
            if isinstance(content, list):
                for part in content:
                    if _field(part, "type") in {"reasoning_text", "text"}:
                        add("text", _field(part, "text"))
            else:
                add("text", _field(item, "text"))
    return blocks


def _reasoning_detail(value: Any) -> tuple[str, Any]:
    detail_type = _field(value, "type")
    if not isinstance(detail_type, str) or any(
        marker in detail_type.lower() for marker in ("encrypted", "signature", "redacted")
    ):
        return "", None
    normalized = detail_type.lower().replace("-", "_")
    if "summary" in normalized:
        return "summary", _field(value, "summary", "text")
    if normalized in {"reasoning", "reasoning.text", "reasoning_text", "text"}:
        return "text", _field(value, "text", "reasoning")
    return "", None


def _normalized_usage(value: Any) -> dict[str, int | float]:
    input_tokens = _nonnegative_number(
        _field(
            value,
            "input_tokens",
            "prompt_tokens",
            "prompt_token_count",
            "promptTokenCount",
        )
    )
    output_tokens = _nonnegative_number(
        _field(
            value,
            "output_tokens",
            "completion_tokens",
            "candidates_token_count",
            "candidatesTokenCount",
        )
    )
    return {
        **({"input_tokens": input_tokens} if input_tokens is not None else {}),
        **({"output_tokens": output_tokens} if output_tokens is not None else {}),
    }


def _nonnegative_number(value: Any) -> int | float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        return None
    return value


def _retained_value_cost(
    value: Any,
    byte_limit: int,
    node_limit: int,
    width_limit: int = _MAX_PROVIDER_MATERIALIZATION_WIDTH,
) -> tuple[int, int] | None:
    if byte_limit < 0 or node_limit <= 0:
        return None
    iterators: list[Iterator[Any]] = [iter((value,))]
    seen: set[int] = set()
    byte_count = 0
    node_count = 0
    while iterators:
        try:
            child = next(iterators[-1])
        except StopIteration:
            iterators.pop()
            continue
        node_count += 1
        if node_count > node_limit:
            return None
        child_type = type(child)
        if child is None or child_type in {bool, int, float}:
            byte_count += 16
        elif child_type is str:
            byte_count += len(child) * 4
        elif child_type in {bytes, bytearray, memoryview}:
            byte_count += len(child)
        else:
            identity = id(child)
            if identity in seen:
                byte_count += 8
                continue
            seen.add(identity)
            if child_type is dict:
                if len(child) > width_limit:
                    return None
                iterators.append(
                    chain.from_iterable((key, item) for key, item in dict.items(child))
                )
            elif child_type in {list, tuple, set, frozenset}:
                if len(child) > width_limit:
                    return None
                iterators.append(iter(child))
            elif isinstance(child, (dict, list, tuple, set, frozenset)):
                return None
            else:
                owned = native_own_data(child)
                extras = _pydantic_extra_data(child)
                if owned or extras:
                    if len(owned) + len(extras) > width_limit:
                        return None
                    iterators.append(
                        chain(
                            owned.keys(),
                            owned.values(),
                            extras.keys(),
                            extras.values(),
                        )
                    )
                else:
                    byte_count += 32
        if byte_count > byte_limit:
            return None
    return byte_count, node_count


def _tool_proposal_fields(
    provider: str, tool: Any, fallback_call_id: str | None = None
) -> dict[str, Any] | None:
    function = _field(tool, "function") or tool
    name = _exact_string(_field(function, "name"))
    if not name:
        return None
    input_value = _field(function, "arguments", "input", "args")
    if input_value is None:
        return None
    call_id = _exact_string(_field(tool, "id", "call_id", "callId")) or fallback_call_id
    return {
        "name": name,
        "input": _parse_exact_json(input_value),
        **({"call_id": call_id, "native_call_id": call_id} if call_id is not None else {}),
    }


def _parse_exact_json(value: Any) -> Any:
    if type(value) is not str or len(value) * 4 > _MAX_PROVIDER_EXCEPTION_SCAN_BYTES:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _normalized_error(error: BaseException) -> dict[str, Any]:
    native_type = type(error).__name__
    normalized_type = "".join(
        f"_{character.lower()}" if character.isupper() else character for character in native_type
    ).lstrip("_")
    result = {
        "type": normalized_type if len(normalized_type) >= 3 else "provider_error",
        "message": _exception_message(error),
        "recoverable": False,
    }
    code = _exact_string(_field(error, "code"))
    status = _field(error, "status_code", "status")
    if code:
        result["code"] = code
    elif isinstance(status, int):
        result["code"] = str(status)
    return result


def _provider_error_evidence(error: BaseException) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "type": type(error).__name__,
        "message": _exception_message(error),
    }
    for target, names in {
        "code": ("code",),
        "status": ("status_code", "status"),
        "request_id": ("request_id",),
        "body": ("body",),
    }.items():
        value = _field(error, *names)
        if value is not None and isinstance(value, (str, int, float, bool, dict, list)):
            evidence[target] = value
    return evidence


def _exact_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _safe_text(value: Any, fallback: str) -> str:
    if type(value) is str:
        return value or fallback
    if type(value) in {bool, int, float}:
        return str(value)
    return fallback


def _exception_message(error: BaseException) -> str:
    try:
        descriptor = inspect.getattr_static(BaseException, "args")
        arguments = descriptor.__get__(error, type(error))
    except BaseException:
        return type(error).__name__
    if type(arguments) is tuple:
        parts: list[str] = []
        retained_bytes = 0
        count = min(
            tuple.__len__(arguments),
            _MAX_PROVIDER_EXCEPTION_SCAN_NODES,
        )
        for index in range(count):
            separator_bytes = 2 if parts else 0
            remaining = _MAX_PROVIDER_EXCEPTION_SCAN_BYTES - retained_bytes - separator_bytes
            if remaining <= 0:
                break
            argument = tuple.__getitem__(arguments, index)
            text = _bounded_exception_argument(argument, remaining)
            if text is None:
                break
            if not text:
                continue
            parts.append(text)
            retained_bytes += separator_bytes + len(text) * 4
        message = "; ".join(parts)
        if message:
            return message
    return type(error).__name__


def _bounded_exception_argument(value: Any, remaining_bytes: int) -> str | None:
    if remaining_bytes <= 0:
        return None
    if type(value) is str:
        character_limit = remaining_bytes // 4
        if character_limit <= 0:
            return None
        return str.__getitem__(value, slice(0, character_limit))
    if type(value) is bool:
        text = "True" if value else "False"
        return text if len(text) * 4 <= remaining_bytes else None
    if type(value) is int:
        # log10(2) < 1, so bit length is a cheap conservative digit bound.
        if int.bit_length(value) + 1 > remaining_bytes // 4:
            return None
        text = str(value)
        return text if len(text) * 4 <= remaining_bytes else None
    if type(value) is float:
        text = str(value)
        return text if len(text) * 4 <= remaining_bytes else None
    return None


def _structured_error_evidence(error: BaseException) -> Any:
    return _find_structured_error(error, set(), 0, _ExceptionScanBudget())


@dataclass
class _ExceptionScanBudget:
    nodes: int = 0
    bytes: int = 0


def _find_structured_error(
    value: Any,
    seen: set[int],
    depth: int,
    budget: _ExceptionScanBudget,
) -> Any:
    budget.nodes += 1
    if depth > 16 or budget.nodes > _MAX_PROVIDER_EXCEPTION_SCAN_NODES:
        return None
    if type(value) is str:
        budget.bytes += len(value) * 4
        if budget.bytes > _MAX_PROVIDER_EXCEPTION_SCAN_BYTES:
            return None
        start = value.find("{")
        if start >= 0:
            try:
                return _find_structured_error(json.loads(value[start:]), seen, depth + 1, budget)
            except (json.JSONDecodeError, TypeError):
                return None
        return None
    if value is None or type(value) in {int, float, bool}:
        return None
    identity = id(value)
    if identity in seen:
        return None
    seen.add(identity)
    children: Iterable[Any]
    if type(value) is dict:
        custom = dict.get(value, "custom")
        if type(custom) is dict and dict.get(custom, "code") == "SEMANTIC_LAYER_CAPTURE_FAILURE_V1":
            remaining_bytes = _MAX_PROVIDER_EXCEPTION_SCAN_BYTES - budget.bytes
            remaining_nodes = _MAX_PROVIDER_EXCEPTION_SCAN_NODES - budget.nodes
            if (
                len(value) > _MAX_PROVIDER_EXCEPTION_SCAN_WIDTH
                or _retained_value_cost(value, remaining_bytes, remaining_nodes) is None
            ):
                return None
            return value
        children = dict.values(value)
    elif type(value) in {list, tuple}:
        children = value
    else:
        owned = native_own_data(value)
        if not owned:
            return None
        children = owned.values()
    for child in islice(children, _MAX_PROVIDER_EXCEPTION_SCAN_WIDTH):
        found = _find_structured_error(child, seen, depth + 1, budget)
        if found is not None:
            return found
    return None


def _tool_calls(result: Any) -> list[Any]:
    content_block = _field(result, "content_block")
    content_block_type = _field(content_block, "type")
    if (
        content_block is not None
        and isinstance(content_block_type, str)
        and "tool" in content_block_type
    ):
        return [content_block]
    content = _field(result, "content")
    if isinstance(content, list):
        return [
            item
            for item in content
            if isinstance((item_type := _field(item, "type")), str) and "tool" in item_type
        ]
    calls: list[Any] = []
    candidates = _field(result, "candidates")
    if isinstance(candidates, list):
        for candidate in candidates:
            parts = _field(_field(candidate, "content"), "parts")
            if not isinstance(parts, list):
                continue
            calls.extend(
                call
                for part in parts
                if (call := _field(part, "function_call", "functionCall")) is not None
            )
    choices = _field(result, "choices")
    if isinstance(choices, list):
        for choice in choices:
            message = _field(choice, "message") or _field(choice, "delta")
            tool_calls = _field(message, "tool_calls", "toolCalls")
            if isinstance(tool_calls, list):
                calls.extend(tool_calls)
    return calls


def _gemini_tool_candidate_indexes(result: Any) -> set[int]:
    indexes: set[int] = set()
    candidates = _field(result, "candidates")
    if not isinstance(candidates, list):
        return indexes
    for candidate_index, candidate in enumerate(candidates):
        parts = _field(_field(candidate, "content"), "parts")
        if not isinstance(parts, list):
            continue
        if any(_field(part, "function_call", "functionCall") is not None for part in parts):
            indexes.add(candidate_index)
    return indexes


def _gemini_terminal_function_responses(request: Any) -> list[Any]:
    contents = _field(request, "contents")
    if not isinstance(contents, list) or not contents:
        return []
    parts = _field(contents[-1], "parts")
    if not isinstance(parts, list) or not parts:
        return []
    responses: list[Any] = []
    reached_user_text = False
    for part in parts:
        response = _field(part, "function_response", "functionResponse")
        if response is not None:
            if reached_user_text:
                return []
            responses.append(response)
            continue
        text = _field(part, "text")
        if isinstance(text, str) and text and responses:
            reached_user_text = True
            continue
        return []
    return responses


def _gemini_request_contents(request: Any) -> Any:
    contents = _field(request, "contents")
    return tuple(contents) if isinstance(contents, list) else contents


def _gemini_afc_history_pairs(
    result: Any,
    request_contents: Any,
) -> tuple[Any, list[tuple[int, int, Any, Any]] | None]:
    history = _field(
        result,
        "automatic_function_calling_history",
        "automaticFunctionCallingHistory",
    )
    if history is None or (isinstance(history, list) and not history):
        return history, []
    if not isinstance(history, list):
        return history, None
    boundary = _gemini_afc_history_boundary(history, request_contents)
    if boundary is None:
        return history, None
    pairs: list[tuple[int, int, Any, Any]] = []
    index = boundary
    while index < len(history):
        content = history[index]
        parts = _field(content, "parts")
        if not isinstance(parts, list):
            return history, None
        calls = [
            (part_index, function_call)
            for part_index, part in enumerate(parts)
            if (function_call := _field(part, "function_call", "functionCall")) is not None
        ]
        unpaired_responses = [
            part
            for part in parts
            if _field(part, "function_response", "functionResponse") is not None
        ]
        if unpaired_responses:
            return history, None
        if not calls:
            return history, None
        if _field(content, "role") != "model" or index + 1 >= len(history):
            return history, None
        response_content = history[index + 1]
        response_parts = _field(response_content, "parts")
        if not isinstance(response_parts, list):
            return history, None
        if _field(response_content, "role") != "user":
            return history, None
        responses = [
            _field(part, "function_response", "functionResponse") for part in response_parts
        ]
        if len(responses) != len(calls) or any(response is None for response in responses):
            return history, None
        call_ids = [
            _exact_string(_field(function_call, "id", "call_id", "callId"))
            for _, function_call in calls
        ]
        response_ids = [
            _exact_string(_field(response, "id", "call_id", "callId")) for response in responses
        ]
        if (
            any(call_id is None for call_id in call_ids)
            or any(response_id is None for response_id in response_ids)
            or len(set(call_ids)) != len(call_ids)
            or set(call_ids) != set(response_ids)
        ):
            return history, None
        responses_by_id = dict(zip(response_ids, responses, strict=True))
        paired_responses = [responses_by_id[call_id] for call_id in call_ids]
        for (part_index, function_call), function_response in zip(
            calls, paired_responses, strict=True
        ):
            call_name = _exact_string(_field(function_call, "name"))
            response_name = _exact_string(_field(function_response, "name"))
            arguments = _field(function_call, "args")
            payload = _field(function_response, "response")
            if (
                call_name is None
                or response_name != call_name
                or arguments is None
                or type(payload) is not dict
                or (("result" in payload) == ("error" in payload))
            ):
                return history, None
            pairs.append((index, part_index, function_call, function_response))
        index += 2
    return (history, pairs) if pairs else (history, None)


def _gemini_afc_history_boundary(history: list[Any], request_contents: Any) -> int | None:
    expected: tuple[Any, ...]
    if isinstance(request_contents, tuple):
        if not all(_gemini_content(value) for value in request_contents):
            return None
        expected = request_contents
    elif _gemini_content(request_contents):
        expected = (request_contents,)
    elif isinstance(request_contents, str):
        if not history or not _gemini_exact_text_content(history[0], request_contents):
            return None
        return 1
    elif _gemini_part(request_contents):
        if not history or not _gemini_exact_part_content(history[0], request_contents):
            return None
        return 1
    else:
        return None
    if len(history) < len(expected):
        return None
    try:
        matches = all(history[index] == value for index, value in enumerate(expected))
    except BaseException:
        return None
    return len(expected) if matches else None


def _gemini_content(value: Any) -> bool:
    return _field(value, "role") in {"user", "model"} and isinstance(_field(value, "parts"), list)


def _gemini_part(value: Any) -> bool:
    return (
        value is not None
        and not _gemini_content(value)
        and any(
            _field(value, name) is not None
            for name in ("text", "function_call", "function_response")
        )
    )


def _gemini_exact_text_content(content: Any, text: str) -> bool:
    parts = _field(content, "parts")
    return (
        _field(content, "role") == "user"
        and isinstance(parts, list)
        and len(parts) == 1
        and _field(parts[0], "text") == text
        and _field(parts[0], "function_call", "functionCall") is None
        and _field(parts[0], "function_response", "functionResponse") is None
    )


def _gemini_exact_part_content(content: Any, expected: Any) -> bool:
    parts = _field(content, "parts")
    if _field(content, "role") != "user" or not isinstance(parts, list) or len(parts) != 1:
        return False
    try:
        return bool(parts[0] == expected)
    except BaseException:
        return False


def _finish_reason(value: Any) -> Any:
    direct = _field(value, "finish_reason", "finishReason", "stop_reason")
    if direct is not None:
        return _enum_string(direct)
    choices = _field(value, "choices", "candidates")
    if isinstance(choices, list) and choices:
        candidate = _field(choices[0], "finish_reason", "finishReason", "stop_reason")
        return _enum_string(candidate) if candidate is not None else None
    return None


def _enum_string(value: Any) -> str:
    if isinstance(value, Enum):
        native = inspect.getattr_static(value, "_value_", None)
        if isinstance(native, str):
            return native
    native = _field(value, "value")
    return native if isinstance(native, str) else _safe_text(value, type(value).__name__)


def _field(value: Any, *names: str) -> Any:
    if type(value) is dict:
        for name in names:
            if name in value:
                return dict.__getitem__(value, name)
        return None
    missing = object()
    for name in names:
        result = native_field(value, name, missing)
        if result is not missing and not is_unsafe_accessor_omission(result):
            return result
        extra = _pydantic_extra_field(value, name, missing)
        if extra is not missing:
            return extra
    return None


def _pydantic_extra_field(value: Any, name: str, default: Any) -> Any:
    extras = _pydantic_extra_data(value)
    return dict.get(extras, name, default)


def _pydantic_extra_data(value: Any) -> dict[str, Any]:
    try:
        bases = type.__getattribute__(type(value), "__mro__")
        is_pydantic = any(
            type.__getattribute__(base, "__module__") == "pydantic.main"
            and type.__getattribute__(base, "__name__") == "BaseModel"
            for base in bases
        )
        if not is_pydantic:
            return {}
        extras = object.__getattribute__(value, "__pydantic_extra__")
    except BaseException:
        return {}
    return extras if type(extras) is dict else {}


def _exact_openai_response_id(provider: str, operation: str, value: Any) -> str | None:
    if provider not in {"openai", "openrouter"} or operation != "responses":
        return None
    if _field(value, "object") != "response":
        return None
    response_id = _field(value, "id")
    return response_id if isinstance(response_id, str) and response_id.strip() else None


def _exact_openai_stream_response_id(provider: str, operation: str, part: Any) -> str | None:
    if provider not in {"openai", "openrouter"} or operation != "responses":
        return None
    if _field(part, "type") not in {
        "response.completed",
        "response.failed",
        "response.incomplete",
    }:
        return None
    response = _field(part, "response")
    if _field(response, "object") != "response":
        return None
    response_id = _field(response, "id")
    return response_id if isinstance(response_id, str) and response_id.strip() else None


def _restore_attribute(target: Any, key: str, wrapper: Any, had_own: bool, own_value: Any) -> None:
    namespace = vars(target) if hasattr(target, "__dict__") else {}
    if namespace.get(key) is not wrapper:
        return
    if had_own:
        setattr(target, key, own_value)
    else:
        delattr(target, key)
