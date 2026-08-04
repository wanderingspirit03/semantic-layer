"""Additive in-process OpenTelemetry span and log processors."""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from ._adapter_native import native_snapshot
from .capture_v1 import AdmissionReceipt, CaptureSource, _trust_official_source

_OTEL_OBSERVATION_COVERAGE = {
    "operation": "otel-observation",
    "domain": "otel.trace-span",
}
_TOOL_EXECUTION_COVERAGE = {
    "operation": "tool-execution",
    "domain": "gen-ai.tool-call",
}
_MODEL_INPUT_ROLES = {"system", "developer", "user", "assistant", "tool"}
_GENAI_SCHEMA_URL = "https://opentelemetry.io/schemas/gen-ai/1.42.0"


@dataclass
class _Trace:
    identity: dict[str, str]
    start: AdmissionReceipt
    name: str
    open_spans: int = 0
    failed: bool = False
    error: Any = None
    model_input: list[tuple[str, AdmissionReceipt]] = field(default_factory=list)
    agent_root_span_id: str | None = None
    control_root_span_id: str | None = None
    agent_input_captured: bool = False
    terminal_agent_span: Any = None


@dataclass
class _Span:
    trace_id: str
    trace: dict[str, str]
    start: AdmissionReceipt
    kind: str
    claimed: bool
    semantic_parent: bool
    input_captured: bool = False


class _Lifecycle:
    def __init__(self, source: OpenTelemetrySource) -> None:
        self.source = source

    def deactivate(self) -> None:
        with self.source.lock:
            self.source.active = False

    def drain(self) -> None:
        with self.source.settled:
            while self.source.inflight_callbacks:
                self.source.settled.wait(timeout=0.05)


class _SpanProcessor:
    def __init__(self, source: OpenTelemetrySource) -> None:
        self.source = source

    def on_start(self, span: Any, parent_context: Any = None) -> None:
        del parent_context
        source = self.source
        if not source._begin_callback():
            return
        try:
            with source.lock:
                self._on_start(span)
        finally:
            source._end_callback()

    def _on_start(self, span: Any) -> None:
        source = self.source
        context = _read_span_context(span)
        identifiers = _context_ids(context)
        if identifiers is None:
            if _has_gen_ai_signal(getattr(span, "attributes", None)):
                source._record_context_gap(
                    "invalid_span_context", "span", _safe_span_snapshot(span)
                )
            return
        trace_id, span_id = identifiers
        attributes = getattr(span, "attributes", None)
        classified = _span_kind(attributes)
        root = source.traces.get(trace_id)
        if (
            root is None
            and classified == "unknown"
            and source._root_semantic(_agent_name(span)) is None
            and not _has_gen_ai_signal(attributes)
        ):
            source.ignored_spans.add(_span_key(trace_id, span_id))
            return
        root = root or source._open_trace(trace_id, span, context, classified)
        if root is None:
            return
        root.open_spans += 1
        if classified == "agent" and root.agent_root_span_id == span_id:
            source.spans[_span_key(trace_id, span_id)] = _Span(
                trace_id,
                root.identity,
                root.start,
                "agent-root",
                True,
                True,
                _attribute_present(attributes, "gen_ai.input.messages"),
            )
            return
        if classified == "agent" and root.control_root_span_id == span_id:
            source.spans[_span_key(trace_id, span_id)] = _Span(
                trace_id,
                root.identity,
                root.start,
                "control-root",
                True,
                True,
                _attribute_present(attributes, "gen_ai.input.messages"),
            )
            return
        parent = _context_ids(getattr(span, "parent", None))
        opened_parent = (
            source.spans.get(_span_key(parent[0], parent[1]))
            if parent is not None and parent[0] == trace_id
            else None
        )
        kind = "agent-scope" if classified == "agent" else classified
        claimed = _has_gen_ai_signal(attributes)
        tool_call_id = _exact_tool_call_id(attributes)
        tool_input_captured = (
            _json_attribute(attributes, "gen_ai.tool.call.arguments")[0] == "valid"
            if kind == "tool"
            else False
        )
        context_refs = (
            _record_model_input(source.sink, root, span, span_id) if kind == "model" else []
        )
        value: dict[str, Any] = {
            "kind": "lifecycle" if kind == "agent-scope" else kind,
            "phase": "start",
            "name": "otel.span",
            "trace": root.identity,
            "native_identity": tool_call_id or span_id,
            "coverage": (
                _TOOL_EXECUTION_COVERAGE if tool_call_id is not None else _OTEL_OBSERVATION_COVERAGE
            ),
            "native": _semantic_span_snapshot(span, kind, "start"),
            "semantic": (
                _agent_scope_semantic(span, "start")
                if kind == "agent-scope"
                else {"type": "capture.redundant", "route": "otel"}
                if kind == "tool" and not tool_input_captured
                else _span_semantic(
                    span,
                    kind,
                    "start",
                    claimed,
                    context_refs,
                    strict_model_messages=source._strict_model_messages,
                )
            ),
        }
        if (
            opened_parent is not None
            and (opened_parent.kind != "unknown" or source._preserve_unknown_span_parents)
            and opened_parent.start.accepted
            and opened_parent.start.record_id is not None
        ):
            value["parent_record_id"] = opened_parent.start.record_id
        start = source.sink.record(value)
        source.spans[_span_key(trace_id, span_id)] = _Span(
            trace_id,
            root.identity,
            start,
            kind,
            claimed,
            kind != "agent-scope"
            or (opened_parent is not None and opened_parent.kind != "unknown"),
            (
                _attribute_present(attributes, "gen_ai.input.messages")
                if kind == "model"
                else tool_input_captured
                if kind == "tool"
                else False
            ),
        )
        if kind == "tool" and _invalid_tool_call_id(attributes):
            _record_content_gap(
                source.sink,
                root.identity,
                start,
                "invalid_tool_call_id",
            )
        if kind == "agent-scope" and opened_parent is not None and opened_parent.kind != "unknown":
            if _has_content(attributes, "gen_ai.input.messages"):
                _record_agent_messages(
                    source.sink,
                    root,
                    span,
                    span_id,
                    "gen_ai.input.messages",
                    "context",
                    start,
                )
            else:
                _record_content_gap(
                    source.sink,
                    root.identity,
                    start,
                    "nested_agent_input_not_captured",
                )
        if (
            claimed
            and parent is not None
            and opened_parent is None
            and _span_key(parent[0], parent[1]) not in source.ignored_spans
        ):
            source.sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "otel.context.gap",
                    "trace": root.identity,
                    "native_identity": span_id,
                    "coverage": _OTEL_OBSERVATION_COVERAGE,
                    "native": {
                        "reason": "unobserved_parent_span",
                        "native_type": "span",
                        "parent_span_id": parent[1],
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "reason": "unobserved_parent_span",
                        "detail": (
                            "The OpenTelemetry parent span was not observed by this source."
                        ),
                    },
                }
            )

    def on_end(self, span: Any) -> None:
        source = self.source
        if not source._begin_callback():
            return
        try:
            with source.lock:
                self._on_end(span)
        finally:
            source._end_callback()

    def _on_end(self, span: Any) -> None:
        source = self.source
        context = _read_span_context(span)
        identifiers = _context_ids(context)
        if identifiers is None:
            if _has_gen_ai_signal(getattr(span, "attributes", None)):
                source._record_context_gap(
                    "invalid_span_context", "span", _safe_span_snapshot(span)
                )
            return
        trace_id, span_id = identifiers
        key = _span_key(trace_id, span_id)
        opened = source.spans.pop(key, None)
        if opened is None:
            if key in source.ignored_spans:
                source.ignored_spans.discard(key)
                return
            if _has_gen_ai_signal(getattr(span, "attributes", None)):
                source._record_context_gap(
                    "span_end_without_observed_start", "span", _span_snapshot(span)
                )
            return
        status_code = getattr(getattr(span, "status", None), "status_code", None)
        failed = status_code in {2, "ERROR"} or str(status_code).endswith("ERROR")
        attributes = getattr(span, "attributes", None)
        tool_call_id = _exact_tool_call_id(attributes)
        claimed = opened.claimed or _has_gen_ai_signal(attributes)
        coverage = (
            _TOOL_EXECUTION_COVERAGE if tool_call_id is not None else _OTEL_OBSERVATION_COVERAGE
        )
        root = source.traces.get(opened.trace_id)
        terminal = opened.start
        terminal_parent = opened.start
        tool_projectable = True
        if root is not None and opened.kind == "model" and not opened.input_captured:
            input_state, _input_value = _message_array_attribute(
                dict(attributes or {}),
                "gen_ai.input.messages",
            )
            if input_state == "valid":
                _record_model_input(source.sink, root, span, span_id)
                _record_content_gap(
                    source.sink,
                    root.identity,
                    opened.start,
                    "model_input_late_unlinked",
                )
            else:
                _record_content_gap(
                    source.sink,
                    root.identity,
                    opened.start,
                    (
                        "model_input_malformed"
                        if input_state == "malformed"
                        else "model_input_not_captured"
                    ),
                )
        if root is not None and opened.kind == "tool" and not opened.input_captured:
            tool_input_state, _tool_input = _json_attribute(
                attributes,
                "gen_ai.tool.call.arguments",
            )
            if tool_input_state == "valid":
                terminal_parent = source.sink.record(
                    {
                        "kind": "tool",
                        "phase": "start",
                        "name": "otel.span.late_input",
                        "trace": root.identity,
                        "native_identity": tool_call_id or span_id,
                        "coverage": coverage,
                        "native": _semantic_span_snapshot(span, "tool", "start"),
                        "semantic": _span_semantic(
                            span,
                            "tool",
                            "start",
                            claimed,
                            strict_model_messages=source._strict_model_messages,
                        ),
                    }
                )
            else:
                tool_projectable = False
                _record_content_gap(
                    source.sink,
                    root.identity,
                    opened.start,
                    (
                        "tool_input_malformed"
                        if tool_input_state == "malformed"
                        else "tool_input_not_captured"
                    ),
                )
        if opened.kind == "agent-root":
            if root is not None:
                root.terminal_agent_span = span
                root.failed = failed
                root.error = _error_snapshot(span) if failed else None
                if not root.agent_input_captured:
                    agent_input_state, _agent_input = _message_array_attribute(
                        dict(attributes or {}),
                        "gen_ai.input.messages",
                    )
                    if agent_input_state == "valid":
                        _record_agent_messages(
                            source.sink,
                            root,
                            span,
                            span_id,
                            "gen_ai.input.messages",
                            "context",
                            root.start,
                        )
                        root.agent_input_captured = True
                    else:
                        _record_content_gap(
                            source.sink,
                            root.identity,
                            root.start,
                            (
                                "agent_input_malformed"
                                if agent_input_state == "malformed"
                                else "agent_input_not_captured"
                            ),
                        )
                    root.agent_input_captured = True
        elif opened.kind == "control-root":
            if root is not None:
                root.terminal_agent_span = span
                root.failed = failed
                root.error = _error_snapshot(span) if failed else None
        else:
            value: dict[str, Any] = {
                "kind": "lifecycle" if opened.kind == "agent-scope" else opened.kind,
                "phase": "error" if failed else "end",
                "name": "otel.span",
                "trace": opened.trace,
                "native_identity": tool_call_id or span_id,
                "coverage": coverage,
                "native": _semantic_span_snapshot(span, opened.kind, "error" if failed else "end"),
                "semantic": (
                    _agent_scope_semantic(span, "error" if failed else "end")
                    if opened.kind == "agent-scope"
                    else _span_semantic(
                        span,
                        opened.kind,
                        "error" if failed else "end",
                        claimed,
                        strict_model_messages=source._strict_model_messages,
                    )
                ),
            }
            if (
                (opened.kind != "unknown" or source._preserve_unknown_span_parents)
                and terminal_parent.accepted
                and terminal_parent.record_id is not None
            ):
                value["parent_record_id"] = terminal_parent.record_id
            if opened.kind == "tool" and not tool_projectable:
                value["semantic"] = {"type": "capture.redundant", "route": "otel"}
            terminal = source.sink.record(value)
        if root is not None and opened.kind == "model":
            output_state, _output_value = (
                _message_array_attribute(
                    dict(attributes or {}),
                    "gen_ai.output.messages",
                )
                if source._strict_model_messages
                else _json_attribute(attributes, "gen_ai.output.messages")
            )
            _append_model_output(root, span, terminal)
            if output_state == "malformed":
                _record_content_gap(
                    source.sink,
                    root.identity,
                    terminal,
                    "model_output_malformed",
                )
            elif not failed and output_state == "missing":
                _record_content_gap(
                    source.sink,
                    root.identity,
                    terminal,
                    "model_output_not_captured",
                )
        if root is not None and opened.kind == "tool" and tool_projectable and not failed:
            tool_output_state, _tool_output = _json_attribute(
                attributes,
                "gen_ai.tool.call.result",
            )
            if tool_output_state != "valid":
                _record_content_gap(
                    source.sink,
                    root.identity,
                    terminal,
                    (
                        "tool_output_malformed"
                        if tool_output_state == "malformed"
                        else "tool_output_not_captured"
                    ),
                )
        if root is not None and opened.kind == "agent-scope" and opened.semantic_parent:
            if _attribute_present(attributes, "gen_ai.output.messages"):
                _record_agent_messages(
                    source.sink,
                    root,
                    span,
                    span_id,
                    "gen_ai.output.messages",
                    "observed",
                    opened.start,
                )
            elif not failed:
                _record_content_gap(
                    source.sink,
                    root.identity,
                    opened.start,
                    "nested_agent_output_not_captured",
                )
        if failed and opened.kind not in {"unknown", "control-root"}:
            if root is not None and (
                root.agent_root_span_id is None or opened.kind == "agent-root"
            ):
                root.failed = True
                root.error = _error_snapshot(span)
            if opened.kind == "model":
                source.sink.record(
                    {
                        "kind": "error",
                        "phase": "event",
                        "name": "otel.span.error",
                        "trace": opened.trace,
                        "native_identity": tool_call_id or span_id,
                        "coverage": coverage,
                        "native": {
                            "error": _error_snapshot(span),
                            "span": _span_snapshot(span),
                        },
                        **(
                            {"parent_record_id": terminal.record_id}
                            if terminal.accepted and terminal.record_id is not None
                            else {}
                        ),
                        "semantic": {
                            "type": "error",
                            "route": "otel",
                            "error": _semantic_error(span),
                        },
                    }
                )
        if root is not None:
            root.open_spans -= 1
            if root.open_spans == 0:
                source._close_trace(opened.trace_id, root)

    def _on_ending(self, span: Any) -> None:
        del span

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        del timeout_millis
        return True

    def shutdown(self) -> None:
        return None


class _LogRecordProcessor:
    def __init__(self, source: OpenTelemetrySource) -> None:
        self.source = source

    def on_emit(self, record: Any) -> None:
        source = self.source
        if not source._begin_callback():
            return
        try:
            with source.lock:
                self._on_emit(record)
        finally:
            source._end_callback()

    def _on_emit(self, record: Any) -> None:
        source = self.source
        payload = getattr(record, "log_record", record)
        claimed = _has_gen_ai_signal(getattr(payload, "attributes", None))
        trace_value = getattr(payload, "trace_id", None)
        span_value = getattr(payload, "span_id", None)
        if trace_value is None and span_value is None:
            if claimed:
                source._record_context_gap("missing_log_context", "log", _log_snapshot(record))
            return
        identifiers = _raw_context_ids(trace_value, span_value)
        if identifiers is None:
            if claimed:
                source._record_context_gap("invalid_log_context", "log", _log_snapshot(record))
            return
        trace_id, span_id = identifiers
        opened = source.spans.get(_span_key(trace_id, span_id))
        root = source.traces.get(trace_id)
        if opened is None or root is None:
            if claimed:
                source._record_context_gap("orphan_log_context", "log", _log_snapshot(record))
            return
        if not claimed:
            return
        value: dict[str, Any] = {
            "kind": "log",
            "phase": "event",
            "name": "otel.log",
            "trace": root.identity,
            "native_identity": span_id,
            "coverage": _OTEL_OBSERVATION_COVERAGE,
            "native": _log_snapshot(record),
            "semantic": _unknown("log"),
        }
        if opened.start.accepted and opened.start.record_id is not None:
            value["parent_record_id"] = opened.start.record_id
        source.sink.record(value)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        del timeout_millis
        return True

    def shutdown(self) -> None:
        return None


class OpenTelemetrySource(CaptureSource):
    def __init__(self, version: str, *, seam: str = "SpanProcessor/LogRecordProcessor") -> None:
        self.metadata = {
            "name": "generic:otel",
            "seam": seam,
            "identity_domain": "otel.trace-span",
            "version": version,
            "official": True,
            "coverage": [
                {**_OTEL_OBSERVATION_COVERAGE, "role": "evidence"},
                {**_TOOL_EXECUTION_COVERAGE, "role": "evidence"},
            ],
        }
        self.sink: Any = None
        self.active = False
        self.lock = threading.RLock()
        self.settled = threading.Condition(self.lock)
        self.inflight_callbacks = 0
        self._preserve_unknown_span_parents = False
        self._strict_model_messages = True
        self.traces: dict[str, _Trace] = {}
        self.spans: dict[str, _Span] = {}
        self.ignored_spans: set[str] = set()
        self.span_processor = _SpanProcessor(self)
        self.log_record_processor = _LogRecordProcessor(self)
        if type(self) is OpenTelemetrySource:
            _trust_official_source(self, "otel")

    def install(self, sink: Any) -> _Lifecycle:
        with self.lock:
            if self.active:
                raise RuntimeError("OpenTelemetry source is already installed")
            self.sink = sink
            self.active = True
        return _Lifecycle(self)

    def _begin_callback(self) -> bool:
        with self.settled:
            if not self.active or self.sink is None:
                return False
            self.inflight_callbacks += 1
            return True

    def _end_callback(self) -> None:
        with self.settled:
            self.inflight_callbacks -= 1
            self.settled.notify_all()

    def _open_trace(self, trace_id: str, span: Any, context: Any, kind: str) -> _Trace | None:
        span_id = _span_id(context)
        flags = int(getattr(context, "trace_flags", 0)) & 0xFF
        name = _agent_name(span)
        root_semantic = self._root_semantic(name)
        schema_url = _instrumentation_schema_url(span)
        agent_claimed = root_semantic is None and kind == "agent"
        agent_root = agent_claimed and schema_url == _GENAI_SCHEMA_URL
        control_agent_root = agent_claimed and not agent_root
        attributes = dict(getattr(span, "attributes", None) or {})
        input_present = "gen_ai.input.messages" in attributes
        input_value = (
            _structured_attribute(attributes["gen_ai.input.messages"]) if input_present else None
        )
        input_discarded = (
            _normalize_model_messages(input_value)[1] if isinstance(input_value, list) else 0
        )
        native = {
            "trace_id": trace_id,
            "root_name": name,
            "run": {"kind": "otel.trace"},
            "coverage": "unknown",
        }
        opened = self.sink.open_trace(
            {
                "name": name if agent_root else "otel.trace",
                "native_identity": trace_id,
                "coverage": _OTEL_OBSERVATION_COVERAGE,
                "parent_context": {"traceparent": f"00-{trace_id}-{span_id}-{flags:02x}"},
                "native": (
                    native
                    if root_semantic is not None
                    else _span_snapshot(span)
                    if agent_claimed
                    else None
                ),
                "semantic": (
                    root_semantic
                    if root_semantic is not None
                    else {
                        "type": "agent.run",
                        "name": name,
                        **({"input": input_value} if isinstance(input_value, list) else {}),
                        "route": "otel",
                        "semconv": schema_url,
                    }
                    if agent_root
                    else {"type": "capture.control", "route": "otel"}
                ),
            }
        )
        if not opened.accepted or opened.identity is None or opened.record_id is None:
            return None
        result = _Trace(
            opened.identity,
            opened,
            name,
            agent_root_span_id=span_id if agent_root else None,
            control_root_span_id=span_id if control_agent_root else None,
            agent_input_captured=input_present if agent_root else False,
        )
        self.traces[trace_id] = result
        if agent_root and input_present:
            if not isinstance(input_value, list):
                _record_content_gap(
                    self.sink,
                    result.identity,
                    result.start,
                    "agent_input_malformed",
                )
            elif input_discarded:
                _record_content_gap(
                    self.sink,
                    result.identity,
                    result.start,
                    "agent_input_partially_malformed",
                )
        if control_agent_root:
            _record_schema_gap(
                self.sink,
                result.identity,
                result.start,
                schema_url,
            )
        return result

    def _root_semantic(self, name: str) -> dict[str, str] | None:
        """Return authoritative root semantics supplied by a specialized OTel route."""

        del name
        return None

    def _close_trace(self, trace_id: str, root: _Trace) -> None:
        terminal = root.terminal_agent_span
        output_present = terminal is not None and _attribute_present(
            getattr(terminal, "attributes", None),
            "gen_ai.output.messages",
        )
        output_value = (
            _structured_attribute(
                dict(getattr(terminal, "attributes", None) or {})["gen_ai.output.messages"]
            )
            if output_present
            else None
        )
        if root.agent_root_span_id is not None and terminal is not None:
            if not output_present and not root.failed:
                _record_content_gap(
                    self.sink,
                    root.identity,
                    root.start,
                    "agent_output_not_captured",
                )
            elif output_present and not isinstance(output_value, list):
                _record_content_gap(
                    self.sink,
                    root.identity,
                    root.start,
                    "agent_output_malformed",
                )
            elif isinstance(output_value, list):
                _normalized, discarded = _normalize_model_messages(output_value)
                if discarded:
                    _record_content_gap(
                        self.sink,
                        root.identity,
                        root.start,
                        "agent_output_partially_malformed",
                    )
        self.sink.record(
            {
                "kind": "lifecycle",
                "phase": "error" if root.failed else "end",
                "name": root.name,
                "trace": root.identity,
                "native_identity": trace_id,
                **(
                    {"parent_record_id": root.start.record_id}
                    if root.start.accepted and root.start.record_id is not None
                    else {}
                ),
                "coverage": _OTEL_OBSERVATION_COVERAGE,
                "native": (
                    _span_snapshot(terminal)
                    if terminal is not None and root.agent_root_span_id is not None
                    else None
                ),
                "semantic": (
                    {
                        "type": "agent.run",
                        "status": (
                            "failed"
                            if root.failed
                            else "completed"
                            if terminal is not None
                            else "unknown"
                        ),
                        **({"output": output_value} if isinstance(output_value, list) else {}),
                        **(
                            {"error": _semantic_error(terminal)}
                            if root.failed and terminal is not None
                            else {}
                        ),
                        "route": "otel",
                        "semconv": _GENAI_SCHEMA_URL,
                    }
                    if root.agent_root_span_id is not None
                    else {"type": "capture.control", "route": "otel"}
                ),
            }
        )
        self.traces.pop(trace_id, None)

    def _record_context_gap(self, reason: str, native_type: str, snapshot: dict[str, Any]) -> None:
        opened = self.sink.open_trace(
            {
                "name": "otel.observability_gap",
                "coverage": _OTEL_OBSERVATION_COVERAGE,
                "native": None,
                "semantic": {"type": "capture.control", "route": "otel"},
            }
        )
        if not opened.accepted or opened.identity is None:
            return
        self.sink.record(
            {
                "kind": "unknown",
                "phase": "gap",
                "name": "otel.context.gap",
                "trace": opened.identity,
                "coverage": _OTEL_OBSERVATION_COVERAGE,
                "native": {
                    "reason": reason,
                    "native_type": native_type,
                    "snapshot": snapshot,
                },
                "semantic": {
                    "type": "capture.gap",
                    "reason": reason,
                    "detail": f"OpenTelemetry {native_type} context could not be correlated.",
                },
            }
        )
        self.sink.record(
            {
                "kind": "lifecycle",
                "phase": "end",
                "name": "otel.observability_gap",
                "trace": opened.identity,
                "coverage": _OTEL_OBSERVATION_COVERAGE,
                "native": None,
                "semantic": {"type": "capture.control", "route": "otel"},
            }
        )


def create_otel_source(*, version: str) -> OpenTelemetrySource:
    return _trust_official_source(OpenTelemetrySource(version), "otel")  # type: ignore[return-value]


def _trace_id(context: Any) -> str | None:
    identifiers = _context_ids(context)
    return identifiers[0] if identifiers is not None else None


def _span_id(context: Any) -> str | None:
    identifiers = _context_ids(context)
    return identifiers[1] if identifiers is not None else None


def _read_span_context(span: Any) -> Any:
    try:
        return span.get_span_context()
    except Exception:
        return None


def _context_ids(context: Any) -> tuple[str, str] | None:
    if context is None or getattr(context, "is_valid", True) is False:
        return None
    return _raw_context_ids(getattr(context, "trace_id", None), getattr(context, "span_id", None))


def _raw_context_ids(trace_id: Any, span_id: Any) -> tuple[str, str] | None:
    if (
        not isinstance(trace_id, int)
        or isinstance(trace_id, bool)
        or not 0 < trace_id < 1 << 128
        or not isinstance(span_id, int)
        or isinstance(span_id, bool)
        or not 0 < span_id < 1 << 64
    ):
        return None
    return f"{trace_id:032x}", f"{span_id:016x}"


def _span_key(trace_id: str, span_id: str) -> str:
    return f"{trace_id}:{span_id}"


def _span_snapshot(span: Any) -> dict[str, Any]:
    context = _read_span_context(span)
    parent = getattr(span, "parent", None)
    resource = getattr(span, "resource", None)
    scope = getattr(span, "instrumentation_scope", None)
    return {
        "name": getattr(span, "name", None),
        "trace_id": _trace_id(context),
        "span_id": _span_id(context),
        "trace_flags": int(getattr(context, "trace_flags", 0)),
        "trace_state": str(getattr(context, "trace_state", "")),
        "parent_span_id": _span_id(parent) if parent is not None else None,
        "kind": str(getattr(span, "kind", "")),
        "start_time": getattr(span, "start_time", None),
        "end_time": getattr(span, "end_time", None),
        "attributes": native_snapshot(dict(getattr(span, "attributes", None) or {})),
        "events": [_event_snapshot(event) for event in (getattr(span, "events", None) or ())],
        "links": [_link_snapshot(link) for link in (getattr(span, "links", None) or ())],
        "status": _status_snapshot(getattr(span, "status", None)),
        "resource": native_snapshot(dict(getattr(resource, "attributes", None) or {})),
        "resource_schema_url": getattr(resource, "schema_url", None),
        "instrumentation_scope": _scope_snapshot(scope),
        "dropped_attributes_count": getattr(span, "dropped_attributes", 0),
        "dropped_events_count": getattr(span, "dropped_events", 0),
        "dropped_links_count": getattr(span, "dropped_links", 0),
    }


def _safe_span_snapshot(span: Any) -> dict[str, Any]:
    try:
        return _span_snapshot(span)
    except Exception as error:
        return {"name": getattr(span, "name", None), "context_error": repr(error)}


def _log_snapshot(record: Any) -> dict[str, Any]:
    payload = getattr(record, "log_record", record)
    resource = getattr(record, "resource", None)
    return {
        "trace_id": _format_raw_id(getattr(payload, "trace_id", None), 32),
        "span_id": _format_raw_id(getattr(payload, "span_id", None), 16),
        "trace_flags": int(getattr(payload, "trace_flags", 0)),
        "timestamp": getattr(payload, "timestamp", None),
        "observed_timestamp": getattr(payload, "observed_timestamp", None),
        "severity_number": str(getattr(payload, "severity_number", "")),
        "severity_text": getattr(payload, "severity_text", None),
        "event_name": getattr(payload, "event_name", None),
        "body": native_snapshot(getattr(payload, "body", None)),
        "attributes": native_snapshot(dict(getattr(payload, "attributes", None) or {})),
        "resource": native_snapshot(dict(getattr(resource, "attributes", None) or {})),
        "resource_schema_url": getattr(resource, "schema_url", None),
        "instrumentation_scope": _scope_snapshot(getattr(record, "instrumentation_scope", None)),
        "dropped_attributes_count": getattr(payload, "dropped_attributes", 0),
    }


def _span_kind(attributes: Any) -> str:
    values = dict(attributes or {})
    if values.get("gen_ai.operation.name") == "invoke_agent":
        return "agent"
    if values.get("gen_ai.operation.name") == "execute_tool":
        return "tool"
    if values.get("gen_ai.operation.name") in {
        "chat",
        "text_completion",
        "generate_content",
    }:
        return "model"
    return "unknown"


def _has_gen_ai_signal(attributes: Any) -> bool:
    return any(
        isinstance(name, str) and name.startswith("gen_ai.") for name in dict(attributes or {})
    )


def _exact_tool_call_id(attributes: Any) -> str | None:
    values = dict(attributes or {})
    if values.get("gen_ai.operation.name") != "execute_tool":
        return None
    tool_name = values.get("gen_ai.tool.name")
    if not isinstance(tool_name, str) or not tool_name.strip():
        return None
    call_id = values.get("gen_ai.tool.call.id")
    return (
        call_id
        if (
            isinstance(call_id, str)
            and call_id.strip()
            and len(call_id) <= 256
            and "\0" not in call_id
        )
        else None
    )


def _invalid_tool_call_id(attributes: Any) -> bool:
    values = dict(attributes or {})
    return (
        values.get("gen_ai.operation.name") == "execute_tool"
        and "gen_ai.tool.call.id" in values
        and _exact_tool_call_id(values) is None
    )


def _semantic_span_snapshot(span: Any, kind: str, phase: str) -> dict[str, Any]:
    snapshot = _span_snapshot(span)
    attributes = dict(getattr(span, "attributes", None) or {})
    if kind == "model":
        if phase == "start":
            snapshot["model"] = attributes.get("gen_ai.request.model")
            snapshot["input"] = _structured_attribute(attributes.get("gen_ai.input.messages"))
        else:
            snapshot["output"] = _structured_attribute(attributes.get("gen_ai.output.messages"))
            usage: dict[str, Any] = {}
            for native_name, semantic_name in (
                ("gen_ai.usage.input_tokens", "input_tokens"),
                ("gen_ai.usage.output_tokens", "output_tokens"),
            ):
                value = attributes.get(native_name)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    usage[semantic_name] = value
            if usage:
                snapshot["usage"] = usage
    elif kind == "tool":
        snapshot["name"] = attributes.get("gen_ai.tool.name")
        snapshot["call_id"] = attributes.get("gen_ai.tool.call.id")
        snapshot["input" if phase == "start" else "output"] = _structured_attribute(
            attributes.get(
                "gen_ai.tool.call.arguments" if phase == "start" else "gen_ai.tool.call.result"
            )
        )
    return snapshot


def _span_semantic(
    span: Any,
    kind: str,
    phase: str,
    claimed: bool,
    context_refs: list[str] | None = None,
    *,
    strict_model_messages: bool = True,
) -> dict[str, Any]:
    attributes = dict(getattr(span, "attributes", None) or {})
    if kind == "model":
        output_state, output_value = (
            _message_array_attribute(
                attributes,
                "gen_ai.output.messages",
            )
            if strict_model_messages
            else _json_attribute(attributes, "gen_ai.output.messages")
        )
        semantic: dict[str, Any] = {
            "type": ("model.request" if phase == "start" else "model.response"),
            "route": "otel",
        }
        model = attributes.get("gen_ai.response.model") or attributes.get("gen_ai.request.model")
        if isinstance(model, str):
            semantic["model"] = model
        if phase == "start":
            semantic["context_refs"] = list(context_refs or [])
        else:
            semantic["status"] = "failed" if phase == "error" else "completed"
            if phase == "error":
                semantic["error"] = _semantic_error(span)
            if output_state == "valid":
                semantic["content"] = _genai_visible_output(output_value)
                reasoning = _genai_output_reasoning(output_value)
                if reasoning:
                    semantic["reasoning"] = reasoning
            if phase != "error":
                semantic["origin"] = "inferred"
            usage: dict[str, int | float] = {}
            for native_name, semantic_name in (
                ("gen_ai.usage.input_tokens", "input_tokens"),
                ("gen_ai.usage.output_tokens", "output_tokens"),
            ):
                value = attributes.get(native_name)
                if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
                    usage[semantic_name] = value
            if usage:
                semantic["usage"] = usage
        return semantic
    if kind == "tool":
        name = attributes.get("gen_ai.tool.name")
        input_state, input_value = _json_attribute(
            attributes,
            "gen_ai.tool.call.arguments",
        )
        output_state, output_value = _json_attribute(
            attributes,
            "gen_ai.tool.call.result",
        )
        return {
            "type": (
                "tool.execution"
                if phase == "start"
                else "tool.error"
                if phase == "error"
                else "tool.result"
            ),
            "route": "otel",
            **({"name": name} if isinstance(name, str) and name.strip() else {}),
            **(
                ({"input": input_value} if input_state == "valid" else {})
                if phase == "start"
                else {
                    "status": "failed" if phase == "error" else "succeeded",
                    **({"error": _semantic_error(span)} if phase == "error" else {}),
                    **({"output": output_value} if output_state == "valid" else {}),
                    **({"origin": "inferred"} if phase != "error" else {}),
                }
            ),
        }
    return (
        {"type": "capture.redundant", "route": "otel"}
        if phase == "start" or not claimed
        else {
            "type": "capture.gap",
            "reason": "unsupported_genai_operation",
            "detail": (
                "The OpenTelemetry span contained GenAI attributes for an unsupported operation."
            ),
        }
    )


def _agent_scope_semantic(span: Any, phase: str) -> dict[str, Any]:
    return {
        "type": "scope",
        "scope_type": "agent",
        "name": _agent_name(span),
        **({"status": "failed" if phase == "error" else "completed"} if phase != "start" else {}),
        **({"error": _semantic_error(span)} if phase == "error" else {}),
        "route": "otel",
    }


def _agent_name(span: Any) -> str:
    attributes = dict(getattr(span, "attributes", None) or {})
    name = attributes.get("gen_ai.agent.name")
    if isinstance(name, str) and name.strip():
        return name
    native_name = getattr(span, "name", None)
    return native_name if isinstance(native_name, str) and native_name else "otel.agent"


def _has_content(attributes: Any, name: str) -> bool:
    values = dict(attributes or {})
    return name in values and values[name] is not None


def _attribute_present(attributes: Any, name: str) -> bool:
    return name in dict(attributes or {})


def _instrumentation_schema_url(span: Any) -> str | None:
    value = getattr(getattr(span, "instrumentation_scope", None), "schema_url", None)
    return value if isinstance(value, str) and value else None


def _record_content_gap(
    sink: Any,
    trace_identity: dict[str, str],
    affected: AdmissionReceipt,
    reason: str,
) -> None:
    sink.record(
        {
            "kind": "unknown",
            "phase": "gap",
            "name": "otel.content.gap",
            "trace": trace_identity,
            **(
                {"parent_record_id": affected.record_id}
                if affected.accepted and affected.record_id is not None
                else {}
            ),
            "native": {
                "reason": reason,
                "semconv": _GENAI_SCHEMA_URL,
            },
            "semantic": {
                "type": "capture.gap",
                "reason": reason,
                "detail": (f"OpenTelemetry did not expose required GenAI content: {reason}."),
            },
        }
    )


def _record_schema_gap(
    sink: Any,
    trace_identity: dict[str, str],
    affected: AdmissionReceipt,
    observed_schema_url: str | None,
) -> None:
    reason = "missing_genai_schema" if observed_schema_url is None else "unsupported_genai_schema"
    sink.record(
        {
            "kind": "unknown",
            "phase": "gap",
            "name": "otel.genai.schema",
            "trace": trace_identity,
            **(
                {"parent_record_id": affected.record_id}
                if affected.accepted and affected.record_id is not None
                else {}
            ),
            "coverage": _OTEL_OBSERVATION_COVERAGE,
            "native": {
                "reason": reason,
                "observed_schema_url": observed_schema_url,
                "supported_schema_url": _GENAI_SCHEMA_URL,
            },
            "semantic": {
                "type": "capture.gap",
                "reason": reason,
                "detail": (
                    "The invoke_agent span did not report an instrumentation "
                    f"schema URL; expected {_GENAI_SCHEMA_URL}."
                    if observed_schema_url is None
                    else "The invoke_agent span reported unsupported "
                    f"instrumentation schema URL {observed_schema_url}; "
                    f"expected {_GENAI_SCHEMA_URL}."
                ),
            },
        }
    )


def _record_model_input(
    sink: Any,
    root: _Trace,
    span: Any,
    span_id: str,
) -> list[str]:
    attributes = dict(getattr(span, "attributes", None) or {})
    if "gen_ai.input.messages" not in attributes:
        return []
    model_input = _structured_attribute(attributes["gen_ai.input.messages"])
    if not isinstance(model_input, list):
        root.model_input.clear()
        _record_model_input_loss(sink, root.identity, span_id, 1)
        return []

    normalized, discarded = _normalize_model_messages(model_input)

    refs: list[str] = []
    prefix = 0
    while (
        prefix < len(normalized)
        and prefix < len(root.model_input)
        and normalized[prefix][0] == root.model_input[prefix][0]
    ):
        receipt = root.model_input[prefix][1]
        _append_receipt_ref(refs, receipt)
        prefix += 1
    del root.model_input[prefix:]
    for key, index, message, semantic in normalized[prefix:]:
        receipt = sink.record(
            {
                "kind": "model",
                "phase": "event",
                "name": "otel.model.context",
                "trace": root.identity,
                "native_identity": f"{span_id}:input:{index}",
                "coverage": _OTEL_OBSERVATION_COVERAGE,
                "native": {
                    "attribute": "gen_ai.input.messages",
                    "index": index,
                    "message": message,
                },
                "semantic": {
                    "type": "message",
                    "route": "otel",
                    "origin": "context",
                    **semantic,
                },
            }
        )
        root.model_input.append((key, receipt))
        _append_receipt_ref(refs, receipt)
    if discarded:
        _record_model_input_loss(sink, root.identity, span_id, discarded)
    return refs


def _record_agent_messages(
    sink: Any,
    root: _Trace,
    span: Any,
    span_id: str,
    attribute: str,
    origin: str,
    parent: AdmissionReceipt,
) -> None:
    attributes = dict(getattr(span, "attributes", None) or {})
    value = _structured_attribute(attributes.get(attribute))
    if not isinstance(value, list):
        _record_content_gap(
            sink,
            root.identity,
            parent,
            (
                "agent_input_malformed"
                if attribute == "gen_ai.input.messages"
                else "agent_output_malformed"
            ),
        )
        return
    messages, discarded = _normalize_model_messages(value)
    for _key, index, message, semantic in messages:
        sink.record(
            {
                "kind": "model",
                "phase": "event",
                "name": "otel.agent.message",
                "trace": root.identity,
                "native_identity": f"{span_id}:{attribute}:{index}",
                "coverage": _OTEL_OBSERVATION_COVERAGE,
                **(
                    {"parent_record_id": parent.record_id}
                    if parent.accepted and parent.record_id is not None
                    else {}
                ),
                "native": {
                    "attribute": attribute,
                    "index": index,
                    "message": message,
                },
                "semantic": {
                    "type": "message",
                    "route": "otel",
                    "origin": origin,
                    **semantic,
                },
            }
        )
    if discarded:
        _record_content_gap(
            sink,
            root.identity,
            parent,
            (
                "agent_input_partially_malformed"
                if attribute == "gen_ai.input.messages"
                else "agent_output_partially_malformed"
            ),
        )


def _append_model_output(
    root: _Trace,
    span: Any,
    receipt: AdmissionReceipt,
) -> None:
    if not receipt.accepted:
        return
    attributes = dict(getattr(span, "attributes", None) or {})
    if "gen_ai.output.messages" not in attributes:
        return
    output = _structured_attribute(attributes["gen_ai.output.messages"])
    if not isinstance(output, list):
        return
    normalized, discarded = _normalize_model_messages(output)
    if discarded:
        return
    root.model_input.extend((key, receipt) for key, _index, _message, _semantic in normalized)


def _normalize_model_messages(
    messages: list[Any],
) -> tuple[list[tuple[str, int, dict[str, Any], dict[str, Any]]], int]:
    discarded = 0
    normalized: list[tuple[str, int, dict[str, Any], dict[str, Any]]] = []
    for index, message in enumerate(messages):
        raw_name = message.get("name") if isinstance(message, dict) else None
        if (
            not isinstance(message, dict)
            or message.get("role") not in _MODEL_INPUT_ROLES
            or not isinstance(message.get("parts"), list)
            or not _is_json_value(message["parts"])
            or (raw_name is not None and (not isinstance(raw_name, str) or not raw_name.strip()))
        ):
            discarded += 1
            continue
        semantic = {
            "role": message["role"],
            "content": message["parts"],
            **({"name": raw_name[:256]} if isinstance(raw_name, str) else {}),
        }
        normalized.append(
            (
                json.dumps(
                    semantic,
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                index,
                message,
                semantic,
            )
        )
    return normalized, discarded


def _genai_output_reasoning(output: Any) -> list[dict[str, str]]:
    """Project explicit GenAI reasoning parts in their observed order."""

    if not isinstance(output, list):
        return []
    reasoning: list[dict[str, str]] = []
    for message in output:
        parts = message.get("parts") if isinstance(message, Mapping) else None
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, Mapping) or part.get("type") != "reasoning":
                continue
            text = part.get("content")
            if isinstance(text, str) and text:
                reasoning.append({"type": "text", "text": text})
    return reasoning


def _genai_visible_output(output: Any) -> Any:
    """Exclude reasoning parts from semantic answer content without changing native evidence."""

    if not isinstance(output, list):
        return output
    visible: list[Any] = []
    for message in output:
        if not isinstance(message, Mapping):
            visible.append(message)
            continue
        parts = message.get("parts")
        if not isinstance(parts, list):
            visible.append(dict(message))
            continue
        visible.append(
            {
                **message,
                "parts": [
                    part
                    for part in parts
                    if not (isinstance(part, Mapping) and part.get("type") == "reasoning")
                ],
            }
        )
    return visible


def _append_receipt_ref(refs: list[str], receipt: AdmissionReceipt) -> None:
    if receipt.accepted and receipt.record_id is not None and receipt.record_id not in refs:
        refs.append(receipt.record_id)


def _record_model_input_loss(
    sink: Any,
    trace: dict[str, str],
    span_id: str,
    discarded: int,
) -> None:
    sink.record(
        {
            "kind": "unknown",
            "phase": "gap",
            "name": "otel.model.input",
            "trace": trace,
            "native_identity": f"{span_id}:input:loss",
            "coverage": _OTEL_OBSERVATION_COVERAGE,
            "native": {
                "attribute": "gen_ai.input.messages",
                "discarded_messages": discarded,
            },
            "semantic": {
                "type": "capture.gap",
                "reason": "model_input_messages_discarded",
                "count": discarded,
                "detail": (
                    "OpenTelemetry model input contained messages that could not be represented."
                ),
            },
        }
    )


def _is_json_value(value: Any) -> bool:
    try:
        json.dumps(value, allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        return False
    return True


def _structured_attribute(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


def _message_array_attribute(
    attributes: dict[str, Any],
    name: str,
) -> tuple[str, list[Any] | None]:
    if name not in attributes:
        return "missing", None
    value = _structured_attribute(attributes[name])
    if not isinstance(value, list):
        return "malformed", None
    _messages, discarded = _normalize_model_messages(value)
    return ("valid", value) if discarded == 0 else ("malformed", None)


def _json_attribute(
    attributes: Any,
    name: str,
) -> tuple[str, Any]:
    values = dict(attributes or {})
    if name not in values:
        return "missing", None
    raw = values[name]
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return "malformed", None
    else:
        value = raw
    return ("valid", value) if _is_json_value(value) else ("malformed", None)


def _event_snapshot(event: Any) -> dict[str, Any]:
    return {
        "name": getattr(event, "name", None),
        "timestamp": getattr(event, "timestamp", None),
        "attributes": native_snapshot(dict(getattr(event, "attributes", None) or {})),
        "dropped_attributes_count": getattr(event, "dropped_attributes", 0),
    }


def _link_snapshot(link: Any) -> dict[str, Any]:
    context = getattr(link, "context", None)
    identifiers = _context_ids(context)
    return {
        "trace_id": identifiers[0] if identifiers else None,
        "span_id": identifiers[1] if identifiers else None,
        "trace_flags": int(getattr(context, "trace_flags", 0)),
        "trace_state": str(getattr(context, "trace_state", "")),
        "attributes": native_snapshot(dict(getattr(link, "attributes", None) or {})),
        "dropped_attributes_count": getattr(link, "dropped_attributes", 0),
    }


def _status_snapshot(status: Any) -> dict[str, Any]:
    return {
        "code": str(getattr(status, "status_code", "")),
        "description": getattr(status, "description", None),
    }


def _scope_snapshot(scope: Any) -> dict[str, Any]:
    return {
        "name": getattr(scope, "name", None),
        "version": getattr(scope, "version", None),
        "schema_url": getattr(scope, "schema_url", None),
        "attributes": native_snapshot(dict(getattr(scope, "attributes", None) or {})),
    }


def _error_snapshot(span: Any) -> dict[str, Any]:
    attributes = dict(getattr(span, "attributes", None) or {})
    return {
        "type": attributes.get("error.type"),
        "status": _status_snapshot(getattr(span, "status", None)),
        "events": [_event_snapshot(event) for event in (getattr(span, "events", None) or ())],
    }


def _semantic_error(span: Any) -> dict[str, Any]:
    attributes = dict(getattr(span, "attributes", None) or {})
    native_type = attributes.get("error.type")
    message = getattr(getattr(span, "status", None), "description", None)
    for event in getattr(span, "events", None) or ():
        if getattr(event, "name", None) != "exception":
            continue
        event_attributes = dict(getattr(event, "attributes", None) or {})
        if not isinstance(native_type, str) or not native_type:
            native_type = event_attributes.get("exception.type")
        if not isinstance(message, str) or not message:
            message = event_attributes.get("exception.message")
        break
    normalized_type = (
        re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", native_type).lower()
        if isinstance(native_type, str) and native_type
        else "otel_span_error"
    )
    normalized_type = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in normalized_type
    ).strip("._-")
    if len(normalized_type) < 3 or not normalized_type[0].isalpha():
        normalized_type = "otel_span_error"
    return {
        "type": normalized_type[:127],
        "message": (
            message[:4096]
            if isinstance(message, str) and message
            else "OpenTelemetry span ended with an error status."
        ),
        "recoverable": False,
    }


def _unknown(native_type: str) -> dict[str, str]:
    return {"coverage": "unknown", "native_type": native_type, "route": "otel"}


def _format_raw_id(value: Any, width: int) -> str | None:
    return f"{value:0{width}x}" if isinstance(value, int) and not isinstance(value, bool) else None
