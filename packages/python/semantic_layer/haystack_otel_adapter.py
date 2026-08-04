"""Caller-registered additive OpenTelemetry capture route for Haystack."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

from ._framework_adapter_shared import (
    _installed_version,
    _record_unavailable_reasoning_gap,
    _source_qualification,
)
from .capture_v1 import _trust_official_source
from .otel import OpenTelemetrySource, _genai_output_reasoning, _genai_visible_output


class _HaystackOpenTelemetrySource(OpenTelemetrySource):
    def __init__(self, version: str, *, seam: str) -> None:
        super().__init__(version, seam=seam)
        otel_version = _installed_version("opentelemetry-sdk", None)
        self.metadata.update(
            {
                "name": "official:haystack-otel",
                "version": (f"haystack-ai={version};opentelemetry-sdk={otel_version}"),
                "qualification": _source_qualification(
                    f"haystack-ai={version};opentelemetry-sdk={otel_version}",
                    exact_versions=frozenset(
                        {"haystack-ai=2.31.0;opentelemetry-sdk=1.42.1"}
                    ),
                    profile="haystack-otel-python-adapter-v1",
                ),
            }
        )
        self._preserve_unknown_span_parents = True
        self._strict_model_messages = False
        self._root_receipts: dict[str, Any] = {}
        self._pipeline_terminals: dict[str, dict[str, Any]] = {}
        self._pipeline_start_outputs: dict[str, tuple[bool, Any]] = {}
        self._haystack_capture_traces: set[str] = set()
        self._model_component_parents: dict[str, tuple[str, str]] = {}
        self._components_with_exact_model_response: set[tuple[str, str]] = set()

    def install(self, sink: Any) -> Any:
        return super().install(_HaystackSink(self, sink))

    def _root_semantic(self, name: str) -> dict[str, str] | None:
        if name not in {"haystack.pipeline.run", "haystack.async_pipeline.run"}:
            return None
        return {
            "type": "workflow.run",
            "framework": "haystack",
            "route": "otel",
            "name": name,
        }

    def _close_trace(self, trace_id: str, root: Any) -> None:
        if trace_id not in self._root_receipts:
            super()._close_trace(trace_id, root)
            return
        terminal = self._pipeline_terminals.pop(trace_id, {})
        self._pipeline_start_outputs.pop(trace_id, None)
        failed = bool(terminal.get("failed"))
        terminal_error = terminal.get("error") if failed else None
        receipt = self._root_receipts.pop(trace_id, None)
        value: dict[str, Any] = {
            "kind": "lifecycle",
            "phase": "error" if failed else "end",
            "name": "otel.trace",
            "trace": root.identity,
            "native_identity": trace_id,
            "coverage": {
                "operation": "otel-observation",
                "domain": "otel.trace-span",
            },
            "native": {
                "trace_id": trace_id,
                "coverage": "unknown",
                **({"error": terminal_error} if terminal_error is not None else {}),
            },
            "semantic": {
                "type": "workflow.run",
                "framework": "haystack",
                "route": "otel",
                "status": "failed" if failed else "succeeded",
                **(
                    {"error": _semantic_error(terminal_error)} if terminal_error is not None else {}
                ),
            },
        }
        if receipt is not None and receipt.accepted and receipt.record_id is not None:
            value["parent_record_id"] = receipt.record_id
        self.sink.record(value)
        capture_trace_id = root.identity.get("trace_id")
        if type(capture_trace_id) is str:
            self._haystack_capture_traces.discard(capture_trace_id)
            self._components_with_exact_model_response = {
                marker
                for marker in self._components_with_exact_model_response
                if marker[0] != capture_trace_id
            }
            self._model_component_parents = {
                request: marker
                for request, marker in self._model_component_parents.items()
                if marker[0] != capture_trace_id
            }
        self.traces.pop(trace_id, None)


class _HaystackSink:
    """Normalize only documented Haystack and GenAI OTel conventions."""

    def __init__(self, source: _HaystackOpenTelemetrySource, sink: Any) -> None:
        self._source = source
        self._sink = sink

    def open_trace(self, value: Any) -> Any:
        receipt = self._sink.open_trace(value)
        native = value.get("native") if type(value) is dict else None
        trace_id = native.get("trace_id") if type(native) is dict else None
        semantic = value.get("semantic") if type(value) is dict else None
        if (
            type(trace_id) is str
            and type(semantic) is dict
            and semantic.get("framework") == "haystack"
        ):
            self._source._root_receipts[trace_id] = receipt
            capture_trace_id = (
                receipt.identity.get("trace_id") if receipt.identity is not None else None
            )
            if type(capture_trace_id) is str:
                self._source._haystack_capture_traces.add(capture_trace_id)
        return receipt

    def record(self, value: Any) -> Any:
        normalized = dict(value)
        native = normalized.get("native")
        native_data: dict[str, Any] = native if type(native) is dict else {}
        native_name = native_data.get("name")
        phase = normalized.get("phase")
        trace_id = native_data.get("trace_id")
        capture_trace = normalized.get("trace")
        capture_trace_id = (
            capture_trace.get("trace_id") if isinstance(capture_trace, Mapping) else None
        )
        if capture_trace_id not in self._source._haystack_capture_traces:
            return self._sink.record(value)

        if native_name in {"haystack.pipeline.run", "haystack.async_pipeline.run"}:
            pipeline = _pipeline_semantic(native_data)
            if phase == "start":
                if type(trace_id) is str:
                    self._source._pipeline_start_outputs[trace_id] = (
                        "output" in pipeline,
                        pipeline.get("output"),
                    )
                normalized["semantic"] = {
                    "type": "capture.redundant",
                    "framework": "haystack",
                    "route": "otel",
                }
            else:
                start_output = (
                    self._source._pipeline_start_outputs.pop(trace_id, (False, None))
                    if type(trace_id) is str
                    else (False, None)
                )
                failed = _span_failed(native_data)
                stale_placeholder = pipeline.get("output", _MISSING) == {} and (
                    not start_output[0] or start_output[1] == {}
                )
                if stale_placeholder:
                    pipeline.pop("output", None)
                normalized["kind"] = "state"
                normalized["semantic"] = {
                    "type": "state.transition",
                    "framework": "haystack",
                    "route": "otel",
                    "state_type": "state.pipeline_io",
                    "value": pipeline,
                }
                if type(trace_id) is str:
                    self._source._pipeline_terminals[trace_id] = {
                        "failed": failed,
                        **({"error": _pipeline_error_snapshot(native_data)} if failed else {}),
                        **({"output": pipeline["output"]} if "output" in pipeline else {}),
                    }
                receipt = self._sink.record(normalized)
                if stale_placeholder and not failed:
                    _record_pipeline_output_gap(
                        self._sink,
                        normalized,
                        receipt,
                    )
                return receipt
        elif native_name == "haystack.component.run":
            normalized["kind"] = "lifecycle"
            normalized["semantic"] = {
                "type": "workflow.step",
                "framework": "haystack",
                "route": "otel",
                "scope_type": "step",
                "name": _component_name(native_data),
                **(
                    {"status": "failed" if _span_failed(native_data) else "succeeded"}
                    if phase != "start"
                    else {}
                ),
            }
        elif normalized.get("kind") == "model" and (
            not isinstance(normalized.get("semantic"), Mapping)
            or normalized["semantic"].get("type") != "message"
        ):
            model_semantic = _model_semantic(native_data, phase)
            original_semantic = normalized.get("semantic")
            if (
                phase == "start"
                and isinstance(original_semantic, Mapping)
                and isinstance(original_semantic.get("context_refs"), list)
            ):
                model_semantic["context_refs"] = list(original_semantic["context_refs"])
            normalized["semantic"] = model_semantic
        elif normalized.get("kind") == "tool":
            normalized["semantic"] = _tool_semantic(native_data, phase)
        elif normalized.get("kind") == "error":
            error = native_data.get("error")
            normalized["semantic"] = {
                "type": "error",
                "framework": "haystack",
                "route": "otel",
                "error": _semantic_error(error),
            }
        receipt = self._sink.record(normalized)
        trace = normalized.get("trace")
        source_parent = normalized.get("parent_record_id")
        if (
            normalized.get("kind") == "model"
            and isinstance(capture_trace_id, str)
            and isinstance(source_parent, str)
            and receipt.accepted
            and receipt.record_id is not None
        ):
            if phase == "start":
                self._source._model_component_parents[receipt.record_id] = (
                    capture_trace_id,
                    source_parent,
                )
            else:
                component_marker = self._source._model_component_parents.pop(
                    source_parent,
                    None,
                )
                if component_marker is not None:
                    self._source._components_with_exact_model_response.add(component_marker)
        if (
            native_name == "haystack.component.run"
            and phase != "start"
            and isinstance(trace, Mapping)
        ):
            component_marker = (
                (capture_trace_id, source_parent)
                if isinstance(capture_trace_id, str) and isinstance(source_parent, str)
                else None
            )
            exact_model_response = (
                component_marker is not None
                and component_marker in self._source._components_with_exact_model_response
            )
            component_evidence = _haystack_generator_reasoning_evidence(native_data)
            if component_evidence is not None and not exact_model_response:
                output, unavailable = component_evidence
                state_value: dict[str, Any] = {
                    "kind": "state",
                    "phase": "event",
                    "name": "haystack.component.output",
                    "trace": dict(trace),
                    "native_identity": (
                        f"{native_data.get('span_id', 'component')}:reasoning-output"
                    ),
                    "native": {
                        "component": _component_name(native_data),
                        "output": output,
                    },
                    "semantic": {
                        "type": "state.transition",
                        "framework": "haystack",
                        "state_type": "state.haystack_generator_output",
                        "value": output,
                    },
                }
                if receipt.accepted and receipt.record_id is not None:
                    state_value["parent_record_id"] = receipt.record_id
                state = self._sink.record(state_value)
                _record_generator_reasoning_correlation_gap(
                    self._sink,
                    dict(trace),
                    affected=state,
                    component=_component_name(native_data),
                )
                _record_unavailable_reasoning_gap(
                    self._sink,
                    dict(trace),
                    framework="haystack",
                    affected=state,
                    count=unavailable,
                    detail=(
                        "Haystack exposed provider reasoning metadata that is not "
                        "readable reasoning text; the metadata remains native evidence."
                    ),
                )
            if component_marker is not None:
                self._source._components_with_exact_model_response.discard(component_marker)
        if (
            normalized.get("kind") == "model"
            and phase != "start"
            and isinstance(trace, Mapping)
        ):
            _record_unavailable_reasoning_gap(
                self._sink,
                dict(trace),
                framework="haystack",
                affected=receipt,
                count=_otel_unavailable_reasoning(native_data.get("output")),
                detail=(
                    "The GenAI OpenTelemetry output exposed encrypted, redacted, "
                    "or signature-only reasoning. It remains in the complete native "
                    "snapshot but is unavailable as readable reasoning."
                ),
            )
        return receipt

    def __getattr__(self, name: str) -> Any:
        return getattr(self._sink, name)


_MISSING = object()


def _component_output(native: dict[str, Any]) -> Any:
    attributes = native.get("attributes")
    if not isinstance(attributes, Mapping):
        return _MISSING
    output = attributes.get("haystack.component.output", _MISSING)
    if not isinstance(output, str):
        return output
    try:
        return json.loads(output)
    except json.JSONDecodeError:
        return output


def _haystack_generator_reasoning_evidence(
    native: dict[str, Any],
) -> tuple[Any, int] | None:
    attributes = native.get("attributes")
    if not isinstance(attributes, Mapping):
        return None
    qualified_type = attributes.get("haystack.component.fully_qualified_type")
    if not (
        isinstance(qualified_type, str)
        and qualified_type.startswith("haystack.components.generators.")
    ):
        return None
    output = _component_output(native)
    replies = output.get("replies") if isinstance(output, Mapping) else None
    if not isinstance(replies, list):
        return None

    readable = False
    unavailable = 0
    for reply in replies:
        if not isinstance(reply, Mapping):
            continue
        parts = reply.get("content")
        if not isinstance(parts, list):
            continue
        for part in parts:
            reasoning_part = part.get("reasoning") if isinstance(part, Mapping) else None
            if not isinstance(reasoning_part, Mapping):
                continue
            text = reasoning_part.get("reasoning_text")
            if isinstance(text, str) and text:
                readable = True
            extra = reasoning_part.get("extra")
            if isinstance(extra, Mapping):
                protected = (
                    extra.get("encrypted_content"),
                    extra.get("signature"),
                    extra.get("redacted_content", extra.get("redacted")),
                )
                if any(value not in (None, "", False, [], {}) for value in protected):
                    unavailable += 1

    if not readable and unavailable == 0:
        return None
    return output, unavailable


def _record_generator_reasoning_correlation_gap(
    sink: Any,
    trace: dict[str, str],
    *,
    affected: Any,
    component: str,
) -> None:
    value: dict[str, Any] = {
        "kind": "unknown",
        "phase": "gap",
        "name": "haystack.generator.reasoning.correlation.gap",
        "trace": trace,
        "native": {
            "component": component,
            "reason": "haystack_generator_reasoning_model_correlation_unavailable",
        },
        "semantic": {
            "type": "capture.gap",
            "framework": "haystack",
            "reason": "haystack_generator_reasoning_model_correlation_unavailable",
            "count": 1,
            "detail": (
                "Haystack content tracing retained generator reasoning output, but "
                "did not expose an exact GenAI model request and response identity."
            ),
        },
    }
    if affected.accepted and affected.record_id is not None:
        value["parent_record_id"] = affected.record_id
    sink.record(value)


def _model_semantic(native: Any, phase: Any) -> dict[str, Any]:
    if type(native) is not dict:
        return {"framework": "haystack", "route": "otel"}
    if phase == "start":
        model = native.get("model")
        return {
            "type": "model.request",
            "framework": "haystack",
            "route": "otel",
            "context_refs": [],
            **({"model": model} if type(model) is str and model else {}),
        }
    usage = _usage(native.get("usage"))
    output = native.get("output", _MISSING)
    reasoning = _genai_output_reasoning(output)
    return {
        "type": "model.response",
        "framework": "haystack",
        "route": "otel",
        "status": "failed" if phase == "error" else "completed",
        **(
            {"content": _genai_visible_output(output)}
            if output is not _MISSING and output is not None
            else {}
        ),
        **({"reasoning": reasoning} if reasoning else {}),
        **({"usage": usage} if usage else {}),
    }


def _otel_unavailable_reasoning(output: Any) -> int:
    if not isinstance(output, list):
        return 0
    count = 0
    for message in output:
        parts = message.get("parts") if isinstance(message, dict) else None
        if not isinstance(parts, list):
            continue
        for part in parts:
            if not isinstance(part, dict) or part.get("type") != "reasoning":
                continue
            text = part.get("content")
            encrypted = part.get("encrypted_content")
            redacted = part.get("redacted_content", part.get("redacted"))
            signature = part.get("signature")
            if encrypted not in (None, "", b"") or redacted not in (None, "", b"", False):
                count += 1
            elif not (isinstance(text, str) and text) and signature not in (None, "", b""):
                count += 1
    return count


def _tool_semantic(native: Any, phase: Any) -> dict[str, Any]:
    if type(native) is not dict:
        return {"framework": "haystack", "route": "otel"}
    call_id = native.get("call_id")
    name = native.get("name")
    if not (type(call_id) is str and call_id and type(name) is str and name):
        return {"framework": "haystack", "route": "otel"}
    if phase == "start":
        return {
            "type": "tool.execution",
            "framework": "haystack",
            "route": "otel",
            "call_id": call_id,
            "name": name,
            "input": native.get("input"),
        }
    error = native.get("status") if phase == "error" else None
    return {
        "type": "tool.error" if phase == "error" else "tool.result",
        "framework": "haystack",
        "route": "otel",
        "call_id": call_id,
        "status": "failed" if phase == "error" else "succeeded",
        **({"output": native.get("output")} if "output" in native else {}),
        **({"error": _semantic_error(error)} if phase == "error" else {}),
    }


def _pipeline_semantic(native: dict[str, Any]) -> dict[str, Any]:
    attributes = native.get("attributes")
    if type(attributes) is not dict:
        return {}
    result: dict[str, Any] = {}
    for native_name, semantic_name in (
        ("haystack.pipeline.input_data", "input"),
        ("haystack.pipeline.output_data", "output"),
    ):
        value = attributes.get(native_name)
        if type(value) is str:
            try:
                result[semantic_name] = json.loads(value)
            except json.JSONDecodeError:
                result[semantic_name] = value
    return result


def _record_pipeline_output_gap(
    sink: Any,
    pipeline_record: dict[str, Any],
    affected: Any,
) -> None:
    value: dict[str, Any] = {
        "kind": "unknown",
        "phase": "gap",
        "name": "haystack.pipeline.output.gap",
        "trace": pipeline_record.get("trace"),
        "native": {
            "reason": "pipeline_output_not_captured",
            "route": "otel",
        },
        "semantic": {
            "type": "capture.gap",
            "reason": "pipeline_output_not_captured",
            "detail": (
                "Haystack OpenTelemetry exposed only the unchanged start-time "
                "pipeline output placeholder."
            ),
        },
    }
    if affected.accepted and affected.record_id is not None:
        value["parent_record_id"] = affected.record_id
    sink.record(value)


def _component_name(native: dict[str, Any]) -> str:
    attributes = native.get("attributes")
    name = attributes.get("haystack.component.name") if type(attributes) is dict else None
    return name if type(name) is str and name else "haystack.component.run"


def _usage(value: Any) -> dict[str, int]:
    if type(value) is not dict:
        return {}
    return {
        name: count
        for name in ("input_tokens", "output_tokens")
        if type(count := value.get(name)) is int and count >= 0
    }


def _span_failed(native: dict[str, Any]) -> bool:
    status = native.get("status")
    code = status.get("code") if type(status) is dict else None
    return code in {2, "ERROR"} or str(code).endswith("ERROR")


def _pipeline_error_snapshot(native: dict[str, Any]) -> dict[str, Any]:
    attributes = native.get("attributes")
    return {
        "type": attributes.get("error.type") if type(attributes) is dict else None,
        "status": native.get("status"),
        "events": native.get("events"),
    }


def _semantic_error(value: Any) -> dict[str, Any]:
    native_type = value.get("type") if type(value) is dict else None
    status = value.get("status") if type(value) is dict else None
    message = None
    events = value.get("events") if type(value) is dict else None
    for event in events if isinstance(events, list) else []:
        if not isinstance(event, dict) or event.get("name") != "exception":
            continue
        attributes = event.get("attributes")
        if not isinstance(attributes, dict):
            continue
        if not isinstance(native_type, str) or not native_type:
            native_type = attributes.get("exception.type")
        message = attributes.get("exception.message")
        break
    if not isinstance(message, str) or not message:
        message = status.get("description") if type(status) is dict else None
    normalized_type = (
        re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", native_type).lower()
        if type(native_type) is str and native_type
        else "observed_error"
    )
    normalized_type = "".join(
        character if character.isalnum() or character in "._-" else "_"
        for character in normalized_type
    ).strip("._-")
    if len(normalized_type) < 3 or not normalized_type[0].isalpha():
        normalized_type = "observed_error"
    return {
        "type": normalized_type[:127],
        "message": (
            message[:4096]
            if type(message) is str and message
            else "OpenTelemetry span reported an error"
        ),
        "recoverable": False,
    }


def haystack_otel_adapter(*, version: str | None = None) -> OpenTelemetrySource:
    """Create processors callers register before starting Haystack pipelines.

    OpenTelemetry providers do not expose reversible processor removal.  This source
    therefore never mutates a provider or Haystack's process-global tracer.  Register
    ``span_processor`` and ``log_record_processor`` on caller-owned providers, install
    this source into Semantic Layer, and configure Haystack tracing before execution.
    """

    return _trust_official_source(
        _HaystackOpenTelemetrySource(
            _installed_version("haystack-ai", version),
            seam="OpenTelemetry TracerProvider/span processor",
        ),
        "otel",
    )  # type: ignore[return-value]
