"""Official langgraph adapter capture adapter."""

from __future__ import annotations

import asyncio
import hashlib
import json
from concurrent.futures import CancelledError as FutureCancelledError
from typing import Any

from pydantic import BaseModel

from ._adapter_native import MAX_SNAPSHOT_DEPTH, MAX_SNAPSHOT_NODES, native_snapshot
from ._framework_adapter_shared import (
    _installed_version,
    _Lifecycle,
    _OpenSpan,
    _OpenTrace,
    _provider_error_native,
    _raw_native,
    _record_unavailable_reasoning_gap,
    _source_qualification,
)
from .capture_v1 import CaptureSource, _trust_official_source


class _LangGraphStateLimitError(Exception):
    pass


class _LangGraphStateBudget:
    def __init__(self, *, max_depth: int = MAX_SNAPSHOT_DEPTH) -> None:
        self.nodes = 0
        self.active: set[int] = set()
        self.max_depth = max_depth

    def enter(self, value: Any, depth: int) -> int | None:
        self.nodes += 1
        if self.nodes > MAX_SNAPSHOT_NODES or depth > self.max_depth:
            raise _LangGraphStateLimitError
        if not _langgraph_recursive_container(value):
            return None
        identity = id(value)
        if identity in self.active:
            raise _LangGraphStateLimitError
        self.active.add(identity)
        return identity

    def leave(self, identity: int | None) -> None:
        if identity is not None:
            self.active.discard(identity)


def _child_semantic_type(kind: str, phase: str) -> str:
    semantic_types = {
        "model": {
            "start": "model.request",
            "end": "model.response",
            "error": "model.error",
        },
        "tool": {
            "start": "tool.execution",
            "end": "tool.result",
            "error": "tool.error",
        },
    }
    return semantic_types[kind][phase]


class _LangGraphAdapter:
    def __init__(self, version: str) -> None:
        self.version = version

    def create_source(self, client: object) -> _LangGraphSource:
        return _trust_official_source(_LangGraphSource(client, self.version), "deep")  # type: ignore[return-value]


class _LangGraphSource(CaptureSource):
    def __init__(self, graph: object, version: str) -> None:
        self.graph = graph
        self.metadata = {
            "name": "official:langgraph-python",
            "seam": "invoke/ainvoke/stream/astream wrappers + RunnableConfig.callbacks",
            "identity_domain": "langgraph.run",
            "version": version,
            "qualification": _source_qualification(
                version,
                exact_versions=frozenset({"1.2.9"}),
                profile="langgraph-python-adapter-v1",
            ),
            "official": True,
            "coverage": [{"operation": "graph-run", "domain": "langgraph.run", "role": "owner"}],
        }

    def install(self, sink: Any) -> _Lifecycle:
        handler = _LangGraphCallbackHandler(sink)
        restores: list[Any] = []
        turn_indexes: dict[str, int] = {}
        implicit_runs = [0]
        for method_name in (
            "invoke",
            "ainvoke",
            "stream",
            "astream",
            "stream_events",
            "astream_events",
        ):
            had_own_method = method_name in vars(self.graph)
            own_method = vars(self.graph).get(method_name)
            original = getattr(self.graph, method_name, None)
            if not callable(original):
                continue

            def wrapper(
                *args: Any,
                __original: Any = original,
                **kwargs: Any,
            ) -> Any:
                positional = list(args)
                config = kwargs.get("config")
                positional_config = len(positional) > 1
                if config is None and positional_config:
                    config = positional[1]
                enriched = _langgraph_config(config, handler, turn_indexes, implicit_runs)
                if positional_config:
                    positional[1] = enriched
                else:
                    kwargs["config"] = enriched
                return __original(*positional, **kwargs)

            try:
                setattr(self.graph, method_name, wrapper)
            except BaseException:
                handler.active = False
                for restore in reversed(restores):
                    restore()
                raise

            def restore(
                name: str = method_name,
                replacement: Any = wrapper,
                prior_was_own: bool = had_own_method,
                prior_own: Any = own_method,
            ) -> None:
                if getattr(self.graph, name, None) is replacement:
                    if prior_was_own:
                        setattr(self.graph, name, prior_own)
                    else:
                        delattr(self.graph, name)

            restores.append(restore)

        def deactivate() -> None:
            handler.active = False
            for restore in reversed(restores):
                restore()
            restores.clear()

        return _Lifecycle(deactivate)


class _LangGraphCallbackHandler:
    raise_error = False
    run_inline = True
    ignore_chain = False
    ignore_llm = False
    ignore_chat_model = False
    ignore_retriever = False
    ignore_custom_event = False
    ignore_agent = False

    def __init__(self, sink: Any) -> None:
        self.sink = sink
        self.active = True
        self.traces: dict[str, _OpenTrace] = {}
        self.runs: dict[str, _OpenSpan] = {}
        self.streamed_runs: set[str] = set()
        self.tool_proposals: dict[str, dict[str, Any]] = {}
        self.state_messages: dict[str, dict[tuple[str, str], tuple[Any, bool, str]]] = {}
        self.context_records: dict[
            str,
            dict[tuple[str, str], tuple[Any, bool, str, str]],
        ] = {}
        self.state_run_ids: dict[str, str] = {}
        self.state_fingerprints: dict[str, set[tuple[str, str]]] = {}
        self.state_compaction_losses: set[str] = set()
        self.paused_traces: set[str] = set()
        self.recorded_errors: dict[str, dict[int, BaseException]] = {}

    def on_chain_start(
        self,
        serialized: dict[str, Any] | None,
        inputs: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        tags: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        name: str | None = None,
        **kwargs: Any,
    ) -> None:
        if not self.active:
            return
        run_key = str(run_id)
        parent = self.runs.get(str(parent_run_id)) if parent_run_id is not None else None
        root_chain = parent_run_id is None
        if root_chain:
            trace = self._open_root(run_key, name or "langgraph.run", metadata or {}, inputs)
        else:
            if parent is None:
                return
            trace = parent.trace
        if trace is None:
            return
        state_run_id = (
            None
            if root_chain
            else _langgraph_node_state_id(
                metadata or {},
                tags or [],
                name,
            )
        )
        if state_run_id is not None:
            self.state_run_ids[run_key] = state_run_id
        start_event: dict[str, Any] = {
            "kind": "state" if root_chain else "lifecycle",
            "phase": "start",
            "name": (
                "langgraph.state.transition"
                if root_chain
                else f"langgraph.step.{name or 'chain'}"
            ),
            "trace": trace,
            "native_identity": run_key,
            "native": {
                "event": "on_chain_start",
                "serialized": serialized,
                "inputs": (
                    _langgraph_state_native_snapshot(inputs)
                    if root_chain or state_run_id is not None
                    else {"omitted": "nested runnable input"}
                ),
                "tags": tags or [],
                "metadata": metadata or {},
                "name": name,
                "kwargs": kwargs,
            },
            "semantic": {
                "type": "capture.redundant" if root_chain else "workflow.step",
                "framework": "langgraph",
                **(
                    {}
                    if root_chain
                    else {
                        "scope_type": "step",
                        "scope_id": f"langgraph.step.{run_key}",
                        "name": name or "chain",
                    }
                ),
            },
        }
        if parent is not None and parent.start.accepted and parent.start.record_id is not None:
            start_event["parent_record_id"] = parent.start.record_id
        start = self.sink.record(start_event)
        self.runs[run_key] = _OpenSpan(
            trace,
            "langgraph.state.transition" if root_chain else f"langgraph.step.{name or 'chain'}",
            start,
        )

    def on_chain_end(
        self,
        outputs: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        if not self.active:
            return
        run_key = str(run_id)
        opened = self.runs.get(run_key)
        if opened is None:
            return
        state_run_id = self.state_run_ids.get(run_key)
        root_chain = parent_run_id is None
        value: dict[str, Any] = {
            "kind": "state" if root_chain else "lifecycle",
            "phase": "end",
            "name": opened.name,
            "trace": opened.trace,
            "native_identity": run_key,
            "native": {
                "event": "on_chain_end",
                "outputs": (
                    _langgraph_state_native_snapshot(outputs)
                    if root_chain or state_run_id is not None
                    else {"omitted": "nested runnable output"}
                ),
                "kwargs": _raw_native(kwargs),
            },
            "semantic": {
                "type": (
                    "capture.redundant"
                    if root_chain
                    else "workflow.step"
                ),
                "framework": "langgraph",
                **(
                    {}
                    if root_chain
                    else {
                        "scope_type": "step",
                        "status": "succeeded",
                    }
                ),
            },
        }
        if opened.start.accepted and opened.start.record_id is not None:
            value["parent_record_id"] = opened.start.record_id
        trace_id = opened.trace["trace_id"]
        paused_root = root_chain and trace_id in self.paused_traces
        seen_messages = self.state_messages.setdefault(trace_id, {})
        context_records = self.context_records.setdefault(trace_id, {})
        compaction_failed = False
        try:
            compacted = (
                None
                if paused_root
                else _record_and_compact_langgraph_final_state(
                    self.sink,
                    opened.trace,
                    outputs,
                    context_records,
                    self.tool_proposals.get(trace_id),
                )
                if root_chain
                else _record_and_compact_langgraph_state(
                    self.sink,
                    opened.trace,
                    outputs,
                    seen_messages,
                    context_records,
                    self.tool_proposals.get(trace_id),
                )
                if state_run_id is not None
                else None
            )
        except BaseException:
            compacted = None
            compaction_failed = True
        state_fingerprint = (
            _langgraph_value_fingerprint(compacted)
            if compacted is not None
            else None
        )
        state_identity = run_key if root_chain else state_run_id
        seen_state = self.state_fingerprints.setdefault(trace_id, set())
        state_observation = (
            (state_identity, state_fingerprint)
            if state_identity is not None and state_fingerprint is not None
            else None
        )
        repeated_state = (
            state_observation is not None
            and state_observation in seen_state
        )
        if compacted is not None and not repeated_state:
            if state_observation is not None:
                seen_state.add(state_observation)
            self.sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": (
                        "langgraph.run.final_state"
                        if root_chain
                        else "langgraph.step.output"
                    ),
                    "trace": opened.trace,
                    "native_identity": f"{state_identity}:state",
                    "native": {"outputs": _langgraph_state_native_snapshot(outputs)},
                    "semantic": {
                        "type": "state.transition",
                        "framework": "langgraph",
                        "state_type": (
                            "langgraph.final_state"
                            if root_chain
                            else "langgraph.step.output"
                        ),
                        **(
                            {"version": state_identity}
                            if state_identity is not None
                            else {}
                        ),
                        "value": compacted,
                    },
                    **(
                        {"parent_record_id": opened.start.record_id}
                        if opened.start.accepted
                        and opened.start.record_id is not None
                        else {}
                    ),
                }
            )
        if compaction_failed and trace_id not in self.state_compaction_losses:
            self.state_compaction_losses.add(trace_id)
            gap: dict[str, Any] = {
                "kind": "unknown",
                "phase": "gap",
                "name": "langgraph.state.compaction.gap",
                "trace": opened.trace,
                "native_identity": f"{run_key}:state-compaction",
                "native": {
                    "event": "on_chain_end",
                    "scope": "root" if root_chain else "step",
                },
                "semantic": {
                    "type": "capture.gap",
                    "framework": "langgraph",
                    "reason": "state_compaction_unavailable",
                    "count": 1,
                    "detail": (
                        "State could not be compacted safely within the "
                        "48-depth/20000-node traversal policy; lifecycle "
                        "and application outcome remain available."
                    ),
                },
            }
            if opened.start.accepted and opened.start.record_id is not None:
                gap["parent_record_id"] = opened.start.record_id
            self.sink.record(gap)
        self.sink.record(value)
        self.runs.pop(run_key, None)
        self.state_run_ids.pop(run_key, None)
        if root_chain:
            self.tool_proposals.pop(trace_id, None)
            self.state_messages.pop(trace_id, None)
            self.context_records.pop(trace_id, None)
            self.state_fingerprints.pop(trace_id, None)
            self.state_compaction_losses.discard(trace_id)
            root = self.traces.pop(run_key, None)
            if root is not None:
                self.sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": "end",
                        "name": root.name,
                        "trace": root.identity,
                        "native_identity": run_key,
                        "native": {"outputs": _langgraph_state_native_snapshot(outputs)},
                        "semantic": {
                            "type": "agent.run",
                            "framework": "langgraph",
                            "status": "unknown" if paused_root else "succeeded",
                            **(
                                {"summary": "LangGraph run paused by GraphInterrupt."}
                                if paused_root
                                else {}
                            ),
                        },
                    }
                )
            self.paused_traces.discard(trace_id)
            self.recorded_errors.pop(trace_id, None)

    def on_chain_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        if not self.active:
            return
        run_key = str(run_id)
        opened = self.runs.pop(run_key, None)
        if opened is None:
            return
        self.state_run_ids.pop(run_key, None)
        trace_id = opened.trace["trace_id"]
        if _is_langgraph_interrupt(error):
            self.paused_traces.add(trace_id)
            pause: dict[str, Any] = {
                "kind": "state",
                "phase": "event",
                "name": "langgraph.interrupt.pause",
                "trace": opened.trace,
                "native_identity": run_key,
                "native": {
                    "event": "on_chain_error",
                    "interrupt": _langgraph_interrupt_value(error),
                    "kwargs": _raw_native(kwargs),
                },
                "semantic": {
                    "type": "state.interrupt",
                    "framework": "langgraph",
                    "state_type": "langgraph.pause",
                    "value": _langgraph_interrupt_value(error),
                },
            }
            if opened.start.accepted and opened.start.record_id is not None:
                pause["parent_record_id"] = opened.start.record_id
            self.sink.record(pause)
            if parent_run_id is not None:
                scope_end: dict[str, Any] = {
                    "kind": "lifecycle",
                    "phase": "end",
                    "name": opened.name,
                    "trace": opened.trace,
                    "native_identity": run_key,
                    "native": {"interrupt": _langgraph_interrupt_value(error)},
                    "semantic": {
                        "type": "workflow.step",
                        "framework": "langgraph",
                        "scope_type": "step",
                        "status": "interrupted",
                    },
                }
                if opened.start.accepted and opened.start.record_id is not None:
                    scope_end["parent_record_id"] = opened.start.record_id
                self.sink.record(scope_end)
            return
        if _is_langgraph_cancellation(error):
            cancelled: dict[str, Any] = {
                "kind": "state" if parent_run_id is None else "lifecycle",
                "phase": "cancelled",
                "name": opened.name,
                "trace": opened.trace,
                "native_identity": run_key,
                "error_identity": error,
                "native": {
                    "event": "on_chain_error",
                    "cancelled": True,
                    "kwargs": _raw_native(kwargs),
                },
                "semantic": {
                    "type": (
                        "capture.redundant"
                        if parent_run_id is None
                        else "workflow.step"
                    ),
                    "framework": "langgraph",
                    **(
                        {}
                        if parent_run_id is None
                        else {"scope_type": "step", "status": "cancelled"}
                    ),
                },
            }
            if opened.start.accepted and opened.start.record_id is not None:
                cancelled["parent_record_id"] = opened.start.record_id
            self.sink.record(cancelled)
            if parent_run_id is None:
                self._finish_cancelled_root(run_key, opened, error)
            return
        value: dict[str, Any] = {
            "kind": "state",
            "phase": "error",
            "name": opened.name,
            "trace": opened.trace,
            "native_identity": run_key,
            "error_identity": error,
            "native": {
                "event": "on_chain_error",
                **_provider_error_native(error),
                "kwargs": _raw_native(kwargs),
            },
            "semantic": {
                "type": "capture.redundant",
                "framework": "langgraph",
            },
        }
        if opened.start.accepted and opened.start.record_id is not None:
            value["parent_record_id"] = opened.start.record_id
        self.sink.record(value)
        self._record_distinct_error(
            opened,
            error,
            name="langgraph.error",
            native_identity=run_key,
        )
        if parent_run_id is None:
            self.tool_proposals.pop(trace_id, None)
            self.state_messages.pop(trace_id, None)
            self.context_records.pop(trace_id, None)
            self.state_fingerprints.pop(trace_id, None)
            self.state_compaction_losses.discard(trace_id)
            self.paused_traces.discard(trace_id)
            self.recorded_errors.pop(trace_id, None)
            root = self.traces.pop(run_key, None)
            if root is not None:
                self.sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": "error",
                        "name": root.name,
                        "trace": root.identity,
                        "native_identity": run_key,
                        "error_identity": error,
                        "native": _provider_error_native(error),
                        "semantic": {
                            "type": "agent.run",
                            "framework": "langgraph",
                            "status": "failed",
                            "error": _semantic_error(error),
                        },
                    }
                )

    def _finish_cancelled_root(
        self,
        run_key: str,
        opened: _OpenSpan,
        error: BaseException,
    ) -> None:
        trace_id = opened.trace["trace_id"]
        self.tool_proposals.pop(trace_id, None)
        self.state_messages.pop(trace_id, None)
        self.context_records.pop(trace_id, None)
        self.state_fingerprints.pop(trace_id, None)
        self.state_compaction_losses.discard(trace_id)
        self.paused_traces.discard(trace_id)
        self.recorded_errors.pop(trace_id, None)
        root = self.traces.pop(run_key, None)
        if root is not None:
            self.sink.record(
                {
                    "kind": "lifecycle",
                    "phase": "cancelled",
                    "name": root.name,
                    "trace": root.identity,
                    "native_identity": run_key,
                    "error_identity": error,
                    "native": {
                        "cancelled": True,
                        "error_type": type(error).__name__,
                    },
                    "semantic": {
                        "type": "agent.run",
                        "framework": "langgraph",
                        "status": "cancelled",
                    },
                }
            )

    def _record_distinct_error(
        self,
        opened: _OpenSpan,
        error: BaseException,
        *,
        name: str,
        native_identity: str | None = None,
    ) -> None:
        trace_id = opened.trace["trace_id"]
        retained_errors = self.recorded_errors.setdefault(trace_id, {})
        known_error = retained_errors.get(id(error))
        if known_error is error:
            return
        retained_errors[id(error)] = error
        self.sink.record(
            {
                "kind": "error",
                "phase": "event",
                "name": name,
                "trace": opened.trace,
                "error_identity": error,
                **(
                    {"native_identity": native_identity}
                    if native_identity is not None
                    else {}
                ),
                "native": _provider_error_native(error),
                "semantic": {
                    "type": "error",
                    "framework": "langgraph",
                    "error": _semantic_error(error),
                },
            }
        )

    def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._start_child(
            "model",
            "langgraph.model.call",
            serialized,
            messages,
            run_id,
            parent_run_id,
            kwargs,
        )

    def on_llm_start(
        self,
        serialized: dict[str, Any],
        prompts: list[str],
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._start_child(
            "model",
            "langgraph.model.call",
            serialized,
            prompts,
            run_id,
            parent_run_id,
            kwargs,
        )

    def on_llm_new_token(
        self,
        token: str,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        chunk: Any = None,
        **kwargs: Any,
    ) -> None:
        opened = self.runs.get(str(run_id))
        if self.active and opened is not None:
            self.streamed_runs.add(str(run_id))
            self.sink.record(
                {
                    "kind": "stream",
                    "phase": "event",
                    "name": "langgraph.model.delta",
                    "trace": opened.trace,
                    "native_identity": str(run_id),
                    "native": {
                        "token": token,
                        "chunk": _raw_native(chunk),
                        "kwargs": _raw_native(kwargs),
                    },
                    "semantic": {
                        "type": "capture.redundant",
                        "framework": "langgraph",
                    },
                }
            )

    def on_llm_end(
        self,
        response: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._end_child("model", run_id, response, None, kwargs)

    def on_llm_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._end_child("model", run_id, None, error, kwargs)

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        inputs: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self._start_child(
            "tool",
            "langgraph.tool.call",
            serialized,
            inputs if inputs is not None else input_str,
            run_id,
            parent_run_id,
            kwargs,
        )

    def on_tool_end(
        self,
        output: Any,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._end_child("tool", run_id, output, None, kwargs)

    def on_tool_error(
        self,
        error: BaseException,
        *,
        run_id: Any,
        parent_run_id: Any = None,
        **kwargs: Any,
    ) -> None:
        self._end_child("tool", run_id, None, error, kwargs)

    def _start_child(
        self,
        kind: str,
        name: str,
        serialized: dict[str, Any],
        inputs: Any,
        run_id: Any,
        parent_run_id: Any,
        kwargs: dict[str, Any],
    ) -> None:
        if not self.active:
            return
        parent = self.runs.get(str(parent_run_id))
        if parent is None:
            return
        key = str(run_id)
        operation_name = (
            serialized.get("name")
            if isinstance(serialized.get("name"), str)
            and serialized["name"].strip()
            else None
        )
        operation_id = (
            kwargs.get("tool_call_id")
            if kind == "tool"
            and isinstance(kwargs.get("tool_call_id"), str)
            and kwargs["tool_call_id"].strip()
            else key
            if kind == "tool"
            else None
        )
        semantic: dict[str, Any] = {
            "type": _child_semantic_type(kind, "start"),
            "framework": "langgraph",
            **(
                {
                    "model": _langgraph_model_name(operation_name, kwargs),
                    "context_refs": _record_langgraph_context(
                        self.sink,
                        parent.trace,
                        inputs,
                        self.context_records.setdefault(
                            parent.trace["trace_id"],
                            {},
                        ),
                        self.tool_proposals.get(parent.trace["trace_id"]),
                    ),
                    **_langgraph_model_request_configuration(kwargs),
                }
                if kind == "model"
                else {}
            ),
            **(
                {
                    "name": operation_name,
                    "input": _raw_native(inputs),
                    "native_call_id": operation_id,
                }
                if operation_name is not None
                else {}
            ),
        }
        if operation_id is not None:
            self.tool_proposals.get(parent.trace["trace_id"], {}).pop(
                operation_id,
                None,
            )
        start_value: dict[str, Any] = {
            "kind": kind,
            "phase": "start",
            "name": name,
            "trace": parent.trace,
            "native_identity": key,
            "native": {
                "serialized": _raw_native(serialized),
                "inputs": _raw_native(inputs),
                "kwargs": _raw_native(
                    _langgraph_model_callback_kwargs(kwargs) if kind == "model" else kwargs
                ),
            },
            "semantic": semantic,
        }
        receipt = self.sink.record(start_value)
        self.runs[key] = _OpenSpan(parent.trace, name, receipt, operation_name, operation_id)
        if kind == "model" and _langgraph_response_format_gap(kwargs):
            gap: dict[str, Any] = {
                "kind": "unknown",
                "phase": "gap",
                "name": "langgraph.model.response_format.gap",
                "trace": parent.trace,
                "native": {"event": "on_chat_model_start"},
                "semantic": {
                    "type": "capture.gap",
                    "framework": "langgraph",
                    "reason": "response_format_not_observed",
                    "count": 1,
                    "detail": (
                        "LangGraph exposed an opaque response_format value without "
                        "a JSON schema or serializable contract."
                    ),
                },
            }
            if receipt.accepted and receipt.record_id is not None:
                gap["parent_record_id"] = receipt.record_id
            self.sink.record(gap)

    def _end_child(
        self,
        kind: str,
        run_id: Any,
        output: Any,
        error: BaseException | None,
        kwargs: dict[str, Any],
    ) -> None:
        if not self.active:
            return
        key = str(run_id)
        opened = self.runs.pop(key, None)
        if opened is None:
            return
        was_streamed = key in self.streamed_runs
        self.streamed_runs.discard(key)
        value: dict[str, Any] = {
            "kind": kind,
            "phase": "error" if error is not None else "end",
            "name": opened.name,
            "trace": opened.trace,
            "native_identity": key,
            **({"error_identity": error} if isinstance(error, BaseException) else {}),
            "native": {
                "output": _raw_native(output),
                **(_provider_error_native(error) if error is not None else {"error": None}),
                "kwargs": _raw_native(kwargs),
            },
            "semantic": {
                "type": (
                    "model.response"
                    if kind == "model"
                    else _child_semantic_type(
                        kind, "error" if error is not None else "end"
                    )
                ),
                "framework": "langgraph",
                **(
                    {
                        "model": opened.operation_name,
                        "status": "failed" if error is not None else "completed",
                        **(
                            {"content": content}
                            if (content := _langgraph_model_content(output)) is not None
                            else {}
                        ),
                        **(
                            {"reasoning": reasoning}
                            if (reasoning := _langgraph_model_reasoning(output))
                            else {}
                        ),
                        **(
                            {"usage": usage}
                            if (usage := _langgraph_usage(output)) is not None
                            else {}
                        ),
                    }
                    if kind == "model"
                    else {}
                ),
                **(
                    {
                        "name": opened.operation_name,
                        "native_call_id": opened.operation_id,
                        "status": "failed" if error is not None else "succeeded",
                        **({"output": _raw_native(output)} if error is None else {}),
                        **(
                            {"error": _semantic_error(error)}
                            if error is not None
                            else {}
                        ),
                    }
                    if kind == "tool" and opened.operation_name is not None
                    else {}
                ),
            },
        }
        if opened.start.accepted and opened.start.record_id is not None:
            value["parent_record_id"] = opened.start.record_id
        ended = self.sink.record(value)
        if kind == "model":
            _record_unavailable_reasoning_gap(
                self.sink,
                opened.trace,
                framework="langgraph",
                affected=ended,
                count=_langgraph_unavailable_reasoning(output),
                detail=(
                    "LangGraph exposed an encrypted, redacted, or signature-only "
                    "reasoning block; opaque bytes were omitted."
                ),
            )
        if ended.accepted and ended.record_id is not None:
            context_records = self.context_records.setdefault(
                opened.trace["trace_id"],
                {},
            )
            if kind == "model":
                for message in _langgraph_output_messages(output):
                    _remember_langgraph_context(
                        context_records,
                        message,
                        ended.record_id,
                    )
            elif kind == "tool":
                _remember_langgraph_context(
                    context_records,
                    output,
                    ended.record_id,
                )
        if kind == "tool":
            self.sink.record(
                {
                    "kind": "tool",
                    "phase": "event",
                    "name": "langgraph.tool.result",
                    "trace": opened.trace,
                    "native_identity": key,
                    "native": {
                        "output": _raw_native(output),
                        **(_provider_error_native(error) if error is not None else {"error": None}),
                    },
                    "semantic": {
                        "type": "capture.redundant",
                        "framework": "langgraph",
                    },
                }
            )
        elif kind == "model" and output is not None:
            for proposal in _langgraph_tool_proposals(output):
                proposal_value: dict[str, Any] = {
                    "kind": "tool",
                    "phase": "event",
                    "name": "langgraph.tool.proposal",
                    "trace": opened.trace,
                    "native_identity": proposal["id"],
                    "native": {"proposal": _raw_native(proposal["raw"])},
                    "semantic": {
                        "type": "tool.proposal",
                        "framework": "langgraph",
                        "name": proposal["name"],
                        "input": _raw_native(proposal["input"]),
                        "native_call_id": proposal["id"],
                    },
                }
                if ended.accepted and ended.record_id is not None:
                    proposal_value["parent_record_id"] = ended.record_id
                proposal_receipt = self.sink.record(proposal_value)
                self.tool_proposals.setdefault(
                    opened.trace["trace_id"],
                    {},
                )[proposal["id"]] = proposal_receipt
        if kind == "model" and output is not None and was_streamed:
            self.sink.record(
                {
                    "kind": "stream",
                    "phase": "event",
                    "name": "langgraph.model.stream.terminal",
                    "trace": opened.trace,
                    "native_identity": key,
                    "native": {"output": _raw_native(output)},
                    "semantic": {
                        "type": "capture.redundant",
                        "framework": "langgraph",
                    },
                }
            )
            usage = _langgraph_usage(output)
            if usage is not None:
                self.sink.record(
                    {
                        "kind": "model",
                        "phase": "event",
                        "name": "langgraph.usage",
                        "trace": opened.trace,
                        "native_identity": key,
                        "native": {"usage": _raw_native(usage)},
                        "semantic": {
                            "type": "capture.redundant",
                            "framework": "langgraph",
                        },
                    }
                )
        if error is not None:
            self._record_distinct_error(
                opened,
                error,
                name=f"{opened.name}.error",
                native_identity=key,
            )

    def _open_root(
        self, run_id: str, name: str, metadata: dict[str, Any], inputs: Any
    ) -> dict[str, str] | None:
        opened = self.sink.open_trace(
            {
                "name": f"langgraph.{name}",
                "native_identity": run_id,
                "conversation_id": metadata.get("semantic_layer_conversation_id"),
                "turn_id": metadata.get("semantic_layer_turn_id"),
                "turn_index": metadata.get("semantic_layer_turn_index"),
                "previous_turn_id": metadata.get("semantic_layer_previous_turn_id"),
                "native": {
                    "inputs": _langgraph_state_native_snapshot(inputs),
                    "metadata": _raw_native(metadata),
                },
                "semantic": {
                    "type": "agent.run",
                    "framework": "langgraph",
                    "name": f"langgraph.{name}",
                    "input": _langgraph_state_native_snapshot(inputs),
                },
            }
        )
        if not opened.accepted or opened.identity is None:
            return None
        identity = dict(opened.identity)
        self.traces[run_id] = _OpenTrace(identity, f"langgraph.{name}")
        return identity


def langgraph_adapter(*, version: str | None = None) -> _LangGraphAdapter:
    return _LangGraphAdapter(_installed_version("langgraph", version))


def _record_and_compact_langgraph_state(
    sink: Any,
    trace: dict[str, str],
    value: Any,
    seen_messages: dict[tuple[str, str], tuple[Any, bool, str]],
    context_records: dict[tuple[str, str], tuple[Any, bool, str, str]],
    tool_proposals: dict[str, Any] | None,
) -> Any:
    _record_langgraph_messages(
        sink,
        trace,
        _langgraph_state_messages(value),
        context_records,
        tool_proposals,
    )
    return _compact_langgraph_state(value, seen_messages, context_records)


def _record_and_compact_langgraph_final_state(
    sink: Any,
    trace: dict[str, str],
    value: Any,
    context_records: dict[tuple[str, str], tuple[Any, bool, str, str]],
    tool_proposals: dict[str, Any] | None,
) -> Any:
    _record_langgraph_messages(
        sink,
        trace,
        _langgraph_state_messages(value),
        context_records,
        tool_proposals,
    )
    return _compact_langgraph_final_state(value, context_records)


def _langgraph_exact_sequence(value: Any) -> list[Any] | None:
    if type(value) is list:
        return [
            list.__getitem__(value, index)
            for index in range(list.__len__(value))
        ]
    if type(value) is tuple:
        return [
            tuple.__getitem__(value, index)
            for index in range(tuple.__len__(value))
        ]
    return None


def _compact_langgraph_state(
    value: Any,
    seen_messages: dict[tuple[str, str], tuple[Any, bool, str]],
    context_records: (
        dict[tuple[str, str], tuple[Any, bool, str, str]] | None
    ) = None,
    budget: _LangGraphStateBudget | None = None,
    depth: int = 0,
) -> Any:
    traversal = budget or _LangGraphStateBudget()
    identity = traversal.enter(value, depth)
    try:
        structured = _langgraph_state_container(value)
        if structured is not None:
            value = structured
        sequence = _langgraph_exact_sequence(value)
        if sequence is not None:
            compacted_items = [
                item
                for item in (
                    _compact_langgraph_state(
                        item,
                        seen_messages,
                        context_records,
                        traversal,
                        depth + 1,
                    )
                    for item in sequence
                )
                if item is not None
            ]
            return compacted_items or None
        if type(value) is not dict:
            return _langgraph_state_json(value, traversal, depth + 1)
        compacted_fields: dict[str, Any] = {}
        for key, item in dict.items(value):
            if not isinstance(key, str) or key == "lc_serializable":
                continue
            messages = _langgraph_message_state_values(key, item)
            if messages is not None:
                retained = []
                for message in messages:
                    if _langgraph_context_reference(context_records, message) is not None:
                        continue
                    compacted_message = _compact_langgraph_message(
                        message,
                        traversal,
                        depth + 1,
                    )
                    message_identity = _langgraph_message_identity(message)
                    fingerprint = _langgraph_value_fingerprint(compacted_message)
                    if message_identity is not None and fingerprint is not None:
                        key_identity, authoritative = message_identity
                        previous = seen_messages.get(key_identity)
                        if previous is not None and previous[2] == fingerprint and (
                            authoritative or previous[0] is message
                        ):
                            continue
                        seen_messages[key_identity] = (
                            message,
                            authoritative,
                            fingerprint,
                        )
                    retained.append(compacted_message)
                if retained:
                    compacted_fields[key] = (
                        retained
                        if type(item) in {list, tuple}
                        else retained[0]
                    )
                continue
            compacted = (
                _compact_langgraph_state(
                    item,
                    seen_messages,
                    context_records,
                    traversal,
                    depth + 1,
                )
                if type(item) in {dict, list, tuple}
                else _langgraph_state_json(item, traversal, depth + 1)
            )
            if compacted is not None or type(item) not in {dict, list, tuple}:
                compacted_fields[key] = compacted
        return compacted_fields or None
    finally:
        traversal.leave(identity)


def _compact_langgraph_final_state(
    value: Any,
    context_records: dict[tuple[str, str], tuple[Any, bool, str, str]],
    budget: _LangGraphStateBudget | None = None,
    depth: int = 0,
) -> Any:
    traversal = budget or _LangGraphStateBudget()
    identity = traversal.enter(value, depth)
    try:
        structured = _langgraph_state_container(value)
        if structured is not None:
            value = structured
        if type(value) is not dict:
            return _langgraph_state_json(value, traversal, depth + 1)
        compacted_fields: dict[str, Any] = {}
        for key, item in dict.items(value):
            if not isinstance(key, str) or key == "lc_serializable":
                continue
            messages = _langgraph_message_state_values(key, item)
            if messages is None:
                compacted = (
                    _compact_langgraph_final_state(
                        item,
                        context_records,
                        traversal,
                        depth + 1,
                    )
                    if type(item) is dict
                    else _langgraph_state_json(item, traversal, depth + 1)
                )
                if compacted is not None or type(item) is not dict:
                    compacted_fields[key] = compacted
                continue
            unknown = [
                _compact_langgraph_message(message, traversal, depth + 1)
                for message in messages
                if _langgraph_context_reference(context_records, message) is None
            ]
            summary: dict[str, Any] = {"count": len(messages)}
            if unknown:
                summary["unreferenced"] = unknown
            compacted_fields[key] = summary
        return compacted_fields or None
    finally:
        traversal.leave(identity)


def _langgraph_state_messages(
    value: Any,
    budget: _LangGraphStateBudget | None = None,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    traversal = budget or _LangGraphStateBudget()

    def visit(item: Any, depth: int) -> None:
        identity = traversal.enter(item, depth)
        try:
            structured = _langgraph_state_container(item)
            if structured is not None:
                item = structured
            if type(item) is dict:
                for key, child in dict.items(item):
                    candidates = (
                        _langgraph_message_state_values(key, child)
                        if isinstance(key, str)
                        else None
                    )
                    if candidates is None:
                        visit(child, depth + 1)
                        continue
                    for candidate in candidates:
                        message = _langgraph_context_message(candidate)
                        if message is not None:
                            messages.append(message)
            elif (sequence := _langgraph_exact_sequence(item)) is not None:
                for child in sequence:
                    visit(child, depth + 1)
        finally:
            traversal.leave(identity)

    visit(value, 0)
    return messages


def _langgraph_message_state_values(key: str, value: Any) -> list[Any] | None:
    if key != "messages" and not key.endswith("_messages"):
        return None
    candidates = _langgraph_exact_sequence(value)
    if candidates is None:
        candidates = [value]
    if key == "messages":
        return (
            candidates
            if all(_langgraph_supported_state_message(item) for item in candidates)
            else None
        )
    return (
        candidates
        if candidates and all(_is_langchain_base_message(item) for item in candidates)
        else None
    )


def _langgraph_supported_state_message(value: Any) -> bool:
    if _is_langchain_base_message(value):
        return True
    if type(value) is not dict:
        return False
    role = dict.get(value, "type") or dict.get(value, "role")
    return (
        role in {"human", "user", "ai", "assistant", "system", "tool"}
        and dict.__contains__(value, "content")
    )


def _is_langchain_base_message(value: Any) -> bool:
    from langchain_core.messages import BaseMessage

    return isinstance(value, BaseMessage)


def _langgraph_state_container(value: Any) -> dict[str, Any] | None:
    from langgraph.types import Command

    if type(value) is Command:
        return {
            key: object.__getattribute__(value, key)
            for key in ("graph", "update", "resume", "goto")
        }
    if isinstance(value, BaseModel):
        try:
            fields = object.__getattribute__(value, "__dict__")
        except BaseException:
            return None
        return fields if type(fields) is dict else None
    return None


def _langgraph_recursive_container(value: Any) -> bool:
    return (
        type(value) in {dict, list, tuple}
        or isinstance(value, BaseModel)
        or _langgraph_state_container(value) is not None
    )


def _compact_langgraph_message(
    value: Any,
    budget: _LangGraphStateBudget | None = None,
    depth: int = 0,
) -> Any:
    traversal = budget or _LangGraphStateBudget()
    identity = traversal.enter(value, depth)
    try:
        return _compact_langgraph_message_value(value, traversal, depth)
    finally:
        traversal.leave(identity)


def _compact_langgraph_message_value(
    value: Any,
    budget: _LangGraphStateBudget,
    depth: int,
) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    message = {
        key: item
        for key in (
            "type",
            "role",
            "content",
            "name",
            "tool_call_id",
            "tool_calls",
            "additional_kwargs",
            "response_metadata",
            "artifact",
        )
        if (item := _langgraph_message_field(value, key)) is not None
    }
    return (
        {
            key: _langgraph_state_json(item, budget, depth + 1)
            for key, item in message.items()
        }
        if message
        else {"type": "unknown_message"}
    )


def _langgraph_message_field(value: Any, name: str) -> Any:
    if type(value) is dict:
        return dict.get(value, name)
    try:
        fields = object.__getattribute__(value, "__dict__")
    except BaseException:
        return None
    return dict.get(fields, name) if type(fields) is dict else None


def _langgraph_node_state_id(
    metadata: dict[str, Any],
    tags: list[str],
    name: str | None,
) -> str | None:
    """Return the official graph-node checkpoint identity, if this is a node run."""

    node = metadata.get("langgraph_node")
    step = metadata.get("langgraph_step")
    if (
        not isinstance(node, str)
        or not node
        or name != node
        or not any(tag.startswith("graph:step:") for tag in tags)
    ):
        return None
    checkpoint_namespace = metadata.get("langgraph_checkpoint_ns")
    if isinstance(checkpoint_namespace, str) and checkpoint_namespace:
        return checkpoint_namespace
    if isinstance(step, int) and not isinstance(step, bool) and step >= 0:
        return f"{node}:{step}"
    return None


def _langgraph_state_json(
    value: Any,
    budget: _LangGraphStateBudget | None = None,
    depth: int = 0,
) -> Any:
    """Normalize official structured state without walking arbitrary objects."""

    traversal = budget or _LangGraphStateBudget()
    identity = traversal.enter(value, depth)
    try:
        if isinstance(value, (dict, list, tuple)) and type(value) not in {
            dict,
            list,
            tuple,
        }:
            raise _LangGraphStateLimitError
        if isinstance(value, BaseModel):
            try:
                fields = object.__getattribute__(value, "__dict__")
            except BaseException:
                return {"type": type(value).__name__}
            return _langgraph_state_json(fields, traversal, depth + 1)
        if type(value) is dict:
            return {
                key: _langgraph_state_json(item, traversal, depth + 1)
                for key, item in dict.items(value)
                if isinstance(key, str)
            }
        sequence = _langgraph_exact_sequence(value)
        if sequence is not None:
            return [
                _langgraph_state_json(item, traversal, depth + 1)
                for item in sequence
            ]
        return value
    finally:
        traversal.leave(identity)


def _langgraph_state_native_snapshot(value: Any) -> Any:
    """Leave enough depth for the surrounding capture-event envelope."""

    try:
        projected = _langgraph_state_json(
            value,
            _LangGraphStateBudget(max_depth=MAX_SNAPSHOT_DEPTH - 8),
        )
    except BaseException:
        return {
            "native_type": "opaque_state",
            "omitted": "state_traversal_limit",
        }
    return native_snapshot(projected)


def _langgraph_value_fingerprint(value: Any) -> str | None:
    try:
        canonical = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, RecursionError):
        return None
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _langgraph_message_identity(
    value: Any,
) -> tuple[tuple[str, str], bool] | None:
    message_id = _langgraph_message_field(value, "id")
    if isinstance(message_id, str) and message_id:
        return (("message_id", message_id), True)
    tool_call_id = _langgraph_message_field(value, "tool_call_id")
    if isinstance(tool_call_id, str) and tool_call_id:
        return (("tool_call_id", tool_call_id), True)
    if type(value) in {dict, list, tuple} or _is_langchain_base_message(value):
        return (("object", str(id(value))), False)
    return None


def _langgraph_config(
    config: Any,
    handler: _LangGraphCallbackHandler,
    turn_indexes: dict[str, int],
    implicit_runs: list[int],
) -> dict[str, Any]:
    result = dict(config or {})
    metadata = dict(result.get("metadata") or {})
    if metadata.get("semantic_layer_capture_installed"):
        return result
    configurable = dict(result.get("configurable") or {})
    requested_conversation = configurable.get("thread_id") or configurable.get("conversation_id")
    if requested_conversation is None or requested_conversation == "":
        conversation = f"langgraph-implicit:{implicit_runs[0]}"
        implicit_runs[0] += 1
        index = 0
        explicit_conversation = False
    else:
        conversation = str(requested_conversation)
        index = turn_indexes.get(conversation, -1) + 1
        explicit_conversation = True
    metadata.update(
        {
            "semantic_layer_capture_installed": True,
            "semantic_layer_conversation_id": conversation,
            "semantic_layer_turn_id": f"{conversation}:{index}",
            "semantic_layer_turn_index": index,
            **(
                {"semantic_layer_previous_turn_id": f"{conversation}:{index - 1}"}
                if index > 0
                else {}
            ),
        }
    )
    callbacks = result.get("callbacks")
    if callbacks is None:
        result["callbacks"] = [handler]
    elif isinstance(callbacks, list):
        result["callbacks"] = [*callbacks, handler]
    elif callable(getattr(callbacks, "copy", None)):
        copied_callbacks = callbacks.copy()
        add_handler = getattr(copied_callbacks, "add_handler", None)
        if not callable(add_handler):
            raise TypeError("LangGraph callback manager copy must expose add_handler")
        add_handler(handler, True)
        result["callbacks"] = copied_callbacks
    else:
        raise TypeError("LangGraph callbacks must be a list or callback manager")
    result["metadata"] = metadata
    if explicit_conversation:
        turn_indexes[conversation] = index
    return result


def _langgraph_model_name(
    operation_name: str | None,
    kwargs: dict[str, Any],
) -> str | None:
    invocation = dict.get(kwargs, "invocation_params")
    if type(invocation) is dict:
        for key in ("model", "model_name", "model_id"):
            value = dict.get(invocation, key)
            if isinstance(value, str) and value:
                return value
    return operation_name


def _langgraph_model_request_configuration(
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    invocation = dict.get(kwargs, "invocation_params")
    if type(invocation) is not dict:
        return {}
    raw_definitions = dict.get(invocation, "tools")
    raw_definition_values = _langgraph_exact_sequence(raw_definitions)
    definitions = (
        [_langgraph_state_json(item) for item in raw_definition_values]
        if raw_definition_values is not None
        and all(type(item) is dict for item in raw_definition_values)
        else []
    )
    names: list[str] = []
    for definition in definitions:
        if type(definition) is not dict:
            continue
        function = dict.get(definition, "function")
        candidate = (
            dict.get(function, "name")
            if type(function) is dict
            else dict.get(definition, "name")
        )
        if isinstance(candidate, str) and candidate and candidate not in names:
            names.append(candidate)
    settings = {
        key: _langgraph_state_json(dict.__getitem__(invocation, key))
        for key in (
            "temperature",
            "top_p",
            "max_tokens",
            "max_completion_tokens",
            "tool_choice",
            "stop",
        )
        if dict.__contains__(invocation, key)
    }
    response_format = _langgraph_response_format_contract(
        dict.get(invocation, "response_format")
    )
    if response_format is not None:
        settings["response_format"] = response_format
    return {
        **({"tools": names} if names else {}),
        **({"tool_definitions": definitions} if definitions else {}),
        **({"settings": settings} if settings else {}),
    }


def _langgraph_model_callback_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    projected = dict.copy(kwargs)
    invocation = dict.get(kwargs, "invocation_params")
    if type(invocation) is not dict or not dict.__contains__(
        invocation,
        "response_format",
    ):
        return projected
    projected_invocation = dict.copy(invocation)
    response_format = _langgraph_response_format_contract(
        dict.get(invocation, "response_format")
    )
    if response_format is None:
        projected_invocation.pop("response_format", None)
    else:
        projected_invocation["response_format"] = response_format
    projected["invocation_params"] = projected_invocation
    return projected


def _langgraph_response_format_gap(kwargs: dict[str, Any]) -> bool:
    invocation = dict.get(kwargs, "invocation_params")
    return (
        type(invocation) is dict
        and dict.__contains__(invocation, "response_format")
        and _langgraph_response_format_contract(
            dict.get(invocation, "response_format")
        )
        is None
    )


def _langgraph_response_format_contract(value: Any) -> dict[str, Any] | None:
    try:
        if type(value) is not dict:
            return None
        projected = value
        projected = _langgraph_state_json(projected)
        json.dumps(projected, ensure_ascii=False)
    except BaseException:
        return None
    return projected if type(projected) is dict else None


def _langgraph_usage(output: Any) -> Any:
    llm_output = getattr(output, "llm_output", None)
    if isinstance(llm_output, dict):
        usage = llm_output.get("token_usage") or llm_output.get("usage")
        if usage is not None:
            return usage
    for group in getattr(output, "generations", None) or []:
        for generation in group:
            message = getattr(generation, "message", None)
            usage = getattr(message, "usage_metadata", None)
            if usage is not None:
                return usage
    return None


def _record_langgraph_context(
    sink: Any,
    trace: dict[str, str],
    inputs: Any,
    context_records: dict[tuple[str, str], tuple[Any, bool, str, str]],
    tool_proposals: dict[str, Any] | None = None,
) -> list[str]:
    return _record_langgraph_messages(
        sink,
        trace,
        _langgraph_context_messages(inputs),
        context_records,
        tool_proposals,
    )


def _record_langgraph_messages(
    sink: Any,
    trace: dict[str, str],
    messages: list[dict[str, Any]],
    context_records: dict[tuple[str, str], tuple[Any, bool, str, str]],
    tool_proposals: dict[str, Any] | None = None,
) -> list[str]:
    references: list[str] = []
    for message in messages:
        existing = _langgraph_context_reference(
            context_records,
            message["native"],
        )
        if existing is not None:
            references.append(existing)
            continue
        receipt = sink.record(
            {
                "kind": "state",
                "phase": "event",
                "name": "langgraph.model.context",
                "trace": trace,
                "native": _raw_native(message["native"]),
                "semantic": {
                    "type": "message",
                    "framework": "langgraph",
                    "role": message["role"],
                    "content": _raw_native(message["content"]),
                    **(
                        {"name": message["name"]}
                        if message.get("name") is not None
                        else {}
                    ),
                    **(
                        {"call_id": message["call_id"]}
                        if message.get("call_id") is not None
                        else {}
                    ),
                },
            }
        )
        if receipt.accepted and receipt.record_id is not None:
            references.append(receipt.record_id)
            _remember_langgraph_context(
                context_records,
                message["native"],
                receipt.record_id,
            )
            call_id = message.get("call_id")
            proposal = (
                tool_proposals.pop(call_id, None)
                if message["role"] == "tool"
                and isinstance(call_id, str)
                and call_id
                and tool_proposals is not None
                else None
            )
            if proposal is not None:
                gap: dict[str, Any] = {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "langgraph.tool.execution.gap",
                    "trace": trace,
                    "native": {
                        "event": "langgraph.state.tool_message",
                        "tool_call_id": call_id,
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "framework": "langgraph",
                        "reason": "tool_execution_not_observed",
                        "count": 1,
                        "detail": (
                            "LangGraph exposed a tool-role result with the exact "
                            "proposal call ID, but no official tool execution "
                            "callback was observed."
                        ),
                    },
                }
                if proposal.accepted and proposal.record_id is not None:
                    gap["parent_record_id"] = proposal.record_id
                sink.record(gap)
    return references


def _remember_langgraph_context(
    context_records: dict[tuple[str, str], tuple[Any, bool, str, str]],
    value: Any,
    record_id: str,
) -> None:
    identity = _langgraph_message_identity(value)
    fingerprint = _langgraph_value_fingerprint(_compact_langgraph_message(value))
    if identity is None or fingerprint is None:
        return
    key, authoritative = identity
    context_records[key] = (
        value,
        authoritative,
        fingerprint,
        record_id,
    )


def _langgraph_context_reference(
    context_records: (
        dict[tuple[str, str], tuple[Any, bool, str, str]] | None
    ),
    value: Any,
) -> str | None:
    if context_records is None:
        return None
    identity = _langgraph_message_identity(value)
    fingerprint = _langgraph_value_fingerprint(_compact_langgraph_message(value))
    if identity is None or fingerprint is None:
        return None
    key, authoritative = identity
    retained = context_records.get(key)
    if (
        retained is None
        or retained[2] != fingerprint
        or (not authoritative and retained[0] is not value)
    ):
        return None
    return retained[3]


def _langgraph_context_messages(inputs: Any) -> list[dict[str, Any]]:
    outer = _langgraph_exact_sequence(inputs)
    if outer is None:
        return []
    nested = _langgraph_exact_sequence(outer[0]) if len(outer) == 1 else None
    if nested is not None:
        values = nested
    elif len(outer) == 1 and isinstance(outer[0], str):
        return [{"role": "user", "content": outer[0], "native": outer[0]}]
    else:
        return []
    messages: list[dict[str, Any]] = []
    roles = {
        "human": "user",
        "user": "user",
        "ai": "assistant",
        "assistant": "assistant",
        "system": "system",
        "tool": "tool",
    }
    for value in values:
        message = _langgraph_context_message(value, roles)
        if message is None:
            return []
        messages.append(message)
    return messages


def _langgraph_context_message(
    value: Any,
    roles: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    role_map = roles or {
        "human": "user",
        "user": "user",
        "ai": "assistant",
        "assistant": "assistant",
        "system": "system",
        "tool": "tool",
    }
    native_role = _langgraph_message_field(
        value,
        "type",
    ) or _langgraph_message_field(value, "role")
    role = role_map.get(native_role) if isinstance(native_role, str) else None
    content = _langgraph_message_field(value, "content")
    if role is None or content is None:
        return None

    return {
        "role": role,
        "content": content,
        "native": value,
        "name": _langgraph_message_field(value, "name"),
        "call_id": _langgraph_message_field(value, "tool_call_id"),
    }


def _langgraph_output_messages(output: Any) -> list[Any]:
    messages: list[Any] = []
    for group in getattr(output, "generations", None) or []:
        for generation in group:
            message = getattr(generation, "message", None)
            if message is not None:
                messages.append(message)
    return messages


def _langgraph_model_content(output: Any) -> Any:
    values: list[Any] = []
    for group in getattr(output, "generations", None) or []:
        for generation in group:
            message = getattr(generation, "message", None)
            content = getattr(message, "content", None)
            if content is None:
                content = getattr(generation, "text", None)
            if content is not None:
                values.append(_langgraph_visible_content(content))
    if len(values) == 1:
        return values[0]
    return values or None


def _langgraph_visible_content(content: Any) -> Any:
    if not isinstance(content, (list, tuple)):
        return _raw_native(content)
    visible: list[Any] = []
    for block in content:
        block_type = (
            block.get("type")
            if isinstance(block, dict)
            else getattr(block, "type", None)
        )
        if block_type in {
            "reasoning",
            "thinking",
            "reasoning_summary",
            "summary",
            "redacted_reasoning",
            "redacted_thinking",
        }:
            continue
        visible.append(_raw_native(block))
    return visible


def _langgraph_model_reasoning(output: Any) -> list[dict[str, str]]:
    blocks: list[dict[str, str]] = []

    def field(value: Any, *names: str) -> Any:
        for name in names:
            if isinstance(value, dict) and name in value:
                return value[name]
            candidate = getattr(value, name, None)
            if candidate is not None:
                return candidate
        return None

    def add(kind: str, text: Any) -> None:
        if kind not in {"text", "summary"} or not isinstance(text, str) or not text:
            return
        blocks.append({"type": kind, "text": text})

    def read_blocks(values: Any) -> list[dict[str, str]]:
        retained: list[dict[str, str]] = []
        for block in values if isinstance(values, (list, tuple)) else []:
            block_type = field(block, "type")
            text = None
            kind = "text"
            if block_type in {"reasoning_summary", "summary"}:
                kind = "summary"
                text = field(block, "text", "summary")
            elif block_type in {"reasoning", "thinking"}:
                text = field(block, "text", "reasoning", "thinking")
            if isinstance(text, str) and text:
                retained.append({"type": kind, "text": text})
        return retained

    for group in field(output, "generations") or []:
        for generation in group:
            message = field(generation, "message")
            additional = field(message, "additional_kwargs", "additionalKwargs")
            content_reasoning = read_blocks(field(message, "content"))
            if content_reasoning:
                blocks.extend(content_reasoning)
                continue
            content_blocks = field(message, "content_blocks", "contentBlocks")
            normalized = read_blocks(content_blocks)
            if normalized:
                blocks.extend(normalized)
                continue
            add("text", field(additional, "reasoning_content", "reasoning"))
    return blocks


def _langgraph_unavailable_reasoning(output: Any) -> int:
    def field(value: Any, *names: str) -> Any:
        for name in names:
            if isinstance(value, dict) and name in value:
                return value[name]
            candidate = getattr(value, name, None)
            if candidate is not None:
                return candidate
        return None

    def unavailable(values: Any) -> int:
        count = 0
        for block in values if isinstance(values, (list, tuple)) else []:
            block_type = field(block, "type")
            if block_type in {"redacted_reasoning", "redacted_thinking"}:
                count += 1
                continue
            if block_type not in {"reasoning", "thinking", "reasoning_summary", "summary"}:
                continue
            text = field(block, "text", "reasoning", "thinking", "summary")
            extras = field(block, "extras")
            marker = next(
                (
                    field(block, name)
                    for name in ("encrypted_content", "redacted_content", "signature")
                    if field(block, name) not in (None, "", b"")
                ),
                None,
            )
            if marker is None and isinstance(extras, dict):
                marker = next(
                    (
                        extras.get(name)
                        for name in ("encrypted_content", "redacted_content", "signature")
                        if extras.get(name) not in (None, "", b"")
                    ),
                    None,
                )
            if not (isinstance(text, str) and text) and marker is not None:
                count += 1
        return count

    count = 0
    for group in field(output, "generations") or []:
        for generation in group:
            message = field(generation, "message")
            content_blocks = field(message, "content_blocks", "contentBlocks")
            if isinstance(content_blocks, (list, tuple)):
                count += unavailable(content_blocks)
                continue
            content = field(message, "content")
            if isinstance(content, (list, tuple)):
                count += unavailable(content)
                continue
            additional = field(message, "additional_kwargs", "additionalKwargs")
            reasoning = field(additional, "reasoning_content", "reasoning")
            if isinstance(reasoning, dict):
                count += unavailable([{"type": "reasoning", **reasoning}])
    return count


def _semantic_error(error: BaseException) -> dict[str, Any]:
    args = object.__getattribute__(error, "args")
    message = (
        args[0]
        if args and isinstance(args[0], str) and args[0]
        else "LangGraph callback failed"
    )
    return {
        "type": "langgraph.error",
        "message": message,
        "recoverable": False,
    }


def _is_langgraph_cancellation(error: BaseException) -> bool:
    return isinstance(
        error,
        (GeneratorExit, asyncio.CancelledError, FutureCancelledError),
    )


def _is_langgraph_interrupt(error: BaseException) -> bool:
    from langgraph.errors import GraphInterrupt

    return isinstance(error, GraphInterrupt)


def _langgraph_interrupt_value(error: BaseException) -> Any:
    from langgraph.types import Interrupt

    args = object.__getattribute__(error, "args")
    interrupts = args[0] if args and isinstance(args[0], tuple) else ()
    return {
        "status": "paused",
        "interrupts": [
            {
                "id": value.id,
                "value": _langgraph_state_native_snapshot(value.value),
            }
            if isinstance(value, Interrupt)
            else _langgraph_state_native_snapshot(value)
            for value in interrupts
        ],
    }


def _langgraph_tool_proposals(output: Any) -> list[dict[str, Any]]:
    generations = getattr(output, "generations", None)
    if not isinstance(generations, (list, tuple)):
        return []
    proposals: list[dict[str, Any]] = []
    for group in generations:
        if not isinstance(group, (list, tuple)):
            continue
        for generation in group:
            message = getattr(generation, "message", None)
            tool_calls = getattr(message, "tool_calls", None)
            if not isinstance(tool_calls, (list, tuple)):
                continue
            for call in tool_calls:
                if not isinstance(call, dict):
                    continue
                call_id = call.get("id")
                name = call.get("name")
                if (
                    isinstance(call_id, str)
                    and call_id.strip()
                    and isinstance(name, str)
                    and name.strip()
                ):
                    proposals.append(
                        {
                            "id": call_id,
                            "name": name,
                            "input": call.get("args"),
                            "raw": call,
                        }
                    )
    return proposals
