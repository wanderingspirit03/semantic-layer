"""Official CrewAI event-bus capture adapter."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from ._adapter_native import native_field, native_snapshot
from ._framework_adapter_shared import _installed_version, _source_qualification
from .capture_v1 import AdmissionReceipt, CaptureSource, _trust_official_source


@dataclass
class _OpenTurn:
    trace: dict[str, str]
    turn_id: str
    root: AdmissionReceipt


@dataclass
class _StreamedToolCall:
    index: int
    name: str | None = None
    arguments: str = ""
    tool_call_id: str | None = None
    response_id: str | None = None

    def native(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "id": self.tool_call_id,
            "type": "function",
            "function": {"name": self.name, "arguments": self.arguments},
        }

    def matches(self, name: Any, arguments: Any) -> bool:
        if not isinstance(name, str) or self.name != name:
            return False
        try:
            streamed_arguments: Any = json.loads(self.arguments)
        except (json.JSONDecodeError, TypeError):
            return False
        if isinstance(arguments, str):
            try:
                arguments = json.loads(arguments)
            except json.JSONDecodeError:
                return False
        return bool(streamed_arguments == arguments)


class _Lifecycle:
    def __init__(self, deactivate: Any, drain: Any) -> None:
        self._deactivate = deactivate
        self._drain = drain

    def deactivate(self) -> None:
        self._deactivate()

    def drain(self) -> None:
        self._drain()


class _CrewAIAdapter:
    def __init__(self, version: str) -> None:
        self.version = version

    def create_source(self, client: object) -> _CrewAISource:
        return _trust_official_source(_CrewAISource(client, self.version), "deep")  # type: ignore[return-value]


class _CrewAISource(CaptureSource):
    def __init__(self, event_bus: object, version: str) -> None:
        self.event_bus = event_bus
        self.metadata = {
            "name": "official:crewai",
            "seam": "BaseEventListener/event bus",
            "identity_domain": "crewai.execution",
            "version": version,
            "qualification": _source_qualification(
                version,
                exact_versions=frozenset({"1.15.2"}),
                profile="crewai-python-adapter-v1",
            ),
            "official": True,
            "coverage": [
                {"operation": "crew-execution", "domain": "crewai.execution", "role": "owner"}
            ],
        }
        self._sink: Any = None
        self._active = False
        self._lock = threading.RLock()
        self._settled = threading.Condition(self._lock)
        self._inflight = 0
        self._turns: dict[str, _OpenTurn] = {}
        self._agent_roots: set[str] = set()
        self._turn_order: dict[str, int] = {}
        self._last_turn: dict[str, str] = {}
        self._failed_turns: set[str] = set()
        self._models: dict[str, AdmissionReceipt] = {}
        self._tools: dict[str, AdmissionReceipt] = {}
        self._handoffs: dict[str, AdmissionReceipt] = {}
        self._event_turn: dict[str, _OpenTurn] = {}
        self._model_turn: dict[str, _OpenTurn] = {}
        self._tool_turn: dict[str, _OpenTurn] = {}
        self._streaming_models: set[str] = set()
        self._streamed_models: set[str] = set()
        self._streamed_tool_calls: dict[str, dict[int, _StreamedToolCall]] = {}
        self._thinking_chunks: dict[str, list[str]] = {}

    def install(self, sink: Any) -> _Lifecycle:
        if self._active:
            raise RuntimeError("CrewAI source is already installed")
        from crewai.events import BaseEventListener  # type: ignore[import-not-found,unused-ignore]
        from crewai.events.types.a2a_events import (  # type: ignore[import-not-found,unused-ignore]
            A2ADelegationCompletedEvent,
            A2ADelegationStartedEvent,
        )
        from crewai.events.types.agent_events import (  # type: ignore[import-not-found,unused-ignore]
            AgentExecutionCompletedEvent,
            AgentExecutionErrorEvent,
            AgentExecutionStartedEvent,
            LiteAgentExecutionCompletedEvent,
            LiteAgentExecutionErrorEvent,
            LiteAgentExecutionStartedEvent,
        )
        from crewai.events.types.crew_events import (  # type: ignore[import-not-found,unused-ignore]
            CrewKickoffCompletedEvent,
            CrewKickoffFailedEvent,
            CrewKickoffStartedEvent,
        )
        from crewai.events.types.llm_events import (  # type: ignore[import-not-found,unused-ignore]
            LLMCallCompletedEvent,
            LLMCallFailedEvent,
            LLMCallStartedEvent,
            LLMStreamChunkEvent,
            LLMThinkingChunkEvent,
        )
        from crewai.events.types.memory_events import (  # type: ignore[import-not-found,unused-ignore]
            MemoryQueryCompletedEvent,
            MemoryQueryFailedEvent,
            MemoryQueryStartedEvent,
            MemoryRetrievalCompletedEvent,
            MemoryRetrievalFailedEvent,
            MemoryRetrievalStartedEvent,
            MemorySaveCompletedEvent,
            MemorySaveFailedEvent,
            MemorySaveStartedEvent,
        )
        from crewai.events.types.task_events import (  # type: ignore[import-not-found,unused-ignore]
            TaskCompletedEvent,
            TaskFailedEvent,
            TaskStartedEvent,
        )
        from crewai.events.types.tool_usage_events import (  # type: ignore[import-not-found,unused-ignore]
            ToolUsageErrorEvent,
            ToolUsageFinishedEvent,
            ToolUsageStartedEvent,
        )

        source = self
        registrations: list[tuple[type[Any], Any]] = []
        event_types = (
            CrewKickoffStartedEvent,
            CrewKickoffCompletedEvent,
            CrewKickoffFailedEvent,
            AgentExecutionStartedEvent,
            AgentExecutionCompletedEvent,
            AgentExecutionErrorEvent,
            LiteAgentExecutionStartedEvent,
            LiteAgentExecutionCompletedEvent,
            LiteAgentExecutionErrorEvent,
            A2ADelegationStartedEvent,
            A2ADelegationCompletedEvent,
            LLMCallStartedEvent,
            LLMStreamChunkEvent,
            LLMThinkingChunkEvent,
            LLMCallCompletedEvent,
            LLMCallFailedEvent,
            ToolUsageStartedEvent,
            ToolUsageFinishedEvent,
            ToolUsageErrorEvent,
            TaskStartedEvent,
            TaskCompletedEvent,
            TaskFailedEvent,
            MemoryQueryStartedEvent,
            MemoryQueryCompletedEvent,
            MemoryQueryFailedEvent,
            MemorySaveStartedEvent,
            MemorySaveCompletedEvent,
            MemorySaveFailedEvent,
            MemoryRetrievalStartedEvent,
            MemoryRetrievalCompletedEvent,
            MemoryRetrievalFailedEvent,
        )

        off = getattr(self.event_bus, "off", None)
        if not callable(off):
            raise TypeError("CrewAI event bus requires off")

        class Listener(BaseEventListener):  # type: ignore[misc,unused-ignore]
            def setup_listeners(self, crewai_event_bus: Any) -> None:
                for event_type in event_types:

                    async def handler(_emitter: Any, event: Any) -> None:
                        with source._settled:
                            source._inflight += 1
                        try:
                            source._on_event(event)
                        finally:
                            with source._settled:
                                source._inflight -= 1
                                source._settled.notify_all()

                    crewai_event_bus.on(event_type)(handler)
                    registrations.append((event_type, handler))

        self._sink = sink
        self._active = True
        try:
            Listener()
        except BaseException:
            self._active = False
            for event_type, handler in reversed(registrations):
                off(event_type, handler)
            raise

        def deactivate() -> None:
            with self._lock:
                for event_type, handler in reversed(registrations):
                    off(event_type, handler)
                registrations.clear()

        def drain() -> None:
            flush = getattr(self.event_bus, "flush", None)
            if callable(flush):
                flush()
            with self._settled:
                while self._inflight:
                    self._settled.wait(timeout=0.05)
                self._active = False

        return _Lifecycle(deactivate, drain)

    def _on_event(self, event: Any) -> None:
        with self._lock:
            if not self._active:
                return
            event_type = str(getattr(event, "type", ""))
            if event_type == "crew_kickoff_started":
                self._start_turn(event)
            elif event_type == "crew_kickoff_completed":
                self._finish_turn(event, failed=False)
            elif event_type == "crew_kickoff_failed":
                self._finish_turn(event, failed=True)
            elif event_type == "agent_execution_started":
                self._start_agent_execution(event)
            elif event_type == "agent_execution_completed":
                self._finish_agent_execution(event, failed=False)
            elif event_type == "agent_execution_error":
                self._finish_agent_execution(event, failed=True)
            elif event_type == "lite_agent_execution_started":
                self._start_agent_execution(event)
            elif event_type == "lite_agent_execution_completed":
                self._finish_agent_execution(event, failed=False)
            elif event_type == "lite_agent_execution_error":
                self._finish_agent_execution(event, failed=True)
            elif event_type == "a2a_delegation_started":
                self._start_handoff(event)
            elif event_type == "a2a_delegation_completed":
                self._finish_handoff(event)
            elif event_type == "llm_call_started":
                self._start_model(event)
            elif event_type == "llm_stream_chunk":
                self._stream_delta(event)
            elif event_type == "llm_thinking_chunk":
                self._thinking_delta(event)
            elif event_type == "llm_call_completed":
                self._finish_model(event)
            elif event_type == "llm_call_failed":
                self._fail_model(event)
            elif event_type == "tool_usage_started":
                self._start_tool(event)
            elif event_type == "tool_usage_finished":
                self._finish_tool(event, failed=False)
            elif event_type == "tool_usage_error":
                self._finish_tool(event, failed=True)
            elif event_type.startswith("task_"):
                self._record_framework_state(event, family="task")
            elif event_type.startswith("memory_"):
                self._record_framework_state(event, family="memory")

    def _turn_for(self, event: Any, *, identity: str | None = None) -> _OpenTurn | None:
        if identity is not None:
            turn = self._model_turn.get(identity) or self._tool_turn.get(identity)
            if turn is not None:
                return turn
        for name in (
            "started_event_id",
            "parent_event_id",
            "triggered_by_event_id",
            "previous_event_id",
            "event_id",
        ):
            value = getattr(event, name, None)
            if isinstance(value, str) and value in self._event_turn:
                return self._event_turn[value]
        return next(iter(self._turns.values())) if len(self._turns) == 1 else None

    def _start_turn(self, event: Any) -> None:
        crew = str(getattr(event, "crew_name", None) or "crewai")
        turn_id = str(getattr(event, "event_id", None) or f"{crew}-turn")
        previous = self._last_turn.get(crew)
        turn_index = self._turn_order.get(crew, 0)
        opened = self._sink.open_trace(
            {
                "name": "crewai.turn",
                "native_identity": turn_id,
                "native": native_snapshot(event),
                "semantic": {
                    "type": "agent.run",
                    "framework": "crewai",
                    "name": "crewai.turn",
                    "input": native_snapshot(_field(event, "inputs", None)),
                },
                "conversation_id": crew,
                "turn_id": turn_id,
                "turn_index": turn_index,
                **({"previous_turn_id": previous} if previous else {}),
            }
        )
        if not opened.accepted or opened.identity is None:
            return
        self._turn_order[crew] = turn_index + 1
        turn = _OpenTurn(opened.identity, turn_id, opened)
        self._turns[turn_id] = turn
        self._event_turn[turn_id] = turn
        self._sink.record(
            {
                "kind": "state",
                "phase": "start",
                "name": "crewai.state.transition",
                "trace": opened.identity,
                "native_identity": turn_id,
                "native": {"state": "running", "event": native_snapshot(event)},
                "semantic": {
                    "type": "state.transition",
                    "framework": "crewai",
                    "state_type": "agent.status",
                    "value": "running",
                },
            }
        )
        if previous in self._failed_turns:
            self._sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "crewai.recovery.retry",
                    "trace": opened.identity,
                    "native_identity": turn_id,
                    "native": {"attempt": 2, "previous_turn_id": previous},
                    "semantic": {
                        "type": "state.transition",
                        "framework": "crewai",
                        "state_type": "agent.recovery",
                        "value": {"previous_turn_id": previous},
                    },
                }
            )

    def _start_agent_execution(self, event: Any) -> None:
        event_id = str(getattr(event, "event_id", None) or "crewai-agent-turn")
        existing = self._explicit_parent_turn(event)
        if existing is not None:
            self._event_turn[event_id] = existing
            return
        conversation = _agent_conversation(event)
        previous = self._last_turn.get(conversation)
        turn_index = self._turn_order.get(conversation, 0)
        native = _agent_execution_native(event)
        if "task_prompt" in native:
            execution_input = native["task_prompt"]
        elif "messages" in native:
            execution_input = native["messages"]
        else:
            execution_input = native.get("task")
        opened = self._sink.open_trace(
            {
                "name": "crewai.agent.execution",
                "native_identity": event_id,
                "native": native,
                "semantic": {
                    "type": "agent.run",
                    "framework": "crewai",
                    "name": "crewai.agent.execution",
                    "input": execution_input,
                },
                "conversation_id": conversation,
                "turn_id": event_id,
                "turn_index": turn_index,
                **({"previous_turn_id": previous} if previous else {}),
            }
        )
        if not opened.accepted or opened.identity is None:
            return
        self._turn_order[conversation] = turn_index + 1
        turn = _OpenTurn(opened.identity, event_id, opened)
        self._turns[event_id] = turn
        self._agent_roots.add(event_id)
        self._event_turn[event_id] = turn
        self._sink.record(
            {
                "kind": "state",
                "phase": "start",
                "name": "crewai.state.transition",
                "trace": opened.identity,
                "native_identity": event_id,
                "native": {"state": "running", "event": native},
                "semantic": {
                    "type": "state.transition",
                    "framework": "crewai",
                    "state_type": "agent.status",
                    "value": "running",
                },
            }
        )

    def _finish_agent_execution(self, event: Any, *, failed: bool) -> None:
        started_event_id = _field(event, "started_event_id", None)
        if (
            isinstance(started_event_id, str)
            and started_event_id in self._event_turn
            and started_event_id not in self._agent_roots
        ):
            self._event_turn.pop(started_event_id, None)
            return
        if (
            not isinstance(started_event_id, str)
            or started_event_id not in self._agent_roots
        ) and self._explicit_parent_turn(event) is not None:
            return
        if not isinstance(started_event_id, str) or started_event_id not in self._agent_roots:
            self._record_agent_completion_gap(event)
            return
        opened = self._event_turn.get(started_event_id)
        if opened is None:
            self._record_agent_completion_gap(event)
            return
        self._turns.pop(opened.turn_id, None)
        self._agent_roots.discard(opened.turn_id)
        native = _agent_execution_native(event)
        error = _field(event, "error", None)
        if failed:
            self._record_error(opened, error or "CrewAI agent execution failed", native)
            self._failed_turns.add(opened.turn_id)
        self._sink.record(
            {
                "kind": "state",
                "phase": "error" if failed else "end",
                "name": "crewai.state.transition",
                "trace": opened.trace,
                "native_identity": opened.turn_id,
                "native": native,
                "semantic": {
                    "type": "state.transition",
                    "framework": "crewai",
                    "state_type": "agent.status",
                    "value": "failed" if failed else "completed",
                },
            }
        )
        self._record_unfinished_handoffs(opened)
        semantic: dict[str, Any] = {
            "type": "agent.run",
            "framework": "crewai",
            "status": "failed" if failed else "succeeded",
        }
        if failed:
            semantic["error"] = _semantic_error(error, "CrewAI agent execution failed")
        else:
            semantic["output"] = native_snapshot(_field(event, "output", None))
        self._sink.record(
            {
                "kind": "lifecycle",
                "phase": "error" if failed else "end",
                "name": "crewai.agent.execution",
                "trace": opened.trace,
                "native_identity": opened.turn_id,
                **(
                    {"error_identity": error}
                    if failed and isinstance(error, BaseException)
                    else {}
                ),
                "native": native,
                "semantic": semantic,
                **self._parent(opened.root),
            }
        )
        conversation = _agent_conversation(event)
        self._last_turn[conversation] = opened.turn_id
        for key, turn in list(self._event_turn.items()):
            if turn is opened:
                self._event_turn.pop(key, None)
        for key, turn in list(self._model_turn.items()):
            if turn is opened:
                self._forget_model(key)
        for key, turn in list(self._tool_turn.items()):
            if turn is opened:
                self._tool_turn.pop(key, None)
                self._tools.pop(key, None)

    def _record_agent_completion_gap(self, event: Any) -> None:
        open_agent_turns = [
            self._turns[turn_id]
            for turn_id in sorted(self._agent_roots)
            if turn_id in self._turns
        ]
        if not open_agent_turns:
            fallback = self._turn_for(event)
            open_agent_turns = [fallback] if fallback is not None else []
        if not open_agent_turns:
            return
        started_event_id = _field(event, "started_event_id", None)
        detail = (
            f"CrewAI completion identity {started_event_id!r} did not match an observed "
            "standalone agent start."
            if isinstance(started_event_id, str) and started_event_id
            else (
                "CrewAI did not expose started_event_id; completion is ambiguous across "
                f"{len(open_agent_turns)} open standalone agent roots."
            )
        )
        for turn in open_agent_turns:
            self._sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "crewai.agent.execution.completion.gap",
                    "trace": turn.trace,
                    "native": _agent_execution_native(event),
                    "semantic": {
                        "type": "capture.gap",
                        "framework": "crewai",
                        "reason": "agent_execution_completion_identity_not_observed",
                        "count": 1,
                        "detail": detail,
                    },
                }
            )

    def _explicit_parent_turn(self, event: Any) -> _OpenTurn | None:
        for name in ("parent_event_id", "triggered_by_event_id", "previous_event_id"):
            value = _field(event, name, None)
            if isinstance(value, str) and value in self._event_turn:
                return self._event_turn[value]
        return None

    def _finish_turn(self, event: Any, *, failed: bool) -> None:
        crew = str(getattr(event, "crew_name", None) or "crewai")
        opened = self._turn_for(event)
        if opened is None:
            matches = [turn for turn in self._turns.values() if turn.turn_id in self._event_turn]
            opened = matches[0] if len(matches) == 1 else None
        if opened is None:
            return
        self._turns.pop(opened.turn_id, None)
        snapshot = native_snapshot(event)
        if failed:
            self._record_error(opened, getattr(event, "error", "CrewAI execution failed"), snapshot)
            self._failed_turns.add(opened.turn_id)
        self._sink.record(
            {
                "kind": "state",
                "phase": "error" if failed else "end",
                "name": "crewai.state.transition",
                "trace": opened.trace,
                "native_identity": opened.turn_id,
                "native": {"error": _field(event, "error", None)}
                if failed
                else {"state": "completed", "output": _field(event, "output", None)},
                "semantic": {
                    "type": "state.transition",
                    "framework": "crewai",
                    "state_type": "agent.status",
                    "value": "failed" if failed else "completed",
                },
            }
        )
        lifecycle_native: dict[str, Any]
        if failed:
            lifecycle_native = {"error": _field(event, "error", None)}
        else:
            lifecycle_native = {"output": _field(event, "output", None)}
            total_tokens = _field(event, "total_tokens", None)
            if (
                isinstance(total_tokens, int)
                and not isinstance(total_tokens, bool)
                and total_tokens > 0
            ):
                lifecycle_native["usage"] = {"total_tokens": total_tokens}
                self._sink.record(
                    {
                        "kind": "model",
                        "phase": "event",
                        "name": "crewai.usage",
                        "trace": opened.trace,
                        "native_identity": f"{opened.turn_id}:usage",
                        "native": {
                            "usage": {"total_tokens": total_tokens},
                            "scope": "crew_instance_cumulative",
                            "measurement": "CrewKickoffCompletedEvent.total_tokens",
                        },
                        "semantic": {
                            "type": "capture.redundant",
                            "framework": "crewai",
                        },
                        **self._parent(opened.root),
                    }
                )
        self._record_unfinished_handoffs(opened)
        self._sink.record(
            {
                "kind": "lifecycle",
                "phase": "error" if failed else "end",
                "name": "crewai.turn",
                "trace": opened.trace,
                "native_identity": opened.turn_id,
                **(
                    {"error_identity": lifecycle_native["error"]}
                    if failed
                    and isinstance(lifecycle_native["error"], BaseException)
                    else {}
                ),
                "native": lifecycle_native,
                "semantic": {
                    "type": "agent.run",
                    "framework": "crewai",
                    "status": "failed" if failed else "succeeded",
                    **(
                        {
                            "error": _semantic_error(
                                _field(event, "error", None),
                                "CrewAI execution failed",
                            )
                        }
                        if failed
                        else {"output": native_snapshot(_field(event, "output", None))}
                    ),
                },
                **self._parent(opened.root),
            }
        )
        self._last_turn[crew] = opened.turn_id
        for key, turn in list(self._event_turn.items()):
            if turn is opened:
                self._event_turn.pop(key, None)
                self._handoffs.pop(key, None)
        for key, turn in list(self._model_turn.items()):
            if turn is opened:
                self._forget_model(key)
        for key, turn in list(self._tool_turn.items()):
            if turn is opened:
                self._tool_turn.pop(key, None)
                self._tools.pop(key, None)

    def _start_model(self, event: Any) -> None:
        turn = self._turn_for(event)
        if turn is None:
            return
        identity = str(getattr(event, "call_id", None) or getattr(event, "event_id", "model"))
        context_refs = self._record_model_context(
            turn,
            identity,
            _field(event, "messages", None),
        )
        tool_definitions = _tool_definitions(_field(event, "tools", None))
        request_settings = _model_request_settings(event)
        semantic: dict[str, Any] = {
            "type": "model.request",
            "framework": "crewai",
            "model": _field(event, "model", None),
            "context_refs": context_refs,
        }
        if tool_definitions:
            semantic["tools"] = _tool_names(tool_definitions)
            semantic["tool_definitions"] = tool_definitions
        if request_settings:
            semantic["settings"] = request_settings
        receipt = self._sink.record(
            {
                "kind": "model",
                "phase": "start",
                "name": "crewai.model",
                "trace": turn.trace,
                "native_identity": identity,
                "native": native_snapshot(event),
                "semantic": semantic,
            }
        )
        self._models[identity] = receipt
        self._model_turn[identity] = turn
        event_id = getattr(event, "event_id", None)
        if isinstance(event_id, str):
            self._event_turn[event_id] = turn
        if getattr(event, "stream", None) is True:
            self._streaming_models.add(identity)

    def _start_handoff(self, event: Any) -> None:
        turn = self._turn_for(event)
        event_id = _field(event, "event_id", None)
        if turn is None or not isinstance(event_id, str) or not event_id:
            return
        receipt = self._sink.record(
            {
                "kind": "state",
                "phase": "start",
                "name": "crewai.agent.handoff",
                "trace": turn.trace,
                "native_identity": event_id,
                "native": native_snapshot(event),
                "semantic": {
                    "type": "agent.handoff",
                    "framework": "crewai",
                    "value": _handoff_value(event, status="started"),
                },
            }
        )
        self._event_turn[event_id] = turn
        if receipt.accepted and receipt.record_id is not None:
            self._handoffs[event_id] = receipt

    def _finish_handoff(self, event: Any) -> None:
        started_event_id = _field(event, "started_event_id", None)
        if not isinstance(started_event_id, str) or not started_event_id:
            self._record_handoff_gap(
                event,
                "CrewAI did not expose started_event_id on the delegation completion.",
            )
            return
        turn = self._turn_for(event, identity=started_event_id)
        parent = self._handoffs.pop(started_event_id, None)
        if turn is None or parent is None:
            self._record_handoff_gap(
                event,
                "CrewAI exposed started_event_id, but its start event was not observed.",
            )
            return
        self._event_turn.pop(started_event_id, None)
        event_id = _field(event, "event_id", None)
        completion: dict[str, Any] = {
            "kind": "state",
            "phase": "end",
            "name": "crewai.agent.handoff",
            "trace": turn.trace,
            "native": native_snapshot(event),
            "semantic": {
                "type": "agent.handoff",
                "framework": "crewai",
                "value": _handoff_value(
                    event,
                    status=str(_field(event, "status", "completed")),
                ),
            },
            **self._parent(parent),
        }
        if isinstance(event_id, str) and event_id:
            completion["native_identity"] = event_id
        self._sink.record(completion)
        if not isinstance(event_id, str) or not event_id:
            self._record_handoff_gap(
                event,
                "CrewAI did not expose event_id on the delegation completion.",
                reason="handoff_completion_identity_not_observed",
                turn=turn,
            )

    def _record_handoff_gap(
        self,
        event: Any,
        detail: str,
        *,
        reason: str = "handoff_correlation_not_observed",
        turn: _OpenTurn | None = None,
    ) -> None:
        turn = turn or self._turn_for(event)
        if turn is None:
            return
        self._sink.record(
            {
                "kind": "unknown",
                "phase": "gap",
                "name": "crewai.agent.handoff.gap",
                "trace": turn.trace,
                "native": native_snapshot(event),
                "semantic": {
                    "type": "capture.gap",
                    "framework": "crewai",
                    "reason": reason,
                    "count": 1,
                    "detail": detail,
                },
            }
        )

    def _record_unfinished_handoffs(self, turn: _OpenTurn) -> None:
        unfinished = [
            (event_id, receipt)
            for event_id, receipt in self._handoffs.items()
            if self._event_turn.get(event_id) is turn
        ]
        for event_id, receipt in unfinished:
            self._sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "crewai.agent.handoff.gap",
                    "trace": turn.trace,
                    "native_identity": event_id,
                    "native": {"started_event_id": event_id},
                    "semantic": {
                        "type": "capture.gap",
                        "framework": "crewai",
                        "reason": "handoff_completion_not_observed",
                        "count": 1,
                        "detail": (
                            "CrewAI ended the agent run before this observed handoff "
                            "delivered a completion."
                        ),
                    },
                    **self._parent(receipt),
                }
            )
            self._handoffs.pop(event_id, None)

    def _record_model_context(
        self,
        turn: _OpenTurn,
        identity: str,
        messages: Any,
    ) -> list[str]:
        snapshot = native_snapshot(messages)
        if not isinstance(snapshot, list):
            return []
        refs: list[str] = []
        for index, message in enumerate(snapshot):
            if not isinstance(message, dict):
                continue
            role = message.get("role")
            if role not in {"system", "developer", "user", "assistant", "tool"}:
                continue
            if "content" not in message:
                continue
            receipt = self._sink.record(
                {
                    "kind": "log",
                    "phase": "event",
                    "name": "crewai.message",
                    "trace": turn.trace,
                    "native_identity": f"{identity}:message:{index}",
                    "native": message,
                    "semantic": {
                        "type": "message",
                        "framework": "crewai",
                        "role": role,
                        "content": message["content"],
                    },
                }
            )
            if receipt.accepted and receipt.record_id is not None:
                refs.append(receipt.record_id)
        return refs

    def _stream_delta(self, event: Any) -> None:
        identity = str(getattr(event, "call_id", None) or "model")
        turn = self._turn_for(event, identity=identity)
        if turn is None:
            return
        self._streamed_models.add(identity)
        self._sink.record(
            {
                "kind": "stream",
                "phase": "event",
                "name": "crewai.model.stream",
                "trace": turn.trace,
                "native_identity": identity,
                "native": {"delta": getattr(event, "chunk", ""), "event": native_snapshot(event)},
                "semantic": {"type": "capture.redundant", "framework": "crewai"},
                **self._parent(self._models.get(identity)),
            }
        )
        tool_call = _field(event, "tool_call", None)
        function = _field(tool_call, "function", None)
        if tool_call is None or function is None:
            return
        index = _field(tool_call, "index", 0)
        if not isinstance(index, int) or isinstance(index, bool):
            return
        pending = self._streamed_tool_calls.setdefault(identity, {}).setdefault(
            index, _StreamedToolCall(index=index)
        )
        name = _field(function, "name", None)
        if isinstance(name, str) and name:
            pending.name = name
        arguments = _field(function, "arguments", "")
        if isinstance(arguments, str) and arguments:
            pending.arguments += arguments
        tool_call_id = _field(tool_call, "id", None)
        if isinstance(tool_call_id, str) and tool_call_id:
            pending.tool_call_id = tool_call_id
        response_id = _field(event, "response_id", None)
        if isinstance(response_id, str) and response_id:
            pending.response_id = response_id

    def _thinking_delta(self, event: Any) -> None:
        identity = str(getattr(event, "call_id", None) or "model")
        turn = self._turn_for(event, identity=identity)
        if turn is None:
            return
        chunk = _field(event, "chunk", None)
        if not isinstance(chunk, str) or not chunk:
            return
        self._thinking_chunks.setdefault(identity, []).append(chunk)
        self._sink.record(
            {
                "kind": "stream",
                "phase": "event",
                "name": "crewai.model.thinking.stream",
                "trace": turn.trace,
                "native_identity": identity,
                "native": {"delta": chunk, "event": native_snapshot(event)},
                "semantic": {"type": "capture.redundant", "framework": "crewai"},
                **self._parent(self._models.get(identity)),
            }
        )

    def _finish_model(self, event: Any) -> None:
        identity = str(getattr(event, "call_id", None) or "model")
        turn = self._turn_for(event, identity=identity)
        if turn is None:
            return
        snapshot = native_snapshot(event)
        parent = self._models.pop(identity, None)
        if parent is None:
            self._forget_model(identity)
            return
        response = _field(event, "response", None)
        usage = _field(event, "usage", None)
        model_native: dict[str, Any] = {"output": response, "event": snapshot}
        if usage:
            model_native["usage"] = usage
        semantic: dict[str, Any] = {
            "type": "model.response",
            "framework": "crewai",
            "status": "completed",
            "content": native_snapshot(response),
        }
        model = _field(event, "model", None)
        if isinstance(model, str) and model:
            semantic["model"] = model
        finish_reason = _field(event, "finish_reason", None)
        if isinstance(finish_reason, str) and finish_reason:
            semantic["finish_reason"] = finish_reason
        semantic_usage = _semantic_usage(usage)
        if semantic_usage:
            semantic["usage"] = semantic_usage
        reasoning_text = "".join(self._thinking_chunks.get(identity, []))
        if reasoning_text:
            semantic["reasoning"] = [{"type": "text", "text": reasoning_text}]
        self._sink.record(
            {
                "kind": "model",
                "phase": "end",
                "name": "crewai.model",
                "trace": turn.trace,
                "native_identity": identity,
                "native": model_native,
                "semantic": semantic,
                **self._parent(parent),
            }
        )
        if identity in self._streaming_models and identity in self._streamed_models:
            terminal: dict[str, Any] = {"output": response}
            finish_reason = _field(event, "finish_reason", None)
            if finish_reason is not None:
                terminal["finish_reason"] = finish_reason
            if usage is not None:
                terminal["usage"] = usage
            self._sink.record(
                {
                    "kind": "stream",
                    "phase": "end",
                    "name": "crewai.model.stream",
                    "trace": turn.trace,
                    "native_identity": identity,
                    "native": terminal,
                    "semantic": {"type": "capture.redundant", "framework": "crewai"},
                    **self._parent(parent),
                }
            )
        self._forget_model(identity)

    def _fail_model(self, event: Any) -> None:
        identity = str(getattr(event, "call_id", None) or "model")
        turn = self._turn_for(event, identity=identity)
        if turn is None:
            return
        parent = self._models.pop(identity, None)
        if parent is None:
            self._forget_model(identity)
            return
        error = getattr(event, "error", "CrewAI model failed")
        reasoning_text = "".join(self._thinking_chunks.get(identity, []))
        self._sink.record(
            {
                "kind": "model",
                "phase": "error",
                "name": "crewai.model",
                "trace": turn.trace,
                "native_identity": identity,
                **(
                    {"error_identity": error}
                    if isinstance(error, BaseException)
                    else {}
                ),
                "native": {"error": error},
                "semantic": {
                    "type": "model.response",
                    "framework": "crewai",
                    "status": "failed",
                    "model": _field(event, "model", None),
                    **(
                        {"reasoning": [{"type": "text", "text": reasoning_text}]}
                        if reasoning_text
                        else {}
                    ),
                },
                **self._parent(parent),
            }
        )
        self._record_error(turn, error, native_snapshot(event))
        self._forget_model(identity)

    def _start_tool(self, event: Any) -> None:
        turn = self._turn_for(event)
        if turn is None:
            return
        self._complete_streamed_tool_model(event, turn)
        identity = str(getattr(event, "event_id", None) or getattr(event, "tool_name", "tool"))
        native = {
            "name": getattr(event, "tool_name", "tool"),
            "input": native_snapshot(getattr(event, "tool_args", {})),
            "event": native_snapshot(event),
        }
        self._sink.record(
            {
                "kind": "tool",
                "phase": "event",
                "name": "crewai.tool.proposal",
                "trace": turn.trace,
                "native_identity": identity,
                "native": native,
                "semantic": {
                    "type": "tool.proposal",
                    "framework": "crewai",
                    "name": native["name"],
                    "input": native["input"],
                    "native_call_id": identity,
                },
            }
        )
        receipt = self._sink.record(
            {
                "kind": "tool",
                "phase": "start",
                "name": "crewai.tool",
                "trace": turn.trace,
                "native_identity": identity,
                "native": native,
                "semantic": {
                    "type": "tool.execution",
                    "framework": "crewai",
                    "name": native["name"],
                    "input": native["input"],
                    "native_call_id": identity,
                },
            }
        )
        self._tools[identity] = receipt
        self._tool_turn[identity] = turn
        event_id = getattr(event, "event_id", None)
        if isinstance(event_id, str):
            self._event_turn[event_id] = turn

    def _finish_tool(self, event: Any, *, failed: bool) -> None:
        identity = str(
            getattr(event, "started_event_id", None)
            or getattr(event, "event_id", None)
            or getattr(event, "tool_name", "tool")
        )
        turn = self._turn_for(event, identity=identity)
        if turn is None:
            return
        parent = self._tools.pop(identity, None)
        self._tool_turn.pop(identity, None)
        snapshot = native_snapshot(event)
        error = _field(event, "error", None)
        self._sink.record(
            {
                "kind": "tool",
                "phase": "error" if failed else "end",
                "name": "crewai.tool",
                "trace": turn.trace,
                "native_identity": identity,
                **(
                    {"error_identity": error}
                    if failed and isinstance(error, BaseException)
                    else {}
                ),
                "native": {"error": error, "call_id": identity}
                if failed
                else {"output": _field(event, "output", None), "call_id": identity},
                "semantic": {
                    "type": "tool.error" if failed else "tool.result",
                    "framework": "crewai",
                    "native_call_id": identity,
                    "status": "failed" if failed else "succeeded",
                    **(
                        {"error": _semantic_error(error, "CrewAI tool execution failed")}
                        if failed
                        else {"output": native_snapshot(_field(event, "output", None))}
                    ),
                },
                **self._parent(parent),
            }
        )
        if failed:
            self._record_error(turn, error or "CrewAI tool failed", snapshot)

    def _complete_streamed_tool_model(self, event: Any, turn: _OpenTurn) -> None:
        tool_name = _field(event, "tool_name", None)
        tool_arguments = _field(event, "tool_args", None)
        exact_tool_call_id = _field(event, "tool_call_id", None)
        candidates: list[tuple[str, _StreamedToolCall]] = []
        for identity, calls in self._streamed_tool_calls.items():
            if (
                self._model_turn.get(identity) is not turn
                or identity not in self._streaming_models
                or identity not in self._streamed_models
            ):
                continue
            candidates.extend(
                (identity, call)
                for call in calls.values()
                if call.matches(tool_name, tool_arguments)
            )
        if not isinstance(exact_tool_call_id, str) or not exact_tool_call_id:
            if candidates:
                self._record_streamed_tool_gap(event, turn, candidates)
            return
        exact_matches = [
            (identity, call)
            for identity, call in candidates
            if call.tool_call_id == exact_tool_call_id
        ]
        if len(exact_matches) != 1:
            if candidates:
                self._record_streamed_tool_gap(event, turn, candidates)
            return
        identity, call = exact_matches[0]
        parent = self._models.pop(identity, None)
        if parent is None:
            self._forget_model(identity)
            return
        tool_call = call.native()
        evidence = {
            "kind": "matching_tool_usage_started",
            "model_call_id": identity,
            "tool_call": tool_call,
            "event": native_snapshot(event),
        }
        model_native: dict[str, Any] = {
            "output": {"tool_calls": [tool_call]},
            "completion_evidence": evidence,
        }
        if call.response_id is not None:
            model_native["response_id"] = call.response_id
        self._sink.record(
            {
                "kind": "model",
                "phase": "end",
                "name": "crewai.model",
                "trace": turn.trace,
                "native_identity": identity,
                "native": model_native,
                "semantic": {
                    "type": "model.response",
                    "framework": "crewai",
                    "status": "completed",
                    "content": model_native["output"],
                },
                **self._parent(parent),
            }
        )
        terminal: dict[str, Any] = {
            "finish_reason": "tool_calls",
            "tool_calls": [tool_call],
            "completion_evidence": evidence,
        }
        if call.response_id is not None:
            terminal["response_id"] = call.response_id
        self._sink.record(
            {
                "kind": "stream",
                "phase": "end",
                "name": "crewai.model.stream",
                "trace": turn.trace,
                "native_identity": identity,
                "native": terminal,
                "semantic": {"type": "capture.redundant", "framework": "crewai"},
                **self._parent(parent),
            }
        )
        self._forget_model(identity)

    def _record_streamed_tool_gap(
        self,
        event: Any,
        turn: _OpenTurn,
        candidates: list[tuple[str, _StreamedToolCall]],
    ) -> None:
        self._sink.record(
            {
                "kind": "unknown",
                "phase": "gap",
                "name": "crewai.streamed_tool_call.unpaired",
                "trace": turn.trace,
                "native": {
                    "event": native_snapshot(event),
                    "candidate_model_call_ids": [identity for identity, _call in candidates],
                    "candidate_tool_calls": [call.native() for _identity, call in candidates],
                },
                "semantic": {
                    "type": "crewai.streamed_tool_call.unpaired",
                    "count": len(candidates),
                },
            }
        )

    def _forget_model(self, identity: str) -> None:
        self._models.pop(identity, None)
        self._model_turn.pop(identity, None)
        self._streaming_models.discard(identity)
        self._streamed_models.discard(identity)
        self._streamed_tool_calls.pop(identity, None)
        self._thinking_chunks.pop(identity, None)

    def _record_error(self, turn: _OpenTurn, error: Any, event: Any) -> None:
        native = {"error": native_snapshot(error), "event": event}
        structured = _field(event, "structured_error", None)
        if isinstance(structured, dict):
            native["structured_error"] = structured
        self._sink.record(
            {
                "kind": "error",
                "phase": "event",
                "name": "crewai.error",
                "trace": turn.trace,
                "native_identity": turn.turn_id,
                **(
                    {"error_identity": error}
                    if isinstance(error, BaseException)
                    else {}
                ),
                "native": native,
                "semantic": {
                    "type": "error",
                    "framework": "crewai",
                    "error": _semantic_error(error, "CrewAI execution failed"),
                },
            }
        )

    def _record_framework_state(self, event: Any, *, family: str) -> None:
        turn = self._turn_for(event)
        if turn is None:
            return
        event_type = str(getattr(event, "type", ""))
        phase = (
            "start"
            if event_type.endswith("_started")
            else "error"
            if event_type.endswith("_failed")
            else "end"
        )
        operation = event_type.removeprefix(f"{family}_").rsplit("_", 1)[0]
        native = (
            _task_event_native(event, event_type) if family == "task" else native_snapshot(event)
        )
        if isinstance(native, dict):
            native["type"] = event_type
        self._sink.record(
            {
                "kind": "state",
                "phase": phase,
                "name": f"crewai.{family}.{operation}",
                "trace": turn.trace,
                "native_identity": str(
                    getattr(event, "event_id", None) or f"{turn.turn_id}:{event_type}"
                ),
                "native": native,
                "semantic": {
                    "type": "state.transition",
                    "framework": "crewai",
                    "state_type": ("task.status" if family == "task" else f"memory.{operation}"),
                    "value": _framework_state_value(
                        family=family,
                        operation=operation,
                        phase=phase,
                        native=native,
                    ),
                },
            }
        )
        if family == "task":
            event_id = _field(event, "event_id", None)
            if phase == "start" and isinstance(event_id, str) and event_id:
                self._event_turn[event_id] = turn
            elif phase in {"end", "error"}:
                started_event_id = _field(event, "started_event_id", None)
                if isinstance(started_event_id, str):
                    self._event_turn.pop(started_event_id, None)

    @staticmethod
    def _parent(receipt: AdmissionReceipt | None) -> dict[str, str]:
        return (
            {"parent_record_id": receipt.record_id}
            if receipt and receipt.accepted and receipt.record_id
            else {}
        )


def crewai_adapter(*, version: str | None = None) -> _CrewAIAdapter:
    """Create the official CrewAI event-bus adapter for the installed version."""

    return _CrewAIAdapter(_installed_version("crewai", version))


def _field(value: Any, name: str, default: Any) -> Any:
    return native_field(value, name, default)


def _agent_execution_native(event: Any) -> dict[str, Any]:
    agent_info = _field(event, "agent_info", None)
    agent = agent_info if isinstance(agent_info, dict) else _field(event, "agent", None)
    task = _field(event, "task", None)
    native: dict[str, Any] = {
        "event_id": _field(event, "event_id", None),
        "agent": {
            "id": _native_identity(_field(agent, "id", None)),
            "name": _field(agent, "name", None),
            "role": _field(agent, "role", None),
        },
        "task": {
            "id": _native_identity(_field(task, "id", None)),
            "name": _field(task, "name", None),
            "description": _field(task, "description", None),
            "expected_output": _field(task, "expected_output", None),
        },
    }
    for name in ("task_prompt", "messages", "output", "error"):
        value = _field(event, name, None)
        if value is not None:
            native[name] = native_snapshot(value)
    tools = _field(event, "tools", None)
    if tools:
        native["tools"] = [
            str(_field(tool, "name", None) or type(tool).__name__)
            for tool in tools
        ]
    return _compact_non_null(native)


def _native_identity(value: Any) -> Any:
    return str(value) if isinstance(value, UUID) else value


def _agent_conversation(event: Any) -> str:
    agent_info = _field(event, "agent_info", None)
    agent = agent_info if isinstance(agent_info, dict) else _field(event, "agent", None)
    return str(
        _field(agent, "id", None)
        or _field(agent, "key", None)
        or _field(agent, "role", None)
        or _field(agent, "name", None)
        or "crewai-agent"
    )


def _semantic_error(error: Any, fallback: str) -> dict[str, Any]:
    snapshot = native_snapshot(error)
    if isinstance(snapshot, BaseException):
        args = object.__getattribute__(snapshot, "args")
        message = args[0] if args and isinstance(args[0], str) and args[0] else fallback
    elif isinstance(snapshot, str) and snapshot:
        message = snapshot
    elif isinstance(snapshot, dict) and isinstance(snapshot.get("message"), str):
        message = snapshot["message"]
    else:
        message = fallback
    return {"type": "crewai.error", "message": message, "recoverable": False}


def _semantic_usage(value: Any) -> dict[str, int]:
    snapshot = native_snapshot(value)
    if not isinstance(snapshot, dict):
        return {}
    usage: dict[str, int] = {}
    for source, target in (
        ("prompt_tokens", "input_tokens"),
        ("input_tokens", "input_tokens"),
        ("completion_tokens", "output_tokens"),
        ("output_tokens", "output_tokens"),
    ):
        count = snapshot.get(source)
        if (
            target not in usage
            and isinstance(count, int)
            and not isinstance(count, bool)
            and count >= 0
        ):
            usage[target] = count
    return usage


def _tool_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    names: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        function = item.get("function")
        name = function.get("name") if isinstance(function, dict) else item.get("name")
        if isinstance(name, str) and name and name not in names:
            names.append(name)
    return names


def _tool_definitions(value: Any) -> list[dict[str, Any]]:
    snapshot = native_snapshot(value)
    if not isinstance(snapshot, list):
        return []
    return [_compact_non_null(item) for item in snapshot if isinstance(item, dict)]


def _model_request_settings(event: Any) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    for name in (
        "temperature",
        "top_p",
        "max_tokens",
        "stream",
        "seed",
        "stop_sequences",
        "frequency_penalty",
        "presence_penalty",
        "n",
    ):
        value = _field(event, name, None)
        if value is not None:
            settings[name] = native_snapshot(value)
    return settings


def _handoff_value(event: Any, *, status: str) -> dict[str, Any]:
    value: dict[str, Any] = {"status": status}
    for source, target in (
        ("agent_id", "source_agent_id"),
        ("agent_role", "source_agent_role"),
        ("task_id", "source_task_id"),
        ("task_description", "task"),
        ("context_id", "context_id"),
        ("endpoint", "endpoint"),
        ("a2a_agent_name", "target_agent_name"),
        ("turn_number", "turn_number"),
        ("is_multiturn", "is_multiturn"),
        ("result", "result"),
        ("error", "error"),
    ):
        field = _field(event, source, None)
        if field is not None:
            value[target] = native_snapshot(field)
    return value


def _compact_non_null(value: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key, child in value.items():
        if child is None:
            continue
        if isinstance(child, dict):
            compact[key] = _compact_non_null(child)
        elif isinstance(child, list):
            compact[key] = [
                _compact_non_null(item) if isinstance(item, dict) else item
                for item in child
                if item is not None
            ]
        else:
            compact[key] = child
    return compact


def _framework_state_value(
    *,
    family: str,
    operation: str,
    phase: str,
    native: Any,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "status": "failed" if phase == "error" else "running" if phase == "start" else "completed"
    }
    if isinstance(native, dict):
        fields: tuple[str, ...]
        if family == "task":
            fields = (
                "task_id",
                "task_name",
                "agent_id",
                "agent_role",
                "context",
                "output",
                "error",
            )
        elif operation == "query":
            fields = (
                "query",
                "results",
                "limit",
                "score_threshold",
                "query_time_ms",
                "error",
            )
        elif operation == "save":
            fields = ("value", "metadata", "save_time_ms", "error")
        else:
            fields = ("memory_content", "retrieval_time_ms", "error")
        for name in fields:
            if name in native:
                value[name] = native[name]
    if family == "memory":
        value["operation"] = operation
    return value


_BASE_EVENT_FIELDS = (
    "timestamp",
    "source_fingerprint",
    "source_type",
    "fingerprint_metadata",
    "task_id",
    "task_name",
    "agent_id",
    "agent_role",
    "event_id",
    "parent_event_id",
    "previous_event_id",
    "triggered_by_event_id",
    "started_event_id",
    "emission_sequence",
)


def _task_event_native(event: Any, event_type: str) -> dict[str, Any]:
    """Project finite official task-event fields without traversing ``event.task``."""
    native: dict[str, Any] = {"native_type": type(event).__name__, "type": event_type}
    for name in _BASE_EVENT_FIELDS:
        value = _field(event, name, None)
        if value is not None:
            native[name] = native_snapshot(value)
    if event_type == "task_started":
        native["context"] = native_snapshot(_field(event, "context", None))
    elif event_type == "task_completed":
        native["output"] = _task_output_native(_field(event, "output", None))
    elif event_type == "task_failed":
        native["error"] = native_snapshot(_field(event, "error", None))
    return native


def _task_output_native(output: Any) -> Any:
    if output is None:
        return None
    native: dict[str, Any] = {"native_type": type(output).__name__}
    for name in (
        "description",
        "name",
        "expected_output",
        "summary",
        "raw",
        "pydantic",
        "json_dict",
        "agent",
        "output_format",
        "messages",
    ):
        value = _field(output, name, None)
        if value is not None:
            native[name] = native_snapshot(value)
    return native
