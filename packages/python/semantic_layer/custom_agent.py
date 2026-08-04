"""Small typed bridge for app-owned custom agent callback streams."""

from __future__ import annotations

import threading
from typing import Any, Literal, TypedDict, TypeGuard, cast

from .capture_v1 import AdmissionReceipt, CaptureSource, TraceIdentity

_MAX_CONTEXT_MESSAGE_IDS = 4096


class _CustomAgentErrorOptional(TypedDict, total=False):
    code: str
    details: Any


class CustomAgentError(_CustomAgentErrorOptional):
    type: str
    message: str
    recoverable: bool


class _RunStartOptional(TypedDict, total=False):
    input: Any
    conversation_id: str
    turn_id: str
    turn_index: int
    previous_turn_id: str


class CustomAgentRunStartEvent(_RunStartOptional):
    type: Literal["run.start"]
    run_id: str
    name: str


class _RunOutcomeOptional(TypedDict, total=False):
    output: Any
    error: CustomAgentError


class CustomAgentRunOutcomeEvent(_RunOutcomeOptional):
    type: Literal["run.outcome"]
    run_id: str
    status: Literal["completed", "failed", "cancelled", "unknown"]


class _MessageOptional(TypedDict, total=False):
    name: str
    call_id: str


class CustomAgentMessageEvent(_MessageOptional):
    type: Literal["message"]
    run_id: str
    message_id: str
    role: Literal["system", "developer", "user", "assistant", "tool"]
    content: Any


class _ModelRequestOptional(TypedDict, total=False):
    model: str
    tools: list[str]
    message_ids: list[str]


class CustomAgentModelRequestEvent(_ModelRequestOptional):
    type: Literal["model.request"]
    run_id: str
    call_id: str


class _Usage(TypedDict, total=False):
    input_tokens: int
    output_tokens: int


class CustomAgentReasoningBlock(TypedDict):
    type: Literal["text", "summary"]
    text: str


class _ModelResponseOptional(TypedDict, total=False):
    call_id: str
    model: str
    content: Any
    reasoning: list[CustomAgentReasoningBlock]
    finish_reason: str
    usage: _Usage
    error: CustomAgentError


class CustomAgentModelResponseEvent(_ModelResponseOptional):
    type: Literal["model.response"]
    run_id: str
    status: Literal["completed", "incomplete", "failed", "cancelled"]


class CustomAgentToolProposalEvent(TypedDict):
    type: Literal["tool.proposal"]
    run_id: str
    call_id: str
    name: str
    input: Any


class CustomAgentToolCallEvent(TypedDict):
    type: Literal["tool.call"]
    run_id: str
    call_id: str
    name: str
    input: Any


class _ToolResultOptional(TypedDict, total=False):
    output: Any
    error: CustomAgentError


class CustomAgentToolResultEvent(_ToolResultOptional):
    type: Literal["tool.result"]
    run_id: str
    call_id: str
    status: Literal["succeeded", "failed", "cancelled"]


CustomAgentEvent = (
    CustomAgentRunStartEvent
    | CustomAgentRunOutcomeEvent
    | CustomAgentMessageEvent
    | CustomAgentModelRequestEvent
    | CustomAgentModelResponseEvent
    | CustomAgentToolProposalEvent
    | CustomAgentToolCallEvent
    | CustomAgentToolResultEvent
)


class _OpenRun(TypedDict):
    identity: TraceIdentity
    start_record_id: str
    name: str


class _Lifecycle:
    def __init__(self, bridge: CustomAgentBridge) -> None:
        self.bridge = bridge

    def deactivate(self) -> None:
        self.bridge._deactivate()

    def drain(self) -> None:
        return None


class _Source(CaptureSource):
    def __init__(
        self,
        bridge: CustomAgentBridge,
        *,
        name: str,
        version: str | None,
        seam: str,
    ) -> None:
        self.bridge = bridge
        self.metadata = {
            "name": name,
            "seam": seam,
            "identity_domain": "custom-agent.event",
            "coverage": [],
            **({"version": version} if version else {}),
        }

    def install(self, sink: Any) -> _Lifecycle:
        self.bridge._install(sink)
        return _Lifecycle(self.bridge)


class CustomAgentBridge:
    """Normalize callbacks without running or scheduling any application work."""

    def __init__(self, *, name: str, version: str | None, seam: str) -> None:
        self.source: CaptureSource = _Source(self, name=name, version=version, seam=seam)
        self._sink: Any = None
        self._active = False
        self._lock = threading.RLock()
        self._runs: dict[str, _OpenRun] = {}
        self._messages: dict[str, str] = {}
        self._models: dict[str, str] = {}
        self._collided_models: set[str] = set()
        self._settled_models: set[str] = set()
        self._model_request_gaps: set[str] = set()
        self._model_response_identity_gaps: set[str] = set()
        self._proposals: set[str] = set()
        self._tools: dict[str, str] = {}
        self._collided_tools: set[str] = set()
        self._settled_tools: set[str] = set()

    def _install(self, sink: Any) -> None:
        with self._lock:
            if self._active:
                raise RuntimeError("Custom agent source is already installed")
            self._sink = sink
            self._active = True

    def _deactivate(self) -> None:
        with self._lock:
            if self._sink is None:
                self._active = False
                return
            for run_id, run in list(self._runs.items()):
                self._record_open_operation_gaps(run_id, run)
                self._gap(
                    run,
                    {
                        "type": "run.outcome",
                        "run_id": run_id,
                        "reason": "source_deactivated",
                    },
                    "run_terminal_not_observed",
                )
                self._sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": "end",
                        "name": run["name"],
                        "trace": run["identity"],
                        "native_identity": run_id,
                        "parent_record_id": run["start_record_id"],
                        "native": {"reason": "source_deactivated"},
                        "semantic": {"type": "agent.run", "status": "unknown"},
                    }
                )
            self._runs.clear()
            self._messages.clear()
            self._models.clear()
            self._collided_models.clear()
            self._settled_models.clear()
            self._model_request_gaps.clear()
            self._model_response_identity_gaps.clear()
            self._proposals.clear()
            self._tools.clear()
            self._collided_tools.clear()
            self._settled_tools.clear()
            self._active = False

    def record(self, event: CustomAgentEvent) -> AdmissionReceipt:
        with self._lock:
            if not self._active or self._sink is None:
                return AdmissionReceipt(False, "source_not_installed")
            if not _exact_identity(event.get("run_id")):
                return AdmissionReceipt(False, "invalid_run_id")

            run_id = event["run_id"]
            event_type = event.get("type")
            if event_type not in _EVENT_TYPES:
                run = self._runs.get(run_id)
                if run is None:
                    return AdmissionReceipt(False, "unknown_event_type")
                return self._gap(run, event, "unknown_event_type")

            if event_type == "run.start":
                start = cast(CustomAgentRunStartEvent, event)
                if not isinstance(start.get("name"), str) or not start["name"].strip():
                    return AdmissionReceipt(False, "invalid_run_name")
                existing = self._runs.get(run_id)
                if existing is not None:
                    return self._gap(existing, event, "duplicate_run_start")
                opened = self._sink.open_trace(
                    {
                        "name": start["name"],
                        "native_identity": run_id,
                        **(
                            {"conversation_id": start["conversation_id"]}
                            if "conversation_id" in start
                            else {}
                        ),
                        **({"turn_id": start["turn_id"]} if "turn_id" in start else {}),
                        **({"turn_index": start["turn_index"]} if "turn_index" in start else {}),
                        **(
                            {"previous_turn_id": start["previous_turn_id"]}
                            if "previous_turn_id" in start
                            else {}
                        ),
                        "native": dict(start),
                        "semantic": {
                            "type": "agent.run",
                            "name": start["name"],
                            **({"input": start["input"]} if "input" in start else {}),
                        },
                    }
                )
                if opened.accepted and opened.identity is not None and opened.record_id is not None:
                    self._runs[run_id] = {
                        "identity": opened.identity,
                        "start_record_id": opened.record_id,
                        "name": start["name"],
                    }
                return cast(AdmissionReceipt, opened)

            run = self._runs.get(run_id)
            if run is None:
                return AdmissionReceipt(False, "unknown_run_id")

            if event_type == "run.outcome":
                outcome = cast(CustomAgentRunOutcomeEvent, event)
                if outcome.get("status") not in _RUN_OUTCOME_STATUSES:
                    return self._gap(run, event, "invalid_status")
                if not _consistent_terminal_error(outcome["status"], outcome.get("error")):
                    return self._gap(run, event, "contradictory_terminal_error")
                self._record_open_operation_gaps(run_id, run)
                receipt = self._sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": (
                            "error"
                            if outcome["status"] == "failed"
                            else "cancelled"
                            if outcome["status"] == "cancelled"
                            else "end"
                        ),
                        "name": run["name"],
                        "trace": run["identity"],
                        "native_identity": run_id,
                        "parent_record_id": run["start_record_id"],
                        "native": dict(outcome),
                        "semantic": {
                            "type": "agent.run",
                            "status": outcome["status"],
                            **({"output": outcome["output"]} if "output" in outcome else {}),
                            **({"error": outcome["error"]} if "error" in outcome else {}),
                        },
                    }
                )
                self._runs.pop(run_id, None)
                self._clear_run_values(run_id, self._messages)
                self._clear_run_values(run_id, self._models)
                self._clear_run_keys(run_id, self._collided_models)
                self._clear_run_keys(run_id, self._settled_models)
                self._model_request_gaps.discard(run_id)
                self._model_response_identity_gaps.discard(run_id)
                self._clear_run_keys(run_id, self._proposals)
                self._clear_run_values(run_id, self._tools)
                self._clear_run_keys(run_id, self._collided_tools)
                self._clear_run_keys(run_id, self._settled_tools)
                return cast(AdmissionReceipt, receipt)

            if event_type == "message":
                message = cast(CustomAgentMessageEvent, event)
                message_id = message.get("message_id")
                if not _exact_identity(message_id):
                    return self._gap(run, event, "invalid_message_id")
                role = message.get("role")
                if role not in _MESSAGE_ROLES or "content" not in message:
                    return self._gap(run, event, "invalid_message")
                invalid_call_id = "call_id" in message and not _exact_identity(
                    message.get("call_id")
                )
                key = _operation_key(run_id, message_id)
                if key in self._messages:
                    return self._gap(run, event, "duplicate_message")
                receipt = self._sink.record(
                    {
                        "kind": "model",
                        "phase": "event",
                        "name": "message",
                        "trace": run["identity"],
                        "native_identity": message_id,
                        "parent_record_id": run["start_record_id"],
                        "native": dict(message),
                        "semantic": {
                            "type": "message",
                            "role": role,
                            "content": message["content"],
                            **({"name": message["name"]} if message.get("name") else {}),
                            **(
                                {"call_id": message["call_id"]}
                                if _exact_identity(message.get("call_id"))
                                else {}
                            ),
                        },
                    }
                )
                if receipt.accepted and receipt.record_id is not None:
                    self._messages[key] = receipt.record_id
                    if invalid_call_id:
                        self._gap(
                            run,
                            event,
                            "invalid_call_id",
                            receipt.record_id,
                        )
                return cast(AdmissionReceipt, receipt)

            call_id = event.get("call_id")
            native_call_id = call_id if _exact_identity(call_id) else None
            idless_model_response = event_type == "model.response" and "call_id" not in event
            if native_call_id is None and not idless_model_response:
                return self._gap(run, event, "invalid_call_id")
            event_key = (
                _operation_key(run_id, native_call_id)
                if native_call_id is not None
                else None
            )

            if event_type == "model.request":
                request = cast(CustomAgentModelRequestEvent, event)
                assert event_key is not None and native_call_id is not None
                if event_key in self._models:
                    request_record_id = self._models.pop(event_key)
                    self._collided_models.add(event_key)
                    return self._gap(
                        run,
                        event,
                        "duplicate_model_request",
                        request_record_id,
                    )
                if (
                    event_key in self._collided_models
                    or event_key in self._settled_models
                ):
                    return self._gap(run, event, "duplicate_model_request")
                context_refs: list[str] | None = None
                message_ids = request.get("message_ids")
                if "message_ids" not in request:
                    pass
                elif not isinstance(message_ids, list):
                    self._gap(run, event, "invalid_message_ids")
                elif len(message_ids) > _MAX_CONTEXT_MESSAGE_IDS:
                    self._gap(run, event, "invalid_message_ids")
                else:
                    resolved_refs: list[str] = []
                    invalid_identity = False
                    unknown_identity = False
                    for message_id in message_ids:
                        if not _exact_identity(message_id):
                            invalid_identity = True
                            continue
                        message_record = self._messages.get(
                            _operation_key(run_id, message_id)
                        )
                        if message_record is None:
                            unknown_identity = True
                            continue
                        resolved_refs.append(message_record)
                    if invalid_identity:
                        self._gap(run, event, "invalid_message_id")
                    if unknown_identity:
                        self._gap(run, event, "unknown_message_id")
                    if not invalid_identity and not unknown_identity:
                        context_refs = resolved_refs
                tools = request.get("tools")
                valid_tools = (
                    list(tools)
                    if isinstance(tools, list) and all(isinstance(tool, str) for tool in tools)
                    else None
                )
                if "tools" in request and valid_tools is None:
                    self._gap(run, event, "invalid_tools")
                receipt = self._sink.record(
                    {
                        "kind": "model",
                        "phase": "start",
                        "name": "model.request",
                        "trace": run["identity"],
                        "native_identity": native_call_id,
                        "parent_record_id": run["start_record_id"],
                        "native": dict(request),
                        "semantic": {
                            "type": "model.request",
                            **({"model": request["model"]} if "model" in request else {}),
                            **({"tools": valid_tools} if valid_tools is not None else {}),
                            **(
                                {"context_refs": context_refs}
                                if context_refs is not None
                                else {}
                            ),
                        },
                    }
                )
                if receipt.accepted and receipt.record_id is not None:
                    self._models[event_key] = receipt.record_id
                return cast(AdmissionReceipt, receipt)

            if event_type == "model.response":
                response = cast(CustomAgentModelResponseEvent, event)
                if event_key is not None and event_key in self._collided_models:
                    self._collided_models.discard(event_key)
                    self._settled_models.add(event_key)
                    return self._gap(run, event, "ambiguous_model_response")
                if event_key is not None and event_key in self._settled_models:
                    return self._gap(run, event, "duplicate_model_response")
                if response.get("status") not in _MODEL_RESPONSE_STATUSES:
                    return self._gap(run, event, "invalid_status")
                request_id = (
                    self._models.get(event_key) if event_key is not None else None
                )
                if not _consistent_terminal_error(response["status"], response.get("error")):
                    return self._gap(run, event, "contradictory_terminal_error")
                missing_content = (
                    response["status"] in {"completed", "incomplete"} and "content" not in response
                )
                reasoning: list[CustomAgentReasoningBlock] | None = None
                invalid_reasoning = False
                if "reasoning" in response:
                    reasoning = _reasoning_blocks(response["reasoning"])
                    invalid_reasoning = reasoning is None
                receipt = self._sink.record(
                    {
                        "kind": "model",
                        "phase": (
                            "error"
                            if response["status"] == "failed"
                            else "cancelled"
                            if response["status"] == "cancelled"
                            else "end"
                        ),
                        "name": "model.response",
                        "trace": run["identity"],
                        **(
                            {"native_identity": native_call_id}
                            if native_call_id is not None
                            else {}
                        ),
                        "parent_record_id": request_id or run["start_record_id"],
                        "native": dict(response),
                        "semantic": {
                            "type": "model.response",
                            "status": response["status"],
                            **({"model": response["model"]} if "model" in response else {}),
                            **({"content": response["content"]} if "content" in response else {}),
                            **({"reasoning": reasoning} if reasoning is not None else {}),
                            **(
                                {"finish_reason": response["finish_reason"]}
                                if "finish_reason" in response
                                else {}
                            ),
                            **({"usage": dict(response["usage"])} if "usage" in response else {}),
                            **({"error": response["error"]} if "error" in response else {}),
                        },
                    }
                )
                if receipt.accepted and receipt.record_id is not None and "error" in response:
                    self._sink.record(
                        {
                            "kind": "error",
                            "phase": "error",
                            "name": "model.response.error",
                            "trace": run["identity"],
                            **(
                                {"native_identity": native_call_id}
                                if native_call_id is not None
                                else {}
                            ),
                            "parent_record_id": receipt.record_id,
                            "native": dict(response["error"]),
                            "semantic": {
                                "type": "agent.error",
                                "error": response["error"],
                            },
                        }
                    )
                if receipt.accepted and receipt.record_id is not None:
                    if request_id is None and run_id not in self._model_request_gaps:
                        self._model_request_gaps.add(run_id)
                        self._gap(
                            run,
                            event,
                            "model_request_not_observed",
                            receipt.record_id,
                        )
                    if (
                        native_call_id is None
                        and run_id not in self._model_response_identity_gaps
                    ):
                        self._model_response_identity_gaps.add(run_id)
                        self._gap(
                            run,
                            event,
                            "model_response_identity_not_observed",
                            receipt.record_id,
                        )
                    if missing_content:
                        self._gap(
                            run,
                            event,
                            "model_content_not_captured",
                            receipt.record_id,
                        )
                    if invalid_reasoning:
                        self._gap(
                            run,
                            event,
                            "invalid_reasoning",
                            receipt.record_id,
                        )
                if event_key is not None:
                    self._models.pop(event_key, None)
                    self._settled_models.add(event_key)
                return cast(AdmissionReceipt, receipt)

            assert event_key is not None and native_call_id is not None
            if event_type == "tool.proposal":
                proposal = cast(CustomAgentToolProposalEvent, event)
                invalid_tool = _invalid_tool_operation_reason(proposal)
                if invalid_tool is not None:
                    return self._gap(run, event, invalid_tool)
                if event_key in self._proposals:
                    return self._gap(run, event, "duplicate_tool_proposal")
                receipt = self._sink.record(
                    {
                        "kind": "tool",
                        "phase": "event",
                        "name": proposal["name"],
                        "trace": run["identity"],
                        "native_identity": native_call_id,
                        "parent_record_id": run["start_record_id"],
                        "native": dict(proposal),
                        "semantic": {
                            "type": "tool.proposal",
                            "name": proposal["name"],
                            "input": proposal["input"],
                        },
                    }
                )
                if receipt.accepted:
                    self._proposals.add(event_key)
                return cast(AdmissionReceipt, receipt)

            if event_type == "tool.call":
                call = cast(CustomAgentToolCallEvent, event)
                invalid_tool = _invalid_tool_operation_reason(call)
                if invalid_tool is not None:
                    return self._gap(run, event, invalid_tool)
                if event_key in self._tools:
                    collided_call_record_id = self._tools.pop(event_key)
                    self._collided_tools.add(event_key)
                    return self._gap(
                        run,
                        event,
                        "duplicate_tool_call",
                        collided_call_record_id,
                    )
                if (
                    event_key in self._collided_tools
                    or event_key in self._settled_tools
                ):
                    return self._gap(run, event, "duplicate_tool_call")
                receipt = self._sink.record(
                    {
                        "kind": "tool",
                        "phase": "start",
                        "name": call["name"],
                        "trace": run["identity"],
                        "native_identity": native_call_id,
                        "parent_record_id": run["start_record_id"],
                        "native": dict(call),
                        "semantic": {
                            "type": "tool.execution",
                            "name": call["name"],
                            "input": call["input"],
                        },
                    }
                )
                if receipt.accepted and receipt.record_id is not None:
                    self._tools[event_key] = receipt.record_id
                return cast(AdmissionReceipt, receipt)

            if event_type != "tool.result":
                return self._gap(run, event, "unknown_event_type")
            result = cast(CustomAgentToolResultEvent, event)
            if event_key in self._collided_tools:
                self._collided_tools.discard(event_key)
                self._settled_tools.add(event_key)
                return self._gap(run, event, "ambiguous_tool_result")
            if event_key in self._settled_tools:
                return self._gap(run, event, "duplicate_tool_result")
            if result.get("status") not in _TOOL_RESULT_STATUSES:
                return self._gap(run, event, "invalid_status")
            call_record_id = self._tools.get(event_key)
            if call_record_id is None:
                return self._gap(run, event, "tool_result_without_call")
            if not _consistent_terminal_error(result["status"], result.get("error")):
                return self._gap(run, event, "contradictory_terminal_error")
            receipt = self._sink.record(
                {
                    "kind": "tool",
                    "phase": (
                        "error"
                        if result["status"] == "failed"
                        else "cancelled"
                        if result["status"] == "cancelled"
                        else "end"
                    ),
                    "name": "tool.result",
                    "trace": run["identity"],
                    "native_identity": native_call_id,
                    "parent_record_id": call_record_id,
                    "native": dict(result),
                    "semantic": {
                        "type": ("tool.error" if result["status"] == "failed" else "tool.result"),
                        "status": result["status"],
                        **({"output": result["output"]} if "output" in result else {}),
                        **({"error": result["error"]} if "error" in result else {}),
                    },
                }
            )
            if (
                receipt.accepted
                and receipt.record_id is not None
                and result["status"] == "succeeded"
                and "output" not in result
            ):
                self._gap(
                    run,
                    event,
                    "tool_output_not_captured",
                    receipt.record_id,
                )
            self._tools.pop(event_key, None)
            self._settled_tools.add(event_key)
            return cast(AdmissionReceipt, receipt)

    def _record_open_operation_gaps(self, run_id: str, run: _OpenRun) -> None:
        for key, record_id in list(self._models.items()):
            if not key.startswith(f"{run_id}\0"):
                continue
            self._gap(
                run,
                {
                    "type": "model.request",
                    "run_id": run_id,
                    "call_id": key[len(run_id) + 1 :],
                },
                "model_request_without_response",
                record_id,
            )
            self._models.pop(key, None)
        for key, record_id in list(self._tools.items()):
            if not key.startswith(f"{run_id}\0"):
                continue
            self._gap(
                run,
                {
                    "type": "tool.call",
                    "run_id": run_id,
                    "call_id": key[len(run_id) + 1 :],
                },
                "tool_call_without_result",
                record_id,
            )
            self._tools.pop(key, None)

    def _gap(
        self,
        run: _OpenRun,
        event: object,
        reason: str,
        parent_record_id: str | None = None,
    ) -> AdmissionReceipt:
        return cast(
            AdmissionReceipt,
            self._sink.record(
                {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "custom-agent.gap",
                    "trace": run["identity"],
                    "parent_record_id": parent_record_id or run["start_record_id"],
                    "native": {"reason": reason, "event": event},
                    "semantic": {
                        "type": "capture.gap",
                        "reason": reason,
                        "detail": f"Custom agent callback gap: {reason}.",
                    },
                }
            ),
        )

    @staticmethod
    def _clear_run_values(run_id: str, values: dict[str, str]) -> None:
        for key in list(values):
            if key.startswith(f"{run_id}\0"):
                values.pop(key, None)

    @staticmethod
    def _clear_run_keys(run_id: str, values: set[str]) -> None:
        for key in list(values):
            if key.startswith(f"{run_id}\0"):
                values.discard(key)


def create_custom_agent_source(
    *,
    name: str,
    version: str | None = None,
    seam: str = "custom-agent.events",
) -> CustomAgentBridge:
    return CustomAgentBridge(name=name, version=version, seam=seam)


def _exact_identity(value: object) -> TypeGuard[str]:
    return (
        isinstance(value, str) and bool(value.strip()) and len(value) <= 256 and "\0" not in value
    )


def _operation_key(run_id: str, call_id: str) -> str:
    return f"{run_id}\0{call_id}"


def _invalid_tool_operation_reason(
    value: object,
) -> Literal["invalid_tool_name", "tool_input_not_captured"] | None:
    if not isinstance(value, dict):
        return "invalid_tool_name"
    name = value.get("name")
    if not isinstance(name, str) or not name.strip() or len(name) > 256:
        return "invalid_tool_name"
    if "input" not in value:
        return "tool_input_not_captured"
    return None


def _consistent_terminal_error(status: str, error: CustomAgentError | None) -> bool:
    return error is None or status == "failed"


_EVENT_TYPES = {
    "run.start",
    "run.outcome",
    "message",
    "model.request",
    "model.response",
    "tool.proposal",
    "tool.call",
    "tool.result",
}
_RUN_OUTCOME_STATUSES = {"completed", "failed", "cancelled", "unknown"}
_MODEL_RESPONSE_STATUSES = {"completed", "incomplete", "failed", "cancelled"}
_TOOL_RESULT_STATUSES = {"succeeded", "failed", "cancelled"}
_MESSAGE_ROLES = {"system", "developer", "user", "assistant", "tool"}


def _reasoning_blocks(
    value: object,
) -> list[CustomAgentReasoningBlock] | None:
    if not isinstance(value, list):
        return None
    blocks: list[CustomAgentReasoningBlock] = []
    for value_block in value:
        if not isinstance(value_block, dict):
            return None
        block_type = value_block.get("type")
        text = value_block.get("text")
        if block_type not in {"text", "summary"} or not isinstance(text, str):
            return None
        blocks.append(
            {
                "type": cast(Literal["text", "summary"], block_type),
                "text": text,
            }
        )
    return blocks
