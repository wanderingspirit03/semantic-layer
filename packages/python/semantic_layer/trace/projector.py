"""Stateful projection of normalized capture evidence into semantic trace records."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, NamedTuple

_SAFE_ID = re.compile(r"^[a-z][a-z0-9._:-]{2,127}$")
_STATE_TYPE = re.compile(r"^[a-z][a-z0-9._-]{2,127}$")
_LOSS_REASON = re.compile(r"^[a-z][a-z0-9_]{2,63}$")
_RUN_TYPES = {"agent.run", "workflow.run", "agent.generation"}
_ORIGINS = {"observed", "context", "inferred"}
_MESSAGE_ROLES = {"system", "developer", "user", "assistant", "tool"}
_MODEL_STATUSES = {"completed", "incomplete", "failed", "cancelled"}
_VERIFICATION_SUBJECTS = {"action", "goal", "delivery", "side_effect", "policy", "custom"}
_VERIFICATION_STATUSES = {"passed", "failed", "unknown"}
_SCOPE_STATUSES = {"completed", "failed", "cancelled", "interrupted", "unknown"}
_REDUNDANT_CONTROL_TYPES = {"agent.trace", "capture.control"}
_OMITTED_PARENT_TYPES = {"capture.control", "capture.redundant"}
_RUNTIME_GAP_NAMES = {
    "semantic_layer.context.gap",
    "semantic_layer.capture_input.gap",
}
_CONTEXT_KINDS = {"message", "model.response", "tool.result"}
_DELIVERABLE_KINDS = {"message", "model.response", "tool.result", "state"}
_DEFAULT_MAX_COMPLETED_RECORDS = 4096
_DEFAULT_MAX_ACTIVE_CORRELATIONS = 4096


class _ModelRequest(NamedTuple):
    record: str
    source_record: str
    parent: str | None
    native_key: tuple[str, str, str] | None


class _ToolProposal(NamedTuple):
    record: str
    source_record: str
    identity_key: str | None


class _ComposedOutput(NamedTuple):
    value: Any
    ambiguous: bool


class SemanticProjector:
    """Project already-scrubbed capture rows without interpreting native payloads."""

    def __init__(
        self,
        *,
        initial_seq: int = 0,
        max_completed_records: int = _DEFAULT_MAX_COMPLETED_RECORDS,
        max_active_correlations: int = _DEFAULT_MAX_ACTIVE_CORRELATIONS,
    ) -> None:
        if (
            isinstance(max_completed_records, bool)
            or not isinstance(max_completed_records, int)
            or max_completed_records < 0
        ):
            raise ValueError("max_completed_records must be a non-negative integer")
        if (
            isinstance(max_active_correlations, bool)
            or not isinstance(max_active_correlations, int)
            or max_active_correlations < 0
        ):
            raise ValueError("max_active_correlations must be a non-negative integer")
        self._seq = initial_seq
        self._max_completed_records = max_completed_records
        self._max_active_correlations = max_active_correlations
        self._roots: dict[str, tuple[str, str]] = {}
        self._records: dict[str, str] = {}
        self._omitted_records: set[str] = set()
        self._record_meta: dict[str, tuple[str, str | None]] = {}
        self._record_roots: dict[str, str | None] = {}
        self._projected_roots: dict[str, str] = {}
        self._context_records: dict[str, str] = {}
        self._expandable_model_requests: set[str] = set()
        self._scopes: dict[str, tuple[str, str]] = {}
        self._tool_proposals: dict[str, _ToolProposal] = {}
        self._tool_proposals_by_identity: dict[str, str] = {}
        self._tool_proposals_by_start: dict[str, str] = {}
        self._tool_calls: dict[
            str, tuple[str, str | None, str, str, frozenset[str]]
        ] = {}
        self._tool_calls_by_start: dict[str, str] = {}
        self._model_requests_by_identity: dict[tuple[str, str, str], _ModelRequest] = {}
        self._model_requests_by_record: dict[str, _ModelRequest] = {}
        self._turns: dict[str, tuple[str, str]] = {}
        self._evicted_turns: dict[str, None] = {}
        self._correlation_history: dict[str, None] = {}
        self._composed_outputs: dict[str, _ComposedOutput] = {}

    def retire_omitted(self, capture: Mapping[str, Any]) -> None:
        """Retire active state completed by a blocked terminal row."""
        semantic_value = capture.get("semantic")
        semantic = semantic_value if isinstance(semantic_value, Mapping) else {}
        event_kind = capture.get("event_kind")
        phase = capture.get("phase")
        semantic_type = semantic.get("type")
        source_parent = self._source_parent(capture)

        if event_kind == "lifecycle" and phase in {"end", "error", "cancelled"}:
            trace_id = self._trace_id(capture)
            root = self._roots.get(trace_id)
            if source_parent is not None and root is not None and root[0] == source_parent:
                self._roots.pop(trace_id, None)
                self._composed_outputs.pop(trace_id, None)
            if source_parent is not None:
                self._scopes.pop(source_parent, None)

        if event_kind == "model" and semantic_type == "model.response":
            parent_request = (
                self._model_requests_by_record.get(source_parent)
                if source_parent is not None
                else None
            )
            native_request = None
            identity = capture.get("native_identity")
            if isinstance(identity, str) and identity:
                native_request = self._model_requests_by_identity.get(
                    (
                        self._trace_id(capture),
                        self._source_id(capture.get("source")),
                        identity,
                    )
                )
            source_parent_meta = (
                self._record_meta.get(source_parent)
                if source_parent is not None
                else None
            )
            declared_parent = (
                source_parent_meta is not None
                and source_parent_meta[0] == "model.request"
            )
            if (
                not (declared_parent and parent_request is None)
                and (
                    parent_request is None
                    or native_request is None
                    or parent_request.record == native_request.record
                )
            ):
                request = parent_request or native_request
            else:
                request = None
            if request is not None:
                self._drop_model_request(request)

        if event_kind == "tool" and semantic_type in {"tool.result", "tool.error"}:
            parent_call_id = (
                self._tool_calls_by_start.get(source_parent)
                if source_parent is not None
                else None
            )
            parent_call = (
                self._tool_calls.get(parent_call_id)
                if parent_call_id is not None
                else None
            )
            parent_compatible = parent_call is not None and all(
                identity in parent_call[4]
                for identity in self._tool_identity_parts(capture, semantic)
            )
            execution_identity = self._tool_execution_identity(capture, semantic)
            semantic_call_id = (
                self._canonical_call_id(capture, execution_identity)
                if execution_identity is not None
                else None
            )
            source_parent_meta = (
                self._record_meta.get(source_parent)
                if source_parent is not None
                else None
            )
            declared_parent = (
                source_parent_meta is not None
                and source_parent_meta[0] == "tool.call"
            )
            call_id = (
                None
                if declared_parent and parent_call_id is None
                else None
                if parent_call_id is not None and not parent_compatible
                else parent_call_id
                if parent_call_id is not None
                else semantic_call_id
            )
            call = self._tool_calls.pop(call_id, None) if call_id is not None else None
            if call is not None:
                self._tool_calls_by_start.pop(call[2], None)

        if (
            event_kind == "tool"
            and semantic_type == "tool.execution"
            and phase in {"start", "event"}
        ):
            local_call_id = self._call_id(capture, semantic)
            local_proposal = self._tool_proposals.get(local_call_id)
            identity_key = self._tool_proposal_identity_key(capture, semantic)
            identity_call_id = (
                self._tool_proposals_by_identity.get(identity_key)
                if identity_key is not None
                else None
            )
            identity_proposal = (
                self._tool_proposals.get(identity_call_id)
                if identity_call_id is not None
                else None
            )
            if (
                local_proposal is None
                or identity_proposal is None
                or local_proposal.record == identity_proposal.record
            ):
                proposal_call_id = (
                    local_call_id if local_proposal is not None else identity_call_id
                )
                proposal = (
                    self._tool_proposals.pop(proposal_call_id, None)
                    if proposal_call_id is not None
                    else None
                )
                if proposal is not None:
                    self._tool_proposals_by_start.pop(proposal.source_record, None)
                    if proposal.identity_key is not None:
                        self._tool_proposals_by_identity.pop(
                            proposal.identity_key,
                            None,
                        )
        self._prune_correlation_history()

    def project(self, capture: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Return zero or more contract-shaped records for one capture row."""
        semantic_value = capture.get("semantic")
        semantic = semantic_value if isinstance(semantic_value, Mapping) else {}
        event_kind = capture.get("event_kind")
        phase = capture.get("phase")
        semantic_type = semantic.get("type")

        if self._transparent_otel_parent(capture, semantic, semantic_type):
            omitted_record_id = self._source_record_id(capture)
            parent = self._parent(capture)
            if parent is not None:
                self._remember_alias(omitted_record_id, parent)
            else:
                self._remember_omitted(omitted_record_id)
            return []

        if (
            event_kind != "loss"
            and self._explicit_parent_unresolved(capture)
            and self._orphan_is_material(
                capture, semantic, event_kind, phase, semantic_type
            )
        ):
            return [self._orphan_loss(capture)]

        if event_kind == "tool":
            tool = self._project_tool(capture, semantic, semantic_type, phase)
            if tool is not None:
                return tool
        if semantic_type == "message":
            message = self._project_message(capture, semantic)
            if message is not None:
                return message
        if event_kind == "model" and semantic_type == "model.request":
            return self._project_model_request(capture, semantic)
        if event_kind == "model" and semantic_type == "model.response":
            return self._project_model_response(capture, semantic, phase)
        if event_kind == "state" and isinstance(semantic_type, str):
            state = self._project_state(capture, semantic, semantic_type)
            if state is not None:
                return state
        if semantic_type == "verification":
            verification = self._project_verification(capture, semantic)
            if verification is not None:
                return verification
        if semantic_type == "capture.gap":
            raw_reason = semantic.get("reason")
            reason = (
                raw_reason
                if isinstance(raw_reason, str) and _LOSS_REASON.fullmatch(raw_reason)
                else "unsupported_semantic_projection"
            )
            data: dict[str, Any] = {
                "reason": reason,
                "stage": "source",
                "count": self._positive_int(semantic.get("count")) or 1,
                "recoverable": False,
            }
            detail = self._bounded(semantic.get("detail"), 4096)
            if detail is not None:
                data["detail"] = detail
            record = self._record(
                capture,
                "loss",
                data,
                parent=self._parent(capture),
            )
            if not self._explicit_parent_unresolved(capture):
                return [record]
            return [
                record,
                self._supplemental_loss(
                    capture,
                    affected=record,
                    reason="unresolved_parent",
                    detail=(
                        "The declared parent was not available in the projected trace."
                    ),
                ),
            ]
        if event_kind == "loss":
            loss = self._project_runtime_loss(capture)
            if loss is not None:
                return loss
        if event_kind == "lifecycle":
            return self._project_lifecycle(capture, semantic, phase)
        if event_kind == "error" or phase == "error":
            error = semantic.get("error")
            if isinstance(error, Mapping):
                normalized = self._error_data(error)
                if normalized is not None:
                    return [
                        self._record(
                            capture, "error", normalized, parent=self._parent(capture)
                        )
                    ]
        if self._redundant_control(capture, semantic, semantic_type):
            parent = self._parent(capture)
            source_record_id = capture.get("record_id")
            if parent is not None and isinstance(source_record_id, str):
                self._remember_alias(source_record_id, parent)
            return []
        if self._material(capture, semantic):
            return [
                self._unsupported(
                    capture,
                    event_kind,
                    phase,
                    semantic_type,
                    semantic.get("count"),
                )
            ]
        return []

    def _project_tool(
        self,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
        semantic_type: object,
        phase: object,
    ) -> list[dict[str, Any]] | None:
        if semantic_type == "tool.proposal":
            return self._project_tool_proposal(capture, semantic)
        if semantic_type == "tool.execution" and phase in {
            "start",
            "event",
        }:
            return self._project_tool_call(capture, semantic)
        if semantic_type in {"tool.result", "tool.error"}:
            return self._project_tool_result(capture, semantic, phase)
        return None

    def _project_tool_proposal(
        self, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        if "input" not in semantic:
            return [self._unsupported_from(capture, semantic)]
        call_id = self._call_id(capture, semantic)
        identity_key = self._tool_proposal_identity_key(capture, semantic)
        if (
            call_id in self._tool_proposals
            or (
                identity_key is not None
                and identity_key in self._tool_proposals_by_identity
            )
            or self._source_record_id(capture) in self._tool_proposals_by_start
            or self._tool_execution_key(capture, semantic) in self._tool_calls
        ):
            return [
                self._correlation_rejection(
                    capture,
                    "duplicate_active_tool_identity",
                    "A tool proposal reused an identity that was already active.",
                )
            ]
        if self._active_correlation_count() >= self._max_active_correlations:
            return [self._active_limit_loss(capture)]
        data: dict[str, Any] = {
            "call_id": call_id,
            "name": self._short(semantic.get("name") or capture.get("name"), "tool"),
            "input": semantic["input"],
        }
        native_call_id = self._display_tool_identity(capture, semantic)
        if native_call_id is not None:
            data["native_call_id"] = native_call_id
        record = self._record(capture, "tool.proposal", data, parent=self._parent(capture))
        proposal = _ToolProposal(
            record=record["id"],
            source_record=self._source_record_id(capture),
            identity_key=identity_key,
        )
        self._tool_proposals[call_id] = proposal
        if identity_key is not None:
            self._tool_proposals_by_identity[identity_key] = call_id
        self._tool_proposals_by_start[self._source_record_id(capture)] = call_id
        self._prune_correlation_history()
        return [record]

    def _project_tool_call(
        self, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        if "input" not in semantic:
            return [self._unsupported_from(capture, semantic)]
        source_start = self._source_record_id(capture)
        local_call_id = self._call_id(capture, semantic)
        local_proposal = self._tool_proposals.get(local_call_id)
        identity_key = self._tool_proposal_identity_key(capture, semantic)
        identity_call_id = (
            self._tool_proposals_by_identity.get(identity_key)
            if identity_key is not None
            else None
        )
        if (
            local_proposal is not None
            and identity_call_id is not None
            and identity_call_id != local_call_id
        ):
            return [
                self._correlation_rejection(
                    capture,
                    "ambiguous_tool_correlation",
                    "Tool execution identities resolved to different active proposals.",
                )
            ]
        proposal_call_id = (
            local_call_id if local_proposal is not None else identity_call_id
        )
        call_id = proposal_call_id or local_call_id
        proposal = (
            self._tool_proposals.get(proposal_call_id)
            if proposal_call_id is not None
            else None
        )
        execution_key = self._tool_execution_key(capture, semantic)
        if execution_key in self._tool_calls or source_start in self._tool_calls_by_start:
            return [
                self._correlation_rejection(
                    capture,
                    "duplicate_active_tool_identity",
                    "A tool call reused an identity that was already active.",
                )
            ]
        if (
            proposal is None
            and self._active_correlation_count() >= self._max_active_correlations
        ):
            return [self._active_limit_loss(capture)]
        data: dict[str, Any] = {
            "call_id": call_id,
            "name": self._short(semantic.get("name") or capture.get("name"), "tool"),
            "input": semantic["input"],
        }
        native_call_id = self._display_tool_identity(capture, semantic)
        if native_call_id is not None:
            data["native_call_id"] = native_call_id
        links = None
        proposal = self._tool_proposals.pop(call_id, None)
        if proposal is not None:
            self._tool_proposals_by_start.pop(proposal.source_record, None)
            if proposal.identity_key is not None:
                self._tool_proposals_by_identity.pop(proposal.identity_key, None)
            links = [{"type": "derived_from", "record": proposal.record}]
        parent = self._parent(capture)
        record = self._record(
            capture,
            "tool.call",
            data,
            parent=parent,
            links=links,
        )
        self._tool_calls[execution_key] = (
            record["id"],
            parent,
            source_start,
            call_id,
            frozenset(self._tool_identity_parts(capture, semantic)),
        )
        self._tool_calls_by_start[source_start] = execution_key
        self._prune_correlation_history()
        return [record]

    def _project_tool_result(
        self,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
        phase: object,
    ) -> list[dict[str, Any]]:
        native_call_id = self._display_tool_identity(capture, semantic)
        canonical_identity = self._tool_execution_identity(capture, semantic)
        semantic_call_id = (
            self._canonical_call_id(capture, canonical_identity)
            if canonical_identity is not None
            else None
        )
        source_parent = self._source_parent(capture)
        parent_call_id = (
            self._tool_calls_by_start.get(source_parent)
            if source_parent is not None
            else None
        )
        source_parent_meta = (
            self._record_meta.get(source_parent)
            if source_parent is not None
            else None
        )
        declared_call_parent = (
            source_parent_meta is not None
            and source_parent_meta[0] == "tool.call"
        )
        parent_call = (
            self._tool_calls.get(parent_call_id)
            if parent_call_id is not None
            else None
        )
        parent_compatible = (
            parent_call is not None
            and all(
                identity in parent_call[4]
                for identity in self._tool_identity_parts(capture, semantic)
            )
        )
        call_id = (
            None
            if declared_call_parent and parent_call_id is None
            else (
                parent_call_id
                if parent_call_id is not None and parent_compatible
                else semantic_call_id or parent_call_id
            )
        )
        if parent_call_id is not None and not parent_compatible:
            call_id = None
        call = self._tool_calls.get(call_id) if call_id is not None else None
        if call_id is None or call is None:
            if (
                parent_call_id is not None
                and parent_call_id not in self._tool_calls
                and source_parent is not None
            ):
                self._tool_calls_by_start.pop(source_parent, None)
            loss_parent = (
                source_parent_meta[1]
                if (
                    source_parent_meta is not None
                    and source_parent_meta[0] == "tool.call"
                )
                else self._parent(capture)
            )
            return [
                self._record(
                    capture,
                    "loss",
                    {
                        "reason": "unmatched_tool_result",
                        "stage": "source",
                        "count": 1,
                        "recoverable": False,
                        "detail": "Tool result had no exact matching tool call.",
                    },
                    parent=loss_parent,
                )
            ]
        (
            call_record,
            containment_parent,
            call_start,
            display_call_id,
            _,
        ) = call
        status = self._tool_status(semantic.get("status"), phase, semantic.get("type"))
        raw_error = semantic.get("error")
        normalized_error = (
            self._error_data(raw_error) if isinstance(raw_error, Mapping) else None
        )
        if normalized_error is not None and status != "failed":
            self._tool_calls.pop(call_id, None)
            self._tool_calls_by_start.pop(call_start, None)
            self._prune_correlation_history()
            return [
                self._record(
                    capture,
                    "loss",
                    {
                        "reason": "contradictory_terminal_error",
                        "stage": "source",
                        "count": 1,
                        "recoverable": False,
                        "detail": (
                            "A non-failed tool result was accompanied by error evidence."
                        ),
                    },
                    parent=containment_parent,
                    links=[{"type": "affects", "record": call_record}],
                )
            ]
        data: dict[str, Any] = {"call_id": display_call_id, "status": status}
        if native_call_id is not None:
            data["native_call_id"] = native_call_id
        if "output" in semantic:
            data["output"] = semantic["output"]
        if status == "failed" and normalized_error is not None:
            data["error"] = normalized_error
        record = self._record(
            capture,
            "tool.result",
            data,
            parent=containment_parent,
            links=[{"type": "result_of", "record": call_record}],
        )
        self._tool_calls.pop(call_id, None)
        self._tool_calls_by_start.pop(call_start, None)
        self._prune_correlation_history()
        return [record]

    def _project_message(
        self, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> list[dict[str, Any]] | None:
        role = semantic.get("role")
        if role not in _MESSAGE_ROLES or "content" not in semantic:
            return None
        data: dict[str, Any] = {"role": role, "content": semantic["content"]}
        name = self._bounded(semantic.get("name"), 256)
        if name is not None:
            data["name"] = name
        call_id = self._safe_id(semantic.get("call_id"))
        if call_id is not None:
            data["call_id"] = call_id
        return [
            self._record(capture, "message", data, parent=self._parent(capture))
        ]

    def _project_model_request(
        self, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> list[dict[str, Any]]:
        source_record_id = self._source_record_id(capture)
        identity = capture.get("native_identity")
        native_key = (
            (
                self._trace_id(capture),
                self._source_id(capture.get("source")),
                identity,
            )
            if isinstance(identity, str) and identity
            else None
        )
        if (
            source_record_id in self._model_requests_by_record
            or (
                native_key is not None
                and native_key in self._model_requests_by_identity
            )
        ):
            return [
                self._correlation_rejection(
                    capture,
                    "duplicate_active_model_identity",
                    "A model request reused an identity that was already active.",
                )
            ]
        if self._active_correlation_count() >= self._max_active_correlations:
            return [self._active_limit_loss(capture)]
        context_refs: list[str] = []
        unresolved_refs = 0
        raw_refs = semantic.get("context_refs")
        if isinstance(raw_refs, list):
            for raw_ref in raw_refs:
                if isinstance(raw_ref, str):
                    resolved = self._context_records.get(raw_ref)
                    if resolved is not None:
                        context_refs.append(resolved)
                        continue
                unresolved_refs += 1
        elif "context_refs" in semantic:
            unresolved_refs = 1
        data: dict[str, Any] = {}
        raw_base_ref = semantic.get("context_base_ref")
        base_invalid = False
        containment_parent = self._parent(capture)
        current_root = (
            self._projected_roots.get(containment_parent)
            if containment_parent is not None
            else None
        )
        if isinstance(raw_base_ref, str):
            resolved_base = self._records.get(raw_base_ref)
            base_meta = self._record_meta.get(raw_base_ref)
            if (
                resolved_base is not None
                and base_meta is not None
                and base_meta[0] == "model.request"
                and current_root is not None
                and self._record_roots.get(raw_base_ref) == current_root
                and raw_base_ref in self._expandable_model_requests
                and isinstance(raw_refs, list)
            ):
                data["context_base_ref"] = resolved_base
            else:
                base_invalid = True
        elif "context_base_ref" in semantic:
            base_invalid = True
        if not base_invalid and "context_refs" in semantic:
            data["context_refs"] = context_refs
        model = self._bounded(semantic.get("model"), 256)
        if model is not None:
            data["model"] = model
        tools = self._short_string_list(semantic.get("tools"))
        if tools:
            data["tools"] = tools
        tool_definitions = semantic.get("tool_definitions")
        if isinstance(tool_definitions, list) and all(
            isinstance(item, Mapping) for item in tool_definitions
        ):
            data["tool_definitions"] = [dict(item) for item in tool_definitions]
        settings = semantic.get("settings")
        if isinstance(settings, Mapping):
            data["settings"] = dict(settings)
        record = self._record(
            capture, "model.request", data, parent=containment_parent
        )
        request = _ModelRequest(
            record=record["id"],
            source_record=source_record_id,
            parent=containment_parent,
            native_key=native_key,
        )
        self._model_requests_by_record[source_record_id] = request
        if "context_refs" in data and unresolved_refs == 0:
            self._expandable_model_requests.add(source_record_id)
        if native_key is not None:
            self._model_requests_by_identity[native_key] = request
        self._prune_correlation_history()
        if base_invalid:
            return [
                record,
                self._context_base_ref_loss(
                    capture,
                    affected_record=record["id"],
                    parent=containment_parent,
                ),
            ]
        if unresolved_refs == 0:
            return [record]
        return [
            record,
            self._context_ref_loss(
                capture,
                affected_record=record["id"],
                count=unresolved_refs,
                parent=containment_parent,
            ),
        ]

    def _project_model_response(
        self,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
        phase: object,
    ) -> list[dict[str, Any]]:
        status = semantic.get("status")
        if phase == "error":
            status = "failed"
        elif phase == "cancelled":
            status = "cancelled"
        elif status not in _MODEL_STATUSES:
            status = "completed"
        data: dict[str, Any] = {"status": status}
        for key in ("model", "finish_reason"):
            value = self._bounded(semantic.get(key), 256)
            if value is not None:
                data[key] = value
        if "content" in semantic:
            data["content"] = semantic["content"]
        reasoning = self._reasoning(semantic.get("reasoning"))
        if reasoning:
            data["reasoning"] = reasoning
        usage = self._usage(semantic.get("usage"))
        if usage is not None:
            data["usage"] = usage

        parent_request: _ModelRequest | None = None
        source_parent = self._source_parent(capture)
        if source_parent is not None:
            parent_request = self._model_requests_by_record.get(source_parent)
        native_request: _ModelRequest | None = None
        identity = capture.get("native_identity")
        if isinstance(identity, str) and identity:
            native_request = self._model_requests_by_identity.get(
                (
                    self._trace_id(capture),
                    self._source_id(capture.get("source")),
                    identity,
                )
            )
        source_parent_meta = (
            self._record_meta.get(source_parent)
            if source_parent is not None
            else None
        )
        declared_request_parent = (
            source_parent_meta is not None
            and source_parent_meta[0] == "model.request"
        )
        if (
            parent_request is not None
            and native_request is not None
            and parent_request.record != native_request.record
        ):
            return [
                self._correlation_rejection(
                    capture,
                    "ambiguous_model_response",
                    "Model response identities resolved to different active requests.",
                    parent=source_parent_meta[1] if source_parent_meta else None,
                )
            ]
        if declared_request_parent and parent_request is None:
            return [
                self._correlation_rejection(
                    capture,
                    "unmatched_model_response",
                    "Model response referenced a request that was no longer active.",
                    parent=source_parent_meta[1] if source_parent_meta else None,
                )
            ]
        request_match = (
            parent_request or native_request
        )
        request = request_match.record if request_match is not None else None
        if request_match is not None:
            containment_parent = request_match.parent
        elif (
            source_parent is not None
            and source_parent_meta is not None
            and source_parent_meta[0] == "model.request"
        ):
            containment_parent = source_parent_meta[1]
        else:
            containment_parent = self._parent(capture)
        links = (
            [{"type": "result_of", "record": request}]
            if request is not None
            else None
        )
        if request_match is not None:
            self._drop_model_request(request_match)
        return [
            self._record(
                capture,
                "model.response",
                data,
                parent=containment_parent,
                links=links,
            )
        ]

    def _project_state(
        self,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
        semantic_type: str,
    ) -> list[dict[str, Any]] | None:
        state_type = semantic.get("state_type")
        if not isinstance(state_type, str):
            state_type = semantic_type
        if (
            not (
                semantic_type.startswith("state.")
                or semantic_type == "agent.handoff"
            )
            or not isinstance(state_type, str)
            or _STATE_TYPE.fullmatch(state_type) is None
        ):
            return None
        data: dict[str, Any] = {"type": state_type}
        if "value" in semantic:
            data["value"] = semantic["value"]
        version = semantic.get("version")
        if self._nonnegative_int(version) is not None:
            data["version"] = version
        else:
            bounded_version = self._bounded(version, 256)
            if bounded_version is not None:
                data["version"] = bounded_version
        result_ref = semantic.get("result_ref")
        result = self._exact_reference(result_ref, {"model.response", "tool.result"})
        links = (
            [{"type": "derived_from", "record": result}]
            if result is not None
            else None
        )
        record = self._record(
            capture,
            "state",
            data,
            parent=self._parent(capture),
            links=links,
        )
        if "result_ref" not in semantic or result is not None:
            return [record]
        return [
            record,
            self._supplemental_loss(
                capture,
                affected=record,
                reason="unresolved_result_ref",
                detail=(
                    "The result reference did not resolve to an earlier "
                    "model.response or tool.result record."
                ),
            ),
        ]

    def _project_verification(
        self, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> list[dict[str, Any]] | None:
        subject = semantic.get("subject")
        status = semantic.get("status")
        if subject not in _VERIFICATION_SUBJECTS or status not in _VERIFICATION_STATUSES:
            return None
        raw_records = semantic.get("records")
        if "records" not in semantic:
            return None
        if not isinstance(raw_records, list):
            raw_records = [None]
        verified: list[str] = []
        unresolved = 0
        for raw_record in raw_records:
            resolved_record = (
                self._records.get(raw_record)
                if isinstance(raw_record, str)
                else None
            )
            if resolved_record is None:
                unresolved += 1
            elif resolved_record not in verified:
                verified.append(resolved_record)
        if not verified:
            return [
                self._record(
                    capture,
                    "loss",
                    {
                        "reason": "unresolved_verification_ref",
                        "stage": "source",
                        "count": unresolved or 1,
                        "recoverable": False,
                        "detail": (
                            "Verification references did not resolve to earlier "
                            "projected records."
                        ),
                    },
                    parent=self._parent(capture),
                )
            ]
        data: dict[str, Any] = {"subject": subject, "status": status}
        summary = self._bounded(semantic.get("summary"), 4096)
        if summary is not None:
            data["summary"] = summary
        verification_record = self._record(
            capture,
            "verification",
            data,
            parent=self._parent(capture),
            links=[
                {"type": "verifies", "record": verified_record}
                for verified_record in verified
            ],
        )
        if unresolved == 0:
            return [verification_record]
        return [
            verification_record,
            self._supplemental_loss(
                capture,
                affected=verification_record,
                reason="unresolved_verification_ref",
                detail=(
                    "One or more verification references were invalid or unavailable."
                ),
                count=unresolved,
            ),
        ]

    def _project_runtime_loss(
        self, capture: Mapping[str, Any]
    ) -> list[dict[str, Any]] | None:
        loss = capture.get("loss")
        if not isinstance(loss, Mapping):
            return None
        raw_reason = loss.get("reason")
        reason = (
            raw_reason
            if isinstance(raw_reason, str) and _LOSS_REASON.fullmatch(raw_reason)
            else "unsupported_semantic_projection"
        )
        data: dict[str, Any] = {
            "reason": reason,
            "stage": self._loss_stage(loss.get("stage")),
            "count": self._positive_int(loss.get("count")) or 1,
            "recoverable": loss.get("recoverable")
            if isinstance(loss.get("recoverable"), bool)
            else False,
        }
        path = self._bounded(loss.get("affected_path"), 2048)
        if path is not None:
            data["path"] = path
        byte_count = self._nonnegative_int(loss.get("bytes"))
        if byte_count is not None:
            data["bytes"] = byte_count
        detail = self._bounded(loss.get("detail"), 4096)
        if detail is not None:
            data["detail"] = detail
        affected = loss.get("affected_record_id")
        affected_record = (
            self._records.get(affected) if isinstance(affected, str) else None
        )
        links = (
            [{"type": "affects", "record": affected_record}]
            if affected_record is not None
            else None
        )
        record = self._record(
            capture,
            "loss",
            data,
            parent=self._parent(capture),
            links=links,
        )
        records = [record]
        if isinstance(affected, str) and affected_record is None:
            records.append(
                self._supplemental_loss(
                    capture,
                    affected=record,
                    reason="unresolved_affected_ref",
                    detail="The affected record reference was invalid or unavailable.",
                )
            )
        if self._explicit_parent_unresolved(capture):
            records.append(
                self._supplemental_loss(
                    capture,
                    affected=record,
                    reason="unresolved_parent",
                    detail=(
                        "The declared parent was not available in the projected trace."
                    ),
                )
            )
        return records

    def _project_lifecycle(
        self,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
        phase: object,
    ) -> list[dict[str, Any]]:
        trace_id = self._trace_id(capture)
        parent = self._parent(capture)
        semantic_type = semantic.get("type")

        if phase == "start":
            source_parent = self._source_parent(capture)
            if (
                source_parent is None
                and semantic_type in _RUN_TYPES
                and trace_id in self._roots
            ):
                return [
                    self._record(
                        capture,
                        "loss",
                        {
                            "reason": "ambiguous_root_start",
                            "stage": "source",
                            "count": 1,
                            "recoverable": False,
                            "detail": (
                                "A second unparented root started while the trace "
                                "already had an active root."
                            ),
                        },
                        remember=False,
                    )
                ]
            if (
                trace_id not in self._roots
                and source_parent is None
                and semantic_type in _RUN_TYPES
            ):
                if self._active_correlation_count() >= self._max_active_correlations:
                    return [self._active_limit_loss(capture, parent=None)]
                data: dict[str, Any] = {
                    "name": self._short(semantic.get("name") or capture.get("name"), "run")
                }
                if "input" in semantic:
                    data["input"] = semantic["input"]
                for key in (
                    "conversation_id",
                    "turn_id",
                    "turn_index",
                    "previous_turn_id",
                ):
                    if key in capture:
                        data[key] = capture[key]
                run_correlation = self._run_correlation(capture.get("run_correlation"))
                if run_correlation is not None:
                    data["correlation"] = run_correlation
                continuation, unresolved_continuation = self._continuation(capture)
                record = self._record(
                    capture,
                    "run.start",
                    data,
                    parent=parent,
                    links=continuation,
                )
                self._roots[trace_id] = (
                    self._source_record_id(capture),
                    record["id"],
                )
                self._prune_correlation_history()
                self._remember_turn(capture, record["id"])
                return (
                    [record, self._unresolved_continuation_loss(capture, record)]
                    if unresolved_continuation
                    else [record]
                )
            declared_scope = semantic.get("scope_type")
            can_scope = parent is not None and (
                declared_scope in {"agent", "turn", "step"}
                or isinstance(capture.get("turn_id"), str)
                or semantic_type in _RUN_TYPES
                or semantic_type in {"scope", "agent.scope", "workflow.step"}
            )
            if not can_scope:
                return (
                    [self._unsupported_from(capture, semantic)]
                    if self._material(capture, semantic)
                    else []
                )
            if self._active_correlation_count() >= self._max_active_correlations:
                return [self._active_limit_loss(capture)]
            scope_type = declared_scope
            if scope_type not in {"agent", "turn", "step"}:
                scope_type = (
                    "turn"
                    if isinstance(capture.get("turn_id"), str)
                    else "agent"
                    if semantic_type == "agent.scope"
                    else "step"
                )
            source_record_id = self._source_record_id(capture)
            scope_id = self._safe_id(semantic.get("scope_id")) or self._hashed_id(
                "scope", source_record_id
            )
            data = {
                "scope_id": scope_id,
                "type": scope_type,
                "phase": "start",
                "name": self._short(semantic.get("name") or capture.get("name"), "scope"),
            }
            continuation, unresolved_continuation = self._continuation(capture)
            record = self._record(
                capture,
                "scope",
                data,
                parent=parent,
                links=continuation,
            )
            self._scopes[source_record_id] = (scope_id, scope_type)
            self._remember_turn(capture, record["id"])
            return (
                [record, self._unresolved_continuation_loss(capture, record)]
                if unresolved_continuation
                else [record]
            )

        source_parent = self._source_parent(capture)
        root = self._roots.get(trace_id)
        source_parent_meta = (
            self._record_meta.get(source_parent)
            if source_parent is not None
            else None
        )
        exact_root_parent = (
            source_parent is not None
            and source_parent_meta is not None
            and source_parent_meta[0] == "run.start"
            and root is not None
            and source_parent == root[0]
            and self._records.get(source_parent) == root[1]
        )
        if (
            phase in {"end", "error", "cancelled"}
            and root is not None
            and exact_root_parent
        ):
            status = self._outcome_status(semantic.get("status"), phase)
            composed_output = self._composed_outputs.pop(trace_id, None)
            data = {"status": status}
            if isinstance(semantic.get("summary"), str):
                data["summary"] = semantic["summary"][:4096]
            if "output" in semantic:
                data["output"] = semantic["output"]
            if (
                composed_output is not None
                and not composed_output.ambiguous
                and "output_ref" not in semantic
                and "output" not in data
            ):
                data["output"] = composed_output.value
            output_ref = semantic.get("output_ref")
            output_record = self._exact_reference(output_ref, _DELIVERABLE_KINDS)
            error = semantic.get("error")
            normalized_error = None
            if isinstance(error, Mapping):
                normalized_error = self._error_data(error)
                if normalized_error is not None:
                    data["error"] = normalized_error
            contradictory_error = (
                normalized_error is not None
                and phase == "end"
                and status != "failed"
            )
            if contradictory_error:
                data["status"] = "unknown"
            record = self._record(
                capture,
                "run.outcome",
                data,
                parent=parent,
                links=(
                    [{"type": "derived_from", "record": output_record}]
                    if output_record is not None
                    else None
                ),
            )
            self._roots.pop(trace_id, None)
            self._prune_correlation_history()
            supplemental: list[dict[str, Any]] = []
            if contradictory_error:
                supplemental.append(
                    self._supplemental_loss(
                        capture,
                        affected=record,
                        reason="contradictory_terminal_error",
                        detail=(
                            "A normal terminal status was accompanied by error evidence."
                        ),
                    )
                )
            if composed_output is not None and composed_output.ambiguous:
                supplemental.append(
                    self._supplemental_loss(
                        capture,
                        affected=record,
                        reason="ambiguous_root_output",
                        detail=(
                            "Multiple direct child lifecycle outputs were eligible "
                            "for root composition, so none was promoted."
                        ),
                    )
                )
            if "output_ref" in semantic and output_record is None:
                supplemental.append(
                    self._supplemental_loss(
                        capture,
                        affected=record,
                        reason="unresolved_output_ref",
                        detail=(
                            "The output reference did not resolve to earlier "
                            "deliverable evidence."
                        ),
                    )
                )
            return [record, *supplemental]

        scope = self._scopes.get(source_parent) if source_parent is not None else None
        if (
            phase in {"end", "error", "cancelled"}
            and source_parent is not None
            and scope is not None
        ):
            scope_id, scope_type = scope
            status = self._scope_status(semantic.get("status"), phase)
            if (
                root is not None
                and semantic_type in _RUN_TYPES
                and source_parent_meta is not None
                and source_parent_meta[1] == root[1]
                and semantic.get("output") is not None
            ):
                candidate = self._composed_outputs.get(trace_id)
                if candidate is None:
                    self._composed_outputs[trace_id] = _ComposedOutput(
                        semantic["output"],
                        False,
                    )
                elif not candidate.ambiguous:
                    self._composed_outputs[trace_id] = _ComposedOutput(None, True)
            record = self._record(
                capture,
                "scope",
                {
                    "scope_id": scope_id,
                    "type": scope_type,
                    "phase": "end",
                    "status": status,
                },
                parent=parent,
            )
            self._scopes.pop(source_parent, None)
            self._prune_correlation_history()
            error = semantic.get("error")
            normalized_error = (
                self._error_data(error) if isinstance(error, Mapping) else None
            )
            if normalized_error is None:
                return [record]
            error_capture = dict(capture)
            error_capture["record_id"] = self._hashed_id(
                "error", self._source_record_id(capture)
            )
            error_capture["blob_refs"] = []
            return [
                record,
                self._record(
                    error_capture,
                    "error",
                    normalized_error,
                    parent=record["id"],
                ),
            ]

        recognized_terminal = (
            phase in {"end", "error", "cancelled"}
            and (
                semantic_type in _RUN_TYPES
                or semantic_type in {"scope", "agent.scope", "workflow.step"}
            )
        )
        if recognized_terminal or self._material(capture, semantic):
            return [self._unsupported_from(capture, semantic)]
        return []

    def _record(
        self,
        capture: Mapping[str, Any],
        kind: str,
        data: dict[str, Any],
        *,
        parent: str | None = None,
        links: list[dict[str, str]] | None = None,
        remember: bool = True,
    ) -> dict[str, Any]:
        self._seq += 1
        semantic = capture.get("semantic")
        raw_origin = semantic.get("origin") if isinstance(semantic, Mapping) else None
        origin = raw_origin if raw_origin in _ORIGINS else "observed"
        record: dict[str, Any] = {
            "id": self._projected_record_id(capture),
            "seq": self._seq,
            "time": self._time(capture.get("observed_at")),
            "kind": kind,
            "origin": origin,
            "source": self._source_id(capture.get("source")),
            "data": data,
        }
        blob_refs = self._blob_refs(capture.get("blob_refs"))
        if blob_refs:
            record["blob_refs"] = blob_refs
        if parent is not None:
            record["parent"] = parent
        if links:
            record["links"] = links
        if remember:
            self._remember(capture, record["id"], kind, parent)
        return record

    def _call_id(
        self, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> str:
        identity = self._canonical_tool_identity(capture, semantic)
        if identity is None:
            identity = self._source_record_id(capture)
        return self._canonical_call_id(capture, identity)

    def _tool_execution_key(
        self,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
    ) -> str:
        identity = self._tool_execution_identity(capture, semantic)
        if identity is None:
            identity = self._source_record_id(capture)
        return self._canonical_call_id(capture, identity)

    @classmethod
    def _tool_proposal_identity_key(
        cls,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
    ) -> str | None:
        identity = cls._canonical_tool_identity(capture, semantic)
        if identity is None:
            return None
        return hashlib.sha256(
            f"{cls._trace_id(capture)}\0{identity}".encode()
        ).hexdigest()

    def _canonical_call_id(
        self, capture: Mapping[str, Any], identity: str
    ) -> str:
        source_id, identity_domain = self._source_namespace(capture.get("source"))
        return self._hashed_id(
            "call",
            "\0".join(
                (
                    source_id,
                    identity_domain,
                    self._trace_id(capture),
                    identity,
                )
            ),
        )

    @classmethod
    def _canonical_tool_identity(
        cls, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> str | None:
        """Choose the full exact identity before contract display bounds."""
        native_call = semantic.get("native_call_id")
        if isinstance(native_call, str) and native_call:
            return native_call
        semantic_call = semantic.get("call_id")
        if isinstance(semantic_call, str) and semantic_call:
            return semantic_call
        native_identity = capture.get("native_identity")
        return (
            native_identity
            if isinstance(native_identity, str) and native_identity
            else None
        )

    @classmethod
    def _tool_execution_identity(
        cls,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
    ) -> str | None:
        semantic_identity = semantic.get("native_call_id")
        if not isinstance(semantic_identity, str) or not semantic_identity:
            semantic_identity = semantic.get("call_id")
        if not isinstance(semantic_identity, str) or not semantic_identity:
            semantic_identity = None
        native_identity = capture.get("native_identity")
        if not isinstance(native_identity, str) or not native_identity:
            native_identity = None
        if (
            semantic_identity is not None
            and native_identity is not None
            and semantic_identity != native_identity
        ):
            return f"{semantic_identity}\0{native_identity}"
        return semantic_identity or native_identity

    @classmethod
    def _tool_identity_parts(
        cls,
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
    ) -> tuple[str, ...]:
        semantic_identity = semantic.get("native_call_id")
        if not isinstance(semantic_identity, str) or not semantic_identity:
            semantic_identity = semantic.get("call_id")
        identities: list[str] = []
        for identity in (semantic_identity, capture.get("native_identity")):
            if isinstance(identity, str) and identity and identity not in identities:
                identities.append(identity)
        return tuple(identities)

    @classmethod
    def _display_tool_identity(
        cls, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> str | None:
        return cls._bounded(cls._canonical_tool_identity(capture, semantic), 256)

    @classmethod
    def _source_namespace(cls, source: object) -> tuple[str, str]:
        if not isinstance(source, Mapping):
            return (cls._text(source, "unknown"), "unknown")
        return (
            cls._text(source.get("source_id"), "unknown"),
            cls._text(source.get("identity_domain"), "unknown"),
        )

    @classmethod
    def _blob_refs(cls, value: object) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            return []
        result: list[dict[str, Any]] = []
        for blob in value:
            if not isinstance(blob, Mapping) or blob.get("scan") != "clean":
                continue
            digest = blob.get("digest")
            byte_length = cls._nonnegative_int(blob.get("byte_length"))
            if (
                not isinstance(digest, str)
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                or byte_length is None
            ):
                continue
            result.append(
                {
                    "path": f"blobs/{digest}.blob",
                    "sha256": digest,
                    "bytes": byte_length,
                    "media_type": cls._bounded(blob.get("mime_type"), 256)
                    or "application/octet-stream",
                    "scan": "clean",
                }
            )
        return result

    @classmethod
    def _error_data(cls, value: Mapping[str, Any]) -> dict[str, Any] | None:
        error_type = value.get("type")
        message = value.get("message")
        recoverable = value.get("recoverable")
        if (
            not isinstance(error_type, str)
            or _STATE_TYPE.fullmatch(error_type) is None
            or not isinstance(message, str)
            or not isinstance(recoverable, bool)
        ):
            return None
        data: dict[str, Any] = {
            "type": error_type,
            "message": message[:4096],
            "recoverable": recoverable,
        }
        code = value.get("code")
        if isinstance(code, str) and code:
            data["code"] = code[:256]
        if "details" in value:
            data["details"] = value["details"]
        return data

    def _unsupported_from(
        self, capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> dict[str, Any]:
        return self._unsupported(
            capture,
            capture.get("event_kind"),
            capture.get("phase"),
            semantic.get("type"),
            semantic.get("count"),
        )

    def _unsupported(
        self,
        capture: Mapping[str, Any],
        event_kind: object,
        phase: object,
        semantic_type: object,
        semantic_count: object = None,
    ) -> dict[str, Any]:
        detail = (
            f"Unsupported {event_kind!s}/{phase!s} semantic record: "
            f"{self._bounded(capture.get('name'), 4096) or 'unnamed'}."
        )
        return self._record(
            capture,
            "loss",
            {
                "reason": "unsupported_semantic_projection",
                "stage": "source",
                "count": self._positive_safe_int(semantic_count) or 1,
                "recoverable": False,
                "detail": detail[:4096],
            },
            parent=self._parent(capture),
        )

    def _parent(self, capture: Mapping[str, Any]) -> str | None:
        source_parent = self._source_parent(capture)
        if source_parent is not None:
            return self._records.get(source_parent)
        return self._root(capture)

    def _exact_reference(
        self, value: object, allowed_kinds: set[str]
    ) -> str | None:
        if not isinstance(value, str):
            return None
        record = self._records.get(value)
        metadata = self._record_meta.get(value)
        if record is None or metadata is None or metadata[0] not in allowed_kinds:
            return None
        return record

    def _explicit_parent_unresolved(self, capture: Mapping[str, Any]) -> bool:
        source_parent = self._source_parent(capture)
        return (
            source_parent is not None
            and source_parent not in self._records
            and source_parent not in self._omitted_records
        )

    def _orphan_loss(self, capture: Mapping[str, Any]) -> dict[str, Any]:
        source_parent = self._source_parent(capture)
        return self._record(
            capture,
            "loss",
            {
                "reason": "unresolved_parent",
                "stage": "source",
                "count": 1,
                "recoverable": False,
                "detail": (
                    "Capture row referenced an explicit parent that was not projected: "
                    f"{source_parent!s}"
                )[:4096],
            },
            remember=False,
        )

    @staticmethod
    def _orphan_is_material(
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
        event_kind: object,
        phase: object,
        semantic_type: object,
    ) -> bool:
        if SemanticProjector._material(capture, semantic):
            return True
        if event_kind == "tool" and semantic_type in {
            "tool.proposal",
            "tool.call",
            "tool.execution",
            "tool.result",
            "tool.error",
        }:
            return True
        if semantic_type in {"message", "model.request", "model.response", "verification"}:
            return True
        if event_kind in {"state", "loss", "error"}:
            return True
        return event_kind == "lifecycle" and phase in {
            "start",
            "end",
            "error",
            "cancelled",
        } and (
            semantic_type in _RUN_TYPES
            or semantic_type in {"scope", "agent.scope", "workflow.step"}
        )

    def _context_ref_loss(
        self,
        capture: Mapping[str, Any],
        *,
        affected_record: str,
        count: int,
        parent: str | None,
    ) -> dict[str, Any]:
        loss_capture = dict(capture)
        loss_capture["record_id"] = f"{self._source_record_id(capture)}:context-loss"
        return self._record(
            loss_capture,
            "loss",
            {
                "reason": "unresolved_context_ref",
                "stage": "source",
                "count": count,
                "recoverable": False,
                "detail": (
                    "Model request context included references that did not resolve "
                    "to message, model.response, or tool.result records."
                ),
            },
            parent=parent,
            links=[{"type": "affects", "record": affected_record}],
        )

    def _context_base_ref_loss(
        self,
        capture: Mapping[str, Any],
        *,
        affected_record: str,
        parent: str | None,
    ) -> dict[str, Any]:
        loss_capture = dict(capture)
        loss_capture["record_id"] = (
            f"{self._source_record_id(capture)}:context-base-loss"
        )
        return self._record(
            loss_capture,
            "loss",
            {
                "reason": "unresolved_context_base_ref",
                "stage": "source",
                "count": 1,
                "recoverable": False,
                "detail": (
                    "The context base did not resolve to an earlier expandable "
                    "model request under the same run root, so the context suffix "
                    "was omitted."
                ),
            },
            parent=parent,
            links=[{"type": "affects", "record": affected_record}],
        )

    def _supplemental_loss(
        self,
        capture: Mapping[str, Any],
        *,
        affected: Mapping[str, Any],
        reason: str,
        detail: str,
        count: int = 1,
    ) -> dict[str, Any]:
        loss_capture = dict(capture)
        source_record = self._source_record_id(capture)
        loss_capture["record_id"] = self._hashed_id(
            "loss", f"{source_record}\0{reason}"
        )
        loss_capture["blob_refs"] = []
        parent = affected.get("parent")
        return self._record(
            loss_capture,
            "loss",
            {
                "reason": reason,
                "stage": "source",
                "count": count,
                "recoverable": False,
                "detail": detail,
            },
            parent=parent if isinstance(parent, str) else None,
            links=[{"type": "affects", "record": str(affected["id"])}],
        )

    def _drop_model_request(
        self, request: _ModelRequest
    ) -> None:
        self._model_requests_by_record.pop(request.source_record, None)
        if request.native_key is not None:
            self._model_requests_by_identity.pop(request.native_key, None)
        self._prune_correlation_history()

    @staticmethod
    def _transparent_otel_parent(
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
        semantic_type: object,
    ) -> bool:
        source = capture.get("source")
        return (
            semantic_type in _OMITTED_PARENT_TYPES
            and semantic.get("route") == "otel"
            and isinstance(source, Mapping)
            and source.get("name") == "generic:otel"
        )

    @staticmethod
    def _source_parent(capture: Mapping[str, Any]) -> str | None:
        correlation = capture.get("correlation")
        if isinstance(correlation, Mapping):
            source_parent = correlation.get("parent_record_id")
            if isinstance(source_parent, str):
                return source_parent
        return None

    def _root(self, capture: Mapping[str, Any]) -> str | None:
        root = self._roots.get(self._trace_id(capture))
        return root[1] if root is not None else None

    def _continuation(
        self, capture: Mapping[str, Any]
    ) -> tuple[list[dict[str, str]] | None, bool]:
        previous_turn = capture.get("previous_turn_id")
        previous = (
            self._turns.get(previous_turn)
            if isinstance(previous_turn, str)
            else None
        )
        return (
            (
                [{"type": "continues_from", "record": previous[0]}]
                if previous is not None
                else None
            ),
            (
                isinstance(previous_turn, str)
                and previous is None
                and previous_turn in self._evicted_turns
            ),
        )

    def _remember_turn(self, capture: Mapping[str, Any], record: str) -> None:
        turn_id = capture.get("turn_id")
        if isinstance(turn_id, str):
            self._evicted_turns.pop(turn_id, None)
            self._turns[turn_id] = (record, self._source_record_id(capture))
            self._prune_turn_history()

    def _remember(
        self,
        capture: Mapping[str, Any],
        record_id: str,
        kind: str,
        parent: str | None,
    ) -> None:
        source_record_id = capture.get("record_id")
        if isinstance(source_record_id, str):
            self._records[source_record_id] = record_id
            self._record_meta[source_record_id] = (kind, parent)
            effective_root = (
                record_id
                if kind == "run.start"
                else self._projected_roots.get(parent)
                if parent is not None
                else None
            )
            self._record_roots[source_record_id] = effective_root
            if effective_root is not None:
                self._projected_roots[record_id] = effective_root
            if kind in _CONTEXT_KINDS:
                self._context_records[source_record_id] = record_id
            self._correlation_history[source_record_id] = None
            self._prune_correlation_history(
                extra_pinned=(
                    source_record_id
                    if self._starts_active_correlation(kind, capture)
                    else None
                )
            )

    def _remember_alias(self, source_record: str, record: str) -> None:
        self._records[source_record] = record
        effective_root = self._projected_roots.get(record)
        if effective_root is not None:
            self._record_roots[source_record] = effective_root
        self._correlation_history[source_record] = None
        self._prune_correlation_history()

    def _remember_omitted(self, source_record: str) -> None:
        self._omitted_records.add(source_record)
        self._correlation_history[source_record] = None
        self._prune_correlation_history()

    def _active_source_records(self) -> set[str]:
        active = {root[0] for root in self._roots.values()}
        active.update(self._scopes)
        active.update(
            proposal.source_record for proposal in self._tool_proposals.values()
        )
        active.update(call[2] for call in self._tool_calls.values())
        active.update(self._model_requests_by_record)
        return active

    def _active_correlation_count(self) -> int:
        return (
            len(self._roots)
            + len(self._scopes)
            + len(self._tool_proposals)
            + len(self._tool_calls)
            + len(self._model_requests_by_record)
        )

    def _active_limit_loss(
        self,
        capture: Mapping[str, Any],
        *,
        parent: str | None = None,
    ) -> dict[str, Any]:
        return self._correlation_rejection(
            capture,
            "active_correlation_limit",
            (
                "The active correlation limit was reached; "
                "the new correlation was rejected."
            ),
            parent=parent,
        )

    def _correlation_rejection(
        self,
        capture: Mapping[str, Any],
        reason: str,
        detail: str,
        *,
        parent: str | None = None,
    ) -> dict[str, Any]:
        rejection_capture = capture
        source_record = self._source_record_id(capture)
        if source_record in self._records:
            rejection_capture = dict(capture)
            rejection_capture["record_id"] = self._hashed_id(
                "rec",
                f"{source_record}\0{reason}\0{self._seq + 1}",
            )
        return self._record(
            rejection_capture,
            "loss",
            {
                "reason": reason,
                "stage": "source",
                "count": 1,
                "recoverable": False,
                "detail": detail,
            },
            parent=self._parent(capture) if parent is None else parent,
        )

    def _prune_correlation_history(self, *, extra_pinned: str | None = None) -> None:
        active = self._active_source_records()
        if extra_pinned is not None:
            active.add(extra_pinned)
        completed = sum(
            source_record not in active
            for source_record in self._correlation_history
        )
        for source_record in tuple(self._correlation_history):
            if completed <= self._max_completed_records:
                break
            if source_record in active:
                continue
            self._correlation_history.pop(source_record, None)
            projected_record = self._records.pop(source_record, None)
            record_meta = self._record_meta.pop(source_record, None)
            self._record_roots.pop(source_record, None)
            if projected_record is not None and record_meta is not None:
                self._projected_roots.pop(projected_record, None)
            self._context_records.pop(source_record, None)
            self._expandable_model_requests.discard(source_record)
            self._omitted_records.discard(source_record)
            completed -= 1
        self._prune_turn_history(active)

    def _prune_turn_history(self, active: set[str] | None = None) -> None:
        if active is None:
            active = self._active_source_records()
        completed = sum(
            source_record not in active
            for _, source_record in self._turns.values()
        )
        for turn_id, (_, source_record) in tuple(self._turns.items()):
            if completed <= self._max_completed_records:
                break
            if source_record in active:
                continue
            self._turns.pop(turn_id, None)
            self._evicted_turns[turn_id] = None
            while len(self._evicted_turns) > self._max_completed_records:
                oldest = next(iter(self._evicted_turns))
                self._evicted_turns.pop(oldest, None)
            completed -= 1

    def _unresolved_continuation_loss(
        self,
        capture: Mapping[str, Any],
        record: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._supplemental_loss(
            capture,
            affected=record,
            reason="unresolved_previous_turn",
            detail=(
                "The previous turn reference was no longer available in "
                "bounded correlation history."
            ),
        )

    @staticmethod
    def _starts_active_correlation(
        kind: str,
        capture: Mapping[str, Any],
    ) -> bool:
        if kind in {"run.start", "model.request", "tool.proposal", "tool.call"}:
            return True
        return kind == "scope" and capture.get("phase") == "start"

    def _projected_record_id(self, capture: Mapping[str, Any]) -> str:
        source_record_id = self._source_record_id(capture)
        return self._safe_id(source_record_id) or self._hashed_id(
            "rec", source_record_id
        )

    @classmethod
    def _source_record_id(cls, capture: Mapping[str, Any]) -> str:
        value = capture.get("record_id")
        if isinstance(value, str) and value:
            return value
        seed = "|".join(
            cls._text(capture.get(key), "")
            for key in ("trace_id", "seq", "event_kind", "phase", "name")
        )
        return f"missing:{hashlib.sha256(seed.encode()).hexdigest()}"

    @classmethod
    def _trace_id(cls, capture: Mapping[str, Any]) -> str:
        return cls._text(capture.get("trace_id"), "unknown-trace")

    @staticmethod
    def _source_id(source: object) -> str:
        raw: object = source
        if isinstance(source, Mapping):
            raw = source.get("source_id") or source.get("name") or "unknown"
        text = raw if isinstance(raw, str) else "unknown"
        digest = hashlib.sha256(text.encode()).hexdigest()[:24]
        return f"src_{digest}"

    @staticmethod
    def _hashed_id(prefix: str, value: str) -> str:
        digest = hashlib.sha256(value.encode()).hexdigest()[:24]
        return f"{prefix}_{digest}"

    @staticmethod
    def _time(value: object) -> str:
        if isinstance(value, str):
            return value
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _text(value: object, default: str) -> str:
        return value if isinstance(value, str) and value else default

    @classmethod
    def _short(cls, value: object, default: str) -> str:
        text = cls._text(value, default)
        return text[:256]

    @staticmethod
    def _bounded(value: object, limit: int) -> str | None:
        if not isinstance(value, str) or not value:
            return None
        return value[:limit]

    @classmethod
    def _short_string_list(cls, value: object) -> list[str]:
        if not isinstance(value, list):
            return []
        result: list[str] = []
        for item in value:
            bounded = cls._bounded(item, 256)
            if bounded is not None and bounded not in result:
                result.append(bounded)
        return result

    @classmethod
    def _usage(cls, value: object) -> dict[str, int] | None:
        if not isinstance(value, Mapping):
            return None
        usage: dict[str, int] = {}
        for source_key, target_key in (
            ("input_tokens", "input_tokens"),
            ("output_tokens", "output_tokens"),
        ):
            count = cls._nonnegative_int(value.get(source_key))
            if count is not None:
                usage[target_key] = count
        return usage or None

    @staticmethod
    def _reasoning(value: object) -> list[dict[str, str]]:
        if not isinstance(value, list):
            return []
        reasoning: list[dict[str, str]] = []
        for item in value:
            if not isinstance(item, Mapping):
                continue
            kind = item.get("type")
            text = item.get("text")
            if kind not in {"text", "summary"} or not isinstance(text, str) or not text:
                continue
            reasoning.append({"type": kind, "text": text})
        return reasoning

    @staticmethod
    def _nonnegative_int(value: object) -> int | None:
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            return value
        return None

    @classmethod
    def _run_correlation(cls, value: object) -> dict[str, Any] | None:
        if not isinstance(value, Mapping):
            return None
        task_id = cls._safe_id(value.get("task_id"))
        execution = value.get("execution")
        if task_id is None or not isinstance(execution, Mapping):
            return None
        system = execution.get("system")
        run_id = cls._safe_id(execution.get("run_id"))
        if (
            not isinstance(system, str)
            or re.fullmatch(r"[a-z][a-z0-9._:-]{2,127}", system) is None
            or run_id is None
        ):
            return None
        projected_execution: dict[str, Any] = {
            "system": system,
            "run_id": run_id,
        }
        for key in ("parent_run_id", "root_run_id"):
            identifier = cls._safe_id(execution.get(key))
            if identifier is not None:
                projected_execution[key] = identifier
        attempt = cls._nonnegative_int(execution.get("attempt"))
        if attempt is not None:
            projected_execution["attempt"] = attempt
        return {"task_id": task_id, "execution": projected_execution}

    @classmethod
    def _positive_int(cls, value: object) -> int | None:
        integer = cls._nonnegative_int(value)
        return integer if integer is not None and integer > 0 else None

    @classmethod
    def _positive_safe_int(cls, value: object) -> int | None:
        integer = cls._positive_int(value)
        return integer if integer is not None and integer <= (1 << 53) - 1 else None

    @staticmethod
    def _loss_stage(value: object) -> str:
        if value in {"snapshot", "serialize"}:
            return "serialize"
        if value in {"scrub", "scan"}:
            return "scrub"
        if value in {"queue", "buffer", "persist", "recover"}:
            return str(value)
        return "source"

    @staticmethod
    def _outcome_status(value: object, phase: object) -> str:
        if phase == "error":
            return "failed"
        if phase == "cancelled":
            return "cancelled"
        if value in {"succeeded", "completed"}:
            return "completed"
        if value in {"failed", "cancelled", "unknown"}:
            return str(value)
        if phase == "end":
            return "completed"
        return "unknown"

    @classmethod
    def _scope_status(cls, value: object, phase: object) -> str:
        if phase == "error":
            return "failed"
        if phase == "cancelled":
            return "cancelled"
        if value == "succeeded":
            return "completed"
        if value in _SCOPE_STATUSES:
            return str(value)
        return "completed" if phase == "end" else "unknown"

    @staticmethod
    def _tool_status(value: object, phase: object, semantic_type: object) -> str:
        if semantic_type == "tool.error" or phase == "error":
            return "failed"
        if phase == "cancelled":
            return "cancelled"
        if value in {"succeeded", "failed", "cancelled"}:
            return str(value)
        return "succeeded"

    @staticmethod
    def _redundant_control(
        capture: Mapping[str, Any],
        semantic: Mapping[str, Any],
        semantic_type: object,
    ) -> bool:
        if semantic_type == "capture.redundant":
            return True
        if (
            capture.get("event_kind") == "correlation"
            and capture.get("phase") == "gap"
            and capture.get("name") in _RUNTIME_GAP_NAMES
        ):
            return True
        if capture.get("event_kind") != "correlation" or capture.get("native") is not None:
            return False
        if semantic_type not in _REDUNDANT_CONTROL_TYPES:
            return False
        return all(
            key in {"type", "framework", "provider", "origin"} for key in semantic
        )

    @staticmethod
    def _material(
        capture: Mapping[str, Any], semantic: Mapping[str, Any]
    ) -> bool:
        if capture.get("native") is not None:
            return True
        return any(
            key not in {"type", "framework", "provider", "origin"}
            for key in semantic
        )

    @staticmethod
    def _safe_id(value: object) -> str | None:
        if isinstance(value, str) and _SAFE_ID.fullmatch(value):
            return value
        return None
