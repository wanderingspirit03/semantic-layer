"""Official google adk adapter capture adapter."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, cast

from pydantic import BaseModel

from ._framework_adapter_shared import (
    _framework_metadata,
    _FrameworkAdapter,
    _installed_version,
    _Lifecycle,
    _OpenTrace,
    _raw_native,
    _source_qualification,
)
from .capture_v1 import AdmissionReceipt, CaptureSource

_PairKind = Literal["model", "tool"]
_PairPhase = Literal["start", "end", "error"]


@dataclass(frozen=True)
class _PendingFailure:
    pair: tuple[str, str, str]
    kind: _PairKind
    error: BaseException
    native: Any
    semantic: dict[str, Any]
    branch: str | None
    agent_name: str | None


def google_adk_adapter(*, version: str | None = None) -> _FrameworkAdapter:
    """Capture Google ADK through an additive official Runner plugin."""

    return _FrameworkAdapter(_installed_version("google-adk", version), _GoogleADKSource)


class _GoogleADKSource(CaptureSource):
    def __init__(self, runner: object, version: str) -> None:
        self.runner = runner
        self.metadata = _framework_metadata(
            "google-adk",
            "plugin_manager.register_plugin/BasePlugin callbacks",
            "google-adk.invocation",
            version,
        )
        self.metadata["qualification"] = _source_qualification(
            version,
            exact_versions=frozenset({"2.4.0"}),
            profile="google-adk-python-adapter-v1",
        )

    def install(self, sink: Any) -> _Lifecycle:
        manager = getattr(self.runner, "plugin_manager", None)
        register = getattr(manager, "register_plugin", None)
        if not callable(register):
            raise TypeError("Google ADK subject must expose plugin_manager.register_plugin")
        try:
            from google.adk.plugins import BasePlugin
        except ImportError as error:  # pragma: no cover
            raise RuntimeError("google-adk is required for the Google ADK adapter") from error
        active = True
        traces: dict[str, _OpenTrace] = {}
        starts: dict[tuple[str, str, str], AdmissionReceipt] = {}
        model_call_indexes: dict[str, int] = {}
        model_call_identities: dict[tuple[str, int], list[str]] = {}
        pending_model_identities: dict[str, list[str]] = {}
        model_reasoning_chunks: dict[tuple[str, int], list[str]] = {}
        failures: dict[tuple[str, _PairKind, str], _PendingFailure] = {}
        final_outputs: dict[str, tuple[Any, AdmissionReceipt | None]] = {}
        turn_indexes: dict[str, int] = {}
        state_events: set[tuple[str, str]] = set()
        context_records: dict[tuple[str, int], tuple[Any, AdmissionReceipt]] = {}
        system_context_records: dict[str, tuple[Any, AdmissionReceipt]] = {}
        response_contents: dict[tuple[str, int], tuple[Any, AdmissionReceipt]] = {}
        proposal_parents: dict[tuple[str, str], AdmissionReceipt] = {}
        event_proposals: set[tuple[str, str]] = set()
        tool_results: dict[tuple[str, str], AdmissionReceipt] = {}

        class CapturePlugin(BasePlugin):
            def __init__(self) -> None:
                super().__init__(name=f"semantic-layer-{id(self):x}")

            async def before_run_callback(self, *, invocation_context: Any) -> None:
                if not active:
                    return
                key = _adk_invocation_id(invocation_context)
                conversation = _adk_session_id(invocation_context)
                index = turn_indexes.get(conversation, -1) + 1
                turn_indexes[conversation] = index
                opened = sink.open_trace(
                    {
                        "name": "google_adk.invocation",
                        "native_identity": key,
                        "conversation_id": conversation,
                        "turn_id": f"{conversation}:{index}",
                        "turn_index": index,
                        **({"previous_turn_id": f"{conversation}:{index - 1}"} if index else {}),
                        "native": _adk_invocation_snapshot(invocation_context),
                        "semantic": {
                            "type": "agent.run",
                            "framework": "google-adk",
                            "name": "google_adk.invocation",
                            **_adk_invocation_input_semantic(invocation_context),
                        },
                    }
                )
                if opened.accepted and opened.identity is not None:
                    traces[key] = _OpenTrace(dict(opened.identity), "google_adk.invocation")
                    initial_state = _adk_invocation_state_delta(invocation_context)
                    if initial_state is not None:
                        event_id, state_delta = initial_state
                        _record_state(key, opened.identity, event_id, state_delta)

            async def after_run_callback(self, *, invocation_context: Any) -> None:
                _finish(invocation_context)

            async def before_model_callback(
                self, *, callback_context: Any, llm_request: Any
            ) -> None:
                reasoning_marker = (
                    _context_invocation_id(callback_context),
                    _adk_model_call_marker(callback_context),
                )
                model_reasoning_chunks.pop(reasoning_marker, None)
                semantic = _adk_model_request_semantic(
                    llm_request,
                    response_schema=_adk_callback_output_schema(callback_context),
                )
                semantic["context_refs"] = _model_context_refs(
                    callback_context,
                    llm_request,
                )
                _pair(
                    callback_context,
                    "model",
                    "google_adk.model.call",
                    "start",
                    _adk_model_request_snapshot(llm_request),
                    semantic,
                )

            async def after_model_callback(
                self, *, callback_context: Any, llm_response: Any
            ) -> None:
                reasoning_marker = (
                    _context_invocation_id(callback_context),
                    _adk_model_call_marker(callback_context),
                )
                if getattr(llm_response, "partial", False):
                    for block in _adk_reasoning(getattr(llm_response, "content", None)):
                        model_reasoning_chunks.setdefault(reasoning_marker, []).append(
                            block["text"]
                        )
                    snapshot = _adk_model_response_snapshot(llm_response)
                    content = snapshot.pop("content", None)
                    _single(
                        callback_context,
                        "stream",
                        "google_adk.model.delta",
                        {
                            "delta": _raw_native(content),
                            **snapshot,
                        },
                        "capture.redundant",
                    )
                    return
                _resolve_failure(callback_context, "model")
                semantic = _adk_model_response_semantic(llm_response)
                partial_reasoning = model_reasoning_chunks.pop(reasoning_marker, [])
                if partial_reasoning and not semantic.get("reasoning"):
                    semantic["reasoning"] = [{"type": "text", "text": "".join(partial_reasoning)}]
                receipt = _pair(
                    callback_context,
                    "model",
                    "google_adk.model.call",
                    "end",
                    _adk_model_response_snapshot(llm_response),
                    semantic,
                )
                if receipt is None or not receipt.accepted or receipt.record_id is None:
                    return
                key = _context_invocation_id(callback_context)
                content = getattr(llm_response, "content", None)
                if content is not None:
                    response_contents[(key, id(content))] = (content, receipt)
                for call_id in _adk_function_call_ids(content):
                    proposal_parents[(key, call_id)] = receipt

            async def before_tool_callback(
                self, *, tool: Any, tool_args: dict[str, Any], tool_context: Any
            ) -> None:
                key = _context_invocation_id(tool_context)
                call_id = _adk_tool_call_id(tool_context)
                proposal_parent = (
                    proposal_parents.pop((key, call_id), None) if call_id is not None else None
                )
                if proposal_parent is None and call_id is not None:
                    proposal_parent = _response_for_call_id(key, call_id)
                if call_id is None or (key, call_id) not in event_proposals:
                    _single(
                        tool_context,
                        "tool",
                        "google_adk.tool.proposal",
                        _adk_tool_snapshot(tool, tool_args),
                        "tool.proposal",
                        semantic={
                            **_adk_tool_semantic(tool, tool_args, tool_context),
                            "type": "tool.proposal",
                        },
                        parent=proposal_parent,
                    )
                _pair(
                    tool_context,
                    "tool",
                    "google_adk.tool.call",
                    "start",
                    _adk_tool_snapshot(tool, tool_args),
                    _adk_tool_semantic(tool, tool_args, tool_context),
                )

            async def after_tool_callback(
                self, *, tool: Any, tool_args: dict[str, Any], tool_context: Any, result: Any
            ) -> None:
                _resolve_failure(tool_context, "tool")
                receipt = _pair(
                    tool_context,
                    "tool",
                    "google_adk.tool.call",
                    "end",
                    _adk_tool_snapshot(tool, tool_args, result=result),
                    _adk_tool_result_semantic(tool_context, result),
                )
                key = _context_invocation_id(tool_context)
                call_id = _adk_tool_call_id(tool_context)
                if (
                    call_id is not None
                    and receipt is not None
                    and receipt.accepted
                    and receipt.record_id is not None
                ):
                    tool_results[(key, call_id)] = receipt
                _single(
                    tool_context,
                    "tool",
                    "google_adk.tool.result",
                    _adk_tool_snapshot(tool, tool_args, result=result),
                    "capture.redundant",
                )

            async def on_model_error_callback(
                self, *, callback_context: Any, llm_request: Any, error: Exception
            ) -> None:
                if not active:
                    return
                model_reasoning_chunks.pop(
                    (
                        _context_invocation_id(callback_context),
                        _adk_model_call_marker(callback_context),
                    ),
                    None,
                )
                _remember_failure(
                    callback_context,
                    "model",
                    "google_adk.model.call",
                    {"request": llm_request},
                    {
                        "type": "model.response",
                        "framework": "google-adk",
                        "status": "failed",
                    },
                    error=error,
                )

            async def on_tool_error_callback(
                self, *, tool: Any, tool_args: dict[str, Any], tool_context: Any, error: Exception
            ) -> None:
                if not active:
                    return
                _remember_failure(
                    tool_context,
                    "tool",
                    "google_adk.tool.call",
                    {"tool": tool, "args": tool_args},
                    {
                        **_adk_tool_semantic(tool, tool_args, tool_context),
                        "type": "tool.error",
                        "status": "failed",
                        "error": _adk_semantic_error(error),
                    },
                    error=error,
                )

            async def on_event_callback(self, *, invocation_context: Any, event: Any) -> None:
                key = _adk_invocation_id(invocation_context)
                opened = traces.get(key)
                if active and opened is not None:
                    _resolve_model_failure_from_event(key, event, opened)
                    _resolve_tool_failures_from_event(key, event, opened)
                    terminal = _adk_event_is_final(event)
                    content = getattr(event, "content", None)
                    _record_event_proposals(
                        key,
                        opened.identity,
                        content,
                    )
                    snapshot = _adk_event_snapshot(event)
                    if terminal:
                        output = content
                        if output is None:
                            output = getattr(event, "output", None)
                        final_outputs[key] = (
                            _adk_compact_native(output),
                            _exact_receipt(response_contents, key, output),
                        )
                    if getattr(event, "partial", False):
                        content = snapshot.pop("content", None)
                        snapshot = {
                            "delta": _raw_native(content),
                            **snapshot,
                        }
                    sink.record(
                        {
                            "kind": "stream",
                            "phase": "event",
                            "name": "google_adk.event",
                            "trace": opened.identity,
                            "native": snapshot,
                            "semantic": {
                                "type": (
                                    "capture.redundant"
                                    if _adk_event_is_redundant(event, terminal)
                                    else "stream.event"
                                ),
                                "framework": "google-adk",
                            },
                        }
                    )
                    usage = getattr(event, "usage_metadata", None)
                    if usage is not None:
                        sink.record(
                            {
                                "kind": "model",
                                "phase": "event",
                                "name": "google_adk.usage",
                                "trace": opened.identity,
                                "native": _adk_usage_snapshot(usage),
                                "semantic": {
                                    "type": "capture.redundant",
                                    "framework": "google-adk",
                                },
                            }
                        )
                    actions = getattr(event, "actions", None)
                    state_delta = getattr(actions, "state_delta", None)
                    if state_delta:
                        result = _exact_tool_result(key, event)
                        _record_state(
                            key,
                            opened.identity,
                            getattr(event, "id", None),
                            state_delta,
                            result=result,
                        )

        def _record_state(
            key: str,
            trace: dict[str, str],
            event_id: Any,
            state_delta: Any,
            *,
            result: AdmissionReceipt | None = None,
        ) -> None:
            identity = str(event_id or f"{key}:state:{len(state_events)}")
            marker = (key, identity)
            if marker in state_events:
                return
            state_events.add(marker)
            semantic: dict[str, Any] = {
                "type": "state.transition",
                "framework": "google-adk",
                "state_type": "session.state_delta",
                "value": _adk_compact_native(state_delta),
            }
            if result is not None and result.accepted and result.record_id is not None:
                semantic["result_ref"] = result.record_id
            sink.record(
                {
                    "kind": "state",
                    "phase": "event",
                    "name": "google_adk.state.transition",
                    "trace": trace,
                    "native_identity": identity,
                    "native": _adk_compact_native(state_delta),
                    "semantic": semantic,
                }
            )

        def _model_context_refs(context: Any, request: Any) -> list[str]:
            key = _context_invocation_id(context)
            opened = traces.get(key)
            if opened is None:
                return []
            invocation_context = getattr(context, "_invocation_context", context)
            session = getattr(invocation_context, "session", None)
            event_contents: dict[int, tuple[Any, Any]] = {}
            for event in getattr(session, "events", None) or []:
                content = getattr(event, "content", None)
                if content is not None:
                    event_contents[id(content)] = (content, event)

            refs: list[str] = []
            config = getattr(request, "config", None)
            system_instruction = getattr(config, "system_instruction", None)
            if system_instruction is not None:
                projected_instruction = _adk_compact_native(system_instruction)
                prior = system_context_records.get(key)
                if prior is not None and _adk_context_equal(prior[0], projected_instruction):
                    receipt = prior[1]
                else:
                    receipt = sink.record(
                        {
                            "kind": "log",
                            "phase": "event",
                            "name": "google_adk.context.system",
                            "trace": opened.identity,
                            "native": projected_instruction,
                            "semantic": {
                                "type": "message",
                                "framework": "google-adk",
                                "role": "system",
                                "content": projected_instruction,
                            },
                        }
                    )
                    system_context_records[key] = (projected_instruction, receipt)
                if receipt.accepted and receipt.record_id is not None:
                    refs.append(receipt.record_id)
            for content in getattr(request, "contents", None) or []:
                response = _exact_receipt(response_contents, key, content)
                if response is not None and response.record_id is not None:
                    refs.append(response.record_id)
                    continue
                causal_receipts = _content_causal_receipts(key, content)
                if causal_receipts:
                    refs.extend(
                        receipt.record_id
                        for receipt in causal_receipts
                        if receipt.record_id is not None
                    )
                    continue
                event_entry = event_contents.get(id(content))
                if event_entry is None or event_entry[0] is not content:
                    continue
                event = event_entry[1]
                marker = (key, id(content))
                prior = context_records.get(marker)
                if prior is not None and prior[0] is content:
                    receipt = prior[1]
                else:
                    event_id = getattr(event, "id", None)
                    receipt = sink.record(
                        {
                            "kind": "log",
                            "phase": "event",
                            "name": "google_adk.context.message",
                            "trace": opened.identity,
                            "native_identity": str(
                                event_id or f"{key}:context:{len(context_records)}"
                            ),
                            "native": _adk_compact_native(content),
                            "semantic": {
                                "type": "message",
                                "framework": "google-adk",
                                "role": _adk_content_role(content),
                                "content": _adk_compact_native(content),
                            },
                        }
                    )
                    context_records[marker] = (content, receipt)
                if receipt.accepted and receipt.record_id is not None:
                    refs.append(receipt.record_id)
            return refs

        def _content_causal_receipts(key: str, content: Any) -> list[AdmissionReceipt]:
            receipts: list[AdmissionReceipt] = []
            for call_id in _adk_function_call_ids(content):
                response = _response_for_call_id(key, call_id)
                if response is None:
                    return []
                if response not in receipts:
                    receipts.append(response)
            for call_id in _adk_function_response_ids(content):
                result = tool_results.get((key, call_id))
                if result is None:
                    return []
                if result not in receipts:
                    receipts.append(result)
            return receipts

        def _response_for_call_id(key: str, call_id: str) -> AdmissionReceipt | None:
            matches: list[AdmissionReceipt] = []
            for (candidate_key, _), (content, receipt) in response_contents.items():
                if (
                    candidate_key == key
                    and call_id in _adk_function_call_ids(content)
                    and receipt not in matches
                ):
                    matches.append(receipt)
            return matches[0] if len(matches) == 1 else None

        def _exact_tool_result(key: str, event: Any) -> AdmissionReceipt | None:
            call_ids = _adk_function_response_ids(getattr(event, "content", None))
            if len(call_ids) != 1:
                return None
            return tool_results.get((key, call_ids[0]))

        def _record_event_proposals(
            key: str,
            trace: dict[str, str],
            content: Any,
        ) -> None:
            for call in _adk_function_calls(content):
                call_id = getattr(call, "id", None)
                name = getattr(call, "name", None)
                if (
                    not isinstance(call_id, str)
                    or not call_id
                    or not isinstance(name, str)
                    or not name
                    or (key, call_id) in event_proposals
                ):
                    continue
                args = getattr(call, "args", None)
                semantic = {
                    "type": "tool.proposal",
                    "framework": "google-adk",
                    "name": name,
                    "input": _adk_compact_native(dict(args) if isinstance(args, dict) else args),
                    "native_call_id": call_id,
                }
                value = {
                    "kind": "tool",
                    "phase": "event",
                    "name": "google_adk.tool.proposal",
                    "trace": trace,
                    "native_identity": call_id,
                    "native": _adk_compact_native(call),
                    "semantic": semantic,
                }
                parent = proposal_parents.get((key, call_id))
                if parent is not None and parent.accepted and parent.record_id is not None:
                    value["parent_record_id"] = parent.record_id
                sink.record(value)
                event_proposals.add((key, call_id))

        def _context_invocation_id(context: Any) -> str:
            invocation_context = getattr(context, "_invocation_context", context)
            return _adk_invocation_id(invocation_context)

        def _failure_marker(
            context: Any,
            kind: _PairKind,
        ) -> tuple[str, _PairKind, str]:
            key = _context_invocation_id(context)
            if kind == "model":
                identity = str(_adk_model_call_marker(context))
            else:
                identity = _adk_tool_call_id(context) or str(id(context))
            return (key, kind, identity)

        def _current_pair(
            context: Any,
            kind: _PairKind,
            name: str,
        ) -> tuple[str, str, str]:
            key = _context_invocation_id(context)
            if kind == "model":
                identities = model_call_identities.get(
                    (key, _adk_model_call_marker(context)),
                    [],
                )
                native_identity = identities[0] if identities else None
                if native_identity is None:
                    pending = pending_model_identities.get(key, [])
                    native_identity = pending[0] if pending else None
            else:
                native_identity = _adk_tool_call_id(context)
            return (key, name, native_identity or f"{key}:{name}")

        def _remember_failure(
            context: Any,
            kind: _PairKind,
            name: str,
            native: Any,
            semantic: dict[str, Any],
            *,
            error: BaseException,
        ) -> None:
            pair = _current_pair(context, kind, name)
            invocation_context = getattr(context, "_invocation_context", context)
            branch = getattr(invocation_context, "branch", None)
            agent = getattr(invocation_context, "agent", None)
            agent_name = getattr(agent, "name", None)
            failures[_failure_marker(context, kind)] = _PendingFailure(
                pair=pair,
                kind=kind,
                error=error,
                native=_adk_compact_native(native),
                semantic=semantic,
                branch=branch if isinstance(branch, str) else None,
                agent_name=agent_name if isinstance(agent_name, str) else None,
            )

        def _resolve_failure(context: Any, kind: _PairKind) -> None:
            failure = failures.pop(_failure_marker(context, kind), None)
            if failure is not None:
                opened = traces.get(failure.pair[0])
                if opened is not None:
                    _record_failure_error(failure, opened, recoverable=True)

        def _resolve_model_failure_from_event(
            key: str,
            event: Any,
            opened: _OpenTrace,
        ) -> None:
            actions = getattr(event, "actions", None)
            marker: tuple[str, _PairKind, str] | None = (
                (key, "model", str(id(actions))) if actions is not None else None
            )
            failure = failures.pop(marker, None) if marker is not None else None
            if (
                failure is None
                and not getattr(event, "partial", False)
                and getattr(event, "content", None) is not None
            ):
                branch = getattr(event, "branch", None)
                author = getattr(event, "author", None)
                candidates = [
                    (candidate_marker, candidate)
                    for candidate_marker, candidate in failures.items()
                    if candidate_marker[0] == key
                    and candidate_marker[1] == "model"
                    and candidate.branch == (branch if isinstance(branch, str) else None)
                    and (candidate.agent_name is None or candidate.agent_name == author)
                ]
                if len(candidates) == 1:
                    candidate_marker, failure = candidates[0]
                    failures.pop(candidate_marker, None)
            if failure is not None:
                _record_failure_error(failure, opened, recoverable=True)
                content = getattr(event, "content", None)
                value: dict[str, Any] = {
                    "kind": "model",
                    "phase": "end",
                    "name": failure.pair[1],
                    "trace": opened.identity,
                    "native_identity": failure.pair[2],
                    "native": _adk_model_response_snapshot(event),
                    "semantic": _adk_model_response_semantic(event),
                }
                start = starts.pop(failure.pair, None)
                if start is not None and start.accepted and start.record_id is not None:
                    value["parent_record_id"] = start.record_id
                receipt = cast(AdmissionReceipt, sink.record(value))
                if content is not None and receipt.accepted and receipt.record_id is not None:
                    response_contents[(key, id(content))] = (content, receipt)
                    for call_id in _adk_function_call_ids(content):
                        proposal_parents[(key, call_id)] = receipt

        def _resolve_tool_failures_from_event(
            key: str,
            event: Any,
            opened: _OpenTrace,
        ) -> None:
            for response in _adk_function_responses(getattr(event, "content", None)):
                call_id = getattr(response, "id", None)
                if not isinstance(call_id, str) or not call_id:
                    continue
                marker: tuple[str, _PairKind, str] = (key, "tool", call_id)
                failure = failures.pop(marker, None)
                if failure is None:
                    continue
                _record_failure_error(failure, opened, recoverable=True)
                output = getattr(response, "response", None)
                value: dict[str, Any] = {
                    "kind": "tool",
                    "phase": "end",
                    "name": failure.pair[1],
                    "trace": opened.identity,
                    "native_identity": call_id,
                    "native": _adk_compact_native(response),
                    "semantic": {
                        "type": "tool.result",
                        "framework": "google-adk",
                        "status": "succeeded",
                        "output": _adk_compact_native(output),
                        "native_call_id": call_id,
                    },
                }
                start = starts.pop(failure.pair, None)
                if start is not None and start.accepted and start.record_id is not None:
                    value["parent_record_id"] = start.record_id
                receipt = cast(AdmissionReceipt, sink.record(value))
                if receipt.accepted and receipt.record_id is not None:
                    tool_results[(key, call_id)] = receipt

        def _pair(
            context: Any,
            kind: _PairKind,
            name: str,
            phase: _PairPhase,
            native: Any,
            semantic: dict[str, Any],
            *,
            error: BaseException | None = None,
        ) -> AdmissionReceipt | None:
            if not active:
                return None
            key = _context_invocation_id(context)
            opened = traces.get(key)
            if opened is None:
                return None
            call_identity = _adk_tool_call_id(context) if kind == "tool" else None
            if kind == "model":
                marker = (key, _adk_model_call_marker(context))
                native_identity: str | None
                if phase == "start":
                    index = model_call_indexes.get(key, 0)
                    model_call_indexes[key] = index + 1
                    native_identity = f"{key}:{name}:{index}"
                    model_call_identities.setdefault(marker, []).append(native_identity)
                    pending_model_identities.setdefault(key, []).append(native_identity)
                else:
                    identities = model_call_identities.get(marker, [])
                    native_identity = identities.pop(0) if identities else None
                    if not identities:
                        model_call_identities.pop(marker, None)
                    pending = pending_model_identities.get(key, [])
                    if native_identity is None and pending:
                        native_identity = pending[0]
                    if native_identity in pending:
                        pending.remove(native_identity)
                    if not pending:
                        pending_model_identities.pop(key, None)
                    if native_identity is None:
                        index = model_call_indexes.get(key, 0)
                        model_call_indexes[key] = index + 1
                        native_identity = f"{key}:{name}:{index}"
            else:
                native_identity = call_identity or f"{key}:{name}"
            value: dict[str, Any] = {
                "kind": kind,
                "phase": phase,
                "name": name,
                "trace": opened.identity,
                "native_identity": native_identity,
                "native": _raw_native(native),
                "semantic": semantic,
            }
            if phase == "error" and error is not None:
                value["error_identity"] = error
            pair = (key, name, native_identity)
            if phase == "start":
                receipt = cast(AdmissionReceipt, sink.record(value))
                starts[pair] = receipt
                return receipt
            else:
                start = starts.pop(pair, None)
                if start is not None and start.accepted and start.record_id is not None:
                    value["parent_record_id"] = start.record_id
                receipt = cast(AdmissionReceipt, sink.record(value))
                if phase == "error" and error is not None:
                    sink.record(
                        {
                            "kind": "error",
                            "phase": "event",
                            "name": f"{name}.error",
                            "trace": opened.identity,
                            "error_identity": error,
                            "native": _adk_error_native(error),
                            "semantic": {
                                "type": "error",
                                "framework": "google-adk",
                                "error": _adk_semantic_error(error),
                            },
                        }
                    )
                return receipt

        def _single(
            context: Any,
            kind: str,
            name: str,
            native: Any,
            semantic_type: str,
            *,
            semantic: dict[str, Any] | None = None,
            parent: AdmissionReceipt | None = None,
        ) -> AdmissionReceipt | None:
            if not active:
                return None
            key = _context_invocation_id(context)
            opened = traces.get(key)
            if opened is not None:
                value = {
                    "kind": kind,
                    "phase": "event",
                    "name": name,
                    "trace": opened.identity,
                    "native_identity": _adk_tool_call_id(context),
                    "native": _adk_compact_native(native),
                    "semantic": semantic
                    or {
                        "type": semantic_type,
                        "framework": "google-adk",
                    },
                }
                if parent is not None and parent.accepted and parent.record_id is not None:
                    value["parent_record_id"] = parent.record_id
                return cast(AdmissionReceipt, sink.record(value))
            return None

        def _finish(context: Any) -> None:
            if not active:
                return
            key = _adk_invocation_id(context)
            _finish_key(key, context)

        def _finish_key(key: str, context: Any | None) -> None:
            opened = traces.pop(key, None)
            error = _finish_failures(key, opened)
            if opened is not None:
                _record_unmatched_starts(key, opened.identity)
            _clear_model_calls(key)
            _clear_starts(key)
            final_output = final_outputs.pop(key, None)
            if opened is not None:
                semantic: dict[str, Any] = {
                    "type": "agent.run",
                    "framework": "google-adk",
                    "status": "failed" if error is not None else "succeeded",
                }
                if error is not None:
                    semantic["error"] = _adk_semantic_error(error)
                elif final_output is not None:
                    output, response = final_output
                    if (
                        response is not None
                        and response.accepted
                        and response.record_id is not None
                    ):
                        semantic["output_ref"] = response.record_id
                    else:
                        semantic["output"] = output
                sink.record(
                    {
                        "kind": "lifecycle",
                        "phase": "error" if error is not None else "end",
                        "name": opened.name,
                        "trace": opened.identity,
                        "native_identity": key,
                        **({"error_identity": error} if error is not None else {}),
                        "native": (
                            _adk_invocation_snapshot(context)
                            if context is not None
                            else {"invocation_id": key}
                        ),
                        "semantic": semantic,
                    }
                )
            _clear_trace_evidence(key)

        def _finish_failures(
            key: str,
            opened: _OpenTrace | None,
        ) -> BaseException | None:
            pending = [
                (marker, failure) for marker, failure in failures.items() if marker[0] == key
            ]
            for marker, failure in pending:
                failures.pop(marker, None)
                if opened is None:
                    continue
                value: dict[str, Any] = {
                    "kind": failure.kind,
                    "phase": "error",
                    "name": failure.pair[1],
                    "trace": opened.identity,
                    "native_identity": failure.pair[2],
                    "error_identity": failure.error,
                    "native": failure.native,
                    "semantic": failure.semantic,
                }
                start = starts.pop(failure.pair, None)
                if start is not None and start.accepted and start.record_id is not None:
                    value["parent_record_id"] = start.record_id
                sink.record(value)
                _record_failure_error(
                    failure,
                    opened,
                    recoverable=False,
                    parent=start,
                )
            return pending[0][1].error if pending else None

        def _record_failure_error(
            failure: _PendingFailure,
            opened: _OpenTrace,
            *,
            recoverable: bool,
            parent: AdmissionReceipt | None = None,
        ) -> None:
            semantic_error = _adk_semantic_error(failure.error)
            semantic_error["recoverable"] = recoverable
            value: dict[str, Any] = {
                "kind": "error",
                "phase": "event",
                "name": f"{failure.pair[1]}.error",
                "trace": opened.identity,
                "native_identity": failure.pair[2],
                "error_identity": failure.error,
                "native": _adk_error_native(failure.error),
                "semantic": {
                    "type": "error",
                    "framework": "google-adk",
                    "error": semantic_error,
                },
            }
            start = parent or starts.get(failure.pair)
            if start is not None and start.accepted and start.record_id is not None:
                value["parent_record_id"] = start.record_id
            sink.record(value)

        def _record_unmatched_starts(key: str, trace: dict[str, str]) -> None:
            unmatched = [
                (marker, receipt) for marker, receipt in starts.items() if marker[0] == key
            ]
            for marker, receipt in unmatched:
                value: dict[str, Any] = {
                    "kind": "unknown",
                    "phase": "gap",
                    "name": "google_adk.pair.completion.gap",
                    "trace": trace,
                    "native_identity": marker[2],
                    "native": {
                        "operation": marker[1],
                        "native_identity": marker[2],
                    },
                    "semantic": {
                        "type": "capture.gap",
                        "framework": "google-adk",
                        "reason": "pair_completion_not_observed",
                        "count": 1,
                        "detail": (
                            f"Google ADK did not deliver a terminal callback for {marker[1]}."
                        ),
                    },
                }
                if receipt.accepted and receipt.record_id is not None:
                    value["parent_record_id"] = receipt.record_id
                sink.record(value)
                starts.pop(marker, None)

        def _clear_trace_evidence(key: str) -> None:
            system_context_records.pop(key, None)
            for context_marker in [marker for marker in context_records if marker[0] == key]:
                context_records.pop(context_marker, None)
            for response_marker in [marker for marker in response_contents if marker[0] == key]:
                response_contents.pop(response_marker, None)
            for proposal_marker in [marker for marker in proposal_parents if marker[0] == key]:
                proposal_parents.pop(proposal_marker, None)
            for result_marker in [marker for marker in tool_results if marker[0] == key]:
                tool_results.pop(result_marker, None)
            for event_marker in [marker for marker in event_proposals if marker[0] == key]:
                event_proposals.discard(event_marker)
            for state_marker in [marker for marker in state_events if marker[0] == key]:
                state_events.discard(state_marker)

        def _clear_model_calls(key: str) -> None:
            model_call_indexes.pop(key, None)
            pending_model_identities.pop(key, None)
            for marker in [marker for marker in model_call_identities if marker[0] == key]:
                model_call_identities.pop(marker, None)
            for marker in [marker for marker in model_reasoning_chunks if marker[0] == key]:
                model_reasoning_chunks.pop(marker, None)

        def _clear_starts(key: str) -> None:
            for marker in [marker for marker in starts if marker[0] == key]:
                starts.pop(marker, None)

        plugin = CapturePlugin()
        try:
            register(plugin)
        except BaseException:
            active = False
            raise

        def deactivate() -> None:
            nonlocal active, sink
            failed_keys = {marker[0] for marker in failures}
            for key in list(traces):
                if key in failed_keys:
                    _finish_key(key, None)
                else:
                    _record_unmatched_starts(key, traces[key].identity)
            active = False
            traces.clear()
            starts.clear()
            model_call_indexes.clear()
            model_call_identities.clear()
            pending_model_identities.clear()
            model_reasoning_chunks.clear()
            failures.clear()
            final_outputs.clear()
            turn_indexes.clear()
            state_events.clear()
            context_records.clear()
            system_context_records.clear()
            response_contents.clear()
            proposal_parents.clear()
            event_proposals.clear()
            tool_results.clear()
            sink = None

        return _Lifecycle(deactivate)


def _adk_invocation_id(context: Any) -> str:
    return str(getattr(context, "invocation_id", None) or id(context))


def _adk_model_call_marker(context: Any) -> int:
    actions = getattr(context, "actions", None)
    return id(actions) if actions is not None else id(context)


def _adk_error_native(error: BaseException) -> dict[str, Any]:
    return {"error": _adk_error_details(error)}


def _adk_error_details(error: BaseException) -> dict[str, Any]:
    try:
        owned = object.__getattribute__(error, "__dict__")
    except (AttributeError, TypeError):
        owned = {}
    message = owned.get("message") if isinstance(owned, dict) else None
    if not isinstance(message, str) or not message:
        try:
            args = object.__getattribute__(error, "args")
        except (AttributeError, TypeError):
            args = ()
        message = args[0] if args and isinstance(args[0], str) and args[0] else type(error).__name__
    details: dict[str, Any] = {
        "type": type(error).__name__,
        "message": message,
    }
    if isinstance(owned, dict):
        code = owned.get("code")
        status = owned.get("status")
        if isinstance(code, int) and not isinstance(code, bool):
            details["code"] = code
        if isinstance(status, str) and status:
            details["status"] = status
    return details


def _adk_session_id(context: Any) -> str:
    session = getattr(context, "session", None)
    return str(getattr(session, "id", None) or getattr(context, "session_id", "adk-default"))


def _adk_tool_call_id(context: Any) -> str | None:
    for name in ("function_call_id", "tool_call_id"):
        value = getattr(context, name, None)
        if value:
            return str(value)
    return None


def _adk_invocation_snapshot(context: Any) -> dict[str, Any]:
    session = getattr(context, "session", None)
    agent = getattr(context, "agent", None)
    return _adk_compact_dict(
        {
            "invocation_id": getattr(context, "invocation_id", None),
            "session_id": getattr(session, "id", None),
            "user_id": getattr(session, "user_id", None),
            "agent_name": getattr(agent, "name", None),
        }
    )


def _adk_invocation_input_semantic(context: Any) -> dict[str, Any]:
    content = getattr(context, "user_content", None)
    return {"input": _adk_compact_native(content)} if content is not None else {}


def _adk_invocation_state_delta(context: Any) -> tuple[Any, Any] | None:
    """Find the native user event that ADK appends before ``before_run``."""

    session = getattr(context, "session", None)
    invocation_id = getattr(context, "invocation_id", None)
    for event in reversed(getattr(session, "events", None) or []):
        if getattr(event, "invocation_id", None) != invocation_id:
            continue
        actions = getattr(event, "actions", None)
        state_delta = getattr(actions, "state_delta", None)
        if state_delta:
            return getattr(event, "id", None), state_delta
    return None


def _adk_model_request_snapshot(request: Any) -> dict[str, Any]:
    return _adk_compact_dict(
        {
            "model": getattr(request, "model", None),
            "contents": getattr(request, "contents", None),
            "config": getattr(request, "config", None),
        }
    )


def _adk_model_request_semantic(
    request: Any,
    *,
    response_schema: Any = None,
) -> dict[str, Any]:
    semantic: dict[str, Any] = {
        "type": "model.request",
        "framework": "google-adk",
    }
    model = getattr(request, "model", None)
    if isinstance(model, str) and model:
        semantic["model"] = model
    tools = _adk_tool_names(getattr(request, "config", None))
    if tools:
        semantic["tools"] = tools
    tool_definitions = _adk_tool_definitions(getattr(request, "config", None))
    if tool_definitions:
        semantic["tool_definitions"] = tool_definitions
    settings = _adk_generation_settings(
        getattr(request, "config", None),
        response_schema=response_schema,
    )
    if settings:
        semantic["settings"] = settings
    return semantic


def _adk_model_response_snapshot(response: Any) -> dict[str, Any]:
    return _adk_compact_dict(
        {
            "content": getattr(response, "content", None),
            "partial": getattr(response, "partial", None),
            "finish_reason": getattr(response, "finish_reason", None),
            "error_code": getattr(response, "error_code", None),
            "error_message": getattr(response, "error_message", None),
            "usage_metadata": getattr(response, "usage_metadata", None),
            "custom_metadata": getattr(response, "custom_metadata", None),
        }
    )


def _adk_model_response_semantic(response: Any) -> dict[str, Any]:
    semantic: dict[str, Any] = {
        "type": "model.response",
        "framework": "google-adk",
        "status": (
            "failed"
            if getattr(response, "error_code", None) or getattr(response, "error_message", None)
            else "completed"
        ),
    }
    content = getattr(response, "content", None)
    if content is not None:
        semantic["content"] = _adk_visible_content(content)
    reasoning = _adk_reasoning(content)
    if reasoning:
        semantic["reasoning"] = reasoning
    finish_reason = getattr(response, "finish_reason", None)
    if finish_reason is not None:
        semantic["finish_reason"] = str(getattr(finish_reason, "value", finish_reason))
    usage = getattr(response, "usage_metadata", None)
    if usage is not None:
        semantic["usage"] = _adk_usage_snapshot(usage)["usage"]
    return semantic


def _adk_reasoning(content: Any) -> list[dict[str, str]]:
    """Project only Gemini parts explicitly marked as model thoughts."""

    reasoning: list[dict[str, str]] = []
    for part in getattr(content, "parts", None) or []:
        if getattr(part, "thought", None) is not True:
            continue
        text = getattr(part, "text", None)
        if isinstance(text, str) and text:
            reasoning.append({"type": "text", "text": text})
    return reasoning


def _adk_visible_content(content: Any) -> Any:
    """Project response content without model-thought parts."""

    compact = _adk_compact_native(content)
    if not isinstance(compact, dict):
        return compact
    source_parts = list(getattr(content, "parts", None) or [])
    visible_indexes = [
        index
        for index, part in enumerate(source_parts)
        if getattr(part, "thought", None) is not True
    ]
    compact_parts = compact.get("parts")
    if isinstance(compact_parts, list):
        compact = {
            **compact,
            "parts": [
                compact_parts[index] for index in visible_indexes if index < len(compact_parts)
            ],
        }
    return compact


def _adk_tool_names(config: Any) -> list[str]:
    names: list[str] = []
    for tool in getattr(config, "tools", None) or []:
        for declaration in getattr(tool, "function_declarations", None) or []:
            name = getattr(declaration, "name", None)
            if isinstance(name, str) and name and name not in names:
                names.append(name)
    return names


def _adk_tool_definitions(config: Any) -> list[dict[str, Any]]:
    definitions: list[dict[str, Any]] = []
    for tool in getattr(config, "tools", None) or []:
        projected = _adk_compact_native(tool)
        if isinstance(projected, dict) and projected:
            definitions.append(projected)
    return definitions


def _adk_generation_settings(
    config: Any,
    *,
    response_schema: Any = None,
) -> dict[str, Any]:
    settings: dict[str, Any] = {}
    for name in (
        "temperature",
        "top_p",
        "top_k",
        "candidate_count",
        "max_output_tokens",
        "stop_sequences",
        "response_logprobs",
        "logprobs",
        "presence_penalty",
        "frequency_penalty",
        "seed",
        "response_mime_type",
        "response_json_schema",
        "safety_settings",
        "tool_config",
        "response_modalities",
        "media_resolution",
        "speech_config",
        "thinking_config",
        "image_config",
        "enable_enhanced_civic_answers",
        "service_tier",
    ):
        value = getattr(config, name, None)
        if value is not None:
            settings[name] = _adk_compact_native(value)
    configured_schema = getattr(config, "response_schema", None)
    normalized_schema = _adk_response_schema(
        configured_schema if configured_schema is not None else response_schema
    )
    if normalized_schema is not None:
        settings["response_schema"] = normalized_schema
    return settings


def _adk_callback_output_schema(context: Any) -> Any:
    invocation = getattr(context, "_invocation_context", None)
    agent = getattr(invocation, "agent", None)
    return getattr(agent, "output_schema", None)


def _adk_response_schema(value: Any) -> dict[str, Any] | None:
    if isinstance(value, type) and issubclass(value, BaseModel):
        return value.model_json_schema()
    if isinstance(value, BaseModel) or type(value) is dict:
        projected = _adk_compact_native(value)
        return projected if isinstance(projected, dict) else None
    return None


def _adk_usage_snapshot(usage: Any) -> dict[str, Any]:
    return _adk_compact_dict(
        {
            "usage": {
                "input_tokens": getattr(usage, "prompt_token_count", None),
                "output_tokens": getattr(usage, "candidates_token_count", None),
                "total_tokens": getattr(usage, "total_token_count", None),
            },
            "raw_usage": usage,
        }
    )


def _adk_tool_snapshot(tool: Any, args: dict[str, Any], *, result: Any = None) -> dict[str, Any]:
    return _adk_compact_dict(
        {
            "name": getattr(tool, "name", None),
            "args": dict(args),
            "result": result,
        }
    )


def _adk_tool_semantic(tool: Any, args: dict[str, Any], context: Any) -> dict[str, Any]:
    semantic: dict[str, Any] = {
        "type": "tool.execution",
        "framework": "google-adk",
        "name": getattr(tool, "name", None),
        "input": _adk_compact_native(dict(args)),
    }
    call_id = _adk_tool_call_id(context)
    if call_id is not None:
        semantic["native_call_id"] = call_id
    return semantic


def _adk_tool_result_semantic(context: Any, result: Any) -> dict[str, Any]:
    semantic: dict[str, Any] = {
        "type": "tool.result",
        "framework": "google-adk",
        "status": "succeeded",
        "output": _adk_compact_native(result),
    }
    call_id = _adk_tool_call_id(context)
    if call_id is not None:
        semantic["native_call_id"] = call_id
    return semantic


def _adk_semantic_error(error: BaseException) -> dict[str, Any]:
    native = _adk_error_details(error)
    return {
        "type": "google_adk_error",
        "message": native["message"],
        "recoverable": False,
        "details": {
            "native_type": native["type"],
            **({"code": native["code"]} if "code" in native else {}),
            **({"status": native["status"]} if "status" in native else {}),
        },
    }


def _adk_event_snapshot(event: Any) -> dict[str, Any]:
    actions = getattr(event, "actions", None)
    return _adk_compact_dict(
        {
            "type": getattr(event, "type", None),
            "id": getattr(event, "id", None),
            "invocation_id": getattr(event, "invocation_id", None),
            "author": getattr(event, "author", None),
            "content": getattr(event, "content", None),
            "partial": getattr(event, "partial", None),
            "turn_complete": getattr(event, "turn_complete", None),
            "usage_metadata": getattr(event, "usage_metadata", None),
            "custom_metadata": getattr(event, "custom_metadata", None),
            "state_delta": getattr(actions, "state_delta", None),
            "error_code": getattr(event, "error_code", None),
            "error_message": getattr(event, "error_message", None),
        }
    )


def _adk_event_output(event: Any) -> Any:
    content = getattr(event, "content", None)
    return _adk_compact_native(content if content is not None else getattr(event, "output", None))


def _adk_content_role(content: Any) -> str:
    parts = getattr(content, "parts", None) or []
    if any(getattr(part, "function_response", None) is not None for part in parts):
        return "tool"
    role = getattr(content, "role", None)
    if role == "model":
        return "assistant"
    if isinstance(role, str) and role in {
        "system",
        "developer",
        "user",
        "assistant",
        "tool",
    }:
        return role
    return "user"


def _adk_function_call_ids(content: Any) -> list[str]:
    result: list[str] = []
    for call in _adk_function_calls(content):
        call_id = getattr(call, "id", None)
        if isinstance(call_id, str) and call_id and call_id not in result:
            result.append(call_id)
    return result


def _adk_function_calls(content: Any) -> list[Any]:
    return [
        call
        for part in getattr(content, "parts", None) or []
        if (call := getattr(part, "function_call", None)) is not None
    ]


def _adk_function_response_ids(content: Any) -> list[str]:
    result: list[str] = []
    for part in getattr(content, "parts", None) or []:
        response = getattr(part, "function_response", None)
        call_id = getattr(response, "id", None)
        if isinstance(call_id, str) and call_id and call_id not in result:
            result.append(call_id)
    return result


def _adk_function_responses(content: Any) -> list[Any]:
    return [
        response
        for part in getattr(content, "parts", None) or []
        if (response := getattr(part, "function_response", None)) is not None
    ]


def _exact_receipt(
    records: dict[tuple[str, int], tuple[Any, AdmissionReceipt]],
    key: str,
    value: Any,
) -> AdmissionReceipt | None:
    entry = records.get((key, id(value)))
    if entry is None or entry[0] is not value:
        return None
    return entry[1]


def _adk_compact_native(value: Any) -> Any:
    """Drop ADK/Pydantic null defaults while retaining material falsey values."""

    if isinstance(value, BaseModel):
        return _adk_compact_native(
            value.model_dump(
                mode="python",
                exclude_none=True,
                exclude_defaults=False,
                exclude_unset=False,
            )
        )
    if type(value) is dict:
        return {
            key: _adk_compact_native(child) for key, child in value.items() if child is not None
        }
    if type(value) is list:
        return [_adk_compact_native(child) for child in value]
    if type(value) is tuple:
        return tuple(_adk_compact_native(child) for child in value)
    return value


def _adk_compact_dict(value: dict[str, Any]) -> dict[str, Any]:
    return cast(dict[str, Any], _adk_compact_native(value))


def _adk_context_equal(left: Any, right: Any) -> bool:
    try:
        result = left == right
    except BaseException:
        return False
    return result if isinstance(result, bool) else False


def _adk_event_is_redundant(event: Any, terminal: bool) -> bool:
    if getattr(event, "partial", False) or terminal:
        return True
    content = getattr(event, "content", None)
    parts = getattr(content, "parts", None) or []
    if any(
        getattr(part, "function_call", None) is not None
        or getattr(part, "function_response", None) is not None
        for part in parts
    ):
        return True
    actions = getattr(event, "actions", None)
    return content is None and bool(
        getattr(actions, "state_delta", None) or getattr(event, "usage_metadata", None) is not None
    )


def _adk_event_is_final(event: Any) -> bool:
    check = getattr(event, "is_final_response", None)
    if not callable(check):
        return False
    try:
        return bool(check())
    except BaseException:
        return False
