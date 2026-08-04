from __future__ import annotations

import json
from importlib.metadata import version as distribution_version
from pathlib import Path
from typing import Any

import pytest
from strands import Agent as StrandsAgent
from strands import tool as strands_tool
from strands.models import Model as StrandsModel

from semantic_layer import initialize, reset_capture_for_tests, strands_adapter
from semantic_layer.strands_adapter import (
    _strands_agent_result_output,
    _strands_exact_message_refs,
    _strands_exact_tool_proposal_parent,
    _strands_exact_tool_result_refs,
    _strands_remember_message,
    _strands_remember_tool_proposals,
    _strands_remember_tool_result,
)


@pytest.fixture(autouse=True)
def _reset_capture() -> Any:
    yield
    reset_capture_for_tests()


def _semantic_records(artifact: str) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in (Path(artifact) / "trace.jsonl").read_text().splitlines()
    ]


class _ToolModel(StrandsModel):
    def update_config(self, **model_config: Any) -> None:
        return None

    def get_config(self) -> dict[str, str]:
        return {"model_id": "fixture"}

    async def structured_output(self, *args: Any, **kwargs: Any) -> Any:
        if False:
            yield None

    async def stream(self, messages: Any, *args: Any, **kwargs: Any) -> Any:
        has_result = any(
            any("toolResult" in block for block in message.get("content", []))
            for message in messages
        )
        events = (
            [
                {"messageStart": {"role": "assistant"}},
                {
                    "contentBlockStart": {
                        "start": {
                            "toolUse": {
                                "name": "lookup",
                                "toolUseId": "strands-call-1",
                            }
                        }
                    }
                },
                {"contentBlockDelta": {"delta": {"toolUse": {"input": "{}"}}}},
                {"contentBlockStop": {}},
                {"messageStop": {"stopReason": "tool_use"}},
            ]
            if not has_result
            else [
                {"messageStart": {"role": "assistant"}},
                {"contentBlockStart": {"start": {}}},
                {"contentBlockDelta": {"delta": {"text": "done"}}},
                {"contentBlockStop": {}},
                {"messageStop": {"stopReason": "end_turn"}},
            ]
        )
        for event in [
            *events,
            {
                "metadata": {
                    "usage": {
                        "inputTokens": 4,
                        "outputTokens": 2,
                        "totalTokens": 6,
                    },
                    "metrics": {"latencyMs": 1},
                }
            },
        ]:
            yield event


def test_strands_1_47_compacts_exact_aliases_into_canonical_context(
    tmp_path: Path,
) -> None:
    assert distribution_version("strands-agents") == "1.47.0"

    @strands_tool
    def lookup() -> str:
        """Return exact fixture output."""

        return "tool-output"

    agent = StrandsAgent(
        model=_ToolModel(),
        tools=[lookup],
        callback_handler=None,
    )
    capture = initialize(output=tmp_path, service_name="strands-compaction-fixture")
    capture.instrument(
        adapter=strands_adapter(version="1.47.0"),
        client=agent,
    )

    result = agent("use the tool")
    assert result.message["content"][0]["text"] == "done"

    records = _semantic_records(capture.shutdown().artifact_path)
    messages = [record for record in records if record["kind"] == "message"]
    requests = [record for record in records if record["kind"] == "model.request"]
    responses = [record for record in records if record["kind"] == "model.response"]
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    tool_result = next(record for record in records if record["kind"] == "tool.result")

    assert len(messages) == 1
    assert messages[0]["data"]["role"] == "user"
    assert len(requests) == len(responses) == 2
    assert requests[0]["data"]["context_refs"] == [messages[0]["id"]]
    assert requests[1]["data"]["context_refs"] == [
        messages[0]["id"],
        responses[0]["id"],
        tool_result["id"],
    ]
    assert proposal["parent"] == responses[0]["id"]
    assert [
        record["data"]["reason"]
        for record in records
        if record["kind"] == "loss"
    ] == ["strands_post_middleware_context_unavailable"]


def test_strands_alias_compaction_requires_same_unchanged_object() -> None:
    response = {"role": "assistant", "content": [{"text": "same"}]}
    message_evidence: dict[int, tuple[object, str, object, list[str]]] = {}
    _strands_remember_message(
        message_evidence,
        response,
        response,
        ["response-record"],
    )

    clone = {"role": "assistant", "content": [{"text": "same"}]}
    assert _strands_exact_message_refs(clone, clone, message_evidence) is None
    response["content"][0]["text"] = "mutated"
    assert _strands_exact_message_refs(response, response, message_evidence) is None

    tool_use = {
        "toolUseId": "call-1",
        "name": "lookup",
        "input": {"query": "same"},
    }
    proposal_message = {
        "role": "assistant",
        "content": [{"toolUse": tool_use}],
    }
    proposal_evidence: dict[int, tuple[object, str, object, str]] = {}
    _strands_remember_tool_proposals(
        proposal_evidence,
        proposal_message,
        "response-record",
    )
    assert (
        _strands_exact_tool_proposal_parent(
            dict(tool_use),
            "call-1",
            proposal_evidence,
        )
        is None
    )
    tool_use["input"]["query"] = "mutated"
    assert (
        _strands_exact_tool_proposal_parent(
            tool_use,
            "call-1",
            proposal_evidence,
        )
        is None
    )

    result = {
        "toolUseId": "call-1",
        "status": "success",
        "content": [{"text": "same"}],
    }
    result_evidence: dict[int, tuple[object, str, object, str]] = {}
    _strands_remember_tool_result(
        result_evidence,
        result,
        "call-1",
        "result-record",
    )
    clone_wrapper = {
        "role": "user",
        "content": [{"toolResult": dict(result)}],
    }
    assert _strands_exact_tool_result_refs(clone_wrapper, result_evidence) is None
    result["content"][0]["text"] = "mutated"
    mutated_wrapper = {
        "role": "user",
        "content": [{"toolResult": result}],
    }
    assert _strands_exact_tool_result_refs(mutated_wrapper, result_evidence) is None


def test_strands_linked_agent_result_keeps_only_distinct_return_fields() -> None:
    class Result:
        def __init__(self) -> None:
            self.stop_reason = "end_turn"
            self.message = {"role": "assistant", "content": [{"text": "done"}]}
            self.metrics = {"cycles": 3}
            self.state = {"messages": ["cumulative"]}
            self.interrupts = None
            self.structured_output = {"answer": "done"}
            self.checkpoint = None

    result = Result()
    assert _strands_agent_result_output(
        result,
        exact_message_link=True,
    ) == {"structured_output": {"answer": "done"}}
    assert _strands_agent_result_output(
        result,
        exact_message_link=False,
    ) == {
        "stop_reason": "end_turn",
        "message": {"role": "assistant", "content": [{"text": "done"}]},
        "metrics": {"cycles": 3},
        "state": {"messages": ["cumulative"]},
        "structured_output": {"answer": "done"},
    }
