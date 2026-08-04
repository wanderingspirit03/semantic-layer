from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from semantic_layer.capture_v1 import AdmissionReceipt
from semantic_layer.crewai_adapter import (
    _CrewAISource,
    _OpenTurn,
    _StreamedToolCall,
)


class _Sink:
    def __init__(self) -> None:
        self.records: list[dict[str, Any]] = []

    def record(self, record: dict[str, Any]) -> AdmissionReceipt:
        self.records.append(record)
        return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")


def _streaming_source() -> tuple[_CrewAISource, _OpenTurn, _Sink]:
    source = _CrewAISource(object(), "fixture")
    sink = _Sink()
    source._sink = sink
    turn = _OpenTurn(
        {
            "trace_id": "trace",
            "operation_id": "operation",
            "session_id": "session",
        },
        "turn",
        AdmissionReceipt(True, record_id="root"),
    )
    for identity, tool_call_id in (("model-1", "call-1"), ("model-2", "call-2")):
        source._models[identity] = AdmissionReceipt(
            True, record_id=f"{identity}-request"
        )
        source._model_turn[identity] = turn
        source._streaming_models.add(identity)
        source._streamed_models.add(identity)
        source._streamed_tool_calls[identity] = {
            0: _StreamedToolCall(
                index=0,
                name="lookup",
                arguments='{"city":"London"}',
                tool_call_id=tool_call_id,
            )
        }
    return source, turn, sink


def test_crewai_does_not_pair_same_name_and_arguments_without_exact_identity() -> None:
    source, turn, sink = _streaming_source()

    source._complete_streamed_tool_model(
        SimpleNamespace(tool_name="lookup", tool_args={"city": "London"}),
        turn,
    )

    assert set(source._models) == {"model-1", "model-2"}
    gaps = [record for record in sink.records if record["phase"] == "gap"]
    assert len(gaps) == 1
    assert gaps[0]["name"] == "crewai.streamed_tool_call.unpaired"
    assert gaps[0]["semantic"]["count"] == 2
    assert not [record for record in sink.records if record["phase"] == "end"]


def test_crewai_completes_only_the_stream_with_exact_tool_call_identity() -> None:
    source, turn, sink = _streaming_source()

    source._complete_streamed_tool_model(
        SimpleNamespace(
            tool_name="lookup",
            tool_args={"city": "London"},
            tool_call_id="call-2",
        ),
        turn,
    )

    assert set(source._models) == {"model-1"}
    assert not [record for record in sink.records if record["phase"] == "gap"]
    ended = [record for record in sink.records if record["phase"] == "end"]
    assert [record["native_identity"] for record in ended] == ["model-2", "model-2"]
