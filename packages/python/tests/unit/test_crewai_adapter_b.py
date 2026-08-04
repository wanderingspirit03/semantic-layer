from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

# Keep the exact-version fixture local; CrewAI initializes its exporter on import.
os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")
pytest.importorskip("crewai")

from crewai import LLM, Agent, Crew, Task  # noqa: E402
from crewai.events import crewai_event_bus  # noqa: E402
from crewai.memory.storage import kickoff_task_outputs_storage  # noqa: E402
from crewai.types.streaming import CrewStreamingOutput  # noqa: E402

from semantic_layer import initialize  # noqa: E402
from semantic_layer.crewai_adapter import crewai_adapter  # noqa: E402
from semantic_layer.validation import validate_artifact  # noqa: E402


def _version() -> str:
    value = json.loads(os.environ.get("SEMANTIC_LAYER_EXPECTED_VERSIONS", "{}")).get(
        "crewai", "1.15.2"
    )
    assert isinstance(value, str)
    return value


def _crew(*, stream: bool = False) -> Crew:
    llm = LLM(model="openai/gpt-4o-mini")
    llm.call = lambda *args, **kwargs: "fixture answer"  # type: ignore[method-assign]
    agent = Agent(
        role="helper",
        goal="answer",
        backstory="fixture",
        llm=llm,
        verbose=False,
    )
    task = Task(
        description="Say fixture",
        expected_output="fixture answer",
        agent=agent,
    )
    return Crew(agents=[agent], tasks=[task], stream=stream, verbose=False)


@pytest.fixture(autouse=True)
def _isolated_crewai_storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = tmp_path / "crewai-storage"
    storage.mkdir()
    monkeypatch.setenv("CREWAI_DISABLE_TELEMETRY", "true")
    monkeypatch.setattr(
        kickoff_task_outputs_storage,
        "db_storage_path",
        lambda: str(storage),
    )


def test_crewai_kickoff_preserves_exact_nonstream_return(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="crewai-return")
    capture.instrument(adapter=crewai_adapter(version=_version()), client=crewai_event_bus)
    crew = _crew()
    first = crew.kickoff()
    second = crew.kickoff()
    assert not isinstance(first, CrewStreamingOutput)
    assert not isinstance(second, CrewStreamingOutput)
    assert first.raw == "fixture answer"
    assert second.raw == "fixture answer"
    assert validate_artifact(capture.shutdown().artifact_path).valid


def test_crewai_kickoff_preserves_lazy_stream_return(tmp_path: Path) -> None:
    capture = initialize(output=tmp_path, service_name="crewai-stream-return")
    capture.instrument(adapter=crewai_adapter(version=_version()), client=crewai_event_bus)
    result = _crew(stream=True).kickoff()
    assert isinstance(result, CrewStreamingOutput)
    list(result)
    assert result.result.raw == "fixture answer"
    assert validate_artifact(capture.shutdown().artifact_path).valid
