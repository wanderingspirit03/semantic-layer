from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import pytest

os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")
pytest.importorskip("crewai")

from crewai import LLM, Agent  # noqa: E402
from crewai.events import crewai_event_bus  # noqa: E402
from crewai.events.types.a2a_events import A2ADelegationStartedEvent  # noqa: E402
from crewai.events.types.agent_events import (  # noqa: E402
    AgentExecutionCompletedEvent,
    AgentExecutionStartedEvent,
    LiteAgentExecutionCompletedEvent,
    LiteAgentExecutionStartedEvent,
)
from crewai.events.types.crew_events import (  # noqa: E402
    CrewKickoffCompletedEvent,
    CrewKickoffStartedEvent,
)
from crewai.events.types.llm_events import (  # noqa: E402
    LLMCallCompletedEvent,
    LLMCallStartedEvent,
    LLMCallType,
)
from crewai.events.types.task_events import TaskStartedEvent  # noqa: E402
from crewai.events.types.tool_usage_events import (  # noqa: E402
    ToolUsageErrorEvent,
    ToolUsageStartedEvent,
)

from semantic_layer import initialize, reset_capture_for_tests  # noqa: E402
from semantic_layer.crewai_adapter import (  # noqa: E402
    _agent_execution_native,
    crewai_adapter,
)


def _emit(event: Any) -> None:
    crewai_event_bus.emit(object(), event)
    crewai_event_bus.flush()


def _replay(event: Any) -> None:
    crewai_event_bus.replay(object(), event)
    crewai_event_bus.flush()


def _records(path: str) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in (Path(path) / "trace.jsonl").read_text().splitlines()
    ]


def test_crewai_native_projection_preserves_uuid_identities_as_strings() -> None:
    agent_id = uuid4()
    task_id = uuid4()
    native = _agent_execution_native(
        SimpleNamespace(
            event_id="event-1",
            agent=SimpleNamespace(id=agent_id, role="Fixture"),
            task=SimpleNamespace(id=task_id, description="Use a tool"),
        )
    )

    assert native["agent"]["id"] == str(agent_id)
    assert native["task"]["id"] == str(task_id)

    unsupported_id = object()
    unsupported = _agent_execution_native(
        SimpleNamespace(event_id="event-2", agent=SimpleNamespace(id=unsupported_id))
    )
    assert unsupported["agent"]["id"] is unsupported_id


def test_crewai_real_agent_kickoff_uses_lite_execution_root(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-real-agent-kickoff")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    llm = LLM(model="openai/gpt-4o-mini")
    llm.call = lambda *_args, **_kwargs: "fixture answer"  # type: ignore[method-assign]
    agent = Agent(
        role="Fixture agent",
        goal="Return a fixture answer",
        backstory="Used to verify CrewAI's public standalone-agent seam.",
        llm=llm,
        verbose=False,
    )

    result = cast(Any, agent.kickoff("Say fixture"))
    records = _records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    assert result.raw == "fixture answer"
    starts = [record for record in records if record["kind"] == "run.start"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(starts) == 1
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "completed"
    assert outcomes[0]["data"]["output"] == "fixture answer"


def test_crewai_real_agent_kickoff_failure_closes_lite_execution_root(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-real-agent-failure")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    marker = RuntimeError("fixture model failed")
    llm = LLM(model="openai/gpt-4o-mini")

    def fail(*_args: Any, **_kwargs: Any) -> str:
        raise marker

    llm.call = fail  # type: ignore[method-assign]
    agent = Agent(
        role="Fixture agent",
        goal="Return a fixture answer",
        backstory="Used to verify CrewAI's public standalone-agent failure seam.",
        llm=llm,
        verbose=False,
    )

    with pytest.raises(RuntimeError) as raised:
        agent.kickoff("Say fixture")
    assert raised.value is marker
    records = _records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    starts = [record for record in records if record["kind"] == "run.start"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    errors = [record for record in records if record["kind"] == "error"]
    assert len(starts) == 1
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "failed"
    assert outcomes[0]["data"]["error"]["message"] == "fixture model failed"
    assert len(errors) == 1
    assert errors[0]["data"]["message"] == "fixture model failed"


def test_crewai_agent_kickoff_failure_is_captured_without_a_crew_root(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-agent-kickoff")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    agent_info = {"id": uuid4(), "role": "Calculator"}
    started = LiteAgentExecutionStartedEvent(
        agent_info=agent_info,
        tools=[],
        messages="Use the failing_tool to do something.",
        event_id="agent-turn-1",
    )
    _emit(started)
    _emit(
        LLMCallStartedEvent(
            call_id="model-1",
            model="fixture-model",
            messages=[{"role": "user", "content": started.messages}],
            tools=[{"type": "function", "function": {"name": "failing_tool"}}],
        )
    )
    _emit(
        LLMCallCompletedEvent(
            call_id="model-1",
            model="fixture-model",
            response={"tool_calls": [{"name": "failing_tool"}]},
            call_type=LLMCallType.LLM_CALL,
            finish_reason="tool_calls",
        )
    )
    tool_started = ToolUsageStartedEvent(
        tool_name="failing_tool",
        tool_args={"value": "fixture"},
        event_id="tool-1",
    )
    _emit(tool_started)
    now = datetime.now(timezone.utc)
    _emit(
        ToolUsageErrorEvent.model_construct(
            tool_name="failing_tool",
            tool_args={"value": "fixture"},
            error="This tool always fails",
            started_at=now,
            finished_at=now,
            event_id="tool-1-error",
            started_event_id=tool_started.event_id,
        )
    )
    _emit(
        LiteAgentExecutionCompletedEvent(
            agent_info=agent_info,
            output="The tool failed and no result was produced.",
            event_id="agent-turn-1-end",
            started_event_id=started.event_id,
        )
    )

    status = capture.shutdown()
    records = _records(status.artifact_path)
    reset_capture_for_tests()

    assert records[0]["kind"] == "run.start"
    assert records[-1]["kind"] == "run.outcome"
    assert records[-1]["data"]["status"] == "completed"
    assert any(record["kind"] == "model.request" for record in records)
    result = next(record for record in records if record["kind"] == "tool.result")
    assert result["data"]["status"] == "failed"
    assert result["data"]["error"]["message"] == "This tool always fails"
    assert any(
        record["kind"] == "error"
        and record["data"]["message"] == "This tool always fails"
        for record in records
    )
    assert not [record for record in records if record["kind"] == "loss"]


def test_crewai_concurrent_identical_agent_roots_finish_by_exact_event_id(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-concurrent-agents")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    agent_info = {"id": "same-agent", "role": "Same"}
    for event_id, message in (
        ("agent-turn-1", "same task"),
        ("agent-turn-2", "same task"),
    ):
        _replay(
            LiteAgentExecutionStartedEvent(
                agent_info=agent_info,
                tools=[],
                messages=message,
                event_id=event_id,
            )
        )
    for event_id, started_event_id, output in (
        ("agent-turn-2-end", "agent-turn-2", "second"),
        ("agent-turn-1-end", "agent-turn-1", "first"),
    ):
        _replay(
            LiteAgentExecutionCompletedEvent(
                agent_info=agent_info,
                output=output,
                event_id=event_id,
                started_event_id=started_event_id,
            )
        )

    records = _records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    starts = [record for record in records if record["kind"] == "run.start"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(starts) == 2
    assert len(outcomes) == 2
    assert {record["data"]["output"] for record in outcomes} == {"first", "second"}
    assert all(record["data"]["status"] == "completed" for record in outcomes)


def test_crewai_missing_agent_completion_identity_records_one_gap(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-agent-gap")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    agent_info = {"id": "agent-1", "role": "Fixture"}
    _replay(
        LiteAgentExecutionStartedEvent(
            agent_info=agent_info,
            tools=[],
            messages="fixture task",
            event_id="agent-turn-1",
            parent_event_id="outside-capture",
        )
    )
    _replay(
        LiteAgentExecutionCompletedEvent(
            agent_info=agent_info,
            output="ambiguous completion",
            event_id="agent-turn-1-end",
        )
    )

    records = _records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    losses = [record for record in records if record["kind"] == "loss"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == (
        "agent_execution_completion_identity_not_observed"
    )
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "cancelled"


def test_crewai_ambiguous_completion_records_a_gap_on_each_open_agent(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-ambiguous-agent-gap")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    agent_info = {"id": "same-agent", "role": "Same agent"}
    for event_id in ("agent-turn-1", "agent-turn-2"):
        _replay(
            LiteAgentExecutionStartedEvent(
                agent_info=agent_info,
                tools=[],
                messages="same task",
                event_id=event_id,
                parent_event_id="outside-capture",
            )
        )
    _replay(
        LiteAgentExecutionCompletedEvent(
            agent_info=agent_info,
            output="ambiguous completion",
            event_id="agent-turn-end",
        )
    )

    records = _records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    losses = [record for record in records if record["kind"] == "loss"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(losses) == 2
    assert {
        record["data"]["reason"] for record in losses
    } == {"agent_execution_completion_identity_not_observed"}
    assert len(outcomes) == 2
    assert all(record["data"]["status"] == "cancelled" for record in outcomes)


def test_crewai_standalone_agent_records_unfinished_handoff_gap(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-unfinished-handoff")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    agent_info = {"id": "agent-1", "role": "Delegator"}
    _replay(
        LiteAgentExecutionStartedEvent(
            agent_info=agent_info,
            tools=[],
            messages="delegate",
            event_id="agent-turn-1",
            parent_event_id="outside-capture",
        )
    )
    _replay(
        A2ADelegationStartedEvent(
            agent_id="agent-1",
            endpoint="https://fixture.invalid/a2a",
            task_description="delegate fixture",
            event_id="handoff-1",
            parent_event_id="agent-turn-1",
        )
    )
    _replay(
        LiteAgentExecutionCompletedEvent(
            agent_info=agent_info,
            output="agent completed",
            event_id="agent-turn-1-end",
            started_event_id="agent-turn-1",
        )
    )

    records = _records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    losses = [record for record in records if record["kind"] == "loss"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "handoff_completion_not_observed"
    assert len(outcomes) == 1
    assert outcomes[0]["data"]["status"] == "completed"


def test_crewai_concurrent_crews_keep_nested_agents_under_their_task_roots(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-concurrent-crews")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    agent = SimpleNamespace(id="same-agent", role="Same agent")
    task = SimpleNamespace(
        id="same-task",
        name="same task",
        description="same task",
        expected_output="an answer",
    )

    for suffix in ("1", "2"):
        _replay(
            CrewKickoffStartedEvent.model_construct(
                event_id=f"crew-{suffix}",
                parent_event_id="outside-capture",
                crew_name=f"crew-{suffix}",
                inputs={"value": suffix},
            )
        )
        _replay(
            TaskStartedEvent.model_construct(
                event_id=f"task-{suffix}",
                parent_event_id=f"crew-{suffix}",
                context=None,
                task=task,
            )
        )
        _replay(
            AgentExecutionStartedEvent.model_construct(
                event_id=f"agent-{suffix}",
                parent_event_id=f"task-{suffix}",
                agent=agent,
                task=task,
                tools=[],
                task_prompt="same task",
            )
        )

    for suffix, output in (("2", "second"), ("1", "first")):
        _replay(
            AgentExecutionCompletedEvent.model_construct(
                event_id=f"agent-{suffix}-end",
                parent_event_id=f"task-{suffix}",
                started_event_id=f"agent-{suffix}",
                agent=agent,
                task=task,
                output=output,
            )
        )
        _replay(
            CrewKickoffCompletedEvent.model_construct(
                event_id=f"crew-{suffix}-end",
                parent_event_id="outside-capture",
                started_event_id=f"crew-{suffix}",
                crew_name=f"crew-{suffix}",
                output=output,
                total_tokens=0,
            )
        )

    records = _records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    starts = [record for record in records if record["kind"] == "run.start"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(starts) == 2
    assert len(outcomes) == 2
    assert {record["data"]["output"] for record in outcomes} == {"first", "second"}
    assert not [record for record in records if record["kind"] == "loss"]


def test_crewai_nested_completion_is_not_misattributed_to_standalone_agent(
    tmp_path: Path,
) -> None:
    reset_capture_for_tests()
    capture = initialize(output=tmp_path, service_name="crewai-mixed-agent-roots")
    capture.instrument(adapter=crewai_adapter(version="1.15.2"), client=crewai_event_bus)
    agent_info = {"id": "same-agent", "role": "Same agent"}
    agent = SimpleNamespace(id="same-agent", role="Same agent")
    task = SimpleNamespace(
        id="same-task",
        name="same task",
        description="same task",
        expected_output="an answer",
    )
    _replay(
        LiteAgentExecutionStartedEvent(
            agent_info=agent_info,
            tools=[],
            messages="standalone",
            event_id="standalone-agent",
            parent_event_id="outside-capture",
        )
    )
    _replay(
        CrewKickoffStartedEvent.model_construct(
            event_id="crew",
            parent_event_id="outside-capture",
            crew_name="crew",
            inputs={},
        )
    )
    _replay(
        TaskStartedEvent.model_construct(
            event_id="task",
            parent_event_id="crew",
            context=None,
            task=task,
        )
    )
    _replay(
        AgentExecutionStartedEvent.model_construct(
            event_id="nested-agent",
            parent_event_id="task",
            agent=agent,
            task=task,
            tools=[],
            task_prompt="same task",
        )
    )
    _replay(
        AgentExecutionCompletedEvent.model_construct(
            event_id="nested-agent-end",
            parent_event_id="task",
            started_event_id="nested-agent",
            agent=agent,
            task=task,
            output="nested",
        )
    )
    _replay(
        LiteAgentExecutionCompletedEvent(
            agent_info=agent_info,
            output="standalone",
            event_id="standalone-agent-end",
            started_event_id="standalone-agent",
        )
    )
    _replay(
        CrewKickoffCompletedEvent.model_construct(
            event_id="crew-end",
            parent_event_id="outside-capture",
            started_event_id="crew",
            crew_name="crew",
            output="crew",
            total_tokens=0,
        )
    )

    records = _records(capture.shutdown().artifact_path)
    reset_capture_for_tests()

    starts = [record for record in records if record["kind"] == "run.start"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(starts) == 2
    assert len(outcomes) == 2
    assert {record["data"]["output"] for record in outcomes} == {"crew", "standalone"}
    assert not [record for record in records if record["kind"] == "loss"]
