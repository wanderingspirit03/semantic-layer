from __future__ import annotations

import json
import os
from enum import Enum
from importlib.metadata import version as distribution_version
from types import SimpleNamespace
from typing import Any

import pytest
from google.adk.models import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types as genai_types
from langgraph.types import Command
from pydantic import BaseModel

from semantic_layer import capture_v1 as capture_module
from semantic_layer import (
    google_adk_adapter,
    langgraph_adapter,
    openai_agents_adapter,
    pydantic_ai_adapter,
    strands_adapter,
)
from semantic_layer._adapter_native import native_snapshot
from semantic_layer._framework_adapter_shared import _source_qualification
from semantic_layer.openai_agents_adapter import _ObservedAsyncIterator


def _version(package: str) -> str:
    expected = json.loads(os.environ.get("SEMANTIC_LAYER_EXPECTED_VERSIONS", "{}"))
    value = expected.get(package, distribution_version(package))
    assert isinstance(value, str)
    return value

PYDANTIC_AI_VERSION = _version("pydantic-ai")
GOOGLE_ADK_VERSION = _version("google-adk")


class _FixtureADKModel(BaseLlm):
    calls: int = 0
    fail_first: bool = False
    failure: Exception | None = None

    async def generate_content_async(
        self, llm_request: Any, stream: bool = False
    ) -> Any:
        del stream
        self.calls += 1
        if self.fail_first and self.calls == 1:
            assert self.failure is not None
            raise self.failure
        has_tool_result = any(
            getattr(part, "function_response", None) is not None
            for content in llm_request.contents
            for part in content.parts or []
        )
        if not has_tool_result and not self.fail_first:
            content = genai_types.Content(
                role="model",
                parts=[
                    genai_types.Part.from_function_call(
                        name="fixture_lookup",
                        args={"value": "a"},
                    )
                ],
            )
        else:
            content = genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="done")],
            )
            yield LlmResponse(
                content=genai_types.Content(
                    role="model",
                    parts=[genai_types.Part.from_text(text="do")],
                ),
                partial=True,
            )
        yield LlmResponse(
            content=content,
            partial=False,
            usage_metadata=genai_types.GenerateContentResponseUsageMetadata(
                prompt_token_count=1,
                candidates_token_count=1,
                total_token_count=2,
            ),
        )


class _CountingSink:
    calls = 0

    def open_trace(self, _value: Any) -> Any:
        self.calls += 1
        return SimpleNamespace(accepted=False, identity=None)

    def record(self, _value: Any) -> Any:
        self.calls += 1
        return SimpleNamespace(accepted=False)


def test_framework_source_seams_match_the_canonical_matrix() -> None:
    actual = {
        "openai-agents-python": openai_agents_adapter(
            version=_version("openai-agents")
        ).create_source(object()).metadata["seam"],
        "langgraph-python": langgraph_adapter(
            version=_version("langgraph")
        ).create_source(object()).metadata["seam"],
        "strands-python": strands_adapter(
            version=_version("strands-agents")
        ).create_source(object()).metadata["seam"],
        "pydanticai-python": pydantic_ai_adapter(
            version=_version("pydantic-ai")
        ).create_source(object()).metadata["seam"],
        "google-adk-python": google_adk_adapter(
            version=_version("google-adk")
        ).create_source(object()).metadata["seam"],
    }
    assert actual == {
        "openai-agents-python": (
            "add_trace_processor/TracingProcessor + "
            "Runner.run_streamed/stream_events proxy"
        ),
        "langgraph-python": (
            "invoke/ainvoke/stream/astream wrappers + RunnableConfig.callbacks"
        ),
        "strands-python": (
            "Agent.add_hook/HookRegistry + Agent.callback_handler wrapper"
        ),
        "pydanticai-python": (
            "Agent.run/run_sync wrappers + AbstractCapability model/tool hooks"
        ),
        "google-adk-python": "plugin_manager.register_plugin/BasePlugin callbacks",
    }


def test_framework_source_qualification_never_implies_a_version_range() -> None:
    exact = _source_qualification(
        "2.9.0",
        exact_versions=frozenset({"2.9.0"}),
        profile="pydantic-ai-python-adapter-v1",
    )
    newer = _source_qualification(
        "2.10.0",
        exact_versions=frozenset({"2.9.0"}),
        profile="pydantic-ai-python-adapter-v1",
    )

    assert exact == {"status": "exact_qualified"}
    assert newer == {
        "status": "capability_checked_unqualified",
        "profile": "pydantic-ai-python-adapter-v1",
    }


def test_framework_adapters_remain_physically_isolated() -> None:
    assert openai_agents_adapter.__module__ == "semantic_layer.openai_agents_adapter"
    assert langgraph_adapter.__module__ == "semantic_layer.langgraph_adapter"
    assert strands_adapter.__module__ == "semantic_layer.strands_adapter"
    assert pydantic_ai_adapter.__module__ == "semantic_layer.pydantic_ai_adapter"
    assert google_adk_adapter.__module__ == "semantic_layer.google_adk_adapter"


def test_native_enum_snapshot_never_executes_hostile_properties() -> None:
    calls = 0

    class HostileEnum(str, Enum):
        VALUE = "safe"

        @property
        def value(self) -> str:
            nonlocal calls
            calls += 1
            raise AssertionError("must not execute")

    captured, losses, blobs = capture_module._safe(HostileEnum.VALUE)
    assert captured == {
        "$semantic_layer_enum": {
            "type": "HostileEnum",
            "name": "VALUE",
            "value": "safe",
        }
    }
    assert losses == []
    assert blobs == []
    assert calls == 0


def test_native_snapshot_serializes_slotted_langgraph_command_without_loss() -> None:
    captured, losses, blobs = capture_module._safe(
        Command(
            goto="write_research_brief",
            update={"messages": ["research accepted"]},
        )
    )

    assert captured == {
        "graph": None,
        "update": {"messages": ["research accepted"]},
        "resume": None,
        "goto": "write_research_brief",
    }
    assert losses == []
    assert blobs == []


def test_native_snapshot_keeps_response_format_class_as_explicit_loss() -> None:
    class ResearchQuestion(BaseModel):
        research_brief: str

    captured, losses, blobs = capture_module._safe(
        {"invocation_params": {"response_format": ResearchQuestion}}
    )

    assert captured == {
        "invocation_params": {
            "response_format": {"$semantic_layer_omitted": "callable"}
        }
    }
    assert losses == [
        (
            "unsupported_native_value",
            "/invocation_params/response_format",
        )
    ]
    assert blobs == []


def test_native_snapshot_omits_binary_larger_than_the_capture_budget() -> None:
    oversized = b"x" * (8 * 1024 * 1024 + 1)

    assert native_snapshot(oversized) == {
        "native_type": "bytes",
        "omitted": "resource_limit",
    }


def test_native_snapshot_omits_string_larger_than_the_capture_budget() -> None:
    oversized = "x" * (8 * 1024 * 1024 + 1)

    assert native_snapshot(oversized) == {
        "native_type": "str",
        "omitted": "resource_limit",
    }


def test_native_snapshot_omits_container_with_oversized_key() -> None:
    oversized_key = "x" * (8 * 1024 * 1024 + 1)

    assert native_snapshot({oversized_key: "value"}) == {
        "native_type": "dict",
        "omitted": "resource_limit",
    }


def test_native_snapshot_stops_traversal_at_the_node_budget() -> None:
    snapshot = native_snapshot(list(range(25_000)))

    assert isinstance(snapshot, list)
    assert len(snapshot) <= 20_000
    assert snapshot[-1] == {
        "native_type": "int",
        "omitted": "resource_limit",
    }


def test_native_snapshot_does_not_format_unbounded_integer_keys() -> None:
    huge_key = 1 << 300_000

    assert native_snapshot({huge_key: "value"}) == {"<int:0>": "value"}


def test_native_snapshot_does_not_admit_unbounded_integer_values() -> None:
    huge_value = 1 << 300_000

    assert native_snapshot(huge_value) == {
        "native_type": "int",
        "omitted": "resource_limit",
    }


def test_core_snapshot_does_not_format_unbounded_error_integers() -> None:
    captured, losses, blobs = capture_module._safe(
        ValueError(1 << 300_000, "context")
    )

    assert captured["message"] == {
        "$semantic_layer_omitted": "resource_limit"
    }
    assert ("serialization_failure", "/message") in losses
    assert blobs == []


@pytest.mark.asyncio
async def test_observed_async_iterator_has_one_consumer_and_exact_cancellation() -> None:
    class Stream:
        def __init__(self) -> None:
            self.values = iter(("one", "two"))
            self.pulls = 0
            self.closes = 0

        async def __anext__(self) -> str:
            self.pulls += 1
            try:
                return next(self.values)
            except StopIteration as error:
                raise StopAsyncIteration from error

        async def aclose(self) -> None:
            self.closes += 1

    target = Stream()
    events: list[Any] = []
    wrapped = _ObservedAsyncIterator(
        target,
        events.append,
        events.append,
        lambda: events.append("complete"),
        lambda: events.append("cancelled"),
    )
    assert await anext(wrapped) == "one"
    await wrapped.aclose()
    assert target.pulls == 1
    assert target.closes == 1
    assert events == ["one", "cancelled"]

    expected = RuntimeError("stream failed")

    class Failing:
        async def __anext__(self) -> str:
            raise expected

    errors: list[BaseException] = []
    failing = _ObservedAsyncIterator(Failing(), lambda _event: None, errors.append)
    with pytest.raises(RuntimeError) as caught:
        await anext(failing)
    assert caught.value is expected
    assert errors == [expected]


def test_openai_agents_registration_rollback_leaves_no_processor() -> None:
    class Subject:
        def __init__(self) -> None:
            self.processors: list[Any] = []

        def add_trace_processor(self, processor: Any) -> None:
            self.processors.append(processor)
            raise RuntimeError("registration retained then failed")

        def remove_trace_processor(self, processor: Any) -> None:
            self.processors.remove(processor)

    subject = Subject()
    with pytest.raises(RuntimeError, match="retained then failed"):
        openai_agents_adapter(
            version=_version("openai-agents")
        ).create_source(subject).install(_CountingSink())
    assert subject.processors == []


def test_framework_install_failures_are_transactional_or_inert() -> None:
    sink = _CountingSink()

    class GraphBase:
        def invoke(self, *args: Any, **kwargs: Any) -> None:
            return None

        def ainvoke(self, *args: Any, **kwargs: Any) -> None:
            return None

    class RejectingGraph(GraphBase):
        def __setattr__(self, name: str, value: Any) -> None:
            if name == "ainvoke":
                raise RuntimeError("graph patch rejected")
            super().__setattr__(name, value)

    graph = RejectingGraph()
    with pytest.raises(RuntimeError, match="graph patch rejected"):
        langgraph_adapter(version=_version("langgraph")).create_source(graph).install(
            sink
        )
    assert "invoke" not in vars(graph)

    class StrandsSubject:
        callback_handler = None

        def __init__(self) -> None:
            self.callbacks: list[Any] = []

        def add_hook(self, callback: Any, _event_type: Any) -> None:
            self.callbacks.append(callback)
            if len(self.callbacks) == 2:
                raise RuntimeError("hook registration rejected")

    strands_subject = StrandsSubject()
    with pytest.raises(RuntimeError, match="hook registration rejected"):
        strands_adapter(version=_version("strands-agents")).create_source(
            strands_subject
        ).install(sink)
    for callback in strands_subject.callbacks:
        callback(object())
    assert sink.calls == 0

    class PydanticBase:
        async def run(self, *args: Any, **kwargs: Any) -> None:
            return None

        def run_sync(self, *args: Any, **kwargs: Any) -> None:
            return None

    class RejectingPydantic(PydanticBase):
        def __setattr__(self, name: str, value: Any) -> None:
            if name == "run_sync":
                raise RuntimeError("pydantic patch rejected")
            super().__setattr__(name, value)

    pydantic_subject = RejectingPydantic()
    with pytest.raises(RuntimeError, match="pydantic patch rejected"):
        pydantic_ai_adapter(version=PYDANTIC_AI_VERSION).create_source(
            pydantic_subject
        ).install(sink)
    assert "run" not in vars(pydantic_subject)


@pytest.mark.asyncio
async def test_google_adk_failed_registration_leaves_plugin_inert() -> None:
    sink = _CountingSink()

    class Manager:
        plugin: Any = None

        def register_plugin(self, plugin: Any) -> None:
            self.plugin = plugin
            raise RuntimeError("plugin registration rejected")

    manager = Manager()
    runner = SimpleNamespace(plugin_manager=manager)
    with pytest.raises(RuntimeError, match="plugin registration rejected"):
        google_adk_adapter(version=GOOGLE_ADK_VERSION).create_source(runner).install(
            sink
        )
    await manager.plugin.before_run_callback(
        invocation_context=SimpleNamespace(invocation_id="late", session=None)
    )
    assert sink.calls == 0
