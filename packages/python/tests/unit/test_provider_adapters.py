from __future__ import annotations

import inspect
import json
import os
from importlib.metadata import version as distribution_version
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import Any

import pytest

import semantic_layer.provider_adapters as provider_adapters_module
from semantic_layer import (
    anthropic_provider_adapter,
    gemini_provider_adapter,
    initialize,
    openai_provider_adapter,
)
from semantic_layer.capture_v1 import AdmissionReceipt, OpenTraceReceipt
from semantic_layer.validation import validate_artifact


class _ObservationError(RuntimeError):
    pass


def _openai_fixture_client(create: Any) -> Any:
    return type(
        "Client",
        (),
        {
            "responses": type("Responses", (), {"create": create})(),
            "chat": type(
                "Chat",
                (),
                {"completions": type("Completions", (), {"create": create})()},
            )(),
        },
    )()


def test_provider_versions_and_seams_are_exact() -> None:
    expected = json.loads(os.environ.get("SEMANTIC_LAYER_EXPECTED_VERSIONS", "{}"))
    for package, value in expected.items():
        assert distribution_version(package) == value
    sources = [
        openai_provider_adapter().create_source(object()),
        openai_provider_adapter(provider="openrouter").create_source(object()),
        anthropic_provider_adapter().create_source(object()),
        gemini_provider_adapter().create_source(object()),
    ]
    assert [(source.metadata["name"], source.metadata["seam"]) for source in sources] == [
        ("provider:openai", "responses.create/chat.completions.create sync+async"),
        (
            "provider:openrouter",
            "OpenAI-compatible responses.create/chat.completions.create sync+async",
        ),
        ("provider:anthropic", "messages._post /v1/messages sync+async"),
        ("provider:gemini", "models/aio.models per-request generate_content seams"),
    ]
    assert [source.metadata["qualification"] for source in sources] == [
        {"status": "exact_qualified"},
        {"status": "exact_qualified"},
        {"status": "exact_qualified"},
        {"status": "exact_qualified"},
    ]


def test_provider_newer_version_has_bounded_capability_qualification(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_adapters_module, "distribution_version", lambda _: "2.46.0")
    source = openai_provider_adapter().create_source(object())

    assert source.metadata["qualification"] == {
        "status": "capability_checked_unqualified",
        "profile": "provider-reasoning-v1",
    }


def test_provider_install_transaction_restores_prior_attribute_shape(
    tmp_path: Path,
) -> None:
    class Responses:
        def create(self, *_args: Any, **_kwargs: Any) -> object:
            return object()

    class Completions:
        pass

    responses = Responses()
    client = type(
        "Client",
        (),
        {"responses": responses, "chat": type("Chat", (), {"completions": Completions()})()},
    )()
    assert "create" not in vars(responses)
    capture = initialize(output=tmp_path, service_name="provider-rollback")
    with pytest.raises(TypeError, match="chat.completions.create"):
        capture.instrument(adapter=openai_provider_adapter(), client=client)
    assert "create" not in vars(responses)
    capture.shutdown()


def test_provider_stream_is_consumed_once_and_return_values_are_unchanged(
    tmp_path: Path,
) -> None:
    class Stream:
        def __init__(self) -> None:
            self._values = iter(
                (
                    {"choices": [{"delta": {"content": "one"}}]},
                    {"choices": [{"delta": {"content": "two"}, "finish_reason": "stop"}]},
                )
            )
            self.pulls = 0

        def __iter__(self) -> Stream:
            return self

        def __next__(self) -> dict[str, Any]:
            self.pulls += 1
            return next(self._values)

    stream = Stream()

    def create(*_args: Any, **_kwargs: Any) -> Stream:
        return stream

    client = type(
        "Client",
        (),
        {
            "responses": type("Responses", (), {"create": create})(),
            "chat": type(
                "Chat",
                (),
                {"completions": type("Completions", (), {"create": create})()},
            )(),
        },
    )()
    capture = initialize(output=tmp_path, service_name="provider-stream")
    capture.instrument(adapter=openai_provider_adapter(), client=client)
    values = list(client.chat.completions.create(model="fixture", stream=True))
    assert [value["choices"][0]["delta"]["content"] for value in values] == [
        "one",
        "two",
    ]
    assert stream.pulls == 3
    assert validate_artifact(capture.shutdown().artifact_path).valid


def test_capture_getters_cannot_prevent_the_provider_call(tmp_path: Path) -> None:
    getter_calls = 0

    class HostileRequest:
        @property
        def model(self) -> str:
            nonlocal getter_calls
            getter_calls += 1
            raise _ObservationError("capture must not read this descriptor")

    sentinel = object()
    calls: list[object] = []

    def create(_self: object, request: object) -> object:
        calls.append(request)
        return sentinel

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-hostile-request")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    request = HostileRequest()
    assert client.responses.create(request) is sentinel
    assert calls == [request]
    assert getter_calls == 0
    capture.shutdown()


def test_capture_sink_failure_cannot_prevent_the_provider_call() -> None:
    sentinel = object()
    calls = 0

    def create(*_args: Any, **_kwargs: Any) -> object:
        nonlocal calls
        calls += 1
        return sentinel

    class FailingSink:
        def open_trace(self, _record: object) -> object:
            raise _ObservationError("capture sink failed")

    client = _openai_fixture_client(create)
    source = openai_provider_adapter().create_source(client)
    lifecycle = source.install(FailingSink())

    assert client.responses.create(model="fixture") is sentinel
    assert calls == 1
    lifecycle.deactivate()


@pytest.mark.asyncio
async def test_capture_sink_failure_cannot_prevent_an_async_provider_call() -> None:
    sentinel = object()
    calls = 0

    async def create(*_args: Any, **_kwargs: Any) -> object:
        nonlocal calls
        calls += 1
        return sentinel

    class FailingSink:
        def open_trace(self, _record: object) -> object:
            raise _ObservationError("capture sink failed")

    client = _openai_fixture_client(create)
    source = openai_provider_adapter().create_source(client)
    lifecycle = source.install(FailingSink())

    assert await client.responses.create(model="fixture") is sentinel
    assert calls == 1
    lifecycle.deactivate()


@pytest.mark.asyncio
async def test_async_stream_close_preserves_the_original_provider_error(
    tmp_path: Path,
) -> None:
    failure = _ObservationError("provider close failed")

    class StreamIterator:
        def __init__(self) -> None:
            self.pulls = 0

        def __aiter__(self) -> StreamIterator:
            return self

        async def __anext__(self) -> dict[str, Any]:
            self.pulls += 1
            if self.pulls > 1:
                raise StopAsyncIteration
            return {"choices": [{"delta": {"content": "observed"}}]}

    class StreamResult:
        def __init__(self) -> None:
            self._iterator = StreamIterator()
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            raise failure

    result = StreamResult()

    async def create(*_args: Any, **_kwargs: Any) -> StreamResult:
        return result

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-close-error")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    observed = await client.responses.create(model="fixture", stream=True)
    assert observed is result
    assert await observed._iterator.__anext__() == {"choices": [{"delta": {"content": "observed"}}]}
    with pytest.raises(_ObservationError) as caught:
        await observed.close()

    assert caught.value is failure
    assert observed.close_calls == 1
    assert observed._iterator._target.pulls == 1
    records = _trace_records((await capture.shutdown_async()).artifact_path)
    assert (
        next(record for record in records if record["kind"] == "run.outcome")["data"]["status"]
        == "failed"
    )


def test_capture_cannot_replace_the_original_provider_error(tmp_path: Path) -> None:
    getter_calls = 0
    stringify_calls = 0

    class ProviderError(RuntimeError):
        @property
        def code(self) -> str:
            nonlocal getter_calls
            getter_calls += 1
            raise _ObservationError("capture must not read error descriptors")

        def __str__(self) -> str:
            nonlocal stringify_calls
            stringify_calls += 1
            raise _ObservationError("capture must not stringify provider errors")

    failure = ProviderError()

    def create(*_args: Any, **_kwargs: Any) -> object:
        raise failure

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-hostile-error")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    with pytest.raises(ProviderError) as caught:
        client.responses.create(model="fixture")
    assert caught.value is failure
    assert getter_calls == 0
    assert stringify_calls == 0
    capture.shutdown()


def test_million_argument_provider_error_is_rethrown_with_bounded_capture_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ProviderError(RuntimeError):
        pass

    failure = ProviderError()
    failure.args = (("x" * 10_000),) + (("tail",) * 999_999)
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_EXCEPTION_SCAN_BYTES", 128)
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_EXCEPTION_SCAN_NODES", 4)

    def create(*_args: Any, **_kwargs: Any) -> object:
        raise failure

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-million-arg-error")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    with pytest.raises(ProviderError) as caught:
        client.responses.create(model="fixture")
    assert caught.value is failure

    records = _trace_records(capture.shutdown().artifact_path)
    error = next(record for record in records if record["kind"] == "error")
    assert len(error["data"]["message"]) <= 32


def test_provider_result_inspection_does_not_invoke_descriptors(tmp_path: Path) -> None:
    descriptor_calls = 0

    class Result:
        @property
        def _iterator(self) -> object:
            nonlocal descriptor_calls
            descriptor_calls += 1
            raise _ObservationError("capture inspected a hostile result descriptor")

        @property
        def usage(self) -> object:
            nonlocal descriptor_calls
            descriptor_calls += 1
            raise _ObservationError("capture inspected a hostile usage descriptor")

    result = Result()

    def create(*_args: Any, **_kwargs: Any) -> Result:
        return result

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-hostile-result")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert client.responses.create(model="fixture") is result
    assert descriptor_calls == 0
    capture.shutdown()


def test_stream_result_wrapping_does_not_probe_close_descriptor(tmp_path: Path) -> None:
    close_descriptor_calls = 0
    original_iterator = iter(({"delta": "one"}, {"delta": "two"}))

    class StreamResult:
        def __init__(self) -> None:
            self._iterator = original_iterator

        @property
        def close(self) -> object:
            nonlocal close_descriptor_calls
            close_descriptor_calls += 1
            raise _ObservationError("capture inspected a hostile close descriptor")

    result = StreamResult()

    def create(*_args: Any, **_kwargs: Any) -> StreamResult:
        return result

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-stream-result")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert client.responses.create(model="fixture", stream=True) is result
    assert close_descriptor_calls == 0
    assert list(result._iterator) == [{"delta": "one"}, {"delta": "two"}]
    capture.shutdown()


def test_stream_result_wrap_failure_restores_the_original_iterator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_iterator = iter((1, 2))

    class StreamResult:
        def __init__(self) -> None:
            self._iterator = original_iterator

    class FailingObservedIterator:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise _ObservationError("wrapper construction failed")

    result = StreamResult()

    def create(*_args: Any, **_kwargs: Any) -> StreamResult:
        return result

    monkeypatch.setattr(provider_adapters_module, "_ObservedIterator", FailingObservedIterator)
    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-wrap-rollback")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert client.responses.create(model="fixture", stream=True) is result
    assert result._iterator is original_iterator
    capture.shutdown()


def test_observation_failure_emits_one_gap_without_changing_the_result() -> None:
    sentinel = object()

    def create(*_args: Any, **_kwargs: Any) -> object:
        return sentinel

    class RecoveringSink:
        def __init__(self) -> None:
            self.failed = False
            self.records: list[dict[str, Any]] = []

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id="root",
                identity={
                    "trace_id": "trace",
                    "operation_id": "operation",
                    "session_id": "session",
                },
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            if not self.failed:
                self.failed = True
                raise _ObservationError("one capture write failed")
            self.records.append(record)
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    client = _openai_fixture_client(create)
    sink = RecoveringSink()
    source = openai_provider_adapter().create_source(client)
    lifecycle = source.install(sink)

    assert client.responses.create(model="fixture") is sentinel
    gaps = [record for record in sink.records if record["phase"] == "gap"]
    assert len(gaps) == 1
    assert gaps[0]["name"] == "openai.observation.failed"
    lifecycle.deactivate()


def test_rejected_provider_request_does_not_advance_context_base() -> None:
    calls = 0

    def create(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"id": f"response-{calls}", "choices": []}

    class RecordingSink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []
            self.requests = 0

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id=f"root-{calls}",
                identity={
                    "trace_id": "shared-trace",
                    "operation_id": f"operation-{calls}",
                },
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            self.records.append(record)
            semantic = record.get("semantic", {})
            if semantic.get("type") == "model.request":
                self.requests += 1
                if self.requests == 2:
                    return AdmissionReceipt(False, "injected_rejection")
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    client = _openai_fixture_client(create)
    sink = RecordingSink()
    lifecycle = openai_provider_adapter().create_source(client).install(sink)

    client.chat.completions.create(
        model="fixture",
        messages=[{"role": "user", "content": "A"}],
    )
    client.chat.completions.create(
        model="fixture",
        messages=[
            {"role": "user", "content": "A"},
            {"role": "tool", "content": "B", "tool_call_id": "call-b"},
        ],
    )
    client.chat.completions.create(
        model="fixture",
        messages=[
            {"role": "user", "content": "A"},
            {"role": "tool", "content": "B", "tool_call_id": "call-b"},
            {"role": "user", "content": "C"},
        ],
    )

    requests = [
        record
        for record in sink.records
        if record.get("semantic", {}).get("type") == "model.request"
    ]
    assert len(requests) == 3
    first_ref = next(
        record_id
        for record_id, record in (
            (f"record-{index}", record)
            for index, record in enumerate(sink.records, 1)
        )
        if record is requests[0]
    )
    assert requests[2]["semantic"]["context_base_ref"] == first_ref
    assert len(requests[2]["semantic"]["context_refs"]) == 2
    assert "messages" not in requests[2]["native"]["request"]["metadata"]
    lifecycle.deactivate()


def test_rejected_provider_message_does_not_claim_or_advance_context() -> None:
    class RecordingSink:
        def __init__(self) -> None:
            self.records: list[tuple[dict[str, Any], str]] = []
            self.messages = 0

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id="root",
                identity={"trace_id": "shared-trace", "operation_id": "operation"},
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            record_id = f"record-{len(self.records) + 1}"
            self.records.append((record, record_id))
            if record.get("semantic", {}).get("type") == "message":
                self.messages += 1
                if self.messages == 2:
                    return AdmissionReceipt(False, "injected_rejection")
            return AdmissionReceipt(True, record_id=record_id)

    client = _openai_fixture_client(lambda *_args, **_kwargs: {"choices": []})
    sink = RecordingSink()
    lifecycle = openai_provider_adapter().create_source(client).install(sink)

    for messages in (
        [{"role": "user", "content": "A"}],
        [
            {"role": "user", "content": "A"},
            {"role": "tool", "content": "B", "tool_call_id": "call-b"},
        ],
        [
            {"role": "user", "content": "A"},
            {"role": "tool", "content": "B", "tool_call_id": "call-b"},
            {"role": "user", "content": "C"},
        ],
    ):
        client.chat.completions.create(model="fixture", messages=messages)

    requests = [
        (record, record_id)
        for record, record_id in sink.records
        if record.get("semantic", {}).get("type") == "model.request"
    ]
    assert requests[0][0]["semantic"]["context_refs"]
    assert "context_refs" not in requests[1][0]["semantic"]
    assert "context_base_ref" not in requests[1][0]["semantic"]
    assert requests[2][0]["semantic"]["context_base_ref"] == requests[0][1]
    assert len(requests[2][0]["semantic"]["context_refs"]) == 2
    lifecycle.deactivate()


def test_resource_limited_message_snapshots_never_form_a_context_base() -> None:
    class RecordingSink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id="root",
                identity={"trace_id": "shared-trace", "operation_id": "operation"},
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            self.records.append(record)
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    def deeply_nested(leaf: str) -> dict[str, Any]:
        value: Any = leaf
        for _ in range(64):
            value = {"next": value}
        return value

    client = _openai_fixture_client(lambda *_args, **_kwargs: {"choices": []})
    sink = RecordingSink()
    lifecycle = openai_provider_adapter().create_source(client).install(sink)

    for leaf in ("A", "B"):
        client.chat.completions.create(
            model="fixture",
            messages=[{"role": "user", "content": deeply_nested(leaf)}],
        )

    requests = [
        record
        for record in sink.records
        if record.get("semantic", {}).get("type") == "model.request"
    ]
    assert len(requests) == 2
    assert all("context_refs" not in record["semantic"] for record in requests)
    assert all("context_base_ref" not in record["semantic"] for record in requests)
    assert "resource_limit" in json.dumps(sink.records)
    lifecycle.deactivate()


def test_unmapped_responses_items_never_form_a_false_context_base() -> None:
    class RecordingSink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id="root",
                identity={"trace_id": "shared-trace", "operation_id": "operation"},
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            self.records.append(record)
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    client = _openai_fixture_client(lambda *_args, **_kwargs: {"choices": []})
    sink = RecordingSink()
    lifecycle = openai_provider_adapter().create_source(client).install(sink)

    for reference_id in ("item-a", "item-b"):
        client.responses.create(
            model="fixture",
            input=[
                {"role": "user", "content": "Inspect it."},
                {"type": "item_reference", "id": reference_id},
            ],
        )

    requests = [
        record
        for record in sink.records
        if record.get("semantic", {}).get("type") == "model.request"
    ]
    assert len(requests) == 2
    assert all("context_refs" not in record["semantic"] for record in requests)
    assert all("context_base_ref" not in record["semantic"] for record in requests)
    gaps = [
        record
        for record in sink.records
        if record.get("semantic", {}).get("type") == "capture.gap"
    ]
    assert len(gaps) == 2
    assert all(
        record["semantic"]["reason"] == "unsupported_native_value"
        for record in gaps
    )
    lifecycle.deactivate()


def test_provider_context_includes_instructions_and_rejects_remote_history() -> None:
    class RecordingSink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id="root",
                identity={"trace_id": "shared-trace", "operation_id": "operation"},
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            self.records.append(record)
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    client = _openai_fixture_client(lambda *_args, **_kwargs: {"choices": []})
    sink = RecordingSink()
    lifecycle = openai_provider_adapter().create_source(client).install(sink)

    for instructions in ("Instruction A", "Instruction B"):
        client.responses.create(
            model="fixture",
            instructions=instructions,
            input="Inspect it.",
        )
    for previous_response_id in ("response-a", "response-b"):
        client.responses.create(
            model="fixture",
            previous_response_id=previous_response_id,
            input="Continue.",
        )
    for prompt_id in ("prompt-a", "prompt-b"):
        client.responses.create(
            model="fixture",
            prompt={"id": prompt_id},
            input="Continue.",
        )

    requests = [
        record
        for record in sink.records
        if record.get("semantic", {}).get("type") == "model.request"
    ]
    assert len(requests[0]["semantic"]["context_refs"]) == 2
    assert len(requests[1]["semantic"]["context_refs"]) == 2
    assert "context_base_ref" not in requests[1]["semantic"]
    assert len(requests) == 6
    assert all(
        "context_refs" not in request["semantic"]
        and "context_base_ref" not in request["semantic"]
        for request in requests[2:]
    )
    assert all(
        "instructions" not in request["native"]["request"]["metadata"]
        for request in requests[:2]
    )
    gaps = [
        record
        for record in sink.records
        if record.get("semantic", {}).get("type") == "capture.gap"
    ]
    assert len(gaps) == 4
    lifecycle.deactivate()


def test_gemini_system_instruction_is_part_of_exact_context() -> None:
    module = provider_adapters_module
    request = {
        "contents": "Inspect it.",
        "config": {"system_instruction": "Instruction A"},
    }

    messages = module._request_messages("gemini", request)

    assert [
        (message["role"], message["content"])
        for message in messages
    ] == [
        ("system", "Instruction A"),
        ("user", "Inspect it."),
    ]
    assert module._request_context_complete("gemini", request, len(messages))
    native = module._provider_request_native(
        "gemini",
        request,
        message_count=len(messages),
    )
    assert native["metadata"]["config"] == {}


def test_provider_context_plan_and_request_admission_are_atomic() -> None:
    calls = 0
    first_request_entered = Event()
    release_first_request = Event()

    def create(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return {"id": f"response-{calls}", "choices": []}

    class BlockingSink:
        def __init__(self) -> None:
            self.records: list[tuple[dict[str, Any], str]] = []
            self.requests = 0

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id=f"root-{calls}",
                identity={
                    "trace_id": "shared-trace",
                    "operation_id": f"operation-{calls}",
                },
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            semantic = record.get("semantic", {})
            record_id = f"record-{len(self.records) + 1}"
            self.records.append((record, record_id))
            if semantic.get("type") == "model.request":
                self.requests += 1
                if self.requests == 1:
                    first_request_entered.set()
                    assert release_first_request.wait(5)
            return AdmissionReceipt(True, record_id=record_id)

    client = _openai_fixture_client(create)
    sink = BlockingSink()
    lifecycle = openai_provider_adapter().create_source(client).install(sink)
    errors: list[BaseException] = []

    def call(messages: list[dict[str, Any]]) -> None:
        try:
            client.chat.completions.create(model="fixture", messages=messages)
        except BaseException as error:
            errors.append(error)

    first = Thread(target=call, args=([{"role": "user", "content": "A"}],))
    second = Thread(
        target=call,
        args=(
            [
                {"role": "user", "content": "A"},
                {"role": "user", "content": "B"},
            ],
        ),
    )
    first.start()
    assert first_request_entered.wait(5)
    second.start()
    release_first_request.set()
    first.join(5)
    second.join(5)

    assert not errors
    assert not first.is_alive()
    assert not second.is_alive()
    requests = [
        (record, record_id)
        for record, record_id in sink.records
        if record.get("semantic", {}).get("type") == "model.request"
    ]
    assert len(requests) == 2
    assert requests[1][0]["semantic"]["context_base_ref"] == requests[0][1]
    assert len(requests[1][0]["semantic"]["context_refs"]) == 1
    lifecycle.deactivate()


def test_provider_context_history_stays_constant_size_for_50k_messages() -> None:
    class CountingSink:
        def __init__(self) -> None:
            self.records = 0

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id="root",
                identity={"trace_id": "shared-trace", "operation_id": "operation"},
            )

        def record(self, _record: dict[str, Any]) -> AdmissionReceipt:
            self.records += 1
            return AdmissionReceipt(True, record_id=f"record-{self.records}")

    client = _openai_fixture_client(lambda *_args, **_kwargs: {"choices": []})
    sink = CountingSink()
    source = openai_provider_adapter().create_source(client)
    lifecycle = source.install(sink)
    repeated = {"role": "user", "content": "bounded"}

    client.chat.completions.create(
        model="fixture",
        messages=[repeated] * 50_000,
    )

    history = source._context_histories["shared-trace"]
    assert history.message_count == 50_000
    assert isinstance(history.digest, str)
    assert isinstance(history.request_ref, str)
    assert not any(
        isinstance(value, (list, dict, set))
        for value in vars(history).values()
    )
    lifecycle.deactivate()


def test_raw_stream_retention_replaces_only_rejected_native_parts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_STREAM_RETAINED_BYTES", 512)
    raw_secret = "raw-only-secret-" + ("x" * 1_024)
    chunks = [
        {"choices": [{"delta": {"content": "kept"}}]},
        {
            "raw_only": raw_secret,
            "choices": [
                {
                    "delta": {"content": "this is a test"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 2, "completion_tokens": 3},
        },
    ]

    def create(*_args: Any, **_kwargs: Any) -> Any:
        yield from chunks

    class RecordingSink:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []

        def open_trace(self, _record: object) -> OpenTraceReceipt:
            return OpenTraceReceipt(
                True,
                record_id="root",
                identity={
                    "trace_id": "trace",
                    "operation_id": "operation",
                    "session_id": "session",
                },
            )

        def record(self, record: dict[str, Any]) -> AdmissionReceipt:
            self.records.append(record)
            return AdmissionReceipt(True, record_id=f"record-{len(self.records)}")

    client = _openai_fixture_client(create)
    sink = RecordingSink()
    lifecycle = openai_provider_adapter().create_source(client).install(sink)

    assert list(client.responses.create(model="fixture", stream=True)) == chunks

    deltas = [record for record in sink.records if record["name"] == "openai.stream.delta"]
    assert deltas[0]["native"]["part"] is chunks[0]
    assert deltas[1]["native"]["part"] == {"observed": True, "retained": False}
    assert raw_secret not in json.dumps(sink.records)
    assert not [record for record in sink.records if record["phase"] == "gap"]
    lifecycle.deactivate()


@pytest.mark.parametrize(
    ("limit_name", "limit_value"),
    [
        ("_MAX_PROVIDER_STREAM_RETAINED_BYTES", 32),
        ("_MAX_PROVIDER_STREAM_RETAINED_NODES", 2),
    ],
)
def test_raw_stream_retention_budget_keeps_complete_semantics_without_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    limit_value: int,
) -> None:
    monkeypatch.setattr(provider_adapters_module, limit_name, limit_value)
    raw_secret = "raw-only-stream-sentinel"
    chunks = [
        {
            "model": "fixture",
            "raw_only": raw_secret,
            "choices": [{"delta": {"reasoning_content": "r" * 64}}],
        },
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-1",
                                "function": {"name": "lookup", "arguments": '{"q":'},
                            }
                        ]
                    }
                }
            ]
        },
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "function": {"arguments": '"value"}'},
                            }
                        ]
                    }
                }
            ]
        },
        {
            "choices": [
                {
                    "delta": {"content": "this is a test"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
        },
    ]

    def create(*_args: Any, **_kwargs: Any) -> Any:
        yield from chunks

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-stream-budget")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert list(client.responses.create(model="fixture", stream=True)) == chunks

    artifact = Path(capture.shutdown().artifact_path)
    records = _trace_records(str(artifact))
    assert raw_secret not in (artifact / "trace.jsonl").read_text()
    assert not [record for record in records if record["kind"] == "loss"]
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["model"] == "fixture"
    assert response["data"]["content"] == "this is a test"
    assert response["data"]["finish_reason"] == "stop"
    assert response["data"]["usage"] == {"input_tokens": 2, "output_tokens": 3}
    assert sum(
        len(item["text"])
        for item in response["data"].get("reasoning", [])
        if item["type"] == "text"
    ) <= 64
    tool_proposal = next(record for record in records if record["kind"] == "tool.proposal")
    assert tool_proposal["data"]["name"] == "lookup"
    assert tool_proposal["data"]["input"] == {"q": "value"}
    assert tool_proposal["data"]["native_call_id"] == "call-1"


def test_terminal_response_replaces_prior_deltas_after_raw_retention_exhaustion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_STREAM_RETAINED_BYTES", 512)
    terminal_response = {
        "model": "fixture",
        "output_text": "this is a test",
        "reasoning": [{"type": "text", "text": "think once"}],
        "usage": {"input_tokens": 2, "output_tokens": 3},
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "function": {
                                "name": "lookup",
                                "arguments": '{"q":"value"}',
                            },
                        }
                    ]
                }
            }
        ],
    }
    chunks = [
        {"type": "response.reasoning_text.delta", "delta": "think once"},
        {
            "type": "response.completed",
            "response": terminal_response,
            "padding": "x" * 1_024,
        },
    ]

    def create(*_args: Any, **_kwargs: Any) -> Any:
        yield from chunks

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-terminal-budget")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert list(client.responses.create(model="fixture", stream=True)) == chunks

    records = _trace_records(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == "this is a test"
    assert response["data"]["reasoning"] == [{"type": "text", "text": "think once"}]
    assert response["data"]["usage"] == {"input_tokens": 2, "output_tokens": 3}
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    assert len(proposals) == 1
    assert proposals[0]["data"]["input"] == {"q": "value"}
    assert not [record for record in records if record["kind"] == "loss"]


def test_canonical_stream_nodes_are_bounded_per_semantic_channel(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_STREAM_SEMANTIC_NODES", 8)
    chunks = [
        *[
            {"model": "fixture", "choices": [{"delta": {"reasoning_content": "r"}}]}
            for _ in range(20)
        ],
        {
            "choices": [
                {
                    "delta": {
                        "tool_calls": [
                            {
                                "index": 0,
                                "id": "call-1",
                                "function": {
                                    "name": "lookup",
                                    "arguments": '{"q":"value"}',
                                },
                            }
                        ]
                    }
                }
            ]
        },
        {
            "choices": [
                {
                    "delta": {"content": "this is a test"},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 2, "completion_tokens": 3},
        },
    ]

    def create(*_args: Any, **_kwargs: Any) -> Any:
        yield from chunks

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-semantic-node-budget")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert list(client.responses.create(model="fixture", stream=True)) == chunks

    records = _trace_records(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == "this is a test"
    assert response["data"]["reasoning"] == [{"type": "text", "text": "r" * 8}]
    assert response["data"]["finish_reason"] == "stop"
    assert response["data"]["usage"] == {"input_tokens": 2, "output_tokens": 3}
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    assert proposal["data"]["input"] == {"q": "value"}
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "provider_stream_semantic_truncated"


def test_stream_materialization_stops_at_width_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    iterations = 0

    class WideChoices(list[object]):
        def __iter__(self) -> Any:
            nonlocal iterations
            for value in super().__iter__():
                iterations += 1
                yield value

    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_MATERIALIZATION_WIDTH", 4)
    wide_choices = WideChoices(
        [{"delta": {"content": str(index)}} for index in range(100)]
    )
    chunks = [
        {"model": "fixture", "choices": wide_choices},
        {
            "choices": [
                {
                    "delta": {"content": "done"},
                    "finish_reason": "stop",
                }
            ]
        },
    ]

    def create(*_args: Any, **_kwargs: Any) -> Any:
        yield from chunks

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-width-budget")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert list(client.responses.create(model="fixture", stream=True)) == chunks
    records = _trace_records(capture.shutdown().artifact_path)

    assert iterations <= 5
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "provider_stream_semantic_truncated"


def test_stream_text_fragments_coalesce_after_width_without_semantic_loss(
    tmp_path: Path,
) -> None:
    chunks = [
        {"choices": [{"delta": {"content": str(index % 10)}}]}
        for index in range(200)
    ]
    chunks.append(
        {
            "choices": [
                {
                    "delta": {},
                    "finish_reason": "stop",
                }
            ]
        }
    )

    def create(*_args: Any, **_kwargs: Any) -> Any:
        yield from chunks

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-fragment-width")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert list(client.responses.create(model="fixture", stream=True)) == chunks
    records = _trace_records(capture.shutdown().artifact_path)

    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == "0123456789" * 20
    assert not [record for record in records if record["kind"] == "loss"]


def test_stream_text_byte_overflow_remains_one_explicit_semantic_loss(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_STREAM_SEMANTIC_BYTES", 10)
    chunks = [
        {"choices": [{"delta": {"content": "12345"}}]},
        {"choices": [{"delta": {"content": "67890"}}]},
        {"choices": [{"delta": {"content": "overflow"}}]},
        {"choices": [{"delta": {}, "finish_reason": "stop"}]},
    ]

    def create(*_args: Any, **_kwargs: Any) -> Any:
        yield from chunks

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-fragment-byte-budget")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert list(client.responses.create(model="fixture", stream=True)) == chunks
    records = _trace_records(capture.shutdown().artifact_path)

    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == "1234567890"
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "provider_stream_semantic_truncated"


def test_gemini_stream_materialization_stops_at_candidate_and_part_width(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_iterations = 0
    part_iterations = 0

    class WideCandidates(list[object]):
        def __iter__(self) -> Any:
            nonlocal candidate_iterations
            for value in super().__iter__():
                candidate_iterations += 1
                yield value

    class WideParts(list[object]):
        def __iter__(self) -> Any:
            nonlocal part_iterations
            for value in super().__iter__():
                part_iterations += 1
                yield value

    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_MATERIALIZATION_WIDTH", 4)
    chunks = [
        {
            "candidates": WideCandidates(
                [{"content": {"parts": [{"text": str(index)}]}} for index in range(100)]
            )
        },
        {
            "candidates": [
                {
                    "content": {
                        "parts": WideParts([{"text": str(index)} for index in range(100)])
                    }
                }
            ]
        },
        {
            "candidates": [
                {
                    "content": {"parts": [{"text": "done"}]},
                    "finishReason": "STOP",
                }
            ]
        },
    ]

    def generate_content_stream(**_kwargs: Any) -> Any:
        yield from chunks

    models = SimpleNamespace(
        generate_content=lambda **_kwargs: {},
        generate_content_stream=generate_content_stream,
    )
    client = SimpleNamespace(
        models=models,
        aio=SimpleNamespace(models=SimpleNamespace(**vars(models))),
    )
    capture = initialize(output=tmp_path, service_name="gemini-width-budget")
    capture.instrument(
        adapter=gemini_provider_adapter(version="2.11.0"),
        client=client,
    )

    assert list(
        client.models.generate_content_stream(model="fixture", contents="Bound the stream")
    ) == chunks
    records = _trace_records(capture.shutdown().artifact_path)

    assert candidate_iterations <= 5
    assert part_iterations <= 5
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "provider_stream_semantic_truncated"


@pytest.mark.parametrize(
    "payload",
    [
        "{" + '"padding":"' + ("x" * 20_000) + '"}',
        {
            **{f"field-{index}": {"ignored": index} for index in range(200)},
            "custom": {"code": "SEMANTIC_LAYER_CAPTURE_FAILURE_V1"},
        },
    ],
)
def test_structured_exception_preprocessing_is_bounded(
    payload: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_EXCEPTION_SCAN_BYTES", 128)
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_EXCEPTION_SCAN_WIDTH", 8)
    error = RuntimeError("provider failed")
    error.payload = payload  # type: ignore[attr-defined]

    assert provider_adapters_module._structured_error_evidence(error) is None


def test_huge_application_container_is_not_enumerated_and_result_is_unchanged(
    tmp_path: Path,
) -> None:
    iterations = 0

    class HugeChoices(list[object]):
        def __iter__(self) -> Any:
            nonlocal iterations
            iterations += 1
            raise _ObservationError("capture enumerated an application container")

    choices = HugeChoices([{}] * 100_000)
    result = {"choices": choices}

    def create(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return result

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-huge-container")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    assert client.responses.create(model="fixture") is result
    assert iterations == 0

    records = _trace_records(capture.shutdown().artifact_path)
    request = next(record for record in records if record["kind"] == "model.request")
    losses = [record for record in records if record["kind"] == "loss"]
    retention_losses = [
        loss
        for loss in losses
        if "openai.evidence.retention_truncated" in loss["data"].get("detail", "")
    ]
    assert request["data"]["context_refs"] == []
    assert len(retention_losses) == 1


def test_unretained_provider_audit_keeps_exact_semantic_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_EVIDENCE_BYTES", 64)

    def create(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"id": "response", "choices": []}

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-large-request")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    client.responses.create(model="fixture", input="x" * 1_024)

    records = _trace_records(capture.shutdown().artifact_path)
    request = next(record for record in records if record["kind"] == "model.request")
    assert len(request["data"]["context_refs"]) == 1
    assert "context_base_ref" not in request["data"]
    assert any(
        record["kind"] == "loss"
        and "openai.evidence.retention_truncated" in record["data"].get("detail", "")
        for record in records
    )


def test_huge_tool_json_is_not_parsed_and_result_is_unchanged(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parses = 0
    huge_arguments = '{"query":"' + ("x" * 20_000) + '"}'
    result = {
        "choices": [
            {
                "message": {
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "function": {
                                "name": "lookup",
                                "arguments": huge_arguments,
                            },
                        }
                    ]
                }
            }
        ]
    }
    original_loads = provider_adapters_module.json.loads

    def counting_loads(value: str, *args: Any, **kwargs: Any) -> Any:
        nonlocal parses
        parses += 1
        return original_loads(value, *args, **kwargs)

    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_STREAM_RETAINED_BYTES", 512 * 1024)
    monkeypatch.setattr(provider_adapters_module, "_MAX_PROVIDER_EXCEPTION_SCAN_BYTES", 1024)

    def create(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return result

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-huge-json")
    capture.instrument(adapter=openai_provider_adapter(), client=client)
    monkeypatch.setattr(provider_adapters_module.json, "loads", counting_loads)

    assert client.responses.create(model="fixture") is result
    assert parses == 0
    capture.shutdown()


def test_raw_generator_protocol_and_observation_are_transparent(tmp_path: Path) -> None:
    class HostileChunk:
        @property
        def type(self) -> str:
            raise _ObservationError("capture must not inspect this descriptor")

    first = HostileChunk()

    def stream() -> Any:
        sent = yield first
        try:
            yield f"sent:{sent}"
        except LookupError:
            yield "handled"
        return "provider-terminal"

    def create(*_args: Any, **_kwargs: Any) -> Any:
        return stream()

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-generator")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    observed = client.responses.create(model="fixture", stream=True)
    assert inspect.isgenerator(observed)
    assert next(observed) is first
    assert observed.send("value") == "sent:value"
    assert observed.throw(LookupError("recover")) == "handled"
    with pytest.raises(StopIteration) as stopped:
        next(observed)
    assert stopped.value.value == "provider-terminal"
    capture.shutdown()


def test_raw_generator_error_before_first_yield_keeps_exact_identity(
    tmp_path: Path,
) -> None:
    failure = _ObservationError("provider failed before the first chunk")

    def stream() -> Any:
        raise failure
        yield

    def create(*_args: Any, **_kwargs: Any) -> Any:
        return stream()

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-generator-error")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    observed = client.responses.create(model="fixture", stream=True)
    with pytest.raises(_ObservationError) as caught:
        next(observed)
    assert caught.value is failure

    records = _trace_records(capture.shutdown().artifact_path)
    assert len([record for record in records if record["kind"] == "error"]) == 1
    assert not [record for record in records if record["kind"] == "model.response"]
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.parametrize(
    ("partial", "expected_response"),
    [
        (
            {
                "model": "fixture",
                "choices": [{"delta": {"content": "observed partial"}}],
            },
            {
                "status": "failed",
                "model": "fixture",
                "content": "observed partial",
            },
        ),
        (
            {
                "type": "response.output_text.delta",
                "delta": "observed partial",
                "response_id": "response-partial",
            },
            {
                "status": "failed",
                "content": "observed partial",
            },
        ),
    ],
)
def test_stream_error_after_observed_chunk_keeps_partial_before_error(
    tmp_path: Path,
    partial: dict[str, Any],
    expected_response: dict[str, Any],
) -> None:
    failure = _ObservationError("provider failed after one chunk")

    def stream() -> Any:
        yield partial
        raise failure

    def create(*_args: Any, **_kwargs: Any) -> Any:
        return stream()

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-partial-error")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    observed = client.responses.create(model="fixture", stream=True)
    assert next(observed) is partial
    with pytest.raises(_ObservationError) as caught:
        next(observed)
    assert caught.value is failure

    records = _trace_records(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    losses = [record for record in records if record["kind"] == "loss"]
    errors = [record for record in records if record["kind"] == "error"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert len(responses) == len(losses) == len(errors) == len(outcomes) == 1
    response = responses[0]
    loss = losses[0]
    error = errors[0]
    outcome = outcomes[0]

    assert response["data"] == expected_response
    assert loss["data"]["reason"] == "stream_terminal_not_observed"
    assert loss["parent"] == response["id"]
    assert response["seq"] < loss["seq"] < error["seq"] < outcome["seq"]
    assert outcome["data"]["status"] == "failed"


@pytest.mark.asyncio
async def test_raw_async_generator_is_observed_lazily_without_protocol_changes(
    tmp_path: Path,
) -> None:
    entered = False

    async def stream() -> Any:
        nonlocal entered
        entered = True
        sent = yield {"choices": [{"delta": {"content": "one"}}]}
        try:
            yield {"choices": [{"delta": {"content": f":{sent}"}}]}
        except LookupError:
            yield {"choices": [{"delta": {"content": ":handled"}, "finish_reason": "stop"}]}

    def create(*_args: Any, **_kwargs: Any) -> Any:
        return stream()

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-async-generator")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    observed = client.responses.create(model="fixture", stream=True)
    assert inspect.isasyncgen(observed)
    assert entered is False
    assert (await observed.__anext__())["choices"][0]["delta"]["content"] == "one"
    assert entered is True
    assert (await observed.asend("two"))["choices"][0]["delta"]["content"] == ":two"
    assert (await observed.athrow(LookupError("recover")))["choices"][0]["delta"][
        "content"
    ] == ":handled"
    with pytest.raises(StopAsyncIteration):
        await observed.__anext__()

    artifact = capture.shutdown()
    records = _trace_records(artifact.artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == "one:two:handled"


@pytest.mark.asyncio
async def test_awaitable_resolving_to_async_generator_stays_lazy(
    tmp_path: Path,
) -> None:
    entered = False

    async def stream() -> Any:
        nonlocal entered
        entered = True
        yield {"choices": [{"delta": {"content": "resolved"}, "finish_reason": "stop"}]}

    async def resolve() -> Any:
        return stream()

    def create(*_args: Any, **_kwargs: Any) -> Any:
        return resolve()

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-awaitable-generator")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    observed = await client.responses.create(model="fixture", stream=True)
    assert inspect.isasyncgen(observed)
    assert entered is False
    assert (await observed.__anext__())["choices"][0]["delta"]["content"] == "resolved"
    assert entered is True
    with pytest.raises(StopAsyncIteration):
        await observed.__anext__()

    records = _trace_records((await capture.shutdown_async()).artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == "resolved"
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.asyncio
async def test_raw_async_generator_error_before_first_yield_keeps_exact_identity(
    tmp_path: Path,
) -> None:
    failure = _ObservationError("async provider failed before the first chunk")

    async def stream() -> Any:
        raise failure
        yield

    def create(*_args: Any, **_kwargs: Any) -> Any:
        return stream()

    client = _openai_fixture_client(create)
    capture = initialize(output=tmp_path, service_name="provider-async-generator-error")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    observed = client.responses.create(model="fixture", stream=True)
    with pytest.raises(_ObservationError) as caught:
        await observed.__anext__()
    assert caught.value is failure

    closed = await capture.shutdown_async()
    records = _trace_records(closed.artifact_path)
    assert len([record for record in records if record["kind"] == "error"]) == 1
    assert not [record for record in records if record["kind"] == "loss"]


def _trace_records(artifact_path: str) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in (Path(artifact_path) / "trace.jsonl").read_text().splitlines()
    ]
