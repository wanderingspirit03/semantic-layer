from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from anthropic import Anthropic, AsyncAnthropic, OverloadedError
from google import genai
from google.genai import types as genai_types
from google.genai.errors import ServerError
from jsonschema import Draft202012Validator, FormatChecker
from openai import AsyncOpenAI, AuthenticationError, OpenAI

from semantic_layer import (
    anthropic_provider_adapter,
    create_custom_agent_source,
    gemini_provider_adapter,
    initialize,
    openai_provider_adapter,
    provider_capture_context,
    reset_capture_for_tests,
)
from semantic_layer.validation import validate_artifact

_TRACE_SCHEMA_PATH = (
    Path(__file__).parents[4] / "contracts/trace/v1/semantic-trace-record.schema.json"
)
_TRACE_VALIDATOR = Draft202012Validator(
    json.loads(_TRACE_SCHEMA_PATH.read_text()),
    format_checker=FormatChecker(),
)


@pytest.fixture(autouse=True)
def _reset_capture() -> Any:
    yield
    reset_capture_for_tests()


def _trace_records(artifact_path: str) -> list[dict[str, Any]]:
    records = [
        json.loads(line) for line in (Path(artifact_path) / "trace.jsonl").read_text().splitlines()
    ]
    for record in records:
        _TRACE_VALIDATOR.validate(record)
    return records


def _openai_completed_response(response_id: str, text: str) -> dict[str, Any]:
    return {
        "id": response_id,
        "object": "response",
        "created_at": 1,
        "status": "completed",
        "model": "gpt-fixture",
        "output": [
            {
                "id": f"message-{response_id}",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": text,
                        "annotations": [],
                        "logprobs": [],
                    }
                ],
            }
        ],
        "parallel_tool_calls": True,
        "tool_choice": "auto",
        "tools": [],
        "usage": {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5},
    }


def test_openai_concurrent_same_name_proposals_keep_exact_native_ids(
    tmp_path: Path,
) -> None:
    lock = Lock()
    response_index = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal response_index
        with lock:
            response_index += 1
            index = response_index
        return httpx.Response(
            200,
            json={
                "id": f"chatcmpl-{index}",
                "object": "chat.completion",
                "created": 1,
                "model": "gpt-fixture",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": None,
                            "reasoning_content": f"considered {index}",
                            "tool_calls": [
                                {
                                    "id": f"call-{index}",
                                    "type": "function",
                                    "function": {
                                        "name": "lookup",
                                        "arguments": json.dumps({"slot": index}),
                                    },
                                }
                            ],
                        },
                        "finish_reason": "tool_calls",
                    }
                ],
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 1,
                    "total_tokens": 3,
                },
            },
        )

    client = OpenAI(
        api_key="openai-provider-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-provider-projection")
    capture.instrument(adapter=openai_provider_adapter(), client=client)

    def run(slot: int) -> None:
        client.chat.completions.create(
            model="gpt-fixture",
            messages=[
                {
                    "role": "tool",
                    "tool_call_id": f"prior-{slot}",
                    "content": f"prior result {slot}",
                },
                {"role": "user", "content": f"request {slot}"},
            ],
            tools=[
                {
                    "type": "function",
                    "function": {
                        "name": "lookup",
                        "description": "fixture",
                        "parameters": {
                            "type": "object",
                            "properties": {"slot": {"type": "number"}},
                        },
                    },
                }
            ],
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        list(executor.map(run, [1, 2]))

    records = _trace_records(capture.shutdown().artifact_path)
    requests = [record for record in records if record["kind"] == "model.request"]
    responses = [record for record in records if record["kind"] == "model.response"]
    proposals = [record for record in records if record["kind"] == "tool.proposal"]

    assert [record["kind"] for record in records].count("run.start") == 2
    assert [record["kind"] for record in records].count("run.outcome") == 2
    assert [record["kind"] for record in records].count("message") == 4
    assert len(requests) == len(responses) == len(proposals) == 2
    assert sorted(record["data"]["native_call_id"] for record in proposals) == [
        "call-1",
        "call-2",
    ]
    assert {record["data"]["name"] for record in proposals} == {"lookup"}
    assert len({record["data"]["call_id"] for record in proposals}) == 2
    assert all(
        any(
            link["type"] == "result_of"
            and any(request["id"] == link["record"] for request in requests)
            for link in response.get("links", [])
        )
        for response in responses
    )
    assert {json.dumps(record["data"]["reasoning"], sort_keys=True) for record in responses} == {
        '[{"text": "considered 1", "type": "text"}]',
        '[{"text": "considered 2", "type": "text"}]',
    }
    assert not [
        record for record in records if record["kind"] in {"tool.call", "tool.result", "loss"}
    ]


def test_openai_responses_preserves_exposed_summary_and_raw_reasoning_blocks(
    tmp_path: Path,
) -> None:
    response = _openai_completed_response("resp-reasoning", "Final answer.")
    response["output"].insert(
        0,
        {
            "id": "reasoning-resp-reasoning",
            "type": "reasoning",
            "status": "completed",
            "summary": [
                {"type": "summary_text", "text": "Checked the evidence."},
                {"type": "summary_text", "text": "Checked the evidence."},
            ],
            "content": [
                {"type": "reasoning_text", "text": "Exposed raw reasoning."},
                {"type": "reasoning_text", "text": "Exposed raw reasoning."},
            ],
            "encrypted_content": "opaque-encrypted-reasoning",
        },
    )

    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=response)

    client = OpenAI(
        api_key="openai-provider-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-reasoning-blocks")
    capture.instrument(adapter=openai_provider_adapter(version="2.45.0"), client=client)

    client.responses.create(model="gpt-fixture", input="Explain.")

    records = _trace_records(capture.shutdown().artifact_path)
    model_response = next(record for record in records if record["kind"] == "model.response")
    assert model_response["data"]["reasoning"] == [
        {"type": "summary", "text": "Checked the evidence."},
        {"type": "summary", "text": "Checked the evidence."},
        {"type": "text", "text": "Exposed raw reasoning."},
        {"type": "text", "text": "Exposed raw reasoning."},
    ]
    assert "opaque-encrypted-reasoning" not in json.dumps(model_response["data"])


def test_openrouter_preserves_ordered_reasoning_details_without_opaque_blocks(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-openrouter-reasoning",
                "object": "chat.completion",
                "created": 1,
                "model": "deepseek/deepseek-r1",
                "choices": [
                    {
                        "index": 0,
                        "message": {
                            "role": "assistant",
                            "content": "Final answer.",
                            "reasoning": "compatibility aggregate must not duplicate details",
                            "reasoning_details": [
                                {"type": "reasoning.summary", "summary": "Plan."},
                                {"type": "reasoning.text", "text": "Inspect A."},
                                {"type": "reasoning.text", "text": "Inspect A."},
                                {
                                    "type": "reasoning.encrypted",
                                    "data": "opaque-encrypted-detail",
                                    "signature": "opaque-signature",
                                },
                                {"type": "reasoning.summary", "text": "Conclude."},
                            ],
                        },
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5},
            },
        )

    client = OpenAI(
        api_key="openrouter-provider-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openrouter-reasoning-details")
    capture.instrument(
        adapter=openai_provider_adapter(version="2.45.0", provider="openrouter"),
        client=client,
    )

    client.chat.completions.create(
        model="deepseek/deepseek-r1",
        messages=[{"role": "user", "content": "Explain."}],
    )

    records = _trace_records(capture.shutdown().artifact_path)
    model_response = next(record for record in records if record["kind"] == "model.response")
    assert model_response["data"]["reasoning"] == [
        {"type": "summary", "text": "Plan."},
        {"type": "text", "text": "Inspect A."},
        {"type": "text", "text": "Inspect A."},
        {"type": "summary", "text": "Conclude."},
    ]
    assert "compatibility aggregate" not in json.dumps(model_response["data"]["reasoning"])
    assert "opaque-encrypted-detail" not in json.dumps(model_response["data"])
    assert "opaque-signature" not in json.dumps(model_response["data"])


def test_openai_2_45_responses_plain_string_is_one_user_context(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=_openai_completed_response("resp-plain-input", "It is windy."),
        )

    client = OpenAI(
        api_key="openai-provider-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-plain-input")
    capture.instrument(adapter=openai_provider_adapter(version="2.45.0"), client=client)

    response = client.responses.create(
        model="gpt-fixture",
        input="What's the weather like in SF?",
    )
    assert response.output_text == "It is windy."

    records = _trace_records(capture.shutdown().artifact_path)
    messages = [record for record in records if record["kind"] == "message"]
    request = next(record for record in records if record["kind"] == "model.request")
    model_response = next(record for record in records if record["kind"] == "model.response")
    assert [(message["data"]["role"], message["data"]["content"]) for message in messages] == [
        ("user", "What's the weather like in SF?")
    ]
    assert request["data"]["context_refs"] == [messages[0]["id"]]
    assert model_response["data"]["content"] == "It is windy."
    assert model_response["links"] == [{"type": "result_of", "record": request["id"]}]
    assert not [record for record in records if record["kind"] == "loss"]


def test_openai_2_45_structured_input_keeps_messages_without_tool_duplicates(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=_openai_completed_response("resp-structured-input", "It is sunny."),
        )

    client = OpenAI(
        api_key="openai-provider-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-structured-input")
    capture.instrument(adapter=openai_provider_adapter(version="2.45.0"), client=client)

    response = client.responses.create(
        model="gpt-fixture",
        input=[
            {
                "type": "message",
                "role": "developer",
                "content": [{"type": "input_text", "text": "Be precise."}],
            },
            {"role": "user", "content": "Weather?"},
            {"role": "assistant", "content": "I will check."},
            {
                "type": "function_call",
                "call_id": "call-prior",
                "name": "lookup",
                "arguments": '{"city":"SF"}',
            },
            {
                "type": "function_call_output",
                "call_id": "call-prior",
                "output": "sunny",
            },
        ],
    )
    assert response.output_text == "It is sunny."

    records = _trace_records(capture.shutdown().artifact_path)
    messages = [record for record in records if record["kind"] == "message"]
    request = next(record for record in records if record["kind"] == "model.request")
    assert [(message["data"]["role"], message["data"]["content"]) for message in messages] == [
        ("developer", [{"type": "input_text", "text": "Be precise."}]),
        ("user", "Weather?"),
        ("assistant", "I will check."),
        (
            "assistant",
            {
                "type": "function_call",
                "call_id": "call-prior",
                "name": "lookup",
                "arguments": '{"city":"SF"}',
            },
        ),
        ("tool", "sunny"),
    ]
    assert messages[-2]["data"]["call_id"] == "call-prior"
    assert messages[-2]["data"]["name"] == "lookup"
    assert messages[-1]["data"]["call_id"] == "call-prior"
    assert request["data"]["context_refs"] == [message["id"] for message in messages]
    assert not [
        record
        for record in records
        if record["kind"] in {"tool.proposal", "tool.call", "tool.result", "loss"}
    ]


def test_openai_2_45_empty_and_opaque_inputs_are_not_invented_as_user_text(
    tmp_path: Path,
) -> None:
    calls = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json=_openai_completed_response(f"resp-input-{calls}", "Done."),
        )

    client = OpenAI(
        api_key="openai-provider-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-empty-input")
    capture.instrument(adapter=openai_provider_adapter(version="2.45.0"), client=client)

    client.responses.create(model="gpt-fixture", input="")
    client.responses.create(model="gpt-fixture", input=[])
    client.responses.create(
        model="gpt-fixture",
        input={"opaque": "provider-owned"},  # type: ignore[arg-type]
    )

    records = _trace_records(capture.shutdown().artifact_path)
    messages = [record for record in records if record["kind"] == "message"]
    requests = [record for record in records if record["kind"] == "model.request"]
    assert [(message["data"]["role"], message["data"]["content"]) for message in messages] == [
        ("user", "")
    ]
    assert requests[0]["data"]["context_refs"] == [messages[0]["id"]]
    assert requests[1]["data"]["context_refs"] == []
    assert "context_refs" not in requests[2]["data"]
    assert "context_base_ref" not in requests[2]["data"]
    assert [record["kind"] for record in records].count("model.response") == 3
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "unsupported_native_value"


def test_openai_2_45_plain_string_error_preserves_request_and_exception(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            headers={"x-request-id": "request-auth"},
            json={"error": {"message": "invalid fixture key", "type": "invalid_request_error"}},
        )

    client = OpenAI(
        api_key="openai-provider-fixture",
        base_url="https://example.invalid/v1",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-plain-input-error")
    capture.instrument(adapter=openai_provider_adapter(version="2.45.0"), client=client)

    with pytest.raises(AuthenticationError) as caught:
        client.responses.create(model="gpt-fixture", input="Try once.")

    records = _trace_records(capture.shutdown().artifact_path)
    message = next(record for record in records if record["kind"] == "message")
    request = next(record for record in records if record["kind"] == "model.request")
    error = next(record for record in records if record["kind"] == "error")
    assert caught.value.status_code == 401
    assert message["data"] == {"role": "user", "content": "Try once."}
    assert request["data"]["context_refs"] == [message["id"]]
    assert error["data"]["type"] == "authentication_error"
    assert "invalid fixture key" in error["data"]["message"]
    assert error["parent"] == request["id"]
    assert not [record for record in records if record["kind"] in {"model.response", "loss"}]


def test_openai_stream_projects_one_terminal_aggregate(tmp_path: Path) -> None:
    response = {
        "id": "resp-stream-terminal",
        "object": "response",
        "created_at": 1,
        "status": "completed",
        "model": "gpt-fixture",
        "output": [
            {
                "id": "message-stream",
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": "streamed answer",
                        "annotations": [],
                        "logprobs": [],
                    }
                ],
            }
        ],
        "parallel_tool_calls": True,
        "tool_choice": "auto",
        "tools": [],
        "usage": {"input_tokens": 2, "output_tokens": 3, "total_tokens": 5},
    }

    def transport(_: httpx.Request) -> httpx.Response:
        events = [
            {
                "type": "response.output_text.delta",
                "sequence_number": 0,
                "item_id": "message-stream",
                "output_index": 0,
                "content_index": 0,
                "delta": "streamed ",
            },
            {"type": "response.completed", "sequence_number": 1, "response": response},
        ]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text="".join(f"data: {json.dumps(event)}\n\n" for event in events) + "data: [DONE]\n\n",
        )

    client = OpenAI(
        api_key="openai-stream-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-stream-projection")
    capture.instrument(adapter=openai_provider_adapter(), client=client)
    events = list(
        client.responses.create(
            model="gpt-fixture",
            input="Stream the answer.",
            stream=True,
        )
    )
    assert len(events) == 2

    records = _trace_records(capture.shutdown().artifact_path)
    request = next(record for record in records if record["kind"] == "model.request")
    message = next(record for record in records if record["kind"] == "message")
    responses = [record for record in records if record["kind"] == "model.response"]
    assert message["data"] == {"role": "user", "content": "Stream the answer."}
    assert request["data"]["context_refs"] == [message["id"]]
    assert len(responses) == 1
    assert responses[0]["data"] == {
        "status": "completed",
        "model": "gpt-fixture",
        "content": "streamed answer",
        "usage": {"input_tokens": 2, "output_tokens": 3},
    }
    assert responses[0]["links"] == [{"type": "result_of", "record": request["id"]}]
    assert not [record for record in records if record["kind"] == "loss"]
    assert len(records) < 8


def test_openai_stream_preserves_mixed_reasoning_block_order_and_repetition(
    tmp_path: Path,
) -> None:
    events = [
        {
            "type": "response.reasoning_summary_text.delta",
            "sequence_number": 0,
            "item_id": "reasoning-stream",
            "output_index": 0,
            "summary_index": 0,
            "delta": "Repeated.",
        },
        {
            "type": "response.reasoning_text.delta",
            "sequence_number": 1,
            "item_id": "reasoning-stream",
            "output_index": 0,
            "content_index": 0,
            "delta": "Raw first.",
        },
        {
            "type": "response.reasoning_summary_text.delta",
            "sequence_number": 2,
            "item_id": "reasoning-stream",
            "output_index": 0,
            "summary_index": 1,
            "delta": "Repeated.",
        },
        {
            "type": "response.reasoning_text.delta",
            "sequence_number": 3,
            "item_id": "reasoning-stream",
            "output_index": 0,
            "content_index": 1,
            "delta": "Raw second.",
        },
    ]

    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text="".join(f"data: {json.dumps(event)}\n\n" for event in events) + "data: [DONE]\n\n",
        )

    client = OpenAI(
        api_key="openai-stream-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-stream-reasoning-order")
    capture.instrument(adapter=openai_provider_adapter(version="2.45.0"), client=client)

    observed = list(client.responses.create(model="gpt-fixture", input="Explain.", stream=True))
    assert len(observed) == 4

    records = _trace_records(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["reasoning"] == [
        {"type": "summary", "text": "Repeated."},
        {"type": "text", "text": "Raw first."},
        {"type": "summary", "text": "Repeated."},
        {"type": "text", "text": "Raw second."},
    ]
    assert not [record for record in records if record["kind"] == "loss"]


def test_openai_chat_stream_folds_text_deltas_into_one_response(tmp_path: Path) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        chunks = [
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1,
                "model": "gpt-fixture",
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": "hello "},
                        "finish_reason": None,
                    }
                ],
            },
            {
                "id": "chatcmpl-stream",
                "object": "chat.completion.chunk",
                "created": 1,
                "model": "gpt-fixture",
                "choices": [
                    {
                        "index": 0,
                        "delta": {"content": "world"},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 2,
                    "total_tokens": 4,
                },
            },
        ]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text="".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks) + "data: [DONE]\n\n",
        )

    client = OpenAI(
        api_key="openai-chat-stream-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openai-chat-stream-projection")
    capture.instrument(adapter=openai_provider_adapter(), client=client)
    list(
        client.chat.completions.create(
            model="gpt-fixture",
            messages=[{"role": "user", "content": "Stream."}],
            stream=True,
            stream_options={"include_usage": True},
        )
    )

    records = _trace_records(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    assert len(responses) == 1
    assert responses[0]["data"] == {
        "status": "completed",
        "model": "gpt-fixture",
        "content": "hello world",
        "finish_reason": "stop",
        "usage": {"input_tokens": 2, "output_tokens": 2},
    }
    assert not [record for record in records if record["kind"] == "loss"]


def test_openrouter_stream_preserves_indexed_reasoning_details_in_order(
    tmp_path: Path,
) -> None:
    details = [
        {"index": 0, "type": "reasoning.summary", "summary": "Plan."},
        {"index": 1, "type": "reasoning.text", "text": "Inspect."},
        {"index": 2, "type": "reasoning.summary", "summary": "Plan."},
        {
            "index": 3,
            "type": "reasoning.encrypted",
            "data": "opaque-encrypted-stream-detail",
            "signature": "opaque-stream-signature",
        },
    ]

    def transport(_: httpx.Request) -> httpx.Response:
        chunks = [
            {
                "id": "chatcmpl-openrouter-stream",
                "object": "chat.completion.chunk",
                "created": 1,
                "model": "deepseek/deepseek-r1",
                "choices": [
                    {
                        "index": 0,
                        "delta": {"reasoning_details": [detail]},
                        "finish_reason": "stop" if position == len(details) - 1 else None,
                    }
                ],
            }
            for position, detail in enumerate(details)
        ]
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text="".join(f"data: {json.dumps(chunk)}\n\n" for chunk in chunks)
            + "data: [DONE]\n\n",
        )

    client = OpenAI(
        api_key="openrouter-stream-fixture",
        base_url="https://example.invalid/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openrouter-stream-reasoning")
    capture.instrument(
        adapter=openai_provider_adapter(version="2.45.0", provider="openrouter"),
        client=client,
    )

    list(
        client.chat.completions.create(
            model="deepseek/deepseek-r1",
            messages=[{"role": "user", "content": "Explain."}],
            stream=True,
        )
    )

    records = _trace_records(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["reasoning"] == [
        {"type": "summary", "text": "Plan."},
        {"type": "text", "text": "Inspect."},
        {"type": "summary", "text": "Plan."},
    ]
    assert "opaque-encrypted-stream-detail" not in json.dumps(response["data"])
    assert "opaque-stream-signature" not in json.dumps(response["data"])


@pytest.mark.asyncio
async def test_openai_async_stream_close_keeps_observed_partial_response(
    tmp_path: Path,
) -> None:
    class ChunkedSSEStream(httpx.AsyncByteStream):
        def __init__(self) -> None:
            self.reads = 0
            self.closes = 0
            self.chunks = [
                (
                    "data: "
                    + json.dumps(
                        {
                            "id": "chatcmpl-partial",
                            "object": "chat.completion.chunk",
                            "created": 1,
                            "model": "gpt-fixture",
                            "choices": [
                                {
                                    "index": 0,
                                    "delta": {
                                        "content": "observed partial",
                                        "reasoning_content": "observed reasoning",
                                    },
                                    "finish_reason": None,
                                }
                            ],
                        }
                    )
                    + "\n\n"
                ).encode(),
                (
                    "data: "
                    + json.dumps(
                        {
                            "id": "chatcmpl-partial",
                            "object": "chat.completion.chunk",
                            "created": 1,
                            "model": "gpt-fixture",
                            "choices": [
                                {
                                    "index": 0,
                                    "delta": {"content": " unobserved"},
                                    "finish_reason": "stop",
                                }
                            ],
                        }
                    )
                    + "\n\n"
                ).encode(),
                b"data: [DONE]\n\n",
            ]

        async def __aiter__(self) -> Any:
            for chunk in self.chunks:
                self.reads += 1
                yield chunk

        async def aclose(self) -> None:
            self.closes += 1

    body = ChunkedSSEStream()

    async def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            stream=body,
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    client = AsyncOpenAI(
        api_key="openai-async-stream-fixture",
        base_url="https://example.invalid/v1",
        http_client=http_client,
    )
    capture = initialize(output=tmp_path, service_name="openai-async-stream-close")
    capture.instrument(adapter=openai_provider_adapter(version="2.45.0"), client=client)

    stream = await client.chat.completions.create(
        model="gpt-fixture",
        messages=[{"role": "user", "content": "Stream."}],
        stream=True,
    )
    first = await stream.__anext__()
    assert first.choices[0].delta.content == "observed partial"
    assert body.reads == 1

    assert await stream.close() is None
    assert body.reads == 1
    assert body.closes == 1

    records = _trace_records((await capture.shutdown_async()).artifact_path)
    request = next(record for record in records if record["kind"] == "model.request")
    response = next(record for record in records if record["kind"] == "model.response")
    loss = next(record for record in records if record["kind"] == "loss")
    outcome = next(record for record in records if record["kind"] == "run.outcome")

    assert response["data"] == {
        "status": "cancelled",
        "model": "gpt-fixture",
        "content": "observed partial",
        "reasoning": [{"type": "text", "text": "observed reasoning"}],
    }
    assert response["links"] == [{"type": "result_of", "record": request["id"]}]
    assert loss["parent"] == response["id"]
    assert loss["data"]["reason"] == "stream_terminal_not_observed"
    assert outcome["data"]["status"] == "cancelled"

    await client.close()


def test_openrouter_failure_is_an_independent_failed_root(tmp_path: Path) -> None:
    calls = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                429,
                headers={"x-request-id": "request-failed"},
                json={"error": {"message": "rate limited", "type": "rate_limit"}},
            )
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-success",
                "object": "chat.completion",
                "created": 1,
                "model": "router-fixture",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "done"},
                        "finish_reason": "stop",
                    }
                ],
            },
        )

    client = OpenAI(
        api_key="openrouter-provider-fixture",
        base_url="https://openrouter.example.invalid/api/v1",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="openrouter-provider-projection")
    capture.instrument(
        adapter=openai_provider_adapter(provider="openrouter"),
        client=client,
    )

    with pytest.raises(Exception):
        client.chat.completions.create(
            model="router-fixture",
            messages=[{"role": "user", "content": "first"}],
        )
    assert (
        client.chat.completions.create(
            model="router-fixture",
            messages=[{"role": "user", "content": "second"}],
        ).id
        == "chatcmpl-success"
    )

    records = _trace_records(capture.shutdown().artifact_path)
    assert [record["kind"] for record in records].count("run.start") == 2
    assert sorted(
        record["data"]["status"] for record in records if record["kind"] == "run.outcome"
    ) == ["completed", "failed"]
    assert [record["kind"] for record in records].count("error") == 1
    assert [record["kind"] for record in records].count("model.response") == 1
    assert not [record for record in records if record["kind"] == "loss"]


def test_provider_and_custom_bridge_share_one_public_observation_root(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "chatcmpl-custom-root",
                "object": "chat.completion",
                "created": 1,
                "model": "router-fixture",
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": "done"},
                        "finish_reason": "stop",
                    }
                ],
            },
        )

    client = OpenAI(
        api_key="openrouter-provider-fixture",
        base_url="https://openrouter.example.invalid/api/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="custom-provider-composition")
    capture.instrument(
        adapter=openai_provider_adapter(provider="openrouter"),
        client=client,
    )
    bridge = create_custom_agent_source(name="callback-agent", version="1.0.0")
    capture.install_source(bridge.source)

    with capture.observe("callback-session", input={"task": "inspect"}):
        assert bridge.record(
            {"type": "run.start", "run_id": "session-1", "name": "callback-agent"}
        ).accepted
        response = client.chat.completions.create(
            model="router-fixture",
            messages=[{"role": "user", "content": "Inspect."}],
        )
        assert response.choices[0].message.content == "done"
        assert bridge.record(
            {
                "type": "tool.call",
                "run_id": "session-1",
                "call_id": "read-1",
                "name": "read",
                "input": {"path": "pyproject.toml"},
            }
        ).accepted
        assert bridge.record(
            {
                "type": "tool.result",
                "run_id": "session-1",
                "call_id": "read-1",
                "status": "succeeded",
                "output": "project metadata",
            }
        ).accepted
        assert bridge.record(
            {
                "type": "run.outcome",
                "run_id": "session-1",
                "status": "completed",
                "output": "done",
            }
        ).accepted

    closed = capture.shutdown()
    report = validate_artifact(
        closed.artifact_path,
        profile="rich-agent",
        required_source_activity=("provider:openrouter", "callback-agent"),
    )
    assert report.valid, report.issues
    assert closed.losses == {}
    records = _trace_records(closed.artifact_path)
    roots = [record for record in records if record["kind"] == "run.start"]
    assert len(roots) == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    by_id = {record["id"]: record for record in records}
    for record in (
        next(record for record in records if record["kind"] == "model.request"),
        next(record for record in records if record["kind"] == "tool.call"),
    ):
        ancestor = record
        while ancestor["id"] != roots[0]["id"]:
            ancestor = by_id[ancestor["parent"]]


def test_openrouter_cumulative_context_uses_linear_base_chain(
    tmp_path: Path,
) -> None:
    turns = 140
    response_index = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal response_index
        index = response_index
        response_index += 1
        final = index == turns - 1
        message: dict[str, Any] = {
            "role": "assistant",
            "content": "complete" if final else None,
        }
        if not final:
            message["tool_calls"] = [
                {
                    "id": f"call-{index}",
                    "type": "function",
                    "function": {
                        "name": "lookup",
                        "arguments": json.dumps({"step": index}),
                    },
                }
            ]
        return httpx.Response(
            200,
            json={
                "id": f"chatcmpl-linear-{index}",
                "object": "chat.completion",
                "created": 1,
                "model": "router-fixture",
                "choices": [
                    {
                        "index": 0,
                        "message": message,
                        "finish_reason": "stop" if final else "tool_calls",
                    }
                ],
            },
        )

    client = OpenAI(
        api_key="openrouter-provider-fixture",
        base_url="https://openrouter.example.invalid/api/v1",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="provider-linear-context")
    capture.instrument(
        adapter=openai_provider_adapter(provider="openrouter"),
        client=client,
    )
    history: list[dict[str, Any]] = [
        {"role": "system", "content": "Use tools precisely."},
        {"role": "user", "content": "Inspect the repository."},
    ]

    with capture.observe("long-running-agent"):
        for index in range(turns):
            response = client.chat.completions.create(
                model="router-fixture",
                messages=history,  # type: ignore[arg-type]
                tools=[
                    {
                        "type": "function",
                        "function": {
                            "name": "lookup",
                            "parameters": {
                                "type": "object",
                                "properties": {"step": {"type": "integer"}},
                            },
                        },
                    }
                ],
            )
            assistant = response.choices[0].message
            if index == turns - 1:
                assert assistant.content == "complete"
                continue
            call = assistant.tool_calls[0]  # type: ignore[index]
            history.extend(
                [
                    {
                        "role": "assistant",
                        "content": assistant.content,
                        "tool_calls": [
                            {
                                "id": call.id,
                                "type": call.type,
                                "function": {
                                    "name": call.function.name,
                                    "arguments": call.function.arguments,
                                },
                            }
                        ],
                    },
                    {
                        "role": "tool",
                        "tool_call_id": call.id,
                        "content": f"result-{index}",
                    },
                ]
            )

    status = capture.shutdown()
    records = _trace_records(status.artifact_path)
    requests = [record for record in records if record["kind"] == "model.request"]
    messages = [record for record in records if record["kind"] == "message"]
    proposals = [record for record in records if record["kind"] == "tool.proposal"]

    assert not [record for record in records if record["kind"] == "loss"]
    assert len(requests) == turns
    assert len(messages) == 2 + (2 * (turns - 1))
    assert len(proposals) == turns - 1
    assert [len(request["data"]["context_refs"]) for request in requests] == [
        2,
        *([2] * (turns - 1)),
    ]
    assert [
        request["data"].get("context_base_ref") for request in requests
    ] == [
        None,
        *[request["id"] for request in requests[:-1]],
    ]

    expanded_context: dict[str, list[str]] = {}
    for request in requests:
        base = request["data"].get("context_base_ref")
        expanded_context[request["id"]] = [
            *(expanded_context[base] if isinstance(base, str) else []),
            *request["data"]["context_refs"],
        ]
    assert expanded_context[requests[-1]["id"]] == [
        message["id"] for message in messages
    ]
    tool_messages = [
        message for message in messages if message["data"]["role"] == "tool"
    ]
    assert [message["data"]["content"] for message in tool_messages] == [
        f"result-{index}" for index in range(turns - 1)
    ]
    assert [message["data"]["call_id"] for message in tool_messages] == [
        f"call-{index}" for index in range(turns - 1)
    ]

    trace_bytes = (Path(status.artifact_path) / "trace.jsonl").stat().st_size
    unique_message_bytes = sum(
        len(json.dumps(message["data"], separators=(",", ":")).encode())
        for message in messages
    )
    assert sum(
        len(request["data"]["context_refs"]) for request in requests
    ) == len(messages)
    assert trace_bytes < (4_096 * len(records)) + (unique_message_bytes * 4)


def test_anthropic_projects_content_usage_and_exact_tool_identity(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "msg-fixture",
                "type": "message",
                "role": "assistant",
                "model": "claude-fixture",
                "content": [
                    {"type": "text", "text": "I will look it up."},
                    {
                        "type": "tool_use",
                        "id": "tool-anthropic",
                        "name": "lookup",
                        "input": {"city": "London"},
                    },
                ],
                "stop_reason": "tool_use",
                "stop_sequence": None,
                "usage": {"input_tokens": 2, "output_tokens": 4},
            },
        )

    client = Anthropic(
        api_key="anthropic-provider-fixture",
        base_url="https://example.invalid",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="anthropic-provider-projection")
    capture.instrument(adapter=anthropic_provider_adapter(), client=client)
    client.messages.create(
        model="claude-fixture",
        max_tokens=32,
        system="Be precise.",
        messages=[{"role": "user", "content": "Weather?"}],
        tools=[
            {
                "name": "lookup",
                "description": "fixture",
                "input_schema": {
                    "type": "object",
                    "properties": {"city": {"type": "string"}},
                    "required": ["city"],
                },
            }
        ],
    )

    records = _trace_records(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    assert [record["kind"] for record in records].count("message") == 2
    assert response["data"] == {
        "status": "completed",
        "model": "claude-fixture",
        "content": "I will look it up.",
        "finish_reason": "tool_use",
        "usage": {"input_tokens": 2, "output_tokens": 4},
    }
    assert proposal["data"]["native_call_id"] == "tool-anthropic"
    assert proposal["data"]["name"] == "lookup"
    assert proposal["data"]["input"] == {"city": "London"}
    assert not [record for record in records if record["kind"] == "loss"]


def test_anthropic_preserves_repeated_thinking_and_excludes_opaque_blocks(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "id": "msg-reasoning",
                "type": "message",
                "role": "assistant",
                "model": "claude-fixture",
                "content": [
                    {
                        "type": "thinking",
                        "thinking": "Checked the evidence.",
                        "signature": "opaque-signature-one",
                    },
                    {"type": "text", "text": "Intermediate visible text."},
                    {
                        "type": "thinking",
                        "thinking": "Checked the evidence.",
                        "signature": "opaque-signature-two",
                    },
                    {"type": "redacted_thinking", "data": "opaque-redacted-thinking"},
                    {"type": "text", "text": "Final answer."},
                ],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 2, "output_tokens": 4},
            },
        )

    client = Anthropic(
        api_key="anthropic-provider-fixture",
        base_url="https://example.invalid",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="anthropic-reasoning-blocks")
    capture.instrument(adapter=anthropic_provider_adapter(version="0.116.0"), client=client)

    client.messages.create(
        model="claude-fixture",
        max_tokens=32,
        messages=[{"role": "user", "content": "Explain."}],
    )

    records = _trace_records(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == "Intermediate visible text.Final answer."
    assert response["data"]["reasoning"] == [
        {"type": "summary", "text": "Checked the evidence."},
        {"type": "summary", "text": "Checked the evidence."},
    ]
    assert "opaque-signature" not in json.dumps(response["data"])
    assert "opaque-redacted-thinking" not in json.dumps(response["data"])


def test_anthropic_0_116_stream_prefers_tool_fragments_and_sanitizes_context(
    tmp_path: Path,
) -> None:
    calls = 0
    events = [
        (
            "message_start",
            {
                "type": "message_start",
                "message": {
                    "id": "msg-stream-tool",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-fixture",
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 3, "output_tokens": 1},
                },
            },
        ),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "thinking",
                    "thinking": "",
                    "signature": "",
                },
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {
                    "type": "thinking_delta",
                    "thinking": "Use the tool.",
                },
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {
                    "type": "signature_delta",
                    "signature": "opaque-signature",
                },
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 0}),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 1,
                "content_block": {
                    "type": "thinking",
                    "thinking": "",
                    "signature": "",
                },
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 1,
                "delta": {
                    "type": "thinking_delta",
                    "thinking": "Use the tool.",
                },
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 1}),
        (
            "content_block_start",
            {
                "type": "content_block_start",
                "index": 2,
                "content_block": {
                    "type": "tool_use",
                    "id": "toolu-stream",
                    "name": "lookup",
                    "input": {},
                },
            },
        ),
        (
            "content_block_delta",
            {
                "type": "content_block_delta",
                "index": 2,
                "delta": {
                    "type": "input_json_delta",
                    "partial_json": '{"city":"London"}',
                },
            },
        ),
        ("content_block_stop", {"type": "content_block_stop", "index": 2}),
        (
            "message_delta",
            {
                "type": "message_delta",
                "delta": {"stop_reason": "tool_use", "stop_sequence": None},
                "usage": {"output_tokens": 8},
            },
        ),
        ("message_stop", {"type": "message_stop"}),
    ]

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                text="".join(
                    f"event: {name}\ndata: {json.dumps(event)}\n\n" for name, event in events
                ),
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(
            200,
            json={
                "id": "msg-final",
                "type": "message",
                "role": "assistant",
                "model": "claude-fixture",
                "content": [{"type": "text", "text": "Done."}],
                "stop_reason": "end_turn",
                "stop_sequence": None,
                "usage": {"input_tokens": 9, "output_tokens": 2},
            },
        )

    client = Anthropic(
        api_key="anthropic-provider-fixture",
        base_url="https://example.invalid",
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="anthropic-stream-projection")
    capture.instrument(
        adapter=anthropic_provider_adapter(version="0.116.0"),
        client=client,
    )

    stream = client.messages.create(
        model="claude-fixture",
        max_tokens=32,
        messages=[{"role": "user", "content": "Weather?"}],
        stream=True,
    )
    observed_events = list(stream)
    client.messages.create(
        model="claude-fixture",
        max_tokens=32,
        messages=[
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "thinking",
                        "thinking": "Use the tool.",
                        "signature": "opaque-signature",
                    },
                    {
                        "type": "tool_use",
                        "id": "toolu-stream",
                        "name": "lookup",
                        "input": {"city": "London"},
                    },
                    {
                        "type": "redacted_thinking",
                        "data": "opaque-redacted-thinking",
                    },
                ],
            }
        ],
    )

    records = _trace_records(capture.shutdown().artifact_path)
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    assistant_context = next(
        record
        for record in records
        if record["kind"] == "message" and record["data"]["role"] == "assistant"
    )
    stream_response = next(
        record
        for record in records
        if record["kind"] == "model.response" and record["data"].get("finish_reason") == "tool_use"
    )

    assert len(observed_events) == len(events)
    assert proposal["data"]["native_call_id"] == "toolu-stream"
    assert proposal["data"]["input"] == {"city": "London"}
    assert stream_response["data"]["reasoning"] == [
        {"type": "summary", "text": "Use the tool."},
        {"type": "summary", "text": "Use the tool."},
    ]
    assert assistant_context["data"]["content"] == [
        {"type": "thinking", "thinking": "Use the tool."},
        {
            "type": "tool_use",
            "id": "toolu-stream",
            "name": "lookup",
            "input": {"city": "London"},
        },
        {"type": "redacted_thinking"},
    ]
    assert "opaque-signature" not in json.dumps(assistant_context["data"])
    assert "opaque-redacted-thinking" not in json.dumps(assistant_context["data"])
    assert not [record for record in records if record["kind"] == "loss"]


def _anthropic_helper_sse() -> str:
    events = [
        {
            "type": "message_start",
            "message": {
                "id": "msg-helper",
                "type": "message",
                "role": "assistant",
                "model": "claude-fixture",
                "content": [],
                "stop_reason": None,
                "stop_sequence": None,
                "usage": {"input_tokens": 3, "output_tokens": 1},
            },
        },
        {
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": "", "signature": ""},
        },
        {
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "Use the tool."},
        },
        {"type": "content_block_stop", "index": 0},
        {
            "type": "content_block_start",
            "index": 1,
            "content_block": {"type": "text", "text": "", "citations": None},
        },
        {
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "text_delta", "text": "I will check."},
        },
        {"type": "content_block_stop", "index": 1},
        {
            "type": "content_block_start",
            "index": 2,
            "content_block": {
                "type": "tool_use",
                "id": "toolu-helper",
                "name": "lookup",
                "input": {},
            },
        },
        {
            "type": "content_block_delta",
            "index": 2,
            "delta": {
                "type": "input_json_delta",
                "partial_json": '{"city":"London"}',
            },
        },
        {"type": "content_block_stop", "index": 2},
        {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use", "stop_sequence": None},
            "usage": {"output_tokens": 8},
        },
        {"type": "message_stop"},
    ]
    return "".join(f"event: {event['type']}\ndata: {json.dumps(event)}\n\n" for event in events)


def test_anthropic_0_116_messages_stream_helper_is_captured_once(
    tmp_path: Path,
) -> None:
    requests = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            200,
            text=_anthropic_helper_sse(),
            headers={"content-type": "text/event-stream"},
        )

    client = Anthropic(
        api_key="anthropic-provider-fixture",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="anthropic-helper")
    capture.instrument(
        adapter=anthropic_provider_adapter(version="0.116.0"),
        client=client,
    )

    manager = client.messages.stream(
        model="claude-fixture",
        max_tokens=32,
        messages=[{"role": "user", "content": "Weather?"}],
    )
    assert requests == 0
    assert capture.status().admitted == 0
    with manager as stream:
        observed = list(stream)
        final_message = stream.get_final_message()

    records = _trace_records(capture.shutdown().artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    assert requests == 1
    assert observed
    assert final_message.id == "msg-helper"
    assert final_message.stop_reason == "tool_use"
    assert len([record for record in records if record["kind"] == "run.start"]) == 1
    assert len(responses) == 1
    assert responses[0]["data"]["content"] == "I will check."
    assert responses[0]["data"]["reasoning"] == [
        {"type": "summary", "text": "Use the tool."}
    ]
    assert len(proposals) == 1
    assert proposals[0]["data"]["native_call_id"] == "toolu-helper"
    assert proposals[0]["data"]["input"] == {"city": "London"}
    assert not [record for record in records if record["kind"] == "loss"]


def test_anthropic_0_116_messages_stream_helper_preserves_early_close(
    tmp_path: Path,
) -> None:
    requests = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            200,
            text=_anthropic_helper_sse(),
            headers={"content-type": "text/event-stream"},
        )

    client = Anthropic(
        api_key="anthropic-provider-fixture",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    original_post = client.messages._post
    capture = initialize(output=tmp_path, service_name="anthropic-helper-close")
    capture.instrument(
        adapter=anthropic_provider_adapter(version="0.116.0"),
        client=client,
    )

    with client.messages.stream(
        model="claude-fixture",
        max_tokens=32,
        messages=[{"role": "user", "content": "Weather?"}],
    ) as stream:
        first = next(iter(stream))

    records = _trace_records(capture.shutdown().artifact_path)
    assert requests == 1
    assert first.type == "message_start"
    assert [record["data"]["status"] for record in records if record["kind"] == "run.outcome"] == [
        "cancelled"
    ]
    assert (
        len(
            [
                record
                for record in records
                if record["kind"] == "loss"
                and record["data"]["reason"] == "stream_terminal_not_observed"
            ]
        )
        == 1
    )
    assert client.messages._post is original_post


@pytest.mark.asyncio
async def test_anthropic_0_116_async_messages_stream_helper_is_captured_once(
    tmp_path: Path,
) -> None:
    requests = 0

    async def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            200,
            text=_anthropic_helper_sse(),
            headers={"content-type": "text/event-stream"},
        )

    client = AsyncAnthropic(
        api_key="anthropic-provider-fixture",
        max_retries=0,
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="anthropic-async-helper")
    capture.instrument(
        adapter=anthropic_provider_adapter(version="0.116.0"),
        client=client,
    )

    manager = client.messages.stream(
        model="claude-fixture",
        max_tokens=32,
        messages=[{"role": "user", "content": "Weather?"}],
    )
    assert requests == 0
    assert capture.status().admitted == 0
    async with manager as stream:
        observed = [event async for event in stream]
        final_message = await stream.get_final_message()

    closed = await capture.shutdown_async()
    records = _trace_records(closed.artifact_path)
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    assert requests == 1
    assert observed
    assert final_message.id == "msg-helper"
    assert len([record for record in records if record["kind"] == "run.start"]) == 1
    assert len(proposals) == 1
    assert proposals[0]["data"]["native_call_id"] == "toolu-helper"
    assert not [record for record in records if record["kind"] == "loss"]


def test_anthropic_0_116_messages_stream_helper_preserves_entry_error(
    tmp_path: Path,
) -> None:
    requests = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            529,
            json={
                "type": "error",
                "error": {
                    "type": "overloaded_error",
                    "message": "fixture overloaded",
                },
            },
            headers={"request-id": "req-helper-overload"},
        )

    client = Anthropic(
        api_key="anthropic-provider-fixture",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="anthropic-helper-error")
    capture.instrument(
        adapter=anthropic_provider_adapter(version="0.116.0"),
        client=client,
    )

    manager = client.messages.stream(
        model="claude-fixture",
        max_tokens=32,
        messages=[{"role": "user", "content": "Try once."}],
    )
    with pytest.raises(OverloadedError) as caught:
        with manager:
            raise AssertionError("unreachable")

    records = _trace_records(capture.shutdown().artifact_path)
    errors = [record for record in records if record["kind"] == "error"]
    assert requests == 1
    assert caught.value.status_code == 529
    assert len(errors) == 1
    assert errors[0]["data"]["type"] == "overloaded_error"
    assert len([record for record in records if record["kind"] == "run.start"]) == 1
    assert not [record for record in records if record["kind"] == "loss"]


def test_anthropic_0_116_non_message_post_is_not_captured(tmp_path: Path) -> None:
    requests = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"input_tokens": 7})

    client = Anthropic(
        api_key="anthropic-provider-fixture",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="anthropic-count-tokens")
    capture.instrument(
        adapter=anthropic_provider_adapter(version="0.116.0"),
        client=client,
    )

    result = client.messages.count_tokens(
        model="claude-fixture",
        messages=[{"role": "user", "content": "Count me."}],
    )

    records = _trace_records(capture.shutdown().artifact_path)
    assert result.input_tokens == 7
    assert requests == 1
    assert records == []


def test_anthropic_0_116_preserves_native_overload_error(tmp_path: Path) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            529,
            json={
                "type": "error",
                "error": {
                    "type": "overloaded_error",
                    "message": "fixture overloaded",
                },
            },
            headers={"request-id": "req-anthropic-overload"},
        )

    client = Anthropic(
        api_key="anthropic-provider-fixture",
        base_url="https://example.invalid",
        max_retries=0,
        http_client=httpx.Client(transport=httpx.MockTransport(transport)),
    )
    capture = initialize(output=tmp_path, service_name="anthropic-error-projection")
    capture.instrument(
        adapter=anthropic_provider_adapter(version="0.116.0"),
        client=client,
    )

    with pytest.raises(OverloadedError) as caught:
        client.messages.create(
            model="claude-fixture",
            max_tokens=32,
            messages=[{"role": "user", "content": "Try once."}],
        )

    records = _trace_records(capture.shutdown().artifact_path)
    error = next(record for record in records if record["kind"] == "error")
    outcome = next(record for record in records if record["kind"] == "run.outcome")
    assert caught.value.status_code == 529
    assert error["data"]["type"] == "overloaded_error"
    assert error["data"]["code"] == "529"
    assert "fixture overloaded" in error["data"]["message"]
    assert outcome["data"]["status"] == "failed"
    assert not [record for record in records if record["kind"] == "loss"]


def test_gemini_projects_one_source_authored_id_for_an_idless_proposal(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "responseId": "gemini-fixture-response",
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "index": 0,
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "functionCall": {
                                        "name": "lookup",
                                        "args": {"city": "London"},
                                    }
                                }
                            ],
                        },
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 2,
                    "candidatesTokenCount": 3,
                    "totalTokenCount": 5,
                },
            },
        )

    client = genai.Client(api_key="gemini-provider-fixture")
    client._api_client._httpx_client = httpx.Client(transport=httpx.MockTransport(transport))
    capture = initialize(output=tmp_path, service_name="gemini-provider-projection")
    capture.instrument(adapter=gemini_provider_adapter(), client=client)
    client.models.generate_content(
        model="gemini-fixture",
        contents=[{"role": "user", "parts": [{"text": "Weather?"}]}],
    )

    records = _trace_records(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    assert [record["kind"] for record in records].count("message") == 1
    assert response["data"] == {
        "status": "completed",
        "model": "gemini-fixture",
        "finish_reason": "STOP",
        "usage": {"input_tokens": 2, "output_tokens": 3},
    }
    assert proposal["data"]["name"] == "lookup"
    assert proposal["data"]["input"] == {"city": "London"}
    assert "native_call_id" not in proposal["data"]
    assert [record["kind"] for record in records].count("tool.proposal") == 1
    assert not [record for record in records if record["kind"] == "loss"]


def test_gemini_preserves_repeated_exposed_thought_parts_without_signatures(
    tmp_path: Path,
) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "responseId": "gemini-reasoning-response",
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "index": 0,
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "thought": True,
                                    "text": "Checked the evidence.",
                                    "thoughtSignature": "b3BhcXVlLW9uZQ==",
                                },
                                {"text": "Intermediate visible text."},
                                {
                                    "thought": True,
                                    "text": "Checked the evidence.",
                                    "thoughtSignature": "b3BhcXVlLXR3bw==",
                                },
                                {"text": "Final answer."},
                            ],
                        },
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 2,
                    "candidatesTokenCount": 3,
                    "totalTokenCount": 5,
                },
            },
        )

    client = genai.Client(api_key="gemini-provider-fixture")
    client._api_client._httpx_client = httpx.Client(
        transport=httpx.MockTransport(transport)
    )
    capture = initialize(output=tmp_path, service_name="gemini-reasoning-blocks")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)

    client.models.generate_content(model="gemini-fixture", contents="Explain.")

    records = _trace_records(capture.shutdown().artifact_path)
    response = next(record for record in records if record["kind"] == "model.response")
    assert response["data"]["content"] == "Intermediate visible text.Final answer."
    assert response["data"]["reasoning"] == [
        {"type": "summary", "text": "Checked the evidence."},
        {"type": "summary", "text": "Checked the evidence."},
    ]
    assert "thoughtSignature" not in json.dumps(response["data"])
    assert "thought_signature" not in json.dumps(response["data"])


def test_gemini_2_11_stream_preserves_call_id_and_sanitizes_thought_context(
    tmp_path: Path,
) -> None:
    calls = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            chunk = {
                "responseId": "gemini-stream-tool",
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "index": 0,
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "thought": True,
                                    "text": "Use the tool.",
                                    "thoughtSignature": "b3BhcXVlLXNpZ25hdHVyZQ==",
                                },
                                {
                                    "thought": True,
                                    "text": "Use the tool.",
                                    "thoughtSignature": "b3BhcXVlLXNpZ25hdHVyZTI=",
                                },
                                {
                                    "functionCall": {
                                        "id": "call-stream",
                                        "name": "lookup",
                                        "args": {"city": "London"},
                                    }
                                },
                            ],
                        },
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 2,
                    "candidatesTokenCount": 3,
                    "totalTokenCount": 5,
                },
            }
            return httpx.Response(
                200,
                text=f"data: {json.dumps(chunk)}\n\n",
                headers={"content-type": "text/event-stream"},
            )
        return httpx.Response(
            200,
            json={
                "responseId": "gemini-final",
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "index": 0,
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": [{"text": "Done."}],
                        },
                    }
                ],
            },
        )

    client = genai.Client(api_key="gemini-provider-fixture")
    client._api_client._httpx_client = httpx.Client(transport=httpx.MockTransport(transport))
    capture = initialize(output=tmp_path, service_name="gemini-stream-projection")
    capture.instrument(
        adapter=gemini_provider_adapter(version="2.11.0"),
        client=client,
    )

    chunks = list(
        client.models.generate_content_stream(
            model="gemini-fixture",
            contents=[{"role": "user", "parts": [{"text": "Weather?"}]}],
        )
    )
    streamed_content = chunks[0].candidates[0].content
    client.models.generate_content(
        model="gemini-fixture",
        contents=[streamed_content],
    )

    records = _trace_records(capture.shutdown().artifact_path)
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    assistant_context = next(
        record
        for record in records
        if record["kind"] == "message" and record["data"]["role"] == "assistant"
    )
    stream_response = next(
        record
        for record in records
        if record["kind"] == "model.response" and record["data"].get("reasoning")
    )

    assert len(chunks) == 1
    assert proposal["data"]["native_call_id"] == "call-stream"
    assert proposal["data"]["input"] == {"city": "London"}
    assert stream_response["data"]["reasoning"] == [
        {"type": "summary", "text": "Use the tool."},
        {"type": "summary", "text": "Use the tool."},
    ]
    assert assistant_context["data"]["content"][0]["text"] == "Use the tool."
    assert assistant_context["data"]["content"][0]["thought"] is True
    assert "thought_signature" not in assistant_context["data"]["content"][0]
    assert "thoughtSignature" not in assistant_context["data"]["content"][0]
    assert not [record for record in records if record["kind"] == "loss"]


def test_gemini_2_11_preserves_native_server_error(tmp_path: Path) -> None:
    def transport(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            json={
                "error": {
                    "code": 503,
                    "message": "fixture unavailable",
                    "status": "UNAVAILABLE",
                }
            },
        )

    client = genai.Client(api_key="gemini-provider-fixture")
    client._api_client._httpx_client = httpx.Client(transport=httpx.MockTransport(transport))
    capture = initialize(output=tmp_path, service_name="gemini-error-projection")
    capture.instrument(
        adapter=gemini_provider_adapter(version="2.11.0"),
        client=client,
    )

    with pytest.raises(ServerError) as caught:
        client.models.generate_content(
            model="gemini-fixture",
            contents="Try once.",
        )

    records = _trace_records(capture.shutdown().artifact_path)
    error = next(record for record in records if record["kind"] == "error")
    outcome = next(record for record in records if record["kind"] == "run.outcome")
    assert caught.value.code == 503
    assert error["data"]["type"] == "server_error"
    assert "fixture unavailable" in error["data"]["message"]
    assert outcome["data"]["status"] == "failed"
    assert not [record for record in records if record["kind"] == "loss"]


def test_gemini_2_11_streamed_afc_captures_each_native_request_once(
    tmp_path: Path,
) -> None:
    requests = 0
    tool_calls: list[str] = []

    def get_current_weather(location: str) -> str:
        tool_calls.append(location)
        return "windy"

    get_current_weather.__annotations__ = {"location": str, "return": str}

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        response = (
            {
                "responseId": "gemini-afc-plan",
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "index": 0,
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "thought": True,
                                    "text": "I should use the weather tool.",
                                    "thoughtSignature": "b3BhcXVlLXBsYW4=",
                                },
                                {
                                    "functionCall": {
                                        "id": "call-weather-1",
                                        "name": "get_current_weather",
                                        "args": {"location": "San Francisco, CA"},
                                    }
                                },
                            ],
                        },
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 12,
                    "candidatesTokenCount": 8,
                    "totalTokenCount": 20,
                },
            }
            if requests == 1
            else {
                "responseId": "gemini-afc-final",
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "index": 0,
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "thought": True,
                                    "text": "The weather tool returned windy.",
                                    "thoughtSignature": "b3BhcXVlLWZpbmFs",
                                },
                                {"text": "The weather is windy."},
                            ],
                        },
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 28,
                    "candidatesTokenCount": 10,
                    "totalTokenCount": 38,
                },
            }
        )
        return httpx.Response(
            200,
            text=f"data: {json.dumps(response)}\n\n",
            headers={"content-type": "text/event-stream"},
        )

    client = genai.Client(api_key="gemini-provider-fixture")
    client._api_client._httpx_client = httpx.Client(transport=httpx.MockTransport(transport))
    capture = initialize(output=tmp_path, service_name="gemini-streamed-afc")
    capture.instrument(
        adapter=gemini_provider_adapter(version="2.11.0"),
        client=client,
    )

    chunks = list(
        client.models.generate_content_stream(
            model="gemini-fixture",
            contents="What is the weather in San Francisco, CA?",
            config=genai_types.GenerateContentConfig(
                tools=[get_current_weather],
                thinking_config=genai_types.ThinkingConfig(include_thoughts=True),
            ),
        )
    )

    records = _trace_records(capture.shutdown().artifact_path)
    model_requests = [record for record in records if record["kind"] == "model.request"]
    model_responses = [record for record in records if record["kind"] == "model.response"]
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    losses = [record for record in records if record["kind"] == "loss"]
    assert chunks[-1].text == "The weather is windy."
    assert requests == 2
    assert tool_calls == ["San Francisco, CA"]
    assert len(model_requests) == 2
    assert len(model_responses) == 2
    assert len(proposals) == 1
    assert proposals[0]["data"]["native_call_id"] == "call-weather-1"
    assert proposals[0]["data"]["input"] == {"location": "San Francisco, CA"}
    assert len(losses) == 1
    assert "gemini.function_responses.unpaired" in losses[0]["data"]["detail"]
    assert not [record for record in records if record["kind"] in {"tool.call", "tool.result"}]
    assert [
        record["data"]["reasoning"] for record in model_responses if record["data"].get("reasoning")
    ] == [
        [{"type": "summary", "text": "I should use the weather tool."}],
        [{"type": "summary", "text": "The weather tool returned windy."}],
    ]
    canonical = json.dumps([record["data"] for record in records])
    assert "b3BhcXVlLXBsYW4=" not in canonical
    assert "b3BhcXVlLWZpbmFs" not in canonical


@pytest.mark.asyncio
async def test_gemini_2_11_async_stream_observes_consumed_chunks(
    tmp_path: Path,
) -> None:
    requests = 0

    async def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        response = {
            "responseId": "gemini-async-stream",
            "modelVersion": "gemini-fixture",
            "candidates": [
                {
                    "index": 0,
                    "finishReason": "STOP",
                    "content": {
                        "role": "model",
                        "parts": [{"text": "Async answer."}],
                    },
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 2,
                "candidatesTokenCount": 3,
                "totalTokenCount": 5,
            },
        }
        return httpx.Response(
            200,
            text=f"data: {json.dumps(response)}\n\n",
            headers={"content-type": "text/event-stream"},
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    client = genai.Client(
        api_key="gemini-provider-fixture",
        http_options=genai_types.HttpOptions(httpx_async_client=http_client),
    )
    capture = initialize(output=tmp_path, service_name="gemini-async-stream")
    capture.instrument(
        adapter=gemini_provider_adapter(version="2.11.0"),
        client=client,
    )

    stream = await client.aio.models.generate_content_stream(
        model="gemini-fixture",
        contents="Answer once.",
    )
    chunks = [chunk async for chunk in stream]

    records = _trace_records((await capture.shutdown_async()).artifact_path)
    responses = [record for record in records if record["kind"] == "model.response"]
    assert requests == 1
    assert [chunk.text for chunk in chunks] == ["Async answer."]
    assert [record["kind"] for record in records].count("run.start") == 1
    assert [record["kind"] for record in records].count("run.outcome") == 1
    assert len(responses) == 1
    assert responses[0]["data"] == {
        "status": "completed",
        "model": "gemini-fixture",
        "content": "Async answer.",
        "finish_reason": "STOP",
        "usage": {"input_tokens": 2, "output_tokens": 3},
    }
    assert not [record for record in records if record["kind"] == "loss"]
    await http_client.aclose()


@pytest.mark.asyncio
async def test_gemini_2_11_async_streamed_afc_captures_each_native_request_once(
    tmp_path: Path,
) -> None:
    requests = 0
    tool_calls: list[str] = []

    async def get_current_weather(location: str) -> str:
        tool_calls.append(location)
        return "windy"

    get_current_weather.__annotations__ = {"location": str, "return": str}

    async def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        response = (
            {
                "responseId": "gemini-async-afc-plan",
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "index": 0,
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "thought": True,
                                    "text": "I should use the weather tool.",
                                    "thoughtSignature": "b3BhcXVlLWFzeW5jLXBsYW4=",
                                },
                                {
                                    "functionCall": {
                                        "id": "call-weather-async",
                                        "name": "get_current_weather",
                                        "args": {"location": "San Francisco, CA"},
                                    }
                                },
                            ],
                        },
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 12,
                    "candidatesTokenCount": 8,
                    "totalTokenCount": 20,
                },
            }
            if requests == 1
            else {
                "responseId": "gemini-async-afc-final",
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "index": 0,
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "thought": True,
                                    "text": "The weather tool returned windy.",
                                    "thoughtSignature": "b3BhcXVlLWFzeW5jLWZpbmFs",
                                },
                                {"text": "The weather is windy."},
                            ],
                        },
                    }
                ],
                "usageMetadata": {
                    "promptTokenCount": 28,
                    "candidatesTokenCount": 10,
                    "totalTokenCount": 38,
                },
            }
        )
        return httpx.Response(
            200,
            text=f"data: {json.dumps(response)}\n\n",
            headers={"content-type": "text/event-stream"},
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    client = genai.Client(
        api_key="gemini-provider-fixture",
        http_options=genai_types.HttpOptions(httpx_async_client=http_client),
    )
    capture = initialize(output=tmp_path, service_name="gemini-async-streamed-afc")
    capture.instrument(
        adapter=gemini_provider_adapter(version="2.11.0"),
        client=client,
    )

    stream = await client.aio.models.generate_content_stream(
        model="gemini-fixture",
        contents="What is the weather in San Francisco, CA?",
        config=genai_types.GenerateContentConfig(
            tools=[get_current_weather],
            thinking_config=genai_types.ThinkingConfig(include_thoughts=True),
        ),
    )
    chunks = [chunk async for chunk in stream]

    records = _trace_records((await capture.shutdown_async()).artifact_path)
    model_requests = [record for record in records if record["kind"] == "model.request"]
    model_responses = [record for record in records if record["kind"] == "model.response"]
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    losses = [record for record in records if record["kind"] == "loss"]
    assert chunks[-1].text == "The weather is windy."
    assert requests == 2
    assert tool_calls == ["San Francisco, CA"]
    assert [record["kind"] for record in records].count("run.start") == 2
    assert [record["kind"] for record in records].count("run.outcome") == 2
    assert len(model_requests) == 2
    assert len(model_responses) == 2
    assert len(proposals) == 1
    assert proposals[0]["data"]["native_call_id"] == "call-weather-async"
    assert proposals[0]["data"]["input"] == {"location": "San Francisco, CA"}
    assert len(losses) == 1
    assert "gemini.function_responses.unpaired" in losses[0]["data"]["detail"]
    assert not [record for record in records if record["kind"] in {"tool.call", "tool.result"}]
    assert [
        record["data"]["reasoning"] for record in model_responses if record["data"].get("reasoning")
    ] == [
        [{"type": "summary", "text": "I should use the weather tool."}],
        [{"type": "summary", "text": "The weather tool returned windy."}],
    ]
    canonical = json.dumps([record["data"] for record in records])
    assert "b3BhcXVlLWFzeW5jLXBsYW4=" not in canonical
    assert "b3BhcXVlLWFzeW5jLWZpbmFs" not in canonical
    await http_client.aclose()


def test_gemini_2_11_stream_error_before_first_chunk_is_preserved(
    tmp_path: Path,
) -> None:
    requests = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            503,
            json={
                "error": {
                    "code": 503,
                    "message": "fixture unavailable",
                    "status": "UNAVAILABLE",
                }
            },
        )

    client = genai.Client(api_key="gemini-provider-fixture")
    client._api_client._httpx_client = httpx.Client(transport=httpx.MockTransport(transport))
    capture = initialize(output=tmp_path, service_name="gemini-stream-error")
    capture.instrument(
        adapter=gemini_provider_adapter(version="2.11.0"),
        client=client,
    )

    stream = client.models.generate_content_stream(
        model="gemini-fixture",
        contents="Try once.",
    )
    with pytest.raises(ServerError) as caught:
        next(stream)

    records = _trace_records(capture.shutdown().artifact_path)
    errors = [record for record in records if record["kind"] == "error"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert requests == 1
    assert caught.value.code == 503
    assert len(errors) == 1
    assert errors[0]["data"]["type"] == "server_error"
    assert "fixture unavailable" in errors[0]["data"]["message"]
    assert [outcome["data"]["status"] for outcome in outcomes] == ["failed"]
    assert not [record for record in records if record["kind"] == "loss"]


@pytest.mark.asyncio
async def test_gemini_2_11_async_stream_initial_error_is_preserved(
    tmp_path: Path,
) -> None:
    requests = 0

    async def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            503,
            json={
                "error": {
                    "code": 503,
                    "message": "fixture unavailable",
                    "status": "UNAVAILABLE",
                }
            },
        )

    http_client = httpx.AsyncClient(transport=httpx.MockTransport(transport))
    client = genai.Client(
        api_key="gemini-provider-fixture",
        http_options=genai_types.HttpOptions(httpx_async_client=http_client),
    )
    capture = initialize(output=tmp_path, service_name="gemini-async-stream-error")
    capture.instrument(
        adapter=gemini_provider_adapter(version="2.11.0"),
        client=client,
    )

    stream = await client.aio.models.generate_content_stream(
        model="gemini-fixture",
        contents="Try once.",
    )
    with pytest.raises(ServerError) as caught:
        await stream.__anext__()

    records = _trace_records((await capture.shutdown_async()).artifact_path)
    errors = [record for record in records if record["kind"] == "error"]
    outcomes = [record for record in records if record["kind"] == "run.outcome"]
    assert requests == 1
    assert caught.value.code == 503
    assert len(errors) == 1
    assert errors[0]["data"]["type"] == "server_error"
    assert "fixture unavailable" in errors[0]["data"]["message"]
    assert [outcome["data"]["status"] for outcome in outcomes] == ["failed"]
    assert not [record for record in records if record["kind"] == "loss"]
    await http_client.aclose()


@pytest.mark.parametrize("tool_kind", ["function", "bound_method"])
def test_gemini_callable_tool_behavior_is_preserved_when_idless_afc_is_unpaired(
    tmp_path: Path, tool_kind: str
) -> None:
    calls: list[str] = []

    def get_current_weather(location: str) -> str:
        calls.append(location)
        return "sunny"

    class Weather:
        def get_current_weather(self, location: str) -> str:
            calls.append(location)
            return "sunny"

    get_current_weather.__annotations__ = {"location": str, "return": str}
    Weather.get_current_weather.__annotations__ = {"location": str, "return": str}
    tool = get_current_weather if tool_kind == "function" else Weather().get_current_weather
    requests = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        parts = (
            [
                {
                    "functionCall": {
                        "name": "get_current_weather",
                        "args": {"location": "Boston"},
                    }
                }
            ]
            if requests == 1
            else [{"text": "The weather is sunny."}]
        )
        return httpx.Response(
            200,
            json={
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "finishReason": "STOP",
                        "content": {
                            "role": "model",
                            "parts": parts,
                        },
                    }
                ],
            },
        )

    client = genai.Client(api_key="gemini-provider-fixture")
    client._api_client._httpx_client = httpx.Client(transport=httpx.MockTransport(transport))
    capture = initialize(output=tmp_path, service_name="gemini-callable-tool")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)
    response = client.models.generate_content(
        model="gemini-fixture",
        contents="Weather?",
        config=genai_types.GenerateContentConfig(tools=[tool]),
    )

    records = _trace_records(capture.shutdown().artifact_path)
    request = next(record for record in records if record["kind"] == "model.request")
    final_response = next(record for record in records if record["kind"] == "model.response")
    assert response.text == "The weather is sunny."
    assert requests == 2
    assert calls == ["Boston"]
    assert request["data"]["tools"] == ["get_current_weather"]
    assert request["seq"] < final_response["seq"]
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    assert len(proposals) == 1
    assert proposals[0]["data"]["name"] == "get_current_weather"
    assert proposals[0]["data"]["input"] == {"location": "Boston"}
    assert not [record for record in records if record["kind"] in {"tool.call", "tool.result"}]
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert "gemini.function_responses.unpaired" in losses[0]["data"]["detail"]


def test_gemini_cumulative_idless_afc_history_emits_one_loss_per_suffix(
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    def get_current_weather(location: str) -> str:
        calls.append(location)
        return "sunny"

    get_current_weather.__annotations__ = {"location": str, "return": str}
    responses = [
        {
            "functionCall": {
                "name": "get_current_weather",
                "args": {"location": "Boston"},
            }
        },
        {"text": "Boston is sunny."},
        {
            "functionCall": {
                "name": "get_current_weather",
                "args": {"location": "London"},
            }
        },
        {"text": "London is sunny."},
    ]
    requests = 0

    def transport(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        part = responses[requests]
        requests += 1
        return httpx.Response(
            200,
            json={
                "modelVersion": "gemini-fixture",
                "candidates": [
                    {
                        "finishReason": "STOP",
                        "content": {"role": "model", "parts": [part]},
                    }
                ],
            },
        )

    client = genai.Client(api_key="gemini-provider-fixture")
    client._api_client._httpx_client = httpx.Client(transport=httpx.MockTransport(transport))
    capture = initialize(output=tmp_path, service_name="gemini-cumulative-afc")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)
    config = genai_types.GenerateContentConfig(tools=[get_current_weather])

    first = client.models.generate_content(
        model="gemini-fixture",
        contents="Weather in Boston?",
        config=config,
    )
    second = client.models.generate_content(
        model="gemini-fixture",
        contents=[
            *first.automatic_function_calling_history,
            genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="Weather in London?")],
            ),
        ],
        config=config,
    )

    records = _trace_records(capture.shutdown().artifact_path)
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    tool_calls = [record for record in records if record["kind"] == "tool.call"]
    results = [record for record in records if record["kind"] == "tool.result"]
    assert first.text == "Boston is sunny."
    assert second.text == "London is sunny."
    assert requests == 4
    assert calls == ["Boston", "London"]
    assert [proposal["data"]["input"] for proposal in proposals] == [
        {"location": "Boston"},
        {"location": "London"},
    ]
    assert not tool_calls
    assert not results
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 2
    assert all("gemini.function_responses.unpaired" in loss["data"]["detail"] for loss in losses)


def test_gemini_unpaired_afc_history_is_one_material_loss(tmp_path: Path) -> None:
    response = genai_types.GenerateContentResponse(
        candidates=[
            genai_types.Candidate(
                content=genai_types.Content(
                    role="model",
                    parts=[genai_types.Part.from_text(text="done")],
                )
            )
        ],
        automatic_function_calling_history=[
            genai_types.Content(
                role="model",
                parts=[
                    genai_types.Part.from_function_call(
                        name="lookup",
                        args={"city": "London"},
                    )
                ],
            ),
            genai_types.Content(
                role="user",
                parts=[
                    genai_types.Part.from_function_response(
                        name="different_tool",
                        response={"result": "sunny"},
                    )
                ],
            ),
        ],
    )

    def generate_content(**_kwargs: Any) -> genai_types.GenerateContentResponse:
        return response

    models = SimpleNamespace(
        generate_content=generate_content,
        generate_content_stream=generate_content,
    )
    client = SimpleNamespace(
        models=models,
        aio=SimpleNamespace(models=SimpleNamespace(**vars(models))),
    )
    capture = initialize(output=tmp_path, service_name="gemini-unpaired-afc")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)

    assert client.models.generate_content(model="fixture", contents="Weather?") is response

    records = _trace_records(capture.shutdown().artifact_path)
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "unsupported_semantic_projection"
    assert "gemini.afc.history.unpaired" in losses[0]["data"]["detail"]
    assert not [
        record
        for record in records
        if record["kind"] in {"tool.proposal", "tool.call", "tool.result"}
    ]


def test_gemini_singleton_afc_pairs_only_with_matching_exact_native_ids(
    tmp_path: Path,
) -> None:
    response = genai_types.GenerateContentResponse(
        candidates=[
            genai_types.Candidate(
                content=genai_types.Content(
                    role="model",
                    parts=[genai_types.Part.from_text(text="done")],
                )
            )
        ],
        automatic_function_calling_history=[
            genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="Weather?")],
            ),
            genai_types.Content(
                role="model",
                parts=[
                    genai_types.Part(
                        function_call=genai_types.FunctionCall(
                            id="provider-call-1",
                            name="lookup",
                            args={"city": "London"},
                        )
                    )
                ],
            ),
            genai_types.Content(
                role="user",
                parts=[
                    genai_types.Part(
                        function_response=genai_types.FunctionResponse(
                            id="provider-call-1",
                            name="lookup",
                            response={"result": "sunny"},
                        )
                    )
                ],
            ),
        ],
    )

    def generate_content(**_kwargs: Any) -> genai_types.GenerateContentResponse:
        return response

    models = SimpleNamespace(
        generate_content=generate_content,
        generate_content_stream=generate_content,
    )
    client = SimpleNamespace(
        models=models,
        aio=SimpleNamespace(models=SimpleNamespace(**vars(models))),
    )
    capture = initialize(output=tmp_path, service_name="gemini-exact-afc")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)

    client.models.generate_content(model="fixture", contents="Weather?")

    records = _trace_records(capture.shutdown().artifact_path)
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    call = next(record for record in records if record["kind"] == "tool.call")
    result = next(record for record in records if record["kind"] == "tool.result")
    assert proposal["data"]["native_call_id"] == "provider-call-1"
    assert call["data"]["native_call_id"] == "provider-call-1"
    assert result["data"]["native_call_id"] == "provider-call-1"
    assert call["links"] == [{"type": "derived_from", "record": proposal["id"]}]
    assert result["links"] == [{"type": "result_of", "record": call["id"]}]
    assert not [record for record in records if record["kind"] == "loss"]


def test_gemini_does_not_pair_multiple_idless_afc_calls_by_position(
    tmp_path: Path,
) -> None:
    response = genai_types.GenerateContentResponse(
        candidates=[
            genai_types.Candidate(
                content=genai_types.Content(
                    role="model",
                    parts=[genai_types.Part.from_text(text="done")],
                )
            )
        ],
        automatic_function_calling_history=[
            genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="Weather?")],
            ),
            genai_types.Content(
                role="model",
                parts=[
                    genai_types.Part.from_function_call(
                        name="lookup",
                        args={"city": "London"},
                    ),
                    genai_types.Part.from_function_call(
                        name="lookup",
                        args={"city": "Paris"},
                    ),
                ],
            ),
            genai_types.Content(
                role="user",
                parts=[
                    genai_types.Part.from_function_response(
                        name="lookup",
                        response={"result": "rain"},
                    ),
                    genai_types.Part.from_function_response(
                        name="lookup",
                        response={"result": "sun"},
                    ),
                ],
            ),
        ],
    )

    def generate_content(**_kwargs: Any) -> genai_types.GenerateContentResponse:
        return response

    models = SimpleNamespace(
        generate_content=generate_content,
        generate_content_stream=generate_content,
    )
    client = SimpleNamespace(
        models=models,
        aio=SimpleNamespace(models=SimpleNamespace(**vars(models))),
    )
    capture = initialize(output=tmp_path, service_name="gemini-ambiguous-afc")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)

    assert client.models.generate_content(model="fixture", contents="Weather?") is response

    records = _trace_records(capture.shutdown().artifact_path)
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert "gemini.afc.history.unpaired" in losses[0]["data"]["detail"]
    assert not [
        record
        for record in records
        if record["kind"] in {"tool.proposal", "tool.call", "tool.result"}
    ]


def test_gemini_does_not_link_multiple_function_responses_by_position(
    tmp_path: Path,
) -> None:
    first_response = {
        "candidates": [
            {
                "content": {
                    "role": "model",
                    "parts": [
                        {
                            "functionCall": {
                                "name": "lookup",
                                "args": {"city": "London"},
                            }
                        },
                        {
                            "functionCall": {
                                "name": "lookup",
                                "args": {"city": "Paris"},
                            }
                        },
                    ],
                }
            }
        ]
    }
    second_response = {"candidates": [{"content": {"role": "model", "parts": [{"text": "done"}]}}]}
    responses = iter((first_response, second_response))

    def generate_content(**_kwargs: Any) -> dict[str, Any]:
        return next(responses)

    models = SimpleNamespace(
        generate_content=generate_content,
        generate_content_stream=generate_content,
    )
    client = SimpleNamespace(
        models=models,
        aio=SimpleNamespace(models=SimpleNamespace(**vars(models))),
    )
    capture = initialize(output=tmp_path, service_name="gemini-ambiguous-results")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)

    with provider_capture_context(
        conversation_id="conversation",
        turn_id="turn-1",
    ):
        client.models.generate_content(model="fixture", contents="Weather?")
    with provider_capture_context(
        conversation_id="conversation",
        turn_id="turn-2",
        previous_turn_id="turn-1",
    ):
        client.models.generate_content(
            model="fixture",
            contents=[
                {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "name": "lookup",
                                "response": {"result": "rain"},
                            }
                        },
                        {
                            "functionResponse": {
                                "name": "lookup",
                                "response": {"result": "sun"},
                            }
                        },
                    ],
                }
            ],
        )

    records = _trace_records(capture.shutdown().artifact_path)
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert "gemini.function_responses.unpaired" in losses[0]["data"]["detail"]
    proposals = [record for record in records if record["kind"] == "tool.proposal"]
    assert len(proposals) == 2
    assert not [record for record in records if record["kind"] in {"tool.call", "tool.result"}]
    proposal_ids = {record["id"] for record in proposals}
    assert not [
        link
        for record in records
        for link in record.get("links", [])
        if link["type"] == "result_of" and link["record"] in proposal_ids
    ]


def test_gemini_does_not_link_one_idless_function_response_by_adjacency(
    tmp_path: Path,
) -> None:
    responses = iter(
        (
            {
                "candidates": [
                    {
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "functionCall": {
                                        "name": "lookup",
                                        "args": {"city": "London"},
                                    }
                                }
                            ],
                        }
                    }
                ]
            },
            {"candidates": [{"content": {"role": "model", "parts": [{"text": "done"}]}}]},
        )
    )

    def generate_content(**_kwargs: Any) -> dict[str, Any]:
        return next(responses)

    models = SimpleNamespace(
        generate_content=generate_content,
        generate_content_stream=generate_content,
    )
    client = SimpleNamespace(
        models=models,
        aio=SimpleNamespace(models=SimpleNamespace(**vars(models))),
    )
    capture = initialize(output=tmp_path, service_name="gemini-idless-single-result")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)

    with provider_capture_context(conversation_id="conversation", turn_id="turn-1"):
        client.models.generate_content(model="fixture", contents="Weather?")
    with provider_capture_context(
        conversation_id="conversation",
        turn_id="turn-2",
        previous_turn_id="turn-1",
    ):
        client.models.generate_content(
            model="fixture",
            contents=[
                {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "name": "lookup",
                                "response": {"result": "sunny"},
                            }
                        }
                    ],
                }
            ],
        )

    records = _trace_records(capture.shutdown().artifact_path)
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert "gemini.function_responses.unpaired" in losses[0]["data"]["detail"]
    assert not [record for record in records if record["kind"] in {"tool.call", "tool.result"}]


def test_gemini_cross_turn_singleton_accepts_matching_exact_native_ids(
    tmp_path: Path,
) -> None:
    responses = iter(
        (
            {
                "candidates": [
                    {
                        "content": {
                            "role": "model",
                            "parts": [
                                {
                                    "functionCall": {
                                        "id": "provider-call-1",
                                        "name": "lookup",
                                        "args": {"city": "London"},
                                    }
                                }
                            ],
                        }
                    }
                ]
            },
            {"candidates": [{"content": {"role": "model", "parts": [{"text": "done"}]}}]},
        )
    )

    def generate_content(**_kwargs: Any) -> dict[str, Any]:
        return next(responses)

    models = SimpleNamespace(
        generate_content=generate_content,
        generate_content_stream=generate_content,
    )
    client = SimpleNamespace(
        models=models,
        aio=SimpleNamespace(models=SimpleNamespace(**vars(models))),
    )
    capture = initialize(output=tmp_path, service_name="gemini-exact-single-result")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)

    with provider_capture_context(conversation_id="conversation", turn_id="turn-1"):
        client.models.generate_content(model="fixture", contents="Weather?")
    with provider_capture_context(
        conversation_id="conversation",
        turn_id="turn-2",
        previous_turn_id="turn-1",
    ):
        client.models.generate_content(
            model="fixture",
            contents=[
                {
                    "role": "user",
                    "parts": [
                        {
                            "functionResponse": {
                                "id": "provider-call-1",
                                "name": "lookup",
                                "response": {"result": "sunny"},
                            }
                        }
                    ],
                }
            ],
        )

    records = _trace_records(capture.shutdown().artifact_path)
    proposal = next(record for record in records if record["kind"] == "tool.proposal")
    assert proposal["data"]["native_call_id"] == "provider-call-1"
    assert not [record for record in records if record["kind"] == "loss"]


def test_gemini_unexpected_text_in_afc_suffix_invalidates_all_pairs(
    tmp_path: Path,
) -> None:
    response = genai_types.GenerateContentResponse(
        candidates=[
            genai_types.Candidate(
                content=genai_types.Content(
                    role="model",
                    parts=[genai_types.Part.from_text(text="done")],
                )
            )
        ],
        automatic_function_calling_history=[
            genai_types.Content(
                role="user",
                parts=[genai_types.Part.from_text(text="Weather?")],
            ),
            genai_types.Content(
                role="model",
                parts=[genai_types.Part.from_text(text="unexpected suffix text")],
            ),
            genai_types.Content(
                role="model",
                parts=[
                    genai_types.Part.from_function_call(
                        name="lookup",
                        args={"city": "London"},
                    )
                ],
            ),
            genai_types.Content(
                role="user",
                parts=[
                    genai_types.Part.from_function_response(
                        name="lookup",
                        response={"result": "sunny"},
                    )
                ],
            ),
        ],
    )

    def generate_content(**_kwargs: Any) -> genai_types.GenerateContentResponse:
        return response

    models = SimpleNamespace(
        generate_content=generate_content,
        generate_content_stream=generate_content,
    )
    client = SimpleNamespace(
        models=models,
        aio=SimpleNamespace(models=SimpleNamespace(**vars(models))),
    )
    capture = initialize(output=tmp_path, service_name="gemini-mixed-afc")
    capture.instrument(adapter=gemini_provider_adapter(version="2.11.0"), client=client)

    assert client.models.generate_content(model="fixture", contents="Weather?") is response

    records = _trace_records(capture.shutdown().artifact_path)
    losses = [record for record in records if record["kind"] == "loss"]
    assert len(losses) == 1
    assert losses[0]["data"]["reason"] == "unsupported_semantic_projection"
    assert not [
        record
        for record in records
        if record["kind"] in {"tool.proposal", "tool.call", "tool.result"}
    ]
