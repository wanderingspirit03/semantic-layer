# Framework reasoning coverage

Maintainers use this page to see what the qualified framework tests prove and
where a public framework seam still limits capture. The
[integration inventory](../sdk/integrations.md) is the source of truth for
tested versions. The [capture contract](../sdk/capture-contract.md) defines the
shared reasoning rules.

Update this page only when a complete bundle test changes the behavior below.
Keep implementation tasks and past defects out of this page.

## Shared test requirements

A framework reasoning test must prove the following behavior when the framework
exposes the evidence:

- Readable reasoning text and summaries remain in source order, including
  intentional repetition.
- Reasoning stays separate from visible answer content.
- Exact identity links reasoning to the matching `model.response`.
- Reasoning already delivered through a stream remains available after a
  failure or observed cancellation.
- Encrypted, signed, protected, redacted, and other opaque values do not become
  readable reasoning.
- The adapter records a named loss when exposed evidence proves a material gap.
  It does not record a loss merely because a model returned no reasoning.

Credential scrubbing still applies to native evidence and blobs. Local rich
capture may retain bounded opaque provider metadata as native evidence, but it
must not present that metadata as readable reasoning.

## Qualified framework coverage

| Integration | Verified capture | Public seam limit |
| --- | --- | --- |
| OpenAI Agents for TypeScript and Python | Final reasoning items, summaries, and consumed raw stream deltas. Exactly owned reasoning delivered before an observed failure or cancellation remains on the response. | Capture sees only stream events that the application consumes. Overlapping or unlinked stream reasoning remains source state with a correlation loss. |
| AI SDK | Final reasoning and consumed reasoning stream parts. Per call telemetry integrations are composed with Semantic Layer in the application's original order. | Capture does not consume an extra copy of a stream. |
| LangGraph for TypeScript and Python | Provider reasoning fields and structured reasoning blocks. Python capture covers `invoke`, `ainvoke`, `stream`, `astream`, `stream_events`, and `astream_events`. | Capture is limited to messages and callbacks that LangGraph exposes. |
| Mastra | Final `ReasoningChunk` values remain on the model response. Reasoning parts consumed through the adapter stream seam remain ordered run state with their native IDs. | Mastra stream parts expose no shared model span identity, so the adapter records a correlation loss instead of assigning them by call order. The application must pass consumed stream parts to the adapter. |
| Strands for TypeScript and Python | Final reasoning blocks and exactly owned reasoning stream updates delivered before failure or observed cancellation. | Updates without a public invocation owner remain native state with a correlation loss. Built in context summarization calls the model outside public agent hooks. Python synchronous streaming does not expose consumer cancellation to the adapter. |
| PydanticAI | `ThinkingPart`, readable `CompactionPart`, and consumed thinking deltas from `run_stream` and `run_stream_sync`, including observed cancellation. Normal `run`, `run_sync`, and direct `iter` retain reasoning from their public model events. | Direct `iter` is not changed into a streaming model call because that would change the model request. |
| Google ADK | Readable parts where `thought=True`. A complete final thought replaces its matching partial aggregation, and independent blocks keep their order. | The adapter requires an application owned Runner and plugin lifecycle. |
| CrewAI | `LLMThinkingChunkEvent` text linked by exact `call_id`. | Standard CrewAI model paths in the qualified version do not emit this event, so the event bus exposes no provider reasoning on those paths. |
| Microsoft Agent Framework | Final and streamed `text_reasoning`, including partial final responses and concurrent calls with exact response and message identity. | Uncorrelated fragments are not attached to a model response. |
| LlamaIndex | Final `ThinkingBlock` on an exact callback response. Consumed `AgentStream.thinking_delta` and `AgentOutput.response` remain ordered workflow state when LlamaIndex exposes no exact model owner. | Unowned workflow reasoning records a named correlation loss instead of being assigned by call order. |
| Haystack through OpenTelemetry | Standard GenAI reasoning output parts on exact model responses. Completed native generator `ReasoningContent` remains component output state when content tracing is enabled. | Native generator output has no exact model request identity at the component seam, so it records a correlation loss. Haystack disables content tracing by default, and interrupted callback only reasoning is absent from completed OpenTelemetry output. |
| OpenClaw | Reasoning from `llm_output` and `before_message_write`, with terminal assistant messages as a fallback. Equal observations are deduplicated only when they share an exact response identity. | Qualified OpenClaw hooks can omit model `callId`. The plugin keeps such reasoning as run state and records a correlation loss instead of inventing a model edge. |

Generic OpenTelemetry capture also projects explicit GenAI reasoning output
parts and removes them from visible answer content. OpenTelemetry cannot infer
reasoning that an application does not publish as an attribute or log value.

## Maintainer evidence

The framework projection tests are the executable evidence for this page:

- TypeScript framework tests live in
  [`packages/sdk/tests`](../../packages/sdk/tests/).
- Python framework tests live in
  [`packages/python/tests/unit`](../../packages/python/tests/unit/).
- OpenClaw reasoning tests live in
  [`packages/openclaw/tests`](../../packages/openclaw/tests/).

When a framework version changes, test the new version through a complete
sealed bundle before changing the version inventory or this coverage summary.
