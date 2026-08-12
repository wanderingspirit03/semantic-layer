# Integrations

The tables below list versions tested through a complete bundle. An unlisted
version can work, but the repository does not claim that it has the same
behavior.

Python adapters read the installed package version and reject a conflicting
override. TypeScript adapters accept an explicit version because loaded modules
do not provide one reliable version source. If a TypeScript application omits
the version, the manifest records the version as unknown.

## TypeScript

| Integration | Adapter | Persisted source | Tested versions |
|---|---|---|---|
| OpenAI | `openAIProviderAdapter` | `provider:openai` | `6.46.0`, `6.45.0` |
| OpenRouter through the OpenAI client | `openAIProviderAdapter` with `provider: 'openrouter'` | `provider:openrouter` | OpenAI client `6.46.0`, `6.45.0` |
| Anthropic | `anthropicProviderAdapter` | `provider:anthropic` | `0.111.0`, `0.110.0` |
| Gemini | `geminiProviderAdapter` | `provider:gemini` | `2.11.0`, `2.10.0` |
| OpenAI Agents | `openAIAgentsAdapter` | `official:openai-agents-js` | `0.13.2`, `0.13.1` |
| AI SDK | `aiSDKAdapter` | `official:ai-sdk` | `7.0.22`, `7.0.21` |
| LangGraph | `langGraphAdapter` | `official:langgraph-js` | `1.4.7`, `1.4.6` |
| Mastra | `mastraAdapter` | `official:mastra` | `1.50.1`, `1.50.0` |
| Strands | `strandsAdapter` | `official:strands-js` | `1.9.0`, `1.8.0` |
| OpenTelemetry | `createOpenTelemetrySource` | `generic:otel` | API `1.9.1`, tracing `2.9.0`, logs `0.220.0`; API `1.9.0`, tracing `1.25.1`, OTLP HTTP exporter `0.52.1` |
| Trigger.dev task attempts | Tavi attempt wrapper with `createOpenTelemetrySource` | manual run correlation and `generic:otel` | Trigger SDK `4.4.4` on Node `22.16.0` with OpenTelemetry API `1.9.0` and tracing `1.25.1` |

The Trigger.dev fixture uses public task context fields and the task-local
`onCancel` hook. Trigger `4.4.4` does not expose a replay relation, so a replay
is a new run under the same protected research task. A forced worker stop
cannot run finalization and is not claimed as captured.

Tavi supports two provider lifetimes. When one provider belongs to one attempt,
add a fresh Arcus processor directly and shut the provider down with the
attempt. When attempts share one process-wide provider, add one permanent
attempt router so concurrent tenants cannot mix spans. Both forms use Tavi's
direct OpenTelemetry `1.25.1` types. They never use processor classes from
Trigger's nested OpenTelemetry `2.x` packages. The
[Tavi Trigger example](../../examples/tavi-trigger-attempt/README.md#task-integration)
shows both attachment forms.

## Python

| Integration | Adapter | Persisted source | Tested version |
|---|---|---|---|
| OpenAI | `openai_provider_adapter` | `provider:openai` | `2.45.0` |
| OpenRouter through the OpenAI client | `openai_provider_adapter` with `provider="openrouter"` | `provider:openrouter` | OpenAI client `2.45.0` |
| Anthropic | `anthropic_provider_adapter` | `provider:anthropic` | `0.116.0` |
| Gemini | `gemini_provider_adapter` | `provider:gemini` | `2.11.0` |
| OpenAI Agents | `openai_agents_adapter` | `official:openai-agents-python` | `0.18.2` |
| LangGraph | `langgraph_adapter` | `official:langgraph-python` | `1.2.9` |
| Strands | `strands_adapter` | `official:strands-python` | `1.47.0` |
| PydanticAI | `pydantic_ai_adapter` | `official:pydanticai` | `2.9.0` |
| Google ADK | `google_adk_adapter` | `official:google-adk` | `2.4.0` |
| CrewAI | `crewai_adapter` | `official:crewai` | `1.15.2` |
| Microsoft Agent Framework | `microsoft_agent_framework_adapter` | `official:microsoft-agent-framework` | `1.11.0` |
| LlamaIndex | `llamaindex_adapter` | `official:llamaindex` | `0.14.23` |
| Haystack through OpenTelemetry | `haystack_otel_adapter` | `official:haystack-otel` | `2.31.0` with OpenTelemetry `1.42.1` |
| OpenTelemetry | `create_otel_source` | `generic:otel` | `1.42.1` |

## OpenClaw

The separate `semantic-layer-openclaw` plugin exactly qualifies OpenClaw
`2026.5.5` and `2026.7.1-2`. Setup accepts other stable releases from
`2026.5.5` onward and applies the known Node.js floor for that release line.
Doctor checks the live hooks and configuration before an unqualified host is
described as capability checked.

OpenClaw `2026.5.5` does not expose resolved tool definitions, model finish
reason, or model call identity through every rich hook. The plugin records a
named loss when a missing identity prevents exact correlation. See the
[OpenClaw setup guide](../openclaw/setup.md) for installation.

## What each seam exposes

Provider adapters observe model requests, responses, messages, usage, tool
proposals, and readable reasoning exposed by the provider. They normally do
not observe the application tool callback, so a provider only trace usually has
no `tool.call` or `tool.result`.

Framework adapters observe their official lifecycle, model, tool, state, and
message callbacks. The exact evidence differs because each framework exposes a
different public interface. The adapter keeps a missing fact absent or records
a named loss.

OpenTelemetry records standard spans and logs. Generic OpenTelemetry does not
claim framework specific handoffs, state, tool proposals, or delivery. Rich
prompt and result content is present only when the application enables the
standard GenAI content attributes.

Custom applications can use manual capture or the typed callback bridge. See
[custom integration](custom-integration.md).

## Reasoning and summaries

Each adapter retains readable reasoning that its tested interface exposes.
Reasoning stays separate from visible answer content. Exact call identity puts
the ordered blocks on the matching `model.response`.

| Integration | Readable evidence |
|---|---|
| OpenAI and OpenRouter | Responses summaries and `reasoning_text`, chat `reasoning_content`, and readable OpenRouter reasoning detail blocks. |
| Anthropic | Readable `thinking` text. |
| Gemini | Readable thought text. |
| OpenAI Agents | Final reasoning items and summaries, plus exactly owned consumed stream reasoning retained after an observed failure or cancellation. Unowned stream reasoning remains source state with a correlation loss. |
| AI SDK | Final reasoning and reasoning stream parts, including calls with application supplied telemetry integrations. |
| LangGraph | Provider and structured framework reasoning across final, stream, and Python event stream entry points. |
| Mastra | Typed final reasoning chunks on model responses. Consumed stream reasoning remains ordered run state with a correlation loss because Mastra exposes no shared model span identity at that seam. |
| Strands | Final reasoning blocks and exactly owned consumed reasoning updates retained after failure or observed cancellation. |
| PydanticAI | `ThinkingPart`, readable `CompactionPart`, and consumed thinking deltas from streaming entry points. Normal and direct iteration paths retain the public model events they expose. |
| Google ADK | Text from parts where `thought=True`, without duplicating complete final thoughts and their partial chunks. |
| CrewAI | `LLMThinkingChunkEvent` text on the response with the matching `call_id`. |
| Microsoft Agent Framework | Final and streamed `text_reasoning` linked by exact response and message identity. |
| LlamaIndex | Final thinking blocks on exact callback responses. Unowned thinking deltas and workflow response fallbacks remain workflow state with a correlation loss. |
| Haystack OpenTelemetry | Explicit GenAI reasoning output parts on exact model responses. Native generator `ReasoningContent` remains component output state when content tracing is enabled. |
| OpenClaw | Provider exposed reasoning text and summaries, with terminal assistant messages as a fallback. |

The adapters do not copy encrypted reasoning bodies, signatures, redacted
thinking bodies, or opaque continuation data into readable reasoning. A
provider that exposes no reasoning produces no loss, because capture has no
evidence that reasoning existed.

The [capture contract](capture-contract.md) defines the shared rules for
reasoning, correlation, outcomes, and losses.

## Setup

Install an adapter on the application object that already owns the provider or
framework call. Capture must not create a second model call or consume a second
copy of a stream.

### OpenAI compatible providers

OpenRouter uses the existing OpenAI client with a distinct source name:

```ts
import OpenAI from 'openai';
import { initialize, openAIProviderAdapter } from 'semantic-layer-capture';

const key = process.env.OPENROUTER_API_KEY!;
const client = new OpenAI({
  apiKey: key,
  baseURL: 'https://openrouter.ai/api/v1',
});
const capture = initialize({
  serviceName: 'agent',
  secretValues: [key],
});
capture.instrument({
  adapter: openAIProviderAdapter({ provider: 'openrouter' }),
  client,
});
```

```python
import os
from openai import OpenAI
from semantic_layer import initialize, openai_provider_adapter

key = os.environ["OPENROUTER_API_KEY"]
client = OpenAI(api_key=key, base_url="https://openrouter.ai/api/v1")
capture = initialize(service_name="agent", secret_values=[key])
capture.instrument(
    adapter=openai_provider_adapter(provider="openrouter"),
    client=client,
)
```

Use the same pattern for the native OpenAI, Anthropic, and Gemini clients with
their matching adapters.

### Framework adapters

Most framework adapters install on an existing object:

```ts
const adapter = langGraphAdapter({ version: '1.4.7' });
capture.instrument({ adapter, client: compiledGraph });
const result = await adapter.invoke(input, config);
```

```python
adapter = langgraph_adapter()
capture.instrument(adapter=adapter, client=compiled_graph)
result = compiled_graph.invoke(input)
```

Use the adapter wrapper when the adapter returns one. The wrapper keeps capture
bound to the framework call and preserves the original return or stream.

Some integrations have a different official install point:

- TypeScript Strands also needs the loaded SDK module so it can register public
  event constructors.
- Mastra uses the adapter source, observability exporter, and stream parts that
  the application consumes.
- CrewAI installs on the process wide `crewai_event_bus`.
- LlamaIndex returns a lifecycle facade. Run the workflow inside one explicit
  turn so its model and tool callbacks share a root.
- Haystack installs the adapter's OpenTelemetry processors on the application's
  existing provider.

The exported types and package tests show the exact arguments for each adapter.

## Known gaps

- Provider adapters normally cannot observe application owned tool execution.
- OpenRouter support covers the tested OpenAI compatible client interface. It
  is not a separate provider client adapter.
- A TypeScript adapter without an explicit version cannot prove the installed
  framework release.
- Generic OpenTelemetry omits ordinary application and infrastructure spans. It
  also cannot infer framework state or handoffs.
- Standard CrewAI model paths in the tested version do not emit
  `LLMThinkingChunkEvent`. The event bus therefore exposes no provider reasoning
  on those paths.
- Strands built in context summarization calls its model outside the public
  agent hooks. The adapter records a named loss after an observed context
  overflow. Python synchronous streaming also does not expose consumer
  cancellation to the adapter.
- PydanticAI direct `iter` capture does not force the model call to stream. It
  records whichever public model events the run emits.
- Haystack's normal OpenTelemetry data does not expose exact model or tool
  identity. The adapter records pipeline structure, and it records model or
  tool details only when exact GenAI attributes are present.
- Haystack native generator reasoning requires content tracing, which Haystack
  disables by default. Component output has no exact model request identity, so
  the adapter keeps it as state with a correlation loss. Reasoning delivered
  only to a streaming callback before an interrupted component is not present
  in a completed OpenTelemetry output.
- An unconsumed stream item is not observed because capture does not create a
  second stream consumer.
- Capture must initialize in the process that owns the framework call. It does
  not inject itself into child processes.
- Normal `adk run` creates its own Runner, so the Google ADK adapter requires an
  application owned Runner and plugin lifecycle.
- A completed framework outcome does not prove that the user goal was met or
  that an answer was delivered.

When adding or upgrading an integration, follow the
[capture contract](capture-contract.md) and add one complete bundle test before
changing the tested version table. The [maintainer testing guide](../maintainers/testing.md)
lists the repository checks. The
[framework reasoning coverage](../maintainers/reasoning-coverage.md) maps the
reasoning behavior and remaining public seam limits.
