# Custom integration

Use the manual interface when the application owns the model and tool call
sites. Use the typed callback bridge when the application already emits stable
run, model, and tool events.

## Manual capture

Wrap a complete application run with `observe` and wrap the tool callback with
`tool`:

```ts
const result = await capture.observe(
  'coding-agent',
  { input: { task: 'inspect the repository' } },
  async (run) => {
    const file = await run.tool(
      'read-file',
      { path: 'README.md' },
      ({ path }) => workspace.read(path),
      { callId: modelToolCall.id },
    );
    return agent.finish(file);
  },
);
```

```python
with capture.observe(
    "coding-agent",
    input={"task": "inspect the repository"},
) as run:
    file = run.tool(
        "read-file",
        {"path": "README.md"},
        lambda value: workspace.read(value["path"]),
        call_id=model_tool_call.id,
    )
    result = agent.finish(file)
    run.set_output(result)
```

The wrapper calls the supplied tool callback once and preserves its return or
exception. Pass the exact native call identity when the model or framework
provides one. If no native identity exists, omit it and let the SDK create a
local identity.

TypeScript records the value returned by the observation callback. A Python
context manager cannot observe a local assignment, so call `run.set_output`
when the application has a terminal value.

Use `run.turn` for meaningful steps inside a long root:

```ts
await capture.observe('workflow', { input: task }, async (run) => {
  await run.turn(
    'plan',
    { conversationId: 'case-7', turnId: 'turn-0', turnIndex: 0 },
    async (turn) => turn.tool('read-brief', { path: 'brief.md' }, readBrief),
  );
});
```

Python uses `with run.turn(...)` and the matching snake case fields. Keep one
capture session open for work that belongs in one bundle. The
[storage guide](storage.md) explains separate sessions and resumed workflows.

Pass exact cross process identities when separate workers belong to one task:

```ts
await capture.observe('search-loop', {
  correlation: {
    taskId: researchId,
    execution: {
      system: 'trigger.dev',
      runId: triggerRunId,
      parentRunId: triggerParentRunId,
      rootRunId: triggerRootRunId,
      attempt: triggerAttemptNumber,
    },
  },
}, runSearch);
```

Python uses `task_id`, `run_id`, `parent_run_id`, `root_run_id`, and `attempt`.
Configure the same tenant scoped `identityKey` for every worker that must join.
The SDK stores protected values only. See
[Cross process correlation](capture-contract.md#cross-process-correlation) for
the privacy and matching rules.

TypeScript callers can pass `cancellationSignal` when an application owned
abort signal is the exact cooperative cancellation boundary. If the callback
throws after that signal is aborted, capture records a cancelled outcome and
rethrows the same value. It does not classify an error as cancelled from its
name or message.

## Typed callback bridge

The bridge fits an agent that already publishes stable callbacks:

```ts
const bridge = createCustomAgentSource({
  name: 'my-agent',
  version: '1.4.0',
});
capture.installSource(bridge.source);

bridge.record({
  type: 'run.start',
  runId,
  name: 'coding-agent',
  input: task,
});
bridge.record({
  type: 'model.request',
  runId,
  callId: modelCall.id,
  model,
  messageIds: [userMessage.id],
});
bridge.record({
  type: 'model.response',
  runId,
  callId: modelCall.id,
  status: 'completed',
  content: response.content,
  reasoning: response.reasoning,
  usage: response.usage,
});
bridge.record({
  type: 'tool.call',
  runId,
  callId: toolCall.id,
  name: toolCall.name,
  input: toolCall.arguments,
});
bridge.record({
  type: 'tool.result',
  runId,
  callId: toolCall.id,
  status: 'succeeded',
  output: toolResult,
});
bridge.record({
  type: 'run.outcome',
  runId,
  status: 'completed',
  output,
});
```

Python provides the same event types through
`create_custom_agent_source`. It uses snake case field names.

Emit every message once with its native message identity, then reference the
message from model requests. Omitted message identities mean that context was
not observed. An empty list means that the integration observed empty context.

Reasoning is an ordered list of blocks with type `text` or `summary` and a text
value. Keep reasoning on the exact model response. If the response callback has
no request boundary, emit the response with its native response identity and
let the bridge record `model_request_not_observed`. Do not create an identity
from a counter, timestamp, model name, content, or list position.

A tool proposal is optional. Emit it only when the model or framework exposes a
proposal before execution. A proposal does not prove that the application ran
the tool.

## Lower level source

Use a `CaptureSource` only when a stable callback cannot fit the manual
interface or typed bridge. A source records through the runtime owned sink and
returns lifecycle methods for deactivation and draining. It must not write
files directly.

TypeScript exports `CaptureSource`, `SourceSink`, `OpenTraceRecord`, and
`SourceRecord`. Python exports `CaptureSource`.

All custom integrations follow the [capture contract](capture-contract.md).
One complete bundle test should prove exact correlation, application behavior,
shutdown, and named losses before the integration is used in production.
