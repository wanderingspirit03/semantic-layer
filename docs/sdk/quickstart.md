# Quickstart

Install the SDK in the application that you want to trace. TypeScript requires
Node 22, Node 24, or Node 25.9 and newer with server ESM. Python requires
Python 3.10 or newer.

Add `.semantic-layer/` to the application's ignore file before capture. Local
traces are plaintext and can contain private data.

## TypeScript

```ts
import { initialize } from 'semantic-layer-capture';

const capture = initialize({
  output: '.semantic-layer/traces',
  serviceName: 'quickstart',
});

const result = await capture.observe(
  'uppercase-example',
  { input: { text: 'hello' } },
  async (run) => run.tool(
    'uppercase',
    { text: 'hello' },
    ({ text }) => text.toUpperCase(),
  ),
);

const closed = await capture.shutdown();
console.log(result, closed.artifactPath, closed.lastError);
```

Await `shutdown()` at the application's lifecycle boundary. It waits for
accepted writes and installed sources, then seals the bundle.

Repeated compatible calls to `initialize` join one capture session for the
process. Use `createCapture(options)` when concurrent jobs in one TypeScript
process need separate bundles. Each handle owns its sources and shutdown.

## Python

```python
from semantic_layer import initialize

capture = initialize(
    output=".semantic-layer/traces",
    service_name="quickstart",
)

with capture.observe("uppercase-example", input={"text": "hello"}) as run:
    result = run.tool(
        "uppercase",
        {"text": "hello"},
        lambda value: value["text"].upper(),
    )
    run.set_output(result)

closed = capture.shutdown()
print(result, closed.artifact_path, closed.last_error)
```

An async Python application can use `async with capture.observe(...)` and await
tool results. Finish the capture session with
`await capture.shutdown_async()`.

## Output

Each capture session creates a bundle below the selected output directory:

```text
.semantic-layer/traces/
└── run-<bundle-id>/
    ├── manifest.json
    ├── trace.jsonl
    └── blobs/          # present only when needed
```

The example produces one run root, one tool call, its result, and one outcome.
Check that shutdown returned `state: closed` without a capture error, then
[validate the bundle](verification.md).

Use a tested [framework or provider integration](integrations.md) when one
matches the application. Use the [custom integration guide](custom-integration.md)
when the application owns its agent loop.
