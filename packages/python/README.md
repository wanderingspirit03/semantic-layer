# Semantic Layer Python SDK

`semantic-layer-capture` records agent and model activity in local semantic
trace bundles. Cloud upload and OpenClaw support use separate packages.

The SDK requires Python 3.10 or newer.

```sh
python -m pip install /path/to/semantic_layer_capture-<version>-py3-none-any.whl
```

## Record a run

```python
from semantic_layer import initialize

capture = initialize(
    output=".semantic-layer/traces",
    service_name="support-agent",
)

with capture.observe("customer-request", input={"order_id": "A-17"}) as run:
    result = run.tool(
        "lookup-order",
        {"order_id": "A-17"},
        lambda value: {**value, "status": "shipped"},
    )
    run.set_output(result)

closed = capture.shutdown()
print(result, closed.artifact_path, closed.last_error)
```

Call `shutdown()` at the application's lifecycle boundary. Async applications
can use `async with capture.observe(...)` and finish with
`await capture.shutdown_async()`.

Local traces are plaintext and can contain private data. Add
`.semantic-layer/` to the application's ignore file.

## Documentation

- [Quickstart](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/quickstart.md)
- [Integrations](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/integrations.md)
- [Custom integration](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/custom-integration.md)
- [Storage](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/storage.md)
- [Privacy and security](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/privacy-and-security.md)
- [Trace format](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/trace-format.md)
