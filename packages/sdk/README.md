# Semantic Layer TypeScript SDK

`semantic-layer-capture` records agent and model activity in local semantic
trace bundles. Cloud upload and OpenClaw support use separate packages.

The SDK requires Node 22, Node 24, or Node 25.9 and newer, with server ESM.

```sh
npm install /path/to/semantic-layer-capture-<version>.tgz
```

## Record a run

```ts
import { initialize } from 'semantic-layer-capture';

const capture = initialize({
  output: '.semantic-layer/traces',
  serviceName: 'support-agent',
});

const result = await capture.observe(
  'customer-request',
  { input: { orderId: 'A-17' } },
  (run) => run.tool(
    'lookup-order',
    { orderId: 'A-17' },
    async ({ orderId }) => ({ orderId, status: 'shipped' }),
  ),
);

const closed = await capture.shutdown();
console.log(result, closed.artifactPath, closed.lastError);
```

Await `shutdown()` at the application's lifecycle boundary. Repeated compatible
calls to `initialize()` join one process capture session. Use `createCapture()`
when concurrent jobs need separate sessions and bundles.

Local traces are plaintext and can contain private data. Add
`.semantic-layer/` to the application's ignore file.

## Documentation

- [Quickstart](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/quickstart.md)
- [Integrations](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/integrations.md)
- [Custom integration](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/custom-integration.md)
- [Storage](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/storage.md)
- [Privacy and security](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/privacy-and-security.md)
- [Trace format](https://github.com/wanderingspirit03/semantic-layer/blob/main/docs/sdk/trace-format.md)
