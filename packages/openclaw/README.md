# Semantic Layer for OpenClaw

`semantic-layer-openclaw` captures OpenClaw activity and sends sealed traces
through the Semantic Layer cloud uploader. OpenClaw `2026.5.5` and
`2026.7.1-2` are fully tested.

Use the [OpenClaw setup guide](../../docs/openclaw/setup.md) to install, check,
or remove the plugin.

The runtime follows these rules:

1. Capture does not change an agent result, message, tool, stream, or exception.
2. The plugin keeps reasoning and summaries that the provider exposes to
   OpenClaw.
3. The plugin scrubs known credentials before it seals and uploads a trace.
4. The plugin keeps exact OpenClaw identifiers when they are available and
   records a named capture gap when OpenClaw does not provide one.
5. The plugin removes its first sealed bundle copy only after the cloud uploader
   has made a complete durable copy or has verified a matching acknowledgement
   receipt.

During an outage, pending traces stay on local disk and the agent continues
normally. A trace can contain private or personal content.

## Trusted cross-system correlation

An authorized dispatcher can bind an opaque research task ID before a run:

```text
semantic-layer.correlation.bind
{ "runId": "<chat.send idempotency key>", "taskId": "<research task ID>" }
```

The Gateway requires `operator.admin`. The dispatcher waits for
`{ "accepted": true }` and then calls `chat.send` with the same run ID as its
idempotency key. The plugin consumes the binding once and writes only protected
task and run tokens. It never reads correlation from prompts, messages, tool
inputs, or tool outputs.

Use the same customer identity key in OpenClaw and any other service that must
join this task. Keep separate ingestion keys and installation IDs for each
installation.

## Development

Run these checks from the repository root:

```sh
pnpm --filter semantic-layer-openclaw test
pnpm --filter semantic-layer-openclaw typecheck
pnpm --filter semantic-layer-openclaw build
```
