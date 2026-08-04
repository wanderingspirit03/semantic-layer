# Semantic Layer for OpenClaw

`semantic-layer-openclaw` is the runtime plugin that captures OpenClaw activity
and sends sealed traces through the Semantic Layer cloud uploader. OpenClaw
`2026.5.5` and `2026.7.1-2` are exactly qualified. Setup and doctor can check
other stable releases from `2026.5.5` onward without treating them as qualified.

Use the [OpenClaw setup guide](../../docs/openclaw/setup.md) to install, check,
disable, or remove the plugin. OpenClaw currently requires the managed Semantic
Layer ingest endpoint.

The runtime follows these capture rules:

- Capture does not change an agent result, message, tool, stream, or exception.
- The plugin keeps reasoning and summaries that the provider exposes to
  OpenClaw.
- The plugin scrubs known credentials before it seals and uploads a trace.
- The plugin uses exact OpenClaw identifiers when they are available and
  records a missing link when OpenClaw does not provide one.

Trace content is not anonymous and can contain private or personal content.

## Development

Run the package checks from the repository root:

```sh
pnpm --dir packages/openclaw test
pnpm --dir packages/openclaw typecheck
pnpm --dir packages/openclaw build
```
