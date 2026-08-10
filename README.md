# Semantic Layer

Semantic Layer records what an agent did so people and coding agents can find
silent and logical failures. The TypeScript and Python SDKs write traces to the
local filesystem. Separate packages provide OpenClaw capture and cloud upload.

## Choose a task

- [Use the TypeScript SDK](packages/sdk/README.md).
- [Use the Python SDK](packages/python/README.md).
- [Install Semantic Layer for OpenClaw](docs/openclaw/setup.md).
- [Upload sealed bundles](packages/cloud/README.md).
- [See tested integrations](docs/sdk/integrations.md).

OpenClaw capture currently requires a managed ingest endpoint. The TypeScript
and Python SDKs do not require cloud access.

## What a trace contains

Each capture session creates a sealed bundle:

```text
run-<bundle-id>/
├── manifest.json
├── trace.jsonl
└── blobs/          # optional large values
```

The trace can include model messages, tool activity, usage, results, errors,
and reasoning exposed by a provider or framework. Semantic Layer records
exposed evidence and does not reconstruct hidden, encrypted, signed, or
provider redacted reasoning.

Semantic Layer scrubs known credentials and configured secret values before it
seals a bundle. A trace can still contain private model and tool data, so store
and share it as private application data.

## Reference

- [SDK documentation](docs/sdk/index.md)
- [Capture contract](docs/sdk/capture-contract.md)
- [Trace format](docs/sdk/trace-format.md)
- [Storage lifecycle](docs/sdk/storage.md)
- [Privacy and security](docs/sdk/privacy-and-security.md)
- [Cloud ingest contract](contracts/ingest/v1/README.md)

## Work on the repository

Use Node.js 24, pnpm 8.15, Python 3.12, and uv to match the main CI job:

```sh
corepack enable
corepack prepare pnpm@8.15.0 --activate
pnpm install --frozen-lockfile
pnpm verify
```

Maintainers should start with the [repository architecture](docs/maintainers/architecture.md)
and [testing guide](docs/maintainers/testing.md). Cloud deployment and incident
procedures are in [maintainer documentation](docs/maintainers/cloud/deployment.md).

The main source directories are:

- `packages/sdk/` for the TypeScript SDK.
- `packages/python/` for the Python SDK.
- `packages/openclaw/` for the OpenClaw plugin.
- `packages/openclaw-setup/` for OpenClaw setup and diagnosis.
- `packages/cloud/` for the uploader.
- `packages/cloud-ingest/` for the private receiver.
- `contracts/` for capture, trace, and ingest schemas.
- `infra/gcp/` for the optional hosted infrastructure.

See [AGENTS.md](AGENTS.md) for the repository working contract.
