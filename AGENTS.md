# Semantic Layer repository guide

Semantic Layer is a local TypeScript and Python SDK that turns evidence
from agents, frameworks, providers, and OpenTelemetry into compact semantic
traces for silent and logical failure analysis.

## Start here

- `README.md` is the newcomer map and product boundary.
- `docs/sdk/index.md` routes users to current SDK behavior.
- `docs/sdk/capture-contract.md` defines shared capture behavior.
- `docs/maintainers/architecture.md` maps module ownership and change locations.
- `docs/sdk/integrations.md` is the authority for supported versions and seams.

## Sources of truth

- `packages/sdk/` and `packages/python/` contain runtime behavior.
- `contracts/capture/v1/` defines normalized capture input.
- `contracts/trace/v1/` defines persisted records and legacy manifests.
  `contracts/trace/v2/` defines the current extended manifest.
- `contracts/ingest/v1/` defines cloud transport shared by every framework.
- `packages/cloud/`, `packages/cloud-ingest/`, and `infra/gcp/` contain the
  optional uploader, receiver, and hosted infrastructure.
- Executable tests settle behavior when prose and implementation disagree.

## Product invariants

- A sealed bundle contains `manifest.json`, `trace.jsonl`, and optional `blobs/`.
- Relations and losses stay inside `trace.jsonl`. Do not add sidecar formats.
- Record only evidence exposed by the integration seam; never invent facts.
- Preserve exact correlation when available and name material capture gaps.
- Capture must not change application results, exceptions, streams, or tools.
- Framework support is limited to exact versions exercised by integration tests.
- Reasoning text and summaries exposed by providers and frameworks are retained.
  Hidden, encrypted, signed, or provider redacted reasoning is never rebuilt.
- The capture SDK remains local. Cloud transport is a separate module and may
  upload only validated sealed bundles after credential scrubbing.

## Working rules

- Make the smallest coherent change and follow existing SDK patterns.
- Keep adapters thin. Shared semantics belong in the projection and runtime layers.
- Keep TypeScript and Python semantics aligned when changing shared behavior.
- Update current documentation in the same change as observable behavior.
- Change source schemas, then regenerate outputs. Never edit generated files by hand.
- Do not commit secrets, real customer data, traces, caches, transcripts, build
  output, or experiment artifacts.
- Remove superseded documentation instead of preserving an implementation diary.

## Verification

- TypeScript: `pnpm test` and
  `pnpm --filter semantic-layer-capture run typecheck`.
- Python: `pnpm test:python`.
- Contracts or generated models: `pnpm contracts:check`.
- Packaging changes: `pnpm test:packages`.
- Shared or cross-package changes: `pnpm verify`.

Before finishing, inspect all tracked and untracked changes in scope and
remove only artifacts introduced by the task.
