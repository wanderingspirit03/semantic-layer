# SDK documentation

Semantic Layer records evidence from agents, frameworks, providers, and
OpenTelemetry as local semantic trace bundles. Another person or coding agent
can inspect those bundles for silent and logical failures.

## Use the SDK

- [Quickstart](quickstart.md) shows one local TypeScript and Python capture.
- [Integrations](integrations.md) lists tested versions, setup patterns, exposed
  evidence, and known gaps.
- [Custom integration](custom-integration.md) explains manual capture and the
  typed callback bridge.
- [Storage](storage.md) explains bundle lifecycle, long runs, and recovery.
- [Privacy and security](privacy-and-security.md) explains credential scrubbing
  and the private data that remains.
- [Bundle validation](verification.md) explains how to check a sealed bundle.

## Understand the trace

- [Capture contract](capture-contract.md) defines evidence origin, correlation,
  reasoning, tools, outcomes, verification, and losses.
- [Trace format](trace-format.md) defines the files, records, links, context
  references, and blobs in a sealed bundle.
- [Trace v1 contracts](../../contracts/trace/v1/) and
  [trace v2 contracts](../../contracts/trace/v2/) contain the machine readable
  schemas.

## Package entry points

- [TypeScript SDK](../../packages/sdk/README.md)
- [Python SDK](../../packages/python/README.md)
- [OpenClaw setup](../openclaw/setup.md)

The TypeScript and Python SDKs write local bundles. The separate cloud package
uploads validated sealed bundles, and the separate OpenClaw package connects
capture to OpenClaw.

## Maintain the repository

- [Architecture](../maintainers/architecture.md) explains module ownership and
  change locations.
- [Testing](../maintainers/testing.md) lists repository checks and integration
  test requirements.
- [Framework reasoning coverage](../maintainers/reasoning-coverage.md) summarizes
  verified framework behavior and limits in public integration seams.
