# Normalized capture contract v1

The capture contract defines the shared seam between adapters and semantic
projection. Adapters translate native provider, framework, custom-agent, or
OpenTelemetry evidence into normalized capture events. The TypeScript and
Python projectors turn accepted events into `semantic_trace_record_v1`
records; bundle writers independently select a documented manifest schema.

## Files

| File | Role |
|---|---|
| `semantic-capture-event.schema.json` | Closed normalized event envelope accepted by the projection seam |
| `semantic-projection-cases.json` | Shared TypeScript/Python cases for observable projection meaning |
| `credential-safety-cases.json` | Shared credential-pattern corpus for both scanners |
| `traceparent-cases.json` | W3C traceparent validity corpus; Python consumes it directly and both runtimes test the same parent-context outcomes |
| `examples/` | Isolated capture-event examples; these are not persisted trace records |

The similar policy names are separate versioned fields:

- capture-event provenance uses `rich_local_credential_scrubbed`;
- manifest v2 capture policy uses `rich-credential-scrubbed`; and
- manifest privacy mode is `local-rich` for current SDK output. The manifest
  schemas reserve `production-safe`, but schema acceptance is not evidence that
  the SDK implements that mode.

Change the canonical schema or shared corpus here first. Regenerate committed
TypeScript models and packaged schema copies with the repository scripts; do
not edit generated output directly. `pnpm contracts:check` verifies schema
generation, examples, and packaged-copy freshness. The shared projection cases
are exercised by the TypeScript and Python conformance tests under
`pnpm verify`.
