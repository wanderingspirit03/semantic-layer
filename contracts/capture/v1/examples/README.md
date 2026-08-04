# Internal capture-event examples

These files exercise the internal adapter-to-projector event schema. They are
not customer trace records. Each event is isolated, so its references need not
resolve here. Do not concatenate these examples into a trace bundle.

Coverage:

| Requirement | Example |
|---|---|
| success | `isolated-success-event.json` |
| stream | `isolated-stream-event.json` |
| tool | `isolated-tool-event.json` |
| error | `isolated-error-event.json` |
| loss | `isolated-loss-event.json` |
| blob | `isolated-blob-event.json` |
| OpenTelemetry | `isolated-otel-event.json` |
| multi-turn | `isolated-multi-turn-event.json` |
| owned coverage decision | `isolated-owned-event.json` |
| ambiguous coverage decision | `isolated-ambiguous-event.json` |
| ambiguity marker linked to its decision | `isolated-ambiguity-marker-event.json` |
| unknown event and extension preservation | `isolated-unknown-event.json` |
Passing this schema proves only that an adapter event has the expected internal
shape. The projector turns those events into `semantic_trace_record_v1`
records. Bundle writers select manifest v1 or v2 according to the documented
producer policy. The library validator checks complete on-disk bundles,
including references, accounting, permissions, blobs, and secrets.
