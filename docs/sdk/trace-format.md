# Trace format

A semantic trace is a sealed directory that an analyzer can read in persisted
order. The machine readable schemas live in
[`contracts/trace/v1`](../../contracts/trace/v1/) and
[`contracts/trace/v2`](../../contracts/trace/v2/).

## Bundle

```text
run-<bundle-id>/
├── manifest.json
├── trace.jsonl
└── blobs/          # present only when the manifest reports blobs
```

`manifest.json` contains facts shared by the bundle. `trace.jsonl` contains one
complete record per line. The optional `blobs/` directory contains large or
binary evidence referenced by records.

Relations and evidence gaps remain in `trace.jsonl`. There are no separate
relation, loss, timeline, or summary files.

One bundle represents one SDK capture session. A bundle can contain several
root runs, and each root has one `run.start` plus at most one `run.outcome`.
Records stay under a root through their parent chain.

Complete examples are available for a
[coding agent](../../contracts/trace/v1/examples/coding-agent/) and a
[framework workflow](../../contracts/trace/v1/examples/framework/).

## Manifest

Local captures without an installation identity use
`semantic_trace_manifest_v1`. Managed captures use
`semantic_trace_manifest_v2`. Both versions declare
`semantic_trace_record_v1` as the record schema.

| Field | Stored value |
|---|---|
| `bundle_id` | The identity of the capture bundle. |
| `record_schema` | The schema used by every trace line. |
| `state` | `open`, `closing`, `sealed`, or `recovered`. |
| `sdk` | The language and SDK version. |
| `privacy_mode` | `production-safe` or `local-rich`. |
| `sources` | Definitions referenced by record source IDs. |
| `trace` | The trace path, counts, byte length, loss count, and SHA256 digest. |
| `blobs` | The blob path, count, and total bytes. |
| `started_at`, `updated_at`, `sealed_at` | Capture lifecycle times. |

A sealed or recovered manifest has `sealed_at` and the digest of the exact
`trace.jsonl` bytes. The `blobs/` directory can be absent when the blob count is
zero.

Manifest v2 also stores the capture policy, an optional opaque installation
identity, and source qualification. `exact_qualified` names an exact tested
source version. `capability_checked_unqualified` means that preflight checks
passed but does not claim full support. `unknown` records that qualification is
not known.

## Records

The manifest declares the record schema once. Each trace line then has this
shape:

```json
{
  "id": "rec_flow_006",
  "seq": 6,
  "time": "2026-07-25T13:00:00.350Z",
  "kind": "tool.result",
  "origin": "observed",
  "source": "src_langgraph",
  "parent": "rec_flow_001",
  "data": {
    "call_id": "call_weather_london",
    "status": "succeeded",
    "output": {
      "temperature_c": 21
    }
  },
  "links": [
    {
      "type": "result_of",
      "record": "rec_flow_005"
    }
  ]
}
```

| Field | Stored value |
|---|---|
| `id` | A unique record identity within the bundle. |
| `seq` | The persisted order, starting at 1 without gaps. |
| `time` | The time when capture received or created the record. |
| `kind` | The schema for the data field. |
| `origin` | `observed`, `context`, or `inferred`. |
| `source` | A source declared in the manifest. |
| `parent` | An optional earlier containment parent. |
| `data` | The payload for the record kind. |
| `links` | Optional typed references to earlier records. |

References point backward, so an analyzer can validate the file in one pass.
The `seq` field gives persisted order. A timestamp on historical context says
when capture received the context, not when the historical event happened.

## Record kinds

| Kind | Purpose |
|---|---|
| `run.start` | Starts an independent run root. |
| `scope` | Records a turn, step, or agent lifecycle. |
| `message` | Stores a message with its role and content. |
| `model.request` | Stores observed model context, tool names, settings, and definitions. |
| `model.response` | Stores status, visible content, exposed reasoning, finish reason, and usage. |
| `tool.proposal` | Stores a model proposed tool call. |
| `tool.call` | Stores the tool call that the application executed. |
| `tool.result` | Stores the result reported for a tool call. |
| `state` | Stores a meaningful state or control change. |
| `error` | Stores an observed error. |
| `verification` | Stores an observed check and its status. |
| `loss` | Names evidence that capture could not retain or correlate. |
| `run.outcome` | Stores how a root run ended. |

The [capture contract](capture-contract.md) defines what these records mean and
when an integration can create them.

## Links

Links describe relations that sequence and containment cannot express.

| Link | The current record points to |
|---|---|
| `result_of` | The request or call that produced the result. |
| `derived_from` | Earlier evidence that supports the record. |
| `verifies` | The action checked by a verification. |
| `affects` | The record affected by a loss or other evidence. |
| `continues_from` | The earlier scope that this scope continues. |
| `branches_from` | The earlier scope from which this scope branched. |

## Model context

A model request can store complete context in `context_refs`, or it can reuse
an unchanged prefix through `context_base_ref` and store only a new suffix.
Both fields refer to earlier records under the same root.

An analyzer expands a requested context by following each base reference and
collecting its suffix. The analyzer should use record, depth, and output limits.
A cycle, forward reference, or different root makes the context invalid.

## Blobs

Large or binary evidence can move to a content addressed blob. The record that
owns the evidence keeps its path, digest, byte count, and media type in
`blob_refs`. An analyzer can therefore find and verify a blob without a second
index.

Blobs stay inside the sealed bundle. The credential scanner checks a blob
before the writer keeps it. A blob does not change the meaning or origin of the
record that references it.

The [storage guide](storage.md) explains bundle lifecycle and recovery. The
[validation guide](verification.md) explains how to check a sealed bundle.
