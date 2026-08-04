# Local capture security model

The local capture security model covers the TypeScript and Python SDKs,
official adapters, custom sources, and OpenTelemetry input. Cloud upload and
hosted storage use a separate security model.

## Assets

A local trace bundle contains:

```text
run/
├── manifest.json
├── trace.jsonl
└── blobs/
```

`blobs/` is optional. The bundle can contain prompts, responses, provider
reasoning, reasoning summaries, tool inputs and results, errors, application
state, source fields, usage, and logs. The bundle is private plaintext and may
contain personal or confidential data.

Configured credentials and known credential formats must not be stored in the
bundle. A loss record is also an asset because it tells an analyst that evidence
was dropped, shortened, or could not be correlated.

## Trust seams

The application, SDK, and bundle run as the same operating system user. The SDK
trusts that user account for local file access, but it does not trust values
received from the application, framework, provider, tool, or OpenTelemetry.

The SDK also treats blob paths, custom source records, errors, object fields,
and lifecycle order as untrusted input. A compromised application or another
process running as the same user is outside this boundary.

Every source writes through the same admission and persistence code. An adapter
cannot bypass the limits, credential scan, path checks, or loss accounting.

## Enforced controls

The SDK creates bundle directories and files that only the owner can read. It
rejects absolute paths, parent traversal, paths outside the bundle, and links in
protected bundle paths. Blob references use content digests and are checked
before sealing.

The SDK does not search environment variables for secrets. A caller can supply
known secret values in memory. The SDK removes those values and supported
credential formats from nested records, errors, source fields, and blobs. It
scans the final bytes before committing a file. If the scan cannot prove that a
payload is safe to write, the SDK omits the payload and records loss when it can
do so safely.

Serialization has limits for nesting, collection size, string size, and total
retained bytes. The SDK does not invoke arbitrary getters, `toJSON`, format
hooks, export helpers, or finalizers while it captures evidence. Cycles and
unsupported values cannot cause unlimited work.

The in process queue has a fixed bound. When the queue is full, the SDK records
`queue_backpressure_drop` through a protected control path. Missing, shortened,
or uncertain evidence cannot appear as complete evidence.

The writer appends accepted records in order. Manifest updates and sealing use
atomic file replacement. Recovery checks the manifest, trace prefix, counts,
digests, and blob references before it treats a bundle as sealed. If a crash
leaves an uncertain tail, recovery records the uncertainty instead of creating
evidence.

Capture cannot replace an application return value, exception, stream, or tool
result. Hooks do not run tools, retry provider calls, consume streams, or invoke
application code to obtain more evidence. A capture failure changes capture
status and trace loss, not the observed application result.

The SDK records only values exposed at the integration seam. It keeps provider
reasoning text and summaries when the provider exposes them. It does not
reconstruct hidden, encrypted, signed, or provider redacted reasoning. If exact
correlation is unavailable, the SDK keeps the ambiguity or records loss.

## Remaining risk

Credential scanning covers explicit values, supported formats, and tested
encodings. It cannot find every possible secret. Credential removal does not
make a bundle anonymous, free of personal data, encrypted, or safe to share.

The local controls do not protect a bundle from the owner account, another
process with the same user, a compromised application, an administrator, the
operating system kernel, or physical device access. They also cannot correct a
framework that changes behavior before the SDK observes it.

Tests and public fixtures must use synthetic data. A maintainer who changes the
writer, serializer, credential scanner, queue, source seam, file layout,
recovery code, or bundle schema must test the affected control.
