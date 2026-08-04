# Storage

The capture SDK writes bundles to local storage. The separate cloud package can
upload a sealed bundle later.

## Bundle files

One capture session creates one directory:

```text
<output>/
└── run-<bundle-id>/
    ├── manifest.json
    ├── trace.jsonl
    └── blobs/          # present only when the bundle has blobs
```

The active writer also uses a private `.writer.lock` file. A successful seal
removes the lock, so the lock is not part of the sealed bundle.

The manifest stores facts about the capture session, source definitions,
counts, byte lengths, loss count, and the trace digest. The trace file is
append only during capture and contains one complete record per line. Large or
binary evidence can live in a content addressed blob that a trace record
references.

Relations and losses stay in `trace.jsonl`. The SDK does not create separate
relation, loss, timeline, or summary files.

## Capture lifecycle

```text
initialize or createCapture -> write records -> flush -> shutdown -> seal
```

In TypeScript, `initialize` joins one compatible capture session for the
process. `createCapture` opens an independent session and bundle. Separate
handles can share an output directory.

In Python, `initialize` owns one compatible capture session for the process.

`flush` waits for accepted writes and updates the open manifest. It does not
finish the capture session. `shutdown` stops normal admission and waits for
accepted work. It then seals the manifest and returns the bundle path.

Always check the capture status returned by shutdown. Application work can
succeed while capture reports a storage or shutdown error.

## Long runs

A bundle represents a capture session, not one agent run. It can contain one
long workflow, several roots, and concurrent model or tool operations. Keep one
session open across normal work in the same process.

A workflow that resumes in another process creates another bundle. Reuse the
same identity key and supply the conversation, turn, turn index, and previous
turn identity at the new root. The SDK stores pseudonymous continuity values,
so an analyzer can order the bundles. Record links still stay inside one
bundle.

The SDK places limits on queued records and completed correlation history.
Active runs, scopes, model requests, and tool calls remain available while they
are open. If old completed evidence is no longer available for a new relation,
the SDK records a named `unresolved_*` loss instead of guessing.

## Recovery and storage failure

At initialization, the SDK finds stale open runs in the selected output
directory. It moves a stale run to a `quarantine-run-*` directory and records
recovery evidence in the new bundle. A malformed or partial tail remains
uncertain, and the SDK does not invent missing records.

If the active trace becomes unwritable, the writer stops normal persistence.
When safe local recovery is possible, it replaces uncertain trace bytes with
one durable `persistence_failure` loss.

## Storage rules

- Add `.semantic-layer/` to the application's ignore file.
- Call shutdown at the application's lifecycle boundary.
- Treat a sealed bundle as completed storage, not successful agent work.
- Do not edit a sealed bundle before validation or analysis.
- Copy the complete directory when moving a bundle.
- Copy every linked bundle when moving a workflow that resumed in another
  process.

The [trace format](trace-format.md) explains the contents of a sealed bundle.
The [privacy guide](privacy-and-security.md) explains how to protect local
trace data.
