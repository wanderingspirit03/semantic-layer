# Capture contract

The SDK records evidence that an integration exposes. It does not reconstruct
hidden events, and it does not claim that a reported result proves an outside
effect.

Framework adapters translate official callbacks into the shared record kinds.
Adapters do not write trace files, and the shared projector does not depend on
framework payload shapes.

```text
official callback
    -> adapter normalization
    -> semantic projector
    -> trace record
```

## Evidence origin

Every record has one origin.

| Origin | Meaning |
|---|---|
| `observed` | The integration exposed the event or value during the run. |
| `context` | The integration received the value as history or model input. |
| `inferred` | The SDK derived the statement from earlier trace records. |

Use `observed` only for a hook, callback, return value, exception, or other
value available at the capture point. The origin means that the SDK observed a
report. It does not prove that the report is true outside the application.

Use `context` for history and input. The record time says when capture received
the context, not when the historical event happened. Context must never become
observed execution.

Use `inferred` only when earlier records prove the statement. Link those
records with `derived_from`. If the available evidence cannot prove a material
fact, record a `loss` instead of guessing.

## Runs and scopes

Each independent framework run starts with `run.start` and ends with at most
one `run.outcome`. Turns, steps, and agents use `scope` records under that root.
A nested scope requires an exact parent receipt.

Normal termination becomes `completed`. Completion means that the framework
or process stopped normally. It does not prove that the agent met its goal or
that an answer reached a user.

An `output_ref` links an outcome to an exact earlier `message`,
`model.response`, `tool.result`, or `state`. A `result_ref` on a state record
links only to an exact earlier `model.response` or `tool.result`. An invalid
receipt produces a named loss. The SDK never pairs records by order, time,
name, or similar content.

## Models and messages

A complete model operation uses this relation:

```text
model.response --result_of--> model.request
```

The adapter uses the same native operation identity on both records, or it
uses an exact receipt from the request. A request can reference earlier
messages, model responses, or tool results as context. An opaque prompt does
not become a user message unless the integration exposes its role and content.

The adapter combines consumed stream chunks into one terminal response.
Capture does not consume another chunk or call a provider finalizer. If the
application stops a stream, the response keeps content already delivered and
uses `cancelled` when the integration observed that boundary.

Provider and framework exposed reasoning is normal capture evidence. The
adapter retains readable reasoning text and summaries as an ordered list on
the exactly correlated `model.response`. Each block has type `text` or
`summary`, and visible answer content stays separate. Repeated blocks stay
repeated unless the adapter can prove that two native fields are the same
field.

Some host hooks expose a complete assistant message with reasoning but omit
the model call identity. A host adapter may retain that message in a
source owned `state` record and add a named correlation loss. The adapter must
not infer a model relation from time, order, model name, or content.

The SDK does not rebuild reasoning that the provider hides or redacts.
Encrypted bodies, signatures, and continuation data are not readable reasoning
and are not stored as reasoning. A provider that exposes no reasoning produces
no loss.

## Tools

Tool evidence uses these relations:

```text
tool.call --derived_from--> tool.proposal
tool.result --result_of--> tool.call
```

The adapter keeps the model proposal separate from the application call. The
proposal stores the proposed name and input, while the call stores what the
application executed. A difference between them remains visible.

Every pair uses the exact native call identity when one exists. Equal names,
inputs, output values, timing, and list position do not prove identity. The
same rule applies when provider and framework hooks report parts of one tool
operation.

A historical tool message has origin `context`. It is not evidence that the
tool ran during the captured run.

## State, verification, and errors

State records describe meaningful changes such as interrupts, resumes, and
handoffs. An adapter stores a source version only when the framework exposes
one. It should retain changes or useful checkpoints instead of copying a full
cumulative state after every callback.

A successful tool result means that the tool reported success. A separate
`verification` record represents an observed check of an action, claim, or
outside effect. Capture never performs an extra read, test, or network request
to create verification evidence.

Errors retain the exposed type, message, recoverability, and optional code or
details. A failed or cancelled callback takes precedence over an old success
field. Contradictory terminal evidence remains visible through an `unknown`
outcome or a named loss instead of becoming a false success.

## Losses

A `loss` records material evidence that capture could not retain or correlate.
Use a short `snake_case` reason and plain detail. Common causes include source
data that the framework did not expose, privacy scrubbing, unsupported native
events, truncation, and persistence failure.

Link a loss to the affected record when one exists. Repeated versions of the
same gap should be combined into a bounded count. A recovered trace keeps its
readable prefix and describes any uncertain tail with a loss.

Missing evidence is not always a loss. For example, a provider that exposes no
reasoning gives the SDK no evidence that reasoning existed. The adapter should
omit the field without making a claim about hidden data.

## Duplicate evidence

Frameworks can report one fact through several official hooks. An adapter may
drop a proven duplicate when another exact record keeps the fact. It must not
deduplicate arbitrary JSON by value, because two equal tool calls can be two
separate attempts.

A terminal outcome should not repeat a final message, model response, tool
result, or cumulative framework state. A distinct return value may remain on
the outcome.

## Application behavior

Capture must preserve the application's return value, exception, stream,
cancellation, iterator behavior, and callback order. It must not run a tool,
retry work, call user code, or perform verification only to improve the trace.

During shutdown, a source stops accepting callbacks and drains work that it
already accepted. Late callbacks are ignored. Capture failures belong in
capture health and must not replace the application's result or exception.

An integration is correct when one bundle test proves these rules against the
exact supported release. The [integration inventory](integrations.md) lists the
versions with that evidence.
