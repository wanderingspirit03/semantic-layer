# Tavi Trigger capture pilot

This directory contains code for one customer pilot. It is not part of the
main SDK or its supported integration list. Keep the pilot implementation,
tests, rollout steps, and customer names inside this directory.

The example wraps one Trigger task attempt. It uses
`semantic-layer-capture@0.2.0-beta.2` and
`semantic-layer-cloud@0.1.0-pilot.4` without adding another published package.
Its context mapping is checked against `@trigger.dev/sdk@4.4.4`.

Use Node 22 for the Trigger task. Trigger uses Node 21 by default, while both
Semantic Layer packages require Node 22 or newer. Set `runtime: "node-22"` in
the Trigger configuration.

```ts
export default defineConfig({
  runtime: 'node-22',
});
```

This setting changes the runtime for every task in the Trigger deployment.
Test the complete deployment in staging before production.

Install the exact package pair in Tavi's Trigger task package:

```sh
npm install --save-exact \
  semantic-layer-capture@0.2.0-beta.2 \
  semantic-layer-cloud@0.1.0-pilot.4
```

## Required secrets

Store these values as Trigger production secrets:

* `SEMANTIC_LAYER_INGEST_KEY`
* `SEMANTIC_LAYER_INSTALLATION_ID`
* `SEMANTIC_LAYER_IDENTITY_KEY`
* `SEMANTIC_LAYER_ENDPOINT`

Do not put the ingest key in source code, a task payload, a command, or a log.
Read each secret inside the worker and pass it directly to the trusted tenant
lookup.

Use a stable service name such as `tavi-trigger-prod-<customer-slug>`. The
Trigger installation ID is separate from every OpenClaw VM installation ID.
One Tavi customer normally has one Trigger installation and one installation
for each OpenClaw VM.

## Tenant lookup

The wrapper accepts trusted tenant configuration or `null`. The caller must
complete its authoritative tenant lookup before calling the wrapper. A
non-null configuration means that the tenant is enrolled. The configuration
includes the tenant's service name, installation ID, identity key, endpoint,
and ingest key.

Pass `null` when the tenant is not enrolled. The wrapper checks for `null`
before creating any file, then calls the task directly and reports
`not_captured`.

The shared wrapper has no customer branch. During a pilot, Tavi's resolver
returns configuration only when the server supplied tenant ID is enrolled. It
returns `null` for every other tenant. Later enrollment adds another trusted
configuration entry. It does not change this wrapper.

Use one stable identity key for all Trigger attempts that belong to the same
customer. The same key is also required on any OpenClaw installation that must
join these bundles. Keep different customers on different identity keys.

## Task integration

Choose the provider attachment based on how long the provider lives. When one
provider belongs to one attempt, add a fresh Arcus processor directly and shut
the provider down with the attempt. The following form uses that pattern:

```ts
const source = createOpenTelemetrySource({ version: '1.25.1' });
attemptProvider.addSpanProcessor(source.spanProcessor);

await runTaviTriggerAttempt({
  // The provider and source belong only to this attempt.
  openTelemetry: { source },
  // ...
});
```

When attempts share one provider, add one permanent attempt router so
concurrent tenants cannot mix spans. Use Tavi's direct OpenTelemetry `1.25.1`
types, and do not use processor classes from Trigger's nested OpenTelemetry
`2.x` packages. The following form creates one router when the worker starts
and adds it beside Latitude exactly once:

```ts
import { createTaviOpenTelemetryAttemptRouter } from './otel-attempt-router.js';

const semanticLayerAttemptRouter = createTaviOpenTelemetryAttemptRouter();
existingTracerProvider.addSpanProcessor(semanticLayerAttemptRouter.spanProcessor);
```

Create one cancellation registry beside the provider attachment. Trigger's task-local
`onCancel` hook aborts the matching attempt and waits for its bounded telemetry
finalization. Create a new OpenTelemetry source inside every enrolled attempt.

```ts
import { task } from '@trigger.dev/sdk/v3';
import { createOpenTelemetrySource } from 'semantic-layer-capture';
import { createTaviTriggerCancellationRegistry } from './trigger-cancellation.js';

const cancellations = createTaviTriggerCancellationRegistry();

export const researchTask = task({
  id: 'tavi-research',
  onCancel: async ({ ctx }) => {
    await cancellations.cancel(ctx.run.id);
  },
  run: async (payload, { ctx }) => {
    const cancellation = cancellations.register(ctx.run.id);
    const source = createOpenTelemetrySource({ version: '1.25.1' });
    return await runTaviTriggerAttempt({
      tenant: trustedTenantOrNull,
      successfulAttemptProfile: 'orchestrator',
      trigger: triggerIdentityFromContext(
        ctx,
        stableResearchId,
        incomingTraceparent,
      ),
      cancellation,
      openTelemetry: {
        source,
        router: semanticLayerAttemptRouter,
      },
      reportDelivery(delivery) {
        existingSafeDiagnosticSink.recordSynchronously(delivery);
      },
      task: async ({ signal }) => await runResearch(payload, { signal }),
    });
  },
});
```

The wrapper maps only public fields from Trigger 4.4.4. It uses `ctx.run.id`,
`ctx.run.parentTaskRunId`, `ctx.run.rootTaskRunId`, and
`ctx.attempt.number`. Trigger 4.4.4 does not expose a replay relation. A replay
therefore appears as a new run under the same research task.

For an attempt-local provider, omit `router` from `openTelemetry`. For a shared
provider, keep the router shown above. Spans outside an enrolled attempt remain
available to Latitude.

Tavi's tool spans need these attributes:

```ts
span.setAttributes({
  'gen_ai.operation.name': 'execute_tool',
  'gen_ai.tool.name': toolName,
  'gen_ai.tool.call.id': nativeCallId,
  'gen_ai.tool.call.arguments': JSON.stringify(toolArguments),
  'gen_ai.tool.call.result': JSON.stringify(toolResult),
});
```

Use `gen_ai.operation.name = "invoke_agent"` on agent roots. Use `chat`,
`text_completion`, or `generate_content` on model spans. Create the tracer with
schema URL `https://opentelemetry.io/schemas/gen-ai/1.42.0`. Keep all existing
Latitude fields.

The task receives the same return value or thrown value from `runResearch`.
Capture, validation, upload, and diagnostic failures do not replace it.

The diagnostic callback receives only one status and an optional safe request
ID. The allowed statuses are `acknowledged`, `timed_out`, `capture_failed`,
`upload_failed`, and `not_captured`. It never receives a secret, local path,
trace content, customer content, or bundle digest.
The callback must store the status synchronously and return `undefined`. An
async diagnostic callback is not supported. This keeps the status visible
without allowing a stalled diagnostic service to delay the customer result or
error.

The wrapper creates a private output directory and a private upload spool for
each attempt. It reports `acknowledged` only after the cloud service confirms
the expected bundle digest. A local pending spool is not considered safe in an
isolated Trigger worker.

A successful `ralph-loop` uses `successfulAttemptProfile: "orchestrator"`. It
must contain protected Trigger correlation, a completed attempt outcome, and a
complete GenAI `invoke_agent` scope from the exact 1.42 schema. It does not need
a model or tool pair.

A successful `search-loop` keeps the default `rich-agent` profile. It must also
contain a model request and response and a tool call and result under the same
root. Do not select a profile by task name inside the shared wrapper. The task
owner passes the profile explicitly.

A successful but empty capture reports `capture_failed` and is not uploaded.
A failed or cancelled research uses structural validation because a provider
can fail before a model or tool result exists. Missing correlation, rejected
records, an invalid bundle, or an unexplained capture error prevents upload.

The wrapper sends raw Trigger correlation through the capture API. Semantic
Layer protects the task, current run, parent run, and root run IDs before it
writes them. Current, parent, and root run IDs use the same identity domain, so
a child bundle can match its protected parent ID to the parent's protected
current run ID. Retries keep the same protected run ID and use a higher attempt
number. A replay has a new Trigger run ID. When Trigger exposes a parent ID but
does not expose a root ID, the mapper leaves the root relation absent. It does
not claim that the child is its own root.

The task-local Trigger cancellation hook starts the same bounded finalization
used by normal completion and waits for it. A cooperative task that settles
within the deadline records a cancelled outcome and can upload. If task cleanup
does not settle before the deadline, the bundle reports `capture_failed` and is
not uploaded. The wrapper still preserves the exact value later thrown by the
task. A forced worker stop cannot run cleanup. The example does not claim
evidence delivery after a forced stop.
Use these exact deadlines:

```ts
{
  shutdownDeadlineMs: 10_000,
  uploadDeadlineMs: 10_000,
  finalizationDeadlineMs: 25_000,
}
```

The final limit covers capture shutdown, validation, spool admission, upload,
and uploader shutdown. It leaves five seconds inside Trigger's 30 second
cancellation grace period. A deadline reports `timed_out` without changing the
customer result or error.

The tests also run a parent ralph loop and child search loop through fresh
OpenTelemetry sources. The parent passes the orchestrator rule. The child
passes the rich agent rule. Both receive an upload acknowledgement and join
through protected task and execution IDs.

## OpenClaw relation

Use the plugin's
[trusted cross-system correlation contract](../../packages/openclaw/README.md#trusted-cross-system-correlation).
Tavi creates the task and run IDs, completes that bind before `chat.send`, and
uses the run ID as the `chat.send` idempotency key.

Deep People forwards the same task ID unchanged to Trigger as `researchId`.
The OpenClaw VM and Trigger installation use separate ingestion keys and
installation IDs, but they use the same customer identity key. Semantic Layer
then writes the same protected task token in both bundles. Cloud ingest and the
setup command need no new field.

Deep People must forward the same task ID without reading it from customer
content. If binding fails, Tavi must not claim the OpenClaw to Trigger link.

## Latitude compatibility

Keep the existing span names, parent relations, content fields, and
Latitude-specific fields. Add only the GenAI 1.42 schema URL and these semantic
operations:

* `invoke_agent` on the agent root
* `execute_tool` on tool execution spans
* `chat`, `text_completion`, or `generate_content` on model spans

These additions are compatible with the existing Latitude exporter. The exact
OpenTelemetry 1.25 test keeps Latitude beside Arcus and proves that Latitude
still receives the same spans. Tavi must repeat that check in staging with its
real exporter before production.

## Staging checks

Run these checks before production:

1. Run Tavi's full Trigger test suite on Node 22.

2. Run the tenant telemetry test with Arcus disabled and enabled. Latitude must
   receive the same expected spans in both runs.

3. Run the deployed OpenTelemetry probe. Confirm Latitude and Arcus delivery.

4. Run one synthetic `ralph-loop` that starts one `search-loop` child. Confirm
   the orchestrator root, the child's model and tool records, the parent and
   child relation, valid bundles, and two cloud acknowledgements.

5. Run a different tenant. It must create no Arcus files, spool, spans, or
   upload.

6. Run two enrolled attempts and one unenrolled attempt at the same time on
   one OpenTelemetry 1.25 provider. Latitude must receive every expected span.
   Each Arcus bundle must contain only its own attempt.

7. Run normal failure, application timeout, cooperative cancellation, retry,
   replay, parallel attempts, and forced upload failure. Telemetry must not
   change the task result or exact thrown value.

8. Search logs for the ingest key, identity key, customer content, local paths,
   bundle digests, and upload response bodies. None may be present.

Return only safe run IDs, attempt numbers, completion state, time, Latitude
received status, Arcus received status, and the safe Arcus delivery status.

## Production and rollback

Do not issue a customer key until the pinned packages have been published and
installed from the public registry in a clean Node 22.16 environment. Then use
the same package versions and Node runtime in production. Store the four
secrets in Trigger production secrets. Enable only the pilot resolver entry.
Run one private verification research and confirm both observability services
with safe identifiers only.

To roll back, disable the pilot resolver entry or deploy the prior Tavi task
version. Leave the Latitude provider and exporter unchanged. Revoke the Trigger
ingestion key. Revert the Node runtime only through a tested deployment
rollback. Keep any bundles that the cloud already acknowledged.

After rollback, run one normal Tavi research and the deployed OpenTelemetry
probe. Confirm that the customer result is unchanged and Latitude still
receives its expected spans before closing the rollback.

## Checks

Run the example tests and type check from the repository root:

```sh
npm ci --prefix pilots/tavi-trigger-attempt
npm test --prefix pilots/tavi-trigger-attempt
npm run typecheck --prefix pilots/tavi-trigger-attempt
```
