# Tavi Trigger attempt capture

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
pnpm add semantic-layer-capture@0.2.0-beta.2 \
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

Create one attempt router when the Trigger worker starts. Add its span
processor to Tavi's existing provider exactly once, beside the Latitude
processor. Do not add an attempt source directly to the provider. OpenTelemetry
1.25 does not provide a safe way to remove that processor, and direct setup can
mix spans from concurrent attempts.

```ts
import { createTaviOpenTelemetryAttemptRouter } from './otel-attempt-router.js';

const semanticLayerAttemptRouter = createTaviOpenTelemetryAttemptRouter();
existingTracerProvider.addSpanProcessor(semanticLayerAttemptRouter.spanProcessor);
```

Create a new OpenTelemetry source for every enrolled attempt. Pass it to the
shared router through the wrapper.

```ts
import { createOpenTelemetrySource } from 'semantic-layer-capture';

const semanticLayerOtelSource = createOpenTelemetrySource({ version: '1.25.1' });

const result = await runTaviTriggerAttempt({
  tenant: trustedTenantOrNull,
  trigger: triggerIdentityFromContext(
    ctx,
    stableResearchId,
    incomingTraceparent,
  ),
  signal: cancellationSignal,
  openTelemetry: {
    source: semanticLayerOtelSource,
    router: semanticLayerAttemptRouter,
  },
  reportDelivery(delivery) {
    existingSafeDiagnosticSink.record(delivery);
  },
  task: async ({ signal }) => await runResearch({ signal }),
});
```

The wrapper maps only public fields from Trigger 4.4.4. It uses `ctx.run.id`,
`ctx.run.parentTaskRunId`, `ctx.run.rootTaskRunId`, and
`ctx.attempt.number`. Trigger 4.4.4 does not expose a replay relation. A replay
therefore appears as a new run under the same research task.

Keep Tavi's existing provider and Latitude processor. Add only the shared
router processor. The router uses the direct OpenTelemetry 1.25 types owned by
Tavi. It does not import a processor from Trigger's nested OpenTelemetry 2.x
packages. It remembers which attempt started each span and sends the completed
span only to that attempt's Semantic Layer source. Spans outside an enrolled
attempt are ignored by Arcus and remain available to Latitude.

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
The wrapper does not wait for a promise returned by this callback. A stalled
diagnostic sink cannot delay the customer result or error.

The wrapper creates a private output directory and a private upload spool for
each attempt. It reports `acknowledged` only after the cloud service confirms
the expected bundle digest. A local pending spool is not considered safe in an
isolated Trigger worker.

A successful research must pass the `rich-agent` profile before upload. It
must contain one root and outcome, one model request and response, and one tool
call and result under the same root. A successful but empty capture reports
`capture_failed` and is not uploaded. A failed or cancelled research uses
structural validation because a provider can fail before a model or tool
result exists. Named loss records remain part of that bundle. Missing required
correlation, rejected records, an invalid bundle, or an unexplained capture
error prevents upload.

The wrapper sends raw Trigger correlation through the capture API. Semantic
Layer protects the task, current run, parent run, and root run IDs before it
writes them. Current, parent, and root run IDs use the same identity domain, so
a child bundle can match its protected parent ID to the parent's protected
current run ID. Retries keep the same protected run ID and use a higher attempt
number. A replay has a new Trigger run ID. When Trigger exposes a parent ID but
does not expose a root ID, the mapper leaves the root relation absent. It does
not claim that the child is its own root.

The Trigger cancellation signal starts the same bounded finalization used by
normal completion. The capture runtime waits for active work within its
deadline, then records a cancelled outcome and a named timeout loss if work has
not settled. The wrapper still preserves the exact value later thrown by the
task. A forced worker stop cannot run cleanup. The example does not claim
evidence delivery after a forced stop.

The tests also run a parent ralph loop and child search loop through fresh
OpenTelemetry sources. Each bundle contains a complete model pair and tool
pair, passes the `rich-agent` validation profile, receives its own upload
acknowledgement, and joins through protected task and execution IDs.

## OpenClaw relation

Trigger parent and child bundles join as soon as both receive the same
`tenantTaskId`. Joining the originating OpenClaw bundle needs one more exact
input at the OpenClaw run start. OpenClaw capture must receive the same
`tenantTaskId` as `correlation.taskId` and use the same customer identity key.

A `tenantTaskId` that appears only inside a captured tool result is not enough.
The reader does not inspect private content or guess a relation. Do not claim
the OpenClaw to Trigger link until Tavi exposes the authoritative task ID at
the OpenClaw run start and a staging trace proves the match.

## Staging checks

Run these checks before production:

1. Run Tavi's full Trigger test suite on Node 22.

2. Run the tenant telemetry test with Arcus disabled and enabled. Latitude must
   receive the same expected spans in both runs.

3. Run the deployed OpenTelemetry probe. Confirm Latitude and Arcus delivery.

4. Run one synthetic `ralph-loop` that starts one `search-loop` child. Confirm
   model and tool records, parent and child relation, valid bundles, and two
   cloud acknowledgements.

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
pnpm install --dir examples/tavi-trigger-attempt --ignore-workspace --frozen-lockfile
pnpm --dir examples/tavi-trigger-attempt run test
pnpm --dir examples/tavi-trigger-attempt run typecheck
```
