# Tavi Trigger capture pilot

This directory contains code for one customer pilot. It is not part of the
main SDK or its supported integration list. Keep the pilot implementation,
tests, rollout steps, and customer names inside this directory.

The example wraps one Trigger task attempt. It uses
`semantic-layer-capture@0.2.0-beta.2` and
`semantic-layer-cloud@0.1.0-pilot.5` without adding another published package.
Its context mapping is checked against `@trigger.dev/sdk@4.4.4`.

The OpenClaw side uses these exact packages:

* `semantic-layer-openclaw@0.1.0-pilot.6`
* `semantic-layer-openclaw-setup@0.1.0-pilot.11`

Plugin `0.1.0-pilot.5` and setup `0.1.0-pilot.10` support only an explicit
binding before a direct Gateway request. They do not add native inbound Slack
correlation.

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
  semantic-layer-cloud@0.1.0-pilot.5
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

Tavi creates one `BasicTracerProvider` for each task attempt and shuts it down
when the attempt ends. Resolve the trusted tenant before creating the Arcus
source. For an enrolled attempt, add a fresh Arcus processor beside the
existing Latitude processor before Tavi creates the tracer or any span.

```ts
const source = createOpenTelemetrySource({ version: '1.25.1' });
attemptProvider.addSpanProcessor(source.spanProcessor);

await runTaviTriggerAttempt({
  // The provider and source belong only to this attempt.
  openTelemetry: { source },
  // ...
});
```

Use Tavi's direct OpenTelemetry `1.25.1` types. Do not import processor classes
from Trigger's nested OpenTelemetry `2.x` packages. Pass `null` to the wrapper
before creating a source or provider processor for an unenrolled tenant. An
unenrolled attempt must create no Arcus source, processor, file, spool, or
upload.

Create one cancellation registry beside the task integration. Trigger's
task-local `onCancel` hook aborts the matching attempt and waits for bounded
telemetry finalization.

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
    const tenant = resolveTrustedTenant(payload.tenantId);
    const source = tenant === null
      ? undefined
      : createOpenTelemetrySource({ version: '1.25.1' });
    return await runTaviTriggerAttempt({
      tenant,
      successfulAttemptProfile: 'orchestrator',
      trigger: triggerIdentityFromContext(
        ctx,
        stableResearchId,
        incomingTraceparent,
      ),
      cancellation,
      ...(source ? { openTelemetry: { source } } : {}),
      reportDelivery(delivery) {
        existingSafeDiagnosticSink.recordSynchronously(delivery);
      },
      task: async ({ signal }) => await withTenantRunTelemetry({
        payload,
        signal,
        // The existing helper creates the attempt provider and shuts it down
        // in its existing finally block after run() settles.
        beforeTracerCreated(provider) {
          // Tavi already adds and owns the Latitude processor here.
          if (source) provider.addSpanProcessor(source.spanProcessor);
        },
        async run({ tracer }) {
          return await runResearch(payload, { signal, tracer });
        },
      }),
    });
  },
});
```

The wrapper maps only public fields from Trigger 4.4.4. It uses `ctx.run.id`,
`ctx.run.parentTaskRunId`, `ctx.run.rootTaskRunId`, and
`ctx.attempt.number`. Trigger 4.4.4 does not expose a replay relation. A replay
therefore appears as a new run under the same research task.

`withTenantRunTelemetry()` runs for every tenant, so the existing Latitude
provider lifecycle does not change. Its existing `finally` block shuts down
the provider after `run()` settles. The Arcus processor is added only when the
trusted resolver enrolls the tenant, and it is added before the helper creates
`tracer`. Do not create a second provider, and do not add a process-wide Arcus
router to the current Tavi runtime.

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

The wrapper creates a private output directory and upload spool for each
attempt. Both are disposable transport state, not a retry queue. After capture
and uploader work become quiescent, the wrapper removes only the exact attempt
directory returned by `mkdtemp()` and never removes the caller's temporary
root. Cleanup applies after acknowledgement, upload or capture failure,
timeout, cancellation, task failure, and partial setup. A cleanup failure does
not replace the customer result or thrown value, but it prevents a successful
telemetry status and fails the staging gate.

The wrapper reports `acknowledged` only after the cloud service confirms the
expected bundle digest. A local pending spool is not considered safe in an
isolated Trigger worker. Capture source removal is transferred to the uploader
only after complete spool admission, and final attempt cleanup happens only
after strict uploader shutdown has joined processing and released spool
ownership.

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
used by normal completion and waits for it. It first gives the cooperative task
up to the smaller of five seconds or the capture shutdown budget to settle, so
Tavi's attempt provider can flush and shut down before Arcus seals the bundle.
The settlement wait and active finalization work share one monotonic 25 second
cutoff. The wrapper reserves uploader teardown time before starting upload.
A cooperative task that settles within the deadline records a cancelled
outcome and can upload. If task cleanup does not settle before the deadline,
the bundle reports `capture_failed` and is not uploaded. The wrapper still
preserves the exact value later thrown by the task. A forced worker stop cannot
run cleanup. The example does not claim evidence delivery after a forced stop.
Use these exact deadlines:

```ts
{
  shutdownDeadlineMs: 10_000,
  uploadDeadlineMs: 10_000,
  finalizationDeadlineMs: 25_000,
}
```

The cutoff covers cooperative settlement, capture shutdown, validation, spool
admission, and upload. At the cutoff the uploader prevents new requests,
aborts active requests, and the wrapper waits for uploader termination and
attempt-directory cleanup before reporting `timed_out` or completing the
cancellation hook. No upload or attempt-state mutation may continue after that
report. Teardown is cooperative: an injected Fetch implementation must honor
the supplied `AbortSignal`. A teardown overrun fails staging but still does not
replace the customer result or error. A forced hard deadline would require a
separate killable worker or process.

The tests also run a parent ralph loop and child search loop through fresh
OpenTelemetry sources. The parent passes the orchestrator rule. The child
passes the rich agent rule. Both receive an upload acknowledgement and join
through protected task and execution IDs.

## OpenClaw relation

Normal Slack requests use OpenClaw's trusted native run ID as the research ID.
OpenClaw creates the ID before `before_model_resolve`. Arcus receives the exact
ID in the trusted hook context and uses it as the protected task ID. A direct
`chat.send` caller can still use the optional Gateway binding described in the
[cross-service correlation contract](../../packages/openclaw/README.md#trusted-cross-system-correlation).

Tavi must move the native run ID into `dispatch_search` through trusted runtime
state. Do not read or accept the research ID from the prompt, model output, or
tool parameters. The smallest Tavi change uses `before_tool_call`, which has
the exact native run ID and tool call ID. The tool execution receives the same
tool call ID.

```ts
const researchIds = createOpenClawToolCorrelationBridge();
registerOpenClawToolCorrelation(api, researchIds);

const dispatchSearch = {
  name: 'dispatch_search',
  async execute(toolCallId, modelParams, signal) {
    return await dispatchSearchWithTrustedCorrelation({
      bridge: researchIds,
      toolCallId,
      modelParams,
      signal,
      dispatch: deepPeople.dispatchSearch,
    });
  },
};
```

The helper removes model supplied correlation fields before dispatch. When no
trusted binding is available, it omits `client_request_id` and keeps the
customer research fail-open. The bridge in `openclaw-tool-correlation.ts` is
bounded, expires stale entries, rejects conflicting bindings, and removes an
entry after use. If Tavi already exposes the trusted run ID directly in its
tool execution context, pass that value as `client_request_id` and omit the
bridge.

Deep People forwards `client_request_id` unchanged to Trigger as `researchId`.
The OpenClaw VM and Trigger installation use separate ingestion keys and
installation IDs. They use the same customer identity key, so Arcus writes the
same protected task token in the OpenClaw, orchestrator, and worker bundles.
OpenClaw also keeps its native run ID as normal structural run evidence. The
protected task token is the value used to join bundles across the three
systems.

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

6. Run two enrolled attempts and one unenrolled attempt at the same time with
   one OpenTelemetry 1.25 provider per attempt. Latitude must receive every
   expected span. Each Arcus bundle must contain only its own attempt, and the
   unenrolled attempt must create no Arcus state.

7. Run normal failure, application timeout, cooperative cancellation, retry,
   replay, parallel attempts, and forced upload failure. Telemetry must not
   change the task result or exact thrown value.

8. Search logs for the ingest key, identity key, customer content, local paths,
   bundle digests, and upload response bodies. None may be present.

9. Send one synthetic Slack message through the noncustomer staging Slack
   installation. It must start one OpenClaw run, one orchestrator attempt, and
   at least one worker attempt. All three bundles must contain the same
   protected task ID. Latitude must receive the same OpenClaw and Trigger spans
   as its baseline run. A direct `chat.send` request does not satisfy this
   check.

Return only safe run IDs, attempt numbers, completion state, time, Latitude
received status, Arcus received status, and the safe Arcus delivery status.
Do not send customer content, trace contents, tool data, or credentials.

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
