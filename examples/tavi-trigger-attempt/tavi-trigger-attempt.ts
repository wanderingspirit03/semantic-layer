import { chmod, mkdir, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  createCapture,
  validateArtifact,
  type OpenTelemetrySource,
} from 'semantic-layer-capture';
import { createCloudUploader, type CloudUploader } from 'semantic-layer-cloud';
import type { TaviOpenTelemetryAttemptRouter } from './otel-attempt-router.js';
import type { TaviTriggerCancellation } from './trigger-cancellation.js';

export type TrustedTaviTenantConfig = Readonly<{
  serviceName: string;
  endpoint: string;
  ingestKey: string;
  installationId: string;
  /** Stable private key used only to correlate hashed Trigger identities. */
  identityKey: string;
  uploadDeadlineMs?: number;
  shutdownDeadlineMs?: number;
  fetch?: typeof globalThis.fetch;
}>;

export type TaviTriggerIdentity = Readonly<{
  runId: string;
  parentRunId?: string;
  rootRunId?: string;
  attemptNumber: number;
  researchId: string;
  traceparent?: string;
}>;

export type TaviTriggerDelivery = Readonly<{
  status:
    | 'acknowledged'
    | 'timed_out'
    | 'capture_failed'
    | 'upload_failed'
    | 'not_captured';
  requestId?: string;
}>;

export type TaviOpenTelemetryAttachment = Readonly<{
  /** Create a fresh source for each Trigger attempt. */
  source: OpenTelemetrySource;
  /** Process-wide router already attached once to Tavi's existing provider. */
  router: TaviOpenTelemetryAttemptRouter;
}>;

export type TaviTriggerAttemptOptions<T> = Readonly<{
  /** Null means the caller's trusted tenant lookup did not enable capture. */
  tenant: TrustedTaviTenantConfig | null;
  trigger: TaviTriggerIdentity;
  openTelemetry?: TaviOpenTelemetryAttachment;
  /** Connects Trigger's task-local onCancel hook to bounded finalization. */
  cancellation?: TaviTriggerCancellation;
  signal?: AbortSignal;
  /** Defaults to the operating system temporary directory. */
  temporaryRoot?: string;
  /** Receives a deliberately small, log-safe delivery result. */
  /** Must persist synchronously. Any returned promise is ignored. */
  reportDelivery?(delivery: TaviTriggerDelivery): void;
  task(context: { signal?: AbortSignal }): T | Promise<T>;
}>;

/**
 * Run one Trigger attempt with fail-open Semantic Layer delivery.
 *
 * This handles cooperative completion and cancellation. A forced worker kill
 * cannot run `finally`, so it is intentionally outside this contract.
 */
export async function runTaviTriggerAttempt<T>(
  options: TaviTriggerAttemptOptions<T>,
): Promise<Awaited<T>> {
  const signal = options.cancellation?.signal ?? options.signal;
  if (options.tenant === null) {
    options.cancellation?.completeTelemetry();
    await reportSafely(options.reportDelivery, { status: 'not_captured' });
    try {
      return await options.task({ signal }) as Awaited<T>;
    } finally {
      options.cancellation?.release();
    }
  }

  const tenant = options.tenant;
  let capture: ReturnType<typeof createCapture> | undefined;
  let unregisterOpenTelemetry: (() => void) | undefined;
  let attemptDirectory: string;
  try {
    const temporaryRoot = options.temporaryRoot ?? tmpdir();
    await mkdir(temporaryRoot, { recursive: true, mode: 0o700 });
    attemptDirectory = await mkdtemp(join(temporaryRoot, 'semantic-layer-trigger-attempt-'));
    await chmod(attemptDirectory, 0o700);
    capture = createCapture({
      output: join(attemptDirectory, 'capture'),
      serviceName: tenant.serviceName,
      installationId: tenant.installationId,
      identityKey: tenant.identityKey,
      secretValues: [tenant.ingestKey, tenant.identityKey],
      shutdownDeadlineMs: tenant.shutdownDeadlineMs ?? 10_000,
    });
    if (options.openTelemetry) {
      capture.installSource(options.openTelemetry.source);
      unregisterOpenTelemetry = options.openTelemetry.router.registerSource(
        options.openTelemetry.source,
      );
    }
  } catch {
    settle(unregisterOpenTelemetry);
    await settleCapture(capture);
    options.cancellation?.completeTelemetry();
    await reportSafely(options.reportDelivery, { status: 'capture_failed' });
    try {
      return await options.task({ signal }) as Awaited<T>;
    } finally {
      options.cancellation?.release();
    }
  }

  let taskOutcome: 'pending' | 'succeeded' | 'failed' = 'pending';
  let finalization: Promise<void> | undefined;
  const finalize = (): Promise<void> => {
    finalization ??= finalizeAttempt({
      capture: capture!,
      unregisterOpenTelemetry,
      attemptDirectory,
      tenant,
      successful: () => taskOutcome === 'succeeded',
      reportDelivery: options.reportDelivery,
    }).finally(() => { options.cancellation?.completeTelemetry(); });
    return finalization;
  };

  const observed = capture.observe(
    'tavi.trigger.research_attempt',
    {
      parentContext: {
        ...(options.trigger.traceparent
          ? { traceparent: options.trigger.traceparent }
          : {}),
        required: options.trigger.parentRunId !== undefined,
      },
      correlation: {
        taskId: options.trigger.researchId,
        execution: {
          system: 'trigger.dev',
          runId: options.trigger.runId,
          ...(options.trigger.parentRunId
            ? { parentRunId: options.trigger.parentRunId }
            : {}),
          ...(options.trigger.rootRunId
            ? { rootRunId: options.trigger.rootRunId }
            : {}),
          attempt: options.trigger.attemptNumber,
        },
      },
      ...(signal ? { cancellationSignal: signal } : {}),
    },
    async () => options.openTelemetry
      ? await options.openTelemetry.router.runWithSource(
        options.openTelemetry.source,
        async () => await options.task({ signal }),
      )
      : await options.task({ signal }),
  );
  void observed.then(
    () => { taskOutcome = 'succeeded'; },
    () => { taskOutcome = 'failed'; },
  );

  const onAbort = () => {
    void finalize();
  };
  signal?.addEventListener('abort', onAbort, { once: true });
  if (signal?.aborted) onAbort();
  try {
    return await observed;
  } finally {
    signal?.removeEventListener('abort', onAbort);
    await finalize();
    options.cancellation?.release();
  }
}

async function finalizeAttempt(input: {
  capture: ReturnType<typeof createCapture>;
  unregisterOpenTelemetry?: () => void;
  attemptDirectory: string;
  tenant: TrustedTaviTenantConfig;
  successful: () => boolean;
  reportDelivery?: (delivery: TaviTriggerDelivery) => void;
}): Promise<void> {
  let captureHealthy = true;

  let artifactPath: string | undefined;
  try {
    const closed = await input.capture.shutdown();
    if (
      closed.state !== 'closed'
      || closed.rejected !== 0
      || closed.lastError !== null
    ) {
      captureHealthy = false;
    } else {
      artifactPath = closed.artifactPath;
      const validation = await validateArtifact(artifactPath, {
        profile: input.successful() ? 'rich-agent' : 'structural',
        secretValues: [input.tenant.ingestKey, input.tenant.identityKey],
      });
      if (!validation.valid) captureHealthy = false;
      if ((closed.losses.missing_correlation_identity ?? 0) !== 0) {
        captureHealthy = false;
      }
    }
  } catch {
    captureHealthy = false;
  } finally {
    settle(input.unregisterOpenTelemetry);
  }

  if (!captureHealthy || !artifactPath) {
    await reportSafely(input.reportDelivery, { status: 'capture_failed' });
    return;
  }

  let uploader: CloudUploader | undefined;
  let delivery: TaviTriggerDelivery = { status: 'upload_failed' };
  try {
    uploader = createCloudUploader({
      endpoint: input.tenant.endpoint,
      ingestKey: input.tenant.ingestKey,
      installationId: input.tenant.installationId,
      spoolDirectory: join(input.attemptDirectory, 'spool'),
      concurrency: 1,
      ...(input.tenant.fetch ? { fetch: input.tenant.fetch } : {}),
    });
    const receipt = await uploader.enqueueArtifact(artifactPath);
    const flushed = await uploader.flush({
      deadlineMs: input.tenant.uploadDeadlineMs ?? 10_000,
    });
    const acknowledged = receipt.state === 'acked' || (
      flushed.ackedBundles === 1
      && flushed.pendingBundles === 0
      && flushed.blockedBundles === 0
      && flushed.awaitingSpoolAdmissionBundles === 0
      && flushed.quarantineBundles === 0
    );
    const requestId = safeRequestId(flushed.lastRequestId);
    delivery = acknowledged
      ? { status: 'acknowledged', ...(requestId ? { requestId } : {}) }
      : flushed.timedOut
        ? { status: 'timed_out', ...(requestId ? { requestId } : {}) }
        : { status: 'upload_failed', ...(requestId ? { requestId } : {}) };
  } catch {
    delivery = { status: 'upload_failed' };
  } finally {
    if (uploader) {
      try { await uploader.shutdown(); } catch { /* fail open */ }
    }
  }
  await reportSafely(input.reportDelivery, delivery);
}

function safeRequestId(value: string | null): string | undefined {
  return value && /^[A-Za-z0-9._:-]{1,128}$/u.test(value) ? value : undefined;
}

function settle(action: (() => void) | undefined): void {
  if (!action) return;
  try { action(); } catch { /* fail open */ }
}

async function settleCapture(
  capture: ReturnType<typeof createCapture> | undefined,
): Promise<void> {
  if (!capture) return;
  try { await capture.shutdown(); } catch { /* fail open */ }
}

async function reportSafely(
  report: ((delivery: TaviTriggerDelivery) => void) | undefined,
  delivery: TaviTriggerDelivery,
): Promise<void> {
  if (!report) return;
  try {
    const result: unknown = report(delivery);
    if (result instanceof Promise) void result.catch(() => {});
  } catch { /* diagnostics never change task behavior */ }
}
