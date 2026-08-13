import { chmod, mkdir, mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import {
  createCapture,
  validateArtifact,
  type OpenTelemetrySource,
} from 'semantic-layer-capture';
import { createCloudUploader, type CloudUploader } from 'semantic-layer-cloud';
import type { TaviTriggerCancellation } from './trigger-cancellation.js';

const COOPERATIVE_CANCELLATION_SETTLE_MS = 5_000;
const UPLOADER_TEARDOWN_RESERVE_MS = 1_000;

export type TrustedTaviTenantConfig = Readonly<{
  serviceName: string;
  endpoint: string;
  ingestKey: string;
  installationId: string;
  /** Stable private key used only to correlate hashed Trigger identities. */
  identityKey: string;
  uploadDeadlineMs?: number;
  shutdownDeadlineMs?: number;
  /** Total capture, validation, upload, and shutdown budget. Defaults to 25 seconds. */
  finalizationDeadlineMs?: number;
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
}>;

export type TaviSuccessfulAttemptProfile = 'orchestrator' | 'rich-agent';

export type TaviTriggerAttemptOptions<T> = Readonly<{
  /** Null means the caller's trusted tenant lookup did not enable capture. */
  tenant: TrustedTaviTenantConfig | null;
  trigger: TaviTriggerIdentity;
  /** Defaults to rich-agent. Orchestrators require only a completed invoke-agent root. */
  successfulAttemptProfile?: TaviSuccessfulAttemptProfile;
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
    settle(() => options.cancellation?.completeTelemetry());
    await reportSafely(options.reportDelivery, { status: 'not_captured' });
    try {
      return await options.task({ signal }) as Awaited<T>;
    } finally {
      settle(() => options.cancellation?.release());
    }
  }

  const tenant = options.tenant;
  let capture: ReturnType<typeof createCapture> | undefined;
  let attemptDirectory: string | undefined;
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
      shutdownDeadlineMs: positiveDeadline(tenant.shutdownDeadlineMs, 10_000),
    });
    if (options.openTelemetry) {
      capture.installSource(options.openTelemetry.source);
    }
  } catch {
    await settleCapture(capture);
    if (attemptDirectory) await removeAttemptDirectory(attemptDirectory);
    settle(() => options.cancellation?.completeTelemetry());
    await reportSafely(options.reportDelivery, { status: 'capture_failed' });
    try {
      return await options.task({ signal }) as Awaited<T>;
    } finally {
      settle(() => options.cancellation?.release());
    }
  }

  let taskOutcome: 'pending' | 'succeeded' | 'failed' = 'pending';
  let finalization: Promise<void> | undefined;
  let cancellationExpiresAt: number | undefined;
  const finalizationDeadlineMs = positiveDeadline(
    tenant.finalizationDeadlineMs,
    25_000,
  );
  const finalize = (expiresAt = cancellationExpiresAt
    ?? performance.now() + finalizationDeadlineMs): Promise<void> => {
    finalization ??= finalizeWithinDeadline({
      capture: capture!,
      attemptDirectory: attemptDirectory!,
      tenant,
      trigger: options.trigger,
      successfulAttemptProfile: options.successfulAttemptProfile ?? 'rich-agent',
      successful: () => taskOutcome === 'succeeded',
      expiresAt,
    }).then(async (delivery) => {
      const cleaned = await removeAttemptDirectory(attemptDirectory!);
      const reported = !cleaned && delivery.status === 'acknowledged'
        ? { status: 'capture_failed' } as const
        : performance.now() >= expiresAt
          ? { status: 'timed_out' } as const
          : delivery;
      await reportSafely(options.reportDelivery, reported);
    }).finally(() => {
      settle(() => options.cancellation?.completeTelemetry());
    });
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
    async () => await options.task({ signal }),
  );
  void observed.then(
    () => { taskOutcome = 'succeeded'; },
    () => { taskOutcome = 'failed'; },
  );

  const onAbort = () => {
    cancellationExpiresAt ??= performance.now() + finalizationDeadlineMs;
    const settleBudgetMs = Math.min(
      COOPERATIVE_CANCELLATION_SETTLE_MS,
      positiveDeadline(options.tenant?.shutdownDeadlineMs, 10_000),
      Math.max(0, cancellationExpiresAt - performance.now()),
    );
    void waitForSettlement(observed, settleBudgetMs)
      .then(async () => await finalize(cancellationExpiresAt));
  };
  signal?.addEventListener('abort', onAbort, { once: true });
  if (signal?.aborted) onAbort();
  try {
    return await observed;
  } finally {
    signal?.removeEventListener('abort', onAbort);
    await finalize();
    settle(() => options.cancellation?.release());
  }
}

async function waitForSettlement(
  promise: Promise<unknown>,
  deadlineMs: number,
): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      promise.then(() => undefined, () => undefined),
      new Promise<void>((resolve) => {
        timer = setTimeout(resolve, deadlineMs);
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

type FinalizationControl = {
  expired: boolean;
  expiresAt: number;
  uploader?: CloudUploader;
};

async function finalizeWithinDeadline(input: {
  capture: ReturnType<typeof createCapture>;
  attemptDirectory: string;
  tenant: TrustedTaviTenantConfig;
  trigger: TaviTriggerIdentity;
  successfulAttemptProfile: TaviSuccessfulAttemptProfile;
  successful: () => boolean;
  expiresAt: number;
}): Promise<TaviTriggerDelivery> {
  const control: FinalizationControl = {
    expired: false,
    expiresAt: input.expiresAt,
  };
  let deadlineShutdown: Promise<void> | undefined;
  const timer = setTimeout(() => {
    control.expired = true;
    if (control.uploader) {
      deadlineShutdown = control.uploader.shutdown().catch(() => {});
    }
  }, Math.max(0, input.expiresAt - performance.now()));
  try {
    return await finalizeAttempt({
      ...input,
      control,
    });
  } catch {
    return activeWorkExpired(control)
      ? { status: 'timed_out' }
      : { status: 'capture_failed' };
  } finally {
    clearTimeout(timer);
    if (deadlineShutdown) await deadlineShutdown;
  }
}

async function finalizeAttempt(input: {
  capture: ReturnType<typeof createCapture>;
  attemptDirectory: string;
  tenant: TrustedTaviTenantConfig;
  trigger: TaviTriggerIdentity;
  successfulAttemptProfile: TaviSuccessfulAttemptProfile;
  successful: () => boolean;
  control: FinalizationControl;
}): Promise<TaviTriggerDelivery> {
  let captureHealthy = true;

  let artifactPath: string | undefined;
  try {
    const closed = await input.capture.shutdown();
    if (activeWorkExpired(input.control)) return { status: 'timed_out' };
    if (
      closed.state !== 'closed'
      || closed.rejected !== 0
      || closed.lastError !== null
    ) {
      captureHealthy = false;
    } else {
      artifactPath = closed.artifactPath;
      const successfulProfile = input.successful()
        ? input.successfulAttemptProfile
        : undefined;
      const validation = await validateArtifact(artifactPath, {
        profile: successfulProfile === 'rich-agent' ? 'rich-agent' : 'structural',
        ...(successfulProfile === 'orchestrator'
          ? {
              requiredEvidence: ['root', 'delivery'] as const,
              requiredSourceActivity: ['generic:otel'] as const,
            }
          : {}),
        secretValues: [input.tenant.ingestKey, input.tenant.identityKey],
      });
      if (activeWorkExpired(input.control)) return { status: 'timed_out' };
      if (!validation.valid) captureHealthy = false;
      if (
        successfulProfile === 'orchestrator'
        && !await validOrchestratorArtifact(artifactPath, input.trigger)
      ) captureHealthy = false;
      if ((closed.losses.missing_correlation_identity ?? 0) !== 0) {
        captureHealthy = false;
      }
    }
  } catch {
    captureHealthy = false;
  }

  if (!captureHealthy || !artifactPath) {
    return { status: 'capture_failed' };
  }

  let uploader: CloudUploader | undefined;
  let uploaderShutdownHealthy = true;
  let delivery: TaviTriggerDelivery = { status: 'upload_failed' };
  try {
    if (activeWorkExpired(input.control)) return { status: 'timed_out' };
    uploader = createCloudUploader({
      endpoint: input.tenant.endpoint,
      ingestKey: input.tenant.ingestKey,
      installationId: input.tenant.installationId,
      spoolDirectory: join(input.attemptDirectory, 'spool'),
      concurrency: 1,
      ...(input.tenant.fetch ? { fetch: input.tenant.fetch } : {}),
    });
    input.control.uploader = uploader;
    const receipt = await uploader.enqueueArtifact(artifactPath, {
      removeSourceAfterAdmissionFrom: join(input.attemptDirectory, 'capture'),
    });
    if (activeWorkExpired(input.control)) return { status: 'timed_out' };
    const remainingMs = input.control.expiresAt
      - performance.now()
      - UPLOADER_TEARDOWN_RESERVE_MS;
    if (remainingMs <= 0) return { status: 'timed_out' };
    const flushed = await uploader.flush({
      deadlineMs: Math.min(
        positiveDeadline(input.tenant.uploadDeadlineMs, 10_000),
        remainingMs,
      ),
    });
    if (activeWorkExpired(input.control)) return { status: 'timed_out' };
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
      try { await uploader.shutdown(); } catch { uploaderShutdownHealthy = false; }
    }
    input.control.uploader = undefined;
  }
  if (!uploaderShutdownHealthy && delivery.status === 'acknowledged') {
    return { status: 'capture_failed' };
  }
  if (activeWorkExpired(input.control)) return { status: 'timed_out' };
  return delivery;
}

async function validOrchestratorArtifact(
  artifactPath: string,
  trigger: TaviTriggerIdentity,
): Promise<boolean> {
  try {
    const [manifestText, traceText] = await Promise.all([
      readFile(join(artifactPath, 'manifest.json'), 'utf8'),
      readFile(join(artifactPath, 'trace.jsonl'), 'utf8'),
    ]);
    const manifest = JSON.parse(manifestText) as {
      sources?: Array<{ id?: unknown; name?: unknown }>;
    };
    const rows = traceText.trim().split('\n').filter(Boolean)
      .map((line) => JSON.parse(line) as Record<string, any>);
    const otelSources = new Set((manifest.sources ?? [])
      .filter((source) => source.name === 'generic:otel' && typeof source.id === 'string')
      .map((source) => source.id as string));
    const root = rows.find((row) => row.kind === 'run.start'
      && row.data?.correlation?.execution?.system === 'trigger.dev'
      && row.data.correlation.execution.attempt === trigger.attemptNumber
      && /^task_[a-f0-9]{64}$/u.test(row.data.correlation.task_id ?? '')
      && /^exec_[a-f0-9]{64}$/u.test(row.data.correlation.execution.run_id ?? ''));
    if (!root) return false;
    const completed = rows.some((row) => row.kind === 'run.outcome'
      && row.parent === root.id
      && row.data?.status === 'completed');
    if (!completed) return false;
    return rows.some((start) => start.kind === 'scope'
      && otelSources.has(start.source)
      && start.parent === root.id
      && start.data?.type === 'step'
      && start.data?.phase === 'start'
      && rows.some((end) => end.kind === 'scope'
        && otelSources.has(end.source)
        && end.parent === start.id
        && end.data?.scope_id === start.data.scope_id
        && end.data?.phase === 'end'
        && end.data?.status === 'completed'));
  } catch {
    return false;
  }
}

function positiveDeadline(value: number | undefined, fallback: number): number {
  const requested = Number.isFinite(value) && Number(value) > 0
    ? Math.floor(Number(value))
    : fallback;
  return Math.max(1, Math.min(requested, fallback));
}

function safeRequestId(value: string | null): string | undefined {
  return value && /^[A-Za-z0-9._:-]{1,128}$/u.test(value) ? value : undefined;
}

function activeWorkExpired(control: FinalizationControl): boolean {
  return control.expired || performance.now() >= control.expiresAt;
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

async function removeAttemptDirectory(attemptDirectory: string): Promise<boolean> {
  try {
    await rm(attemptDirectory, { recursive: true, force: true });
    return true;
  } catch {
    return false;
  }
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
