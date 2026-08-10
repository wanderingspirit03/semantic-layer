import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash, createHmac, randomBytes } from 'node:crypto';
import { resolve } from 'node:path';
import {
  LocalArtifact, type ArtifactSource, type ArtifactSourceClass, type EventDraft,
} from './artifact.js';
import { safeSerialize } from './error-evidence.js';
import { resolveParentContext, type ResolvedParentContext } from './parent-context.js';
import { CredentialScanner } from './secret-scanner.js';
import {
  SourceOwnershipRegistry, type CoverageIdentity, type InstalledSource,
} from './source-ownership.js';
import type {
  AdmissionReceipt, CaptureHandle, CaptureSource, CaptureStatus, CoverageKey, InitializeOptions,
  ObservationOptions, ObservationScope, OpenTraceRecord, OpenTraceReceipt, SourceLifecycle,
  LossReason, RunCorrelationInput, SemanticCaptureEventV1, SourceMetadata, SourceRecord, SourceSink,
  ToolObservationOptions, TraceIdentity,
} from './types.js';

const MAX_TURN_ORDER_ENTRIES = 1024;
const MAX_OPEN_SOURCE_TRACES = 4096;
type ScopeContext = {
  trace: TraceIdentity;
  identities: TurnIdentity;
  parentRecordId?: string;
  traceparent?: string;
};
let singleton: { runtime: CaptureRuntime; handle: CaptureHandle } | undefined;
type TurnIdentity = {
  conversation_id?: string;
  turn_id?: string;
  turn_index?: number;
  previous_turn_id?: string;
};
type ProtectedRunCorrelation = NonNullable<SemanticCaptureEventV1['run_correlation']>;
type CaptureInputFault = {
  field: string;
  error: unknown;
  affectedPath?: string;
  lossReason?: LossReason;
};
type ObservationSnapshot = { options: ObservationOptions; faults: CaptureInputFault[] };
type OpenTraceSnapshot = { input: OpenTraceRecord; faults: CaptureInputFault[] };
type SourceRecordSnapshot = {
  input?: SourceRecord;
  errorIdentity?: object;
  trace?: TraceIdentity;
  operationId?: string;
  identityShapeValid: boolean;
  faults: CaptureInputFault[];
};
type RecordAdmissionOptions = Readonly<{
  allowDuringClosing?: boolean;
}>;

class CaptureRuntime {
  private readonly artifact: LocalArtifact;
  private readonly ambientScope = new AsyncLocalStorage<ObservationScope>();
  private readonly scopeContexts = new WeakMap<ObservationScope, ScopeContext>();
  private readonly lifecycles: Array<{ source: InstalledSource; lifecycle: SourceLifecycle }> = [];
  private readonly identityKey: Uint8Array;
  private readonly ownership: SourceOwnershipRegistry;
  private readonly activeChildren = new Set<Promise<unknown>>();
  private readonly sourceErrorIdentities = new Map<string, WeakSet<object>>();
  private readonly conversationOrder = new Map<string, { turnId: string; index: number }>();
  private readonly turnIndexes = new Map<string, number>();
  private readonly openSourceTraces = new Map<string, {
    metadata: SourceMetadata;
    identity: TraceIdentity;
    identities: TurnIdentity;
    name: string;
    startRecordId: string;
    nativeIdentity?: string;
    coverage?: CoverageIdentity;
    traceparent?: string;
    manualRoot: boolean;
  }>();
  private state: 'accepting' | 'closing' | 'closed' = 'accepting';
  private sourceAdmissionOpen = true;
  private shutdownPromise?: Promise<CaptureStatus>;
  private readonly deadlineMs: number;
  private readonly identityMode: 'hashed' | 'raw';
  private readonly compatibility: string;

  constructor(options: InitializeOptions) {
    if (!options.serviceName?.trim()) throw new TypeError('serviceName is required');
    validateInstallationId(options.installationId);
    const queueCapacityBytes = options.queueCapacityBytes ?? 64 * 1024 * 1024;
    if (queueCapacityBytes !== 64 * 1024 * 1024) throw new TypeError('queue capacity is fixed at 64 MiB');
    this.deadlineMs = options.shutdownDeadlineMs ?? 10_000;
    if (!Number.isSafeInteger(this.deadlineMs) || this.deadlineMs < 1 || this.deadlineMs > 60_000) {
      throw new TypeError('shutdownDeadlineMs must be between 1 and 60000');
    }
    this.identityMode = options.identityMode ?? 'hashed';
    this.identityKey = identityKey(options.identityKey);
    this.ownership = new SourceOwnershipRegistry(options.serviceName, options.sourceOwnership, this.identityKey);
    this.compatibility = JSON.stringify({
      output: resolve(options.output ?? '.semantic-layer/traces'),
      serviceName: options.serviceName,
      installationId: options.installationId,
      identityMode: this.identityMode,
      shutdownDeadlineMs: this.deadlineMs,
      queueCapacityBytes,
      secretDigest: secretDigest(options.secretValues),
      identityKeyDigest: identityKeyOptionDigest(options.identityKey),
      sourceOwnershipDigest: this.ownership.compatibilityDigest,
    });
    this.artifact = new LocalArtifact({
      output: resolve(options.output ?? '.semantic-layer/traces'),
      serviceName: options.serviceName,
      installationId: options.installationId,
      language: 'typescript',
      runtimeVersion: process.version,
      scanner: new CredentialScanner(options.secretValues),
      queueCapacityBytes,
      ownershipManifest: () => this.ownership.manifest(),
    });
    this.artifact.registerSource(manualArtifactSource);
    this.artifact.registerSource(runtimeArtifactSource);
    for (const finding of this.artifact.recoveryFindings) {
      const traceId = id('trace');
      this.artifact.recordLoss('crash_recovery', traceId, undefined, `/prior_runs/${finding.run}`);
      if (finding.uncertainTail) this.artifact.recordLoss('uncertain_tail', traceId, undefined, `/prior_runs/${finding.run}/trace.jsonl`);
    }
  }

  handle(): CaptureHandle {
    return Object.freeze({
      instrument: (input: { adapter: CaptureSource | { createSource(client: object): CaptureSource }; client: object }) => this.instrument(input),
      installSource: (source: CaptureSource) => this.installSource(source),
      observe: <T>(name: string, options: ObservationOptions, run: (scope: ObservationScope) => T | Promise<T>) => this.observe(name, options, run),
      tool: <Input, Output>(
        name: string,
        input: Input,
        run: (input: Input) => Output | Promise<Output>,
        options?: ToolObservationOptions,
      ) => this.tool(name, input, run, options),
      emit: (name: string, value?: unknown) => this.emit(name, value),
      status: () => this.status(),
      flush: () => this.flush(),
      shutdown: () => this.shutdown(),
    });
  }

  accepts(options: InitializeOptions): boolean {
    return this.compatibility === JSON.stringify({
      output: resolve(options.output ?? '.semantic-layer/traces'),
      serviceName: options.serviceName,
      installationId: options.installationId,
      identityMode: options.identityMode ?? 'hashed',
      shutdownDeadlineMs: options.shutdownDeadlineMs ?? 10_000,
      queueCapacityBytes: options.queueCapacityBytes ?? 64 * 1024 * 1024,
      secretDigest: secretDigest(options.secretValues),
      identityKeyDigest: identityKeyOptionDigest(options.identityKey),
      sourceOwnershipDigest: new SourceOwnershipRegistry(
        options.serviceName, options.sourceOwnership, this.identityKey,
      ).compatibilityDigest,
    });
  }

  instrument(input: { adapter: CaptureSource | { createSource(client: object): CaptureSource }; client: object }): SourceLifecycle {
    const source = 'install' in input.adapter ? input.adapter : input.adapter.createSource(input.client);
    return this.installSource(source);
  }

  installSource(source: CaptureSource): SourceLifecycle {
    if (this.state !== 'accepting') throw new Error('capture is not accepting sources');
    validateMetadata(source.metadata);
    const registration = this.ownership.register(source);
    const installed = registration.source;
    const metadata = installed.metadata;
    // Sources may synchronously submit evidence from install(). Declare that evidence first,
    // including when installation subsequently fails and the original error is rethrown.
    if (!registration.reused) this.artifact.registerSource(toArtifactSource(installed));
    let lifecycle: SourceLifecycle;
    try {
      lifecycle = source.install(this.sourceSink(installed));
    } catch (error) {
      this.artifact.failSourceInstallation(installed.sourceId);
      throw error;
    }
    if (!lifecycle || typeof lifecycle.deactivate !== 'function' || typeof lifecycle.drain !== 'function') {
      this.artifact.failSourceInstallation(installed.sourceId);
      throw new TypeError('source install must return deactivate and drain');
    }
    this.ownership.activate(installed);
    this.artifact.activateSource(installed.sourceId);
    this.lifecycles.push({ source: installed, lifecycle });
    return lifecycle;
  }

  async observe<T>(
    name: string,
    options: ObservationOptions,
    run: (scope: ObservationScope) => T | Promise<T>,
  ): Promise<Awaited<T>> {
    return await this.trackChild(this.runObservation(name, options, run));
  }

  private async runObservation<T>(
    name: string,
    options: ObservationOptions,
    run: (scope: ObservationScope) => T | Promise<T>,
  ): Promise<Awaited<T>> {
    if (this.state !== 'accepting') return await run(inertScope()) as Awaited<T>;
    const snapshot = snapshotObservationOptions(options);
    const trace: TraceIdentity = Object.freeze({ runId: this.artifact.runId, traceId: id('trace') });
    const identity = this.turnIdentity(snapshot.options);
    const parentContext = resolvedSnapshotParent(snapshot, undefined);
    const start = this.record(manualMetadata, {
      kind: 'lifecycle', phase: 'start', name, trace,
      native: { metadata: snapshot.options.metadata ?? {} },
      semantic: {
        type: 'agent.run',
        name,
        input: snapshot.options.input ?? null,
      },
      runCorrelation: this.protectRunCorrelation(snapshot.options.correlation),
      traceparent: parentContext.traceparent,
    }, identity);
    this.recordInputFaults(snapshot.faults, trace, identity, start.accepted ? start.recordId : undefined, parentContext.traceparent);
    this.recordContextGap(parentContext, trace, identity, start.accepted ? start.recordId : undefined);
    this.recordTurnConflict(snapshot.options, identity, trace.traceId, start.accepted ? start.recordId : undefined);
    if (snapshot.options.previousTurnId && (snapshot.options.turnIndex === undefined || snapshot.options.turnIndex === 0)) {
      this.artifact.recordLoss('turn_order_ambiguous', trace.traceId);
    }
    let active = true;
    const scope = this.createScope(
      trace,
      identity,
      () => active,
      start.accepted ? start.recordId : undefined,
      parentContext.traceparent,
    );
    try {
      const result = await this.ambientScope.run(scope, run, scope);
      active = false;
      this.sourceErrorIdentities.delete(trace.traceId);
      this.record(manualMetadata, {
        kind: 'lifecycle', phase: 'end', name, trace, native: null,
        semantic: { type: 'agent.run', status: 'succeeded', output: result ?? null },
        ...(start.accepted ? { parentRecordId: start.recordId } : {}),
        traceparent: parentContext.traceparent,
      }, identity, { allowDuringClosing: true });
      return result as Awaited<T>;
    } catch (error) {
      active = false;
      const cancelled = cancellationWasObserved(snapshot.options.cancellationSignal);
      const sourceErrors = this.sourceErrorIdentities.get(trace.traceId);
      const sourceOwnsError = isErrorIdentity(error) && sourceErrors?.has(error) === true;
      this.sourceErrorIdentities.delete(trace.traceId);
      if (!sourceOwnsError) {
        this.record(manualMetadata, {
          kind: 'error', phase: 'error', name, trace, native: { error },
          semantic: { type: 'agent.error' },
          ...(start.accepted ? { parentRecordId: start.recordId } : {}),
          traceparent: parentContext.traceparent,
        }, identity, { allowDuringClosing: true });
      }
      this.record(manualMetadata, {
        kind: 'lifecycle', phase: cancelled ? 'cancelled' : 'error', name, trace, native: null,
        semantic: { type: 'agent.run', status: cancelled ? 'cancelled' : 'failed' },
        ...(start.accepted ? { parentRecordId: start.recordId } : {}),
        traceparent: parentContext.traceparent,
      }, identity, { allowDuringClosing: true });
      throw error;
    }
  }

  async tool<Input, Output>(
    name: string,
    input: Input,
    run: (input: Input) => Output | Promise<Output>,
    options?: ToolObservationOptions,
  ): Promise<Output> {
    const scope = this.ambientScope.getStore();
    return scope ? scope.tool(name, input, run, options) : await run(input) as Output;
  }

  emit(name: string, value?: unknown): AdmissionReceipt {
    const scope = this.ambientScope.getStore();
    return scope ? scope.emit(name, value) : rejected('no_active_observation');
  }

  status(): CaptureStatus { return this.artifact.status(); }
  flush(): Promise<CaptureStatus> { return this.artifact.flush(); }

  shutdown(): Promise<CaptureStatus> {
    try {
      this.ownership.assertPolicyResolved();
    } catch (error) {
      return Promise.reject(error);
    }
    this.shutdownPromise ??= this.runShutdown();
    return this.shutdownPromise;
  }

  private async runShutdown(): Promise<CaptureStatus> {
    this.state = 'closing';
    this.artifact.beginClosing();
    const deadline = Date.now() + this.deadlineMs;
    for (const { source, lifecycle } of [...this.lifecycles].reverse()) {
      const outcome = await settleBefore(Promise.resolve().then(() => lifecycle.deactivate()), deadline);
      this.artifact.sourceDeactivated(source.sourceId, outcome === 'settled' ? 'complete' : 'failed');
      if (outcome === 'timeout') {
        this.artifact.recordLoss('shutdown_timeout', id('trace'));
        this.artifact.degrade('shutdown deadline expired during source deactivation');
        continue;
      }
      if (outcome === 'failed') this.recordTeardownFailure();
    }
    for (const { source, lifecycle } of [...this.lifecycles].reverse()) {
      const outcome = await settleBefore(Promise.resolve().then(() => lifecycle.drain()), deadline);
      this.artifact.sourceDrained(source.sourceId, outcome === 'settled' ? 'complete' : 'failed');
      if (outcome === 'timeout') {
        this.artifact.recordLoss('shutdown_timeout', id('trace'));
        this.artifact.degrade('shutdown deadline expired during source drain');
        continue;
      }
      if (outcome === 'failed') this.recordTeardownFailure();
    }
    while (this.activeChildren.size > 0) {
      const outcome = await settleBefore(Promise.allSettled([...this.activeChildren]), deadline);
      if (outcome === 'timeout') {
        this.artifact.recordLoss('shutdown_timeout', id('trace'));
        this.artifact.degrade('shutdown deadline expired with active child operations');
        break;
      }
    }
    this.sourceAdmissionOpen = false;
    for (const open of this.openSourceTraces.values()) {
      this.record(open.metadata, {
        kind: 'lifecycle', phase: 'cancelled', name: open.name, trace: open.identity,
        native: { shutdown_cancelled: true }, parentRecordId: open.startRecordId,
        ...(open.nativeIdentity !== undefined ? { nativeIdentity: open.nativeIdentity } : {}),
        contractCoverage: open.coverage,
      }, open.identities, { allowDuringClosing: true });
    }
    this.openSourceTraces.clear();
    if (await this.artifact.prepareOwnershipFinalization()) this.ownership.resetEvidence();
    await this.artifact.flush();
    const decisions = this.ownership.freeze();
    await this.artifact.flush();
    for (const decision of decisions) {
      if (decision.status !== 'ambiguous') continue;
      await this.artifact.recordLoss(
        'source_rejection',
        id('trace'),
        undefined,
        '/coverage/ownership/ambiguous',
      ).settled;
    }
    if (await this.artifact.prepareOwnershipFinalization()) {
      this.ownership.resetAfterPersistenceRecovery();
      this.ownership.freeze();
      await this.artifact.flush();
    }
    this.ownership.finalize();
    const result = await this.artifact.seal();
    this.turnIndexes.clear();
    this.conversationOrder.clear();
    this.state = result.state === 'closed' ? 'closed' : 'closing';
    return result;
  }

  private sourceSink(source: InstalledSource): SourceSink {
    const { metadata } = source;
    return Object.freeze({
      openTrace: (input: OpenTraceRecord): OpenTraceReceipt => {
        if (!this.sourceAdmissionOpen || this.state !== 'accepting') return rejected('source_frozen');
        if (this.openSourceTraces.size >= MAX_OPEN_SOURCE_TRACES) {
          const loss = this.artifact.recordLoss(
            'source_rejection',
            id('trace'),
            undefined,
            '/open_source_traces/capacity',
          );
          this.artifact.degrade('open source trace capacity reached');
          return rejected('source_capacity', loss.settled);
        }
        const snapshot = snapshotOpenTraceRecord(input);
        const captured = snapshot.input;
        const scope = this.ambientScope.getStore();
        const ambient = scope?.active ? this.scopeContexts.get(scope) : undefined;
        const parentContext = resolvedOpenTraceParent(snapshot, ambient?.traceparent);
        const identity = Object.freeze({
          runId: this.artifact.runId,
          traceId: ambient?.trace.traceId ?? id('trace'),
          operationId: id('operation'),
        });
        const identities = ambient?.identities ?? this.turnIdentity(captured);
        const candidateCoverage = snapshot.faults.some((fault) => fault.field === 'coverage')
          ? undefined
          : this.ownership.coverageIdentity(source, captured.nativeIdentity, captured.coverage);
        const reservation = this.ownership.reserve(source, candidateCoverage);
        if (reservation.overflow) this.artifact.recordOwnershipLimit(reservation.overflow);
        const coverage = reservation.coverage;
        const receipt = this.record(metadata, {
          kind: 'lifecycle', phase: 'start', name: captured.name, trace: identity,
          nativeIdentity: captured.nativeIdentity, native: captured.native ?? null,
          semantic: captured.semantic,
          runCorrelation: this.protectRunCorrelation(captured.correlation),
          contractCoverage: coverage,
          ...(ambient?.parentRecordId ? { parentRecordId: ambient.parentRecordId } : {}),
          traceparent: parentContext.traceparent,
        }, identities);
        reservation.settle(receipt.accepted);
        this.recordInputFaults(snapshot.faults, identity, identities, receipt.accepted ? receipt.recordId : undefined, parentContext.traceparent);
        this.recordContextGap(parentContext, identity, identities, receipt.accepted ? receipt.recordId : undefined);
        if (receipt.accepted) this.openSourceTraces.set(sourceOperationKey(identity), {
          metadata, identity, identities, name: captured.name, startRecordId: receipt.recordId,
          manualRoot: ambient !== undefined,
          ...(captured.nativeIdentity !== undefined
            ? { nativeIdentity: captured.nativeIdentity }
            : {}),
          ...(coverage ? { coverage } : {}),
          ...(parentContext.traceparent ? { traceparent: parentContext.traceparent } : {}),
        });
        return receipt.accepted ? { ...receipt, identity } : receipt;
      },
      record: (input: SourceRecord) => {
        if (!this.sourceAdmissionOpen) return rejected('source_frozen');
        const snapshot = snapshotSourceRecord(input);
        const captured = snapshot.input;
        if (!captured) {
          const evidence = this.sourceIdentityEvidence(
            metadata, snapshot.trace, snapshot.operationId,
          );
          const issued = snapshot.identityShapeValid && snapshot.trace
            ? this.openSourceTraces.get(sourceOperationKey(snapshot.trace))
            : undefined;
          const identityAccepted = issued?.metadata === metadata
            && snapshot.trace !== undefined
            && sameTraceIdentity(issued.identity, snapshot.trace);
          const trace = evidence?.identity ?? runtimeTraceIdentity(this.artifact.runId);
          const settlements = [this.recordInputFaults(
            snapshot.faults, trace, evidence?.identities ?? {},
            evidence?.startRecordId, evidence?.traceparent,
          )];
          if (!identityAccepted) {
            const reason = !snapshot.identityShapeValid || !snapshot.trace
              ? 'invalid_identity_shape'
              : snapshot.trace.runId === this.artifact.runId
                ? 'unissued_or_closed_trace_identity'
                : 'foreign_run_identity';
            settlements.push(this.recordSourceIdentityRejection(
              snapshot.trace, snapshot.operationId, undefined, reason, evidence, trace,
            ));
          }
          const settled = Promise.all(settlements).then(() => undefined);
          return rejected('invalid_record', settled);
        }
        const key = sourceOperationKey(captured.trace);
        const candidate = this.openSourceTraces.get(key);
        const open = snapshot.identityShapeValid
          && candidate?.metadata === metadata
          && sameTraceIdentity(candidate.identity, captured.trace)
          ? candidate
          : undefined;
        if (!open) {
          const evidence = this.sourceIdentityEvidence(metadata, captured.trace);
          const reason = captured.trace.runId === this.artifact.runId
            ? 'invalid_record'
            : 'foreign_run_identity';
          const rejectionReason = !snapshot.identityShapeValid
            ? 'invalid_identity_shape'
            : captured.trace.runId === this.artifact.runId
              ? 'unissued_or_closed_trace_identity'
              : 'foreign_run_identity';
          const trace = evidence?.identity ?? runtimeTraceIdentity(this.artifact.runId);
          const settled = Promise.all([
            this.recordSourceIdentityRejection(
              captured.trace, captured.trace.operationId, captured,
              rejectionReason, evidence, trace,
            ),
            this.recordInputFaults(
              snapshot.faults,
              trace,
              evidence?.identities ?? {},
              evidence?.startRecordId,
              evidence?.traceparent,
            ),
          ]).then(() => undefined);
          return rejected(reason, settled);
        }
        const candidateCoverage = snapshot.faults.some((fault) => fault.field === 'coverage')
          ? undefined
          : captured.coverage
            ? captured.nativeIdentity
              ? this.ownership.coverageIdentity(source, captured.nativeIdentity, captured.coverage)
              : open?.coverage && sameCoverage(open.coverage, this.ownership.coverage(source, captured.coverage))
              ? open.coverage
              : undefined
            : open?.coverage
              ?? this.ownership.coverageIdentity(source, captured.nativeIdentity, undefined);
        const reservation = this.ownership.reserve(source, candidateCoverage);
        if (reservation.overflow) this.artifact.recordOwnershipLimit(reservation.overflow);
        const coverage = reservation.coverage;
        const receipt = this.record(metadata, {
          ...captured,
          contractCoverage: coverage,
          ...(open && !captured.parentRecordId ? { parentRecordId: open.startRecordId } : {}),
          ...(open?.traceparent ? { traceparent: open.traceparent } : {}),
        }, open?.identities ?? {}, { allowDuringClosing: true });
        reservation.settle(receipt.accepted);
        if (
          receipt.accepted
          && open.manualRoot
          && captured.kind === 'error'
          && hasStructuredSemanticError(captured.semantic)
          && snapshot.errorIdentity
        ) {
          let identities = this.sourceErrorIdentities.get(captured.trace.traceId);
          if (!identities) {
            identities = new WeakSet<object>();
            this.sourceErrorIdentities.set(captured.trace.traceId, identities);
          }
          identities.add(snapshot.errorIdentity);
        }
        this.recordInputFaults(snapshot.faults, captured.trace, open?.identities ?? {}, receipt.accepted ? receipt.recordId : undefined, open?.traceparent);
        const terminalParent = captured.parentRecordId ?? open.startRecordId;
        if (
          captured.kind === 'lifecycle'
          && ['end', 'error', 'cancelled'].includes(captured.phase)
          && terminalParent === open.startRecordId
        ) {
          this.openSourceTraces.delete(key);
        }
        return receipt;
      },
    });
  }

  private createScope(
    trace: TraceIdentity,
    identities: TurnIdentity,
    isActive: () => boolean,
    parentRecordId?: string,
    traceparent?: string,
    manualCallIds = new Set<string>(),
  ): ObservationScope {
    const scope: ObservationScope = {
      traceId: trace.traceId,
      get active() { return isActive(); },
      emit: (eventName, value) => {
        if (isActive()) {
          const semantic = manualSemantic(eventName, value);
          return this.record(manualMetadata, {
            kind: eventKind(eventName), phase: 'event', name: eventName, trace,
            ...(semantic ? { semantic } : {}),
            native: semantic?.type === 'state.transition' ? null : value ?? null,
            ...(parentRecordId ? { parentRecordId } : {}),
            traceparent,
          }, identities);
        }
        this.artifact.recordLoss('source_rejection', trace.traceId);
        return rejected('scope_closed');
      },
      tool: <Input, Output>(
        toolName: string,
        input: Input,
        execute: (input: Input) => Output | Promise<Output>,
        options?: ToolObservationOptions,
      ) => {
        if (!isActive()) {
          this.artifact.recordLoss('source_rejection', trace.traceId);
          return Promise.resolve(execute(input)) as Promise<Output>;
        }
        const callIdFaults: CaptureInputFault[] = [];
        const observedCallId = readCaptureValue(options ?? {}, 'callId', callIdFaults);
        const requestedCallId = observedCallId.ok ? observedCallId.value : undefined;
        const requestedNativeIdentity = manualCallIdentity(requestedCallId);
        const duplicateCallId = requestedNativeIdentity !== undefined
          && manualCallIds.has(requestedNativeIdentity);
        const nativeIdentity = duplicateCallId ? undefined : requestedNativeIdentity;
        if (!observedCallId.ok || (requestedCallId !== undefined && !nativeIdentity)) {
          const reason = duplicateCallId ? 'duplicate_call_id' : 'invalid_call_id';
          this.record(manualMetadata, {
            kind: 'unknown',
            phase: 'gap',
            name: 'manual.tool.identity',
            trace,
            native: observedCallId.ok
              ? { call_id: requestedCallId }
              : { reason: 'call_id_unreadable' },
            semantic: {
              type: 'capture.gap',
              reason,
              detail: duplicateCallId
                ? 'The supplied manual tool call ID was reused; local correlation was used.'
                : 'The supplied manual tool call ID was invalid; local correlation was used.',
            },
            ...(parentRecordId ? { parentRecordId } : {}),
            traceparent,
          }, identities);
        }
        if (requestedNativeIdentity !== undefined && !duplicateCallId) {
          manualCallIds.add(requestedNativeIdentity);
        }
        const start = this.record(manualMetadata, {
          kind: 'tool', phase: 'start', name: toolName, trace, native: null,
          semantic: { type: 'tool.execution', name: toolName, input },
          ...(nativeIdentity ? { nativeIdentity } : {}),
          ...(parentRecordId ? { parentRecordId } : {}),
          traceparent,
        }, identities);
        const operation = (async (): Promise<Output> => {
          try {
            const output = await execute(input) as Output;
            this.record(manualMetadata, {
              kind: 'tool', phase: 'end', name: toolName, trace, native: null,
              semantic: { type: 'tool.result', status: 'succeeded', output: output ?? null },
              ...(nativeIdentity ? { nativeIdentity } : {}),
              ...(start.accepted ? { parentRecordId: start.recordId } : {}),
              traceparent,
            }, identities, { allowDuringClosing: true });
            return output;
          } catch (error) {
            this.record(manualMetadata, {
              kind: 'tool', phase: 'error', name: toolName, trace, native: { error },
              semantic: { type: 'tool.error', status: 'failed' },
              ...(nativeIdentity ? { nativeIdentity } : {}),
              ...(start.accepted ? { parentRecordId: start.recordId } : {}),
              traceparent,
            }, identities, { allowDuringClosing: true });
            throw error;
          }
        })();
        return this.trackChild(operation);
      },
      turn: <T>(turnName: string, options: ObservationOptions, run: (child: ObservationScope) => T | Promise<T>) => {
        if (!isActive()) return Promise.resolve(run(inertScope())) as Promise<T>;
        const operation = (async (): Promise<T> => {
          const snapshot = snapshotObservationOptions(options);
          const turnIdentities = this.turnIdentity(snapshot.options);
          const childContext = resolvedSnapshotParent(snapshot, traceparent);
          const start = this.record(manualMetadata, {
            kind: 'lifecycle', phase: 'start', name: turnName, trace,
            native: { input: snapshot.options.input ?? null, metadata: snapshot.options.metadata ?? {} },
            semantic: { type: 'scope', scope_type: 'turn', name: turnName },
            ...(parentRecordId ? { parentRecordId } : {}),
            traceparent: childContext.traceparent,
          }, turnIdentities);
          this.recordInputFaults(snapshot.faults, trace, turnIdentities, start.accepted ? start.recordId : undefined, childContext.traceparent);
          this.recordContextGap(childContext, trace, turnIdentities, start.accepted ? start.recordId : undefined);
          this.recordTurnConflict(snapshot.options, turnIdentities, trace.traceId, start.accepted ? start.recordId : undefined);
          let childActive = true;
          const child = this.createScope(
            trace,
            turnIdentities,
            () => childActive,
            start.accepted ? start.recordId : parentRecordId,
            childContext.traceparent,
            manualCallIds,
          );
          try {
            const result = await this.ambientScope.run(child, run, child);
            childActive = false;
            this.record(manualMetadata, {
              kind: 'lifecycle', phase: 'end', name: turnName, trace, native: null,
              semantic: { type: 'scope', scope_type: 'turn', status: 'succeeded' },
              ...(start.accepted ? { parentRecordId: start.recordId } : {}),
              traceparent: childContext.traceparent,
            }, turnIdentities, { allowDuringClosing: true });
            return result as T;
          } catch (error) {
            childActive = false;
            this.record(manualMetadata, {
              kind: 'lifecycle', phase: 'error', name: turnName, trace, native: { error },
              semantic: { type: 'scope', scope_type: 'turn', status: 'failed' },
              ...(start.accepted ? { parentRecordId: start.recordId } : {}),
              traceparent: childContext.traceparent,
            }, turnIdentities, { allowDuringClosing: true });
            throw error;
          }
        })();
        return this.trackChild(operation);
      },
    };
    this.scopeContexts.set(scope, {
      trace, identities,
      ...(parentRecordId ? { parentRecordId } : {}),
      ...(traceparent ? { traceparent } : {}),
    });
    return scope;
  }

  private trackChild<T>(operation: Promise<T>): Promise<T> {
    this.activeChildren.add(operation);
    void operation.then(
      () => this.activeChildren.delete(operation),
      () => this.activeChildren.delete(operation),
    );
    return operation;
  }

  private recordTeardownFailure(): void {
    this.artifact.degrade('source teardown failed during shutdown');
    this.artifact.recordLoss('source_rejection', id('trace'));
  }

  private record(
    metadata: SourceMetadata,
    input: SourceRecord & {
      traceparent?: string;
      contractCoverage?: CoverageIdentity;
      runCorrelation?: ProtectedRunCorrelation;
    },
    identities: TurnIdentity = {},
    admission: RecordAdmissionOptions = {},
  ): AdmissionReceipt {
    if (input.trace.runId !== this.artifact.runId) return rejected('foreign_run_identity');
    const native = safeSerialize(input.native);
    const semantic = safeSerialize(input.semantic ?? {});
    const scrubbedNative = this.artifact.scanner.scrub(native.value);
    const scrubbedSemantic = this.artifact.scanner.scrub(semantic.value);
    const blobs = this.artifact.prepareBlobs([...native.blobs, ...semantic.blobs]);
    const source = eventSource(metadata, this.ownership.source(metadata));
    const semanticValue = isJsonObject(scrubbedSemantic.value)
      ? scrubbedSemantic.value
      : { value: scrubbedSemantic.value };
    const receipt = this.artifact.admit({
      trace_id: input.trace.traceId,
      ...identities,
      ...(input.runCorrelation ? { run_correlation: input.runCorrelation } : {}),
      source,
      ...(input.contractCoverage ? { coverage: {
        operation: input.contractCoverage.operation,
        domain: input.contractCoverage.domain,
        identity_token: input.contractCoverage.identityToken,
      } } : {}),
      ...(input.nativeIdentity ? { native_identity: input.nativeIdentity } : {}),
      event_kind: input.kind,
      phase: input.phase,
      name: input.name,
      native: scrubbedNative.value,
      semantic: metadata === manualMetadata
        ? enrichManualSemantic(input, semanticValue, scrubbedNative.value)
        : semanticValue,
      correlation: {
        ...(input.parentRecordId ? { parent_record_id: input.parentRecordId } : {}),
        ...(input.traceparent ? { traceparent: input.traceparent } : {}),
      },
      loss_refs: [],
      blob_refs: blobs.refs,
    }, false, blobs.staged, admission.allowDuringClosing ?? false);
    if (receipt.accepted) {
      let nativeSnapshotResourceLimitRecorded = false;
      for (const loss of [...native.losses, ...semantic.losses]) {
        if (loss.nativeSnapshotResourceLimit) {
          if (nativeSnapshotResourceLimitRecorded) continue;
          nativeSnapshotResourceLimitRecorded = true;
        }
        this.artifact.recordLoss(loss.reason, input.trace.traceId, receipt.recordId, loss.path);
      }
      const redactionCount = scrubbedNative.redactions + scrubbedSemantic.redactions;
      if (redactionCount > 0) {
        this.artifact.recordLoss(
          'credential_redaction', input.trace.traceId, receipt.recordId,
          undefined, undefined, redactionCount,
        );
      }
      for (const blocked of blobs.blocked) {
        this.artifact.recordLoss('blob_scan_blocked', input.trace.traceId, receipt.recordId, blocked.path, blocked.bytes.byteLength);
      }
    }
    return receipt;
  }

  private recordContextGap(
    context: ResolvedParentContext,
    trace: TraceIdentity,
    identities: TurnIdentity,
    affectedRecordId?: string,
  ): void {
    if (!context.gap) return;
    const gap = this.record(runtimeMetadata, {
      kind: 'correlation', phase: 'gap', name: 'semantic_layer.context.gap', trace,
      native: { reason: context.gap, boundary: 'process', error: context.error ?? null },
      ...(affectedRecordId ? { parentRecordId: affectedRecordId } : {}),
      traceparent: context.traceparent,
    }, identities);
    this.artifact.recordLoss(
      'missing_parent_context',
      trace.traceId,
      gap.accepted ? gap.recordId : affectedRecordId,
    );
  }

  private recordInputFaults(
    faults: CaptureInputFault[],
    trace: TraceIdentity,
    identities: TurnIdentity,
    affectedRecordId?: string,
    traceparent?: string,
  ): Promise<void> {
    if (faults.length === 0) return Promise.resolve();
    const gap = this.record(runtimeMetadata, {
      kind: 'correlation', phase: 'gap', name: 'semantic_layer.capture_input.gap', trace,
      native: {
        fields: faults.map((fault) => fault.field),
        errors: faults.map((fault) => ({ field: fault.field, error: fault.error })),
      },
      ...(affectedRecordId ? { parentRecordId: affectedRecordId } : {}),
      traceparent,
    }, identities);
    const settlements = [gap.settled];
    const grouped = new Map<LossReason, number>();
    for (const fault of faults) {
      if (fault.lossReason) {
        grouped.set(fault.lossReason, (grouped.get(fault.lossReason) ?? 0) + 1);
        continue;
      }
      settlements.push(this.artifact.recordLoss(
        'serialization_failure',
        trace.traceId,
        gap.accepted ? gap.recordId : affectedRecordId,
        fault.affectedPath ?? `/capture_input/${fault.field}`,
      ).settled);
    }
    for (const [reason, count] of grouped) {
      settlements.push(this.artifact.recordLoss(
        reason,
        trace.traceId,
        gap.accepted ? gap.recordId : affectedRecordId,
        reason === 'missing_correlation_identity' ? '/run_correlation' : undefined,
        undefined,
        count,
      ).settled);
    }
    return Promise.all(settlements).then(() => undefined);
  }

  private sourceIdentityEvidence(
    metadata: SourceMetadata,
    submitted?: TraceIdentity,
    operationId?: string,
  ): (typeof this.openSourceTraces extends Map<string, infer Entry> ? Entry : never) | undefined {
    const key = operationId ?? (submitted ? sourceOperationKey(submitted) : undefined);
    const byOperation = key ? this.openSourceTraces.get(key) : undefined;
    if (byOperation?.metadata === metadata) return byOperation;
    if (!submitted) return undefined;
    for (const opened of this.openSourceTraces.values()) {
      if (opened.metadata === metadata && opened.identity.traceId === submitted.traceId) return opened;
    }
    return undefined;
  }

  private recordSourceIdentityRejection(
    submittedTrace: TraceIdentity | undefined,
    submittedOperationId: string | undefined,
    submitted: SourceRecord | undefined,
    reason: 'invalid_identity_shape' | 'unissued_or_closed_trace_identity' | 'foreign_run_identity',
    evidence?: (typeof this.openSourceTraces extends Map<string, infer Entry> ? Entry : never),
    trace: TraceIdentity = evidence?.identity ?? runtimeTraceIdentity(this.artifact.runId),
  ): Promise<void> {
    const gap = this.record(runtimeMetadata, {
      kind: 'correlation', phase: 'gap', name: 'semantic_layer.source_identity.gap', trace,
      native: {
        reason,
        submitted_identity: submittedTrace ?? null,
        ...(submittedOperationId ? { submitted_operation_id: submittedOperationId } : {}),
        ...(submitted ? { submitted_record: {
          kind: submitted.kind, phase: submitted.phase, name: submitted.name,
          ...(submitted.nativeIdentity ? { native_identity: submitted.nativeIdentity } : {}),
        } } : {}),
      },
      ...(evidence ? { parentRecordId: evidence.startRecordId } : {}),
      traceparent: evidence?.traceparent,
    }, evidence?.identities ?? {}, { allowDuringClosing: true });
    const loss = this.artifact.recordLoss(
      'source_rejection',
      trace.traceId,
      gap.accepted ? gap.recordId : evidence?.startRecordId,
      '/trace',
    );
    return Promise.all([gap.settled, loss.settled]).then(() => undefined);
  }

  private turnIdentity(input: Pick<ObservationOptions, 'conversationId' | 'turnId' | 'turnIndex' | 'previousTurnId'>): TurnIdentity {
    return {
      ...(input.conversationId ? { conversation_id: this.identity('conversation', input.conversationId) } : {}),
      ...(input.turnId ? { turn_id: this.identity('turn', input.turnId) } : {}),
      ...(input.turnIndex === undefined ? {} : { turn_index: input.turnIndex }),
      ...(input.previousTurnId ? { previous_turn_id: this.identity('turn', input.previousTurnId) } : {}),
    };
  }

  private recordTurnConflict(
    input: Pick<ObservationOptions, 'conversationId' | 'turnId' | 'turnIndex' | 'previousTurnId'>,
    identities: TurnIdentity,
    traceId: string,
    affectedRecordId?: string,
  ): void {
    if (!identities.turn_id || input.turnIndex === undefined) return;
    const priorIndex = this.turnIndexes.get(identities.turn_id);
    const previous = identities.previous_turn_id;
    const conversation = identities.conversation_id ? this.conversationOrder.get(identities.conversation_id) : undefined;
    const conflict = previous === identities.turn_id
      || (priorIndex !== undefined && priorIndex !== input.turnIndex)
      || (conversation !== undefined && conversation.turnId !== identities.turn_id && input.turnIndex <= conversation.index)
      || (conversation !== undefined && previous !== undefined && previous !== conversation.turnId);
    const turnEvicted = setBoundedIndex(
      this.turnIndexes,
      identities.turn_id,
      input.turnIndex,
    );
    let conversationEvicted = false;
    if (identities.conversation_id && (!conversation || input.turnIndex >= conversation.index)) {
      conversationEvicted = setBoundedIndex(
        this.conversationOrder,
        identities.conversation_id,
        { turnId: identities.turn_id, index: input.turnIndex },
      );
    }
    if (conflict || turnEvicted || conversationEvicted) {
      this.artifact.recordLoss('turn_order_ambiguous', traceId, affectedRecordId);
    }
  }

  private identity(prefix: string, value: string): string {
    if (this.identityMode === 'raw') {
      const start = `${prefix}_`;
      const normalized = value.toLowerCase().replace(/[^a-z0-9._:-]/gu, '_');
      if (
        normalized === value
        && start.length + value.length >= 8
        && start.length + value.length <= 128
      ) {
        return `${start}${value}`;
      }
      const suffix = `_${createHash('sha256').update(value).digest('hex').slice(0, 16)}`;
      return `${start}${normalized.slice(0, 128 - start.length - suffix.length)}${suffix}`;
    }
    return `${prefix}_${createHmac('sha256', this.identityKey).update(value).digest('hex')}`;
  }

  private protectRunCorrelation(
    value: RunCorrelationInput | undefined,
  ): ProtectedRunCorrelation | undefined {
    if (!value) return undefined;
    const executionIdentity = (runId: string): string => `exec_${createHmac(
      'sha256',
      this.identityKey,
    ).update(value.execution.system).update('\0run\0').update(runId).digest('hex')}`;
    return {
      task_id: `task_${createHmac('sha256', this.identityKey)
        .update('task\0').update(value.taskId).digest('hex')}`,
      execution: {
        system: value.execution.system,
        run_id: executionIdentity(value.execution.runId),
        ...(value.execution.parentRunId
          ? { parent_run_id: executionIdentity(value.execution.parentRunId) }
          : {}),
        ...(value.execution.rootRunId
          ? { root_run_id: executionIdentity(value.execution.rootRunId) }
          : {}),
        ...(value.execution.attempt === undefined
          ? {}
          : { attempt: value.execution.attempt }),
      },
    };
  }
}

const manualMetadata: SourceMetadata = {
  name: 'manual', seam: 'observe/tool/emit', identityDomain: 'manual.operation', coverage: [],
  official: false, qualification: { status: 'exact_qualified' },
};
const runtimeMetadata: SourceMetadata = {
  name: 'semantic-layer-runtime', seam: 'capture-runtime', identityDomain: 'semantic-layer',
  coverage: [], official: true, qualification: { status: 'exact_qualified' },
};
const manualArtifactSource: ArtifactSource = {
  metadata: manualMetadata, sourceId: 'builtin/manual', sourceClass: 'builtin_manual',
};
const runtimeArtifactSource: ArtifactSource = {
  metadata: runtimeMetadata, sourceId: 'builtin/semantic-layer-runtime', sourceClass: 'builtin_runtime',
};

function toArtifactSource(source: InstalledSource): ArtifactSource {
  const sourceClass: ArtifactSourceClass = source.sourceClass === 'deep'
    ? 'framework_deep'
    : source.sourceClass;
  return { metadata: source.metadata, sourceId: source.sourceId, sourceClass };
}

function eventSource(
  metadata: SourceMetadata,
  installed: InstalledSource | undefined,
): EventDraft['source'] {
  const sourceId = metadata === manualMetadata
    ? 'builtin/manual'
    : metadata === runtimeMetadata
      ? 'builtin/semantic-layer-runtime'
      : installed?.sourceId;
  if (!sourceId) throw new Error(`capture source is not registered: ${metadata.name}`);
  return {
    source_id: sourceId,
    name: metadata.name,
    seam: metadata.seam,
    identity_domain: metadata.identityDomain,
    official: metadata.official ?? false,
    ...(metadata.version ? { version: metadata.version } : {}),
  };
}

function sameCoverage(left: CoverageKey, right: CoverageKey | undefined): boolean {
  return right !== undefined && left.operation === right.operation && left.domain === right.domain;
}

export function initialize(options: InitializeOptions): CaptureHandle {
  if (singleton) {
    if (!singleton.runtime.accepts(options)) throw new Error('initialize received options incompatible with the active capture runtime');
    return singleton.handle;
  }
  const runtime = new CaptureRuntime(options);
  const handle = runtime.handle();
  singleton = { runtime, handle };
  return handle;
}

export function createCapture(options: InitializeOptions): CaptureHandle {
  return new CaptureRuntime(options).handle();
}

export class SemanticLayer {
  static initialize(options: InitializeOptions): CaptureHandle { return initialize(options); }
}

export async function resetCaptureForTests(): Promise<void> {
  if (singleton) await singleton.runtime.shutdown();
  singleton = undefined;
}

function snapshotObservationOptions(input: unknown): ObservationSnapshot {
  const faults: CaptureInputFault[] = [];
  const options: ObservationOptions = {};
  assignOptionalString(options, 'conversationId', readCaptureValue(input, 'conversationId', faults), faults);
  assignOptionalString(options, 'turnId', readCaptureValue(input, 'turnId', faults), faults);
  assignTurnIndex(options, readCaptureValue(input, 'turnIndex', faults), faults);
  assignOptionalString(options, 'previousTurnId', readCaptureValue(input, 'previousTurnId', faults), faults);
  const observedInput = readCaptureValue(input, 'input', faults);
  if (observedInput.ok) options.input = observedInput.value;
  const metadata = readCaptureValue(input, 'metadata', faults);
  if (metadata.ok && metadata.value !== undefined) {
    if (metadata.value && typeof metadata.value === 'object' && !Array.isArray(metadata.value)) {
      options.metadata = metadata.value as Record<string, unknown>;
    } else {
      faults.push(invalidInput('metadata', 'must be an object'));
    }
  }
  const parentContext = readCaptureValue(input, 'parentContext', faults);
  if (parentContext.ok) options.parentContext = parentContext.value as ObservationOptions['parentContext'];
  const cancellationSignal = readCaptureValue(input, 'cancellationSignal', faults);
  if (cancellationSignal.ok && cancellationSignal.value !== undefined) {
    const aborted = readCaptureValue(cancellationSignal.value, 'aborted', faults);
    if (aborted.ok && typeof aborted.value === 'boolean') {
      options.cancellationSignal = cancellationSignal.value as Readonly<{ aborted: boolean }>;
    } else if (aborted.ok) {
      faults.push(invalidInput('cancellationSignal.aborted', 'must be a boolean'));
    }
  }
  const correlation = snapshotRunCorrelation(input, faults);
  if (correlation) options.correlation = correlation;
  return { options, faults };
}

function snapshotOpenTraceRecord(input: unknown): OpenTraceSnapshot {
  const observation = snapshotObservationIdentities(input);
  const captured: OpenTraceRecord = { name: 'source.unreadable', ...observation.options };
  const name = readCaptureValue(input, 'name', observation.faults);
  if (name.ok) {
    if (typeof name.value === 'string' && name.value.trim()) captured.name = name.value;
    else observation.faults.push(invalidInput('name', 'must be a non-empty string'));
  }
  const nativeIdentity = readCaptureValue(input, 'nativeIdentity', observation.faults);
  if (nativeIdentity.ok && nativeIdentity.value !== undefined) {
    if (typeof nativeIdentity.value === 'string' && nativeIdentity.value.trim()
      && codePointLength(nativeIdentity.value) <= 512) {
      captured.nativeIdentity = nativeIdentity.value;
    } else observation.faults.push(invalidInput(
      'nativeIdentity', 'must be a non-empty string of at most 512 characters',
    ));
  }
  const native = readCaptureValue(input, 'native', observation.faults);
  if (native.ok) captured.native = native.value;
  const semantic = readCaptureValue(input, 'semantic', observation.faults);
  if (semantic.ok && semantic.value !== undefined) {
    if (semantic.value && typeof semantic.value === 'object' && !Array.isArray(semantic.value)) {
      captured.semantic = semantic.value as Record<string, unknown>;
    } else observation.faults.push(invalidInput('semantic', 'must be an object'));
  }
  const coverage = snapshotCoverage(input, observation.faults);
  if (coverage) captured.coverage = coverage;
  const parentContext = readCaptureValue(input, 'parentContext', observation.faults);
  if (parentContext.ok) captured.parentContext = parentContext.value as OpenTraceRecord['parentContext'];
  return { input: captured, faults: observation.faults };
}

function snapshotSourceRecord(input: unknown): SourceRecordSnapshot {
  const faults: CaptureInputFault[] = [];
  const traceValue = readCaptureValue(input, 'trace', faults);
  const identityShapeValid = traceValue.ok && exactTraceIdentityShape(traceValue.value);
  const traceSnapshot = traceValue.ok
    ? snapshotTraceIdentity(traceValue.value, faults)
    : {};
  const trace = traceSnapshot.trace;
  const kindValue = readCaptureValue(input, 'kind', faults);
  const phaseValue = readCaptureValue(input, 'phase', faults);
  const nameValue = readCaptureValue(input, 'name', faults);
  const nativeValue = readCaptureValue(input, 'native', faults);
  const kinds = new Set(['lifecycle', 'model', 'tool', 'state', 'log', 'error', 'stream', 'correlation', 'unknown']);
  const phases = new Set(['start', 'event', 'end', 'error', 'cancelled', 'gap']);
  if (!kindValue.ok || typeof kindValue.value !== 'string' || !kinds.has(kindValue.value)) {
    faults.push(invalidInput(
      'event_kind', 'must be a source event kind; loss rows are runtime-owned', '/event_kind',
    ));
  }
  if (!phaseValue.ok || typeof phaseValue.value !== 'string' || !phases.has(phaseValue.value)) {
    faults.push(invalidInput('phase', 'must be a supported event phase'));
  }
  if (!nameValue.ok || typeof nameValue.value !== 'string' || !nameValue.value.trim()) {
    faults.push(invalidInput('name', 'must be a non-empty string'));
  }
  if (!trace || !kindValue.ok || !phaseValue.ok || !nameValue.ok
    || !kinds.has(String(kindValue.value)) || !phases.has(String(phaseValue.value))
    || typeof nameValue.value !== 'string' || !nameValue.value.trim()) {
    return {
      faults, identityShapeValid,
      ...(trace ? { trace } : {}),
      ...(traceSnapshot.operationId ? { operationId: traceSnapshot.operationId } : {}),
    };
  }
  const captured: SourceRecord = {
    trace,
    kind: kindValue.value as SourceRecord['kind'],
    phase: phaseValue.value as SourceRecord['phase'],
    name: nameValue.value,
    native: nativeValue.ok ? nativeValue.value : null,
  };
  const nativeIdentity = readCaptureValue(input, 'nativeIdentity', faults);
  if (nativeIdentity.ok && nativeIdentity.value !== undefined) {
    if (typeof nativeIdentity.value === 'string' && nativeIdentity.value.trim()
      && codePointLength(nativeIdentity.value) <= 512) {
      captured.nativeIdentity = nativeIdentity.value;
    } else faults.push(invalidInput(
      'nativeIdentity', 'must be a non-empty string of at most 512 characters',
    ));
  }
  const parentRecordId = readCaptureValue(input, 'parentRecordId', faults);
  assignOptionalString(captured, 'parentRecordId', parentRecordId, faults);
  const semantic = readCaptureValue(input, 'semantic', faults);
  if (semantic.ok && semantic.value !== undefined) {
    if (semantic.value && typeof semantic.value === 'object' && !Array.isArray(semantic.value)) {
      captured.semantic = semantic.value as Record<string, unknown>;
    } else faults.push(invalidInput('semantic', 'must be an object'));
  }
  const errorIdentity = readCaptureValue(input, 'errorIdentity', faults);
  let capturedErrorIdentity: object | undefined;
  if (errorIdentity.ok && errorIdentity.value !== undefined) {
    if (isErrorIdentity(errorIdentity.value)) capturedErrorIdentity = errorIdentity.value;
    else faults.push(invalidInput('errorIdentity', 'must be a non-null error object'));
  }
  const coverage = snapshotCoverage(input, faults);
  if (coverage) captured.coverage = coverage;
  return {
    input: captured, trace, identityShapeValid, faults,
    ...(capturedErrorIdentity ? { errorIdentity: capturedErrorIdentity } : {}),
    ...(traceSnapshot.operationId ? { operationId: traceSnapshot.operationId } : {}),
  };
}

function snapshotCoverage(
  input: unknown,
  faults: CaptureInputFault[],
): CoverageKey | undefined {
  const value = readCaptureValue(input, 'coverage', faults);
  if (!value.ok || value.value === undefined) return undefined;
  if (!value.value || typeof value.value !== 'object') {
    faults.push(invalidInput('coverage', 'must contain operation and domain strings'));
    return undefined;
  }
  const operation = readCaptureValue(value.value, 'operation', faults);
  const domain = readCaptureValue(value.value, 'domain', faults);
  if (!operation.ok || typeof operation.value !== 'string' || !operation.value.trim()
    || !domain.ok || typeof domain.value !== 'string' || !domain.value.trim()) {
    faults.push(invalidInput('coverage', 'must contain operation and domain strings'));
    return undefined;
  }
  return Object.freeze({ operation: operation.value, domain: domain.value });
}

function snapshotTraceIdentity(
  input: unknown,
  faults: CaptureInputFault[],
): { trace?: TraceIdentity; operationId?: string } {
  const runId = readCaptureValue(input, 'runId', faults);
  const traceId = readCaptureValue(input, 'traceId', faults);
  const operationId = readCaptureValue(input, 'operationId', faults);
  if (!runId.ok || typeof runId.value !== 'string') faults.push(invalidInput('trace.runId', 'must be a string'));
  if (!traceId.ok || typeof traceId.value !== 'string') faults.push(invalidInput('trace.traceId', 'must be a string'));
  const operationEvidence = operationId.ok && typeof operationId.value === 'string'
    ? operationId.value
    : undefined;
  if (!runId.ok || !traceId.ok || typeof runId.value !== 'string' || typeof traceId.value !== 'string') {
    return operationEvidence ? { operationId: operationEvidence } : {};
  }
  if (operationId.ok && operationId.value !== undefined && typeof operationId.value !== 'string') {
    faults.push(invalidInput('trace.operationId', 'must be a string'));
  }
  return {
    trace: Object.freeze({
      runId: runId.value,
      traceId: traceId.value,
      ...(operationEvidence ? { operationId: operationEvidence } : {}),
    }),
    ...(operationEvidence ? { operationId: operationEvidence } : {}),
  };
}

function exactTraceIdentityShape(input: unknown): boolean {
  if (!input || typeof input !== 'object') return false;
  try {
    const keys = Reflect.ownKeys(input);
    return keys.length === 3
      && keys.includes('runId')
      && keys.includes('traceId')
      && keys.includes('operationId');
  } catch {
    return false;
  }
}

function snapshotObservationIdentities(input: unknown): {
  options: Pick<
    ObservationOptions,
    'conversationId' | 'turnId' | 'turnIndex' | 'previousTurnId' | 'correlation'
  >;
  faults: CaptureInputFault[];
} {
  const faults: CaptureInputFault[] = [];
  const options: Pick<
    ObservationOptions,
    'conversationId' | 'turnId' | 'turnIndex' | 'previousTurnId' | 'correlation'
  > = {};
  assignOptionalString(options, 'conversationId', readCaptureValue(input, 'conversationId', faults), faults);
  assignOptionalString(options, 'turnId', readCaptureValue(input, 'turnId', faults), faults);
  assignTurnIndex(options, readCaptureValue(input, 'turnIndex', faults), faults);
  assignOptionalString(options, 'previousTurnId', readCaptureValue(input, 'previousTurnId', faults), faults);
  const correlation = snapshotRunCorrelation(input, faults);
  if (correlation) options.correlation = correlation;
  return { options, faults };
}

function snapshotRunCorrelation(
  input: unknown,
  faults: CaptureInputFault[],
): RunCorrelationInput | undefined {
  const observed = readCaptureValue(input, 'correlation', faults);
  if (!observed.ok || observed.value === undefined) return undefined;
  if (!observed.value || typeof observed.value !== 'object' || Array.isArray(observed.value)) {
    faults.push(invalidInput('correlation', 'must be an object'));
    return undefined;
  }
  const taskId = readCaptureValue(observed.value, 'taskId', faults);
  const execution = readCaptureValue(observed.value, 'execution', faults);
  const taskIdValue = taskId.ok && validCorrelationText(taskId.value)
    ? taskId.value
    : undefined;
  if (!taskIdValue) {
    faults.push(invalidInput(
      'correlation.taskId',
      'must be a non-empty string of at most 512 characters',
      undefined,
      'missing_correlation_identity',
    ));
  }
  if (!execution.ok || !execution.value || typeof execution.value !== 'object'
    || Array.isArray(execution.value)) {
    faults.push(invalidInput(
      'correlation.execution',
      'must be an object',
      undefined,
      'missing_correlation_identity',
    ));
    return undefined;
  }
  const system = readCaptureValue(execution.value, 'system', faults);
  const runId = readCaptureValue(execution.value, 'runId', faults);
  const parentRunId = readCaptureValue(execution.value, 'parentRunId', faults);
  const rootRunId = readCaptureValue(execution.value, 'rootRunId', faults);
  const attempt = readCaptureValue(execution.value, 'attempt', faults);
  const systemValue = system.ok && typeof system.value === 'string'
    && /^[a-z][a-z0-9._:-]{2,127}$/u.test(system.value)
    ? system.value
    : undefined;
  if (!systemValue) {
    faults.push(invalidInput(
      'correlation.execution.system',
      'must be a lowercase identifier of 3 to 128 characters',
    ));
  }
  const runIdValue = runId.ok && validCorrelationText(runId.value)
    ? runId.value
    : undefined;
  if (!runIdValue) {
    faults.push(invalidInput(
      'correlation.execution.runId',
      'must be a non-empty string of at most 512 characters',
      undefined,
      'missing_correlation_identity',
    ));
  }
  for (const [field, value] of [
    ['parentRunId', parentRunId],
    ['rootRunId', rootRunId],
  ] as const) {
    if (value.ok && value.value !== undefined && !validCorrelationText(value.value)) {
      faults.push(invalidInput(
        `correlation.execution.${field}`,
        'must be a non-empty string of at most 512 characters',
      ));
    }
  }
  const parentRunIdValue = parentRunId.ok && validCorrelationText(parentRunId.value)
    ? parentRunId.value
    : undefined;
  const rootRunIdValue = rootRunId.ok && validCorrelationText(rootRunId.value)
    ? rootRunId.value
    : undefined;
  const attemptValue = attempt.ok && Number.isSafeInteger(attempt.value)
    && Number(attempt.value) >= 0
    ? attempt.value as number
    : undefined;
  if (attempt.ok && attempt.value !== undefined && (
    !Number.isSafeInteger(attempt.value) || Number(attempt.value) < 0
  )) {
    faults.push(invalidInput(
      'correlation.execution.attempt',
      'must be a non-negative safe integer',
    ));
  }
  if (!taskIdValue
    || !systemValue
    || !runIdValue) {
    return undefined;
  }
  return {
    taskId: taskIdValue,
    execution: {
      system: systemValue,
      runId: runIdValue,
      ...(parentRunIdValue
        ? { parentRunId: parentRunIdValue }
        : {}),
      ...(rootRunIdValue
        ? { rootRunId: rootRunIdValue }
        : {}),
      ...(attemptValue !== undefined
        ? { attempt: attemptValue }
        : {}),
    },
  };
}

function validCorrelationText(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0
    && codePointLength(value) <= 512;
}

function cancellationWasObserved(
  signal: Readonly<{ aborted: boolean }> | undefined,
): boolean {
  if (!signal) return false;
  try {
    return signal.aborted === true;
  } catch {
    return false;
  }
}

function resolvedSnapshotParent(snapshot: ObservationSnapshot, inherited?: string): ResolvedParentContext {
  const fault = snapshot.faults.find((item) => item.field === 'parentContext');
  if (fault) return {
    ...(inherited ? { traceparent: inherited } : {}),
    gap: 'parent_context_unreadable', error: fault.error,
  };
  return resolveParentContext(snapshot.options.parentContext, inherited);
}

function resolvedOpenTraceParent(snapshot: OpenTraceSnapshot, inherited?: string): ResolvedParentContext {
  const fault = snapshot.faults.find((item) => item.field === 'parentContext');
  if (fault) return {
    ...(inherited ? { traceparent: inherited } : {}),
    gap: 'parent_context_unreadable', error: fault.error,
  };
  return resolveParentContext(snapshot.input.parentContext, inherited);
}

function readCaptureValue(
  input: unknown,
  field: string,
  faults: CaptureInputFault[],
): { ok: true; value: unknown } | { ok: false } {
  try {
    if (!input || typeof input !== 'object') throw new TypeError('capture input must be an object');
    return { ok: true, value: Reflect.get(input, field) };
  } catch (error) {
    faults.push({ field, error });
    return { ok: false };
  }
}

function assignOptionalString<T extends Record<string, unknown>>(
  target: T,
  field: keyof T & string,
  observed: { ok: true; value: unknown } | { ok: false },
  faults: CaptureInputFault[],
): void {
  if (!observed.ok || observed.value === undefined) return;
  if (typeof observed.value === 'string') target[field] = observed.value as T[keyof T & string];
  else faults.push(invalidInput(field, 'must be a string'));
}

function assignBoundedOptionalString<T extends Record<string, unknown>>(
  target: T,
  field: keyof T & string,
  observed: { ok: true; value: unknown } | { ok: false },
  faults: CaptureInputFault[],
  maxCodePoints: number,
): void {
  if (!observed.ok || observed.value === undefined) return;
  if (typeof observed.value === 'string' && codePointLength(observed.value) <= maxCodePoints) {
    target[field] = observed.value as T[keyof T & string];
  } else faults.push(invalidInput(field, `must be a string of at most ${maxCodePoints} characters`));
}

function assignTurnIndex<T extends { turnIndex?: number }>(
  target: T,
  observed: { ok: true; value: unknown } | { ok: false },
  faults: CaptureInputFault[],
): void {
  if (!observed.ok || observed.value === undefined) return;
  if (Number.isSafeInteger(observed.value) && Number(observed.value) >= 0) target.turnIndex = Number(observed.value);
  else faults.push(invalidInput('turnIndex', 'must be a non-negative safe integer'));
}

function invalidInput(
  field: string,
  requirement: string,
  affectedPath?: string,
  lossReason?: LossReason,
): CaptureInputFault {
  return {
    field,
    error: new TypeError(`${field} ${requirement}`),
    ...(affectedPath ? { affectedPath } : {}),
    ...(lossReason ? { lossReason } : {}),
  };
}

function codePointLength(value: string): number { return [...value].length; }

function validateMetadata(metadata: SourceMetadata): void {
  if (!metadata?.name || !metadata.seam || !metadata.identityDomain || !Array.isArray(metadata.coverage)) {
    throw new TypeError('source metadata requires name, seam, identityDomain, and coverage');
  }
}

function validateInstallationId(value: string | undefined): void {
  if (value !== undefined && !/^install_[A-Za-z0-9_-]{22,128}$/u.test(value)) {
    throw new TypeError(
      'installationId must start with install_ and contain 22 to 128 opaque characters',
    );
  }
}

function rejected(
  reason: string,
  settled: Promise<void> = Promise.resolve(),
): { accepted: false; reason: string; settled: Promise<void> } {
  return { accepted: false, reason, settled };
}

function inertScope(): ObservationScope {
  return {
    traceId: 'trace_inert00000000', active: false,
    emit: () => rejected('runtime_closed'),
    tool: async <Input, Output>(
      _name: string,
      input: Input,
      run: (input: Input) => Output | Promise<Output>,
      _options?: ToolObservationOptions,
    ) => await run(input) as Output,
    turn: async <T>(_name: string, _options: ObservationOptions, run: (scope: ObservationScope) => T | Promise<T>) => await run(inertScope()) as Awaited<T>,
  };
}

function id(prefix: string): string { return `${prefix}_${randomBytes(16).toString('hex')}`; }

function manualCallIdentity(value: unknown): string | undefined {
  return typeof value === 'string'
    && value.trim().length > 0
    && codePointLength(value) <= 256
    && !value.includes('\u0000')
    ? value
    : undefined;
}

function runtimeTraceIdentity(runId: string): TraceIdentity {
  return Object.freeze({ runId, traceId: id('trace') });
}

function sameTraceIdentity(left: TraceIdentity, right: TraceIdentity): boolean {
  return left.runId === right.runId
    && left.traceId === right.traceId
    && left.operationId === right.operationId;
}

function sourceOperationKey(identity: TraceIdentity): string {
  return identity.operationId ?? identity.traceId;
}

function eventKind(name: string): 'state' | 'log' | 'stream' | 'unknown' {
  if (name === 'stream.delta' || name === 'stream.terminal') return 'stream';
  if (name.startsWith('state.') || name.includes('.state')) return 'state';
  if (name.startsWith('log.') || name.includes('.log')) return 'log';
  return 'unknown';
}

function manualSemantic(name: string, value: unknown): Record<string, unknown> | undefined {
  if (name === 'stream.delta' || name === 'stream.terminal') return { type: name };
  if (name.startsWith('state.') || name.includes('.state')) {
    return {
      type: 'state.transition',
      state_type: manualStateType(name),
      value: value ?? null,
    };
  }
  return undefined;
}

function manualStateType(name: string): string {
  if (/^[a-z][a-z0-9._-]{2,127}$/.test(name)) return name;
  const normalized = name.toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '_')
    .replace(/^[^a-z]+/, '')
    .slice(0, 128);
  return /^[a-z][a-z0-9._-]{2,127}$/.test(normalized)
    ? normalized
    : 'state.update';
}

function enrichManualSemantic(
  input: Pick<SourceRecord, 'kind' | 'phase'>,
  semantic: EventDraft['semantic'],
  native: EventDraft['native'],
): EventDraft['semantic'] {
  const semanticType = typeof semantic.type === 'string' ? semantic.type : undefined;
  const hasError = semanticType === 'agent.error'
    || semanticType === 'tool.error'
    || (semanticType === 'scope' && input.phase === 'error');
  if (!hasError || !isJsonObject(native) || native.error === undefined) return semantic;
  return { ...semantic, error: normalizedError(native.error) };
}

function normalizedError(value: EventDraft['native']): EventDraft['semantic'] {
  const captured = isJsonObject(value) ? value : undefined;
  const message = typeof captured?.message === 'string'
    ? captured.message
    : typeof value === 'string'
      ? value
      : 'A non-Error value was thrown.';
  const code = typeof captured?.code === 'string' && captured.code.length
    ? [...captured.code].slice(0, 256).join('')
    : undefined;
  return {
    type: normalizedErrorType(captured?.name),
    message: [...message].slice(0, 4096).join(''),
    recoverable: false,
    ...(code ? { code } : {}),
  };
}

function hasStructuredSemanticError(value: SourceRecord['semantic']): boolean {
  if (!value || typeof value !== 'object') return false;
  const error = value.error as Record<string, unknown> | undefined;
  return Boolean(
    error
    && typeof error.type === 'string'
    && typeof error.message === 'string'
    && typeof error.recoverable === 'boolean',
  );
}

function isErrorIdentity(value: unknown): value is object {
  return typeof value === 'object' && value !== null;
}

function normalizedErrorType(value: unknown): string {
  if (typeof value !== 'string') return 'thrown_value';
  const normalized = value
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '_')
    .replace(/^[^a-z]+/, '')
    .slice(0, 128);
  return /^[a-z][a-z0-9._-]{2,127}$/.test(normalized)
    ? normalized
    : 'execution_error';
}

function isJsonObject(value: unknown): value is Record<string, EventDraft['native']> {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value));
}

function secretDigest(values: readonly string[] | undefined): string {
  const hash = createHash('sha256');
  for (const value of values ?? []) hash.update(String(Buffer.byteLength(value))).update(':').update(value).update('\0');
  return hash.digest('hex');
}

function setBoundedIndex<Key, Value>(
  index: Map<Key, Value>,
  key: Key,
  value: Value,
): boolean {
  if (index.has(key)) index.delete(key);
  index.set(key, value);
  if (index.size <= MAX_TURN_ORDER_ENTRIES) return false;
  const oldest = index.keys().next();
  if (!oldest.done) index.delete(oldest.value);
  return true;
}

async function settleBefore(value: Promise<unknown>, deadline: number): Promise<'settled' | 'failed' | 'timeout'> {
  const remaining = Math.max(0, deadline - Date.now());
  return await Promise.race([
    value.then(() => 'settled' as const, () => 'failed' as const),
    new Promise<'timeout'>((resolvePromise) => setTimeout(() => resolvePromise('timeout'), remaining)),
  ]);
}

function identityKey(value: string | Uint8Array | undefined): Uint8Array {
  if (value === undefined) return randomBytes(32);
  const bytes = typeof value === 'string' ? Buffer.from(value) : Uint8Array.from(value);
  if (bytes.byteLength < 16) throw new TypeError('identityKey must contain at least 16 bytes');
  return bytes;
}

function identityKeyOptionDigest(value: string | Uint8Array | undefined): string {
  if (value === undefined) return 'ephemeral';
  return createHash('sha256').update(typeof value === 'string' ? Buffer.from(value) : value).digest('hex');
}
