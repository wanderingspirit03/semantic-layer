export type JsonPrimitive = null | boolean | number | string;
export type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export const LOSS_REASONS = [
  'credential_redaction', 'configured_redaction', 'scrubber_failure_payload_omitted',
  'serialization_failure', 'unsafe_getter_avoided', 'unsafe_helper_avoided',
  'size_overflow_blobbed', 'size_overflow_discarded', 'blob_scan_blocked',
  'queue_backpressure_drop', 'unsupported_native_value', 'source_rejection',
  'filter_limit_exclusion', 'missing_parent_context', 'parser_error_malformed_bytes',
  'missing_correlation_identity', 'crash_recovery', 'uncertain_tail',
  'shutdown_timeout', 'turn_order_ambiguous',
  'persistence_failure',
] as const;
export type LossReason = typeof LOSS_REASONS[number];
export type SourceEventKind = 'lifecycle' | 'model' | 'tool' | 'state' | 'log' | 'error' | 'stream' | 'correlation' | 'unknown';
export type EventKind = SourceEventKind | 'loss';
export type EventPhase = 'start' | 'event' | 'end' | 'error' | 'cancelled' | 'gap';

export type SourceMetadata = {
  name: string;
  seam: string;
  identityDomain: string;
  coverage: readonly CoverageClaim[];
  version?: string;
  official?: boolean;
  qualification?: SourceQualification;
};

export type SourceQualification = Readonly<{
  status: 'exact_qualified' | 'capability_checked_unqualified' | 'unknown';
  profile?: string;
}>;

export type CoverageClaim = { operation: string; domain: string; role?: 'owner' | 'evidence' };
export type CoverageKey = Readonly<{ operation: string; domain: string }>;
export type SourceOwnershipRule = Readonly<{
  action: 'promote';
  source: string;
  operation: string;
  domain: string;
}>;
export type SourceOwnership = Readonly<{
  namespace?: string;
  rules?: readonly SourceOwnershipRule[];
}>;
export type TraceIdentity = Readonly<{
  runId: string;
  traceId: string;
  /** Runtime-owned discriminator for concurrent source operations sharing one SDK trace. */
  operationId?: string;
}>;
export type AcceptedReceipt = { accepted: true; recordId: string; settled: Promise<void> };
export type RejectedReceipt = { accepted: false; reason: string; settled: Promise<void> };
export type AdmissionReceipt = AcceptedReceipt | RejectedReceipt;
export type OpenTraceReceipt = (AcceptedReceipt & { identity: TraceIdentity }) | RejectedReceipt;

export type RunCorrelationInput = Readonly<{
  taskId: string;
  execution: Readonly<{
    system: string;
    runId: string;
    parentRunId?: string;
    rootRunId?: string;
    attempt?: number;
  }>;
}>;

export type OpenTraceRecord = {
  name: string;
  coverage?: CoverageKey;
  nativeIdentity?: string;
  conversationId?: string;
  turnId?: string;
  turnIndex?: number;
  previousTurnId?: string;
  parentContext?: ParentContext;
  correlation?: RunCorrelationInput;
  native?: unknown;
  /** Exact semantic authority for this lifecycle root, when the source seam guarantees it. */
  semantic?: Record<string, unknown>;
};

export type SourceRecord = {
  kind: SourceEventKind;
  phase: EventPhase;
  name: string;
  trace: TraceIdentity;
  native: unknown;
  /** Runtime-only identity of the exact caught error object. Never persisted. */
  errorIdentity?: object;
  coverage?: CoverageKey;
  nativeIdentity?: string;
  parentRecordId?: string;
  semantic?: Record<string, unknown>;
};

export interface SourceSink {
  openTrace(input: OpenTraceRecord): OpenTraceReceipt;
  record(input: SourceRecord): AdmissionReceipt;
}

export interface SourceLifecycle {
  deactivate(): void | Promise<void>;
  drain(): void | Promise<void>;
}

export interface CaptureSource {
  metadata: SourceMetadata;
  install(sink: SourceSink): SourceLifecycle;
}

export type ObservationOptions = {
  conversationId?: string;
  turnId?: string;
  turnIndex?: number;
  previousTurnId?: string;
  /** W3C context received from a caller or process boundary. Active OTel is joined automatically. */
  parentContext?: ParentContext;
  /** Exact cross-process identities. Values are HMAC protected before persistence. */
  correlation?: RunCorrelationInput;
  /** An application-owned signal whose aborted state proves cooperative cancellation. */
  cancellationSignal?: Readonly<{ aborted: boolean }>;
  input?: unknown;
  metadata?: Record<string, unknown>;
};

export type ToolObservationOptions = {
  /** Exact tool-call identity exposed by the model or agent runtime. */
  callId?: string;
};

export type ParentContext = Readonly<{
  traceparent?: string;
  /** Emit an explicit observability gap if neither supplied nor active OTel context is valid. */
  required?: boolean;
}>;

export interface ObservationScope {
  readonly traceId: string;
  readonly active: boolean;
  emit(name: string, value?: unknown): AdmissionReceipt;
  tool<Input, Output>(
    name: string,
    input: Input,
    run: (input: Input) => Output | Promise<Output>,
    options?: ToolObservationOptions,
  ): Promise<Output>;
  turn<T>(name: string, options: ObservationOptions, run: (scope: ObservationScope) => T | Promise<T>): Promise<T>;
}

export type CaptureStatus = {
  state: 'accepting' | 'closing' | 'closed';
  runId: string;
  artifactPath: string;
  admitted: number;
  persisted: number;
  rejected: number;
  losses: Record<string, number>;
  activeSources: SourceMetadata[];
  queue: {
    capacityBytes: number;
    controlReserveBytes: number;
    pendingBytes: number;
    pendingControlBytes: number;
    highWaterBytes: number;
    coalescedGaps: number;
  };
  lastError: string | null;
};

export type InitializeOptions = {
  output?: string;
  serviceName: string;
  /** Stable random managed-installation identity. Supplying it emits manifest v2. */
  installationId?: string;
  secretValues?: readonly string[];
  identityMode?: 'hashed' | 'raw';
  identityKey?: string | Uint8Array;
  shutdownDeadlineMs?: number;
  queueCapacityBytes?: number;
  sourceOwnership?: SourceOwnership;
};

export interface CaptureHandle {
  instrument(input: { adapter: CaptureSource | { createSource(client: object): CaptureSource }; client: object }): SourceLifecycle;
  installSource(source: CaptureSource): SourceLifecycle;
  observe<T>(name: string, options: ObservationOptions, run: (scope: ObservationScope) => T | Promise<T>): Promise<Awaited<T>>;
  tool<Input, Output>(
    name: string,
    input: Input,
    run: (input: Input) => Output | Promise<Output>,
    options?: ToolObservationOptions,
  ): Promise<Output>;
  emit(name: string, value?: unknown): AdmissionReceipt;
  status(): CaptureStatus;
  flush(): Promise<CaptureStatus>;
  shutdown(): Promise<CaptureStatus>;
}

export type { SemanticCaptureEventV1 } from './generated.js';
