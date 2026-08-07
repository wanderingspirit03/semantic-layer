import { createHash, randomBytes } from 'node:crypto';
import {
  closeSync, createReadStream, existsSync, fsyncSync, lstatSync, mkdirSync, openSync,
  readFileSync, readdirSync, realpathSync, renameSync, unlinkSync, writeFileSync,
} from 'node:fs';
import { open as openFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import type { CapturedBlob } from './error-evidence.js';
import { CredentialScanner } from './secret-scanner.js';
import { SemanticProjector, type SemanticTraceRecord } from '../trace/semantic-projector.js';
import type {
  AdmissionReceipt, CaptureStatus, LossReason, SemanticCaptureEventV1, SourceMetadata,
} from './types.js';
import { assertNoSymbolicLinkComponents, secureOwnerOnly } from './permissions.js';
import type { OwnershipManifest } from './source-ownership.js';

const SDK_NAME = 'semantic-layer-capture';
const SDK_VERSION = '0.2.0-beta.1';
const CONTROL_RESERVE_BYTES = 64 * 1024;
const PROJECTION_OVERHEAD_BYTES = 2 * 1024;

export type ArtifactOptions = {
  output: string;
  serviceName: string;
  installationId?: string;
  language: 'typescript' | 'python';
  runtimeVersion: string;
  scanner: CredentialScanner;
  queueCapacityBytes: number;
  ownershipManifest: () => OwnershipManifest;
};

export type ArtifactSourceClass =
  | 'builtin_manual' | 'builtin_runtime' | 'framework_deep' | 'provider' | 'otel' | 'custom';
export type ArtifactSource = Readonly<{
  metadata: SourceMetadata;
  sourceId: string;
  sourceClass: ArtifactSourceClass;
}>;
type ArtifactSourceState = ArtifactSource & {
  lifecycle: {
    activation: 'pending' | 'active' | 'failed';
    deactivation: 'pending' | 'complete' | 'failed' | 'not_applicable';
    drain: 'pending' | 'complete' | 'failed' | 'not_applicable';
  };
};

export type EventDraft = {
  trace_id: string;
  conversation_id?: string;
  turn_id?: string;
  turn_index?: number;
  previous_turn_id?: string;
  source: SemanticCaptureEventV1['source'];
  coverage?: SemanticCaptureEventV1['coverage'];
  native_identity?: string;
  event_kind: SemanticCaptureEventV1['event_kind'];
  phase: SemanticCaptureEventV1['phase'];
  name: string;
  native: SemanticCaptureEventV1['native'];
  semantic: SemanticCaptureEventV1['semantic'];
  correlation: SemanticCaptureEventV1['correlation'];
  loss?: SemanticCaptureEventV1['loss'];
  loss_refs: string[];
  blob_refs: SemanticCaptureEventV1['blob_refs'];
};

export class LocalArtifact {
  readonly runId = id('run');
  readonly artifactPath: string;
  readonly scanner: CredentialScanner;
  readonly recoveryFindings: Array<{ run: string; uncertainTail: boolean }>;
  private readonly tracePath: string;
  private readonly manifestPath: string;
  private readonly lockPath: string;
  private readonly startedAt = new Date().toISOString();
  private sealedAt: string | undefined;
  private sealedTraceDigest: string | undefined;
  private projector = new SemanticProjector();
  private readonly persistedLosses = new Map<string, number>();
  private readonly sources = new Map<string, ArtifactSourceState>();
  private readonly activeSourceIds = new Set<string>();
  private writeChain: Promise<void> = Promise.resolve();
  private state: 'accepting' | 'closing' | 'closed' = 'accepting';
  private seq = 0;
  private rawSeq = 0;
  private bytes = 0;
  private admitted = 0;
  private persisted = 0;
  private rejected = 0;
  private readonly rejectionByReason = new Map<LossReason, number>();
  private pendingBytes = 0;
  private pendingControlBytes = 0;
  private highWaterBytes = 0;
  private coalescedGapCount = 0;
  private readonly coalesced = new Map<string, {
    draft: EventDraft;
    count: number;
    bytes: number;
    settled: Promise<void>;
    resolve: () => void;
  }>();
  private lastError: string | null = null;
  private persistenceFailed = false;
  private traceLosses = 0;

  constructor(private readonly options: ArtifactOptions) {
    this.scanner = options.scanner;
    const output = canonicalOutputPath(options.output);
    mkdirOwnerOnly(output);
    this.recoveryFindings = findStaleRuns(output);
    this.artifactPath = join(output, `run-${this.runId.slice(4)}`);
    mkdirOwnerOnly(this.artifactPath);
    this.tracePath = join(this.artifactPath, 'trace.jsonl');
    this.manifestPath = join(this.artifactPath, 'manifest.json');
    this.lockPath = join(this.artifactPath, '.writer.lock');
    const fd = openSync(this.lockPath, 'wx', 0o600);
    try {
      writeFileSync(fd, `${JSON.stringify({ pid: process.pid, run_id: this.runId, created_at: this.startedAt })}\n`);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    secureOwnerOnly(this.lockPath, 'file');
    writeFileSync(this.tracePath, '', { mode: 0o600 });
    secureOwnerOnly(this.tracePath, 'file');
    this.writeManifest('open');
  }

  registerSource(source: ArtifactSource, active = false): void {
    const builtin = source.sourceClass === 'builtin_manual' || source.sourceClass === 'builtin_runtime';
    this.sources.set(source.sourceId, {
      ...source,
      metadata: Object.freeze({ ...source.metadata, coverage: [...source.metadata.coverage] }),
      lifecycle: builtin
        ? { activation: 'active', deactivation: 'not_applicable', drain: 'not_applicable' }
        : { activation: active ? 'active' : 'pending', deactivation: 'pending', drain: 'pending' },
    });
    if (active || builtin) this.activeSourceIds.add(source.sourceId);
    this.writeManifest(this.state === 'accepting' ? 'open' : this.state === 'closing' ? 'closing' : 'sealed');
  }

  activateSource(sourceId: string): void {
    const source = this.sources.get(sourceId);
    if (!source) throw new Error(`source is not registered: ${sourceId}`);
    source.lifecycle.activation = 'active';
    if (source.lifecycle.deactivation === 'not_applicable') source.lifecycle.deactivation = 'pending';
    if (source.lifecycle.drain === 'not_applicable') source.lifecycle.drain = 'pending';
    this.activeSourceIds.add(sourceId);
  }

  failSourceInstallation(sourceId: string): void {
    const source = this.sources.get(sourceId);
    const active = this.activeSourceIds.has(sourceId);
    if (source && !active) {
      source.lifecycle.activation = 'failed';
      source.lifecycle.deactivation = 'not_applicable';
      source.lifecycle.drain = 'not_applicable';
    }
    if (!active) this.activeSourceIds.delete(sourceId);
    this.degrade('source installation failed');
    this.recordLoss('source_rejection', id('trace'));
  }

  sourceDeactivated(sourceId: string, outcome: 'complete' | 'failed'): void {
    const source = this.sources.get(sourceId);
    if (source && source.lifecycle.deactivation !== 'not_applicable'
      && (outcome === 'failed' || source.lifecycle.deactivation !== 'failed')) {
      source.lifecycle.deactivation = outcome;
    }
    this.activeSourceIds.delete(sourceId);
  }

  sourceDrained(sourceId: string, outcome: 'complete' | 'failed'): void {
    const source = this.sources.get(sourceId);
    if (source && source.lifecycle.drain !== 'not_applicable'
      && (outcome === 'failed' || source.lifecycle.drain !== 'failed')) {
      source.lifecycle.drain = outcome;
    }
  }

  recordOwnershipLimit(reason: 'group_limit'): void {
    this.degrade(`coverage ownership ${reason}`);
    this.recordLoss('source_rejection', id('trace'), undefined, '/coverage/ownership/group_limit');
  }

  degrade(message: string): void {
    this.lastError = message.slice(0, 1024);
  }

  prepareBlobs(blobs: readonly CapturedBlob[]): {
    refs: SemanticCaptureEventV1['blob_refs'];
    blocked: CapturedBlob[];
    staged: CapturedBlob[];
  } {
    const refs: SemanticCaptureEventV1['blob_refs'] = [];
    const blocked: CapturedBlob[] = [];
    const staged: CapturedBlob[] = [];
    const seen = new Set<string>();
    for (const blob of blobs) {
      if (seen.has(blob.digest)) continue;
      seen.add(blob.digest);
      const clean = this.scanner.scan(blob.bytes);
      refs.push({
        digest: blob.digest,
        algorithm: 'sha256',
        mime_type: blob.mimeType,
        byte_length: blob.bytes.byteLength,
        scan: clean ? 'clean' : 'blocked',
        inline_omitted: true,
        source_path: blob.path || '/',
      });
      if (!clean) {
        blocked.push(blob);
        continue;
      }
      staged.push(blob);
    }
    return { refs, blocked, staged };
  }

  admit(
    draft: EventDraft,
    control = false,
    stagedBlobs: readonly CapturedBlob[] = [],
    allowDuringClosing = false,
    strictControl = false,
    allowPayloadOmission = true,
  ): AdmissionReceipt {
    if (this.state !== 'accepting' && !((control || allowDuringClosing) && this.state === 'closing')) {
      return rejected('runtime_closed');
    }
    const row: SemanticCaptureEventV1 = {
      ...draft,
      schema: 'semantic_capture_event_v1',
      run_id: this.runId,
      record_id: id('record'),
      seq: this.rawSeq + 1,
      observed_at: new Date().toISOString(),
      monotonic_ns: Number(process.hrtime.bigint()),
      provenance: {
        language: this.options.language,
        sdk_name: SDK_NAME,
        sdk_version: SDK_VERSION,
        capture_policy: 'rich_local_credential_scrubbed' as const,
      },
    };
    const rawEncoded = Buffer.from(`${JSON.stringify(row)}\n`);
    if (!this.scanner.scan(rawEncoded)) {
      if (!allowPayloadOmission) {
        this.markRejected('scrubber_failure_payload_omitted');
        return rejected('fallback_failed');
      }
      this.projector.retireOmitted(row);
      const loss = this.recordLoss(
        'scrubber_failure_payload_omitted',
        draft.trace_id,
        undefined,
        blockedTopLevelPath(row, this.scanner),
        undefined,
        1,
      );
      return rejected(
        loss.accepted ? 'payload_omitted' : 'fallback_failed',
        loss.settled,
      );
    }
    // Projection is stateful, so capacity is reserved from a conservative upper bound
    // before project() can establish any parent/call correlation state.
    const projectionBytes = projectionReservation(rawEncoded.byteLength);
    const stagedBlobBytes = stagedBlobs.reduce(
      (total, blob) => total + blob.bytes.byteLength,
      0,
    );
    const reservedBytes = projectionBytes + stagedBlobBytes;
    const pendingDataBytes = this.pendingBytes - this.pendingControlBytes;
    if (!control && pendingDataBytes + reservedBytes > this.options.queueCapacityBytes) {
      this.markRejected('queue_backpressure_drop');
      const loss = this.recordLoss(
        'queue_backpressure_drop',
        draft.trace_id,
        undefined,
        undefined,
        rawEncoded.byteLength + stagedBlobBytes,
      );
      return rejected('queue_backpressure', loss.settled);
    }
    if (control && !strictControl && this.pendingControlBytes + reservedBytes > CONTROL_RESERVE_BYTES) {
      return this.coalesceControl(draft, rawEncoded.byteLength);
    }

    const projected = this.projector.project(row);
    const encoded = encodeProjected(projected);
    if (encoded.byteLength > projectionBytes) {
      // This is an internal invariant, not a backpressure rejection after mutation.
      // Recovering the bundle on flush/seal preserves a truthful durable gap.
      this.lastError = 'semantic projection exceeded its reserved capacity';
      this.persistenceFailed = true;
    }
    if (!this.scanner.scan(encoded)) {
      this.lastError = 'projected trace final secret scan failed';
      this.persistenceFailed = true;
    }

    this.rawSeq += 1;
    this.admitted += 1;
    if (projected.length === 0) {
      return { accepted: true, recordId: row.record_id, settled: Promise.resolve() };
    }
    this.pendingBytes += reservedBytes;
    if (control) this.pendingControlBytes += reservedBytes;
    this.highWaterBytes = Math.max(this.highWaterBytes, this.pendingBytes);

    let settle!: () => void;
    const settled = new Promise<void>((resolvePromise) => { settle = resolvePromise; });
    this.writeChain = this.writeChain.then(async () => {
      const createdBlobs: string[] = [];
      try {
        if (this.persistenceFailed) throw new Error('projection admission was not safe to persist');
        if (stagedBlobs.length) mkdirOwnerOnly(join(this.artifactPath, 'blobs'));
        for (const blob of stagedBlobs) {
          const destination = join(this.artifactPath, 'blobs', `${blob.digest}.blob`);
          if (existsSync(destination)) continue;
          const temporary = `${destination}.${row.record_id}.new`;
          const fd = openSync(temporary, 'wx', 0o600);
          try {
            writeFileSync(fd, blob.bytes);
            fsyncSync(fd);
          } finally {
            closeSync(fd);
          }
          renameSync(temporary, destination);
          secureOwnerOnly(destination, 'file');
          createdBlobs.push(destination);
        }
        if (createdBlobs.length) syncDirectory(join(this.artifactPath, 'blobs'));
        const handle = await openFile(this.tracePath, 'a', 0o600);
        try {
          await handle.write(encoded);
          await handle.sync();
        } finally {
          await handle.close();
        }
        this.persisted += projected.length;
        this.seq = projected.at(-1)?.seq ?? this.seq;
        this.traceLosses += projected.filter((record) => record.kind === 'loss').length;
        for (const [reason, count] of projectedLossCounts(projected)) {
          this.persistedLosses.set(
            reason,
            (this.persistedLosses.get(reason) ?? 0) + count,
          );
        }
        this.bytes += encoded.byteLength;
      } catch {
        for (const path of createdBlobs) {
          try { unlinkSync(path); } catch { /* another admitted row may already own it */ }
        }
        this.lastError = 'trace record persistence failed';
        this.persistenceFailed = true;
        this.markRejected('persistence_failure');
      } finally {
        this.pendingBytes -= reservedBytes;
        if (control) this.pendingControlBytes -= reservedBytes;
        settle();
      }
    });
    return { accepted: true, recordId: row.record_id, settled };
  }

  recordLoss(
    reason: LossReason,
    traceId: string,
    affectedRecordId?: string | null,
    path?: string,
    bytes?: number,
    count?: number,
  ): AdmissionReceipt {
    return this.admit({
      trace_id: traceId,
      source: runtimeEventSource(),
      event_kind: 'loss',
      phase: 'gap',
      name: `semantic_layer.loss.${reason}`,
      native: null,
      semantic: {},
      correlation: {},
      loss: {
        reason,
        stage: lossStage(reason),
        recoverable: [
          'credential_redaction',
          'scrubber_failure_payload_omitted',
          'unsafe_getter_avoided',
          'unsafe_helper_avoided',
        ].includes(reason),
        ...(affectedRecordId ? { affected_record_id: affectedRecordId } : {}),
        ...(path ? { affected_path: path } : {}),
        ...(bytes === undefined ? {} : { bytes }),
        ...(count === undefined ? {} : { count }),
      },
      loss_refs: [],
      blob_refs: [],
    }, true, [], false, false, false);
  }

  async flush(): Promise<CaptureStatus> {
    await this.writeChain;
    await this.drainCoalesced();
    this.writeManifest(this.state === 'accepting' ? 'open' : this.state === 'closing' ? 'closing' : 'sealed');
    return this.status();
  }

  async prepareOwnershipFinalization(): Promise<boolean> {
    await this.writeChain;
    await this.drainCoalesced();
    if (!this.persistenceFailed) return false;
    await this.recoverFromPersistenceFailure();
    return true;
  }

  beginClosing(): void {
    if (this.state !== 'accepting') return;
    this.state = 'closing';
    this.writeManifest('closing');
  }

  async seal(): Promise<CaptureStatus> {
    if (this.state === 'closed') return this.status();
    this.beginClosing();
    await this.writeChain;
    await this.drainCoalesced();
    if (this.persistenceFailed) await this.recoverFromPersistenceFailure();
    if (this.finalizePendingSourceLifecycles()) {
      this.degrade('source lifecycle incomplete at seal');
      const loss = this.recordLoss('source_rejection', id('trace'));
      await loss.settled;
    }
    this.state = 'closed';
    this.sealedAt ??= new Date().toISOString();
    try {
      this.sealedTraceDigest = await hashFile(this.tracePath);
      this.writeManifest('sealed');
    } catch {
      this.sealedTraceDigest = undefined;
      this.lastError = 'manifest persistence failed during seal';
      this.state = 'closing';
      return this.status();
    }
    try { unlinkSync(this.lockPath); } catch { /* lock absence is harmless after seal */ }
    return this.status();
  }

  private async recoverFromPersistenceFailure(): Promise<void> {
    const failureCount = this.rejectionByReason.get('persistence_failure') ?? 1;
    assertNoSymbolicLinkComponents(this.tracePath, 'file');
    const currentTrace = readFileSync(this.tracePath);
    if (currentTrace.byteLength < this.bytes) {
      throw new Error('trace persistence recovery could not restore the last durable prefix');
    }
    const temporary = `${this.tracePath}.recovering`;
    const fd = openSync(temporary, 'w', 0o600);
    try {
      writeFileSync(fd, currentTrace.subarray(0, this.bytes));
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(temporary, this.tracePath);
    secureOwnerOnly(this.tracePath, 'file');
    syncDirectory(this.artifactPath);
    this.projector = new SemanticProjector(this.seq);
    this.pendingBytes = 0;
    this.pendingControlBytes = 0;
    for (const entry of this.coalesced.values()) entry.resolve();
    this.coalesced.clear();
    this.lastError = 'trace persistence failed; uncertain batch was replaced by durable gap evidence';
    this.persistenceFailed = false;
    this.admit({
      trace_id: id('trace'),
      source: runtimeEventSource(),
      event_kind: 'loss', phase: 'gap', name: 'semantic_layer.loss.persistence_failure',
      native: null, semantic: {}, correlation: {},
      loss: {
        reason: 'persistence_failure', stage: 'persist', recoverable: false,
        affected_path: '/trace.jsonl', count: failureCount,
      },
      loss_refs: [], blob_refs: [],
    }, true);
    await this.writeChain;
  }

  status(): CaptureStatus {
    return {
      state: this.state,
      runId: this.runId,
      artifactPath: this.artifactPath,
      admitted: this.admitted,
      persisted: this.persisted,
      rejected: this.rejected,
      losses: Object.fromEntries(this.persistedLosses),
      activeSources: [...this.sources.values()]
        .filter((source) => this.activeSourceIds.has(source.sourceId))
        .map((source) => source.metadata),
      queue: {
        capacityBytes: this.options.queueCapacityBytes,
        controlReserveBytes: CONTROL_RESERVE_BYTES,
        pendingBytes: this.pendingBytes,
        pendingControlBytes: this.pendingControlBytes,
        highWaterBytes: this.highWaterBytes,
        coalescedGaps: this.coalescedGapCount,
      },
      lastError: this.lastError,
    };
  }

  private writeManifest(state: 'open' | 'closing' | 'sealed'): void {
    const sealed = state === 'sealed';
    const traceDigest = sealed ? this.sealedTraceDigest : null;
    if (sealed && !traceDigest) throw new Error('sealed trace digest is unavailable');
    const blobs = blobStats(join(this.artifactPath, 'blobs'));
    const manifestV2 = this.options.installationId !== undefined;
    const registeredSources = [...this.sources.values()].map((source) => ({
      id: projectedSourceId(source.sourceId),
      name: source.metadata.name,
      seam: source.metadata.seam,
      ...(source.metadata.version
        ? { version: source.metadata.version }
        : manifestV2 && isBuiltinSource(source.sourceClass)
          ? { version: SDK_VERSION }
          : {}),
      ...(manifestV2 ? {
        qualification: source.metadata.qualification ?? defaultQualification(source.sourceClass),
      } : {}),
    }));
    const manifest = {
      schema: manifestV2 ? 'semantic_trace_manifest_v2' : 'semantic_trace_manifest_v1',
      record_schema: 'semantic_trace_record_v1',
      bundle_id: this.runId,
      state,
      sdk: {
        language: this.options.language,
        version: SDK_VERSION,
      },
      privacy_mode: 'local-rich',
      ...(manifestV2 ? {
        capture_policy: 'rich-credential-scrubbed',
        installation_id: this.options.installationId,
      } : {}),
      started_at: this.startedAt,
      updated_at: new Date().toISOString(),
      ...(sealed && this.sealedAt ? { sealed_at: this.sealedAt } : {}),
      sources: registeredSources.length ? registeredSources : [{
        id: projectedSourceId('builtin/semantic-layer-runtime'),
        name: 'semantic-layer-runtime',
        seam: 'capture-runtime',
        ...(manifestV2 ? {
          version: SDK_VERSION,
          qualification: { status: 'exact_qualified' },
        } : {}),
      }],
      trace: {
        path: 'trace.jsonl',
        records: this.persisted,
        last_seq: this.seq,
        bytes: this.bytes,
        losses: this.traceLosses,
        sha256: traceDigest,
      },
      blobs: {
        path: 'blobs',
        count: blobs.count,
        bytes: blobs.bytes,
      },
    };
    const bytes = Buffer.from(`${JSON.stringify(manifest, null, 2)}\n`);
    if (!this.scanner.scan(bytes)) throw new Error('manifest final secret scan blocked persistence');
    const temporary = `${this.manifestPath}.new`;
    const fd = openSync(temporary, 'w', 0o600);
    try {
      writeFileSync(fd, bytes);
      fsyncSync(fd);
    } finally {
      closeSync(fd);
    }
    renameSync(temporary, this.manifestPath);
    secureOwnerOnly(this.manifestPath, 'file');
    syncDirectory(this.artifactPath);
  }

  private coalesceControl(draft: EventDraft, bytes: number): AdmissionReceipt {
    const reason = draft.loss?.reason ?? 'source_rejection';
    const ownershipMarker = draft.loss?.affected_path === '/coverage/ownership/group_limit'
      ? draft.loss.affected_path
      : null;
    const key = JSON.stringify([reason, ownershipMarker]);
    let entry = this.coalesced.get(key);
    if (!entry) {
      let resolve!: () => void;
      const settled = new Promise<void>((done) => { resolve = done; });
      entry = { draft, count: 0, bytes: 0, settled, resolve };
      this.coalesced.set(key, entry);
    }
    entry.count += 1;
    entry.bytes += bytes;
    this.coalescedGapCount += 1;
    return rejected('control_gap_coalesced', entry.settled);
  }

  private async drainCoalesced(): Promise<void> {
    if (!this.coalesced.size) return;
    const entries = [...this.coalesced.values()];
    this.coalesced.clear();
    for (const entry of entries) {
      const receipt = this.admit({
        ...entry.draft,
        loss: entry.draft.loss ? {
          ...entry.draft.loss,
          count: entry.count,
          bytes: entry.bytes,
          detail: 'coalesced bounded control-lane gaps',
        } : undefined,
      }, true);
      await receipt.settled;
      entry.resolve();
    }
  }

  private markRejected(reason: LossReason): void {
    this.rejected += 1;
    this.rejectionByReason.set(reason, (this.rejectionByReason.get(reason) ?? 0) + 1);
  }

  private finalizePendingSourceLifecycles(): boolean {
    let changed = false;
    for (const source of this.sources.values()) {
      if (source.sourceClass === 'builtin_manual' || source.sourceClass === 'builtin_runtime') continue;
      if (source.lifecycle.activation === 'pending') {
        source.lifecycle.activation = 'failed';
        source.lifecycle.deactivation = 'not_applicable';
        source.lifecycle.drain = 'not_applicable';
        changed = true;
        continue;
      }
      if (source.lifecycle.activation !== 'active') continue;
      if (source.lifecycle.deactivation === 'pending') {
        source.lifecycle.deactivation = 'failed';
        changed = true;
      }
      if (source.lifecycle.drain === 'pending') {
        source.lifecycle.drain = 'failed';
        changed = true;
      }
    }
    return changed;
  }
}

function blockedTopLevelPath(
  row: SemanticCaptureEventV1,
  scanner: CredentialScanner,
): string {
  for (const key of Object.keys(row) as Array<keyof SemanticCaptureEventV1>) {
    const encoded = Buffer.from(JSON.stringify({ [key]: row[key] }));
    if (!scanner.scan(encoded)) return `/${String(key)}`;
  }
  return '/record';
}

function runtimeEventSource(): SemanticCaptureEventV1['source'] {
  return {
    source_id: 'builtin/semantic-layer-runtime',
    name: 'semantic-layer-runtime',
    seam: 'capture-runtime',
    identity_domain: 'semantic-layer',
    official: true,
  };
}

function syncDirectory(path: string): void {
  try {
    const directory = openSync(path, 'r');
    try { fsyncSync(directory); } finally { closeSync(directory); }
  } catch {
    // Windows and some filesystems do not support directory fsync.
  }
}

function findStaleRuns(output: string): Array<{ run: string; uncertainTail: boolean }> {
  const findings: Array<{ run: string; uncertainTail: boolean }> = [];
  for (const name of readdirSync(output)) {
    if (!name.startsWith('run-')) continue;
    const path = join(output, name);
    try {
      const run = lstatSync(path);
      if (run.isSymbolicLink() || !run.isDirectory()) {
        findings.push({ run: name, uncertainTail: true });
        quarantineRun(output, name);
        continue;
      }
    } catch {
      continue;
    }
    const lockPath = join(path, '.writer.lock');
    if (existsSync(lockPath)) {
      try {
        const lockEntry = lstatSync(lockPath);
        if (lockEntry.isSymbolicLink() || !lockEntry.isFile()) continue;
        const lock = JSON.parse(readFileSync(lockPath, 'utf8')) as { pid?: number };
        if (lock.pid && processAlive(lock.pid)) continue;
      } catch {
        // An unreadable existing lock is conservatively treated as live.
        continue;
      }
    }
    try {
      assertNoSymbolicLinkComponents(join(path, 'manifest.json'), 'file');
      assertNoSymbolicLinkComponents(join(path, 'trace.jsonl'), 'file');
      const manifest = JSON.parse(readFileSync(join(path, 'manifest.json'), 'utf8')) as { state?: string };
      if (!['open', 'closing'].includes(manifest.state ?? '')) continue;
      const trace = readFileSync(join(path, 'trace.jsonl'));
      let uncertainTail = trace.byteLength > 0 && trace[trace.byteLength - 1] !== 0x0a;
      if (!uncertainTail) {
        try {
          for (const line of trace.toString('utf8').split('\n').filter(Boolean)) JSON.parse(line);
        } catch { uncertainTail = true; }
      }
      findings.push({ run: name, uncertainTail });
      quarantineRun(output, name);
    } catch {
      // A partially-created prior run is itself uncertain recovery evidence.
      findings.push({ run: name, uncertainTail: true });
      quarantineRun(output, name);
    }
  }
  return findings;
}

function processAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    return (error as NodeJS.ErrnoException).code === 'EPERM';
  }
}

function quarantineRun(output: string, name: string): void {
  const source = join(output, name);
  try {
    const entry = lstatSync(source);
    if (entry.isDirectory() && !entry.isSymbolicLink()) {
      try { unlinkSync(join(source, '.writer.lock')); } catch { /* absent or unreadable lock */ }
    }
  } catch {
    return;
  }
  let destination = join(output, `quarantine-${name}`);
  if (existsSync(destination)) destination = join(output, `quarantine-${name}-${Date.now()}`);
  try {
    renameSync(source, destination);
    syncDirectory(output);
  } catch {
    // If quarantine cannot be made durable, discovery will repeat on the next run.
  }
}

function id(prefix: string): string {
  return `${prefix}_${randomBytes(16).toString('hex')}`;
}

function defaultQualification(
  sourceClass: ArtifactSourceClass,
): { status: 'exact_qualified' | 'unknown' } {
  return isBuiltinSource(sourceClass)
    ? { status: 'exact_qualified' }
    : { status: 'unknown' };
}

function isBuiltinSource(sourceClass: ArtifactSourceClass): boolean {
  return sourceClass === 'builtin_manual' || sourceClass === 'builtin_runtime';
}

function canonicalOutputPath(path: string): string {
  const target = resolve(path);
  assertNoSymbolicLinkComponents(target);
  let existing = target;
  while (existing.length > 0) {
    try {
      const canonical = realpathSync(existing);
      const entry = lstatSync(canonical);
      if (!entry.isDirectory()) {
        throw new Error(`capture path ancestor is not a directory: ${existing}`);
      }
      return resolve(canonical, relative(existing, target));
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
    const parent = dirname(existing);
    if (parent === existing) throw new Error(`cannot resolve an existing ancestor for ${target}`);
    existing = parent;
  }
  throw new Error(`cannot resolve an existing ancestor for ${target}`);
}

function mkdirOwnerOnly(path: string): void {
  assertNoSymbolicLinkComponents(path);
  if (existsSync(path)) {
    secureOwnerOnly(path, 'directory');
    return;
  }
  const missing: string[] = [];
  let cursor = path;
  while (!existsSync(cursor)) {
    missing.push(cursor);
    const parent = dirname(cursor);
    if (parent === cursor) throw new Error(`cannot resolve an existing ancestor for ${path}`);
    cursor = parent;
  }
  for (const component of missing.reverse()) {
    mkdirSync(component, { mode: 0o700 });
    secureOwnerOnly(component, 'directory');
  }
}

function encodeProjected(records: readonly SemanticTraceRecord[]): Buffer {
  if (records.length === 0) return Buffer.alloc(0);
  return Buffer.from(records.map((record) => `${JSON.stringify(record)}\n`).join(''));
}

function projectedLossCounts(
  records: readonly SemanticTraceRecord[],
): Map<string, number> {
  const counts = new Map<string, number>();
  for (const record of records) {
    if (record.kind !== 'loss') continue;
    const reason = record.data.reason;
    const count = record.data.count;
    if (
      typeof reason !== 'string'
      || typeof count !== 'number'
      || !Number.isSafeInteger(count)
      || count <= 0
    ) {
      continue;
    }
    counts.set(reason, (counts.get(reason) ?? 0) + count);
  }
  return counts;
}

function projectionReservation(rawBytes: number): number {
  // The projector emits at most two rows, reuses only serialized input fields, and adds
  // bounded normalization metadata. Keep the bound explicit so future projector growth
  // trips the recovery invariant instead of silently overrunning the queue.
  return rawBytes * 2 + PROJECTION_OVERHEAD_BYTES;
}

function projectedSourceId(value: string): string {
  return `src_${createHash('sha256').update(value).digest('hex').slice(0, 24)}`;
}

function blobStats(path: string): { count: number; bytes: number } {
  if (!existsSync(path)) return { count: 0, bytes: 0 };
  assertNoSymbolicLinkComponents(path, 'directory');
  let count = 0;
  let bytes = 0;
  for (const name of readdirSync(path)) {
    const file = join(path, name);
    try {
      const stat = lstatSync(file);
      if (stat.isSymbolicLink()) continue;
      if (!stat.isFile() || !name.endsWith('.blob')) continue;
      count += 1;
      bytes += stat.size;
    } catch {
      // A concurrent failed admission may have removed its temporary blob.
    }
  }
  return { count, bytes };
}

async function hashFile(path: string): Promise<string> {
  assertNoSymbolicLinkComponents(path, 'file');
  const hash = createHash('sha256');
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest('hex');
}

function rejected(reason: string, settled: Promise<void> = Promise.resolve()): AdmissionReceipt & { reason: string } {
  return { accepted: false, reason, settled };
}

function lossStage(reason: LossReason): 'source' | 'serialize' | 'scrub' | 'queue' | 'persist' | 'recover' {
  if (reason.includes('credential') || reason.includes('scrubber')) return 'scrub';
  if (reason.includes('getter') || reason.includes('helper') || reason.includes('serialization')) return 'serialize';
  if (reason.includes('queue')) return 'queue';
  if (reason.includes('persistence')) return 'persist';
  if (reason.includes('crash') || reason.includes('tail')) return 'recover';
  return 'source';
}
