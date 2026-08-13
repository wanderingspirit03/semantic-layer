import { createHash, randomUUID } from 'node:crypto';
import {
  chmodSync,
  constants,
  lstatSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
} from 'node:fs';
import {
  chmod,
  link,
  lstat,
  mkdir,
  open,
  readFile,
  realpath,
  readdir,
  rename,
  rm,
  stat,
} from 'node:fs/promises';
import { basename, dirname, join, posix, resolve, sep } from 'node:path';
import { validateArtifact } from 'semantic-layer-capture';

export const INGEST_PROTOCOL_VERSION = '1' as const;
export const PART_SIZE_BYTES = 8 * 1024 * 1024;
export const MAX_FILE_BYTES = 256 * 1024 * 1024;
export const MAX_BUNDLE_BYTES = 512 * 1024 * 1024;
export const DEFAULT_MAX_SPOOL_BYTES = 5 * 1024 * 1024 * 1024;
export const MAX_CONCURRENCY = 2;

const BUNDLE_DIGEST_DOMAIN = Buffer.from('semantic-layer-bundle-v1\0', 'utf8');
const MAX_BACKOFF_MS = 5 * 60 * 1_000;
const BASE_BACKOFF_MS = 500;
const INSTALLATION_ID = /^install_[A-Za-z0-9_-]{22,128}$/u;
const EXTERNAL_ENTRY_DOMAIN = Buffer.from(
  'semantic-layer-external-spool-entry-v1\0',
  'utf8',
);

export type CloudUploaderOptions = {
  endpoint: string;
  ingestKey?: string;
  installationId?: string;
  spoolDirectory?: string;
  maxSpoolBytes?: number;
  concurrency?: number;
  fetch?: typeof globalThis.fetch;
};

export type EnqueueReceipt = {
  bundleId: string;
  bundleDigest: string;
  state: 'pending' | 'acked' | 'awaiting_spool_admission';
};

export type EnqueueOptions = {
  removeSourceAfterAdmissionFrom?: string;
};

export type UploadFailure = {
  bundleDigest: string;
  code: string;
  message: string;
  at: string;
  attempts: number;
};

export type OldestPending = {
  bundleDigest: string;
  enqueuedAt: string;
  ageMs: number;
};

export type CloudUploaderStatus = {
  lifecycle: 'running' | 'shutdown';
  pendingBundles: number;
  ackedBundles: number;
  blockedBundles: number;
  awaitingSpoolAdmissionBundles: number;
  quarantineBundles: number;
  pendingBytes: number;
  blockedBytes: number;
  awaitingSpoolAdmissionBytes: number;
  quarantineBytes: number;
  spoolBytes: number;
  maxSpoolBytes: number;
  pressure: 'ok' | 'warning' | 'critical' | 'full';
  warnings: string[];
  oldestPending: OldestPending | null;
  pausedAuth: boolean;
  retryingBundles: number;
  nextRetryAt: string | null;
  quotaLimited: boolean;
  lastAcknowledgedAt: string | null;
  lastRequestId: string | null;
  failures: UploadFailure[];
};

export type FlushOptions = { deadlineMs: number };
export type FlushResult = CloudUploaderStatus & {
  uploadedBundles: number;
  timedOut: boolean;
};

export interface CloudUploader {
  enqueueArtifact(
    sealedArtifactPath: string,
    options?: EnqueueOptions,
  ): Promise<EnqueueReceipt>;
  flush(options: FlushOptions): Promise<FlushResult>;
  status(): CloudUploaderStatus;
  shutdown(): Promise<void>;
}

export class CloudUploaderError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.name = 'CloudUploaderError';
    this.code = code;
  }
}

type Manifest = Record<string, unknown> & {
  bundle_id?: unknown;
  state?: unknown;
};
type PartDescriptor = { index: number; size_bytes: number; sha256: string };
type FileDescriptor = {
  file_id: string;
  path: string;
  size_bytes: number;
  sha256: string;
  parts: PartDescriptor[];
};
type BundleInventory = {
  bundleId: string;
  bundleDigest: string;
  manifest: Manifest;
  files: FileDescriptor[];
  bytes: number;
};
type PersistedState = {
  failures: UploadFailure[];
  lastAcknowledgedAt?: string;
};
type ExternalSpoolState = {
  entry_id: string;
  source_path: string;
  bundle_id?: string;
  bundle_digest?: string;
  source_bytes: number;
  code: string;
  message: string;
  created_at: string;
};

type NormalizedCloudUploaderOptions = {
  endpoint: string;
  configuredKey: string | undefined;
  installationId: string | undefined;
  spool: string;
  maxSpoolBytes: number;
  concurrency: number;
  fetchImpl: typeof globalThis.fetch;
};

type SharedUploaderEntry = {
  core: DurableCloudUploader;
  options: NormalizedCloudUploaderOptions;
  initialization: Promise<void>;
  references: number;
  closing: boolean;
};

const UPLOADER_REGISTRY = Symbol.for(
  'semantic-layer-cloud.uploader-registry.v1',
);

export function createCloudUploader(
  options: CloudUploaderOptions,
): CloudUploader {
  const normalized = normalizeCloudUploaderOptions(options);
  const registry = sharedUploaderRegistry();
  const existing = registry.get(normalized.spool);
  if (existing) {
    if (existing.closing) {
      throw new CloudUploaderError(
        'SPOOL_SHUTTING_DOWN',
        'spool owner is still shutting down',
      );
    }
    if (!compatibleUploaderOptions(existing.options, normalized)) {
      throw new CloudUploaderError(
        'SPOOL_CONFIG_CONFLICT',
        'spool is already open with different uploader settings',
      );
    }
    existing.references += 1;
    return new SharedCloudUploaderLease(existing, registry);
  }

  const core = new DurableCloudUploader(normalized);
  const entry: SharedUploaderEntry = {
    core,
    options: normalized,
    initialization: core.initialization(),
    references: 1,
    closing: false,
  };
  registry.set(normalized.spool, entry);
  void entry.initialization.catch(() => {
    if (registry.get(normalized.spool) === entry)
      registry.delete(normalized.spool);
  });
  return new SharedCloudUploaderLease(entry, registry);
}

function sharedUploaderRegistry(): Map<string, SharedUploaderEntry> {
  const root = globalThis as unknown as Record<symbol, unknown>;
  const existing = root[UPLOADER_REGISTRY];
  if (existing instanceof Map)
    return existing as Map<string, SharedUploaderEntry>;
  const registry = new Map<string, SharedUploaderEntry>();
  root[UPLOADER_REGISTRY] = registry;
  return registry;
}

function normalizeCloudUploaderOptions(
  options: CloudUploaderOptions,
): NormalizedCloudUploaderOptions {
  if (!options.endpoint)
    throw new CloudUploaderError('ENDPOINT_REQUIRED', 'endpoint is required');
  let endpoint: URL;
  try {
    endpoint = new URL(options.endpoint);
  } catch {
    throw new CloudUploaderError(
      'ENDPOINT_INVALID',
      'endpoint must be an absolute HTTP(S) URL',
    );
  }
  if (!['http:', 'https:'].includes(endpoint.protocol)) {
    throw new CloudUploaderError(
      'ENDPOINT_INVALID',
      'endpoint must be an absolute HTTP(S) URL',
    );
  }
  const loopback =
    endpoint.hostname === 'localhost' ||
    endpoint.hostname === '127.0.0.1' ||
    endpoint.hostname === '[::1]';
  if (endpoint.protocol !== 'https:' && !loopback) {
    throw new CloudUploaderError(
      'ENDPOINT_INSECURE',
      'endpoint must use HTTPS except on loopback',
    );
  }
  if (options.ingestKey !== undefined && options.ingestKey.length < 8) {
    throw new CloudUploaderError(
      'INGEST_KEY_INVALID',
      'ingestKey must contain at least 8 characters',
    );
  }
  const installationId =
    options.installationId ?? process.env.SEMANTIC_LAYER_INSTALLATION_ID;
  if (installationId !== undefined && !INSTALLATION_ID.test(installationId)) {
    throw new CloudUploaderError(
      'INSTALLATION_ID_INVALID',
      'installationId must be an opaque install_ identifier',
    );
  }
  const concurrency = options.concurrency ?? MAX_CONCURRENCY;
  if (
    !Number.isInteger(concurrency) ||
    concurrency < 1 ||
    concurrency > MAX_CONCURRENCY
  ) {
    throw new CloudUploaderError(
      'CONCURRENCY_INVALID',
      'concurrency must be 1 or 2',
    );
  }
  const maxSpoolBytes = options.maxSpoolBytes ?? DEFAULT_MAX_SPOOL_BYTES;
  if (!Number.isSafeInteger(maxSpoolBytes) || maxSpoolBytes < 1) {
    throw new CloudUploaderError(
      'MAX_SPOOL_BYTES_INVALID',
      'maxSpoolBytes must be a positive safe integer',
    );
  }
  return {
    endpoint: options.endpoint.replace(/\/+$/, ''),
    configuredKey: options.ingestKey,
    installationId,
    spool: resolve(options.spoolDirectory ?? '.semantic-layer/cloud-spool'),
    maxSpoolBytes,
    concurrency,
    fetchImpl: options.fetch ?? globalThis.fetch,
  };
}

function compatibleUploaderOptions(
  left: NormalizedCloudUploaderOptions,
  right: NormalizedCloudUploaderOptions,
): boolean {
  return (
    left.endpoint === right.endpoint &&
    left.configuredKey === right.configuredKey &&
    left.installationId === right.installationId &&
    left.maxSpoolBytes === right.maxSpoolBytes &&
    left.concurrency === right.concurrency &&
    left.fetchImpl === right.fetchImpl
  );
}

class SharedCloudUploaderLease implements CloudUploader {
  private stopped = false;
  private shutdownPromise: Promise<void> | undefined;

  constructor(
    private readonly entry: SharedUploaderEntry,
    private readonly registry: Map<string, SharedUploaderEntry>,
  ) {}

  enqueueArtifact(
    sealedArtifactPath: string,
    options?: EnqueueOptions,
  ): Promise<EnqueueReceipt> {
    this.assertRunning();
    return this.entry.core.enqueueArtifact(sealedArtifactPath, options);
  }

  flush(options: FlushOptions): Promise<FlushResult> {
    this.assertRunning();
    return this.entry.core.flush(options);
  }

  status(): CloudUploaderStatus {
    const status = this.entry.core.status();
    return this.stopped ? { ...status, lifecycle: 'shutdown' } : status;
  }

  shutdown(): Promise<void> {
    this.shutdownPromise ??= this.performShutdown();
    return this.shutdownPromise;
  }

  private async performShutdown(): Promise<void> {
    this.stopped = true;
    this.entry.references -= 1;
    if (this.entry.references > 0) return;
    this.entry.closing = true;
    try {
      await this.entry.core.shutdown();
    } finally {
      try {
        await this.entry.core.termination();
      } finally {
        if (this.registry.get(this.entry.options.spool) === this.entry) {
          this.registry.delete(this.entry.options.spool);
        }
      }
    }
  }

  private assertRunning(): void {
    if (this.stopped)
      throw new CloudUploaderError(
        'UPLOADER_SHUTDOWN',
        'uploader lease is shut down',
      );
  }
}

export function computeBundleDigest(
  files: readonly Pick<FileDescriptor, 'path' | 'size_bytes' | 'sha256'>[],
): string {
  const digest = createHash('sha256');
  digest.update(BUNDLE_DIGEST_DOMAIN);
  for (const file of [...files].sort((left, right) =>
    Buffer.compare(Buffer.from(left.path), Buffer.from(right.path)),
  )) {
    const pathBytes = Buffer.from(file.path, 'utf8');
    digest.update(Buffer.from(String(pathBytes.byteLength), 'utf8'));
    digest.update(':');
    digest.update(pathBytes);
    digest.update('\0');
    digest.update(Buffer.from(String(file.size_bytes), 'utf8'));
    digest.update('\0');
    digest.update(file.sha256.toLowerCase());
    digest.update('\0');
  }
  return digest.digest('hex');
}

class DurableCloudUploader implements CloudUploader {
  private readonly endpoint: string;
  private readonly configuredKey: string | undefined;
  private readonly installationId: string | undefined;
  private readonly spool: string;
  private readonly maxSpoolBytes: number;
  private readonly concurrency: number;
  private readonly fetchImpl: typeof globalThis.fetch;
  private readonly ready: Promise<void>;
  private snapshot: CloudUploaderStatus;
  private persisted: PersistedState = { failures: [] };
  private processing: Promise<void> | undefined;
  private scheduledProcessing: Promise<void> | undefined;
  private retryTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly retryAt = new Map<string, number>();
  private readonly attempts = new Map<string, number>();
  private readonly activeRequests = new Set<AbortController>();
  private admission = Promise.resolve();
  private readonly ownerToken = randomUUID();
  private ownsSpool = false;
  private exitCleanup: (() => void) | undefined;
  private pausedKey: string | undefined;
  private lastRequestId: string | null = null;
  private stopped = false;
  private terminated: Promise<void> | undefined;

  constructor(options: NormalizedCloudUploaderOptions) {
    this.endpoint = options.endpoint;
    this.configuredKey = options.configuredKey;
    this.installationId = options.installationId;
    this.spool = options.spool;
    this.maxSpoolBytes = options.maxSpoolBytes;
    this.concurrency = options.concurrency;
    this.fetchImpl = options.fetchImpl;
    this.snapshot = emptyStatus(options.maxSpoolBytes);
    this.bootstrapStatusSynchronously();
    this.ready = this.initialize();
  }

  initialization(): Promise<void> {
    return this.ready;
  }

  termination(): Promise<void> {
    return this.terminated ?? Promise.resolve();
  }

  async enqueueArtifact(
    sealedArtifactPath: string,
    options?: EnqueueOptions,
  ): Promise<EnqueueReceipt> {
    await this.ready;
    this.assertRunning();
    const admitted = this.admission.then(async () => {
      const receipt = await this.enqueueArtifactOwned(sealedArtifactPath);
      if (
        options?.removeSourceAfterAdmissionFrom &&
        receipt.state !== 'awaiting_spool_admission'
      ) {
        try {
          await removeDirectArtifactChild(
            sealedArtifactPath,
            options.removeSourceAfterAdmissionFrom,
          );
        } catch (error) {
          await this.recordFailure(
            receipt.bundleDigest,
            'SOURCE_CLEANUP_FAILED',
            error instanceof Error ? error.message : String(error),
          );
          await this.refreshStatus();
        }
      }
      return receipt;
    });
    this.admission = admitted.then(
      () => undefined,
      () => undefined,
    );
    return admitted;
  }

  private async enqueueArtifactOwned(
    sealedArtifactPath: string,
  ): Promise<EnqueueReceipt> {
    const source = resolve(sealedArtifactPath);
    const key = this.currentKey();
    let inventory: BundleInventory;
    try {
      inventory = await inventoryBundle(source);
    } catch (error) {
      await this.retainRejectedSource(source, error);
      throw error;
    }
    if (inventory.manifest.state !== 'sealed') {
      throw new CloudUploaderError(
        'ARTIFACT_NOT_SEALED',
        'manifest.state must be sealed',
      );
    }
    try {
      assertManifestAccepted(inventory.manifest, this.installationId);
    } catch (error) {
      const failure =
        error instanceof CloudUploaderError
          ? error
          : new CloudUploaderError(
              'MANIFEST_VERSION_UNSUPPORTED',
              'manifest is not accepted for upload',
            );
      await this.retainExternalState(
        'upload_blocked',
        source,
        failure,
        inventory,
      );
      await this.refreshStatus();
      throw failure;
    }
    await this.refreshStatus();

    await this.validateSourceForAdmission(source, inventory, key);

    const acked = join(this.spool, 'acked', inventory.bundleDigest);
    if (await exists(acked)) {
      if (!(await this.acknowledgementMatches(acked, inventory))) {
        throw new CloudUploaderError(
          'ACK_STATE_INVALID',
          'acknowledged spool state does not contain the expected receipt',
        );
      }
      return {
        bundleId: inventory.bundleId,
        bundleDigest: inventory.bundleDigest,
        state: 'acked',
      };
    }
    if (await exists(join(this.spool, 'quarantine', inventory.bundleDigest))) {
      throw new CloudUploaderError(
        'BUNDLE_QUARANTINED',
        'this exact bundle is retained in quarantine and requires operator action',
      );
    }
    if (
      await exists(join(this.spool, 'upload_blocked', inventory.bundleDigest))
    ) {
      throw new CloudUploaderError(
        'UPLOAD_BLOCKED',
        'this exact bundle has a non-retryable local upload block and requires operator action',
      );
    }
    const awaiting = join(
      this.spool,
      'awaiting_spool_admission',
      inventory.bundleDigest,
    );
    if (await exists(awaiting)) {
      const persistedSource = safeExternalSourcePath(source, key);
      if (persistedSource) {
        await writeOwnerJson(
          join(awaiting, 'state.json'),
          externalState(
            persistedSource,
            new CloudUploaderError(
              'SPOOL_FULL',
              'spool capacity would be exceeded; existing data was retained',
            ),
            inventory,
            inventory.bundleDigest,
            inventory.bytes,
          ),
        );
      }
      if (this.snapshot.spoolBytes + inventory.bytes <= this.maxSpoolBytes) {
        return this.stageInventory(source, inventory, key, awaiting, true);
      }
      return {
        bundleId: inventory.bundleId,
        bundleDigest: inventory.bundleDigest,
        state: 'awaiting_spool_admission',
      };
    }
    const pending = join(this.spool, 'pending', inventory.bundleDigest);
    if (await exists(pending)) {
      if (!(await this.pendingBundleMatches(pending, inventory))) {
        throw new CloudUploaderError(
          'PENDING_STATE_INVALID',
          'pending spool state does not contain the expected bundle bytes',
        );
      }
      return {
        bundleId: inventory.bundleId,
        bundleDigest: inventory.bundleDigest,
        state: 'pending',
      };
    }
    if (this.snapshot.spoolBytes + inventory.bytes > this.maxSpoolBytes) {
      if (!safeExternalSourcePath(source, key)) {
        const error = new CloudUploaderError(
          'SOURCE_PATH_UNSAFE',
          'artifact path contains configured credential material and cannot be retained for re-admission',
        );
        await this.retainExternalState(
          'upload_blocked',
          source,
          error,
          inventory,
        );
        await this.recordFailure(
          inventory.bundleDigest,
          error.code,
          error.message,
        );
        await this.refreshStatus();
        throw error;
      }
      await this.retainExternalState(
        'awaiting_spool_admission',
        source,
        new CloudUploaderError(
          'SPOOL_FULL',
          'spool capacity would be exceeded; existing data was retained',
        ),
        inventory,
      );
      await this.recordFailure(
        inventory.bundleDigest,
        'SPOOL_FULL',
        'spool capacity would be exceeded; existing data was retained',
      );
      await this.refreshStatus();
      this.scheduleProcessing(1_000);
      return {
        bundleId: inventory.bundleId,
        bundleDigest: inventory.bundleDigest,
        state: 'awaiting_spool_admission',
      };
    }

    return this.stageInventory(source, inventory, key, undefined, true);
  }

  private async stageInventory(
    source: string,
    inventory: BundleInventory,
    key: string | undefined,
    awaitingItem?: string,
    prevalidated = false,
  ): Promise<EnqueueReceipt> {
    if (!prevalidated)
      await this.validateSourceForAdmission(source, inventory, key);
    const pending = join(this.spool, 'pending', inventory.bundleDigest);
    const stagingItem = join(this.spool, 'status', `staging-${randomUUID()}`);
    const stagedBundle = join(stagingItem, 'bundle');
    await mkdir(stagedBundle, { recursive: true, mode: 0o700 });
    try {
      await writeOwnerJson(join(stagingItem, 'reservation.json'), {
        bundle_id: inventory.bundleId,
        bundle_digest: inventory.bundleDigest,
        bytes: inventory.bytes,
        source_prevalidated: true,
      });
      await this.refreshStatus();
      await copyBundleSnapshot(source, stagedBundle, inventory);
      const validation = await validateArtifact(stagedBundle, {
        profile: 'structural',
        ...(key ? { secretValues: [key] } : {}),
      });
      if (!validation.valid) {
        const error = new CloudUploaderError(
          'ARTIFACT_INVALID',
          `artifact failed structural validation: ${validation.issues.join(', ')}`,
        );
        if (
          validation.secretMatches > 0 ||
          validation.issues.includes('SECRET_MATCH')
        ) {
          await this.retainExternalState(
            'quarantine',
            source,
            new CloudUploaderError(
              'ARTIFACT_UNSAFE',
              'artifact contains credential material and cannot enter the upload spool',
            ),
            inventory,
          );
          if (awaitingItem)
            await rm(awaitingItem, { recursive: true, force: true });
          throw new CloudUploaderError(
            'ARTIFACT_UNSAFE',
            'artifact contains credential material and cannot enter the upload spool',
          );
        }
        await rm(join(stagingItem, 'reservation.json'), { force: true });
        await writeOwnerJson(
          join(stagingItem, 'state.json'),
          externalState(
            safeExternalSourcePath(source, key),
            error,
            inventory,
            inventory.bundleDigest,
          ),
        );
        await syncDirectory(stagingItem);
        await rename(
          stagingItem,
          join(this.spool, 'upload_blocked', inventory.bundleDigest),
        );
        await syncDirectory(join(this.spool, 'upload_blocked'));
        if (awaitingItem) {
          await rm(awaitingItem, { recursive: true, force: true });
          await syncDirectory(join(this.spool, 'awaiting_spool_admission'));
        }
        await this.recordFailure(
          inventory.bundleDigest,
          error.code,
          error.message,
        );
        throw error;
      }
      const copiedInventory = await inventoryBundle(stagedBundle);
      if (
        copiedInventory.bundleDigest !== inventory.bundleDigest ||
        copiedInventory.bytes !== inventory.bytes
      ) {
        throw new CloudUploaderError(
          'SPOOL_FILE_CHANGED',
          'artifact changed while it was entering the durable spool',
        );
      }
      await rm(join(stagingItem, 'reservation.json'), { force: true });
      await syncDirectory(stagingItem);
      try {
        await rename(stagingItem, pending);
        await syncDirectory(join(this.spool, 'pending'));
      } catch (error) {
        if (!(await exists(pending))) throw error;
      }
      if (awaitingItem) {
        await rm(awaitingItem, { recursive: true, force: true });
        await syncDirectory(join(this.spool, 'awaiting_spool_admission'));
      }
      await this.refreshStatus();
      this.scheduleProcessing(0);
      return {
        bundleId: inventory.bundleId,
        bundleDigest: inventory.bundleDigest,
        state: 'pending',
      };
    } finally {
      if (await exists(stagingItem))
        await rm(stagingItem, { recursive: true, force: true });
      await this.refreshStatus();
    }
  }

  private async validateSourceForAdmission(
    source: string,
    inventory: BundleInventory,
    key: string | undefined,
  ): Promise<void> {
    const validation = await validateArtifact(source, {
      profile: 'structural',
      ...(key ? { secretValues: [key] } : {}),
    });
    if (!validation.valid) {
      const unsafe =
        validation.secretMatches > 0 ||
        validation.issues.includes('SECRET_MATCH');
      const error = unsafe
        ? new CloudUploaderError(
            'ARTIFACT_UNSAFE',
            'artifact contains credential material and cannot enter the upload spool',
          )
        : new CloudUploaderError(
            'ARTIFACT_INVALID',
            `artifact failed structural validation: ${validation.issues.join(', ')}`,
          );
      await this.retainExternalState(
        unsafe ? 'quarantine' : 'upload_blocked',
        source,
        error,
        inventory,
      );
      await this.recordFailure(
        inventory.bundleDigest,
        error.code,
        error.message,
      );
      await this.refreshStatus();
      throw error;
    }
    const verified = await inventoryBundle(source);
    if (
      verified.bundleDigest !== inventory.bundleDigest ||
      verified.bytes !== inventory.bytes
    ) {
      throw new CloudUploaderError(
        'SPOOL_FILE_CHANGED',
        'artifact changed while it was validated for spool admission',
      );
    }
  }

  private async retainRejectedSource(
    source: string,
    error: unknown,
  ): Promise<void> {
    if (!(error instanceof CloudUploaderError)) return;
    if (
      error.code === 'ARTIFACT_NOT_SEALED' ||
      error.code === 'SPOOL_FILE_CHANGED'
    )
      return;
    const state =
      error.code === 'ARTIFACT_UNSAFE' ? 'quarantine' : 'upload_blocked';
    await this.retainExternalState(state, source, error);
    await this.refreshStatus();
  }

  private async retainExternalState(
    state: 'awaiting_spool_admission' | 'upload_blocked' | 'quarantine',
    source: string,
    error: CloudUploaderError,
    inventory?: BundleInventory,
  ): Promise<void> {
    const entryId = inventory?.bundleDigest ?? externalEntryId(source);
    const item = join(this.spool, state, entryId);
    if (await exists(item)) return;
    const temporary = join(this.spool, state, `.external-${randomUUID()}.tmp`);
    await mkdir(temporary, { recursive: false, mode: 0o700 });
    try {
      const sourceBytes = inventory?.bytes ?? (await sourceBytesSafely(source));
      const persistedSource = safeExternalSourcePath(source, this.currentKey());
      await writeOwnerJson(
        join(temporary, 'state.json'),
        externalState(persistedSource, error, inventory, entryId, sourceBytes),
      );
      await syncDirectory(temporary);
      await rename(temporary, item);
      await syncDirectory(join(this.spool, state));
    } catch (writeError) {
      if (await exists(temporary))
        await rm(temporary, { recursive: true, force: true });
      throw writeError;
    }
  }

  private async admitWaitingArtifacts(): Promise<void> {
    await this.refreshStatus();
    const root = join(this.spool, 'awaiting_spool_admission');
    for (const name of await listDirectories(root)) {
      const item = join(root, name);
      let state: ExternalSpoolState;
      try {
        state = await readExternalState(item);
      } catch {
        await this.moveExternalState(
          item,
          'quarantine',
          name,
          'AWAITING_STATE_INVALID',
          'awaiting-admission state is unreadable and requires operator action',
        );
        continue;
      }
      let inventory: BundleInventory;
      try {
        inventory = await inventoryBundle(state.source_path);
        assertManifestAccepted(inventory.manifest, this.installationId);
      } catch (error) {
        const failure =
          error instanceof CloudUploaderError
            ? error
            : new CloudUploaderError(
                'ARTIFACT_UNSAFE',
                'awaiting artifact could not be safely read',
              );
        await this.moveExternalState(
          item,
          failure.code === 'ARTIFACT_UNSAFE' ? 'quarantine' : 'upload_blocked',
          name,
          failure.code,
          failure.message,
        );
        await this.recordFailure(name, failure.code, failure.message);
        continue;
      }
      if (
        inventory.bundleDigest !== state.bundle_digest ||
        inventory.bytes !== state.source_bytes
      ) {
        await this.moveExternalState(
          item,
          'quarantine',
          name,
          'AWAITING_SOURCE_CHANGED',
          'awaiting artifact changed before durable spool admission',
        );
        await this.recordFailure(
          name,
          'AWAITING_SOURCE_CHANGED',
          'awaiting artifact changed before durable spool admission',
        );
        continue;
      }
      await this.refreshStatus();
      if (this.snapshot.spoolBytes + inventory.bytes > this.maxSpoolBytes)
        continue;
      try {
        await this.stageInventory(
          state.source_path,
          inventory,
          this.currentKey(),
          item,
        );
        await this.clearFailure(name);
      } catch {
        // stageInventory durably classifies validation/safety failures before returning.
      }
    }
    await this.refreshStatus();
  }

  private async moveExternalState(
    source: string,
    destinationState: 'upload_blocked' | 'quarantine',
    entryId: string,
    code: string,
    message: string,
  ): Promise<void> {
    const state = await readExternalState(source).catch(
      () =>
        ({
          entry_id: entryId,
          source_path: '',
          source_bytes: 0,
          code,
          message,
          created_at: new Date().toISOString(),
        }) satisfies ExternalSpoolState,
    );
    await writeOwnerJson(join(source, 'state.json'), {
      ...state,
      code,
      message,
    });
    let destination = join(this.spool, destinationState, entryId);
    if (await exists(destination))
      destination = `${destination}-${randomUUID()}`;
    await rename(source, destination);
    await syncDirectory(join(this.spool, destinationState));
  }

  async flush(options: FlushOptions): Promise<FlushResult> {
    await this.ready;
    if (!Number.isFinite(options.deadlineMs) || options.deadlineMs < 0) {
      throw new CloudUploaderError(
        'DEADLINE_INVALID',
        'deadlineMs must be a non-negative finite number',
      );
    }
    const deadline = Date.now() + options.deadlineMs;
    const startingAcked = this.snapshot.ackedBundles;
    let deadlineReached = false;
    do {
      this.resumeIfKeyChanged();
      const remainingBeforeAttempt = deadline - Date.now();
      if (remainingBeforeAttempt <= 0) {
        deadlineReached = this.snapshot.pendingBundles > 0;
        break;
      }
      const attempt = this.processPending();
      const outcome = await Promise.race([
        attempt.then(() => 'complete' as const),
        delay(remainingBeforeAttempt).then(() => 'deadline' as const),
      ]);
      if (outcome === 'deadline') {
        deadlineReached = true;
        this.abortActiveRequests();
        break;
      }
      await this.refreshStatus();
      if (
        this.snapshot.pendingBundles === 0 ||
        this.snapshot.pausedAuth ||
        this.stopped
      )
        break;
      const remaining = deadline - Date.now();
      if (remaining <= 0) break;
      const nextRetry = Math.min(
        ...[...this.retryAt.values()],
        Date.now() + 50,
      );
      await delay(Math.min(remaining, Math.max(1, nextRetry - Date.now())));
    } while (Date.now() <= deadline);

    const status = this.status();
    return {
      ...status,
      uploadedBundles: Math.max(0, status.ackedBundles - startingAcked),
      timedOut:
        status.pendingBundles > 0 &&
        !status.pausedAuth &&
        (deadlineReached || Date.now() >= deadline),
    };
  }

  status(): CloudUploaderStatus {
    return {
      ...this.snapshot,
      warnings: [...this.snapshot.warnings],
      failures: this.snapshot.failures.map((failure) => ({ ...failure })),
      oldestPending: this.snapshot.oldestPending
        ? { ...this.snapshot.oldestPending }
        : null,
    };
  }

  async shutdown(): Promise<void> {
    await this.ready;
    if (this.terminated) {
      await this.terminated;
      return;
    }
    this.stopped = true;
    this.snapshot = { ...this.snapshot, lifecycle: 'shutdown' };
    if (this.retryTimer) clearTimeout(this.retryTimer);
    this.abortActiveRequests();
    const work = [
      this.admission,
      ...(this.processing ? [this.processing] : []),
      ...(this.scheduledProcessing ? [this.scheduledProcessing] : []),
    ];
    this.terminated = Promise.allSettled(work).then(async () =>
      this.releaseSpoolOwnership(),
    );
    await this.terminated;
  }

  private async initialize(): Promise<void> {
    await Promise.all([
      mkdir(join(this.spool, 'pending'), { recursive: true, mode: 0o700 }),
      mkdir(join(this.spool, 'acked'), { recursive: true, mode: 0o700 }),
      mkdir(join(this.spool, 'upload_blocked'), {
        recursive: true,
        mode: 0o700,
      }),
      mkdir(join(this.spool, 'awaiting_spool_admission'), {
        recursive: true,
        mode: 0o700,
      }),
      mkdir(join(this.spool, 'quarantine'), { recursive: true, mode: 0o700 }),
      mkdir(join(this.spool, 'status'), { recursive: true, mode: 0o700 }),
    ]);
    await chmod(this.spool, 0o700);
    await this.acquireSpoolOwnership();
    try {
      const statePath = join(this.spool, 'status', 'state.json');
      try {
        const parsed = JSON.parse(
          await readFile(statePath, 'utf8'),
        ) as Partial<PersistedState>;
        if (Array.isArray(parsed.failures))
          this.persisted.failures = parsed.failures.slice(-100);
        if (typeof parsed.lastAcknowledgedAt === 'string')
          this.persisted.lastAcknowledgedAt = parsed.lastAcknowledgedAt;
      } catch {
        // A missing or corrupt diagnostic snapshot never makes durable pending data unavailable.
      }
      await Promise.all([
        this.recoverExternalStateTemps('awaiting_spool_admission'),
        this.recoverExternalStateTemps('upload_blocked'),
        this.recoverExternalStateTemps('quarantine'),
      ]);
      await this.recoverStaging();
      await this.compactAcknowledgedBundles();
      await this.admitWaitingArtifacts();
      await this.refreshStatus();
      this.scheduleProcessing(0);
    } catch (error) {
      await this.releaseSpoolOwnership();
      throw error;
    }
  }

  private async acquireSpoolOwnership(): Promise<void> {
    const lock = join(this.spool, 'status', 'owner.lock');
    const temporary = join(
      this.spool,
      'status',
      `.owner-${this.ownerToken}.tmp`,
    );
    await writeOwnerJson(temporary, {
      pid: process.pid,
      token: this.ownerToken,
      acquired_at: new Date().toISOString(),
    });
    try {
      for (let attempt = 0; attempt < 3; attempt += 1) {
        try {
          await link(temporary, lock);
          await syncDirectory(dirname(lock));
          this.ownsSpool = true;
          this.installExitCleanup();
          return;
        } catch (error) {
          if (errorCode(error) !== 'EEXIST') throw error;
          const owner = await readOwnerLock(lock);
          if (owner && processAlive(owner.pid)) {
            throw new CloudUploaderError(
              'SPOOL_IN_USE',
              `spool is already owned by process ${owner.pid}`,
            );
          }
          await rm(lock, { force: true });
          await syncDirectory(dirname(lock));
        }
      }
      throw new CloudUploaderError(
        'SPOOL_IN_USE',
        'spool ownership could not be acquired',
      );
    } finally {
      await rm(temporary, { force: true });
    }
  }

  private async recoverExternalStateTemps(state: string): Promise<void> {
    const root = join(this.spool, state);
    for (const entry of await readdir(root, { withFileTypes: true })) {
      if (
        !entry.isDirectory() ||
        entry.isSymbolicLink() ||
        !entry.name.startsWith('.external-')
      )
        continue;
      const temporary = join(root, entry.name);
      try {
        const retained = await readExternalState(temporary);
        let destination = join(root, retained.entry_id);
        if (await exists(destination))
          destination = `${destination}-recovered-${randomUUID()}`;
        await rename(temporary, destination);
      } catch {
        const destination = join(root, `corrupt-external-${randomUUID()}`);
        await rename(temporary, destination);
      }
    }
    await syncDirectory(root);
  }

  private async releaseSpoolOwnership(): Promise<void> {
    if (!this.ownsSpool) return;
    const lock = join(this.spool, 'status', 'owner.lock');
    const owner = await readOwnerLock(lock);
    if (owner?.token === this.ownerToken) {
      await rm(lock, { force: true });
      await syncDirectory(dirname(lock));
    }
    this.ownsSpool = false;
    if (this.exitCleanup) process.off('exit', this.exitCleanup);
    this.exitCleanup = undefined;
  }

  private installExitCleanup(): void {
    if (this.exitCleanup) return;
    this.exitCleanup = () => {
      if (!this.ownsSpool) return;
      try {
        const owner = JSON.parse(
          readFileSync(join(this.spool, 'status', 'owner.lock'), 'utf8'),
        ) as {
          token?: unknown;
        };
        if (owner.token === this.ownerToken) {
          rmSync(join(this.spool, 'status', 'owner.lock'), { force: true });
        }
      } catch {
        // Process exit cleanup is best-effort; stale ownership is recovered on startup.
      }
    };
    process.once('exit', this.exitCleanup);
  }

  private async recoverStaging(): Promise<void> {
    const statusRoot = join(this.spool, 'status');
    for (const name of await listDirectories(statusRoot)) {
      if (!name.startsWith('staging-')) continue;
      const item = join(statusRoot, name);
      const bundle = join(item, 'bundle');
      let reservation: {
        bundle_digest?: unknown;
        bytes?: unknown;
        source_prevalidated?: unknown;
      } = {};
      try {
        reservation = JSON.parse(
          await readFile(join(item, 'reservation.json'), 'utf8'),
        ) as typeof reservation;
        if (
          reservation.source_prevalidated !== true ||
          typeof reservation.bundle_digest !== 'string'
        ) {
          throw new CloudUploaderError(
            'STAGING_ATTESTATION_MISSING',
            'crash staging lacks a safe pre-copy validation attestation',
          );
        }
        const validation = await validateArtifact(bundle, {
          profile: 'structural',
        });
        if (!validation.valid) {
          throw new CloudUploaderError(
            'STAGING_ARTIFACT_INVALID',
            'crash staging does not contain a complete structurally valid bundle',
          );
        }
        const inventory = await inventoryBundle(bundle);
        if (inventory.bundleDigest !== reservation.bundle_digest) {
          throw new CloudUploaderError(
            'STAGING_DIGEST_MISMATCH',
            'crash staging bytes do not match their prevalidated source digest',
          );
        }
        if (inventory.manifest.state !== 'sealed')
          throw new CloudUploaderError(
            'STAGING_NOT_SEALED',
            'crash staging is not sealed',
          );
        const destinations = [
          join(this.spool, 'pending', inventory.bundleDigest),
          join(this.spool, 'acked', inventory.bundleDigest),
          join(this.spool, 'upload_blocked', inventory.bundleDigest),
          join(this.spool, 'awaiting_spool_admission', inventory.bundleDigest),
          join(this.spool, 'quarantine', inventory.bundleDigest),
        ];
        if (await anyExists(destinations)) {
          await this.retainStagingFailure(
            item,
            name,
            reservation,
            'STAGING_DUPLICATE',
            'crash staging duplicates an already retained bundle state',
          );
          continue;
        }
        await rm(join(item, 'reservation.json'), { force: true });
        await syncDirectory(item);
        await rename(item, destinations[0]!);
        await syncDirectory(join(this.spool, 'pending'));
      } catch (error) {
        const failure =
          error instanceof CloudUploaderError
            ? error
            : new CloudUploaderError(
                'STAGING_INCOMPLETE',
                'crash staging is incomplete and requires operator action',
              );
        await this.retainStagingFailure(
          item,
          name,
          reservation,
          failure.code,
          failure.message,
        );
      }
    }
    await syncDirectory(statusRoot);
  }

  private async retainStagingFailure(
    item: string,
    name: string,
    reservation: { bytes?: unknown },
    code: string,
    message: string,
  ): Promise<void> {
    await rm(join(item, 'bundle'), { recursive: true, force: true });
    const sourceBytes =
      Number.isSafeInteger(reservation.bytes) &&
      (reservation.bytes as number) >= 0
        ? (reservation.bytes as number)
        : await directoryBytes(item);
    await writeOwnerJson(join(item, 'state.json'), {
      entry_id: name,
      source_path: '',
      source_bytes: sourceBytes,
      code,
      message,
      created_at: new Date().toISOString(),
    } satisfies ExternalSpoolState);
    const destinationRoot = join(this.spool, 'quarantine');
    let destination = join(destinationRoot, name);
    if (await exists(destination))
      destination = `${destination}-${randomUUID()}`;
    await rename(item, destination);
    await syncDirectory(destinationRoot);
  }

  private async compactAcknowledgedBundles(): Promise<void> {
    const root = join(this.spool, 'acked');
    const entries = await readdir(root, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isDirectory()) continue;
      const item = join(root, entry.name);
      let receipt: unknown;
      try {
        receipt = JSON.parse(await readFile(join(item, 'receipt.json'), 'utf8'));
      } catch {
        continue;
      }
      if (
        !isRecord(receipt) ||
        receipt.status !== 'acknowledged' ||
        receipt.bundle_digest !== entry.name ||
        typeof receipt.bundle_id !== 'string' ||
        typeof receipt.acknowledged_at !== 'string'
      ) {
        continue;
      }
      try {
        const bundle = join(item, 'bundle');
        if (!(await exists(bundle))) continue;
        const inventory = await inventoryBundle(bundle);
        if (
          receipt.bundle_id !== inventory.bundleId ||
          receipt.bundle_digest !== inventory.bundleDigest
        ) {
          continue;
        }
        await rm(join(item, 'bundle'), { recursive: true, force: true });
        await syncDirectory(item);
      } catch (error) {
        await this.recordFailure(
          entry.name,
          'ACK_COMPACTION_FAILED',
          error instanceof Error ? error.message : String(error),
        );
      }
    }
  }

  private async acknowledgementMatches(
    item: string,
    inventory: BundleInventory,
  ): Promise<boolean> {
    try {
      const receipt = JSON.parse(
        await readFile(join(item, 'receipt.json'), 'utf8'),
      ) as unknown;
      return (
        isRecord(receipt) &&
        receipt.status === 'acknowledged' &&
        receipt.bundle_id === inventory.bundleId &&
        receipt.bundle_digest === inventory.bundleDigest &&
        typeof receipt.acknowledged_at === 'string'
      );
    } catch {
      return false;
    }
  }

  private async pendingBundleMatches(
    item: string,
    inventory: BundleInventory,
  ): Promise<boolean> {
    try {
      const pending = await inventoryBundle(join(item, 'bundle'));
      return (
        pending.bundleId === inventory.bundleId &&
        pending.bundleDigest === inventory.bundleDigest &&
        pending.bytes === inventory.bytes
      );
    } catch {
      return false;
    }
  }

  private bootstrapStatusSynchronously(): void {
    for (const name of [
      'pending',
      'acked',
      'upload_blocked',
      'awaiting_spool_admission',
      'quarantine',
      'status',
    ]) {
      mkdirSync(join(this.spool, name), { recursive: true, mode: 0o700 });
    }
    chmodSync(this.spool, 0o700);
    try {
      const parsed = JSON.parse(
        readFileSync(join(this.spool, 'status', 'state.json'), 'utf8'),
      ) as Partial<PersistedState>;
      if (Array.isArray(parsed.failures))
        this.persisted.failures = parsed.failures.slice(-100);
      if (typeof parsed.lastAcknowledgedAt === 'string')
        this.persisted.lastAcknowledgedAt = parsed.lastAcknowledgedAt;
    } catch {
      // Async initialization uses the same safe missing/corrupt-state fallback.
    }
    const pending = describeItemsSync(join(this.spool, 'pending'));
    const acked = describeAcknowledgedItemsSync(join(this.spool, 'acked'));
    const blocked = describeRetainedItemsSync(
      join(this.spool, 'upload_blocked'),
    );
    const awaiting = describeExternalItemsSync(
      join(this.spool, 'awaiting_spool_admission'),
    );
    const quarantine = describeRetainedItemsSync(
      join(this.spool, 'quarantine'),
    );
    const reservedBytes = stagingReservationBytesSync(
      join(this.spool, 'status'),
    );
    const spoolBytes =
      pending.bytes +
      acked.bytes +
      blocked.spoolBytes +
      awaiting.spoolBytes +
      quarantine.spoolBytes +
      reservedBytes;
    const fraction = spoolBytes / this.maxSpoolBytes;
    const pressure =
      fraction >= 1
        ? 'full'
        : fraction >= 0.9
          ? 'critical'
          : fraction >= 0.7
            ? 'warning'
            : 'ok';
    const warnings: string[] = [];
    if (pressure === 'warning') warnings.push('SPOOL_USAGE_70_PERCENT');
    if (pressure === 'critical') warnings.push('SPOOL_USAGE_90_PERCENT');
    if (
      pressure === 'full' ||
      this.persisted.failures.some((failure) => failure.code === 'SPOOL_FULL')
    ) {
      warnings.push('SPOOL_FULL');
    }
    const key = this.currentKey();
    if (!key) warnings.push('INGEST_KEY_MISSING');
    this.snapshot = {
      lifecycle: 'running',
      pendingBundles: pending.count,
      ackedBundles: acked.count,
      blockedBundles: blocked.count,
      awaitingSpoolAdmissionBundles: awaiting.count,
      quarantineBundles: quarantine.count,
      pendingBytes: pending.bytes,
      blockedBytes: blocked.logicalBytes,
      awaitingSpoolAdmissionBytes: awaiting.sourceBytes,
      quarantineBytes: quarantine.logicalBytes,
      spoolBytes,
      maxSpoolBytes: this.maxSpoolBytes,
      pressure,
      warnings,
      oldestPending: pending.oldest
        ? {
            bundleDigest: pending.oldest.name,
            enqueuedAt: new Date(pending.oldest.time).toISOString(),
            ageMs: Math.max(0, Date.now() - pending.oldest.time),
          }
        : null,
      pausedAuth: !key,
      retryingBundles: 0,
      nextRetryAt: null,
      quotaLimited: this.persisted.failures.some(
        (failure) => failure.code === 'HTTP_429',
      ),
      lastAcknowledgedAt: this.persisted.lastAcknowledgedAt ?? null,
      lastRequestId: this.lastRequestId,
      failures: this.persisted.failures.map((failure) => ({ ...failure })),
    };
  }

  private async processPending(): Promise<void> {
    if (this.processing) return this.processing;
    this.processing = this.processPendingBatch().finally(() => {
      this.processing = undefined;
    });
    return this.processing;
  }

  private async processPendingBatch(): Promise<void> {
    if (this.stopped) return;
    if (this.snapshot.awaitingSpoolAdmissionBundles > 0) {
      const admission = this.admission.then(() => this.admitWaitingArtifacts());
      this.admission = admission.then(
        () => undefined,
        () => undefined,
      );
      await admission;
    }
    this.resumeIfKeyChanged();
    const key = this.currentKey();
    if (!key) {
      this.pausedKey = '';
      await this.refreshStatus();
      this.scheduleProcessing(1_000);
      return;
    }
    if (this.pausedKey !== undefined) return;
    const now = Date.now();
    const names = (await listDirectories(join(this.spool, 'pending')))
      .filter((name) => (this.retryAt.get(name) ?? 0) <= now)
      .slice(0, this.concurrency);
    if (names.length === 0) {
      if (this.snapshot.awaitingSpoolAdmissionBundles > 0)
        this.scheduleProcessing(1_000);
      return;
    }
    await Promise.all(names.map((digest) => this.uploadPending(digest, key)));
    await this.refreshStatus();
    if (this.snapshot.pendingBundles > 0) this.scheduleNextRetry();
    else if (this.snapshot.awaitingSpoolAdmissionBundles > 0)
      this.scheduleProcessing(1_000);
  }

  private async uploadPending(
    bundleDigest: string,
    key: string,
  ): Promise<void> {
    const item = join(this.spool, 'pending', bundleDigest);
    const bundle = join(item, 'bundle');
    let inventory: BundleInventory;
    let acknowledgedAt: string;
    try {
      inventory = await inventoryBundle(bundle);
      if (inventory.bundleDigest !== bundleDigest) {
        await this.quarantine(
          bundleDigest,
          'SPOOL_DIGEST_MISMATCH',
          'pending bundle digest no longer matches its path',
        );
        return;
      }
      await this.sendJson(
        `/v1/bundles/${encodeURIComponent(inventory.bundleId)}/begin`,
        {
          protocol_version: INGEST_PROTOCOL_VERSION,
          bundle_digest: inventory.bundleDigest,
          manifest: inventory.manifest,
          files: inventory.files.map((file) => ({
            file_id: file.file_id,
            path: file.path,
            size_bytes: file.size_bytes,
            sha256: file.sha256,
            parts: file.parts.length,
          })),
        },
        key,
      );
      for (const file of inventory.files) {
        const handle = await open(joinPortable(bundle, file.path), 'r');
        try {
          for (const part of file.parts) {
            const body = Buffer.allocUnsafe(part.size_bytes);
            let offset = 0;
            while (offset < part.size_bytes) {
              const { bytesRead } = await handle.read(
                body,
                offset,
                part.size_bytes - offset,
                part.index * PART_SIZE_BYTES + offset,
              );
              if (bytesRead === 0) {
                throw new CloudUploaderError(
                  'SPOOL_FILE_CHANGED',
                  'pending file changed during upload',
                );
              }
              offset += bytesRead;
            }
            if (sha256(body) !== part.sha256) {
              throw new CloudUploaderError(
                'SPOOL_FILE_CHANGED',
                'pending file changed during upload',
              );
            }
            await this.sendPart(
              `/v1/bundles/${encodeURIComponent(inventory.bundleId)}/files/${file.file_id}/parts/${part.index}`,
              body,
              part.sha256,
              key,
            );
          }
        } finally {
          await handle.close();
        }
      }
      const completion = await this.sendJson(
        `/v1/bundles/${encodeURIComponent(inventory.bundleId)}/complete`,
        {
          protocol_version: INGEST_PROTOCOL_VERSION,
          bundle_digest: inventory.bundleDigest,
        },
        key,
      );
      if (
        !isRecord(completion) ||
        completion.status !== 'complete' ||
        completion.bundle_digest !== inventory.bundleDigest
      ) {
        throw new CloudUploaderError(
          'INGEST_RESPONSE_INVALID',
          'ingest completion response did not acknowledge the expected bundle digest',
        );
      }
      acknowledgedAt = new Date().toISOString();
      await writeOwnerJson(join(item, 'receipt.json'), {
        protocol_version: INGEST_PROTOCOL_VERSION,
        bundle_id: inventory.bundleId,
        bundle_digest: inventory.bundleDigest,
        status: 'acknowledged',
        acknowledged_at: acknowledgedAt,
      });
    } catch (error) {
      if (this.stopped) return;
      const failure = classifyFailure(error);
      if (failure.kind === 'auth') {
        this.pausedKey = key;
        await this.recordFailure(bundleDigest, failure.code, failure.message);
      } else if (failure.kind === 'quarantine') {
        await this.quarantine(bundleDigest, failure.code, failure.message);
      } else if (failure.kind === 'blocked') {
        await this.blockPending(bundleDigest, failure.code, failure.message);
      } else {
        const attempts = (this.attempts.get(bundleDigest) ?? 0) + 1;
        this.attempts.set(bundleDigest, attempts);
        const ceiling = Math.min(
          MAX_BACKOFF_MS,
          BASE_BACKOFF_MS * 2 ** Math.min(attempts - 1, 20),
        );
        this.retryAt.set(
          bundleDigest,
          Date.now() + Math.floor(Math.random() * (ceiling + 1)),
        );
        await this.recordFailure(
          bundleDigest,
          failure.code,
          failure.message,
          attempts,
        );
      }
      return;
    }

    const acknowledgedItem = join(this.spool, 'acked', bundleDigest);
    try {
      await rename(item, acknowledgedItem);
      await Promise.all([
        syncDirectory(join(this.spool, 'pending')),
        syncDirectory(join(this.spool, 'acked')),
      ]);
      this.retryAt.delete(bundleDigest);
      this.attempts.delete(bundleDigest);
      this.persisted.lastAcknowledgedAt = acknowledgedAt;
      await this.clearFailure(bundleDigest);
      await writeOwnerJson(
        join(this.spool, 'status', 'state.json'),
        this.persisted,
      );
    } catch (error) {
      await this.recordFailure(
        bundleDigest,
        'ACK_STATE_COMMIT_FAILED',
        error instanceof Error ? error.message : String(error),
      );
      return;
    }

    try {
      await rm(join(acknowledgedItem, 'bundle'), {
        recursive: true,
        force: true,
      });
      await syncDirectory(acknowledgedItem);
    } catch (error) {
      await this.recordFailure(
        bundleDigest,
        'ACK_COMPACTION_FAILED',
        error instanceof Error ? error.message : String(error),
      );
    }
  }

  private async sendJson(
    path: string,
    body: unknown,
    key: string,
  ): Promise<unknown> {
    return this.withRequest(async (signal) => {
      const response = await this.fetchImpl(`${this.endpoint}${path}`, {
        method: 'POST',
        headers: {
          authorization: `Bearer ${key}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify(body),
        signal,
      });
      assertResponse(response);
      this.observeRequestId(response);
      try {
        return await response.json();
      } catch {
        throw new CloudUploaderError(
          'INGEST_RESPONSE_INVALID',
          'ingest returned an invalid JSON response',
        );
      }
    });
  }

  private async sendPart(
    path: string,
    body: Buffer,
    sha256: string,
    key: string,
  ): Promise<void> {
    await this.withRequest(async (signal) => {
      const response = await this.fetchImpl(`${this.endpoint}${path}`, {
        method: 'PUT',
        headers: {
          authorization: `Bearer ${key}`,
          'content-type': 'application/octet-stream',
          'x-semantic-layer-part-sha256': sha256,
        },
        body,
        signal,
      });
      assertResponse(response);
      this.observeRequestId(response);
    });
  }

  private async withRequest<Result>(
    action: (signal: AbortSignal) => Promise<Result>,
  ): Promise<Result> {
    this.assertRunning();
    const controller = new AbortController();
    this.activeRequests.add(controller);
    const timeout = setTimeout(() => controller.abort(), 30_000);
    timeout.unref?.();
    try {
      return await action(controller.signal);
    } finally {
      clearTimeout(timeout);
      this.activeRequests.delete(controller);
    }
  }

  private abortActiveRequests(): void {
    for (const controller of this.activeRequests) controller.abort();
  }

  private observeRequestId(response: Response): void {
    const requestId = response.headers.get('x-request-id');
    if (requestId && /^[A-Za-z0-9._:-]{1,128}$/u.test(requestId)) {
      this.lastRequestId = requestId;
    }
  }

  private async quarantine(
    bundleDigest: string,
    code: string,
    message: string,
  ): Promise<void> {
    await this.recordFailure(bundleDigest, code, message);
    const source = join(this.spool, 'pending', bundleDigest);
    if (await exists(source))
      await rename(source, join(this.spool, 'quarantine', bundleDigest));
    this.retryAt.delete(bundleDigest);
    this.attempts.delete(bundleDigest);
  }

  private async blockPending(
    bundleDigest: string,
    code: string,
    message: string,
  ): Promise<void> {
    await this.recordFailure(bundleDigest, code, message);
    const source = join(this.spool, 'pending', bundleDigest);
    if (await exists(source)) {
      let inventory: BundleInventory | undefined;
      try {
        inventory = await inventoryBundle(join(source, 'bundle'));
      } catch {
        // The immutable pending bytes are still retained even if reinventory fails.
      }
      await writeOwnerJson(
        join(source, 'state.json'),
        externalState(
          join(source, 'bundle'),
          new CloudUploaderError(code, message),
          inventory,
          bundleDigest,
        ),
      );
      await rename(source, join(this.spool, 'upload_blocked', bundleDigest));
      await syncDirectory(join(this.spool, 'upload_blocked'));
    }
    this.retryAt.delete(bundleDigest);
    this.attempts.delete(bundleDigest);
  }

  private async recordFailure(
    bundleDigest: string,
    code: string,
    message: string,
    attempts?: number,
  ): Promise<void> {
    const previous = this.persisted.failures.find(
      (failure) => failure.bundleDigest === bundleDigest,
    );
    this.persisted.failures = this.persisted.failures.filter(
      (failure) => failure.bundleDigest !== bundleDigest,
    );
    this.persisted.failures.push({
      bundleDigest,
      code,
      message,
      at: new Date().toISOString(),
      attempts: attempts ?? (previous?.attempts ?? 0) + 1,
    });
    this.persisted.failures = this.persisted.failures.slice(-100);
    await writeOwnerJson(
      join(this.spool, 'status', 'state.json'),
      this.persisted,
    );
  }

  private async clearFailure(bundleDigest: string): Promise<void> {
    const next = this.persisted.failures.filter(
      (failure) => failure.bundleDigest !== bundleDigest,
    );
    if (next.length === this.persisted.failures.length) return;
    this.persisted.failures = next;
    await writeOwnerJson(
      join(this.spool, 'status', 'state.json'),
      this.persisted,
    );
  }

  private async refreshStatus(): Promise<void> {
    const [pending, acked, blocked, awaiting, quarantine, reservedBytes] =
      await Promise.all([
        describeItems(join(this.spool, 'pending')),
        describeAcknowledgedItems(join(this.spool, 'acked')),
        describeRetainedItems(join(this.spool, 'upload_blocked')),
        describeExternalItems(join(this.spool, 'awaiting_spool_admission')),
        describeRetainedItems(join(this.spool, 'quarantine')),
        stagingReservationBytes(join(this.spool, 'status')),
      ]);
    const spoolBytes =
      pending.bytes +
      acked.bytes +
      blocked.spoolBytes +
      awaiting.spoolBytes +
      quarantine.spoolBytes +
      reservedBytes;
    const fraction = spoolBytes / this.maxSpoolBytes;
    const pressure =
      fraction >= 1
        ? 'full'
        : fraction >= 0.9
          ? 'critical'
          : fraction >= 0.7
            ? 'warning'
            : 'ok';
    const warnings: string[] = [];
    if (pressure === 'warning') warnings.push('SPOOL_USAGE_70_PERCENT');
    if (pressure === 'critical') warnings.push('SPOOL_USAGE_90_PERCENT');
    if (pressure === 'full') warnings.push('SPOOL_FULL');
    if (
      this.persisted.failures.some(
        (failure) => failure.code === 'SPOOL_FULL',
      ) &&
      !warnings.includes('SPOOL_FULL')
    ) {
      warnings.push('SPOOL_FULL');
    }
    const key = this.currentKey();
    const pausedAuth = !key || this.pausedKey !== undefined;
    if (!key) warnings.push('INGEST_KEY_MISSING');
    if (this.pausedKey !== undefined && key) warnings.push('AUTH_PAUSED');
    const oldest = pending.oldest;
    const retryTimes = [...this.retryAt.values()].filter(
      (value) => value > Date.now(),
    );
    const nextRetry = Math.min(...retryTimes);
    this.snapshot = {
      lifecycle: this.stopped ? 'shutdown' : 'running',
      pendingBundles: pending.count,
      ackedBundles: acked.count,
      blockedBundles: blocked.count,
      awaitingSpoolAdmissionBundles: awaiting.count,
      quarantineBundles: quarantine.count,
      pendingBytes: pending.bytes,
      blockedBytes: blocked.logicalBytes,
      awaitingSpoolAdmissionBytes: awaiting.sourceBytes,
      quarantineBytes: quarantine.logicalBytes,
      spoolBytes,
      maxSpoolBytes: this.maxSpoolBytes,
      pressure,
      warnings,
      oldestPending: oldest
        ? {
            bundleDigest: oldest.name,
            enqueuedAt: new Date(oldest.time).toISOString(),
            ageMs: Math.max(0, Date.now() - oldest.time),
          }
        : null,
      pausedAuth,
      retryingBundles: this.retryAt.size,
      nextRetryAt: Number.isFinite(nextRetry)
        ? new Date(nextRetry).toISOString()
        : null,
      quotaLimited: this.persisted.failures.some(
        (failure) => failure.code === 'HTTP_429',
      ),
      lastAcknowledgedAt: this.persisted.lastAcknowledgedAt ?? null,
      lastRequestId: this.lastRequestId,
      failures: this.persisted.failures.map((failure) => ({ ...failure })),
    };
  }

  private scheduleProcessing(delayMs: number): void {
    if (this.stopped) return;
    if (this.retryTimer) clearTimeout(this.retryTimer);
    this.retryTimer = setTimeout(() => {
      this.retryTimer = undefined;
      const scheduled = this.processPending()
        .then(() => this.refreshStatus())
        .catch(() => {
          if (!this.snapshot.warnings.includes('SPOOL_RUNTIME_ERROR')) {
            this.snapshot = {
              ...this.snapshot,
              warnings: [...this.snapshot.warnings, 'SPOOL_RUNTIME_ERROR'],
            };
          }
        })
        .finally(() => {
          if (this.scheduledProcessing === scheduled) {
            this.scheduledProcessing = undefined;
          }
        });
      this.scheduledProcessing = scheduled;
    }, delayMs);
    this.retryTimer.unref?.();
  }

  private scheduleNextRetry(): void {
    if (this.pausedKey !== undefined) {
      this.scheduleProcessing(1_000);
      return;
    }
    const now = Date.now();
    const next = Math.min(
      ...[...this.retryAt.values()].filter((value) => value > now),
    );
    this.scheduleProcessing(
      Number.isFinite(next) ? Math.max(0, next - now) : 0,
    );
  }

  private currentKey(): string | undefined {
    return this.configuredKey ?? process.env.SEMANTIC_LAYER_INGEST_KEY;
  }

  private resumeIfKeyChanged(): void {
    if (this.pausedKey === undefined) return;
    const key = this.currentKey() ?? '';
    if (key !== this.pausedKey) {
      this.pausedKey = undefined;
      this.snapshot = { ...this.snapshot, pausedAuth: !key };
      this.scheduleProcessing(0);
    }
  }

  private assertRunning(): void {
    if (this.stopped)
      throw new CloudUploaderError(
        'UPLOADER_SHUTDOWN',
        'uploader has shut down',
      );
  }
}

class HttpFailure extends Error {
  constructor(readonly status: number) {
    super(`ingest returned HTTP ${status}`);
  }
}

function assertResponse(response: Response): void {
  if (!response.ok) throw new HttpFailure(response.status);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function classifyFailure(error: unknown): {
  kind: 'retry' | 'auth' | 'blocked' | 'quarantine';
  code: string;
  message: string;
} {
  if (error instanceof HttpFailure) {
    if (error.status === 401 || error.status === 403) {
      return {
        kind: 'auth',
        code: `HTTP_${error.status}`,
        message: 'ingest authentication failed',
      };
    }
    if (error.status === 409) {
      return {
        kind: 'quarantine',
        code: 'DIGEST_CONFLICT',
        message: 'ingest rejected a conflicting bundle digest',
      };
    }
    if (error.status === 429 || error.status >= 500) {
      return {
        kind: 'retry',
        code: `HTTP_${error.status}`,
        message: `temporary ingest failure (HTTP ${error.status})`,
      };
    }
    return {
      kind: 'blocked',
      code: `HTTP_${error.status}`,
      message: `ingest permanently rejected bundle (HTTP ${error.status})`,
    };
  }
  if (error instanceof CloudUploaderError) {
    if (
      error.code === 'SPOOL_FILE_CHANGED' ||
      error.code === 'ARTIFACT_UNSAFE'
    ) {
      return { kind: 'quarantine', code: error.code, message: error.message };
    }
    return { kind: 'blocked', code: error.code, message: error.message };
  }
  return {
    kind: 'retry',
    code: 'NETWORK_ERROR',
    message: networkFailureMessage(error),
  };
}

function networkFailureMessage(error: unknown): string {
  if (error instanceof Error && error.name === 'AbortError')
    return 'ingest request timed out';
  const errorRecord = isRecord(error) ? error : undefined;
  const cause = isRecord(errorRecord?.cause) ? errorRecord.cause : undefined;
  const code =
    typeof cause?.code === 'string' && /^[A-Z0-9_]{1,64}$/u.test(cause.code)
      ? cause.code
      : undefined;
  return code ? `ingest request failed (${code})` : 'ingest request failed';
}

async function copyBundleSnapshot(
  source: string,
  destination: string,
  inventory: BundleInventory,
): Promise<void> {
  const sourceStat = await lstat(source);
  if (!sourceStat.isDirectory() || sourceStat.isSymbolicLink()) {
    throw new CloudUploaderError(
      'ARTIFACT_UNSAFE',
      'artifact root must be a real directory',
    );
  }
  await chmod(destination, 0o700);
  const directories = new Set([destination]);
  for (const file of inventory.files) {
    const sourcePath = joinPortable(source, file.path);
    const destinationPath = joinPortable(destination, file.path);
    const before = await lstat(sourcePath);
    if (
      !before.isFile() ||
      before.isSymbolicLink() ||
      before.size !== file.size_bytes
    ) {
      throw new CloudUploaderError(
        'SPOOL_FILE_CHANGED',
        'artifact changed while it was entering the durable spool',
      );
    }
    const parent = dirname(destinationPath);
    await mkdir(parent, { recursive: true, mode: 0o700 });
    directories.add(parent);
    const input = await open(
      sourcePath,
      constants.O_RDONLY | constants.O_NOFOLLOW,
    );
    let output: Awaited<ReturnType<typeof open>> | undefined;
    const digest = createHash('sha256');
    try {
      output = await open(destinationPath, 'wx', 0o600);
      const opened = await input.stat();
      if (
        !opened.isFile() ||
        opened.size !== file.size_bytes ||
        opened.dev !== before.dev ||
        opened.ino !== before.ino
      ) {
        throw new CloudUploaderError(
          'SPOOL_FILE_CHANGED',
          'artifact changed while it was entering the durable spool',
        );
      }
      let position = 0;
      while (position < file.size_bytes) {
        const buffer = Buffer.allocUnsafe(
          Math.min(1024 * 1024, file.size_bytes - position),
        );
        const { bytesRead } = await input.read(
          buffer,
          0,
          buffer.byteLength,
          position,
        );
        if (bytesRead === 0)
          throw new CloudUploaderError(
            'SPOOL_FILE_CHANGED',
            'artifact changed while it was entering the durable spool',
          );
        digest.update(buffer.subarray(0, bytesRead));
        let written = 0;
        while (written < bytesRead) {
          const result = await output.write(
            buffer,
            written,
            bytesRead - written,
            position + written,
          );
          if (result.bytesWritten === 0)
            throw new CloudUploaderError(
              'SPOOL_WRITE_FAILED',
              'durable spool write made no progress',
            );
          written += result.bytesWritten;
        }
        position += bytesRead;
      }
      const extra = await input.read(Buffer.alloc(1), 0, 1, file.size_bytes);
      const after = await input.stat();
      if (
        extra.bytesRead !== 0 ||
        after.size !== file.size_bytes ||
        digest.digest('hex') !== file.sha256
      ) {
        throw new CloudUploaderError(
          'SPOOL_FILE_CHANGED',
          'artifact changed while it was entering the durable spool',
        );
      }
      await output.sync();
    } finally {
      await Promise.allSettled([
        input.close(),
        ...(output ? [output.close()] : []),
      ]);
    }
  }
  for (const directory of [...directories].sort(
    (left, right) => right.length - left.length,
  )) {
    await syncDirectory(directory);
  }
}

async function inventoryBundle(bundle: string): Promise<BundleInventory> {
  const bundleStat = await lstat(bundle);
  if (!bundleStat.isDirectory() || bundleStat.isSymbolicLink()) {
    throw new CloudUploaderError(
      'ARTIFACT_UNSAFE',
      'bundle root must be a real directory',
    );
  }
  const realRoot = await realpath(bundle);
  const paths = await listFiles(bundle);
  const files: FileDescriptor[] = [];
  let bytes = 0;
  for (const path of paths.sort((left, right) =>
    Buffer.compare(Buffer.from(left), Buffer.from(right)),
  )) {
    const absolute = joinPortable(bundle, path);
    const entry = await lstat(absolute);
    if (!entry.isFile() || entry.isSymbolicLink()) {
      throw new CloudUploaderError(
        'ARTIFACT_UNSAFE',
        'artifact may contain only regular files and directories',
      );
    }
    if (entry.size > MAX_FILE_BYTES) {
      throw new CloudUploaderError(
        'FILE_TOO_LARGE',
        `artifact file exceeds ${MAX_FILE_BYTES} bytes`,
      );
    }
    if (bytes + entry.size > MAX_BUNDLE_BYTES) {
      throw new CloudUploaderError(
        'BUNDLE_TOO_LARGE',
        `artifact exceeds ${MAX_BUNDLE_BYTES} bytes`,
      );
    }
    await assertRealArtifactPath(realRoot, path, absolute);
    const file = await hashFile(absolute, entry);
    await assertRealArtifactPath(realRoot, path, absolute);
    if (file.size_bytes !== entry.size)
      throw new CloudUploaderError(
        'SPOOL_FILE_CHANGED',
        'artifact changed while it was inventoried',
      );
    bytes += file.size_bytes;
    files.push({
      file_id: createHash('sha256').update(path, 'utf8').digest('hex'),
      path,
      ...file,
    });
  }
  if (!paths.includes('manifest.json') || !paths.includes('trace.jsonl')) {
    throw new CloudUploaderError(
      'ARTIFACT_INVALID',
      'artifact must contain manifest.json and trace.jsonl',
    );
  }
  let manifest: Manifest;
  try {
    manifest = JSON.parse(
      await readFile(join(bundle, 'manifest.json'), 'utf8'),
    ) as Manifest;
  } catch {
    throw new CloudUploaderError(
      'ARTIFACT_INVALID',
      'manifest.json is not valid JSON',
    );
  }
  if (typeof manifest.bundle_id !== 'string') {
    throw new CloudUploaderError(
      'ARTIFACT_INVALID',
      'manifest bundle_id is missing',
    );
  }
  return {
    bundleId: manifest.bundle_id,
    bundleDigest: computeBundleDigest(files),
    manifest,
    files,
    bytes,
  };
}

function assertManifestAccepted(
  manifest: Manifest,
  installationId: string | undefined,
): void {
  if (
    manifest.schema !== 'semantic_trace_manifest_v1' &&
    manifest.schema !== 'semantic_trace_manifest_v2'
  ) {
    throw new CloudUploaderError(
      'MANIFEST_VERSION_UNSUPPORTED',
      'manifest schema must be semantic_trace_manifest_v1 or semantic_trace_manifest_v2',
    );
  }
  if (manifest.record_schema !== 'semantic_trace_record_v1') {
    throw new CloudUploaderError(
      'RECORD_VERSION_UNSUPPORTED',
      'manifest record_schema must be semantic_trace_record_v1',
    );
  }
  if (manifest.schema !== 'semantic_trace_manifest_v2') return;
  const manifestInstallation = manifest.installation_id;
  if (
    manifestInstallation !== undefined &&
    (typeof manifestInstallation !== 'string' ||
      !INSTALLATION_ID.test(manifestInstallation))
  ) {
    throw new CloudUploaderError(
      'MANIFEST_INSTALLATION_ID_INVALID',
      'manifest installation_id is not a valid opaque installation identifier',
    );
  }
  if (installationId !== undefined && manifestInstallation !== installationId) {
    throw new CloudUploaderError(
      'INSTALLATION_ID_MISMATCH',
      'managed manifest installation_id does not match the configured installation',
    );
  }
}

function externalEntryId(source: string): string {
  return createHash('sha256')
    .update(EXTERNAL_ENTRY_DOMAIN)
    .update(resolve(source), 'utf8')
    .digest('hex');
}

function safeExternalSourcePath(
  source: string,
  credential: string | undefined,
): string {
  const absolute = resolve(source);
  if (!credential) return absolute;
  const bytes = Buffer.from(credential, 'utf8');
  const variants = [
    credential,
    encodeURIComponent(credential),
    bytes.toString('base64'),
    bytes.toString('base64url'),
  ];
  return variants.some(
    (variant) => variant.length >= 8 && absolute.includes(variant),
  )
    ? ''
    : absolute;
}

function externalState(
  source: string,
  error: CloudUploaderError,
  inventory: BundleInventory | undefined,
  entryId: string,
  sourceBytes = inventory?.bytes ?? 0,
): ExternalSpoolState {
  return {
    entry_id: entryId,
    source_path: source ? resolve(source) : '',
    ...(inventory
      ? {
          bundle_id: inventory.bundleId,
          bundle_digest: inventory.bundleDigest,
        }
      : {}),
    source_bytes: sourceBytes,
    code: error.code,
    message: error.message,
    created_at: new Date().toISOString(),
  };
}

async function readExternalState(item: string): Promise<ExternalSpoolState> {
  const value = JSON.parse(
    await readFile(join(item, 'state.json'), 'utf8'),
  ) as Partial<ExternalSpoolState>;
  if (
    typeof value.entry_id !== 'string' ||
    typeof value.source_path !== 'string' ||
    !Number.isSafeInteger(value.source_bytes) ||
    (value.source_bytes as number) < 0 ||
    typeof value.code !== 'string' ||
    typeof value.message !== 'string' ||
    typeof value.created_at !== 'string'
  ) {
    throw new CloudUploaderError(
      'SPOOL_STATE_INVALID',
      'external spool state is invalid',
    );
  }
  return value as ExternalSpoolState;
}

async function sourceBytesSafely(source: string): Promise<number> {
  try {
    const info = await lstat(source);
    if (info.isFile() && !info.isSymbolicLink()) return info.size;
    if (info.isDirectory() && !info.isSymbolicLink())
      return directoryBytes(source);
  } catch {
    // State remains useful even when a rejected source cannot be measured safely.
  }
  return 0;
}

async function hashFile(
  path: string,
  expected: Awaited<ReturnType<typeof lstat>>,
): Promise<Pick<FileDescriptor, 'size_bytes' | 'sha256' | 'parts'>> {
  const whole = createHash('sha256');
  const parts: PartDescriptor[] = [];
  let size = 0;
  let partIndex = 0;
  const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const opened = await handle.stat();
    if (
      opened.dev !== expected.dev ||
      opened.ino !== expected.ino ||
      opened.size !== expected.size
    ) {
      throw new CloudUploaderError(
        'SPOOL_FILE_CHANGED',
        'artifact changed while it was inventoried',
      );
    }
    let hasNextPart = true;
    while (hasNextPart) {
      const buffer = Buffer.allocUnsafe(PART_SIZE_BYTES);
      let partBytes = 0;
      while (partBytes < PART_SIZE_BYTES) {
        const { bytesRead } = await handle.read(
          buffer,
          partBytes,
          PART_SIZE_BYTES - partBytes,
          size + partBytes,
        );
        if (bytesRead === 0) break;
        partBytes += bytesRead;
      }
      if (partBytes === 0) {
        hasNextPart = false;
        continue;
      }
      const part = buffer.subarray(0, partBytes);
      whole.update(part);
      size += partBytes;
      parts.push({
        index: partIndex++,
        size_bytes: partBytes,
        sha256: sha256(part),
      });
      hasNextPart = partBytes === PART_SIZE_BYTES;
    }
  } finally {
    await handle.close();
  }
  return { size_bytes: size, sha256: whole.digest('hex'), parts };
}

async function assertRealArtifactPath(
  realRoot: string,
  portablePath: string,
  absolute: string,
): Promise<void> {
  const expected = resolve(realRoot, ...portablePath.split('/'));
  const actual = await realpath(absolute);
  if (
    actual !== expected ||
    (actual !== realRoot && !actual.startsWith(`${realRoot}${sep}`))
  ) {
    throw new CloudUploaderError(
      'ARTIFACT_UNSAFE',
      'artifact path traverses a symbolic-link or replaced parent directory',
    );
  }
}

function sha256(bytes: Buffer): string {
  return createHash('sha256').update(bytes).digest('hex');
}

async function listFiles(root: string, prefix = ''): Promise<string[]> {
  const output: string[] = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const relative = prefix ? posix.join(prefix, entry.name) : entry.name;
    const absolute = join(root, entry.name);
    const entryStat = await lstat(absolute);
    if (entryStat.isSymbolicLink())
      throw new CloudUploaderError(
        'ARTIFACT_UNSAFE',
        'artifact must not contain symbolic links',
      );
    if (entryStat.isDirectory())
      output.push(...(await listFiles(absolute, relative)));
    else if (entryStat.isFile()) output.push(relative);
    else
      throw new CloudUploaderError(
        'ARTIFACT_UNSAFE',
        'artifact may contain only regular files and directories',
      );
  }
  return output;
}

function joinPortable(root: string, portablePath: string): string {
  if (
    portablePath.startsWith('/') ||
    portablePath
      .split('/')
      .some((part) => part === '' || part === '.' || part === '..')
  ) {
    throw new CloudUploaderError(
      'ARTIFACT_UNSAFE',
      'artifact contains an unsafe path',
    );
  }
  const absolute = resolve(root, ...portablePath.split('/'));
  if (absolute !== root && !absolute.startsWith(`${resolve(root)}${sep}`)) {
    throw new CloudUploaderError(
      'ARTIFACT_UNSAFE',
      'artifact contains an unsafe path',
    );
  }
  return absolute;
}

async function listDirectories(root: string): Promise<string[]> {
  try {
    const entries = await readdir(root, { withFileTypes: true });
    return entries
      .filter(
        (entry) =>
          entry.isDirectory() &&
          !entry.isSymbolicLink() &&
          !entry.name.startsWith('.'),
      )
      .map((entry) => entry.name)
      .sort();
  } catch {
    return [];
  }
}

async function describeItems(root: string): Promise<{
  count: number;
  bytes: number;
  oldest: { name: string; time: number } | null;
}> {
  const names = await listDirectories(root);
  let bytes = 0;
  let oldest: { name: string; time: number } | null = null;
  for (const name of names) {
    const item = join(root, name);
    bytes += await directoryBytes(item);
    const info = await stat(item);
    const time = info.birthtimeMs || info.ctimeMs || info.mtimeMs;
    if (!oldest || time < oldest.time) oldest = { name, time };
  }
  return { count: names.length, bytes, oldest };
}

async function describeAcknowledgedItems(root: string): Promise<{
  count: number;
  bytes: number;
  oldest: { name: string; time: number } | null;
}> {
  const described = await describeItems(root);
  let count = 0;
  for (const name of await listDirectories(root)) {
    try {
      const item = join(root, name);
      const receipt = JSON.parse(
        await readFile(join(item, 'receipt.json'), 'utf8'),
      ) as unknown;
      let valid =
        isRecord(receipt) &&
        receipt.status === 'acknowledged' &&
        receipt.bundle_digest === name &&
        typeof receipt.bundle_id === 'string' &&
        typeof receipt.acknowledged_at === 'string';
      const bundle = join(item, 'bundle');
      if (valid && isRecord(receipt) && (await exists(bundle))) {
        const inventory = await inventoryBundle(bundle);
        valid =
          receipt.bundle_id === inventory.bundleId &&
          receipt.bundle_digest === inventory.bundleDigest;
      }
      if (valid) count += 1;
    } catch {
      // Invalid acknowledgement directories still consume spool bytes.
    }
  }
  return { ...described, count };
}

function describeItemsSync(root: string): {
  count: number;
  bytes: number;
  oldest: { name: string; time: number } | null;
} {
  let names: string[];
  try {
    names = readdirSync(root, { withFileTypes: true })
      .filter(
        (entry) =>
          entry.isDirectory() &&
          !entry.isSymbolicLink() &&
          !entry.name.startsWith('.'),
      )
      .map((entry) => entry.name)
      .sort();
  } catch {
    names = [];
  }
  let bytes = 0;
  let oldest: { name: string; time: number } | null = null;
  for (const name of names) {
    const item = join(root, name);
    bytes += directoryBytesSync(item);
    const info = statSync(item);
    const time = info.birthtimeMs || info.ctimeMs || info.mtimeMs;
    if (!oldest || time < oldest.time) oldest = { name, time };
  }
  return { count: names.length, bytes, oldest };
}

function describeAcknowledgedItemsSync(root: string): {
  count: number;
  bytes: number;
  oldest: { name: string; time: number } | null;
} {
  const described = describeItemsSync(root);
  let count = 0;
  let names: string[] = [];
  try {
    names = readdirSync(root, { withFileTypes: true })
      .filter(
        (entry) =>
          entry.isDirectory() &&
          !entry.isSymbolicLink() &&
          !entry.name.startsWith('.'),
      )
      .map((entry) => entry.name);
  } catch {
    // The async initializer creates the directory and refreshes status.
  }
  for (const name of names) {
    try {
      const receipt = JSON.parse(
        readFileSync(join(root, name, 'receipt.json'), 'utf8'),
      ) as unknown;
      if (
        isRecord(receipt) &&
        receipt.status === 'acknowledged' &&
        receipt.bundle_digest === name &&
        typeof receipt.bundle_id === 'string' &&
        typeof receipt.acknowledged_at === 'string'
      ) {
        try {
          lstatSync(join(root, name, 'bundle'));
        } catch {
          count += 1;
        }
      }
    } catch {
      // Invalid acknowledgement directories still consume spool bytes.
    }
  }
  return { ...described, count };
}

async function describeRetainedItems(root: string): Promise<{
  count: number;
  logicalBytes: number;
  spoolBytes: number;
}> {
  const names = await listDirectories(root);
  let logicalBytes = 0;
  let spoolBytes = 0;
  for (const name of names) {
    const item = join(root, name);
    const itemBytes = await directoryBytes(item);
    spoolBytes += itemBytes;
    try {
      logicalBytes += (await readExternalState(item)).source_bytes;
    } catch {
      logicalBytes += itemBytes;
    }
  }
  return { count: names.length, logicalBytes, spoolBytes };
}

function describeRetainedItemsSync(root: string): {
  count: number;
  logicalBytes: number;
  spoolBytes: number;
} {
  let names: string[];
  try {
    names = readdirSync(root, { withFileTypes: true })
      .filter(
        (entry) =>
          entry.isDirectory() &&
          !entry.isSymbolicLink() &&
          !entry.name.startsWith('.'),
      )
      .map((entry) => entry.name)
      .sort();
  } catch {
    names = [];
  }
  let logicalBytes = 0;
  let spoolBytes = 0;
  for (const name of names) {
    const item = join(root, name);
    const itemBytes = directoryBytesSync(item);
    spoolBytes += itemBytes;
    try {
      const value = JSON.parse(
        readFileSync(join(item, 'state.json'), 'utf8'),
      ) as { source_bytes?: unknown };
      logicalBytes +=
        Number.isSafeInteger(value.source_bytes) &&
        (value.source_bytes as number) >= 0
          ? (value.source_bytes as number)
          : itemBytes;
    } catch {
      logicalBytes += itemBytes;
    }
  }
  return { count: names.length, logicalBytes, spoolBytes };
}

async function describeExternalItems(root: string): Promise<{
  count: number;
  sourceBytes: number;
  spoolBytes: number;
}> {
  const names = await listDirectories(root);
  let sourceBytes = 0;
  let spoolBytes = 0;
  for (const name of names) {
    const item = join(root, name);
    spoolBytes += await directoryBytes(item);
    try {
      sourceBytes += (await readExternalState(item)).source_bytes;
    } catch {
      // Corrupt entries still count toward spool capacity and remain operator-visible.
    }
  }
  return { count: names.length, sourceBytes, spoolBytes };
}

function describeExternalItemsSync(root: string): {
  count: number;
  sourceBytes: number;
  spoolBytes: number;
} {
  let names: string[];
  try {
    names = readdirSync(root, { withFileTypes: true })
      .filter(
        (entry) =>
          entry.isDirectory() &&
          !entry.isSymbolicLink() &&
          !entry.name.startsWith('.'),
      )
      .map((entry) => entry.name)
      .sort();
  } catch {
    names = [];
  }
  let sourceBytes = 0;
  let spoolBytes = 0;
  for (const name of names) {
    const item = join(root, name);
    spoolBytes += directoryBytesSync(item);
    try {
      const value = JSON.parse(
        readFileSync(join(item, 'state.json'), 'utf8'),
      ) as { source_bytes?: unknown };
      if (
        Number.isSafeInteger(value.source_bytes) &&
        (value.source_bytes as number) >= 0
      ) {
        sourceBytes += value.source_bytes as number;
      }
    } catch {
      // Corrupt entries still count toward spool capacity and remain operator-visible.
    }
  }
  return { count: names.length, sourceBytes, spoolBytes };
}

function stagingReservationBytesSync(statusRoot: string): number {
  let total = 0;
  for (const name of listStagingDirectoriesSync(statusRoot)) {
    const item = join(statusRoot, name);
    try {
      const reservation = JSON.parse(
        readFileSync(join(item, 'reservation.json'), 'utf8'),
      ) as { bytes?: unknown };
      if (
        !Number.isSafeInteger(reservation.bytes) ||
        (reservation.bytes as number) < 0
      )
        throw new Error('invalid reservation');
      total += reservation.bytes as number;
    } catch {
      total += directoryBytesSync(item);
    }
  }
  return total;
}

async function stagingReservationBytes(statusRoot: string): Promise<number> {
  let total = 0;
  for (const name of (await listDirectories(statusRoot)).filter((entry) =>
    entry.startsWith('staging-'),
  )) {
    const item = join(statusRoot, name);
    try {
      const reservation = JSON.parse(
        await readFile(join(item, 'reservation.json'), 'utf8'),
      ) as { bytes?: unknown };
      if (
        !Number.isSafeInteger(reservation.bytes) ||
        (reservation.bytes as number) < 0
      )
        throw new Error('invalid reservation');
      total += reservation.bytes as number;
    } catch {
      total += await directoryBytes(item);
    }
  }
  return total;
}

function listStagingDirectoriesSync(statusRoot: string): string[] {
  try {
    return readdirSync(statusRoot, { withFileTypes: true })
      .filter(
        (entry) =>
          entry.isDirectory() &&
          !entry.isSymbolicLink() &&
          entry.name.startsWith('staging-'),
      )
      .map((entry) => entry.name)
      .sort();
  } catch {
    return [];
  }
}

function directoryBytesSync(root: string): number {
  let bytes = 0;
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    const info = lstatSync(path);
    if (info.isDirectory() && !info.isSymbolicLink())
      bytes += directoryBytesSync(path);
    else if (info.isFile()) bytes += info.size;
  }
  return bytes;
}

async function directoryBytes(root: string): Promise<number> {
  let bytes = 0;
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) bytes += await directoryBytes(path);
    else if (entry.isFile()) bytes += (await stat(path)).size;
  }
  return bytes;
}

async function writeOwnerJson(path: string, value: unknown): Promise<void> {
  await mkdir(dirname(path), { recursive: true, mode: 0o700 });
  const temporary = join(
    dirname(path),
    `.${basename(path)}.${randomUUID()}.tmp`,
  );
  const handle = await open(temporary, 'w', 0o600);
  try {
    await handle.writeFile(`${JSON.stringify(value, null, 2)}\n`);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporary, path);
  await syncDirectory(dirname(path));
}

async function syncDirectory(path: string): Promise<void> {
  let handle: Awaited<ReturnType<typeof open>> | undefined;
  try {
    handle = await open(path, 'r');
    await handle.sync();
  } catch (error) {
    const code =
      typeof error === 'object' && error !== null && 'code' in error
        ? String(error.code)
        : '';
    if (!['EINVAL', 'ENOTSUP', 'EPERM', 'EISDIR'].includes(code)) throw error;
  } finally {
    await handle?.close();
  }
}

async function removeDirectArtifactChild(
  artifactPath: string,
  outputDirectory: string,
): Promise<void> {
  const metadata = await lstat(artifactPath);
  if (!metadata.isDirectory() || metadata.isSymbolicLink())
    throw new Error('sealed capture path is not a regular directory');
  const [outputRoot, artifact] = await Promise.all([
    realpath(resolve(outputDirectory)),
    realpath(resolve(artifactPath)),
  ]);
  if (dirname(artifact) !== outputRoot)
    throw new Error('sealed capture path is outside the capture output root');
  await rm(artifact, { recursive: true, force: true });
}

async function exists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch {
    return false;
  }
}

async function anyExists(paths: readonly string[]): Promise<boolean> {
  return (await Promise.all(paths.map(exists))).some(Boolean);
}

async function readOwnerLock(
  path: string,
): Promise<{ pid: number; token: string } | undefined> {
  try {
    const value = JSON.parse(await readFile(path, 'utf8')) as {
      pid?: unknown;
      token?: unknown;
    };
    if (
      Number.isSafeInteger(value.pid) &&
      (value.pid as number) > 0 &&
      typeof value.token === 'string'
    ) {
      return { pid: value.pid as number, token: value.token };
    }
  } catch {
    // A malformed lock cannot identify a live owner and is recovered as stale.
  }
  return undefined;
}

function processAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return errorCode(error) !== 'ESRCH';
  }
}

function errorCode(error: unknown): string {
  return typeof error === 'object' && error !== null && 'code' in error
    ? String(error.code)
    : '';
}

function emptyStatus(maxSpoolBytes: number): CloudUploaderStatus {
  return {
    lifecycle: 'running',
    pendingBundles: 0,
    ackedBundles: 0,
    blockedBundles: 0,
    awaitingSpoolAdmissionBundles: 0,
    quarantineBundles: 0,
    pendingBytes: 0,
    blockedBytes: 0,
    awaitingSpoolAdmissionBytes: 0,
    quarantineBytes: 0,
    spoolBytes: 0,
    maxSpoolBytes,
    pressure: 'ok',
    warnings: [],
    oldestPending: null,
    pausedAuth: false,
    retryingBundles: 0,
    nextRetryAt: null,
    quotaLimited: false,
    lastAcknowledgedAt: null,
    lastRequestId: null,
    failures: [],
  };
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}
