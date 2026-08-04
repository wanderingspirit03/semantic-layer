import { createHash, randomUUID } from 'node:crypto';
import type { ObjectStore } from './storage.js';

export type MeterLimits = {
  maxActiveUploads: number;
  maxIncompleteBytes: number;
};

export const DEFAULT_METER_LIMITS: MeterLimits = {
  maxActiveUploads: 64,
  maxIncompleteBytes: 16 * 1024 * 1024 * 1024,
};
const ROLLING_WINDOW_MS = 24 * 60 * 60 * 1000;

type ActiveUpload = { digest: string; bytes: number; admitted_at: string };
type Ledger = {
  schema: 'semantic_layer_ingest_meter_v1';
  window_started_at: string;
  accepted_bundles: number;
  accepted_bytes: number;
  stored_completed_bytes: number;
  auth_failures: number;
  validation_failures: number;
  conflict_failures: number;
  active: Record<string, ActiveUpload>;
  ingest_leases: Record<string, Record<string, string>>;
  pending_deletions: Record<string, { digest: string; completed_bytes: number; accounting_ready: boolean }>;
  deleted: Record<string, string>;
};

export type Reservation = {
  admitted: boolean;
  created: boolean;
  reason?: 'capacity' | 'conflict';
  activeUploads: number;
  incompleteBytes: number;
  pressure: 'ok' | 'warning' | 'critical' | 'hard';
};

export type MeterStatus = {
  acceptedBundles: number;
  acceptedBytes: number;
  storedCompletedBytes: number;
  activeUploads: number;
  incompleteBytes: number;
  pendingDeletions: number;
};

export async function readMeterStatus(store: ObjectStore, path: string): Promise<MeterStatus> {
  const current = await store.readVersioned(path);
  const ledger = parseLedger(current.bytes);
  return {
    acceptedBundles: ledger.accepted_bundles,
    acceptedBytes: ledger.accepted_bytes,
    storedCompletedBytes: ledger.stored_completed_bytes,
    activeUploads: Object.keys(ledger.active).length,
    incompleteBytes: Object.values(ledger.active).reduce((total, upload) => total + upload.bytes, 0),
    pendingDeletions: Object.keys(ledger.pending_deletions).length,
  };
}

export async function reserveUpload(
  store: ObjectStore,
  path: string,
  bundleId: string,
  digest: string,
  bytes: number,
  limits: MeterLimits,
): Promise<Reservation> {
  return updateLedger<Reservation>(store, path, (ledger) => {
    const prior = ownValue(ledger.active, bundleId);
    const incompleteBytes = Object.values(ledger.active).reduce((total, upload) => total + upload.bytes, 0);
    const activeUploads = Object.keys(ledger.active).length;
    if (ownValue(ledger.deleted, bundleId) || ownValue(ledger.pending_deletions, bundleId)) return {
      ledger,
      result: { admitted: false, created: false, reason: 'conflict', ...meterStats(activeUploads, incompleteBytes, limits) },
      write: false,
    };
    if (prior) return {
      ledger,
      result: prior.digest === digest
        ? { admitted: true, created: false, ...meterStats(activeUploads, incompleteBytes, limits) }
        : { admitted: false, created: false, reason: 'conflict', ...meterStats(activeUploads, incompleteBytes, limits) },
      write: false,
    };
    if (activeUploads >= limits.maxActiveUploads || incompleteBytes + bytes > limits.maxIncompleteBytes) {
      return {
        ledger,
        result: { admitted: false, created: false, reason: 'capacity', activeUploads, incompleteBytes, pressure: 'hard' },
        write: false,
      };
    }
    ledger.active[bundleId] = { digest, bytes, admitted_at: new Date().toISOString() };
    ledger.accepted_bundles += 1;
    ledger.accepted_bytes += bytes;
    return {
      ledger,
      result: { admitted: true, created: true, ...meterStats(activeUploads + 1, incompleteBytes + bytes, limits) },
      write: true,
    };
  });
}

function meterStats(activeUploads: number, incompleteBytes: number, limits: MeterLimits) {
  const ratio = Math.max(activeUploads / limits.maxActiveUploads, incompleteBytes / limits.maxIncompleteBytes);
  const pressure = ratio >= 0.9 ? 'critical' : ratio >= 0.7 ? 'warning' : 'ok';
  return { activeUploads, incompleteBytes, pressure } as const;
}

export async function cancelReservation(
  store: ObjectStore,
  path: string,
  bundleId: string,
  digest: string,
): Promise<void> {
  await updateLedger(store, path, (ledger) => {
    const prior = ownValue(ledger.active, bundleId);
    if (!prior || prior.digest !== digest) return { ledger, result: undefined, write: false };
    delete ledger.active[bundleId];
    ledger.accepted_bundles = Math.max(0, ledger.accepted_bundles - 1);
    ledger.accepted_bytes = Math.max(0, ledger.accepted_bytes - prior.bytes);
    return { ledger, result: undefined, write: true };
  });
}

export async function finishUpload(
  store: ObjectStore,
  path: string,
  bundleId: string,
  digest: string,
): Promise<void> {
  await updateLedger(store, path, (ledger) => {
    const prior = ownValue(ledger.active, bundleId);
    if (!prior || prior.digest !== digest) return { ledger, result: undefined, write: false };
    delete ledger.active[bundleId];
    ledger.stored_completed_bytes += prior.bytes;
    return { ledger, result: undefined, write: true };
  });
}

export async function recordMeterFailure(
  store: ObjectStore,
  path: string,
  kind: 'auth' | 'validation' | 'conflict',
): Promise<void> {
  await updateLedger(store, path, (ledger) => {
    if (kind === 'auth') ledger.auth_failures += 1;
    else if (kind === 'validation') ledger.validation_failures += 1;
    else ledger.conflict_failures += 1;
    return { ledger, result: undefined, write: true };
  });
}

export async function recordApprovedDeletion(
  store: ObjectStore,
  path: string,
  bundleId: string,
  digest: string,
  completedBytes: number,
): Promise<void> {
  const prepared = await prepareApprovedDeletion(store, path, bundleId, digest, completedBytes);
  if (prepared === 'busy') throw new Error('approved deletion is blocked by active ingest');
  if (prepared === 'deleted') return;
  await updatePreparedDeletionAccounting(store, path, bundleId, digest, completedBytes);
  await finalizeApprovedDeletion(store, path, bundleId, digest);
}

export async function acquireBundleIngestLease(
  store: ObjectStore,
  path: string,
  bundleId: string,
): Promise<string | undefined> {
  const leaseId = randomUUID();
  const acquiredAt = new Date().toISOString();
  return updateLedger<string | undefined>(store, path, (ledger) => {
    if (ownValue(ledger.deleted, bundleId) || ownValue(ledger.pending_deletions, bundleId)) {
      return { ledger, result: undefined, write: false };
    }
    const leases = ownValue(ledger.ingest_leases, bundleId) ?? {};
    leases[leaseId] = acquiredAt;
    ledger.ingest_leases[bundleId] = leases;
    return { ledger, result: leaseId, write: true };
  });
}

export async function releaseBundleIngestLease(
  store: ObjectStore,
  path: string,
  bundleId: string,
  leaseId: string,
): Promise<void> {
  await updateLedger(store, path, (ledger) => {
    const leases = ownValue(ledger.ingest_leases, bundleId);
    if (!leases || !Object.hasOwn(leases, leaseId)) return { ledger, result: undefined, write: false };
    delete leases[leaseId];
    if (Object.keys(leases).length === 0) delete ledger.ingest_leases[bundleId];
    return { ledger, result: undefined, write: true };
  });
}

export async function readBundleIngestLeaseStatus(
  store: ObjectStore,
  path: string,
  bundleId: string,
): Promise<{ count: number; fingerprint: string }> {
  const current = await store.readVersioned(path);
  const leases = ownValue(parseLedger(current.bytes).ingest_leases, bundleId) ?? {};
  return { count: Object.keys(leases).length, fingerprint: leaseFingerprint(leases) };
}

export async function clearBundleIngestLeases(
  store: ObjectStore,
  path: string,
  bundleId: string,
  expectedFingerprint: string,
): Promise<'cleared' | 'empty' | 'changed'> {
  return updateLedger(store, path, (ledger) => {
    const leases = ownValue(ledger.ingest_leases, bundleId) ?? {};
    if (Object.keys(leases).length === 0) return { ledger, result: 'empty' as const, write: false };
    if (leaseFingerprint(leases) !== expectedFingerprint) {
      return { ledger, result: 'changed' as const, write: false };
    }
    delete ledger.ingest_leases[bundleId];
    return { ledger, result: 'cleared' as const, write: true };
  });
}

export type DeletionPreparation = 'prepared' | 'pending' | 'deleted' | 'busy';

export async function prepareApprovedDeletion(
  store: ObjectStore,
  path: string,
  bundleId: string,
  digest: string,
  completedBytes: number,
): Promise<DeletionPreparation> {
  return updateLedger<DeletionPreparation>(store, path, (ledger) => {
    const deleted = ownValue(ledger.deleted, bundleId);
    if (deleted) {
      if (deleted !== digest) throw new Error('approved deletion conflicts with deletion tombstone');
      return { ledger, result: 'deleted', write: false };
    }
    const pending = ownValue(ledger.pending_deletions, bundleId);
    if (pending) {
      if (pending.digest !== digest || pending.completed_bytes !== completedBytes) throw new Error('approved deletion conflicts with pending deletion');
      return { ledger, result: 'pending', write: false };
    }
    if (Object.keys(ownValue(ledger.ingest_leases, bundleId) ?? {}).length > 0) {
      return { ledger, result: 'busy', write: false };
    }
    ledger.pending_deletions[bundleId] = { digest, completed_bytes: completedBytes, accounting_ready: false };
    return { ledger, result: 'prepared', write: true };
  });
}

export async function pendingApprovedDeletion(
  store: ObjectStore,
  path: string,
  bundleId: string,
): Promise<{ digest: string; completedBytes: number; accountingReady: boolean } | undefined> {
  const current = await store.readVersioned(path);
  const pending = ownValue(parseLedger(current.bytes).pending_deletions, bundleId);
  return pending ? {
    digest: pending.digest,
    completedBytes: pending.completed_bytes,
    accountingReady: pending.accounting_ready,
  } : undefined;
}

export async function completedApprovedDeletion(
  store: ObjectStore,
  path: string,
  bundleId: string,
): Promise<string | undefined> {
  const current = await store.readVersioned(path);
  return ownValue(parseLedger(current.bytes).deleted, bundleId);
}

export async function updatePreparedDeletionAccounting(
  store: ObjectStore,
  path: string,
  bundleId: string,
  digest: string,
  completedBytes: number,
): Promise<void> {
  await updateLedger(store, path, (ledger) => {
    const pending = ownValue(ledger.pending_deletions, bundleId);
    if (!pending || pending.digest !== digest) throw new Error('approved deletion was not prepared');
    if (pending.accounting_ready) {
      if (pending.completed_bytes !== completedBytes) {
        throw new Error('approved deletion accounting is already finalized');
      }
      return { ledger, result: undefined, write: false };
    }
    pending.completed_bytes = completedBytes;
    pending.accounting_ready = true;
    return { ledger, result: undefined, write: true };
  });
}

export async function finalizeApprovedDeletion(
  store: ObjectStore,
  path: string,
  bundleId: string,
  digest: string,
): Promise<void> {
  await updateLedger(store, path, (ledger) => {
    if (ownValue(ledger.deleted, bundleId) === digest) return { ledger, result: undefined, write: false };
    const pending = ownValue(ledger.pending_deletions, bundleId);
    if (!pending || pending.digest !== digest) throw new Error('approved deletion was not prepared');
    if (!pending.accounting_ready) throw new Error('approved deletion accounting was not prepared');
    const active = ownValue(ledger.active, bundleId);
    if (active && active.digest === digest) delete ledger.active[bundleId];
    ledger.stored_completed_bytes = Math.max(0, ledger.stored_completed_bytes - pending.completed_bytes);
    delete ledger.pending_deletions[bundleId];
    ledger.deleted[bundleId] = digest;
    return { ledger, result: undefined, write: true };
  });
}

async function updateLedger<T>(
  store: ObjectStore,
  path: string,
  mutate: (ledger: Ledger) => { ledger: Ledger; result: T; write: boolean },
): Promise<T> {
  for (let attempt = 0; attempt < 16; attempt += 1) {
    const current = await store.readVersioned(path);
    const ledger = parseLedger(current.bytes);
    const windowStarted = Date.parse(ledger.window_started_at);
    if (!Number.isFinite(windowStarted) || Date.now() - windowStarted >= ROLLING_WINDOW_MS) {
      ledger.window_started_at = new Date().toISOString();
      ledger.accepted_bundles = 0;
      ledger.accepted_bytes = 0;
    }
    const update = mutate(ledger);
    if (!update.write) return update.result;
    if (await store.writeConditional(path, Buffer.from(JSON.stringify(update.ledger)), current.version)) return update.result;
  }
  throw new Error('meter ledger contention exceeded retry budget');
}

function parseLedger(bytes: Buffer | undefined): Ledger {
  if (!bytes) return {
    schema: 'semantic_layer_ingest_meter_v1',
    window_started_at: new Date().toISOString(),
    accepted_bundles: 0,
    accepted_bytes: 0,
    stored_completed_bytes: 0,
    auth_failures: 0,
    validation_failures: 0,
    conflict_failures: 0,
    active: {},
    ingest_leases: {},
    pending_deletions: {},
    deleted: {},
  };
  const value = JSON.parse(bytes.toString('utf8')) as Ledger;
  if (value.schema !== 'semantic_layer_ingest_meter_v1' || !value.active || typeof value.active !== 'object') {
    throw new Error('meter ledger is invalid');
  }
  value.auth_failures ??= 0;
  value.validation_failures ??= 0;
  value.conflict_failures ??= 0;
  value.ingest_leases ??= {};
  value.deleted ??= {};
  value.pending_deletions ??= {};
  for (const pending of Object.values(value.pending_deletions)) pending.accounting_ready ??= false;
  return value;
}

function ownValue<T>(record: Record<string, T>, key: string): T | undefined {
  return Object.hasOwn(record, key) ? record[key] : undefined;
}

function leaseFingerprint(leases: Record<string, string>): string {
  const entries = Object.entries(leases).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0);
  return createHash('sha256').update(JSON.stringify(entries)).digest('hex');
}
