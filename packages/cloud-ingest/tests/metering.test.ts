import { describe, expect, it } from 'vitest';
import {
  acquireBundleIngestLease,
  finalizeApprovedDeletion,
  prepareApprovedDeletion,
  recordApprovedDeletion,
  recordMeterFailure,
  releaseBundleIngestLease,
  reserveUpload,
  updatePreparedDeletionAccounting,
} from '../src/metering.js';
import { MemoryObjectStore } from '../src/storage.js';

const path = 'metering/tenants/tenant_a1/installations/install_AAAAAAAAAAAAAAAAAAAAAA/ledger.json';

describe('per-installation metering', () => {
  it('uses conditional storage updates so concurrent begins cannot over-admit', async () => {
    const store = new MemoryObjectStore();
    const limits = { maxActiveUploads: 1, maxIncompleteBytes: 1024 };
    const results = await Promise.all([
      reserveUpload(store, path, 'bundle_abc', 'a'.repeat(64), 100, limits),
      reserveUpload(store, path, 'bundle_def', 'b'.repeat(64), 100, limits),
    ]);
    expect(results.filter((result) => result.admitted)).toHaveLength(1);
    expect(results.filter((result) => result.reason === 'capacity')).toHaveLength(1);
  });

  it('keeps bounded safe failure counters without storing request content', async () => {
    const store = new MemoryObjectStore();
    await recordMeterFailure(store, path, 'auth');
    await recordMeterFailure(store, path, 'validation');
    await recordMeterFailure(store, path, 'conflict');
    const ledger = JSON.parse((await store.read(path))!.toString('utf8')) as Record<string, unknown>;
    expect(ledger).toMatchObject({ auth_failures: 1, validation_failures: 1, conflict_failures: 1 });
    expect(JSON.stringify(ledger)).not.toContain('authorization');
  });

  it('resets accepted bundle and byte counters at the rolling-day boundary', async () => {
    const store = new MemoryObjectStore();
    await store.writeConditional(path, Buffer.from(JSON.stringify({
      schema: 'semantic_layer_ingest_meter_v1',
      window_started_at: '2020-01-01T00:00:00.000Z',
      accepted_bundles: 99,
      accepted_bytes: 9999,
      stored_completed_bytes: 10,
      auth_failures: 0,
      validation_failures: 0,
      conflict_failures: 0,
      active: {},
      deleted: {},
    })), null);
    await reserveUpload(store, path, 'bundle_abc', 'a'.repeat(64), 100, {
      maxActiveUploads: 64, maxIncompleteBytes: 1024,
    });
    expect(JSON.parse((await store.read(path))!.toString('utf8'))).toMatchObject({
      accepted_bundles: 1,
      accepted_bytes: 100,
      stored_completed_bytes: 10,
    });
  });

  it('never reuses an immutable bundle ID after an approved deletion', async () => {
    const store = new MemoryObjectStore();
    const digest = 'a'.repeat(64);
    await recordApprovedDeletion(store, path, 'bundle_abc', digest, 0);
    const reservation = await reserveUpload(store, path, 'bundle_abc', digest, 100, {
      maxActiveUploads: 64, maxIncompleteBytes: 1024,
    });
    expect(reservation).toMatchObject({ admitted: false, created: false, reason: 'conflict' });
  });

  it('quiesces live ingest before preparing an irreversible deletion tombstone', async () => {
    const store = new MemoryObjectStore();
    const digest = 'a'.repeat(64);
    const lease = await acquireBundleIngestLease(store, path, 'bundle_abc');
    expect(lease).toBeDefined();
    expect(await prepareApprovedDeletion(store, path, 'bundle_abc', digest, 0)).toBe('busy');
    await releaseBundleIngestLease(store, path, 'bundle_abc', lease!);
    expect(await prepareApprovedDeletion(store, path, 'bundle_abc', digest, 0)).toBe('prepared');
    expect(await acquireBundleIngestLease(store, path, 'bundle_abc')).toBeUndefined();
    expect(await prepareApprovedDeletion(store, path, 'bundle_abc', digest, 0)).toBe('pending');
    await expect(finalizeApprovedDeletion(store, path, 'bundle_abc', digest)).rejects.toThrow(
      'approved deletion accounting was not prepared',
    );
    await updatePreparedDeletionAccounting(store, path, 'bundle_abc', digest, 0);
    await expect(updatePreparedDeletionAccounting(store, path, 'bundle_abc', digest, 1)).rejects.toThrow(
      'approved deletion accounting is already finalized',
    );
    await finalizeApprovedDeletion(store, path, 'bundle_abc', digest);
    expect(await prepareApprovedDeletion(store, path, 'bundle_abc', digest, 0)).toBe('deleted');
  });

  it('treats valid IDs that match Object prototype names as ordinary bundle IDs', async () => {
    const store = new MemoryObjectStore();
    const reservation = await reserveUpload(store, path, 'constructor', 'a'.repeat(64), 100, {
      maxActiveUploads: 64, maxIncompleteBytes: 1024,
    });
    expect(reservation).toMatchObject({ admitted: true, created: true });
  });
});
