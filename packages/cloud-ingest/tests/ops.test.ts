import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, it } from 'vitest';
import {
  acquireBundleIngestLease,
  finishUpload,
  prepareApprovedDeletion,
  readBundleIngestLeaseStatus,
  reserveUpload,
} from '../src/metering.js';
import { runOps } from '../src/ops.js';
import { bundleDigest, sha256 } from '../src/protocol.js';
import { MemoryObjectStore } from '../src/storage.js';

const roots: string[] = [];
const installation = 'install_AAAAAAAAAAAAAAAAAAAAAA';
afterEach(async () => Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true }))));

it('lists and fetches complete bundles while deletion requires exact confirmation', async () => {
  const store = new MemoryObjectStore();
  const meterStore = new MemoryObjectStore();
  const prefix = `tenants/tenant_a1/installations/${installation}/bundles/bundle_abc/`;
  const manifest = Buffer.from('{}');
  const trace = Buffer.alloc(0);
  await store.writeImmutable(`${prefix}manifest.json`, manifest);
  await store.writeImmutable(`${prefix}trace.jsonl`, trace);
  const digest = bundleDigest([
    { path: 'manifest.json', size_bytes: manifest.byteLength, sha256: sha256(manifest) },
    { path: 'trace.jsonl', size_bytes: trace.byteLength, sha256: sha256(trace) },
  ]);
  await store.writeImmutable(`${prefix}complete.json`, Buffer.from(JSON.stringify({
    protocol_version: '1', bundle_id: 'bundle_abc', bundle_digest: digest,
  })));
  const uploadPrefix = `uploads/tenants/tenant_a1/installations/${installation}/bundles/bundle_abc/`;
  await store.writeImmutable(`${uploadPrefix}begin.json`, Buffer.from('{}'));
  const meterPath = `metering/tenants/tenant_a1/installations/${installation}/ledger.json`;
  await reserveUpload(meterStore, meterPath, 'bundle_abc', digest, manifest.byteLength + trace.byteLength, {
    maxActiveUploads: 64, maxIncompleteBytes: 1024 * 1024,
  });
  await finishUpload(meterStore, meterPath, 'bundle_abc', digest);
  const output: string[] = [];
  expect(await runOps(['list', 'tenant_a1', installation], { store, output: (line) => output.push(line) })).toBe(0);
  expect(output).toContain(`tenant_a1/${installation}/bundle_abc`);
  expect(await runOps(['list-incomplete', 'tenant_a1', installation], { store, output: (line) => output.push(line) })).toBe(0);
  expect(output).toContain(`tenant_a1/${installation}/bundle_abc`);
  const meterOutput: string[] = [];
  expect(await runOps(['meter-status', 'tenant_a1', installation], { store, meterStore, output: (line) => meterOutput.push(line) })).toBe(0);
  expect(JSON.parse(meterOutput[0]!)).toMatchObject({ active_uploads: 0, stored_completed_bytes: 2, pending_deletions: 0 });
  const root = await mkdtemp(join(tmpdir(), 'ingest-ops-')); roots.push(root);
  const destination = join(root, 'bundle');
  expect(await runOps(['fetch', 'tenant_a1', installation, 'bundle_abc', destination], { store, output: () => undefined })).toBe(0);
  expect(await readFile(join(destination, 'manifest.json'), 'utf8')).toBe('{}');
  expect(await runOps(['fetch', 'tenant_a1', installation, 'bundle_abc', destination], { store, output: () => undefined })).toBe(2);
  expect(await runOps(['delete', 'tenant_a1', installation, 'bundle_abc'], { store, output: () => undefined })).toBe(0);
  expect(await store.read(`${prefix}complete.json`)).toBeDefined();
  expect(await runOps(['delete', 'tenant_a1', installation, 'bundle_abc', `--confirm=tenant_a1/${installation}/bundle_abc`], { store, output: () => undefined })).toBe(2);
  expect(await store.read(`${prefix}complete.json`)).toBeDefined();
  await prepareApprovedDeletion(meterStore, meterPath, 'bundle_abc', digest, 0);
  expect(await runOps([
    'delete', 'tenant_a1', installation, 'bundle_abc',
    `--confirm=tenant_a1/${installation}/bundle_abc`, '--approval=change_1234',
  ], { store, meterStore, output: () => undefined })).toBe(0);
  expect(await store.read(`${prefix}complete.json`)).toBeUndefined();
  const audit = await store.list(`audit/tenants/tenant_a1/installations/${installation}/deletions/`);
  expect(audit).toHaveLength(1);
  expect(JSON.parse((await store.read(audit[0]!))!.toString('utf8'))).toMatchObject({
    tenant: 'tenant_a1', installation, bundle: 'bundle_abc', approval: 'change_1234', object_count: 4,
  });
  expect(await store.list(uploadPrefix)).toEqual([]);
  expect(JSON.parse((await meterStore.read(meterPath))!.toString('utf8'))).toMatchObject({
    stored_completed_bytes: 0,
    deleted: { bundle_abc: digest },
  });
  expect(await runOps([
    'delete', 'tenant_a1', installation, 'bundle_abc',
    `--confirm=tenant_a1/${installation}/bundle_abc`, '--approval=change_1234',
  ], { store, meterStore, output: () => undefined })).toBe(0);
  expect(await store.list(`audit/tenants/tenant_a1/installations/${installation}/deletions/`)).toHaveLength(1);
});

it('refuses to validate a prefix until its completion marker exists', async () => {
  const store = new MemoryObjectStore();
  const prefix = `tenants/tenant_a1/installations/${installation}/bundles/bundle_abc/`;
  await store.writeImmutable(`${prefix}manifest.json`, Buffer.from('{}'));
  await store.writeImmutable(`${prefix}trace.jsonl`, Buffer.alloc(0));
  const output: string[] = [];
  expect(await runOps(['validate', 'tenant_a1', installation, 'bundle_abc'], { store, output: (line) => output.push(line) })).toBe(2);
  expect(output).toEqual(['bundle is not complete']);
});

it('refuses a completion marker whose identity or digest does not match the stored files', async () => {
  const store = new MemoryObjectStore();
  const prefix = `tenants/tenant_a1/installations/${installation}/bundles/bundle_abc/`;
  await store.writeImmutable(`${prefix}manifest.json`, Buffer.from('{}'));
  await store.writeImmutable(`${prefix}trace.jsonl`, Buffer.alloc(0));
  await store.writeImmutable(`${prefix}complete.json`, Buffer.from(JSON.stringify({
    protocol_version: '1', bundle_id: 'bundle_other', bundle_digest: 'a'.repeat(64),
  })));
  const output: string[] = [];
  expect(await runOps(['validate', 'tenant_a1', installation, 'bundle_abc'], { store, output: (line) => output.push(line) })).toBe(2);
  expect(output).toEqual(['bundle completion marker is invalid']);
});

it('reads legacy v1 bundles only through explicit legacy commands', async () => {
  const store = new MemoryObjectStore();
  const prefix = 'tenants/tenant_a1/bundles/bundle_abc/';
  const manifest = Buffer.from('{}');
  const trace = Buffer.alloc(0);
  await store.writeImmutable(`${prefix}manifest.json`, manifest);
  await store.writeImmutable(`${prefix}trace.jsonl`, trace);
  await store.writeImmutable(`${prefix}complete.json`, Buffer.from(JSON.stringify({
    protocol_version: '1', bundle_id: 'bundle_abc', bundle_digest: bundleDigest([
      { path: 'manifest.json', size_bytes: manifest.byteLength, sha256: sha256(manifest) },
      { path: 'trace.jsonl', size_bytes: 0, sha256: sha256(trace) },
    ]),
  })));
  const root = await mkdtemp(join(tmpdir(), 'legacy-ingest-ops-')); roots.push(root);
  expect(await runOps(['fetch', 'tenant_a1', installation, 'bundle_abc', join(root, 'wrong')], { store, output: () => undefined })).toBe(2);
  expect(await runOps(['legacy-fetch', 'tenant_a1', 'bundle_abc', join(root, 'bundle')], { store, output: () => undefined })).toBe(0);
  expect(await readFile(join(root, 'bundle', 'manifest.json'), 'utf8')).toBe('{}');
  expect(await runOps(['legacy-delete', 'tenant_a1', 'bundle_abc'], { store, output: () => undefined })).toBe(2);
});

it('recovers an orphaned ingest lease only with drained-scope confirmation and durable audit', async () => {
  const store = new MemoryObjectStore();
  const meterStore = new MemoryObjectStore();
  const meterPath = `metering/tenants/tenant_a1/installations/${installation}/ledger.json`;
  expect(await acquireBundleIngestLease(meterStore, meterPath, 'bundle_abc')).toBeDefined();
  expect(await runOps([
    'clear-ingest-leases', 'tenant_a1', installation, 'bundle_abc', '--approval=incident_1234',
  ], { store, meterStore, output: () => undefined })).toBe(2);
  expect((await readBundleIngestLeaseStatus(meterStore, meterPath, 'bundle_abc')).count).toBe(1);
  expect(await runOps([
    'clear-ingest-leases', 'tenant_a1', installation, 'bundle_abc',
    `--confirm-drained=tenant_a1/${installation}/bundle_abc`, '--approval=incident_1234',
  ], { store, meterStore, output: () => undefined })).toBe(0);
  expect((await readBundleIngestLeaseStatus(meterStore, meterPath, 'bundle_abc')).count).toBe(0);
  const audits = await store.list(`audit/tenants/tenant_a1/installations/${installation}/lease-recovery/`);
  expect(audits).toHaveLength(1);
  expect(JSON.parse((await store.read(audits[0]!))!.toString('utf8'))).toMatchObject({
    kind: 'bundle_ingest_lease_recovery', approval: 'incident_1234', lease_count: 1,
  });
});
