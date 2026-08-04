import { createHash } from 'node:crypto';
import { afterEach, describe, expect, it } from 'vitest';
import { createIngestServer } from '../src/http.js';
import { recordApprovedDeletion, reserveUpload } from '../src/metering.js';
import { runOps } from '../src/ops.js';
import { bundleDigest, sha256 } from '../src/protocol.js';
import { MemoryObjectStore } from '../src/storage.js';

const key = 'test-secret-key-with-enough-entropy';
const tenant = 'tenant_a1';
const installation = 'install_AAAAAAAAAAAAAAAAAAAAAA';

function manifest(trace: Buffer) {
  return {
    schema: 'semantic_trace_manifest_v1', record_schema: 'semantic_trace_record_v1',
    bundle_id: 'bundle_abc', state: 'sealed',
    sdk: { language: 'typescript', version: '0.2.0' }, privacy_mode: 'production-safe',
    started_at: '2026-07-25T12:00:00.000Z', updated_at: '2026-07-25T12:00:01.000Z',
    sealed_at: '2026-07-25T12:00:01.000Z',
    sources: [{ id: 'source_sdk', name: 'test' }],
    trace: { path: 'trace.jsonl', records: 0, last_seq: 0, bytes: trace.byteLength, losses: 0, sha256: sha256(trace) },
    blobs: { path: 'blobs', count: 0, bytes: 0 },
  };
}

function fixture(manifestOverride?: ReturnType<typeof manifest>) {
  const trace = Buffer.alloc(0);
  const value = manifestOverride ?? manifest(trace);
  const manifestBytes = Buffer.from(`${JSON.stringify(value, null, 2)}\n`);
  const files = [
    { file_id: sha256(Buffer.from('manifest.json')), path: 'manifest.json', size_bytes: manifestBytes.byteLength, sha256: sha256(manifestBytes), parts: 1 },
    { file_id: sha256(Buffer.from('trace.jsonl')), path: 'trace.jsonl', size_bytes: 0, sha256: sha256(trace), parts: 0 },
  ];
  return { trace, value, manifestBytes, files, digest: bundleDigest(files) };
}

async function start(
  keyRegistry: Parameters<typeof createIngestServer>[0]['keyRegistry'] = {
    [sha256(Buffer.from(key))]: { tenant_id: tenant, installation_id: installation, status: 'active' },
  },
  overrides: Partial<Parameters<typeof createIngestServer>[0]> = {},
) {
  const store = new MemoryObjectStore();
  const meterStore = new MemoryObjectStore();
  const service = createIngestServer({
    store,
    meterStore,
    keyRegistry,
    logger: () => undefined,
    ...overrides,
  });
  await new Promise<void>((resolve) => service.server.listen(0, '127.0.0.1', resolve));
  const address = service.server.address();
  if (!address || typeof address === 'string') throw new Error('missing address');
  return { ...service, store, meterStore, url: `http://127.0.0.1:${address.port}` };
}

const servers: Array<ReturnType<typeof createIngestServer>> = [];
afterEach(async () => Promise.all(servers.splice(0).map((item) => item.close())));

async function request(url: string, path: string, init: RequestInit = {}) {
  return fetch(`${url}${path}`, { ...init, headers: { authorization: `Bearer ${key}`, ...init.headers } });
}

async function requestAs(url: string, token: string, path: string, init: RequestInit = {}) {
  return fetch(`${url}${path}`, { ...init, headers: { authorization: `Bearer ${token}`, ...init.headers } });
}

describe('cloud ingest HTTP contract', () => {
  it('returns one safe request ID header on success and error responses', async () => {
    const service = await start(); servers.push(service);
    const supplied = '018f47e2-8c2d-7a5f-8d3c-3b32d45e71aa';
    const health = await fetch(`${service.url}/health`, { headers: { 'x-request-id': supplied.toUpperCase() } });
    expect(health.status).toBe(200);
    expect(health.headers.get('x-request-id')).toBe(supplied);
    const item = fixture();
    const begun = await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-request-id': supplied.toUpperCase() },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }),
    });
    expect(begun.status).toBe(201);
    expect(begun.headers.get('x-request-id')).toBe(supplied);
    const denied = await fetch(`${service.url}/v1/bundles/bundle_abc/begin`, {
      method: 'POST', headers: { 'x-request-id': 'unsafe request ID' },
    });
    expect(denied.status).toBe(401);
    expect(denied.headers.get('x-request-id')).toMatch(/^[0-9a-f-]{36}$/u);
    expect((await denied.json() as { request_id: string }).request_id).toBe(denied.headers.get('x-request-id'));
  });

  it('is healthy without exposing tenant data and rejects invalid credentials', async () => {
    const service = await start(); servers.push(service);
    const health = await fetch(`${service.url}/health`);
    expect(health.status).toBe(200);
    expect(await health.json()).toEqual({ status: 'ok' });
    expect((await fetch(`${service.url}/internal/health`)).status).toBe(401);
    const denied = await fetch(`${service.url}/v1/bundles/bundle_abc/begin`, { method: 'POST' });
    expect(denied.status).toBe(401);
  });

  it('verifies that an active key belongs to the requested installation without writing state', async () => {
    const secondKey = 'second-test-secret-key-with-enough-entropy';
    const secondInstallation = 'install_BBBBBBBBBBBBBBBBBBBBBB';
    const legacyKey = 'legacy-test-secret-key-with-enough-entropy';
    const service = await start({
      [sha256(Buffer.from(key))]: {
        tenant_id: tenant,
        installation_id: installation,
        status: 'active',
      },
      [sha256(Buffer.from(secondKey))]: {
        tenant_id: tenant,
        installation_id: secondInstallation,
        status: 'active',
      },
      [sha256(Buffer.from(legacyKey))]: tenant,
    });
    servers.push(service);
    const verify = (token: string, installationId: unknown) =>
      requestAs(service.url, token, '/v1/auth/verify', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ installation_id: installationId }),
      });

    const accepted = await verify(key, installation);
    expect(accepted.status).toBe(200);
    expect(await accepted.json()).toEqual({ status: 'ok' });
    expect(await service.store.list('')).toEqual([]);
    expect(await service.meterStore.list('')).toEqual([]);
    expect((await verify(key, secondInstallation)).status).toBe(403);
    expect((await verify(secondKey, secondInstallation)).status).toBe(200);
    expect((await verify(key, 'customer-vm-1')).status).toBe(400);
    expect((await verify(legacyKey, installation)).status).toBe(403);
    expect(await service.store.list('')).toEqual([]);
  });

  it('uploads exact parts and publishes complete.json last', async () => {
    const service = await start(); servers.push(service);
    const item = fixture();
    const begin = await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }),
    });
    expect(begin.status).toBe(201);
    const part = await request(service.url, `/v1/bundles/bundle_abc/files/${item.files[0]!.file_id}/parts/0`, {
      method: 'PUT', headers: { 'x-semantic-layer-part-sha256': sha256(item.manifestBytes) }, body: item.manifestBytes,
    });
    expect(part.status).toBe(201);
    const complete = await request(service.url, '/v1/bundles/bundle_abc/complete', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest }),
    });
    expect(complete.status).toBe(201);
    const prefix = `tenants/${tenant}/installations/${installation}/bundles/bundle_abc/`;
    expect(await service.store.read(`${prefix}manifest.json`)).toEqual(item.manifestBytes);
    expect(JSON.parse((await service.store.read(`${prefix}complete.json`))!.toString())).toMatchObject({ bundle_digest: item.digest });
    expect(service.store.writes.at(-1)).toBe(`${prefix}complete.json`);
    expect((await request(service.url, '/v1/bundles/bundle_abc/complete', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest }) })).status).toBe(200);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }) })).status).toBe(200);
  });

  it('makes exact replay idempotent and rejects conflicting immutable writes', async () => {
    const service = await start(); servers.push(service);
    const item = fixture();
    const body = JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files });
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body })).status).toBe(201);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body })).status).toBe(200);
    const changed = JSON.parse(body); changed.bundle_digest = createHash('sha256').update('different').digest('hex');
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(changed) })).status).toBe(400);
    const conflictingFiles = item.files.map((file, index) => index === 0 ? { ...file, sha256: 'f'.repeat(64) } : file);
    const conflicting = { protocol_version: '1', bundle_digest: bundleDigest(conflictingFiles), manifest: item.value, files: conflictingFiles };
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(conflicting) })).status).toBe(409);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        protocol_version: '1', bundle_digest: item.digest, files: item.files,
        manifest: { ...item.value, sdk: { ...item.value.sdk, version: '0.2.1' } },
      }),
    })).status).toBe(409);
  });

  it('rejects unsafe descriptors, oversize files, and completion with missing parts', async () => {
    const service = await start(); servers.push(service);
    const item = fixture();
    const unsafeFiles = item.files.map((file, index) => index === 0 ? { ...file, path: '../manifest.json' } : file);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ protocol_version: '1', bundle_digest: bundleDigest(unsafeFiles), manifest: item.value, files: unsafeFiles }) })).status).toBe(400);
    const extraPropertyFiles = item.files.map((file, index) => index === 0 ? { ...file, unexpected: true } : file);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: extraPropertyFiles }) })).status).toBe(400);
    const oversizeFiles = item.files.map((file, index) => index === 0 ? { ...file, size_bytes: 256 * 1024 * 1024 + 1, parts: 33 } : file);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ protocol_version: '1', bundle_digest: bundleDigest(oversizeFiles), manifest: item.value, files: oversizeFiles }) })).status).toBe(400);
    const body = JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files, tenant: 'tenant_attacker' });
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body })).status).toBe(400);
    expect((await service.store.list('uploads/tenant_attacker/')).length).toBe(0);
    const validBody = JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files });
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', { method: 'POST', headers: { 'content-type': 'application/json' }, body: validBody })).status).toBe(201);
    expect((await request(service.url, '/v1/bundles/bundle_abc/complete', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest }) })).status).toBe(400);
  });

  it('rejects an unsealed manifest before reserving incomplete-upload capacity', async () => {
    const service = await start(); servers.push(service);
    const item = fixture({ ...manifest(Buffer.alloc(0)), state: 'open', sealed_at: undefined } as ReturnType<typeof manifest>);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }),
    })).status).toBe(400);
    const ledgerPath = `metering/tenants/${tenant}/installations/${installation}/ledger.json`;
    expect(JSON.parse((await service.meterStore.read(ledgerPath))!.toString('utf8'))).toMatchObject({
      accepted_bundles: 0,
      accepted_bytes: 0,
      active: {},
      validation_failures: 1,
    });
  });

  it('enforces the exact versioned completion request shape', async () => {
    const service = await start(); servers.push(service);
    const digest = 'a'.repeat(64);
    const requestComplete = (body: unknown) => request(service.url, '/v1/bundles/bundle_abc/complete', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body),
    });
    expect((await requestComplete({ bundle_digest: digest })).status).toBe(400);
    expect((await requestComplete({ protocol_version: '2', bundle_digest: digest })).status).toBe(400);
    expect((await requestComplete({ protocol_version: '1', bundle_digest: digest, tenant: 'tenant_attacker' })).status).toBe(400);
  });

  it('keeps legacy tenant-only prefixes read-only while allowing exact v1 replay', async () => {
    const service = await start({ [sha256(Buffer.from(key))]: tenant }); servers.push(service);
    const item = fixture();
    const body = JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files });
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })).status).toBe(403);
    expect(await service.store.list(`uploads/${tenant}/bundle_abc/`)).toEqual([]);
    await service.store.writeImmutable(
      `tenants/${tenant}/bundles/bundle_abc/complete.json`,
      Buffer.from(JSON.stringify({ protocol_version: '1', bundle_id: 'bundle_abc', bundle_digest: item.digest })),
    );
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })).status).toBe(200);
    await service.store.delete([`tenants/${tenant}/bundles/bundle_abc/complete.json`]);
    await service.store.writeImmutable(
      `tenants/${tenant}/bundles/bundle_abc/complete.json`,
      Buffer.from(JSON.stringify({ bundle_digest: item.digest, injected: true })),
    );
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })).status).toBe(500);
  });

  it('reconciles metering on managed begin replay after a completion acknowledgement failure', async () => {
    const service = await start(); servers.push(service);
    const item = fixture();
    const meterPath = `metering/tenants/${tenant}/installations/${installation}/ledger.json`;
    const totalBytes = item.files.reduce((total, file) => total + file.size_bytes, 0);
    await reserveUpload(service.meterStore, meterPath, 'bundle_abc', item.digest, totalBytes, {
      maxActiveUploads: 64, maxIncompleteBytes: 1024 * 1024,
    });
    await service.store.writeImmutable(
      `tenants/${tenant}/installations/${installation}/bundles/bundle_abc/complete.json`,
      Buffer.from(JSON.stringify({ protocol_version: '1', bundle_id: 'bundle_abc', bundle_digest: item.digest })),
    );
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }),
    })).status).toBe(200);
    expect(JSON.parse((await service.meterStore.read(meterPath))!.toString('utf8'))).toMatchObject({
      stored_completed_bytes: totalBytes,
      active: {},
    });
  });

  it('reconstructs a missing meter reservation for existing begin state and honors deletion tombstones', async () => {
    const service = await start(); servers.push(service);
    const item = fixture();
    const body = JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files });
    const meterPath = `metering/tenants/${tenant}/installations/${installation}/ledger.json`;
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })).status).toBe(201);
    await service.meterStore.delete([meterPath]);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })).status).toBe(200);
    expect(JSON.parse((await service.meterStore.read(meterPath))!.toString('utf8')).active).toHaveProperty('bundle_abc');
    await recordApprovedDeletion(service.meterStore, meterPath, 'bundle_abc', item.digest, 0);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })).status).toBe(409);
    expect((await request(service.url, `/v1/bundles/bundle_abc/files/${item.files[0]!.file_id}/parts/0`, {
      method: 'PUT', headers: { 'x-semantic-layer-part-sha256': sha256(item.manifestBytes) }, body: item.manifestBytes,
    })).status).toBe(409);
    expect((await request(service.url, '/v1/bundles/bundle_abc/complete', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest }),
    })).status).toBe(409);
  });

  it('blocks operator deletion while a real part request is writing, then tombstones the bundle', async () => {
    let markWriting!: () => void;
    let allowWrite!: () => void;
    const writing = new Promise<void>((resolve) => { markWriting = resolve; });
    const allowed = new Promise<void>((resolve) => { allowWrite = resolve; });
    class BlockingPartStore extends MemoryObjectStore {
      private blocked = false;
      override async writeImmutable(path: string, bytes: Buffer, contentType?: string) {
        if (!this.blocked && path.includes('/parts/')) {
          this.blocked = true;
          markWriting();
          await allowed;
        }
        return super.writeImmutable(path, bytes, contentType);
      }
    }
    const store = new BlockingPartStore();
    const meterStore = new MemoryObjectStore();
    const service = await start(undefined, { store, meterStore }); servers.push(service);
    const item = fixture();
    const body = JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files });
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body,
    })).status).toBe(201);
    const partRequest = request(service.url, `/v1/bundles/bundle_abc/files/${item.files[0]!.file_id}/parts/0`, {
      method: 'PUT', headers: { 'x-semantic-layer-part-sha256': sha256(item.manifestBytes) }, body: item.manifestBytes,
    });
    await writing;
    const output: string[] = [];
    const deletion = ['delete', tenant, installation, 'bundle_abc',
      `--confirm=${tenant}/${installation}/bundle_abc`, '--approval=change_1234'];
    expect(await runOps(deletion, { store, meterStore, output: (line) => output.push(line) })).toBe(2);
    expect(output).toEqual(['deletion is blocked by active ingest; retry after the request finishes']);
    expect(await store.list(`audit/tenants/${tenant}/installations/${installation}/deletions/`)).toEqual([]);
    allowWrite();
    expect((await partRequest).status).toBe(201);
    expect(await runOps(deletion, { store, meterStore, output: () => undefined })).toBe(0);
    expect((await request(service.url, '/v1/bundles/bundle_abc/complete', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest }),
    })).status).toBe(409);
  });

  it('binds a managed v2 manifest to the installation derived from its key', async () => {
    const service = await start(); servers.push(service);
    const v2 = {
      ...manifest(Buffer.alloc(0)),
      schema: 'semantic_trace_manifest_v2',
      installation_id: installation,
      capture_policy: 'rich-credential-scrubbed',
      sources: [{
        id: 'source_sdk', name: 'test', version: '2026.5.5',
        qualification: { status: 'exact_qualified', profile: 'openclaw-2026.5.5' },
      }],
    } as ReturnType<typeof manifest>;
    const item = fixture(v2);
    const begin = (value: unknown) => request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(value),
    });
    const body = { protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files };
    expect((await begin({ ...body, manifest: { ...item.value, installation_id: 'install_BBBBBBBBBBBBBBBBBBBBBB' } })).status).toBe(403);
    expect((await begin(body)).status).toBe(201);
    expect((await request(service.url, `/v1/bundles/bundle_abc/files/${item.files[0]!.file_id}/parts/0`, {
      method: 'PUT', headers: { 'x-semantic-layer-part-sha256': sha256(item.manifestBytes) }, body: item.manifestBytes,
    })).status).toBe(201);
    expect((await request(service.url, '/v1/bundles/bundle_abc/complete', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest }),
    })).status).toBe(201);
    expect(await service.store.read(`tenants/${tenant}/installations/${installation}/bundles/bundle_abc/manifest.json`)).toEqual(item.manifestBytes);
  });

  it('reserves generous per-installation active capacity before writing begin state', async () => {
    const logs: Array<Record<string, unknown>> = [];
    const service = await start(undefined, {
      meterLimits: { maxActiveUploads: 1, maxIncompleteBytes: 1024 * 1024 * 1024 },
      logger: (entry) => logs.push(entry),
    });
    servers.push(service);
    const first = fixture();
    const secondManifest = { ...manifest(Buffer.alloc(0)), bundle_id: 'bundle_def' };
    const second = fixture(secondManifest);
    const sendBegin = (bundle: string, item: ReturnType<typeof fixture>) => request(service.url, `/v1/bundles/${bundle}/begin`, {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }),
    });
    expect((await sendBegin('bundle_abc', first)).status).toBe(201);
    expect((await sendBegin('bundle_def', second)).status).toBe(429);
    expect(logs.slice(0, 2).map((entry) => entry.meter_pressure)).toEqual(['critical', 'hard']);
    expect(await service.store.list(`uploads/tenants/${tenant}/installations/${installation}/bundles/bundle_def/`)).toEqual([]);
    expect((await request(service.url, `/v1/bundles/bundle_abc/files/${first.files[0]!.file_id}/parts/0`, {
      method: 'PUT', headers: { 'x-semantic-layer-part-sha256': sha256(first.manifestBytes) }, body: first.manifestBytes,
    })).status).toBe(201);
    expect((await request(service.url, '/v1/bundles/bundle_abc/complete', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: first.digest }),
    })).status).toBe(201);
    expect((await sendBegin('bundle_def', second)).status).toBe(201);
  });

  it('rejects an incomplete-byte reservation before creating upload state', async () => {
    const item = fixture();
    const totalBytes = item.files.reduce((total, file) => total + file.size_bytes, 0);
    const service = await start(undefined, {
      meterLimits: { maxActiveUploads: 64, maxIncompleteBytes: totalBytes - 1 },
    });
    servers.push(service);
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }),
    })).status).toBe(429);
    expect(await service.store.list(`uploads/tenants/${tenant}/installations/${installation}/bundles/bundle_abc/`)).toEqual([]);
    expect(JSON.parse((await service.meterStore.read(
      `metering/tenants/${tenant}/installations/${installation}/ledger.json`,
    ))!.toString('utf8'))).toMatchObject({ active: {}, ingest_leases: {} });
  });

  it('releases its meter reservation when begin-state storage fails', async () => {
    class FailingBeginStore extends MemoryObjectStore {
      override async writeImmutable(path: string, bytes: Buffer, contentType?: string) {
        if (path.endsWith('/begin.json')) throw new Error('injected evidence write failure');
        return super.writeImmutable(path, bytes, contentType);
      }
    }
    const store = new FailingBeginStore();
    const meterStore = new MemoryObjectStore();
    const service = await start(undefined, { store, meterStore });
    servers.push(service);
    const item = fixture();
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }),
    })).status).toBe(500);
    const ledgerPath = `metering/tenants/${tenant}/installations/${installation}/ledger.json`;
    expect(JSON.parse((await meterStore.read(ledgerPath))!.toString('utf8'))).toMatchObject({
      accepted_bundles: 0,
      accepted_bytes: 0,
      active: {},
    });
  });

  it('isolates two installation keys for one tenant and revokes either independently', async () => {
    const secondKey = 'second-test-secret-key-with-enough-entropy';
    const revokedKey = 'revoked-test-secret-key-with-enough-entropy';
    const secondInstallation = 'install_BBBBBBBBBBBBBBBBBBBBBB';
    const service = await start({
      [sha256(Buffer.from(key))]: { tenant_id: tenant, installation_id: installation, status: 'active' },
      [sha256(Buffer.from(secondKey))]: { tenant_id: tenant, installation_id: secondInstallation, status: 'active' },
      [sha256(Buffer.from(revokedKey))]: { tenant_id: tenant, installation_id: secondInstallation, status: 'revoked' },
    });
    servers.push(service);
    const item = fixture();
    const init = {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files }),
    };
    expect((await requestAs(service.url, key, '/v1/bundles/bundle_abc/begin', init)).status).toBe(201);
    expect((await requestAs(service.url, secondKey, '/v1/bundles/bundle_abc/begin', init)).status).toBe(201);
    expect(await service.store.read(`uploads/tenants/${tenant}/installations/${installation}/bundles/bundle_abc/begin.json`)).toBeDefined();
    expect(await service.store.read(`uploads/tenants/${tenant}/installations/${secondInstallation}/bundles/bundle_abc/begin.json`)).toBeDefined();
    expect((await requestAs(service.url, revokedKey, '/v1/bundles/bundle_other/begin', init)).status).toBe(401);
    const ledger = await service.meterStore.read(`metering/tenants/${tenant}/installations/${secondInstallation}/ledger.json`);
    expect(JSON.parse(ledger!.toString('utf8'))).toMatchObject({ auth_failures: 1 });
  });

  it('logs only safe opaque identifiers, counts, status, latency, and request ID', async () => {
    const logs: Array<Record<string, unknown>> = [];
    const service = await start(undefined, { logger: (entry) => logs.push(entry) }); servers.push(service);
    const item = fixture();
    expect((await request(service.url, '/v1/bundles/bundle_abc/begin', {
      method: 'POST', headers: { 'content-type': 'application/json', 'x-request-id': `Bearer-${key}` },
      body: JSON.stringify({
        protocol_version: '1', bundle_digest: item.digest, manifest: item.value, files: item.files,
        ingestion_key: key,
      }),
    })).status).toBe(400);
    expect(logs).toHaveLength(1);
    expect(Object.keys(logs[0]!).sort()).toEqual([
      'bytes', 'installation', 'latency_ms', 'request_id', 'status', 'tenant',
    ]);
    expect(JSON.stringify(logs)).not.toContain(key);
    expect(JSON.stringify(logs)).not.toContain(sha256(Buffer.from(key)));
    await request(service.url, `/v1/bundles/${key}/begin`, { method: 'POST', body: '{}' });
    expect(JSON.stringify(logs)).not.toContain(key);
  });
});

it('matches the shared descriptor digest test vector', () => {
  expect(bundleDigest([
    { path: 'manifest.json', size_bytes: 123, sha256: 'a'.repeat(64) },
    { path: 'trace.jsonl', size_bytes: 456, sha256: 'b'.repeat(64) },
    { path: 'blobs/z.bin', size_bytes: 7, sha256: 'c'.repeat(64) },
  ])).toBe('366eb97f10e3b44880407c6b0c61aceb5b3ba37b09b3d0840f0d80a6dee7883b');
  expect(bundleDigest([
    { path: 'blobs/a.bin', size_bytes: 1, sha256: 'd'.repeat(64) },
    { path: 'blobs/Z.bin', size_bytes: 2, sha256: 'e'.repeat(64) },
  ])).toBe('745a11a0961b93ad9e26902ac0d8ae9648ccf73577e04740888532f625fb25a3');
});
