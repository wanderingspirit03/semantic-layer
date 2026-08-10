import { createHash } from 'node:crypto';
import { chmod, mkdtemp, readFile, readdir, rm, stat, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { describe, expect, test } from 'vitest';
import { discoverCompleted, syncTenant } from '../src/bundles.js';
import type { DownloadResult, ReadOnlyStore } from '../src/store.js';

const tenant = 'tenant_test';
const installation = 'install_0123456789abcdef0123456789abcdef';
const bundle = 'bundle_code_example';

class MemoryReadStore implements ReadOnlyStore {
  constructor(readonly objects = new Map<string, Buffer>()) {}
  async list(prefix: string) { return [...this.objects.keys()].filter((path) => path.startsWith(prefix)).sort(); }
  async readSmall(path: string, maximumBytes: number) {
    const bytes = this.objects.get(path);
    if (!bytes) return undefined;
    if (bytes.byteLength > maximumBytes) throw new Error('too large');
    return Buffer.from(bytes);
  }
  async download(path: string, destination: string, maximumBytes: number): Promise<DownloadResult> {
    const bytes = this.objects.get(path);
    if (!bytes) throw new Error(`missing ${path}`);
    if (bytes.byteLength > maximumBytes) throw new Error('too large');
    await writeFile(destination, bytes, { flag: 'wx', mode: 0o600 });
    return { size: bytes.byteLength, sha256: sha256(bytes) };
  }
  async testPermissions() { return ['storage.objects.get', 'storage.objects.list']; }
}

describe('completed bundle sync', () => {
  test('discovers, validates, downloads, and then skips a completed bundle', async () => {
    const store = await fixtureStore(bundle);
    const root = await mkdtemp(join(tmpdir(), 'sl-traces-sync-'));
    await chmod(root, 0o700);

    const references = await discoverCompleted(store);
    expect(references).toHaveLength(1);
    expect(references[0]).toMatchObject({ tenant, installation, bundle });

    const first = await syncTenant(store, tenant, root);
    expect(first.map((result) => result.status)).toEqual(['downloaded']);
    const local = join(root, tenant, installation, bundle);
    expect((await stat(local)).mode & 0o777).toBe(0o700);
    expect((await stat(join(local, 'manifest.json'))).mode & 0o777).toBe(0o600);

    const second = await syncTenant(store, tenant, root);
    expect(second.map((result) => result.status)).toEqual(['skipped']);
  });

  test('reports a manifest scope mismatch and publishes no final directory', async () => {
    const scopedBundle = 'bundle_wrong_scope';
    const store = await fixtureStore(scopedBundle);
    const root = await mkdtemp(join(tmpdir(), 'sl-traces-scope-'));
    await chmod(root, 0o700);
    const result = await syncTenant(store, tenant, root);
    expect(result).toHaveLength(1);
    expect(result[0]?.status).toBe('failed');
    expect(result[0]?.error).toMatch(/manifest bundle ID/u);
    await expect(stat(join(root, tenant, installation, scopedBundle))).rejects.toMatchObject({ code: 'ENOENT' });
  });

  test('never overwrites a changed local bundle', async () => {
    const store = await fixtureStore(bundle);
    const root = await mkdtemp(join(tmpdir(), 'sl-traces-conflict-'));
    await chmod(root, 0o700);
    expect((await syncTenant(store, tenant, root))[0]?.status).toBe('downloaded');
    const manifestPath = join(root, tenant, installation, bundle, 'manifest.json');
    await writeFile(manifestPath, '{}\n', { mode: 0o600 });
    const repeat = await syncTenant(store, tenant, root);
    expect(repeat[0]?.status).toBe('failed');
    expect(repeat[0]?.error).toMatch(/conflicts with GCP/u);
    expect(await readFile(manifestPath, 'utf8')).toBe('{}\n');
  });

  test('removes its temporary directory after a failed download', async () => {
    const base = await fixtureStore(bundle);
    const store = new MemoryReadStore(base.objects);
    const originalDownload = store.download.bind(store);
    store.download = async (path, destination) => {
      if (path.endsWith('/trace.jsonl')) throw new Error('simulated read failure');
      return originalDownload(path, destination);
    };
    const root = await mkdtemp(join(tmpdir(), 'sl-traces-interrupt-'));
    await chmod(root, 0o700);
    const result = await syncTenant(store, tenant, root);
    expect(result[0]?.status).toBe('failed');
    const installationRoot = join(root, tenant, installation);
    expect((await readdir(installationRoot)).filter((name) => name.startsWith('.'))).toEqual([]);
    await expect(stat(join(installationRoot, bundle))).rejects.toMatchObject({ code: 'ENOENT' });
  });

  test('fails visibly when a listed completion marker is malformed', async () => {
    const marker = `tenants/${tenant}/installations/${installation}/bundles/${bundle}/complete.json`;
    const store = new MemoryReadStore(new Map([[marker, Buffer.from('{}')]]));
    await expect(discoverCompleted(store)).rejects.toThrow(`invalid completion marker: ${marker}`);
  });

  test('rejects bundle bytes that do not match the valid completion digest', async () => {
    const store = await fixtureStore(bundle);
    const manifestPath = `tenants/${tenant}/installations/${installation}/bundles/${bundle}/manifest.json`;
    store.objects.set(manifestPath, Buffer.concat([store.objects.get(manifestPath)!, Buffer.from(' ')]));
    const root = await mkdtemp(join(tmpdir(), 'sl-traces-digest-'));
    await chmod(root, 0o700);
    const result = await syncTenant(store, tenant, root);
    expect(result[0]?.status).toBe('failed');
    expect(result[0]?.error).toMatch(/bundle digest does not match/u);
  });

  test('discovers separate installations and keeps a partial sync failure visible', async () => {
    const otherInstallation = 'install_abcdef0123456789abcdef0123456789';
    const secondBundle = 'bundle_second_test';
    const first = await fixtureStore(bundle);
    const second = await fixtureStore(secondBundle, { manifestBundle: secondBundle, installation: otherInstallation });
    const secondPrefix = `tenants/${tenant}/installations/${otherInstallation}/bundles/${secondBundle}/`;
    second.objects.set(`${secondPrefix}blobs/../unsafe`, Buffer.from('unsafe'));
    const store = new MemoryReadStore(new Map([...first.objects, ...second.objects]));
    const references = await discoverCompleted(store, tenant);
    expect(new Set(references.map((reference) => reference.installation))).toEqual(new Set([installation, otherInstallation]));

    const root = await mkdtemp(join(tmpdir(), 'sl-traces-partial-'));
    await chmod(root, 0o700);
    const result = await syncTenant(store, tenant, root);
    expect(result.map((entry) => entry.status).sort()).toEqual(['downloaded', 'failed']);
    expect(result.find((entry) => entry.status === 'failed')?.error).toMatch(/unsafe object path/u);
  });

  test('downloads a declared blob and rejects a structurally invalid bundle', async () => {
    const blobStore = await fixtureStore(bundle, { blob: Buffer.from('separate blob evidence') });
    const root = await mkdtemp(join(tmpdir(), 'sl-traces-blob-'));
    await chmod(root, 0o700);
    expect((await syncTenant(blobStore, tenant, root))[0]?.status).toBe('downloaded');
    expect(await readFile(join(root, tenant, installation, bundle, 'blobs', 'evidence.bin'), 'utf8')).toBe('separate blob evidence');

    const invalidBundle = 'bundle_invalid_test';
    const invalidStore = await fixtureStore(invalidBundle, { manifestBundle: invalidBundle, invalidRecord: true });
    const invalid = await syncTenant(invalidStore, tenant, root);
    expect(invalid[0]?.status).toBe('failed');
    expect(invalid[0]?.error).toMatch(/bundle validation failed/u);
    await expect(stat(join(root, tenant, installation, invalidBundle))).rejects.toMatchObject({ code: 'ENOENT' });
  });

  test('refuses a symbolic link added to a local bundle', async () => {
    const store = await fixtureStore(bundle);
    const root = await mkdtemp(join(tmpdir(), 'sl-traces-link-'));
    await chmod(root, 0o700);
    expect((await syncTenant(store, tenant, root))[0]?.status).toBe('downloaded');
    const local = join(root, tenant, installation, bundle);
    const manifest = join(local, 'manifest.json');
    const outside = join(root, 'outside.json');
    await writeFile(outside, await readFile(manifest), { mode: 0o600 });
    await rm(manifest);
    await symlink(outside, manifest);
    const repeat = await syncTenant(store, tenant, root);
    expect(repeat[0]?.status).toBe('failed');
    expect(repeat[0]?.error).toMatch(/symbolic link/u);
  });
});

async function fixtureStore(
  scopeBundle: string,
  options: { manifestBundle?: string; installation?: string; blob?: Buffer; invalidRecord?: boolean } = {},
): Promise<MemoryReadStore> {
  const [manifestSource, traceSource] = await Promise.all([
    readFile(new URL('../../../contracts/trace/v1/examples/coding-agent/manifest.json', import.meta.url)),
    readFile(new URL('../../../contracts/trace/v1/examples/coding-agent/trace.jsonl', import.meta.url)),
  ]);
  const manifestValue = JSON.parse(manifestSource.toString('utf8')) as Record<string, unknown>;
  manifestValue.bundle_id = options.manifestBundle ?? bundle;
  const records = traceSource.toString('utf8').trim().split('\n').map((line) => JSON.parse(line) as Record<string, unknown>);
  if (options.invalidRecord) delete records[0]!.kind;
  if (options.blob) {
    records[2]!.blob_refs = [{
      path: 'blobs/evidence.bin',
      bytes: options.blob.byteLength,
      sha256: sha256(options.blob),
      media_type: 'application/octet-stream',
      scan: 'clean',
    }];
  }
  const trace = Buffer.from(`${records.map((record) => JSON.stringify(record)).join('\n')}\n`);
  const traceAccounting = manifestValue.trace as Record<string, unknown>;
  traceAccounting.bytes = trace.byteLength;
  traceAccounting.sha256 = sha256(trace);
  const blobAccounting = manifestValue.blobs as Record<string, unknown>;
  blobAccounting.count = options.blob ? 1 : 0;
  blobAccounting.bytes = options.blob?.byteLength ?? 0;
  const manifest = Buffer.from(`${JSON.stringify(manifestValue, null, 2)}\n`);
  const installationId = options.installation ?? installation;
  const prefix = `tenants/${tenant}/installations/${installationId}/bundles/${scopeBundle}/`;
  const descriptors = [
    { path: 'manifest.json', bytes: manifest },
    { path: 'trace.jsonl', bytes: trace },
    ...(options.blob ? [{ path: 'blobs/evidence.bin', bytes: options.blob }] : []),
  ];
  const digest = bundleDigest(descriptors.map(({ path, bytes }) => ({ path, size: bytes.byteLength, sha256: sha256(bytes) })));
  return new MemoryReadStore(new Map([
    [`${prefix}manifest.json`, manifest],
    [`${prefix}trace.jsonl`, trace],
    ...(options.blob ? [[`${prefix}blobs/evidence.bin`, options.blob] as const] : []),
    [`${prefix}complete.json`, Buffer.from(`${JSON.stringify({ protocol_version: '1', bundle_id: scopeBundle, bundle_digest: digest })}\n`)],
  ]));
}

function sha256(bytes: Buffer): string {
  return createHash('sha256').update(bytes).digest('hex');
}

function bundleDigest(files: Array<{ path: string; size: number; sha256: string }>): string {
  const hash = createHash('sha256').update('semantic-layer-bundle-v1\0');
  for (const file of files.sort((left, right) => Buffer.compare(Buffer.from(left.path), Buffer.from(right.path)))) {
    hash.update(`${Buffer.byteLength(file.path)}:${file.path}\0${file.size}\0${file.sha256}\0`);
  }
  return hash.digest('hex');
}
