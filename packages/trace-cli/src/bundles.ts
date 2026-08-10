import { createHash, randomUUID } from 'node:crypto';
import { createReadStream } from 'node:fs';
import {
  chmod,
  lstat,
  mkdir,
  readFile,
  readdir,
  rename,
  rm,
} from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { validateArtifact } from 'semantic-layer-capture';
import type { DownloadResult, ReadOnlyStore } from './store.js';
import { assertNoSymbolicLinkComponents } from './paths.js';

const MAX_MARKER_BYTES = 4096;
const MAX_FILE_BYTES = 256 * 1024 * 1024;
const MAX_BUNDLE_BYTES = 512 * 1024 * 1024;

export type BundleReference = {
  tenant: string;
  installation: string;
  bundle: string;
  digest: string;
  markerPath: string;
};

export type SyncResult = BundleReference & {
  status: 'downloaded' | 'skipped' | 'failed';
  error?: string;
};

type Descriptor = { path: string; size: number; sha256: string };

export async function discoverCompleted(store: ReadOnlyStore, tenant?: string): Promise<BundleReference[]> {
  if (tenant !== undefined && !validId(tenant)) throw new TypeError('tenant ID is invalid');
  const prefix = tenant ? `tenants/${tenant}/installations/` : 'tenants/';
  const names = await store.list(prefix);
  const references: BundleReference[] = [];
  for (const markerPath of names.filter((name) => name.endsWith('/complete.json'))) {
    const match = /^tenants\/([^/]+)\/installations\/([^/]+)\/bundles\/([^/]+)\/complete\.json$/u.exec(markerPath);
    if (!match) continue;
    const [, candidateTenant, installation, bundle] = match;
    if (!validId(candidateTenant) || !validInstallationId(installation) || !validId(bundle)) continue;
    const marker = await readMarker(store, markerPath, bundle);
    if (!marker) throw new Error(`invalid completion marker: ${markerPath}`);
    references.push({
      tenant: candidateTenant,
      installation,
      bundle,
      digest: marker.bundle_digest,
      markerPath,
    });
  }
  return references.sort(compareReferences);
}

export async function syncTenant(
  store: ReadOnlyStore,
  tenant: string,
  outputRoot: string,
): Promise<SyncResult[]> {
  const references = await discoverCompleted(store, tenant);
  const results: SyncResult[] = [];
  // One bundle at a time keeps memory and network use bounded even at the
  // protocol maximum bundle size.
  for (const reference of references) {
    try {
      const status = await syncBundle(store, reference, outputRoot);
      results.push({ ...reference, status });
    } catch (error) {
      results.push({
        ...reference,
        status: 'failed',
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return results;
}

export async function syncBundle(
  store: ReadOnlyStore,
  reference: BundleReference,
  outputRoot: string,
): Promise<'downloaded' | 'skipped'> {
  const root = resolve(outputRoot);
  await ensurePrivateDirectory(root);
  const installationRoot = join(root, reference.tenant, reference.installation);
  await ensurePrivateDirectory(installationRoot);
  const destination = join(installationRoot, reference.bundle);
  if (await exists(destination)) {
    const digest = await localBundleDigest(destination);
    if (digest !== reference.digest) {
      throw new Error(`local bundle conflicts with GCP: ${destination}; move the local directory aside and run sync again`);
    }
    const report = await validateArtifact(destination);
    if (!report.valid) {
      throw new Error(`local bundle is invalid (${report.issues.join(', ')}): ${destination}; move the local directory aside and run sync again`);
    }
    const manifest = JSON.parse(await readFile(join(destination, 'manifest.json'), 'utf8')) as Record<string, unknown>;
    assertManifestScope(manifest, reference);
    return 'skipped';
  }

  const temporary = join(installationRoot, `.${reference.bundle}.tmp-${randomUUID()}`);
  await mkdir(temporary, { mode: 0o700 });
  try {
    const prefix = dirname(reference.markerPath) + '/';
    const objectNames = (await store.list(prefix)).filter((name) => name !== reference.markerPath);
    if (objectNames.length > 1024) throw new Error('completed bundle contains too many files');
    const relativePaths = objectNames.map((name) => name.slice(prefix.length));
    if (!relativePaths.includes('manifest.json') || !relativePaths.includes('trace.jsonl')) {
      throw new Error('completed bundle is missing manifest.json or trace.jsonl');
    }
    if (new Set(relativePaths).size !== relativePaths.length) throw new Error('completed bundle contains duplicate paths');
    const descriptors: Descriptor[] = [];
    let total = 0;
    for (let index = 0; index < objectNames.length; index += 1) {
      const objectName = objectNames[index]!;
      const relative = relativePaths[index]!;
      if (!validBundlePath(relative)) throw new Error(`refusing unsafe object path: ${relative}`);
      const target = join(temporary, ...relative.split('/'));
      await ensurePrivateDirectory(dirname(target));
      const downloaded = await store.download(objectName, target, MAX_FILE_BYTES);
      enforceSizes(relative, downloaded, total);
      total += downloaded.size;
      descriptors.push({ path: relative, size: downloaded.size, sha256: downloaded.sha256 });
    }
    if (bundleDigest(descriptors) !== reference.digest) {
      throw new Error('bundle digest does not match complete.json');
    }
    const report = await validateArtifact(temporary);
    if (!report.valid) throw new Error(`bundle validation failed: ${report.issues.join(', ')}`);
    const manifest = JSON.parse(await readFile(join(temporary, 'manifest.json'), 'utf8')) as Record<string, unknown>;
    assertManifestScope(manifest, reference);
    try {
      await rename(temporary, destination);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST' && (error as NodeJS.ErrnoException).code !== 'ENOTEMPTY') throw error;
      const digest = await localBundleDigest(destination);
      if (digest !== reference.digest) throw new Error(`another process created a conflicting local bundle: ${destination}`);
      const winningReport = await validateArtifact(destination);
      if (!winningReport.valid) throw new Error(`another process created an invalid local bundle: ${destination}`);
      const winningManifest = JSON.parse(await readFile(join(destination, 'manifest.json'), 'utf8')) as Record<string, unknown>;
      assertManifestScope(winningManifest, reference);
      await rm(temporary, { recursive: true, force: true });
      return 'skipped';
    }
    return 'downloaded';
  } catch (error) {
    await rm(temporary, { recursive: true, force: true });
    throw error;
  }
}

export async function localBundleDigest(root: string): Promise<string> {
  await assertNoSymbolicLinkComponents(root);
  const rootMetadata = await lstat(root);
  if (!rootMetadata.isDirectory() || rootMetadata.isSymbolicLink()) throw new Error(`unsafe local bundle directory: ${root}`);
  const descriptors: Descriptor[] = [];
  for (const relative of await localBundlePaths(root)) {
    const path = join(root, ...relative.split('/'));
    const metadata = await lstat(path);
    if (!metadata.isFile() || metadata.isSymbolicLink()) throw new Error(`unsafe local bundle path: ${path}`);
    const hash = createHash('sha256');
    let size = 0;
    for await (const chunk of createReadStream(path)) {
      const bytes = chunk as Buffer;
      size += bytes.byteLength;
      hash.update(bytes);
    }
    descriptors.push({ path: relative, size, sha256: hash.digest('hex') });
  }
  return bundleDigest(descriptors);
}

export async function findLocalBundles(root: string, tenant?: string): Promise<string[]> {
  if (tenant !== undefined && !validId(tenant)) throw new TypeError('tenant ID is invalid');
  const base = tenant ? join(resolve(root), tenant) : resolve(root);
  await assertNoSymbolicLinkComponents(base);
  if (!await exists(base)) return [];
  const found: string[] = [];
  const tenants = tenant ? [tenant] : await directoryNames(base);
  for (const tenantName of tenants) {
    if (!validId(tenantName)) continue;
    const tenantRoot = tenant ? base : join(base, tenantName);
    for (const installation of await directoryNames(tenantRoot)) {
      if (!validInstallationId(installation)) continue;
      for (const bundle of await directoryNames(join(tenantRoot, installation))) {
        if (!validId(bundle)) continue;
        const candidate = join(tenantRoot, installation, bundle);
        if (await exists(join(candidate, 'manifest.json')) && await exists(join(candidate, 'trace.jsonl'))) {
          found.push(candidate);
        }
      }
    }
  }
  return found.sort();
}

export async function readLocalBundle(path: string): Promise<{ manifest: Record<string, unknown>; records: Record<string, unknown>[] }> {
  await assertNoSymbolicLinkComponents(path);
  const [manifestText, traceText] = await Promise.all([
    readFile(join(path, 'manifest.json'), 'utf8'),
    readFile(join(path, 'trace.jsonl'), 'utf8'),
  ]);
  const records = traceText.split('\n').filter(Boolean).map((line) => JSON.parse(line) as Record<string, unknown>);
  return { manifest: JSON.parse(manifestText) as Record<string, unknown>, records };
}

async function readMarker(store: ReadOnlyStore, path: string, bundle: string): Promise<{ bundle_digest: string } | undefined> {
  const bytes = await store.readSmall(path, MAX_MARKER_BYTES);
  if (!bytes) return undefined;
  try {
    const marker = JSON.parse(bytes.toString('utf8')) as Record<string, unknown>;
    const keys = Object.keys(marker).sort();
    if (keys.join(',') !== 'bundle_digest,bundle_id,protocol_version'
      || marker.protocol_version !== '1'
      || marker.bundle_id !== bundle
      || typeof marker.bundle_digest !== 'string'
      || !/^[0-9a-f]{64}$/u.test(marker.bundle_digest)) return undefined;
    return { bundle_digest: marker.bundle_digest };
  } catch {
    return undefined;
  }
}

function bundleDigest(files: readonly Descriptor[]): string {
  const hash = createHash('sha256').update('semantic-layer-bundle-v1\0');
  for (const file of [...files].sort((left, right) => Buffer.compare(Buffer.from(left.path), Buffer.from(right.path)))) {
    const pathBytes = Buffer.byteLength(file.path, 'utf8');
    hash.update(`${pathBytes}:${file.path}\0${file.size}\0${file.sha256.toLowerCase()}\0`);
  }
  return hash.digest('hex');
}

async function localBundlePaths(root: string): Promise<string[]> {
  const result: string[] = [];
  async function visit(directory: string, prefix: string): Promise<void> {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isSymbolicLink()) throw new Error(`bundle contains a symbolic link: ${relative}`);
      if (entry.isDirectory()) await visit(join(directory, entry.name), relative);
      else if (entry.isFile() && validBundlePath(relative)) result.push(relative);
      else throw new Error(`bundle contains an unsafe path: ${relative}`);
    }
  }
  await visit(root, '');
  return result.sort();
}

async function directoryNames(path: string): Promise<string[]> {
  try {
    return (await readdir(path, { withFileTypes: true }))
      .filter((entry) => entry.isDirectory() && !entry.isSymbolicLink())
      .map((entry) => entry.name)
      .sort();
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [];
    throw error;
  }
}

async function ensurePrivateDirectory(path: string): Promise<void> {
  await assertNoSymbolicLinkComponents(path);
  await mkdir(path, { recursive: true, mode: 0o700 });
  await assertNoSymbolicLinkComponents(path);
  const metadata = await lstat(path);
  if (!metadata.isDirectory() || metadata.isSymbolicLink()) throw new Error(`not a safe directory: ${path}`);
  if (process.platform !== 'win32') await chmod(path, 0o700);
}

async function exists(path: string): Promise<boolean> {
  try { await lstat(path); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
    throw error;
  }
}

function enforceSizes(path: string, file: DownloadResult, previousTotal: number): void {
  if (file.size > MAX_FILE_BYTES) throw new Error(`bundle file is too large: ${path}`);
  if (previousTotal + file.size > MAX_BUNDLE_BYTES) throw new Error('bundle is too large');
}

export function assertManifestScope(manifest: Record<string, unknown>, reference: Pick<BundleReference, 'installation' | 'bundle'>): void {
  if (manifest.bundle_id !== reference.bundle) {
    throw new Error('manifest bundle ID does not match the storage scope');
  }
  if (manifest.schema === 'semantic_trace_manifest_v2' && manifest.installation_id !== reference.installation) {
    throw new Error('manifest installation ID does not match the storage scope');
  }
}

function validId(value: unknown): value is string {
  return typeof value === 'string' && /^[a-z][a-z0-9._:-]{2,127}$/u.test(value);
}

function validInstallationId(value: unknown): value is string {
  return typeof value === 'string' && /^install_[A-Za-z0-9_-]{22,128}$/u.test(value);
}

function validBundlePath(value: string): boolean {
  if (value === 'manifest.json' || value === 'trace.jsonl') return true;
  if (value.length > 512 || !value.startsWith('blobs/')) return false;
  return value.split('/').every((part) => part !== '' && part !== '.' && part !== '..' && /^[A-Za-z0-9._-]+$/u.test(part));
}

function compareReferences(left: BundleReference, right: BundleReference): number {
  return `${left.tenant}/${left.installation}/${left.bundle}`.localeCompare(`${right.tenant}/${right.installation}/${right.bundle}`);
}
