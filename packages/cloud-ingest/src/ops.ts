#!/usr/bin/env node
import { randomUUID } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  clearBundleIngestLeases,
  completedApprovedDeletion,
  finalizeApprovedDeletion,
  pendingApprovedDeletion,
  prepareApprovedDeletion,
  readBundleIngestLeaseStatus,
  readMeterStatus,
  updatePreparedDeletionAccounting,
} from './metering.js';
import { bundleDigest, sha256, validBundlePath, validId, validInstallationId } from './protocol.js';
import { GcsObjectStore, type ObjectStore } from './storage.js';
import { validateCanonicalBundle } from './validation.js';

type Dependencies = { store: ObjectStore; meterStore?: ObjectStore; output?: (line: string) => void };

export async function runOps(args: readonly string[], dependencies: Dependencies): Promise<number> {
  const output = dependencies.output ?? console.log;
  const [command, tenant, installation, bundle, destination, ...rest] = args;
  if (command === 'list') {
    if (!validId(tenant) || !validInstallationId(installation)) return usage(output);
    const scope = `tenants/${tenant}/installations/${installation}/bundles/`;
    const names = await dependencies.store.list(scope);
    const bundles: string[] = [];
    for (const name of names.filter((candidate) => candidate.endsWith('/complete.json'))) {
      const match = /^tenants\/([^/]+)\/installations\/([^/]+)\/bundles\/([^/]+)\/complete\.json$/u.exec(name);
      if (!match) continue;
      const marker = await readCompletionMarker(dependencies.store, name, match[3]!);
      if (marker) bundles.push(`${match[1]}/${match[2]}/${match[3]}`);
    }
    bundles.forEach(output); return 0;
  }
  if (command === 'list-incomplete') {
    if (!validId(tenant) || !validInstallationId(installation) || bundle !== undefined) return usage(output);
    const scope = `uploads/tenants/${tenant}/installations/${installation}/bundles/`;
    for (const name of await dependencies.store.list(scope)) {
      const match = /^uploads\/tenants\/([^/]+)\/installations\/([^/]+)\/bundles\/([^/]+)\/begin\.json$/u.exec(name);
      if (match) output(`${match[1]}/${match[2]}/${match[3]}`);
    }
    return 0;
  }
  if (command === 'meter-status') {
    if (!validId(tenant) || !validInstallationId(installation) || bundle !== undefined || !dependencies.meterStore) return usage(output);
    const status = await readMeterStatus(dependencies.meterStore, `metering/tenants/${tenant}/installations/${installation}/ledger.json`);
    output(JSON.stringify({
      accepted_bundles: status.acceptedBundles,
      accepted_bytes: status.acceptedBytes,
      stored_completed_bytes: status.storedCompletedBytes,
      active_uploads: status.activeUploads,
      incomplete_bytes: status.incompleteBytes,
      pending_deletions: status.pendingDeletions,
    }));
    return 0;
  }
  if (command === 'legacy-list') {
    if (!validId(tenant) || installation !== undefined) return usage(output);
    const names = await dependencies.store.list(`tenants/${tenant}/bundles/`);
    for (const name of names.filter((candidate) => candidate.endsWith('/complete.json'))) {
      const match = /^tenants\/([^/]+)\/bundles\/([^/]+)\/complete\.json$/u.exec(name);
      if (match && await readCompletionMarker(dependencies.store, name, match[2]!)) output(`${match[1]}/${match[2]}`);
    }
    return 0;
  }
  if (command === 'legacy-fetch' || command === 'legacy-validate') {
    const legacyBundle = installation;
    const legacyDestination = bundle;
    if (!validId(tenant) || !validId(legacyBundle) || (command === 'legacy-fetch' && !legacyDestination)) return usage(output);
    const legacyPrefix = `tenants/${tenant}/bundles/${legacyBundle}/`;
    return command === 'legacy-fetch'
      ? fetchBundle(dependencies.store, legacyPrefix, legacyBundle, legacyDestination!, output)
      : validateBundle(dependencies.store, legacyPrefix, legacyBundle, output);
  }
  if (!validId(tenant) || !validInstallationId(installation) || !validId(bundle)) return usage(output);
  const prefix = `tenants/${tenant}/installations/${installation}/bundles/${bundle}/`;
  if (command === 'clear-ingest-leases') {
    const options = [destination, ...rest];
    const confirmation = options.find((value) => value?.startsWith('--confirm-drained='))?.slice('--confirm-drained='.length);
    const approval = options.find((value) => value?.startsWith('--approval='))?.slice('--approval='.length);
    if (confirmation !== `${tenant}/${installation}/${bundle}`
      || !approval
      || !/^[A-Za-z0-9._:-]{3,128}$/u.test(approval)
      || !dependencies.meterStore) {
      output('lease recovery requires the metering store, recorded approval, and exact --confirm-drained scope'); return 2;
    }
    const meterPath = `metering/tenants/${tenant}/installations/${installation}/ledger.json`;
    const snapshot = await readBundleIngestLeaseStatus(dependencies.meterStore, meterPath, bundle);
    if (snapshot.count === 0) { output(`no ingest leases under ${prefix}`); return 0; }
    const requestedAt = new Date().toISOString();
    await dependencies.store.writeImmutable(
      `audit/tenants/${tenant}/installations/${installation}/lease-recovery/${requestedAt.replace(/[:.]/gu, '-')}-${randomUUID()}.json`,
      Buffer.from(`${JSON.stringify({
        protocol_version: '1', kind: 'bundle_ingest_lease_recovery', tenant, installation, bundle,
        approval, lease_count: snapshot.count, lease_fingerprint: snapshot.fingerprint, requested_at: requestedAt,
      })}\n`),
      'application/json',
    );
    const cleared = await clearBundleIngestLeases(
      dependencies.meterStore, meterPath, bundle, snapshot.fingerprint,
    );
    if (cleared === 'changed') { output('ingest lease set changed; recovery stopped'); return 2; }
    output(`cleared ${snapshot.count} drained ingest lease(s) under ${prefix}`); return 0;
  }
  if (command === 'fetch') {
    if (!destination) return usage(output);
    return fetchBundle(dependencies.store, prefix, bundle, destination, output);
  }
  if (command === 'validate') {
    return validateBundle(dependencies.store, prefix, bundle, output);
  }
  if (command === 'delete') {
    const options = [destination, ...rest];
    const confirmation = options.find((value) => value?.startsWith('--confirm='))?.slice('--confirm='.length);
    const approval = options.find((value) => value?.startsWith('--approval='))?.slice('--approval='.length);
    const uploadPrefix = `uploads/tenants/${tenant}/installations/${installation}/bundles/${bundle}/`;
    let names = [
      ...await dependencies.store.list(prefix),
      ...await dependencies.store.list(uploadPrefix),
    ];
    if (confirmation !== `${tenant}/${installation}/${bundle}`) { output(`dry-run: would delete ${names.length} objects under ${prefix}`); return 0; }
    if (!approval || !/^[A-Za-z0-9._:-]{3,128}$/u.test(approval)) {
      output('deletion requires --approval=<recorded-approval-reference>'); return 2;
    }
    if (!dependencies.meterStore) {
      output('confirmed deletion requires the metering store'); return 2;
    }
    const meterPath = `metering/tenants/${tenant}/installations/${installation}/ledger.json`;
    const deletedDigest = await completedApprovedDeletion(dependencies.meterStore, meterPath, bundle);
    if (deletedDigest) {
      names = [
        ...await dependencies.store.list(prefix),
        ...await dependencies.store.list(uploadPrefix),
      ];
      if (names.length) throw new Error('deleted bundle tombstone conflicts with retained evidence objects');
      output(`already deleted 0 objects under ${prefix}`); return 0;
    }
    const marker = await readCompletionMarker(dependencies.store, `${prefix}complete.json`, bundle);
    const beginBytes = await dependencies.store.read(`${uploadPrefix}begin.json`);
    let beginDigest: string | undefined;
    try {
      const value = beginBytes ? JSON.parse(beginBytes.toString('utf8')) as Record<string, unknown> : undefined;
      if (typeof value?.bundle_digest === 'string' && /^[0-9a-f]{64}$/u.test(value.bundle_digest)) beginDigest = value.bundle_digest;
    } catch { /* corrupt upload state still remains explicitly deletable */ }
    const pending = await pendingApprovedDeletion(dependencies.meterStore, meterPath, bundle);
    const digest = marker?.bundle_digest ?? beginDigest ?? pending?.digest;
    if (!digest) { output('deletion scope has no recoverable bundle digest'); return 2; }
    let completedBytes = pending?.completedBytes ?? 0;
    if (!pending && marker) {
      for (const name of names.filter((name) => name.startsWith(prefix) && name !== `${prefix}complete.json`)) {
        completedBytes += (await dependencies.store.read(name))?.byteLength ?? 0;
      }
    }
    const preparation = await prepareApprovedDeletion(dependencies.meterStore, meterPath, bundle, digest, completedBytes);
    if (preparation === 'busy') {
      output('deletion is blocked by active ingest; retry after the request finishes'); return 2;
    }
    names = [
      ...await dependencies.store.list(prefix),
      ...await dependencies.store.list(uploadPrefix),
    ];
    if (preparation === 'prepared' || (preparation === 'pending' && !pending?.accountingReady)) {
      const stableMarker = await readCompletionMarker(dependencies.store, `${prefix}complete.json`, bundle);
      if (stableMarker && stableMarker.bundle_digest !== digest) throw new Error('approved deletion scope changed while it was quiescing');
      completedBytes = 0;
      if (stableMarker) {
        for (const name of names.filter((name) => name.startsWith(prefix) && name !== `${prefix}complete.json`)) {
          completedBytes += (await dependencies.store.read(name))?.byteLength ?? 0;
        }
      }
      await updatePreparedDeletionAccounting(dependencies.meterStore, meterPath, bundle, digest, completedBytes);
    }
    const requestedAt = new Date().toISOString();
    await dependencies.store.writeImmutable(
      `audit/tenants/${tenant}/installations/${installation}/deletions/${requestedAt.replace(/[:.]/gu, '-')}-${randomUUID()}.json`,
      Buffer.from(`${JSON.stringify({
        protocol_version: '1',
        kind: 'bundle_deletion',
        tenant,
        installation,
        bundle,
        approval,
        bundle_digest: digest,
        completed_bytes: completedBytes,
        object_count: names.length,
        requested_at: requestedAt,
      })}\n`),
      'application/json',
    );
    await dependencies.store.delete(names);
    if ((await dependencies.store.list(prefix)).length || (await dependencies.store.list(uploadPrefix)).length) {
      throw new Error('approved deletion did not remove its exact evidence scope');
    }
    await finalizeApprovedDeletion(dependencies.meterStore, meterPath, bundle, digest);
    output(`deleted ${names.length} objects under ${prefix}`); return 0;
  }
  return usage(output);
}

type CompleteMarker = { protocol_version: '1'; bundle_id: string; bundle_digest: string };

async function readCompletionMarker(store: ObjectStore, path: string, bundle: string): Promise<CompleteMarker | undefined> {
  const bytes = await store.read(path);
  if (!bytes) return undefined;
  try {
    const marker = JSON.parse(bytes.toString('utf8')) as Record<string, unknown>;
    const keys = Object.keys(marker).sort();
    if (keys.length !== 3
      || keys[0] !== 'bundle_digest'
      || keys[1] !== 'bundle_id'
      || keys[2] !== 'protocol_version'
      || marker.protocol_version !== '1'
      || marker.bundle_id !== bundle
      || typeof marker.bundle_digest !== 'string'
      || !/^[0-9a-f]{64}$/u.test(marker.bundle_digest)) return undefined;
    return marker as CompleteMarker;
  } catch {
    return undefined;
  }
}

async function loadCompletedBundle(
  store: ObjectStore,
  prefix: string,
  bundle: string,
): Promise<{ files?: Map<string, Buffer>; error: string }> {
  const markerPath = `${prefix}complete.json`;
  if (!await store.read(markerPath)) return { error: 'bundle is not complete' };
  const marker = await readCompletionMarker(store, markerPath, bundle);
  if (!marker) return { error: 'bundle completion marker is invalid' };
  const files = new Map<string, Buffer>();
  for (const name of (await store.list(prefix)).filter((candidate) => candidate !== markerPath)) {
    const relative = name.slice(prefix.length);
    if (!validBundlePath(relative)) return { error: `refusing unsafe object path: ${relative}` };
    const bytes = await store.read(name);
    if (bytes) files.set(relative, bytes);
  }
  const digest = bundleDigest([...files].map(([path, bytes]) => ({
    path,
    size_bytes: bytes.byteLength,
    sha256: sha256(bytes),
  })));
  if (digest !== marker.bundle_digest) return { error: 'bundle completion marker is invalid' };
  return { files, error: '' };
}

function usage(output: (line: string) => void): number {
  output('usage: semantic-layer-ingest-ops list <tenant> <installation> | list-incomplete <tenant> <installation> | meter-status <tenant> <installation> | fetch <tenant> <installation> <bundle> <directory> | validate <tenant> <installation> <bundle> | delete <tenant> <installation> <bundle> [--confirm=<tenant>/<installation>/<bundle> --approval=<reference>] | clear-ingest-leases <tenant> <installation> <bundle> --confirm-drained=<tenant>/<installation>/<bundle> --approval=<reference> | legacy-list <tenant> | legacy-fetch <tenant> <bundle> <directory> | legacy-validate <tenant> <bundle>');
  return 2;
}

async function fetchBundle(
  store: ObjectStore,
  prefix: string,
  bundle: string,
  destination: string,
  output: (line: string) => void,
): Promise<number> {
  const completed = await loadCompletedBundle(store, prefix, bundle);
  if (!completed.files) { output(completed.error); return 2; }
  const root = resolve(destination);
  try {
    await mkdir(root, { mode: 0o700 });
  } catch (error) {
    if ((error as { code?: string }).code === 'EEXIST') { output('fetch destination must not exist'); return 2; }
    throw error;
  }
  for (const [relative, bytes] of completed.files) {
    const path = join(root, ...relative.split('/'));
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await writeFile(path, bytes, { mode: 0o600, flag: 'wx' });
  }
  output(root); return 0;
}

async function validateBundle(store: ObjectStore, prefix: string, bundle: string, output: (line: string) => void) {
  const completed = await loadCompletedBundle(store, prefix, bundle);
  if (!completed.files) { output(completed.error); return 2; }
  const issues = await validateCanonicalBundle(completed.files);
  output(issues.length ? `invalid: ${issues.join(',')}` : 'valid'); return issues.length ? 2 : 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const bucket = process.env.SEMANTIC_LAYER_BUCKET;
  const meterBucket = process.env.SEMANTIC_LAYER_METER_BUCKET;
  if (!bucket || !meterBucket) { console.error('SEMANTIC_LAYER_BUCKET and SEMANTIC_LAYER_METER_BUCKET are required'); process.exitCode = 2; }
  else process.exitCode = await runOps(process.argv.slice(2), {
    store: new GcsObjectStore(bucket),
    meterStore: new GcsObjectStore(meterBucket),
  });
}
