import { randomUUID } from 'node:crypto';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { isDeepStrictEqual } from 'node:util';
import {
  acquireBundleIngestLease,
  cancelReservation,
  DEFAULT_METER_LIMITS,
  finishUpload,
  recordMeterFailure,
  releaseBundleIngestLease,
  reserveUpload,
  type MeterLimits,
} from './metering.js';
import { bundleDigest, parseDescriptors, PART_BYTES, sha256, validFileId, validId, validInstallationId, type FileDescriptor } from './protocol.js';
import { ObjectConflictError, type ObjectStore } from './storage.js';
import { validateCanonicalBundle, validateManifestEnvelope } from './validation.js';

type Begin = { protocol_version: '1'; bundle_digest: string; manifest: Record<string, unknown>; files: FileDescriptor[] };
export type KeyRegistryEntry = string | {
  tenant_id: string;
  installation_id: string;
  status: 'active' | 'revoked';
};
type AuthScope = { tenant: string; installation?: string; legacy: boolean };
type LogEntry = { request_id: string; tenant?: string; installation?: string; bytes: number; status: number; latency_ms: number; active_uploads?: number; incomplete_bytes?: number; meter_pressure?: 'ok' | 'warning' | 'critical' | 'hard' };
type Options = {
  store: ObjectStore;
  meterStore?: ObjectStore;
  meterLimits?: Partial<MeterLimits>;
  keyRegistry: Record<string, KeyRegistryEntry>;
  logger?: (entry: LogEntry) => void;
};

class HttpError extends Error {
  constructor(readonly status: number, readonly code: string, readonly scope?: AuthScope) { super(code); }
}

export function createIngestServer(options: Options) {
  const meterStore = options.meterStore ?? options.store;
  const meterLimits = { ...DEFAULT_METER_LIMITS, ...options.meterLimits };
  if (!Number.isSafeInteger(meterLimits.maxActiveUploads)
    || meterLimits.maxActiveUploads <= 0
    || !Number.isSafeInteger(meterLimits.maxIncompleteBytes)
    || meterLimits.maxIncompleteBytes <= 0) {
    throw new Error('meter limits must be positive safe integers');
  }
  const server = createServer(async (request, response) => {
    const started = performance.now();
    const requestIdHeader = request.headers['x-request-id'];
    const requestId = typeof requestIdHeader === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(requestIdHeader)
      ? requestIdHeader.toLowerCase()
      : randomUUID();
    response.setHeader('x-request-id', requestId);
    let tenant: string | undefined;
    let installation: string | undefined;
    let scope: AuthScope | undefined;
    let bundleId: string | undefined;
    let fileId: string | undefined;
    let bytes = 0;
    let status = 500;
    const meterLog: Partial<Pick<LogEntry, 'active_uploads' | 'incomplete_bytes' | 'meter_pressure'>> = {};
    try {
      if (request.method === 'GET' && request.url === '/health') {
        status = send(response, 200, { status: 'ok' }); return;
      }
      scope = authenticate(request, options.keyRegistry);
      tenant = scope.tenant;
      installation = scope.installation;
      const url = new URL(request.url ?? '/', 'http://localhost');
      let match = /^\/v1\/bundles\/([^/]+)\/begin$/u.exec(url.pathname);
      if (request.method === 'POST' && match) {
        bundleId = decodeURIComponent(match[1]!);
        status = await withBundleIngestLease(meterStore, scope, bundleId, async () => {
          const body = await readJson(request, 1024 * 1024); bytes = body.bytes;
          return begin(options.store, meterStore, meterLimits, meterLog, scope!, bundleId!, body.value, response);
        }); return;
      }
      match = /^\/v1\/bundles\/([^/]+)\/files\/([^/]+)\/parts\/(\d+)$/u.exec(url.pathname);
      if (request.method === 'PUT' && match) {
        bundleId = decodeURIComponent(match[1]!); fileId = decodeURIComponent(match[2]!);
        const partIndex = Number(match[3]);
        status = await withBundleIngestLease(meterStore, scope, bundleId, async () => {
          const body = await readBytes(request, PART_BYTES); bytes = body.byteLength;
          return putPart(options.store, scope!, bundleId!, fileId!, partIndex, request.headers['x-semantic-layer-part-sha256'], body, response);
        }); return;
      }
      match = /^\/v1\/bundles\/([^/]+)\/complete$/u.exec(url.pathname);
      if (request.method === 'POST' && match) {
        bundleId = decodeURIComponent(match[1]!);
        status = await withBundleIngestLease(meterStore, scope, bundleId, async () => {
          const body = await readJson(request, 4096); bytes = body.bytes;
          return complete(options.store, meterStore, scope!, bundleId!, body.value, response);
        }); return;
      }
      throw new HttpError(404, 'NOT_FOUND');
    } catch (error) {
      const failure = error instanceof HttpError ? error : error instanceof ObjectConflictError ? new HttpError(409, 'IMMUTABLE_CONFLICT') : new HttpError(500, 'INTERNAL_ERROR');
      if (!scope && failure.scope) {
        scope = failure.scope;
        tenant = scope.tenant;
        installation = scope.installation;
      }
      if (scope && !scope.legacy) {
        const failureKind = failure.status === 401
          ? 'auth'
          : failure.status === 409
            ? 'conflict'
            : failure.status === 400
              ? 'validation'
              : undefined;
        if (failureKind) {
          try { await recordMeterFailure(meterStore, meteringPath(scope), failureKind); } catch { /* request outcome remains authoritative */ }
        }
      }
      status = send(response, failure.status, { error: failure.code, request_id: requestId });
    } finally {
      (options.logger ?? ((entry) => console.log(JSON.stringify(entry))))({ request_id: requestId, ...(tenant ? { tenant } : {}), ...(installation ? { installation } : {}), ...meterLog, bytes, status, latency_ms: Math.round(performance.now() - started) });
    }
  });
  return { server, close: () => new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve())) };
}

async function withBundleIngestLease<T>(
  meterStore: ObjectStore,
  scope: AuthScope,
  bundleId: string,
  action: () => Promise<T>,
): Promise<T> {
  if (scope.legacy || !validId(bundleId)) return action();
  const path = meteringPath(scope);
  const lease = await acquireBundleIngestLease(meterStore, path, bundleId);
  if (!lease) throw new HttpError(409, 'BUNDLE_CONFLICT');
  try {
    return await action();
  } finally {
    try { await releaseBundleIngestLease(meterStore, path, bundleId, lease); }
    catch { /* an unreleased lease deliberately leaves deletion blocked */ }
  }
}

function authenticate(request: IncomingMessage, registry: Record<string, KeyRegistryEntry>): AuthScope {
  const header = request.headers.authorization;
  if (!header?.startsWith('Bearer ') || header.length <= 7) throw new HttpError(401, 'UNAUTHORIZED');
  const entry = registry[sha256(Buffer.from(header.slice(7), 'utf8'))];
  if (typeof entry === 'string') {
    if (!validId(entry)) throw new HttpError(401, 'UNAUTHORIZED');
    return { tenant: entry, legacy: true };
  }
  if (!entry || !validId(entry.tenant_id) || !validInstallationId(entry.installation_id)) {
    throw new HttpError(401, 'UNAUTHORIZED');
  }
  if (entry.status !== 'active') throw new HttpError(401, 'UNAUTHORIZED', {
    tenant: entry.tenant_id,
    installation: entry.installation_id,
    legacy: false,
  });
  return { tenant: entry.tenant_id, installation: entry.installation_id, legacy: false };
}

async function begin(store: ObjectStore, meterStore: ObjectStore, meterLimits: MeterLimits, meterLog: Partial<Pick<LogEntry, 'active_uploads' | 'incomplete_bytes' | 'meter_pressure'>>, scope: AuthScope, bundleId: string, value: unknown, response: ServerResponse) {
  if (!validId(bundleId) || !value || typeof value !== 'object') throw new HttpError(400, 'INVALID_BEGIN');
  const candidate = value as Record<string, unknown>;
  const keys = Object.keys(candidate).sort();
  const files = parseDescriptors(candidate.files);
  if (keys.length !== 4
    || keys[0] !== 'bundle_digest'
    || keys[1] !== 'files'
    || keys[2] !== 'manifest'
    || keys[3] !== 'protocol_version'
    || candidate.protocol_version !== '1'
    || typeof candidate.bundle_digest !== 'string'
    || !/^[0-9a-f]{64}$/u.test(candidate.bundle_digest)
    || !files
    || !candidate.manifest
    || typeof candidate.manifest !== 'object'
    || Array.isArray(candidate.manifest)) throw new HttpError(400, 'INVALID_BEGIN');
  if ((candidate.manifest as Record<string, unknown>).bundle_id !== bundleId) throw new HttpError(400, 'BUNDLE_ID_MISMATCH');
  const manifestSchema = (candidate.manifest as Record<string, unknown>).schema;
  if (manifestSchema !== 'semantic_trace_manifest_v1' && manifestSchema !== 'semantic_trace_manifest_v2') {
    throw new HttpError(400, 'MANIFEST_SCHEMA_UNSUPPORTED');
  }
  if (manifestSchema === 'semantic_trace_manifest_v2') {
    if (scope.legacy) throw new HttpError(403, 'LEGACY_REGISTRY_READ_ONLY');
    if ((candidate.manifest as Record<string, unknown>).installation_id !== scope.installation) {
      throw new HttpError(403, 'INSTALLATION_MISMATCH');
    }
  }
  if ((await validateManifestEnvelope(candidate.manifest)).length) throw new HttpError(400, 'MANIFEST_INVALID');
  if (bundleDigest(files) !== candidate.bundle_digest) throw new HttpError(400, 'BUNDLE_DIGEST_MISMATCH');
  const normalized: Begin = { protocol_version: '1', bundle_digest: candidate.bundle_digest, manifest: candidate.manifest as Record<string, unknown>, files };
  const marker = await readCompleteMarker(store, scope, bundleId);
  if (marker) {
    if (marker.bundle_digest !== normalized.bundle_digest) throw new HttpError(409, 'BUNDLE_CONFLICT');
    if (!scope.legacy) await finishUpload(meterStore, meteringPath(scope), bundleId, normalized.bundle_digest);
    return send(response, 200, { status: 'complete', bundle_digest: normalized.bundle_digest });
  }
  if (scope.legacy) throw new HttpError(403, 'LEGACY_REGISTRY_READ_ONLY');
  const path = beginPath(scope, bundleId);
  const old = await store.read(path);
  if (old) {
    const previous = parseBegin(old);
    if (!isDeepStrictEqual(previous, normalized)) throw new HttpError(409, 'BUNDLE_CONFLICT');
  }
  const totalBytes = files.reduce((total, file) => total + file.size_bytes, 0);
  const meterPath = meteringPath(scope);
  const reservation = await reserveUpload(meterStore, meterPath, bundleId, normalized.bundle_digest, totalBytes, meterLimits);
  meterLog.active_uploads = reservation.activeUploads;
  meterLog.incomplete_bytes = reservation.incompleteBytes;
  meterLog.meter_pressure = reservation.pressure;
  if (!reservation.admitted) {
    if (reservation.reason === 'conflict') throw new HttpError(409, 'BUNDLE_CONFLICT');
    throw new HttpError(429, 'INGEST_CAPACITY_EXCEEDED');
  }
  if (old) return send(response, 200, { status: 'exists', bundle_digest: normalized.bundle_digest });
  try {
    await store.writeImmutable(path, Buffer.from(JSON.stringify(normalized)), 'application/json');
  } catch (error) {
    if (error instanceof ObjectConflictError) {
      const winner = await store.read(path);
      if (winner && isDeepStrictEqual(parseBegin(winner), normalized)) {
        return send(response, 200, { status: 'exists', bundle_digest: normalized.bundle_digest });
      }
    }
    if (reservation.created) await cancelReservation(meterStore, meterPath, bundleId, normalized.bundle_digest);
    if (error instanceof ObjectConflictError) throw new HttpError(409, 'BUNDLE_CONFLICT');
    throw error;
  }
  return send(response, 201, { status: 'begun', bundle_digest: normalized.bundle_digest });
}

async function putPart(store: ObjectStore, scope: AuthScope, bundleId: string, fileId: string, index: number, digestHeader: string | string[] | undefined, bytes: Buffer, response: ServerResponse) {
  if (scope.legacy) throw new HttpError(403, 'LEGACY_REGISTRY_READ_ONLY');
  if (!validId(bundleId) || !validFileId(fileId) || !Number.isSafeInteger(index)) throw new HttpError(400, 'INVALID_PART');
  if (typeof digestHeader !== 'string' || !/^[0-9a-f]{64}$/u.test(digestHeader) || sha256(bytes) !== digestHeader) throw new HttpError(400, 'PART_DIGEST_MISMATCH');
  const begin = await loadBegin(store, scope, bundleId);
  const file = begin.files.find((item) => item.file_id === fileId);
  if (!file || index < 0 || index >= file.parts) throw new HttpError(400, 'INVALID_PART');
  const expected = index === file.parts - 1 ? file.size_bytes - (index * PART_BYTES) : PART_BYTES;
  if (bytes.byteLength !== expected) throw new HttpError(400, 'PART_SIZE_MISMATCH');
  const result = await store.writeImmutable(partPath(scope, bundleId, begin.bundle_digest, fileId, index), bytes);
  return send(response, result === 'created' ? 201 : 200, { status: result });
}

async function complete(store: ObjectStore, meterStore: ObjectStore, scope: AuthScope, bundleId: string, value: unknown, response: ServerResponse) {
  if (!validId(bundleId) || !value || typeof value !== 'object') throw new HttpError(400, 'INVALID_COMPLETE');
  const candidate = value as Record<string, unknown>;
  const digest = candidate.bundle_digest;
  const keys = Object.keys(candidate).sort();
  if (candidate.protocol_version !== '1'
    || keys.length !== 2
    || keys[0] !== 'bundle_digest'
    || keys[1] !== 'protocol_version'
    || typeof digest !== 'string'
    || !/^[0-9a-f]{64}$/u.test(digest)) throw new HttpError(400, 'INVALID_COMPLETE');
  const finalPrefix = finalBundlePrefix(scope, bundleId);
  const marker = await readCompleteMarker(store, scope, bundleId);
  if (marker) {
    if (marker.bundle_digest !== digest) throw new HttpError(409, 'BUNDLE_CONFLICT');
    if (!scope.legacy) await finishUpload(meterStore, meteringPath(scope), bundleId, digest);
    return send(response, 200, { status: 'complete', bundle_digest: digest });
  }
  if (scope.legacy) throw new HttpError(403, 'LEGACY_REGISTRY_READ_ONLY');
  const begin = await loadBegin(store, scope, bundleId);
  if (begin.bundle_digest !== digest) throw new HttpError(409, 'BUNDLE_CONFLICT');
  const assembled = new Map<string, Buffer>();
  for (const file of begin.files) {
    const destination = assembledPath(scope, bundleId, digest, file.file_id);
    const sources = Array.from({ length: file.parts }, (_, index) => partPath(scope, bundleId, digest, file.file_id, index));
    for (const source of sources) if (!await store.read(source)) throw new HttpError(400, 'PART_MISSING');
    await store.composeImmutable(sources, destination);
    const bytes = await store.read(destination);
    if (!bytes || bytes.byteLength !== file.size_bytes || sha256(bytes) !== file.sha256) throw new HttpError(400, 'FILE_DIGEST_MISMATCH');
    assembled.set(file.path, bytes);
  }
  const uploadedManifest = assembled.get('manifest.json');
  let parsedManifest: unknown;
  try { parsedManifest = JSON.parse(uploadedManifest?.toString('utf8') ?? ''); } catch { throw new HttpError(400, 'MANIFEST_INVALID'); }
  if (!isDeepStrictEqual(parsedManifest, begin.manifest)) throw new HttpError(400, 'MANIFEST_MISMATCH');
  const issues = await validateCanonicalBundle(assembled);
  if (issues.length) throw new HttpError(400, 'BUNDLE_INVALID');
  for (const file of begin.files) {
    const source = assembledPath(scope, bundleId, digest, file.file_id);
    const target = `${finalPrefix}${file.path}`;
    await store.copyImmutable(source, target);
    const copied = await store.read(target);
    if (!copied || sha256(copied) !== file.sha256) throw new HttpError(409, 'IMMUTABLE_CONFLICT');
  }
  const completeBytes = Buffer.from(JSON.stringify({ protocol_version: '1', bundle_id: bundleId, bundle_digest: digest }));
  await store.writeImmutable(`${finalPrefix}complete.json`, completeBytes, 'application/json');
  await finishUpload(meterStore, meteringPath(scope), bundleId, digest);
  return send(response, 201, { status: 'complete', bundle_digest: digest });
}

function installationPrefix(scope: AuthScope) {
  if (!scope.installation) return `tenants/${scope.tenant}/`;
  return `tenants/${scope.tenant}/installations/${scope.installation}/`;
}
function meteringPath(scope: AuthScope) { return `metering/${installationPrefix(scope)}ledger.json`; }
function finalBundlePrefix(scope: AuthScope, bundleId: string) { return `${installationPrefix(scope)}bundles/${bundleId}/`; }
function uploadBundlePrefix(scope: AuthScope, bundleId: string) {
  if (!scope.installation) return `uploads/${scope.tenant}/${bundleId}/`;
  return `uploads/${installationPrefix(scope)}bundles/${bundleId}/`;
}
function beginPath(scope: AuthScope, bundleId: string) { return `${uploadBundlePrefix(scope, bundleId)}begin.json`; }
function partPath(scope: AuthScope, bundleId: string, digest: string, fileId: string, index: number) { return `${uploadBundlePrefix(scope, bundleId)}${digest}/files/${fileId}/parts/${String(index).padStart(5, '0')}`; }
function assembledPath(scope: AuthScope, bundleId: string, digest: string, fileId: string) { return `${uploadBundlePrefix(scope, bundleId)}${digest}/assembled/${fileId}`; }
function parseBegin(bytes: Buffer): Begin { try { return JSON.parse(bytes.toString('utf8')) as Begin; } catch { throw new HttpError(500, 'UPLOAD_STATE_INVALID'); } }
async function loadBegin(store: ObjectStore, scope: AuthScope, bundleId: string) { const bytes = await store.read(beginPath(scope, bundleId)); if (!bytes) throw new HttpError(404, 'UPLOAD_NOT_FOUND'); return parseBegin(bytes); }
async function readCompleteMarker(store: ObjectStore, scope: AuthScope, bundleId: string): Promise<{ protocol_version: '1'; bundle_id: string; bundle_digest: string } | undefined> {
  const bytes = await store.read(`${finalBundlePrefix(scope, bundleId)}complete.json`);
  if (!bytes) return undefined;
  try {
    const marker = JSON.parse(bytes.toString('utf8')) as Record<string, unknown>;
    const keys = Object.keys(marker).sort();
    if (keys.length !== 3
      || keys[0] !== 'bundle_digest'
      || keys[1] !== 'bundle_id'
      || keys[2] !== 'protocol_version'
      || marker.protocol_version !== '1'
      || marker.bundle_id !== bundleId
      || typeof marker.bundle_digest !== 'string'
      || !/^[0-9a-f]{64}$/u.test(marker.bundle_digest)) {
      throw new Error('invalid marker');
    }
    return marker as { protocol_version: '1'; bundle_id: string; bundle_digest: string };
  }
  catch { throw new HttpError(500, 'COMPLETE_MARKER_INVALID'); }
}

async function readJson(request: IncomingMessage, limit: number) { const data = await readBytes(request, limit); try { return { value: JSON.parse(data.toString('utf8')) as unknown, bytes: data.byteLength }; } catch { throw new HttpError(400, 'INVALID_JSON'); } }
async function readBytes(request: IncomingMessage, limit: number): Promise<Buffer> {
  const declared = request.headers['content-length'];
  if (typeof declared === 'string' && (!/^\d+$/u.test(declared) || Number(declared) > limit)) throw new HttpError(413, 'PAYLOAD_TOO_LARGE');
  const chunks: Buffer[] = []; let size = 0;
  for await (const chunk of request) { const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk); size += bytes.byteLength; if (size > limit) throw new HttpError(413, 'PAYLOAD_TOO_LARGE'); chunks.push(bytes); }
  return Buffer.concat(chunks);
}
function send(response: ServerResponse, status: number, value: unknown) { if (!response.headersSent) { response.writeHead(status, { 'content-type': 'application/json', 'cache-control': 'no-store' }); response.end(JSON.stringify(value)); } return status; }
