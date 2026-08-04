import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { Ajv2020, type ValidateFunction } from 'ajv/dist/2020.js';
import * as formatsNamespace from 'ajv-formats';

type Json = Record<string, unknown>;
type RecordMetadata = {
  kind?: string;
  containmentRoot?: string;
};
type BlobDeclaration = {
  bytes?: number;
  sha256?: string;
};
type ValidationState = {
  sources: ReadonlySet<string>;
  seen: Map<string, RecordMetadata>;
  expandableRequests: Set<string>;
  outcomes: Set<string>;
  blobs: Map<string, BlobDeclaration>;
  issues: Set<string>;
  losses: number;
};

const MAX_JSONL_RECORD_BYTES = 8 * 1024 * 1024;

let validators: Promise<{ manifestV1: ValidateFunction; manifestV2: ValidateFunction; record: ValidateFunction }> | undefined;

async function schemas() {
  validators ??= (async () => {
    const [manifestV1, manifestV2, record] = await Promise.all([
      readFile(new URL('../../../contracts/trace/v1/semantic-trace-manifest.schema.json', import.meta.url), 'utf8'),
      readFile(new URL('../../../contracts/trace/v2/semantic-trace-manifest.schema.json', import.meta.url), 'utf8'),
      readFile(new URL('../../../contracts/trace/v1/semantic-trace-record.schema.json', import.meta.url), 'utf8'),
    ]);
    const ajv = new Ajv2020({ allErrors: true, strict: false });
    (formatsNamespace.default as unknown as (instance: Ajv2020) => void)(ajv);
    return {
      manifestV1: ajv.compile(JSON.parse(manifestV1)),
      manifestV2: ajv.compile(JSON.parse(manifestV2)),
      record: ajv.compile(JSON.parse(record)),
    };
  })();
  return validators;
}

export async function validateCanonicalBundle(files: ReadonlyMap<string, Buffer>): Promise<string[]> {
  const manifestBytes = files.get('manifest.json');
  const traceBytes = files.get('trace.jsonl');
  if (!manifestBytes || !traceBytes) return ['REQUIRED_FILE_MISSING'];

  let manifestValue: unknown;
  try {
    manifestValue = JSON.parse(manifestBytes.toString('utf8')) as unknown;
  } catch {
    return ['ARTIFACT_UNREADABLE'];
  }

  const issues = new Set<string>();
  const manifest = isJsonObject(manifestValue) ? manifestValue : {};
  for (const issue of await validateManifestEnvelope(manifestValue)) issues.add(issue);
  const validator = await schemas();
  const sources = declaredSources(manifest, issues);
  const state: ValidationState = {
    sources,
    seen: new Map(),
    expandableRequests: new Set(),
    outcomes: new Set(),
    blobs: new Map(),
    issues,
    losses: 0,
  };

  let records = 0;
  let start = 0;
  while (start <= traceBytes.byteLength) {
    const newline = traceBytes.indexOf(0x0a, start);
    const end = newline === -1 ? traceBytes.byteLength : newline;
    if (end > start) {
      records += 1;
      const length = end - start;
      if (length > MAX_JSONL_RECORD_BYTES) {
        issues.add('TRACE_RECORD_TOO_LARGE');
      } else {
        let candidate: unknown;
        try {
          candidate = JSON.parse(traceBytes.toString('utf8', start, end)) as unknown;
        } catch {
          return ['ARTIFACT_UNREADABLE'];
        }
        validateRecord(candidate, records, validator.record, state);
      }
    }
    if (newline === -1) break;
    start = newline + 1;
  }

  const manifestTrace = isJsonObject(manifest.trace) ? manifest.trace : undefined;
  if (manifest.state !== 'sealed') issues.add('ARTIFACT_NOT_SEALED');
  if (manifestTrace?.records !== records || manifestTrace.last_seq !== records) issues.add('MANIFEST_COUNT_MISMATCH');
  if (manifestTrace?.bytes !== traceBytes.byteLength) issues.add('MANIFEST_BYTE_COUNT_MISMATCH');
  if (manifestTrace?.losses !== state.losses) issues.add('LOSS_COUNT_MISMATCH');
  if (manifestTrace?.sha256 !== createHash('sha256').update(traceBytes).digest('hex')) issues.add('TRACE_DIGEST_MISMATCH');

  let blobBytes = 0;
  for (const [path, declaration] of state.blobs) {
    const bytes = files.get(path);
    if (!bytes) {
      issues.add('BLOB_MISSING');
      continue;
    }
    blobBytes += bytes.byteLength;
    if (bytes.byteLength !== declaration.bytes) issues.add('BLOB_LENGTH_MISMATCH');
    if (createHash('sha256').update(bytes).digest('hex') !== declaration.sha256) issues.add('BLOB_DIGEST_MISMATCH');
  }
  const allowed = new Set(['manifest.json', 'trace.jsonl', ...state.blobs.keys()]);
  for (const path of files.keys()) if (!allowed.has(path)) issues.add('UNDECLARED_ARTIFACT_FILE');
  const manifestBlobs = isJsonObject(manifest.blobs) ? manifest.blobs : undefined;
  if (manifestBlobs?.count !== state.blobs.size) issues.add('MANIFEST_BLOB_COUNT_MISMATCH');
  if (manifestBlobs?.bytes !== blobBytes) issues.add('MANIFEST_BLOB_BYTE_COUNT_MISMATCH');
  return [...issues].sort();
}

export async function validateManifestEnvelope(manifestValue: unknown): Promise<string[]> {
  const manifest = isJsonObject(manifestValue) ? manifestValue : {};
  const validator = await schemas();
  const validateManifest = manifest.schema === 'semantic_trace_manifest_v1'
    ? validator.manifestV1
    : manifest.schema === 'semantic_trace_manifest_v2'
      ? validator.manifestV2
      : undefined;
  const issues = new Set<string>();
  if (!validateManifest) issues.add('MANIFEST_SCHEMA_UNSUPPORTED');
  else if (!validateManifest(manifestValue)) issues.add('MANIFEST_SCHEMA_INVALID');
  if (manifest.state !== 'sealed') issues.add('ARTIFACT_NOT_SEALED');
  return [...issues].sort();
}

function declaredSources(manifest: Json, issues: Set<string>): Set<string> {
  const sources = new Set<string>();
  for (const candidate of Array.isArray(manifest.sources) ? manifest.sources : []) {
    if (!isJsonObject(candidate)) continue;
    const id = boundedIdentifier(candidate.id);
    if (!id) continue;
    if (sources.has(id)) issues.add('SOURCE_DECLARATION_INVALID');
    sources.add(id);
  }
  return sources;
}

function validateRecord(
  candidate: unknown,
  expectedSequence: number,
  validateSchema: ValidateFunction,
  state: ValidationState,
): void {
  if (!validateSchema(candidate)) state.issues.add('RECORD_SCHEMA_INVALID');
  if (!isJsonObject(candidate)) {
    state.issues.add('SEQUENCE_INVALID');
    return;
  }
  const row = candidate;
  if (row.seq !== expectedSequence) state.issues.add('SEQUENCE_INVALID');
  if (row.kind === 'loss') state.losses += 1;

  const id = boundedIdentifier(row.id);
  const parent = boundedIdentifier(row.parent);
  const kind = typeof row.kind === 'string' && row.kind.length <= 64 ? row.kind : undefined;
  if (id && state.seen.has(id)) state.issues.add('RECORD_ID_DUPLICATE');
  if (typeof row.source !== 'string' || !state.sources.has(row.source)) state.issues.add('SOURCE_UNDECLARED');
  if (row.parent !== undefined && (!parent || !state.seen.has(parent))) state.issues.add('PARENT_REFERENCE_INVALID');
  for (const candidateLink of Array.isArray(row.links) ? row.links : []) {
    const linkedId = isJsonObject(candidateLink)
      ? boundedIdentifier(candidateLink.record)
      : undefined;
    if (!linkedId || !state.seen.has(linkedId)) state.issues.add('LINK_REFERENCE_INVALID');
  }
  if (row.kind === 'run.start' && row.parent !== undefined) state.issues.add('ROOT_START_INVALID');

  const containmentRoot = row.kind === 'run.start'
    ? id
    : parent === undefined
      ? undefined
      : state.seen.get(parent)?.containmentRoot;
  if (row.kind === 'model.request') validateModelContext(row, state, containmentRoot);
  if (row.kind === 'run.outcome') {
    const root = parent === undefined ? undefined : state.seen.get(parent);
    if (!root || root.kind !== 'run.start') state.issues.add('ROOT_OUTCOME_INVALID');
    else if (parent !== undefined && state.outcomes.has(parent)) state.issues.add('ROOT_OUTCOME_DUPLICATE');
    else if (parent !== undefined) state.outcomes.add(parent);
  }
  for (const candidateReference of Array.isArray(row.blob_refs) ? row.blob_refs : []) {
    if (!isJsonObject(candidateReference)) continue;
    const path = boundedBlobPath(candidateReference.path);
    if (!path) continue;
    const bytes = typeof candidateReference.bytes === 'number' ? candidateReference.bytes : undefined;
    const sha256 = typeof candidateReference.sha256 === 'string' && candidateReference.sha256.length <= 64
      ? candidateReference.sha256
      : undefined;
    const prior = state.blobs.get(path);
    if (prior && (prior.bytes !== candidateReference.bytes || prior.sha256 !== candidateReference.sha256)) {
      state.issues.add('BLOB_REFERENCE_MISMATCH');
    } else {
      state.blobs.set(path, { bytes, sha256 });
    }
  }
  if (id) state.seen.set(id, { ...(kind ? { kind } : {}), ...(containmentRoot ? { containmentRoot } : {}) });
}

function isJsonObject(value: unknown): value is Json {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function boundedIdentifier(value: unknown): string | undefined {
  return typeof value === 'string' && value.length <= 128 ? value : undefined;
}

function boundedBlobPath(value: unknown): string | undefined {
  return typeof value === 'string' && value.length <= 512 ? value : undefined;
}

function validateModelContext(
  row: Json,
  state: ValidationState,
  currentRoot: string | undefined,
): void {
  const data = isJsonObject(row.data) ? row.data : undefined;
  const hasContextRefs = data?.context_refs !== undefined;
  let valid = true;
  if (hasContextRefs) {
    if (!Array.isArray(data.context_refs)) valid = false;
    else for (const id of data.context_refs) {
      const contextId = boundedIdentifier(id);
      const context = contextId ? state.seen.get(contextId) : undefined;
      if (!context || !context.kind || !['message', 'model.response', 'tool.result'].includes(context.kind)) valid = false;
    }
    if (!valid) state.issues.add('CONTEXT_REFERENCE_INVALID');
  }
  if (data?.context_base_ref !== undefined) {
    const baseId = boundedIdentifier(data.context_base_ref);
    const base = baseId ? state.seen.get(baseId) : undefined;
    if (
      !hasContextRefs
      || !base
      || base.kind !== 'model.request'
      || baseId === undefined
      || !state.expandableRequests.has(baseId)
      || currentRoot === undefined
      || base.containmentRoot !== currentRoot
    ) {
      state.issues.add('CONTEXT_BASE_REFERENCE_INVALID');
      valid = false;
    }
  }
  const rowId = boundedIdentifier(row.id);
  if (rowId && hasContextRefs && valid) state.expandableRequests.add(rowId);
}
