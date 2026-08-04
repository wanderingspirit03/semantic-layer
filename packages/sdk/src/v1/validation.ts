import { createHash } from 'node:crypto';
import { lstat, readFile, readdir } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { Ajv2020, type ValidateFunction } from 'ajv/dist/2020.js';
import * as formatsNamespace from 'ajv-formats';
import { assertNoSymbolicLinkComponents } from './permissions.js';
import { CredentialScanner } from './secret-scanner.js';

export type ValidationProfile = 'structural' | 'rich-agent';
export type RequiredEvidence = 'root' | 'model' | 'tool' | 'delivery';
const REQUIRED_EVIDENCE = new Set<RequiredEvidence>([
  'root',
  'model',
  'tool',
  'delivery',
]);
export type SourceActivity = {
  sourceId: string;
  name: string;
  records: number;
};
export type ValidationReport = {
  valid: boolean;
  profile: ValidationProfile;
  issues: string[];
  rows: number;
  secretMatches: number;
  sourceActivity: SourceActivity[];
};
export type ValidationOptions = {
  secretValues?: readonly string[];
  profile?: ValidationProfile;
  requiredEvidence?: readonly RequiredEvidence[];
  requiredSourceActivity?: readonly string[];
};
type InjectedArtifact = {
  manifest: unknown;
  rows: unknown[];
} & ValidationOptions;

type JsonRecord = Record<string, any>;
type ArtifactEntry = {
  path: string;
  kind: 'directory' | 'file' | 'other';
};

const CONTEXT_RECORD_KINDS = new Set(['message', 'model.response', 'tool.result']);

let validators: Promise<{
  manifestV1: ValidateFunction;
  manifestV2: ValidateFunction;
  record: ValidateFunction;
}> | undefined;

export async function validateArtifact(
  artifactPath: string,
  input?: InjectedArtifact | ValidationOptions,
): Promise<ValidationReport> {
  const profile = input?.profile ?? 'structural';
  if (profile !== 'structural' && profile !== 'rich-agent') {
    throw Object.assign(new TypeError('profile must be structural or rich-agent'), {
      code: 'VALIDATION_PROFILE_INVALID',
    });
  }

  const injected = input && 'manifest' in input ? input : undefined;
  let manifest: JsonRecord;
  let rows: JsonRecord[];
  let traceBytes: Buffer;
  if (injected) {
    manifest = injected.manifest as JsonRecord;
    rows = injected.rows as JsonRecord[];
    traceBytes = encodeRows(rows);
  } else {
    try {
      assertNoSymbolicLinkComponents(artifactPath, 'directory');
      const root = await lstat(resolve(artifactPath));
      if (!root.isDirectory() || root.isSymbolicLink()) throw new Error('artifact root is not a directory');
      [manifest, traceBytes] = await Promise.all([
        readBundleFile(artifactPath, 'manifest.json')
          .then((bytes) => JSON.parse(bytes.toString('utf8'))) as Promise<JsonRecord>,
        readBundleFile(artifactPath, 'trace.jsonl'),
      ]);
      rows = parseRows(traceBytes);
    } catch {
      return {
        valid: false,
        profile,
        issues: ['ARTIFACT_UNREADABLE'],
        rows: 0,
        secretMatches: 0,
        sourceActivity: [],
      };
    }
  }

  const issues: string[] = [];
  const schema = await loadValidators();
  const manifestValidator = manifest?.schema === 'semantic_trace_manifest_v1'
    ? schema.manifestV1
    : manifest?.schema === 'semantic_trace_manifest_v2'
      ? schema.manifestV2
      : undefined;
  if (!manifestValidator || !manifestValidator(manifest)) issues.push('MANIFEST_SCHEMA_INVALID');
  rows.forEach((row, index) => {
    if (!schema.record(row)) issues.push('RECORD_SCHEMA_INVALID');
    if (row?.seq !== index + 1) issues.push('SEQUENCE_INVALID');
  });

  validateReferences(manifest, rows, issues);
  validateManifestAccounting(manifest, rows, traceBytes, issues);
  if (profile === 'rich-agent') validateRichAgentProfile(rows, issues);
  validateRequiredEvidence(rows, input?.requiredEvidence ?? [], issues);
  const sourceActivity = sourceActivityOf(manifest, rows);
  validateRequiredSourceActivity(
    sourceActivity,
    input?.requiredSourceActivity ?? [],
    issues,
  );

  const scanner = new CredentialScanner(input?.secretValues);
  let secretMatches = 0;
  if (injected) {
    secretMatches += scanner.scan(Buffer.from(JSON.stringify(manifest))) ? 0 : 1;
    secretMatches += scanner.scan(traceBytes) ? 0 : 1;
  } else {
    if (manifest?.state !== 'sealed' && manifest?.state !== 'recovered') {
      issues.push('ARTIFACT_NOT_SEALED');
    }
    const filesystem = await validateFilesystem(
      artifactPath,
      manifest,
      rows,
      scanner,
      issues,
    );
    secretMatches += filesystem.secretMatches;
  }
  if (secretMatches > 0) issues.push('SECRET_MATCH');

  const uniqueIssues = [...new Set(issues)].sort();
  return {
    valid: uniqueIssues.length === 0,
    profile,
    issues: uniqueIssues,
    rows: rows.length,
    secretMatches,
    sourceActivity,
  };
}

async function loadValidators(): Promise<Awaited<NonNullable<typeof validators>>> {
  validators ??= (async () => {
    const [manifestV1, manifestV2, record] = await Promise.all([
      readFile(
        new URL('../../schemas/semantic-trace-manifest.schema.json', import.meta.url),
        'utf8',
      ),
      readFile(
        new URL('../../schemas/semantic-trace-manifest-v2.schema.json', import.meta.url),
        'utf8',
      ),
      readFile(
        new URL('../../schemas/semantic-trace-record.schema.json', import.meta.url),
        'utf8',
      ),
    ]);
    const ajv = new Ajv2020({ allErrors: true, strict: false });
    const installFormats = formatsNamespace.default as unknown as (instance: Ajv2020) => void;
    installFormats(ajv);
    return {
      manifestV1: ajv.compile(JSON.parse(manifestV1)),
      manifestV2: ajv.compile(JSON.parse(manifestV2)),
      record: ajv.compile(JSON.parse(record)),
    };
  })();
  return validators;
}

function validateReferences(manifest: JsonRecord, rows: JsonRecord[], issues: string[]): void {
  const declaredSources = new Set<string>();
  for (const source of manifest?.sources ?? []) {
    if (declaredSources.has(source?.id)) issues.push('SOURCE_DECLARATION_INVALID');
    if (typeof source?.id === 'string') declaredSources.add(source.id);
  }

  const seen = new Map<string, JsonRecord>();
  const containmentRoots = new Map<string, string>();
  const expandableRequests = new Set<string>();
  const outcomes = new Map<string, number>();
  for (const row of rows) {
    if (seen.has(row?.id)) issues.push('RECORD_ID_DUPLICATE');
    if (!declaredSources.has(row?.source)) issues.push('SOURCE_UNDECLARED');

    if (row?.parent !== undefined && !seen.has(row.parent)) {
      issues.push('PARENT_REFERENCE_INVALID');
    }
    for (const link of Array.isArray(row?.links) ? row.links : []) {
      if (!seen.has(link?.record)) issues.push('LINK_REFERENCE_INVALID');
    }

    if (row?.kind === 'run.start' && row.parent !== undefined) {
      issues.push('ROOT_START_INVALID');
    }
    const containmentRoot = row?.kind === 'run.start'
      ? row.id
      : containmentRoots.get(row?.parent);
    if (typeof row?.id === 'string' && typeof containmentRoot === 'string') {
      containmentRoots.set(row.id, containmentRoot);
    }
    if (row?.kind === 'model.request') {
      validateModelRequestContext(
        row,
        seen,
        containmentRoots,
        expandableRequests,
        containmentRoot,
        issues,
      );
    }
    if (row?.kind === 'run.outcome') {
      const root = row.parent === undefined ? undefined : seen.get(row.parent);
      if (!root || root.kind !== 'run.start') {
        issues.push('ROOT_OUTCOME_INVALID');
      } else {
        const count = (outcomes.get(root.id) ?? 0) + 1;
        outcomes.set(root.id, count);
        if (count > 1) issues.push('ROOT_OUTCOME_DUPLICATE');
      }
    }

    if (typeof row?.id === 'string') seen.set(row.id, row);
  }
}

function validateModelRequestContext(
  row: JsonRecord,
  seen: Map<string, JsonRecord>,
  containmentRoots: Map<string, string>,
  expandableRequests: Set<string>,
  currentRoot: string | undefined,
  issues: string[],
): void {
  const data = row?.data;
  let contextRefsValid = true;
  const hasContextRefs = data?.context_refs !== undefined;
  if (data?.context_refs !== undefined) {
    if (!Array.isArray(data.context_refs)) {
      issues.push('CONTEXT_REFERENCE_INVALID');
      contextRefsValid = false;
    } else {
      for (const reference of data.context_refs) {
        const context = typeof reference === 'string' ? seen.get(reference) : undefined;
        if (!context || !CONTEXT_RECORD_KINDS.has(context.kind)) {
          issues.push('CONTEXT_REFERENCE_INVALID');
          contextRefsValid = false;
        }
      }
    }
  }

  let baseValid = true;
  if (data?.context_base_ref !== undefined) {
    const baseId = data.context_base_ref;
    const base = typeof baseId === 'string' ? seen.get(baseId) : undefined;
    const baseRoot = typeof baseId === 'string'
      ? containmentRoots.get(baseId)
      : undefined;
    if (
      !hasContextRefs
      || !base
      || base.kind !== 'model.request'
      || !expandableRequests.has(baseId)
      || currentRoot === undefined
      || baseRoot === undefined
      || currentRoot !== baseRoot
    ) {
      issues.push('CONTEXT_BASE_REFERENCE_INVALID');
      baseValid = false;
    }
  }

  if (
    typeof row?.id === 'string'
    && hasContextRefs
    && contextRefsValid
    && baseValid
  ) {
    expandableRequests.add(row.id);
  }
}

function validateManifestAccounting(
  manifest: JsonRecord,
  rows: JsonRecord[],
  traceBytes: Buffer,
  issues: string[],
): void {
  const trace = manifest?.trace;
  if (trace?.records !== rows.length || trace?.last_seq !== rows.length) {
    issues.push('MANIFEST_COUNT_MISMATCH');
  }
  if (trace?.bytes !== traceBytes.byteLength) {
    issues.push('MANIFEST_BYTE_COUNT_MISMATCH');
  }
  const losses = rows.filter((row) => row?.kind === 'loss').length;
  if (trace?.losses !== losses) issues.push('LOSS_COUNT_MISMATCH');
  const digest = createHash('sha256').update(traceBytes).digest('hex');
  if (trace?.sha256 !== digest) issues.push('TRACE_DIGEST_MISMATCH');
}

function validateRichAgentProfile(rows: JsonRecord[], issues: string[]): void {
  const evidence = evidenceRoots(rows);
  if (!evidence.rootPair) issues.push('PROFILE_PAIR_MISSING:root');
  if (!evidence.modelPair) issues.push('PROFILE_PAIR_MISSING:model');
  if (!evidence.toolPair) issues.push('PROFILE_PAIR_MISSING:tool');
  if (
    evidence.rootPair
    && evidence.modelPair
    && evidence.toolPair
    && ![...evidence.outcomeRoots].some((root) => (
      evidence.modelRoots.has(root) && evidence.toolRoots.has(root)
    ))
  ) {
    issues.push('PROFILE_PAIR_ROOT_MISMATCH');
  }
}

function evidenceRoots(rows: JsonRecord[]): {
  rootPair: boolean;
  modelPair: boolean;
  toolPair: boolean;
  delivery: boolean;
  outcomeRoots: Set<string>;
  modelRoots: Set<string>;
  toolRoots: Set<string>;
} {
  const records = new Map<string, JsonRecord>();
  const containmentRoots = new Map<string, string>();
  for (const row of rows) {
    if (typeof row?.id !== 'string') continue;
    records.set(row.id, row);
    const root = row.kind === 'run.start'
      ? row.id
      : containmentRoots.get(row.parent);
    if (root !== undefined) containmentRoots.set(row.id, root);
  }
  const outcomeRoots = new Set<string>();
  for (const row of rows) {
    if (
      row?.kind === 'run.outcome'
      && typeof row.parent === 'string'
      && records.get(row.parent)?.kind === 'run.start'
    ) {
      outcomeRoots.add(row.parent);
    }
  }
  const modelRoots = resultPairRoots(
    rows,
    records,
    containmentRoots,
    'model.response',
    'model.request',
    'completed',
  );
  const toolRoots = resultPairRoots(
    rows,
    records,
    containmentRoots,
    'tool.result',
    'tool.call',
    'succeeded',
  );
  const rootPair = outcomeRoots.size > 0;
  const modelPair = modelRoots.size > 0;
  const toolPair = toolRoots.size > 0;
  const delivery = rows.some((row) => {
    if (
      row?.kind !== 'run.outcome'
      || row.data?.status !== 'completed'
      || typeof row.parent !== 'string'
    ) return false;
    if (Object.prototype.hasOwnProperty.call(row.data ?? {}, 'output')) return true;
    return (Array.isArray(row.links) ? row.links : []).some((link) => (
      link?.type === 'derived_from'
      && typeof link.record === 'string'
      && successfulDeliveryTarget(records.get(link.record))
      && containmentRoots.get(link.record) === row.parent
    ));
  });
  return {
    rootPair,
    modelPair,
    toolPair,
    delivery,
    outcomeRoots,
    modelRoots,
    toolRoots,
  };
}

function successfulDeliveryTarget(record: JsonRecord | undefined): boolean {
  if (record?.kind === 'message' || record?.kind === 'state') return true;
  if (record?.kind === 'model.response') return record.data?.status === 'completed';
  if (record?.kind === 'tool.result') return record.data?.status === 'succeeded';
  return false;
}

function validateRequiredEvidence(
  rows: JsonRecord[],
  required: readonly RequiredEvidence[],
  issues: string[],
): void {
  const evidence = evidenceRoots(rows);
  const available: Record<RequiredEvidence, boolean> = {
    root: evidence.rootPair,
    model: evidence.modelPair,
    tool: evidence.toolPair,
    delivery: evidence.delivery,
  };
  for (const requirement of new Set(required)) {
    if (!REQUIRED_EVIDENCE.has(requirement)) {
      throw Object.assign(
        new TypeError(`requiredEvidence contains unknown value: ${String(requirement)}`),
        { code: 'REQUIRED_EVIDENCE_INVALID' },
      );
    }
    if (!available[requirement]) issues.push(`REQUIRED_EVIDENCE_MISSING:${requirement}`);
  }
}

function sourceActivityOf(manifest: JsonRecord, rows: JsonRecord[]): SourceActivity[] {
  const counts = new Map<string, number>();
  for (const row of rows) {
    if (typeof row?.source === 'string') {
      counts.set(row.source, (counts.get(row.source) ?? 0) + 1);
    }
  }
  return (Array.isArray(manifest?.sources) ? manifest.sources : []).flatMap((source) => (
    typeof source?.id === 'string' && typeof source?.name === 'string'
      ? [{
        sourceId: source.id,
        name: source.name,
        records: counts.get(source.id) ?? 0,
      }]
      : []
  ));
}

function validateRequiredSourceActivity(
  activity: readonly SourceActivity[],
  required: readonly string[],
  issues: string[],
): void {
  for (const name of new Set(required)) {
    if (!activity.some((source) => source.name === name && source.records > 0)) {
      issues.push(`REQUIRED_SOURCE_ACTIVITY_MISSING:${name}`);
    }
  }
}

function resultPairRoots(
  rows: JsonRecord[],
  records: Map<string, JsonRecord>,
  containmentRoots: Map<string, string>,
  resultKind: string,
  requestKind: string,
  resultStatus: string,
): Set<string> {
  const roots = new Set<string>();
  for (const row of rows) {
    if (row?.kind !== resultKind || row.data?.status !== resultStatus) continue;
    const resultRoot = containmentRoots.get(row.id);
    for (const link of Array.isArray(row.links) ? row.links : []) {
      if (link?.type !== 'result_of' || typeof link.record !== 'string') continue;
      const request = records.get(link.record);
      if (
        request?.kind === requestKind
        && resultRoot !== undefined
        && containmentRoots.get(request.id) === resultRoot
      ) {
        roots.add(resultRoot);
      }
    }
  }
  return roots;
}

async function validateFilesystem(
  artifactPath: string,
  manifest: JsonRecord,
  rows: JsonRecord[],
  scanner: CredentialScanner,
  issues: string[],
): Promise<{ secretMatches: number }> {
  let entries: ArtifactEntry[];
  try {
    entries = await walkEntries(artifactPath);
    if (!ownerOnly((await lstat(artifactPath)).mode)) issues.push('DIRECTORY_PERMISSION_INVALID');
  } catch {
    issues.push('ARTIFACT_UNREADABLE');
    return { secretMatches: 0 };
  }

  const blobs = declaredBlobs(rows, issues);
  const allowedFiles = new Set(['manifest.json', 'trace.jsonl', ...blobs.keys()]);
  const allowedDirectories = new Set(['blobs']);
  for (const path of blobs.keys()) {
    let directory = dirname(path);
    while (directory !== '.' && directory !== '') {
      allowedDirectories.add(directory);
      directory = dirname(directory);
    }
  }

  let secretMatches = 0;
  for (const entry of entries) {
    const allowed = entry.kind === 'file'
      ? allowedFiles.has(entry.path)
      : entry.kind === 'directory' && allowedDirectories.has(entry.path);
    if (!allowed) issues.push('UNDECLARED_ARTIFACT_FILE');
    if (entry.kind === 'other') issues.push('FILE_TYPE_INVALID');

    let fileStat;
    try {
      fileStat = await lstat(join(artifactPath, entry.path));
    } catch {
      issues.push('ARTIFACT_UNREADABLE');
      continue;
    }
    if (!ownerOnly(fileStat.mode)) {
      issues.push(entry.kind === 'directory'
        ? 'DIRECTORY_PERMISSION_INVALID'
        : 'FILE_PERMISSION_INVALID');
    }
    if (entry.kind === 'file') {
      try {
        const bytes = await readBundleFile(artifactPath, entry.path);
        if (!scanner.scan(bytes)) secretMatches += 1;
      } catch {
        issues.push('ARTIFACT_UNREADABLE');
      }
    }
  }

  let blobBytes = 0;
  for (const [path, reference] of blobs) {
    try {
      const bytes = await readBundleFile(artifactPath, path);
      blobBytes += bytes.byteLength;
      if (bytes.byteLength !== reference.bytes) issues.push('BLOB_LENGTH_MISMATCH');
      if (createHash('sha256').update(bytes).digest('hex') !== reference.sha256) {
        issues.push('BLOB_DIGEST_MISMATCH');
      }
    } catch {
      issues.push('BLOB_MISSING');
    }
  }
  if (manifest?.blobs?.count !== blobs.size) issues.push('MANIFEST_BLOB_COUNT_MISMATCH');
  if (manifest?.blobs?.bytes !== blobBytes) issues.push('MANIFEST_BLOB_BYTE_COUNT_MISMATCH');
  return { secretMatches };
}

function declaredBlobs(rows: JsonRecord[], issues: string[]): Map<string, JsonRecord> {
  const declarations = new Map<string, JsonRecord>();
  for (const row of rows) {
    for (const reference of Array.isArray(row?.blob_refs) ? row.blob_refs : []) {
      if (!safeBlobPath(reference?.path)) continue;
      const previous = declarations.get(reference.path);
      if (
        previous
        && (previous.sha256 !== reference.sha256 || previous.bytes !== reference.bytes)
      ) {
        issues.push('BLOB_REFERENCE_MISMATCH');
      } else {
        declarations.set(reference.path, reference);
      }
    }
  }
  return declarations;
}

function safeBlobPath(value: unknown): value is string {
  if (typeof value !== 'string' || value.length > 512 || !value.startsWith('blobs/')) {
    return false;
  }
  const segments = value.split('/');
  return segments.length > 1
    && segments.every((segment) => (
      segment !== '.'
      && segment !== '..'
      && /^[a-zA-Z0-9._-]+$/u.test(segment)
    ));
}

function ownerOnly(mode: number): boolean {
  return (mode & 0o077) === 0;
}

function parseRows(bytes: Buffer): JsonRecord[] {
  const text = bytes.toString('utf8');
  return text.split('\n').filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as JsonRecord);
}

function encodeRows(rows: JsonRecord[]): Buffer {
  if (rows.length === 0) return Buffer.alloc(0);
  return Buffer.from(rows.map((row) => `${JSON.stringify(row)}\n`).join(''));
}

async function walkEntries(root: string, relative = ''): Promise<ArtifactEntry[]> {
  const entries = await readdir(join(root, relative), { withFileTypes: true });
  const output: ArtifactEntry[] = [];
  for (const entry of entries) {
    const child = relative ? `${relative}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      output.push({ path: child, kind: 'directory' });
      output.push(...await walkEntries(root, child));
    } else if (entry.isFile()) {
      output.push({ path: child, kind: 'file' });
    } else {
      output.push({ path: child, kind: 'other' });
    }
  }
  return output;
}

async function readBundleFile(root: string, relative: string): Promise<Buffer> {
  const segments = relative.split('/');
  if (segments.length === 0 || segments.some((segment) => (
    !segment || segment === '.' || segment === '..' || segment.includes('\\') || segment.includes('\0')
  ))) {
    throw new Error('artifact path is not portable and relative');
  }
  let current = resolve(root);
  for (let index = 0; index < segments.length; index += 1) {
    current = join(current, segments[index]!);
    const entry = await lstat(current);
    if (entry.isSymbolicLink()) throw new Error('artifact path contains a symbolic link or junction');
    if (index < segments.length - 1 && !entry.isDirectory()) {
      throw new Error('artifact path component is not a directory');
    }
    if (index === segments.length - 1 && !entry.isFile()) {
      throw new Error('artifact path is not a regular file');
    }
  }
  return await readFile(current);
}
