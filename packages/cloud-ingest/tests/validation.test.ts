import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { expect, it } from 'vitest';
import { validateCanonicalBundle } from '../src/validation.js';

async function frameworkFixture() {
  const root = new URL('../../../contracts/trace/v1/examples/framework/', import.meta.url);
  return {
    manifest: JSON.parse(await readFile(new URL('manifest.json', root), 'utf8')) as Record<string, any>,
    trace: await readFile(new URL('trace.jsonl', root)),
  };
}

it('accepts an existing framework-neutral canonical bundle', async () => {
  const fixture = await frameworkFixture();
  expect(await validateCanonicalBundle(new Map([
    ['manifest.json', Buffer.from(`${JSON.stringify(fixture.manifest)}\n`)],
    ['trace.jsonl', fixture.trace],
  ]))).toEqual([]);
});

it('rejects a recovered artifact until the producer seals it', async () => {
  const fixture = await frameworkFixture();
  fixture.manifest.state = 'recovered';
  expect(await validateCanonicalBundle(new Map([
    ['manifest.json', Buffer.from(`${JSON.stringify(fixture.manifest)}\n`)],
    ['trace.jsonl', fixture.trace],
  ]))).toContain('ARTIFACT_NOT_SEALED');
});

it('selects strict manifest v2 while retaining record schema v1', async () => {
  const fixture = await frameworkFixture();
  const manifest = {
    ...fixture.manifest,
    schema: 'semantic_trace_manifest_v2',
    installation_id: 'install_AAAAAAAAAAAAAAAAAAAAAA',
    capture_policy: 'rich-credential-scrubbed',
    sources: fixture.manifest.sources.map((source: Record<string, unknown>) => ({
      ...source,
      qualification: { status: 'exact_qualified', profile: 'openclaw-2026.5.5' },
    })),
  };
  expect(await validateCanonicalBundle(new Map([
    ['manifest.json', Buffer.from(`${JSON.stringify(manifest)}\n`)],
    ['trace.jsonl', fixture.trace],
  ]))).toEqual([]);
  expect(await validateCanonicalBundle(new Map([
    ['manifest.json', Buffer.from(`${JSON.stringify({ ...manifest, schema: 'semantic_trace_manifest_v3' })}\n`)],
    ['trace.jsonl', fixture.trace],
  ]))).toContain('MANIFEST_SCHEMA_UNSUPPORTED');
});

it('accepts multiple independent roots without framework-specific rules', async () => {
  const fixture = await frameworkFixture();
  const original = fixture.trace.toString('utf8').split('\n').filter(Boolean).map((line) => JSON.parse(line) as Record<string, any>);
  const replacements = new Map(original.map((row) => [row.id as string, `copy.${row.id}`]));
  const replace = (value: any): any => {
    if (typeof value === 'string') return replacements.get(value) ?? value;
    if (Array.isArray(value)) return value.map(replace);
    if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, replace(item)]));
    return value;
  };
  const copy = original.map((row, index) => ({ ...replace(row), seq: original.length + index + 1 }));
  const rows = [...original, ...copy];
  const trace = Buffer.from(rows.map((row) => `${JSON.stringify(row)}\n`).join(''));
  const manifest = {
    ...fixture.manifest,
    bundle_id: 'bundle_multi_root',
    trace: {
      ...fixture.manifest.trace,
      records: rows.length,
      last_seq: rows.length,
      bytes: trace.byteLength,
      losses: rows.filter((row) => row.kind === 'loss').length,
      sha256: createHash('sha256').update(trace).digest('hex'),
    },
  };
  expect(await validateCanonicalBundle(new Map([
    ['manifest.json', Buffer.from(`${JSON.stringify(manifest)}\n`)],
    ['trace.jsonl', trace],
  ]))).toEqual([]);
});

it('returns validation issues instead of throwing for non-object JSONL rows', async () => {
  const fixture = await frameworkFixture();
  const trace = Buffer.from('null\n');
  const manifest = {
    ...fixture.manifest,
    trace: {
      ...fixture.manifest.trace,
      records: 1,
      last_seq: 1,
      bytes: trace.byteLength,
      losses: 0,
      sha256: createHash('sha256').update(trace).digest('hex'),
    },
  };
  await expect(validateCanonicalBundle(new Map([
    ['manifest.json', Buffer.from(`${JSON.stringify(manifest)}\n`)],
    ['trace.jsonl', trace],
  ]))).resolves.toContain('RECORD_SCHEMA_INVALID');
});

it('accepts a valid final JSONL record without a trailing newline', async () => {
  const fixture = await frameworkFixture();
  const trace = fixture.trace.subarray(0, fixture.trace.at(-1) === 0x0a ? fixture.trace.byteLength - 1 : undefined);
  const manifest = {
    ...fixture.manifest,
    trace: {
      ...fixture.manifest.trace,
      bytes: trace.byteLength,
      sha256: createHash('sha256').update(trace).digest('hex'),
    },
  };
  expect(await validateCanonicalBundle(new Map([
    ['manifest.json', Buffer.from(`${JSON.stringify(manifest)}\n`)],
    ['trace.jsonl', trace],
  ]))).toEqual([]);
});

it('rejects an oversized newline-free record without parsing it', async () => {
  const fixture = await frameworkFixture();
  const trace = Buffer.alloc((8 * 1024 * 1024) + 1, 0x20);
  const manifest = {
    ...fixture.manifest,
    trace: {
      ...fixture.manifest.trace,
      records: 1,
      last_seq: 1,
      bytes: trace.byteLength,
      losses: 0,
      sha256: createHash('sha256').update(trace).digest('hex'),
    },
  };
  await expect(validateCanonicalBundle(new Map([
    ['manifest.json', Buffer.from(`${JSON.stringify(manifest)}\n`)],
    ['trace.jsonl', trace],
  ]))).resolves.toContain('TRACE_RECORD_TOO_LARGE');
});
