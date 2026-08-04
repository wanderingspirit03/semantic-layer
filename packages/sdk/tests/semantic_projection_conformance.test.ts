import { readFile } from 'node:fs/promises';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { expect, it } from 'vitest';

import { SemanticProjector } from '../src/trace/semantic-projector.js';
import type { SemanticCaptureEventV1 } from '../src/v1/generated.js';

type Corpus = {
  defaults: SemanticCaptureEventV1;
  cases: Array<{
    name: string;
    events: Array<Partial<SemanticCaptureEventV1> & Pick<
      SemanticCaptureEventV1,
      'record_id' | 'event_kind' | 'phase' | 'name'
    >>;
    expected: unknown[];
  }>;
};

type CorpusEvent = Corpus['cases'][number]['events'][number];
type TraceRecord = Record<string, any>;

it('projects the shared semantic corpus exactly', async () => {
  const root = new URL('../../../', import.meta.url);
  const [corpus, captureSchema, recordSchema] = await Promise.all([
    readFile(new URL('contracts/capture/v1/semantic-projection-cases.json', root), 'utf8')
      .then((value) => JSON.parse(value) as Corpus),
    readFile(new URL('contracts/capture/v1/semantic-capture-event.schema.json', root), 'utf8')
      .then(JSON.parse),
    readFile(new URL('contracts/trace/v1/semantic-trace-record.schema.json', root), 'utf8')
      .then(JSON.parse),
  ]);
  const ajv = new Ajv2020({ allErrors: true, strict: false });
  addFormats(ajv);
  const validateCapture = ajv.compile(captureSchema);
  const validateRecord = ajv.compile(recordSchema);

  for (const fixture of corpus.cases) {
    const projector = new SemanticProjector();
    const projected = fixture.events.flatMap((event, index) => {
      const row = {
        ...corpus.defaults,
        ...event,
        seq: index + 1,
        monotonic_ns: index + 1,
      } as SemanticCaptureEventV1;
      expect(validateCapture(row), `${fixture.name}: invalid capture input`).toBe(true);
      return projector.project(row);
    });
    for (const record of projected) {
      expect(validateRecord(record), `${fixture.name}: invalid trace output`).toBe(true);
    }
    expect(
      normalizeGeneratedIdentifiers(projected, fixture.events),
      fixture.name,
    ).toEqual(fixture.expected);
  }
});

it('normalizes generated IDs without rewriting caller IDs or relationships', () => {
  const normalized = normalizeGeneratedIdentifiers([
    {
      id: 'loss_internal_hash',
      source: 'src_internal_hash',
      kind: 'loss',
      data: {},
    },
    {
      id: 'rec_caller',
      source: 'src_internal_hash',
      kind: 'state',
      parent: 'loss_internal_hash',
      links: [{ type: 'derived_from', record: 'loss_internal_hash' }],
      data: { type: 'state.done' },
    },
  ], [{ record_id: 'rec_caller', semantic: {} } as CorpusEvent]);

  expect(normalized).toEqual([
    {
      id: '__generated_record_1__',
      source: '__generated_source_1__',
      kind: 'loss',
      data: {},
    },
    {
      id: 'rec_caller',
      source: '__generated_source_1__',
      kind: 'state',
      parent: '__generated_record_1__',
      links: [{ type: 'derived_from', record: '__generated_record_1__' }],
      data: { type: 'state.done' },
    },
  ]);
});

function normalizeGeneratedIdentifiers(
  records: TraceRecord[],
  events: CorpusEvent[],
): TraceRecord[] {
  const callerRecordIds = new Set(events.map((event) => event.record_id));
  const callerScopeIds = new Set(events.flatMap((event) => (
    typeof event.semantic?.scope_id === 'string' ? [event.semantic.scope_id] : []
  )));
  const recordIds = new Map<string, string>();
  const sourceIds = new Map<string, string>();
  const callIds = new Map<string, string>();
  const scopeIds = new Map<string, string>();

  for (const record of records) {
    if (typeof record.id === 'string' && !callerRecordIds.has(record.id)) {
      placeholder(recordIds, record.id, 'record');
    }
    if (typeof record.source === 'string') placeholder(sourceIds, record.source, 'source');
    if (
      ['tool.proposal', 'tool.call', 'tool.result'].includes(record.kind)
      && typeof record.data?.call_id === 'string'
    ) {
      placeholder(callIds, record.data.call_id, 'call');
    }
    if (
      record.kind === 'scope'
      && typeof record.data?.scope_id === 'string'
      && !callerScopeIds.has(record.data.scope_id)
    ) {
      placeholder(scopeIds, record.data.scope_id, 'scope');
    }
  }

  return records.map((input) => {
    const record = structuredClone(input);
    record.id = recordIds.get(record.id) ?? record.id;
    record.source = sourceIds.get(record.source) ?? record.source;
    if (typeof record.parent === 'string') {
      record.parent = recordIds.get(record.parent) ?? record.parent;
    }
    for (const link of Array.isArray(record.links) ? record.links : []) {
      link.record = recordIds.get(link.record) ?? link.record;
    }
    if (typeof record.data?.call_id === 'string' && callIds.has(record.data.call_id)) {
      record.data.call_id = callIds.get(record.data.call_id);
    }
    if (typeof record.data?.scope_id === 'string' && scopeIds.has(record.data.scope_id)) {
      record.data.scope_id = scopeIds.get(record.data.scope_id);
    }
    return record;
  });
}

function placeholder(
  values: Map<string, string>,
  value: string,
  kind: string,
): string {
  const existing = values.get(value);
  if (existing) return existing;
  const generated = `__generated_${kind}_${values.size + 1}__`;
  values.set(value, generated);
  return generated;
}
