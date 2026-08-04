import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';

const contractDirectory = dirname(fileURLToPath(import.meta.url));
const exampleNames = ['coding-agent', 'framework'];

const manifestSchema = JSON.parse(
  await readFile(join(contractDirectory, 'semantic-trace-manifest.schema.json'), 'utf8'),
);
const recordSchema = JSON.parse(
  await readFile(join(contractDirectory, 'semantic-trace-record.schema.json'), 'utf8'),
);

const ajv = new Ajv2020({ allErrors: true, strict: true, strictRequired: false });
addFormats(ajv);
const validateManifest = ajv.compile(manifestSchema);
const validateRecord = ajv.compile(recordSchema);

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function schemaErrors(validator) {
  return ajv.errorsText(validator.errors, { separator: '\n' });
}

function oneLink(record, type, recordsById) {
  const links = (record.links ?? []).filter((link) => link.type === type);
  assert(links.length === 1, `${record.id}: expected exactly one ${type} link`);
  return recordsById.get(links[0].record);
}

assert(
  !validateRecord({
    id: 'rec_path_check',
    seq: 1,
    time: '2026-07-25T00:00:00.000Z',
    kind: 'run.start',
    origin: 'observed',
    source: 'src_check',
    data: { name: 'path check' },
    blob_refs: [{
      path: 'blobs/../../secret',
      sha256: '0'.repeat(64),
      bytes: 1,
      media_type: 'text/plain',
      scan: 'clean',
    }],
  }),
  'blob paths must not escape the bundle',
);

async function validateExample(name) {
  const directory = join(contractDirectory, 'examples', name);
  const manifest = JSON.parse(await readFile(join(directory, 'manifest.json'), 'utf8'));
  const rawTrace = await readFile(join(directory, 'trace.jsonl'));
  const records = rawTrace
    .toString('utf8')
    .split('\n')
    .filter((line) => line.length > 0)
    .map((line, index) => {
      try {
        return JSON.parse(line);
      } catch (error) {
        throw new Error(`${name}: trace line ${index + 1} is not JSON: ${error.message}`);
      }
    });

  assert(
    validateManifest(manifest),
    `${name}: manifest schema failed\n${schemaErrors(validateManifest)}`,
  );

  const sourceIds = new Set();
  for (const source of manifest.sources) {
    assert(!sourceIds.has(source.id), `${name}: duplicate source ${source.id}`);
    sourceIds.add(source.id);
  }

  const recordsById = new Map();
  const containmentRoots = new Map();
  const expandableRequests = new Set();
  const outcomesByRun = new Map();
  for (const [index, record] of records.entries()) {
    assert(
      validateRecord(record),
      `${name}: ${record.id ?? `line ${index + 1}`} schema failed\n${schemaErrors(validateRecord)}`,
    );
    assert(record.seq === index + 1, `${record.id}: seq is not contiguous`);
    assert(sourceIds.has(record.source), `${record.id}: unknown source ${record.source}`);
    assert(!recordsById.has(record.id), `${name}: duplicate record id ${record.id}`);

    if (record.parent !== undefined) {
      assert(recordsById.has(record.parent), `${record.id}: parent must reference an earlier record`);
    }
    const containmentRoot = record.kind === 'run.start'
      ? record.id
      : containmentRoots.get(record.parent);
    if (containmentRoot !== undefined) containmentRoots.set(record.id, containmentRoot);
    for (const link of record.links ?? []) {
      assert(
        recordsById.has(link.record),
        `${record.id}: ${link.type} must reference an earlier record`,
      );
    }
    if (record.kind === 'model.request') {
      const base = record.data.context_base_ref === undefined
        ? undefined
        : recordsById.get(record.data.context_base_ref);
      if (record.data.context_base_ref !== undefined) {
        assert(record.data.context_refs !== undefined, `${record.id}: context base requires context refs`);
        assert(base?.kind === 'model.request', `${record.id}: context base is not an earlier model request`);
        assert(expandableRequests.has(base.id), `${record.id}: context base is not expandable`);
        assert(
          containmentRoot !== undefined
            && containmentRoots.get(base.id) !== undefined
            && containmentRoots.get(base.id) === containmentRoot,
          `${record.id}: context base belongs to another run`,
        );
      }
      for (const contextId of record.data.context_refs ?? []) {
        const context = recordsById.get(contextId);
        assert(
          ['message', 'model.response', 'tool.result'].includes(context?.kind),
          `${record.id}: ${contextId} is not reusable earlier context`,
        );
      }
      if (record.data.context_refs !== undefined) expandableRequests.add(record.id);
    }

    recordsById.set(record.id, record);

    if (record.kind === 'model.response') {
      const request = oneLink(record, 'result_of', recordsById);
      assert(request?.kind === 'model.request', `${record.id}: result_of is not a model request`);
    }
    if (record.kind === 'run.outcome') {
      const startedRun = recordsById.get(record.parent);
      assert(startedRun?.kind === 'run.start', `${record.id}: parent is not a run.start`);
      assert(!outcomesByRun.has(record.parent), `${record.id}: run has more than one outcome`);
      outcomesByRun.set(record.parent, record.id);
    }
    if (record.kind === 'tool.call') {
      const proposal = oneLink(record, 'derived_from', recordsById);
      assert(proposal?.kind === 'tool.proposal', `${record.id}: derived_from is not a tool proposal`);
      assert(proposal.data.call_id === record.data.call_id, `${record.id}: call_id differs from proposal`);
    }
    if (record.kind === 'tool.result') {
      const call = oneLink(record, 'result_of', recordsById);
      assert(call?.kind === 'tool.call', `${record.id}: result_of is not a tool call`);
      assert(call.data.call_id === record.data.call_id, `${record.id}: call_id does not match call`);
    }
    if (record.kind === 'verification') {
      assert(
        (record.links ?? []).some((link) => link.type === 'verifies'),
        `${record.id}: expected at least one verifies link`,
      );
    }
  }

  const runStarts = records.filter((record) => record.kind === 'run.start');
  assert(runStarts.length > 0, `${name}: expected at least one run.start`);
  for (const startedRun of runStarts) {
    assert(outcomesByRun.has(startedRun.id), `${startedRun.id}: sealed example has no run.outcome`);
  }
  assert(manifest.trace.records === records.length, `${name}: record count mismatch`);
  assert(manifest.trace.last_seq === records.length, `${name}: last_seq mismatch`);
  assert(
    manifest.trace.losses === records.filter((record) => record.kind === 'loss').length,
    `${name}: loss count mismatch`,
  );
  assert(manifest.trace.bytes === rawTrace.byteLength, `${name}: byte count mismatch`);
  assert(
    manifest.trace.sha256 === createHash('sha256').update(rawTrace).digest('hex'),
    `${name}: trace digest mismatch`,
  );

  return records.length;
}

let totalRecords = 0;
for (const name of exampleNames) {
  const count = await validateExample(name);
  totalRecords += count;
  console.log(`validated ${name}: ${count} records`);
}
console.log(`validated ${exampleNames.length} bundles: ${totalRecords} records`);

await import('../v2/validate-contract.mjs');
