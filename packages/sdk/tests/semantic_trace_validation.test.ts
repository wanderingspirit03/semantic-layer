import { createHash } from 'node:crypto';
import {
  chmod, mkdir, mkdtemp, readFile, symlink, unlink, writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { basename, dirname, join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

import {
  initialize,
  resetCaptureForTests,
  validateArtifact,
  type CaptureSource,
  type SourceSink,
} from '../src/index.js';

afterEach(async () => resetCaptureForTests());

describe('semantic trace v1 validation', () => {
  it('accepts a sealed rich-agent bundle when all evidence shares one root', async () => {
    const fixture = await richAgentBundle();
    const statelessRows: Array<Record<string, any>> = fixture.rows
      .filter((row) => !['state', 'error', 'verification'].includes(row.kind))
      .map((row, index) => ({ ...row, seq: index + 1 }));

    expect(statelessRows.filter((row) => row.kind === 'run.start')).toHaveLength(1);
    expect(await validateArtifact(fixture.artifactPath, { profile: 'structural' }))
      .toMatchObject({ valid: true, issues: [], secretMatches: 0 });
    expect(await validateArtifact(fixture.artifactPath, { profile: 'rich-agent' }))
      .toMatchObject({ valid: true, issues: [], secretMatches: 0 });
    expect(await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, statelessRows),
      rows: statelessRows,
      profile: 'rich-agent',
    })).toMatchObject({ valid: true, issues: [], secretMatches: 0 });
  });

  it('requires a completed model response for model evidence', async () => {
    const fixture = await richAgentBundle();

    for (const status of ['failed', 'incomplete', 'cancelled']) {
      const rows = structuredClone(fixture.rows);
      const response = rows.find((row) => row.kind === 'model.response')!;
      response.data.status = status;

      const report = await validateArtifact('', {
        manifest: accountForRows(fixture.manifest, rows),
        rows,
        requiredEvidence: ['model'],
      });

      expect(report.issues).toContain('REQUIRED_EVIDENCE_MISSING:model');

      const rich = await validateArtifact('', {
        manifest: accountForRows(fixture.manifest, rows),
        rows,
        profile: 'rich-agent',
      });
      expect(rich.issues).toContain('PROFILE_PAIR_MISSING:model');
    }
  });

  it('requires a succeeded tool result for tool evidence', async () => {
    const fixture = await richAgentBundle();

    for (const status of ['failed', 'cancelled']) {
      const rows = structuredClone(fixture.rows);
      const result = rows.find((row) => row.kind === 'tool.result')!;
      result.data.status = status;

      const report = await validateArtifact('', {
        manifest: accountForRows(fixture.manifest, rows),
        rows,
        requiredEvidence: ['tool'],
      });

      expect(report.issues).toContain('REQUIRED_EVIDENCE_MISSING:tool');

      const rich = await validateArtifact('', {
        manifest: accountForRows(fixture.manifest, rows),
        rows,
        profile: 'rich-agent',
      });
      expect(rich.issues).toContain('PROFILE_PAIR_MISSING:tool');
    }
  });

  it('rejects rich-agent evidence split across independent roots', async () => {
    const fixture = await richAgentBundle();
    const rows = structuredClone(fixture.rows);
    const root = rows.find((row) => row.kind === 'run.start')!;
    const outcome = rows.find((row) => row.kind === 'run.outcome')!;
    const toolCallIndex = rows.findIndex((row) => row.kind === 'tool.call');
    const secondRoot = {
      ...structuredClone(root),
      id: 'root_second',
      data: { ...root.data, name: 'second-agent' },
    };
    rows.splice(toolCallIndex, 0, secondRoot);
    for (const row of rows) {
      if (row.kind === 'tool.call' || row.kind === 'tool.result') {
        row.parent = secondRoot.id;
      }
    }
    rows.push({
      ...structuredClone(outcome),
      id: 'outcome_second',
      parent: secondRoot.id,
    });
    rows.forEach((row, index) => {
      row.seq = index + 1;
    });

    const report = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, rows),
      rows,
      profile: 'rich-agent',
    });

    expect(report.valid).toBe(false);
    expect(report.issues).toContain('PROFILE_PAIR_ROOT_MISMATCH');
  });

  it('rejects duplicate identities, forward relationships, undeclared sources, and duplicate outcomes', async () => {
    const fixture = await richAgentBundle();
    const rows = structuredClone(fixture.rows);
    const root = rows.find((row) => row.kind === 'run.start')!;
    const state = rows.find((row) => row.kind === 'state')!;
    const outcome = rows.find((row) => row.kind === 'run.outcome')!;
    const modelRequest = rows.find((row) => row.kind === 'model.request')!;
    const modelResponse = rows.find((row) => row.kind === 'model.response')!;

    modelResponse.id = modelRequest.id;
    state.parent = outcome.id;
    state.links = [{ type: 'affects', record: outcome.id }];
    state.source = 'src_missing';
    rows.push({
      ...structuredClone(outcome),
      id: 'outcome_duplicate',
      seq: rows.length + 1,
    });

    const report = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, rows),
      rows,
    });
    expect(report.valid).toBe(false);
    expect(report.issues).toEqual(expect.arrayContaining([
      'LINK_REFERENCE_INVALID',
      'PARENT_REFERENCE_INVALID',
      'RECORD_ID_DUPLICATE',
      'ROOT_OUTCOME_DUPLICATE',
      'SOURCE_UNDECLARED',
    ]));
  });

  it('accepts a linear context-base append chain without materializing prefixes', async () => {
    const fixture = await richAgentBundle();
    const rows = structuredClone(fixture.rows);
    const root = rows.find((row) => row.kind === 'run.start')!;
    const response = rows.find((row) => row.kind === 'model.response')!;
    const outcomeIndex = rows.findIndex((row) => row.kind === 'run.outcome');
    let base = rows.find((row) => row.kind === 'model.request')!.id;
    const chain = Array.from({ length: 128 }, (_, index) => {
      const id = `request_context_${index}`;
      const row = {
        id,
        seq: 0,
        time: root.time,
        kind: 'model.request',
        origin: 'observed',
        source: root.source,
        parent: root.id,
        data: {
          context_base_ref: base,
          context_refs: [response.id],
        },
      };
      base = id;
      return row;
    });
    rows.splice(outcomeIndex, 0, ...chain);
    rows.forEach((row, index) => {
      row.seq = index + 1;
    });

    const report = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, rows),
      rows,
    });

    expect(report).toMatchObject({ valid: true, issues: [] });
  });

  it('rejects invalid context refs and non-request, forward, cyclic, or cross-root bases', async () => {
    const fixture = await richAgentBundle();
    const rows = structuredClone(fixture.rows);
    const root = rows.find((row) => row.kind === 'run.start')!;
    const request = rows.find((row) => row.kind === 'model.request')!;
    const response = rows.find((row) => row.kind === 'model.response')!;
    request.data.context_refs = [request.id];
    request.data.context_base_ref = 'request_forward';
    const outcomeIndex = rows.findIndex((row) => row.kind === 'run.outcome');
    const secondRoot = {
      ...structuredClone(root),
      id: 'root_context_other',
      seq: 0,
      data: { ...root.data, name: 'other context root' },
    };
    const forward = {
      ...structuredClone(request),
      id: 'request_forward',
      seq: 0,
      data: {
        context_base_ref: request.id,
        context_refs: [response.id],
      },
    };
    const nonRequest = {
      ...structuredClone(request),
      id: 'request_non_request_base',
      seq: 0,
      data: { context_base_ref: response.id, context_refs: [] },
    };
    const crossRoot = {
      ...structuredClone(request),
      id: 'request_cross_root',
      seq: 0,
      parent: secondRoot.id,
      data: { context_base_ref: request.id, context_refs: [] },
    };
    rows.splice(outcomeIndex, 0, forward, nonRequest, secondRoot, crossRoot);
    rows.forEach((row, index) => {
      row.seq = index + 1;
    });

    const report = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, rows),
      rows,
    });

    expect(report.valid).toBe(false);
    expect(report.issues).toEqual(expect.arrayContaining([
      'CONTEXT_REFERENCE_INVALID',
      'CONTEXT_BASE_REFERENCE_INVALID',
    ]));
  });

  it('rejects a context base when both model requests are rootless', async () => {
    const fixture = await richAgentBundle();
    const rows = structuredClone(fixture.rows);
    const request = rows.find((row) => row.kind === 'model.request')!;
    const rootlessBase: Record<string, any> = {
      ...structuredClone(request),
      id: 'request_rootless_base',
      seq: 0,
      data: { context_refs: [] },
    };
    delete rootlessBase.parent;
    const rootlessAppend: Record<string, any> = {
      ...structuredClone(request),
      id: 'request_rootless_append',
      seq: 0,
      data: {
        context_base_ref: rootlessBase.id,
        context_refs: [],
      },
    };
    delete rootlessAppend.parent;
    const outcomeIndex = rows.findIndex((row) => row.kind === 'run.outcome');
    rows.splice(outcomeIndex, 0, rootlessBase, rootlessAppend);
    rows.forEach((row, index) => {
      row.seq = index + 1;
    });

    const report = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, rows),
      rows,
    });

    expect(report.valid).toBe(false);
    expect(report.issues).toContain('CONTEXT_BASE_REFERENCE_INVALID');
  });

  it('checks trace accounting and the semantic rich-agent evidence floor', async () => {
    const fixture = await richAgentBundle();
    const manifest = structuredClone(fixture.manifest);
    manifest.trace.records += 1;
    manifest.trace.last_seq += 1;
    manifest.trace.bytes += 1;
    manifest.trace.losses += 1;
    manifest.trace.sha256 = '0'.repeat(64);
    const sparseRows = fixture.rows.filter((row) => ![
      'model.request',
      'model.response',
      'tool.call',
      'tool.result',
      'state',
    ].includes(row.kind));

    const accounting = await validateArtifact('', { manifest, rows: fixture.rows });
    expect(accounting.issues).toEqual(expect.arrayContaining([
      'LOSS_COUNT_MISMATCH',
      'MANIFEST_BYTE_COUNT_MISMATCH',
      'MANIFEST_COUNT_MISMATCH',
      'TRACE_DIGEST_MISMATCH',
    ]));

    const rich = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, sparseRows),
      rows: sparseRows,
      profile: 'rich-agent',
    });
    expect(rich.issues).toEqual(expect.arrayContaining([
      'PROFILE_PAIR_MISSING:model',
      'PROFILE_PAIR_MISSING:tool',
    ]));
  });

  it('accounts manifest losses by persisted rows, not semantic data.count', async () => {
    const fixture = await richAgentBundle();
    const rows = structuredClone(fixture.rows);
    const root = rows.find((row) => row.kind === 'run.start')!;
    const outcomeIndex = rows.findIndex((row) => row.kind === 'run.outcome');
    rows.splice(outcomeIndex, 0, {
      id: 'loss_counted_gap',
      seq: 0,
      time: root.time,
      kind: 'loss',
      origin: 'observed',
      source: root.source,
      parent: root.id,
      data: {
        reason: 'fixture_missing_evidence',
        stage: 'source',
        count: 3,
        recoverable: false,
      },
    });
    rows.forEach((row, index) => {
      row.seq = index + 1;
    });
    const manifest = accountForRows(fixture.manifest, rows);

    expect(await validateArtifact('', { manifest, rows }))
      .toMatchObject({ valid: true, issues: [] });

    manifest.trace.losses = 3;
    expect((await validateArtifact('', { manifest, rows })).issues)
      .toContain('LOSS_COUNT_MISMATCH');
  });

  it('checks only explicitly required source activity and semantic evidence', async () => {
    const fixture = await richAgentBundle();
    const withoutDelivery = await validateArtifact('', {
      manifest: fixture.manifest,
      rows: fixture.rows,
      requiredEvidence: ['root', 'model', 'tool', 'delivery'],
      requiredSourceActivity: ['validator-fixture', 'missing-source'],
    });

    expect(withoutDelivery.valid).toBe(false);
    expect(withoutDelivery.issues).toEqual(expect.arrayContaining([
      'REQUIRED_EVIDENCE_MISSING:delivery',
      'REQUIRED_SOURCE_ACTIVITY_MISSING:missing-source',
    ]));
    expect(withoutDelivery.sourceActivity).toEqual(expect.arrayContaining([
      expect.objectContaining({ name: 'validator-fixture', records: expect.any(Number) }),
    ]));

    const wrongDeliveryRows = structuredClone(fixture.rows);
    const wrongDeliveryOutcome = wrongDeliveryRows.find((row) => row.kind === 'run.outcome')!;
    const toolCall = wrongDeliveryRows.find((row) => row.kind === 'tool.call')!;
    wrongDeliveryOutcome.links = [{ type: 'derived_from', record: toolCall.id }];
    const wrongDelivery = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, wrongDeliveryRows),
      rows: wrongDeliveryRows,
      requiredEvidence: ['delivery'],
    });
    expect(wrongDelivery.issues).toContain('REQUIRED_EVIDENCE_MISSING:delivery');

    const failedOutcomeRows = structuredClone(fixture.rows);
    const failedOutcome = failedOutcomeRows.find((row) => row.kind === 'run.outcome')!;
    failedOutcome.data = { status: 'failed', output: 'partial' };
    const failedOutcomeReport = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, failedOutcomeRows),
      rows: failedOutcomeRows,
      requiredEvidence: ['delivery'],
    });
    expect(failedOutcomeReport.issues).toContain('REQUIRED_EVIDENCE_MISSING:delivery');

    const failedResponseRows = structuredClone(fixture.rows);
    const failedResponse = failedResponseRows.find((row) => row.kind === 'model.response')!;
    failedResponse.data.status = 'failed';
    const failedResponseOutcome = failedResponseRows.find((row) => row.kind === 'run.outcome')!;
    failedResponseOutcome.links = [{ type: 'derived_from', record: failedResponse.id }];
    const failedResponseReport = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, failedResponseRows),
      rows: failedResponseRows,
      requiredEvidence: ['delivery'],
    });
    expect(failedResponseReport.issues).toContain('REQUIRED_EVIDENCE_MISSING:delivery');

    const rows = structuredClone(fixture.rows);
    const outcome = rows.find((row) => row.kind === 'run.outcome')!;
    const modelResponse = rows.find((row) => row.kind === 'model.response')!;
    outcome.links = [{ type: 'derived_from', record: modelResponse.id }];
    const complete = await validateArtifact('', {
      manifest: accountForRows(fixture.manifest, rows),
      rows,
      requiredEvidence: ['root', 'model', 'tool', 'delivery'],
      requiredSourceActivity: ['validator-fixture'],
    });

    expect(complete).toMatchObject({ valid: true, issues: [] });
  });

  it('keeps structurally valid idle bundles valid unless activity is required', async () => {
    const fixture = await richAgentBundle();
    const idleManifest = accountForRows(fixture.manifest, []);

    const structural = await validateArtifact('', {
      manifest: idleManifest,
      rows: [],
    });
    expect(structural).toMatchObject({
      valid: true,
      issues: [],
      sourceActivity: expect.arrayContaining([
        expect.objectContaining({ name: 'validator-fixture', records: 0 }),
      ]),
    });

    const required = await validateArtifact('', {
      manifest: idleManifest,
      rows: [],
      requiredEvidence: ['root'],
      requiredSourceActivity: ['validator-fixture'],
    });
    expect(required.valid).toBe(false);
    expect(required.issues).toEqual(expect.arrayContaining([
      'REQUIRED_EVIDENCE_MISSING:root',
      'REQUIRED_SOURCE_ACTIVITY_MISSING:validator-fixture',
    ]));
  });

  it('rejects unknown required semantic evidence', async () => {
    const fixture = await richAgentBundle();

    await expect(validateArtifact('', {
      manifest: fixture.manifest,
      rows: fixture.rows,
      requiredEvidence: ['unsupported' as any],
    })).rejects.toMatchObject({
      code: 'REQUIRED_EVIDENCE_INVALID',
    });
  });

  it('rejects changed blobs, unsafe permissions, undeclared files, and credentials', async () => {
    const fixture = await richAgentBundle();
    const blob = fixture.rows.flatMap((row) => row.blob_refs ?? [])[0];
    expect(blob).toBeDefined();

    await writeFile(join(fixture.artifactPath, blob.path), Buffer.from([9]));
    await chmod(join(fixture.artifactPath, 'manifest.json'), 0o644);
    await writeFile(
      join(fixture.artifactPath, 'notes.txt'),
      `sk-or-v1-${'a'.repeat(64)}`,
      { mode: 0o600 },
    );

    const report = await validateArtifact(fixture.artifactPath);
    expect(report.secretMatches).toBe(1);
    expect(report.issues).toEqual(expect.arrayContaining([
      'BLOB_DIGEST_MISMATCH',
      'BLOB_LENGTH_MISMATCH',
      'FILE_PERMISSION_INVALID',
      'MANIFEST_BLOB_BYTE_COUNT_MISMATCH',
      'SECRET_MATCH',
      'UNDECLARED_ARTIFACT_FILE',
    ]));
  });

  it('never treats schema-invalid blob paths as filesystem locations', async () => {
    for (const invalidPath of ['blobs/unsafe\\..\\outside', 'blobs/unsafe\u0000outside']) {
      const fixture = await richAgentBundle();
      const rows = structuredClone(fixture.rows);
      const reference = rows.flatMap((row) => row.blob_refs ?? [])[0];
      expect(reference).toBeDefined();
      if (!reference) throw new Error('fixture did not contain a blob reference');
      reference.path = invalidPath;
      const manifest = accountForRows(fixture.manifest, rows);
      await writeFile(
        join(fixture.artifactPath, 'trace.jsonl'),
        rows.map((row) => `${JSON.stringify(row)}\n`).join(''),
      );
      await writeFile(
        join(fixture.artifactPath, 'manifest.json'),
        `${JSON.stringify(manifest, null, 2)}\n`,
      );

      const report = await validateArtifact(fixture.artifactPath);
      expect(report.issues).toContain('RECORD_SCHEMA_INVALID');
      expect(report.issues).not.toContain('BLOB_MISSING');
      await resetCaptureForTests();
    }
  });

  it.skipIf(process.platform === 'win32')(
    'rejects a linked manifest before reading outside the bundle',
    async () => {
      const fixture = await richAgentBundle();
      const external = join(
        await mkdtemp(join(tmpdir(), 'semantic-validation-external-')),
        'manifest.json',
      );
      await writeFile(external, JSON.stringify(fixture.manifest), { mode: 0o600 });
      const manifestPath = join(fixture.artifactPath, 'manifest.json');
      await unlink(manifestPath);
      await symlink(external, manifestPath);

      expect(await validateArtifact(fixture.artifactPath)).toMatchObject({
        valid: false,
        issues: ['ARTIFACT_UNREADABLE'],
      });
    },
  );

  it.skipIf(process.platform === 'win32')(
    'rejects direct and intermediate linked artifact roots before reading',
    async () => {
      const fixture = await richAgentBundle();
      const sandbox = await mkdtemp(join(tmpdir(), 'semantic-validation-root-link-'));
      const direct = join(sandbox, 'direct');
      await symlink(fixture.artifactPath, direct, 'dir');

      const intermediate = join(sandbox, 'intermediate');
      await symlink(dirname(fixture.artifactPath), intermediate, 'dir');

      for (const path of [direct, join(intermediate, basename(fixture.artifactPath))]) {
        expect(await validateArtifact(path)).toMatchObject({
          valid: false,
          issues: ['ARTIFACT_UNREADABLE'],
        });
      }
    },
  );
});

async function richAgentBundle(): Promise<{
  artifactPath: string;
  manifest: Record<string, any>;
  rows: Array<Record<string, any>>;
}> {
  const output = await mkdtemp(join(tmpdir(), 'semantic-trace-validation-'));
  const capture = initialize({ output, serviceName: 'validator-fixture' });
  let sink: SourceSink | undefined;
  const source: CaptureSource = {
    metadata: {
      name: 'validator-fixture',
      seam: 'fixture.callback',
      identityDomain: 'fixture.operation',
      coverage: [],
    },
    install(installed) {
      sink = installed;
      return { deactivate() {}, drain() {} };
    },
  };
  capture.installSource(source);

  const root = sink!.openTrace({
    name: 'coding-agent',
    nativeIdentity: 'run-1',
    semantic: {
      type: 'agent.run',
      name: 'coding-agent',
      input: Uint8Array.from([1, 2, 3, 4]),
    },
  });
  if (!root.accepted) throw new Error(root.reason);
  const model = sink!.record({
    kind: 'model',
    phase: 'start',
    name: 'model',
    trace: root.identity,
    parentRecordId: root.recordId,
    nativeIdentity: 'model-1',
    native: null,
    semantic: { type: 'model.request', model: 'fixture-model', context_refs: [] },
  });
  if (!model.accepted) throw new Error(model.reason);
  sink!.record({
    kind: 'model',
    phase: 'end',
    name: 'model',
    trace: root.identity,
    parentRecordId: model.recordId,
    nativeIdentity: 'model-1',
    native: null,
    semantic: {
      type: 'model.response',
      status: 'completed',
      model: 'fixture-model',
      content: 'Use the tool.',
    },
  });
  const tool = sink!.record({
    kind: 'tool',
    phase: 'start',
    name: 'read_file',
    trace: root.identity,
    parentRecordId: root.recordId,
    nativeIdentity: 'call-1',
    native: null,
    semantic: { type: 'tool.execution', name: 'read_file', input: { path: 'a.ts' } },
  });
  if (!tool.accepted) throw new Error(tool.reason);
  sink!.record({
    kind: 'tool',
    phase: 'end',
    name: 'read_file',
    trace: root.identity,
    parentRecordId: tool.recordId,
    nativeIdentity: 'call-1',
    native: null,
    semantic: { type: 'tool.result', status: 'succeeded', output: { text: 'safe' } },
  });
  sink!.record({
    kind: 'state',
    phase: 'event',
    name: 'checkpoint',
    trace: root.identity,
    parentRecordId: root.recordId,
    native: null,
    semantic: {
      type: 'state.transition',
      state_type: 'state.checkpoint',
      version: 1,
      value: { verified: true },
    },
  });
  sink!.record({
    kind: 'lifecycle',
    phase: 'end',
    name: 'coding-agent',
    trace: root.identity,
    parentRecordId: root.recordId,
    native: null,
    semantic: { type: 'agent.run', status: 'succeeded' },
  });

  const closed = await capture.shutdown();
  const manifest = JSON.parse(
    await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'),
  ) as Record<string, any>;
  const rows = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, any>);
  return { artifactPath: closed.artifactPath, manifest, rows };
}

function accountForRows(
  input: Record<string, any>,
  rows: Array<Record<string, any>>,
): Record<string, any> {
  const manifest = structuredClone(input);
  const trace = Buffer.from(rows.map((row) => `${JSON.stringify(row)}\n`).join(''));
  manifest.trace = {
    ...manifest.trace,
    records: rows.length,
    last_seq: rows.length,
    bytes: trace.byteLength,
    losses: rows.filter((row) => row.kind === 'loss').length,
    sha256: createHash('sha256').update(trace).digest('hex'),
  };
  return manifest;
}
