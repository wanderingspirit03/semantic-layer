import { createHash } from 'node:crypto';
import {
  chmod, mkdir, mkdtemp, readFile, readdir, rmdir, stat, unlink,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { afterEach, describe, expect, it } from 'vitest';

import {
  initialize,
  resetCaptureForTests,
  validateArtifact,
  type CaptureSource,
  type SourceSink,
} from '../src/index.js';
import { LocalArtifact } from '../src/v1/artifact.js';
import { CredentialScanner } from '../src/v1/secret-scanner.js';

afterEach(async () => resetCaptureForTests());

describe('compact semantic trace writer', () => {
  it('emits a validated manifest v2 for a managed installation', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-managed-writer-'));
    const installationId = 'install_abcdefghijklmnopqrstuv';
    const capture = initialize({
      output,
      serviceName: 'managed-writer-test',
      installationId,
    });

    await capture.observe('managed-run', {}, async () => 'done');
    const closed = await capture.shutdown();
    const manifest = JSON.parse(
      await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'),
    );

    expect(manifest).toMatchObject({
      schema: 'semantic_trace_manifest_v2',
      record_schema: 'semantic_trace_record_v1',
      installation_id: installationId,
      capture_policy: 'rich-credential-scrubbed',
    });
    expect(manifest.sources).toEqual(expect.arrayContaining([
      expect.objectContaining({
        name: 'manual',
        qualification: { status: 'exact_qualified' },
      }),
    ]));
    await expect(validateArtifact(closed.artifactPath)).resolves.toMatchObject({
      valid: true,
      issues: [],
    });
  });

  it('rejects installation identities that are not opaque managed IDs', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-managed-id-'));
    expect(() => initialize({
      output,
      serviceName: 'managed-id-test',
      installationId: 'customer-hostname',
    })).toThrow(/installationId/u);
  });

  it('persists a manual root/tool trace and a contract-valid hashed manifest', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-writer-'));
    const capture = initialize({ output, serviceName: 'writer-test' });

    await capture.observe('coding-agent', { input: { task: 'inspect' } }, async (scope) => (
      scope.tool('read_file', { path: 'a.ts' }, async () => ({ text: 'safe' }))
    ));
    const closed = await capture.shutdown();

    const tracePath = join(closed.artifactPath, 'trace.jsonl');
    const traceBytes = await readFile(tracePath);
    const rows = jsonLines(traceBytes);
    const manifest = JSON.parse(await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'));

    expect(rows.map((row) => row.kind)).toEqual([
      'run.start', 'tool.call', 'tool.result', 'run.outcome',
    ]);
    expect(rows.map((row) => row.seq)).toEqual([1, 2, 3, 4]);
    expect(rows[2].links).toEqual([{ type: 'result_of', record: rows[1].id }]);
    expect(rows[1].parent).toBe(rows[0].id);
    expect(rows[2].parent).toBe(rows[0].id);
    expect(rows[3].parent).toBe(rows[0].id);

    expect(manifest).toMatchObject({
      schema: 'semantic_trace_manifest_v1',
      record_schema: 'semantic_trace_record_v1',
      state: 'sealed',
      sdk: { language: 'typescript', version: '0.2.0-beta.0' },
      privacy_mode: 'local-rich',
      trace: {
        path: 'trace.jsonl',
        records: 4,
        last_seq: 4,
        bytes: traceBytes.byteLength,
        losses: 0,
        sha256: createHash('sha256').update(traceBytes).digest('hex'),
      },
      blobs: { path: 'blobs', count: 0, bytes: 0 },
    });
    expect(manifest.sources).toContainEqual({
      id: projectedSourceId('builtin/manual'),
      name: 'manual',
      seam: 'observe/tool/emit',
    });
    expect(new Set(rows.map((row) => row.source))).toEqual(new Set([
      projectedSourceId('builtin/manual'),
    ]));
    expect(await contractIssues(manifest, rows)).toEqual([]);
    expect((await stat(tracePath)).mode & 0o777).toBe(0o600);
    await expect(stat(join(closed.artifactPath, 'capture.jsonl'))).rejects.toMatchObject({
      code: 'ENOENT',
    });
  });

  it('accepts a redundant source row without writing it and aliases its children', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-redundant-'));
    const capture = initialize({ output, serviceName: 'redundant-test' });
    let sink: SourceSink | undefined;
    capture.installSource(source('fixture', (installed) => { sink = installed; }));

    const root = sink!.openTrace({
      name: 'fixture-run',
      semantic: { type: 'agent.run', name: 'fixture-run' },
    });
    expect(root.accepted).toBe(true);
    if (!root.accepted) throw new Error(root.reason);
    const duplicate = sink!.record({
      kind: 'lifecycle',
      phase: 'event',
      name: 'duplicate wrapper',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'capture.redundant' },
    });
    expect(duplicate.accepted).toBe(true);
    if (!duplicate.accepted) throw new Error(duplicate.reason);
    const child = sink!.record({
      kind: 'unknown',
      phase: 'event',
      name: 'user message',
      trace: root.identity,
      native: null,
      parentRecordId: duplicate.recordId,
      semantic: { type: 'message', role: 'user', content: 'hello' },
    });
    expect(child.accepted).toBe(true);
    if (!child.accepted) throw new Error(child.reason);
    sink!.record({
      kind: 'lifecycle',
      phase: 'end',
      name: 'fixture-run',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'agent.run', status: 'succeeded' },
    });

    const closed = await capture.shutdown();
    const rows = jsonLines(await readFile(join(closed.artifactPath, 'trace.jsonl')));
    expect(rows.map((row) => row.kind)).toEqual(['run.start', 'message', 'run.outcome']);
    expect(rows[1]).toMatchObject({ id: child.recordId, parent: root.recordId });
    expect(rows.some((row) => row.id === duplicate.recordId)).toBe(false);
    expect(closed.admitted).toBe(4);
    expect(closed.persisted).toBe(3);
  });

  it('reports persisted source gap counts by semantic reason', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-counted-gap-'));
    const capture = initialize({ output, serviceName: 'counted-gap-test' });
    let sink: SourceSink | undefined;
    capture.installSource(source('counted-gap', (installed) => { sink = installed; }));

    const root = sink!.openTrace({
      name: 'counted-gap-run',
      semantic: { type: 'workflow.run', name: 'counted-gap-run' },
    });
    if (!root.accepted) throw new Error(root.reason);
    sink!.record({
      kind: 'unknown',
      phase: 'gap',
      name: 'counted-gap',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: {
        type: 'capture.gap',
        reason: 'fixture_missing_evidence',
        count: 3,
      },
    });
    sink!.record({
      kind: 'lifecycle',
      phase: 'end',
      name: 'counted-gap-run',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'workflow.run', status: 'succeeded' },
    });

    const closed = await capture.shutdown();
    const rows = jsonLines(await readFile(join(closed.artifactPath, 'trace.jsonl')));
    const losses = rows.filter((row) => row.kind === 'loss');
    const manifest = JSON.parse(
      await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'),
    );

    expect(losses).toHaveLength(1);
    expect(losses[0].data).toMatchObject({
      reason: 'fixture_missing_evidence',
      count: 3,
    });
    expect(closed.losses).toEqual({ fixture_missing_evidence: 3 });
    expect(manifest.trace.losses).toBe(1);
  });

  it('reports projector-generated supplemental loss counts after persistence', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-supplemental-gap-'));
    const capture = initialize({ output, serviceName: 'supplemental-gap-test' });
    let sink: SourceSink | undefined;
    capture.installSource(source('supplemental-gap', (installed) => { sink = installed; }));

    const root = sink!.openTrace({
      name: 'supplemental-gap-run',
      semantic: { type: 'agent.run', name: 'supplemental-gap-run' },
    });
    if (!root.accepted) throw new Error(root.reason);
    sink!.record({
      kind: 'model',
      phase: 'start',
      name: 'model request',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: {
        type: 'model.request',
        context_refs: ['missing-context-a', 'missing-context-b'],
      },
    });
    sink!.record({
      kind: 'lifecycle',
      phase: 'end',
      name: 'supplemental-gap-run',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'agent.run', status: 'succeeded' },
    });

    const closed = await capture.shutdown();
    const losses = jsonLines(await readFile(join(closed.artifactPath, 'trace.jsonl')))
      .filter((row) => row.kind === 'loss');

    expect(losses).toHaveLength(1);
    expect(losses[0].data).toMatchObject({
      reason: 'unresolved_context_ref',
      count: 2,
    });
    expect(closed.losses).toEqual({ unresolved_context_ref: 2 });
  });

  it('counts coalesced runtime losses once from their persisted semantic count', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-coalesced-losses-'));
    const artifact = new LocalArtifact({
      output,
      serviceName: 'coalesced-losses-test',
      language: 'typescript',
      runtimeVersion: process.version,
      scanner: new CredentialScanner(),
      queueCapacityBytes: 64 * 1024 * 1024,
      ownershipManifest: () => ({} as never),
    });
    const lossCount = 40;

    const receipts = Array.from({ length: lossCount }, () => (
      artifact.recordLoss('source_rejection', 'trace_coalesced_losses')
    ));
    const closed = await artifact.seal();
    await Promise.all(receipts.map((receipt) => receipt.settled));
    const rows = jsonLines(await readFile(join(closed.artifactPath, 'trace.jsonl')));
    const losses = rows.filter((row) => row.kind === 'loss');
    const manifest = JSON.parse(
      await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'),
    );

    expect(losses.some((row) => row.data.count > 1)).toBe(true);
    expect(losses.reduce((total, row) => total + row.data.count, 0)).toBe(lossCount);
    expect(closed.losses).toEqual({ source_rejection: lossCount });
    expect(manifest.trace.losses).toBe(losses.length);
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  });

  it('writes both records from one terminal scope projection in one ordered batch', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-multi-'));
    const capture = initialize({ output, serviceName: 'multi-record-test' });
    let sink: SourceSink | undefined;
    capture.installSource(source('multi', (installed) => { sink = installed; }));

    const root = sink!.openTrace({
      name: 'workflow',
      semantic: { type: 'agent.run', name: 'workflow' },
    });
    if (!root.accepted) throw new Error(root.reason);
    const scopeStart = sink!.record({
      kind: 'lifecycle',
      phase: 'start',
      name: 'step',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'scope', scope_type: 'step', scope_id: 'scope_step', name: 'step' },
    });
    if (!scopeStart.accepted) throw new Error(scopeStart.reason);
    const terminal = sink!.record({
      kind: 'lifecycle',
      phase: 'error',
      name: 'step',
      trace: root.identity,
      native: null,
      parentRecordId: scopeStart.recordId,
      semantic: {
        type: 'scope',
        scope_type: 'step',
        status: 'failed',
        error: { type: 'tool_error', message: 'failed safely', recoverable: false },
      },
    });
    if (!terminal.accepted) throw new Error(terminal.reason);
    sink!.record({
      kind: 'lifecycle',
      phase: 'error',
      name: 'workflow',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'agent.run', status: 'failed' },
    });

    const closed = await capture.shutdown();
    const rows = jsonLines(await readFile(join(closed.artifactPath, 'trace.jsonl')));
    expect(rows.map((row) => row.kind)).toEqual([
      'run.start', 'scope', 'scope', 'error', 'run.outcome',
    ]);
    expect(rows.map((row) => row.seq)).toEqual([1, 2, 3, 4, 5]);
    expect(rows[2]).toMatchObject({
      id: terminal.recordId,
      kind: 'scope',
      data: { phase: 'end', status: 'failed' },
    });
    expect(rows[3]).toMatchObject({
      parent: terminal.recordId,
      kind: 'error',
      data: { type: 'tool_error', message: 'failed safely', recoverable: false },
    });
    expect(closed.admitted).toBe(4);
    expect(closed.persisted).toBe(5);
  });

  it('persists only clean referenced blobs and accounts for them in the manifest', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-blob-'));
    const capture = initialize({ output, serviceName: 'blob-test' });
    const payload = Uint8Array.from([1, 2, 3, 4]);
    await capture.observe('binary-run', { input: payload }, async () => undefined);
    const closed = await capture.shutdown();

    const rows = jsonLines(await readFile(join(closed.artifactPath, 'trace.jsonl')));
    const reference = rows[0].blob_refs[0];
    expect(reference).toMatchObject({
      sha256: createHash('sha256').update(payload).digest('hex'),
      bytes: payload.byteLength,
      media_type: 'application/octet-stream',
      scan: 'clean',
    });
    expect(await readFile(join(closed.artifactPath, reference.path))).toEqual(Buffer.from(payload));
    const manifest = JSON.parse(await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'));
    expect(manifest.blobs).toEqual({ path: 'blobs', count: 1, bytes: payload.byteLength });
  });

  it('charges staged blob bytes to the bounded admission queue', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-blob-queue-'));
    const capture = initialize({ output, serviceName: 'blob-queue-test' });
    const payload = new Uint8Array(1024 * 1024);

    const observation = capture.observe('binary-run', { input: payload }, async () => undefined);
    expect(capture.status().queue.pendingBytes).toBeGreaterThanOrEqual(payload.byteLength);
    await observation;
    expect((await capture.shutdown()).queue.highWaterBytes)
      .toBeGreaterThanOrEqual(payload.byteLength);
  });

  it('includes staged blob bytes in queue-rejection loss evidence', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-blob-rejection-'));
    const artifact = new LocalArtifact({
      output,
      serviceName: 'blob-rejection-test',
      language: 'typescript',
      runtimeVersion: process.version,
      scanner: new CredentialScanner(),
      queueCapacityBytes: 64 * 1024,
      ownershipManifest: () => ({} as never),
    });
    const bytes = new Uint8Array(70 * 1024);
    const digest = createHash('sha256').update(bytes).digest('hex');
    const receipt = artifact.admit({
      trace_id: 'trace_blob_rejection',
      source: {
        source_id: 'builtin/semantic-layer-runtime',
        name: 'semantic-layer-runtime',
        seam: 'capture-runtime',
        identity_domain: 'semantic-layer',
        official: true,
      },
      event_kind: 'unknown',
      phase: 'event',
      name: 'oversized-staged-blob',
      native: null,
      semantic: {},
      correlation: {},
      loss_refs: [],
      blob_refs: [],
    }, false, [{
      bytes,
      digest,
      path: '/native/payload',
      mimeType: 'application/octet-stream',
    }]);

    expect(receipt).toMatchObject({ accepted: false, reason: 'queue_backpressure' });
    await receipt.settled;
    const closed = await artifact.seal();
    const rows = jsonLines(await readFile(join(closed.artifactPath, 'trace.jsonl')));
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      kind: 'loss',
      data: {
        reason: 'queue_backpressure_drop',
        bytes: expect.any(Number),
      },
    });
    expect(Number(rows[0].data.bytes)).toBeGreaterThan(bytes.byteLength);
  });

  it('recomputes the trace digest when a failed seal is retried after closing rows', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-seal-retry-'));
    const artifact = new LocalArtifact({
      output,
      serviceName: 'seal-retry-test',
      language: 'typescript',
      runtimeVersion: process.version,
      scanner: new CredentialScanner(),
      queueCapacityBytes: 64 * 1024 * 1024,
      ownershipManifest: () => ({} as never),
    });
    await artifact.recordLoss('source_rejection', 'trace_sealretry').settled;

    const manifestPath = join(artifact.artifactPath, 'manifest.json');
    artifact.beginClosing();
    await unlink(manifestPath);
    await mkdir(manifestPath);
    expect(await artifact.seal()).toMatchObject({ state: 'closing' });

    await artifact.recordLoss('source_rejection', 'trace_sealretry').settled;
    await rmdir(manifestPath);
    const closed = await artifact.seal();

    expect(closed.state).toBe('closed');
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  });

  it('keeps a long multi-step run ordered, correlated, and bounded in one compact file', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-long-run-'));
    const capture = initialize({ output, serviceName: 'long-run-test' });
    const applicationResults: string[] = [];
    const flushStatuses: ReturnType<typeof capture.status>[] = [];
    const stepCount = 10;

    await capture.observe('repair-workspace', { input: { task: 'fix failing checks' } }, async (root) => {
      for (let step = 0; step < stepCount; step += 1) {
        const results = await root.turn(
          `inspect-step-${step}`,
          { input: { step } },
          async (turn) => Promise.all([
            turn.tool('inspect_workspace', { step, lane: 'source' }, async (input) => (
              `${input.step}:${input.lane}`
            )),
            turn.tool('inspect_workspace', { step, lane: 'tests' }, async (input) => (
              `${input.step}:${input.lane}`
            )),
          ]),
        );
        applicationResults.push(...results);
        if ((step + 1) % 3 === 0) flushStatuses.push(await capture.flush());
      }
    });

    const closed = await capture.shutdown();
    const tracePath = join(closed.artifactPath, 'trace.jsonl');
    const traceBytes = await readFile(tracePath);
    const rows = jsonLines(traceBytes);
    const manifest = JSON.parse(await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'));

    expect(applicationResults).toEqual(Array.from(
      { length: stepCount },
      (_, step) => [`${step}:source`, `${step}:tests`],
    ).flat());
    expect(flushStatuses).toHaveLength(3);
    for (const status of [...flushStatuses, closed]) {
      expect(status.queue.pendingBytes).toBe(0);
      expect(status.queue.pendingControlBytes).toBe(0);
      expect(status.queue.highWaterBytes).toBeGreaterThan(0);
      expect(status.queue.highWaterBytes).toBeLessThanOrEqual(status.queue.capacityBytes);
    }

    expect(rows.map((row) => row.seq)).toEqual(
      Array.from({ length: rows.length }, (_, index) => index + 1),
    );
    expect(rows.filter((row) => row.kind === 'scope')).toHaveLength(stepCount * 2);
    const calls = rows.filter((row) => row.kind === 'tool.call');
    const results = rows.filter((row) => row.kind === 'tool.result');
    expect(calls).toHaveLength(stepCount * 2);
    expect(results).toHaveLength(stepCount * 2);
    expect(new Set(calls.map((row) => row.data.name))).toEqual(new Set(['inspect_workspace']));
    const callsByRecord = new Map(calls.map((row) => [row.id, row]));
    for (const result of results) {
      const resultLink = result.links.find((link: Record<string, string>) => (
        link.type === 'result_of'
      ));
      const call = callsByRecord.get(resultLink?.record);
      expect(call).toBeDefined();
      if (!call) throw new Error('tool result did not reference a captured call');
      expect(result.data.call_id).toBe(call.data.call_id);
      expect(result.parent).toBe(call.parent);
      expect(result.data.output).toBe(`${call.data.input.step}:${call.data.input.lane}`);
    }

    expect(closed.rejected).toBe(0);
    expect(closed.losses).toEqual({});
    expect(rows.some((row) => row.kind === 'loss')).toBe(false);
    expect(manifest).toMatchObject({
      state: 'sealed',
      trace: {
        path: 'trace.jsonl',
        records: rows.length,
        last_seq: rows.length,
        bytes: traceBytes.byteLength,
        losses: 0,
        sha256: createHash('sha256').update(traceBytes).digest('hex'),
      },
      blobs: { path: 'blobs', count: 0, bytes: 0 },
    });
    expect(await readdir(closed.artifactPath)).toEqual(['manifest.json', 'trace.jsonl']);
    expect(await contractIssues(manifest, rows)).toEqual([]);
  });

  it('preserves the durable prefix and records one loss after an append failure', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-recovery-'));
    const capture = initialize({ output, serviceName: 'recovery-test' });
    const durableBlob = Uint8Array.from([7, 8, 9]);
    await capture.observe('first', { input: durableBlob }, async () => undefined);
    await capture.flush();

    const tracePath = join(capture.status().artifactPath, 'trace.jsonl');
    const durableRows = jsonLines(await readFile(tracePath));
    const durableBlobPath = durableRows[0].blob_refs[0].path;
    await chmod(tracePath, 0o400);
    await capture.observe('will-fail-to-append', {}, async () => undefined);
    const closed = await capture.shutdown();

    const rows = jsonLines(await readFile(tracePath));
    expect(rows.slice(0, durableRows.length)).toEqual(durableRows);
    expect(rows).toHaveLength(durableRows.length + 1);
    expect(rows.at(-1)).toMatchObject({
      seq: durableRows.length + 1,
      kind: 'loss',
      data: {
        reason: 'persistence_failure',
        stage: 'persist',
        recoverable: false,
        path: '/trace.jsonl',
      },
    });
    const manifest = JSON.parse(await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'));
    expect(manifest.trace).toMatchObject({
      records: rows.length,
      last_seq: rows.length,
      losses: 1,
    });
    expect(manifest.blobs).toEqual({ path: 'blobs', count: 1, bytes: durableBlob.byteLength });
    expect(await readFile(join(closed.artifactPath, durableBlobPath))).toEqual(Buffer.from(durableBlob));
    expect(await contractIssues(manifest, rows)).toEqual([]);
  });

  it('does not report a semantic loss whose persistence failed', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-trace-loss-recovery-'));
    const artifact = new LocalArtifact({
      output,
      serviceName: 'loss-recovery-test',
      language: 'typescript',
      runtimeVersion: process.version,
      scanner: new CredentialScanner(),
      queueCapacityBytes: 64 * 1024 * 1024,
      ownershipManifest: () => ({} as never),
    });
    await artifact.recordLoss('source_rejection', 'trace_loss_recovery').settled;
    const tracePath = join(artifact.artifactPath, 'trace.jsonl');
    await chmod(tracePath, 0o400);
    await artifact.recordLoss('shutdown_timeout', 'trace_loss_recovery').settled;

    const closed = await artifact.seal();
    const rows = jsonLines(await readFile(tracePath));
    const recovery = rows.at(-1);

    expect(rows.map((row) => row.data.reason)).toEqual([
      'source_rejection',
      'persistence_failure',
    ]);
    expect(recovery).toMatchObject({
      kind: 'loss',
      data: { reason: 'persistence_failure', count: 1 },
    });
    expect(closed.losses).toEqual({
      source_rejection: 1,
      persistence_failure: 1,
    });
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  });
});

function source(name: string, installed: (sink: SourceSink) => void): CaptureSource {
  return {
    metadata: {
      name,
      seam: `${name}.fixture`,
      identityDomain: `${name}.operation`,
      coverage: [],
    },
    install(sink) {
      installed(sink);
      return { deactivate() {}, drain() {} };
    },
  };
}

function jsonLines(bytes: Buffer): Array<Record<string, any>> {
  return bytes.toString('utf8').trim().split('\n').filter(Boolean).map((line) => JSON.parse(line));
}

function projectedSourceId(value: string): string {
  return `src_${createHash('sha256').update(value).digest('hex').slice(0, 24)}`;
}

async function contractIssues(
  manifest: Record<string, unknown>,
  rows: Array<Record<string, unknown>>,
): Promise<string[]> {
  const contracts = fileURLToPath(new URL('../../../contracts/trace/v1/', import.meta.url));
  const [manifestSchema, recordSchema] = await Promise.all([
    readFile(join(contracts, 'semantic-trace-manifest.schema.json'), 'utf8').then(JSON.parse),
    readFile(join(contracts, 'semantic-trace-record.schema.json'), 'utf8').then(JSON.parse),
  ]);
  const ajv = new Ajv2020({ allErrors: true, strict: false });
  addFormats(ajv);
  const validateManifest = ajv.compile(manifestSchema);
  const validateRecord = ajv.compile(recordSchema);
  const issues: string[] = [];
  if (!validateManifest(manifest)) issues.push(...(validateManifest.errors ?? []).map(String));
  for (const row of rows) {
    if (!validateRecord(row)) issues.push(...(validateRecord.errors ?? []).map(String));
  }
  return issues;
}
