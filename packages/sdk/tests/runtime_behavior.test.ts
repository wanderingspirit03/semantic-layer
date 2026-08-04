import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  stat,
  symlink,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import { snapshotRecord } from '../src/adapters/native-snapshot.js';
import { CredentialScanner } from '../src/v1/secret-scanner.js';
import {
  createCapture,
  initialize,
  resetCaptureForTests,
  validateArtifact,
  type CaptureSource,
  type SourceSink,
} from '../src/index.js';
import type { SemanticTraceRecord } from '../src/trace/semantic-projector.js';

afterEach(async () => resetCaptureForTests());

describe('runtime production guarantees', () => {
  it('creates independent concurrent bundles under the same output root', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-independent-'));
    const first = createCapture({ output, serviceName: 'runtime-first' });
    const second = createCapture({ output, serviceName: 'runtime-second' });

    await Promise.all([
      first.observe('first-run', {}, async () => 'first'),
      second.observe('second-run', {}, async () => 'second'),
    ]);
    const [firstClosed, secondClosed] = await Promise.all([
      first.shutdown(),
      second.shutdown(),
    ]);

    expect(first).not.toBe(second);
    expect(firstClosed.runId).not.toBe(secondClosed.runId);
    expect(firstClosed.artifactPath).not.toBe(secondClosed.artifactPath);
    expect((await traceRows(firstClosed.artifactPath)).filter((row) => row.kind === 'run.start'))
      .toEqual([expect.objectContaining({ data: expect.objectContaining({ name: 'first-run' }) })]);
    expect((await traceRows(secondClosed.artifactPath)).filter((row) => row.kind === 'run.start'))
      .toEqual([expect.objectContaining({ data: expect.objectContaining({ name: 'second-run' }) })]);
    await expect(Promise.all([
      validateArtifact(firstClosed.artifactPath),
      validateArtifact(secondClosed.artifactPath),
    ])).resolves.toEqual([
      expect.objectContaining({ valid: true, issues: [] }),
      expect.objectContaining({ valid: true, issues: [] }),
    ]);
  });

  it('keeps tool and emit context isolated between capture handles', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-handle-context-'));
    const first = createCapture({ output, serviceName: 'runtime-context-first' });
    const second = createCapture({ output, serviceName: 'runtime-context-second' });

    await first.observe('first-run', {}, async () => {
      await second.observe('second-run', {}, async () => {
        await first.tool('first-tool', 'first-input', async (input) => `${input}-result`);
        expect(first.emit('state.first-event', { owner: 'first' })).toMatchObject({ accepted: true });
        await second.tool('second-tool', 'second-input', async (input) => `${input}-result`);
        expect(second.emit('state.second-event', { owner: 'second' })).toMatchObject({ accepted: true });
      });
    });

    const [firstRows, secondRows] = await Promise.all([
      first.shutdown().then((closed) => traceRows(closed.artifactPath)),
      second.shutdown().then((closed) => traceRows(closed.artifactPath)),
    ]);
    const firstEvidence = firstRows.flatMap((row) => [row.data.name, row.data.type]);
    const secondEvidence = secondRows.flatMap((row) => [row.data.name, row.data.type]);
    expect(firstEvidence).toEqual(expect.arrayContaining([
      'first-run', 'first-tool', 'state.first-event',
    ]));
    expect(firstEvidence).not.toEqual(expect.arrayContaining([
      'second-run', 'second-tool', 'state.second-event',
    ]));
    expect(secondEvidence).toEqual(expect.arrayContaining([
      'second-run', 'second-tool', 'state.second-event',
    ]));
    expect(secondEvidence).not.toEqual(expect.arrayContaining([
      'first-run', 'first-tool', 'state.first-event',
    ]));
  });

  it('keeps installed source context isolated between capture handles', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-source-context-'));
    const first = createCapture({ output, serviceName: 'runtime-source-first' });
    const second = createCapture({ output, serviceName: 'runtime-source-second' });
    let firstSink: SourceSink | undefined;
    let secondSink: SourceSink | undefined;
    first.installSource(fixtureSource((installed) => { firstSink = installed; }));
    second.installSource(fixtureSource((installed) => { secondSink = installed; }));

    await first.observe('first-run', {}, async (firstScope) => {
      await second.observe('second-run', {}, async (secondScope) => {
        const firstRoot = firstSink!.openTrace({
          name: 'first-source-run',
          semantic: { type: 'agent.run', name: 'first-source-run' },
        });
        const secondRoot = secondSink!.openTrace({
          name: 'second-source-run',
          semantic: { type: 'agent.run', name: 'second-source-run' },
        });
        expect(firstRoot.accepted).toBe(true);
        expect(secondRoot.accepted).toBe(true);
        if (!firstRoot.accepted || !secondRoot.accepted) throw new Error('source root rejected');
        expect(firstRoot.identity.traceId).toBe(firstScope.traceId);
        expect(secondRoot.identity.traceId).toBe(secondScope.traceId);
        firstSink!.record({
          kind: 'lifecycle',
          phase: 'end',
          name: 'first-source-run',
          trace: firstRoot.identity,
          native: null,
          parentRecordId: firstRoot.recordId,
          semantic: { type: 'agent.run', status: 'succeeded' },
        });
        secondSink!.record({
          kind: 'lifecycle',
          phase: 'end',
          name: 'second-source-run',
          trace: secondRoot.identity,
          native: null,
          parentRecordId: secondRoot.recordId,
          semantic: { type: 'agent.run', status: 'succeeded' },
        });
      });
    });

    const [firstClosed, secondClosed] = await Promise.all([
      first.shutdown(),
      second.shutdown(),
    ]);
    await expect(Promise.all([
      validateArtifact(firstClosed.artifactPath),
      validateArtifact(secondClosed.artifactPath),
    ])).resolves.toEqual([
      expect.objectContaining({ valid: true, issues: [] }),
      expect.objectContaining({ valid: true, issues: [] }),
    ]);
  });

  it('keeps continuation identities joinable across two sealed bundles', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-continuation-'));
    const identityKey = 'fixture-continuation-key-32-bytes';
    const first = initialize({
      output,
      serviceName: 'runtime-continuation',
      identityKey,
    });
    await first.observe('turn-one', {
      conversationId: 'conversation-a',
      turnId: 'turn-one',
      turnIndex: 0,
    }, async () => 'first');
    const firstRows = await traceRows((await first.shutdown()).artifactPath);
    await resetCaptureForTests();

    const second = initialize({
      output,
      serviceName: 'runtime-continuation',
      identityKey,
    });
    await second.observe('turn-two', {
      conversationId: 'conversation-a',
      turnId: 'turn-two',
      turnIndex: 1,
      previousTurnId: 'turn-one',
    }, async () => 'second');
    const secondRows = await traceRows((await second.shutdown()).artifactPath);

    const firstStart = firstRows.find((row) => row.kind === 'run.start')!;
    const secondStart = secondRows.find((row) => row.kind === 'run.start')!;
    expect(secondStart.data).toMatchObject({
      conversation_id: firstStart.data.conversation_id,
      previous_turn_id: firstStart.data.turn_id,
      turn_index: 1,
    });
    expect(secondStart.data.turn_id).not.toBe(firstStart.data.turn_id);
  });

  it('keeps raw conversation and turn identities valid for arbitrary input', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-raw-identities-'));
    const capture = initialize({
      output,
      serviceName: 'runtime-raw-identities',
      identityMode: 'raw',
    });
    await capture.observe('raw-identities', {
      conversationId: `UPPER/ü/${'x'.repeat(300)}`,
      turnId: `TURN/ü/${'y'.repeat(300)}`,
      previousTurnId: 'PREVIOUS/Ü',
      turnIndex: 1,
    }, async () => undefined);
    await capture.observe('case-upper', {
      turnId: 'A',
      turnIndex: 0,
    }, async () => undefined);
    await capture.observe('case-lower', {
      turnId: 'a',
      turnIndex: 0,
    }, async () => undefined);

    const closed = await capture.shutdown();
    const starts = (await traceRows(closed.artifactPath))
      .filter((row) => row.kind === 'run.start');
    const start = starts.find((row) => row.data.name === 'raw-identities')!;
    for (const identity of [
      start.data.conversation_id,
      start.data.turn_id,
      start.data.previous_turn_id,
    ]) {
      expect(identity).toMatch(/^[a-z][a-z0-9._:-]{7,127}$/);
    }
    expect(starts.find((row) => row.data.name === 'case-upper')?.data.turn_id)
      .not.toBe(starts.find((row) => row.data.name === 'case-lower')?.data.turn_id);
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  });

  it('bounds retained turn-order history and records correlation loss on eviction', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-turn-history-'));
    const capture = initialize({ output, serviceName: 'runtime-turn-history' });
    for (let index = 0; index < 1_025; index += 1) {
      await capture.observe(`turn-${index}`, {
        conversationId: `conversation-${index}`,
        turnId: `turn-${index}`,
        turnIndex: 0,
      }, async () => undefined);
    }

    const closed = await capture.shutdown();
    expect(closed.losses.turn_order_ambiguous).toBeGreaterThan(0);
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  }, 30_000);

  it('retains turn-order correlation across flush boundaries', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-turn-flush-'));
    const capture = initialize({ output, serviceName: 'runtime-turn-flush' });
    await capture.observe('first', {
      conversationId: 'conversation',
      turnId: 'turn',
      turnIndex: 0,
    }, async () => undefined);
    await capture.flush();
    await capture.observe('conflicting', {
      conversationId: 'conversation',
      turnId: 'turn',
      turnIndex: 1,
    }, async () => undefined);

    expect((await capture.shutdown()).losses.turn_order_ambiguous).toBe(1);
  });

  it('preserves application return and error identity and executes each tool once', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-parity-'));
    const capture = initialize({ output, serviceName: 'runtime-parity' });
    const returned = { exact: true };
    const failure = new Error('exact failure');
    let calls = 0;

    await expect(capture.observe('success', {}, async (scope) => (
      scope.tool('lookup', { key: 'safe' }, async () => {
        calls += 1;
        return returned;
      })
    ))).resolves.toBe(returned);
    await expect(capture.observe('failure', {}, async (scope) => (
      scope.tool('lookup', { key: 'missing' }, async () => {
        calls += 1;
        throw failure;
      })
    ))).rejects.toBe(failure);

    const rows = await traceRows((await capture.shutdown()).artifactPath);
    expect(calls).toBe(2);
    expect(rows.filter((row) => row.kind === 'tool.call')).toHaveLength(2);
    expect(rows.filter((row) => row.kind === 'tool.result')).toHaveLength(2);
    expect(rows.find((row) => (
      row.kind === 'tool.result' && row.data.status === 'failed'
    ))?.data.error).toMatchObject({ message: 'exact failure' });
    expect(rows.filter((row) => row.kind === 'run.outcome').map((row) => row.data.status))
      .toEqual(['completed', 'failed']);
  });

  it('suppresses the manual wrapper error only for the exact admitted source error object', async () => {
    const failure = new Error('exact source failure');
    const result = await sourceErrorTrace('same-error-identity', failure, failure);

    expect(result.sourceAccepted).toBe(true);
    expect(result.rows.filter((row) => row.kind === 'error')).toHaveLength(1);
    expect(result.rows.filter((row) => row.kind === 'run.outcome')).toEqual([
      expect.objectContaining({ data: expect.objectContaining({ status: 'failed' }) }),
    ]);
    expect(result.text).not.toContain('errorIdentity');
  });

  it('preserves a distinct outer error after a source caught an earlier error', async () => {
    const sourceFailure = new Error('source failure');
    const outerFailure = new Error('outer failure');
    const result = await sourceErrorTrace(
      'distinct-error-identities',
      sourceFailure,
      outerFailure,
    );

    expect(result.rows.filter((row) => row.kind === 'error')).toHaveLength(2);
    expect(result.rows.filter((row) => row.kind === 'error').map((row) => (
      row.data.message
    ))).toEqual(['source failure', 'outer failure']);
  });

  it('preserves errors with equal type and message when their object identities differ', async () => {
    const result = await sourceErrorTrace(
      'lookalike-error-identities',
      new TypeError('same failure'),
      new TypeError('same failure'),
    );

    expect(result.rows.filter((row) => row.kind === 'error')).toHaveLength(2);
    expect(result.rows.filter((row) => row.kind === 'error').map((row) => row.data))
      .toEqual([
        expect.objectContaining({ type: 'type_error', message: 'same failure' }),
        expect.objectContaining({ type: 'type_error', message: 'same failure' }),
      ]);
  });

  it('does not suppress an outer error without an admitted runtime identity', async () => {
    const failure = new Error('unregistered failure');
    const missing = await sourceErrorTrace(
      'missing-error-identity',
      failure,
      failure,
      { includeIdentity: false },
    );
    expect(missing.rows.filter((row) => row.kind === 'error')).toHaveLength(2);

    await resetCaptureForTests();
    const rejected = await sourceErrorTrace(
      'rejected-error-identity',
      failure,
      failure,
      { rejectSourceError: true },
    );
    expect(rejected.sourceAccepted).toBe(false);
    expect(rejected.rows.filter((row) => row.kind === 'error')).toHaveLength(1);
  });

  it('waits for detached child work, drains sources, then freezes late callbacks', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-shutdown-'));
    const capture = initialize({ output, serviceName: 'runtime-shutdown' });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    let childFinished = false;
    let sink: SourceSink | undefined;
    let drainReceipt: ReturnType<SourceSink['record']> | undefined;
    let root: ReturnType<SourceSink['openTrace']> | undefined;
    const lifecycle: string[] = [];
    const source: CaptureSource = {
      metadata: {
        name: 'fixture:shutdown',
        seam: 'fixture.callbacks',
        identityDomain: 'fixture.operation',
        coverage: [],
      },
      install(installed) {
        sink = installed;
        root = installed.openTrace({
          name: 'source-run',
          semantic: { type: 'agent.run', name: 'source-run' },
        });
        return {
          deactivate() { lifecycle.push('deactivate'); },
          drain() {
            lifecycle.push('drain');
            if (root?.accepted) {
              drainReceipt = installed.record({
                kind: 'lifecycle',
                phase: 'end',
                name: 'source-run',
                trace: root.identity,
                native: null,
                parentRecordId: root.recordId,
                semantic: { type: 'agent.run', status: 'succeeded' },
              });
            }
          },
        };
      },
    };
    capture.installSource(source);
    await capture.observe('detached-child', {}, async (scope) => {
      void scope.tool('slow', {}, async () => {
        await gate;
        childFinished = true;
        return 'done';
      });
    });

    const closing = capture.shutdown();
    await Promise.resolve();
    expect(childFinished).toBe(false);
    release();
    const closed = await closing;

    expect(childFinished).toBe(true);
    expect(lifecycle).toEqual(['deactivate', 'drain']);
    expect(drainReceipt).toMatchObject({ accepted: true });
    expect(root?.accepted).toBe(true);
    if (!root?.accepted) throw new Error('source root was rejected');
    expect(sink!.record({
      kind: 'state',
      phase: 'event',
      name: 'late',
      trace: root.identity,
      native: null,
      semantic: { type: 'state', state_type: 'late', value: true },
    })).toMatchObject({ accepted: false, reason: 'source_frozen' });
    const rows = await traceRows(closed.artifactPath);
    expect(rows.some((row) => row.kind === 'tool.result'
      && row.data.status === 'succeeded')).toBe(true);
    expect(rows.some((row) => row.kind === 'run.outcome'
      && row.data.status === 'completed')).toBe(true);
  });

  it('keeps exact source identities immutable and rejects forged identities with a durable loss', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-identity-'));
    const capture = initialize({ output, serviceName: 'runtime-identity' });
    let sink: SourceSink | undefined;
    capture.installSource(fixtureSource((installed) => { sink = installed; }));
    const root = sink!.openTrace({
      name: 'identity-run',
      semantic: { type: 'agent.run', name: 'identity-run' },
    });
    expect(root.accepted).toBe(true);
    if (!root.accepted) throw new Error(root.reason);
    expect(Object.isFrozen(root.identity)).toBe(true);

    const forged = sink!.record({
      kind: 'state',
      phase: 'event',
      name: 'forged',
      trace: { ...root.identity, traceId: 'trace_forged' },
      native: null,
      semantic: { type: 'state', state_type: 'forged', value: true },
    });
    expect(forged).toMatchObject({ accepted: false, reason: 'invalid_record' });
    await forged.settled;
    sink!.record({
      kind: 'lifecycle',
      phase: 'end',
      name: 'identity-run',
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'agent.run', status: 'succeeded' },
    });

    const rows = await traceRows((await capture.shutdown()).artifactPath);
    expect(rows.some((row) => row.kind === 'state'
      && row.data.type === 'forged')).toBe(false);
    expect(rows.some((row) => row.kind === 'loss'
      && row.data.reason === 'source_rejection')).toBe(true);
  });

  it('never persists configured secrets and keeps trace files owner-only', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-secret-'));
    const secret = 'sdk-secret-value';
    const capture = initialize({
      output,
      serviceName: 'runtime-secret',
      secretValues: [secret],
    });
    await capture.observe('safe-run', {}, async (scope) => {
      const receipt = scope.emit('unsafe-state', { token: secret });
      await receipt.settled;
      return 'safe';
    });

    const closed = await capture.shutdown();
    const tracePath = join(closed.artifactPath, 'trace.jsonl');
    const text = await readFile(tracePath, 'utf8');
    expect(text).not.toContain(secret);
    expect((await stat(tracePath)).mode & 0o777).toBe(0o600);
    expect(JSON.parse(await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8')))
      .toMatchObject({ state: 'sealed' });
    expect(traceRowsFromText(text).some((row) => row.kind === 'loss'
      && ['credential_redaction', 'scrubber_failure_payload_omitted'].includes(
        String(row.data.reason),
      ))).toBe(true);
  });

  it('coalesces one record redactions into one counted loss', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-redaction-count-'));
    const secret = 'sdk-secret-value';
    const capture = initialize({
      output,
      serviceName: 'runtime-redaction-count',
      secretValues: [secret],
    });
    await capture.observe('redaction-run', {}, async (scope) => {
      await scope.emit('redacted-state', { first: secret, second: secret }).settled;
    });

    const rows = await traceRows((await capture.shutdown()).artifactPath);
    const losses = rows.filter((row) => (
      row.kind === 'loss' && row.data.reason === 'credential_redaction'
    ));
    expect(losses).toHaveLength(1);
    expect(losses[0]?.data.count).toBe(2);
  });

  it('accepts a signed query redacted immediately before a JSON quote escape', () => {
    const scanner = new CredentialScanner();
    const scrubbed = scanner.scrub({
      output: 'command "https://example.test/callback?api_key=sensitive-query-value"',
    });
    const encoded = Buffer.from(JSON.stringify(scrubbed.value));

    expect(scrubbed.redactions).toBe(1);
    expect(encoded.toString('utf8')).not.toContain('sensitive-query-value');
    expect(scanner.scan(encoded)).toBe(true);
  });
  it('rejects configured secrets too short for reliable artifact scanning', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-short-secret-'));
    expect(() => initialize({
      output,
      serviceName: 'runtime-short-secret',
      secretValues: ['abc'],
    })).toThrow('secretValues entries must contain at least 8 bytes');
  });

  it('does not mistake its redaction sentinel for the original configured secret', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-sentinel-secret-'));
    const capture = initialize({
      output,
      serviceName: 'runtime-sentinel-secret',
      secretValues: ['REDACTED', ''],
    });
    await capture.observe(
      'sentinel-run',
      { input: { credential: 'prefix-REDACTED-suffix' } },
      async () => 'safe',
    );

    const closed = await capture.shutdown();
    const traceText = await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8');
    const manifestText = await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8');
    expect(traceText).not.toContain('REDACTED');
    expect(manifestText).not.toContain('REDACTED');
    const rows = traceRowsFromText(traceText);
    expect(rows.find((row) => row.kind === 'run.start')?.data.input)
      .toEqual({ credential: 'prefix-[SL:0000000000000000]-suffix' });
    expect(rows.filter((row) => (
      row.kind === 'loss' && row.data.reason === 'credential_redaction'
    ))).toHaveLength(1);
    expect(await validateArtifact(closed.artifactPath, {
      secretValues: ['REDACTED', ''],
    })).toMatchObject({ valid: true, issues: [], secretMatches: 0 });
  });

  it.skipIf(process.platform === 'win32')(
    'rejects an output symlink before changing its target permissions',
    async () => {
      const sandbox = await mkdtemp(join(tmpdir(), 'semantic-runtime-output-link-'));
      const target = join(sandbox, 'target');
      const output = join(sandbox, 'output');
      await mkdir(target);
      await chmod(target, 0o755);
      await symlink(target, output, 'dir');

      expect(() => initialize({
        output,
        serviceName: 'runtime-output-link',
      })).toThrow(/symbolic link/);
      expect((await stat(target)).mode & 0o777).toBe(0o755);
    },
  );

  it.skipIf(process.platform === 'win32')(
    'rejects an intermediate output symlink but allows the macOS tmp alias',
    async () => {
      const sandbox = await mkdtemp(join(tmpdir(), 'semantic-runtime-intermediate-link-'));
      const external = await mkdtemp(join(tmpdir(), 'semantic-runtime-intermediate-target-'));
      const linkedParent = join(sandbox, 'linked');
      await symlink(external, linkedParent, 'dir');

      expect(() => initialize({
        output: join(linkedParent, 'traces'),
        serviceName: 'runtime-intermediate-link',
      })).toThrow(/symbolic link/);

      const aliased = await mkdtemp('/tmp/semantic-runtime-system-alias-');
      const capture = initialize({
        output: join(aliased, 'traces'),
        serviceName: 'runtime-system-alias',
      });
      expect((await capture.shutdown()).state).toBe('closed');
    },
  );

  it.skipIf(process.platform === 'win32')(
    'secures every SDK-created output component without chmodding existing ancestors',
    async () => {
      const sandbox = await mkdtemp(join(tmpdir(), 'semantic-runtime-permission-boundary-'));
      const existingAncestor = join(sandbox, 'workspace');
      await mkdir(existingAncestor);
      await chmod(existingAncestor, 0o755);
      const privacyBoundary = join(existingAncestor, '.semantic-layer');
      const output = join(privacyBoundary, 'traces');

      const capture = initialize({
        output,
        serviceName: 'runtime-permission-boundary',
      });
      const closed = await capture.shutdown();

      expect((await stat(existingAncestor)).mode & 0o777).toBe(0o755);
      expect((await stat(privacyBoundary)).mode & 0o777).toBe(0o700);
      expect((await stat(output)).mode & 0o777).toBe(0o700);
      expect((await stat(closed.artifactPath)).mode & 0o777).toBe(0o700);
      expect((await stat(join(closed.artifactPath, 'trace.jsonl'))).mode & 0o777).toBe(0o600);
      expect((await stat(join(closed.artifactPath, 'manifest.json'))).mode & 0o777).toBe(0o600);
    },
  );

  it.skipIf(process.platform === 'win32')(
    'does not chmod a pre-existing privacy-boundary ancestor',
    async () => {
      const sandbox = await mkdtemp(join(tmpdir(), 'semantic-runtime-existing-boundary-'));
      const privacyBoundary = join(sandbox, '.semantic-layer');
      await mkdir(privacyBoundary);
      await chmod(privacyBoundary, 0o750);
      const output = join(privacyBoundary, 'traces');

      const capture = initialize({
        output,
        serviceName: 'runtime-existing-boundary',
      });
      const closed = await capture.shutdown();

      expect((await stat(privacyBoundary)).mode & 0o777).toBe(0o750);
      expect((await stat(output)).mode & 0o777).toBe(0o700);
      expect((await stat(closed.artifactPath)).mode & 0o777).toBe(0o700);
    },
  );

  it('rolls back blobs staged for a source root rejected by the final secret scan', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-blob-rollback-'));
    const secret = 'native-identity-secret';
    const capture = initialize({
      output,
      serviceName: 'runtime-blob-rollback',
      secretValues: [secret],
    });
    let opened: ReturnType<SourceSink['openTrace']> | undefined;
    capture.installSource({
      metadata: {
        name: 'fixture:blob-rollback',
        seam: 'fixture.open',
        identityDomain: 'fixture.operation',
        coverage: [],
      },
      install(sink) {
        opened = sink.openTrace({
          name: 'rejected-root',
          nativeIdentity: secret,
          native: { body: new Uint8Array([1, 2, 3]) },
          semantic: { type: 'agent.run', name: 'rejected-root' },
        });
        return { deactivate() {}, drain() {} };
      },
    });
    expect(opened).toMatchObject({
      accepted: false,
      reason: 'final_secret_scan_blocked',
    });
    await opened!.settled;
    expect(capture.status().queue.pendingBytes).toBe(0);

    const closed = await capture.shutdown();
    await expect(stat(join(closed.artifactPath, 'blobs'))).rejects.toMatchObject({
      code: 'ENOENT',
    });
    expect((await traceRows(closed.artifactPath)).some((row) => (
      row.kind === 'run.start' && row.data.name === 'rejected-root'
    ))).toBe(false);
  });

  it('quarantines an abandoned writer and records crash and uncertain-tail losses once', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-crash-recovery-'));
    const stale = join(output, 'run-stale-fixture');
    await mkdir(stale, { recursive: true, mode: 0o700 });
    await writeFile(
      join(stale, 'manifest.json'),
      JSON.stringify({ state: 'open' }),
      { mode: 0o600 },
    );
    await writeFile(
      join(stale, '.writer.lock'),
      JSON.stringify({ pid: 999_999_999 }),
      { mode: 0o600 },
    );
    await writeFile(join(stale, 'trace.jsonl'), '{"partial":', { mode: 0o600 });

    const capture = initialize({ output, serviceName: 'runtime-crash-recovery' });
    const rows = await traceRows((await capture.shutdown()).artifactPath);
    expect(rows.filter((row) => row.kind === 'loss').map((row) => row.data.reason))
      .toEqual(['crash_recovery', 'uncertain_tail']);
    expect(await readdir(output)).toContain('quarantine-run-stale-fixture');

    await resetCaptureForTests();
    const next = initialize({ output, serviceName: 'runtime-crash-recovery' });
    expect((await traceRows((await next.shutdown()).artifactPath)).some((row) => (
      row.kind === 'loss' && row.data.reason === 'crash_recovery'
    ))).toBe(false);
  });

  it.skipIf(process.platform === 'win32')(
    'quarantines a linked stale run without touching its external target',
    async () => {
      const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-linked-recovery-'));
      const external = await mkdtemp(join(tmpdir(), 'semantic-runtime-external-run-'));
      const externalLock = join(external, '.writer.lock');
      await writeFile(externalLock, '{"external":true}', { mode: 0o600 });
      await symlink(external, join(output, 'run-linked'), 'dir');

      const capture = initialize({ output, serviceName: 'runtime-linked-recovery' });
      await capture.shutdown();

      expect(await readFile(externalLock, 'utf8')).toBe('{"external":true}');
      expect(await readdir(output)).toContain('quarantine-run-linked');
    },
  );

  it('records missing required parent context without blocking application work', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-context-'));
    const capture = initialize({ output, serviceName: 'runtime-context' });
    const result = await capture.observe('context-run', {
      parentContext: { required: true },
    }, async () => 'application-result');
    expect(result).toBe('application-result');

    const rows = await traceRows((await capture.shutdown()).artifactPath);
    expect(rows.some((row) => row.kind === 'loss'
      && row.data.reason === 'missing_parent_context')).toBe(true);
    expect(rows.some((row) => row.kind === 'loss'
      && row.data.reason === 'unsupported_semantic_projection')).toBe(false);
  });

  it('persists one explicit loss for each admitted event truncated by native snapshot limits', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-snapshot-limit-'));
    const capture = initialize({ output, serviceName: 'runtime-snapshot-limit' });
    let sink: SourceSink | undefined;
    capture.installSource(fixtureSource((installed) => { sink = installed; }));
    const limited = snapshotRecord({
      first: new Array(500_000),
      second: new Array(500_000),
    });
    const root = sink!.openTrace({
      name: 'snapshot-limited',
      native: { first: limited, second: limited },
      semantic: { type: 'agent.run', name: 'snapshot-limited' },
    });
    expect(root.accepted).toBe(true);
    if (!root.accepted) throw new Error(root.reason);
    sink!.record({
      trace: root.identity,
      kind: 'lifecycle',
      phase: 'end',
      name: 'snapshot-limited',
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'agent.run', status: 'succeeded' },
    });

    const rows = await traceRows((await capture.shutdown()).artifactPath);
    const losses = rows.filter((row) => (
      row.kind === 'loss'
      && row.data.reason === 'serialization_failure'
    ));
    expect(losses).toHaveLength(1);
    expect(losses[0]?.links).toEqual([
      expect.objectContaining({ type: 'affects' }),
    ]);
  });

  it('keeps compatible initialization idempotent and rejects incompatible options', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-singleton-'));
    const first = initialize({ output, serviceName: 'runtime-singleton' });
    expect(initialize({ output, serviceName: 'runtime-singleton' })).toBe(first);
    expect(() => initialize({ output, serviceName: 'different' }))
      .toThrow('initialize received options incompatible with the active capture runtime');
  });

  it('keeps independent captures separate from the initialize singleton', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-singleton-coexistence-'));
    const independent = createCapture({ output, serviceName: 'runtime-singleton' });
    const singletonCapture = initialize({ output, serviceName: 'runtime-singleton' });
    const anotherIndependent = createCapture({ output, serviceName: 'runtime-singleton' });

    expect(initialize({ output, serviceName: 'runtime-singleton' })).toBe(singletonCapture);
    expect(independent).not.toBe(singletonCapture);
    expect(anotherIndependent).not.toBe(singletonCapture);
    expect(anotherIndependent).not.toBe(independent);
    expect(() => initialize({ output, serviceName: 'different' }))
      .toThrow('initialize received options incompatible with the active capture runtime');

    await Promise.all([
      independent.observe('independent-run', {}, async () => undefined),
      singletonCapture.observe('singleton-run', {}, async () => undefined),
      anotherIndependent.observe('another-independent-run', {}, async () => undefined),
    ]);
    const closed = await Promise.all([
      independent.shutdown(),
      singletonCapture.shutdown(),
      anotherIndependent.shutdown(),
    ]);
    expect(new Set(closed.map((status) => status.runId)).size).toBe(3);
    await expect(Promise.all(closed.map((status) => validateArtifact(status.artifactPath))))
      .resolves.toEqual([
        expect.objectContaining({ valid: true, issues: [] }),
        expect.objectContaining({ valid: true, issues: [] }),
        expect.objectContaining({ valid: true, issues: [] }),
      ]);
  });
});

function fixtureSource(installed: (sink: SourceSink) => void): CaptureSource {
  return {
    metadata: {
      name: 'fixture:source',
      seam: 'fixture.callback',
      identityDomain: 'fixture.operation',
      coverage: [],
    },
    install(sink) {
      installed(sink);
      return { deactivate() {}, drain() {} };
    },
  };
}

async function sourceErrorTrace(
  name: string,
  sourceError: Error,
  thrown: Error,
  options: {
    includeIdentity?: boolean;
    rejectSourceError?: boolean;
  } = {},
): Promise<{ rows: SemanticTraceRecord[]; sourceAccepted: boolean; text: string }> {
  const output = await mkdtemp(join(tmpdir(), 'semantic-runtime-source-error-'));
  const capture = initialize({ output, serviceName: `runtime-${name}` });
  let sink: SourceSink | undefined;
  capture.installSource(fixtureSource((installed) => { sink = installed; }));
  let sourceAccepted = false;

  await capture.observe(name, {}, async () => {
    const root = sink!.openTrace({
      name: `${name}.source`,
      semantic: { type: 'agent.run', name: `${name}.source` },
    });
    if (!root.accepted) throw new Error(root.reason);
    const closeSource = () => sink!.record({
      kind: 'lifecycle' as const,
      phase: 'error' as const,
      name: `${name}.source`,
      trace: root.identity,
      native: null,
      parentRecordId: root.recordId,
      semantic: { type: 'agent.run', status: 'failed' },
    });
    if (options.rejectSourceError) closeSource();
    const receipt = sink!.record({
      kind: 'error',
      phase: 'error',
      name: `${name}.error`,
      trace: root.identity,
      native: { name: sourceError.name, message: sourceError.message },
      semantic: {
        type: 'agent.error',
        error: {
          type: sourceError instanceof TypeError ? 'type_error' : 'error',
          message: sourceError.message,
          recoverable: false,
        },
      },
      ...(options.includeIdentity === false ? {} : { errorIdentity: sourceError }),
    });
    sourceAccepted = receipt.accepted;
    if (!options.rejectSourceError) closeSource();
    throw thrown;
  }).then(
    () => { throw new Error('observation unexpectedly succeeded'); },
    (error: unknown) => {
      if (error !== thrown) throw error;
    },
  );

  const closed = await capture.shutdown();
  const text = await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8');
  return { rows: traceRowsFromText(text), sourceAccepted, text };
}

async function traceRows(path: string): Promise<SemanticTraceRecord[]> {
  return traceRowsFromText(await readFile(join(path, 'trace.jsonl'), 'utf8'));
}

function traceRowsFromText(text: string): SemanticTraceRecord[] {
  return text.trim().split('\n').filter(Boolean)
    .map((line) => JSON.parse(line) as SemanticTraceRecord);
}
