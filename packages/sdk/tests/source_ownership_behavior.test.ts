import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { afterEach, describe, expect, it } from 'vitest';

import {
  initialize,
  openAIProviderAdapter,
  resetCaptureForTests,
  validateArtifact,
  type CaptureSource,
  type SourceOwnership,
} from '../src/index.js';
import type { SemanticTraceRecord } from '../src/trace/semantic-projector.js';
import { SourceOwnershipRegistry, trustOfficialSource } from '../src/v1/source-ownership.js';

afterEach(async () => resetCaptureForTests());

describe('source ownership through the public SDK', () => {
  it('requires an observed version for exact source qualification', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-source-qualification-'));
    const capture = initialize({ output, serviceName: 'source-qualification' });

    expect(() => capture.installSource({
      metadata: {
        name: 'unversioned-exact-source',
        seam: 'fixture.callback',
        identityDomain: 'fixture.operation',
        coverage: [],
        qualification: { status: 'exact_qualified' },
      },
      install() { return { deactivate() {}, drain() {} }; },
    })).toThrow(/exact_qualified.*version/u);
  });

  it('rejects source fields that cannot fit the manifest schema', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-ownership-source-fields-'));
    const capture = initialize({ output, serviceName: 'source-field-limits' });
    const long = 'x'.repeat(257);
    for (const metadata of [
      {
        name: long,
        seam: 'fixture.callback',
        identityDomain: 'fixture.operation',
        coverage: [],
      },
      {
        name: 'long-seam',
        seam: long,
        identityDomain: 'fixture.operation',
        coverage: [],
      },
      {
        name: 'long-version',
        seam: 'fixture.callback',
        version: long,
        identityDomain: 'fixture.operation',
        coverage: [],
      },
    ]) {
      expect(() => capture.installSource({
        metadata,
        install() { return { deactivate() {}, drain() {} }; },
      })).toThrow(/256/);
    }
  });

  it('rejects ineffective preference and keeps exact overlap authority explicit', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-ownership-preference-'));
    const unsupportedPreference = {
      rules: [{
        action: 'prefer',
        source: 'official/provider:openai',
        operation: 'model-call',
        domain: 'provider.request',
      }],
    } as unknown as SourceOwnership;
    expect(() => initialize({
      output,
      serviceName: 'unsupported-preference',
      sourceOwnership: unsupportedPreference,
    })).toThrow(/prefer is unsupported/);

    const capture = initialize({
      output: join(output, 'captured'),
      serviceName: 'ownership-explicit',
    });
    const preferred = providerClient();
    const duplicate = providerClient();
    const secondary = providerClient();
    capture.instrument({
      adapter: openAIProviderAdapter({ version: 'fixture' }),
      client: preferred,
    });
    capture.instrument({
      adapter: openAIProviderAdapter({ version: 'fixture' }),
      client: duplicate,
    });
    capture.instrument({
      adapter: openAIProviderAdapter({ version: 'fixture', provider: 'openrouter' }),
      client: secondary,
    });

    preferred.responses.create({ id: 'request-1' });
    secondary.responses.create({ id: 'request-1' });

    const closed = await capture.shutdown();
    const manifest = JSON.parse(
      await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'),
    ) as { sources: Array<{ name: string }> };
    const rows = await traceRows(closed.artifactPath);
    const starts = rows.filter((row) => row.kind === 'run.start');
    const requests = rows.filter((row) => row.kind === 'model.request');
    const ambiguity = rows.filter((row) => (
      row.kind === 'loss' && row.data.path === '/coverage/ownership/ambiguous'
    ));

    expect(manifest.sources.filter((source) => source.name === 'provider:openai'))
      .toHaveLength(1);
    expect(manifest.sources.filter((source) => source.name === 'provider:openrouter'))
      .toHaveLength(1);
    expect(starts).toHaveLength(2);
    expect(new Set(starts.map((row) => row.source)).size).toBe(2);
    expect(requests).toHaveLength(2);
    expect(new Set(requests.map((row) => row.source))).toEqual(
      new Set(starts.map((row) => row.source)),
    );
    expect(ambiguity).toHaveLength(1);
    expect(rows.filter((row) => row.kind === 'loss')).toHaveLength(1);
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  });

  it('records one semantic loss for an ambiguous exact overlap', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-ownership-ambiguity-'));
    const capture = initialize({ output, serviceName: 'ownership-ambiguity' });
    const first = providerClient();
    const second = providerClient();
    capture.instrument({
      adapter: openAIProviderAdapter({ version: 'fixture' }),
      client: first,
    });
    capture.instrument({
      adapter: openAIProviderAdapter({ version: 'fixture', provider: 'openrouter' }),
      client: second,
    });

    first.responses.create({ id: 'request-1' });
    second.responses.create({ id: 'request-1' });

    const closed = await capture.shutdown();
    const rows = await traceRows(closed.artifactPath);
    const ambiguous = rows.filter((row) => (
      row.kind === 'loss' && row.data.path === '/coverage/ownership/ambiguous'
    ));

    expect(ambiguous).toHaveLength(1);
    expect(rows.filter((row) => row.kind === 'loss')).toHaveLength(1);
    expect(ambiguous[0]?.data).toMatchObject({
      reason: 'source_rejection',
      count: 1,
    });
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  });

  it('uses declared coverage roles as the overlap authority', async () => {
    const cases = [
      {
        name: 'owner-with-evidence',
        roles: ['owner', 'evidence'] as const,
        expected: {
          status: 'owned',
          primarySourceId: 'official/owner-with-evidence-0',
          secondarySourceIds: ['official/owner-with-evidence-1'],
        },
      },
      {
        name: 'multiple-owners',
        roles: ['owner', 'owner', 'evidence'] as const,
        expected: {
          status: 'ambiguous',
          secondarySourceIds: [],
        },
      },
      {
        name: 'evidence-only',
        roles: ['evidence', 'evidence'] as const,
        expected: {
          status: 'evidence_only',
          secondarySourceIds: [],
        },
      },
      {
        name: 'omitted-role-is-owner',
        roles: [undefined, 'evidence'] as const,
        expected: {
          status: 'owned',
          primarySourceId: 'official/omitted-role-is-owner-0',
          secondarySourceIds: ['official/omitted-role-is-owner-1'],
        },
      },
    ];

    for (const scenario of cases) {
      const registry = new SourceOwnershipRegistry(
        scenario.name,
        undefined,
        Buffer.alloc(32, 7),
      );
      scenario.roles.forEach((role, index) => {
        const source = trustOfficialSource(
          overlapSource(`${scenario.name}-${index}`, role),
          'deep',
        );
        const registered = registry.register(source).source;
        registry.activate(registered);
        const identity = registry.coverageIdentity(
          registered,
          'shared-request',
          { operation: 'model-call', domain: 'fixture.model' },
        );
        registry.reserve(registered, identity).settle(true);
      });

      expect(registry.freeze()).toEqual([
        expect.objectContaining(scenario.expected),
      ]);
    }
  });

  it('does not let a rejected owner reservation claim authority', () => {
    const registry = new SourceOwnershipRegistry(
      'ownership-rollback',
      undefined,
      Buffer.alloc(32, 7),
    );
    const reserve = (
      name: string,
      role: 'owner' | 'evidence',
      accepted: boolean,
    ) => {
      const registered = registry.register(
        trustOfficialSource(overlapSource(name, role), 'deep'),
      ).source;
      registry.activate(registered);
      const identity = registry.coverageIdentity(
        registered,
        'shared-request',
        { operation: 'model-call', domain: 'fixture.model' },
      );
      registry.reserve(registered, identity).settle(accepted);
    };

    reserve('rejected-owner', 'owner', false);
    reserve('evidence-a', 'evidence', true);
    reserve('evidence-b', 'evidence', true);

    expect(registry.freeze()).toEqual([
      expect.objectContaining({
        status: 'evidence_only',
        participantSourceIds: ['official/evidence-a', 'official/evidence-b'],
        secondarySourceIds: [],
      }),
    ]);
  });

  it('bounds ownership groups and preserves the total in semantic losses', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-ownership-group-bound-'));
    const capture = initialize({ output, serviceName: 'ownership-group-bound' });
    capture.installSource(manyGroupsSource(4_098));

    const closed = await capture.shutdown();
    const rows = await traceRows(closed.artifactPath);
    const overflow = rows.filter((row) => (
      row.kind === 'loss' && row.data.path === '/coverage/ownership/group_limit'
    ));

    expect(overflow.reduce((total, row) => total + Number(row.data.count), 0)).toBe(2);
    expect(closed.losses.source_rejection).toBe(2);
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  }, 30_000);

  it('bounds open source traces and releases capacity after a terminal record', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-open-source-bound-'));
    const capture = initialize({ output, serviceName: 'open-source-bound' });
    const outcomes: { rejectedReason?: string; reopened?: boolean } = {};
    capture.installSource({
      metadata: {
        name: 'open-source-bound',
        seam: 'fixture.callback',
        identityDomain: 'fixture.operation',
        coverage: [],
      },
      install(sink) {
        const opened = Array.from({ length: 4096 }, (_, index) => {
          const receipt = sink.openTrace({
            name: `open-${index}`,
            semantic: { type: 'capture.redundant' },
          });
          if (!receipt.accepted) throw new Error(receipt.reason);
          return receipt;
        });
        const overflow = sink.openTrace({
          name: 'overflow',
          semantic: { type: 'capture.redundant' },
        });
        outcomes.rejectedReason = overflow.accepted ? undefined : overflow.reason;

        const first = opened.shift();
        if (!first) throw new Error('expected an admitted source trace');
        const terminal = sink.record({
          trace: first.identity,
          kind: 'lifecycle',
          phase: 'end',
          name: 'open-0',
          native: null,
          semantic: { type: 'capture.redundant' },
        });
        if (!terminal.accepted) throw new Error(terminal.reason);

        const reopened = sink.openTrace({
          name: 'reopened',
          semantic: { type: 'capture.redundant' },
        });
        outcomes.reopened = reopened.accepted;
        if (reopened.accepted) opened.push(reopened);
        for (const receipt of opened) {
          const closed = sink.record({
            trace: receipt.identity,
            kind: 'lifecycle',
            phase: 'end',
            name: 'closed',
            native: null,
            semantic: { type: 'capture.redundant' },
          });
          if (!closed.accepted) throw new Error(closed.reason);
        }
        return { deactivate() {}, drain() {} };
      },
    });

    expect(outcomes).toEqual({
      rejectedReason: 'source_capacity',
      reopened: true,
    });
    const closed = await capture.shutdown();
    const rows = await traceRows(closed.artifactPath);
    const capacityLosses = rows.filter((row) => (
      row.kind === 'loss' && row.data.path === '/open_source_traces/capacity'
    ));
    expect(capacityLosses).toHaveLength(1);
    expect(capacityLosses[0]?.data).toMatchObject({
      reason: 'source_rejection',
      count: 1,
    });
    expect(await validateArtifact(closed.artifactPath))
      .toMatchObject({ valid: true, issues: [] });
  }, 30_000);
});

function providerClient() {
  const create = (_request: unknown) => ({
    object: 'response',
    status: 'completed',
    output: [],
  });
  return {
    responses: { create },
    chat: { completions: { create } },
  };
}

function manyGroupsSource(count: number): CaptureSource {
  const coverage = { operation: 'model-call', domain: 'fixture.model' } as const;
  return {
    metadata: {
      name: 'many-groups',
      seam: 'fixture.callback',
      identityDomain: 'fixture.request',
      coverage: [{ ...coverage, role: 'owner' }],
    },
    install(sink) {
      const opened = sink.openTrace({
        name: 'many-groups.run',
        semantic: { type: 'workflow.run', name: 'many-groups.run' },
      });
      if (!opened.accepted) throw new Error(opened.reason);
      for (let index = 0; index < count; index += 1) {
        sink.record({
          trace: opened.identity,
          kind: 'log',
          phase: 'event',
          name: 'many-groups.evidence',
          nativeIdentity: `request-${index}`,
          coverage,
          native: null,
          semantic: { type: 'capture.redundant' },
        });
      }
      sink.record({
        trace: opened.identity,
        kind: 'lifecycle',
        phase: 'end',
        name: 'many-groups.run',
        native: null,
        semantic: { type: 'workflow.run', status: 'succeeded' },
      });
      return { deactivate() {}, drain() {} };
    },
  };
}

function overlapSource(
  name: string,
  role: 'owner' | 'evidence' | undefined,
): CaptureSource {
  const coverage = { operation: 'model-call', domain: 'fixture.model' } as const;
  return {
    metadata: {
      name,
      seam: `fixture.${name}`,
      identityDomain: 'fixture.request',
      coverage: [{ ...coverage, ...(role === undefined ? {} : { role }) }],
    },
    install(sink) {
      const opened = sink.openTrace({
        name: `${name}.run`,
        semantic: { type: 'workflow.run', name: `${name}.run` },
      });
      if (!opened.accepted) throw new Error(opened.reason);
      sink.record({
        trace: opened.identity,
        kind: 'log',
        phase: 'event',
        name: `${name}.evidence`,
        nativeIdentity: 'shared-request',
        coverage,
        native: null,
        semantic: { type: 'capture.redundant' },
      });
      sink.record({
        trace: opened.identity,
        kind: 'lifecycle',
        phase: 'end',
        name: `${name}.run`,
        native: null,
        semantic: { type: 'workflow.run', status: 'succeeded' },
      });
      return { deactivate() {}, drain() {} };
    },
  };
}

async function traceRows(artifactPath: string): Promise<SemanticTraceRecord[]> {
  return (await readFile(join(artifactPath, 'trace.jsonl'), 'utf8'))
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as SemanticTraceRecord);
}
