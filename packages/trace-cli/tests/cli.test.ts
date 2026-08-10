import { createHash } from 'node:crypto';
import { chmod, copyFile, mkdir, mkdtemp, readFile, rename, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCapture } from 'semantic-layer-capture';
import { describe, expect, test } from 'vitest';
import { helpText, main, safeHuman } from '../src/bin.js';

const tenant = 'tenant_test';
const installation = 'install_0123456789abcdef0123456789abcdef';
const bundle = 'bundle_code_example';

describe('CLI privacy and guidance', () => {
  test('root and command help are side effect free and explain the workflow', async () => {
    const root = await mkdtemp(join(tmpdir(), 'sl-traces-help-'));
    const config = join(root, 'missing', 'config.json');
    const capture = streams(false);
    expect(await main(['--help'], { streams: capture.value, configFile: config })).toBe(0);
    expect(capture.out()).toMatch(/Workflow[\s\S]*gcloud auth application-default login/u);
    expect(capture.out()).toMatch(/Find VMs/u);
    expect(capture.out()).toMatch(/SEMANTIC_LAYER_TRACES_CONFIG/u);

    for (const command of ['configure', 'doctor', 'tenants', 'installations', 'sync', 'list', 'related', 'show']) {
      const commandCapture = streams(false);
      expect(await main([command, '--help'], { streams: commandCapture.value, configFile: config })).toBe(0);
      expect(commandCapture.out()).toMatch(/^semantic-layer-traces/u);
    }
  });

  test('escapes terminal, bidi, and clipboard control characters', () => {
    expect(safeHuman('a\u001b]52;c;bad\u0007\r\b\u202eb')).toBe('a\\u001b]52;c;bad\\u0007\\u000d\\u0008\\u202eb');
  });

  test('turns missing credentials into an exact recovery command', async () => {
    const setup = await configuredOnly();
    const capture = streams(false);
    const failingStore = {
      list: async () => { throw new Error('Could not load the default credentials.'); },
      readSmall: async () => undefined,
      download: async () => { throw new Error('unused'); },
      testPermissions: async () => [],
    };
    expect(await main(['tenants', '--environment', 'staging'], {
      streams: capture.value,
      configFile: setup.config,
      store: () => failingStore,
    })).toBe(1);
    expect(capture.errors()).toBe('ERROR Google Application Default Credentials are unavailable; run: gcloud auth application-default login\n');
  });

  test('hides content in scripts and JSON unless it is explicit', async () => {
    const setup = await localFixture();
    const scope = `${tenant}/${installation}/${bundle}`;

    const scripted = streams(false);
    expect(await main(['show', scope, '--environment', 'staging'], { streams: scripted.value, configFile: setup.config })).toBe(0);
    expect(scripted.out()).toMatch(/Content is hidden/u);
    expect(scripted.out()).not.toMatch(/PRIVATE TRACE CONTENT follows/u);

    const json = streams(true);
    expect(await main(['show', '--json', scope, '--environment', 'staging'], { streams: json.value, configFile: setup.config })).toBe(0);
    expect(Object.keys(JSON.parse(json.out()))).toEqual(['summary']);

    const explicit = streams(false);
    expect(await main(['show', scope, '--environment', 'staging', '--include-content', '--json'], { streams: explicit.value, configFile: setup.config })).toBe(0);
    expect(JSON.parse(explicit.out()).records.length).toBeGreaterThan(0);

    const interactive = streams(true);
    expect(await main(['show', scope, '--environment', 'staging'], { streams: interactive.value, configFile: setup.config })).toBe(0);
    expect(interactive.out()).toMatch(/^PRIVATE TRACE CONTENT follows/u);

    const summaryOnly = streams(true);
    expect(await main(['show', scope, '--environment', 'staging', '--summary-only'], { streams: summaryOnly.value, configFile: setup.config })).toBe(0);
    expect(summaryOnly.out()).toMatch(/Content is hidden/u);
  });

  test('keeps list safe and exposes no destructive command', async () => {
    const setup = await localFixture();
    const capture = streams(false);
    expect(await main(['list', '--environment', 'staging', '--tenant', tenant, '--json'], {
      streams: capture.value,
      configFile: setup.config,
    })).toBe(0);
    const rows = JSON.parse(capture.out()) as Array<Record<string, unknown>>;
    expect(rows).toHaveLength(1);
    expect(rows[0]).not.toHaveProperty('data');
    expect(rows[0]).not.toHaveProperty('content');
    expect(rows[0]).not.toHaveProperty('toolInputs');
    const trace = (await readFile(new URL('../../../contracts/trace/v1/examples/coding-agent/trace.jsonl', import.meta.url), 'utf8'))
      .trim().split('\n').map((line) => JSON.parse(line));
    const privateContent = trace.find((record) => typeof record.data?.content === 'string')?.data.content;
    expect(typeof privateContent).toBe('string');
    expect(capture.out()).not.toContain(privateContent);
    expect(helpText()).not.toMatch(/^\s+(delete|upload|rotate-key|meter-status)\b/mu);
  });

  test('shows exact parent and root relations without exposing protected identities', async () => {
    const setup = await correlatedFixture();
    const capture = streams(false);
    expect(await main(['related', setup.seed, '--environment', 'staging', '--json'], {
      streams: capture.value,
      configFile: setup.config,
    }), capture.errors()).toBe(0);

    const result = JSON.parse(capture.out());
    expect(result.nodes, JSON.stringify(result)).toHaveLength(2);
    expect(result.nodes.every((node: Record<string, unknown>) => (
      typeof node.scope === 'string'
      && typeof node.root === 'string'
      && node.system === 'trigger.dev'
      && node.attempt === 1
    ))).toBe(true);
    expect(result.edges.map((edge: Record<string, unknown>) => edge.type).sort())
      .toEqual(['parent', 'root']);
    expect(result.warnings).toEqual([]);
    expect(capture.out()).not.toContain(setup.protectedTask);
    expect(capture.out()).not.toContain(setup.protectedParent);
    expect(capture.out()).not.toContain(setup.protectedChild);
    expect(capture.out()).not.toContain('private research content');

    const human = streams(false);
    expect(await main(['related', setup.seed, '--environment', 'staging'], {
      streams: human.value,
      configFile: setup.config,
    }), human.errors()).toBe(0);
    expect(human.out()).toMatch(/EDGE parent/u);
    for (const protectedValue of [
      setup.protectedTask,
      setup.protectedParent,
      setup.protectedChild,
      'private research content',
    ]) expect(human.out()).not.toContain(protectedValue);
  });

  test('keeps retries and replays distinct and reports only safe identity warnings', async () => {
    const setup = await correlatedFixture();
    const protectedReplay = `run_${'4'.repeat(64)}`;
    const protectedOrphan = `run_${'5'.repeat(64)}`;
    const protectedMissing = `run_${'6'.repeat(64)}`;
    await addLocalBundle(setup.output, 'retry', {
      task_id: setup.protectedTask,
      execution: {
        system: 'trigger.dev',
        run_id: setup.protectedParent,
        root_run_id: setup.protectedParent,
        attempt: 2,
      },
    });
    await addLocalBundle(setup.output, 'replay', {
      task_id: setup.protectedTask,
      execution: {
        system: 'trigger.dev',
        run_id: protectedReplay,
        root_run_id: protectedReplay,
        attempt: 1,
      },
    });
    await addLocalBundle(setup.output, 'duplicate', {
      task_id: setup.protectedTask,
      execution: {
        system: 'trigger.dev',
        run_id: setup.protectedParent,
        root_run_id: setup.protectedParent,
        attempt: 1,
      },
    });
    await addLocalBundle(setup.output, 'orphan', {
      task_id: setup.protectedTask,
      execution: {
        system: 'trigger.dev',
        run_id: protectedOrphan,
        parent_run_id: protectedMissing,
        root_run_id: protectedMissing,
        attempt: 1,
      },
    });
    await addLocalBundle(setup.output, 'old-bundle');
    const invalidScope = await addLocalBundle(setup.output, 'invalid-bundle', {
      task_id: setup.protectedTask,
      execution: {
        system: 'trigger.dev',
        run_id: `run_${'9'.repeat(64)}`,
        attempt: 1,
      },
    });
    const invalidTrace = join(setup.output, ...invalidScope.split('/'), 'trace.jsonl');
    await writeFile(invalidTrace, `${await readFile(invalidTrace, 'utf8')}private invalid content`, {
      mode: 0o600,
    });

    const capture = streams(false);
    expect(await main(['related', setup.seed, '--environment', 'staging', '--json'], {
      streams: capture.value,
      configFile: setup.config,
    }), capture.errors()).toBe(0);
    const result = JSON.parse(capture.out());

    expect(result.nodes).toHaveLength(6);
    expect(result.nodes.filter((node: Record<string, unknown>) => node.retry)).toHaveLength(3);
    expect(new Set(result.nodes.map((node: Record<string, unknown>) => (
      `${node.scope}#${node.root}`
    ))).size).toBe(6);
    expect(result.warnings.map((warning: Record<string, unknown>) => warning.code).sort())
      .toEqual([
        'ambiguous_parent',
        'ambiguous_root',
        'duplicate_execution_attempt',
        'duplicate_execution_attempt',
        'invalid_bundle_skipped',
        'missing_identity',
        'unresolved_parent',
        'unresolved_root',
      ]);
    for (const protectedValue of [
      setup.protectedTask,
      setup.protectedParent,
      setup.protectedChild,
      protectedReplay,
      protectedOrphan,
      protectedMissing,
    ]) expect(capture.out()).not.toContain(protectedValue);
  });

  test('requires an exact root selector when a seed bundle has multiple roots', async () => {
    const setup = await correlatedFixture();
    const secondTask = `task_${'7'.repeat(64)}`;
    const secondRun = `run_${'8'.repeat(64)}`;
    await appendRunStart(join(setup.output, ...setup.seed.split('/')), {
      task_id: secondTask,
      execution: {
        system: 'trigger.dev',
        run_id: secondRun,
        root_run_id: secondRun,
        attempt: 1,
      },
    });

    const ambiguous = streams(false);
    expect(await main(['related', setup.seed, '--environment', 'staging', '--json'], {
      streams: ambiguous.value,
      configFile: setup.config,
    }), ambiguous.errors()).toBe(0);
    const warning = JSON.parse(ambiguous.out());
    expect(warning.nodes).toEqual([]);
    expect(warning.warnings.map((item: Record<string, unknown>) => item.code))
      .toContain('ambiguous_seed_roots');

    const selected = streams(false);
    expect(await main([
      'related', setup.seed,
      '--root', setup.seedRoot,
      '--environment', 'staging',
      '--json',
    ], { streams: selected.value, configFile: setup.config }), selected.errors()).toBe(0);
    expect(JSON.parse(selected.out()).nodes).toHaveLength(2);
    expect(selected.out()).not.toContain(secondTask);
    expect(selected.out()).not.toContain(secondRun);
  });

  test('does not join equal protected run tokens across execution systems', async () => {
    const setup = await correlatedFixture();
    const otherScope = await addLocalBundle(setup.output, 'other-system', {
      task_id: setup.protectedTask,
      execution: {
        system: 'other.system',
        run_id: setup.protectedParent,
        root_run_id: setup.protectedParent,
        attempt: 1,
      },
    });

    const capture = streams(false);
    expect(await main(['related', setup.seed, '--environment', 'staging', '--json'], {
      streams: capture.value,
      configFile: setup.config,
    }), capture.errors()).toBe(0);
    const result = JSON.parse(capture.out());

    expect(result.nodes.map((node: Record<string, unknown>) => node.scope)).toContain(otherScope);
    expect(result.edges.some((edge: Record<string, any>) => edge.to.scope === otherScope))
      .toBe(false);
    expect(capture.out()).not.toContain(setup.protectedParent);
  });

  test('does not choose a parent or root attempt when retries are ambiguous', async () => {
    const setup = await correlatedFixture();
    await addLocalBundle(setup.output, 'ambiguous-retry', {
      task_id: setup.protectedTask,
      execution: {
        system: 'trigger.dev',
        run_id: setup.protectedParent,
        root_run_id: setup.protectedParent,
        attempt: 2,
      },
    });

    const capture = streams(false);
    expect(await main(['related', setup.seed, '--environment', 'staging', '--json'], {
      streams: capture.value,
      configFile: setup.config,
    }), capture.errors()).toBe(0);
    const result = JSON.parse(capture.out());

    expect(result.edges).toEqual([]);
    expect(result.warnings.map((warning: Record<string, unknown>) => warning.code).sort())
      .toEqual(['ambiguous_parent', 'ambiguous_root']);
    expect(capture.out()).not.toContain(setup.protectedParent);
  });

  test('skips a validated bundle whose manifest scope does not match its path', async () => {
    const setup = await correlatedFixture();
    const mismatchedScope = await addLocalBundle(setup.output, 'scope-mismatch', {
      task_id: setup.protectedTask,
      execution: {
        system: 'trigger.dev',
        run_id: `run_${'b'.repeat(64)}`,
        attempt: 1,
      },
    });
    const manifestPath = join(setup.output, ...mismatchedScope.split('/'), 'manifest.json');
    const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
    manifest.installation_id = 'install_abcdefghijklmnopqrstuvwxyz';
    await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });

    const capture = streams(false);
    expect(await main(['related', setup.seed, '--environment', 'staging', '--json'], {
      streams: capture.value,
      configFile: setup.config,
    }), capture.errors()).toBe(0);
    const result = JSON.parse(capture.out());

    expect(result.nodes).toHaveLength(2);
    expect(result.warnings).toContainEqual({
      code: 'invalid_bundle_skipped',
      scope: mismatchedScope,
    });
    expect(capture.out()).not.toContain(`run_${'b'.repeat(64)}`);
  });

  test('doctor fails without read access and reports broad access without hiding it', async () => {
    const setup = await configuredOnly();
    const denied = streams(false);
    const deniedStore = storeWithPermissions([], true);
    expect(await main(['doctor', '--environment', 'staging', '--json'], {
      streams: denied.value, configFile: setup.config, store: () => deniedStore,
    })).toBe(1);
    expect(JSON.parse(denied.out()).missingRead).toEqual(['storage.objects.list', 'storage.objects.get']);

    const broad = streams(false);
    const broadStore = storeWithPermissions([
      'storage.objects.get', 'storage.objects.list', 'storage.objects.create', 'storage.objects.delete',
    ]);
    expect(await main(['doctor', '--environment', 'staging', '--json'], {
      streams: broad.value, configFile: setup.config, store: () => broadStore,
    })).toBe(0);
    expect(JSON.parse(broad.out()).broadPermissions).toEqual(['storage.objects.create', 'storage.objects.delete']);
    expect(JSON.parse(broad.out()).readAccess).toBe(true);
    expect((await stat(setup.config)).mode & 0o777).toBe(0o600);

    const readDenied = streams(false);
    const readDeniedStore = {
      ...storeWithPermissions(['storage.objects.list']),
      list: async () => ['tenants/tenant_test/installations/install_0123456789abcdef0123456789abcdef/bundles/bundle_test/complete.json'],
      readSmall: async () => { throw new Error('permission denied'); },
    };
    expect(await main(['doctor', '--environment', 'staging', '--json'], {
      streams: readDenied.value, configFile: setup.config, store: () => readDeniedStore,
    })).toBe(1);
    expect(JSON.parse(readDenied.out()).missingRead).toEqual(['storage.objects.get']);
  });
});

async function localFixture(): Promise<{ config: string }> {
  const root = await mkdtemp(join(tmpdir(), 'sl-traces-show-'));
  const output = join(root, 'traces');
  const local = join(output, tenant, installation, bundle);
  await mkdir(local, { recursive: true, mode: 0o700 });
  for (const directory of [output, join(output, tenant), join(output, tenant, installation), local]) await chmod(directory, 0o700);
  for (const name of ['manifest.json', 'trace.jsonl']) {
    await copyFile(new URL(`../../../contracts/trace/v1/examples/coding-agent/${name}`, import.meta.url), join(local, name));
    await chmod(join(local, name), 0o600);
  }
  const config = join(root, 'config', 'config.json');
  const configured = streams(false);
  expect(await main([
    'configure', 'staging',
    '--project', 'gen-lang-client-0396687706',
    '--bucket', 'semantic-layer-bundles-staging-819468298912',
    '--output', output,
  ], { streams: configured.value, configFile: config })).toBe(0);
  return { config };
}

async function configuredOnly(): Promise<{ config: string }> {
  const root = await mkdtemp(join(tmpdir(), 'sl-traces-config-'));
  const config = join(root, 'config', 'config.json');
  const configured = streams(false);
  expect(await main([
    'configure', 'staging',
    '--project', 'gen-lang-client-0396687706',
    '--bucket', 'semantic-layer-bundles-staging-819468298912',
    '--output', join(root, 'traces'),
  ], { streams: configured.value, configFile: config })).toBe(0);
  return { config };
}

async function correlatedFixture(): Promise<{
  config: string;
  output: string;
  seed: string;
  seedRoot: string;
  protectedTask: string;
  protectedParent: string;
  protectedChild: string;
}> {
  const root = await mkdtemp(join(tmpdir(), 'sl-traces-related-'));
  const output = join(root, 'traces');
  const config = join(root, 'config', 'config.json');
  const configured = streams(false);
  expect(await main([
    'configure', 'staging',
    '--project', 'gen-lang-client-0396687706',
    '--bucket', 'semantic-layer-bundles-staging-819468298912',
    '--output', output,
  ], { streams: configured.value, configFile: config })).toBe(0);

  const protectedTask = `task_${'1'.repeat(64)}`;
  const protectedParent = `run_${'2'.repeat(64)}`;
  const protectedChild = `run_${'3'.repeat(64)}`;
  const identityKey = 'fixture-related-identity-key-32-bytes';
  const parent = createCapture({
    output: join(root, 'parent-stage'),
    serviceName: 'related-fixture',
    installationId: installation,
    identityKey,
  });
  await parent.observe('private research content', {
    correlation: {
      taskId: 'private-task-token',
      execution: {
        system: 'trigger.dev',
        runId: 'private-parent-run-token',
        rootRunId: 'private-parent-run-token',
        attempt: 1,
      },
    },
  }, async () => 'private parent result');
  const parentClosed = await parent.shutdown();
  await injectCorrelation(parentClosed.artifactPath, {
    task_id: protectedTask,
    execution: {
      system: 'trigger.dev',
      run_id: protectedParent,
      root_run_id: protectedParent,
      attempt: 1,
    },
  });
  const parentBundle = JSON.parse(
    await readFile(join(parentClosed.artifactPath, 'manifest.json'), 'utf8'),
  ).bundle_id as string;
  const parentRecords = (await readFile(
    join(parentClosed.artifactPath, 'trace.jsonl'),
    'utf8',
  )).trim().split('\n').map((line) => JSON.parse(line));
  const seedRoot = parentRecords.find((record) => record.kind === 'run.start').id as string;

  const child = createCapture({
    output: join(root, 'child-stage'),
    serviceName: 'related-fixture',
    installationId: installation,
    identityKey,
  });
  await child.observe('private search content', {
    correlation: {
      taskId: 'private-task-token',
      execution: {
        system: 'trigger.dev',
        runId: 'private-child-run-token',
        parentRunId: 'private-parent-run-token',
        rootRunId: 'private-parent-run-token',
        attempt: 1,
      },
    },
  }, async () => 'private child result');
  const childClosed = await child.shutdown();
  await injectCorrelation(childClosed.artifactPath, {
    task_id: protectedTask,
    execution: {
      system: 'trigger.dev',
      run_id: protectedChild,
      parent_run_id: protectedParent,
      root_run_id: protectedParent,
      attempt: 1,
    },
  });
  const childBundle = JSON.parse(
    await readFile(join(childClosed.artifactPath, 'manifest.json'), 'utf8'),
  ).bundle_id as string;

  const installationRoot = join(output, tenant, installation);
  await mkdir(installationRoot, { recursive: true, mode: 0o700 });
  await rename(parentClosed.artifactPath, join(installationRoot, parentBundle));
  await rename(childClosed.artifactPath, join(installationRoot, childBundle));
  return {
    config,
    output,
    seed: `${tenant}/${installation}/${parentBundle}`,
    seedRoot,
    protectedTask,
    protectedParent,
    protectedChild,
  };
}

async function appendRunStart(
  artifactPath: string,
  correlation: Record<string, unknown>,
): Promise<void> {
  const tracePath = join(artifactPath, 'trace.jsonl');
  const manifestPath = join(artifactPath, 'manifest.json');
  const records = (await readFile(tracePath, 'utf8')).trim().split('\n').map((line) => JSON.parse(line));
  const original = records.find((record) => record.kind === 'run.start');
  records.push({
    ...original,
    id: `record_${'a'.repeat(32)}`,
    seq: Math.max(...records.map((record) => record.seq)) + 1,
    data: { ...original.data, correlation },
  });
  const trace = `${records.map((record) => JSON.stringify(record)).join('\n')}\n`;
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
  manifest.trace.records = records.length;
  manifest.trace.last_seq = records.at(-1).seq;
  manifest.trace.bytes = Buffer.byteLength(trace);
  manifest.trace.sha256 = createHash('sha256').update(trace).digest('hex');
  await writeFile(tracePath, trace, { mode: 0o600 });
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });
}

async function addLocalBundle(
  output: string,
  stageName: string,
  correlation?: Record<string, unknown>,
): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), `sl-traces-related-${stageName}-`));
  const capture = createCapture({
    output: join(root, 'stage'),
    serviceName: 'related-fixture',
    installationId: installation,
  });
  await capture.observe('private research content', {}, async () => 'private result');
  const closed = await capture.shutdown();
  if (correlation) await injectCorrelation(closed.artifactPath, correlation);
  const bundle = JSON.parse(
    await readFile(join(closed.artifactPath, 'manifest.json'), 'utf8'),
  ).bundle_id as string;
  await rename(closed.artifactPath, join(output, tenant, installation, bundle));
  return `${tenant}/${installation}/${bundle}`;
}

async function injectCorrelation(
  artifactPath: string,
  correlation: Record<string, unknown>,
): Promise<void> {
  const tracePath = join(artifactPath, 'trace.jsonl');
  const manifestPath = join(artifactPath, 'manifest.json');
  const records = (await readFile(tracePath, 'utf8')).trim().split('\n').map((line) => JSON.parse(line));
  const start = records.find((record) => record.kind === 'run.start');
  start.data.correlation = correlation;
  const trace = `${records.map((record) => JSON.stringify(record)).join('\n')}\n`;
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
  manifest.trace.bytes = Buffer.byteLength(trace);
  manifest.trace.sha256 = createHash('sha256').update(trace).digest('hex');
  await writeFile(tracePath, trace, { mode: 0o600 });
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });
}

function streams(isTTY: boolean) {
  const output: string[] = [];
  const errors: string[] = [];
  return {
    value: { out: (text: string) => output.push(text), error: (text: string) => errors.push(text), isTTY },
    out: () => output.join(''),
    errors: () => errors.join(''),
  };
}

function storeWithPermissions(permissions: string[], denyList = false) {
  return {
    list: async () => {
      if (denyList) throw new Error('permission denied');
      return [];
    },
    readSmall: async () => undefined,
    download: async () => { throw new Error('unused'); },
    testPermissions: async () => permissions,
  };
}
