import { chmod, copyFile, mkdir, mkdtemp, readFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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

    for (const command of ['configure', 'doctor', 'tenants', 'installations', 'sync', 'list', 'show']) {
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
