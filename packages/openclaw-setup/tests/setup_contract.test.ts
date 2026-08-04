import {
  chmod,
  lstat,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  commandTimeoutMs,
  configuredString,
  consumeSetupSecretEnvironment,
  createManagedSetupCredentials,
  createPluginConfig,
  doctorCommandPlan,
  doctorQualificationDescription,
  generateIdentityKey,
  generateInstallationId,
  hiddenQuestion,
  installationIdOption,
  installationConfigFailures,
  main,
  standaloneSpoolGatewayFailures,
  standaloneSpoolUploaderOptions,
  packageInstallSpec,
  pluginRuntimeFailures,
  REQUIRED_PLUGIN_HOOKS,
  runOpenClaw,
  securityAuditFailures,
  setupCommandPlan,
} from '../src/bin.js';

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((path) => rm(path, { force: true, recursive: true })),
  );
});

describe('client setup command contract', () => {
  it('uses the exact native npm package spec and one explicit restart', () => {
    const commands = setupCommandPlan(
      'npm:semantic-layer-openclaw@0.1.0-pilot.1',
      '/safe/patch.json',
    );
    expect(commands[0]).toEqual([
      'plugins',
      'install',
      'npm:semantic-layer-openclaw@0.1.0-pilot.1',
      '--pin',
    ]);
    expect(commands).toContainEqual([
      'config',
      'patch',
      '--file',
      '/safe/patch.json',
      '--dry-run',
    ]);
    expect(commands).toContainEqual([
      'config',
      'patch',
      '--file',
      '/safe/patch.json',
    ]);
    expect(commands).toContainEqual(['config', 'validate']);
    expect(commands).toContainEqual(['secrets', 'audit', '--check']);
    expect(commands).toContainEqual(['security', 'audit', '--json']);
    expect(
      commands.filter((args) => args[0] === 'gateway' && args[1] === 'restart'),
    ).toHaveLength(1);
  });

  it('does not apply npm-only pinning to a local qualification tarball', () => {
    expect(setupCommandPlan('/tmp/plugin.tgz', '/safe/patch.json')[0]).toEqual([
      'plugins',
      'install',
      '/tmp/plugin.tgz',
    ]);
  });

  it('allows native install and restart commands enough time on a fresh host', () => {
    expect(
      commandTimeoutMs([
        'plugins',
        'install',
        'npm:semantic-layer-openclaw@0.1.0-pilot.1',
      ]),
    ).toBe(300_000);
    expect(commandTimeoutMs(['gateway', 'restart'])).toBe(120_000);
    expect(commandTimeoutMs(['config', 'validate'])).toBe(30_000);
  });

  it('includes identity generation and health and security doctor gates', () => {
    expect(generateIdentityKey()).toMatch(/^[a-f0-9]{64}$/u);
    expect(doctorCommandPlan()).toEqual([
      ['config', 'validate'],
      ['secrets', 'audit', '--check'],
      ['security', 'audit', '--json'],
      ['gateway', 'status', '--deep', '--require-rpc'],
    ]);
  });

  it('describes capability checking only after the full doctor succeeds', () => {
    expect(doctorQualificationDescription('exact_qualified')).toBe(
      'exact-qualified',
    );
    expect(
      doctorQualificationDescription('capability_checked_unqualified'),
    ).toBe('capability-checked but unqualified');
  });

  it('preserves one assigned installation identity across setup reruns and key rotation', () => {
    const firstInstallationId = generateInstallationId();
    const otherInstallationId = generateInstallationId();
    const first = createManagedSetupCredentials(
      'ingest-one',
      undefined,
      'i'.repeat(64),
      firstInstallationId,
    );
    const otherVm = createManagedSetupCredentials(
      'ingest-other',
      undefined,
      'j'.repeat(64),
      otherInstallationId,
    );
    const rotated = createManagedSetupCredentials(
      'ingest-two',
      first,
      'k'.repeat(64),
    );

    expect(generateInstallationId()).toMatch(/^install_[a-f0-9]{32}$/u);
    expect(first.installationId).toBe(firstInstallationId);
    expect(otherVm.installationId).not.toBe(first.installationId);
    expect(rotated).toEqual({
      ingestKey: 'ingest-two',
      identityKey: first.identityKey,
      installationId: first.installationId,
    });
    const pluginConfig = createPluginConfig({
      endpoint: 'https://ingest.example.test',
      serviceName: 'customer-openclaw',
      outputDirectory: '/safe/traces',
      spoolDirectory: '/safe/spool',
      credentials: first,
    });
    expect(pluginConfig).toMatchObject({
      installationId: first.installationId,
      ingestKey: {
        source: 'file',
        provider: 'semantic_layer',
        id: '/ingestKey',
      },
      identityKey: {
        source: 'file',
        provider: 'semantic_layer',
        id: '/identityKey',
      },
    });
    expect(pluginConfig).not.toHaveProperty('richCapture');
    expect(() =>
      createManagedSetupCredentials('ingest-three', {
        ...first,
        installationId: 'customer-hostname',
      }),
    ).toThrow(/installation ID/u);
    expect(() =>
      createManagedSetupCredentials(
        'ingest-four',
        undefined,
        'm'.repeat(64),
      ),
    ).toThrow(/required/u);
  });

  it('uses an operator-provisioned installation identity and refuses to replace it on a rerun', () => {
    const requested = 'install_0123456789abcdef0123456789abcdef';
    const credentials = createManagedSetupCredentials(
      'ingest-one',
      undefined,
      'i'.repeat(64),
      requested,
    );

    expect(credentials.installationId).toBe(requested);
    expect(
      createManagedSetupCredentials(
        'ingest-two',
        credentials,
        undefined,
        requested,
      ),
    ).toMatchObject({ installationId: requested });
    expect(() =>
      createManagedSetupCredentials(
        'ingest-three',
        credentials,
        undefined,
        'install_fedcba9876543210fedcba9876543210',
      ),
    ).toThrow(/does not match/u);
  });

  it('accepts one valid installation ID option and rejects unsafe forms', () => {
    const requested = 'install_0123456789abcdef0123456789abcdef';
    expect(installationIdOption(undefined)).toBeUndefined();
    expect(installationIdOption(['--installation-id', requested])).toBe(
      requested,
    );
    expect(() => installationIdOption(['--installation-id'])).toThrow(
      /requires a value/u,
    );
    expect(() =>
      installationIdOption(['--installation-id', 'customer-vm-1']),
    ).toThrow(/install_/u);
    expect(() =>
      installationIdOption([
        '--installation-id',
        requested,
        '--installation-id',
        requested,
      ]),
    ).toThrow(/only once/u);
  });

  it('rejects a configured installation identity that differs from owner-only setup state', () => {
    const expected = 'install_0123456789abcdef0123456789abcdef';
    expect(
      installationConfigFailures(JSON.stringify(expected), expected),
    ).toEqual([]);
    expect(
      installationConfigFailures(
        JSON.stringify('install_fedcba9876543210fedcba9876543210'),
        expected,
      ),
    ).toEqual([expect.stringContaining('does not match')]);
    expect(
      installationConfigFailures(JSON.stringify('customer-hostname'), expected),
    ).toEqual([expect.stringContaining('valid random installation ID')]);
  });

  it('allows standalone spool ownership only while the Gateway is stopped', () => {
    expect(
      standaloneSpoolGatewayFailures(
        JSON.stringify({
          service: { runtime: { status: 'stopped' } },
        }),
      ),
    ).toEqual([]);
    expect(
      standaloneSpoolGatewayFailures(
        JSON.stringify({
          service: { runtime: { status: 'running' } },
        }),
      ),
    ).toEqual([expect.stringContaining('Stop the OpenClaw Gateway')]);
    expect(standaloneSpoolGatewayFailures('not-json')).toEqual([
      expect.stringContaining('Stop the OpenClaw Gateway'),
    ]);
  });

  it('uses the stable installation identity for standalone spool ownership and bundle scope', () => {
    expect(
      standaloneSpoolUploaderOptions(
        {
          ingestKey: 'ingest-secret',
          identityKey: 'identity-secret',
          installationId: 'install_0123456789abcdef0123456789abcdef',
        },
        'https://ingest.example.test',
        '/safe/spool',
      ),
    ).toEqual({
      endpoint: 'https://ingest.example.test',
      spoolDirectory: '/safe/spool',
      ingestKey: 'ingest-secret',
      installationId: 'install_0123456789abcdef0123456789abcdef',
    });
  });

  it('accepts only the exact public pilot or an absolute test tarball override', () => {
    expect(packageInstallSpec(undefined)).toBe(
      'npm:semantic-layer-openclaw@0.1.0-pilot.1',
    );
    expect(
      packageInstallSpec('npm:semantic-layer-openclaw@0.1.0-pilot.1'),
    ).toBe('npm:semantic-layer-openclaw@0.1.0-pilot.1');
    expect(packageInstallSpec('npm-pack:/tmp/plugin.tgz')).toBe(
      '/tmp/plugin.tgz',
    );
    expect(packageInstallSpec('npm-pack:relative.tgz')).toBeUndefined();
    expect(
      packageInstallSpec('git:https://malicious.example/repo'),
    ).toBeUndefined();
  });

  it('requires an enabled, loaded runtime plugin with every capture hook', () => {
    expect(REQUIRED_PLUGIN_HOOKS).toHaveLength(16);
    const healthyReport = JSON.stringify({
      plugin: {
        id: 'semantic-layer-openclaw',
        enabled: true,
        status: 'loaded',
      },
      typedHooks: REQUIRED_PLUGIN_HOOKS.map((name) => ({ name })),
      diagnostics: [{ level: 'warn', message: 'non-fatal qualification note' }],
    });
    expect(pluginRuntimeFailures(healthyReport)).toEqual([]);

    const disabledReport = JSON.stringify({
      plugin: {
        id: 'semantic-layer-openclaw',
        enabled: false,
        status: 'disabled',
      },
      typedHooks: REQUIRED_PLUGIN_HOOKS.slice(1).map((name) => ({ name })),
      diagnostics: [{ level: 'error', message: 'load failed' }],
    });
    expect(pluginRuntimeFailures(disabledReport)).toEqual(
      expect.arrayContaining([
        expect.stringContaining('enabled'),
        expect.stringContaining('loaded'),
        expect.stringContaining(REQUIRED_PLUGIN_HOOKS[0]),
        expect.stringContaining('load failed'),
      ]),
    );
  });

  it('rejects malformed runtime inspection output', () => {
    expect(pluginRuntimeFailures('not json')).toEqual([
      expect.stringContaining('valid JSON'),
    ]);
    expect(
      pluginRuntimeFailures(
        JSON.stringify({
          plugin: { enabled: true, status: 'loaded' },
          typedHooks: REQUIRED_PLUGIN_HOOKS.map((name) => ({ name })),
        }),
      ),
    ).toEqual([expect.stringContaining('diagnostics')]);
  });

  it('parses exact config-get strings and gates critical security findings', () => {
    expect(configuredString('"https://ingest.example"\n')).toBe(
      'https://ingest.example',
    );
    expect(configuredString('/var/lib/openclaw/spool\n')).toBe(
      '/var/lib/openclaw/spool',
    );
    expect(() => configuredString('{}')).toThrow(/string/u);
    expect(
      securityAuditFailures(
        JSON.stringify({ summary: { critical: 0, warn: 2, info: 1 } }),
      ),
    ).toEqual([]);
    expect(
      securityAuditFailures(
        JSON.stringify({ summary: { critical: 1, warn: 0, info: 0 } }),
      ),
    ).toEqual([expect.stringContaining('1 critical')]);
    expect(securityAuditFailures('not-json')).toEqual([
      expect.stringContaining('valid JSON'),
    ]);
  });

  it('consumes configured secrets and removes them from the source environment', () => {
    const environment = {
      SEMANTIC_LAYER_INGEST_KEY: 'ingest-secret',
      SEMANTIC_LAYER_IDENTITY_KEY: 'identity-secret',
      SAFE_VALUE: 'preserved',
    };
    expect(consumeSetupSecretEnvironment(environment)).toEqual({
      ingestKey: 'ingest-secret',
      identityKey: 'identity-secret',
    });
    expect(environment).toEqual({ SAFE_VALUE: 'preserved' });
  });

  it('pauses the terminal after reading a hidden ingestion key', async () => {
    const listeners = new Set<(chunk: Buffer) => void>();
    const rawModes: boolean[] = [];
    const writes: string[] = [];
    let paused = true;
    const input = {
      isTTY: true,
      setRawMode(enabled: boolean) {
        rawModes.push(enabled);
      },
      resume() {
        paused = false;
      },
      pause() {
        paused = true;
      },
      on(_event: 'data', listener: (chunk: Buffer) => void) {
        listeners.add(listener);
      },
      off(_event: 'data', listener: (chunk: Buffer) => void) {
        listeners.delete(listener);
      },
    };
    const output = {
      write(value: string) {
        writes.push(value);
      },
    };

    const answer = hiddenQuestion('Ingestion key (hidden): ', input, output);
    for (const listener of listeners) listener(Buffer.from('ingest-secret\r'));

    await expect(answer).resolves.toBe('ingest-secret');
    expect(paused).toBe(true);
    expect(rawModes).toEqual([true, false]);
    expect(writes.join('')).not.toContain('ingest-secret');
  });

  it('never passes setup secrets to an OpenClaw child process', async () => {
    const directory = await mkdtemp(
      join(tmpdir(), 'semantic-layer-openclaw-setup-'),
    );
    temporaryDirectories.push(directory);
    const executable = join(directory, 'openclaw-env-check');
    await writeFile(
      executable,
      [
        '#!/usr/bin/env node',
        'process.stdout.write(JSON.stringify({ ingest: process.env.SEMANTIC_LAYER_INGEST_KEY, identity: process.env.SEMANTIC_LAYER_IDENTITY_KEY, safe: process.env.SAFE_VALUE }));',
        '',
      ].join('\n'),
    );
    await chmod(executable, 0o700);

    const previous = {
      bin: process.env.OPENCLAW_BIN,
      ingest: process.env.SEMANTIC_LAYER_INGEST_KEY,
      identity: process.env.SEMANTIC_LAYER_IDENTITY_KEY,
      safe: process.env.SAFE_VALUE,
    };
    process.env.OPENCLAW_BIN = executable;
    process.env.SEMANTIC_LAYER_INGEST_KEY = 'ingest-secret';
    process.env.SEMANTIC_LAYER_IDENTITY_KEY = 'identity-secret';
    process.env.SAFE_VALUE = 'preserved';
    try {
      const result = runOpenClaw(['--version']);
      expect(result.ok).toBe(true);
      expect(JSON.parse(result.stdout)).toEqual({ safe: 'preserved' });
    } finally {
      restoreEnvironment('OPENCLAW_BIN', previous.bin);
      restoreEnvironment('SEMANTIC_LAYER_INGEST_KEY', previous.ingest);
      restoreEnvironment('SEMANTIC_LAYER_IDENTITY_KEY', previous.identity);
      restoreEnvironment('SAFE_VALUE', previous.safe);
    }
  });

  it('configures one fresh host with its assigned installation ID in one setup run', async () => {
    const directory = await mkdtemp(
      join(tmpdir(), 'semantic-layer-openclaw-setup-e2e-'),
    );
    temporaryDirectories.push(directory);
    const executable = join(directory, 'openclaw-fixture');
    const installationId = 'install_0123456789abcdef0123456789abcdef';
    const endpoint = 'https://ingest.example.test';
    const spoolDirectory = join(directory, 'semantic-layer', 'cloud-spool');
    const openClawConfigPath = join(directory, 'openclaw.json');
    const verifiedInstallationIds: string[] = [];
    const latitudeEntry = {
      enabled: true,
      config: { projectId: 'latitude-project', environment: 'staging' },
    };
    await writeFile(
      openClawConfigPath,
      JSON.stringify({ plugins: { entries: { latitude: latitudeEntry } } }),
    );
    await writeFile(
      executable,
      [
        '#!/usr/bin/env node',
        "import { readFileSync, writeFileSync } from 'node:fs';",
        "if (process.env.SEMANTIC_LAYER_INGEST_KEY || process.env.SEMANTIC_LAYER_IDENTITY_KEY) process.exit(23);",
        'const args = process.argv.slice(2);',
        'const merge = (left, right) => { for (const [key, value] of Object.entries(right)) { left[key] = value && typeof value === "object" && !Array.isArray(value) ? merge(left[key] && typeof left[key] === "object" && !Array.isArray(left[key]) ? left[key] : {}, value) : value; } return left; };',
        "if (args[0] === '--version') process.stdout.write('OpenClaw 2026.5.5\\n');",
        `else if (args[0] === 'plugins' && args[1] === 'inspect') process.stdout.write(${JSON.stringify(JSON.stringify({ plugin: { id: 'semantic-layer-openclaw', enabled: true, status: 'loaded' }, typedHooks: REQUIRED_PLUGIN_HOOKS.map((name) => ({ name })), diagnostics: [] }))});`,
        "else if (args[0] === 'security' && args[1] === 'audit') process.stdout.write(JSON.stringify({ summary: { critical: 0 } }));",
        "else if (args[0] === 'config' && args[1] === 'patch' && !args.includes('--dry-run')) { const patchPath = args[args.indexOf('--file') + 1]; const current = JSON.parse(readFileSync(process.env.OPENCLAW_CONFIG_PATH, 'utf8')); const patch = JSON.parse(readFileSync(patchPath, 'utf8')); writeFileSync(process.env.OPENCLAW_CONFIG_PATH, JSON.stringify(merge(current, patch))); }",
        `else if (args[0] === 'config' && args[1] === 'get' && args[2] === 'gateway.bind') process.stdout.write('loopback\\n');`,
        `else if (args[0] === 'config' && args[1] === 'get' && args[2].endsWith('.config.endpoint')) process.stdout.write(${JSON.stringify(JSON.stringify(endpoint))});`,
        `else if (args[0] === 'config' && args[1] === 'get' && args[2].endsWith('.config.spoolDirectory')) process.stdout.write(${JSON.stringify(JSON.stringify(spoolDirectory))});`,
        `else if (args[0] === 'config' && args[1] === 'get' && args[2].endsWith('.config.installationId')) process.stdout.write(${JSON.stringify(JSON.stringify(installationId))});`,
        "else if (args[0] === 'config' && args[1] === 'get') process.stdout.write('true\\n');",
        '',
      ].join('\n'),
    );
    await chmod(executable, 0o700);

    const previous = {
      bin: process.env.OPENCLAW_BIN,
      state: process.env.OPENCLAW_STATE_DIR,
      config: process.env.OPENCLAW_CONFIG_PATH,
      ingest: process.env.SEMANTIC_LAYER_INGEST_KEY,
      identity: process.env.SEMANTIC_LAYER_IDENTITY_KEY,
      fetch: globalThis.fetch,
    };
    process.env.OPENCLAW_BIN = executable;
    process.env.OPENCLAW_STATE_DIR = directory;
    process.env.OPENCLAW_CONFIG_PATH = openClawConfigPath;
    process.env.SEMANTIC_LAYER_INGEST_KEY = 'ingest-secret';
    process.env.SEMANTIC_LAYER_IDENTITY_KEY = 'i'.repeat(64);
    globalThis.fetch = async (input, init) => {
      const url = String(input);
      if (url.endsWith('/health')) return new Response('{"status":"ok"}');
      if (url.endsWith('/v1/auth/verify')) {
        const body = JSON.parse(String(init?.body)) as {
          installation_id?: string;
        };
        if (body.installation_id) {
          verifiedInstallationIds.push(body.installation_id);
        }
        return new Response('{"status":"ok"}');
      }
      return new Response('{"error":"NOT_FOUND"}', { status: 404 });
    };
    try {
      await expect(
        main([
          'setup',
          '--endpoint',
          endpoint,
          '--service-name',
          'customer-openclaw-vm-01',
          '--installation-id',
          installationId,
        ]),
      ).resolves.toBe(0);
      const credentialPath = join(
        directory,
        'semantic-layer',
        'credentials.json',
      );
      expect(JSON.parse(await readFile(credentialPath, 'utf8'))).toEqual({
        ingestKey: 'ingest-secret',
        identityKey: 'i'.repeat(64),
        installationId,
      });
      expect((await lstat(credentialPath)).mode & 0o077).toBe(0);
      expect(verifiedInstallationIds).toEqual([
        installationId,
        installationId,
      ]);
      const openClawConfig = JSON.parse(
        await readFile(openClawConfigPath, 'utf8'),
      ) as {
        plugins: { entries: Record<string, unknown> };
      };
      expect(openClawConfig.plugins.entries.latitude).toEqual(latitudeEntry);
      expect(openClawConfig.plugins.entries).toHaveProperty(
        'semantic-layer-openclaw',
      );
      await expect(
        lstat(
          join(directory, 'semantic-layer', 'openclaw-config-patch.json'),
        ),
      ).rejects.toMatchObject({ code: 'ENOENT' });
    } finally {
      restoreEnvironment('OPENCLAW_BIN', previous.bin);
      restoreEnvironment('OPENCLAW_STATE_DIR', previous.state);
      restoreEnvironment('OPENCLAW_CONFIG_PATH', previous.config);
      restoreEnvironment('SEMANTIC_LAYER_INGEST_KEY', previous.ingest);
      restoreEnvironment('SEMANTIC_LAYER_IDENTITY_KEY', previous.identity);
      globalThis.fetch = previous.fetch;
    }
  });
});

function restoreEnvironment(name: string, value: string | undefined): void {
  if (value === undefined) delete process.env[name];
  else process.env[name] = value;
}
