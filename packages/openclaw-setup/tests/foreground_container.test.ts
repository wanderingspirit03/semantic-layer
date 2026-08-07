import { spawn, type ChildProcess } from 'node:child_process';
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, describe, expect, it } from 'vitest';
import { REQUIRED_PLUGIN_HOOKS } from '../src/bin.js';

const fixtureRoot = join(dirname(fileURLToPath(import.meta.url)), 'fixtures');
const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(
    temporaryDirectories
      .splice(0)
      .map((path) => rm(path, { force: true, recursive: true })),
  );
});

describe('foreground container lifecycle', () => {
  it('resumes without another key after the Gateway exits zero and its supervisor terminates setup', async () => {
    const directory = await mkdtemp(
      join(tmpdir(), 'semantic-layer-openclaw-foreground-'),
    );
    temporaryDirectories.push(directory);
    const stateDirectory = join(directory, 'state');
    const configPath = join(stateDirectory, 'openclaw.json');
    const openClawBin = join(directory, 'openclaw-fixture.mjs');
    const pluginMarker = join(directory, 'plugin-installed');
    const installCountPath = join(directory, 'install-count');
    const setupPidPath = join(directory, 'setup.pid');
    const pluginInstallPath = join(
      stateDirectory,
      'npm',
      'node_modules',
      'semantic-layer-openclaw',
    );
    await mkdir(stateDirectory, { recursive: true });
    await writeFile(
      configPath,
      `${JSON.stringify({
        gateway: { auth: { token: 'preserved-token' }, bind: 'custom' },
        plugins: { entries: { latitude: { enabled: true } } },
      })}\n`,
    );
    await writeFile(
      openClawBin,
      openClawFixture(pluginInstallPath),
      { mode: 0o700 },
    );
    await chmod(openClawBin, 0o700);

    const environment = {
      ...process.env,
      ACTIVE_CONFIG_PATH: configPath,
      INSTALL_COUNT_PATH: installCountPath,
      NODE_OPTIONS: `--import=${join(fixtureRoot, 'fetch-shim.mjs')}`,
      OPENCLAW_BIN: openClawBin,
      OPENCLAW_CONFIG_PATH: configPath,
      OPENCLAW_STATE_DIR: stateDirectory,
      PLUGIN_MARKER: pluginMarker,
      SETUP_PID_PATH: setupPidPath,
    };
    const gateway = spawn(
      process.execPath,
      [join(fixtureRoot, 'foreground-gateway.mjs')],
      { env: environment, stdio: ['ignore', 'pipe', 'pipe'] },
    );
    await waitForText(gateway, 'READY');

    const setupArgs = [
      join(fixtureRoot, 'run-setup.mjs'),
      'setup',
      '--container',
      '--endpoint',
      'https://ingest.example.test',
      '--service-name',
      'foreground-customer',
      '--installation-id',
      'install_0123456789abcdef0123456789abcdef',
    ];
    const first = spawn(process.execPath, setupArgs, {
      env: environment,
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    await writeFile(setupPidPath, String(first.pid));
    const secret = 'sentinel-foreground-ingest-key';
    first.stdin?.end(`${secret}\n`);
    const [gatewayResult, firstResult] = await Promise.all([
      collect(gateway),
      collect(first),
    ]);

    expect(gatewayResult).toMatchObject({ code: 0, signal: null });
    expect(firstResult.signal).toBe('SIGTERM');
    expect(`${firstResult.stdout}\n${firstResult.stderr}`).not.toContain(secret);
    const credentialsPath = join(
      stateDirectory,
      'semantic-layer',
      'credentials.json',
    );
    const installationStatePath = join(
      stateDirectory,
      'semantic-layer',
      'installation.json',
    );
    expect((await lstat(credentialsPath)).mode & 0o077).toBe(0);
    expect((await lstat(installationStatePath)).mode & 0o077).toBe(0);
    const interruptedConfig = JSON.parse(
      await readFile(configPath, 'utf8'),
    ) as Record<string, any>;
    expect(interruptedConfig.gateway).toEqual({
      auth: { token: 'preserved-token' },
      bind: 'custom',
    });
    expect(interruptedConfig.plugins.entries.latitude).toEqual({
      enabled: true,
    });
    expect(
      interruptedConfig.plugins.entries['semantic-layer-openclaw'],
    ).toBeDefined();
    expect(
      interruptedConfig.plugins.installs['semantic-layer-openclaw'],
    ).toBeDefined();

    const resumed = spawn(process.execPath, setupArgs, {
      env: environment,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const resumedResult = await collect(resumed);
    expect(resumedResult).toMatchObject({ code: 0, signal: null });
    expect(resumedResult.stdout).toContain('OK. The complete config is ready.');
    expect(await readFile(installCountPath, 'utf8')).toBe('1');
  }, 20_000);
});

function openClawFixture(pluginInstallPath: string): string {
  const report = {
    plugin: {
      id: 'semantic-layer-openclaw',
      enabled: true,
      status: 'loaded',
      packageName: 'semantic-layer-openclaw',
      version: '0.1.0-pilot.4',
    },
    install: {
      resolvedName: 'semantic-layer-openclaw',
      resolvedVersion: '0.1.0-pilot.4',
      installPath: pluginInstallPath,
      source: 'npm',
      spec: 'semantic-layer-openclaw@0.1.0-pilot.4',
    },
    typedHooks: REQUIRED_PLUGIN_HOOKS.map((name) => ({ name })),
    diagnostics: [],
  };
  return [
    '#!/usr/bin/env node',
    "import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';",
    'const args = process.argv.slice(2);',
    'const get = (root, path) => path.split(".").reduce((value, key) => value && typeof value === "object" ? value[key] : undefined, root);',
    `const report = ${JSON.stringify(report)};`,
    "if (args[0] === '--version') process.stdout.write('OpenClaw 2026.5.5\\n');",
    "else if (args[0] === 'plugins' && args[1] === 'inspect') { if (!existsSync(process.env.PLUGIN_MARKER)) { process.stderr.write('Plugin not found: semantic-layer-openclaw\\n'); process.exitCode = 1; } else process.stdout.write(JSON.stringify(report)); }",
    "else if (args[0] === 'plugins' && args[1] === 'install') { const credentials = process.env.OPENCLAW_STATE_DIR + '/semantic-layer/credentials.json'; const state = process.env.OPENCLAW_STATE_DIR + '/semantic-layer/installation.json'; if (!existsSync(credentials) || !existsSync(state)) process.exit(25); mkdirSync(report.install.installPath, { recursive: true }); writeFileSync(process.env.PLUGIN_MARKER, 'installed'); const config = JSON.parse(readFileSync(process.env.OPENCLAW_CONFIG_PATH, 'utf8')); config.plugins ??= {}; config.plugins.entries ??= {}; config.plugins.installs ??= {}; config.plugins.entries['semantic-layer-openclaw'] = { enabled: true }; config.plugins.installs['semantic-layer-openclaw'] = report.install; writeFileSync(process.env.OPENCLAW_CONFIG_PATH, JSON.stringify(config)); const count = existsSync(process.env.INSTALL_COUNT_PATH) ? Number(readFileSync(process.env.INSTALL_COUNT_PATH, 'utf8')) : 0; writeFileSync(process.env.INSTALL_COUNT_PATH, String(count + 1)); }",
    "else if (args[0] === 'config' && args[1] === 'file') process.stdout.write(process.env.OPENCLAW_CONFIG_PATH + '\\n');",
    "else if (args[0] === 'config' && args[1] === 'get') { const config = JSON.parse(readFileSync(process.env.OPENCLAW_CONFIG_PATH, 'utf8')); let value = get(config, args[2]); if (args[2] === 'plugins.entries' && value) value = { ...value, 'memory-core': { config: {} } }; if (value === undefined) { process.stderr.write('Config path not found: ' + args[2] + '\\n'); process.exitCode = 1; } else process.stdout.write(JSON.stringify(value)); }",
    '',
  ].join('\n');
}

async function waitForText(child: ChildProcess, expected: string): Promise<void> {
  await new Promise<void>((resolveValue, reject) => {
    const timeout = setTimeout(() => reject(new Error('fixture readiness timed out')), 5_000);
    child.stdout?.on('data', (chunk: Buffer) => {
      if (!chunk.toString('utf8').includes(expected)) return;
      clearTimeout(timeout);
      resolveValue();
    });
    child.once('exit', (code) => {
      clearTimeout(timeout);
      reject(new Error(`fixture exited before readiness: ${String(code)}`));
    });
  });
}

async function collect(child: ChildProcess): Promise<{
  code: number | null;
  signal: NodeJS.Signals | null;
  stderr: string;
  stdout: string;
}> {
  let stdout = '';
  let stderr = '';
  child.stdout?.on('data', (chunk: Buffer) => {
    stdout += chunk.toString('utf8');
  });
  child.stderr?.on('data', (chunk: Buffer) => {
    stderr += chunk.toString('utf8');
  });
  return await new Promise((resolveValue, reject) => {
    const timeout = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new Error('child process timed out'));
    }, 15_000);
    child.once('error', reject);
    child.once('exit', (code, signal) => {
      clearTimeout(timeout);
      resolveValue({ code, signal, stderr, stdout });
    });
  });
}
