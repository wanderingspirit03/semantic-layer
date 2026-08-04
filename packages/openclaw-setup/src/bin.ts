#!/usr/bin/env node
import { randomBytes } from 'node:crypto';
import { realpathSync } from 'node:fs';
import {
  chmod,
  lstat,
  mkdir,
  readFile,
  rename,
  rm,
  writeFile,
} from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createInterface } from 'node:readline/promises';
import { stdin, stdout } from 'node:process';
import { fileURLToPath } from 'node:url';
import { createCloudUploader } from 'semantic-layer-cloud';
import { checkCompatibility, type CompatibilityResult } from './preflight.js';

const PLUGIN_ID = 'semantic-layer-openclaw';
const PACKAGE_SPEC = 'npm:semantic-layer-openclaw@0.1.0-pilot.1';
const SECRET_ENVIRONMENT_KEYS = [
  'SEMANTIC_LAYER_INGEST_KEY',
  'SEMANTIC_LAYER_IDENTITY_KEY',
] as const;
export const REQUIRED_PLUGIN_HOOKS = [
  'before_model_resolve',
  'before_prompt_build',
  'agent_end',
  'model_call_started',
  'model_call_ended',
  'llm_input',
  'llm_output',
  'before_message_write',
  'before_tool_call',
  'after_tool_call',
  'message_received',
  'message_sent',
  'subagent_spawned',
  'subagent_ended',
  'gateway_start',
  'gateway_stop',
] as const;

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const command = argv[0] ?? 'help';
  if (command === 'help' || command === '--help' || command === '-h') {
    stdout.write(helpText());
    return 0;
  }
  if (command === 'doctor') return doctor();
  if (command === 'status') return cloudSpoolCommand(false);
  if (command === 'drain') return cloudSpoolCommand(true);
  if (command === 'rotate-key') return rotateIngestionKey();
  if (command === 'dry-run') return setup({ dryRun: true });
  if (command === 'setup')
    return setup({ dryRun: argv.includes('--dry-run'), args: argv.slice(1) });
  process.stderr.write(`Unknown command: ${command}\n\n${helpText()}`);
  return 2;
}

async function doctor(): Promise<number> {
  const failures: string[] = [];
  let credentials: SetupCredentials | undefined;
  let endpoint: string | undefined;
  let qualification: CompatibilityResult['qualification'] = 'unknown';
  const host = runOpenClaw(['--version']);
  if (!host.ok)
    failures.push(`OpenClaw CLI unavailable: ${host.stderr || host.stdout}`);
  const hostVersion = firstVersion(host.stdout);
  if (host.ok && !hostVersion)
    failures.push('Could not parse OpenClaw version.');
  if (hostVersion) {
    const compatibility = checkCompatibility(
      hostVersion,
      process.versions.node,
    );
    qualification = compatibility.qualification;
    for (const warning of compatibility.warnings)
      stdout.write(`WARN ${warning}\n`);
    failures.push(...compatibility.errors);
  }

  const credentialPath = credentialsPath();
  try {
    const metadata = await lstat(credentialPath);
    if (!metadata.isFile() || metadata.isSymbolicLink())
      failures.push(`${credentialPath} must be a regular file, not a link.`);
    if ((metadata.mode & 0o077) !== 0)
      failures.push(`${credentialPath} must be owner-only (chmod 600).`);
    if (
      typeof process.getuid === 'function' &&
      metadata.uid !== process.getuid()
    )
      failures.push(`${credentialPath} must be owned by the current user.`);
    const parsed = JSON.parse(await readFile(credentialPath, 'utf8')) as Record<
      string,
      unknown
    >;
    if (
      typeof parsed.ingestKey !== 'string' ||
      typeof parsed.identityKey !== 'string' ||
      typeof parsed.installationId !== 'string' ||
      !isInstallationId(parsed.installationId)
    ) {
      failures.push(
        `${credentialPath} is missing ingestKey, identityKey, or a valid installationId.`,
      );
    } else
      credentials = {
        ingestKey: parsed.ingestKey,
        identityKey: parsed.identityKey,
        installationId: parsed.installationId,
      };
  } catch {
    failures.push(`${credentialPath} is unavailable; run setup.`);
  }

  if (host.ok) {
    const inspect = runOpenClaw([
      'plugins',
      'inspect',
      PLUGIN_ID,
      '--runtime',
      '--json',
    ]);
    if (!inspect.ok)
      failures.push(
        `Plugin runtime inspection failed: ${inspect.stderr || inspect.stdout}`,
      );
    else failures.push(...pluginRuntimeFailures(inspect.stdout));
    for (const args of doctorCommandPlan()) {
      const result = runOpenClaw(args);
      if (!result.ok)
        failures.push(
          `OpenClaw ${args.join(' ')} failed: ${result.stderr || result.stdout}`,
        );
      else if (args[0] === 'security' && args[1] === 'audit')
        failures.push(...securityAuditFailures(result.stdout));
    }
    const bind = runOpenClaw(['config', 'get', 'gateway.bind']);
    if (!bind.ok || !/loopback/u.test(bind.stdout))
      failures.push('Gateway bind must be loopback.');
    const endpointResult = runOpenClaw([
      'config',
      'get',
      `plugins.entries.${PLUGIN_ID}.config.endpoint`,
    ]);
    if (!endpointResult.ok) {
      failures.push(
        `Plugin endpoint config lookup failed: ${endpointResult.stderr || endpointResult.stdout}`,
      );
    } else {
      try {
        endpoint = configuredString(endpointResult.stdout);
      } catch (error) {
        failures.push(error instanceof Error ? error.message : String(error));
      }
    }
    const spoolResult = runOpenClaw([
      'config',
      'get',
      `plugins.entries.${PLUGIN_ID}.config.spoolDirectory`,
    ]);
    if (!spoolResult.ok)
      failures.push(
        `Plugin spool config lookup failed: ${spoolResult.stderr || spoolResult.stdout}`,
      );
    else {
      try {
        configuredString(spoolResult.stdout);
      } catch (error) {
        failures.push(error instanceof Error ? error.message : String(error));
      }
    }
    const installationResult = runOpenClaw([
      'config',
      'get',
      `plugins.entries.${PLUGIN_ID}.config.installationId`,
    ]);
    if (!installationResult.ok) {
      failures.push(
        `Plugin installation ID config lookup failed: ${installationResult.stderr || installationResult.stdout}`,
      );
    } else if (credentials) {
      failures.push(
        ...installationConfigFailures(
          installationResult.stdout,
          credentials.installationId,
        ),
      );
    }
    const requiredBooleanSettings: ReadonlyArray<readonly [string, string]> = [
      [`plugins.entries.${PLUGIN_ID}.enabled`, 'plugin enabled'],
      [
        `plugins.entries.${PLUGIN_ID}.hooks.allowConversationAccess`,
        'rich conversation access',
      ],
    ];
    for (const [path, label] of requiredBooleanSettings) {
      const result = runOpenClaw(['config', 'get', path]);
      if (!result.ok || result.stdout.trim() !== 'true')
        failures.push(`Effective ${label} configuration must be true.`);
    }
    if (endpoint) {
      const health = await cloudHealth(endpoint);
      if (!health.ok)
        failures.push(`Cloud ingest health failed: ${health.error}`);
    }
  }
  if (endpoint && credentials) {
    const authentication = await cloudAuthentication(
      endpoint,
      credentials.ingestKey,
      credentials.installationId,
    );
    if (!authentication.ok)
      failures.push(
        `Cloud ingest authentication failed: ${authentication.error}`,
      );
  }
  if (failures.length > 0) {
    for (const failure of failures) process.stderr.write(`FAIL ${failure}\n`);
    return 1;
  }
  const completedQualification: CompatibilityResult['qualification'] =
    qualification === 'exact_qualified'
      ? 'exact_qualified'
      : 'capability_checked_unqualified';
  stdout.write(
    `OK OpenClaw ${hostVersion}; Node ${process.versions.node}; ${doctorQualificationDescription(completedQualification)} host; plugin runtime and owner-only credentials verified.\n`,
  );
  return 0;
}

export function doctorQualificationDescription(
  qualification: CompatibilityResult['qualification'],
): 'exact-qualified' | 'capability-checked but unqualified' | 'unknown' {
  if (qualification === 'exact_qualified') return 'exact-qualified';
  if (qualification === 'capability_checked_unqualified') {
    return 'capability-checked but unqualified';
  }
  return 'unknown';
}

async function setup(options: {
  dryRun: boolean;
  args?: string[];
}): Promise<number> {
  const host = runOpenClaw(['--version']);
  if (!host.ok) {
    process.stderr.write(
      `OpenClaw CLI unavailable: ${host.stderr || host.stdout}\n`,
    );
    return 1;
  }
  const hostVersion = firstVersion(host.stdout);
  if (!hostVersion) {
    process.stderr.write('Could not parse OpenClaw version.\n');
    return 1;
  }
  const compatibility = checkCompatibility(hostVersion, process.versions.node);
  for (const warning of compatibility.warnings)
    stdout.write(`WARN ${warning}\n`);
  if (!compatibility.ok) {
    for (const error of compatibility.errors)
      process.stderr.write(`FAIL ${error}\n`);
    return 1;
  }

  const packageSpec = packageInstallSpec(
    process.env.SEMANTIC_LAYER_OPENCLAW_PACKAGE_SPEC,
  );
  if (!packageSpec) {
    process.stderr.write(
      'SEMANTIC_LAYER_OPENCLAW_PACKAGE_SPEC must be the exact public pilot or npm-pack:/absolute/path.tgz.\n',
    );
    return 1;
  }
  let requestedInstallationId: string | undefined;
  try {
    requestedInstallationId = installationIdOption(options.args);
  } catch (error) {
    process.stderr.write(
      `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return 2;
  }
  if (options.dryRun) {
    stdout.write(
      [
        `Would install ${packageSpec} with the native OpenClaw plugin installer.`,
        'Would create an owner-only JSON credential file and configure file SecretRefs.',
        'Would enable rich conversation hooks, restart the Gateway exactly once, then run doctor.',
        'No files or OpenClaw state were changed.',
        '',
      ].join('\n'),
    );
    return 0;
  }

  const prompt = createInterface({ input: stdin, output: stdout });
  try {
    const endpoint =
      optionValue(options.args, '--endpoint') ??
      (await prompt.question('HTTPS ingest endpoint: ')).trim();
    const serviceName =
      optionValue(options.args, '--service-name') ??
      (await prompt.question('Service name: ')).trim();
    const credentialPath = credentialsPath();
    let existingCredentials: SetupCredentials | undefined;
    try {
      await lstat(credentialPath);
      existingCredentials = await readSetupCredentials(false);
    } catch (error) {
      if ((error as { code?: string }).code !== 'ENOENT') {
        process.stderr.write(
          `Existing credentials are unsafe or unreadable: ${error instanceof Error ? error.message : String(error)}\n`,
        );
        return 1;
      }
    }
    if (!existingCredentials && !requestedInstallationId) {
      process.stderr.write(
        'FAIL First setup requires --installation-id with the ID assigned to this host.\n',
      );
      return 2;
    }
    const configuredSecrets = consumeSetupSecretEnvironment();
    if (!configuredSecrets.ingestKey) prompt.close();
    const ingestKey =
      configuredSecrets.ingestKey ??
      (await hiddenQuestion('Ingestion key (hidden): '));
    if (!ingestKey) {
      process.stderr.write(
        'An ingestion key is required and identity key must contain 32+ characters. Secrets are never accepted on command lines.\n',
      );
      return 1;
    }
    let credentials: SetupCredentials;
    try {
      credentials = createManagedSetupCredentials(
        ingestKey,
        existingCredentials,
        configuredSecrets.identityKey,
        requestedInstallationId,
      );
    } catch (error) {
      process.stderr.write(
        `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
      );
      return 1;
    }
    if (credentials.identityKey.length < 32) {
      process.stderr.write(
        'An ingestion key is required and identity key must contain 32+ characters. Secrets are never accepted on command lines.\n',
      );
      return 1;
    }
    if (!endpoint.startsWith('https://') || !serviceName) {
      process.stderr.write(
        'Endpoint must use HTTPS and service name must not be empty.\n',
      );
      return 1;
    }
    const health = await cloudHealth(endpoint);
    if (!health.ok) {
      process.stderr.write(
        `Cloud ingest health failed before setup: ${health.error}\n`,
      );
      return 1;
    }
    const authentication = await cloudAuthentication(
      endpoint,
      ingestKey,
      credentials.installationId,
    );
    if (!authentication.ok) {
      process.stderr.write(
        `Cloud ingest authentication failed before setup: ${authentication.error}\n`,
      );
      return 1;
    }

    const installCommand = pluginInstallCommand(packageSpec);
    const installResult = runOpenClaw(installCommand);
    if (!installResult.ok) {
      process.stderr.write(
        `OpenClaw command failed (${installCommand.join(' ')}): ${installResult.stderr || installResult.stdout}\n`,
      );
      return 1;
    }

    const stateDirectory = dirname(dirname(credentialPath));
    const outputDirectory = join(stateDirectory, 'semantic-layer', 'traces');
    const spoolDirectory = join(
      stateDirectory,
      'semantic-layer',
      'cloud-spool',
    );
    await mkdir(dirname(credentialPath), { recursive: true, mode: 0o700 });
    await mkdir(outputDirectory, { recursive: true, mode: 0o700 });
    await mkdir(spoolDirectory, { recursive: true, mode: 0o700 });
    await chmod(dirname(credentialPath), 0o700);
    await chmod(outputDirectory, 0o700);
    await chmod(spoolDirectory, 0o700);
    await writeCredentialsAtomically(credentials);

    const patchPath = join(
      dirname(credentialPath),
      'openclaw-config-patch.json',
    );
    const patch = createOpenClawConfigPatch({
      endpoint,
      serviceName,
      credentialPath,
      outputDirectory,
      spoolDirectory,
      credentials,
    });
    await writeFile(patchPath, `${JSON.stringify(patch, null, 2)}\n`, {
      mode: 0o600,
      flag: 'w',
    });
    await chmod(patchPath, 0o600);

    for (const args of setupCommandPlan(packageSpec, patchPath).slice(1)) {
      const result = runOpenClaw(args);
      if (!result.ok) {
        process.stderr.write(
          `OpenClaw command failed (${args.join(' ')}): ${result.stderr || result.stdout}\n`,
        );
        return 1;
      }
      if (args[0] === 'security' && args[1] === 'audit') {
        const securityFailures = securityAuditFailures(result.stdout);
        if (securityFailures.length > 0) {
          for (const failure of securityFailures)
            process.stderr.write(`FAIL ${failure}\n`);
          return 1;
        }
      }
      if (
        args[0] === 'config' &&
        args[1] === 'patch' &&
        !args.includes('--dry-run')
      ) {
        await rm(patchPath, { force: true });
      }
    }
    stdout.write(`Durable upload spool: ${spoolDirectory}\n`);
  } finally {
    prompt.close();
  }
  return doctor();
}

function createOpenClawConfigPatch(input: {
  endpoint: string;
  serviceName: string;
  credentialPath: string;
  outputDirectory: string;
  spoolDirectory: string;
  credentials: SetupCredentials;
}): Record<string, unknown> {
  return {
    secrets: {
      providers: {
        semantic_layer: {
          source: 'file',
          path: input.credentialPath,
          mode: 'json',
        },
      },
    },
    gateway: { mode: 'local', bind: 'loopback' },
    plugins: {
      entries: {
        [PLUGIN_ID]: {
          enabled: true,
          hooks: { allowConversationAccess: true, timeoutMs: 10_000 },
          config: createPluginConfig({
            endpoint: input.endpoint,
            serviceName: input.serviceName,
            outputDirectory: input.outputDirectory,
            spoolDirectory: input.spoolDirectory,
            credentials: input.credentials,
          }),
        },
      },
    },
  };
}

async function rotateIngestionKey(): Promise<number> {
  const previous = await readSetupCredentials().catch((error: unknown) => {
    process.stderr.write(
      `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return undefined;
  });
  if (!previous) return 1;
  const endpoint = (() => {
    try {
      return requiredConfiguredString(
        `plugins.entries.${PLUGIN_ID}.config.endpoint`,
      );
    } catch (error) {
      process.stderr.write(
        `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
      );
      return undefined;
    }
  })();
  if (!endpoint) return 1;
  const configured = consumeSetupSecretEnvironment();
  const nextKey =
    configured.ingestKey ??
    (await hiddenQuestion('New ingestion key (hidden): '));
  if (!nextKey) {
    process.stderr.write('FAIL A new ingestion key is required.\n');
    return 1;
  }
  const authentication = await cloudAuthentication(
    endpoint,
    nextKey,
    previous.installationId,
  );
  if (!authentication.ok) {
    process.stderr.write(
      `FAIL New cloud ingest authentication failed: ${authentication.error}\n`,
    );
    return 1;
  }
  await writeCredentialsAtomically(
    createManagedSetupCredentials(nextKey, previous),
  );
  const reload = runOpenClaw(['secrets', 'reload', '--json']);
  if (!reload.ok) {
    await writeCredentialsAtomically(previous);
    runOpenClaw(['secrets', 'reload', '--json']);
    process.stderr.write(
      `FAIL OpenClaw secrets reload failed; the previous key was restored: ${reload.stderr || reload.stdout}\n`,
    );
    return 1;
  }
  const result = await doctor();
  if (result !== 0) {
    await writeCredentialsAtomically(previous);
    runOpenClaw(['secrets', 'reload', '--json']);
    process.stderr.write(
      'FAIL Rotation verification failed; the previous key was restored.\n',
    );
    return 1;
  }
  stdout.write('OK ingestion key rotated; stable identity key preserved.\n');
  return 0;
}

export function runOpenClaw(args: string[]): {
  ok: boolean;
  stdout: string;
  stderr: string;
} {
  const result = spawnSync(process.env.OPENCLAW_BIN ?? 'openclaw', args, {
    encoding: 'utf8',
    env: openClawChildEnvironment(),
    shell: false,
    timeout: commandTimeoutMs(args),
  });
  return {
    ok: result.status === 0 && !result.error,
    stdout: result.stdout ?? '',
    stderr: result.error?.message ?? result.stderr ?? '',
  };
}

export function consumeSetupSecretEnvironment(
  environment: Record<string, string | undefined> = process.env,
): { ingestKey: string | undefined; identityKey: string | undefined } {
  const secrets = {
    ingestKey: environment.SEMANTIC_LAYER_INGEST_KEY,
    identityKey: environment.SEMANTIC_LAYER_IDENTITY_KEY,
  };
  for (const key of SECRET_ENVIRONMENT_KEYS) delete environment[key];
  return secrets;
}

function openClawChildEnvironment(): NodeJS.ProcessEnv {
  const environment = { ...process.env };
  for (const key of SECRET_ENVIRONMENT_KEYS) delete environment[key];
  return environment;
}

export function pluginRuntimeFailures(value: string): string[] {
  let report: unknown;
  try {
    report = JSON.parse(value);
  } catch {
    return ['Plugin runtime inspection did not return valid JSON.'];
  }
  if (!isRecord(report))
    return ['Plugin runtime inspection JSON must be an object.'];

  const failures: string[] = [];
  const plugin = isRecord(report.plugin) ? report.plugin : undefined;
  if (plugin?.enabled !== true)
    failures.push(
      'Plugin runtime inspection must report the plugin as enabled.',
    );
  if (plugin?.status !== 'loaded')
    failures.push('Plugin runtime inspection must report status loaded.');

  const registeredHooks = new Set(
    Array.isArray(report.typedHooks)
      ? report.typedHooks
          .filter(isRecord)
          .map((entry) => entry.name)
          .filter((name): name is string => typeof name === 'string')
      : [],
  );
  const missingHooks = REQUIRED_PLUGIN_HOOKS.filter(
    (hook) => !registeredHooks.has(hook),
  );
  if (missingHooks.length > 0)
    failures.push(
      `Plugin runtime inspection is missing required hooks: ${missingHooks.join(', ')}.`,
    );

  if (!Array.isArray(report.diagnostics)) {
    failures.push('Plugin runtime inspection must include diagnostics.');
  }
  const fatalDiagnostics = Array.isArray(report.diagnostics)
    ? report.diagnostics.filter(
        (entry) =>
          isRecord(entry) &&
          (entry.level === 'error' || entry.level === 'fatal'),
      )
    : [];
  for (const diagnostic of fatalDiagnostics) {
    const message =
      typeof diagnostic.message === 'string'
        ? diagnostic.message
        : 'unknown runtime failure';
    failures.push(
      `Plugin runtime inspection reported a fatal diagnostic: ${message}`,
    );
  }
  return failures;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export type SetupCredentials = {
  ingestKey: string;
  identityKey: string;
  installationId: string;
};

type ExistingSetupCredentials = Omit<SetupCredentials, 'installationId'> & {
  installationId?: string;
};

export function createManagedSetupCredentials(
  ingestKey: string,
  existing?: ExistingSetupCredentials,
  requestedIdentityKey?: string,
  requestedInstallationId?: string,
): SetupCredentials {
  if (
    existing?.installationId !== undefined &&
    !isInstallationId(existing.installationId)
  ) {
    throw new TypeError(
      'Existing setup state contains an invalid installation ID.',
    );
  }
  if (
    requestedInstallationId !== undefined &&
    !isInstallationId(requestedInstallationId)
  ) {
    throw new TypeError(
      'Requested installation ID must use the install_ prefix followed by 32 lowercase hexadecimal characters.',
    );
  }
  if (
    existing?.installationId !== undefined &&
    requestedInstallationId !== undefined &&
    existing.installationId !== requestedInstallationId
  ) {
    throw new TypeError(
      'Requested installation ID does not match the existing setup state.',
    );
  }
  const installationId =
    existing?.installationId ?? requestedInstallationId;
  if (installationId === undefined) {
    throw new TypeError(
      'An assigned installation ID is required for first setup.',
    );
  }
  return {
    ingestKey,
    identityKey:
      existing?.identityKey ?? requestedIdentityKey ?? generateIdentityKey(),
    installationId,
  };
}

export function createPluginConfig(input: {
  endpoint: string;
  serviceName: string;
  outputDirectory: string;
  spoolDirectory: string;
  credentials: SetupCredentials;
}): Record<string, unknown> {
  return {
    endpoint: input.endpoint,
    serviceName: input.serviceName,
    outputDirectory: input.outputDirectory,
    spoolDirectory: input.spoolDirectory,
    installationId: input.credentials.installationId,
    ingestKey: { source: 'file', provider: 'semantic_layer', id: '/ingestKey' },
    identityKey: {
      source: 'file',
      provider: 'semantic_layer',
      id: '/identityKey',
    },
  };
}

export function configuredString(value: string): string {
  const trimmed = value.trim();
  if (!trimmed)
    throw new Error('OpenClaw config lookup returned an empty value.');
  try {
    const parsed = JSON.parse(trimmed) as unknown;
    if (typeof parsed !== 'string' || !parsed)
      throw new Error('OpenClaw config lookup must return a string.');
    return parsed;
  } catch (error) {
    if (error instanceof SyntaxError) return trimmed;
    throw error;
  }
}

export function installationConfigFailures(
  value: string,
  expected: string,
): string[] {
  let configured: string;
  try {
    configured = configuredString(value);
  } catch (error) {
    return [error instanceof Error ? error.message : String(error)];
  }
  if (!isInstallationId(configured)) {
    return [
      'OpenClaw plugin configuration must contain a valid random installation ID.',
    ];
  }
  return configured === expected
    ? []
    : [
        'OpenClaw plugin installation ID does not match owner-only setup state.',
      ];
}

export function securityAuditFailures(value: string): string[] {
  let report: unknown;
  try {
    report = JSON.parse(value) as unknown;
  } catch {
    return ['OpenClaw security audit did not return valid JSON.'];
  }
  if (
    !isRecord(report) ||
    !isRecord(report.summary) ||
    !Number.isInteger(report.summary.critical)
  ) {
    return ['OpenClaw security audit JSON is missing summary.critical.'];
  }
  const critical = report.summary.critical as number;
  return critical > 0
    ? [`OpenClaw security audit reported ${critical} critical finding(s).`]
    : [];
}

async function readSetupCredentials(
  requireInstallation = true,
): Promise<SetupCredentials> {
  const path = credentialsPath();
  const metadata = await lstat(path);
  if (!metadata.isFile() || metadata.isSymbolicLink())
    throw new Error(`${path} must be a regular file, not a link.`);
  if ((metadata.mode & 0o077) !== 0)
    throw new Error(`${path} must be owner-only (chmod 600).`);
  if (typeof process.getuid === 'function' && metadata.uid !== process.getuid())
    throw new Error(`${path} must be owned by the current user.`);
  const value = JSON.parse(await readFile(path, 'utf8')) as Record<
    string,
    unknown
  >;
  const installationMissing = value.installationId === undefined;
  const installationInvalid =
    typeof value.installationId === 'string'
      ? !isInstallationId(value.installationId)
      : !installationMissing;
  if (
    typeof value.ingestKey !== 'string' ||
    typeof value.identityKey !== 'string' ||
    installationInvalid ||
    (requireInstallation && installationMissing)
  ) {
    throw new Error(
      `${path} is missing ingestKey, identityKey, or a valid installationId.`,
    );
  }
  return {
    ingestKey: value.ingestKey,
    identityKey: value.identityKey,
    installationId:
      typeof value.installationId === 'string' &&
      isInstallationId(value.installationId)
        ? value.installationId
        : generateInstallationId(),
  };
}

async function writeCredentialsAtomically(
  value: SetupCredentials,
): Promise<void> {
  const path = credentialsPath();
  const temporaryPath = `${path}.next-${process.pid}-${Date.now()}`;
  await writeFile(temporaryPath, `${JSON.stringify(value)}\n`, {
    mode: 0o600,
    flag: 'wx',
  });
  await chmod(temporaryPath, 0o600);
  await rename(temporaryPath, path);
}

function requiredConfiguredString(path: string): string {
  const result = runOpenClaw(['config', 'get', path]);
  if (!result.ok)
    throw new Error(
      `OpenClaw config get ${path} failed: ${result.stderr || result.stdout}`,
    );
  return configuredString(result.stdout);
}

async function cloudSpoolCommand(drain: boolean): Promise<number> {
  let uploader: ReturnType<typeof createCloudUploader> | undefined;
  try {
    const gateway = runOpenClaw(['gateway', 'status', '--json']);
    const ownershipFailures = standaloneSpoolGatewayFailures(gateway.stdout);
    if (ownershipFailures.length > 0) throw new Error(ownershipFailures[0]);
    const credentials = await readSetupCredentials();
    const endpoint = requiredConfiguredString(
      `plugins.entries.${PLUGIN_ID}.config.endpoint`,
    );
    const spoolDirectory = requiredConfiguredString(
      `plugins.entries.${PLUGIN_ID}.config.spoolDirectory`,
    );
    uploader = createCloudUploader(
      standaloneSpoolUploaderOptions(credentials, endpoint, spoolDirectory),
    );
    const result = await uploader.flush({ deadlineMs: drain ? 10_000 : 0 });
    stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    return result.quarantineBundles > 0 ? 1 : 0;
  } catch (error) {
    process.stderr.write(
      `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return 1;
  } finally {
    if (uploader) await uploader.shutdown();
  }
}

export function standaloneSpoolGatewayFailures(report: string): string[] {
  const requirement =
    'Stop the OpenClaw Gateway before running standalone spool status/drain; the live Gateway owns the spool.';
  try {
    const parsed = JSON.parse(report) as Record<string, unknown>;
    const service = isRecord(parsed.service) ? parsed.service : undefined;
    const runtime = isRecord(service?.runtime) ? service.runtime : undefined;
    return runtime?.status === 'stopped' ? [] : [requirement];
  } catch {
    return [requirement];
  }
}

export function standaloneSpoolUploaderOptions(
  credentials: SetupCredentials,
  endpoint: string,
  spoolDirectory: string,
) {
  return {
    endpoint,
    spoolDirectory,
    ingestKey: credentials.ingestKey,
    installationId: credentials.installationId,
  };
}

export function commandTimeoutMs(args: string[]): number {
  if (args[0] === 'plugins' && args[1] === 'install') return 300_000;
  if (args[0] === 'gateway' && args[1] === 'restart') return 120_000;
  if (args[0] === 'security' && args[1] === 'audit') return 120_000;
  return 30_000;
}

function firstVersion(value: string): string | undefined {
  return value.match(/\d{4}\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?/u)?.[0];
}

function credentialsPath(): string {
  const state = process.env.OPENCLAW_STATE_DIR
    ? resolve(process.env.OPENCLAW_STATE_DIR)
    : join(homedir(), '.openclaw');
  return join(state, 'semantic-layer', 'credentials.json');
}

function optionValue(
  args: string[] | undefined,
  name: string,
): string | undefined {
  const index = args?.indexOf(name) ?? -1;
  const value = index >= 0 ? args?.[index + 1] : undefined;
  return value && !value.startsWith('--') ? value.trim() : undefined;
}

export function installationIdOption(
  args: string[] | undefined,
): string | undefined {
  const matches = (args ?? []).reduce<number[]>((indexes, value, index) => {
    if (value === '--installation-id') indexes.push(index);
    return indexes;
  }, []);
  if (matches.length === 0) return undefined;
  if (matches.length > 1)
    throw new TypeError('--installation-id may be provided only once.');
  const index = matches[0];
  if (index === undefined) return undefined;
  const value = args?.[index + 1];
  if (!value || value.startsWith('--'))
    throw new TypeError('--installation-id requires a value.');
  const trimmed = value.trim();
  if (!isInstallationId(trimmed)) {
    throw new TypeError(
      '--installation-id must use the install_ prefix followed by 32 lowercase hexadecimal characters.',
    );
  }
  return trimmed;
}

export function setupCommandPlan(
  packageSpec: string,
  patchPath: string,
): string[][] {
  return [
    pluginInstallCommand(packageSpec),
    ['config', 'patch', '--file', patchPath, '--dry-run'],
    ['config', 'patch', '--file', patchPath],
    ['config', 'validate'],
    ['secrets', 'audit', '--check'],
    ['security', 'audit', '--json'],
    ['gateway', 'restart'],
  ];
}

function pluginInstallCommand(packageSpec: string): string[] {
  return packageSpec.startsWith('npm:')
    ? ['plugins', 'install', packageSpec, '--pin']
    : ['plugins', 'install', packageSpec];
}

export function doctorCommandPlan(): string[][] {
  return [
    ['config', 'validate'],
    ['secrets', 'audit', '--check'],
    ['security', 'audit', '--json'],
    ['gateway', 'status', '--deep', '--require-rpc'],
  ];
}

export function generateIdentityKey(): string {
  return randomBytes(32).toString('hex');
}

export function generateInstallationId(): string {
  return `install_${randomBytes(16).toString('hex')}`;
}

function isInstallationId(value: string): boolean {
  return /^install_[a-f0-9]{32}$/u.test(value);
}

export function packageInstallSpec(
  value: string | undefined,
): string | undefined {
  if (!value || value === PACKAGE_SPEC) return PACKAGE_SPEC;
  if (!value.startsWith('npm-pack:')) return undefined;
  const path = value.slice('npm-pack:'.length);
  return path.startsWith('/') && path.endsWith('.tgz') ? path : undefined;
}

async function cloudHealth(
  endpoint: string,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const response = await fetch(`${endpoint.replace(/\/+$/u, '')}/health`, {
      signal: AbortSignal.timeout(10_000),
    });
    if (!response.ok) return { ok: false, error: `HTTP ${response.status}` };
    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

async function cloudAuthentication(
  endpoint: string,
  ingestKey: string,
  installationId: string,
): Promise<{ ok: boolean; error?: string }> {
  try {
    const response = await fetch(
      `${endpoint.replace(/\/+$/u, '')}/v1/auth/verify`,
      {
        method: 'POST',
        headers: {
          authorization: `Bearer ${ingestKey}`,
          'content-type': 'application/json',
        },
        body: JSON.stringify({ installation_id: installationId }),
        signal: AbortSignal.timeout(10_000),
      },
    );
    if (response.ok) {
      const value = (await response.json()) as unknown;
      if (
        value &&
        typeof value === 'object' &&
        !Array.isArray(value) &&
        (value as Record<string, unknown>).status === 'ok'
      ) {
        return { ok: true };
      }
      return { ok: false, error: 'verification response was malformed' };
    }
    return {
      ok: false,
      error: `ingestion key and installation ID verification returned HTTP ${response.status}`,
    };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

type HiddenQuestionInput = {
  isTTY?: boolean;
  setRawMode?: (enabled: boolean) => unknown;
  resume: () => unknown;
  pause: () => unknown;
  on: (event: 'data', listener: (chunk: Buffer) => void) => unknown;
  off: (event: 'data', listener: (chunk: Buffer) => void) => unknown;
};

type HiddenQuestionOutput = {
  write: (value: string) => unknown;
};

export async function hiddenQuestion(
  label: string,
  input: HiddenQuestionInput = stdin,
  output: HiddenQuestionOutput = stdout,
): Promise<string> {
  if (!input.isTTY || typeof input.setRawMode !== 'function') {
    const prompt = createInterface({ input: stdin, output: stdout });
    try {
      return (await prompt.question(label)).trim();
    } finally {
      prompt.close();
    }
  }
  output.write(label);
  input.setRawMode(true);
  input.resume();
  return await new Promise<string>((resolveValue, reject) => {
    let value = '';
    const cleanup = () => {
      input.off('data', onData);
      input.setRawMode?.(false);
      input.pause();
      output.write('\n');
    };
    const onData = (chunk: Buffer) => {
      for (const byte of chunk) {
        if (byte === 3) {
          cleanup();
          reject(new Error('setup cancelled'));
          return;
        }
        if (byte === 10 || byte === 13) {
          cleanup();
          resolveValue(value.trim());
          return;
        }
        if (byte === 127 || byte === 8) value = value.slice(0, -1);
        else value += String.fromCharCode(byte);
      }
    };
    input.on('data', onData);
  });
}

function helpText(): string {
  return `semantic-layer-openclaw-setup\n\nUsage:\n  semantic-layer-openclaw-setup setup [--endpoint URL] [--service-name NAME] [--installation-id ID] [--dry-run]\n  semantic-layer-openclaw-setup dry-run\n  semantic-layer-openclaw-setup doctor\n  semantic-layer-openclaw-setup status\n  semantic-layer-openclaw-setup drain\n  semantic-layer-openclaw-setup rotate-key\n  semantic-layer-openclaw-setup --help\n\nFirst setup requires the installation ID assigned to that host. Setup and rotate-key ask for an ingestion key in a hidden prompt. Secrets are never accepted on command lines. Each managed host must use its assigned installation ID and ingestion key. Run doctor while the Gateway is live. Stop the Gateway before standalone status or drain. Drain stops after 10 seconds.\n`;
}

if (
  process.argv[1] &&
  realpathSync(fileURLToPath(import.meta.url)) === realpathSync(process.argv[1])
) {
  process.exitCode = await main();
}
