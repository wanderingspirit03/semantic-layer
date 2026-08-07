#!/usr/bin/env node
import { randomBytes } from 'node:crypto';
import { realpathSync } from 'node:fs';
import {
  chmod,
  lstat,
  mkdir,
  open,
  readFile,
  realpath,
  readdir,
  rename,
  rm,
  rmdir,
  writeFile,
} from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { spawnSync } from 'node:child_process';
import { createInterface } from 'node:readline/promises';
import { stdin, stdout } from 'node:process';
import { fileURLToPath } from 'node:url';
import JSON5 from 'json5';
import { createCloudUploader } from 'semantic-layer-cloud';
import { checkCompatibility, type CompatibilityResult } from './preflight.js';

const PLUGIN_ID = 'semantic-layer-openclaw';
const PLUGIN_PACKAGE = 'semantic-layer-openclaw';
const PLUGIN_VERSION = '0.1.0-pilot.4';
const PACKAGE_SPEC = `npm:${PLUGIN_PACKAGE}@${PLUGIN_VERSION}`;
const DEFAULT_OPENCLAW_SPOOL_BYTES = 1024 * 1024 * 1024;
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

export async function main(
  argv = process.argv.slice(2),
  testOptions: {
    readSetupIngestKey?: () => Promise<string>;
    readRotateIngestKey?: () => Promise<string>;
  } = {},
): Promise<number> {
  const command = argv[0] ?? 'help';
  const args = argv.slice(1);
  try {
    validateCommandArguments(command, args);
  } catch (error) {
    process.stderr.write(
      `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return 2;
  }
  if (command === 'help' || command === '--help' || command === '-h') {
    stdout.write(helpText());
    return 0;
  }
  if (command === 'doctor') {
    const gatewayUrl = optionValue(args, '--gateway-url');
    return doctor({
      container: args.includes('--container'),
      ...(gatewayUrl ? { gatewayUrl } : {}),
    });
  }
  if (command === 'status') return cloudSpoolCommand(false);
  if (command === 'drain') return cloudSpoolCommand(true);
  if (command === 'rotate-key')
    return rotateIngestionKey(testOptions.readRotateIngestKey);
  if (command === 'uninstall') return uninstall(args);
  if (command === 'dry-run') return setup({ dryRun: true });
  if (command === 'setup')
    return setup({
      dryRun: argv.includes('--dry-run'),
      args: argv.slice(1),
      ...(testOptions.readSetupIngestKey
        ? { readIngestKey: testOptions.readSetupIngestKey }
        : {}),
    });
  process.stderr.write(`Unknown command: ${command}\n\n${helpText()}`);
  return 2;
}

async function doctor(options: {
  container: boolean;
  gatewayUrl?: string;
}): Promise<number> {
  const failures: string[] = [];
  let credentials: SetupCredentials | undefined;
  let installationState: InstallationState | undefined;
  let endpoint: string | undefined;
  let spoolDirectory: string | undefined;
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
  try {
    installationState = await readInstallationState();
  } catch {
    failures.push(`${installationStatePath()} is unavailable; run setup.`);
  }
  if (
    credentials &&
    installationState &&
    credentials.installationId !== installationState.installationId
  ) {
    failures.push(
      'Installation ID differs between credentials and owner-only setup state.',
    );
  }
  if (installationState && options.container && installationState.setupMode !== 'container')
    failures.push(
      `${installationStatePath()} does not describe a container installation; rerun setup --container.`,
    );
  if (installationState && !options.container && installationState.setupMode === 'container')
    failures.push('This installation requires doctor --container.');

  if (host.ok) {
    try {
      const installed = inspectInstalledPlugin();
      if (!installed)
        failures.push('Semantic Layer plugin package is not installed.');
      else if (
        installed.packageName !== PLUGIN_PACKAGE ||
        installed.version !== PLUGIN_VERSION
      )
        failures.push(
          `Installed plugin must be ${PLUGIN_PACKAGE}@${PLUGIN_VERSION}.`,
        );
      else {
        const pluginPathFailure = await installedPluginPathFailure(installed);
        if (pluginPathFailure) failures.push(pluginPathFailure);
      }
    } catch (error) {
      failures.push(error instanceof Error ? error.message : String(error));
    }
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
    for (const args of doctorCommandPlan(options.container)) {
      const result = runOpenClaw(args);
      if (!result.ok)
        failures.push(
          `OpenClaw ${args.join(' ')} failed: ${result.stderr || result.stdout}`,
        );
      else if (args[0] === 'security' && args[1] === 'audit')
        failures.push(...securityAuditFailures(result.stdout));
    }
    if (options.container) {
      const gatewayFailure = await containerGatewayHealthFailure(
        options.gatewayUrl,
      );
      if (gatewayFailure) failures.push(gatewayFailure);
    }
    if (!options.container) {
      const bind = runOpenClaw(['config', 'get', 'gateway.bind']);
      if (!bind.ok || !/loopback/u.test(bind.stdout))
        failures.push('Gateway bind must be loopback.');
    }
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
        if (installationState && endpoint !== installationState.endpoint)
          failures.push(
            'Plugin endpoint does not match owner-only setup state.',
          );
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
        spoolDirectory = configuredString(spoolResult.stdout);
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
      if (
        installationState &&
        credentials.installationId !== installationState.installationId
      )
        failures.push(
          'Plugin installation ID does not match owner-only installation state.',
        );
    }
    const serviceNameResult = runOpenClaw([
      'config',
      'get',
      `plugins.entries.${PLUGIN_ID}.config.serviceName`,
    ]);
    if (!serviceNameResult.ok) {
      failures.push(
        `Plugin service name lookup failed: ${serviceNameResult.stderr || serviceNameResult.stdout}`,
      );
    } else if (installationState) {
      try {
        if (
          configuredString(serviceNameResult.stdout) !==
          installationState.serviceName
        )
          failures.push(
            'Plugin service name does not match owner-only setup state.',
          );
      } catch (error) {
        failures.push(error instanceof Error ? error.message : String(error));
      }
    }
    const spoolLimitResult = runOpenClaw([
      'config',
      'get',
      `plugins.entries.${PLUGIN_ID}.config.maxSpoolBytes`,
      '--json',
    ]);
    if (
      !spoolLimitResult.ok ||
      Number.parseInt(spoolLimitResult.stdout.trim(), 10) !==
        DEFAULT_OPENCLAW_SPOOL_BYTES
    ) {
      failures.push(
        `Plugin spool limit must be ${DEFAULT_OPENCLAW_SPOOL_BYTES} bytes.`,
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
    for (const pluginId of installationState?.preservedEnabledPluginIds ?? []) {
      const result = runOpenClaw([
        'config',
        'get',
        `plugins.entries.${pluginId}.enabled`,
      ]);
      if (!result.ok || result.stdout.trim() !== 'true')
        failures.push(
          `Plugin ${pluginId} was enabled before Semantic Layer setup and must remain enabled.`,
        );
    }
    if (endpoint) {
      const health = await cloudHealth(endpoint);
      if (!health.ok)
        failures.push(`Cloud ingest health failed: ${health.error}`);
    }
  }
  if (options.container && spoolDirectory) {
    const spool = await inspectSpoolDirectory(spoolDirectory);
    if (!spool.ok) failures.push(`Upload spool check failed: ${spool.error}`);
    else {
      stdout.write(
        `Upload spool: ${spool.bytes} of ${DEFAULT_OPENCLAW_SPOOL_BYTES} bytes.\n`,
      );
      if (spool.bytes >= DEFAULT_OPENCLAW_SPOOL_BYTES)
        failures.push('Upload spool is full.');
      else if (spool.bytes >= DEFAULT_OPENCLAW_SPOOL_BYTES * 0.7)
        stdout.write('WARN Upload spool is at least 70 percent full.\n');
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
    `OK OpenClaw ${hostVersion}; Node ${process.versions.node}; ${doctorQualificationDescription(completedQualification)} host; plugin runtime and owner-only credentials verified${options.container ? '; container Gateway healthy' : ''}.\n`,
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
  readIngestKey?: () => Promise<string>;
}): Promise<number> {
  const mode: SetupMode = options.args?.includes('--container')
    ? 'container'
    : 'service';
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
        mode === 'container'
          ? 'Would stage the complete plugin config away from the live Gateway, then commit it. The foreground Gateway may exit after that final commit.'
          : 'Would enable rich conversation hooks, restart the Gateway exactly once, then run doctor.',
        'No files or OpenClaw state were changed.',
        '',
      ].join('\n'),
    );
    return 0;
  }

  let prompt: ReturnType<typeof createInterface> | undefined;
  const ask = async (label: string): Promise<string> => {
    prompt ??= createInterface({ input: stdin, output: stdout });
    return (await prompt.question(label)).trim();
  };
  try {
    const endpoint =
      optionValue(options.args, '--endpoint') ??
      (await ask('HTTPS ingest endpoint: '));
    const serviceName =
      optionValue(options.args, '--service-name') ??
      (await ask('Service name: '));
    const credentialPath = credentialsPath();
    let existingCredentials: SetupCredentials | undefined;
    let existingInstallationState: InstallationState | undefined;
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
    try {
      await lstat(installationStatePath());
      existingInstallationState = await readInstallationState();
    } catch (error) {
      if ((error as { code?: string }).code !== 'ENOENT') {
        process.stderr.write(
          `Existing installation state is unsafe or unreadable: ${error instanceof Error ? error.message : String(error)}\n`,
        );
        return 1;
      }
    }
    const assignedInstallationId =
      requestedInstallationId ??
      existingCredentials?.installationId ??
      existingInstallationState?.installationId;
    if (!assignedInstallationId) {
      process.stderr.write(
        'FAIL First setup requires --installation-id with the ID assigned to this host.\n',
      );
      return 2;
    }
    if (!endpoint.startsWith('https://') || !serviceName) {
      process.stderr.write(
        'Endpoint must use HTTPS and service name must not be empty.\n',
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
    let snapshot: ManagedSetupSnapshot;
    try {
      snapshot = managedSetupSnapshot();
    } catch {
      process.stderr.write(
        'FAIL Could not inspect the current OpenClaw setup state.\n',
      );
      return 1;
    }
    const requestedState = createInstallationState({
      endpoint,
      installationId: assignedInstallationId,
      preservedEnabledPluginIds:
        existingInstallationState?.preservedEnabledPluginIds ??
        enabledOtherPluginIds(snapshot.pluginEntries),
      serviceName,
      mode,
    });
    const ownershipFailures = managedOwnershipFailures({
      credentials: existingCredentials,
      installationState: existingInstallationState,
      requestedState,
      snapshot,
    });
    if (ownershipFailures.length > 0) {
      for (const failure of ownershipFailures)
        process.stderr.write(`FAIL ${failure}\n`);
      return 1;
    }
    prompt?.close();
    prompt = undefined;
    let ingestKey = existingCredentials?.ingestKey;
    if (!ingestKey) {
      try {
        ingestKey = await (options.readIngestKey ??
          (() => hiddenQuestion('Ingestion key (hidden): ')))();
      } catch {
        process.stderr.write('FAIL Could not read the hidden ingestion key.\n');
        return 1;
      }
      if (!ingestKey) {
        process.stderr.write(
          'An ingestion key is required and identity key must contain 32+ characters. Secrets are never accepted on command lines.\n',
        );
        return 1;
      }
    }
    let credentials: SetupCredentials;
    try {
      credentials = createManagedSetupCredentials(
        ingestKey,
        existingCredentials,
        undefined,
        assignedInstallationId,
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
    let configBackup: ConfigFileSnapshot;
    try {
      configBackup = await readActiveConfigFile();
      await removeStaleConfigFiles(dirname(configBackup.path));
    } catch (error) {
      process.stderr.write(
        `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
      );
      return 1;
    }
    const candidatePath = join(
      dirname(configBackup.path),
      `.semantic-layer-config-${process.pid}-${Date.now()}.json`,
    );
    const installCandidatePath = join(
      dirname(configBackup.path),
      `.semantic-layer-install-${process.pid}-${Date.now()}.json`,
    );

    const credentialBackup = existingCredentials
      ? await readFile(credentialPath)
      : undefined;
    const stateBackup = existingInstallationState
      ? await readFile(installationStatePath())
      : undefined;
    const createdDirectories: string[] = [];
    let installedThisRun = false;
    try {
      for (const directory of [
        dirname(credentialPath),
        outputDirectory,
        spoolDirectory,
      ]) {
        if (!(await pathExists(directory))) createdDirectories.push(directory);
        await mkdir(directory, { recursive: true, mode: 0o700 });
        await chmod(directory, 0o700);
      }
      await writeCredentialsAtomically(credentials);
      await writeInstallationStateAtomically(requestedState);

      let configBase = configBackup;
      let finalPlugin = snapshot.plugin;
      if (!snapshot.plugin) {
        await writeNewConfigFile(installCandidatePath, configBackup.contents);
        const installCommand = pluginInstallCommand(packageSpec);
        const installResult = runOpenClaw(installCommand, {
          OPENCLAW_CONFIG_PATH: installCandidatePath,
        });
        if (!installResult.ok)
          throw new Error(
            `OpenClaw command failed (${installCommand.join(' ')}): ${installResult.stderr || installResult.stdout}`,
          );
        installedThisRun = true;
        const installed = inspectInstalledPlugin({
          OPENCLAW_CONFIG_PATH: installCandidatePath,
        });
        if (
          !installed ||
          installed.packageName !== PLUGIN_PACKAGE ||
          installed.version !== PLUGIN_VERSION
        ) {
          throw new Error(
            `Installed plugin is not ${PLUGIN_PACKAGE}@${PLUGIN_VERSION}.`,
          );
        }
        finalPlugin = installed;
        configBase = await readConfigFile(installCandidatePath);
      }
      if (!finalPlugin)
        throw new Error('Semantic Layer plugin package is not installed.');
      const pluginPathFailure = await installedPluginPathFailure(finalPlugin);
      if (pluginPathFailure) throw new Error(pluginPathFailure);

      const patch = createOpenClawConfigPatch({
        endpoint,
        serviceName,
        credentialPath,
        outputDirectory,
        spoolDirectory,
        credentials,
        mode,
      });
      await writeManagedConfigCandidate(configBase, patch, candidatePath);

      const setupCommands = setupCommandPlan(packageSpec, mode).slice(1);
      for (const args of setupCommands.filter(
        (command) =>
          !(command[0] === 'gateway' && command[1] === 'restart'),
      )) {
        const result = runOpenClaw(args, {
          OPENCLAW_CONFIG_PATH: candidatePath,
        });
        if (!result.ok)
          throw new Error(
            `OpenClaw command failed (${args.join(' ')}): ${result.stderr || result.stdout}`,
          );
        if (args[0] === 'security' && args[1] === 'audit') {
          const securityFailures = securityAuditFailures(result.stdout);
          if (securityFailures.length > 0)
            throw new Error(securityFailures.join(' '));
        }
      }
      const candidateConfig = await readConfigFile(candidatePath);
      const preservedFailures = preservedFileConfigurationFailures(
        configBackup,
        candidateConfig,
        mode === 'container',
      );
      if (preservedFailures.length > 0)
        throw new Error(preservedFailures.join(' '));
      await installConfigCandidate(candidatePath, configBackup);
      if (mode === 'service') {
        const restart = runOpenClaw(['gateway', 'restart']);
        if (!restart.ok)
          throw new Error(
            `OpenClaw command failed (gateway restart): ${restart.stderr || restart.stdout}`,
          );
      }
      stdout.write(`Plugin: ${finalPlugin.installPath ?? 'installed'}\n`);
      stdout.write(`Credentials: ${credentialPath}\n`);
      stdout.write(`Local traces: ${outputDirectory}\n`);
      stdout.write(`Durable upload spool: ${spoolDirectory}\n`);
      stdout.write(
        `Upload spool limit: ${DEFAULT_OPENCLAW_SPOOL_BYTES} bytes\n`,
      );
    } catch (error) {
      const rollbackFailures = await rollbackManagedSetup({
        credentialBackup,
        configBackup,
        createdDirectories,
        installedThisRun,
        stateBackup,
      });
      process.stderr.write(
        `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
      );
      for (const failure of rollbackFailures)
        process.stderr.write(`FAIL Rollback: ${failure}\n`);
      return 1;
    } finally {
      await Promise.all([
        rm(candidatePath, { force: true }),
        rm(installCandidatePath, { force: true }),
      ]);
    }
  } finally {
    prompt?.close();
  }
  if (mode === 'container') {
    stdout.write(
      'OK. The complete config is ready. If the foreground Gateway stopped, restart its machine. Rerun this setup command to confirm completion, then run doctor --container.\n',
    );
    return 0;
  }
  return doctor({ container: false });
}

function createOpenClawConfigPatch(input: {
  endpoint: string;
  serviceName: string;
  credentialPath: string;
  outputDirectory: string;
  spoolDirectory: string;
  credentials: SetupCredentials;
  mode: SetupMode;
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
    ...(input.mode === 'service'
      ? { gateway: { mode: 'local', bind: 'loopback' } }
      : {}),
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

async function uninstall(args: string[]): Promise<number> {
  if (
    !args.includes('--container')
    || !args.includes('--acknowledge-external-restart')
  ) return 2;
  stdout.write(
    'IMPORTANT The foreground Gateway may exit during uninstall. Restart the owning container or machine from its external control plane after this command.\n',
  );
  const host = runOpenClaw(['--version']);
  if (!host.ok) {
    process.stderr.write(
      `FAIL OpenClaw CLI unavailable: ${host.stderr || host.stdout}\n`,
    );
    return 1;
  }
  let credentials: SetupCredentials | undefined;
  let installationState: InstallationState | undefined;
  let snapshot: ManagedSetupSnapshot;
  let configBackup: ConfigFileSnapshot;
  try {
    credentials = await readOptionalManagedFile(
      credentialsPath(),
      () => readSetupCredentials(),
    );
    installationState = await readOptionalManagedFile(
      installationStatePath(),
      readInstallationState,
    );
    if (!credentials && !installationState)
      throw new Error('No managed Semantic Layer installation state was found.');
    snapshot = managedSetupSnapshot();
    configBackup = await readActiveConfigFile();
    await removeStaleConfigFiles(dirname(configBackup.path));
  } catch (error) {
    process.stderr.write(
      `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return 1;
  }
  if (
    (installationState && installationState.setupMode !== 'container')
    || (
      installationState
      && credentials
      && installationState.installationId !== credentials.installationId
    )
  ) {
    process.stderr.write(
      'FAIL Owner-only setup state does not describe this container installation.\n',
    );
    return 1;
  }
  if (
    !installationState
    && (snapshot.plugin !== undefined || snapshot.pluginEntry.exists)
  ) {
    process.stderr.write(
      'FAIL Installation state is required before container uninstall can remove an active plugin. Rerun setup --container to restore it.\n',
    );
    return 1;
  }
  if (
    snapshot.plugin &&
    (snapshot.plugin.packageName !== PLUGIN_PACKAGE ||
      snapshot.plugin.version !== PLUGIN_VERSION)
  ) {
    process.stderr.write(
      'FAIL Installed plugin does not match the managed package and version.\n',
    );
    return 1;
  }
  const candidatePath = join(
    dirname(configBackup.path),
    `.semantic-layer-uninstall-${process.pid}-${Date.now()}.json`,
  );
  const operationPath = join(
    dirname(configBackup.path),
    `.semantic-layer-uninstall-operation-${process.pid}-${Date.now()}.json`,
  );
  let liveConfig: 'original' | 'clean' = 'original';
  let pluginState: 'installed' | 'removed' | 'unknown' = snapshot.plugin
    ? 'installed'
    : 'removed';
  try {
    await writeUninstallConfigCandidate(configBackup, candidatePath);
    const dryRun = runOpenClaw(['config', 'validate'], {
      OPENCLAW_CONFIG_PATH: candidatePath,
    });
    if (!dryRun.ok)
      throw new Error(
        `OpenClaw config cleanup validation failed: ${dryRun.stderr || dryRun.stdout}`,
      );
    if (snapshot.plugin) {
      await writeUninstallConfigCandidate(configBackup, operationPath);
      const uninstallDryRun = runOpenClaw(
        ['plugins', 'uninstall', PLUGIN_ID, '--dry-run'],
        { OPENCLAW_CONFIG_PATH: operationPath },
      );
      if (!uninstallDryRun.ok)
        throw new Error(
          `OpenClaw plugin uninstall dry-run failed: ${uninstallDryRun.stderr || uninstallDryRun.stdout}`,
        );
    }
    await installConfigCandidate(candidatePath, configBackup);
    liveConfig = 'clean';
    if (snapshot.plugin) {
      const uninstallResult = runOpenClaw(
        ['plugins', 'uninstall', PLUGIN_ID, '--force'],
        { OPENCLAW_CONFIG_PATH: operationPath },
      );
      if (!uninstallResult.ok) {
        try {
          pluginState = inspectInstalledPlugin() ? 'installed' : 'removed';
        } catch {
          pluginState = 'unknown';
        }
        throw new Error(
          `OpenClaw plugin uninstall failed: ${uninstallResult.stderr || uninstallResult.stdout}`,
        );
      }
      pluginState = 'removed';
    }
    const installedConfig = await readConfigFile(configBackup.path);
    const preservedFailures = preservedFileConfigurationFailures(
      configBackup,
      installedConfig,
    );
    if (preservedFailures.length > 0)
      throw new Error(preservedFailures.join(' '));
    await rm(credentialsPath(), { force: true });
    await rm(installationStatePath(), { force: true });
  } catch (error) {
    let restoreFailure = '';
    try {
      if (liveConfig === 'clean' && pluginState === 'installed') {
        await restoreConfigFile(configBackup);
        liveConfig = 'original';
      }
    } catch (restoreError) {
      restoreFailure = ` Config recovery failed: ${restoreError instanceof Error ? restoreError.message : String(restoreError)}`;
    }
    process.stderr.write(
      `FAIL ${error instanceof Error ? error.message : String(error)}${restoreFailure}\n`,
    );
    return 1;
  } finally {
    await Promise.all([
      rm(candidatePath, { force: true }),
      rm(operationPath, { force: true }),
    ]);
  }
  const stateDirectory = managedStateDirectory();
  const installationId = installationState?.installationId
    ?? credentials?.installationId;
  stdout.write('OK Semantic Layer was removed from OpenClaw.\n');
  stdout.write(`Revoke installation: ${installationId}\n`);
  stdout.write(
    `Preserved local traces: ${join(stateDirectory, 'semantic-layer', 'traces')}\n`,
  );
  stdout.write(
    `Preserved upload spool: ${join(stateDirectory, 'semantic-layer', 'cloud-spool')}\n`,
  );
  stdout.write(
    'Restart the owning container or machine now from its external control plane.\n',
  );
  return 0;
}

async function containerGatewayHealthFailure(
  gatewayUrl?: string,
): Promise<string | undefined> {
  let config: ConfigFileSnapshot;
  try {
    config = await readActiveConfigFile();
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
  const probePath = join(
    dirname(config.path),
    `.semantic-layer-doctor-${process.pid}-${Date.now()}.json`,
  );
  try {
    await removeStaleConfigFiles(dirname(config.path));
    const value = parseConfigObject(config);
    const gateway = ensureConfigObject(value, 'gateway', 'gateway');
    gateway.mode = 'local';
    if (gatewayUrl) gateway.port = gatewayPort(gatewayUrl);
    await writeNewConfigFile(
      probePath,
      Buffer.from(`${JSON.stringify(value, null, 2)}\n`, 'utf8'),
    );
    const health = runOpenClaw(['gateway', 'health', '--json'], {
      OPENCLAW_CONFIG_PATH: probePath,
    });
    return health.ok
      ? undefined
      : 'OpenClaw container Gateway health failed at the configured local port.';
  } catch (error) {
    return `OpenClaw container Gateway health setup failed: ${error instanceof Error ? error.message : String(error)}`;
  } finally {
    await rm(probePath, { force: true });
  }
}

export function gatewayPort(value: string): number {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new TypeError('--gateway-url must be a valid ws:// loopback URL.');
  }
  if (
    parsed.protocol !== 'ws:' ||
    !['127.0.0.1', 'localhost', '[::1]'].includes(parsed.hostname) ||
    parsed.username ||
    parsed.password ||
    parsed.pathname !== '/' ||
    parsed.search ||
    parsed.hash ||
    !parsed.port
  ) {
    throw new TypeError(
      '--gateway-url must be a ws:// loopback URL with an explicit port and no credentials or path.',
    );
  }
  const port = Number(parsed.port);
  if (!Number.isInteger(port) || port < 1 || port > 65_535)
    throw new TypeError('--gateway-url must contain a valid port.');
  return port;
}

async function removeStaleConfigFiles(directory: string): Promise<void> {
  const pattern = /^\.semantic-layer-(?:config|install|doctor|restore|rollback-config|uninstall|uninstall-operation)-(\d+)-(\d+)\.json$/u;
  for (const name of await readdir(directory)) {
    const match = pattern.exec(name);
    if (!match) continue;
    const pid = Number(match[1]);
    if (pid === process.pid || processIsAlive(pid)) continue;
    const path = join(directory, name);
    const metadata = await lstat(path).catch(() => undefined);
    if (
      !metadata
      || !metadata.isFile()
      || metadata.isSymbolicLink()
      || (typeof process.getuid === 'function' && metadata.uid !== process.getuid())
    ) continue;
    await rm(path, { force: true });
  }
}

function processIsAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as { code?: string }).code === 'EPERM';
  }
}

async function rotateIngestionKey(
  readIngestKey: () => Promise<string> = () =>
    hiddenQuestion('New ingestion key (hidden): '),
): Promise<number> {
  const previous = await readSetupCredentials().catch((error: unknown) => {
    process.stderr.write(
      `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
    );
    return undefined;
  });
  if (!previous) return 1;
  const installationState = await readInstallationState().catch(
    (error: unknown) => {
      process.stderr.write(
        `FAIL ${error instanceof Error ? error.message : String(error)}\n`,
      );
      return undefined;
    },
  );
  if (!installationState) return 1;
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
  let nextKey = configured.ingestKey;
  if (!nextKey) {
    try {
      nextKey = await readIngestKey();
    } catch {
      process.stderr.write('FAIL Could not read the hidden ingestion key.\n');
      return 1;
    }
  }
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
  const result = await doctor({
    container: installationState.setupMode === 'container',
  });
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

export function runOpenClaw(
  args: string[],
  environmentOverrides: NodeJS.ProcessEnv = {},
): {
  ok: boolean;
  stdout: string;
  stderr: string;
} {
  const result = spawnSync(process.env.OPENCLAW_BIN ?? 'openclaw', args, {
    encoding: 'utf8',
    env: { ...openClawChildEnvironment(), ...environmentOverrides },
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

type ConfigValueSnapshot =
  | { exists: false }
  | { exists: true; value: unknown };

type InstalledPlugin = {
  installPath?: string;
  packageName: string;
  source?: string;
  spec?: string;
  version: string;
};

type ManagedSetupSnapshot = {
  gateway: ConfigValueSnapshot;
  plugin: InstalledPlugin | undefined;
  pluginEntry: ConfigValueSnapshot;
  pluginEntries: ConfigValueSnapshot;
  secrets: ConfigValueSnapshot;
  secretProvider: ConfigValueSnapshot;
};

type ConfigFileSnapshot = {
  contents: Buffer;
  mode: number;
  path: string;
};

function configValueSnapshot(path: string): ConfigValueSnapshot {
  const result = runOpenClaw(['config', 'get', path, '--json']);
  if (!result.ok) {
    if (/Config path not found:/u.test(`${result.stderr}\n${result.stdout}`))
      return { exists: false };
    throw new Error(
      `OpenClaw config get ${path} failed: ${result.stderr || result.stdout}`,
    );
  }
  try {
    return { exists: true, value: JSON.parse(result.stdout) as unknown };
  } catch {
    throw new Error(`OpenClaw config get ${path} did not return valid JSON.`);
  }
}

function inspectInstalledPlugin(
  environmentOverrides: NodeJS.ProcessEnv = {},
): InstalledPlugin | undefined {
  const result = runOpenClaw(
    ['plugins', 'inspect', PLUGIN_ID, '--json'],
    environmentOverrides,
  );
  if (!result.ok) {
    if (/Plugin not found:/u.test(`${result.stderr}\n${result.stdout}`))
      return undefined;
    throw new Error(
      `OpenClaw plugin inspection failed: ${result.stderr || result.stdout}`,
    );
  }
  let report: unknown;
  try {
    report = JSON.parse(result.stdout) as unknown;
  } catch {
    throw new Error('OpenClaw plugin inspection did not return valid JSON.');
  }
  if (!isRecord(report))
    throw new Error('OpenClaw plugin inspection JSON must be an object.');
  const plugin = isRecord(report.plugin) ? report.plugin : undefined;
  const install = isRecord(report.install) ? report.install : undefined;
  const packageName =
    typeof install?.resolvedName === 'string'
      ? install.resolvedName
      : typeof plugin?.packageName === 'string'
        ? plugin.packageName
        : undefined;
  const version =
    typeof install?.resolvedVersion === 'string'
      ? install.resolvedVersion
      : typeof install?.version === 'string'
        ? install.version
        : typeof plugin?.version === 'string'
          ? plugin.version
          : undefined;
  if (!packageName || !version)
    throw new Error(
      'OpenClaw plugin inspection is missing managed package identity.',
    );
  return {
    packageName,
    version,
    ...(typeof install?.installPath === 'string'
      ? { installPath: install.installPath }
      : {}),
    ...(typeof install?.source === 'string' ? { source: install.source } : {}),
    ...(typeof install?.spec === 'string' ? { spec: install.spec } : {}),
  };
}

async function installedPluginPathFailure(
  installed: InstalledPlugin,
): Promise<string | undefined> {
  if (!installed.installPath || !isAbsolute(installed.installPath))
    return 'Installed plugin path is missing or is not absolute.';
  try {
    const [stateRoot, installPath] = await Promise.all([
      realpath(managedStateDirectory()),
      realpath(installed.installPath),
    ]);
    const child = relative(stateRoot, installPath);
    if (
      child === '' ||
      child === '..' ||
      child.startsWith(`..${sep}`) ||
      isAbsolute(child)
    ) {
      return `Installed plugin must be under the persistent OpenClaw state directory ${stateRoot}.`;
    }
  } catch (error) {
    return `Installed plugin path could not be verified: ${error instanceof Error ? error.message : String(error)}`;
  }
  return undefined;
}

function managedSetupSnapshot(): ManagedSetupSnapshot {
  return {
    gateway: configValueSnapshot('gateway'),
    plugin: inspectInstalledPlugin(),
    pluginEntry: configValueSnapshot(`plugins.entries.${PLUGIN_ID}`),
    pluginEntries: configValueSnapshot('plugins.entries'),
    secrets: configValueSnapshot('secrets'),
    secretProvider: configValueSnapshot('secrets.providers.semantic_layer'),
  };
}

function preservedFileConfigurationFailures(
  before: ConfigFileSnapshot,
  after: ConfigFileSnapshot,
  preserveGateway = true,
): string[] {
  const beforeConfig = parseConfigObject(before);
  const afterConfig = parseConfigObject(after);
  const failures: string[] = [];
  if (
    preserveGateway &&
    canonicalJson(beforeConfig.gateway) !== canonicalJson(afterConfig.gateway)
  )
    failures.push('Setup changed Gateway configuration.');
  if (
    canonicalJson(withoutManagedSetup(beforeConfig, preserveGateway)) !==
    canonicalJson(withoutManagedSetup(afterConfig, preserveGateway))
  ) {
    failures.push('Setup changed OpenClaw configuration outside its managed fields.');
  }
  return failures;
}

function withoutManagedSetup(
  config: Record<string, unknown>,
  preserveGateway: boolean,
): unknown {
  const value = structuredClone(config);
  deleteConfigValue(value, ['secrets', 'providers', 'semantic_layer']);
  deleteConfigValue(value, ['plugins', 'entries', PLUGIN_ID]);
  deleteConfigValue(value, ['plugins', 'installs', PLUGIN_ID]);
  deleteConfigValue(value, ['meta', 'lastTouchedVersion']);
  deleteConfigValue(value, ['meta', 'lastTouchedAt']);
  removeEmptyConfigObject(value, ['secrets', 'providers']);
  removeEmptyConfigObject(value, ['secrets']);
  removeEmptyConfigObject(value, ['plugins', 'entries']);
  removeEmptyConfigObject(value, ['plugins', 'installs']);
  removeEmptyConfigObject(value, ['plugins']);
  removeEmptyConfigObject(value, ['meta']);
  if (!preserveGateway) delete value.gateway;
  return value;
}

function removeEmptyConfigObject(
  config: Record<string, unknown>,
  path: string[],
): void {
  let parent: Record<string, unknown> = config;
  for (const key of path.slice(0, -1)) {
    const next = parent[key];
    if (!isRecord(next)) return;
    parent = next;
  }
  const leaf = path.at(-1);
  if (!leaf) return;
  const value = parent[leaf];
  if (isRecord(value) && Object.keys(value).length === 0) delete parent[leaf];
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value))
    return `[${value.map((entry) => canonicalJson(entry)).join(',')}]`;
  if (isRecord(value))
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(',')}}`;
  return JSON.stringify(value) ?? 'undefined';
}

function createInstallationState(input: {
  endpoint: string;
  installationId: string;
  mode: SetupMode;
  preservedEnabledPluginIds: string[];
  serviceName: string;
}): InstallationState {
  return {
    schema: 'semantic_layer_openclaw_installation_v1',
    endpoint: input.endpoint,
    installationId: input.installationId,
    maxSpoolBytes: DEFAULT_OPENCLAW_SPOOL_BYTES,
    pluginPackage: PLUGIN_PACKAGE,
    pluginVersion: PLUGIN_VERSION,
    preservedEnabledPluginIds: input.preservedEnabledPluginIds,
    serviceName: input.serviceName,
    setupMode: input.mode,
  };
}

function enabledOtherPluginIds(snapshot: ConfigValueSnapshot): string[] {
  if (!snapshot.exists || !isRecord(snapshot.value)) return [];
  return Object.entries(snapshot.value)
    .filter(
      ([pluginId, entry]) =>
        pluginId !== PLUGIN_ID &&
        /^[A-Za-z0-9._-]+$/u.test(pluginId) &&
        isRecord(entry) &&
        entry.enabled === true,
    )
    .map(([pluginId]) => pluginId)
    .sort();
}

function managedOwnershipFailures(input: {
  credentials: SetupCredentials | undefined;
  installationState: InstallationState | undefined;
  requestedState: InstallationState;
  snapshot: ManagedSetupSnapshot;
}): string[] {
  const failures: string[] = [];
  const hasOwnerState = Boolean(
    input.credentials || input.installationState,
  );
  if (input.installationState) {
    if (
      canonicalJson(input.installationState) !==
      canonicalJson(input.requestedState)
    ) {
      failures.push(
        'Requested endpoint, service name, installation ID, mode, or pinned package differs from existing installation state.',
      );
    }
    if (
      input.credentials &&
      input.credentials.installationId !==
        input.installationState.installationId
    ) {
      failures.push(
        'Existing credentials do not match existing installation state.',
      );
    }
  }
  if (
    input.credentials &&
    input.credentials.installationId !== input.requestedState.installationId
  ) {
    failures.push(
      'Existing credentials do not match the requested installation ID.',
    );
  }
  if (!hasOwnerState) {
    const incompleteNativeInstall = Boolean(
      input.snapshot.plugin && input.snapshot.pluginEntry.exists,
    );
    if (incompleteNativeInstall) {
      failures.push(
        'Incomplete Semantic Layer setup detected: the plugin package and config entry exist, but owner-only credentials and installation state are missing. Remove this partial native plugin install before retrying with a fresh key.',
      );
    } else if (input.snapshot.plugin) {
      failures.push(
        'The Semantic Layer plugin already exists without managed installation state.',
      );
    } else if (input.snapshot.pluginEntry.exists) {
      failures.push(
        'The Semantic Layer plugin config entry already exists without managed installation state.',
      );
    }
    if (input.snapshot.secretProvider.exists && !incompleteNativeInstall)
      failures.push(
        'The semantic_layer secret provider already exists without managed installation state.',
      );
  }
  if (
    input.snapshot.plugin &&
    (input.snapshot.plugin.packageName !== PLUGIN_PACKAGE ||
      input.snapshot.plugin.version !== PLUGIN_VERSION)
  ) {
    failures.push(
      `Installed plugin must be ${PLUGIN_PACKAGE}@${PLUGIN_VERSION}; upgrades require a separate controlled release.`,
    );
  }
  return failures;
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if ((error as { code?: string }).code === 'ENOENT') return false;
    throw error;
  }
}

async function readActiveConfigFile(): Promise<ConfigFileSnapshot> {
  const active = runOpenClaw(['config', 'file']);
  if (!active.ok || !active.stdout.trim())
    throw new Error(
      `OpenClaw config file lookup failed: ${active.stderr || active.stdout}`,
    );
  return readConfigFile(resolveUserPath(active.stdout.trim()));
}

async function readConfigFile(path: string): Promise<ConfigFileSnapshot> {
  const metadata = await lstat(path);
  if (!metadata.isFile() || metadata.isSymbolicLink())
    throw new Error(`${path} must be a regular config file, not a link.`);
  if (typeof process.getuid === 'function' && metadata.uid !== process.getuid())
    throw new Error(`${path} must be owned by the current user.`);
  return {
    contents: await readFile(path),
    mode: metadata.mode & 0o777,
    path,
  };
}

async function writeManagedConfigCandidate(
  base: ConfigFileSnapshot,
  patch: Record<string, unknown>,
  candidatePath: string,
): Promise<void> {
  const config = parseConfigObject(base);
  const secrets = requiredPatchObject(patch, 'secrets');
  const providers = requiredPatchObject(secrets, 'providers');
  const provider = requiredPatchObject(providers, 'semantic_layer');
  const plugins = requiredPatchObject(patch, 'plugins');
  const entries = requiredPatchObject(plugins, 'entries');
  const pluginEntry = requiredPatchObject(entries, PLUGIN_ID);
  setConfigValue(config, ['secrets', 'providers', 'semantic_layer'], provider);
  setConfigValue(config, ['plugins', 'entries', PLUGIN_ID], pluginEntry);
  if (isRecord(patch.gateway)) {
    const gateway = ensureConfigObject(config, 'gateway', 'gateway');
    Object.assign(gateway, patch.gateway);
  }
  await writeNewConfigFile(
    candidatePath,
    Buffer.from(`${JSON.stringify(config, null, 2)}\n`, 'utf8'),
  );
}

async function writeUninstallConfigCandidate(
  base: ConfigFileSnapshot,
  candidatePath: string,
): Promise<void> {
  const config = parseConfigObject(base);
  deleteConfigValue(config, ['secrets', 'providers', 'semantic_layer']);
  deleteConfigValue(config, ['plugins', 'entries', PLUGIN_ID]);
  await writeNewConfigFile(
    candidatePath,
    Buffer.from(`${JSON.stringify(config, null, 2)}\n`, 'utf8'),
  );
}

function parseConfigObject(snapshot: ConfigFileSnapshot): Record<string, unknown> {
  let value: unknown;
  try {
    value = JSON5.parse(snapshot.contents.toString('utf8')) as unknown;
  } catch (error) {
    throw new Error(
      `${snapshot.path} could not be parsed as OpenClaw JSON5: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  if (!isRecord(value))
    throw new Error(`${snapshot.path} must contain a config object.`);
  return value;
}

function requiredPatchObject(
  value: Record<string, unknown>,
  key: string,
): Record<string, unknown> {
  const child = value[key];
  if (!isRecord(child))
    throw new Error(`Managed config patch is missing ${key}.`);
  return child;
}

function ensureConfigObject(
  parent: Record<string, unknown>,
  key: string,
  path: string,
): Record<string, unknown> {
  const existing = parent[key];
  if (existing === undefined) {
    const created: Record<string, unknown> = {};
    parent[key] = created;
    return created;
  }
  if (!isRecord(existing))
    throw new Error(`OpenClaw config path ${path} must be an object.`);
  return existing;
}

function setConfigValue(
  root: Record<string, unknown>,
  path: string[],
  value: unknown,
): void {
  let parent = root;
  for (const [index, key] of path.slice(0, -1).entries()) {
    parent = ensureConfigObject(
      parent,
      key ?? '',
      path.slice(0, index + 1).join('.'),
    );
  }
  const leaf = path.at(-1);
  if (!leaf) throw new Error('Managed config path must not be empty.');
  parent[leaf] = value;
}

function deleteConfigValue(
  root: Record<string, unknown>,
  path: string[],
): void {
  let parent = root;
  for (const key of path.slice(0, -1)) {
    const child = parent[key];
    if (child === undefined) return;
    if (!isRecord(child))
      throw new Error(`OpenClaw config path ${path.join('.')} is invalid.`);
    parent = child;
  }
  const leaf = path.at(-1);
  if (leaf) delete parent[leaf];
}

async function writeNewConfigFile(path: string, contents: Buffer): Promise<void> {
  const handle = await open(path, 'wx', 0o600);
  try {
    await handle.writeFile(contents);
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function installConfigCandidate(
  candidatePath: string,
  original: ConfigFileSnapshot,
): Promise<void> {
  await chmod(candidatePath, original.mode);
  await rename(candidatePath, original.path);
  await syncConfigDirectory(dirname(original.path));
}

async function restoreConfigFile(original: ConfigFileSnapshot): Promise<void> {
  const restorePath = join(
    dirname(original.path),
    `.semantic-layer-restore-${process.pid}-${Date.now()}.json`,
  );
  try {
    await writeNewConfigFile(restorePath, original.contents);
    await chmod(restorePath, original.mode);
    await rename(restorePath, original.path);
    await syncConfigDirectory(dirname(original.path));
  } finally {
    await rm(restorePath, { force: true });
  }
}

async function syncConfigDirectory(path: string): Promise<void> {
  let handle: Awaited<ReturnType<typeof open>> | undefined;
  try {
    handle = await open(path, 'r');
    await handle.sync();
  } catch (error) {
    const code = (error as { code?: string }).code ?? '';
    if (!['EINVAL', 'ENOTSUP', 'EPERM', 'EISDIR'].includes(code)) throw error;
  } finally {
    await handle?.close();
  }
}

async function inspectSpoolDirectory(
  root: string,
): Promise<{ ok: true; bytes: number } | { ok: false; error: string }> {
  try {
    const rootMetadata = await lstat(root);
    if (!rootMetadata.isDirectory() || rootMetadata.isSymbolicLink())
      throw new Error('spool root must be a regular directory, not a link');
    if ((rootMetadata.mode & 0o077) !== 0)
      throw new Error('spool root must be owner-only');
    let bytes = 0;
    const pending = [root];
    while (pending.length > 0) {
      const directory = pending.pop();
      if (!directory) continue;
      for (const entry of await readdir(directory, { withFileTypes: true })) {
        const path = join(directory, entry.name);
        if (entry.isSymbolicLink())
          throw new Error(`spool contains a symbolic link: ${path}`);
        if (entry.isDirectory()) {
          pending.push(path);
          continue;
        }
        if (!entry.isFile())
          throw new Error(`spool contains an unsupported entry: ${path}`);
        const metadata = await lstat(path);
        if ((metadata.mode & 0o077) !== 0)
          throw new Error(`spool file must be owner-only: ${path}`);
        bytes += metadata.size;
      }
    }
    return { ok: true, bytes };
  } catch (error) {
    return {
      ok: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

async function rollbackManagedSetup(input: {
  credentialBackup: Buffer | undefined;
  configBackup: ConfigFileSnapshot;
  createdDirectories: string[];
  installedThisRun: boolean;
  stateBackup: Buffer | undefined;
}): Promise<string[]> {
  const failures: string[] = [];
  if (input.installedThisRun) {
    const cleanupPath = join(
      dirname(input.configBackup.path),
      `.semantic-layer-rollback-config-${process.pid}-${Date.now()}.json`,
    );
    try {
      await writeUninstallConfigCandidate(input.configBackup, cleanupPath);
      const uninstallResult = runOpenClaw(
        ['plugins', 'uninstall', PLUGIN_ID, '--force'],
        { OPENCLAW_CONFIG_PATH: cleanupPath },
      );
      if (!uninstallResult.ok)
        failures.push(
          `Plugin uninstall failed: ${uninstallResult.stderr || uninstallResult.stdout}`,
        );
    } catch (error) {
      failures.push(
        `Plugin uninstall failed: ${error instanceof Error ? error.message : String(error)}`,
      );
    } finally {
      await rm(cleanupPath, { force: true });
    }
  }
  try {
    await restoreConfigFile(input.configBackup);
  } catch (error) {
    failures.push(
      `Config restore failed: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  try {
    if (input.credentialBackup)
      await writeOwnerFileAtomically(credentialsPath(), input.credentialBackup);
    else await rm(credentialsPath(), { force: true });
  } catch (error) {
    failures.push(
      `Credential restore failed: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  try {
    if (input.stateBackup)
      await writeOwnerFileAtomically(
        installationStatePath(),
        input.stateBackup,
      );
    else await rm(installationStatePath(), { force: true });
  } catch (error) {
    failures.push(
      `Installation state restore failed: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
  for (const directory of [...input.createdDirectories].reverse()) {
    try {
      await rmdir(directory);
    } catch (error) {
      if (!['ENOENT', 'ENOTEMPTY'].includes((error as { code?: string }).code ?? ''))
        failures.push(
          `Directory cleanup failed for ${directory}: ${error instanceof Error ? error.message : String(error)}`,
        );
    }
  }
  return failures;
}

export type SetupCredentials = {
  ingestKey: string;
  identityKey: string;
  installationId: string;
};

type SetupMode = 'service' | 'container';

type InstallationState = {
  schema: 'semantic_layer_openclaw_installation_v1';
  endpoint: string;
  installationId: string;
  maxSpoolBytes: number;
  pluginPackage: typeof PLUGIN_PACKAGE;
  pluginVersion: typeof PLUGIN_VERSION;
  preservedEnabledPluginIds: string[];
  serviceName: string;
  setupMode: SetupMode;
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
      'Requested installation ID must use the install_ prefix followed by 22 to 128 letters, numbers, underscores, or hyphens.',
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
    maxSpoolBytes: DEFAULT_OPENCLAW_SPOOL_BYTES,
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
  const contents = await readFile(path, 'utf8');
  let parsed: unknown;
  try {
    parsed = JSON.parse(contents) as unknown;
  } catch {
    throw new Error(`${path} contains invalid JSON.`);
  }
  if (!isRecord(parsed))
    throw new Error(`${path} must contain a credential object.`);
  const value = parsed;
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

async function readInstallationState(): Promise<InstallationState> {
  const path = installationStatePath();
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
  if (
    value.schema !== 'semantic_layer_openclaw_installation_v1' ||
    typeof value.endpoint !== 'string' ||
    !value.endpoint.startsWith('https://') ||
    typeof value.installationId !== 'string' ||
    !isInstallationId(value.installationId) ||
    value.maxSpoolBytes !== DEFAULT_OPENCLAW_SPOOL_BYTES ||
    value.pluginPackage !== PLUGIN_PACKAGE ||
    value.pluginVersion !== PLUGIN_VERSION ||
    !Array.isArray(value.preservedEnabledPluginIds) ||
    !value.preservedEnabledPluginIds.every(
      (pluginId) =>
        typeof pluginId === 'string' &&
        pluginId !== PLUGIN_ID &&
        /^[A-Za-z0-9._-]+$/u.test(pluginId),
    ) ||
    new Set(value.preservedEnabledPluginIds).size !==
      value.preservedEnabledPluginIds.length ||
    typeof value.serviceName !== 'string' ||
    !value.serviceName ||
    (value.setupMode !== 'service' && value.setupMode !== 'container')
  ) {
    throw new Error(`${path} does not contain valid managed installation state.`);
  }
  return value as InstallationState;
}

async function readOptionalManagedFile<T>(
  path: string,
  read: () => Promise<T>,
): Promise<T | undefined> {
  try {
    return await read();
  } catch (error) {
    if ((error as { code?: string }).code === 'ENOENT') return undefined;
    throw new Error(
      `${path} could not be read: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
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

async function writeInstallationStateAtomically(
  value: InstallationState,
): Promise<void> {
  await writeOwnerFileAtomically(
    installationStatePath(),
    `${JSON.stringify(value)}\n`,
  );
}

async function writeOwnerFileAtomically(
  path: string,
  value: string | Buffer,
): Promise<void> {
  const temporaryPath = `${path}.next-${process.pid}-${Date.now()}`;
  await writeFile(temporaryPath, value, { mode: 0o600, flag: 'wx' });
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
  return join(managedStateDirectory(), 'semantic-layer', 'credentials.json');
}

function installationStatePath(): string {
  return join(
    managedStateDirectory(),
    'semantic-layer',
    'installation.json',
  );
}

function managedStateDirectory(): string {
  const configuredState = process.env.OPENCLAW_STATE_DIR?.trim();
  if (configuredState) return resolveUserPath(configuredState);
  const configuredFile = process.env.OPENCLAW_CONFIG_PATH?.trim();
  if (configuredFile) return dirname(resolveUserPath(configuredFile));
  const configuredHome = process.env.OPENCLAW_HOME?.trim();
  if (configuredHome)
    return join(resolveUserPath(configuredHome), '.openclaw');
  const activeConfig = runOpenClaw(['config', 'file']);
  if (activeConfig.ok && activeConfig.stdout.trim())
    return dirname(resolveUserPath(activeConfig.stdout.trim()));
  return join(homedir(), '.openclaw');
}

function resolveUserPath(value: string): string {
  if (value === '~') return homedir();
  if (value.startsWith('~/')) return resolve(homedir(), value.slice(2));
  return resolve(value);
}

function optionValue(
  args: string[] | undefined,
  name: string,
): string | undefined {
  const index = args?.indexOf(name) ?? -1;
  const value = index >= 0 ? args?.[index + 1] : undefined;
  return value && !value.startsWith('--') ? value.trim() : undefined;
}

function validateCommandArguments(command: string, args: string[]): void {
  const valueOptions =
    command === 'setup'
      ? new Set(['--endpoint', '--service-name', '--installation-id'])
      : command === 'doctor'
        ? new Set(['--gateway-url'])
      : new Set<string>();
  const flagOptions =
    command === 'setup'
      ? new Set(['--container', '--dry-run'])
      : command === 'doctor'
        ? new Set(['--container'])
        : command === 'uninstall'
          ? new Set(['--container', '--acknowledge-external-restart'])
        : new Set<string>();
  const seen = new Set<string>();
  for (let index = 0; index < args.length; index += 1) {
    const option = args[index];
    if (!option) continue;
    if (seen.has(option)) throw new TypeError(`${option} may be provided only once.`);
    if (flagOptions.has(option)) {
      seen.add(option);
      continue;
    }
    if (valueOptions.has(option)) {
      const value = args[index + 1];
      if (!value || value.startsWith('--'))
        throw new TypeError(`${option} requires a value.`);
      seen.add(option);
      index += 1;
      continue;
    }
    throw new TypeError(`Unknown option for ${command}: ${option}`);
  }
  if (command === 'uninstall' && !seen.has('--container'))
    throw new TypeError('uninstall currently requires --container.');
  if (command === 'doctor' && seen.has('--gateway-url') && !seen.has('--container'))
    throw new TypeError('--gateway-url requires doctor --container.');
  if (command === 'doctor' && seen.has('--gateway-url'))
    gatewayPort(optionValue(args, '--gateway-url') ?? '');
  if (
    command === 'uninstall'
    && !seen.has('--acknowledge-external-restart')
  ) {
    throw new TypeError(
      'uninstall requires --acknowledge-external-restart because a foreground Gateway exit can stop the owning container or machine.',
    );
  }
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
      '--installation-id must use the install_ prefix followed by 22 to 128 letters, numbers, underscores, or hyphens.',
    );
  }
  return trimmed;
}

export function setupCommandPlan(
  packageSpec: string,
  mode: SetupMode = 'service',
): string[][] {
  const commands = [
    pluginInstallCommand(packageSpec),
    ['config', 'validate'],
  ];
  if (mode === 'service')
    commands.push(
      ['security', 'audit', '--json'],
      ['gateway', 'restart'],
    );
  return commands;
}

function pluginInstallCommand(packageSpec: string): string[] {
  return packageSpec.startsWith('npm:')
    ? ['plugins', 'install', packageSpec, '--pin']
    : ['plugins', 'install', packageSpec];
}

export function doctorCommandPlan(container = false): string[][] {
  const commands = [['config', 'validate']];
  if (!container)
    commands.push(
      ['security', 'audit', '--json'],
      ['gateway', 'status', '--deep', '--require-rpc'],
    );
  return commands;
}

export function generateIdentityKey(): string {
  return randomBytes(32).toString('hex');
}

export function generateInstallationId(): string {
  return `install_${randomBytes(16).toString('hex')}`;
}

function isInstallationId(value: string): boolean {
  return /^install_[A-Za-z0-9_-]{22,128}$/u.test(value);
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
    const iterable = input as HiddenQuestionInput & AsyncIterable<Buffer | string>;
    if (typeof iterable[Symbol.asyncIterator] !== 'function')
      throw new Error('Hidden input stream is not readable.');
    output.write(label);
    let value = '';
    for await (const chunk of iterable) {
      for (const character of Buffer.from(chunk).toString('utf8')) {
        if (character === '\n' || character === '\r') {
          output.write('\n');
          return value.trim();
        }
        value += character;
        if (value.length > 4096)
          throw new Error('Hidden input exceeds the 4096 character limit.');
      }
    }
    output.write('\n');
    return value.trim();
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
        else {
          value += String.fromCharCode(byte);
          if (value.length > 4096) {
            cleanup();
            reject(new Error('Hidden input exceeds the 4096 character limit.'));
            return;
          }
        }
      }
    };
    input.on('data', onData);
  });
}

function helpText(): string {
  return `semantic-layer-openclaw-setup\n\nUsage:\n  semantic-layer-openclaw-setup setup [--container] [--endpoint URL] [--service-name NAME] [--installation-id ID] [--dry-run]\n  semantic-layer-openclaw-setup dry-run\n  semantic-layer-openclaw-setup doctor [--container] [--gateway-url URL]\n  semantic-layer-openclaw-setup uninstall --container --acknowledge-external-restart\n  semantic-layer-openclaw-setup status\n  semantic-layer-openclaw-setup drain\n  semantic-layer-openclaw-setup rotate-key\n  semantic-layer-openclaw-setup --help\n\nFirst setup requires the installation ID assigned to that host. Setup and rotate-key ask for an ingestion key in a hidden prompt. A setup rerun uses the stored owner-only key. Secrets are never accepted on command lines. Each managed host must use its assigned installation ID and ingestion key. Container setup preserves Gateway settings, but committing the complete plugin configuration can stop a foreground Gateway. Restart the machine from its control plane, then rerun the same setup command to confirm completion without another key. Use --gateway-url when the runtime port is not saved in OpenClaw configuration. Run doctor while the Gateway is live. Container uninstall can also stop a foreground Gateway, so it requires an explicit external restart acknowledgement. Stop a service-managed Gateway before standalone status or drain. Drain stops after 10 seconds.\n`;
}

if (
  process.argv[1] &&
  realpathSync(fileURLToPath(import.meta.url)) === realpathSync(process.argv[1])
) {
  process.exitCode = await main();
}
