#!/usr/bin/env node
import { randomUUID } from 'node:crypto';
import { realpathSync } from 'node:fs';
import { lstat } from 'node:fs/promises';
import { resolve, sep } from 'node:path';
import { stdout, stderr } from 'node:process';
import { fileURLToPath } from 'node:url';
import { validateArtifact } from 'semantic-layer-capture';
import {
  discoverCompleted,
  findLocalBundles,
  readLocalBundle,
  syncTenant,
  assertManifestScope,
  type BundleReference,
} from './bundles.js';
import { configPath, getEnvironment, saveEnvironment } from './config.js';
import { GcsReadOnlyStore, WRITE_PERMISSIONS, type ReadOnlyStore } from './store.js';
import { contentRecords, formatSummary, summarize, type TraceSummary } from './summary.js';

type Streams = { out: (text: string) => void; error: (text: string) => void; isTTY: boolean };
type Runtime = {
  streams: Streams;
  configFile: string;
  store?: (project: string, bucket: string) => ReadOnlyStore;
};

type Parsed = { positionals: string[]; flags: Map<string, string | true> };

export async function main(argv = process.argv.slice(2), injected: Partial<Runtime> = {}): Promise<number> {
  const runtime: Runtime = {
    streams: injected.streams ?? {
      out: (text) => stdout.write(text),
      error: (text) => stderr.write(text),
      isTTY: Boolean(stdout.isTTY),
    },
    configFile: injected.configFile ?? configPath(),
    ...(injected.store ? { store: injected.store } : {}),
  };
  const command = argv[0] ?? 'help';
  if (['help', '--help', '-h'].includes(command)) {
    runtime.streams.out(helpText());
    return 0;
  }
  try {
    const parsed = parse(argv.slice(1));
    if (flag(parsed, 'help', false)) {
      runtime.streams.out(helpText(command));
      return 0;
    }
    if (command === 'configure') return await configure(parsed, runtime);
    if (command === 'doctor') return await doctor(parsed, runtime);
    if (command === 'tenants') return await tenants(parsed, runtime);
    if (command === 'installations') return await installations(parsed, runtime);
    if (command === 'sync') return await sync(parsed, runtime);
    if (command === 'list') return await list(parsed, runtime);
    if (command === 'show') return await show(parsed, runtime);
    throw usageError(`unknown command: ${command}`);
  } catch (error) {
    let message = error instanceof Error ? error.message : String(error);
    if (/default credentials|application default credentials/iu.test(message)) {
      message = 'Google Application Default Credentials are unavailable; run: gcloud auth application-default login';
    }
    runtime.streams.error(`ERROR ${safeHuman(message)}\n`);
    if ((error as { usage?: boolean }).usage) runtime.streams.error('Run semantic-layer-traces --help for the full workflow.\n');
    return (error as { usage?: boolean }).usage ? 2 : 1;
  }
}

async function configure(parsed: Parsed, runtime: Runtime): Promise<number> {
  const environment = positional(parsed, 0, 'environment');
  noExtraPositionals(parsed, 1);
  const project = requiredFlag(parsed, 'project');
  const bucket = requiredFlag(parsed, 'bucket');
  const output = resolve(requiredFlag(parsed, 'output'));
  onlyFlags(parsed, ['project', 'bucket', 'output', 'json']);
  await saveEnvironment(environment, { project, bucket, output }, runtime.configFile);
  writeResult(runtime, parsed, { environment, project, bucket, output, config: runtime.configFile },
    `Saved ${environment} in ${runtime.configFile}\nLocal traces: ${output}\nNext: semantic-layer-traces doctor --environment ${environment}\n`);
  return 0;
}

async function doctor(parsed: Parsed, runtime: Runtime): Promise<number> {
  noExtraPositionals(parsed, 0);
  onlyFlags(parsed, ['environment', 'json']);
  const { name, config, store } = await cloudContext(parsed, runtime);
  const permissions = await store.testPermissions();
  const broad = WRITE_PERMISSIONS.filter((permission) => permissions.includes(permission));
  let localPrivate = false;
  try {
    const metadata = await lstat(config.output);
    localPrivate = metadata.isDirectory() && !metadata.isSymbolicLink()
      && (process.platform === 'win32' || (metadata.mode & 0o077) === 0);
  } catch { /* reported below */ }
  let listAccess = false;
  let readAccess = false;
  try {
    const names = await store.list('tenants/');
    listAccess = true;
    const marker = names.find((name) => name.endsWith('/complete.json'));
    if (marker) {
      readAccess = Boolean(await store.readSmall(marker, 4096));
    } else {
      await store.readSmall(`tenants/.semantic-layer-traces-read-check-${randomUUID()}`, 0);
      readAccess = true;
    }
  } catch { /* effective access is reported below */ }
  const missingRead = [
    ...(!listAccess ? ['storage.objects.list'] : []),
    ...(!readAccess ? ['storage.objects.get'] : []),
  ];
  const ok = localPrivate && listAccess && readAccess;
  const result = {
    ok,
    environment: name,
    project: config.project,
    bucket: config.bucket,
    output: config.output,
    permissions,
    missingRead,
    broadPermissions: broad,
    localPrivate,
    listAccess,
    readAccess,
  };
  const lines = [
    `${ok ? 'OK' : 'FAIL'} environment ${name}`,
    `${!listAccess || !readAccess ? 'FAIL' : 'OK'} GCP list and read access`,
    `${localPrivate ? 'OK' : 'FAIL'} private local output ${config.output}`,
    broad.length
      ? `WARN this identity also has ${broad.join(', ')}; use the trace reader group for read only access`
      : 'OK no object create, update, or delete permission detected',
  ];
  writeResult(runtime, parsed, result, `${lines.join('\n')}\n`);
  return ok ? 0 : 1;
}

async function tenants(parsed: Parsed, runtime: Runtime): Promise<number> {
  noExtraPositionals(parsed, 0);
  onlyFlags(parsed, ['environment', 'json']);
  const { store } = await cloudContext(parsed, runtime);
  const references = await discoverCompleted(store);
  const grouped = new Map<string, BundleReference[]>();
  for (const reference of references) grouped.set(reference.tenant, [...(grouped.get(reference.tenant) ?? []), reference]);
  const result = [...grouped].map(([tenant, bundles]) => ({
    tenant,
    installations: new Set(bundles.map((bundle) => bundle.installation)).size,
    completedBundles: bundles.length,
  }));
  writeResult(runtime, parsed, result, result.length
    ? `${result.map((row) => `${row.tenant}  installations=${row.installations}  bundles=${row.completedBundles}`).join('\n')}\n`
    : 'No completed tenants found.\n');
  return 0;
}

async function installations(parsed: Parsed, runtime: Runtime): Promise<number> {
  noExtraPositionals(parsed, 0);
  onlyFlags(parsed, ['environment', 'tenant', 'json']);
  const tenant = requiredFlag(parsed, 'tenant');
  const { store } = await cloudContext(parsed, runtime);
  const references = await discoverCompleted(store, tenant);
  const counts = new Map<string, number>();
  for (const reference of references) counts.set(reference.installation, (counts.get(reference.installation) ?? 0) + 1);
  const result = [...counts].sort().map(([installation, completedBundles]) => ({ tenant, installation, completedBundles }));
  writeResult(runtime, parsed, result, result.length
    ? `${result.map((row) => `${row.installation}  bundles=${row.completedBundles}`).join('\n')}\n`
    : `No completed installations found for ${tenant}.\n`);
  return 0;
}

async function sync(parsed: Parsed, runtime: Runtime): Promise<number> {
  noExtraPositionals(parsed, 0);
  onlyFlags(parsed, ['environment', 'tenant', 'json']);
  const tenant = requiredFlag(parsed, 'tenant');
  const { config, store } = await cloudContext(parsed, runtime);
  const results = await syncTenant(store, tenant, config.output);
  writeResult(runtime, parsed, results, results.length
    ? `${results.map((result) => `${result.status.toUpperCase()} ${result.tenant}/${result.installation}/${result.bundle}${result.error ? `: ${result.error}` : ''}`).join('\n')}\n`
    : `No completed bundles found for ${tenant}.\n`);
  return results.some((result) => result.status === 'failed') ? 1 : 0;
}

async function list(parsed: Parsed, runtime: Runtime): Promise<number> {
  noExtraPositionals(parsed, 0);
  onlyFlags(parsed, ['environment', 'tenant', 'json']);
  const tenant = optionalFlag(parsed, 'tenant');
  const environment = optionalFlag(parsed, 'environment') ?? 'staging';
  const config = await getEnvironment(environment, runtime.configFile);
  const paths = await findLocalBundles(config.output, tenant);
  const summaries: TraceSummary[] = [];
  for (const path of paths) summaries.push(await localSummary(path, config.output));
  writeResult(runtime, parsed, summaries, summaries.length
    ? `${summaries.map(formatSummary).join('\n\n')}\n`
    : `No local bundles found in ${config.output}. Run: semantic-layer-traces sync --environment ${environment} --tenant <TENANT_ID>\n`);
  return summaries.every((summary) => summary.valid) ? 0 : 1;
}

async function show(parsed: Parsed, runtime: Runtime): Promise<number> {
  const scope = positional(parsed, 0, 'tenant/installation/bundle');
  noExtraPositionals(parsed, 1);
  onlyFlags(parsed, ['environment', 'json', 'include-content', 'summary-only']);
  const environment = optionalFlag(parsed, 'environment') ?? 'staging';
  const config = await getEnvironment(environment, runtime.configFile);
  const parts = scope.split('/');
  if (parts.length !== 3 || parts.some((part) => !part || part === '.' || part === '..')) throw usageError('show scope must be tenant/installation/bundle');
  const path = resolve(config.output, ...parts);
  if (!path.startsWith(`${resolve(config.output)}${sep}`)) throw usageError('show scope is unsafe');
  const validation = await validateArtifact(path);
  if (!validation.valid) {
    throw new Error(`local bundle is invalid (${validation.issues.join(', ')}): ${path}; move it aside and run sync again`);
  }
  const { manifest, records } = await readLocalBundle(path);
  assertManifestScope(manifest, { installation: parts[1]!, bundle: parts[2]! });
  const summary = summarize({ tenant: parts[0]!, installation: parts[1]!, bundle: parts[2]! }, manifest, records, validation);
  const includeContent = flag(parsed, 'include-content', false)
    || (runtime.streams.isTTY && !flag(parsed, 'json', false) && !flag(parsed, 'summary-only', false));
  if (flag(parsed, 'include-content', false) && flag(parsed, 'summary-only', false)) {
    throw usageError('--include-content and --summary-only cannot be used together');
  }
  const result = includeContent ? { summary, records: contentRecords(records) } : { summary };
  if (flag(parsed, 'json', false)) {
    runtime.streams.out(`${jsonText(result)}\n`);
  } else {
    if (includeContent) {
      runtime.streams.out('PRIVATE TRACE CONTENT follows. This stays local, but do not copy it into shared logs.\n\n');
    }
    runtime.streams.out(`${safeHuman(formatSummary(summary))}\n`);
    if (includeContent) {
      runtime.streams.out(`${jsonText(contentRecords(records))}\n`);
    } else {
      runtime.streams.out('\nContent is hidden. Pass --include-content to print private trace content.\n');
    }
  }
  return validation.valid ? 0 : 1;
}

async function localSummary(path: string, root: string): Promise<TraceSummary> {
  const relative = path.slice(resolve(root).length + 1).split(sep);
  const validation = await validateArtifact(path);
  if (!validation.valid) {
    throw new Error(`local bundle is invalid (${validation.issues.join(', ')}): ${path}; move it aside and run sync again`);
  }
  const { manifest, records } = await readLocalBundle(path);
  assertManifestScope(manifest, { installation: relative[1]!, bundle: relative[2]! });
  return summarize({ tenant: relative[0]!, installation: relative[1]!, bundle: relative[2]! }, manifest, records, validation);
}

async function cloudContext(parsed: Parsed, runtime: Runtime) {
  const name = optionalFlag(parsed, 'environment') ?? 'staging';
  const config = await getEnvironment(name, runtime.configFile);
  const store = runtime.store?.(config.project, config.bucket) ?? new GcsReadOnlyStore(config.project, config.bucket);
  return { name, config, store };
}

function parse(args: readonly string[]): Parsed {
  const booleanFlags = new Set(['help', 'json', 'include-content', 'summary-only']);
  const positionals: string[] = [];
  const flags = new Map<string, string | true>();
  for (let index = 0; index < args.length; index += 1) {
    const value = args[index]!;
    if (!value.startsWith('--')) { positionals.push(value); continue; }
    const equal = value.indexOf('=');
    const name = value.slice(2, equal === -1 ? undefined : equal);
    if (!name || flags.has(name)) throw usageError(`invalid or repeated flag: ${value}`);
    if (equal !== -1) { flags.set(name, value.slice(equal + 1)); continue; }
    if (booleanFlags.has(name)) { flags.set(name, true); continue; }
    const next = args[index + 1];
    if (next && !next.startsWith('--')) { flags.set(name, next); index += 1; }
    else flags.set(name, true);
  }
  return { positionals, flags };
}

function requiredFlag(parsed: Parsed, name: string): string {
  const value = parsed.flags.get(name);
  if (typeof value !== 'string' || !value) throw usageError(`missing --${name}`);
  return value;
}

function optionalFlag(parsed: Parsed, name: string): string | undefined {
  const value = parsed.flags.get(name);
  if (value === true) throw usageError(`--${name} needs a value`);
  return value;
}

function flag(parsed: Parsed, name: string, fallback: boolean): boolean {
  const value = parsed.flags.get(name);
  if (value === undefined) return fallback;
  if (value !== true) throw usageError(`--${name} does not take a value`);
  return true;
}

function positional(parsed: Parsed, index: number, name: string): string {
  const value = parsed.positionals[index];
  if (!value) throw usageError(`missing ${name}`);
  return value;
}

function noExtraPositionals(parsed: Parsed, count: number): void {
  if (parsed.positionals.length > count) throw usageError(`unexpected argument: ${parsed.positionals[count]}`);
}

function onlyFlags(parsed: Parsed, allowed: string[]): void {
  for (const name of parsed.flags.keys()) if (!allowed.includes(name)) throw usageError(`unknown flag: --${name}`);
}

function usageError(message: string): Error {
  return Object.assign(new TypeError(message), { usage: true });
}

function writeResult(runtime: Runtime, parsed: Parsed, value: unknown, human: string): void {
  runtime.streams.out(flag(parsed, 'json', false) ? `${jsonText(value)}\n` : safeHuman(human));
}

export function safeHuman(value: string): string {
  return [...value].map((character) => {
    const code = character.codePointAt(0)!;
    const unsafe = (code >= 0 && code <= 8)
      || (code >= 11 && code <= 31)
      || (code >= 127 && code <= 159)
      || code === 0x200e
      || code === 0x200f
      || (code >= 0x202a && code <= 0x202e)
      || (code >= 0x2066 && code <= 0x2069);
    return unsafe ? `\\u${code.toString(16).padStart(4, '0')}` : character;
  }).join('');
}

function jsonText(value: unknown): string {
  return JSON.stringify(value, null, 2).replace(/[\u2028\u2029\u200e\u200f\u202a-\u202e\u2066-\u2069]/gu, (character) => (
    `\\u${character.codePointAt(0)!.toString(16).padStart(4, '0')}`
  ));
}

export function helpText(command?: string): string {
  const header = `semantic-layer-traces${command ? ` ${command}` : ''}\n\n`;
  if (command) return `${header}${commandHelp(command)}`;
  return `${header}Read only access to completed Semantic Layer traces in Google Cloud Storage.\n\nWorkflow\n  1. Sign in: gcloud auth application-default login\n  2. Configure: semantic-layer-traces configure staging --project <PROJECT> --bucket <BUCKET> --output <PRIVATE_DIRECTORY> --json\n  3. Check access: semantic-layer-traces doctor --environment staging --json\n  4. Find customers: semantic-layer-traces tenants --environment staging --json\n  5. Find VMs: semantic-layer-traces installations --environment staging --tenant <TENANT> --json\n  6. Sync traces: semantic-layer-traces sync --environment staging --tenant <TENANT> --json\n  7. Inspect safely: semantic-layer-traces list --environment staging --tenant <TENANT> --json\n  8. Optional detail: semantic-layer-traces show <TENANT>/<INSTALLATION>/<BUNDLE> --environment staging --summary-only --json\n\nFor coding agents\n  The commands above are the complete safe workflow. Configure creates its directories.\n  Use list for normal summaries. Show is optional. Do not pass --include-content.\n  Set SEMANTIC_LAYER_TRACES_CONFIG to use a nondefault config file.\n\nCommands\n  configure      Save nonsecret project, bucket, and local output settings\n  doctor         Check GCP read access and local permissions without writing to GCP\n  tenants        Discover tenants that have completed bundles\n  installations  List VM installation IDs for one tenant\n  sync           Download and validate all completed bundles for one tenant\n  list           Summarize validated local bundles without private content\n  show           Show one local bundle; content is hidden in scripts and JSON by default\n\nGlobal behavior\n  --json          Stable machine readable output\n  --help          Command help\n\nTrace content is private plaintext. Keep exports outside repositories. There are no upload or delete commands.\n`;
}

function commandHelp(command: string): string {
  const usage: Record<string, string> = {
    configure: 'Usage: semantic-layer-traces configure <ENVIRONMENT> --project <PROJECT> --bucket <BUCKET> --output <ABSOLUTE_DIRECTORY> [--json]\nCreates the private config and output directories when they do not exist.\n',
    doctor: 'Usage: semantic-layer-traces doctor [--environment staging] [--json]\nChecks effective object permissions. A broad-permission warning does not fail the check.\n',
    tenants: 'Usage: semantic-layer-traces tenants [--environment staging] [--json]\n',
    installations: 'Usage: semantic-layer-traces installations --tenant <TENANT> [--environment staging] [--json]\nInstallation IDs distinguish separate OpenClaw VMs.\n',
    sync: 'Usage: semantic-layer-traces sync --tenant <TENANT> [--environment staging] [--json]\nSafe to repeat. Existing matching bundles are skipped. Conflicts are never overwritten.\n',
    list: 'Usage: semantic-layer-traces list [--tenant <TENANT>] [--environment staging] [--json]\nNever prints prompts, reasoning, tool input, or tool output.\n',
    show: 'Usage: semantic-layer-traces show <TENANT>/<INSTALLATION>/<BUNDLE> [--environment staging] [--summary-only] [--include-content] [--json]\nInteractive output includes private content unless --summary-only is used. Noninteractive and JSON output hide content unless --include-content is explicit.\n',
  };
  if (!usage[command]) throw usageError(`unknown command: ${command}`);
  return usage[command];
}

if (process.argv[1] && realpathSync(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = await main();
}
