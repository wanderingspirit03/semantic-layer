import { randomUUID } from 'node:crypto';
import { chmod, lstat, mkdir, readFile, rename, writeFile } from 'node:fs/promises';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { assertNoSymbolicLinkComponents } from './paths.js';

export const CONFIG_VERSION = 1;

export type EnvironmentConfig = {
  project: string;
  bucket: string;
  output: string;
};

type ConfigFile = {
  version: 1;
  environments: Record<string, EnvironmentConfig>;
};

export function configPath(environment = process.env): string {
  if (environment.SEMANTIC_LAYER_TRACES_CONFIG) {
    return resolve(environment.SEMANTIC_LAYER_TRACES_CONFIG);
  }
  const root = environment.XDG_CONFIG_HOME
    ? resolve(environment.XDG_CONFIG_HOME)
    : join(homedir(), '.config');
  return join(root, 'semantic-layer-traces', 'config.json');
}

export async function readConfig(path = configPath()): Promise<ConfigFile> {
  await assertNoSymbolicLinkComponents(path);
  try {
    const metadata = await lstat(path);
    if (!metadata.isFile() || metadata.isSymbolicLink()) {
      throw new Error(`configuration must be a regular file: ${path}`);
    }
    if (process.platform !== 'win32' && (metadata.mode & 0o077) !== 0) {
      throw new Error(`configuration is not owner private; run: chmod 600 ${path}`);
    }
    const value = JSON.parse(await readFile(path, 'utf8')) as Partial<ConfigFile>;
    if (value.version !== CONFIG_VERSION || !value.environments || typeof value.environments !== 'object') {
      throw new Error(`configuration version is not supported: ${path}`);
    }
    return value as ConfigFile;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
      return { version: CONFIG_VERSION, environments: {} };
    }
    throw error;
  }
}

export async function saveEnvironment(
  name: string,
  value: EnvironmentConfig,
  path = configPath(),
): Promise<void> {
  validateEnvironmentName(name);
  validateEnvironmentConfig(value);
  const config = await readConfig(path);
  for (const [environment, existing] of Object.entries(config.environments)) {
    if (environment !== name && resolve(existing.output) === resolve(value.output)) {
      throw new Error(`output directory is already used by environment ${environment}; choose a separate directory`);
    }
  }
  const directory = dirname(path);
  await assertNoSymbolicLinkComponents(directory);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await assertNoSymbolicLinkComponents(directory);
  if (process.platform !== 'win32') await chmod(directory, 0o700);
  await assertNoSymbolicLinkComponents(value.output);
  await mkdir(value.output, { recursive: true, mode: 0o700 });
  await assertNoSymbolicLinkComponents(value.output);
  if (process.platform !== 'win32') await chmod(value.output, 0o700);
  const next: ConfigFile = {
    version: CONFIG_VERSION,
    environments: { ...config.environments, [name]: value },
  };
  const temporary = join(directory, `.config-${randomUUID()}.tmp`);
  await writeFile(temporary, `${JSON.stringify(next, null, 2)}\n`, { mode: 0o600, flag: 'wx' });
  await rename(temporary, path);
  if (process.platform !== 'win32') await chmod(path, 0o600);
}

export async function getEnvironment(name: string, path = configPath()): Promise<EnvironmentConfig> {
  validateEnvironmentName(name);
  const config = await readConfig(path);
  const value = config.environments[name];
  if (!value) {
    throw new Error(`environment ${name} is not configured; run: semantic-layer-traces configure ${name} --project <GCP_PROJECT_ID> --bucket <EVIDENCE_BUCKET> --output <PRIVATE_LOCAL_DIRECTORY>`);
  }
  validateEnvironmentConfig(value);
  await assertNoSymbolicLinkComponents(value.output);
  return value;
}

export function validateEnvironmentName(value: string): void {
  if (!/^[a-z][a-z0-9-]{0,31}$/u.test(value)) {
    throw new TypeError('environment must start with a letter and contain only lowercase letters, numbers, and hyphens');
  }
}

function validateEnvironmentConfig(value: EnvironmentConfig): void {
  if (!/^[a-z][a-z0-9-]{4,61}[a-z0-9]$/u.test(value.project)) {
    throw new TypeError('project is not a valid GCP project ID');
  }
  if (!/^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$/u.test(value.bucket)) {
    throw new TypeError('bucket is not a valid Cloud Storage bucket name');
  }
  if (!value.output || !resolve(value.output).startsWith('/')) {
    throw new TypeError('output must be an absolute directory');
  }
  value.output = resolve(value.output);
}
