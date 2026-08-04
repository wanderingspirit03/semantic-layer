import { spawnSync, type SpawnSyncReturns } from 'node:child_process';
import { chmodSync, lstatSync, statSync } from 'node:fs';
import {
  join, parse, resolve, sep,
} from 'node:path';

type PathKind = 'directory' | 'file';
type Runner = (
  command: string,
  args: readonly string[],
) => Pick<SpawnSyncReturns<string>, 'status' | 'stderr' | 'error'>;

export function secureOwnerOnly(
  path: string,
  kind: PathKind,
  options: { platform?: NodeJS.Platform; identity?: string; runner?: Runner } = {},
): void {
  assertNoSymbolicLinkComponents(path, kind);
  const platform = options.platform ?? process.platform;
  if (platform !== 'win32') {
    chmodSync(path, kind === 'directory' ? 0o700 : 0o600);
    return;
  }
  const identity = options.identity ?? windowsIdentity(process.env);
  if (!identity) throw new Error('owner-only Windows ACL requires USERNAME');
  const runner = options.runner ?? defaultRunner;
  const result = runner('icacls', windowsAclArgs(path, kind, identity));
  if (result.error || result.status !== 0) {
    throw new Error(`owner-only Windows ACL failed: ${result.error?.message ?? result.stderr ?? 'unknown'}`);
  }
}

export function assertNoSymbolicLinkComponents(
  path: string,
  targetKind?: PathKind,
): void {
  const target = resolve(path);
  const root = parse(target).root;
  let current = root;
  const components = target.slice(root.length).split(sep).filter(Boolean);
  const paths = [root, ...components.map((component) => {
    current = join(current, component);
    return current;
  })];
  for (const candidate of paths) {
    try {
      const entry = lstatSync(candidate);
      if (entry.isSymbolicLink()) {
        if (allowedSystemAlias(candidate) && statSync(candidate).isDirectory()) continue;
        throw new Error(`capture path contains a symbolic link or junction: ${candidate}`);
      }
      if (candidate === target && targetKind) {
        const expected = targetKind === 'directory' ? entry.isDirectory() : entry.isFile();
        if (!expected) throw new Error(`capture path is not a ${targetKind}: ${candidate}`);
      } else if (candidate !== target && !entry.isDirectory()) {
        throw new Error(`capture path ancestor is not a directory: ${candidate}`);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      return;
    }
  }
}

function allowedSystemAlias(path: string): boolean {
  return process.platform === 'darwin'
    && ['/var', '/tmp', '/etc'].includes(path);
}

export function windowsAclArgs(path: string, kind: PathKind, identity: string): readonly string[] {
  const rights = kind === 'directory' ? '(OI)(CI)F' : 'F';
  return [
    path, '/inheritance:r', '/remove:g', '*S-1-1-0', '*S-1-5-11', '*S-1-5-32-545',
    '/grant:r', `${identity}:${rights}`,
  ];
}

function windowsIdentity(environment: NodeJS.ProcessEnv): string | undefined {
  const username = environment.USERNAME?.trim();
  if (!username) return undefined;
  const domain = environment.USERDOMAIN?.trim();
  return domain ? `${domain}\\${username}` : username;
}

function defaultRunner(command: string, args: readonly string[]): SpawnSyncReturns<string> {
  return spawnSync(command, [...args], { encoding: 'utf8', windowsHide: true });
}
