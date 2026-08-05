import { lstat, stat } from 'node:fs/promises';
import { join, parse, resolve, sep } from 'node:path';

export async function assertNoSymbolicLinkComponents(path: string): Promise<void> {
  const target = resolve(path);
  const root = parse(target).root;
  let current = root;
  for (const component of target.slice(root.length).split(sep).filter(Boolean)) {
    current = join(current, component);
    try {
      const metadata = await lstat(current);
      if (metadata.isSymbolicLink()) {
        if (await allowedMacSystemAlias(current)) continue;
        throw new Error(`path contains a symbolic link: ${current}`);
      }
      if (current !== target && !metadata.isDirectory()) {
        throw new Error(`path ancestor is not a directory: ${current}`);
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') return;
      throw error;
    }
  }
}

async function allowedMacSystemAlias(path: string): Promise<boolean> {
  if (process.platform !== 'darwin' || !['/tmp', '/var', '/etc'].includes(path)) return false;
  return (await stat(path)).isDirectory();
}
