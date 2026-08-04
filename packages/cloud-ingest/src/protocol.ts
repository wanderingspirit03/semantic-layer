import { createHash } from 'node:crypto';

export const PART_BYTES = 8 * 1024 * 1024;
export const MAX_FILE_BYTES = 256 * 1024 * 1024;
export const MAX_BUNDLE_BYTES = 512 * 1024 * 1024;

export type FileDescriptor = {
  file_id: string;
  path: string;
  size_bytes: number;
  sha256: string;
  parts: number;
};

export function sha256(bytes: Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex');
}

export function bundleDigest(files: readonly Pick<FileDescriptor, 'path' | 'size_bytes' | 'sha256'>[]): string {
  const hash = createHash('sha256').update('semantic-layer-bundle-v1\0');
  for (const file of [...files].sort((left, right) => Buffer.compare(Buffer.from(left.path, 'utf8'), Buffer.from(right.path, 'utf8')))) {
    const pathBytes = Buffer.byteLength(file.path, 'utf8');
    hash.update(`${pathBytes}:${file.path}\0${file.size_bytes}\0${file.sha256.toLowerCase()}\0`);
  }
  return hash.digest('hex');
}

export function validId(value: unknown): value is string {
  return typeof value === 'string' && /^[a-z][a-z0-9._:-]{2,127}$/u.test(value);
}

export function validFileId(value: unknown): value is string {
  return typeof value === 'string' && /^[0-9a-f]{64}$/u.test(value);
}

export function validInstallationId(value: unknown): value is string {
  return typeof value === 'string' && /^install_[A-Za-z0-9_-]{22,128}$/u.test(value);
}

export function validBundlePath(value: unknown): value is string {
  if (value === 'manifest.json' || value === 'trace.jsonl') return true;
  if (typeof value !== 'string' || value.length > 512 || !value.startsWith('blobs/')) return false;
  return value.split('/').every((part) => part !== '' && part !== '.' && part !== '..' && /^[A-Za-z0-9._-]+$/u.test(part));
}

export function parseDescriptors(value: unknown): FileDescriptor[] | undefined {
  if (!Array.isArray(value) || value.length < 2 || value.length > 1024) return undefined;
  const files: FileDescriptor[] = [];
  const ids = new Set<string>();
  const paths = new Set<string>();
  let total = 0;
  for (const candidate of value) {
    if (!candidate || typeof candidate !== 'object') return undefined;
    const file = candidate as Record<string, unknown>;
    if (Object.keys(file).sort().join(',') !== 'file_id,parts,path,sha256,size_bytes') return undefined;
    if (!validFileId(file.file_id) || !validBundlePath(file.path) || file.file_id !== sha256(Buffer.from(file.path, 'utf8'))) return undefined;
    if (!Number.isSafeInteger(file.size_bytes) || (file.size_bytes as number) < 0 || (file.size_bytes as number) > MAX_FILE_BYTES) return undefined;
    if (typeof file.sha256 !== 'string' || !/^[0-9a-f]{64}$/u.test(file.sha256)) return undefined;
    const expectedParts = Math.ceil((file.size_bytes as number) / PART_BYTES);
    if (file.parts !== expectedParts || !Number.isSafeInteger(file.parts)) return undefined;
    if (ids.has(file.file_id) || paths.has(file.path)) return undefined;
    ids.add(file.file_id); paths.add(file.path); total += file.size_bytes as number;
    files.push(file as FileDescriptor);
  }
  if (!paths.has('manifest.json') || !paths.has('trace.jsonl') || total > MAX_BUNDLE_BYTES) return undefined;
  return files;
}
