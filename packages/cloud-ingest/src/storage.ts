import { Storage, type Bucket, type File } from '@google-cloud/storage';

export class ObjectConflictError extends Error {}

export interface ObjectStore {
  read(path: string): Promise<Buffer | undefined>;
  writeImmutable(path: string, bytes: Buffer, contentType?: string): Promise<'created' | 'exists'>;
  composeImmutable(sources: readonly string[], destination: string): Promise<'created' | 'exists'>;
  copyImmutable(source: string, destination: string): Promise<'created' | 'exists'>;
  list(prefix: string): Promise<string[]>;
  delete(paths: readonly string[]): Promise<void>;
  readVersioned(path: string): Promise<{ bytes?: Buffer; version: string | null }>;
  writeConditional(path: string, bytes: Buffer, expectedVersion: string | null): Promise<boolean>;
}

export class MemoryObjectStore implements ObjectStore {
  readonly objects = new Map<string, Buffer>();
  readonly writes: string[] = [];
  private readonly versions = new Map<string, number>();
  async read(path: string) { const value = this.objects.get(path); return value && Buffer.from(value); }
  async writeImmutable(path: string, bytes: Buffer) {
    const old = this.objects.get(path);
    if (old) {
      if (!old.equals(bytes)) throw new ObjectConflictError(`immutable object conflict: ${path}`);
      return 'exists' as const;
    }
    this.objects.set(path, Buffer.from(bytes)); this.versions.set(path, 1); this.writes.push(path); return 'created' as const;
  }
  async composeImmutable(sources: readonly string[], destination: string) {
    const chunks = await Promise.all(sources.map(async (path) => {
      const value = await this.read(path); if (!value) throw new Error(`missing source: ${path}`); return value;
    }));
    return this.writeImmutable(destination, Buffer.concat(chunks));
  }
  async copyImmutable(source: string, destination: string) {
    const value = await this.read(source); if (!value) throw new Error(`missing source: ${source}`);
    return this.writeImmutable(destination, value);
  }
  async list(prefix: string) { return [...this.objects.keys()].filter((path) => path.startsWith(prefix)).sort(); }
  async delete(paths: readonly string[]) { for (const path of paths) { this.objects.delete(path); this.versions.delete(path); } }
  async readVersioned(path: string) {
    const bytes = this.objects.get(path);
    return { ...(bytes ? { bytes: Buffer.from(bytes) } : {}), version: bytes ? String(this.versions.get(path) ?? 1) : null };
  }
  async writeConditional(path: string, bytes: Buffer, expectedVersion: string | null) {
    const current = this.objects.get(path);
    const version = current ? String(this.versions.get(path) ?? 1) : null;
    if (version !== expectedVersion) return false;
    this.objects.set(path, Buffer.from(bytes));
    this.versions.set(path, (this.versions.get(path) ?? 0) + 1);
    this.writes.push(path);
    return true;
  }
}

export class GcsObjectStore implements ObjectStore {
  private readonly bucket: Bucket;
  constructor(bucketName: string) {
    this.bucket = new Storage().bucket(bucketName);
  }
  private file(path: string): File { return this.bucket.file(path); }
  async read(path: string) {
    try { const [bytes] = await this.file(path).download(); return bytes as Buffer; }
    catch (error) { if ((error as { code?: number }).code === 404) return undefined; throw error; }
  }
  async writeImmutable(path: string, bytes: Buffer, contentType = 'application/octet-stream') {
    try {
      await this.file(path).save(bytes, { resumable: false, contentType, preconditionOpts: { ifGenerationMatch: 0 } });
      return 'created' as const;
    } catch (error) {
      if ((error as { code?: number }).code !== 412) throw error;
      const old = await this.read(path);
      if (old?.equals(bytes)) return 'exists' as const;
      throw new ObjectConflictError(`immutable object conflict: ${path}`);
    }
  }
  async composeImmutable(sources: readonly string[], destination: string) {
    if (sources.length === 0) return this.writeImmutable(destination, Buffer.alloc(0));
    try {
      await this.bucket.combine(sources.map((path) => this.file(path)), this.file(destination), { ifGenerationMatch: 0 });
      return 'created' as const;
    } catch (error) {
      if ((error as { code?: number }).code === 412) return 'exists' as const;
      throw error;
    }
  }
  async copyImmutable(source: string, destination: string) {
    try {
      await this.file(source).copy(this.file(destination), { preconditionOpts: { ifGenerationMatch: 0 } });
      return 'created' as const;
    } catch (error) {
      if ((error as { code?: number }).code === 412) return 'exists' as const;
      throw error;
    }
  }
  async list(prefix: string) { const [files] = await this.bucket.getFiles({ prefix }); return files.map((file) => file.name).sort(); }
  async delete(paths: readonly string[]) { await Promise.all(paths.map((path) => this.file(path).delete({ ignoreNotFound: true }))); }
  async readVersioned(path: string) {
    try {
      const [metadata] = await this.file(path).getMetadata();
      const generation = metadata.generation;
      if (!generation) throw new Error(`object generation unavailable: ${path}`);
      const [bytes] = await this.bucket.file(path, { generation }).download();
      return { bytes: bytes as Buffer, version: String(generation) };
    } catch (error) {
      if ((error as { code?: number }).code === 404) return { version: null };
      throw error;
    }
  }
  async writeConditional(path: string, bytes: Buffer, expectedVersion: string | null) {
    try {
      await this.file(path).save(bytes, {
        resumable: false,
        contentType: 'application/json',
        preconditionOpts: { ifGenerationMatch: expectedVersion ?? 0 },
      });
      return true;
    } catch (error) {
      if ((error as { code?: number }).code === 412) return false;
      throw error;
    }
  }
}
