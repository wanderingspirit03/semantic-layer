import { createHash } from 'node:crypto';
import { createWriteStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { Storage, type Bucket } from '@google-cloud/storage';

export const READ_PERMISSIONS = ['storage.objects.get', 'storage.objects.list'] as const;
export const WRITE_PERMISSIONS = ['storage.objects.create', 'storage.objects.update', 'storage.objects.delete'] as const;

export type DownloadResult = { size: number; sha256: string };

export interface ReadOnlyStore {
  list(prefix: string): Promise<string[]>;
  readSmall(path: string, maximumBytes: number): Promise<Buffer | undefined>;
  download(path: string, destination: string, maximumBytes: number): Promise<DownloadResult>;
  testPermissions(): Promise<string[]>;
}

export class GcsReadOnlyStore implements ReadOnlyStore {
  private readonly storage: Storage;
  private readonly bucket: Bucket;

  constructor(project: string, bucket: string) {
    this.storage = new Storage({ projectId: project });
    this.bucket = this.storage.bucket(bucket);
  }

  async list(prefix: string): Promise<string[]> {
    await this.authenticate();
    const [files] = await this.bucket.getFiles({ prefix, autoPaginate: true });
    return files.map((file) => file.name).sort();
  }

  async readSmall(path: string, maximumBytes: number): Promise<Buffer | undefined> {
    await this.authenticate();
    const current = this.bucket.file(path);
    try {
      const [metadata] = await current.getMetadata();
      const size = Number(metadata.size ?? Number.NaN);
      const generation = metadata.generation;
      if (!Number.isSafeInteger(size) || size < 0 || size > maximumBytes) {
        throw new Error(`object is larger than ${maximumBytes} bytes: ${path}`);
      }
      if (!generation) throw new Error(`object generation is unavailable: ${path}`);
      const file = this.bucket.file(path, { generation });
      const [bytes] = await file.download();
      if (bytes.byteLength !== size) throw new Error(`object size changed during download: ${path}`);
      return bytes as Buffer;
    } catch (error) {
      if ((error as { code?: number }).code === 404) return undefined;
      throw error;
    }
  }

  async download(path: string, destination: string, maximumBytes: number): Promise<DownloadResult> {
    await this.authenticate();
    const [metadata] = await this.bucket.file(path).getMetadata();
    const expectedSize = Number(metadata.size ?? Number.NaN);
    const generation = metadata.generation;
    if (!Number.isSafeInteger(expectedSize) || expectedSize < 0 || expectedSize > maximumBytes) {
      throw new Error(`object is larger than ${maximumBytes} bytes: ${path}`);
    }
    if (!generation) throw new Error(`object generation is unavailable: ${path}`);
    const hash = createHash('sha256');
    let size = 0;
    const source = this.bucket.file(path, { generation }).createReadStream();
    source.on('data', (chunk: Buffer) => {
      size += chunk.byteLength;
      hash.update(chunk);
    });
    await pipeline(source, createWriteStream(destination, { flags: 'wx', mode: 0o600 }));
    if (size !== expectedSize) throw new Error(`object size changed during download: ${path}`);
    return { size, sha256: hash.digest('hex') };
  }

  async testPermissions(): Promise<string[]> {
    await this.authenticate();
    const requested = [...READ_PERMISSIONS, ...WRITE_PERMISSIONS];
    const [allowed] = await this.bucket.iam.testPermissions(requested);
    return Object.entries(allowed)
      .filter(([, granted]) => granted)
      .map(([permission]) => permission)
      .sort();
  }

  private async authenticate(): Promise<void> {
    await this.storage.authClient.getClient();
  }
}
