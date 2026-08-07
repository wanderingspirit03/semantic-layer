import { createHash } from 'node:crypto';
import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from 'node:http';
import {
  chmod,
  cp,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  rename,
  rm,
  stat,
  writeFile,
} from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Ajv2020 } from 'ajv/dist/2020.js';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { createCapture } from 'semantic-layer-capture';
import { computeBundleDigest, createCloudUploader } from '../src/index.js';

const roots: string[] = [];
const servers: Array<ReturnType<typeof createServer>> = [];

afterEach(async () => {
  await Promise.all(
    servers
      .splice(0)
      .map((server) => new Promise<void>((done) => server.close(() => done()))),
  );
});

describe('semantic-layer-cloud', () => {
  it('requires HTTPS except for explicit loopback development endpoints', async () => {
    expect(() =>
      createCloudUploader({
        endpoint: 'http://ingest.example.test',
        ingestKey: 'test-ingest-key',
        spoolDirectory: '/tmp/unused-spool',
      }),
    ).toThrowError(expect.objectContaining({ code: 'ENDPOINT_INSECURE' }));
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-loopback-'),
    );
    const uploader = createCloudUploader({
      endpoint: 'http://127.0.0.1:4321',
      ingestKey: 'test-ingest-key',
      spoolDirectory: join(root, 'spool'),
    });
    await uploader.shutdown();
  });

  it('matches the canonical bundle digest test vector', () => {
    expect(
      computeBundleDigest([
        { path: 'trace.jsonl', size_bytes: 456, sha256: 'b'.repeat(64) },
        { path: 'blobs/z.bin', size_bytes: 7, sha256: 'c'.repeat(64) },
        { path: 'manifest.json', size_bytes: 123, sha256: 'a'.repeat(64) },
      ]),
    ).toBe('366eb97f10e3b44880407c6b0c61aceb5b3ba37b09b3d0840f0d80a6dee7883b');
    expect(
      computeBundleDigest([
        { path: 'blobs/a.bin', size_bytes: 1, sha256: 'd'.repeat(64) },
        { path: 'blobs/Z.bin', size_bytes: 2, sha256: 'e'.repeat(64) },
      ]),
    ).toBe('745a11a0961b93ad9e26902ac0d8ae9648ccf73577e04740888532f625fb25a3');
  });

  it('rejects an oversize file before it enters the pending spool', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-oversize-'),
    );
    const artifact = await copyExample(root);
    const file = await open(join(artifact, 'too-large.bin'), 'w', 0o600);
    await file.truncate(256 * 1024 * 1024 + 1);
    await file.close();
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
    });
    await expect(uploader.enqueueArtifact(artifact)).rejects.toMatchObject({
      code: 'FILE_TOO_LARGE',
    });
    expect(await readdir(join(root, 'spool', 'pending'))).toEqual([]);
    expect(uploader.status()).toMatchObject({ blockedBundles: 1 });
    expect((await stat(join(artifact, 'too-large.bin'))).size).toBe(
      256 * 1024 * 1024 + 1,
    );
    await uploader.shutdown();
  });

  it('surfaces spool admission pressure without deleting pending data', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-capacity-'),
    );
    const artifact = await copyExample(root);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
      maxSpoolBytes: 1,
    });
    await expect(uploader.enqueueArtifact(artifact)).resolves.toMatchObject({
      state: 'awaiting_spool_admission',
    });
    expect(uploader.status()).toMatchObject({
      pendingBundles: 0,
      awaitingSpoolAdmissionBundles: 1,
      warnings: ['SPOOL_FULL'],
      failures: [{ code: 'SPOOL_FULL' }],
    });
    await uploader.shutdown();
  });

  it('admits at most one bundle when concurrent enqueues compete for one bundle of capacity', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-concurrent-capacity-'),
    );
    const first = await copyExample(root, 'artifact-one');
    const second = await copyExample(root, 'artifact-two');
    await rewriteBundleId(second, 'bundle_framework_examplf');
    const maxSpoolBytes = await bundleBytes(first);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory: join(root, 'spool'),
      maxSpoolBytes,
    });
    const secondLease = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory: join(root, 'spool'),
      maxSpoolBytes,
    });

    const results = await Promise.allSettled([
      uploader.enqueueArtifact(first),
      secondLease.enqueueArtifact(second),
    ]);

    expect(results).toMatchObject([
      { status: 'fulfilled' },
      { status: 'fulfilled' },
    ]);
    const states = results.flatMap((result) =>
      result.status === 'fulfilled' ? [result.value.state] : [],
    );
    expect(states.sort()).toEqual(['awaiting_spool_admission', 'pending']);
    expect(uploader.status()).toMatchObject({
      pendingBundles: 1,
      awaitingSpoolAdmissionBundles: 1,
    });
    await uploader.shutdown();
    await secondLease.shutdown();
  });

  it('durably re-admits a sealed artifact after spool capacity returns', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-readmission-'),
    );
    const first = await copyExample(root, 'artifact-one');
    const second = await copyExample(root, 'artifact-two');
    await rewriteBundleId(second, 'bundle_framework_examplf');
    const spoolDirectory = join(root, 'spool');
    const maxSpoolBytes = (await bundleBytes(first)) + 2_048;
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory,
      maxSpoolBytes,
    });

    const pending = await uploader.enqueueArtifact(first);
    const waiting = await uploader.enqueueArtifact(second);
    expect(waiting.state).toBe('awaiting_spool_admission');
    await uploader.shutdown();

    // Simulate an explicit operator capacity action. The uploader itself never
    // deletes a retained state.
    await rm(join(spoolDirectory, 'pending', pending.bundleDigest), {
      recursive: true,
    });
    const restarted = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory,
      maxSpoolBytes,
    });
    await restarted.flush({ deadlineMs: 0 });

    expect(restarted.status()).toMatchObject({
      pendingBundles: 1,
      awaitingSpoolAdmissionBundles: 0,
    });
    expect(await readdir(join(spoolDirectory, 'pending'))).toEqual([
      waiting.bundleDigest,
    ]);
    await restarted.shutdown();
  });

  it('retains fixed validation failures in upload_blocked without retrying', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-blocked-'));
    const artifact = await copyExample(root);
    await writeFile(join(artifact, 'trace.jsonl'), '{}\n', { mode: 0o600 });
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
    });

    await expect(uploader.enqueueArtifact(artifact)).rejects.toMatchObject({
      code: 'ARTIFACT_INVALID',
    });
    expect(uploader.status()).toMatchObject({
      pendingBundles: 0,
      blockedBundles: 1,
      quarantineBundles: 0,
    });
    expect(await readdir(artifact)).toContain('manifest.json');
    await uploader.shutdown();
  });

  it('quarantines credential-bearing artifacts without copying the credential into spool state', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-secret-'));
    const artifact = await copyExample(root);
    const ingestKey = 'sk-test-secret-value-123456789';
    await insertSecretAndRehash(artifact, ingestKey);
    const spoolDirectory = join(root, 'spool');
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey,
      spoolDirectory,
    });

    await expect(uploader.enqueueArtifact(artifact)).rejects.toMatchObject({
      code: 'ARTIFACT_UNSAFE',
    });
    expect(uploader.status()).toMatchObject({
      pendingBundles: 0,
      blockedBundles: 0,
      quarantineBundles: 1,
    });
    expect(JSON.stringify(uploader.status())).not.toContain(ingestKey);
    expect(await readTreeText(spoolDirectory)).not.toContain(ingestKey);
    expect(await readFile(join(artifact, 'trace.jsonl'), 'utf8')).toContain(
      ingestKey,
    );
    await uploader.shutdown();
  });

  it('counts a durable reservation while an admitted snapshot is still staging', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-reservation-'),
    );
    const artifact = await copyExample(root);
    await addBlob(artifact, Buffer.alloc(16 * 1024 * 1024, 0x61));
    const bytes = await bundleBytes(artifact);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory: join(root, 'spool'),
      maxSpoolBytes: bytes,
    });

    let settled = false;
    const enqueue = uploader.enqueueArtifact(artifact).finally(() => {
      settled = true;
    });
    let observedReservation = false;
    while (!settled) {
      const status = uploader.status();
      if (status.pendingBundles === 0 && status.spoolBytes === bytes) {
        observedReservation = true;
        break;
      }
      await new Promise<void>((resolveImmediate) =>
        setImmediate(resolveImmediate),
      );
    }
    await enqueue;

    expect(observedReservation).toBe(true);
    expect(uploader.status()).toMatchObject({
      pendingBundles: 1,
      spoolBytes: bytes,
    });
    await uploader.shutdown();
  });

  it('recovers attested complete staging and retains incomplete crash state visibly', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-staging-recovery-'),
    );
    const artifact = await copyExample(root);
    const spool = join(root, 'spool');
    const recoverable = join(spool, 'status', 'staging-recoverable');
    const incomplete = join(spool, 'status', 'staging-incomplete');
    await mkdir(recoverable, { recursive: true, mode: 0o700 });
    await cp(artifact, join(recoverable, 'bundle'), { recursive: true });
    await writeFile(
      join(recoverable, 'reservation.json'),
      JSON.stringify({
        bytes: await bundleBytes(artifact),
        bundle_digest: await bundleDigestForTest(artifact),
        source_prevalidated: true,
      }),
      { mode: 0o600 },
    );
    await mkdir(join(incomplete, 'bundle'), { recursive: true, mode: 0o700 });
    await writeFile(
      join(incomplete, 'reservation.json'),
      JSON.stringify({
        bytes: 1_000_000,
        bundle_digest: 'a'.repeat(64),
        source_prevalidated: true,
      }),
      { mode: 0o600 },
    );

    const recoverableBytes = await bundleBytes(artifact);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory: spool,
    });
    expect(uploader.status()).toMatchObject({
      spoolBytes: recoverableBytes + 1_000_000,
    });
    await uploader.flush({ deadlineMs: 0 });

    expect(uploader.status()).toMatchObject({
      pendingBundles: 1,
      blockedBundles: 0,
      quarantineBundles: 1,
    });
    const quarantined = await readdir(join(spool, 'quarantine'));
    await expect(
      stat(join(spool, 'quarantine', quarantined[0]!, 'bundle')),
    ).rejects.toMatchObject({ code: 'ENOENT' });
    expect(
      (await readdir(join(spool, 'status'))).filter((name) =>
        name.startsWith('staging-'),
      ),
    ).toEqual([]);
    await uploader.shutdown();
  });

  it('fails closed on unattested crash staging after credential rotation', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-staging-secret-'),
    );
    const artifact = await copyExample(root);
    const oldKey = 'old-ingest-secret-key-123456';
    await insertSecretAndRehash(artifact, oldKey);
    const spool = join(root, 'spool');
    const staging = join(spool, 'status', 'staging-unattested');
    await mkdir(staging, { recursive: true, mode: 0o700 });
    await cp(artifact, join(staging, 'bundle'), { recursive: true });
    await writeFile(
      join(staging, 'reservation.json'),
      JSON.stringify({ bytes: await bundleBytes(artifact) }),
      { mode: 0o600 },
    );

    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'rotated-ingest-key-123456',
      spoolDirectory: spool,
    });
    await uploader.flush({ deadlineMs: 0 });

    expect(uploader.status()).toMatchObject({
      pendingBundles: 0,
      quarantineBundles: 1,
    });
    expect(await readTreeText(spool)).not.toContain(oldKey);
    await uploader.shutdown();
  });

  it('shares one spool owner inside a process and rejects a live foreign owner', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-owner-'));
    const spoolDirectory = join(root, 'spool');
    const first = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory,
    });
    const sameProcess = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory,
    });
    await expect(first.flush({ deadlineMs: 0 })).resolves.toMatchObject({
      lifecycle: 'running',
    });
    await expect(sameProcess.flush({ deadlineMs: 0 })).resolves.toMatchObject({
      lifecycle: 'running',
    });
    expect(() =>
      createCloudUploader({
        endpoint: 'https://different.invalid',
        spoolDirectory,
      }),
    ).toThrowError(expect.objectContaining({ code: 'SPOOL_CONFIG_CONFLICT' }));

    await first.shutdown();
    expect(first.status()).toMatchObject({ lifecycle: 'shutdown' });
    await expect(sameProcess.flush({ deadlineMs: 0 })).resolves.toMatchObject({
      lifecycle: 'running',
    });
    await sameProcess.shutdown();

    const foreignSpool = join(root, 'foreign-spool');
    await mkdir(join(foreignSpool, 'status'), { recursive: true, mode: 0o700 });
    await writeFile(
      join(foreignSpool, 'status', 'owner.lock'),
      JSON.stringify({ pid: 1, token: 'foreign' }),
      { mode: 0o600 },
    );
    const competing = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory: foreignSpool,
    });
    await expect(competing.flush({ deadlineMs: 0 })).rejects.toMatchObject({
      code: 'SPOOL_IN_USE',
    });

    await writeFile(
      join(spoolDirectory, 'status', 'owner.lock'),
      JSON.stringify({ pid: 2_147_483_647, token: 'stale' }),
      { mode: 0o600 },
    );
    const replacement = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory,
    });
    await expect(replacement.flush({ deadlineMs: 0 })).resolves.toMatchObject({
      lifecycle: 'running',
    });
    await replacement.shutdown();
  });

  it('releases spool ownership when an embedding process exits without closing its lease', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-process-exit-'),
    );
    const spoolDirectory = join(root, 'spool');
    const existingExitListeners = new Set(process.listeners('exit'));
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory,
    });
    await uploader.flush({ deadlineMs: 0 });

    const cleanup = process
      .listeners('exit')
      .find((listener) => !existingExitListeners.has(listener));
    expect(cleanup).toBeDefined();
    cleanup?.call(process, 0);

    await expect(
      stat(join(spoolDirectory, 'status', 'owner.lock')),
    ).rejects.toMatchObject({ code: 'ENOENT' });
    await uploader.shutdown();
  });

  it('rejects a structurally valid recovered artifact because cloud accepts only sealed state', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-sealed-'));
    const artifact = await copyExample(root);
    const manifestPath = join(artifact, 'manifest.json');
    const manifest = JSON.parse(await readFile(manifestPath, 'utf8'));
    manifest.state = 'recovered';
    await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, {
      mode: 0o600,
    });
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
    });
    await expect(uploader.enqueueArtifact(artifact)).rejects.toMatchObject({
      code: 'ARTIFACT_NOT_SEALED',
    });
    await uploader.shutdown();
  });

  it('binds managed manifest v2 to the configured installation without exposing its key', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-installation-'),
    );
    const artifact = await copyExample(root);
    await rewriteAsManifestV2(
      artifact,
      'install_0123456789abcdef0123456789abcdef',
    );
    const ingestKey = 'managed-installation-key-123456';
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey,
      installationId: 'install_abcdef0123456789abcdef0123456789',
      spoolDirectory: join(root, 'spool'),
    });

    await expect(uploader.enqueueArtifact(artifact)).rejects.toMatchObject({
      code: 'INSTALLATION_ID_MISMATCH',
      message:
        'managed manifest installation_id does not match the configured installation',
    });
    expect(uploader.status()).toMatchObject({
      blockedBundles: 1,
      pendingBundles: 0,
    });
    expect(JSON.stringify(uploader.status())).not.toContain(ingestKey);
    await uploader.shutdown();
  });

  it('accepts a matching manifest v2 while retaining manifest v1 compatibility', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-manifest-v2-'),
    );
    const artifact = await copyExample(root);
    const installationId = 'install_0123456789abcdef0123456789abcdef';
    await rewriteAsManifestV2(artifact, installationId);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      installationId,
      spoolDirectory: join(root, 'spool'),
    });

    await expect(uploader.enqueueArtifact(artifact)).resolves.toMatchObject({
      state: 'pending',
    });
    expect(uploader.status()).toMatchObject({
      pendingBundles: 1,
      blockedBundles: 0,
    });
    await uploader.shutdown();
  });

  it('uploads the exact sealed bundle bytes and retains only its acknowledgement receipt', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-'));
    roots.push(root);
    const artifact = await copyExample(root);
    const requests: Array<{
      method: string;
      url: string;
      body: Buffer;
      headers: IncomingMessage['headers'];
    }> = [];
    const endpoint = await listen((request, response) =>
      collect(request, response, requests),
    );
    const uploader = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
      fetch: async (input, init) => {
        expect(new Headers(init?.headers).has('content-length')).toBe(false);
        return fetch(input, init);
      },
    });

    const queued = await uploader.enqueueArtifact(artifact);
    const flushed = await uploader.flush({ deadlineMs: 5_000 });

    expect(flushed.pendingBundles).toBe(0);
    expect(flushed.lastAcknowledgedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/u);
    expect(flushed.lastRequestId).toBe('request-test-123');
    expect(queued.bundleId).toBe('bundle_framework_example');
    expect(requests.map(({ method, url }) => `${method} ${url}`)).toEqual([
      'POST /v1/bundles/bundle_framework_example/begin',
      'PUT /v1/bundles/bundle_framework_example/files/ffa5b716b5a57837f7929dfcca4b4dfdeb97210a7fd5a12d2f1978846d6f1743/parts/0',
      'PUT /v1/bundles/bundle_framework_example/files/a9d391aa3be408589f36b11dddab84e5cf636cfe9011f7d1186286640ba04588/parts/0',
      'POST /v1/bundles/bundle_framework_example/complete',
    ]);
    expect(requests[1]?.body).toEqual(
      await readFile(join(artifact, 'manifest.json')),
    );
    expect(requests[2]?.body).toEqual(
      await readFile(join(artifact, 'trace.jsonl')),
    );
    expect(
      requests.every(({ body }) => !body.includes('test-ingest-key-123456')),
    ).toBe(true);
    const beginSchema = JSON.parse(
      await readFile(
        resolve(
          dirname(fileURLToPath(import.meta.url)),
          '../../../contracts/ingest/v1/semantic-ingest-begin-request.schema.json',
        ),
        'utf8',
      ),
    );
    const validateBegin = new Ajv2020({ strict: false }).compile(beginSchema);
    const beginBody = JSON.parse(requests[0]?.body.toString('utf8') ?? '{}');
    expect(
      validateBegin(beginBody),
      validateBegin.errors?.map((error) => error.message).join(', '),
    ).toBe(true);
    expect(
      beginBody.files.every((file: { parts: unknown }) =>
        Number.isInteger(file.parts),
      ),
    ).toBe(true);

    const acked = await readdir(join(root, 'spool', 'acked'));
    expect(acked).toEqual([queued.bundleDigest]);
    expect(
      await readdir(join(root, 'spool', 'acked', queued.bundleDigest)),
    ).toEqual(['receipt.json']);
    expect(
      JSON.parse(
        await readFile(
          join(root, 'spool', 'acked', queued.bundleDigest, 'receipt.json'),
          'utf8',
        ),
      ),
    ).toMatchObject({
      bundle_digest: queued.bundleDigest,
      status: 'acknowledged',
    });
    expect((await stat(join(root, 'spool'))).mode & 0o777).toBe(0o700);
    expect(
      (
        await stat(
          join(root, 'spool', 'acked', queued.bundleDigest, 'receipt.json'),
        )
      ).mode & 0o777,
    ).toBe(0o600);
    await uploader.shutdown();
  });

  it('removes an admitted source only when its owner transfers cleanup to the uploader', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-source-'));
    roots.push(root);
    const artifact = await copyExample(root);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
      fetch: async () => {
        throw new Error('offline');
      },
    });

    const queued = await uploader.enqueueArtifact(artifact, {
      removeSourceAfterAdmissionFrom: root,
    });

    expect(queued.state).toBe('pending');
    await expect(stat(artifact)).rejects.toMatchObject({ code: 'ENOENT' });
    expect(
      await stat(
        join(root, 'spool', 'pending', queued.bundleDigest, 'bundle'),
      ),
    ).toBeDefined();
    await uploader.shutdown();
  });

  it('retains a source when a cleanup root does not own that direct child', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-source-'));
    roots.push(root);
    const artifact = await copyExample(root);
    const unrelatedRoot = join(root, 'unrelated');
    await mkdir(unrelatedRoot);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
      fetch: async () => {
        throw new Error('offline');
      },
    });

    await uploader.enqueueArtifact(artifact, {
      removeSourceAfterAdmissionFrom: unrelatedRoot,
    });

    expect((await stat(artifact)).isDirectory()).toBe(true);
    await uploader.shutdown();
  });

  it('compacts a legacy acknowledged bundle after restart', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-acked-retention-'),
    );
    const artifact = await copyExample(root);
    const endpoint = await listen((request, response) => {
      const chunks: Buffer[] = [];
      request.on('data', (chunk: Buffer) => chunks.push(chunk));
      request.on('end', () =>
        respondSuccess(request, response, Buffer.concat(chunks)),
      );
    });
    const spoolDirectory = join(root, 'spool');
    const uploader = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory,
    });
    const queued = await uploader.enqueueArtifact(artifact);
    await uploader.flush({ deadlineMs: 5_000 });
    const receiptPath = join(
      spoolDirectory,
      'acked',
      queued.bundleDigest,
      'receipt.json',
    );
    const receipt = JSON.parse(await readFile(receiptPath, 'utf8')) as Record<
      string,
      unknown
    >;
    receipt.acknowledged_at = '2000-01-01T00:00:00.000Z';
    await writeFile(receiptPath, `${JSON.stringify(receipt)}\n`, {
      mode: 0o600,
    });
    await cp(
      artifact,
      join(spoolDirectory, 'acked', queued.bundleDigest, 'bundle'),
      { recursive: true },
    );
    await uploader.shutdown();

    const restarted = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory,
    });
    await restarted.flush({ deadlineMs: 0 });
    expect(restarted.status().ackedBundles).toBe(1);
    expect(await readdir(join(spoolDirectory, 'acked'))).toEqual([
      queued.bundleDigest,
    ]);
    expect(
      await readdir(join(spoolDirectory, 'acked', queued.bundleDigest)),
    ).toEqual(['receipt.json']);
    await restarted.shutdown();
  });

  it('preserves an acknowledged bundle when its receipt is not valid', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-invalid-acked-receipt-'),
    );
    roots.push(root);
    const artifact = await copyExample(root);
    const spoolDirectory = join(root, 'spool');
    const seed = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory,
      fetch: async () => {
        throw new Error('offline');
      },
    });
    const queued = await seed.enqueueArtifact(artifact);
    await seed.shutdown();
    const digest = queued.bundleDigest;
    const acknowledged = join(spoolDirectory, 'acked', digest);
    await rename(join(spoolDirectory, 'pending', digest), acknowledged);
    await writeFile(
      join(acknowledged, 'receipt.json'),
      `${JSON.stringify({
        status: 'acknowledged',
        bundle_id: 'wrong_bundle_id',
        bundle_digest: digest,
        acknowledged_at: new Date().toISOString(),
      })}\n`,
      { mode: 0o600 },
    );

    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory,
    });
    await uploader.flush({ deadlineMs: 0 });

    expect(await readdir(acknowledged)).toEqual(['bundle', 'receipt.json']);
    expect(uploader.status().ackedBundles).toBe(0);
    await uploader.shutdown();
  });

  it('rejects a matching pending directory when its bundle bytes were changed', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-invalid-pending-'),
    );
    roots.push(root);
    const artifact = await copyExample(root);
    const spoolDirectory = join(root, 'spool');
    const originalKey = process.env.SEMANTIC_LAYER_INGEST_KEY;
    delete process.env.SEMANTIC_LAYER_INGEST_KEY;
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      spoolDirectory,
    });
    try {
      const queued = await uploader.enqueueArtifact(artifact);
      await writeFile(
        join(
          spoolDirectory,
          'pending',
          queued.bundleDigest,
          'bundle',
          'trace.jsonl',
        ),
        '{"changed":true}\n',
        { mode: 0o600 },
      );

      await expect(uploader.enqueueArtifact(artifact)).rejects.toMatchObject({
        code: 'PENDING_STATE_INVALID',
      });
    } finally {
      await uploader.shutdown();
      if (originalKey === undefined)
        delete process.env.SEMANTIC_LAYER_INGEST_KEY;
      else process.env.SEMANTIC_LAYER_INGEST_KEY = originalKey;
    }
  });

  it('uses a valid acknowledgement receipt without another network request', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-acked-dedup-'),
    );
    roots.push(root);
    const artifact = await copyExample(root);
    let requests = 0;
    const endpoint = await listen((request, response) => {
      requests += 1;
      const chunks: Buffer[] = [];
      request.on('data', (chunk: Buffer) => chunks.push(chunk));
      request.on('end', () =>
        respondSuccess(request, response, Buffer.concat(chunks)),
      );
    });
    const spoolDirectory = join(root, 'spool');
    const uploader = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory,
    });
    const queued = await uploader.enqueueArtifact(artifact);
    await uploader.flush({ deadlineMs: 5_000 });
    const completedRequests = requests;

    await expect(uploader.enqueueArtifact(artifact)).resolves.toMatchObject({
      bundleId: queued.bundleId,
      bundleDigest: queued.bundleDigest,
      state: 'acked',
    });
    expect(requests).toBe(completedRequests);
    await uploader.shutdown();
  });

  it('keeps an outage pending and resumes it after process restart', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-restart-'));
    const artifact = await copyExample(root);
    const spoolDirectory = join(root, 'spool');
    const offline = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory,
      fetch: async () => {
        throw Object.assign(new Error('offline'), {
          cause: { code: 'UND_ERR_CONNECT_TIMEOUT' },
        });
      },
    });

    const queued = await offline.enqueueArtifact(artifact);
    const outage = await offline.flush({ deadlineMs: 250 });
    expect(outage.pendingBundles).toBe(1);
    expect(outage.failures).toMatchObject([
      {
        bundleDigest: queued.bundleDigest,
        code: 'NETWORK_ERROR',
        message: 'ingest request failed (UND_ERR_CONNECT_TIMEOUT)',
      },
    ]);
    await offline.shutdown();

    const requests: Array<{
      method: string;
      url: string;
      body: Buffer;
      headers: IncomingMessage['headers'];
    }> = [];
    const endpoint = await listen((request, response) =>
      collect(request, response, requests),
    );
    const restarted = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory,
    });
    expect(restarted.status().pendingBundles).toBe(1);
    const result = await restarted.flush({ deadlineMs: 5_000 });
    expect(result.pendingBundles).toBe(0);
    expect(result.ackedBundles).toBe(1);
    expect(requests.at(-1)?.url).toBe(
      '/v1/bundles/bundle_framework_example/complete',
    );
    await restarted.shutdown();
  });

  it('honors a flush deadline even when fetch never settles', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-deadline-'),
    );
    const artifact = await copyExample(root);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
      fetch: async () => await new Promise<Response>(() => {}),
    });
    await uploader.enqueueArtifact(artifact);
    const started = Date.now();
    const result = await uploader.flush({ deadlineMs: 20 });
    expect(Date.now() - started).toBeLessThan(250);
    expect(result).toMatchObject({ pendingBundles: 1, timedOut: true });
    await uploader.shutdown();
  });

  it('pauses on rejected authentication and resumes when the environment key changes', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-auth-'));
    const artifact = await copyExample(root);
    const originalKey = process.env.SEMANTIC_LAYER_INGEST_KEY;
    process.env.SEMANTIC_LAYER_INGEST_KEY = 'rejected-ingest-key';
    const observedAuth: string[] = [];
    let reject = true;
    const endpoint = await listen((request, response) => {
      observedAuth.push(request.headers.authorization ?? '');
      const chunks: Buffer[] = [];
      request.on('data', (chunk: Buffer) => chunks.push(chunk));
      request.on('end', () => {
        if (reject) {
          response.writeHead(401);
          response.end();
        } else respondSuccess(request, response, Buffer.concat(chunks));
      });
    });
    const uploader = createCloudUploader({
      endpoint,
      spoolDirectory: join(root, 'spool'),
    });
    try {
      await uploader.enqueueArtifact(artifact);
      const paused = await uploader.flush({ deadlineMs: 1_000 });
      expect(paused.pausedAuth).toBe(true);
      expect(paused.pendingBundles).toBe(1);
      expect(paused.failures.at(-1)?.code).toBe('HTTP_401');

      reject = false;
      process.env.SEMANTIC_LAYER_INGEST_KEY = 'rotated-ingest-key';
      const resumed = await uploader.flush({ deadlineMs: 5_000 });
      expect(resumed.pendingBundles).toBe(0);
      expect(resumed.pausedAuth).toBe(false);
      expect(observedAuth).toContain('Bearer rejected-ingest-key');
      expect(observedAuth).toContain('Bearer rotated-ingest-key');
    } finally {
      await uploader.shutdown();
      if (originalKey === undefined)
        delete process.env.SEMANTIC_LAYER_INGEST_KEY;
      else process.env.SEMANTIC_LAYER_INGEST_KEY = originalKey;
    }
  });

  it('quarantines a digest conflict without retrying it', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-conflict-'),
    );
    const artifact = await copyExample(root);
    let requests = 0;
    const endpoint = await listen((request, response) => {
      requests += 1;
      request.resume();
      response.writeHead(409);
      response.end();
    });
    const uploader = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
    });
    const queued = await uploader.enqueueArtifact(artifact);
    const result = await uploader.flush({ deadlineMs: 1_000 });
    expect(result.pendingBundles).toBe(0);
    expect(result.quarantineBundles).toBe(1);
    expect(result.failures.at(-1)).toMatchObject({
      bundleDigest: queued.bundleDigest,
      code: 'DIGEST_CONFLICT',
    });
    expect(requests).toBe(1);
    await expect(uploader.enqueueArtifact(artifact)).rejects.toMatchObject({
      code: 'BUNDLE_QUARANTINED',
    });
    expect(uploader.status().pendingBundles).toBe(0);
    await uploader.shutdown();
  });

  it('moves a fixed server validation rejection to upload_blocked', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-server-blocked-'),
    );
    const artifact = await copyExample(root);
    let requests = 0;
    const endpoint = await listen((request, response) => {
      requests += 1;
      request.resume();
      response.writeHead(422);
      response.end();
    });
    const uploader = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
    });
    await uploader.enqueueArtifact(artifact);

    const result = await uploader.flush({ deadlineMs: 1_000 });

    expect(result).toMatchObject({
      pendingBundles: 0,
      blockedBundles: 1,
      quarantineBundles: 0,
    });
    expect(result.failures.at(-1)?.code).toBe('HTTP_422');
    expect(requests).toBe(1);
    await uploader.shutdown();
  });

  it('replays an idempotent upload when the first completion acknowledgement is lost', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-idempotent-'),
    );
    const artifact = await copyExample(root);
    let beginRequests = 0;
    let completeRequests = 0;
    const endpoint = await listen((request, response) => {
      const chunks: Buffer[] = [];
      request.on('data', (chunk: Buffer) => chunks.push(chunk));
      request.on('end', () => {
        if (request.url?.endsWith('/begin')) beginRequests += 1;
        if (request.url?.endsWith('/complete')) {
          completeRequests += 1;
          if (completeRequests === 1) {
            response.writeHead(500);
            response.end();
            return;
          }
        }
        respondSuccess(request, response, Buffer.concat(chunks));
      });
    });
    const uploader = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
    });
    await uploader.enqueueArtifact(artifact);
    const result = await uploader.flush({ deadlineMs: 5_000 });
    expect(result.pendingBundles).toBe(0);
    expect(result.ackedBundles).toBe(1);
    expect(beginRequests).toBe(2);
    expect(completeRequests).toBe(2);
    await uploader.shutdown();
  });

  it('retries HTTP 429 without losing or quarantining the pending bundle', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-429-'));
    const artifact = await copyExample(root);
    let beginRequests = 0;
    const endpoint = await listen((request, response) => {
      const chunks: Buffer[] = [];
      request.on('data', (chunk: Buffer) => chunks.push(chunk));
      request.on('end', () => {
        if (request.url?.endsWith('/begin')) {
          beginRequests += 1;
          if (beginRequests === 1) {
            response.writeHead(429);
            response.end();
            return;
          }
        }
        respondSuccess(request, response, Buffer.concat(chunks));
      });
    });
    const uploader = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
    });
    await uploader.enqueueArtifact(artifact);

    const result = await uploader.flush({ deadlineMs: 5_000 });

    expect(result).toMatchObject({
      pendingBundles: 0,
      ackedBundles: 1,
      quarantineBundles: 0,
    });
    expect(beginRequests).toBe(2);
    await uploader.shutdown();
  });

  it('reports active retry and quota state without exposing request data', async () => {
    const root = await mkdtemp(
      join(tmpdir(), 'semantic-layer-cloud-quota-status-'),
    );
    const artifact = await copyExample(root);
    const random = vi.spyOn(Math, 'random').mockReturnValue(0.99);
    const uploader = createCloudUploader({
      endpoint: 'https://ingest.invalid',
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
      fetch: async () => new Response(null, { status: 429 }),
    });
    try {
      await uploader.enqueueArtifact(artifact);
      await vi.waitFor(
        () => {
          const status = uploader.status();
          expect(status.pendingBundles).toBe(1);
          expect(status.quotaLimited).toBe(true);
          expect(status.retryingBundles).toBe(1);
          expect(status.nextRetryAt).toMatch(/^\d{4}-\d{2}-\d{2}T/u);
        },
        { timeout: 5_000 },
      );

      expect(JSON.stringify(uploader.status())).not.toContain(
        'test-ingest-key-123456',
      );
    } finally {
      random.mockRestore();
      await uploader.shutdown();
    }
  });

  it('stages and uploads a sealed bundle containing safe omission evidence', async () => {
    const root = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-omission-'));
    const privateValue = 'private-final-scan-value';
    const capture = createCapture({
      output: join(root, 'traces'),
      serviceName: 'cloud-safe-omission',
      secretValues: [privateValue],
    });
    capture.installSource({
      metadata: {
        name: 'fixture:safe-omission',
        seam: 'fixture.callback',
        identityDomain: 'fixture.operation',
        coverage: [],
      },
      install(sink) {
        const opened = sink.openTrace({
          name: 'safe-root',
          semantic: { type: 'agent.run', name: 'safe-root' },
        });
        if (!opened.accepted) throw new Error(opened.reason);
        sink.record({
          kind: 'state',
          phase: 'event',
          name: 'omitted-state',
          trace: opened.identity,
          parentRecordId: opened.recordId,
          nativeIdentity: privateValue,
          native: null,
          semantic: { type: 'state.omitted', value: true },
        });
        sink.record({
          kind: 'lifecycle',
          phase: 'end',
          name: 'safe-root',
          trace: opened.identity,
          parentRecordId: opened.recordId,
          native: null,
          semantic: { type: 'agent.run', status: 'succeeded' },
        });
        return { deactivate() {}, drain() {} };
      },
    });
    const sealed = await capture.shutdown();
    const requests: Array<{
      method: string;
      url: string;
      body: Buffer;
      headers: IncomingMessage['headers'];
    }> = [];
    const endpoint = await listen((request, response) =>
      collect(request, response, requests));
    const uploader = createCloudUploader({
      endpoint,
      ingestKey: 'test-ingest-key-123456',
      spoolDirectory: join(root, 'spool'),
    });
    try {
      await expect(uploader.enqueueArtifact(sealed.artifactPath)).resolves.toMatchObject({
        state: 'pending',
      });
      await expect(uploader.flush({ deadlineMs: 5_000 })).resolves.toMatchObject({
        pendingBundles: 0,
        uploadedBundles: 1,
      });
      const uploaded = Buffer.concat(requests.map((request) => request.body));
      expect(uploaded.includes(privateValue)).toBe(false);
      expect(uploaded.includes('scrubber_failure_payload_omitted')).toBe(true);
      expect(requests.at(-1)?.url).toMatch(/\/complete$/u);
    } finally {
      await uploader.shutdown();
      await rm(root, { recursive: true, force: true });
    }
  });
});

async function copyExample(
  root: string,
  directory = 'artifact',
): Promise<string> {
  const here = dirname(fileURLToPath(import.meta.url));
  const source = resolve(
    here,
    '../../../contracts/trace/v1/examples/framework',
  );
  const destination = join(root, directory);
  await cp(source, destination, { recursive: true });
  await chmod(destination, 0o700);
  for (const name of await readdir(destination))
    await chmod(join(destination, name), 0o600);
  return destination;
}

async function rewriteBundleId(
  artifact: string,
  bundleId: string,
): Promise<void> {
  const path = join(artifact, 'manifest.json');
  const manifest = JSON.parse(await readFile(path, 'utf8')) as Record<
    string,
    unknown
  >;
  manifest.bundle_id = bundleId;
  await writeFile(path, `${JSON.stringify(manifest, null, 2)}\n`, {
    mode: 0o600,
  });
}

async function rewriteAsManifestV2(
  artifact: string,
  installationId: string,
): Promise<void> {
  const path = join(artifact, 'manifest.json');
  const manifest = JSON.parse(await readFile(path, 'utf8')) as Record<
    string,
    any
  >;
  manifest.schema = 'semantic_trace_manifest_v2';
  manifest.installation_id = installationId;
  manifest.capture_policy = 'rich-credential-scrubbed';
  manifest.sources = manifest.sources.map(
    (source: Record<string, unknown>) => ({
      ...source,
      qualification: { status: 'exact_qualified' },
    }),
  );
  await writeFile(path, `${JSON.stringify(manifest, null, 2)}\n`, {
    mode: 0o600,
  });
}

async function bundleBytes(artifact: string): Promise<number> {
  let total = 0;
  for (const entry of await readdir(artifact, { withFileTypes: true })) {
    const path = join(artifact, entry.name);
    total += entry.isDirectory()
      ? await bundleBytes(path)
      : (await stat(path)).size;
  }
  return total;
}

async function bundleDigestForTest(artifact: string): Promise<string> {
  const files: Array<{ path: string; size_bytes: number; sha256: string }> = [];
  const visit = async (directory: string, prefix = ''): Promise<void> => {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const absolute = join(directory, entry.name);
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) await visit(absolute, relative);
      else {
        const bytes = await readFile(absolute);
        files.push({
          path: relative,
          size_bytes: bytes.byteLength,
          sha256: createHash('sha256').update(bytes).digest('hex'),
        });
      }
    }
  };
  await visit(artifact);
  return computeBundleDigest(files);
}

async function addBlob(artifact: string, blob: Buffer): Promise<void> {
  const blobPath = 'blobs/large.bin';
  const blobs = join(artifact, 'blobs');
  await mkdir(blobs, { mode: 0o700 });
  await writeFile(join(artifact, blobPath), blob, { mode: 0o600 });
  const tracePath = join(artifact, 'trace.jsonl');
  const rows = (await readFile(tracePath, 'utf8'))
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
  rows[0] = {
    ...rows[0],
    blob_refs: [
      {
        path: blobPath,
        bytes: blob.byteLength,
        sha256: createHash('sha256').update(blob).digest('hex'),
        media_type: 'application/octet-stream',
        scan: 'clean',
      },
    ],
  };
  const trace = Buffer.from(
    rows.map((row) => `${JSON.stringify(row)}\n`).join(''),
  );
  await writeFile(tracePath, trace, { mode: 0o600 });
  const manifestPath = join(artifact, 'manifest.json');
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8')) as Record<
    string,
    any
  >;
  manifest.trace.bytes = trace.byteLength;
  manifest.trace.sha256 = createHash('sha256').update(trace).digest('hex');
  manifest.blobs.count = 1;
  manifest.blobs.bytes = blob.byteLength;
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, {
    mode: 0o600,
  });
}

async function insertSecretAndRehash(
  artifact: string,
  secret: string,
): Promise<void> {
  const tracePath = join(artifact, 'trace.jsonl');
  const rows = (await readFile(tracePath, 'utf8'))
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, any>);
  rows[0].data.input.city = secret;
  const trace = Buffer.from(
    rows.map((row) => `${JSON.stringify(row)}\n`).join(''),
  );
  await writeFile(tracePath, trace, { mode: 0o600 });
  const manifestPath = join(artifact, 'manifest.json');
  const manifest = JSON.parse(await readFile(manifestPath, 'utf8')) as Record<
    string,
    any
  >;
  manifest.trace.bytes = trace.byteLength;
  manifest.trace.sha256 = createHash('sha256').update(trace).digest('hex');
  await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`, {
    mode: 0o600,
  });
}

async function readTreeText(root: string): Promise<string> {
  const chunks: string[] = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) chunks.push(await readTreeText(path));
    else if (entry.isFile()) chunks.push(await readFile(path, 'utf8'));
  }
  return chunks.join('\n');
}

async function listen(
  handler: (request: IncomingMessage, response: ServerResponse) => void,
): Promise<string> {
  const server = createServer(handler);
  servers.push(server);
  await new Promise<void>((done) => server.listen(0, '127.0.0.1', done));
  const address = server.address();
  if (!address || typeof address === 'string')
    throw new Error('test server did not bind');
  return `http://127.0.0.1:${address.port}`;
}

function collect(
  request: IncomingMessage,
  response: ServerResponse,
  requests: Array<{
    method: string;
    url: string;
    body: Buffer;
    headers: IncomingMessage['headers'];
  }>,
): void {
  const chunks: Buffer[] = [];
  request.on('data', (chunk: Buffer) => chunks.push(chunk));
  request.on('end', () => {
    requests.push({
      method: request.method ?? '',
      url: request.url ?? '',
      body: Buffer.concat(chunks),
      headers: request.headers,
    });
    respondSuccess(request, response, Buffer.concat(chunks));
  });
}

function respondSuccess(
  request: IncomingMessage,
  response: ServerResponse,
  body: Buffer,
): void {
  let responseBody: Record<string, unknown> = { status: 'ok' };
  if (request.url?.endsWith('/complete')) {
    const parsed = JSON.parse(body.toString('utf8')) as {
      bundle_digest: string;
    };
    responseBody = { status: 'complete', bundle_digest: parsed.bundle_digest };
  }
  response.writeHead(200, {
    'content-type': 'application/json',
    'x-request-id': 'request-test-123',
  });
  response.end(JSON.stringify(responseBody));
}
