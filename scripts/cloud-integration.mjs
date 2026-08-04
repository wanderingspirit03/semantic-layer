import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, readFile, readdir, realpath, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve, sep } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const mode = process.argv[2];

if (mode === 'contracts') {
  await validateIngestContracts();
} else if (mode === 'e2e') {
  await runLocalEndToEnd();
} else if (mode === 'staging') {
  await runStagingEndToEnd();
} else {
  throw new Error('usage: node scripts/cloud-integration.mjs contracts | e2e | staging');
}

async function validateIngestContracts() {
  const [{ Ajv2020 }, formatsNamespace] = await Promise.all([
    import('ajv/dist/2020.js'),
    import('ajv-formats'),
  ]);
  const contractRoot = join(root, 'contracts/ingest/v1');
  const names = (await readdir(contractRoot))
    .filter((name) => name.endsWith('.schema.json'))
    .sort();
  assert.deepEqual(names, [
    'semantic-ingest-begin-request.schema.json',
    'semantic-ingest-complete-request.schema.json',
    'semantic-ingest-complete-response.schema.json',
    'semantic-ingest-receipt.schema.json',
  ]);

  const ajv = new Ajv2020({ allErrors: true, strict: true });
  const addFormats = formatsNamespace.default;
  addFormats(ajv);
  const validators = new Map();
  for (const name of names) {
    const schema = JSON.parse(await readFile(join(contractRoot, name), 'utf8'));
    validators.set(name, ajv.compile(schema));
  }

  const sha = 'a'.repeat(64);
  assertValid(validators.get('semantic-ingest-begin-request.schema.json'), {
    protocol_version: '1',
    bundle_digest: sha,
    manifest: {
      schema: 'semantic_trace_manifest_v1',
      bundle_id: 'bundle_contract',
      state: 'sealed',
    },
    files: [
      { file_id: sha, path: 'manifest.json', size_bytes: 1, sha256: sha, parts: 1 },
      { file_id: 'b'.repeat(64), path: 'trace.jsonl', size_bytes: 1, sha256: sha, parts: 1 },
    ],
  });
  assertValid(validators.get('semantic-ingest-begin-request.schema.json'), {
    protocol_version: '1',
    bundle_digest: sha,
    manifest: {
      schema: 'semantic_trace_manifest_v2',
      bundle_id: 'bundle_contract_v2',
      state: 'sealed',
      capture_policy: 'rich-credential-scrubbed',
      installation_id: 'install_abcdefghijklmnopqrstuv',
    },
    files: [
      { file_id: sha, path: 'manifest.json', size_bytes: 1, sha256: sha, parts: 1 },
      { file_id: 'b'.repeat(64), path: 'trace.jsonl', size_bytes: 1, sha256: sha, parts: 1 },
    ],
  });
  assertInvalid(validators.get('semantic-ingest-begin-request.schema.json'), {
    protocol_version: '1',
    bundle_digest: sha,
    manifest: {
      schema: 'semantic_trace_manifest_v3',
      bundle_id: 'bundle_contract_v3',
      state: 'sealed',
    },
    files: [
      { file_id: sha, path: 'manifest.json', size_bytes: 1, sha256: sha, parts: 1 },
      { file_id: 'b'.repeat(64), path: 'trace.jsonl', size_bytes: 1, sha256: sha, parts: 1 },
    ],
  });
  assertValid(validators.get('semantic-ingest-complete-request.schema.json'), {
    protocol_version: '1',
    bundle_digest: sha,
  });
  assertValid(validators.get('semantic-ingest-complete-response.schema.json'), {
    status: 'complete',
    bundle_digest: sha,
  });
  assertValid(validators.get('semantic-ingest-receipt.schema.json'), {
    protocol_version: '1',
    bundle_id: 'bundle_contract',
    bundle_digest: sha,
    status: 'acknowledged',
    acknowledged_at: '2026-08-01T12:00:00.000Z',
  });

  process.stdout.write(`Validated ${names.length} semantic ingest schemas\n`);
}

function assertValid(validate, value) {
  assert.equal(
    validate(value),
    true,
    (validate.errors ?? []).map((error) => `${error.instancePath} ${error.message}`).join(', '),
  );
}

function assertInvalid(validate, value) {
  assert.equal(validate(value), false, 'expected schema validation to reject the value');
}

async function runLocalEndToEnd() {
  const work = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-e2e-'));
  const installationId = 'install_AAAAAAAAAAAAAAAAAAAAAA';
  let service;
  let uploader;
  try {
    const [capturePackage, cloudPackage, ingestHttp, ingestStorage] = await Promise.all([
      import('../packages/sdk/dist/index.js'),
      import('../packages/cloud/dist/index.js'),
      import('../packages/cloud-ingest/dist/http.js'),
      import('../packages/cloud-ingest/dist/storage.js'),
    ]);

    const artifacts = await createValidatedArtifacts(capturePackage, work, installationId);

    const ingestKey = 'local-e2e-ingest-key';
    const tenant = 'tenant_local';
    const store = new ingestStorage.MemoryObjectStore();
    const meterStore = new ingestStorage.MemoryObjectStore();
    service = ingestHttp.createIngestServer({
      store,
      meterStore,
      keyRegistry: {
        [createHash('sha256').update(ingestKey, 'utf8').digest('hex')]: {
          tenant_id: tenant,
          installation_id: installationId,
          status: 'active',
        },
      },
      logger: () => undefined,
    });
    const requestPaths = [];
    service.server.prependListener('request', (request) => {
      requestPaths.push(request.url ?? '');
    });
    await new Promise((resolveListen, rejectListen) => {
      service.server.once('error', rejectListen);
      service.server.listen(0, '127.0.0.1', resolveListen);
    });
    const address = service.server.address();
    assert(address && typeof address !== 'string', 'local ingest did not bind');

    uploader = cloudPackage.createCloudUploader({
      endpoint: `http://127.0.0.1:${address.port}`,
      ingestKey,
      installationId,
      spoolDirectory: join(work, 'spool'),
    });
    for (const [index, artifact] of artifacts.entries()) {
      const queued = await uploader.enqueueArtifact(artifact.path);
      const flushed = await uploader.flush({ deadlineMs: 10_000 });
      assert.equal(
        flushed.pendingBundles,
        0,
        `uploader did not drain: ${JSON.stringify(flushed)}`,
      );
      assert.equal(flushed.ackedBundles, index + 1);
      assert.equal(flushed.quarantineBundles, 0);
      assert.equal(flushed.timedOut, false);
      await assertExactCloudBundle(store, tenant, installationId, artifact.path, queued);

      const replay = await uploader.enqueueArtifact(artifact.path);
      assert.equal(replay.state, 'acked');
      assert.equal(replay.bundleDigest, queued.bundleDigest);
    }
    assert.equal(requestPaths.filter((path) => path.endsWith('/begin')).length, artifacts.length);
    assert.equal(requestPaths.filter((path) => path.endsWith('/complete')).length, artifacts.length);
    assert(
      requestPaths.some((path) => path.endsWith('/parts/2')),
      `large bundle did not exercise multipart upload beyond two parts; trace bytes: ${artifacts[2].summary.manifest.trace.bytes}`,
    );
    for (const path of requestPaths) {
      assert.match(
        path,
        /^\/v1\/bundles\/[^/]+\/(?:begin|complete|files\/[^/]+\/parts\/\d+)$/u,
        `producer-specific backend route observed: ${path}`,
      );
    }
    process.stdout.write(
      'TypeScript multi-root, Python, and blob/loss bundles passed the shared local ingest E2E\n',
    );
  } finally {
    if (uploader) await uploader.shutdown();
    if (service) await service.close();
    await rm(work, { recursive: true, force: true });
  }
}

async function runStagingEndToEnd() {
  const endpoint = process.env.SEMANTIC_LAYER_E2E_ENDPOINT;
  const ingestKey = process.env.SEMANTIC_LAYER_INGEST_KEY;
  const bucket = process.env.SEMANTIC_LAYER_E2E_BUCKET;
  const tenant = process.env.SEMANTIC_LAYER_E2E_TENANT;
  const installationId = process.env.SEMANTIC_LAYER_E2E_INSTALLATION;
  assert(endpoint?.startsWith('https://'), 'SEMANTIC_LAYER_E2E_ENDPOINT must be HTTPS');
  assert(ingestKey, 'SEMANTIC_LAYER_INGEST_KEY is required');
  assert(bucket && /^[a-z0-9._-]+$/u.test(bucket), 'SEMANTIC_LAYER_E2E_BUCKET is invalid');
  assert(tenant && /^[A-Za-z0-9._-]+$/u.test(tenant), 'SEMANTIC_LAYER_E2E_TENANT is invalid');
  assert(
    installationId && /^install_[A-Za-z0-9_-]{22,128}$/u.test(installationId),
    'SEMANTIC_LAYER_E2E_INSTALLATION is invalid',
  );

  const work = await mkdtemp(join(tmpdir(), 'semantic-layer-cloud-staging-e2e-'));
  let uploader;
  try {
    const [capturePackage, cloudPackage] = await Promise.all([
      import('../packages/sdk/dist/index.js'),
      import('../packages/cloud/dist/index.js'),
    ]);
    const artifacts = await createValidatedArtifacts(capturePackage, work, installationId);
    uploader = cloudPackage.createCloudUploader({
      endpoint,
      ingestKey,
      installationId,
      spoolDirectory: join(work, 'spool'),
    });
    const tokenResult = spawnSync('gcloud', ['auth', 'print-access-token'], {
      cwd: root,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      timeout: 30_000,
    });
    if (tokenResult.status !== 0) {
      throw new Error(`gcloud access token failed\n${tokenResult.stderr}`);
    }
    const accessToken = tokenResult.stdout.trim();
    assert(accessToken, 'gcloud returned an empty access token');

    const uploaded = [];
    for (const artifact of artifacts) {
      const queued = await uploader.enqueueArtifact(artifact.path);
      const flushed = await uploader.flush({ deadlineMs: 60_000 });
      assert.equal(flushed.pendingBundles, 0);
      assert.equal(flushed.quarantineBundles, 0);
      assert.equal(flushed.timedOut, false);

      await assertExactGcsBundle(
        bucket,
        tenant,
        installationId,
        artifact.path,
        queued,
        accessToken,
      );

      const replay = await uploader.enqueueArtifact(artifact.path);
      assert.equal(replay.state, 'acked');
      assert.equal(replay.bundleDigest, queued.bundleDigest);
      uploaded.push({ bundle_id: queued.bundleId, bundle_digest: queued.bundleDigest });
    }
    process.stdout.write(`${JSON.stringify({ uploaded })}\n`);
  } finally {
    if (uploader) await uploader.shutdown();
    await rm(work, { recursive: true, force: true });
  }
}

async function createValidatedArtifacts(capturePackage, work, installationId) {
  const artifacts = [
    await createTypeScriptMultiRootBundle(capturePackage, work, installationId),
    await createPythonBundle(work, installationId),
    await createTypeScriptBlobLossBundle(capturePackage, work, installationId),
  ];
  const bundleIds = new Set();
  for (const artifact of artifacts) {
    const summary = await validateArtifact(capturePackage, artifact.path);
    assert.equal(summary.manifest.sdk.language, artifact.language);
    assert.equal(summary.manifest.schema, 'semantic_trace_manifest_v2');
    assert.equal(summary.manifest.installation_id, installationId);
    assert.equal(bundleIds.has(summary.manifest.bundle_id), false, 'fixture bundle IDs must be unique');
    bundleIds.add(summary.manifest.bundle_id);
    artifact.summary = summary;
  }
  assert.equal(artifacts[0].summary.rows.filter((row) => row.kind === 'run.start').length, 2);
  assert(artifacts[2].summary.manifest.blobs.count > 0, 'blob fixture did not retain a blob');
  assert(artifacts[2].summary.rows.some((row) => row.kind === 'loss'), 'loss fixture has no explicit loss');
  return artifacts;
}

async function createTypeScriptMultiRootBundle(capturePackage, work, installationId) {
  const capture = capturePackage.createCapture({
    output: join(work, 'typescript-multi-root'),
    serviceName: 'typescript-multi-root-e2e',
    installationId,
  });
  const results = await Promise.all([
    capture.observe('first-root', { input: { value: 'first' } }, (scope) => (
      scope.tool('identity', { value: 'first' }, async (input) => input.value)
    )),
    capture.observe('second-root', { input: { value: 'second' } }, (scope) => (
      scope.tool('identity', { value: 'second' }, async (input) => input.value)
    )),
  ]);
  assert.deepEqual(results, ['first', 'second'], 'capture changed concurrent root results');
  const closed = await capture.shutdown();
  assert.equal(closed.state, 'closed');
  return { path: closed.artifactPath, language: 'typescript' };
}

async function createTypeScriptBlobLossBundle(capturePackage, work, installationId) {
  const capture = capturePackage.createCapture({
    output: join(work, 'typescript-blob-loss'),
    serviceName: 'typescript-blob-loss-e2e',
    installationId,
  });
  const payload = Uint8Array.from([0, 1, 2, 3, 4, 250, 251, 252, 253, 254, 255]);
  const result = await capture.observe('blob-and-loss', {
    input: payload,
    parentContext: { required: true },
  }, async (scope) => {
    const traceChunk = 'x'.repeat(512 * 1024);
    for (let index = 0; index < 40; index += 1) {
      const returned = await scope.tool(
        'large-trace-chunk',
        { index, traceChunk },
        async (input) => input.index,
      );
      assert.equal(returned, index, 'capture changed a large-trace tool result');
    }
    return 'application-result';
  });
  assert.equal(result, 'application-result', 'capture changed the blob/loss result');
  const closed = await capture.shutdown();
  assert.equal(closed.state, 'closed');
  return { path: closed.artifactPath, language: 'typescript' };
}

async function createPythonBundle(work, installationId) {
  const output = resolve(work, 'python');
  const program = [
    'import json, sys',
    'from pathlib import Path',
    'from semantic_layer import initialize',
    'from semantic_layer.validation import validate_artifact',
    'capture = initialize(output=Path(sys.argv[1]), service_name="python-cloud-e2e", installation_id=sys.argv[2])',
    'with capture.observe("python-root", input={"producer": "python"}) as scope:',
    '    result = scope.tool("identity", {"value": "python"}, lambda value: value["value"])',
    '    scope.set_output(result)',
    'assert result == "python"',
    'closed = capture.shutdown()',
    'assert validate_artifact(closed.artifact_path).valid',
    'print(json.dumps({"artifact_path": closed.artifact_path}))',
  ].join('\n');
  const result = spawnSync('uv', [
    'run',
    '--isolated',
    '--frozen',
    '--project',
    join(root, 'packages/python'),
    'python',
    '-c',
    program,
    output,
    installationId,
  ], {
    cwd: root,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    timeout: 300_000,
  });
  if (result.status !== 0) {
    throw new Error(`Python bundle generation failed\n${result.stdout}${result.stderr}`);
  }
  const line = result.stdout.trim().split('\n').findLast((value) => value.startsWith('{'));
  assert(line, 'Python bundle generator did not report an artifact path');
  const artifactPath = await realpath(resolve(JSON.parse(line).artifact_path));
  const canonicalOutput = await realpath(output);
  assert(
    artifactPath.startsWith(`${canonicalOutput}${sep}`),
    `Python artifact escaped its private temporary output: ${artifactPath}`,
  );
  return { path: artifactPath, language: 'python' };
}

async function validateArtifact(capturePackage, artifactPath) {
  const validation = await capturePackage.validateArtifact(artifactPath);
  assert.equal(validation.valid, true, validation.issues.join(', '));
  const manifest = JSON.parse(await readFile(join(artifactPath, 'manifest.json'), 'utf8'));
  const rows = (await readFile(join(artifactPath, 'trace.jsonl'), 'utf8'))
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  assert.equal(manifest.state, 'sealed');
  return { manifest, rows };
}

async function assertExactCloudBundle(store, tenant, installationId, artifactPath, queued) {
  const prefix = `tenants/${tenant}/installations/${installationId}/bundles/${queued.bundleId}/`;
  const files = await bundleFiles(artifactPath);
  for (const file of files) {
    assert.deepEqual(
      await store.read(`${prefix}${file.relative}`),
      await readFile(file.path),
      `ingest changed ${file.relative} bytes`,
    );
  }
  const markerPath = `${prefix}complete.json`;
  const markerBytes = await store.read(markerPath);
  assert(markerBytes, 'cloud completion marker is missing');
  const marker = JSON.parse(markerBytes.toString('utf8'));
  assert.equal(marker.bundle_id, queued.bundleId);
  assert.equal(marker.bundle_digest, queued.bundleDigest);
  assert.equal(store.writes.at(-1), markerPath, 'complete.json was not written last');
  assert.deepEqual(
    await store.list(prefix),
    [...files.map((file) => `${prefix}${file.relative}`), markerPath].sort(),
    'cloud bundle contains unexpected files',
  );
}

async function assertExactGcsBundle(
  bucket,
  tenant,
  installationId,
  artifactPath,
  queued,
  accessToken,
) {
  const localFiles = await bundleFiles(artifactPath);
  const prefix = `tenants/${tenant}/installations/${installationId}/bundles/${queued.bundleId}/`;
  const listUrl = new URL(`https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(bucket)}/o`);
  listUrl.searchParams.set('prefix', prefix);
  const listing = await fetch(listUrl, { headers: { authorization: `Bearer ${accessToken}` } });
  assert.equal(listing.status, 200, `GCS list failed with ${listing.status}`);
  const listed = await listing.json();
  const remoteFiles = (listed.items ?? []).map((item) => item.name);
  assert.deepEqual(
    remoteFiles.sort(),
    [...localFiles.map((file) => `${prefix}${file.relative}`), `${prefix}complete.json`].sort(),
    'downloaded cloud bundle contains unexpected files',
  );
  for (const local of localFiles) {
    const response = await fetch(
      `https://storage.googleapis.com/download/storage/v1/b/${encodeURIComponent(bucket)}/o/${encodeURIComponent(`${prefix}${local.relative}`)}?alt=media`,
      { headers: { authorization: `Bearer ${accessToken}` } },
    );
    assert.equal(response.status, 200, `GCS read failed for ${local.relative}`);
    assert.deepEqual(
      Buffer.from(await response.arrayBuffer()),
      await readFile(local.path),
      `staging ingest changed ${local.relative} bytes`,
    );
  }
  const markerResponse = await fetch(
    `https://storage.googleapis.com/download/storage/v1/b/${encodeURIComponent(bucket)}/o/${encodeURIComponent(`${prefix}complete.json`)}?alt=media`,
    { headers: { authorization: `Bearer ${accessToken}` } },
  );
  assert.equal(markerResponse.status, 200, 'GCS completion marker read failed');
  const marker = await markerResponse.json();
  assert.equal(marker.bundle_id, queued.bundleId);
  assert.equal(marker.bundle_digest, queued.bundleDigest);
}

async function bundleFiles(directory, prefix = '') {
  const files = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await bundleFiles(path, relative));
    else if (entry.isFile()) files.push({ relative, path });
    else throw new Error(`bundle contains unsupported filesystem entry: ${relative}`);
  }
  return files.sort((left, right) => left.relative.localeCompare(right.relative));
}
