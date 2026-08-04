import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import test from 'node:test';

const required = new Set([
  'package.json',
  'pnpm-lock.yaml',
  'pnpm-workspace.yaml',
  'tsconfig.base.json',
  'contracts/trace/v1/semantic-trace-manifest.schema.json',
  'contracts/trace/v1/semantic-trace-record.schema.json',
  'contracts/trace/v2/semantic-trace-manifest.schema.json',
  'infra/gcp/cloudbuild.yaml',
  'packages/cloud-ingest/Dockerfile',
  'packages/cloud-ingest/package.json',
  'packages/cloud-ingest/tsconfig.json',
]);

const allowed = (path) => required.has(path)
  || path === '.gcloudignore'
  || path.startsWith('packages/cloud-ingest/src/');

test('Cloud Build stages only the ingest build allowlist', () => {
  const output = execFileSync('gcloud', ['meta', 'list-files-for-upload', '.', '--verbosity=none'], {
    cwd: new URL('../../..', import.meta.url),
    encoding: 'utf8',
  });
  const files = output.split(/\r?\n/u).filter(Boolean).map((path) => path.replace(/^\.\//u, ''));
  const unexpected = files.filter((path) => !allowed(path));
  assert.deepEqual(unexpected, [], `unexpected Cloud Build context files:\n${unexpected.join('\n')}`);
  for (const path of required) assert.ok(files.includes(path), `missing required Cloud Build context file: ${path}`);
  for (const sensitive of [
    'private/', 'datasets/', 'logs/', 'outputs/', 'reports/', 'research/', 'tmp/',
    'traces/', '.worktrees/', '.codex-public-validation-work2/', 'synthetic-customer/',
  ]) assert.equal(files.some((path) => path.startsWith(sensitive)), false, `sensitive context escaped: ${sensitive}`);
});
