import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';

const main = await readFile(new URL('../main.tf', import.meta.url), 'utf8');
const build = await readFile(new URL('../cloudbuild.yaml', import.meta.url), 'utf8');
const dockerfile = await readFile(new URL('../../../packages/cloud-ingest/Dockerfile', import.meta.url), 'utf8');

test('provisions the pilot security, retention, delivery, and alert boundaries', () => {
  for (const resource of [
    'google_artifact_registry_repository',
    'google_billing_budget',
    'google_logging_metric',
    'google_monitoring_alert_policy',
    'google_storage_bucket_iam_member" "operators',
    'google_storage_managed_folder" "completed_evidence',
    'google_storage_managed_folder_iam_member" "trace_readers',
  ]) assert.match(main, new RegExp(`resource "${resource}`, 'u'));
  assert.match(main, /public_access_prevention\s+=\s+"enforced"/u);
  assert.match(main, /retention_duration_seconds\s+=\s+0/u);
  assert.match(main, /google_project_iam_audit_config" "storage_data_write/u);
  assert.doesNotMatch(main, /lifecycle_rule/u);
  assert.match(main, /google_storage_bucket" "metering/u);
  assert.match(main, /google_storage_bucket" "build_source/u);
  assert.match(main, /storage\.objects\.update/u);
  assert.match(main, /completed_evidence[^}]*name\s+=\s+"tenants\/"/u);
  assert.match(main, /trace_readers[^}]*completed_evidence[^}]*operator_objects/u);
  assert.doesNotMatch(main, /trace_readers[^}]*operator_delete/u);
  assert.match(main, /log_type\s+=\s+"DATA_READ"/u);
  assert.match(main, /append_only_deletion_audit_prefix/u);
  assert.match(main, /evidence_and_upload_prefixes_only/u);
  assert.match(main, /version\s+=\s+var\.key_registry_secret_version/u);
  assert.doesNotMatch(main, /version\s+=\s+"latest"/u);
  assert.match(main, /jsonPayload\.meter_pressure/u);
  assert.match(main, /max_instance_request_concurrency\s+=\s+1/u);
  assert.match(main, /timeout\s+=\s+"900s"/u);
  assert.match(main, /cloudbuild\.googleapis\.com/u);
  assert.match(main, /memory\s+=\s+"4Gi"/u);
  assert.match(main, /roles\/cloudbuild\.builds\.editor/u);
  assert.match(main, /roles\/iam\.serviceAccountUser/u);
  assert.match(main, /build_source[\s\S]*roles\/storage\.objectViewer/u);
  assert.match(build, /packages\/cloud-ingest\/Dockerfile/u);
  assert.match(dockerfile, /^FROM node:22-bookworm-slim@sha256:[0-9a-f]{64}/mu);
  assert.match(dockerfile, /pnpm install --frozen-lockfile[^\n]*--ignore-scripts/u);
  assert.match(dockerfile, /pnpm --filter @semantic-layer\/cloud-ingest deploy --prod/u);
  assert.doesNotMatch(dockerfile, /package-lock\.json/u);
});
