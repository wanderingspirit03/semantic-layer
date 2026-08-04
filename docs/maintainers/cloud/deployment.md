# Deploy cloud ingest to Google Cloud

Semantic Layer maintainers use this guide to deploy the private ingest service.
Installations do not need Google Cloud access. Staging and production use
separate projects, buckets, service accounts, key registries, and Terraform
state.

The Terraform module in [`infra/gcp`](../../../infra/gcp/) creates one Cloud Run
service in `europe-west2`. It also creates private evidence, metering, and build
source buckets. The module includes Artifact Registry, Secret Manager, service
accounts, alerts, and a project budget.

## Prepare the environment

Keep project values and Terraform state outside the repository. Use a separate
set of values for each environment.

```sh
SL_PROJECT_ID="<GCP_PROJECT_ID>"
SL_REGION="europe-west2"
SL_TFVARS="<PRIVATE_TFVARS_PATH>"
```

The private variables file provides globally unique bucket names, an immutable
container image digest, the billing account, the monthly budget, the billing
currency, the operations email address, and operator identities. Use the
`currencyCode` returned by `gcloud billing accounts describe`.

The running service receives these values:

* `SEMANTIC_LAYER_BUCKET` names the evidence bucket.
* `SEMANTIC_LAYER_METER_BUCKET` names the admission meter bucket.
* `SEMANTIC_LAYER_KEY_REGISTRY_JSON` contains hashed ingestion keys and their
  tenant and installation scopes.
* Cloud Run supplies `PORT`.

## Check the project and quota

Run the checks against the exact project that you plan to deploy. Save the
machine readable output with the private deployment record.

```sh
gcloud projects describe "$SL_PROJECT_ID"
gcloud billing projects describe "$SL_PROJECT_ID"
gcloud services list --enabled --project="$SL_PROJECT_ID"
gcloud run regions describe "$SL_REGION" --project="$SL_PROJECT_ID"
gcloud beta quotas info list \
  --service=run.googleapis.com \
  --project="$SL_PROJECT_ID"
```

The quota command is part of the beta Google Cloud CLI. Check its current syntax
before adding it to automation.

Do not deploy unless each condition is true:

* Billing is active, and the budget alert has a named recipient.
* Organization policy permits public HTTPS access to Cloud Run.
* The APIs used by the Terraform module are available.
* Regional CPU, memory, instance, and build quota has room for the configured
  maximum instance count and expected retries.
* The deployer can create the required buckets and IAM bindings.
* The runtime service account can use only the two data buckets and its registry
  secret.
* Staging and production values are not mixed.

Record the project number, date, checked quota, current use, limit, required
room, and reviewer. A missing or unclear quota value stops deployment.

## Check the key registry

The registry maps the lowercase SHA 256 digest of each ingestion key to an
opaque tenant and installation:

```json
{
  "<64_HEX_SHA256_OF_RANDOM_KEY>": {
    "tenant_id": "<OPAQUE_TENANT_ID>",
    "installation_id": "<OPAQUE_INSTALLATION_ID>",
    "status": "active"
  }
}
```

Create each random key with an approved secret tool. Store the plain key in the
secret tool, and give it to the installation once through a private channel.
Secret Manager stores only the reviewed hash registry.

Terraform creates the registry secret but does not put a secret version in
Terraform state. On the first deployment, create the required services and add
the first version from a private local file:

```sh
terraform -chdir=infra/gcp init
terraform -chdir=infra/gcp apply \
  -target=google_project_service.required \
  -target=google_artifact_registry_repository.ingest \
  -target=google_secret_manager_secret.key_registry \
  -target=google_storage_bucket.build_source \
  -target=google_service_account.build \
  -target=google_artifact_registry_repository_iam_member.build \
  -target=google_project_iam_member.build_logs \
  -target=google_storage_bucket_iam_member.build_source \
  -target=google_storage_bucket_iam_member.operator_build_source \
  -target=google_project_iam_member.operator_builds \
  -target=google_service_account_iam_member.operator_build_identity \
  -target=google_secret_manager_secret_iam_member.operator_versions \
  -var-file="$SL_TFVARS"

gcloud secrets versions add "<REGISTRY_SECRET_NAME>" \
  --data-file="<PRIVATE_REGISTRY_JSON_PATH>" \
  --project="$SL_PROJECT_ID"
```

The module requires a numeric `key_registry_secret_version`. After adding a
registry version, update that input and deploy a new Cloud Run revision. Do not
use `latest`, because instances in one revision could load different registries.

## Build the image

Run the package checks before building the image:

```sh
node infra/gcp/test/build-context.test.mjs
pnpm --filter @semantic-layer/cloud-ingest test
pnpm --filter @semantic-layer/cloud-ingest typecheck
pnpm --filter @semantic-layer/cloud-ingest build
```

The root `.gcloudignore` limits the build context to the ingest image files.
Check the image for known vulnerabilities, then submit the build with the
dedicated build service account:

```sh
gcloud builds submit . \
  --config="infra/gcp/cloudbuild.yaml" \
  --substitutions="_IMAGE=<REGIONAL_REPOSITORY>/<IMAGE>:<CANDIDATE_TAG>" \
  --gcs-source-staging-dir="gs://<BUILD_SOURCE_BUCKET>/source" \
  --service-account="<BUILD_SERVICE_ACCOUNT>" \
  --project="$SL_PROJECT_ID"
```

Resolve the uploaded image to an `@sha256:` reference and put that reference in
the private variables file. Do not deploy a mutable tag.

## Apply the deployment

Review and save a Terraform plan before applying it:

```sh
terraform -chdir=infra/gcp plan \
  -var-file="$SL_TFVARS" \
  -out="<PRIVATE_PLAN_PATH>"

terraform -chdir=infra/gcp apply "<PRIVATE_PLAN_PATH>"
```

Cloud Run has public HTTPS ingress because installations upload from their own
hosts. Every `/v1` route still requires an ingestion key.

After the apply, inspect both data buckets using the names from Terraform
outputs:

```sh
gcloud storage buckets describe "gs://<BUCKET_OUTPUT>"
```

Both buckets must be in `europe-west2`. Public access prevention and uniform
bucket access must be active. Versioning, soft delete, and lifecycle rules must
be absent.

## Test staging

Deploy the candidate to staging first. Check the running service and stored
objects before using the same image digest in production.

The staging test must show that:

* `/health` returns only `{"status":"ok"}`.
* Missing and invalid keys fail without exposing a tenant.
* TypeScript, Python, multi root, blob, and loss fixtures use the same protocol.
* Repeating the same upload succeeds, while conflicting bytes return `409`.
* The service writes `complete.json` last and ignores incomplete prefixes.
* One installation key cannot write into another installation.
* Logs contain only the approved identifiers and counts.
* Capacity, ingest error, authentication, validation, and budget alerts work.

Record the image digest, Cloud Run revision, Terraform configuration, quota
check, registry version, smoke bundle IDs, and previous revision in the private
release record.

## Roll back

Send all traffic to the previous working revision. Keep the failed revision,
stored bundles, incomplete uploads, registry versions, and logs while you
investigate.

```sh
gcloud run services update-traffic "<SERVICE_NAME>" \
  --region="$SL_REGION" \
  --project="$SL_PROJECT_ID" \
  --to-revisions="<PREVIOUS_REVISION>=100"
```

After rollback, test health, invalid authentication, one repeated upload, and
bundle validation. A service rollback must not delete stored data.
