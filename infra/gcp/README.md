# Google Cloud infrastructure

The Terraform module deploys one Semantic Layer ingest environment. Use a
separate project, state backend, and variables file for staging and production.

## Module interface

[`variables.tf`](variables.tf) defines all inputs. The example variables files
show their shape and must not contain private environment values. Important
inputs include globally unique bucket names, an immutable image digest, a
numeric key registry version, billing settings, alert recipients, and operator
identities.

[`outputs.tf`](outputs.tf) returns the ingest URL, bucket names, registry secret,
runtime service account, Artifact Registry repository, build source bucket, and
build service account. Deployment and operations use these outputs instead of
reconstructing resource names.

The module creates:

* One Cloud Run service in `europe-west2`.
* Separate evidence, metering, and build source buckets.
* Public access prevention and uniform bucket access.
* Dedicated runtime and build service accounts.
* An explicitly versioned Secret Manager key registry.
* Artifact Registry and the required build permissions.
* Ingest, authentication, validation, capacity, meter, and budget alerts.

The evidence bucket contains immutable bundle objects. The metering bucket
contains counters, leases, and deletion tombstones that require conditional
updates. Completed and incomplete bundle objects have no lifecycle deletion,
versioning, or soft delete.

The [deployment guide](../../docs/maintainers/cloud/deployment.md) explains the
project checks, image build, Terraform apply, staging test, and rollback. The
[operations guide](../../docs/maintainers/cloud/operations.md) explains service
checks, key rotation, bundle inspection, and exact deletion.

## Checks

```sh
terraform -chdir=infra/gcp fmt -check
terraform -chdir=infra/gcp init -backend=false
terraform -chdir=infra/gcp validate
node infra/gcp/test/config.test.mjs
node infra/gcp/test/build-context.test.mjs
```

The checks validate the module and its policy assertions. They do not verify a
live project, quota, image, registry version, or endpoint.
