# Operate cloud ingest

Semantic Layer maintainers use this guide to check the hosted ingest service,
investigate failures, rotate keys, and remove one exact bundle. Installation
owners use the uploader status and doctor commands instead of Google Cloud or
the private operations tool.

## Check service health

A healthy environment has successful Cloud Run uploads in `europe-west2` and
completed bundles within the expected delivery time. Persistent authentication,
validation, conflict, capacity, or server errors require investigation.

Cloud Logging may contain only request IDs, server assigned tenant and
installation IDs, byte counts, admission counts, pressure state, HTTP status,
and latency. Do not log authorization headers, key hashes, manifests, trace
records, prompts, tool bodies, bundle paths, or raw exceptions.

Use placeholders when you run a query or share a command:

```sh
gcloud run services describe "<SERVICE_NAME>" \
  --region="europe-west2" \
  --project="<GCP_PROJECT_ID>"

gcloud logging read \
  'resource.type="cloud_run_revision" AND resource.labels.service_name="<SERVICE_NAME>" AND jsonPayload.status>=500' \
  --project="<GCP_PROJECT_ID>" \
  --limit=50
```

Keep staging and production queries separate. Store any detailed exports in the
approved private incident location.

## Monitor the service

Terraform creates alerts for server errors, repeated authentication failures,
validation conflicts, capacity rejection, meter pressure, and budget limits.
Confirm that the email notification channel works after each deployment.

Add environment specific alerts for Cloud Run latency, regional quota use, and
missing completed bundles from active installations. Installation hosts must
report the oldest pending bundle and local spool pressure, because Cloud Run
cannot see the local spool.

## Investigate upload failures

For a network error, `429`, or server error, check the Cloud Run revision, GCS,
quota use, error rate, and latency. Leave pending bundles in place, because the
uploader retries them. If the current revision caused the failure, roll back and
confirm that the oldest pending age starts to fall.

For a persistent `401` or `403`, check the endpoint and numeric Secret Manager
version without asking for the plain key. Confirm that the expected key hash is
active in the correct environment. Rotate the key if the registry or key may be
wrong.

For validation failure or `409`, preserve the local bundle and receipt. Use the
request ID and opaque bundle ID to find the request. A prefix without
`complete.json` is incomplete and must not be treated as a bundle. Compare the
declared and stored digests without editing either copy.

For spool pressure, restore the endpoint or key first. The default local limit
is 5 GiB, with warnings at 70 percent and 90 percent. At capacity the uploader
keeps existing state and stops admitting new uploads. Do not delete pending
bundles to clear the warning.

For quota pressure, compare current use with the dated deployment check and the
configured maximum instance count. Lower a rollout or request more quota before
the service reaches its limit. A change to CPU, memory, concurrency, or maximum
instances requires a staging deployment.

## Inspect stored bundles

The private `semantic-layer-ingest-ops` tool uses Application Default
Credentials and the two bucket environment variables. Run it only from an
approved maintainer workstation in the correct environment.

```sh
semantic-layer-ingest-ops list "<TENANT_ID>" "<INSTALLATION_ID>"
semantic-layer-ingest-ops list-incomplete "<TENANT_ID>" "<INSTALLATION_ID>"
semantic-layer-ingest-ops meter-status "<TENANT_ID>" "<INSTALLATION_ID>"
semantic-layer-ingest-ops fetch "<TENANT_ID>" \
  "<INSTALLATION_ID>" "<BUNDLE_ID>" "<PRIVATE_LOCAL_DIRECTORY>"
semantic-layer-ingest-ops validate "<TENANT_ID>" \
  "<INSTALLATION_ID>" "<BUNDLE_ID>"
```

`fetch` refuses an incomplete bundle and creates files that only the owner can
read. Remove the local copy under the incident data handling rules when you no
longer need it.

## Delete one bundle

Deletion is a dry run unless the command receives the exact tenant,
installation, and bundle confirmation:

```sh
semantic-layer-ingest-ops delete "<TENANT_ID>" \
  "<INSTALLATION_ID>" "<BUNDLE_ID>"

semantic-layer-ingest-ops delete "<TENANT_ID>" \
  "<INSTALLATION_ID>" "<BUNDLE_ID>" \
  --confirm="<TENANT_ID>/<INSTALLATION_ID>/<BUNDLE_ID>" \
  --approval="<APPROVAL_REFERENCE>"
```

The confirmed command writes an immutable audit object before it deletes the
exact completed and temporary object prefixes. It also updates the installation
meter. If an ingest request holds a live lease for the bundle, deletion stops.
All later upload requests for a prepared deletion return `409`.

A process crash can leave a lease behind. Do not decide that a lease is unused
from its age. Revoke the installation key or stop ingress, stop the affected
service instances, and confirm that no request remains active. Then clear the
exact lease set:

```sh
semantic-layer-ingest-ops clear-ingest-leases "<TENANT_ID>" \
  "<INSTALLATION_ID>" "<BUNDLE_ID>" \
  --confirm-drained="<TENANT_ID>/<INSTALLATION_ID>/<BUNDLE_ID>" \
  --approval="<APPROVAL_REFERENCE>"
```

The command writes a recovery audit and stops if the inspected lease set
changes. Run the normal delete command after the lease is cleared. Confirmed
deletion cannot be recovered from the evidence bucket because versioning and
soft delete are off.

## Rotate an ingestion key

Create a new random key and add its lowercase SHA 256 digest to a new registry
version. Keep the old digest active during the change. Update the numeric secret
version in Terraform, deploy the Cloud Run revision, and confirm that every
instance uses it.

Replace the key on the installation and upload a test bundle. After the upload
is acknowledged, remove the old digest in another registry version and deploy
that version. Confirm that the old key is rejected.

## Check stored data settings

The evidence and incomplete upload prefixes have no automatic deletion. Check
both buckets after each deployment or policy change. Versioning, soft delete,
and lifecycle rules must remain off.

Admission limits restrict active uploads and incomplete bytes for each
installation. Use `list-incomplete` and `meter-status` when capacity alerts
fire. Delete only an exact bundle with the command above. Never delete a broad
prefix to make room.
