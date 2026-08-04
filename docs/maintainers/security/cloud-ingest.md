# Cloud ingest security model

The cloud ingest security model covers upload from a local
`semantic-layer-cloud` spool, through Cloud Run, to private Google Cloud Storage.
The [local capture security model](local-capture.md) covers capture, credential
removal, local writing, and sealing.

## Assets

The main assets are sealed trace bundles, pending local bundles, ingestion keys,
the hashed key registry, tenant and installation assignments, metering state,
completion markers, audit records, and service logs.

Trace bundles can contain prompts, model output, reasoning, tool data, errors,
source code, personal data, and business data. Credential removal does not make
the content anonymous or safe to share.

`complete.json` is the cloud completion marker. A stored prefix without that
file is an incomplete upload and cannot be used as a completed trace.

## Trust seams

```text
installation host       public HTTPS             Google Cloud
sealed bundle  ->  local spool  ->  Cloud Run  ->  private storage
```

The installation owner account is trusted for local file access. The service
treats trace bytes, manifests, paths, IDs, request headers, and network requests
as untrusted.

Cloud Run is public so installations can upload from their hosts. Only
`/health` is open without an application key. Google Cloud Storage and the
private operations tool are not public.

Staging and production use different projects, buckets, service accounts, key
registries, and installation keys. A tenant ID is a storage name and does not
grant access.

The evidence bucket stores create only bundle objects. A separate metering
bucket stores mutable admission counters and bundle leases. Separating the two
prevents meter updates from weakening evidence immutability.

## Enforced controls

Each installation and environment has a random write only ingestion key. The
client supplies the key in memory or through `SEMANTIC_LAYER_INGEST_KEY`. The
public CLI does not accept the key as an argument.

The service stores only lowercase SHA 256 key digests in Secret Manager. It
derives the tenant and installation from the matched registry entry. Values in
the URL, body, or manifest cannot choose storage scope. A manifest v2
installation must match the registry entry. An ingestion key cannot read or
list data.

The receiver accepts only the files and limits defined by the
[ingest protocol](../../../contracts/ingest/v1/README.md). It checks paths,
identifiers, sizes, part and file digests, the bundle digest, manifest equality,
schemas, record sequence, references, counts, and blobs before completion. The
receiver does not change bundle bytes or add framework meaning.

Google Cloud Storage creates every temporary and final object only when the
object does not already exist. Repeating the same content returns the previous
success. Different content for the same identity returns `409`. The service
writes `complete.json` after every other object, and readers ignore incomplete
prefixes.

The evidence and metering buckets are private and located in `europe-west2`.
Public access prevention and uniform bucket access are active. Google manages
encryption at rest. Staging and production do not share buckets or credentials.

The runtime service account can create and read evidence objects. It can create,
read, and conditionally update the separate metering ledger. It can read only
its key registry secret. Maintainer access is separate from runtime access.

Admission limits reserve active uploads and incomplete bytes for each
installation before an upload begins. The service reports capacity and pressure
using identifiers and counts. Cloud Run limits, Google Cloud quota, and budgets
add bounds outside the application.

The uploader stores pending bundles in an owner only local spool. It retries
network failures, `429`, and server errors, and it pauses after `401` or `403`.
It quarantines an immutable conflict. Upload failure cannot change an
application result. At spool capacity, local capture and sealing continue while
new upload admission stops.

Logs contain only request IDs, service assigned tenant and installation IDs,
counts, pressure state, status, and latency. Error responses contain a bounded
code and request ID. The service does not log keys, key digests, manifests,
trace content, caller supplied bundle IDs, or raw exceptions.

Completed and incomplete objects have no automatic deletion, versioning, or
soft delete. An exact bundle deletion requires a dry run, full scope
confirmation, an approval reference, and an immutable audit object. A bundle
lease prevents deletion while an ingest request is active. Preparing deletion
creates a permanent tombstone, so later writes for that bundle return `409`.

A lease does not expire from age. Lease recovery requires stopped or revoked
ingress, proof that no request remains active, exact scope confirmation, and a
recovery audit. The recovery command stops if the lease set changes.

## Remaining risk

A stolen ingestion key can upload valid bundles into its assigned installation
until the key is revoked. Storing only a digest does not protect a weak key, so
each key must contain enough random data.

A valid installation can submit false trace content. Immutable storage proves
that stored bytes did not change, but it does not prove that those bytes describe
the application truthfully.

Cloud ingest does not provide anonymous traces, separate content encryption, a
customer read API, a customer delete API, live trace streaming, regional
failover, or protection from a compromised installation host, Google Cloud
administrator, runtime service, or valid key holder.

Public Cloud Run ingress exposes `/health` and the authenticated upload routes
to internet traffic. Request limits, instance limits, quota, budget alerts, and
monitoring reduce resource use but do not remove denial of service risk.

Confirmed deletion cannot be recovered from the evidence bucket. The private
operator role has broad bucket access, so the command checks, immutable audit
record, Google Cloud audit log, and protected operator credentials remain
important controls.
