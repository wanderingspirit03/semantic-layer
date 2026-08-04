# Semantic Layer cloud ingest

`@semantic-layer/cloud-ingest` is the private Cloud Run receiver. It accepts
sealed Semantic Trace bundles and stores their exact bytes in private Google
Cloud Storage. It is not an installation package or a read API.

## Runtime interface

The service reads these environment variables:

* `SEMANTIC_LAYER_BUCKET` names the immutable evidence bucket.
* `SEMANTIC_LAYER_METER_BUCKET` names the mutable admission meter bucket.
* `SEMANTIC_LAYER_KEY_REGISTRY_JSON` maps lowercase ingestion key digests to
  tenant and installation scopes.
* `PORT` sets the HTTP port. Cloud Run supplies it.

The registry has this shape:

```json
{
  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef": {
    "tenant_id": "tenant_customer1_prod",
    "installation_id": "install_AAAAAAAAAAAAAAAAAAAAAA",
    "status": "active"
  }
}
```

The service derives both storage IDs from the matched key digest. A request or
manifest cannot select another scope. Set `status` to `revoked` to reject an
installation.

Tenant only string entries support legacy reads of an existing manifest v1
completion. They cannot begin, upload, complete, or delete data.

## Storage interface

The service writes these prefixes:

```text
tenants/<tenant>/installations/<installation>/bundles/<bundle>/
uploads/tenants/<tenant>/installations/<installation>/bundles/<bundle>/
metering/tenants/<tenant>/installations/<installation>/ledger.json
```

The evidence bucket contains immutable upload and bundle objects. The separate
metering bucket contains admission counters, bundle leases, and deletion
tombstones that require conditional updates.

Before it creates upload state, the service reserves declared bytes in the
installation ledger. The defaults allow 64 active uploads and 16 GiB of
incomplete bytes for each installation. A full reservation returns `429`
without creating upload state.

The [ingest protocol](../../contracts/ingest/v1/README.md) is the only exact
source for routes, request fields, digests, limits, responses, and completion
rules.

## Private command interface

The built package includes `semantic-layer-ingest-ops`. Maintainers can list,
fetch, validate, and delete exact bundle scopes, inspect meter state, and clear
an abandoned lease after ingress has stopped. The
[operations guide](../../docs/maintainers/cloud/operations.md) defines the safe
command sequence.

## Checks

```sh
pnpm --filter @semantic-layer/cloud-ingest test
pnpm --filter @semantic-layer/cloud-ingest typecheck
pnpm --filter @semantic-layer/cloud-ingest build
```
