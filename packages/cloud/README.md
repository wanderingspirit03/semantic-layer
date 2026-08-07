# semantic-layer-cloud

`semantic-layer-cloud` queues sealed Semantic Trace bundles on local disk and
uploads their exact bytes. It works with manifest v1 and v2 bundles that use
trace record schema v1. The package requires Node.js 22, Node.js 24, or
Node.js 25.9 or later.

## Install

```sh
npm install /path/to/semantic-layer-cloud-<version>.tgz
export SEMANTIC_LAYER_INGEST_KEY='<INSTALLATION_KEY>'
```

The CLI has no key argument. Supply the key through the environment or pass it
in memory to the library.

## Library interface

```ts
import { createCloudUploader } from 'semantic-layer-cloud';

const uploader = createCloudUploader({
  endpoint: 'https://semantic-ingest.example.run.app',
  installationId: process.env.SEMANTIC_LAYER_INSTALLATION_ID,
  spoolDirectory: '.semantic-layer/cloud-spool',
});

const receipt = await uploader.enqueueArtifact(sealedArtifactPath);
const result = await uploader.flush({ deadlineMs: 10_000 });
const status = uploader.status();
await uploader.shutdown();
```

`enqueueArtifact()` validates and copies a sealed bundle into the local spool.
The copy is complete before the promise resolves, so the network can be offline.
The source bundle is not changed. A returned `pending` state means the complete
bundle is in durable pending storage. A returned `acked` state means a matching
acknowledgement receipt already exists. An `awaiting_spool_admission` state does
not authorize the caller to remove its source bundle.

A host adapter that owns a dedicated trace output directory can pass
`removeSourceAfterAdmissionFrom` to `enqueueArtifact()`. The uploader then
removes only a direct, regular child of that directory, and only after a
complete pending copy or matching acknowledgement exists. Unsafe paths,
staging failures, and a full spool leave the source bundle in place.

`flush()` uploads pending bundles until the deadline. `status()` returns bundle
and byte counts, spool pressure, the oldest pending bundle, retry state,
authentication pause state, quota state, the latest request ID, and safe failure
details. `shutdown()` stops background work and leaves stored bundles in place.

`createCloudUploader()` accepts `endpoint`, `ingestKey`, `installationId`,
`spoolDirectory`, `maxSpoolBytes`, `concurrency`, and a Fetch compatible
function. The key and installation ID may come from
`SEMANTIC_LAYER_INGEST_KEY` and `SEMANTIC_LAYER_INSTALLATION_ID`. Concurrency
defaults to 2 and cannot be greater than 2.

Callers in one process can open the same resolved spool when their options
match. Each caller receives a lease over the same queue and retry loop. The last
caller to shut down releases the spool. Different options return
`SPOOL_CONFIG_CONFLICT`, while another process that owns the spool returns
`SPOOL_IN_USE`.

## CLI interface

```sh
semantic-layer-cloud doctor --endpoint https://semantic-ingest.example.run.app
semantic-layer-cloud upload /path/to/bundle --endpoint https://semantic-ingest.example.run.app
semantic-layer-cloud watch .semantic-layer/traces --endpoint https://semantic-ingest.example.run.app
semantic-layer-cloud status --spool-directory .semantic-layer/cloud-spool
```

Set `SEMANTIC_LAYER_ENDPOINT` when you want to omit `--endpoint`. `upload` waits
up to five minutes for acknowledgement by default. `watch` queues each new
bundle directory until the process stops. The standalone `status` command needs
the spool owner lock, so the live uploader process must be stopped first.

## Local queue

The uploader keeps queued bundles under `spoolDirectory`, and files are private
to the local owner. Keep `.semantic-layer/` out of source control. The default
queue limit is 5 GiB. A full queue stops new upload admission without deleting
stored bundles.

After the ingest service acknowledges the exact bundle digest, the uploader
keeps `receipt.json` and removes the acknowledged bundle bytes from its spool.
It checks old acknowledgement directories again when it starts. A missing or
invalid receipt never counts as an acknowledged bundle and does not authorize
cleanup.

The [ingest protocol](../../contracts/ingest/v1/README.md) defines HTTP
requests, retry behavior, digests, limits, and completion rules.

## Checks

```sh
pnpm --filter semantic-layer-cloud test
pnpm --filter semantic-layer-cloud typecheck
pnpm --filter semantic-layer-cloud build
```
