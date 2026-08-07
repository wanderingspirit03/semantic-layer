# Semantic ingest protocol v1

Protocol v1 uploads the exact files from one sealed Semantic Trace bundle. It
accepts manifest v1 or v2, while trace records remain v1. The protocol does not
depend on the framework, provider, or language that produced the bundle.

The uploader and receiver must pass the same protocol tests. The JSON schemas in
this directory define field names, required fields, value formats, and limits
for each field. The rules below relate those fields across a request or bundle.

## Authentication and request IDs

Every `/v1` request must use HTTPS and include this header:

```http
Authorization: Bearer <INGESTION_KEY>
```

The service hashes the key and looks up the tenant and installation in its
private registry. A URL, request body, or manifest cannot select another
storage scope. A manifest v2 `installation_id` must match the installation from
the registry.

Tenant only registry entries exist for legacy reads. A tenant only entry may
confirm an existing manifest v1 completion with the same digest, but it cannot
begin, upload, complete, or delete a bundle.

Every response includes `X-Request-Id`. The service returns a valid caller
supplied UUID in lowercase. If the caller does not supply a valid UUID, the
service creates one. An error response has this form, and its `request_id`
matches the response header:

```json
{"error":"<CODE>","request_id":"<UUID>"}
```

`GET /health` does not require authentication and returns only
`{"status":"ok"}`. A successful health request does not test a key, storage
access, or bundle upload.

Setup checks that a key belongs to its assigned installation without creating
upload state:

```http
POST /v1/auth/verify
Content-Type: application/json
Authorization: Bearer <INGESTION_KEY>

{"installation_id":"<INSTALLATION_ID>"}
```

The request and response use the auth verify schemas in this directory. A
matching active key and installation return `200` with `{"status":"ok"}`. An
invalid request returns `400`, and a valid installation ID that does not match
the key returns `403`. A legacy key that only identifies a tenant also returns
`403`.

## Accepted bundle

The service accepts a directory with these files:

```text
<bundle>/
├── manifest.json
├── trace.jsonl
└── blobs/
```

`blobs/` is optional. No other path is allowed. Both the uploader and receiver
require `manifest.state` to be `sealed`, a supported manifest discriminator,
and `record_schema` to be `semantic_trace_record_v1`.

The uploader sends each file without changing its bytes. It does not add a
receipt to the bundle or change trace records.

The protocol limits are:

* Each part is at most 8 MiB.
* Each file is at most 256 MiB.
* Each bundle is at most 512 MiB.
* Each bundle has at most 1,024 file descriptors.
* A begin request body is at most 1 MiB.
* A complete request body is at most 4 KiB.

## File descriptors

Each descriptor contains `file_id`, `path`, `size_bytes`, `sha256`, and
`parts`. The digest fields use lowercase SHA 256 values.

The `file_id` is the lowercase SHA 256 digest of the UTF 8 bundle path.
`manifest.json` and `trace.jsonl` are required paths. Every other path must be
contained under `blobs/`. Paths and file IDs must be unique. Empty files have
zero parts.

Files are split into consecutive 8 MiB parts. Each part except the final part
must be exactly 8 MiB. The final part contains the remaining declared bytes.
The declared part count must match the file size.

## Bundle digest

Start a SHA 256 hash with the UTF 8 bytes for
`semantic-layer-bundle-v1`, followed by one zero byte. Sort the descriptors by
the unsigned lexicographic order of their UTF 8 path bytes. Feed these values
for each descriptor without JSON encoding:

```text
decimal UTF 8 path byte length
":"
path UTF 8 bytes
"\0"
decimal size_bytes
"\0"
lowercase file sha256
"\0"
```

The canonical test input is supplied out of order:

```json
[
  {"path":"trace.jsonl","size_bytes":456,"sha256":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
  {"path":"blobs/z.bin","size_bytes":7,"sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
  {"path":"manifest.json","size_bytes":123,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
]
```

The expected bundle digest is
`366eb97f10e3b44880407c6b0c61aceb5b3ba37b09b3d0840f0d80a6dee7883b`.

Path ordering is case sensitive. The descriptors
`(blobs/a.bin, 1, d times 64)` and `(blobs/Z.bin, 2, e times 64)` produce
`745a11a0961b93ad9e26902ac0d8ae9648ccf73577e04740888532f625fb25a3`.
Uploader and receiver tests must cover both vectors.

## Begin a bundle

```http
POST /v1/bundles/<BUNDLE_ID>/begin
Content-Type: application/json
Authorization: Bearer <INGESTION_KEY>

<BEGIN_DOCUMENT>
```

The request uses `semantic-ingest-begin-request.schema.json`. The path ID must
equal `manifest.bundle_id`. The service validates the descriptors and computes
the bundle digest before it creates immutable upload state. The request contains
the parsed manifest and also describes `manifest.json` as an exact uploaded
file.

* `201` with `status: "begun"` means the service created upload state.
* `200` with `status: "exists"` means the same digest already began.
* `200` with `status: "complete"` means the same bundle is already complete.
* `409` means the bundle ID is already bound to different content.

## Upload file parts

```http
PUT /v1/bundles/<BUNDLE_ID>/files/<FILE_ID>/parts/<ZERO_BASED_INDEX>
Authorization: Bearer <INGESTION_KEY>
X-Semantic-Layer-Part-Sha256: <LOWERCASE_SHA256>
Content-Type: application/octet-stream

<EXACT_PART_BYTES>
```

The service checks the file ID, part index, byte length, and part digest before
it stores a part.

* `201` with `status: "created"` means the service stored a new part.
* `200` with `status: "exists"` means the same part already exists.
* `409` means an immutable object already contains different bytes.

## Complete a bundle

```http
POST /v1/bundles/<BUNDLE_ID>/complete
Content-Type: application/json
Authorization: Bearer <INGESTION_KEY>

{"protocol_version":"1","bundle_digest":"<LOWERCASE_SHA256>"}
```

The request uses `semantic-ingest-complete-request.schema.json`, and a success
response uses `semantic-ingest-complete-response.schema.json`. Before success,
the service checks all parts, file sizes, file digests, bundle digest, manifest
equality, selected manifest schema, and the trace record v1 contract.

* `201` with `status: "complete"` means the service created the completion.
* `200` with `status: "complete"` means the same bundle was already complete.
* `400` means the input is invalid or incomplete.
* `409` means the digest or an immutable object conflicts with stored data.

## Completion and retry rules

Temporary objects are stored under `uploads/`. Completed objects use this
layout:

```text
tenants/<TENANT_ID>/installations/<INSTALLATION_ID>/bundles/<BUNDLE_ID>/
├── manifest.json
├── trace.jsonl
├── blobs/
└── complete.json
```

Every object is created with the Google Cloud Storage equivalent of
`ifGenerationMatch=0`. The service writes `complete.json` last. Readers must
ignore a prefix that does not contain that file.

Parts and completion are immutable and idempotent for the same digest. The
uploader may move a spool entry from `pending` to `acked` only after it validates
the digest in the completion response.

After that durable move, an uploader may remove its acknowledged bundle copy
and keep the local acknowledgement receipt. A local `acked` entry is valid only
when its receipt names the expected bundle ID and digest. Missing or invalid
receipts must not authorize source cleanup.

Clients retry network errors, `429`, and `5xx` responses with delay and random
jitter. They pause after `401` or `403`, and they quarantine a `409` conflict.
They never edit a sealed bundle to make a rejected upload pass.
