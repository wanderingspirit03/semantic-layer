# Semantic Layer trace CLI

This command lets approved team members find, download, and inspect completed
Semantic Layer traces in Google Cloud Storage. It is read only. It cannot
upload, change, or delete cloud data.

The npm package is public. Trace access is not public. Google Cloud IAM decides
which people can read the evidence bucket.

The pilot supports macOS and Linux. Trace files contain private plaintext.
Keep the local output directory outside source repositories and shared folders.

## Install

Use Node.js 22 or a supported newer version.

```sh
npm install --global semantic-layer-traces@0.1.0-pilot.1
```

Sign into Google Cloud with your work account:

```sh
gcloud auth application-default login
```

The CLI uses Application Default Credentials. It does not store Google
credentials and does not use customer ingestion keys.

By default, config is stored under the operating system config directory. Set
`SEMANTIC_LAYER_TRACES_CONFIG` to an absolute file path when a coding agent or
temporary test needs a separate config file.

## First setup

Save the nonsecret staging settings:

```sh
semantic-layer-traces configure staging \
  --project "<GCP_PROJECT_ID>" \
  --bucket "<EVIDENCE_BUCKET>" \
  --output "<PRIVATE_LOCAL_DIRECTORY>"
```

Use a different output directory for each environment. The CLI creates the
config file and local output with owner only permissions.

Check access before downloading anything:

```sh
semantic-layer-traces doctor --environment staging
```

`doctor` makes no cloud changes. It checks the effective object permissions
for the signed in identity. A team owner may see a warning about broad
permissions. Normal users should receive only object list and read access
through the trace reader group.

## Find and sync traces

Find tenant IDs:

```sh
semantic-layer-traces tenants --environment staging
```

Find the OpenClaw installations for one tenant:

```sh
semantic-layer-traces installations \
  --environment staging \
  --tenant "<TENANT_ID>"
```

Each installation ID represents one OpenClaw installation. Two or more VMs
remain separate when setup gives each VM its own installation ID. Do not clone
one VM credential file onto another VM.

Download every completed bundle for the tenant:

```sh
semantic-layer-traces sync \
  --environment staging \
  --tenant "<TENANT_ID>"
```

Sync is safe to repeat. It skips a matching local bundle. It never replaces a
conflicting local bundle. It downloads one bundle at a time into a private
temporary directory, checks the completion digest, validates the trace, checks
the bundle identity, and then moves the finished directory into place.

## Inspect local traces

Show safe summaries for one tenant:

```sh
semantic-layer-traces list \
  --environment staging \
  --tenant "<TENANT_ID>"
```

This command does not print prompts, reasoning, model output, tool input, tool
output, outcome summaries, or error messages. It does show safe capture loss
reason codes and whether token usage was captured, so a zero does not hide a
missing usage field.

Show one safe summary:

```sh
semantic-layer-traces show \
  "<TENANT_ID>/<INSTALLATION_ID>/<BUNDLE_ID>" \
  --environment staging \
  --summary-only
```

An interactive `show` prints private trace content by default after a warning.
When output is redirected or `--json` is used, content stays hidden unless you
pass `--include-content`.

Do not use `--include-content` in scheduled jobs or shared logs.

## Use from a coding agent

A coding agent needs no separate prompt or skill. Ask it to run:

```sh
semantic-layer-traces --help
```

The root help prints the complete setup and use flow in order. Every command
also supports `--help`, and every read command supports `--json`.

A safe agent request is:

```text
Use semantic-layer-traces to discover the staging tenants, sync the selected
tenant, and report trace summaries. Start by reading the CLI help. Do not print
private trace content.
```

## Local files and cleanup

The CLI writes bundles as:

```text
<OUTPUT_DIRECTORY>/<TENANT_ID>/<INSTALLATION_ID>/<BUNDLE_ID>/
```

Each bundle contains only its original `manifest.json`, `trace.jsonl`, and any
original `blobs` directory. The CLI does not add state files to a bundle.

The pilot has no delete command. When local review is complete, remove local
copies with your normal operating system process and follow the agreed
retention policy. Removing a local copy does not remove the GCP copy.

## Errors

Exit code `0` means the command succeeded. Exit code `1` means cloud access,
download, validation, or local storage failed. Exit code `2` means the command
arguments are invalid.

If a local bundle conflicts with GCP, move that exact local bundle directory
aside and run sync again. The CLI will not overwrite it.
