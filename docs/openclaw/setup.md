# OpenClaw setup

Semantic Layer supports two OpenClaw setups. Use container mode when OpenClaw
Gateway is the container's foreground process. Use service mode when OpenClaw
manages Gateway as a host service.

OpenClaw `2026.5.5` and `2026.7.1-2` are fully tested. Setup also accepts later
stable versions from `2026.5.5`, but those versions are not fully qualified
until they have passed the same complete trace test.

Node.js support follows the OpenClaw release. OpenClaw releases before
`2026.5.12` need Node.js 22.14.0 or later on Node 22, or Node.js 24.0.0 or later
on Node 24. Releases from `2026.5.12` need Node.js 22.16.0 or later. Releases
from `2026.5.18` need Node.js 22.19.0 or later. Releases from `2026.7.1` need
Node.js 22.22.3 or later on Node 22, or Node.js 24.15.0 or later on Node 24.
Node.js 25.9.0 or later is also supported by the package contract.

## Values you need

Arcus provides three values for each OpenClaw installation:

1. The HTTPS ingest endpoint.
2. A separate installation ID for this OpenClaw installation.
3. A separate ingestion key for the same installation ID.

Choose a service name that helps your team identify the installation. Do not
use a person's name, email address, or account ID.

The setup command asks for the ingestion key in a hidden prompt. The key is not
accepted as a command option and is not printed. If you have several VMs or
containers, run setup once on each one with its own installation ID and key.

## Container installation

The pinned container pilot uses setup package `0.1.0-pilot.5` and runtime
plugin `0.1.0-pilot.2`.

Run this command inside the OpenClaw container as the same user that runs
OpenClaw:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.5 setup \
  --container \
  --endpoint "<SEMANTIC_LAYER_ENDPOINT>" \
  --service-name "<SERVICE_NAME>" \
  --installation-id "<INSTALLATION_ID>"
```

Paste the matching ingestion key when the hidden prompt appears.

Setup first checks the key and installation ID with the ingest service. It does
not change the host if this check fails. After the check succeeds, setup does
the following work:

1. It installs the pinned Semantic Layer plugin through OpenClaw.
2. It stores the key and installation identity in owner only files.
3. It enables only the Semantic Layer plugin entry and its secret reference.
4. It validates the final OpenClaw configuration.
5. It confirms that every Gateway value and every other plugin entry stayed the
   same.

Setup checks the Semantic Layer credential file and secret references. It does
not fail because of an existing secret warning in another part of the customer
config.

Container setup does not change Gateway mode, bind address, host, port, or any
other Gateway value. It does not disable Latitude or another plugin. It does
not call a service manager and does not restart the container.

A successful setup ends with output like this:

```text
Plugin: /customer/.openclaw/npm/node_modules/semantic-layer-openclaw
Credentials: /customer/.openclaw/semantic-layer/credentials.json
Local traces: /customer/.openclaw/semantic-layer/traces
Durable upload spool: /customer/.openclaw/semantic-layer/cloud-spool
Upload spool limit: 1073741824 bytes
OK. Restart the container, then run doctor --container.
```

The exact root follows the active OpenClaw home or state directory. Setup
prints the resolved paths. On the Tavi pilot container, that root is normally
`/customer/.openclaw` on the persistent volume.

Restart the container through the platform that owns it. Do not run an
OpenClaw service restart for this container flow.

After Gateway is healthy, run:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.5 doctor --container
```

Doctor is read only. It checks the pinned plugin identity, persistent plugin
location, plugin hooks,
credentials, installation state, file permissions, endpoint authentication,
local spool, the plugins that were enabled before setup, and the running
Gateway. It does not require a local bind or service manager, and it does not
create a trace.

The final success line is:

```text
OK OpenClaw 2026.5.5; Node 24.14.0; exact-qualified host; plugin runtime and owner-only credentials verified; container Gateway healthy.
```

The Node.js version in this example is the Tavi pilot version. The command
prints the version that is actually running.

## Confirm one complete trace

Run one normal OpenClaw task after doctor succeeds. Ask the agent to use one
tool and return a final answer. For example, ask it to list the files in a new
empty test folder and explain what it found.

The task must finish normally. Run doctor again and send the installation ID
to the Arcus pilot contact. Arcus confirms that the complete bundle arrived for
that installation. The OpenClaw installation cannot read the managed cloud
bucket directly.

For this pilot, Nick at Arcus confirms the first trace and handles setup
failures through the existing shared pilot channel. Send the command output and
installation ID. Never send the ingestion key.

Semantic Layer captures the prompts, model responses, provider exposed
reasoning, tool inputs, tool results, usage, errors, and run relationships that
OpenClaw exposes. Known credentials are scrubbed before a trace is sealed. A
trace can still contain private or personal content, so customer approval is
required before the pilot starts.

## Safe repeated setup

You can run the same setup command again with the same values. Setup checks the
existing owner state and pinned plugin instead of creating a second install.
It stops if the endpoint, service name, installation ID, plugin package, or
plugin version differs.

An upgrade uses a new pinned setup release and a separate upgrade procedure.
The pilot command does not replace an existing plugin with a different version.

Setup builds and validates a temporary complete config before it replaces the
active config. This works with valid OpenClaw files that rely on channel
defaults. If setup fails after it starts changing the host, it removes the new
Semantic Layer plugin and restores the original config file directly. The
original bytes, including comments, formatting, and the Gateway token, are
restored. It also restores credentials and installation state. The command
returns a nonzero status and does not print the ingestion key.

## Local storage and cleanup

The OpenClaw pilot spool limit is 1 GiB. Doctor warns at 70 percent and fails
when the spool is full. During an outage, pending traces stay on disk and the
agent continues normally.

The runtime seals a trace in the local traces directory. It removes that first
copy only after the uploader has copied the complete bundle into durable
pending storage or has found a valid acknowledgement receipt. After the cloud
service acknowledges the exact bundle digest, the uploader keeps a small local
receipt and removes the second bundle copy from the spool. Invalid or incomplete
local state is retained for investigation and is never treated as an
acknowledgement.

## Remove a container installation

Run this command inside the container:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.5 uninstall --container
```

The command removes the Semantic Layer plugin, its config entry, its secret
provider, credentials, and installation state. It preserves Gateway settings,
Latitude, every other plugin, local traces, and the upload spool. It prints the
installation ID that Arcus must revoke and does not restart the container.

Restart the container through its platform. Confirm that OpenClaw and the other
plugins still work. Send the printed installation ID to the Arcus pilot contact
if the key must be revoked or uploaded pilot traces must be deleted. Arcus uses
that exact installation ID to limit the cloud deletion request.

## Service managed installation

For a host where OpenClaw manages Gateway as a service, omit `--container` from
setup and doctor. Service mode keeps Gateway on the local host, performs the
OpenClaw security audit, restarts Gateway once, and completes doctor itself.

Standalone `status` and `drain` commands require the service managed Gateway to
be stopped because the live plugin owns the spool. They are not part of the
container operator flow.
