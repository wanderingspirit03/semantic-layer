# OpenClaw setup

Semantic Layer exactly qualifies OpenClaw `2026.5.5` and `2026.7.1-2`. Setup
also accepts other stable releases from `2026.5.5` onward, then doctor checks
their live plugin hooks and configuration. These hosts remain unqualified until
they are exercised directly. See the [integration list](../sdk/integrations.md)
for the qualified versions.

Use a Node.js version supported by both packages. OpenClaw `2026.7.1`,
`2026.7.1-1`, and `2026.7.1-2` require Node.js `22.22.3` or newer on Node 22,
`24.15.0` or newer on Node 24, or `25.9.0` or newer. Setup applies the known
lower Node.js floors required by earlier OpenClaw releases.

OpenClaw currently requires the managed Semantic Layer ingest endpoint. You
need the following values before you start:

- The HTTPS ingest endpoint.
- A separate ingestion key for this OpenClaw host.
- The installation ID assigned to the same ingestion key.
- A service name that identifies the host in traces. Do not put a person's
  name, email address, or account ID in the service name.

The setup command asks for the ingestion key without showing it. Do not put
the key in the command or in an OpenClaw configuration file. Each host must
use its own ingestion key and installation ID. If you add another host later,
ask for one new key and installation ID for that host.

The first setup requires `--installation-id`. Later setup runs keep the stored
installation ID. If you pass a different ID on a later run, setup stops without
replacing it.

## Install Semantic Layer

Run the following command on the OpenClaw host. Replace the endpoint, service
name, and installation ID with the values for the installation.

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 setup \
  --endpoint "<SEMANTIC_LAYER_ENDPOINT>" \
  --service-name "<SERVICE_NAME>" \
  --installation-id "<INSTALLATION_ID>"
```

Setup performs the following work:

- It checks the OpenClaw and Node.js versions.
- It checks that the endpoint accepts the assigned ingestion key and
  installation ID, then asks for the key without showing it.
- It installs and enables the Semantic Layer plugin.
- It stores the key in a private file and puts a reference in the OpenClaw
  configuration.
- It stores the assigned installation ID and refuses to replace it with a
  different ID on a later setup run.
- It keeps the Gateway on the local host, restarts it once, and checks the
  completed installation.

Setup changes the Semantic Layer plugin entry. It does not disable Latitude or
other installed plugins.

Semantic Layer captures model messages, tool activity, usage, and reasoning
that the provider exposes to OpenClaw. It scrubs known credentials before it
seals and uploads a trace. A trace can still contain private or personal
content.

## Check the installation

Run doctor after setup and whenever you need to check the installation.

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 doctor
```

Doctor checks the installed version, plugin state, file permissions, OpenClaw
configuration, Gateway, endpoint health, and the exact ingestion key and
installation ID pair. It does not create a trace or print the key.

Next, run one synthetic task through the normal OpenClaw interface. Use a
reasoning model and ask it to use at least one tool. For example, ask it to list
the files in an empty test folder, explain what it found, and return a final
answer. Run doctor again after OpenClaw finishes the task. The task must return
normally, and doctor must exit successfully with an `OK` result. Confirmation
that the managed service stored the trace is a separate cloud check because the
OpenClaw host has no read access to cloud storage.

Stop if setup or doctor fails. Also stop if OpenClaw no longer starts, the
Gateway is no longer limited to the local host, or a key appears in terminal
output. Keep the error output and fix the reported problem before you run setup
again. Do not guess configuration field names or place the key directly in the
configuration.

## Change or remove the installation

Rotate the ingestion key with the following command. The command asks for
the new key without showing it and checks the key before replacing the old one.

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 rotate-key
```

Disable capture without removing the package:

```sh
openclaw plugins disable semantic-layer-openclaw
openclaw gateway restart
```

Review the uninstall first, then remove the package:

```sh
openclaw plugins uninstall semantic-layer-openclaw --dry-run
openclaw plugins uninstall semantic-layer-openclaw
```

Disabling or uninstalling the plugin does not delete traces that already exist
on the host or in the managed service.
