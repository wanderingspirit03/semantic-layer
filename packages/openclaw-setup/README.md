# Semantic Layer OpenClaw setup

`semantic-layer-openclaw-setup` installs and checks the
`semantic-layer-openclaw` runtime plugin. The setup package is separate from the
runtime package because installation and maintenance commands should not run
inside the OpenClaw plugin process.

Use the [OpenClaw setup guide](../../docs/openclaw/setup.md) for the required
values and the complete installation steps.

## Commands

Check what setup would do without changing the host:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 dry-run
```

Install and configure the runtime plugin:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 setup \
  --endpoint "<SEMANTIC_LAYER_ENDPOINT>" \
  --service-name "<SERVICE_NAME>" \
  --installation-id "<INSTALLATION_ID>"
```

Check the live installation:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 doctor
```

Replace the ingestion key through a hidden prompt:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 rotate-key
```

`status` reports queued uploads, and `drain` makes one upload attempt. Stop the
OpenClaw Gateway before running either command.

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 status
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.2 drain
```

Setup and key rotation ask for the ingestion key without showing it. The
commands do not accept a key on the command line. Each host must use the
installation ID and ingestion key assigned to that host. The first setup
requires `--installation-id`. A setup rerun keeps the existing installation ID
and rejects a different assigned ID. Setup and doctor also ask the ingest
service to verify that the key belongs to the stored installation ID.
