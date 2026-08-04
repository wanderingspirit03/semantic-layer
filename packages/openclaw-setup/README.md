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
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.1 dry-run
```

Install and configure the runtime plugin:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.1 setup \
  --endpoint "<SEMANTIC_LAYER_ENDPOINT>" \
  --service-name "<SERVICE_NAME>"
```

Check the live installation:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.1 doctor
```

Replace the ingestion key through a hidden prompt:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.1 rotate-key
```

`status` reports queued uploads, and `drain` makes one upload attempt. Stop the
OpenClaw Gateway before running either command.

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.1 status
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.1 drain
```

Setup and key rotation ask for the ingestion key without showing it. The
commands do not accept a key on the command line.
