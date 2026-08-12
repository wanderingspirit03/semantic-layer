# Semantic Layer OpenClaw setup

`semantic-layer-openclaw-setup` installs and checks the
`semantic-layer-openclaw` runtime plugin. The setup package stays separate from
the runtime because installation work must not run inside the Gateway process.

Use the [OpenClaw setup guide](../../docs/openclaw/setup.md) for the complete
container and service flows.

## Container commands

Install the pinned pilot release:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.11 setup \
  --container \
  --endpoint "<SEMANTIC_LAYER_ENDPOINT>" \
  --service-name "<SERVICE_NAME>" \
  --installation-id "<INSTALLATION_ID>"
```

The final config change can stop a foreground Gateway. If the machine stops,
start it through its platform. Run the same setup command again. The second run
uses the stored owner only key and does not ask for another key.

Then check the live installation. Pass the local Gateway URL when its runtime
port is not saved in the OpenClaw config:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.11 doctor \
  --container \
  --gateway-url "ws://127.0.0.1:3001"
```

Remove only the Semantic Layer installation:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.11 uninstall --container --acknowledge-external-restart
```

Setup and key rotation ask for the ingestion key in a hidden prompt. They do
not accept a key on the command line. Each OpenClaw installation needs its own
installation ID and key.

When OpenClaw traces must join another service, inject the shared customer
identity key as `SEMANTIC_LAYER_IDENTITY_KEY` for the first setup process. Use
a secret manager and do not put the value in a command or log. Setup removes
the variable before it starts OpenClaw child processes. A rerun rejects a
different identity key.

Container setup preserves every config value outside the fields that Semantic
Layer manages. This includes Gateway, Slack, Latitude, and all other plugins. It
installs against a private config first. It writes the owner only credentials
and setup state before the final config change can stop the foreground Gateway.
Doctor is read only. It checks the selected local Gateway port without putting
the Gateway secret on the command line. It also checks the plugin hooks and the
trusted correlation Gateway method. Uninstall
preserves local traces and the upload spool. The acknowledgement flag confirms
that the operator can restart the owning container or machine if the foreground
Gateway exits during removal. Uninstall commits the clean config before it
removes the package. If the container stops during removal, start it and run the
same uninstall command again.

## Service commands

Service managed hosts use the same setup and doctor commands without
`--container`. `rotate-key`, `status`, and `drain` are available for this flow.
Stop the OpenClaw Gateway before standalone `status` or `drain` because the live
plugin owns the spool.
