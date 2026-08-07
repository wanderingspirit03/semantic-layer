# Semantic Layer OpenClaw setup

`semantic-layer-openclaw-setup` installs and checks the
`semantic-layer-openclaw` runtime plugin. The setup package stays separate from
the runtime because installation work must not run inside the Gateway process.

Use the [OpenClaw setup guide](../../docs/openclaw/setup.md) for the complete
container and service flows.

## Container commands

Install the pinned pilot release:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.8 setup \
  --container \
  --endpoint "<SEMANTIC_LAYER_ENDPOINT>" \
  --service-name "<SERVICE_NAME>" \
  --installation-id "<INSTALLATION_ID>"
```

Restart the container through its platform, then check the live installation:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.8 doctor --container
```

Remove only the Semantic Layer installation:

```sh
npx -y semantic-layer-openclaw-setup@0.1.0-pilot.8 uninstall --container --acknowledge-external-restart
```

Setup and key rotation ask for the ingestion key in a hidden prompt. They do
not accept a key on the command line. Each OpenClaw installation needs its own
installation ID and key.

Container setup preserves all Gateway values and all other plugins. It does not
restart the container. Doctor is read only. It checks the configured local
Gateway port without putting the Gateway secret on the command line. Uninstall
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
