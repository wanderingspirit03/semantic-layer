# Privacy and security

The SDK writes plaintext traces for the local user. It scrubs supported
credentials, but it does not make trace content anonymous or encrypted.

## Credential scrubbing

The SDK scans supported credential formats and exact secret values supplied at
initialization. It does not read every environment variable to find secrets.

Pass any legacy, custom, or unknown credential through `secretValues` in
TypeScript or `secret_values` in Python:

```ts
initialize({
  serviceName: 'support-agent',
  secretValues: [configuredApiKey],
});
```

```python
initialize(
    service_name="support-agent",
    secret_values=[configured_api_key],
)
```

Each configured value must contain at least eight UTF8 bytes. Short values are
rejected because scanning for them would remove common trace text. The SDK does
not write accepted secret values to the manifest.

Before sealing, the SDK scans parsed JSON and JSONL values. It also scans opaque
blob bytes and checks rendered files for encodings of configured secrets.
Content addressed blob paths stay inside the bundle.

Credential scrubbing uses high confidence patterns so it does not remove
ordinary identifiers that happen to start with `sk-`. Supply an exact secret
value when its format is not in the built in scanner.

## Data that remains

The implemented privacy mode is `local-rich`. A trace can contain prompts,
model responses, reasoning, tool input and output, source code, file paths,
errors, state, personal data, and business data.

Capture protects files with owner only permissions where the operating system
supports them. The local user, the application process, and other processes
running as that user remain inside the trust boundary. The SDK does not protect
against a compromised host, administrator access, or a secret in an unsupported
format that was not supplied explicitly.

Capture pressure, unsafe omissions, and persistence gaps become `loss` records
when safe persistence is still possible. A loss explains missing trace
evidence. It does not make the remaining content safe to share.

## Safe use

- Add `.semantic-layer/` to the application's ignore file.
- Use synthetic data in public examples and tests.
- Review every bundle file before sharing a trace.
- Supply exact known credentials during controlled capture.
- Delete local traces under the application's data rules.

The optional cloud package accepts only sealed, validated, credential scrubbed
bundles. Upload does not make the trace anonymous. The
[local threat model](../maintainers/security/local-capture.md) and
[cloud threat model](../maintainers/security/cloud-ingest.md) define the full
security boundaries.
