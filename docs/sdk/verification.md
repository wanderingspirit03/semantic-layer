# Bundle validation

Validation checks whether a sealed bundle is internally consistent. It does not
decide whether the agent met its goal.

## Validate a bundle

TypeScript:

```ts
import { validateArtifact } from 'semantic-layer-capture';

const report = await validateArtifact('/path/to/run-...');
if (!report.valid) {
  console.error(report.issues);
}
```

Python:

```python
from semantic_layer.validation import validate_artifact

report = validate_artifact("/path/to/run-...")
if not report.valid:
    print(report.issues)
```

Both SDKs use the `structural` profile by default. Structural validation checks
schemas, sequence, identities, sources, backward references, manifest counts,
the trace digest, blob integrity, allowed files, local permissions, and secret
sentinels.

A structurally valid trace can still contain `loss` records. Read each loss to
understand which evidence was unavailable.

## Rich agent profile

Use `rich-agent` when the scenario should contain a complete root, model
operation, and tool operation:

```ts
const report = await validateArtifact(path, {
  profile: 'rich-agent',
});
```

```python
report = validate_artifact(path, profile="rich-agent")
```

The profile requires one root start and outcome, one model request and
response, and one tool call and result under the same root. It does not require
state, error, verification, or raw stream records.

Do not use the profile for a run that can truthfully end before a successful
model or tool result. For example, a provider error trace should remain valid
when it contains the attempted request and observed error.

## Scenario requirements

Callers can require evidence and source activity that a specific test or
workflow guarantees:

```ts
const report = await validateArtifact(path, {
  requiredEvidence: ['root', 'model', 'tool', 'delivery'],
  requiredSourceActivity: ['official:openai-agents-js'],
});
```

```python
report = validate_artifact(
    path,
    required_evidence=("root", "model", "tool", "delivery"),
    required_source_activity=("official:openai-agents-python",),
)
```

Source requirements use the exact source name from the manifest. Require only
evidence that the scenario guarantees. A model can choose a valid path without
a tool, so an optional tool choice must not become a validation failure.

Capture health is separate from bundle content. Check the status returned by
shutdown and validate only the sealed artifact path. A semantic loss does not
by itself mean that the capture runtime was unhealthy.

Repository contributors should follow the
[maintainer testing guide](../maintainers/testing.md).
