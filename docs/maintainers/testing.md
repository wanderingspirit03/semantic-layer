# Testing changes

Tests should prove observable capture behavior and the bundle produced by the
public interface. A callback mapping test alone does not prove an integration.

## Focused checks

Validate the committed format examples:

```sh
node contracts/trace/v1/validate-examples.mjs
node contracts/trace/v2/validate-contract.mjs
```

Run the writer and validator tests:

```sh
pnpm --filter semantic-layer-capture exec vitest run \
  tests/semantic_trace_writer.test.ts \
  tests/semantic_trace_validation.test.ts

uv run --project packages/python --extra dev pytest \
  packages/python/tests/unit/test_semantic_trace_writer.py \
  packages/python/tests/unit/test_semantic_trace_validation.py
```

Run the integration fixture for any adapter that changes. The
[integration inventory](../sdk/integrations.md) lists the tested releases.

Run the full repository check before merging a change that affects shared
behavior:

```sh
pnpm verify
```

The full check covers contracts, linting, type checking, SDK tests, package
builds, cloud packages, OpenClaw packages, local upload protocol tests, and
package installation tests.

## Integration bundle test

One complete bundle test for an adapter should prove the following behavior:

- The application receives the same return, exception, stream, cancellation,
  and callback order with capture installed.
- Exact native identities link model and tool records.
- Historical context does not become observed execution.
- The adapter does not copy routine stream chunks or duplicate callbacks into
  the semantic trace.
- Readable reasoning exposed by the interface stays ordered and correlated.
- Visible answer content and usage stay separate from reasoning.
- Material missing evidence becomes a bounded loss.
- Every record and the sealed manifest validate.

Tests should not prescribe one private helper or one model reasoning path. Add
a version to the integration inventory only after its complete bundle test
passes against that exact installed release.

## Trace readability

Give a fresh coding agent only the sealed bundle, the
[trace format](../sdk/trace-format.md), and a short analysis task. The agent
should be able to do the following work:

1. Reconstruct the persisted sequence.
2. Find the apparent goal and cite its source record.
3. Match tool proposals, calls, and results.
4. Find observed state changes, retries, handoffs, and resumes.
5. Separate visible answer content from exposed reasoning and summaries.
6. Find usage and follow each blob reference.
7. Separate framework completion from goal satisfaction.
8. Identify losses and missing evidence without inventing relations.
9. Cite exact record identities.

The readability check evaluates whether another agent can analyze the bundle.
The capture SDK does not classify failures itself.

## Environment tests

Local package tests do not qualify a deployed cloud endpoint, external
credentials, a distributed package, or a native OpenClaw installation. Run the
deployment and installation checks for the environment that will receive the
release.
