import { chmod, mkdir, mkdtemp, readFile, readdir, rm, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  createOpenTelemetrySource,
  validateArtifact,
  type OpenTelemetrySource,
} from 'semantic-layer-capture';
import {
  runTaviTriggerAttempt,
  type TaviTriggerDelivery,
  type TrustedTaviTenantConfig,
} from './tavi-trigger-attempt.js';
import { triggerIdentityFromContext } from './trigger-context.js';

const roots: string[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  await Promise.all(roots.splice(0).map((root) => rm(root, { recursive: true, force: true })));
});

describe('runTaviTriggerAttempt', () => {
  it('does no capture work when trusted tenant configuration is absent', async () => {
    const root = await privateRoot();
    const deliveries: TaviTriggerDelivery[] = [];
    const value = { exact: true };

    await expect(runTaviTriggerAttempt({
      tenant: null,
      trigger: triggerIdentity(),
      temporaryRoot: root,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => value,
    })).resolves.toBe(value);

    expect(deliveries).toEqual([{ status: 'not_captured' }]);
    expect(await readdir(root)).toEqual([]);
  });

  it('captures and acknowledges an enrolled tenant without changing its value', async () => {
    const root = await privateRoot();
    const deliveries: TaviTriggerDelivery[] = [];
    const value = { candidates: 3 };

    await expect(runTaviTriggerAttempt({
      tenant: tenant(),
      trigger: triggerIdentity(),
      temporaryRoot: root,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => value,
    })).resolves.toBe(value);

    expect(deliveries).toEqual([{ status: 'acknowledged', requestId: 'request-safe-1' }]);
    const attempt = await onlyAttempt(root);
    expect((await stat(attempt)).mode & 0o777).toBe(0o700);
    await expect(validateArtifact(await onlyBundle(attempt))).resolves.toMatchObject({ valid: true });
  });

  it('isolates two enrolled tenants with different trusted configuration', async () => {
    const root = await privateRoot();
    const firstAuth: string[] = [];
    const secondAuth: string[] = [];
    const first = tenant(successfulFetch(undefined, (auth) => { firstAuth.push(auth); }), {
      serviceName: 'tavi-trigger-first',
      ingestKey: 'first-trigger-ingest-secret',
      installationId: 'install_11111111111111111111111111111111',
      identityKey: 'first-stable-identity-secret',
    });
    const second = tenant(successfulFetch(undefined, (auth) => { secondAuth.push(auth); }), {
      serviceName: 'tavi-trigger-second',
      ingestKey: 'second-trigger-ingest-secret',
      installationId: 'install_22222222222222222222222222222222',
      identityKey: 'second-stable-identity-secret',
    });

    await Promise.all([
      runTaviTriggerAttempt({
        tenant: first,
        trigger: triggerIdentity({ runId: 'first-run' }),
        temporaryRoot: root,
        openTelemetry: {
          source: inertOpenTelemetrySource(),
          attach() {},
        },
        task: async () => 'first',
      }),
      runTaviTriggerAttempt({
        tenant: second,
        trigger: triggerIdentity({ runId: 'second-run' }),
        temporaryRoot: root,
        openTelemetry: {
          source: inertOpenTelemetrySource(),
          attach() {},
        },
        task: async () => 'second',
      }),
    ]);

    const attempts = await attemptDirectories(root);
    expect(attempts).toHaveLength(2);
    const details = await Promise.all(attempts.map(async (attempt) => {
      const bundle = await onlyBundle(attempt);
      const manifest = JSON.parse(await readFile(join(bundle, 'manifest.json'), 'utf8')) as {
        bundle_id: string;
        installation_id: string;
        sources: Array<{ id: string; name: string }>;
      };
      const acked = await readdir(join(attempt, 'spool', 'acked'));
      return {
        manifest,
        acked,
        sourceId: manifest.sources.find((source) => source.name === 'test:otel')?.id,
        retainedText: await readTreeText(attempt),
      };
    }));
    expect(new Set(details.map(({ manifest }) => manifest.installation_id)))
      .toEqual(new Set([first.installationId, second.installationId]));
    expect(new Set(details.map(({ manifest }) => manifest.bundle_id)).size).toBe(2);
    expect(new Set(details.flatMap(({ acked }) => acked)).size).toBe(2);
    expect(new Set(details.map(({ sourceId }) => sourceId)).size).toBe(2);
    expect(firstAuth.length).toBeGreaterThan(0);
    expect(secondAuth.length).toBeGreaterThan(0);
    expect(new Set(firstAuth)).toEqual(new Set([`Bearer ${first.ingestKey}`]));
    expect(new Set(secondAuth)).toEqual(new Set([`Bearer ${second.ingestKey}`]));
    expect(details.every(({ retainedText }) => (
      !retainedText.includes(first.ingestKey) && !retainedText.includes(second.ingestKey)
    ))).toBe(true);
  });

  it('preserves the exact thrown value and still acknowledges its evidence', async () => {
    const root = await privateRoot();
    const failure = new Error('research failed');
    const deliveries: TaviTriggerDelivery[] = [];

    await expect(runTaviTriggerAttempt({
      tenant: tenant(),
      trigger: triggerIdentity(),
      temporaryRoot: root,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => { throw failure; },
    })).rejects.toBe(failure);

    expect(deliveries).toEqual([{ status: 'acknowledged', requestId: 'request-safe-1' }]);
  });

  it('keeps parallel attempts in separate private bundles', async () => {
    const root = await privateRoot();
    const deliveries: TaviTriggerDelivery[] = [];

    await Promise.all([
      runTaviTriggerAttempt({
        tenant: tenant(),
        trigger: triggerIdentity({ runId: 'parallel-a' }),
        temporaryRoot: root,
        reportDelivery: (delivery) => { deliveries.push(delivery); },
        task: async () => 'alpha-result',
      }),
      runTaviTriggerAttempt({
        tenant: tenant(),
        trigger: triggerIdentity({ runId: 'parallel-b' }),
        temporaryRoot: root,
        reportDelivery: (delivery) => { deliveries.push(delivery); },
        task: async () => 'beta-result',
      }),
    ]);

    const attempts = await attemptDirectories(root);
    expect(attempts).toHaveLength(2);
    const traces = await Promise.all(attempts.map(async (attempt) => (
      readFile(join(await onlyBundle(attempt), 'trace.jsonl'), 'utf8')
    )));
    expect(traces.filter((trace) => trace.includes('alpha-result'))).toHaveLength(1);
    expect(traces.filter((trace) => trace.includes('beta-result'))).toHaveLength(1);
    expect(deliveries).toHaveLength(2);
  });

  it('seals an application timeout without replacing its identity', async () => {
    const root = await privateRoot();
    const timeout = Object.assign(new Error('application deadline'), { name: 'TimeoutError' });
    const deliveries: TaviTriggerDelivery[] = [];

    await expect(runTaviTriggerAttempt({
      tenant: tenant(),
      trigger: triggerIdentity(),
      temporaryRoot: root,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => { throw timeout; },
    })).rejects.toBe(timeout);

    expect(deliveries[0]?.status).toBe('acknowledged');
    await expect(validateArtifact(await onlyBundle(await onlyAttempt(root))))
      .resolves.toMatchObject({ valid: true });
  });

  it('runs cleanup once for cooperative cancellation and preserves the cancellation value', async () => {
    const root = await privateRoot();
    const controller = new AbortController();
    const cancellation = Object.assign(new Error('cancelled by Trigger'), { name: 'AbortError' });
    const deliveries: TaviTriggerDelivery[] = [];
    let completions = 0;
    let taskStarted!: () => void;
    const started = new Promise<void>((resolve) => { taskStarted = resolve; });
    const config = tenant(successfulFetch(() => { completions += 1; }));

    const running = runTaviTriggerAttempt({
      tenant: config,
      trigger: triggerIdentity(),
      temporaryRoot: root,
      signal: controller.signal,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async ({ signal }) => await new Promise<never>((_resolve, reject) => {
        taskStarted();
        signal?.addEventListener('abort', () => reject(cancellation), { once: true });
      }),
    });
    await started;
    controller.abort();

    await expect(running).rejects.toBe(cancellation);
    expect(deliveries).toEqual([{ status: 'acknowledged', requestId: 'request-safe-1' }]);
    expect(completions).toBe(1);
    const rows = await traceRows(await onlyBundle(await onlyAttempt(root)));
    expect(rows).toContainEqual(expect.objectContaining({
      kind: 'run.outcome',
      data: expect.objectContaining({ status: 'cancelled' }),
    }));
  });

  it('fails open when upload is permanently rejected', async () => {
    const root = await privateRoot();
    const deliveries: TaviTriggerDelivery[] = [];
    const value = { customerSuccess: true };

    await expect(runTaviTriggerAttempt({
      tenant: tenant(async () => new Response(null, { status: 422 })),
      trigger: triggerIdentity(),
      temporaryRoot: root,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => value,
    })).resolves.toBe(value);

    expect(deliveries).toEqual([{ status: 'upload_failed' }]);
  });

  it('reports a bounded upload timeout without breaking customer work', async () => {
    const root = await privateRoot();
    const deliveries: TaviTriggerDelivery[] = [];
    const config = {
      ...tenant(async () => await new Promise<Response>(() => {})),
      uploadDeadlineMs: 20,
    };

    await expect(runTaviTriggerAttempt({
      tenant: config,
      trigger: triggerIdentity(),
      temporaryRoot: root,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => 'customer-result',
    })).resolves.toBe('customer-result');

    expect(deliveries).toEqual([{ status: 'timed_out' }]);
  });

  it('reports capture setup failure without breaking customer work', async () => {
    const root = await privateRoot();
    const deliveries: TaviTriggerDelivery[] = [];
    const config = { ...tenant(), serviceName: '' };

    await expect(runTaviTriggerAttempt({
      tenant: config,
      trigger: triggerIdentity(),
      temporaryRoot: root,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => 'customer-result',
    })).resolves.toBe('customer-result');

    expect(deliveries).toEqual([{ status: 'capture_failed' }]);
  });

  it('makes retries and replays distinguishable while retaining stable research correlation', async () => {
    const root = await privateRoot();
    for (const [runId, attemptNumber] of [
      ['original-run', 0],
      ['original-run', 1],
      ['replay-run', 0],
    ] as const) {
      await runTaviTriggerAttempt({
        tenant: tenant(),
        trigger: triggerIdentity({ runId, attemptNumber }),
        temporaryRoot: root,
        task: async () => 'done',
      });
    }

    const starts = await Promise.all((await attemptDirectories(root)).map(async (attempt) => {
      const rows = (await readFile(join(await onlyBundle(attempt), 'trace.jsonl'), 'utf8'))
        .trim().split('\n').map((line) => JSON.parse(line) as Record<string, any>);
      return rows.find((row) => row.kind === 'run.start')!;
    }));
    const correlations = starts.map((row) => row.data.correlation);
    expect(new Set(correlations.map((value) => value.task_id))).toHaveLength(1);
    const attemptsByRun = new Map<string, typeof correlations>();
    for (const value of correlations) {
      const runId = value.execution.run_id as string;
      attemptsByRun.set(runId, [...(attemptsByRun.get(runId) ?? []), value]);
    }
    expect([...attemptsByRun.values()].map((values) => values.length).sort()).toEqual([1, 2]);
    expect([...attemptsByRun.values()].find((values) => values.length === 2)
      ?.map((value) => value.execution.attempt).sort()).toEqual([0, 1]);
    expect(correlations.map((value) => value.execution.attempt).sort()).toEqual([0, 0, 1]);
    expect(starts.every((row) => row.data.input === null)).toBe(true);
  });

  it('protects current, parent, and root Trigger run IDs in one identity domain', async () => {
    const root = await privateRoot();
    await Promise.all([
      runTaviTriggerAttempt({
        tenant: tenant(),
        trigger: triggerIdentity({
          runId: 'parent-run',
          rootRunId: 'parent-run',
          parentRunId: '',
          attemptNumber: 10,
        }),
        temporaryRoot: root,
        task: async () => 'parent',
      }),
      runTaviTriggerAttempt({
        tenant: tenant(),
        trigger: triggerIdentity({
          runId: 'child-run',
          parentRunId: 'parent-run',
          rootRunId: 'parent-run',
          attemptNumber: 11,
        }),
        temporaryRoot: root,
        task: async () => 'child',
      }),
    ]);
    const correlations = await Promise.all((await attemptDirectories(root)).map(async (attempt) => {
      const rows = (await readFile(join(await onlyBundle(attempt), 'trace.jsonl'), 'utf8'))
        .trim().split('\n').map((line) => JSON.parse(line) as Record<string, any>);
      return rows.find((row) => row.kind === 'run.start')!.data.correlation;
    }));
    const parent = correlations.find((value) => value.execution.attempt === 10)!;
    const child = correlations.find((value) => value.execution.attempt === 11)!;
    expect(child.execution.parent_run_id).toBe(parent.execution.run_id);
    expect(child.execution.root_run_id).toBe(parent.execution.run_id);
  });

  it('maps the public Trigger 4.4.4 context fields without OTel types', () => {
    expect(triggerIdentityFromContext({
      run: {
        id: 'run-current',
        parentTaskRunId: 'run-parent',
        rootTaskRunId: 'run-root',
      },
      attempt: { number: 2 },
    }, 'research-stable', '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01'))
      .toEqual({
        runId: 'run-current',
        parentRunId: 'run-parent',
        rootRunId: 'run-root',
        attemptNumber: 2,
        researchId: 'research-stable',
        traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01',
      });
  });

  it('captures and acknowledges correlated ralph and search rich-agent bundles', async () => {
    const root = await privateRoot();
    const deliveries: TaviTriggerDelivery[] = [];
    const config = tenant();
    const researchId = 'research-parent-child';
    const parentRunId = 'ralph-loop-run';
    const parentSource = createOpenTelemetrySource({ version: '1.25.1' });
    const childSource = createOpenTelemetrySource({ version: '1.25.1' });

    await runTaviTriggerAttempt({
      tenant: config,
      trigger: triggerIdentity({
        runId: parentRunId,
        parentRunId: '',
        rootRunId: parentRunId,
        researchId,
      }),
      temporaryRoot: root,
      openTelemetry: { source: parentSource, attach() {} },
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => {
        emitRichOpenTelemetry(parentSource, '1', 'ralph-loop');
        return 'complete';
      },
    });
    await runTaviTriggerAttempt({
      tenant: config,
      trigger: triggerIdentity({
        runId: 'search-loop-run',
        parentRunId,
        rootRunId: parentRunId,
        researchId,
      }),
      temporaryRoot: root,
      openTelemetry: { source: childSource, attach() {} },
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => {
        emitRichOpenTelemetry(childSource, '2', 'search-loop');
        return 'complete';
      },
    });

    expect(deliveries).toEqual([
      { status: 'acknowledged', requestId: 'request-safe-1' },
      { status: 'acknowledged', requestId: 'request-safe-1' },
    ]);
    const bundles = await Promise.all((await attemptDirectories(root)).map(onlyBundle));
    expect(bundles).toHaveLength(2);
    for (const bundle of bundles) {
      await expect(validateArtifact(bundle, {
        profile: 'rich-agent',
        secretValues: [config.ingestKey, config.identityKey],
      })).resolves.toMatchObject({ valid: true, issues: [] });
      const kinds = (await traceRows(bundle)).map((row) => row.kind);
      expect(kinds).toEqual(expect.arrayContaining([
        'model.request',
        'model.response',
        'tool.call',
        'tool.result',
      ]));
    }
    const starts = (await Promise.all(bundles.map(traceRows)))
      .flatMap((rows) => rows.filter((row) => row.kind === 'run.start'))
      .filter((row) => row.data.correlation?.execution.system === 'trigger.dev');
    expect(starts).toHaveLength(2);
    const parent = starts.find((row) => row.data.correlation.execution.parent_run_id === undefined)!;
    const child = starts.find((row) => row.data.correlation.execution.parent_run_id !== undefined)!;
    expect(child.data.correlation.task_id).toBe(parent.data.correlation.task_id);
    expect(child.data.correlation.execution.parent_run_id)
      .toBe(parent.data.correlation.execution.run_id);
    expect(child.data.correlation.execution.root_run_id)
      .toBe(parent.data.correlation.execution.run_id);
  });

  it('attaches an existing OTel source only for the enabled tenant', async () => {
    const root = await privateRoot();
    const attached: OpenTelemetrySource[] = [];
    const source = inertOpenTelemetrySource();
    await runTaviTriggerAttempt({
      tenant: tenant(),
      trigger: triggerIdentity(),
      temporaryRoot: root,
      openTelemetry: { source, attach: (value) => { attached.push(value); } },
      task: async () => 'done',
    });
    expect(attached).toEqual([source]);
  });

  it('never logs or emits the key, paths, or captured content', async () => {
    const root = await privateRoot();
    const messages: unknown[] = [];
    for (const method of ['log', 'info', 'warn', 'error', 'debug'] as const) {
      vi.spyOn(console, method).mockImplementation((...values) => { messages.push(values); });
    }
    const deliveries: TaviTriggerDelivery[] = [];
    const config = tenant();
    const customerContent = 'private candidate profile';

    await runTaviTriggerAttempt({
      tenant: config,
      trigger: triggerIdentity(),
      temporaryRoot: root,
      reportDelivery: (delivery) => { deliveries.push(delivery); },
      task: async () => customerContent,
    });

    const emitted = JSON.stringify({ deliveries, messages });
    expect(messages).toEqual([]);
    expect(emitted).not.toContain(config.ingestKey);
    expect(emitted).not.toContain(root);
    expect(emitted).not.toContain(customerContent);
  });
});

async function privateRoot(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), 'tavi-trigger-example-test-'));
  roots.push(root);
  await chmod(root, 0o700);
  return root;
}

function tenant(
  fetch = successfulFetch(),
  overrides: Partial<TrustedTaviTenantConfig> = {},
): TrustedTaviTenantConfig {
  return {
    serviceName: 'tavi-trigger-enrolled',
    endpoint: 'https://semantic-ingest.example.test',
    ingestKey: 'trigger-ingest-key-secret',
    installationId: 'install_0123456789abcdef0123456789abcdef',
    identityKey: 'stable-trigger-identity-key-secret',
    fetch,
    uploadDeadlineMs: 1_000,
    ...overrides,
  };
}

function triggerIdentity(overrides: Partial<{
  runId: string;
  parentRunId: string;
  rootRunId: string;
  attemptNumber: number;
  researchId: string;
}> = {}) {
  const parentRunId = overrides.parentRunId === ''
    ? undefined
    : overrides.parentRunId ?? 'trigger-parent-1';
  return {
    runId: overrides.runId ?? 'trigger-run-1',
    ...(parentRunId ? { parentRunId } : {}),
    rootRunId: overrides.rootRunId ?? 'trigger-root-1',
    attemptNumber: overrides.attemptNumber ?? 0,
    researchId: overrides.researchId ?? 'research-stable-1',
    traceparent: '00-0123456789abcdef0123456789abcdef-0123456789abcdef-01',
  };
}

function successfulFetch(
  onComplete?: () => void,
  onAuthorization?: (authorization: string) => void,
): typeof fetch {
  return async (input, init) => {
    onAuthorization?.(new Headers(init?.headers).get('authorization') ?? '');
    const path = new URL(String(input)).pathname;
    if (path.endsWith('/complete')) {
      onComplete?.();
      const body = JSON.parse(String(init?.body)) as { bundle_digest: string };
      return new Response(JSON.stringify({
        status: 'complete',
        bundle_digest: body.bundle_digest,
      }), {
        status: 200,
        headers: { 'content-type': 'application/json', 'x-request-id': 'request-safe-1' },
      });
    }
    if (path.endsWith('/begin')) {
      return new Response('{}', {
        status: 200,
        headers: { 'content-type': 'application/json', 'x-request-id': 'request-safe-1' },
      });
    }
    return new Response(null, {
      status: 200,
      headers: { 'x-request-id': 'request-safe-1' },
    });
  };
}

async function readTreeText(root: string): Promise<string> {
  const values: string[] = [];
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) values.push(await readTreeText(path));
    else if (entry.isFile()) values.push(await readFile(path, 'utf8'));
  }
  return values.join('\n');
}

async function attemptDirectories(root: string): Promise<string[]> {
  return (await readdir(root, { withFileTypes: true }))
    .filter((entry) => entry.isDirectory())
    .map((entry) => join(root, entry.name));
}

async function onlyAttempt(root: string): Promise<string> {
  const attempts = await attemptDirectories(root);
  expect(attempts).toHaveLength(1);
  return attempts[0]!;
}

async function onlyBundle(attempt: string): Promise<string> {
  const captureRoot = join(attempt, 'capture');
  const bundles = (await readdir(captureRoot, { withFileTypes: true }))
    .filter((entry) => entry.isDirectory() && entry.name.startsWith('run-'));
  expect(bundles).toHaveLength(1);
  return join(captureRoot, bundles[0]!.name);
}

async function traceRows(bundle: string): Promise<Array<Record<string, any>>> {
  return (await readFile(join(bundle, 'trace.jsonl'), 'utf8'))
    .trim().split('\n').filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, any>);
}

type OTelSpan = Parameters<OpenTelemetrySource['spanProcessor']['onStart']>[0];

function emitRichOpenTelemetry(
  source: OpenTelemetrySource,
  seed: string,
  agentName: string,
): void {
  const traceId = seed.padStart(32, '0');
  const agent = otelSpan(traceId, `${seed}1`.padStart(16, '0'), undefined, {
    'gen_ai.operation.name': 'invoke_agent',
    'gen_ai.agent.name': agentName,
    'gen_ai.input.messages': JSON.stringify([
      { role: 'user', parts: [{ type: 'text', content: 'find candidates' }] },
    ]),
  });
  source.spanProcessor.onStart(agent, {});
  const agentContext = agent.spanContext();
  const model = otelSpan(traceId, `${seed}2`.padStart(16, '0'), agentContext, {
    'gen_ai.operation.name': 'chat',
    'gen_ai.request.model': 'fixture-model',
    'gen_ai.input.messages': JSON.stringify([
      { role: 'user', parts: [{ type: 'text', content: 'find candidates' }] },
    ]),
    'gen_ai.output.messages': JSON.stringify([
      { role: 'assistant', parts: [{ type: 'text', content: 'using search' }] },
    ]),
  });
  source.spanProcessor.onStart(model, {});
  source.spanProcessor.onEnd(model);
  const tool = otelSpan(traceId, `${seed}3`.padStart(16, '0'), agentContext, {
    'gen_ai.operation.name': 'execute_tool',
    'gen_ai.tool.name': 'candidate_search',
    'gen_ai.tool.call.id': `call-${seed}`,
    'gen_ai.tool.call.arguments': '{"query":"executive"}',
    'gen_ai.tool.call.result': '{"count":2}',
  });
  source.spanProcessor.onStart(tool, {});
  source.spanProcessor.onEnd(tool);
  agent.attributes = {
    ...agent.attributes,
    'gen_ai.output.messages': JSON.stringify([
      { role: 'assistant', parts: [{ type: 'text', content: 'complete' }] },
    ]),
  };
  source.spanProcessor.onEnd(agent);
}

function otelSpan(
  traceId: string,
  spanId: string,
  parentSpanContext: ReturnType<OTelSpan['spanContext']> | undefined,
  attributes: Record<string, unknown>,
): OTelSpan {
  return {
    spanContext: () => ({ traceId, spanId, traceFlags: 1 }),
    ...(parentSpanContext ? { parentSpanContext } : {}),
    name: String(attributes['gen_ai.operation.name']),
    attributes,
    events: [],
    links: [],
    status: { code: 0 },
    resource: { attributes: { 'service.name': 'tavi-trigger-test' } },
    instrumentationScope: {
      name: 'tavi-trigger-test',
      version: '1',
      schemaUrl: 'https://opentelemetry.io/schemas/gen-ai/1.42.0',
    },
    droppedAttributesCount: 0,
    droppedEventsCount: 0,
    droppedLinksCount: 0,
  };
}

function inertOpenTelemetrySource(): OpenTelemetrySource {
  return {
    metadata: {
      name: 'test:otel',
      seam: 'test',
      identityDomain: 'test.otel',
      coverage: [],
    },
    spanProcessor: { onStart() {}, onEnd() {}, async forceFlush() {}, async shutdown() {} },
    logRecordProcessor: { onEmit() {}, async forceFlush() {}, async shutdown() {} },
    install() { return { deactivate() {}, drain() {} }; },
  };
}
