import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { context, SpanStatusCode, trace } from '@opentelemetry/api';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from '@opentelemetry/sdk-trace-base';
import { afterEach, expect, it } from 'vitest';

import {
  createOpenTelemetrySource,
  initialize,
  resetCaptureForTests,
  validateArtifact,
} from 'semantic-layer-capture';

const schemaUrl = 'https://opentelemetry.io/schemas/gen-ai/1.42.0';

afterEach(async () => resetCaptureForTests());

it('keeps the application exporter and projects an exact-version rich agent trace', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-1-25-rich-'));
  const capture = initialize({ output, serviceName: 'otel-1-25-fixture' });
  const source = createOpenTelemetrySource({ version: '1.25.1' });
  capture.installSource(source);

  const baselineExporter = new InMemorySpanExporter();
  const baselineProvider = new BasicTracerProvider();
  baselineProvider.addSpanProcessor(new SimpleSpanProcessor(baselineExporter));
  emitRichFixture(baselineProvider.getTracer('tavi-fixture', '1', { schemaUrl }));
  await baselineProvider.forceFlush();

  const applicationExporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider();
  provider.addSpanProcessor(new SimpleSpanProcessor(applicationExporter));
  provider.addSpanProcessor(source.spanProcessor);
  const tracer = provider.getTracer('tavi-fixture', '1', { schemaUrl });

  const agent = tracer.startSpan('invoke_agent ralph-loop', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'ralph-loop',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'research' }] },
      ]),
    },
  });
  const agentContext = trace.setSpan(context.active(), agent);
  const model = tracer.startSpan('chat fixture-model', {
    attributes: {
      'gen_ai.operation.name': 'chat',
      'gen_ai.request.model': 'fixture-model',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'research' }] },
      ]),
    },
  }, agentContext);
  model.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'use search' }] },
  ]));
  model.end();
  const tool = tracer.startSpan('execute_tool search', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'search',
      'gen_ai.tool.call.id': 'call-search-1',
      'gen_ai.tool.call.arguments': '{"query":"fixture"}',
    },
  }, agentContext);
  tool.setAttribute('gen_ai.tool.call.result', '{"count":1}');
  tool.end();
  agent.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'done' }] },
  ]));
  agent.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const validation = await validateArtifact(closed.artifactPath, {
    profile: 'rich-agent',
  });
  const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));

  expect(new OTLPTraceExporter()).toMatchObject({ export: expect.any(Function) });
  expect(applicationExporter.getFinishedSpans().map((span) => span.name)).toEqual([
    'chat fixture-model',
    'execute_tool search',
    'invoke_agent ralph-loop',
  ]);
  expect(applicationExporter.getFinishedSpans().map(applicationSpanEvidence))
    .toEqual(baselineExporter.getFinishedSpans().map(applicationSpanEvidence));
  expect(records.map((record) => record.kind)).toEqual([
    'run.start',
    'message',
    'model.request',
    'model.response',
    'tool.call',
    'tool.result',
    'run.outcome',
  ]);
  expect(validation).toMatchObject({ valid: true, issues: [] });
  await baselineProvider.shutdown();
  await provider.shutdown();
});

it('keeps exact-version child agent content correlated with its parent run', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-1-25-parent-'));
  const capture = initialize({ output, serviceName: 'otel-1-25-parent-fixture' });
  const source = createOpenTelemetrySource({ version: '1.25.1' });
  capture.installSource(source);

  const provider = new BasicTracerProvider();
  provider.addSpanProcessor(source.spanProcessor);
  const tracer = provider.getTracer('tavi-fixture', '1', { schemaUrl });
  const parent = tracer.startSpan('invoke_agent ralph-loop', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'ralph-loop',
      'gen_ai.input.messages': '[{"role":"user","parts":[]}]',
    },
  });
  const parentContext = trace.setSpan(context.active(), parent);
  const child = tracer.startSpan('invoke_agent search-loop', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'search-loop',
      'gen_ai.input.messages': '[{"role":"user","parts":[{"type":"text","content":"find candidates"}]}]',
    },
  }, parentContext);
  child.setAttribute('gen_ai.output.messages',
    '[{"role":"assistant","parts":[{"type":"text","content":"found candidates"}]}]');
  child.end();
  parent.setAttribute('gen_ai.output.messages', '[{"role":"assistant","parts":[]}]');
  parent.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));
  const scopes = records.filter((record) => record.kind === 'scope');
  const childMessages = records.filter((record) => record.kind === 'message'
    && record.data.content.length > 0);

  expect(scopes.map((record) => record.data.phase)).toEqual(['start', 'end']);
  expect(childMessages.map((record) => record.data.content[0].content)).toEqual([
    'find candidates',
    'found candidates',
  ]);
  expect(childMessages.every((record) => record.parent === scopes[0].id)).toBe(true);
  await provider.shutdown();
});

it('preserves an exact-version status message when a run fails', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-1-25-error-'));
  const capture = initialize({ output, serviceName: 'otel-1-25-error-fixture' });
  const source = createOpenTelemetrySource({ version: '1.25.1' });
  capture.installSource(source);

  const provider = new BasicTracerProvider();
  provider.addSpanProcessor(source.spanProcessor);
  const tracer = provider.getTracer('tavi-fixture', '1', { schemaUrl });
  const agent = tracer.startSpan('invoke_agent timed-out', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'timed-out',
      'gen_ai.input.messages': '[{"role":"user","parts":[]}]',
    },
  });
  agent.setStatus({ code: SpanStatusCode.ERROR, message: 'research timed out' });
  agent.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));
  const outcome = records.find((record) => record.kind === 'run.outcome');

  expect(outcome.data).toMatchObject({
    status: 'failed',
    error: { message: 'research timed out' },
  });
  await provider.shutdown();
});

function emitRichFixture(tracer) {
  const agent = tracer.startSpan('invoke_agent ralph-loop', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'ralph-loop',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'research' }] },
      ]),
    },
  });
  const agentContext = trace.setSpan(context.active(), agent);
  const model = tracer.startSpan('chat fixture-model', {
    attributes: {
      'gen_ai.operation.name': 'chat',
      'gen_ai.request.model': 'fixture-model',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'research' }] },
      ]),
    },
  }, agentContext);
  model.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'use search' }] },
  ]));
  model.end();
  const tool = tracer.startSpan('execute_tool search', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'search',
      'gen_ai.tool.call.id': 'call-search-1',
      'gen_ai.tool.call.arguments': '{"query":"fixture"}',
    },
  }, agentContext);
  tool.setAttribute('gen_ai.tool.call.result', '{"count":1}');
  tool.end();
  agent.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'done' }] },
  ]));
  agent.end();
}

function applicationSpanEvidence(span) {
  return {
    name: span.name,
    kind: span.kind,
    hasParent: Boolean(span.parentSpanId),
    attributes: span.attributes,
    status: span.status,
    events: span.events,
    links: span.links,
    droppedAttributesCount: span.droppedAttributesCount,
    droppedEventsCount: span.droppedEventsCount,
    droppedLinksCount: span.droppedLinksCount,
    instrumentationLibrary: span.instrumentationLibrary,
  };
}
