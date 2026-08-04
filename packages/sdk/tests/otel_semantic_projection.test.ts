import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { context, SpanStatusCode, trace } from '@opentelemetry/api';
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
} from '../src/index.js';

afterEach(async () => resetCaptureForTests());

it('projects a current GenAI invoke_agent tree as one rich agent run', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-agent-'));
  const capture = initialize({ output, serviceName: 'otel-agent-fixture' });
  const source = createOpenTelemetrySource({ version: '2.9.0' });
  capture.installSource(source);
  const provider = new BasicTracerProvider({ spanProcessors: [source.spanProcessor] });
  const tracer = provider.getTracer(
    'genai-fixture',
    '1',
    { schemaUrl: 'https://opentelemetry.io/schemas/gen-ai/1.42.0' },
  );
  const application = tracer.startSpan('application workflow');
  const applicationContext = trace.setSpan(context.active(), application);
  const agent = tracer.startSpan('invoke_agent coding-agent', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'coding-agent',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'read README' }] },
      ]),
    },
  }, applicationContext);
  const agentContext = trace.setSpan(context.active(), agent);
  const model = tracer.startSpan('chat fixture-model', {
    attributes: {
      'gen_ai.operation.name': 'chat',
      'gen_ai.request.model': 'fixture-model',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'read README' }] },
      ]),
    },
  }, agentContext);
  model.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'using read_file' }] },
  ]));
  model.end();
  const tool = tracer.startSpan('execute_tool read_file', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'read_file',
      'gen_ai.tool.call.id': 'call-readme',
      'gen_ai.tool.call.arguments': '{"path":"README.md"}',
    },
  }, agentContext);
  tool.setAttribute('gen_ai.tool.call.result', '{"text":"contents"}');
  tool.end();
  const childAgent = tracer.startSpan('invoke_agent reviewer', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'reviewer',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'review contents' }] },
      ]),
    },
  }, agentContext);
  childAgent.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'looks good' }] },
  ]));
  childAgent.end();
  agent.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'done' }] },
  ]));
  agent.end();
  application.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const rows = (await readFile(`${closed.artifactPath}/trace.jsonl`, 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));
  expect(rows.map((row) => row.kind)).toEqual([
    'run.start',
    'message',
    'model.request',
    'model.response',
    'tool.call',
    'tool.result',
    'scope',
    'message',
    'scope',
    'message',
    'run.outcome',
  ]);
  const root = rows[0];
  expect(rows.slice(1).every((row) => (
    row.kind === 'message' || row.kind === 'scope' || row.parent === root.id
  ))).toBe(true);
  const scopes = rows.filter((row) => row.kind === 'scope');
  expect(scopes.map((row) => row.data.phase)).toEqual(['start', 'end']);
  expect(scopes[1]?.parent).toBe(scopes[0]?.id);
  await expect(validateArtifact(closed.artifactPath, { profile: 'rich-agent' }))
    .resolves.toMatchObject({ valid: true, issues: [] });
  await provider.shutdown();
});

it('captures invoke_agent input added before span end without a false gap', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-late-input-'));
  const capture = initialize({ output, serviceName: 'otel-late-input-fixture' });
  const source = createOpenTelemetrySource({ version: '2.9.0' });
  capture.installSource(source);
  const provider = new BasicTracerProvider({ spanProcessors: [source.spanProcessor] });
  const tracer = provider.getTracer(
    'genai-fixture',
    '1',
    { schemaUrl: 'https://opentelemetry.io/schemas/gen-ai/1.42.0' },
  );
  const agent = tracer.startSpan('invoke_agent late-input', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'late-input',
    },
  });
  agent.setAttribute('gen_ai.input.messages', JSON.stringify([
    { role: 'user', parts: [{ type: 'text', content: 'late but valid' }] },
  ]));
  agent.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'done' }] },
  ]));
  agent.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const rows = (await readFile(`${closed.artifactPath}/trace.jsonl`, 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));
  expect(rows.map((row) => row.kind)).toEqual([
    'run.start',
    'message',
    'run.outcome',
  ]);
  expect(rows[1]?.data).toMatchObject({
    role: 'user',
    content: [{ type: 'text', content: 'late but valid' }],
  });
  await expect(validateArtifact(closed.artifactPath))
    .resolves.toMatchObject({ valid: true, issues: [] });
  await provider.shutdown();
});

it('keeps unsupported GenAI schemas as explicit control evidence', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-schema-gap-'));
  const capture = initialize({ output, serviceName: 'otel-schema-gap-fixture' });
  const source = createOpenTelemetrySource({ version: '2.9.0' });
  capture.installSource(source);
  const provider = new BasicTracerProvider({ spanProcessors: [source.spanProcessor] });
  const tracer = provider.getTracer(
    'genai-fixture',
    '1',
    { schemaUrl: 'https://opentelemetry.io/schemas/gen-ai/1.41.0' },
  );
  const agent = tracer.startSpan('invoke_agent unsupported-schema', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'unsupported-schema',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'hello' }] },
      ]),
    },
  });
  agent.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'world' }] },
  ]));
  agent.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const rows = (await readFile(`${closed.artifactPath}/trace.jsonl`, 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));
  expect(rows).toHaveLength(1);
  expect(rows[0]).toMatchObject({
    kind: 'loss',
    data: {
      reason: 'unsupported_genai_schema',
      count: 1,
    },
  });
  expect(rows[0]?.data.detail).toContain(
    'https://opentelemetry.io/schemas/gen-ai/1.41.0',
  );
  expect(rows.some((row) => row.kind === 'run.start')).toBe(false);
  await expect(validateArtifact(closed.artifactPath))
    .resolves.toMatchObject({ valid: true, issues: [] });
  await provider.shutdown();
});

it('marks malformed rich agent messages instead of projecting them as content', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-agent-malformed-'));
  const capture = initialize({ output, serviceName: 'otel-agent-malformed-fixture' });
  const source = createOpenTelemetrySource({ version: '2.9.0' });
  capture.installSource(source);
  const provider = new BasicTracerProvider({ spanProcessors: [source.spanProcessor] });
  const tracer = provider.getTracer(
    'genai-fixture',
    '1',
    { schemaUrl: 'https://opentelemetry.io/schemas/gen-ai/1.42.0' },
  );
  const agent = tracer.startSpan('invoke_agent malformed-content', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'malformed-content',
      'gen_ai.input.messages': '"not a message array"',
    },
  });
  agent.setAttribute('gen_ai.output.messages', '"also not a message array"');
  agent.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const rows = (await readFile(`${closed.artifactPath}/trace.jsonl`, 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));
  expect(rows.map((row) => row.kind)).toEqual([
    'run.start',
    'loss',
    'loss',
    'run.outcome',
  ]);
  expect(rows.filter((row) => row.kind === 'loss').map((row) => row.data.reason))
    .toEqual(['agent_input_malformed', 'agent_output_malformed']);
  expect(rows[0]?.data).not.toHaveProperty('input');
  expect(rows[3]?.data).not.toHaveProperty('output');
  await expect(validateArtifact(closed.artifactPath))
    .resolves.toMatchObject({ valid: true, issues: [] });
  await provider.shutdown();
});

it('backfills late tool input and preserves structured OTel failures', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-late-tool-error-'));
  const capture = initialize({ output, serviceName: 'otel-late-tool-error-fixture' });
  const source = createOpenTelemetrySource({ version: '2.9.0' });
  capture.installSource(source);
  const provider = new BasicTracerProvider({ spanProcessors: [source.spanProcessor] });
  const tracer = provider.getTracer(
    'genai-fixture',
    '1',
    { schemaUrl: 'https://opentelemetry.io/schemas/gen-ai/1.42.0' },
  );
  const agent = tracer.startSpan('invoke_agent failing-agent', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.agent.name': 'failing-agent',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'run the tool' }] },
      ]),
    },
  });
  const agentContext = trace.setSpan(context.active(), agent);
  const model = tracer.startSpan('chat late-input', {
    attributes: {
      'gen_ai.operation.name': 'chat',
      'gen_ai.request.model': 'fixture-model',
    },
  }, agentContext);
  model.setAttribute('gen_ai.input.messages', JSON.stringify([
    { role: 'user', parts: [{ type: 'text', content: 'late model input' }] },
  ]));
  model.setAttribute('gen_ai.output.messages', JSON.stringify([
    { role: 'assistant', parts: [{ type: 'text', content: 'tool failed' }] },
  ]));
  model.recordException(new Error('model transport failed'));
  model.setStatus({ code: SpanStatusCode.ERROR });
  model.end();
  const tool = tracer.startSpan('execute_tool late-input', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'read_file',
      'gen_ai.tool.call.id': 'call-late',
    },
  }, agentContext);
  tool.setAttribute('gen_ai.tool.call.arguments', '{"path":"README.md"}');
  tool.recordException(new Error('permission denied'));
  tool.setStatus({ code: SpanStatusCode.ERROR });
  tool.end();
  agent.recordException(new Error('agent failed'));
  agent.setStatus({ code: SpanStatusCode.ERROR });
  agent.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const rows = (await readFile(`${closed.artifactPath}/trace.jsonl`, 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));
  const request = rows.find((row) => row.kind === 'model.request');
  const response = rows.find((row) => row.kind === 'model.response');
  const call = rows.find((row) => row.kind === 'tool.call');
  const result = rows.find((row) => row.kind === 'tool.result');
  const modelError = rows.find((row) => row.kind === 'error');
  const outcome = rows.find((row) => row.kind === 'run.outcome');
  expect(request?.data.context_refs).toEqual([]);
  expect(rows.some((row) => (
    row.kind === 'loss' && row.data.reason === 'model_input_late_unlinked'
  ))).toBe(true);
  expect(call?.data).toMatchObject({
    native_call_id: 'call-late',
    name: 'read_file',
    input: { path: 'README.md' },
  });
  expect(result?.data).toMatchObject({
    native_call_id: 'call-late',
    status: 'failed',
    error: {
      type: 'error',
      message: 'permission denied',
      recoverable: false,
    },
  });
  expect(response?.data.status).toBe('failed');
  expect(modelError?.data).toMatchObject({
    type: 'error',
    message: 'model transport failed',
    recoverable: false,
  });
  expect(outcome?.data).toMatchObject({
    status: 'failed',
    error: {
      type: 'error',
      message: 'agent failed',
      recoverable: false,
    },
  });
  expect(rows.some((row) => (
    row.kind === 'loss' && row.data.reason === 'tool_input_not_captured'
  ))).toBe(false);
  await expect(validateArtifact(closed.artifactPath))
    .resolves.toMatchObject({ valid: true, issues: [] });
  await expect(validateArtifact(closed.artifactPath, { profile: 'rich-agent' }))
    .resolves.toMatchObject({
      valid: false,
      issues: ['PROFILE_PAIR_MISSING:model', 'PROFILE_PAIR_MISSING:tool'],
    });
  await provider.shutdown();
});

it('keeps OTel additive, projects exact GenAI fields, and bounds outside-seam losses', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-bundle-'));
  const capture = initialize({ output, serviceName: 'otel-bundle-fixture' });
  const source = createOpenTelemetrySource({ version: '2.9.0' });
  capture.installSource(source);

  const applicationExporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({
    spanProcessors: [
      new SimpleSpanProcessor(applicationExporter),
      source.spanProcessor,
    ],
  });
  const tracer = provider.getTracer('fixture');
  const root = tracer.startSpan('application workflow');
  const rootContext = trace.setSpan(context.active(), root);

  const userParts = [{ type: 'text', content: 'question' }];
  const participantName = 'requester-'.repeat(40);
  const boundedParticipant = participantName.slice(0, 256);
  const assistantMessages = [
    {
      role: 'assistant',
      name: 'fixture-agent',
      parts: [{ type: 'text', content: 'answer' }],
    },
    {
      role: 'assistant',
      name: 'fixture-agent-alternative',
      parts: [{ type: 'text', content: 'alternative answer' }],
    },
  ];
  const modelSpan = tracer.startSpan('chat', {
    attributes: {
      'gen_ai.operation.name': 'chat',
      'gen_ai.request.model': 'fixture-model',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', name: participantName, parts: userParts },
        { role: 'invalid', parts: [{ type: 'text', content: 'discarded one' }] },
        { role: 'invalid', parts: [{ type: 'text', content: 'discarded two' }] },
        { role: 'user', parts: 'not structured' },
      ]),
      'gen_ai.usage.input_tokens': 3,
      'gen_ai.usage.output_tokens': 2,
    },
  }, rootContext);
  modelSpan.setAttribute('gen_ai.output.messages', JSON.stringify(assistantMessages));
  // The terminal remains a model response even if mutable attributes later drift.
  modelSpan.setAttribute('gen_ai.operation.name', 'invoke_agent');
  modelSpan.end();
  const followupParts = [{ type: 'text', content: 'follow-up' }];
  const cumulativeModelSpan = tracer.startSpan('chat cumulative', {
    attributes: {
      'gen_ai.operation.name': 'chat',
      'gen_ai.request.model': 'fixture-model',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', name: participantName, parts: userParts },
        ...assistantMessages,
        { role: 'user', name: participantName, parts: followupParts },
      ]),
    },
  }, rootContext);
  cumulativeModelSpan.setAttribute('gen_ai.output.messages', JSON.stringify([{
    role: 'assistant',
    parts: [{ type: 'text', content: 'follow-up answer' }],
  }]));
  cumulativeModelSpan.end();
  const toolSpan = tracer.startSpan('execute_tool read_file', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'read_file',
      'gen_ai.tool.call.id': 'call-readme',
      'gen_ai.tool.call.arguments': '{"path":"README.md"}',
    },
  }, rootContext);
  toolSpan.setAttribute('gen_ai.tool.call.result', '{"text":"contents"}');
  toolSpan.end();
  tracer.startSpan('execute_tool without input', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'lookup',
      'gen_ai.tool.call.id': 'call-missing-input',
      'gen_ai.tool.call.result': '"not projected as a result"',
    },
  }, rootContext).end();
  tracer.startSpan('agent invocation', {
    attributes: {
      'gen_ai.operation.name': 'invoke_agent',
      'gen_ai.input.messages': '"agent input"',
      'gen_ai.output.messages': '"agent output"',
    },
  }, rootContext).end();
  tracer.startSpan('database query', {}, rootContext).end();
  const routineSpanNames = Array.from(
    { length: 20 },
    (_, index) => `${index % 2 === 0 ? 'database' : 'container'} ${index}`,
  );
  for (const name of routineSpanNames) {
    tracer.startSpan(name, {
      attributes: { 'service.component': 'fixture' },
    }, rootContext).end();
  }
  root.end();
  source.spanProcessor.onStart({
    name: 'invalid ordinary span',
    spanContext: () => ({ traceId: '0'.repeat(32), spanId: '0'.repeat(16) }),
  }, context.active());
  source.spanProcessor.onStart({
    name: 'invalid GenAI span',
    attributes: { 'gen_ai.operation.name': 'invoke_agent' },
    spanContext: () => ({ traceId: '0'.repeat(32), spanId: '0'.repeat(16) }),
  }, context.active());
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const beforeLateSpan = await readFile(`${closed.artifactPath}/trace.jsonl`, 'utf8');
  tracer.startSpan('application still owns provider').end();
  await provider.forceFlush();

  expect(applicationExporter.getFinishedSpans().map((span) => span.name)).toEqual([
    'chat',
    'chat cumulative',
    'execute_tool read_file',
    'execute_tool without input',
    'agent invocation',
    'database query',
    ...routineSpanNames,
    'application workflow',
    'application still owns provider',
  ]);
  expect(await readFile(`${closed.artifactPath}/trace.jsonl`, 'utf8')).toEqual(beforeLateSpan);

  const rows = beforeLateSpan.trim().split('\n').map((line) => JSON.parse(line));
  const manifest = JSON.parse(
    await readFile(`${closed.artifactPath}/manifest.json`, 'utf8'),
  );
  const messages = rows.filter((row) => row.kind === 'message');
  const requests = rows.filter((row) => row.kind === 'model.request');
  const responses = rows.filter((row) => row.kind === 'model.response');
  const message = messages[0];
  const request = requests[0];
  const response = responses[0];
  const call = rows.find((row) => row.kind === 'tool.call');
  const result = rows.find((row) => row.kind === 'tool.result');
  const losses = rows.filter((row) => row.kind === 'loss');

  expect(rows).toHaveLength(15);
  expect(messages).toHaveLength(5);
  expect(message).toMatchObject({
    origin: 'context',
    data: { role: 'user', name: boundedParticipant, content: userParts },
  });
  expect(message?.seq).toBeLessThan(request?.seq);
  expect(request?.data).toMatchObject({
    model: 'fixture-model',
    context_refs: [message?.id],
  });
  expect(requests[1]?.data.context_refs).toEqual([
    messages[1]?.id,
    messages[2]?.id,
    messages[3]?.id,
    messages[4]?.id,
  ]);
  expect(new Set(requests[1]?.data.context_refs)).toHaveProperty('size', 4);
  expect(messages[4]?.data).toEqual({
    role: 'user',
    name: boundedParticipant,
    content: followupParts,
  });
  expect(responses[1]?.links).toEqual([{
    type: 'result_of',
    record: requests[1]?.id,
  }]);
  expect(response).toMatchObject({
    origin: 'inferred',
    links: [{ type: 'result_of', record: request?.id }],
    data: {
      status: 'completed',
      model: 'fixture-model',
      content: assistantMessages,
      usage: { input_tokens: 3, output_tokens: 2 },
    },
  });
  expect(call?.data).toMatchObject({
    name: 'read_file',
    native_call_id: 'call-readme',
    input: { path: 'README.md' },
  });
  expect(result).toMatchObject({
    origin: 'inferred',
    links: [{ type: 'result_of', record: call?.id }],
    data: {
      native_call_id: 'call-readme',
      status: 'succeeded',
      output: { text: 'contents' },
    },
  });
  expect(rows.filter((row) => row.kind === 'tool.call')).toHaveLength(1);
  expect(rows.filter((row) => row.kind === 'tool.result')).toHaveLength(1);
  expect(losses.map((row) => row.data.reason).sort()).toEqual([
    'invalid_span_context',
    'missing_genai_schema',
    'model_input_messages_discarded',
    'tool_input_not_captured',
  ]);
  expect(losses.filter((row) => row.data.reason === 'tool_input_not_captured')).toHaveLength(1);
  const modelInputLoss = losses.filter(
    (row) => row.data.reason === 'model_input_messages_discarded',
  );
  expect(modelInputLoss).toHaveLength(1);
  expect(modelInputLoss[0]?.data.count).toBe(3);
  expect(losses.filter((row) => row !== modelInputLoss[0])
    .every((row) => row.data.count === 1)).toBe(true);
  expect(rows.filter((row) => row.kind === 'scope')).toHaveLength(0);
  expect(rows.some((row) => [
    'run.start',
    'run.outcome',
    'state',
    'tool.proposal',
  ].includes(row.kind))).toBe(false);
  expect(manifest).toMatchObject({
    state: 'sealed',
    sources: expect.arrayContaining([expect.objectContaining({ name: 'generic:otel' })]),
    trace: { path: 'trace.jsonl', records: rows.length, losses: losses.length },
  });
  await expect(validateArtifact(closed.artifactPath)).resolves.toMatchObject({
    valid: true,
    issues: [],
  });
  await provider.shutdown();
});

it('keeps unknown GenAI operations and malformed model output as named losses', async () => {
  const output = await mkdtemp(join(tmpdir(), 'semantic-otel-malformed-output-'));
  const capture = initialize({ output, serviceName: 'otel-malformed-output-fixture' });
  const source = createOpenTelemetrySource({ version: '2.9.0' });
  capture.installSource(source);
  const provider = new BasicTracerProvider({ spanProcessors: [source.spanProcessor] });
  const tracer = provider.getTracer('genai-fixture');

  const model = tracer.startSpan('chat malformed output', {
    attributes: {
      'gen_ai.operation.name': 'chat',
      'gen_ai.request.model': 'fixture-model',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'hello' }] },
      ]),
    },
  });
  model.setAttribute('gen_ai.output.messages', '"not a message array"');
  model.end();
  tracer.startSpan('embeddings future operation', {
    attributes: { 'gen_ai.operation.name': 'embeddings' },
  }).end();
  const malformedToolOutput = tracer.startSpan('execute_tool malformed output', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'lookup',
      'gen_ai.tool.call.id': 'call-malformed-output',
      'gen_ai.tool.call.arguments': '{"query":"hello"}',
    },
  });
  malformedToolOutput.setAttribute('gen_ai.tool.call.result', 'not-json');
  malformedToolOutput.end();
  const malformedToolInput = tracer.startSpan('execute_tool malformed input', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'lookup',
      'gen_ai.tool.call.id': 'call-malformed-input',
      'gen_ai.tool.call.arguments': 'not-json',
      'gen_ai.tool.call.result': '{"ok":true}',
    },
  });
  malformedToolInput.end();
  tracer.startSpan('execute_tool oversized ID', {
    attributes: {
      'gen_ai.operation.name': 'execute_tool',
      'gen_ai.tool.name': 'lookup',
      'gen_ai.tool.call.id': 'x'.repeat(257),
      'gen_ai.tool.call.arguments': '{"query":"hello"}',
      'gen_ai.tool.call.result': '{"ok":true}',
    },
  }).end();
  const typedError = tracer.startSpan('chat typed error', {
    attributes: {
      'gen_ai.operation.name': 'chat',
      'gen_ai.input.messages': JSON.stringify([
        { role: 'user', parts: [{ type: 'text', content: 'fail' }] },
      ]),
      'error.type': 'ProviderThrottle',
    },
  });
  typedError.setStatus({ code: SpanStatusCode.ERROR, message: 'rate limited' });
  typedError.end();
  await provider.forceFlush();

  const closed = await capture.shutdown();
  const rows = (await readFile(`${closed.artifactPath}/trace.jsonl`, 'utf8'))
    .trim().split('\n').map((line) => JSON.parse(line));
  const response = rows.find((row) => row.kind === 'model.response');
  expect(response?.data).not.toHaveProperty('content');
  const malformedResult = rows.find((row) => (
    row.kind === 'tool.result'
    && row.data.native_call_id === 'call-malformed-output'
  ));
  expect(malformedResult?.data).not.toHaveProperty('output');
  expect(rows.find((row) => (
    row.kind === 'error' && row.data.type === 'provider_throttle'
  ))).toBeDefined();
  expect(rows.some((row) => row.data.native_call_id === 'x'.repeat(256))).toBe(false);
  expect(rows.filter((row) => row.kind === 'loss').map((row) => row.data.reason).sort())
    .toEqual([
      'invalid_tool_call_id',
      'model_output_malformed',
      'tool_input_malformed',
      'tool_output_malformed',
      'unsupported_genai_operation',
    ]);
  await expect(validateArtifact(closed.artifactPath)).resolves.toMatchObject({ valid: true });
  await provider.shutdown();
});
