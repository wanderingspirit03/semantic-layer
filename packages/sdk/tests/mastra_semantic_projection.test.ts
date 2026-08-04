import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import * as MastraCurrent from 'mastra-current/observability';
import * as MastraPrevious from 'mastra-previous/observability';
import {
  ChunkFrom as CurrentChunkFrom,
  type AgentChunkType as CurrentAgentChunkType,
  type ReasoningChunk as CurrentReasoningChunk,
} from 'mastra-current/stream';
import {
  ChunkFrom as PreviousChunkFrom,
  type AgentChunkType as PreviousAgentChunkType,
  type ReasoningChunk as PreviousReasoningChunk,
} from 'mastra-previous/stream';
import { afterEach, describe, expect, it } from 'vitest';

import { initialize, mastraAdapter, resetCaptureForTests } from '../src/index.js';

const currentFinalReasoning: CurrentReasoningChunk[] = [
  {
    type: 'reasoning',
    runId: 'reasoning-run',
    from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-1', text: 'Check the account. ' },
  },
  {
    type: 'reasoning',
    runId: 'reasoning-run',
    from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-2', text: 'Check the account. ' },
  },
  {
    type: 'reasoning',
    runId: 'reasoning-run',
    from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-3', text: 'Then answer.' },
  },
];

const previousFinalReasoning: PreviousReasoningChunk[] = [
  {
    type: 'reasoning',
    runId: 'reasoning-run',
    from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-1', text: 'Check the account. ' },
  },
  {
    type: 'reasoning',
    runId: 'reasoning-run',
    from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-2', text: 'Check the account. ' },
  },
  {
    type: 'reasoning',
    runId: 'reasoning-run',
    from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-3', text: 'Then answer.' },
  },
];

const currentReasoningStream: CurrentAgentChunkType[] = [
  {
    type: 'reasoning-start', runId: 'stream-reasoning-run', from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-1' },
  },
  {
    type: 'reasoning-delta', runId: 'stream-reasoning-run', from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-1', text: 'Check the account. ' },
  },
  {
    type: 'reasoning-end', runId: 'stream-reasoning-run', from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-1' },
  },
  {
    type: 'reasoning-start', runId: 'stream-reasoning-run', from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-2' },
  },
  {
    type: 'reasoning-delta', runId: 'stream-reasoning-run', from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-2', text: 'Check the account. ' },
  },
  {
    type: 'reasoning-end', runId: 'stream-reasoning-run', from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-2' },
  },
  {
    type: 'redacted-reasoning', runId: 'stream-reasoning-run', from: CurrentChunkFrom.AGENT,
    payload: { id: 'reasoning-redacted-1', data: 'opaque-provider-carrier' },
  },
];

const previousReasoningStream: PreviousAgentChunkType[] = [
  {
    type: 'reasoning-start', runId: 'stream-reasoning-run', from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-1' },
  },
  {
    type: 'reasoning-delta', runId: 'stream-reasoning-run', from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-1', text: 'Check the account. ' },
  },
  {
    type: 'reasoning-end', runId: 'stream-reasoning-run', from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-1' },
  },
  {
    type: 'reasoning-start', runId: 'stream-reasoning-run', from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-2' },
  },
  {
    type: 'reasoning-delta', runId: 'stream-reasoning-run', from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-2', text: 'Check the account. ' },
  },
  {
    type: 'reasoning-end', runId: 'stream-reasoning-run', from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-stream-2' },
  },
  {
    type: 'redacted-reasoning', runId: 'stream-reasoning-run', from: PreviousChunkFrom.AGENT,
    payload: { id: 'reasoning-redacted-1', data: 'opaque-provider-carrier' },
  },
];

afterEach(async () => resetCaptureForTests());

async function expectContractRecords(records: unknown[]): Promise<void> {
  const schema = JSON.parse(await readFile(
    new URL('../../../contracts/trace/v1/semantic-trace-record.schema.json', import.meta.url),
    'utf8',
  ));
  const ajv = new Ajv2020({ allErrors: true, strict: true, strictRequired: false });
  addFormats(ajv);
  const validate = ajv.compile(schema);
  for (const record of records) {
    expect(validate(record), ajv.errorsText(validate.errors)).toBe(true);
  }
}

it('reports Mastra qualification from the observed version only', () => {
  expect(mastraAdapter({ version: '1.50.1' }).source.metadata.qualification).toEqual({
    status: 'exact_qualified',
  });
  expect(mastraAdapter({ version: '2.0.0' }).source.metadata.qualification).toEqual({
    status: 'capability_checked_unqualified', profile: 'mastra-observability-exporter-v1',
  });
  expect(mastraAdapter().source.metadata.qualification).toEqual({ status: 'unknown' });
});

describe.each([
  ['1.50.1', MastraCurrent],
  ['1.50.0', MastraPrevious],
] as const)('Mastra semantic trace %s', (version, Mastra) => {
  it('projects the exact typed final reasoning chunks in order with repetitions', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-final-reasoning-'));
    const capture = initialize({ output, serviceName: 'mastra-final-reasoning' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);
    const traceId = '00000000000000000000000000000011';
    const root = {
      id: 'reasoning-agent',
      traceId,
      name: 'reasoning agent',
      type: Mastra.SpanType.AGENT_RUN,
      isRootSpan: true,
      isEvent: false,
      startTime: new Date('2026-08-03T10:00:00.000Z'),
      metadata: { runId: 'reasoning-run' },
    };
    const model = {
      id: 'reasoning-model',
      traceId,
      parentSpanId: root.id,
      name: 'reasoning model',
      type: Mastra.SpanType.MODEL_GENERATION,
      isRootSpan: false,
      isEvent: false,
      startTime: new Date('2026-08-03T10:00:00.100Z'),
      input: { messages: [{ role: 'user', content: 'Answer carefully.' }] },
      attributes: { model: 'fixture-model' },
    };
    await adapter.exporter.exportTracingEvent({
      type: Mastra.TracingEventType.SPAN_STARTED,
      exportedSpan: root as never,
    });
    await adapter.exporter.exportTracingEvent({
      type: Mastra.TracingEventType.SPAN_STARTED,
      exportedSpan: model as never,
    });
    await adapter.exporter.exportTracingEvent({
      type: Mastra.TracingEventType.SPAN_ENDED,
      exportedSpan: {
        ...model,
        endTime: new Date('2026-08-03T10:00:01.000Z'),
        output: {
          text: 'The account is ready.',
          reasoning: version === '1.50.1' ? currentFinalReasoning : previousFinalReasoning,
        },
      } as never,
    });

    await capture.flush();
    const records = (await readFile(
      join(capture.status().artifactPath, 'trace.jsonl'),
      'utf8',
    )).trim().split('\n').map((line) => JSON.parse(line));
    expect(records.find((record) => record.kind === 'model.response')).toMatchObject({
      data: {
        content: { text: 'The account is ready.' },
        reasoning: [
          { type: 'text', text: 'Check the account. ' },
          { type: 'text', text: 'Check the account. ' },
          { type: 'text', text: 'Then answer.' },
        ],
      },
    });
    await expectContractRecords(records);
  });

  it('retains application-consumed reasoning parts without assigning a model owner', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-stream-reasoning-'));
    const capture = initialize({ output, serviceName: 'mastra-stream-reasoning' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);
    const traceId = '00000000000000000000000000000012';
    const root = {
      id: 'stream-reasoning-agent',
      traceId,
      name: 'stream reasoning agent',
      type: Mastra.SpanType.AGENT_RUN,
      isRootSpan: true,
      isEvent: false,
      startTime: new Date('2026-08-03T11:00:00.000Z'),
      metadata: { runId: 'stream-reasoning-run' },
    };
    const model = {
      id: 'stream-reasoning-model',
      traceId,
      parentSpanId: root.id,
      name: 'stream reasoning model',
      type: Mastra.SpanType.MODEL_GENERATION,
      isRootSpan: false,
      isEvent: false,
      startTime: new Date('2026-08-03T11:00:00.100Z'),
      input: { messages: [{ role: 'user', content: 'Answer carefully.' }] },
      attributes: { model: 'fixture-model' },
    };
    await adapter.exporter.exportTracingEvent({
      type: Mastra.TracingEventType.SPAN_STARTED,
      exportedSpan: root as never,
    });
    await adapter.exporter.exportTracingEvent({
      type: Mastra.TracingEventType.SPAN_STARTED,
      exportedSpan: model as never,
    });
    const consumed = version === '1.50.1' ? currentReasoningStream : previousReasoningStream;
    const before = structuredClone(consumed);
    for (const part of consumed) {
      expect(adapter.recordStreamPart('stream-reasoning-run', part).accepted).toBe(true);
    }
    expect(consumed).toEqual(before);
    await adapter.exporter.exportTracingEvent({
      type: Mastra.TracingEventType.SPAN_ENDED,
      exportedSpan: {
        ...model,
        endTime: new Date('2026-08-03T11:00:01.000Z'),
        output: { text: 'The account is ready.' },
      } as never,
    });

    await capture.flush();
    const records = (await readFile(
      join(capture.status().artifactPath, 'trace.jsonl'),
      'utf8',
    )).trim().split('\n').map((line) => JSON.parse(line));
    const responses = records.filter((record) => record.kind === 'model.response');
    expect(responses).toHaveLength(1);
    expect(responses[0].data).toMatchObject({ content: { text: 'The account is ready.' } });
    expect(responses[0].data).not.toHaveProperty('reasoning');
    const states = records.filter((record) => record.kind === 'state');
    expect(states).toHaveLength(7);
    expect(states.map((record) => record.data.value)).toEqual(before);
    expect(records.filter((record) => record.kind === 'loss').map((record) => (
      record.data.reason
    ))).toEqual([
      'mastra_reasoning_correlation_unavailable',
      'mastra_reasoning_correlation_unavailable',
      'mastra_reasoning_correlation_unavailable',
      'reasoning_unavailable',
    ]);
    await expectContractRecords(records);
  });

  it('does not guess a reasoning owner when model calls overlap', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-overlapping-reasoning-'));
    const capture = initialize({ output, serviceName: 'mastra-overlapping-reasoning' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);
    const traceId = '00000000000000000000000000000013';
    const root = {
      id: 'overlap-agent', traceId, name: 'overlap agent', type: Mastra.SpanType.AGENT_RUN,
      isRootSpan: true, isEvent: false, startTime: new Date('2026-08-03T12:00:00.000Z'),
      metadata: { runId: 'stream-reasoning-run' },
    };
    const model = (id: string) => ({
      id, traceId, parentSpanId: root.id, name: id, type: Mastra.SpanType.MODEL_GENERATION,
      isRootSpan: false, isEvent: false, startTime: new Date('2026-08-03T12:00:00.100Z'),
      input: { messages: [{ role: 'user', content: id }] }, attributes: { model: 'fixture-model' },
    });
    const first = model('overlap-model-1');
    const second = model('overlap-model-2');
    const emit = async (
      type: typeof Mastra.TracingEventType[keyof typeof Mastra.TracingEventType],
      span: Record<string, unknown>,
    ) => adapter.exporter.exportTracingEvent({ type, exportedSpan: span as never });
    await emit(Mastra.TracingEventType.SPAN_STARTED, root);
    await emit(Mastra.TracingEventType.SPAN_STARTED, first);
    await emit(Mastra.TracingEventType.SPAN_STARTED, second);
    const consumed = version === '1.50.1' ? currentReasoningStream : previousReasoningStream;
    for (const part of consumed.slice(0, 3)) {
      expect(adapter.recordStreamPart('stream-reasoning-run', part).accepted).toBe(true);
    }
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...first, endTime: new Date('2026-08-03T12:00:01.000Z'), output: { text: 'first' },
    });
    for (const part of consumed.slice(3, 6)) {
      expect(adapter.recordStreamPart('stream-reasoning-run', part).accepted).toBe(true);
    }
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...second, endTime: new Date('2026-08-03T12:00:02.000Z'), output: { text: 'second' },
    });

    await capture.flush();
    const records = (await readFile(
      join(capture.status().artifactPath, 'trace.jsonl'),
      'utf8',
    )).trim().split('\n').map((line) => JSON.parse(line));
    const responses = records.filter((record) => record.kind === 'model.response');
    expect(responses).toHaveLength(2);
    expect(responses[0].data).not.toHaveProperty('reasoning');
    expect(responses[1].data).toMatchObject({ content: { text: 'second' } });
    expect(responses[1].data).not.toHaveProperty('reasoning');
    expect(records.filter((record) => record.kind === 'state')).toHaveLength(6);
    expect(records.filter((record) => record.kind === 'loss')).toMatchObject([
      { data: { reason: 'mastra_reasoning_correlation_unavailable', count: 1 } },
      { data: { reason: 'mastra_reasoning_correlation_unavailable', count: 1 } },
    ]);
    await expectContractRecords(records);
  });

  it('uses the exact application-consumed stream error without duplicating the manual error', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-error-identity-'));
    const capture = initialize({ output, serviceName: 'mastra-error-identity' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);
    const failure = new TypeError('native Mastra stream failure');

    await expect(capture.observe('manual Mastra run', {}, async () => {
      await adapter.exporter.exportTracingEvent({
        type: Mastra.TracingEventType.SPAN_STARTED,
        exportedSpan: {
          id: 'agent-root',
          traceId: '00000000000000000000000000000001',
          name: 'failure agent',
          type: Mastra.SpanType.AGENT_RUN,
          isRootSpan: true,
          isEvent: false,
          startTime: new Date('2026-07-31T10:00:00.000Z'),
          metadata: { runId: 'failure-run' },
        } as never,
      });
      expect(adapter.recordStreamError('failure-run', failure).accepted).toBe(true);
      throw failure;
    })).rejects.toBe(failure);

    const closed = await capture.shutdown();
    const text = await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8');
    const rows = text.trim().split('\n').map((line) => JSON.parse(line) as {
      kind: string;
      data: Record<string, unknown>;
    });
    expect(rows.filter((row) => row.kind === 'error')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          type: 'type_error',
          message: 'native Mastra stream failure',
        }),
      }),
    ]);
    expect(rows.filter((row) => row.kind === 'run.outcome')).toEqual([
      expect.objectContaining({ data: expect.objectContaining({ status: 'failed' }) }),
    ]);
    expect(text).not.toContain('errorIdentity');
  });

  it('projects exact model, tool, and workflow relationships without duplicate losses', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-projection-'));
    const capture = initialize({ output, serviceName: 'mastra-semantic-fixture' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);

    const emit = async (
      type: typeof Mastra.TracingEventType[keyof typeof Mastra.TracingEventType],
      span: Record<string, unknown>,
    ) => adapter.exporter.exportTracingEvent({ type, exportedSpan: span as never });
    const base = (id: string, traceId: string, type: string, parentSpanId?: string) => ({
      id,
      traceId,
      parentSpanId,
      name: id,
      type,
      isRootSpan: !parentSpanId,
      isEvent: false,
      startTime: new Date('2026-07-26T10:00:00.000Z'),
    });

    const agentTraceId = '00000000000000000000000000000001';
    const agent = {
      ...base('agent-root', agentTraceId, Mastra.SpanType.AGENT_RUN),
      name: 'support agent',
      input: { request: 'find the order' },
      metadata: { runId: 'agent-run-1' },
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, agent);

    const model = {
      ...base('model-generation-1', agentTraceId, Mastra.SpanType.MODEL_GENERATION, agent.id),
      input: { messages: [{ role: 'user', content: 'find the order' }] },
      attributes: { model: 'fixture-model' },
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, model);
    const chunk = {
      ...base('model-chunk-1', agentTraceId, Mastra.SpanType.MODEL_CHUNK, model.id),
      attributes: { chunkType: 'text-delta', sequenceNumber: 1 },
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, chunk);
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...chunk,
      endTime: new Date('2026-07-26T10:00:00.500Z'),
      output: { text: 'I will look it up.' },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...model,
      endTime: new Date('2026-07-26T10:00:01.000Z'),
      output: {
        text: 'I will look it up.',
        reasoningText: 'fallback reasoning must not duplicate structured blocks',
        reasoning: [
          {
            type: 'reasoning', runId: 'agent-run-1', from: 'AGENT',
            payload: { id: 'reasoning-1', text: 'Checked the order context.' },
          },
          {
            type: 'reasoning', runId: 'agent-run-1', from: 'AGENT',
            payload: { id: 'reasoning-2', text: 'Checked the order context.' },
          },
        ],
      },
      attributes: {
        model: 'fixture-model',
        responseModel: 'fixture-model-2026',
        finishReason: 'tool-calls',
        usage: { inputTokens: 11, outputTokens: 5 },
      },
    });

    const tool = {
      ...base('tool-span-1', agentTraceId, Mastra.SpanType.TOOL_CALL, agent.id),
      name: "tool: 'lookup_order'",
      entityId: 'lookup_order',
      entityName: 'lookup_order',
      input: { order_id: 'A-17' },
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, tool);
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...tool,
      endTime: new Date('2026-07-26T10:00:02.000Z'),
      output: { status: 'shipped' },
      attributes: { success: true },
    });
    expect(adapter.recordStreamPart('agent-run-1', {
      type: 'text-delta',
      payload: { text: 'Routine streamed text is already retained by the model response.' },
    }).accepted).toBe(true);
    expect(adapter.recordStreamPart('agent-run-1', {
      type: 'tool-call',
      payload: {
        toolCallId: 'server-stream-call-1',
        toolName: 'lookup_order',
        args: { order_id: 'A-17' },
      },
    }).accepted).toBe(true);
    expect(adapter.recordStreamPart('agent-run-1', {
      type: 'tool-result',
      payload: {
        toolCallId: 'server-stream-call-1',
        toolName: 'lookup_order',
        result: { status: 'shipped' },
      },
    }).accepted).toBe(true);
    expect(adapter.recordStreamPart('agent-run-1', {
      type: 'tool-call',
      payload: {
        toolCallId: 'client-call-1',
        toolName: 'notify_customer',
        args: { channel: 'email' },
        providerExecuted: false,
        observability: { traceparent: '00-client-tool-carrier' },
      },
    }).accepted).toBe(true);
    expect(adapter.recordStreamPart('agent-run-1', {
      type: 'tool-result',
      payload: {
        toolCallId: 'client-call-1',
        toolName: 'notify_customer',
        result: { delivered: true },
      },
    }).accepted).toBe(true);
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...agent,
      endTime: new Date('2026-07-26T10:00:03.000Z'),
      output: { answer: 'The order shipped.' },
    });

    const workflowTraceId = '00000000000000000000000000000002';
    const workflow = {
      ...base('workflow-root', workflowTraceId, Mastra.SpanType.WORKFLOW_RUN),
      name: 'fulfil order',
      input: { order_id: 'A-17' },
      metadata: { runId: 'workflow-run-1' },
      attributes: { status: 'running' },
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, workflow);
    const step = {
      ...base('workflow-step-1', workflowTraceId, Mastra.SpanType.WORKFLOW_STEP, workflow.id),
      name: 'reserve stock',
      input: { sku: 'SKU-1' },
      attributes: { status: 'running' },
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, step);
    await emit(Mastra.TracingEventType.SPAN_UPDATED, {
      ...step,
      output: { reserved: 1 },
      attributes: { status: 'running' },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...step,
      endTime: new Date('2026-07-26T10:00:04.000Z'),
      output: { reserved: 1 },
      attributes: { status: 'success' },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...workflow,
      endTime: new Date('2026-07-26T10:00:05.000Z'),
      output: { completed: true },
      attributes: { status: 'success' },
    });

    await capture.flush();
    const records = (await readFile(
      join(capture.status().artifactPath, 'trace.jsonl'),
      'utf8',
    )).trim().split('\n').map((line) => JSON.parse(line));

    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    expect(records.map((record) => record.kind)).toEqual([
      'run.start',
      'message',
      'model.request',
      'model.response',
      'tool.call',
      'tool.result',
      'tool.call',
      'tool.result',
      'run.outcome',
      'run.start',
      'scope',
      'state',
      'scope',
      'run.outcome',
    ]);

    const meaningful = records.filter((record) => record.kind !== 'loss');
    const [agentStart, message, modelRequest, modelResponse, toolCall, toolResult, streamToolCall,
      streamToolResult, agentOutcome, workflowStart, stepStart, state, stepEnd,
      workflowOutcome] = meaningful;
    expect(agentStart).toMatchObject({
      data: { name: 'support agent', input: { request: 'find the order' } },
    });
    expect(modelRequest).toMatchObject({
      parent: agentStart.id,
      data: { model: 'fixture-model', context_refs: [message.id] },
    });
    expect(message).toMatchObject({
      parent: agentStart.id,
      data: { role: 'user', content: 'find the order' },
    });
    expect(modelResponse).toMatchObject({
      parent: agentStart.id,
      data: {
        status: 'completed',
        model: 'fixture-model-2026',
        content: { text: 'I will look it up.' },
        reasoning: [
          { type: 'text', text: 'Checked the order context.' },
          { type: 'text', text: 'Checked the order context.' },
        ],
        finish_reason: 'tool-calls',
        usage: { input_tokens: 11, output_tokens: 5 },
      },
      links: [{ type: 'result_of', record: modelRequest.id }],
    });
    expect(toolCall).toMatchObject({
      parent: agentStart.id,
      data: {
        native_call_id: 'tool-span-1',
        name: 'lookup_order',
        input: { order_id: 'A-17' },
      },
    });
    expect(toolResult).toMatchObject({
      parent: agentStart.id,
      data: {
        call_id: toolCall.data.call_id,
        native_call_id: 'tool-span-1',
        status: 'succeeded',
        output: { status: 'shipped' },
      },
      links: [{ type: 'result_of', record: toolCall.id }],
    });
    expect(streamToolCall).toMatchObject({
      parent: agentStart.id,
      data: {
        native_call_id: 'client-call-1',
        name: 'notify_customer',
        input: { channel: 'email' },
      },
    });
    expect(streamToolResult).toMatchObject({
      parent: agentStart.id,
      data: {
        call_id: streamToolCall.data.call_id,
        native_call_id: 'client-call-1',
        status: 'succeeded',
        output: { delivered: true },
      },
      links: [{ type: 'result_of', record: streamToolCall.id }],
    });
    expect(agentOutcome).toMatchObject({
      parent: agentStart.id,
      data: {
        status: 'completed',
        output: { answer: 'The order shipped.' },
      },
    });
    expect(workflowStart).toMatchObject({
      data: { name: 'fulfil order', input: { order_id: 'A-17' } },
    });
    expect(stepStart).toMatchObject({
      parent: workflowStart.id,
      data: {
        scope_id: 'mastra:workflow-step-1',
        type: 'step',
        phase: 'start',
        name: 'reserve stock',
      },
    });
    expect(state).toMatchObject({
      parent: stepStart.id,
      data: { type: 'state.workflow_step', value: { reserved: 1 } },
    });
    expect(stepEnd).toMatchObject({
      parent: stepStart.id,
      data: {
        scope_id: 'mastra:workflow-step-1',
        type: 'step',
        phase: 'end',
        status: 'completed',
      },
    });
    expect(workflowOutcome).toMatchObject({
      parent: workflowStart.id,
      data: { status: 'completed', output: { completed: true } },
    });
    await expectContractRecords(records);
  });

  it('reuses an unchanged same-object prefix and records a mutated suffix', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-steps-'));
    const capture = initialize({ output, serviceName: 'mastra-step-fixture' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);

    const emit = async (
      type: typeof Mastra.TracingEventType[keyof typeof Mastra.TracingEventType],
      span: Record<string, unknown>,
    ) => adapter.exporter.exportTracingEvent({ type, exportedSpan: span as never });
    const base = (id: string, traceId: string, type: string, parentSpanId?: string) => ({
      id,
      traceId,
      parentSpanId,
      name: id,
      type,
      isRootSpan: !parentSpanId,
      isEvent: false,
      startTime: new Date('2026-07-27T10:00:00.000Z'),
    });

    const traceId = '00000000000000000000000000000003';
    const agent = {
      ...base('agent-root', traceId, Mastra.SpanType.AGENT_RUN),
      name: 'tool agent',
      input: { request: 'look it up' },
    };
    const generation = {
      ...base('generation-1', traceId, Mastra.SpanType.MODEL_GENERATION, agent.id),
      input: {
        messages: [
          { role: 'system', content: 'Use the available tool.' },
          { role: 'user', content: 'Look up A-17.' },
        ],
      },
      attributes: { model: 'fixture-model' },
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, agent);
    await emit(Mastra.TracingEventType.SPAN_UPDATED, agent);
    await emit(Mastra.TracingEventType.SPAN_STARTED, generation);

    const firstStep = {
      ...base('model-step-1', traceId, Mastra.SpanType.MODEL_STEP, generation.id),
      attributes: { stepIndex: 0 },
    };
    const firstInference = {
      ...base('model-inference-1', traceId, Mastra.SpanType.MODEL_INFERENCE, firstStep.id),
      attributes: {
        model: 'fixture-model',
        availableTools: ['lookup_order'],
        stepIndex: 0,
      },
    };
    const firstContext = [
      { role: 'system', content: 'Use the available tool.' },
      { role: 'user', content: 'Look up A-17.' },
    ];
    await emit(Mastra.TracingEventType.SPAN_STARTED, firstStep);
    await emit(Mastra.TracingEventType.SPAN_STARTED, firstInference);
    await emit(Mastra.TracingEventType.SPAN_UPDATED, firstStep);
    await emit(Mastra.TracingEventType.SPAN_UPDATED, { ...firstStep, input: firstContext });
    const toolChunk = {
      ...base('tool-chunk-1', traceId, Mastra.SpanType.MODEL_CHUNK, firstInference.id),
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, toolChunk);
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...toolChunk,
      output: {
        toolCallId: 'call-1',
        toolName: 'lookup_order',
        toolInput: { order_id: 'A-17' },
      },
    });

    const tool = {
      ...base('tool-step-1', traceId, Mastra.SpanType.TOOL_CALL, firstStep.id),
      entityName: 'lookup_order',
      input: { order_id: 'A-17' },
      attributes: { toolCallId: 'call-1' },
    };
    await emit(Mastra.TracingEventType.SPAN_STARTED, tool);
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...tool,
      output: { status: 'shipped' },
      attributes: { success: true },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...firstInference,
      output: {
        text: '',
        toolCalls: [{ toolCallId: 'call-1', toolName: 'lookup_order' }],
      },
      attributes: {
        ...firstInference.attributes,
        finishReason: 'tool-calls',
        usage: { inputTokens: 10, outputTokens: 2 },
      },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...firstStep,
      input: firstContext,
      output: {
        text: '',
        toolCalls: [{ toolCallId: 'call-1', toolName: 'lookup_order' }],
      },
      attributes: {
        stepIndex: 0,
        finishReason: 'tool-calls',
        usage: { inputTokens: 10, outputTokens: 2 },
      },
    });

    const secondStep = {
      ...base('model-step-2', traceId, Mastra.SpanType.MODEL_STEP, generation.id),
      attributes: { stepIndex: 1 },
    };
    firstContext[1].content = 'Look up A-17 with the latest details.';
    const secondContext = [
      ...firstContext,
      {
        role: 'assistant',
        content: [{ type: 'tool-call', toolCallId: 'call-1', toolName: 'lookup_order' }],
      },
      {
        role: 'tool',
        content: [{ type: 'tool-result', toolCallId: 'call-1', output: { status: 'shipped' } }],
      },
    ];
    await emit(Mastra.TracingEventType.SPAN_STARTED, secondStep);
    await emit(Mastra.TracingEventType.SPAN_UPDATED, {
      ...generation,
      attributes: { model: 'fallback-model' },
    });
    await emit(Mastra.TracingEventType.SPAN_UPDATED, { ...secondStep, input: secondContext });
    await emit(Mastra.TracingEventType.SPAN_UPDATED, { ...secondStep, input: secondContext });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...secondStep,
      input: secondContext,
      output: { text: 'The order shipped.', toolCalls: [] },
      attributes: {
        stepIndex: 1,
        finishReason: 'stop',
        usage: { inputTokens: 20, outputTokens: 4 },
      },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...generation,
      output: { text: 'The order shipped.' },
      attributes: {
        model: 'fixture-model',
        finishReason: 'stop',
        usage: { inputTokens: 30, outputTokens: 6 },
      },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...agent,
      output: { text: 'The order shipped.' },
    });

    await capture.flush();
    const records = (await readFile(
      join(capture.status().artifactPath, 'trace.jsonl'),
      'utf8',
    )).trim().split('\n').map((line) => JSON.parse(line));

    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    expect(records.map((record) => record.kind)).toEqual([
      'run.start',
      'message',
      'message',
      'model.request',
      'tool.proposal',
      'tool.call',
      'tool.result',
      'model.response',
      'message',
      'message',
      'message',
      'model.request',
      'model.response',
      'run.outcome',
    ]);
    const requests = records.filter((record) => record.kind === 'model.request');
    const responses = records.filter((record) => record.kind === 'model.response');
    const messages = records.filter((record) => record.kind === 'message');
    const proposal = records.find((record) => record.kind === 'tool.proposal');
    const call = records.find((record) => record.kind === 'tool.call');
    expect(requests).toHaveLength(2);
    expect(responses).toHaveLength(2);
    expect(messages).toHaveLength(5);
    expect(requests[0]).toMatchObject({
      data: {
        model: 'fixture-model',
        tools: ['lookup_order'],
        context_refs: [messages[0].id, messages[1].id],
      },
    });
    expect(requests[1]).toMatchObject({
      data: {
        model: 'fallback-model',
        context_refs: [
          messages[0].id,
          messages[2].id,
          messages[3].id,
          messages[4].id,
        ],
      },
    });
    expect(messages[2].data).toMatchObject({
      role: 'user',
      content: 'Look up A-17 with the latest details.',
    });
    expect(proposal).toMatchObject({
      parent: requests[0].id,
      data: {
        native_call_id: 'call-1',
        name: 'lookup_order',
        input: { order_id: 'A-17' },
      },
    });
    expect(call).toMatchObject({
      data: {
        native_call_id: 'call-1',
        name: 'lookup_order',
        input: { order_id: 'A-17' },
      },
      links: [{ type: 'derived_from', record: proposal.id }],
    });
    expect(responses[0]).toMatchObject({
      data: {
        status: 'completed',
        model: 'fixture-model',
        finish_reason: 'tool-calls',
        usage: { input_tokens: 10, output_tokens: 2 },
      },
      links: [{ type: 'result_of', record: requests[0].id }],
    });
    expect(responses[1]).toMatchObject({
      data: {
        status: 'completed',
        model: 'fallback-model',
        finish_reason: 'stop',
        usage: { input_tokens: 20, output_tokens: 4 },
      },
      links: [{ type: 'result_of', record: requests[1].id }],
    });
    await expectContractRecords(records);
  });

  it('records a named loss instead of heuristically correlating a tool span', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-tool-correlation-'));
    const capture = initialize({ output, serviceName: 'mastra-tool-correlation-fixture' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);

    const emit = async (
      type: typeof Mastra.TracingEventType[keyof typeof Mastra.TracingEventType],
      span: Record<string, unknown>,
    ) => adapter.exporter.exportTracingEvent({ type, exportedSpan: span as never });
    const base = (id: string, traceId: string, type: string, parentSpanId?: string) => ({
      id,
      traceId,
      parentSpanId,
      name: id,
      type,
      isRootSpan: !parentSpanId,
      isEvent: false,
      startTime: new Date('2026-07-27T10:20:00.000Z'),
    });
    const traceId = '00000000000000000000000000000006';
    const agent = base('correlation-agent', traceId, Mastra.SpanType.AGENT_RUN);
    const generation = base(
      'correlation-generation',
      traceId,
      Mastra.SpanType.MODEL_GENERATION,
      agent.id,
    );
    const step = base('correlation-step', traceId, Mastra.SpanType.MODEL_STEP, generation.id);
    const inference = base(
      'correlation-inference',
      traceId,
      Mastra.SpanType.MODEL_INFERENCE,
      step.id,
    );
    const chunk = base(
      'correlation-chunk',
      traceId,
      Mastra.SpanType.MODEL_CHUNK,
      inference.id,
    );
    const tool = {
      ...base('unrelated-tool-span-id', traceId, Mastra.SpanType.TOOL_CALL, step.id),
      entityName: 'lookup_order',
      input: { order_id: 'A-17' },
    };

    await emit(Mastra.TracingEventType.SPAN_STARTED, agent);
    await emit(Mastra.TracingEventType.SPAN_STARTED, generation);
    await emit(Mastra.TracingEventType.SPAN_STARTED, step);
    await emit(Mastra.TracingEventType.SPAN_STARTED, inference);
    await emit(Mastra.TracingEventType.SPAN_UPDATED, {
      ...step,
      input: [{ role: 'user', content: 'Look up A-17.' }],
    });
    await emit(Mastra.TracingEventType.SPAN_STARTED, chunk);
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...chunk,
      output: {
        toolCallId: 'call-1',
        toolName: 'lookup_order',
        toolInput: { order_id: 'A-17' },
      },
    });
    await emit(Mastra.TracingEventType.SPAN_STARTED, tool);
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...tool,
      output: { status: 'shipped' },
      attributes: { success: true },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...agent,
      output: { text: 'The order shipped.' },
    });

    await capture.flush();
    const records = (await readFile(
      join(capture.status().artifactPath, 'trace.jsonl'),
      'utf8',
    )).trim().split('\n').map((line) => JSON.parse(line));
    const proposal = records.find((record) => record.kind === 'tool.proposal');
    const call = records.find((record) => record.kind === 'tool.call');
    expect(records.filter((record) => record.kind === 'loss')).toMatchObject([{
      data: {
        reason: 'mastra_tool_correlation_unavailable',
        stage: 'source',
        count: 1,
        recoverable: false,
      },
    }]);
    expect(call.links ?? []).not.toContainEqual({
      type: 'derived_from',
      record: proposal.id,
    });
    await expectContractRecords(records);
  });

  it('re-records the suffix after an exact context-prefix identity changes', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-cloned-context-'));
    const capture = initialize({ output, serviceName: 'mastra-cloned-context-fixture' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);

    const emit = async (
      type: typeof Mastra.TracingEventType[keyof typeof Mastra.TracingEventType],
      span: Record<string, unknown>,
    ) => adapter.exporter.exportTracingEvent({ type, exportedSpan: span as never });
    const base = (id: string, traceId: string, type: string, parentSpanId?: string) => ({
      id,
      traceId,
      parentSpanId,
      name: id,
      type,
      isRootSpan: !parentSpanId,
      isEvent: false,
      startTime: new Date('2026-07-27T10:30:00.000Z'),
    });

    const traceId = '00000000000000000000000000000005';
    const agent = {
      ...base('clone-agent', traceId, Mastra.SpanType.AGENT_RUN),
      input: { request: 'answer' },
    };
    const generation = {
      ...base('clone-generation', traceId, Mastra.SpanType.MODEL_GENERATION, agent.id),
      input: { messages: [] },
      attributes: { model: 'fixture-model' },
    };
    const firstMessages = [
      { role: 'system', content: 'Answer briefly.' },
      { role: 'user', content: 'Answer.' },
    ];
    const changedPrefixMessages = [{ ...firstMessages[0] }, firstMessages[1]];

    await emit(Mastra.TracingEventType.SPAN_STARTED, agent);
    await emit(Mastra.TracingEventType.SPAN_STARTED, generation);
    for (const [index, messages] of [firstMessages, changedPrefixMessages].entries()) {
      const step = {
        ...base(`clone-step-${index}`, traceId, Mastra.SpanType.MODEL_STEP, generation.id),
        attributes: { stepIndex: index },
      };
      await emit(Mastra.TracingEventType.SPAN_STARTED, step);
      await emit(Mastra.TracingEventType.SPAN_UPDATED, { ...step, input: messages });
      await emit(Mastra.TracingEventType.SPAN_ENDED, {
        ...step,
        input: messages,
        output: { text: `answer-${index}`, toolCalls: [] },
        attributes: { stepIndex: index, finishReason: 'stop' },
      });
    }
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...generation,
      output: { text: 'answer-1' },
      attributes: { model: 'fixture-model', finishReason: 'stop' },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...agent,
      output: { text: 'answer-1' },
    });

    await capture.flush();
    const records = (await readFile(
      join(capture.status().artifactPath, 'trace.jsonl'),
      'utf8',
    )).trim().split('\n').map((line) => JSON.parse(line));
    const messages = records.filter((record) => record.kind === 'message');
    const requests = records.filter((record) => record.kind === 'model.request');
    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    expect(messages).toHaveLength(4);
    expect(requests).toHaveLength(2);
    expect(requests[0].data.context_refs).toEqual([
      messages[0].id,
      messages[1].id,
    ]);
    expect(requests[1].data.context_refs).toEqual([
      messages[2].id,
      messages[3].id,
    ]);
    await expectContractRecords(records);
  });

  it('closes an abandoned model step from the exact failed generation', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-mastra-model-error-'));
    const capture = initialize({ output, serviceName: 'mastra-model-error-fixture' });
    const adapter = mastraAdapter({ version });
    capture.installSource(adapter.source);

    const emit = async (
      type: typeof Mastra.TracingEventType[keyof typeof Mastra.TracingEventType],
      span: Record<string, unknown>,
    ) => adapter.exporter.exportTracingEvent({ type, exportedSpan: span as never });
    const base = (id: string, traceId: string, type: string, parentSpanId?: string) => ({
      id,
      traceId,
      parentSpanId,
      name: id,
      type,
      isRootSpan: !parentSpanId,
      isEvent: false,
      startTime: new Date('2026-07-27T11:00:00.000Z'),
    });

    const traceId = '00000000000000000000000000000004';
    const agent = {
      ...base('failed-agent', traceId, Mastra.SpanType.AGENT_RUN),
      input: { request: 'answer' },
    };
    const generation = {
      ...base('failed-generation', traceId, Mastra.SpanType.MODEL_GENERATION, agent.id),
      input: { messages: [{ role: 'user', content: 'answer' }] },
      attributes: { model: 'fixture-model' },
    };
    const step = {
      ...base('failed-step', traceId, Mastra.SpanType.MODEL_STEP, generation.id),
      attributes: { stepIndex: 0 },
    };
    const inference = {
      ...base('failed-inference', traceId, Mastra.SpanType.MODEL_INFERENCE, step.id),
      attributes: { model: 'fixture-model', stepIndex: 0 },
    };
    const context = [{ role: 'user', content: 'answer' }];

    await emit(Mastra.TracingEventType.SPAN_STARTED, agent);
    await emit(Mastra.TracingEventType.SPAN_STARTED, generation);
    await emit(Mastra.TracingEventType.SPAN_STARTED, step);
    await emit(Mastra.TracingEventType.SPAN_STARTED, inference);
    await emit(Mastra.TracingEventType.SPAN_UPDATED, { ...step, input: context });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...generation,
      output: { text: '' },
      attributes: { model: 'fixture-model', finishReason: 'error' },
      errorInfo: { name: 'ModelError', message: 'The generation failed.' },
    });
    await emit(Mastra.TracingEventType.SPAN_ENDED, {
      ...agent,
      errorInfo: { name: 'ModelError', message: 'The model call failed.' },
    });

    await capture.flush();
    const records = (await readFile(
      join(capture.status().artifactPath, 'trace.jsonl'),
      'utf8',
    )).trim().split('\n').map((line) => JSON.parse(line));
    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    expect(records.map((record) => record.kind)).toEqual([
      'run.start',
      'message',
      'model.request',
      'model.response',
      'error',
      'run.outcome',
    ]);
    const request = records.find((record) => record.kind === 'model.request')!;
    expect(records.find((record) => record.kind === 'model.response')).toMatchObject({
      data: { status: 'failed', finish_reason: 'error' },
      links: [{ type: 'result_of', record: request.id }],
    });
    expect(records.find((record) => record.kind === 'run.outcome')).toMatchObject({
      data: { status: 'failed' },
    });
    await expectContractRecords(records);
  });
});
