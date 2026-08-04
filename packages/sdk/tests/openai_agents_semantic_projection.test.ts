import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import * as AgentsCurrent from 'agents-current';
import * as AgentsPrevious from 'agents-previous';
import { afterEach, describe, expect, it } from 'vitest';

import {
  initialize,
  openAIAgentsAdapter,
  resetCaptureForTests,
  validateArtifact,
} from '../src/index.js';
import type { SemanticTraceRecord } from '../src/trace/semantic-projector.js';

type AgentsModule = typeof AgentsCurrent | typeof AgentsPrevious;
type CurrentTool = ReturnType<typeof AgentsCurrent.tool>;
type PreviousTool = ReturnType<typeof AgentsPrevious.tool>;
type AgentsTool = CurrentTool | PreviousTool;
type CurrentTraceProvider = ReturnType<typeof AgentsCurrent.getGlobalTraceProvider>;
type PreviousTraceProvider = ReturnType<typeof AgentsPrevious.getGlobalTraceProvider>;
type AgentsTraceProvider = CurrentTraceProvider | PreviousTraceProvider;
type CurrentTrace = ReturnType<CurrentTraceProvider['createTrace']>;
type PreviousTrace = ReturnType<PreviousTraceProvider['createTrace']>;
type AgentsTrace = CurrentTrace | PreviousTrace;
type CompatibleSpanOptions =
  & Parameters<CurrentTraceProvider['createSpan']>[0]
  & Parameters<PreviousTraceProvider['createSpan']>[0];
type CompatibleCustomSpanOptions =
  & Parameters<typeof AgentsCurrent.createCustomSpan>[0]
  & Parameters<typeof AgentsPrevious.createCustomSpan>[0];

afterEach(async () => resetCaptureForTests());

it('reports OpenAI Agents qualification from the observed version only', () => {
  const subject = { addTraceProcessor() {}, async run() {} };

  expect(openAIAgentsAdapter({ version: '0.13.2' })
    .createSource(subject).metadata.qualification).toEqual({ status: 'exact_qualified' });
  expect(openAIAgentsAdapter({ version: '1.0.0' })
    .createSource(subject).metadata.qualification).toEqual({
      status: 'capability_checked_unqualified', profile: 'openai-agents-tracing-processor-v1',
    });
  expect(openAIAgentsAdapter().createSource(subject).metadata.qualification).toEqual({
    status: 'unknown',
  });
});

describe.each([
  ['0.13.2', AgentsCurrent],
  ['0.13.1', AgentsPrevious],
] as const)('OpenAI Agents semantic projection %s', (version, Agents) => {
  it('maps a streamed Runner agent span to a zero-loss agent scope', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-runner-'));
    const capture = initialize({ output, serviceName: 'openai-agents-runner-fixture' });
    const fixtureModel = {
      async getResponse(): Promise<never> {
        throw new Error('The streamed Runner fixture must use getStreamedResponse.');
      },
      async *getStreamedResponse() {
        const response = {
          id: 'fixture-response',
          usage: new Agents.Usage({
            requests: 1,
            inputTokens: 0,
            outputTokens: 0,
            totalTokens: 0,
          }),
          output: [{
            id: 'fixture-message',
            status: 'completed' as const,
            type: 'message' as const,
            role: 'assistant' as const,
            content: [{
              type: 'output_text' as const,
              text: 'Fixture response.',
              providerData: { annotations: [] },
            }],
          }],
        };
        const span = Agents.getGlobalTraceProvider().createSpan({
          data: {
            type: 'generation',
            model: 'fixture-model',
            model_config: {},
            input: [{ role: 'user', content: 'Run once.' }],
            output: response.output,
            usage: { input_tokens: 0, output_tokens: 0 },
          },
        });
        await span.start();
        try {
          yield { type: 'response_done' as const, response };
        } finally {
          await span.end();
        }
      },
    };
    const runner = new Agents.Runner({ model: fixtureModel });
    const agent = new Agents.Agent({ name: 'Fixture Agent' });
    const adapter = openAIAgentsAdapter({ version });
    let nativeResult: unknown;
    capture.instrument({
      adapter,
      client: {
        addTraceProcessor: Agents.addTraceProcessor,
        async run(...args: unknown[]) {
          nativeResult = await Reflect.apply(runner.run, runner, args);
          return nativeResult;
        },
      },
    });
    Agents.setTracingDisabled(false);

    try {
      const stream = await adapter.run(agent, 'Run once.', { stream: true }) as AsyncIterable<unknown> & {
        completed: Promise<unknown>;
      };
      expect(stream).toBe(nativeResult);
      let eventCount = 0;
      for await (const _event of stream) eventCount += 1;
      await stream.completed;
      await Agents.getGlobalTraceProvider().forceFlush();
      expect(eventCount).toBeGreaterThan(0);

      const closed = await capture.shutdown();
      expect(await validateArtifact(closed.artifactPath)).toMatchObject({
        valid: true,
        issues: [],
      });
      const records = await semanticRecords(closed.artifactPath);
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
      const scopes = records.filter((record) => record.kind === 'scope');
      expect(scopes.map((record) => record.data)).toEqual([
        expect.objectContaining({
          type: 'agent',
          phase: 'start',
          name: 'Fixture Agent',
        }),
        expect.objectContaining({
          type: 'agent',
          phase: 'end',
          status: 'completed',
        }),
      ]);
      const modelRecords = records.filter((record) => (
        record.kind === 'model.request' || record.kind === 'model.response'
      ));
      expect(modelRecords).toHaveLength(2);
      expect(modelRecords.every((record) => record.parent === scopes[0]?.id)).toBe(true);
      expect(records.filter((record) => record.kind === 'run.outcome')).toEqual([
        expect.objectContaining({
          data: expect.objectContaining({
            status: 'completed',
            output: 'Fixture response.',
          }),
        }),
      ]);
    } finally {
      Agents.setTracingDisabled(true);
    }
  });

  it('marks the active model and agent scope cancelled when the application closes a stream', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-stream-cancel-'));
    const capture = initialize({ output, serviceName: 'openai-agents-stream-cancel-fixture' });
    const trace = { traceId: 'trace-stream-cancel', name: 'Agent workflow' };
    const agentSpan = {
      traceId: trace.traceId,
      spanId: 'span-agent-cancel',
      parentId: null,
      spanData: { type: 'agent', name: 'Cancelled Agent' },
    };
    const generationSpan = {
      traceId: trace.traceId,
      spanId: 'span-generation-cancel',
      parentId: agentSpan.spanId,
      spanData: {
        type: 'generation',
        model: 'fixture-model',
        model_config: {},
        input: [{ role: 'user', content: 'Start, then stop.' }],
        output: [],
      },
    };
    let processor: {
      onTraceStart(value: typeof trace): void | Promise<void>;
      onTraceEnd(value: typeof trace): void | Promise<void>;
      onSpanStart(value: typeof agentSpan | typeof generationSpan): void | Promise<void>;
      onSpanEnd(value: typeof agentSpan | typeof generationSpan): void | Promise<void>;
    } | undefined;
    let finishDeferredSpans!: () => void;
    const deferredSpansFinished = new Promise<void>((resolve) => {
      finishDeferredSpans = resolve;
    });

    class StreamedRunResult implements AsyncIterableIterator<unknown> {
      private readonly events = [new Agents.RunRawModelStreamEvent({
          type: 'model',
          event: {
            type: 'response.created',
            response: { id: 'resp-cancelled-reasoning' },
          },
        } as never), new Agents.RunRawModelStreamEvent({
          type: 'model',
          event: {
            type: 'response.reasoning_summary_text.delta',
            item_id: 'reasoning-cancelled-1',
            output_index: 0,
            summary_index: 0,
            sequence_number: 2,
            delta: 'Keep this ',
          },
        } as never), new Agents.RunRawModelStreamEvent({
          type: 'model',
          event: {
            type: 'response.reasoning_summary_text.delta',
            item_id: 'reasoning-cancelled-1',
            output_index: 0,
            summary_index: 0,
            sequence_number: 3,
            delta: 'summary.',
          },
        } as never)];

      [Symbol.asyncIterator]() {
        return this;
      }

      async next(): Promise<IteratorResult<unknown>> {
        const event = this.events.shift();
        return {
          done: false,
          value: event,
        };
      }

      async return(): Promise<IteratorResult<unknown>> {
        if (!processor) throw new Error('Fixture processor was not installed');
        const installed = processor;
        setTimeout(() => {
          void (async () => {
            await installed.onSpanEnd(generationSpan);
            await installed.onSpanEnd(agentSpan);
            await installed.onTraceEnd(trace);
            finishDeferredSpans();
          })();
        }, 0);
        return { done: true, value: undefined };
      }
    }

    const subject = {
      StreamedRunResult,
      addTraceProcessor(value: NonNullable<typeof processor>) {
        processor = value;
      },
      async run() {
        if (!processor) throw new Error('Fixture processor was not installed');
        await processor.onTraceStart(trace);
        await processor.onSpanStart(agentSpan);
        await processor.onSpanStart(generationSpan);
        return new StreamedRunResult();
      },
    };
    const adapter = openAIAgentsAdapter({ version });
    capture.instrument({ adapter, client: subject });

    const stream = await adapter.run() as AsyncIterable<unknown>;
    let consumed = 0;
    for await (const _event of stream) {
      consumed += 1;
      if (consumed === 3) break;
    }
    await deferredSpansFinished;

    const closed = await capture.shutdown();
    expect(await validateArtifact(closed.artifactPath)).toMatchObject({
      valid: true,
      issues: [],
    });
    const records = await semanticRecords(closed.artifactPath);
    expect(records.filter((record) => record.kind === 'loss')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          detail: 'openai_agents_reasoning_model_request_correlation_not_captured',
        }),
      }),
    ]);
    const responses = records.filter((record) => record.kind === 'model.response');
    expect(responses).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          status: 'cancelled',
          finish_reason: 'application_return',
        }),
      }),
    ]);
    expect(responses[0]?.data.reasoning).toBeUndefined();
    expect(records.filter((record) => (
      record.kind === 'state'
      && record.data.type === 'openai_agents.stream.reasoning_partial'
    ))).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          value: expect.objectContaining({
            status: 'cancelled',
            response_id: 'resp-cancelled-reasoning',
            reasoning: [{ type: 'summary', text: 'Keep this summary.' }],
          }),
        }),
      }),
    ]);
    expect(records.filter((record) => (
      record.kind === 'scope' && record.data.phase === 'end'
    ))).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({ status: 'cancelled' }),
      }),
    ]);
    expect(records.filter((record) => record.kind === 'run.outcome')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({ status: 'cancelled' }),
      }),
    ]);
  });

  it('does not assign reasoning deltas when response lifecycles overlap', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-overlap-'));
    const capture = initialize({ output, serviceName: 'openai-agents-overlap-fixture' });
    const trace = { traceId: 'trace-stream-overlap', name: 'Overlapping responses' };
    let processor: {
      onTraceStart(value: typeof trace): void | Promise<void>;
      onTraceEnd(value: typeof trace): void | Promise<void>;
    } | undefined;
    const events = ['resp-overlap-a', 'resp-overlap-b'].map((id, index) => (
      new Agents.RunRawModelStreamEvent({
        type: 'model',
        event: {
          type: 'response.created',
          sequence_number: index + 1,
          response: { id },
        },
      } as never)
    ));
    events.push(new Agents.RunRawModelStreamEvent({
      type: 'model',
      event: {
        type: 'response.reasoning_text.delta',
        sequence_number: 3,
        item_id: 'reasoning-overlap',
        output_index: 0,
        content_index: 0,
        delta: 'Do not guess.',
      },
    } as never));

    class StreamedRunResult implements AsyncIterableIterator<unknown> {
      [Symbol.asyncIterator]() { return this; }
      async next(): Promise<IteratorResult<unknown>> {
        const value = events.shift();
        return value ? { done: false, value } : { done: true, value: undefined };
      }
      async return(): Promise<IteratorResult<unknown>> {
        return { done: true, value: undefined };
      }
    }
    const subject = {
      StreamedRunResult,
      addTraceProcessor(value: NonNullable<typeof processor>) { processor = value; },
      async run() {
        if (!processor) throw new Error('Fixture processor was not installed');
        await processor.onTraceStart(trace);
        return new StreamedRunResult();
      },
    };
    const adapter = openAIAgentsAdapter({ version });
    capture.instrument({ adapter, client: subject });
    const stream = await adapter.run() as AsyncIterable<unknown>;
    let consumed = 0;
    for await (const _event of stream) {
      consumed += 1;
      if (consumed === 3) break;
    }
    await processor!.onTraceEnd(trace);

    const records = await semanticRecords((await capture.shutdown()).artifactPath);
    expect(records.filter((record) => record.kind === 'model.response')).toEqual([]);
    expect(records.filter((record) => (
      record.kind === 'state'
      && record.data.type === 'openai_agents.stream.reasoning_partial'
    ))).toEqual([]);
    expect(records.filter((record) => record.kind === 'loss')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          detail: 'openai_agents_reasoning_response_correlation_not_captured',
        }),
      }),
    ]);
  });

  it('correlates streamed tool proposals with observed execution results', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-stream-tool-'));
    const capture = initialize({ output, serviceName: 'openai-agents-stream-tool-fixture' });
    const callId = 'call-stream-exact-1';
    const toolInput = '{"path":"src/stream.ts"}';
    let turn = 0;
    let toolExecutions = 0;
    const fixtureModel = {
      async getResponse(): Promise<never> {
        throw new Error('The streamed tool fixture must use getStreamedResponse.');
      },
      async *getStreamedResponse(request: { input: unknown }) {
        turn += 1;
        const response = turn === 1 ? {
          id: 'fixture-stream-tool-response',
          usage: new Agents.Usage({
            requests: 1,
            inputTokens: 1,
            outputTokens: 1,
            totalTokens: 2,
          }),
          output: [{
            type: 'function_call' as const,
            callId,
            name: 'read_file',
            arguments: toolInput,
            status: 'completed' as const,
          }],
        } : {
          id: 'fixture-stream-final-response',
          usage: new Agents.Usage({
            requests: 1,
            inputTokens: 1,
            outputTokens: 1,
            totalTokens: 2,
          }),
          output: [{
            id: 'fixture-stream-final-message',
            status: 'completed' as const,
            type: 'message' as const,
            role: 'assistant' as const,
            content: [{
              type: 'output_text' as const,
              text: 'Stream tool complete.',
              providerData: { annotations: [] },
            }],
          }],
        };
        const span = Agents.getGlobalTraceProvider().createSpan({
          data: {
            type: 'generation',
            model: 'fixture-model',
            model_config: {},
            input: fixtureModelInput(request.input),
            output: response.output,
            usage: { input_tokens: 1, output_tokens: 1 },
          },
        });
        await span.start();
        try {
          yield { type: 'response_done' as const, response };
        } finally {
          await span.end();
        }
      },
    };
    const readFileTool = Agents.tool({
      name: 'read_file',
      description: 'Read a fixture file.',
      parameters: {
        type: 'object',
        properties: { path: { type: 'string' } },
        required: ['path'],
        additionalProperties: false,
      },
      execute(input: unknown) {
        toolExecutions += 1;
        expect(input).toEqual({ path: 'src/stream.ts' });
        return 'export const streamed = true;';
      },
    });
    const runner = new Agents.Runner({ model: fixtureModel });
    const agent = fixtureToolAgent(Agents, 'Fixture Stream Tool Agent', readFileTool);
    const adapter = openAIAgentsAdapter({ version });
    let nativeResult: unknown;
    capture.instrument({
      adapter,
      client: {
        addTraceProcessor: Agents.addTraceProcessor,
        async run(...args: unknown[]) {
          nativeResult = await Reflect.apply(runner.run, runner, args);
          return nativeResult;
        },
      },
    });
    Agents.setTracingDisabled(false);

    try {
      const stream = await adapter.run(agent, 'Inspect src/stream.ts.', { stream: true }) as
        AsyncIterable<unknown> & { completed: Promise<unknown> };
      expect(stream).toBe(nativeResult);
      let eventCount = 0;
      for await (const _event of stream) eventCount += 1;
      await stream.completed;
      expect(eventCount).toBeGreaterThan(0);
      expect(toolExecutions).toBe(1);

      await Agents.getGlobalTraceProvider().forceFlush();
      const closed = await capture.shutdown();
      expect(await validateArtifact(closed.artifactPath, { profile: 'rich-agent' }))
        .toMatchObject({ valid: true, issues: [] });
      const records = await semanticRecords(closed.artifactPath);
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
      const proposal = records.find((record) => record.kind === 'tool.proposal')!;
      expect(records.filter((record) => record.kind === 'tool.proposal')).toHaveLength(1);
      expect(proposal.data).toMatchObject({
        native_call_id: callId,
        name: 'read_file',
        input: toolInput,
      });
      const call = records.find((record) => record.kind === 'tool.call')!;
      expect(call.data).toMatchObject({
        native_call_id: callId,
        name: 'read_file',
        input: toolInput,
      });
      expect(call.links).toContainEqual({ type: 'derived_from', record: proposal.id });
      const toolResult = records.find((record) => record.kind === 'tool.result')!;
      expect(toolResult.data).toMatchObject({
        native_call_id: callId,
        status: 'succeeded',
        output: 'export const streamed = true;',
      });
      expect(toolResult.links).toContainEqual({ type: 'result_of', record: call.id });
      expect(records.filter((record) => (
        record.kind === 'message'
        && record.origin === 'observed'
        && record.data.role === 'tool'
      ))).toEqual([]);
      expect(records.filter((record) => record.kind === 'run.outcome')).toEqual([
        expect.objectContaining({
          data: expect.objectContaining({
            status: 'completed',
            output: 'Stream tool complete.',
          }),
        }),
      ]);
    } finally {
      Agents.setTracingDisabled(true);
    }
  });

  it('correlates non-stream RunResult proposals with observed execution results', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-result-'));
    const capture = initialize({ output, serviceName: 'openai-agents-result-fixture' });
    const callId = 'call-exact-1';
    const toolInput = '{"path":"src/app.ts"}';
    let turn = 0;
    let toolExecutions = 0;
    const fixtureModel = {
      async getResponse(request: { input: unknown }) {
        turn += 1;
        const response = turn === 1 ? {
          responseId: 'fixture-tool-response',
          usage: new Agents.Usage({
            requests: 1,
            inputTokens: 1,
            outputTokens: 1,
            totalTokens: 2,
          }),
          output: [{
            type: 'function_call' as const,
            callId,
            name: 'read_file',
            arguments: toolInput,
            status: 'completed' as const,
          }],
        } : {
          responseId: 'fixture-final-response',
          usage: new Agents.Usage({
            requests: 1,
            inputTokens: 1,
            outputTokens: 1,
            totalTokens: 2,
          }),
          output: [{
            id: 'fixture-final-message',
            status: 'completed' as const,
            type: 'message' as const,
            role: 'assistant' as const,
            content: [{
              type: 'output_text' as const,
              text: 'The answer is 42.',
              providerData: { annotations: [] },
            }],
          }],
        };
        const span = Agents.getGlobalTraceProvider().createSpan({
          data: {
            type: 'generation',
            model: 'fixture-model',
            model_config: {},
            input: fixtureModelInput(request.input),
            output: response.output,
            usage: { input_tokens: 1, output_tokens: 1 },
          },
        });
        await span.start();
        await span.end();
        return response;
      },
      async *getStreamedResponse(): AsyncIterable<never> {
        yield await Promise.reject(
          new Error('The non-stream Runner fixture must use getResponse.'),
        );
      },
    };
    const readFileTool = Agents.tool({
      name: 'read_file',
      description: 'Read a fixture file.',
      parameters: {
        type: 'object',
        properties: { path: { type: 'string' } },
        required: ['path'],
        additionalProperties: false,
      },
      execute(input: unknown) {
        toolExecutions += 1;
        expect(input).toEqual({ path: 'src/app.ts' });
        return 'export const answer = 42;';
      },
    });
    const runner = new Agents.Runner({ model: fixtureModel });
    const agent = fixtureToolAgent(Agents, 'Fixture Tool Agent', readFileTool);
    const adapter = openAIAgentsAdapter({ version });
    let nativeResult: unknown;
    capture.instrument({
      adapter,
      client: {
        addTraceProcessor: Agents.addTraceProcessor,
        async run(...args: unknown[]) {
          nativeResult = await Reflect.apply(runner.run, runner, args);
          return nativeResult;
        },
      },
    });
    Agents.setTracingDisabled(false);

    try {
      const result = await adapter.run(agent, 'Inspect src/app.ts.');
      expect(result).toBe(nativeResult);
      expect(toolExecutions).toBe(1);

      await Agents.getGlobalTraceProvider().forceFlush();
      const closed = await capture.shutdown();
      expect(await validateArtifact(closed.artifactPath, { profile: 'rich-agent' })).toMatchObject({
        valid: true,
        issues: [],
      });
      const records = await semanticRecords(closed.artifactPath);
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);

      const proposal = records.find((record) => record.kind === 'tool.proposal')!;
      expect(records.filter((record) => record.kind === 'tool.proposal')).toHaveLength(1);
      expect(proposal.data).toMatchObject({
        native_call_id: callId,
        name: 'read_file',
        input: toolInput,
      });
      const call = records.find((record) => record.kind === 'tool.call')!;
      expect(call.data).toMatchObject({
        native_call_id: callId,
        name: 'read_file',
        input: toolInput,
      });
      expect(call.links).toContainEqual({ type: 'derived_from', record: proposal.id });
      const toolResult = records.find((record) => record.kind === 'tool.result')!;
      expect(toolResult.data).toMatchObject({
        native_call_id: callId,
        status: 'succeeded',
        output: 'export const answer = 42;',
      });
      expect(toolResult.links).toContainEqual({ type: 'result_of', record: call.id });
      expect(records.filter((record) => (
        record.kind === 'message'
        && record.origin === 'observed'
        && record.data.role === 'tool'
      ))).toEqual([]);
      expect(records.filter((record) => record.kind === 'run.outcome')).toEqual([
        expect.objectContaining({
          data: expect.objectContaining({
            status: 'completed',
            output: 'The answer is 42.',
          }),
        }),
      ]);
    } finally {
      Agents.setTracingDisabled(true);
    }
  });

  it('settles multiple Runner invocations inside one trace with the latest exact output', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-existing-trace-'));
    const capture = initialize({ output, serviceName: 'openai-agents-existing-trace-fixture' });
    let turn = 0;
    const fixtureModel = {
      async getResponse(request: { input: unknown }) {
        turn += 1;
        const response = {
          responseId: `fixture-existing-trace-response-${turn}`,
          usage: new Agents.Usage({
            requests: 1,
            inputTokens: 1,
            outputTokens: 1,
            totalTokens: 2,
          }),
          output: [{
            id: `fixture-existing-trace-message-${turn}`,
            status: 'completed' as const,
            type: 'message' as const,
            role: 'assistant' as const,
            content: [{
              type: 'output_text' as const,
              text: `Existing trace output ${turn}.`,
              providerData: { annotations: [] },
            }],
          }],
        };
        const span = Agents.getGlobalTraceProvider().createSpan({
          data: {
            type: 'generation',
            model: 'fixture-model',
            model_config: {},
            input: fixtureModelInput(request.input),
            output: response.output,
            usage: { input_tokens: 1, output_tokens: 1 },
          },
        });
        await span.start();
        await span.end();
        return response;
      },
      async *getStreamedResponse(): AsyncIterable<never> {
        yield await Promise.reject(
          new Error('The existing-trace fixture must use getResponse.'),
        );
      },
    };
    const runner = new Agents.Runner({ model: fixtureModel });
    const agent = new Agents.Agent({ name: 'Existing Trace Agent' });
    const adapter = openAIAgentsAdapter({ version });
    let nativeResult: unknown;
    capture.instrument({
      adapter,
      client: {
        addTraceProcessor: Agents.addTraceProcessor,
        async run(...args: unknown[]) {
          nativeResult = await Reflect.apply(runner.run, runner, args);
          return nativeResult;
        },
      },
    });
    Agents.setTracingDisabled(false);

    try {
      const result = await Agents.withTrace(
        `existing-trace-${version}`,
        async () => {
          await adapter.run(agent, 'First run inside the existing trace.');
          return await adapter.run(agent, 'Second run inside the existing trace.');
        },
      );
      expect(result).toBe(nativeResult);

      await Agents.getGlobalTraceProvider().forceFlush();
      const records = await semanticRecords((await capture.shutdown()).artifactPath);
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
      expect(records.filter((record) => record.kind === 'run.start')).toHaveLength(1);
      expect(records.filter((record) => record.kind === 'model.request')).toHaveLength(2);
      expect(records.filter((record) => record.kind === 'model.response')).toHaveLength(2);
      expect(records.filter((record) => record.kind === 'run.outcome')).toEqual([
        expect.objectContaining({
          data: expect.objectContaining({
            status: 'completed',
            output: 'Existing trace output 2.',
          }),
        }),
      ]);
    } finally {
      Agents.setTracingDisabled(true);
    }
  });

  it('records one error for a failed agent scope', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-failed-scope-'));
    const capture = initialize({ output, serviceName: 'openai-agents-failed-scope-fixture' });
    capture.instrument({ adapter: openAIAgentsAdapter({ version }), client: Agents });
    Agents.setTracingDisabled(false);

    try {
      const provider = Agents.getGlobalTraceProvider();
      const trace = provider.createTrace({ name: 'failed-agent-run' });
      await trace.start();
      const agentSpan = fixtureSpan(Agents, provider, {
        data: { type: 'agent', name: 'Failing Agent' },
      }, trace);
      await agentSpan.start();
      agentSpan.setError({
        message: 'fixture agent failure',
        data: { code: 'AGENT_FIXTURE_FAILURE' },
      });
      await agentSpan.end();
      await trace.end();
      await provider.forceFlush();

      const closed = await capture.shutdown();
      expect(await validateArtifact(closed.artifactPath)).toMatchObject({
        valid: true,
        issues: [],
      });
      const records = await semanticRecords(closed.artifactPath);
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
      expect(records.filter((record) => record.kind === 'scope').map((record) => record.data))
        .toEqual([
          expect.objectContaining({ type: 'agent', phase: 'start', name: 'Failing Agent' }),
          expect.objectContaining({ type: 'agent', phase: 'end', status: 'failed' }),
        ]);
      expect(records.filter((record) => record.kind === 'error')).toEqual([
        expect.objectContaining({
          data: expect.objectContaining({
            type: 'agent_span_error',
            code: 'AGENT_FIXTURE_FAILURE',
          }),
        }),
      ]);
    } finally {
      Agents.setTracingDisabled(true);
    }
  });

  it('closes an open agent span as a cancelled scope on shutdown', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-cancelled-scope-'));
    const capture = initialize({ output, serviceName: 'openai-agents-cancelled-scope-fixture' });
    capture.instrument({ adapter: openAIAgentsAdapter({ version }), client: Agents });
    Agents.setTracingDisabled(false);
    const provider = Agents.getGlobalTraceProvider();
    const trace = provider.createTrace({ name: 'cancelled-agent-run' });
    const agentSpan = fixtureSpan(Agents, provider, {
      data: { type: 'agent', name: 'Cancelled Agent' },
    }, trace);

    try {
      await trace.start();
      await agentSpan.start();
      await provider.forceFlush();
      const closed = await capture.shutdown();
      expect(await validateArtifact(closed.artifactPath)).toMatchObject({
        valid: true,
        issues: [],
      });
      const records = await semanticRecords(closed.artifactPath);
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
      expect(records.filter((record) => record.kind === 'scope').map((record) => record.data))
        .toEqual([
          expect.objectContaining({ type: 'agent', phase: 'start', name: 'Cancelled Agent' }),
          expect.objectContaining({ type: 'agent', phase: 'end', status: 'cancelled' }),
        ]);
    } finally {
      await agentSpan.end();
      await trace.end();
      Agents.setTracingDisabled(true);
    }
  });

  it('keeps unknown native span shapes explicit as loss', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-unknown-span-'));
    const capture = initialize({ output, serviceName: 'openai-agents-unknown-span-fixture' });
    capture.instrument({ adapter: openAIAgentsAdapter({ version }), client: Agents });
    Agents.setTracingDisabled(false);

    try {
      const provider = Agents.getGlobalTraceProvider();
      const trace = provider.createTrace({ name: 'unknown-span-run' });
      await trace.start();
      const customSpan = fixtureCustomSpan(Agents, {
        data: { name: 'unsupported-fixture', data: {} },
      }, trace);
      await customSpan.start();
      await customSpan.end();
      await trace.end();
      await provider.forceFlush();

      const closed = await capture.shutdown();
      expect(await validateArtifact(closed.artifactPath)).toMatchObject({
        valid: true,
        issues: [],
      });
      const records = await semanticRecords(closed.artifactPath);
      expect(records.filter((record) => record.kind === 'loss')).toHaveLength(2);
    } finally {
      Agents.setTracingDisabled(true);
    }
  });

  it('seals exact context, model, proposal, and error evidence into trace.jsonl', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-agents-projection-'));
    const capture = initialize({ output, serviceName: 'openai-agents-semantic-fixture' });
    capture.instrument({ adapter: openAIAgentsAdapter({ version }), client: Agents });
    Agents.setTracingDisabled(false);

    try {
      const provider = Agents.getGlobalTraceProvider();
      const trace = provider.createTrace({
        name: 'coding-agent',
        groupId: `conversation-${version}`,
      });
      await trace.start();

      const callId = 'span_0123456789abcdef01234567';
      const toolInput = '{"path":"src/app.ts"}';
      const generation = fixtureSpan(Agents, provider, {
        data: {
          type: 'generation',
          model: 'fixture-model',
          model_config: {},
          input: [
            { role: 'user', content: 'Inspect src/app.ts.' },
            {
              role: 'tool',
              tool_call_id: 'historical-call',
              content: 'Earlier run output.',
            },
            {
              role: 'assistant',
              content: [{
                type: 'text',
                text: 'Previous answer.',
                reasoning: 'private provider reasoning',
                reasoning_details: [{ type: 'reasoning.text', text: 'private' }],
              }],
            },
          ],
          output: [{
            type: 'reasoning',
            rawContent: [
              { type: 'reasoning_text', text: 'Inspected the requested path.' },
              { type: 'reasoning_text', text: 'Inspected the requested path.' },
            ],
            content: [
              { type: 'input_text', text: 'I checked the path.' },
              { type: 'input_text', text: 'I checked the path.' },
            ],
          }, {
            type: 'reasoning',
            providerData: { encrypted_content: 'opaque-provider-payload' },
          }, {
            type: 'function_call',
            call_id: callId,
            name: 'read_file',
            arguments: toolInput,
          }],
          usage: { input_tokens: 4, output_tokens: 2 },
        },
      }, trace);
      await generation.start();
      await generation.end();

      const tool = fixtureSpan(Agents, provider, {
        spanId: callId,
        data: {
          type: 'function',
          name: 'read_file',
          input: toolInput,
          output: 'export const answer = 42;',
        },
      }, trace);
      await tool.start();
      await tool.end();

      const handoff = fixtureSpan(Agents, provider, {
        data: {
          type: 'handoff',
          from_agent: 'Coding Agent',
          to_agent: 'Review Agent',
        },
      }, trace);
      await handoff.start();
      await handoff.end();

      const failedGeneration = fixtureSpan(Agents, provider, {
        data: {
          type: 'generation',
          model: 'fixture-model',
          model_config: {},
          input: [
            { role: 'user', content: 'Inspect src/app.ts.' },
            {
              role: 'tool',
              tool_call_id: 'historical-call',
              content: 'Earlier run output.',
            },
            {
              role: 'assistant',
              content: [{
                type: 'text',
                text: 'Previous answer.',
                reasoning: 'private provider reasoning',
                reasoning_details: [{ type: 'reasoning.text', text: 'private' }],
              }],
            },
            { role: 'user', content: 'Now compile it.' },
          ],
          output: [],
        },
      }, trace);
      await failedGeneration.start();
      failedGeneration.setError({
        message: 'fixture model failure',
        data: { code: 'MODEL_FIXTURE_FAILURE' },
      });
      await failedGeneration.end();
      await trace.end();
      await provider.forceFlush();

      const closed = await capture.shutdown();
      expect(await validateArtifact(closed.artifactPath)).toMatchObject({
        valid: true,
        issues: [],
      });
      const records = await semanticRecords(closed.artifactPath);

      expect(records.filter((record) => record.kind === 'loss')).toMatchObject([{
        data: { reason: 'reasoning_unavailable', stage: 'source', count: 1, recoverable: false },
      }]);
      expect(records.filter((record) => record.kind === 'run.start')).toHaveLength(1);
      expect(records.filter((record) => record.kind === 'run.outcome')).toHaveLength(1);

      const context = records.filter((record) => record.kind === 'message');
      expect(context.map((record) => [record.origin, record.data.role, record.data.content]))
        .toEqual([
          ['context', 'user', 'Inspect src/app.ts.'],
          ['context', 'tool', 'Earlier run output.'],
          ['context', 'assistant', [{
            type: 'text',
            text: 'Previous answer.',
            reasoning: 'private provider reasoning',
            reasoning_details: [{ type: 'reasoning.text', text: 'private' }],
          }]],
          ['context', 'user', 'Now compile it.'],
        ]);
      expect(context[1]?.data.call_id).toBe('historical-call');
      expect(JSON.stringify(context)).toContain('private provider reasoning');
      expect(JSON.stringify(context)).toContain('reasoning_details');
      expect(records.filter((record) => record.kind !== 'message')
        .every((record) => record.origin === 'observed')).toBe(true);

      const requests = records.filter((record) => record.kind === 'model.request');
      const responses = records.filter((record) => record.kind === 'model.response');
      expect(requests).toHaveLength(2);
      expect(responses).toHaveLength(2);
      expect(requests.map((record) => record.data.context_refs)).toEqual([
        context.slice(0, 3).map((record) => record.id),
        context.map((record) => record.id),
      ]);
      expect(responses.every((response, index) => (
        response.links?.some((link) => (
          link.type === 'result_of' && link.record === requests[index]?.id
        ))
      ))).toBe(true);
      expect(responses.map((response) => response.data.status)).toEqual([
        'completed',
        'failed',
      ]);
      expect(responses[0]?.data.usage).toEqual({ input_tokens: 4, output_tokens: 2 });
      expect(responses[0]?.data.reasoning).toEqual([
        { type: 'text', text: 'Inspected the requested path.' },
        { type: 'text', text: 'Inspected the requested path.' },
        { type: 'summary', text: 'I checked the path.' },
        { type: 'summary', text: 'I checked the path.' },
      ]);

      const proposal = records.find((record) => record.kind === 'tool.proposal')!;
      expect(proposal.data).toMatchObject({
        native_call_id: callId,
        name: 'read_file',
        input: toolInput,
      });
      expect(records.filter((record) => (
        record.kind === 'tool.call' || record.kind === 'tool.result'
      ))).toEqual([]);
      expect(records.filter((record) => record.kind === 'state')).toEqual([
        expect.objectContaining({
          data: {
            type: 'agent.handoff',
            value: {
              status: 'completed',
              from_agent: 'Coding Agent',
              to_agent: 'Review Agent',
            },
          },
        }),
      ]);

      expect(records.filter((record) => record.kind === 'error')).toEqual([
        expect.objectContaining({
          data: {
            type: 'framework_span_error',
            message: 'fixture model failure',
            recoverable: false,
            code: 'MODEL_FIXTURE_FAILURE',
            details: { code: 'MODEL_FIXTURE_FAILURE' },
          },
        }),
      ]);
      expect(records.map((record) => record.seq)).toEqual(
        records.map((_, index) => index + 1),
      );
    } finally {
      Agents.setTracingDisabled(true);
    }
  });
});

function fixtureToolAgent(Agents: AgentsModule, name: string, tool: AgentsTool) {
  if (Agents === AgentsCurrent) {
    return new AgentsCurrent.Agent({ name, tools: [tool as CurrentTool] });
  }
  return new AgentsPrevious.Agent({ name, tools: [tool as PreviousTool] });
}

function fixtureSpan(
  Agents: AgentsModule,
  provider: AgentsTraceProvider,
  options: CompatibleSpanOptions,
  parent: AgentsTrace,
) {
  if (Agents === AgentsCurrent) {
    return (provider as CurrentTraceProvider).createSpan(options, parent as CurrentTrace);
  }
  return (provider as PreviousTraceProvider).createSpan(options, parent as PreviousTrace);
}

function fixtureCustomSpan(
  Agents: AgentsModule,
  options: CompatibleCustomSpanOptions,
  parent: AgentsTrace,
) {
  if (Agents === AgentsCurrent) {
    return AgentsCurrent.createCustomSpan(options, parent as CurrentTrace);
  }
  return AgentsPrevious.createCustomSpan(options, parent as PreviousTrace);
}

function fixtureModelInput(input: unknown): Array<Record<string, unknown>> {
  if (!Array.isArray(input) || input.some((item) => (
    typeof item !== 'object' || item === null || Array.isArray(item)
  ))) {
    throw new TypeError('OpenAI Agents fixture expected record-array model input');
  }
  return input as Array<Record<string, unknown>>;
}

async function semanticRecords(path: string): Promise<SemanticTraceRecord[]> {
  const records = (await readFile(join(path, 'trace.jsonl'), 'utf8'))
    .trim()
    .split('\n')
    .map((line) => JSON.parse(line) as SemanticTraceRecord);
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
  return records;
}
