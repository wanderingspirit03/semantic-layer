import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import * as StrandsCurrent from 'strands-current';
import * as StrandsPrevious from 'strands-previous';
import { afterEach, describe, expect, it } from 'vitest';

import {
  initialize,
  resetCaptureForTests,
  strandsAdapter,
  validateArtifact,
} from '../src/index.js';
import type { SemanticTraceRecord } from '../src/trace/semantic-projector.js';

type HookConstructor = abstract new (...args: any[]) => object;

class HookHarness {
  private readonly callbacks = new Map<
    HookConstructor,
    Set<(event: any) => unknown>
  >();

  addHook(type: HookConstructor, callback: (event: any) => unknown): () => void {
    const callbacks = this.callbacks.get(type) ?? new Set();
    callbacks.add(callback);
    this.callbacks.set(type, callbacks);
    return () => callbacks.delete(callback);
  }

  async emit(event: object): Promise<void> {
    for (const callback of this.callbacks.get(event.constructor as HookConstructor) ?? []) {
      await callback(event);
    }
  }
}

afterEach(async () => resetCaptureForTests());

type StrandsFixture = readonly [
  version: '1.9.0' | '1.8.0',
  sdk: typeof StrandsCurrent | typeof StrandsPrevious,
];

const strandsFixtures: readonly StrandsFixture[] = [
  ['1.9.0', StrandsCurrent],
  ['1.8.0', StrandsPrevious],
];

it('reports Strands qualification from the observed version only', () => {
  const hooks = new HookHarness();

  expect(strandsAdapter({ version: '1.9.0', sdk: StrandsCurrent })
    .createSource(hooks).metadata.qualification).toEqual({ status: 'exact_qualified' });
  expect(strandsAdapter({ version: '2.0.0', sdk: StrandsCurrent })
    .createSource(hooks).metadata.qualification).toEqual({
      status: 'capability_checked_unqualified', profile: 'strands-hook-provider-v1',
    });
  expect(strandsAdapter({ sdk: StrandsCurrent })
    .createSource(hooks).metadata.qualification).toEqual({ status: 'unknown' });
});

describe.each(strandsFixtures)('Strands semantic projection %s', (version, Strands) => {
  it('keeps interleaved same-agent reasoning with its exact invocation state', async () => {
    const hooks = new HookHarness();
    const output = await mkdtemp(join(tmpdir(), 'semantic-strands-concurrent-reasoning-'));
    const capture = initialize({ output, serviceName: 'strands-concurrent-reasoning' });
    capture.instrument({ adapter: strandsAdapter({ version, sdk: Strands }), client: hooks });
    const agent = { messages: [] } as unknown as StrandsCurrent.LocalAgent;
    const model = {} as never;
    const states = [{ turnId: 'one' }, { turnId: 'two' }];
    for (const state of states) {
      await hooks.emit(new Strands.BeforeInvocationEvent({ agent, invocationState: state }));
      await hooks.emit(new Strands.BeforeModelCallEvent({ agent, model, invocationState: state }));
    }
    for (const [index, state] of states.entries()) {
      await hooks.emit(new Strands.ModelStreamUpdateEvent({
        agent,
        event: new Strands.ModelContentBlockDeltaEvent({
          type: 'modelContentBlockDeltaEvent',
          delta: { reasoningContent: { text: `reasoning-${index + 1}` } } as unknown as StrandsCurrent.ContentBlockDelta,
        }),
        invocationState: state,
      }));
    }
    for (const state of [...states].reverse()) {
      await hooks.emit(new Strands.AfterModelCallEvent({
        agent, model, invocationState: state, attemptCount: 1, error: new Error('failed'),
      }));
    }

    const closed = await capture.shutdown();
    const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
      .trim().split('\n').map((line) => JSON.parse(line) as SemanticTraceRecord);
    expect(records.filter((record) => record.kind === 'model.response')
      .map((record) => {
        const reasoning = record.data.reasoning;
        if (!Array.isArray(reasoning)) return undefined;
        const block = reasoning[0];
        return block && typeof block === 'object' && !Array.isArray(block)
          ? block.text
          : undefined;
      }).sort()).toEqual([
        'reasoning-1', 'reasoning-2',
      ]);
  });

  it('retains ordered repeated reasoning deltas when the model call fails', async () => {
    const hooks = new HookHarness();
    const output = await mkdtemp(join(tmpdir(), 'semantic-strands-failed-reasoning-'));
    const capture = initialize({ output, serviceName: 'strands-failed-reasoning' });
    capture.instrument({ adapter: strandsAdapter({ version, sdk: Strands }), client: hooks });
    const agent = { messages: [] } as unknown as StrandsCurrent.LocalAgent;
    const state = {};
    const model = {} as never;

    await hooks.emit(new Strands.BeforeInvocationEvent({ agent, invocationState: state }));
    await hooks.emit(new Strands.BeforeModelCallEvent({ agent, model, invocationState: state }));
    for (const text of ['first', 'first']) {
      await hooks.emit(new Strands.ModelStreamUpdateEvent({
        agent,
        event: new Strands.ModelContentBlockDeltaEvent({
          type: 'modelContentBlockDeltaEvent',
          delta: { reasoningContent: { text } } as unknown as StrandsCurrent.ContentBlockDelta,
        }),
        invocationState: state,
      }));
    }
    await hooks.emit(new Strands.AfterModelCallEvent({
      agent,
      model,
      invocationState: state,
      attemptCount: 1,
      error: new Error('stream failed'),
    }));

    const closed = await capture.shutdown();
    const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
      .trim().split('\n').map((line) => JSON.parse(line) as SemanticTraceRecord);
    expect(records.find((record) => record.kind === 'model.response')).toMatchObject({
      data: {
        status: 'failed',
        reasoning: [
          { type: 'text', text: 'first' },
          { type: 'text', text: 'first' },
        ],
      },
    });
  });

  it('retains reasoning deltas when the model call is cancelled', async () => {
    const hooks = new HookHarness();
    const output = await mkdtemp(join(tmpdir(), 'semantic-strands-cancelled-reasoning-'));
    const capture = initialize({ output, serviceName: 'strands-cancelled-reasoning' });
    capture.instrument({ adapter: strandsAdapter({ version, sdk: Strands }), client: hooks });
    const agent = { messages: [] } as unknown as StrandsCurrent.LocalAgent;
    const state = {};
    const model = {} as never;
    await hooks.emit(new Strands.BeforeInvocationEvent({ agent, invocationState: state }));
    await hooks.emit(new Strands.BeforeModelCallEvent({ agent, model, invocationState: state }));
    await hooks.emit(new Strands.ModelStreamUpdateEvent({
      agent,
      event: new Strands.ModelContentBlockDeltaEvent({
        type: 'modelContentBlockDeltaEvent',
        delta: { reasoningContent: { text: 'before cancel' } } as unknown as StrandsCurrent.ContentBlockDelta,
      }),
      invocationState: state,
    }));
    const cancellation = new Error('cancelled');
    cancellation.name = 'AbortError';
    await hooks.emit(new Strands.AfterModelCallEvent({
      agent, model, invocationState: state, attemptCount: 1, error: cancellation,
    }));

    const closed = await capture.shutdown();
    const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
      .trim().split('\n').map((line) => JSON.parse(line) as SemanticTraceRecord);
    expect(records.find((record) => record.kind === 'model.response')).toMatchObject({
      data: {
        status: 'cancelled',
        reasoning: [{ type: 'text', text: 'before cancel' }],
      },
    });
  });

  it('retains reasoning deltas when a completed response omits them', async () => {
    const hooks = new HookHarness();
    const output = await mkdtemp(join(tmpdir(), 'semantic-strands-completed-reasoning-'));
    const capture = initialize({ output, serviceName: 'strands-completed-reasoning' });
    capture.instrument({ adapter: strandsAdapter({ version, sdk: Strands }), client: hooks });
    const agent = { messages: [] } as unknown as StrandsCurrent.LocalAgent;
    const state = {};
    const model = {} as never;
    const message = new Strands.Message({
      role: 'assistant',
      content: [new Strands.TextBlock('done')],
    });

    await hooks.emit(new Strands.BeforeInvocationEvent({ agent, invocationState: state }));
    await hooks.emit(new Strands.BeforeModelCallEvent({ agent, model, invocationState: state }));
    for (const text of ['first', 'first']) {
      await hooks.emit(new Strands.ModelStreamUpdateEvent({
        agent,
        event: new Strands.ModelContentBlockDeltaEvent({
          type: 'modelContentBlockDeltaEvent',
          delta: { reasoningContent: { text } } as unknown as StrandsCurrent.ContentBlockDelta,
        }),
        invocationState: state,
      }));
    }
    await hooks.emit(new Strands.AfterModelCallEvent({
      agent,
      model,
      invocationState: state,
      attemptCount: 1,
      stopData: { message, stopReason: 'endTurn' },
    }));

    const closed = await capture.shutdown();
    const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
      .trim().split('\n').map((line) => JSON.parse(line) as SemanticTraceRecord);
    expect(records.find((record) => record.kind === 'model.response')).toMatchObject({
      data: {
        status: 'completed',
        content: [{ text: 'done' }],
        reasoning: [
          { type: 'text', text: 'first' },
          { type: 'text', text: 'first' },
        ],
      },
    });
  });

  it('reports the unobservable context-reduction call after overflow', async () => {
    const hooks = new HookHarness();
    const output = await mkdtemp(join(tmpdir(), 'semantic-strands-context-reduction-'));
    const capture = initialize({ output, serviceName: 'strands-context-reduction' });
    capture.instrument({ adapter: strandsAdapter({ version, sdk: Strands }), client: hooks });
    const agent = { messages: [] } as unknown as StrandsCurrent.LocalAgent;
    const state = {};
    const model = {} as never;
    await hooks.emit(new Strands.BeforeInvocationEvent({ agent, invocationState: state }));
    await hooks.emit(new Strands.BeforeModelCallEvent({ agent, model, invocationState: state }));
    const overflow = new Error('context window exceeded');
    overflow.name = 'ContextWindowOverflowError';
    await hooks.emit(new Strands.AfterModelCallEvent({
      agent, model, invocationState: state, attemptCount: 1, error: overflow,
    }));

    const closed = await capture.shutdown();
    const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
      .trim().split('\n').map((line) => JSON.parse(line) as SemanticTraceRecord);
    expect(records.find((record) => (
      record.kind === 'loss'
      && record.data.reason === 'strands_context_reduction_unobserved'
    ))).toMatchObject({
      data: {
        reason: 'strands_context_reduction_unobserved',
        count: 1,
        recoverable: false,
      },
    });
  });

  it('projects official hook evidence into one compact, exactly correlated trace', async () => {
    const hooks = new HookHarness();
    const output = await mkdtemp(join(tmpdir(), 'semantic-strands-projection-'));
    const capture = initialize({ output, serviceName: 'strands-semantic-fixture' });
    capture.instrument({ adapter: strandsAdapter({ version, sdk: Strands }), client: hooks });

    const model = { modelId: `fixture-model-${version}` } as never;
    const agent = {
      messages: [] as any[],
      systemPrompt: 'Answer from verified weather evidence.',
      toolRegistry: {
        list: () => [{
          toolSpec: {
            name: 'weather',
            description: 'Looks up current weather.',
            inputSchema: {
              type: 'object',
              properties: { city: { type: 'string' } },
              required: ['city'],
            },
          },
        }],
      },
    };
    const eventAgent = agent as unknown as StrandsCurrent.LocalAgent;
    const state = {
      conversationId: `strands-conversation-${version}`,
      turnId: `strands-turn-${version}`,
      turnIndex: 1,
    };
    const userMessage = new Strands.Message({
      role: 'user',
      content: [new Strands.TextBlock('Find the current weather.')],
    });
    const modelError = new Error('temporary model failure');
    const toolUse = {
      name: 'weather',
      toolUseId: `tool-weather-${version}`,
      input: { city: 'London' },
    };
    const toolUseBlock = new Strands.ToolUseBlock(toolUse);
    const assistantMessage = new Strands.Message({
      role: 'assistant',
      content: [toolUseBlock],
    });
    const finalAssistantMessage = new Strands.Message({
      role: 'assistant',
      content: [
        new Strands.ReasoningBlock({ text: 'Checked the tool evidence.' }),
        new Strands.ReasoningBlock({ text: 'Checked the tool evidence.' }),
        new Strands.ReasoningBlock({ redactedContent: new Uint8Array([1, 2, 3]) }),
        new Strands.TextBlock('It is sunny.'),
      ],
    });

    await hooks.emit(new Strands.BeforeInvocationEvent({
      agent: eventAgent, invocationState: state,
    }));
    agent.messages.push(userMessage);
    await hooks.emit(new Strands.MessageAddedEvent({
      agent: eventAgent, message: userMessage, invocationState: state,
    }));
    await hooks.emit(new Strands.BeforeModelCallEvent({
      agent: eventAgent, model, invocationState: state, projectedInputTokens: 5,
    }));
    const failedModel = new Strands.AfterModelCallEvent({
      agent: eventAgent,
      model,
      invocationState: state,
      attemptCount: 1,
      error: modelError,
    });
    failedModel.retry = true;
    await hooks.emit(failedModel);
    await hooks.emit(new Strands.BeforeModelCallEvent({
      agent: eventAgent, model, invocationState: state, projectedInputTokens: 5,
    }));
    await hooks.emit(new Strands.ModelStreamUpdateEvent({
      agent: eventAgent,
      event: new Strands.ModelContentBlockDeltaEvent({
        type: 'modelContentBlockDeltaEvent',
        delta: { text: 'transient' } as unknown as StrandsCurrent.ContentBlockDelta,
      }),
      invocationState: state,
    }));
    await hooks.emit(new Strands.ModelStreamUpdateEvent({
      agent: eventAgent,
      event: new Strands.ModelMetadataEvent({
        type: 'modelMetadataEvent',
        usage: { inputTokens: 5, outputTokens: 3, totalTokens: 8 },
      }),
      invocationState: state,
    }));
    await hooks.emit(new Strands.ContentBlockEvent({
      agent: eventAgent,
      contentBlock: toolUseBlock,
      invocationState: state,
    }));
    await hooks.emit(new Strands.ModelMessageEvent({
      agent: eventAgent,
      message: assistantMessage,
      stopReason: 'toolUse',
      invocationState: state,
    }));
    await hooks.emit(new Strands.AfterModelCallEvent({
      agent: eventAgent,
      model,
      invocationState: state,
      attemptCount: 2,
      stopData: { message: assistantMessage, stopReason: 'toolUse' },
    }));
    await hooks.emit(new Strands.BeforeToolCallEvent({
      agent: eventAgent, toolUse, tool: undefined, invocationState: state,
    }));
    await hooks.emit(new Strands.ToolStreamUpdateEvent({
      agent: eventAgent,
      event: new Strands.ToolStreamEvent({ data: { progress: 50 } }),
      invocationState: state,
    }));
    const result = new Strands.ToolResultBlock({
      toolUseId: toolUse.toolUseId,
      status: 'success',
      content: [{ text: 'sunny' }] as unknown as StrandsCurrent.ToolResultContent[],
    });
    await hooks.emit(new Strands.AfterToolCallEvent({
      agent: eventAgent, toolUse, tool: undefined, result, invocationState: state,
    }));
    await hooks.emit(new Strands.ToolResultEvent({
      agent: eventAgent, result, invocationState: state,
    }));
    const toolResultMessage = new Strands.Message({
      role: 'user',
      content: [result],
    });
    agent.messages.push(assistantMessage);
    await hooks.emit(new Strands.MessageAddedEvent({
      agent: eventAgent, message: assistantMessage, invocationState: state,
    }));
    agent.messages.push(toolResultMessage);
    await hooks.emit(new Strands.MessageAddedEvent({
      agent: eventAgent, message: toolResultMessage, invocationState: state,
    }));
    await hooks.emit(new Strands.BeforeModelCallEvent({
      agent: eventAgent, model, invocationState: state, projectedInputTokens: 8,
    }));
    await hooks.emit(new Strands.ModelMessageEvent({
      agent: eventAgent,
      message: finalAssistantMessage,
      stopReason: 'endTurn',
      invocationState: state,
    }));
    await hooks.emit(new Strands.AfterModelCallEvent({
      agent: eventAgent,
      model,
      invocationState: state,
      attemptCount: 1,
      stopData: { message: finalAssistantMessage, stopReason: 'endTurn' },
    }));
    agent.messages.push(finalAssistantMessage);
    await hooks.emit(new Strands.MessageAddedEvent({
      agent: eventAgent, message: finalAssistantMessage, invocationState: state,
    }));
    const agentResult = new Strands.AgentResult({
      stopReason: 'endTurn',
      lastMessage: finalAssistantMessage,
      invocationState: state,
      structuredOutput: { forecast: 'sunny' },
    });
    await hooks.emit(new Strands.AgentResultEvent({
      agent: eventAgent, result: agentResult, invocationState: state,
    }));
    await hooks.emit(new Strands.AfterInvocationEvent({
      agent: eventAgent, invocationState: state,
    }));

    const closed = await capture.shutdown();
    expect(await validateArtifact(closed.artifactPath)).toMatchObject({
      valid: true,
      issues: [],
    });
    const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as SemanticTraceRecord);

    expect(records.map((record) => record.kind)).toEqual([
      'run.start',
      'message',
      'message',
      'loss',
      'model.request',
      'model.response',
      'state',
      'model.request',
      'tool.proposal',
      'model.response',
      'tool.call',
      'tool.result',
      'model.request',
      'model.response',
      'loss',
      'run.outcome',
    ]);
    expect(records.filter((record) => record.kind === 'loss')).toMatchObject([
      {
        data: {
          reason: 'strands_post_middleware_context_unavailable',
          stage: 'source',
          count: 1,
          recoverable: false,
        },
      },
      {
        data: {
          reason: 'reasoning_unavailable',
          stage: 'source',
          count: 1,
          recoverable: false,
        },
      },
    ]);

    const root = records[0];
    const retry = records.find((record) => record.kind === 'state')!;
    const modelRequests = records.filter((record) => record.kind === 'model.request');
    const modelResponses = records.filter((record) => (
      record.kind === 'model.response' && record.data.status === 'completed'
    ));
    const messages = records.filter((record) => record.kind === 'message');
    const proposal = records.find((record) => record.kind === 'tool.proposal')!;
    const call = records.find((record) => record.kind === 'tool.call')!;
    const toolResult = records.find((record) => record.kind === 'tool.result')!;
    const outcome = records.find((record) => record.kind === 'run.outcome')!;

    expect(retry).toMatchObject({
      parent: records[5].id,
      data: { type: 'model.retry', value: { attempt: 2 } },
    });
    expect(modelRequests[1].parent).toBe(retry.id);
    expect(modelRequests.map((request) => request.data.model)).toEqual([
      `fixture-model-${version}`,
      `fixture-model-${version}`,
      `fixture-model-${version}`,
    ]);
    expect(modelRequests.map((request) => request.data.tools)).toEqual([
      ['weather'],
      ['weather'],
      ['weather'],
    ]);
    expect(messages.some((message) => (
      message.data.role === 'system'
      && message.data.content === 'Answer from verified weather evidence.'
    ))).toBe(true);
    expect(modelRequests[0].data.context_refs).toEqual([records[2].id, records[1].id]);
    expect(modelRequests[1].data.context_refs).toEqual([records[2].id, records[1].id]);
    expect(modelResponses[0]).toMatchObject({
      parent: retry.id,
      data: {
        status: 'completed',
        finish_reason: 'toolUse',
        usage: { input_tokens: 5, output_tokens: 3 },
      },
      links: [{ type: 'result_of', record: modelRequests[1].id }],
    });
    expect(proposal.parent).toBe(modelRequests[1].id);
    expect(proposal.data).toMatchObject({
      native_call_id: toolUse.toolUseId,
      name: 'weather',
      input: { city: 'London' },
    });
    expect(call).toMatchObject({
      parent: root.id,
      data: {
        call_id: proposal.data.call_id,
        native_call_id: toolUse.toolUseId,
        name: 'weather',
        input: { city: 'London' },
      },
      links: [{ type: 'derived_from', record: proposal.id }],
    });
    expect(toolResult).toMatchObject({
      parent: root.id,
      data: {
        call_id: call.data.call_id,
        native_call_id: toolUse.toolUseId,
        status: 'succeeded',
        output: [{ text: 'sunny' }],
      },
      links: [{ type: 'result_of', record: call.id }],
    });
    expect(modelRequests[2].data.context_refs).toEqual([
      records[2].id,
      records[1].id,
      modelResponses[0].id,
      toolResult.id,
    ]);
    expect(modelResponses[1]).toMatchObject({
      parent: root.id,
      data: {
        status: 'completed',
        finish_reason: 'endTurn',
        reasoning: [
          { type: 'text', text: 'Checked the tool evidence.' },
          { type: 'text', text: 'Checked the tool evidence.' },
        ],
      },
      links: [{ type: 'result_of', record: modelRequests[2].id }],
    });
    expect(records.filter((record) => record.kind === 'message')).toEqual([
      records[1],
      records[2],
    ]);
    expect(outcome).toMatchObject({
      parent: root.id,
      data: {
        status: 'completed',
        output: { forecast: 'sunny' },
      },
    });
    expect(JSON.stringify(outcome)).not.toContain('"traces"');
    expect(JSON.stringify(outcome)).not.toContain('"metrics"');
    expect(JSON.stringify(outcome)).not.toContain('"lastMessage"');
    expect(records.map((record) => record.seq)).toEqual(
      records.map((_, index) => index + 1),
    );
    await expectContractRecords(records);
    expect(agent.messages).toEqual([
      userMessage,
      assistantMessage,
      toolResultMessage,
      finalAssistantMessage,
    ]);
  });

  it('keeps changed and same-payload messages unless exact identity proves an alias', async () => {
    const hooks = new HookHarness();
    const output = await mkdtemp(join(tmpdir(), 'semantic-strands-changed-message-'));
    const capture = initialize({ output, serviceName: 'strands-changed-message-fixture' });
    capture.instrument({ adapter: strandsAdapter({ version, sdk: Strands }), client: hooks });

    const agent = { messages: [] as any[] };
    const eventAgent = agent as unknown as StrandsCurrent.LocalAgent;
    const model = {} as never;
    const state = {};
    const assistant = new Strands.Message({
      role: 'assistant',
      content: [new Strands.TextBlock('first')],
    });
    await hooks.emit(new Strands.BeforeInvocationEvent({
      agent: eventAgent, invocationState: state,
    }));
    await hooks.emit(new Strands.BeforeModelCallEvent({
      agent: eventAgent, model, invocationState: state,
    }));
    await hooks.emit(new Strands.AfterModelCallEvent({
      agent: eventAgent,
      model,
      invocationState: state,
      attemptCount: 1,
      stopData: { message: assistant, stopReason: 'endTurn' },
    }));
    assistant.content.splice(0, 1, new Strands.TextBlock('changed'));
    agent.messages.push(assistant);
    await hooks.emit(new Strands.MessageAddedEvent({
      agent: eventAgent, message: assistant, invocationState: state,
    }));
    const samePayload = new Strands.Message({
      role: 'assistant',
      content: [new Strands.TextBlock('changed')],
    });
    agent.messages.push(samePayload);
    await hooks.emit(new Strands.MessageAddedEvent({
      agent: eventAgent, message: samePayload, invocationState: state,
    }));
    await hooks.emit(new Strands.AgentResultEvent({
      agent: eventAgent,
      result: new Strands.AgentResult({
        stopReason: 'endTurn',
        lastMessage: assistant,
        invocationState: state,
      }),
      invocationState: state,
    }));

    const closed = await capture.shutdown();
    const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as SemanticTraceRecord);
    expect(records.map((record) => record.kind)).toEqual([
      'run.start',
      'loss',
      'model.request',
      'model.response',
      'message',
      'message',
      'run.outcome',
    ]);
    expect(records.filter((record) => record.kind === 'loss')).toMatchObject([{
      data: {
        reason: 'strands_post_middleware_context_unavailable',
        count: 1,
      },
    }]);
    await expectContractRecords(records);
  });
});

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
