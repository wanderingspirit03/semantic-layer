import { mkdtemp, readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import * as LangGraphCurrent from 'langgraph-current';
import * as LangGraphCurrentPrebuilt from 'langgraph-current/prebuilt';
import * as LangGraphPrevious from 'langgraph-previous';
import * as LangGraphPreviousPrebuilt from 'langgraph-previous/prebuilt';
import {
  AIMessage,
  AIMessageChunk,
  BaseMessage,
  HumanMessage,
  ToolMessage,
} from 'langchain-core/messages';
import { CallbackManagerForLLMRun } from 'langchain-core/callbacks/manager';
import { BaseChatModel } from 'langchain-core/language_models/chat_models';
import { ChatGenerationChunk } from 'langchain-core/outputs';
import { DynamicStructuredTool } from 'langchain-core/tools';
import { z } from 'zod';
import { afterEach, describe, expect, it } from 'vitest';

import {
  initialize,
  langGraphAdapter,
  resetCaptureForTests,
  validateArtifact,
} from '../src/index.js';
import type { SemanticTraceRecord } from '../src/trace/semantic-projector.js';

afterEach(async () => resetCaptureForTests());

const require = createRequire(import.meta.url);

function installedVersion(alias: 'langgraph-current' | 'langgraph-previous'): string {
  return (require(`${alias}/package.json`) as { version: string }).version;
}

async function traceRows(path: string): Promise<SemanticTraceRecord[]> {
  return (await readFile(join(path, 'trace.jsonl'), 'utf8'))
    .trim()
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line) as SemanticTraceRecord);
}

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

it('reports LangGraph qualification from the observed version only', () => {
  const graph = { invoke() {}, stream() {} };

  expect(langGraphAdapter({ version: '1.4.7' })
    .createSource(graph).metadata.qualification).toEqual({ status: 'exact_qualified' });
  expect(langGraphAdapter({ version: '2.0.0' })
    .createSource(graph).metadata.qualification).toEqual({
      status: 'capability_checked_unqualified', profile: 'langgraph-runnable-callbacks-v1',
    });
  expect(langGraphAdapter().createSource(graph).metadata.qualification).toEqual({
    status: 'unknown',
  });
});

type LangGraphFixture = readonly [
  version: '1.4.7' | '1.4.6',
  sdk: typeof LangGraphCurrent | typeof LangGraphPrevious,
  prebuilt: typeof LangGraphCurrentPrebuilt | typeof LangGraphPreviousPrebuilt,
  packageAlias: 'langgraph-current' | 'langgraph-previous',
];

const langGraphFixtures: readonly LangGraphFixture[] = [
  ['1.4.7', LangGraphCurrent, LangGraphCurrentPrebuilt, 'langgraph-current'],
  ['1.4.6', LangGraphPrevious, LangGraphPreviousPrebuilt, 'langgraph-previous'],
];

describe.each(langGraphFixtures)('LangGraph JS %s semantic projection', (
  version,
  LangGraph,
  LangGraphPrebuilt,
  packageAlias,
) => {
  it('references the exact human and tool context exposed to a streamed chat model', async () => {
    const State = LangGraph.Annotation.Root({
      messages: LangGraph.Annotation<BaseMessage[]>({
        reducer: (left, right) => [...left, ...right],
        default: () => [],
      }),
    });
    class ContextFixtureModel extends BaseChatModel {
      private calls = 0;
      _llmType(): string { return 'semantic-context-fixture'; }
      async _generate() {
        this.calls += 1;
        const content = this.calls === 1
          ? 'The archived result needs confirmation.'
          : 'The archived result is CANARY-17.';
        return {
          generations: [{
            text: content,
            message: new AIMessage(content),
          }],
        };
      }
    }
    const model = new ContextFixtureModel({});
    const graph = new LangGraph.StateGraph(State)
      .addNode('answer', async ({ messages }) => ({
        messages: [await model.invoke(messages)],
      }))
      .addNode('confirm', async ({ messages }) => ({
        messages: [await model.invoke(messages)],
      }))
      .addEdge(LangGraph.START, 'answer')
      .addEdge('answer', 'confirm')
      .addEdge('confirm', LangGraph.END)
      .compile();
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-context-projection-'));
    const capture = initialize({ output, serviceName: 'langgraph-context-projection' });
    const adapter = langGraphAdapter({ version });
    capture.instrument({ adapter, client: graph });
    const input = {
      messages: [
        new HumanMessage('Read the archived result.'),
        new ToolMessage({
          content: 'CANARY-17',
          name: 'archive_lookup',
          tool_call_id: 'call-archive',
        }),
      ],
    };

    for await (const _event of await adapter.streamEvents(
      input,
      { version: 'v2' },
    ) as AsyncIterable<unknown>) {
      // The application remains the only stream consumer.
    }

    const closed = await capture.shutdown();
    const records = await traceRows(closed.artifactPath);
    await expectContractRecords(records);

    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    const messages = records.filter((record) => record.kind === 'message');
    expect(messages).toHaveLength(3);
    expect(messages.map((record) => record.data)).toEqual([
      { role: 'user', content: 'Read the archived result.' },
      {
        role: 'tool',
        content: 'CANARY-17',
        name: 'archive_lookup',
        call_id: 'call-archive',
      },
      {
        role: 'assistant',
        content: 'The archived result needs confirmation.',
      },
    ]);
    expect(messages.every((record) => record.origin === 'context')).toBe(true);
    expect(records.filter((record) => record.kind === 'tool.call')).toEqual([]);
    const requests = records.filter((record) => record.kind === 'model.request');
    expect(requests).toHaveLength(2);
    expect(requests.map((record) => record.data.context_refs)).toEqual([
      messages.slice(0, 2).map((record) => record.id),
      messages.map((record) => record.id),
    ]);
    const responses = records.filter((record) => record.kind === 'model.response');
    const runOutcome = one(records, 'run.outcome');
    expect(responses).toHaveLength(2);
    expect(runOutcome.links).toContainEqual({
      type: 'derived_from',
      record: responses.at(-1)?.id,
    });
  });

  it('projects one real graph run into a clear model and tool timeline without avoidable losses', async () => {
    const State = LangGraph.Annotation.Root({
      value: LangGraph.Annotation<string>({
        reducer: (_left, right) => right,
        default: () => '',
      }),
    });
    class FixtureModel extends BaseChatModel {
      _llmType(): string { return 'semantic-fixture'; }
      async _generate() {
        return {
          generations: [{
            text: 'use lookup',
            message: new AIMessage({
              content: 'use lookup',
              tool_calls: [{
                id: 'call-semantic',
                name: 'lookup',
                args: { input: 'fixed' },
                type: 'tool_call',
              }],
              usage_metadata: { input_tokens: 4, output_tokens: 2, total_tokens: 6 },
            }),
          }],
        };
      }
      async *_streamResponseChunks(
        _messages: BaseMessage[],
        _options: this['ParsedCallOptions'],
        runManager?: CallbackManagerForLLMRun,
      ): AsyncGenerator<ChatGenerationChunk> {
        const chunk = new ChatGenerationChunk({
          text: 'use lookup',
          message: new AIMessageChunk({
            content: 'use lookup',
            additional_kwargs: {
              reasoning_content: 'fallback that must not duplicate structured evidence',
              reasoning_details: [
                { type: 'reasoning.summary', summary: 'Checked graph context.' },
                { type: 'reasoning.text', text: 'Checked the exposed graph context.' },
                { type: 'reasoning.text', text: 'Checked the exposed graph context.' },
                { type: 'reasoning.encrypted', data: 'opaque-provider-payload' },
              ],
            },
            tool_call_chunks: [{
              id: 'call-semantic',
              name: 'lookup',
              args: '{"input":"fixed"}',
              index: 0,
            }],
            usage_metadata: { input_tokens: 4, output_tokens: 2, total_tokens: 6 },
          }),
        });
        yield chunk;
        await runManager?.handleLLMNewToken(
          'use lookup',
          { prompt: 0, completion: 0 },
          undefined,
          undefined,
          undefined,
          { chunk },
        );
      }
    }
    const model = new FixtureModel({});
    const tool = new DynamicStructuredTool({
      name: 'lookup',
      description: 'deterministic lookup',
      schema: z.object({ input: z.string() }),
      func: async ({ input }) => `found:${input}`,
    });
    const graph = new LangGraph.StateGraph(State)
      .addNode('agent_step', async () => {
        let response = '';
        for await (const chunk of await model.stream([new HumanMessage('find fixed')])) {
          response += chunk.content;
        }
        const result = await tool.invoke({
          id: 'call-semantic',
          name: 'lookup',
          args: { input: 'fixed' },
          type: 'tool_call',
        });
        return { value: `${response}:${result}` };
      })
      .addEdge(LangGraph.START, 'agent_step')
      .addEdge('agent_step', LangGraph.END)
      .compile();
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-projection-'));
    const capture = initialize({ output, serviceName: 'langgraph-semantic-projection' });
    const adapter = langGraphAdapter({ version });
    capture.instrument({ adapter, client: graph });

    const result = await adapter.invoke(
      { value: '' },
      {
        metadata: {
          thread_id: `semantic-${version}`,
          turn_id: 'turn-semantic',
          turn_index: 0,
        },
      },
    ) as { value: string };
    expect(result.value).toContain('use lookup:');

    const closed = await capture.shutdown();
    const records = await traceRows(closed.artifactPath);
    await expectContractRecords(records);

    expect(records.filter((record) => record.kind === 'loss')).toMatchObject([{
      data: { reason: 'reasoning_unavailable', stage: 'source', count: 1, recoverable: false },
    }]);
    const runStart = one(records, 'run.start');
    const runOutcome = one(records, 'run.outcome');
    expect(runOutcome.parent).toBe(runStart.id);
    expect(runOutcome.data).toMatchObject({ status: 'completed' });
    const deliveryLink = runOutcome.links?.find((link) => link.type === 'derived_from');
    const deliveredState = records.find((record) => record.id === deliveryLink?.record);
    expect(deliveredState?.kind).toBe('state');

    const modelRequest = one(records, 'model.request');
    const modelResponse = one(records, 'model.response');
    expect(modelRequest.data.model).toBe('FixtureModel');
    expect(modelResponse.data).toMatchObject({
      status: 'completed',
      content: 'use lookup',
      reasoning: [
        { type: 'summary', text: 'Checked graph context.' },
        { type: 'text', text: 'Checked the exposed graph context.' },
        { type: 'text', text: 'Checked the exposed graph context.' },
      ],
      usage: { input_tokens: 4, output_tokens: 2 },
    });
    expect(modelResponse.links).toContainEqual({
      type: 'result_of',
      record: modelRequest.id,
    });

    const proposal = one(records, 'tool.proposal');
    const call = one(records, 'tool.call');
    const toolResult = one(records, 'tool.result');
    expect(proposal.data).toMatchObject({
      native_call_id: 'call-semantic',
      name: 'lookup',
      input: { input: 'fixed' },
    });
    expect(call.data).toMatchObject({
      native_call_id: 'call-semantic',
      name: 'lookup',
      input: { input: 'fixed' },
    });
    expect(call.links).toContainEqual({ type: 'derived_from', record: proposal.id });
    expect(toolResult.links).toContainEqual({ type: 'result_of', record: call.id });
    expect(toolResult.data).toMatchObject({ status: 'succeeded' });
    await expect(validateArtifact(closed.artifactPath, {
      profile: 'rich-agent',
      requiredEvidence: ['root', 'model', 'tool', 'delivery'],
    })).resolves.toMatchObject({ valid: true, issues: [] });
  });

  it('keeps a ToolNode interrupt and resume as one exact tool call and result', async () => {
    expect(installedVersion(packageAlias)).toBe(version);
    let executions = 0;
    const tool = new DynamicStructuredTool({
      name: 'approval_lookup',
      description: 'lookup after explicit approval',
      schema: z.object({ input: z.string() }),
      func: async ({ input }) => {
        executions += 1;
        const approval = LangGraph.interrupt({ question: 'approve?', input });
        return `approved:${approval}:${input}`;
      },
    });
    const graph = new LangGraph.StateGraph(LangGraph.MessagesAnnotation)
      .addNode('tools', new LangGraphPrebuilt.ToolNode([tool]))
      .addEdge(LangGraph.START, 'tools')
      .addEdge('tools', LangGraph.END)
      .compile({ checkpointer: new LangGraph.MemorySaver() });
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-resume-projection-'));
    const capture = initialize({ output, serviceName: 'langgraph-resume-projection' });
    const adapter = langGraphAdapter({ version });
    capture.instrument({ adapter, client: graph });
    const config = {
      configurable: { thread_id: `resume-semantic-${version}` },
      metadata: {
        thread_id: `resume-semantic-${version}`,
        turn_id: 'turn-resume',
        turn_index: 0,
      },
    };

    const inputMessage = new AIMessage({
      content: '',
      tool_calls: [{
        id: 'call-approval',
        name: 'approval_lookup',
        args: { input: 'fixed' },
        type: 'tool_call',
      }],
    });
    const paused = await adapter.invoke(
      { messages: [inputMessage] },
      config,
    ) as { __interrupt__?: unknown[]; messages: BaseMessage[] };
    expect(paused.__interrupt__).toHaveLength(1);
    expect(paused.messages).toEqual([inputMessage]);
    const resumed = await adapter.invoke(
      new LangGraph.Command({ resume: 'approved' }),
      config,
    ) as { messages: BaseMessage[] };
    expect(resumed.messages.at(-1)).toBeInstanceOf(ToolMessage);
    expect(resumed.messages.at(-1)?.content).toBe('approved:approved:fixed');
    expect(executions).toBe(2);

    const closed = await capture.shutdown();
    const records = await traceRows(closed.artifactPath);
    const manifest = JSON.parse(await readFile(
      join(closed.artifactPath, 'manifest.json'),
      'utf8',
    )) as { sources: Array<{ name: string; version?: string }> };
    await expectContractRecords(records);

    expect(manifest.sources).toContainEqual(expect.objectContaining({
      name: 'official:langgraph-js',
      version: installedVersion(packageAlias),
    }));
    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    expect(records.filter((record) => record.kind === 'run.start')).toHaveLength(1);
    expect(records.filter((record) => record.kind === 'run.outcome')).toHaveLength(1);
    const calls = records.filter((record) => record.kind === 'tool.call');
    const results = records.filter((record) => record.kind === 'tool.result');
    expect(calls).toHaveLength(1);
    expect(calls[0]?.data).toMatchObject({
      native_call_id: 'call-approval',
      name: 'approval_lookup',
      input: { input: 'fixed' },
    });
    expect(results).toHaveLength(1);
    expect(results[0]?.data).toMatchObject({
      native_call_id: 'call-approval',
      status: 'succeeded',
      output: 'approved:approved:fixed',
    });
    expect(results[0]?.links).toContainEqual({
      type: 'result_of',
      record: calls[0]?.id,
    });
    const interrupts = records.filter((record) => (
      record.kind === 'state' && record.data.type === 'state.interrupt'
    ));
    const resumes = records.filter((record) => (
      record.kind === 'state' && record.data.type === 'state.resume'
    ));
    expect(interrupts.length).toBeGreaterThan(0);
    expect(resumes).toHaveLength(1);
    expect(resumes[0]?.parent).toBe(interrupts.at(-1)?.id);
  });

  it('retains divergent ToolNode callbacks with the same call ID as explicit ambiguity', async () => {
    const observedInputs: string[] = [];
    const tool = new DynamicStructuredTool({
      name: 'same_id_lookup',
      description: 'records each distinct input',
      schema: z.object({ input: z.string() }),
      func: async ({ input }) => {
        observedInputs.push(input);
        return `found:${input}`;
      },
    });
    const graph = new LangGraph.StateGraph(LangGraph.MessagesAnnotation)
      .addNode('tools', new LangGraphPrebuilt.ToolNode([tool]))
      .addEdge(LangGraph.START, 'tools')
      .addEdge('tools', LangGraph.END)
      .compile();
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-divergent-tool-id-'));
    const capture = initialize({ output, serviceName: 'langgraph-divergent-tool-id' });
    const adapter = langGraphAdapter({ version });
    capture.instrument({ adapter, client: graph });
    const result = await adapter.invoke({
      messages: [new AIMessage({
        content: '',
        tool_calls: [
          {
            id: 'call-reused',
            name: 'same_id_lookup',
            args: { input: 'left' },
            type: 'tool_call',
          },
          {
            id: 'call-reused',
            name: 'same_id_lookup',
            args: { input: 'right' },
            type: 'tool_call',
          },
        ],
      })],
    }) as { messages: BaseMessage[] };

    expect(observedInputs).toEqual(['left', 'right']);
    expect(result.messages.slice(-2).map((message) => message.content)).toEqual([
      'found:left',
      'found:right',
    ]);

    const closed = await capture.shutdown();
    const records = await traceRows(closed.artifactPath);
    await expectContractRecords(records);

    expect(records.filter((record) => record.kind === 'tool.call')).toHaveLength(2);
    expect(records.filter((record) => (
      record.kind === 'loss'
      && record.data.reason === 'ambiguous_tool_correlation'
    ))).toHaveLength(1);
  });

  it('retains identical parallel sibling ToolNode calls without assuming duplication', async () => {
    const observedInputs: string[] = [];
    const tool = new DynamicStructuredTool({
      name: 'parallel_lookup',
      description: 'records each sibling execution',
      schema: z.object({ input: z.string() }),
      func: async ({ input }) => {
        observedInputs.push(input);
        return `found:${input}`;
      },
    });
    const graph = new LangGraph.StateGraph(LangGraph.MessagesAnnotation)
      .addNode('tools', new LangGraphPrebuilt.ToolNode([tool]))
      .addEdge(LangGraph.START, 'tools')
      .addEdge('tools', LangGraph.END)
      .compile();
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-parallel-tool-id-'));
    const capture = initialize({ output, serviceName: 'langgraph-parallel-tool-id' });
    const adapter = langGraphAdapter({ version });
    capture.instrument({ adapter, client: graph });

    const result = await adapter.invoke({
      messages: [new AIMessage({
        content: '',
        tool_calls: [
          {
            id: 'call-parallel',
            name: 'parallel_lookup',
            args: { input: 'fixed' },
            type: 'tool_call',
          },
          {
            id: 'call-parallel',
            name: 'parallel_lookup',
            args: { input: 'fixed' },
            type: 'tool_call',
          },
        ],
      })],
    }) as { messages: BaseMessage[] };

    expect(observedInputs).toEqual(['fixed', 'fixed']);
    expect(result.messages.slice(-2).map((message) => message.content)).toEqual([
      'found:fixed',
      'found:fixed',
    ]);

    const closed = await capture.shutdown();
    const records = await traceRows(closed.artifactPath);
    await expectContractRecords(records);

    expect(records.filter((record) => record.kind === 'tool.call')).toHaveLength(2);
    expect(records.filter((record) => (
      record.kind === 'loss'
      && record.data.reason === 'ambiguous_tool_correlation'
    ))).toHaveLength(1);
  });

  it('collapses proven exact nested ToolNode callbacks without changing the result', async () => {
    const inner = new DynamicStructuredTool({
      name: 'nested_lookup',
      description: 'inner deterministic lookup',
      schema: z.object({ input: z.string() }),
      func: async ({ input }) => `found:${input}`,
    });
    const outer = new DynamicStructuredTool({
      name: 'nested_lookup',
      description: 'outer deterministic lookup',
      schema: z.object({ input: z.string() }),
      func: async ({ input }, runManager, parentConfig) => inner.invoke(
        {
          id: 'call-nested',
          name: 'nested_lookup',
          args: { input },
          type: 'tool_call',
        },
        {
          ...parentConfig,
          callbacks: runManager?.getChild(),
        },
      ),
    });
    const graph = new LangGraph.StateGraph(LangGraph.MessagesAnnotation)
      .addNode('tools', new LangGraphPrebuilt.ToolNode([outer]))
      .addEdge(LangGraph.START, 'tools')
      .addEdge('tools', LangGraph.END)
      .compile();
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-nested-tool-id-'));
    const capture = initialize({ output, serviceName: 'langgraph-nested-tool-id' });
    const adapter = langGraphAdapter({ version });
    capture.instrument({ adapter, client: graph });

    const result = await adapter.invoke({
      messages: [new AIMessage({
        content: '',
        tool_calls: [{
          id: 'call-nested',
          name: 'nested_lookup',
          args: { input: 'fixed' },
          type: 'tool_call',
        }],
      })],
    }) as { messages: BaseMessage[] };
    expect(result.messages.at(-1)?.content).toBe('found:fixed');

    const closed = await capture.shutdown();
    const records = await traceRows(closed.artifactPath);
    await expectContractRecords(records);

    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    const call = one(records, 'tool.call');
    const toolResult = one(records, 'tool.result');
    expect(call.data).toMatchObject({
      native_call_id: 'call-nested',
      name: 'nested_lookup',
      input: { input: 'fixed' },
    });
    expect(toolResult.data).toMatchObject({
      native_call_id: 'call-nested',
      status: 'succeeded',
      output: 'found:fixed',
    });
    expect(toolResult.links).toContainEqual({
      type: 'result_of',
      record: call.id,
    });
  });

  it('projects an observed graph failure without guessing a successful outcome', async () => {
    const State = LangGraph.Annotation.Root({
      value: LangGraph.Annotation<string>({
        reducer: (_left, right) => right,
        default: () => '',
      }),
    });
    const graph = new LangGraph.StateGraph(State)
      .addNode('failing_step', () => {
        throw Object.assign(new Error('fixture graph failed'), {
          code: 'FIXTURE_GRAPH_FAILURE',
          detail: { retryable: false },
        });
      })
      .addEdge(LangGraph.START, 'failing_step')
      .addEdge('failing_step', LangGraph.END)
      .compile();
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-failure-projection-'));
    const capture = initialize({ output, serviceName: 'langgraph-failure-projection' });
    const adapter = langGraphAdapter({ version });
    capture.instrument({ adapter, client: graph });

    await expect(adapter.invoke({ value: '' }) as Promise<unknown>)
      .rejects.toThrow('fixture graph failed');
    const closed = await capture.shutdown();
    const records = await traceRows(closed.artifactPath);
    await expectContractRecords(records);

    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    const outcome = one(records, 'run.outcome');
    expect(outcome.data).toMatchObject({
      status: 'failed',
      error: {
        type: 'langgraph_error',
        message: 'fixture graph failed',
        recoverable: false,
        code: 'FIXTURE_GRAPH_FAILURE',
        details: { retryable: false },
      },
    });
    expect(records.some((record) => (
      record.kind === 'error'
      && record.data.message === 'fixture graph failed'
    ))).toBe(true);
  });
});

function one(records: SemanticTraceRecord[], kind: SemanticTraceRecord['kind']): SemanticTraceRecord {
  const matches = records.filter((record) => record.kind === kind);
  expect(matches, `expected one ${kind}`).toHaveLength(1);
  return matches[0]!;
}
