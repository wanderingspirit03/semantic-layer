import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import AnthropicCurrent from 'anthropic-current';
import AnthropicPrevious from 'anthropic-previous';
import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import { GoogleGenAI as GeminiCurrent } from 'gemini-current';
import { GoogleGenAI as GeminiPrevious } from 'gemini-previous';
import OpenAICurrent from 'openai-current';
import OpenAIPrevious from 'openai-previous';
import { afterEach, describe, expect, it } from 'vitest';

import {
  anthropicProviderAdapter,
  geminiProviderAdapter,
  initialize,
  openAIProviderAdapter,
  resetCaptureForTests,
  withProviderCaptureContext,
} from '../src/index.js';
import type { SemanticTraceRecord } from '../src/trace/semantic-projector.js';
import type { SourceRecord, SourceSink } from '../src/v1/types.js';

type OpenAIClient =
  | InstanceType<typeof OpenAICurrent>
  | InstanceType<typeof OpenAIPrevious>;
type OpenAICreate =
  & InstanceType<typeof OpenAICurrent>['chat']['completions']['create']
  & InstanceType<typeof OpenAIPrevious>['chat']['completions']['create'];
type OpenAIResponseCreate =
  & InstanceType<typeof OpenAICurrent>['responses']['create']
  & InstanceType<typeof OpenAIPrevious>['responses']['create'];
type AnthropicClient =
  | InstanceType<typeof AnthropicCurrent>
  | InstanceType<typeof AnthropicPrevious>;
type AnthropicCreate =
  & InstanceType<typeof AnthropicCurrent>['messages']['create']
  & InstanceType<typeof AnthropicPrevious>['messages']['create'];
type AnthropicBetaCreate =
  & InstanceType<typeof AnthropicCurrent>['beta']['messages']['create']
  & InstanceType<typeof AnthropicPrevious>['beta']['messages']['create'];

afterEach(async () => resetCaptureForTests());

it('qualifies only the exact exercised direct-provider SDK versions', () => {
  const openAIClient = {
    responses: { create() {} }, chat: { completions: { create() {} } },
  };
  const anthropicClient = {
    messages: { create() {} }, beta: { messages: { create() {} } },
  };
  const geminiClient = {
    models: { generateContentInternal() {}, generateContentStreamInternal() {} },
  };

  expect(openAIProviderAdapter({ version: '6.46.0' })
    .createSource(openAIClient).metadata.qualification).toEqual({ status: 'exact_qualified' });
  expect(openAIProviderAdapter({ provider: 'openrouter', version: '6.45.0' })
    .createSource(openAIClient).metadata.qualification).toEqual({ status: 'exact_qualified' });
  expect(anthropicProviderAdapter({ version: '0.111.0' })
    .createSource(anthropicClient).metadata.qualification).toEqual({ status: 'exact_qualified' });
  expect(geminiProviderAdapter({ version: '2.10.0' })
    .createSource(geminiClient).metadata.qualification).toEqual({ status: 'exact_qualified' });

  expect(openAIProviderAdapter({ version: '6.47.0' })
    .createSource(openAIClient).metadata.qualification).toEqual({
      status: 'capability_checked_unqualified',
      profile: 'openai-compatible-responses-chat-v1',
    });
  expect(anthropicProviderAdapter().createSource(anthropicClient).metadata.qualification).toEqual({
    status: 'unknown',
  });
  expect(anthropicProviderAdapter({ version: '0.112.0' })
    .createSource(anthropicClient).metadata.qualification).toEqual({
      status: 'capability_checked_unqualified', profile: 'anthropic-messages-v1',
    });
  expect(geminiProviderAdapter({ version: '2.12.0' })
    .createSource(geminiClient).metadata.qualification).toEqual({
      status: 'capability_checked_unqualified', profile: 'gemini-generate-content-v1',
    });
});

describe.each([
  ['6.46.0', OpenAICurrent],
  ['6.45.0', OpenAIPrevious],
] as const)('OpenAI provider semantic trace %s', (version, OpenAI) => {
  it('observes a bundled-name APIPromise stream without replacing or consuming it', async () => {
    const client = new OpenAI({
      apiKey: 'openai-renamed-promise-fixture',
      baseURL: 'https://example.invalid/v1',
      fetch: async () => new Response([
        'data: {"id":"chatcmpl-renamed","object":"chat.completion.chunk","created":1,"model":"gpt-fixture","choices":[{"index":0,"delta":{"content":"bundled "},"finish_reason":null}]}',
        '',
        'data: {"id":"chatcmpl-renamed","object":"chat.completion.chunk","created":1,"model":"gpt-fixture","choices":[{"index":0,"delta":{"content":"stream"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}',
        '',
        'data: [DONE]',
        '',
      ].join('\n'), { headers: { 'content-type': 'text/event-stream' } }),
    });
    const completions = client.chat.completions as unknown as Record<PropertyKey, unknown>;
    const originalCreate = completions.create as (...args: unknown[]) => unknown;
    let nativePromise: unknown;
    let promiseConstructor: object | undefined;
    let originalConstructorName: PropertyDescriptor | undefined;
    const trackedCreate = function trackedCreate(this: unknown, ...args: unknown[]): unknown {
      nativePromise = Reflect.apply(originalCreate, this, args);
      promiseConstructor = (nativePromise as { constructor: object }).constructor;
      originalConstructorName = Object.getOwnPropertyDescriptor(promiseConstructor, 'name');
      Object.defineProperty(promiseConstructor, 'name', {
        configurable: true,
        value: '_APIPromise',
      });
      return nativePromise;
    };
    Object.defineProperty(completions, 'create', {
      configurable: true,
      writable: true,
      value: trackedCreate,
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-renamed-promise-'));
    const capture = initialize({ output, serviceName: 'openai-renamed-promise' });
    capture.instrument({
      adapter: openAIProviderAdapter({ provider: 'openrouter', version }),
      client,
    });

    try {
      const returned = openAICreate(client)({
        model: 'gpt-fixture',
        messages: [{ role: 'user', content: 'Stream.' }],
        stream: true,
        stream_options: { include_usage: true },
      });
      expect(returned).toBe(nativePromise);
      expect(returned.constructor.name).toBe('_APIPromise');

      const stream = await returned;
      const events = [];
      for await (const event of stream) events.push(event);
      expect(events).toHaveLength(2);

      const records = await traceRecords((await capture.shutdown()).artifactPath);
      const request = records.find((record) => record.kind === 'model.request')!;
      const responses = records.filter((record) => record.kind === 'model.response');
      expect(responses).toHaveLength(1);
      expect(responses[0]).toMatchObject({
        data: {
          model: 'gpt-fixture',
          content: 'bundled stream',
          finish_reason: 'stop',
          usage: { input_tokens: 2, output_tokens: 2 },
        },
        links: [{ type: 'result_of', record: request.id }],
      });
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
      expect(records.filter((record) => record.kind === 'run.outcome').map(
        (record) => record.data.status,
      )).toEqual(['completed']);
    } finally {
      if (promiseConstructor && originalConstructorName) {
        Object.defineProperty(promiseConstructor, 'name', originalConstructorName);
      }
    }
  });

  it('closes an unobservable APIPromise as unknown without changing its result', async () => {
    const client = new OpenAI({
      apiKey: 'openai-unobservable-promise-fixture',
      baseURL: 'https://example.invalid/v1',
      fetch: async () => new Response(JSON.stringify({
        id: 'chatcmpl-unobservable',
        object: 'chat.completion',
        created: 1,
        model: 'gpt-fixture',
        choices: [{
          index: 0,
          message: { role: 'assistant', content: 'native result' },
          finish_reason: 'stop',
        }],
      }), { headers: { 'content-type': 'application/json' } }),
    });
    const completions = client.chat.completions as unknown as Record<PropertyKey, unknown>;
    const originalCreate = completions.create as (...args: unknown[]) => unknown;
    let nativePromise: unknown;
    const trackedCreate = function trackedCreate(this: unknown, ...args: unknown[]): unknown {
      nativePromise = Reflect.apply(originalCreate, this, args);
      Object.defineProperty(nativePromise, 'then', {
        configurable: false,
        writable: false,
        value: (nativePromise as { then: unknown }).then,
      });
      return nativePromise;
    };
    Object.defineProperty(completions, 'create', {
      configurable: true,
      writable: true,
      value: trackedCreate,
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-unobservable-promise-'));
    const capture = initialize({ output, serviceName: 'openai-unobservable-promise' });
    capture.instrument({ adapter: openAIProviderAdapter({ version }), client });

    const returned = openAICreate(client)({
      model: 'gpt-fixture',
      messages: [{ role: 'user', content: 'Return normally.' }],
    });
    expect(returned).toBe(nativePromise);
    await expect(returned).resolves.toMatchObject({
      id: 'chatcmpl-unobservable',
      choices: [{ message: { content: 'native result' } }],
    });

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    expect(records.filter((record) => record.kind === 'model.response')).toEqual([]);
    expect(records.filter((record) => record.kind === 'loss')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          reason: 'unsupported_native_value',
          detail: 'provider_promise_unobservable',
        }),
      }),
    ]);
    expect(records.filter((record) => record.kind === 'run.outcome').map(
      (record) => record.data.status,
    )).toEqual(['unknown']);
  });

  it('observes APIPromise success through catch without changing the returned Promise', async () => {
    const client = new OpenAI({
      apiKey: 'openai-catch-fixture',
      baseURL: 'https://example.invalid/v1',
      fetch: async () => new Response(JSON.stringify({
        id: 'chatcmpl-catch',
        object: 'chat.completion',
        created: 1,
        model: 'gpt-fixture',
        choices: [{
          index: 0,
          message: { role: 'assistant', content: 'caught success' },
          finish_reason: 'stop',
        }],
      }), { headers: { 'content-type': 'application/json' } }),
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-catch-'));
    const capture = initialize({ output, serviceName: 'openai-catch' });
    capture.instrument({ adapter: openAIProviderAdapter({ version }), client });

    const native = openAICreate(client)({
      model: 'gpt-fixture',
      messages: [{ role: 'user', content: 'Catch success.' }],
    });
    const observed = native.catch(() => ({ impossible: true }));
    expect(observed).not.toBe(native);
    expect(observed.constructor).toBe(Promise);
    await expect(observed).resolves.toMatchObject({
      id: 'chatcmpl-catch',
      choices: [{ message: { content: 'caught success' } }],
    });

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    expect(records.filter((record) => record.kind === 'model.response')).toHaveLength(1);
    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    expect(records.filter((record) => record.kind === 'run.outcome').map(
      (record) => record.data.status,
    )).toEqual(['completed']);
  });

  it('observes APIPromise finally through an ordinary Promise without changing its result', async () => {
    const client = new OpenAI({
      apiKey: 'openai-finally-fixture',
      baseURL: 'https://example.invalid/v1',
      fetch: async () => new Response(JSON.stringify({
        id: 'chatcmpl-finally',
        object: 'chat.completion',
        created: 1,
        model: 'gpt-fixture',
        choices: [{
          index: 0,
          message: { role: 'assistant', content: 'final success' },
          finish_reason: 'stop',
        }],
      }), { headers: { 'content-type': 'application/json' } }),
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-finally-'));
    const capture = initialize({ output, serviceName: 'openai-finally' });
    capture.instrument({ adapter: openAIProviderAdapter({ version }), client });
    let finalizers = 0;

    const native = openAICreate(client)({
      model: 'gpt-fixture',
      messages: [{ role: 'user', content: 'Finally succeed.' }],
    });
    const observed = native.finally(() => {
      finalizers += 1;
      return 'ignored';
    });
    expect(observed).not.toBe(native);
    expect(observed.constructor).toBe(Promise);
    await expect(observed).resolves.toMatchObject({
      id: 'chatcmpl-finally',
      choices: [{ message: { content: 'final success' } }],
    });
    expect(finalizers).toBe(1);

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    expect(records.filter((record) => record.kind === 'model.response')).toHaveLength(1);
    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    expect(records.filter((record) => record.kind === 'run.outcome').map(
      (record) => record.data.status,
    )).toEqual(['completed']);
  });

  it('keeps concurrent same-name proposals exact and historical tool messages as context', async () => {
    let responseIndex = 0;
    const fetch = async (): Promise<Response> => {
      responseIndex += 1;
      const index = responseIndex;
      await Promise.resolve();
      return new Response(JSON.stringify({
        id: `chatcmpl-${index}`,
        object: 'chat.completion',
        created: 1,
        model: 'gpt-fixture',
        choices: [{
          index: 0,
          message: {
            role: 'assistant',
            content: null,
            reasoning_content: `considered ${index}`,
            tool_calls: [{
              id: `call-${index}`,
              type: 'function',
              function: { name: 'lookup', arguments: `{"slot":${index}}` },
            }],
          },
          finish_reason: 'tool_calls',
        }],
        usage: { prompt_tokens: 2, completion_tokens: 1, total_tokens: 3 },
      }), { headers: { 'content-type': 'application/json' } });
    };
    const client = new OpenAI({
      apiKey: 'openai-provider-fixture',
      baseURL: 'https://example.invalid/v1',
      fetch,
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-projection-'));
    const capture = initialize({
      output,
      serviceName: 'openai-provider-projection',
      secretValues: ['openai-provider-fixture'],
    });
    capture.instrument({ adapter: openAIProviderAdapter({ version }), client });

    await Promise.all([1, 2].map((slot) => openAICreate(client)({
      model: 'gpt-fixture',
      messages: [
        { role: 'tool', tool_call_id: `prior-${slot}`, content: `prior result ${slot}` },
        { role: 'user', content: `request ${slot}` },
      ],
      tools: [{
        type: 'function',
        function: {
          name: 'lookup',
          description: 'fixture',
          parameters: { type: 'object', properties: { slot: { type: 'number' } } },
        },
      }],
    })));

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    const requests = records.filter((record) => record.kind === 'model.request');
    const responses = records.filter((record) => record.kind === 'model.response');
    const proposals = records.filter((record) => record.kind === 'tool.proposal');

    expect(records.filter((record) => record.kind === 'run.start')).toHaveLength(2);
    expect(records.filter((record) => record.kind === 'run.outcome')).toHaveLength(2);
    expect(records.filter((record) => record.kind === 'message')).toHaveLength(4);
    expect(requests).toHaveLength(2);
    expect(responses).toHaveLength(2);
    expect(proposals).toHaveLength(2);
    expect(proposals.map((record) => record.data.native_call_id).sort()).toEqual([
      'call-1',
      'call-2',
    ]);
    expect(proposals.every((record) => record.data.name === 'lookup')).toBe(true);
    expect(new Set(proposals.map((record) => record.data.call_id)).size).toBe(2);
    expect(responses.every((record) => requests.some((request) => (
      record.links?.some((link) => link.type === 'result_of' && link.record === request.id)
    )))).toBe(true);
    expect(responses.map((record) => record.data.reasoning).sort()).toEqual([
      [{ type: 'text', text: 'considered 1' }],
      [{ type: 'text', text: 'considered 2' }],
    ]);
    expect(records.some((record) => record.kind === 'tool.call'
      || record.kind === 'tool.result')).toBe(false);
    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
  });

  it('preserves ordered OpenRouter reasoning details without changing the native response', async () => {
    const nativeDetails = [{
      type: 'reasoning.summary', summary: 'Checked the constraints.', id: 'summary-1', index: 0,
    }, {
      type: 'reasoning.text', text: 'Calculated the result.',
      signature: 'opaque-reasoning-signature', id: 'text-1', index: 1,
    }, {
      type: 'reasoning.encrypted', data: 'opaque-encrypted-reasoning',
      id: 'encrypted-1', index: 2,
    }, {
      type: 'reasoning.summary', summary: 'Checked the constraints.', id: 'summary-2', index: 3,
    }];
    const client = new OpenAI({
      apiKey: 'openrouter-reasoning-details-fixture',
      baseURL: 'https://example.invalid/v1',
      fetch: async () => new Response(JSON.stringify({
        id: 'chatcmpl-openrouter-reasoning', object: 'chat.completion', created: 1,
        model: 'deepseek/deepseek-r1',
        choices: [{
          index: 0,
          message: {
            role: 'assistant', content: 'The answer is 42.',
            reasoning: 'compatibility duplicate', reasoning_details: nativeDetails,
          },
          finish_reason: 'stop',
        }],
      }), { headers: { 'content-type': 'application/json' } }),
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openrouter-reasoning-details-'));
    const capture = initialize({ output, serviceName: 'openrouter-reasoning-details' });
    capture.instrument({
      adapter: openAIProviderAdapter({ provider: 'openrouter', version }), client,
    });

    const returned = await openAICreate(client)({
      model: 'deepseek/deepseek-r1', messages: [{ role: 'user', content: 'Calculate.' }],
    });
    const returnedMessage = returned.choices[0]?.message as unknown as Record<string, unknown>;
    expect(returnedMessage.reasoning_details).toEqual(nativeDetails);

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    expect(records.find((record) => record.kind === 'model.response')?.data.reasoning).toEqual([
      { type: 'summary', text: 'Checked the constraints.' },
      { type: 'text', text: 'Calculated the result.' },
      { type: 'summary', text: 'Checked the constraints.' },
    ]);
    expect(JSON.stringify(records)).not.toContain('compatibility duplicate');
    expect(JSON.stringify(records)).not.toContain('opaque-reasoning-signature');
    expect(JSON.stringify(records)).not.toContain('opaque-encrypted-reasoning');
    expect(records.filter((record) => record.kind === 'loss')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          reason: 'unsupported_native_value', detail: 'openrouter.reasoning.opaque_unavailable',
        }),
      }),
    ]);
  });

  it('preserves streamed reasoning detail order and exact contiguous fragments', async () => {
    const chunk = (detail: Record<string, unknown>): string => JSON.stringify({
      id: 'chatcmpl-openrouter-stream', object: 'chat.completion.chunk', created: 1,
      model: 'deepseek/deepseek-r1',
      choices: [{ index: 0, delta: { reasoning_details: [detail] }, finish_reason: null }],
    });
    const client = new OpenAI({
      apiKey: 'openrouter-stream-reasoning-fixture', baseURL: 'https://example.invalid/v1',
      fetch: async () => new Response([
        `data: ${chunk({ type: 'reasoning.summary', summary: 'Checked ', id: 'summary-1' })}`,
        '',
        `data: ${chunk({
          type: 'reasoning.encrypted', data: 'opaque-interleaved', id: 'encrypted-1',
        })}`,
        '',
        `data: ${chunk({ type: 'reasoning.summary', summary: 'constraints.', id: 'summary-1' })}`,
        '',
        `data: ${chunk({ type: 'reasoning.text', text: 'Raw ', id: 'text-1' })}`,
        '',
        `data: ${chunk({
          type: 'reasoning.text', text: 'reasoning.', signature: 'opaque-signature', id: 'text-1',
        })}`,
        '',
        `data: ${chunk({ type: 'reasoning.summary', summary: 'Checked ', id: 'summary-2' })}`,
        '',
        `data: ${JSON.stringify({
          id: 'chatcmpl-openrouter-stream', object: 'chat.completion.chunk', created: 1,
          model: 'deepseek/deepseek-r1',
          choices: [{ index: 0, delta: { content: 'answer' }, finish_reason: 'stop' }],
        })}`,
        '', 'data: [DONE]', '',
      ].join('\n'), { headers: { 'content-type': 'text/event-stream' } }),
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openrouter-stream-reasoning-'));
    const capture = initialize({ output, serviceName: 'openrouter-stream-reasoning' });
    capture.instrument({
      adapter: openAIProviderAdapter({ provider: 'openrouter', version }), client,
    });

    const stream = await openAICreate(client)({
      model: 'deepseek/deepseek-r1', messages: [{ role: 'user', content: 'Calculate.' }],
      stream: true,
    });
    const applicationEvents = [];
    for await (const event of stream) applicationEvents.push(event);
    expect(JSON.stringify(applicationEvents)).toContain('opaque-signature');

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    expect(records.find((record) => record.kind === 'model.response')?.data).toMatchObject({
      content: 'answer',
      reasoning: [
        { type: 'summary', text: 'Checked ' },
        { type: 'summary', text: 'constraints.' },
        { type: 'text', text: 'Raw reasoning.' },
        { type: 'summary', text: 'Checked ' },
      ],
    });
    expect(JSON.stringify(records)).not.toContain('opaque-interleaved');
    expect(JSON.stringify(records)).not.toContain('opaque-signature');
    expect(records.filter((record) => record.kind === 'loss')).toHaveLength(1);
  });

  it('preserves consumed reasoning when the application cancels a provider stream', async () => {
    const first = JSON.stringify({
      id: 'chatcmpl-cancelled', object: 'chat.completion.chunk', created: 1,
      model: 'deepseek/deepseek-r1',
      choices: [{
        index: 0,
        delta: { reasoning_details: [{
          type: 'reasoning.text', text: 'Consumed reasoning.', id: 'reasoning-1', index: 0,
        }] },
        finish_reason: null,
      }],
    });
    const second = JSON.stringify({
      id: 'chatcmpl-cancelled', object: 'chat.completion.chunk', created: 1,
      model: 'deepseek/deepseek-r1',
      choices: [{ index: 0, delta: { content: 'unconsumed' }, finish_reason: 'stop' }],
    });
    const client = new OpenAI({
      apiKey: 'openrouter-cancelled-fixture', baseURL: 'https://example.invalid/v1',
      fetch: async () => new Response(
        `data: ${first}\n\ndata: ${second}\n\ndata: [DONE]\n\n`,
        { headers: { 'content-type': 'text/event-stream' } },
      ),
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openrouter-cancelled-'));
    const capture = initialize({ output, serviceName: 'openrouter-cancelled' });
    capture.instrument({
      adapter: openAIProviderAdapter({ provider: 'openrouter', version }), client,
    });

    const stream = await openAICreate(client)({
      model: 'deepseek/deepseek-r1', messages: [{ role: 'user', content: 'Calculate.' }],
      stream: true,
    });
    for await (const _event of stream) break;

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    expect(records.find((record) => record.kind === 'model.response')?.data).toMatchObject({
      status: 'cancelled', reasoning: [{ type: 'text', text: 'Consumed reasoning.' }],
    });
    expect(JSON.stringify(records)).not.toContain('unconsumed');
  });

  it('keeps OpenAI Responses raw reasoning, summaries, and incomplete status', async () => {
    const response = {
      id: 'resp-openai-reasoning', object: 'response', created_at: 1, status: 'incomplete',
      model: 'gpt-fixture',
      output: [{
        id: 'reasoning-summary-1', type: 'reasoning',
        summary: [{ type: 'summary_text', text: 'Summary one.' }],
      }, {
        id: 'reasoning-text-1', type: 'reasoning', summary: [],
        content: [{ type: 'reasoning_text', text: 'Exposed raw reasoning.' }],
        encrypted_content: 'opaque-response-encrypted-reasoning',
      }, {
        id: 'reasoning-summary-2', type: 'reasoning', summary: ['Summary one.'],
      }, {
        id: 'compaction-1', type: 'compaction',
        encrypted_content: 'opaque-compaction-encrypted-content',
      }],
      usage: { input_tokens: 2, output_tokens: 3, total_tokens: 5 },
    };
    const added = {
      type: 'response.output_item.added', output_index: 1, sequence_number: 0,
      item: response.output[1],
    };
    const compactionAdded = {
      type: 'response.output_item.added', output_index: 3, sequence_number: 1,
      item: response.output[3],
    };
    const terminal = { type: 'response.incomplete', sequence_number: 2, response };
    const client = new OpenAI({
      apiKey: 'openai-response-reasoning-fixture', baseURL: 'https://example.invalid/v1',
      fetch: async () => new Response([
        `data: ${JSON.stringify(added)}`, '', `data: ${JSON.stringify(compactionAdded)}`,
        '', `data: ${JSON.stringify(terminal)}`,
        '', 'data: [DONE]', '',
      ].join('\n'), { headers: { 'content-type': 'text/event-stream' } }),
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-openai-response-reasoning-'));
    const capture = initialize({ output, serviceName: 'openai-response-reasoning' });
    capture.instrument({ adapter: openAIProviderAdapter({ version }), client });

    const stream = await openAIResponseCreate(client)({
      model: 'gpt-fixture', input: 'Calculate.', stream: true,
    });
    const applicationEvents = [];
    for await (const event of stream) applicationEvents.push(event);
    expect(JSON.stringify(applicationEvents)).toContain('opaque-response-encrypted-reasoning');
    expect(JSON.stringify(applicationEvents)).toContain('opaque-compaction-encrypted-content');

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    expect(records.find((record) => record.kind === 'model.response')?.data).toMatchObject({
      status: 'incomplete',
      reasoning: [
        { type: 'summary', text: 'Summary one.' },
        { type: 'text', text: 'Exposed raw reasoning.' },
        { type: 'summary', text: 'Summary one.' },
      ],
    });
    expect(JSON.stringify(records)).not.toContain('opaque-response-encrypted-reasoning');
    expect(JSON.stringify(records)).not.toContain('opaque-compaction-encrypted-content');
    expect(records.filter((record) => record.kind === 'loss')).toHaveLength(1);
  });
});

it('compacts cumulative provider context across concurrent, equal, divergent, and shrinking requests', async () => {
  let responseIndex = 0;
  const client = new OpenAICurrent({
    apiKey: 'openai-context-history-fixture',
    baseURL: 'https://example.invalid/v1',
    fetch: async () => {
      responseIndex += 1;
      return new Response(JSON.stringify({
        id: `chatcmpl-context-${responseIndex}`,
        object: 'chat.completion',
        created: 1,
        model: 'gpt-fixture',
        choices: [{
          index: 0,
          message: { role: 'assistant', content: `answer ${responseIndex}` },
          finish_reason: 'stop',
        }],
      }), { headers: { 'content-type': 'application/json' } });
    },
  });
  const output = await mkdtemp(join(tmpdir(), 'semantic-openai-context-history-'));
  const capture = initialize({ output, serviceName: 'openai-context-history' });
  capture.instrument({ adapter: openAIProviderAdapter({ version: '6.46.0' }), client });
  const call = (contents: string[]) => client.chat.completions.create({
    model: 'gpt-fixture',
    messages: contents.map((content) => ({ role: 'user' as const, content })),
  });

  await capture.observe('context history', {}, async () => {
    await Promise.all([
      call(['A']),
      call(['A', 'B']),
    ]);
    await call(['A', 'B', 'C']);
    await call(['A', 'B', 'C']);
    await call(['A', 'X']);
    await call(['A']);
  });

  const records = await traceRecords((await capture.shutdown()).artifactPath);
  const messages = records.filter((record) => record.kind === 'message');
  const requests = records.filter((record) => record.kind === 'model.request');
  expect(messages.map((record) => record.data.content)).toEqual(['A', 'B', 'C', 'A', 'X', 'A']);
  expect(requests).toHaveLength(6);
  expect(requests.map((request) => request.data)).toEqual([
    { context_refs: [messages[0].id], model: 'gpt-fixture' },
    {
      context_base_ref: requests[0].id,
      context_refs: [messages[1].id],
      model: 'gpt-fixture',
    },
    {
      context_base_ref: requests[1].id,
      context_refs: [messages[2].id],
      model: 'gpt-fixture',
    },
    {
      context_base_ref: requests[2].id,
      context_refs: [],
      model: 'gpt-fixture',
    },
    {
      context_refs: [messages[3].id, messages[4].id],
      model: 'gpt-fixture',
    },
    {
      context_refs: [messages[5].id],
      model: 'gpt-fixture',
    },
  ]);
  expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
});

it('keeps roleless Responses function calls and outputs as model-visible context', async () => {
  const client = new OpenAICurrent({
    apiKey: 'openai-responses-context-fixture',
    baseURL: 'https://example.invalid/v1',
    fetch: async () => new Response(JSON.stringify({
      id: 'resp-context',
      object: 'response',
      created_at: 1,
      status: 'completed',
      model: 'gpt-fixture',
      output: [{
        id: 'message-context',
        type: 'message',
        status: 'completed',
        role: 'assistant',
        content: [{ type: 'output_text', text: 'done', annotations: [], logprobs: [] }],
      }],
      tools: [],
      parallel_tool_calls: true,
      tool_choice: 'auto',
    }), { headers: { 'content-type': 'application/json' } }),
  });
  const output = await mkdtemp(join(tmpdir(), 'semantic-openai-responses-context-'));
  const capture = initialize({ output, serviceName: 'openai-responses-context' });
  capture.instrument({ adapter: openAIProviderAdapter({ version: '6.46.0' }), client });

  await client.responses.create({
    model: 'gpt-fixture',
    input: [
      { role: 'user', content: 'Use the tool.' },
      {
        type: 'function_call',
        call_id: 'call-weather',
        name: 'get_weather',
        arguments: '{"city":"London"}',
      },
      {
        type: 'function_call_output',
        call_id: 'call-weather',
        output: '{"temperature_c":21}',
      },
    ],
  } as never);

  const records = await traceRecords((await capture.shutdown()).artifactPath);
  const messages = records.filter((record) => record.kind === 'message');
  expect(messages.map((record) => record.data.role)).toEqual(['user', 'assistant', 'tool']);
  expect(messages[1].data).toMatchObject({
    role: 'assistant',
    name: 'get_weather',
    call_id: 'call-weather',
    content: {
      type: 'function_call',
      call_id: 'call-weather',
      name: 'get_weather',
      arguments: '{"city":"London"}',
    },
  });
  expect(messages[2].data).toMatchObject({
    role: 'tool',
    call_id: 'call-weather',
    content: '{"temperature_c":21}',
  });
  expect(records.find((record) => record.kind === 'model.request')?.data.context_refs)
    .toEqual(messages.map((message) => message.id));
  expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
});

it.each([
  ['message admission', 'message'],
  ['request admission', 'request'],
] as const)('does not advance provider context history after rejected %s', async (_name, rejected) => {
  const calls: SourceRecord[] = [];
  const acceptedRequestIds: string[] = [];
  let sequence = 0;
  let rejectionArmed = false;
  let rejectedOnce = false;
  const receipt = () => ({
    accepted: true as const,
    recordId: `record_provider_${++sequence}`,
    settled: Promise.resolve(),
  });
  const sink: SourceSink = {
    openTrace: () => ({ ...receipt(), identity: {
      runId: 'run_provider_history',
      traceId: 'trace_provider_history',
      operationId: `operation_provider_${sequence}`,
    } }),
    record(input) {
      calls.push(input);
      const shouldReject = rejectionArmed && !rejectedOnce && (
        (rejected === 'message' && input.name === 'openai.context.message')
        || (rejected === 'request' && input.name === 'openai.request')
      );
      if (shouldReject) {
        rejectedOnce = true;
        return {
          accepted: false as const,
          reason: 'fixture_rejection',
          settled: Promise.resolve(),
        };
      }
      const result = receipt();
      if (input.name === 'openai.request') acceptedRequestIds.push(result.recordId);
      return result;
    },
  };
  const response = {
    id: 'resp-fixture',
    object: 'response',
    status: 'completed',
    model: 'gpt-fixture',
    output: [],
  };
  const client = {
    responses: { create: (_request: unknown) => response },
    chat: { completions: { create: (_request: unknown) => response } },
  };
  const source = openAIProviderAdapter({ version: 'fixture' }).createSource(client);
  const lifecycle = source.install(sink);
  try {
    client.responses.create({
      model: 'gpt-fixture',
      input: [{ role: 'user', content: 'A' }],
    });
    rejectionArmed = true;
    client.responses.create({
      model: 'gpt-fixture',
      input: [
        { role: 'user', content: 'A' },
        { role: 'user', content: 'B' },
      ],
    });
    client.responses.create({
      model: 'gpt-fixture',
      input: [
        { role: 'user', content: 'A' },
        { role: 'user', content: 'B' },
        { role: 'user', content: 'C' },
      ],
    });
  } finally {
    await lifecycle.deactivate();
  }

  const contextMessages = calls.filter((call) => call.name === 'openai.context.message');
  const requests = calls.filter((call) => call.name === 'openai.request');
  expect(contextMessages).toHaveLength(4);
  expect(requests).toHaveLength(3);
  expect(requests[2].semantic).toMatchObject({
    type: 'model.request',
    context_refs: expect.any(Array),
    context_base_ref: acceptedRequestIds[0],
  });
  expect((requests[2].semantic as Record<string, any>).context_refs).toHaveLength(2);
  expect((requests[2].native as Record<string, any>).request).toEqual({
    message_count: 3,
    metadata: { model: 'gpt-fixture' },
  });
});

it('does not base-chain provider context whose exact native value was not retained', async () => {
  const calls: SourceRecord[] = [];
  let sequence = 0;
  const accepted = () => ({
    accepted: true as const,
    recordId: `record_provider_exact_${++sequence}`,
    settled: Promise.resolve(),
  });
  const sink: SourceSink = {
    openTrace: () => ({
      ...accepted(),
      identity: {
        runId: 'run_provider_exact',
        traceId: 'trace_provider_exact',
        operationId: `operation_provider_exact_${sequence}`,
      },
    }),
    record(input) {
      calls.push(input);
      return accepted();
    },
  };
  const response = {
    id: 'resp-exact-fixture',
    object: 'response',
    status: 'completed',
    model: 'gpt-fixture',
    output: [],
  };
  const client = {
    responses: { create: (_request: unknown) => response },
    chat: { completions: { create: (_request: unknown) => response } },
  };
  const nested = (leaf: string): Record<string, unknown> => {
    let value: Record<string, unknown> = { leaf };
    for (let index = 0; index < 30; index += 1) value = { child: value };
    return value;
  };
  const lifecycle = openAIProviderAdapter({ version: 'fixture' }).createSource(client).install(sink);
  try {
    client.responses.create({
      model: 'gpt-fixture',
      input: [{ role: 'user', content: nested('first') }],
    });
    client.responses.create({
      model: 'gpt-fixture',
      input: [{ role: 'user', content: nested('second') }],
    });
  } finally {
    await lifecycle.deactivate();
  }

  const requests = calls.filter((call) => call.name === 'openai.request');
  const gaps = calls.filter((call) => call.name === 'openai.context.message_not_exact');
  expect(requests).toHaveLength(2);
  expect(requests.every((request) => (
    !Object.prototype.hasOwnProperty.call(request.semantic, 'context_base_ref')
  ))).toBe(true);
  expect(gaps).toHaveLength(2);
  expect(gaps.every((gap) => gap.semantic?.reason === 'unsupported_native_value')).toBe(true);
});

it('does not base-chain Responses context when an unrecognized input item changes', async () => {
  const calls: SourceRecord[] = [];
  let sequence = 0;
  const accepted = () => ({
    accepted: true as const,
    recordId: `record_provider_item_${++sequence}`,
    settled: Promise.resolve(),
  });
  const sink: SourceSink = {
    openTrace: () => ({
      ...accepted(),
      identity: {
        runId: 'run_provider_item',
        traceId: 'trace_provider_item',
        operationId: `operation_provider_item_${sequence}`,
      },
    }),
    record(input) {
      calls.push(input);
      return accepted();
    },
  };
  const response = {
    id: 'resp-item-fixture',
    object: 'response',
    status: 'completed',
    model: 'gpt-fixture',
    output: [],
  };
  const client = {
    responses: { create: (_request: unknown) => response },
    chat: { completions: { create: (_request: unknown) => response } },
  };
  const lifecycle = openAIProviderAdapter({ version: 'fixture' }).createSource(client).install(sink);
  try {
    client.responses.create({
      model: 'gpt-fixture',
      input: [{ role: 'user', content: 'A' }],
    });
    for (const id of ['item-reference-1', 'item-reference-2']) {
      client.responses.create({
        model: 'gpt-fixture',
        input: [
          { role: 'user', content: 'A' },
          { type: 'item_reference', id },
        ],
      });
    }
  } finally {
    await lifecycle.deactivate();
  }

  const requests = calls.filter((call) => call.name === 'openai.request');
  const gaps = calls.filter((call) => call.name === 'openai.context.item_unrecognized');
  expect(requests).toHaveLength(3);
  expect(requests[0].semantic).toHaveProperty('context_refs');
  for (const request of requests.slice(1)) {
    expect(request.semantic).not.toHaveProperty('context_base_ref');
    expect(request.semantic).not.toHaveProperty('context_refs');
  }
  expect(gaps).toHaveLength(2);
  expect(gaps.map((gap) => gap.semantic)).toEqual([
    expect.objectContaining({
      type: 'capture.gap',
      reason: 'unsupported_native_value',
      count: 1,
      detail: 'openai.context.item_unrecognized',
    }),
    expect.objectContaining({
      type: 'capture.gap',
      reason: 'unsupported_native_value',
      count: 1,
      detail: 'openai.context.item_unrecognized',
    }),
  ]);
});

it('includes changed Responses instructions in exact context', async () => {
  const calls: SourceRecord[] = [];
  let sequence = 0;
  const accepted = () => ({
    accepted: true as const,
    recordId: `record_provider_instructions_${++sequence}`,
    settled: Promise.resolve(),
  });
  const sink: SourceSink = {
    openTrace: () => ({
      ...accepted(),
      identity: {
        runId: 'run_provider_instructions',
        traceId: 'trace_provider_instructions',
        operationId: `operation_provider_instructions_${sequence}`,
      },
    }),
    record(input) {
      calls.push(input);
      return accepted();
    },
  };
  const response = {
    id: 'resp-instructions-fixture',
    object: 'response',
    status: 'completed',
    model: 'gpt-fixture',
    output: [],
  };
  const client = {
    responses: { create: (_request: unknown) => response },
    chat: { completions: { create: (_request: unknown) => response } },
  };
  const lifecycle = openAIProviderAdapter({ version: 'fixture' }).createSource(client).install(sink);
  try {
    for (const instructions of ['instruction A', 'instruction B']) {
      client.responses.create({
        model: 'gpt-fixture',
        instructions,
        input: [{ role: 'user', content: 'same user input' }],
      });
    }
  } finally {
    await lifecycle.deactivate();
  }

  const messages = calls.filter((call) => call.name === 'openai.context.message');
  const requests = calls.filter((call) => call.name === 'openai.request');
  expect(messages.map((message) => message.semantic)).toEqual([
    expect.objectContaining({ role: 'system', content: 'instruction A' }),
    expect.objectContaining({ role: 'user', content: 'same user input' }),
    expect.objectContaining({ role: 'system', content: 'instruction B' }),
    expect.objectContaining({ role: 'user', content: 'same user input' }),
  ]);
  expect(requests).toHaveLength(2);
  expect(requests[1].semantic).not.toHaveProperty('context_base_ref');
  expect((requests[1].semantic as Record<string, any>).context_refs).toHaveLength(2);
  expect(requests.map((request) => (request.native as Record<string, any>).request)).toEqual([
    { message_count: 2, metadata: { model: 'gpt-fixture' } },
    { message_count: 2, metadata: { model: 'gpt-fixture' } },
  ]);
  expect(calls.filter((call) => call.name === 'openai.context.item_unrecognized')).toEqual([]);
});

it.each([
  ['previous_response_id', 'response A', 'response B'],
  ['conversation', 'conversation A', 'conversation B'],
  [
    'prompt',
    { id: 'prompt-template', version: 'A', variables: { topic: 'same' } },
    { id: 'prompt-template', version: 'B', variables: { topic: 'same' } },
  ],
] as const)('does not claim exact Responses context with unresolved %s', async (
  field,
  first,
  second,
) => {
  const calls: SourceRecord[] = [];
  let sequence = 0;
  const accepted = () => ({
    accepted: true as const,
    recordId: `record_provider_inherited_${++sequence}`,
    settled: Promise.resolve(),
  });
  const sink: SourceSink = {
    openTrace: () => ({
      ...accepted(),
      identity: {
        runId: 'run_provider_inherited',
        traceId: 'trace_provider_inherited',
        operationId: `operation_provider_inherited_${sequence}`,
      },
    }),
    record(input) {
      calls.push(input);
      return accepted();
    },
  };
  const response = {
    id: 'resp-inherited-fixture',
    object: 'response',
    status: 'completed',
    model: 'gpt-fixture',
    output: [],
  };
  const client = {
    responses: { create: (_request: unknown) => response },
    chat: { completions: { create: (_request: unknown) => response } },
  };
  const lifecycle = openAIProviderAdapter({ version: 'fixture' }).createSource(client).install(sink);
  try {
    for (const inherited of [first, second]) {
      client.responses.create({
        model: 'gpt-fixture',
        input: [{ role: 'user', content: 'same user input' }],
        [field]: inherited,
      });
    }
  } finally {
    await lifecycle.deactivate();
  }

  const requests = calls.filter((call) => call.name === 'openai.request');
  const gaps = calls.filter((call) => call.name === 'openai.context.item_unrecognized');
  expect(requests).toHaveLength(2);
  for (const request of requests) {
    expect(request.semantic).not.toHaveProperty('context_base_ref');
    expect(request.semantic).not.toHaveProperty('context_refs');
  }
  expect(gaps).toHaveLength(2);
  expect(gaps.every((gap) => gap.semantic?.reason === 'unsupported_native_value')).toBe(true);
});

it('includes changed Gemini system instructions in exact context', async () => {
  const calls: SourceRecord[] = [];
  let sequence = 0;
  const accepted = () => ({
    accepted: true as const,
    recordId: `record_gemini_instructions_${++sequence}`,
    settled: Promise.resolve(),
  });
  const sink: SourceSink = {
    openTrace: () => ({
      ...accepted(),
      identity: {
        runId: 'run_gemini_instructions',
        traceId: 'trace_gemini_instructions',
        operationId: `operation_gemini_instructions_${sequence}`,
      },
    }),
    record(input) {
      calls.push(input);
      return accepted();
    },
  };
  const response = {
    responseId: 'gemini-instructions-fixture',
    modelVersion: 'gemini-fixture',
    candidates: [],
  };
  const client = {
    models: {
      generateContentInternal: (_request: unknown) => response,
      generateContentStreamInternal: (_request: unknown) => response,
    },
  };
  const lifecycle = geminiProviderAdapter({ version: 'fixture' }).createSource(client).install(sink);
  try {
    for (const systemInstruction of ['instruction A', 'instruction B']) {
      client.models.generateContentInternal({
        model: 'gemini-fixture',
        contents: 'same user input',
        config: { systemInstruction, temperature: 0.2 },
      });
    }
  } finally {
    await lifecycle.deactivate();
  }

  const messages = calls.filter((call) => call.name === 'gemini.context.message');
  const requests = calls.filter((call) => call.name === 'gemini.request');
  expect(messages.map((message) => message.semantic)).toEqual([
    expect.objectContaining({ role: 'system', content: 'instruction A' }),
    expect.objectContaining({ role: 'user', content: 'same user input' }),
    expect.objectContaining({ role: 'system', content: 'instruction B' }),
    expect.objectContaining({ role: 'user', content: 'same user input' }),
  ]);
  expect(requests).toHaveLength(2);
  expect(requests[1].semantic).not.toHaveProperty('context_base_ref');
  expect((requests[1].semantic as Record<string, any>).context_refs).toHaveLength(2);
  expect(requests.map((request) => (request.native as Record<string, any>).request)).toEqual([
    {
      message_count: 2,
      metadata: { model: 'gemini-fixture', config: { temperature: 0.2 } },
    },
    {
      message_count: 2,
      metadata: { model: 'gemini-fixture', config: { temperature: 0.2 } },
    },
  ]);
  expect(calls.filter((call) => call.name === 'gemini.context.item_unrecognized')).toEqual([]);
});

it('keeps constant-size provider history across 50,000 exact messages', async () => {
  let contextMessageCount = 0;
  const requests: Array<{ input: SourceRecord; recordId: string }> = [];
  let sequence = 0;
  const accepted = () => ({
    accepted: true as const,
    recordId: `record_provider_bound_${++sequence}`,
    settled: Promise.resolve(),
  });
  const sink: SourceSink = {
    openTrace: () => ({
      ...accepted(),
      identity: {
        runId: 'run_provider_bound',
        traceId: 'trace_provider_bound',
        operationId: `operation_provider_bound_${sequence}`,
      },
    }),
    record(input) {
      if (input.name === 'openai.context.message') contextMessageCount += 1;
      const result = accepted();
      if (input.name === 'openai.request') {
        requests.push({ input, recordId: result.recordId });
      }
      return result;
    },
  };
  const response = {
    id: 'resp-bound-fixture',
    object: 'response',
    status: 'completed',
    model: 'gpt-fixture',
    output: [],
  };
  const client = {
    responses: { create: (_request: unknown) => response },
    chat: { completions: { create: (_request: unknown) => response } },
  };
  const input = Array.from({ length: 50_000 }, (_, index) => ({
    role: 'user',
    content: `message ${index}`,
  }));
  const lifecycle = openAIProviderAdapter({ version: 'fixture' }).createSource(client).install(sink);
  try {
    client.responses.create({ model: 'gpt-fixture', input });
    client.responses.create({ model: 'gpt-fixture', input });
  } finally {
    await lifecycle.deactivate();
  }

  expect(contextMessageCount).toBe(50_000);
  expect(requests).toHaveLength(2);
  expect(requests[0].input.semantic).toMatchObject({
    type: 'model.request',
    context_refs: expect.any(Array),
  });
  expect((requests[0].input.semantic as Record<string, any>).context_refs).toHaveLength(50_000);
  expect(requests[1].input.semantic).toEqual({
    type: 'model.request',
    provider: 'openai',
    model: 'gpt-fixture',
    context_base_ref: requests[0].recordId,
    context_refs: [],
  });
}, 30_000);

it('keeps a lossless linear context chain beyond 256 provider requests', async () => {
  const response = {
    id: 'resp-long-chain',
    object: 'response',
    status: 'completed',
    model: 'gpt-fixture',
    output: [],
  };
  const client = {
    responses: { create: (_request: unknown) => response },
    chat: { completions: { create: (_request: unknown) => response } },
  };
  const output = await mkdtemp(join(tmpdir(), 'semantic-provider-long-context-chain-'));
  const capture = initialize({ output, serviceName: 'provider-long-context-chain' });
  capture.instrument({ adapter: openAIProviderAdapter({ version: 'fixture' }), client });
  const input: Array<{ role: 'user'; content: string }> = [];

  await capture.observe('long provider context', {}, () => {
    for (let index = 0; index < 300; index += 1) {
      input.push({ role: 'user', content: `message ${index}` });
      client.responses.create({ model: 'gpt-fixture', input });
    }
  });

  const records = await traceRecords((await capture.shutdown()).artifactPath);
  const messages = records.filter((record) => record.kind === 'message');
  const requests = records.filter((record) => record.kind === 'model.request');
  expect(messages).toHaveLength(300);
  expect(requests).toHaveLength(300);
  expect(requests[0].data.context_refs).toEqual([messages[0].id]);
  for (let index = 1; index < requests.length; index += 1) {
    expect(requests[index].data.context_base_ref).toBe(requests[index - 1].id);
    expect(requests[index].data.context_refs).toEqual([messages[index].id]);
  }
  expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
}, 30_000);

it('projects one OpenAI stream terminal instead of every consumed delta', async () => {
  const response = {
    id: 'resp-stream-terminal',
    object: 'response',
    created_at: 1,
    status: 'completed',
    model: 'gpt-fixture',
    output: [{
      id: 'reasoning-stream',
      type: 'reasoning',
      summary: [{ type: 'summary_text', text: 'Checked the stream evidence.' }],
    }, {
      id: 'message-stream',
      type: 'message',
      status: 'completed',
      role: 'assistant',
      content: [{
        type: 'output_text',
        text: 'streamed answer',
        annotations: [],
        logprobs: [],
      }],
    }],
    parallel_tool_calls: true,
    tool_choice: 'auto',
    tools: [],
    usage: { input_tokens: 2, output_tokens: 3, total_tokens: 5 },
  };
  const client = new OpenAICurrent({
    apiKey: 'openai-stream-fixture',
    baseURL: 'https://example.invalid/v1',
    fetch: async () => new Response([
      `data: ${JSON.stringify({
        type: 'response.output_text.delta',
        sequence_number: 0,
        item_id: 'message-stream',
        output_index: 0,
        content_index: 0,
        delta: 'streamed ',
      })}`,
      '',
      `data: ${JSON.stringify({
        type: 'response.completed',
        sequence_number: 1,
        response,
      })}`,
      '',
      'data: [DONE]',
      '',
    ].join('\n'), { headers: { 'content-type': 'text/event-stream' } }),
  });
  const output = await mkdtemp(join(tmpdir(), 'semantic-openai-stream-projection-'));
  const capture = initialize({ output, serviceName: 'openai-stream-projection' });
  capture.instrument({
    adapter: openAIProviderAdapter({ version: '6.46.0' }),
    client,
  });

  const stream = await client.responses.create({
    model: 'gpt-fixture',
    input: [{ role: 'user', content: 'Stream the answer.' }],
    stream: true,
  });
  const events = [];
  for await (const event of stream) events.push(event);
  expect(events).toHaveLength(2);

  const records = await traceRecords((await capture.shutdown()).artifactPath);
  const request = records.find((record) => record.kind === 'model.request')!;
  const terminal = records.find((record) => record.kind === 'model.response')!;
  expect(records.filter((record) => record.kind === 'model.response')).toHaveLength(1);
  expect(terminal.data).toMatchObject({
    status: 'completed',
    model: 'gpt-fixture',
    content: 'streamed answer',
    reasoning: [{ type: 'summary', text: 'Checked the stream evidence.' }],
    usage: { input_tokens: 2, output_tokens: 3 },
  });
  expect(terminal.links).toEqual([{ type: 'result_of', record: request.id }]);
  expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
  expect(records.length).toBeLessThan(8);
});

it('folds chat-completion stream deltas into one compact response', async () => {
  const client = new OpenAICurrent({
    apiKey: 'openai-chat-stream-fixture',
    baseURL: 'https://example.invalid/v1',
    fetch: async () => new Response([
      'data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"gpt-fixture","choices":[{"index":0,"delta":{"reasoning_content":"check ","content":"hello "},"finish_reason":null}]}',
      '',
      'data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"gpt-fixture","choices":[{"index":0,"delta":{"reasoning_content":"done","content":"world"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}',
      '',
      'data: [DONE]',
      '',
    ].join('\n'), { headers: { 'content-type': 'text/event-stream' } }),
  });
  const output = await mkdtemp(join(tmpdir(), 'semantic-openai-chat-stream-projection-'));
  const capture = initialize({ output, serviceName: 'openai-chat-stream-projection' });
  capture.instrument({
    adapter: openAIProviderAdapter({ version: '6.46.0' }),
    client,
  });
  const stream = await client.chat.completions.create({
    model: 'gpt-fixture',
    messages: [{ role: 'user', content: 'Stream.' }],
    stream: true,
    stream_options: { include_usage: true },
  });
  for await (const _event of stream) {
    // The application remains the only stream consumer.
  }

  const records = await traceRecords((await capture.shutdown()).artifactPath);
  expect(records.find((record) => record.kind === 'model.response')?.data).toMatchObject({
    model: 'gpt-fixture',
    content: 'hello world',
    reasoning: [{ type: 'text', text: 'check done' }],
    finish_reason: 'stop',
    usage: { input_tokens: 2, output_tokens: 2 },
  });
  expect(records.filter((record) => record.kind === 'model.response')).toHaveLength(1);
  expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
});

it('bounds retained stream evidence without changing application chunks', async () => {
  const fragments = Array.from({ length: 3_000 }, () => 'x'.repeat(128));
  const body = [
    ...fragments.map((content, index) => [
      'data: ',
      JSON.stringify({
        id: 'chatcmpl-long-stream',
        object: 'chat.completion.chunk',
        created: 1,
        model: 'gpt-fixture',
        choices: [{
          index: 0,
          delta: { content },
          finish_reason: index === fragments.length - 1 ? 'stop' : null,
        }],
      }),
    ].join('')),
    'data: [DONE]',
    '',
  ].join('\n\n');
  const client = new OpenAICurrent({
    apiKey: 'openai-long-stream-fixture',
    baseURL: 'https://example.invalid/v1',
    fetch: async () => new Response(body, {
      headers: { 'content-type': 'text/event-stream' },
    }),
  });
  const output = await mkdtemp(join(tmpdir(), 'semantic-openai-long-stream-'));
  const capture = initialize({ output, serviceName: 'openai-long-stream' });
  capture.instrument({
    adapter: openAIProviderAdapter({ version: '6.46.0' }),
    client,
  });

  const stream = await client.chat.completions.create({
    model: 'gpt-fixture',
    messages: [{ role: 'user', content: 'Stream a long response.' }],
    stream: true,
  });
  const observed: string[] = [];
  for await (const event of stream) {
    observed.push(event.choices[0]?.delta.content ?? '');
  }
  expect(observed).toEqual(fragments);

  const records = await traceRecords((await capture.shutdown()).artifactPath);
  const losses = records.filter((record) => record.kind === 'loss');
  expect(losses).toEqual([
    expect.objectContaining({
      data: expect.objectContaining({
        reason: 'serialization_failure',
        detail: 'openai.stream.retention_truncated',
      }),
    }),
  ]);
  const retained = records.find((record) => record.kind === 'model.response')?.data.content;
  expect(typeof retained).toBe('string');
  expect(String(retained).length).toBeLessThan(fragments.join('').length);
});

it('keeps OpenRouter failures independent from later provider calls', async () => {
  let calls = 0;
  const client = new OpenAICurrent({
    apiKey: 'openrouter-provider-fixture',
    baseURL: 'https://openrouter.example.invalid/api/v1',
    maxRetries: 0,
    fetch: async () => {
      calls += 1;
      if (calls === 1) {
        return new Response(JSON.stringify({
          error: { message: 'rate limited', type: 'rate_limit' },
        }), {
          status: 429,
          headers: { 'content-type': 'application/json', 'x-request-id': 'request-failed' },
        });
      }
      return new Response(JSON.stringify({
        id: 'chatcmpl-success',
        object: 'chat.completion',
        created: 1,
        model: 'router-fixture',
        choices: [{
          index: 0,
          message: { role: 'assistant', content: 'done' },
          finish_reason: 'stop',
        }],
      }), { headers: { 'content-type': 'application/json' } });
    },
  });
  const output = await mkdtemp(join(tmpdir(), 'semantic-openrouter-projection-'));
  const capture = initialize({ output, serviceName: 'openrouter-provider-projection' });
  capture.instrument({
    adapter: openAIProviderAdapter({ provider: 'openrouter', version: '6.46.0' }),
    client,
  });

  await expect(client.chat.completions.create({
    model: 'router-fixture',
    messages: [{ role: 'user', content: 'first' }],
  })).rejects.toThrow();
  await expect(client.chat.completions.create({
    model: 'router-fixture',
    messages: [{ role: 'user', content: 'second' }],
  })).resolves.toMatchObject({ id: 'chatcmpl-success' });

  const records = await traceRecords((await capture.shutdown()).artifactPath);
  const roots = records.filter((record) => record.kind === 'run.start');
  const outcomes = records.filter((record) => record.kind === 'run.outcome');
  expect(roots).toHaveLength(2);
  expect(outcomes.map((record) => record.data.status).sort()).toEqual([
    'completed',
    'failed',
  ]);
  expect(records.filter((record) => record.kind === 'error')).toHaveLength(1);
  expect(records.filter((record) => record.kind === 'model.response')).toHaveLength(1);
  expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
});

it('keeps one provider error when an OpenAI failure crosses a manual run boundary', async () => {
  const client = new OpenAICurrent({
    apiKey: 'openai-nested-error-fixture',
    baseURL: 'https://example.invalid/v1',
    maxRetries: 0,
    fetch: async () => new Response(JSON.stringify({
      error: {
        message: 'fixture rate limit',
        type: 'rate_limit_error',
        code: 'rate_limit_exceeded',
      },
    }), {
      status: 429,
      headers: {
        'content-type': 'application/json',
        'x-request-id': 'request-nested-failure',
      },
    }),
  });
  const output = await mkdtemp(join(tmpdir(), 'semantic-openai-nested-error-'));
  const capture = initialize({ output, serviceName: 'openai-nested-error' });
  capture.instrument({
    adapter: openAIProviderAdapter({ version: '6.46.0' }),
    client,
  });

  await expect(capture.observe('agent.run', {}, async () => {
    await client.chat.completions.create({
      model: 'gpt-fixture',
      messages: [{ role: 'user', content: 'Fail once.' }],
      stream: true,
    });
  })).rejects.toThrow('fixture rate limit');

  const records = await traceRecords((await capture.shutdown()).artifactPath);
  expect(records.filter((record) => record.kind === 'error')).toEqual([
    expect.objectContaining({
      data: {
        type: 'error',
        message: '429 fixture rate limit',
        recoverable: false,
        code: 'rate_limit_exceeded',
      },
    }),
  ]);
  expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
  expect(records.filter((record) => record.kind === 'run.outcome')).toEqual([
    expect.objectContaining({ data: expect.objectContaining({ status: 'failed' }) }),
  ]);
});

describe.each([
  ['0.111.0', AnthropicCurrent],
  ['0.110.0', AnthropicPrevious],
] as const)('Anthropic provider semantic trace %s', (version, Anthropic) => {
  it('projects exact content, usage, and native tool identity', async () => {
    const client = new Anthropic({
      apiKey: 'anthropic-provider-fixture',
      baseURL: 'https://example.invalid',
      fetch: async () => new Response(JSON.stringify({
        id: 'msg-fixture',
        type: 'message',
        role: 'assistant',
        model: 'claude-fixture',
        content: [
          {
            type: 'thinking', thinking: 'Checked the weather constraints.',
            signature: 'opaque-anthropic-signature-1',
          },
          { type: 'redacted_thinking', data: 'opaque-redacted-anthropic-thinking' },
          {
            type: 'thinking', thinking: 'Checked the weather constraints.',
            signature: 'opaque-anthropic-signature-2',
          },
          { type: 'text', text: 'I will look it up.' },
          {
            type: 'tool_use',
            id: 'tool-anthropic',
            name: 'lookup',
            input: { city: 'London' },
          },
        ],
        stop_reason: 'tool_use',
        stop_sequence: null,
        usage: { input_tokens: 2, output_tokens: 4 },
      }), { headers: { 'content-type': 'application/json' } }),
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-anthropic-projection-'));
    const capture = initialize({ output, serviceName: 'anthropic-provider-projection' });
    capture.instrument({ adapter: anthropicProviderAdapter({ version }), client });

    await anthropicCreate(client)({
      model: 'claude-fixture',
      max_tokens: 32,
      system: 'Be precise.',
      messages: [{ role: 'user', content: 'Weather?' }],
      tools: [{
        name: 'lookup',
        description: 'fixture',
        input_schema: {
          type: 'object',
          properties: { city: { type: 'string' } },
          required: ['city'],
        },
      }],
    });

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    expect(records.filter((record) => record.kind === 'message')).toHaveLength(2);
    expect(records.find((record) => record.kind === 'model.response')?.data).toMatchObject({
      status: 'completed',
      model: 'claude-fixture',
      content: 'I will look it up.',
      reasoning: [
        { type: 'summary', text: 'Checked the weather constraints.' },
        { type: 'summary', text: 'Checked the weather constraints.' },
      ],
      finish_reason: 'tool_use',
      usage: { input_tokens: 2, output_tokens: 4 },
    });
    expect(records.find((record) => record.kind === 'tool.proposal')?.data).toMatchObject({
      native_call_id: 'tool-anthropic',
      name: 'lookup',
      input: { city: 'London' },
    });
    expect(records.filter((record) => record.kind === 'loss')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          reason: 'unsupported_native_value', detail: 'anthropic.reasoning.opaque_unavailable',
        }),
      }),
    ]);
    expect(JSON.stringify(records)).not.toContain('opaque-anthropic-signature');
    expect(JSON.stringify(records)).not.toContain('opaque-redacted-anthropic-thinking');
  });

  it('observes beta messages.create without replacing its APIPromise', async () => {
    const client = new Anthropic({
      apiKey: 'anthropic-beta-provider-fixture',
      baseURL: 'https://example.invalid',
      fetch: async () => new Response(JSON.stringify({
        id: 'msg-beta-fixture',
        type: 'message',
        role: 'assistant',
        model: 'claude-beta-fixture',
        content: [{ type: 'text', text: 'Beta response.' }],
        stop_reason: 'end_turn',
        stop_sequence: null,
        usage: { input_tokens: 3, output_tokens: 2 },
      }), { headers: { 'content-type': 'application/json' } }),
    });
    const betaMessages = client.beta.messages as unknown as Record<PropertyKey, unknown>;
    const originalCreate = betaMessages.create as (...args: unknown[]) => unknown;
    let nativePromise: unknown;
    const trackedCreate = function trackedBetaCreate(this: unknown, ...args: unknown[]): unknown {
      nativePromise = Reflect.apply(originalCreate, this, args);
      return nativePromise;
    };
    Object.defineProperty(betaMessages, 'create', {
      configurable: true,
      writable: true,
      value: trackedCreate,
    });

    const output = await mkdtemp(join(tmpdir(), 'semantic-anthropic-beta-projection-'));
    const capture = initialize({ output, serviceName: 'anthropic-beta-provider-projection' });
    capture.instrument({ adapter: anthropicProviderAdapter({ version }), client });

    const returned = anthropicBetaCreate(client)({
      model: 'claude-beta-fixture',
      max_tokens: 32,
      messages: [{ role: 'user', content: 'Use the beta route.' }],
    });
    expect(returned).toBe(nativePromise);
    await expect(returned).resolves.toMatchObject({
      id: 'msg-beta-fixture',
      content: [{ type: 'text', text: 'Beta response.' }],
    });

    const closed = await capture.shutdown();
    expect(betaMessages.create).toBe(trackedCreate);
    const records = await traceRecords(closed.artifactPath);
    expect(records.find((record) => record.kind === 'model.response')?.data).toMatchObject({
      status: 'completed',
      model: 'claude-beta-fixture',
      content: 'Beta response.',
      usage: { input_tokens: 3, output_tokens: 2 },
    });
    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
  });

  it('reconstructs streamed tool input and omits opaque thinking signatures from context', async () => {
    let calls = 0;
    const client = new Anthropic({
      apiKey: 'anthropic-stream-tool-fixture',
      baseURL: 'https://example.invalid',
      maxRetries: 0,
      fetch: async () => {
        calls += 1;
        if (calls === 1) {
          const events = [
            ['message_start', {
              type: 'message_start',
              message: {
                id: 'msg-stream-tool',
                type: 'message',
                role: 'assistant',
                model: 'claude-fixture',
                content: [],
                stop_reason: null,
                stop_sequence: null,
                usage: { input_tokens: 3, output_tokens: 1 },
              },
            }],
            ['content_block_start', {
              type: 'content_block_start',
              index: 0,
              content_block: { type: 'thinking', thinking: '', signature: '' },
            }],
            ['content_block_delta', {
              type: 'content_block_delta',
              index: 0,
              delta: { type: 'thinking_delta', thinking: 'Use the tool.' },
            }],
            ['content_block_delta', {
              type: 'content_block_delta',
              index: 0,
              delta: { type: 'signature_delta', signature: 'opaque-signature' },
            }],
            ['content_block_stop', { type: 'content_block_stop', index: 0 }],
            ['content_block_start', {
              type: 'content_block_start',
              index: 1,
              content_block: {
                type: 'tool_use',
                id: 'toolu-stream',
                name: 'get_weather',
                input: {},
              },
            }],
            ['content_block_delta', {
              type: 'content_block_delta',
              index: 1,
              delta: {
                type: 'input_json_delta',
                partial_json: '{"location":"San Francisco"}',
              },
            }],
            ['content_block_stop', { type: 'content_block_stop', index: 1 }],
            ['message_delta', {
              type: 'message_delta',
              delta: { stop_reason: 'tool_use', stop_sequence: null },
              usage: { output_tokens: 8 },
            }],
            ['message_stop', { type: 'message_stop' }],
          ];
          return new Response(events.map(([name, data]) => (
            `event: ${name}\ndata: ${JSON.stringify(data)}\n\n`
          )).join(''), { headers: { 'content-type': 'text/event-stream' } });
        }
        return new Response(JSON.stringify({
          id: 'msg-final',
          type: 'message',
          role: 'assistant',
          model: 'claude-fixture',
          content: [{ type: 'text', text: 'It is 73f.' }],
          stop_reason: 'end_turn',
          stop_sequence: null,
          usage: { input_tokens: 9, output_tokens: 4 },
        }), { headers: { 'content-type': 'application/json' } });
      },
    });
    const output = await mkdtemp(join(tmpdir(), 'semantic-anthropic-stream-tool-'));
    const capture = initialize({ output, serviceName: 'anthropic-stream-tool' });
    capture.instrument({ adapter: anthropicProviderAdapter({ version }), client });

    await capture.observe('agent.run', {}, async () => {
      const stream = client.messages.stream({
        model: 'claude-fixture',
        max_tokens: 64,
        messages: [{ role: 'user', content: 'Weather?' }],
        tools: [{
          name: 'get_weather',
          description: 'fixture',
          input_schema: {
            type: 'object',
            properties: { location: { type: 'string' } },
            required: ['location'],
          },
        }],
      });
      for await (const _event of stream) {
        // Preserve the official application-owned iterator.
      }
      const first = await stream.finalMessage();
      const result = await capture.tool(
        'get_weather',
        { location: 'San Francisco' },
        async () => '73f',
        { callId: 'toolu-stream' },
      );
      await anthropicCreate(client)({
        model: 'claude-fixture',
        max_tokens: 64,
        messages: [
          { role: 'user', content: 'Weather?' },
          { role: 'assistant', content: first.content },
          {
            role: 'user',
            content: [{
              type: 'tool_result',
              tool_use_id: 'toolu-stream',
              content: result,
            }],
          },
        ],
      });
    });

    const records = await traceRecords((await capture.shutdown()).artifactPath);
    const proposal = records.find((record) => record.kind === 'tool.proposal')!;
    expect(proposal.data).toMatchObject({
      native_call_id: 'toolu-stream',
      name: 'get_weather',
      input: { location: 'San Francisco' },
    });
    const call = records.find((record) => record.kind === 'tool.call')!;
    expect(call.data.call_id).toBe(proposal.data.call_id);
    expect(call.links).toEqual([{ type: 'derived_from', record: proposal.id }]);
    expect(records.find((record) => record.kind === 'tool.result')?.links)
      .toEqual([{ type: 'result_of', record: call.id }]);
    const assistantContext = records.find((record) => (
      record.kind === 'message' && record.data.role === 'assistant'
    ));
    expect(assistantContext?.data.content).toEqual([
      { type: 'thinking', thinking: 'Use the tool.' },
      {
        type: 'tool_use',
        id: 'toolu-stream',
        name: 'get_weather',
        input: { location: 'San Francisco' },
      },
    ]);
    expect(JSON.stringify(assistantContext)).not.toContain('opaque-signature');
    expect(records.find((record) => (
      record.kind === 'model.response' && Array.isArray(record.data.reasoning)
    ))?.data.reasoning).toEqual([{ type: 'summary', text: 'Use the tool.' }]);
    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
  });
});

describe.each([
  ['2.11.0', GeminiCurrent],
  ['2.10.0', GeminiPrevious],
] as const)('Gemini provider semantic trace %s', (version, GoogleGenAI) => {
  it('preserves repeated non-stream thought summaries and omits thought signatures', async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async () => new Response(JSON.stringify({
      responseId: 'gemini-thought-summaries', modelVersion: 'gemini-fixture',
      candidates: [{
        index: 0, finishReason: 'STOP',
        content: {
          role: 'model',
          parts: [{
            thought: true, text: 'Checked the constraints.',
            thoughtSignature: 'opaque-gemini-signature-1',
          }, {
            thought: true, text: 'Checked the constraints.',
            thoughtSignature: 'opaque-gemini-signature-2',
          }, { text: 'The answer is 42.' }],
        },
      }],
    }), { headers: { 'content-type': 'application/json' } });
    try {
      const client = new GoogleGenAI({ apiKey: 'gemini-thought-summaries-fixture' });
      const output = await mkdtemp(join(tmpdir(), 'semantic-gemini-thought-summaries-'));
      const capture = initialize({ output, serviceName: 'gemini-thought-summaries' });
      capture.instrument({ adapter: geminiProviderAdapter({ version }), client });

      await client.models.generateContent({ model: 'gemini-fixture', contents: 'Calculate.' });

      const records = await traceRecords((await capture.shutdown()).artifactPath);
      expect(records.find((record) => record.kind === 'model.response')?.data).toMatchObject({
        content: 'The answer is 42.',
        reasoning: [
          { type: 'summary', text: 'Checked the constraints.' },
          { type: 'summary', text: 'Checked the constraints.' },
        ],
      });
      expect(JSON.stringify(records)).not.toContain('opaque-gemini-signature');
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  const correlatedTurns = async (
    proposals: Array<{ id?: string; name: string }>,
    responses: Array<{ id?: string; name: string }>,
  ): Promise<SemanticTraceRecord[]> => {
    const originalFetch = globalThis.fetch;
    let requestIndex = 0;
    globalThis.fetch = async () => {
      requestIndex += 1;
      return new Response(JSON.stringify(requestIndex === 1 ? {
        responseId: 'gemini-correlation-proposal',
        modelVersion: 'gemini-fixture',
        candidates: [{
          index: 0,
          finishReason: 'STOP',
          content: {
            role: 'model',
            parts: proposals.map((proposal) => ({
              functionCall: {
                ...(proposal.id ? { id: proposal.id } : {}),
                name: proposal.name,
                args: { value: proposal.name },
              },
            })),
          },
        }],
      } : {
        responseId: 'gemini-correlation-result',
        modelVersion: 'gemini-fixture',
        candidates: [{
          index: 0,
          finishReason: 'STOP',
          content: { role: 'model', parts: [{ text: 'done' }] },
        }],
      }), { headers: { 'content-type': 'application/json' } });
    };
    try {
      const client = new GoogleGenAI({ apiKey: 'gemini-correlation-fixture' });
      const output = await mkdtemp(join(tmpdir(), 'semantic-gemini-correlation-'));
      const capture = initialize({ output, serviceName: 'gemini-correlation' });
      capture.instrument({ adapter: geminiProviderAdapter({ version }), client });
      await withProviderCaptureContext({
        conversationId: 'conversation',
        turnId: 'proposal-turn',
      }, () => client.models.generateContent({
        model: 'gemini-fixture',
        contents: [{ role: 'user', parts: [{ text: 'Call tools.' }] }],
      }));
      await withProviderCaptureContext({
        conversationId: 'conversation',
        turnId: 'result-turn',
        previousTurnId: 'proposal-turn',
      }, () => client.models.generateContent({
        model: 'gemini-fixture',
        contents: [{
          role: 'user',
          parts: responses.map((response) => ({
            functionResponse: {
              ...(response.id ? { id: response.id } : {}),
              name: response.name,
              response: { output: response.name },
            },
          })),
        }],
      } as never));
      return await traceRecords((await capture.shutdown()).artifactPath);
    } finally {
      globalThis.fetch = originalFetch;
    }
  };

  it('emits an exact streamed proposal before AFC tool execution and omits thought signatures', async () => {
    const originalFetch = globalThis.fetch;
    let requests = 0;
    globalThis.fetch = async () => {
      requests += 1;
      const response = requests === 1 ? {
        responseId: 'gemini-afc-proposal',
        modelVersion: 'gemini-fixture',
        candidates: [{
          index: 0,
          finishReason: 'STOP',
          content: {
            role: 'model',
            parts: [
              {
                thought: true,
                text: 'Use the light tool.',
                thoughtSignature: 'opaque-thought-signature',
              },
              {
                functionCall: {
                  id: 'call-light',
                  name: 'controlLight',
                  args: { brightness: 25 },
                },
              },
            ],
          },
        }],
        usageMetadata: {
          promptTokenCount: 3,
          candidatesTokenCount: 4,
          totalTokenCount: 7,
        },
      } : {
        responseId: 'gemini-afc-final',
        modelVersion: 'gemini-fixture',
        candidates: [{
          index: 0,
          finishReason: 'STOP',
          content: { role: 'model', parts: [{ text: 'Light adjusted.' }] },
        }],
        usageMetadata: {
          promptTokenCount: 8,
          candidatesTokenCount: 3,
          totalTokenCount: 11,
        },
      };
      return new Response(`data: ${JSON.stringify(response)}\n\n`, {
        headers: { 'content-type': 'text/event-stream' },
      });
    };
    try {
      const client = new GoogleGenAI({ apiKey: 'gemini-afc-fixture' });
      const output = await mkdtemp(join(tmpdir(), 'semantic-gemini-afc-'));
      const capture = initialize({ output, serviceName: 'gemini-afc' });
      capture.instrument({ adapter: geminiProviderAdapter({ version }), client });

      await capture.observe('agent.run', {}, async () => {
        const stream = await client.models.generateContentStream({
          model: 'gemini-fixture',
          contents: 'Dim the light.',
          config: {
            tools: [{
              tool: async () => ({
                functionDeclarations: [{
                  name: 'controlLight',
                  parametersJsonSchema: { type: 'object' },
                }],
              }),
              callTool: async (calls: Array<{
                id?: string;
                name?: string;
                args?: Record<string, unknown>;
              }>) => capture.tool(
                'controlLight',
                calls,
                async () => [{
                  functionResponse: {
                    id: calls[0]?.id,
                    name: 'controlLight',
                    response: { brightness: 25 },
                  },
                }],
                { callId: calls[0]?.id },
              ),
            }],
          },
        } as never);
        for await (const _chunk of stream) {
          // The application remains the only consumer.
        }
      });

      const records = await traceRecords((await capture.shutdown()).artifactPath);
      const proposal = records.find((record) => record.kind === 'tool.proposal')!;
      const call = records.find((record) => record.kind === 'tool.call')!;
      const result = records.find((record) => record.kind === 'tool.result')!;
      expect(proposal.data).toMatchObject({
        native_call_id: 'call-light',
        name: 'controlLight',
        input: { brightness: 25 },
      });
      expect(proposal.seq).toBeLessThan(call.seq);
      expect(call.data.call_id).toBe(proposal.data.call_id);
      expect(call.links).toEqual([{ type: 'derived_from', record: proposal.id }]);
      expect(result.links).toEqual([{ type: 'result_of', record: call.id }]);
      expect(records.filter((record) => record.kind === 'tool.proposal')).toHaveLength(1);
      expect(records.find((record) => record.kind === 'model.response')?.data.reasoning).toEqual([
        { type: 'summary', text: 'Use the light tool.' },
      ]);
      expect(JSON.stringify(records)).not.toContain('opaque-thought-signature');
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it('keeps exact correlation when tool proposals arrive in separate stream parts', async () => {
    const originalFetch = globalThis.fetch;
    let requestIndex = 0;
    globalThis.fetch = async () => {
      requestIndex += 1;
      if (requestIndex === 1) {
        const events = [
          {
            responseId: 'gemini-split-proposals',
            modelVersion: 'gemini-fixture',
            candidates: [{
              index: 0,
              content: {
                role: 'model',
                parts: [{
                  functionCall: {
                    id: 'call-a',
                    name: 'first',
                    args: { value: 'first' },
                  },
                }],
              },
            }],
          },
          {
            responseId: 'gemini-split-proposals',
            modelVersion: 'gemini-fixture',
            candidates: [{
              index: 0,
              finishReason: 'STOP',
              content: {
                role: 'model',
                parts: [{
                  functionCall: {
                    id: 'call-b',
                    name: 'second',
                    args: { value: 'second' },
                  },
                }],
              },
            }],
          },
        ];
        return new Response(events.map((event) => (
          `data: ${JSON.stringify(event)}\n\n`
        )).join(''), {
          headers: { 'content-type': 'text/event-stream' },
        });
      }
      return new Response(JSON.stringify({
        responseId: 'gemini-split-results',
        modelVersion: 'gemini-fixture',
        candidates: [{
          index: 0,
          finishReason: 'STOP',
          content: { role: 'model', parts: [{ text: 'done' }] },
        }],
      }), { headers: { 'content-type': 'application/json' } });
    };
    try {
      const client = new GoogleGenAI({ apiKey: 'gemini-split-fixture' });
      const output = await mkdtemp(join(tmpdir(), 'semantic-gemini-split-'));
      const capture = initialize({ output, serviceName: 'gemini-split' });
      capture.instrument({ adapter: geminiProviderAdapter({ version }), client });

      await withProviderCaptureContext({
        conversationId: 'conversation',
        turnId: 'proposal-turn',
      }, async () => {
        const stream = await client.models.generateContentStream({
          model: 'gemini-fixture',
          contents: 'Call both tools.',
        });
        for await (const _chunk of stream) {
          // The application remains the only consumer.
        }
      });
      await withProviderCaptureContext({
        conversationId: 'conversation',
        turnId: 'result-turn',
        previousTurnId: 'proposal-turn',
      }, () => client.models.generateContent({
        model: 'gemini-fixture',
        contents: [{
          role: 'user',
          parts: [
            {
              functionResponse: {
                id: 'call-b',
                name: 'second',
                response: { output: 'second' },
              },
            },
            {
              functionResponse: {
                id: 'call-a',
                name: 'first',
                response: { output: 'first' },
              },
            },
          ],
        }],
      } as never));

      const records = await traceRecords((await capture.shutdown()).artifactPath);
      expect(records.filter((record) => record.kind === 'tool.proposal').map(
        (record) => record.data.native_call_id,
      )).toEqual(['call-a', 'call-b']);
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it('projects an id-less function proposal once from the terminal response', async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async () => new Response(JSON.stringify({
      responseId: 'gemini-fixture-response',
      modelVersion: 'gemini-fixture',
      candidates: [{
        index: 0,
        finishReason: 'STOP',
        content: {
          role: 'model',
          parts: [{
            functionCall: { name: 'lookup', args: { city: 'London' } },
          }],
        },
      }],
      usageMetadata: {
        promptTokenCount: 2,
        candidatesTokenCount: 3,
        totalTokenCount: 5,
      },
    }), { headers: { 'content-type': 'application/json' } });
    try {
      const client = new GoogleGenAI({ apiKey: 'gemini-provider-fixture' });
      const output = await mkdtemp(join(tmpdir(), 'semantic-gemini-projection-'));
      const capture = initialize({ output, serviceName: 'gemini-provider-projection' });
      capture.instrument({ adapter: geminiProviderAdapter({ version }), client });

      await client.models.generateContent({
        model: 'gemini-fixture',
        contents: [{ role: 'user', parts: [{ text: 'Weather?' }] }],
      });

      const records = await traceRecords((await capture.shutdown()).artifactPath);
      const proposal = records.find((record) => record.kind === 'tool.proposal');
      expect(records.filter((record) => record.kind === 'message')).toHaveLength(1);
      expect(records.find((record) => record.kind === 'model.response')?.data).toMatchObject({
        status: 'completed',
        model: 'gemini-fixture',
        finish_reason: 'STOP',
        usage: { input_tokens: 2, output_tokens: 3 },
      });
      expect(proposal?.data).toMatchObject({
        name: 'lookup',
        input: { city: 'London' },
      });
      expect(proposal?.data).not.toHaveProperty('native_call_id');
      expect(records.filter((record) => record.kind === 'tool.proposal')).toHaveLength(1);
      expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });

  it.each([
    {
      name: 'one id-less response',
      proposals: [{ name: 'first' }],
      responses: [{ name: 'first' }],
    },
    {
      name: 'multiple reversed id-less responses',
      proposals: [{ name: 'first' }, { name: 'second' }],
      responses: [{ name: 'second' }, { name: 'first' }],
    },
    {
      name: 'mismatched exact response IDs',
      proposals: [{ id: 'call-a', name: 'first' }],
      responses: [{ id: 'call-b', name: 'first' }],
    },
  ])('records one explicit loss for $name', async ({ proposals, responses }) => {
    const records = await correlatedTurns(proposals, responses);

    expect(records.filter((record) => (
      record.kind === 'tool.call' || record.kind === 'tool.result'
    ))).toEqual([]);
    expect(records.filter((record) => record.kind === 'loss')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          reason: 'source_rejection',
          detail: 'gemini.function_responses.unpaired',
        }),
      }),
    ]);
  });

  it('correlates reordered function responses only by exact native ID', async () => {
    const records = await correlatedTurns(
      [{ id: 'call-a', name: 'first' }, { id: 'call-b', name: 'second' }],
      [{ id: 'call-b', name: 'second' }, { id: 'call-a', name: 'first' }],
    );

    expect(records.filter((record) => record.kind === 'loss')).toEqual([]);
    expect(records.filter((record) => record.kind === 'tool.proposal').map(
      (record) => record.data.native_call_id,
    )).toEqual(['call-a', 'call-b']);
  });

  it('preserves an exact proposal ID beside an id-less sibling and reports the gap', async () => {
    const records = await correlatedTurns(
      [{ id: 'call-a', name: 'first' }, { name: 'second' }],
      [{ id: 'call-a', name: 'first' }, { name: 'second' }],
    );

    const proposals = records.filter((record) => record.kind === 'tool.proposal');
    expect(proposals[0]?.data.native_call_id).toBe('call-a');
    expect(proposals[1]?.data).not.toHaveProperty('native_call_id');
    expect(records.filter((record) => record.kind === 'loss')).toHaveLength(1);
  });
});

function openAICreate(client: OpenAIClient): OpenAICreate {
  return client.chat.completions.create.bind(client.chat.completions) as OpenAICreate;
}

function openAIResponseCreate(client: OpenAIClient): OpenAIResponseCreate {
  return client.responses.create.bind(client.responses) as OpenAIResponseCreate;
}

function anthropicCreate(client: AnthropicClient): AnthropicCreate {
  return client.messages.create.bind(client.messages) as AnthropicCreate;
}

function anthropicBetaCreate(client: AnthropicClient): AnthropicBetaCreate {
  return client.beta.messages.create.bind(client.beta.messages) as AnthropicBetaCreate;
}

async function traceRecords(artifactPath: string): Promise<SemanticTraceRecord[]> {
  const text = await readFile(join(artifactPath, 'trace.jsonl'), 'utf8');
  const records = text.trim().split('\n').map(
    (line) => JSON.parse(line) as SemanticTraceRecord,
  );
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
