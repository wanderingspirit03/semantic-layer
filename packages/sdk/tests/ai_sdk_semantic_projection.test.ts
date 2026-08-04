import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import Ajv2020 from 'ajv/dist/2020.js';
import addFormats from 'ajv-formats';
import * as AICurrent from 'ai-current';
import * as AIPrevious from 'ai-previous';
import * as AICurrentTest from 'ai-current/test';
import * as AIPreviousTest from 'ai-previous/test';
import { afterEach, describe, expect, it } from 'vitest';

import { aiSDKAdapter, initialize, resetCaptureForTests } from '../src/index.js';
import type { SemanticTraceRecord } from '../src/trace/semantic-projector.js';

afterEach(async () => resetCaptureForTests());

const usage = {
  inputTokens: { total: 2, noCache: 2, cacheRead: 0, cacheWrite: 0 },
  outputTokens: { total: 3, text: 3, reasoning: 0 },
};

type PreparedMessage = {
  role: string;
  content: unknown;
};

it('reports AI SDK qualification from the observed version only', () => {
  const subject = { registerTelemetry() {}, streamText() {} };

  expect(aiSDKAdapter({ version: '7.0.22' })
    .createSource(subject).metadata.qualification).toEqual({ status: 'exact_qualified' });
  expect(aiSDKAdapter({ version: '8.0.0' })
    .createSource(subject).metadata.qualification).toEqual({
      status: 'capability_checked_unqualified', profile: 'ai-sdk-telemetry-v1',
    });
  expect(aiSDKAdapter().createSource(subject).metadata.qualification).toEqual({
    status: 'unknown',
  });
});

describe.each([
  ['7.0.22', AICurrent, AICurrentTest],
  ['7.0.21', AIPrevious, AIPreviousTest],
] as const)('AI SDK semantic projection %s', (version, AI, AITest) => {
  it.each(['telemetry', 'experimental_telemetry'] as const)(
    'composes per-call %s integrations with Semantic Layer telemetry',
    async (telemetryAlias) => {
      const callbacks: string[] = [];
      const firstIntegration = {
        onStart() {
          callbacks.push('first:start');
          return 'first-start';
        },
        onLanguageModelCallEnd() {
          callbacks.push('first:model-end');
          return 'first-model-end';
        },
        onEnd() {
          callbacks.push('first:end');
          return 'first-end';
        },
      };
      const secondIntegration = {
        onStart() {
          callbacks.push('second:start');
          return 'second-start';
        },
        onLanguageModelCallEnd() {
          callbacks.push('second:model-end');
          return 'second-model-end';
        },
        onEnd() {
          callbacks.push('second:end');
          return 'second-end';
        },
      };
      const integrations = Object.freeze([firstIntegration, secondIntegration]);
      const telemetry = Object.freeze({ integrations });
      const input = Object.freeze({
        model: new AITest.MockLanguageModelV3({
          provider: 'fixture-provider',
          modelId: 'fixture-per-call-telemetry',
          doStream: {
            stream: AITest.simulateReadableStream({
              chunks: [
                { type: 'stream-start', warnings: [] },
                { type: 'reasoning-start', id: 'reasoning' },
                {
                  type: 'reasoning-delta',
                  id: 'reasoning',
                  delta: 'Visible reasoning.',
                },
                { type: 'reasoning-end', id: 'reasoning' },
                { type: 'text-start', id: 'answer' },
                { type: 'text-delta', id: 'answer', delta: 'Final answer.' },
                { type: 'text-end', id: 'answer' },
                {
                  type: 'finish',
                  finishReason: { unified: 'stop', raw: 'stop' },
                  usage,
                },
              ],
            }),
          },
        }),
        prompt: 'Answer with exposed reasoning.',
        [telemetryAlias]: telemetry,
      });
      const output = await mkdtemp(join(tmpdir(), 'semantic-ai-sdk-per-call-'));
      const capture = initialize({
        output,
        serviceName: 'ai-sdk-per-call-telemetry',
      });
      const adapter = aiSDKAdapter({ version });
      capture.instrument({ adapter, client: AI });

      const result = adapter.streamText(input) as { text: Promise<string> };
      await expect(result.text).resolves.toBe('Final answer.');

      expect(input[telemetryAlias]).toBe(telemetry);
      expect((input[telemetryAlias] as typeof telemetry).integrations).toBe(integrations);
      expect(callbacks).toEqual([
        'first:start',
        'second:start',
        'first:model-end',
        'second:model-end',
        'first:end',
        'second:end',
      ]);

      const closed = await capture.shutdown();
      const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line) as SemanticTraceRecord);
      const response = records.find((record) => record.kind === 'model.response');
      const outcome = records.find((record) => record.kind === 'run.outcome');

      expect(response?.data.reasoning).toEqual([{ type: 'text', text: 'Visible reasoning.' }]);
      expect(outcome?.data).toMatchObject({
        status: 'completed',
        output: 'Final answer.',
      });
    },
  );

  it('projects one real tool workflow into a compact, schema-valid trace', async () => {
    const textChunks = Array.from({ length: 24 }, (_, index) => ({
      type: 'text-delta' as const,
      id: 'answer',
      delta: index === 0 ? 'sunny' : `-${index}`,
    }));
    const finalText = textChunks.map((chunk) => chunk.delta).join('');
    const model = new AITest.MockLanguageModelV3({
      provider: 'fixture-provider',
      modelId: 'fixture-semantic-trace',
      doStream: [
        {
          stream: AITest.simulateReadableStream({ chunks: [
            { type: 'stream-start', warnings: [] },
            {
              type: 'tool-call',
              toolCallId: 'weather-call',
              toolName: 'weather',
              input: '{"city":"London"}',
            },
            {
              type: 'finish',
              finishReason: { unified: 'tool-calls', raw: 'tool-calls' },
              usage,
            },
          ] }),
        },
        {
          stream: AITest.simulateReadableStream({ chunks: [
            { type: 'stream-start', warnings: [] },
            { type: 'reasoning-start', id: 'reasoning-before' },
            {
              type: 'reasoning-delta',
              id: 'reasoning-before',
              delta: 'Checked the exposed weather evidence first.',
            },
            { type: 'reasoning-end', id: 'reasoning-before' },
            { type: 'text-start', id: 'answer' },
            ...textChunks,
            { type: 'text-end', id: 'answer' },
            { type: 'reasoning-start', id: 'reasoning-after' },
            {
              type: 'reasoning-delta',
              id: 'reasoning-after',
              delta: 'Then verified the final wording.',
              providerMetadata: {
                fixture: { opaqueReasoningLikeValue: 'not semantic reasoning' },
              },
            },
            { type: 'reasoning-end', id: 'reasoning-after' },
            {
              type: 'finish',
              finishReason: { unified: 'stop', raw: 'stop' },
              usage,
            },
          ] }),
        },
      ],
    });
    const weather = version === '7.0.22'
      ? AICurrent.tool({
          inputSchema: AICurrent.jsonSchema<{ city: string }>({
            type: 'object',
            properties: { city: { type: 'string' } },
            required: ['city'],
            additionalProperties: false,
          }),
          execute: async ({ city }) => ({ city, condition: 'sunny' }),
        })
      : AIPrevious.tool({
          inputSchema: AIPrevious.jsonSchema<{ city: string }>({
            type: 'object',
            properties: { city: { type: 'string' } },
            required: ['city'],
            additionalProperties: false,
          }),
          execute: async ({ city }) => ({ city, condition: 'sunny' }),
        });
    const output = await mkdtemp(join(tmpdir(), 'semantic-ai-sdk-projection-'));
    const capture = initialize({ output, serviceName: 'ai-sdk-semantic-projection' });
    const adapter = aiSDKAdapter({ version });
    capture.instrument({ adapter, client: AI });

    const result = adapter.streamText({
      model,
      instructions: 'Use verified weather evidence.',
      prompt: 'Check London weather.',
      prepareStep: async ({
        stepNumber,
        messages: preparedMessages,
      }: {
        stepNumber: number;
        messages: PreparedMessage[];
      }) => ({
        ...(stepNumber === 1 ? {
          messages: preparedMessages.map((message, index) => (
            index === 0 && message.role === 'user'
              ? { ...message, content: 'Check London weather now.' }
              : message
          )),
        } : {}),
      }),
      stopWhen: version === '7.0.22'
        ? AICurrent.stepCountIs(2)
        : AIPrevious.stepCountIs(2),
      tools: { weather },
    }) as { fullStream: AsyncIterable<unknown> };
    for await (const _part of result.fullStream) {
      // The application, not capture, is the only stream consumer.
    }

    const closed = await capture.shutdown();
    const records = (await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8'))
      .trim()
      .split('\n')
      .map((line) => JSON.parse(line) as SemanticTraceRecord);
    const meaningful = records.filter((record) => record.kind !== 'loss');
    const losses = records.filter((record) => record.kind === 'loss');

    const root = meaningful.find((record) => record.kind === 'run.start')!;
    const outcome = meaningful.find((record) => record.kind === 'run.outcome')!;
    const messages = meaningful.filter((record) => record.kind === 'message');
    const requests = meaningful.filter((record) => record.kind === 'model.request');
    const responses = meaningful.filter((record) => record.kind === 'model.response');
    const proposal = meaningful.find((record) => record.kind === 'tool.proposal')!;
    const call = meaningful.find((record) => record.kind === 'tool.call')!;
    const toolResult = meaningful.find((record) => record.kind === 'tool.result')!;

    expect(root.data).toMatchObject({
      name: 'ai_sdk.ai.streamText',
      input: { messages: [{ role: 'user', content: 'Check London weather.' }] },
    });
    expect(requests).toHaveLength(2);
    expect(requests.map((record) => record.data.context_refs)).toEqual([
      [messages[0]?.id, messages[1]?.id],
      [messages[0]?.id, ...messages.slice(2).map((message) => message.id)],
    ]);
    expect(messages.map((message) => message.data.role)).toEqual([
      'system',
      'user',
      'user',
      'assistant',
      'tool',
    ]);
    expect(messages[0]?.data.content).toEqual('Use verified weather evidence.');
    expect(messages[1]?.data.content).toEqual('Check London weather.');
    expect(messages[2]?.data.content).toEqual('Check London weather now.');
    expect(responses).toHaveLength(2);
    expect(responses.map((record) => record.data.usage)).toEqual([
      { input_tokens: 2, output_tokens: 3 },
      { input_tokens: 2, output_tokens: 3 },
    ]);
    expect(responses[1]?.data.reasoning).toEqual([
      {
        type: 'text',
        text: 'Checked the exposed weather evidence first.',
      },
      {
        type: 'text',
        text: 'Then verified the final wording.',
      },
    ]);
    expect(responses.every((record, index) => (
      record.links?.some((link) => (
        link.type === 'result_of' && link.record === requests[index]?.id
      ))
    ))).toBe(true);
    expect(proposal.data).toMatchObject({
      name: 'weather',
      input: { city: 'London' },
    });
    expect(call).toMatchObject({
      parent: root.id,
      data: { name: 'weather', input: { city: 'London' } },
      links: [{ type: 'derived_from', record: proposal.id }],
    });
    expect(toolResult).toMatchObject({
      parent: root.id,
      data: {
        status: 'succeeded',
        output: { city: 'London', condition: 'sunny' },
      },
      links: [{ type: 'result_of', record: call.id }],
    });
    expect(outcome).toMatchObject({
      parent: root.id,
      data: { status: 'completed', output: finalText },
    });
    expect(meaningful.filter((record) => record.kind === 'state')).toEqual([]);

    // The official terminal callbacks already contain the consumed content and
    // records or false losses.
    expect(losses).toEqual([]);
    expect(records.length).toBeLessThan(15);
    expect(records.map((record) => record.seq)).toEqual(
      records.map((_, index) => index + 1),
    );
    await expectSchema(records);
  });
});

async function expectSchema(records: unknown[]): Promise<void> {
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
