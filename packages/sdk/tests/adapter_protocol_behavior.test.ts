import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import * as LangGraphCurrent from 'langgraph-current';
import * as LangGraphPrevious from 'langgraph-previous';
import * as StrandsCurrent from 'strands-current';
import * as StrandsPrevious from 'strands-previous';
import { afterEach, describe, expect, it } from 'vitest';

import {
  geminiProviderAdapter,
  initialize,
  langGraphAdapter,
  openAIProviderAdapter,
  resetCaptureForTests,
  strandsAdapter,
} from '../src/index.js';

afterEach(async () => resetCaptureForTests());

describe.each([
  ['1.4.7', LangGraphCurrent],
  ['1.4.6', LangGraphPrevious],
] as const)('LangGraph %s protocol parity', (version, _LangGraph) => {
  it('preserves iterator result and error identity without a second consumer', async () => {
    const nextResult = { done: false as const, value: { state: 'next' } };
    const returnResult = { done: true as const, value: { state: 'cancelled' } };
    const upstreamError = new Error('native graph stream failure');
    let iteratorCreations = 0;
    const nativeIterator: AsyncIterator<unknown> = {
      next: async () => nextResult,
      return: async () => returnResult,
      throw: async () => { throw upstreamError; },
    };
    const nativeStream = {
      [Symbol.asyncIterator]() {
        iteratorCreations += 1;
        return nativeIterator;
      },
    };
    const subject = {
      invoke: () => ({ ok: true }),
      stream: () => nativeStream,
    };
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-protocol-'));
    const capture = initialize({ output, serviceName: 'langgraph-protocol' });
    const adapter = langGraphAdapter({ version });
    const lifecycle = capture.instrument({ adapter, client: subject });

    const observed = adapter.stream({ value: 'fixed' }) as AsyncIterable<unknown>;
    const iterator = observed[Symbol.asyncIterator]();
    expect(await iterator.next()).toBe(nextResult);
    expect(await iterator.return!()).toBe(returnResult);
    await expect(iterator.throw!(upstreamError)).rejects.toBe(upstreamError);
    expect(iteratorCreations).toBe(1);

    lifecycle.deactivate();
    expect(adapter.stream({ value: 'inactive' })).toBe(nativeStream);
    await capture.shutdown();
  });

  it('uses the exact propagating stream error to avoid a duplicate manual error', async () => {
    const upstreamError = new TypeError('native graph stream failure');
    const nativeStream = {
      [Symbol.asyncIterator]() {
        return {
          next: async () => { throw upstreamError; },
        };
      },
    };
    const subject = {
      invoke: () => ({ ok: true }),
      stream(_input: unknown, config?: Record<string, unknown>) {
        const callbacks = config?.callbacks as Array<Record<string, unknown>>;
        const handler = callbacks.at(-1);
        const start = handler?.handleChainStart;
        if (typeof start !== 'function') throw new Error('capture callback missing');
        Reflect.apply(start, handler, [
          { name: 'failure-graph' },
          { value: 'fixed' },
          'failure-run',
          undefined,
          [],
          {},
          'chain',
          'failure-graph',
        ]);
        return nativeStream;
      },
    };
    const output = await mkdtemp(join(tmpdir(), 'semantic-langgraph-error-identity-'));
    const capture = initialize({ output, serviceName: 'langgraph-error-identity' });
    const adapter = langGraphAdapter({ version });
    capture.instrument({ adapter, client: subject });

    await expect(capture.observe('manual graph run', {}, async () => {
      for await (const _part of adapter.stream({ value: 'fixed' }) as AsyncIterable<unknown>) {
        // The upstream failure occurs before a part is delivered.
      }
    })).rejects.toBe(upstreamError);

    const closed = await capture.shutdown();
    const text = await readFile(join(closed.artifactPath, 'trace.jsonl'), 'utf8');
    const rows = text.trim().split('\n').map((line) => JSON.parse(line) as {
      kind: string;
      data: Record<string, unknown>;
    });
    expect(rows.filter((row) => row.kind === 'error')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          type: 'langgraph_error',
          message: 'native graph stream failure',
        }),
      }),
    ]);
    expect(rows.filter((row) => row.kind === 'run.outcome')).toEqual([
      expect.objectContaining({ data: expect.objectContaining({ status: 'failed' }) }),
    ]);
    expect(text).not.toContain('errorIdentity');
  });
});

it('preserves exact native provider Promise identity and custom properties', async () => {
  const marker = { exact: true };
  let officialPromise: (Promise<{ id: string }> & { marker: object }) | undefined;
  const models = {
    generateContentInternal: () => Promise.resolve({ id: 'native-value' }),
    generateContentStreamInternal: () => Promise.resolve({}),
    generateContent() {
      const promise = (async () => await this.generateContentInternal())() as
        Promise<{ id: string }> & { marker: object };
      promise.marker = marker;
      officialPromise = promise;
      return promise;
    },
    async generateContentStream() { return await this.generateContentStreamInternal(); },
  };
  const output = await mkdtemp(join(tmpdir(), 'semantic-provider-promise-'));
  const capture = initialize({ output, serviceName: 'provider-promise' });
  capture.instrument({
    adapter: geminiProviderAdapter({ version: 'test' }),
    client: { models },
  });

  const returned = models.generateContent();
  expect(returned).toBe(officialPromise);
  expect(returned.constructor).toBe(Promise);
  expect(returned.marker).toBe(marker);
  await expect(returned).resolves.toEqual({ id: 'native-value' });
  await capture.shutdown();
});

it('rolls back provider patches when every required seam cannot be installed', async () => {
  const originalResponse = () => Promise.resolve({ id: 'unused' });
  const originalChat = () => Promise.resolve({ id: 'unused' });
  const completions = {} as { create: typeof originalChat };
  Object.defineProperty(completions, 'create', {
    configurable: false,
    enumerable: true,
    writable: false,
    value: originalChat,
  });
  const client = {
    responses: { create: originalResponse },
    chat: { completions },
  };
  const output = await mkdtemp(join(tmpdir(), 'semantic-provider-transactional-'));
  const capture = initialize({ output, serviceName: 'provider-transactional' });

  expect(() => capture.instrument({
    adapter: openAIProviderAdapter({ version: 'test' }),
    client,
  })).toThrow();
  expect(client.responses.create).toBe(originalResponse);
  expect(client.chat.completions.create).toBe(originalChat);
});

it('leaves a frozen provider stream to the application without consuming it', async () => {
  let iteratorCreations = 0;
  const stream = Object.freeze({
    async *[Symbol.asyncIterator]() {
      iteratorCreations += 1;
      yield { delta: 'application-owned' };
    },
  });
  const create = () => stream;
  const client = { responses: { create }, chat: { completions: { create } } };
  const output = await mkdtemp(join(tmpdir(), 'semantic-provider-frozen-stream-'));
  const capture = initialize({ output, serviceName: 'provider-frozen-stream' });
  capture.instrument({
    adapter: openAIProviderAdapter({ version: 'test' }),
    client,
  });

  const returned = await client.responses.create();
  expect(returned).toBe(stream);
  const parts: unknown[] = [];
  for await (const part of returned) parts.push(part);
  expect(parts).toEqual([{ delta: 'application-owned' }]);
  expect(iteratorCreations).toBe(1);
  await capture.shutdown();
});

describe.each([
  ['1.9.0', StrandsCurrent],
  ['1.8.0', StrandsPrevious],
] as const)('Strands %s installation', (version, Strands) => {
  it('removes already registered hooks when a later registration fails', () => {
    type HookConstructor = abstract new (...args: any[]) => object;
    const callbacks = new Map<HookConstructor, Set<(event: any) => unknown>>();
    let registrations = 0;
    const subject = {
      addHook(type: HookConstructor, callback: (event: any) => unknown) {
        registrations += 1;
        if (registrations === 4) throw new Error('registration failed');
        const values = callbacks.get(type) ?? new Set();
        values.add(callback);
        callbacks.set(type, values);
        return () => values.delete(callback);
      },
    };
    const source = strandsAdapter({ version, sdk: Strands }).createSource(subject);
    const rejected = {
      accepted: false as const,
      reason: 'test',
      settled: Promise.resolve(),
    };
    expect(() => source.install({
      openTrace: () => rejected,
      record: () => rejected,
    })).toThrow('registration failed');
    expect([...callbacks.values()].every((values) => values.size === 0)).toBe(true);
  });
});
