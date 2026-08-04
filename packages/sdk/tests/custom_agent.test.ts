import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';
import {
  createCustomAgentSource,
  initialize,
  resetCaptureForTests,
  type CustomAgentEvent,
} from '../src/index.js';

afterEach(async () => {
  await resetCaptureForTests();
});

describe('custom agent capture', () => {
  it('keeps exact model and tool identities in one compact run', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent', version: '1' });
    capture.installSource(bridge.source);

    expect(bridge.record({
      type: 'run.start',
      runId: 'run-a',
      name: 'coding-agent',
      input: { task: 'read README' },
    }).accepted).toBe(true);
    bridge.record({
      type: 'message',
      runId: 'run-a',
      messageId: 'message-1',
      role: 'user',
      content: [{ type: 'text', text: 'read README' }],
    });
    bridge.record({
      type: 'model.request',
      runId: 'run-a',
      callId: 'model-1',
      model: 'fixture-model',
      messageIds: ['message-1'],
    });
    bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'model-1',
      status: 'completed',
      content: [{ type: 'text', text: 'I will read it.' }],
      usage: { inputTokens: 3, outputTokens: 5 },
    });
    bridge.record({
      type: 'tool.proposal',
      runId: 'run-a',
      callId: 'tool-1',
      name: 'read_file',
      input: { path: 'README.md' },
    });
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'tool-1',
      name: 'read_file',
      input: { path: 'README.md' },
    });
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'tool-1',
      status: 'succeeded',
      output: { text: 'contents' },
    });
    bridge.record({
      type: 'run.outcome',
      runId: 'run-a',
      status: 'completed',
      output: 'done',
    });

    const records = await sealedRecords(await capture.shutdown());
    expect(records.map((record) => record.kind)).toEqual([
      'run.start',
      'message',
      'model.request',
      'model.response',
      'tool.proposal',
      'tool.call',
      'tool.result',
      'run.outcome',
    ]);
    const call = records.find((record) => record.kind === 'tool.call')!;
    const result = records.find((record) => record.kind === 'tool.result')!;
    const proposal = records.find((record) => record.kind === 'tool.proposal')!;
    const message = records.find((record) => record.kind === 'message')!;
    const request = records.find((record) => record.kind === 'model.request')!;
    expect(request.data.context_refs).toEqual([message.id]);
    expect(proposal.data.native_call_id).toBe('tool-1');
    expect(call.data.native_call_id).toBe('tool-1');
    expect(result.data.native_call_id).toBe('tool-1');
    expect(call.data.call_id).toBe(proposal.data.call_id);
    expect(result.data.call_id).toBe(call.data.call_id);
    expect(call.links).toContainEqual({ type: 'derived_from', record: proposal.id });
    expect(result.links).toContainEqual({ type: 'result_of', record: call.id });
  });

  it('keeps standalone model responses and reports missing request coverage once per run', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-standalone-model-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'pi-shaped-agent' });

    expect(bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'pi-response-1',
      status: 'completed',
      model: 'fixture-model',
      content: [{ type: 'text', text: 'I inspected the repository.' }],
      reasoning: [{ type: 'summary', text: 'Checked the relevant files.' }],
      finishReason: 'stop',
      usage: { inputTokens: 17, outputTokens: 9 },
    }).accepted).toBe(true);
    expect(bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'pi-response-2',
      status: 'cancelled',
      model: 'fixture-model',
    }).accepted).toBe(true);
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'cancelled' });

    const records = await sealedRecords(await capture.shutdown());
    const responses = records.filter((record) => record.kind === 'model.response');
    expect(responses).toHaveLength(2);
    expect(responses[0]?.data).toMatchObject({
      status: 'completed',
      model: 'fixture-model',
      content: [{ type: 'text', text: 'I inspected the repository.' }],
      reasoning: [{ type: 'summary', text: 'Checked the relevant files.' }],
      finish_reason: 'stop',
      usage: { input_tokens: 17, output_tokens: 9 },
    });
    expect(responses[1]?.data.status).toBe('cancelled');
    expect(responses.every((response) => (
      !response.links?.some((link) => link.type === 'result_of')
    ))).toBe(true);
    expect(records.filter((record) => (
      record.kind === 'loss' && record.data.reason === 'model_request_not_observed'
    ))).toHaveLength(1);
    expect(records.find((record) => record.kind === 'run.outcome')?.data.status)
      .toBe('cancelled');
  });

  it('preserves exact model context presence without partial references', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-context-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });
    for (const messageId of ['message-1', 'message-2']) {
      bridge.record({
        type: 'message',
        runId: 'run-a',
        messageId,
        role: 'user',
        content: messageId,
      });
    }

    const requests: Array<Extract<CustomAgentEvent, { type: 'model.request' }>> = [
      { type: 'model.request', runId: 'run-a', callId: 'omitted' },
      { type: 'model.request', runId: 'run-a', callId: 'empty', messageIds: [] },
      {
        type: 'model.request',
        runId: 'run-a',
        callId: 'valid',
        messageIds: ['message-1', 'message-2'],
      },
      {
        type: 'model.request',
        runId: 'run-a',
        callId: 'mixed',
        messageIds: ['message-1', 'unknown-message'],
      },
      {
        type: 'model.request',
        runId: 'run-a',
        callId: 'malformed',
        messageIds: 42,
      } as unknown as Extract<CustomAgentEvent, { type: 'model.request' }>,
    ];
    for (const request of requests) {
      bridge.record(request);
      bridge.record({
        type: 'model.response',
        runId: 'run-a',
        callId: request.callId,
        status: 'completed',
        content: null,
      });
    }
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'completed' });

    const records = await sealedRecords(await capture.shutdown());
    const messages = records.filter((record) => record.kind === 'message');
    const modelRequests = records.filter((record) => record.kind === 'model.request');
    expect(modelRequests).toHaveLength(5);
    expect(modelRequests[0]?.data).not.toHaveProperty('context_refs');
    expect(modelRequests[1]?.data.context_refs).toEqual([]);
    expect(modelRequests[2]?.data.context_refs).toEqual(messages.map((record) => record.id));
    expect(modelRequests[3]?.data).not.toHaveProperty('context_refs');
    expect(modelRequests[4]?.data).not.toHaveProperty('context_refs');
    expect(records.filter((record) => record.kind === 'loss').map((record) => (
      record.data.reason
    ))).toEqual(['unknown_message_id', 'invalid_message_ids']);
  });

  it('separates concurrent runs and accepts call/result without a proposal', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-concurrent-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);

    for (const runId of ['run-a', 'run-b']) {
      bridge.record({ type: 'run.start', runId, name: 'agent' });
      bridge.record({
        type: 'tool.call',
        runId,
        callId: 'shared-native-id',
        name: 'lookup',
        input: { runId },
      });
    }
    for (const runId of ['run-b', 'run-a']) {
      bridge.record({
        type: 'tool.result',
        runId,
        callId: 'shared-native-id',
        status: 'succeeded',
        output: { runId },
      });
      bridge.record({ type: 'run.outcome', runId, status: 'completed' });
    }

    const records = await sealedRecords(await capture.shutdown());
    const calls = records.filter((record) => record.kind === 'tool.call');
    const results = records.filter((record) => record.kind === 'tool.result');
    expect(calls).toHaveLength(2);
    expect(results).toHaveLength(2);
    expect(new Set(calls.map((record) => record.data.call_id)).size).toBe(2);
    for (const result of results) {
      const linked = result.links?.find((link) => link.type === 'result_of')?.record;
      expect(calls.some((call) => call.id === linked && call.parent === result.parent)).toBe(true);
    }
  });

  it('uses a supplied manual call ID without changing tool behavior', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-manual-call-ts-')),
      serviceName: 'custom-test',
    });
    const returned = { exact: true };
    const result = await capture.observe('manual-agent', {}, (run) => run.tool(
      'read_file',
      { path: 'README.md' },
      () => returned,
      { callId: 'native-call-7' },
    ));
    expect(result).toBe(returned);

    await capture.observe('manual-agent-invalid-id', {}, (run) => run.tool(
      'read_file',
      { path: 'README.md' },
      () => returned,
      { callId: '\u0000' },
    ));
    const records = await sealedRecords(await capture.shutdown());
    const call = records.find((record) => record.kind === 'tool.call')!;
    const toolResult = records.find((record) => record.kind === 'tool.result')!;
    expect(call.data.native_call_id).toBe('native-call-7');
    expect(toolResult.data.call_id).toBe(call.data.call_id);
    expect(records.find((record) => (
      record.kind === 'loss' && record.data.reason === 'invalid_call_id'
    ))).toBeDefined();
  });

  it('quarantines a reused manual call ID without changing either tool', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-manual-duplicate-ts-')),
      serviceName: 'custom-test',
    });
    let releaseFirst!: (value: string) => void;
    let releaseSecond!: (value: string) => void;
    let executions = 0;

    await capture.observe('manual-agent', {}, async (run) => {
      const first = run.tool('first', {}, () => {
        executions += 1;
        return new Promise<string>((resolve) => { releaseFirst = resolve; });
      }, { callId: 'reused-call' });
      const second = run.tool('second', {}, () => {
        executions += 1;
        return new Promise<string>((resolve) => { releaseSecond = resolve; });
      }, { callId: 'reused-call' });
      releaseSecond('second-result');
      await expect(second).resolves.toBe('second-result');
      releaseFirst('first-result');
      await expect(first).resolves.toBe('first-result');
    });

    const records = await sealedRecords(await capture.shutdown());
    const calls = records.filter((record) => record.kind === 'tool.call');
    const results = records.filter((record) => record.kind === 'tool.result');
    expect(executions).toBe(2);
    expect(calls).toHaveLength(2);
    expect(results).toHaveLength(2);
    expect(calls.filter((record) => record.data.native_call_id === 'reused-call'))
      .toHaveLength(1);
    expect(new Set(calls.map((record) => record.data.call_id)).size).toBe(2);
    for (const result of results) {
      const linked = result.links?.find((link) => link.type === 'result_of')?.record;
      expect(calls.some((call) => call.id === linked)).toBe(true);
    }
    expect(records.filter((record) => (
      record.kind === 'loss' && record.data.reason === 'duplicate_call_id'
    ))).toHaveLength(1);
  });

  it('never guesses terminals after custom callback collisions or replay', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-collision-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });

    bridge.record({ type: 'model.request', runId: 'run-a', callId: 'model-collision' });
    bridge.record({ type: 'model.request', runId: 'run-a', callId: 'model-collision' });
    bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'model-collision',
      status: 'completed',
      content: null,
    });

    for (let index = 0; index < 2; index += 1) {
      bridge.record({
        type: 'tool.call',
        runId: 'run-a',
        callId: 'tool-collision',
        name: `lookup-${index}`,
        input: { index },
      });
    }
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'tool-collision',
      status: 'succeeded',
      output: 'ambiguous',
    });

    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'tool-replay',
      name: 'lookup',
      input: null,
    });
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'tool-replay',
      status: 'succeeded',
      output: 'first',
    });
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'tool-replay',
      name: 'lookup',
      input: null,
    });
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'tool-replay',
      status: 'succeeded',
      output: 'replayed',
    });
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'completed' });

    const records = await sealedRecords(await capture.shutdown());
    expect(records.filter((record) => record.kind === 'model.request')).toHaveLength(1);
    expect(records.filter((record) => record.kind === 'model.response')).toHaveLength(0);
    expect(records.filter((record) => record.kind === 'tool.call')).toHaveLength(2);
    expect(records.filter((record) => record.kind === 'tool.result')).toHaveLength(1);
    expect(records.filter((record) => record.kind === 'loss').map((record) => (
      record.data.reason
    ))).toEqual([
      'duplicate_model_request',
      'ambiguous_model_response',
      'duplicate_tool_call',
      'ambiguous_tool_result',
      'duplicate_tool_call',
      'duplicate_tool_result',
    ]);
  });

  it('gaps malformed custom tool callbacks without poisoning corrected callbacks', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-malformed-tool-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'bad-name',
      name: '',
      input: null,
    });
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'corrected',
      name: 'lookup',
    } as unknown as CustomAgentEvent);
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'corrected',
      name: 'lookup',
      input: null,
    });
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'corrected',
      status: 'succeeded',
      output: null,
    });
    bridge.record({
      type: 'message',
      runId: 'run-a',
      role: 'user',
      content: 'missing ID',
    } as unknown as CustomAgentEvent);
    bridge.record({
      type: 'model.request',
      runId: 'run-a',
      callId: 'malformed-model',
      messageIds: 42,
      tools: 42,
    } as unknown as CustomAgentEvent);
    bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'malformed-model',
      status: 'completed',
      content: null,
    });
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'x'.repeat(257),
      name: 'lookup',
      input: null,
    });
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'completed' });

    const records = await sealedRecords(await capture.shutdown());
    expect(records.filter((record) => record.kind === 'tool.call')).toHaveLength(1);
    expect(records.filter((record) => record.kind === 'tool.result')).toHaveLength(1);
    expect(records.filter((record) => record.kind === 'loss').map((record) => (
      record.data.reason
    ))).toEqual([
      'invalid_tool_name',
      'tool_input_not_captured',
      'invalid_message_id',
      'invalid_message_ids',
      'invalid_tools',
      'invalid_call_id',
    ]);
  });

  it('preserves structured model errors and makes missing terminals explicit', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-loss-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });
    bridge.record({ type: 'model.request', runId: 'run-a', callId: 'model-1' });
    bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'model-1',
      status: 'failed',
      error: {
        type: 'provider_error',
        message: 'provider rejected the request',
        recoverable: true,
      },
    });
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'tool-1',
      name: 'read_file',
      input: { path: 'README.md' },
    });
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'failed' });

    const records = await sealedRecords(await capture.shutdown());
    expect(records.find((record) => record.kind === 'error')?.data).toMatchObject({
      type: 'provider_error',
      message: 'provider rejected the request',
      recoverable: true,
    });
    expect(records.filter((record) => record.kind === 'loss')).toHaveLength(1);
    expect(records.find((record) => record.kind === 'loss')?.data)
      .toMatchObject({ reason: 'tool_call_without_result' });
  });

  it('turns unknown callbacks and invalid terminal statuses into gaps', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-boundary-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });

    bridge.record({ type: 'not.a.real.event', runId: 'run-a' } as unknown as CustomAgentEvent);
    bridge.record({ type: 'model.request', runId: 'run-a', callId: 'model-1' });
    bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'model-1',
      status: 'not-a-status',
    } as unknown as CustomAgentEvent);
    bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'model-1',
      status: 'cancelled',
    });
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'tool-1',
      name: 'lookup',
      input: {},
    });
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'tool-1',
      status: 'not-a-status',
    } as unknown as CustomAgentEvent);
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'tool-1',
      status: 'cancelled',
    });
    bridge.record({
      type: 'run.outcome',
      runId: 'run-a',
      status: 'not-a-status',
    } as unknown as CustomAgentEvent);
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'completed' });

    const records = await sealedRecords(await capture.shutdown());
    const reasons = records
      .filter((record) => record.kind === 'loss')
      .map((record) => record.data.reason);
    expect(reasons).toEqual([
      'unknown_event_type',
      'invalid_status',
      'invalid_status',
      'invalid_status',
    ]);
    expect(records.filter((record) => record.kind === 'tool.result')).toHaveLength(1);
  });

  it('marks missing model and tool evidence while treating null as observed', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-content-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });

    for (const [callId, status, content] of [
      ['model-completed', 'completed', undefined],
      ['model-incomplete', 'incomplete', undefined],
      ['model-null', 'completed', null],
    ] as const) {
      bridge.record({ type: 'model.request', runId: 'run-a', callId });
      bridge.record({
        type: 'model.response',
        runId: 'run-a',
        callId,
        status,
        ...(content === undefined ? {} : { content }),
      });
    }
    for (const [callId, output] of [
      ['tool-missing', undefined],
      ['tool-null', null],
    ] as const) {
      bridge.record({
        type: 'tool.call',
        runId: 'run-a',
        callId,
        name: 'lookup',
        input: {},
      });
      bridge.record({
        type: 'tool.result',
        runId: 'run-a',
        callId,
        status: 'succeeded',
        ...(output === undefined ? {} : { output }),
      });
    }
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'completed' });

    const records = await sealedRecords(await capture.shutdown());
    const reasons = records
      .filter((record) => record.kind === 'loss')
      .map((record) => record.data.reason);
    expect(reasons).toEqual([
      'model_content_not_captured',
      'model_content_not_captured',
      'tool_output_not_captured',
    ]);
    expect(records.find((record) => (
      record.kind === 'model.response' && record.data.content === null
    ))).toBeDefined();
    expect(records.find((record) => (
      record.kind === 'tool.result' && record.data.output === null
    ))).toBeDefined();
  });

  it('keeps valid reasoning and omits invalid reasoning with an explicit gap', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-reasoning-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });
    bridge.record({ type: 'model.request', runId: 'run-a', callId: 'model-valid' });
    bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'model-valid',
      status: 'completed',
      content: 'answer',
      reasoning: [{ type: 'summary', text: 'checked the file' }],
    });
    bridge.record({ type: 'model.request', runId: 'run-a', callId: 'model-invalid' });
    bridge.record({
      type: 'model.response',
      runId: 'run-a',
      callId: 'model-invalid',
      status: 'completed',
      content: 'answer',
      reasoning: [{ type: 'private-thought', text: 42 }],
    } as unknown as CustomAgentEvent);
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'completed' });

    const records = await sealedRecords(await capture.shutdown());
    const responses = records.filter((record) => record.kind === 'model.response');
    expect(responses[0]?.data.reasoning).toEqual([
      { type: 'summary', text: 'checked the file' },
    ]);
    expect(responses[1]?.data.reasoning).toBeUndefined();
    expect(records.find((record) => (
      record.kind === 'loss' && record.data.reason === 'invalid_reasoning'
    ))).toBeDefined();
  });

  it('keeps messages with invalid optional call IDs and rejects errors on cancellation', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-message-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });
    bridge.record({
      type: 'message',
      runId: 'run-a',
      messageId: 'message-1',
      role: 'tool',
      content: 'done',
      callId: '\u0000',
    });
    bridge.record({
      type: 'tool.call',
      runId: 'run-a',
      callId: 'tool-1',
      name: 'lookup',
      input: {},
    });
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'tool-1',
      status: 'cancelled',
      error: {
        type: 'cancelled',
        message: 'cancelled',
        recoverable: true,
      },
    });
    bridge.record({
      type: 'tool.result',
      runId: 'run-a',
      callId: 'tool-1',
      status: 'cancelled',
    });
    bridge.record({ type: 'run.outcome', runId: 'run-a', status: 'completed' });

    const records = await sealedRecords(await capture.shutdown());
    const message = records.find((record) => record.kind === 'message');
    expect(message).toBeDefined();
    expect(message?.data.call_id).toBeUndefined();
    expect(records.filter((record) => record.kind === 'loss').map((record) => (
      record.data.reason
    ))).toEqual(['invalid_call_id', 'contradictory_terminal_error']);
  });

  it('records an unknown outcome when capture stops before the run terminal', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-custom-deactivate-ts-')),
      serviceName: 'custom-test',
    });
    const bridge = createCustomAgentSource({ name: 'fixture-agent' });
    capture.installSource(bridge.source);
    bridge.record({ type: 'run.start', runId: 'run-a', name: 'agent' });

    const records = await sealedRecords(await capture.shutdown());
    expect(records.find((record) => (
      record.kind === 'loss' && record.data.reason === 'run_terminal_not_observed'
    ))).toBeDefined();
    expect(records.find((record) => record.kind === 'run.outcome')?.data.status)
      .toBe('unknown');
  });

  it('reads manual call ID options once and never blocks tool execution', async () => {
    const capture = initialize({
      output: await mkdtemp(join(tmpdir(), 'semantic-manual-options-ts-')),
      serviceName: 'custom-test',
    });
    let statefulReads = 0;
    let throwingReads = 0;
    let executions = 0;
    const stateful = Object.defineProperty({}, 'callId', {
      get() {
        statefulReads += 1;
        return statefulReads === 1 ? 'native-once' : '\u0000';
      },
    });
    const throwing = Object.defineProperty({}, 'callId', {
      get() {
        throwingReads += 1;
        throw new Error('getter failed');
      },
    });

    await capture.observe('manual-agent', {}, async (run) => {
      await run.tool('stateful', {}, () => {
        executions += 1;
        return 'stateful-result';
      }, stateful);
      await run.tool('throwing', {}, () => {
        executions += 1;
        return 'throwing-result';
      }, throwing);
    });

    const records = await sealedRecords(await capture.shutdown());
    expect({ statefulReads, throwingReads, executions }).toEqual({
      statefulReads: 1,
      throwingReads: 1,
      executions: 2,
    });
    expect(records.find((record) => (
      record.kind === 'tool.call' && record.data.native_call_id === 'native-once'
    ))).toBeDefined();
    expect(records.find((record) => (
      record.kind === 'loss' && record.data.reason === 'invalid_call_id'
    ))).toBeDefined();
  });
});

type TraceRecord = {
  id: string;
  kind: string;
  parent?: string;
  links?: Array<{ type: string; record: string }>;
  data: Record<string, unknown>;
};

async function sealedRecords(status: { artifactPath: string }): Promise<TraceRecord[]> {
  const text = await readFile(join(status.artifactPath, 'trace.jsonl'), 'utf8');
  return text.trim().split('\n').map((line) => JSON.parse(line) as TraceRecord);
}
