import { describe, expect, it, vi } from 'vitest';
import {
  createPluginDefinition,
  type PluginDependencies,
} from '../src/plugin.js';
import type { CaptureSource, SourceSink } from '../src/contracts.js';

type Handler = (
  event: Record<string, unknown>,
  context: Record<string, unknown>,
) => unknown;
type GatewayHandler = (input: {
  params: Record<string, unknown>;
  respond(ok: boolean, payload?: unknown): void;
}) => unknown;

describe('OpenClaw run capture', () => {
  it('persists exact qualification for both exercised hosts', async () => {
    const cases = [
      ['2026.5.5', 'exact_qualified'],
      ['2026.7.1-2', 'exact_qualified'],
      ['2026.8.0', 'unknown'],
    ] as const;

    for (const [hostVersion, status] of cases) {
      const harness = captureHarness();
      const { handlers } = registerHarness(harness.dependencies, hostVersion);
      const context = {
        runId: `run-${hostVersion}`,
        sessionId: `session-${hostVersion}`,
      };
      handlers.before_model_resolve!(
        { runId: context.runId, prompt: 'start' },
        context,
      );
      await handlers.agent_end!(
        { runId: context.runId, success: true },
        context,
      );

      expect(harness.sourceMetadata).toContainEqual(
        expect.objectContaining({
          name: 'openclaw',
          version: hostVersion,
          qualification: { status, profile: 'openclaw.plugin.typed-hooks' },
        }),
      );
    }
  });

  it('persists unknown qualification and a named gap when a required hook registration fails', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(
      harness.dependencies,
      '2026.7.1-2',
      'message_sent',
    );
    const context = {
      runId: 'run-unavailable-hook',
      sessionId: 'session-unavailable-hook',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(harness.sourceMetadata).toContainEqual(
      expect.objectContaining({
        version: '2026.7.1-2',
        qualification: {
          status: 'unknown',
          profile: 'openclaw.plugin.typed-hooks',
        },
      }),
    );
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        phase: 'gap',
        native: expect.objectContaining({
          type: 'openclaw.hook.unavailable',
          hook: 'message_sent',
        }),
      }),
    );
  });

  it('creates one independent capture per run and queues sealed artifacts without waiting for network', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);

    handlers.before_model_resolve!(
      {
        runId: 'run-a',
        prompt: 'first',
        messages: [],
        systemPrompt: 'system',
      },
      { runId: 'run-a', sessionId: 'session-shared' },
    );
    handlers.before_model_resolve!(
      {
        runId: 'run-b',
        prompt: 'second',
        messages: [],
        systemPrompt: 'system',
      },
      { runId: 'run-b', sessionId: 'session-shared' },
    );

    await Promise.all([
      handlers.agent_end!(
        { runId: 'run-a', success: true, messages: [] },
        {
          runId: 'run-a',
          sessionId: 'session-shared',
        },
      ),
      handlers.agent_end!(
        { runId: 'run-b', success: true, messages: [] },
        {
          runId: 'run-b',
          sessionId: 'session-shared',
        },
      ),
    ]);

    expect(harness.captures).toHaveLength(2);
    expect(harness.opened.map((record) => record.turnId)).toEqual([
      'run-a',
      'run-b',
    ]);
    expect(harness.opened[0]?.conversationId).toBe(
      harness.opened[1]?.conversationId,
    );
    expect(harness.opened[0]?.conversationId).not.toContain('session-shared');
    expect(harness.enqueued.sort()).toEqual(['/sealed/run-a', '/sealed/run-b']);
    expect(
      harness.captureOptions.every((options) => options.identityMode === 'raw'),
    ).toBe(true);
    expect(
      harness.captureOptions.every(
        (options) =>
          options.installationId === 'install_0123456789abcdef0123456789abcdef',
      ),
    ).toBe(true);
    expect(harness.uploaderOptions).toEqual([
      expect.objectContaining({
        endpoint: 'https://ingest.example.test',
        installationId: 'install_0123456789abcdef0123456789abcdef',
      }),
    ]);
    expect(JSON.stringify(harness.records)).not.toContain(
      'identity-secret-value',
    );
    expect(JSON.stringify(harness.records)).not.toContain(
      'ingest-secret-value',
    );
  });

  it('consumes an admin-bound research task ID when the matching run starts', async () => {
    const harness = captureHarness();
    const { handlers, bindCorrelation } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-correlated',
      sessionId: 'session-correlated',
    };

    expect(bindCorrelation({
      runId: context.runId,
      taskId: 'research-correlated',
    })).toEqual({ accepted: true });
    expect(bindCorrelation({
      runId: context.runId,
      taskId: 'research-correlated',
    })).toEqual({ accepted: true });
    expect(bindCorrelation({
      runId: context.runId,
      taskId: 'conflicting-research',
    })).toEqual({ accepted: false, reason: 'conflict' });

    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    expect(bindCorrelation({
      runId: context.runId,
      taskId: 'research-correlated',
    })).toEqual({ accepted: false, reason: 'run_active' });
    await handlers.agent_end!({ runId: context.runId, success: true }, context);
    expect(bindCorrelation({
      runId: context.runId,
      taskId: 'research-correlated',
    })).toEqual({ accepted: false, reason: 'run_consumed' });

    expect(harness.captureOptions).toContainEqual(expect.objectContaining({
      identityKey: 'identity-secret-value-which-is-long-enough',
    }));
    expect(harness.opened).toContainEqual(expect.objectContaining({
      correlation: {
        taskId: 'research-correlated',
        execution: {
          system: 'openclaw',
          runId: context.runId,
          rootRunId: context.runId,
        },
      },
    }));
    expect(bindCorrelation({ runId: '', taskId: 'research' }))
      .toEqual({ accepted: false, reason: 'invalid_request' });

    for (let index = 0; index < 1_024; index += 1) {
      expect(bindCorrelation({
        runId: `pending-${index}`,
        taskId: `research-${index}`,
      })).toEqual({ accepted: true });
    }
    expect(bindCorrelation({ runId: 'pending-over-capacity', taskId: 'research' }))
      .toEqual({ accepted: false, reason: 'capacity_reached' });
  });

  it('does not attach an expired in-memory correlation binding', () => {
    vi.useFakeTimers();
    try {
      vi.setSystemTime(new Date('2026-08-12T12:00:00Z'));
      const harness = captureHarness();
      const { handlers, bindCorrelation } = registerHarness(harness.dependencies);
      expect(bindCorrelation({ runId: 'run-expired', taskId: 'research-expired' }))
        .toEqual({ accepted: true });

      vi.setSystemTime(new Date('2026-08-12T12:05:01Z'));
      handlers.before_model_resolve!(
        { runId: 'run-expired', prompt: 'start' },
        { runId: 'run-expired', sessionId: 'session-expired' },
      );

      expect(harness.opened[0]).not.toHaveProperty('correlation');
    } finally {
      vi.useRealTimers();
    }
  });

  it('keeps local capture and sealing active when the durable spool is full', async () => {
    const harness = captureHarness({
      pressure: 'full',
      enqueueState: 'awaiting_spool_admission',
    });
    const { handlers, errors } = registerHarness(harness.dependencies);
    const context = { runId: 'run-full', sessionId: 'session-full' };

    expect(() =>
      handlers.before_model_resolve!({ prompt: 'still runs' }, context),
    ).not.toThrow();
    await expect(
      handlers.agent_end!(
        {
          runId: context.runId,
          success: true,
          messages: [{ role: 'assistant', content: 'done' }],
        },
        context,
      ),
    ).resolves.toBeUndefined();

    expect(harness.captures).toHaveLength(1);
    expect(harness.shutdowns).toEqual(['/sealed/run-a']);
    expect(harness.enqueued).toEqual(['/sealed/run-a']);
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'lifecycle',
        phase: 'end',
        name: 'openclaw.agent.run',
      }),
    );
    expect(errors.join('\n')).toContain(
      'local run capture continues and sealed bundles remain available',
    );
    expect(errors.join('\n')).toContain('awaiting local spool admission');
  });

  it('retains a sealed local capture and agent outcome when cloud staging fails', async () => {
    const harness = captureHarness({
      enqueueError: new Error('fixture staging failure'),
    });
    const { handlers, errors } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-staging-failure',
      sessionId: 'session-staging-failure',
    };

    handlers.before_model_resolve!({ prompt: 'still runs' }, context);
    await expect(
      handlers.agent_end!(
        { runId: context.runId, success: true },
        context,
      ),
    ).resolves.toBeUndefined();

    expect(harness.shutdowns).toEqual(['/sealed/run-a']);
    expect(harness.enqueued).toEqual(['/sealed/run-a']);
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'lifecycle',
        phase: 'end',
        name: 'openclaw.agent.run',
      }),
    );
    expect(errors.join('\n')).toContain(
      'the local artifact was retained and the agent was unaffected',
    );
    expect(errors.join('\n')).toContain('fixture staging failure');
  });

  it('always records a bounded terminal outcome when ordinary hook evidence exceeds queue capacity', async () => {
    const harness = captureHarness();
    const handlers: Partial<Record<string, Handler>> = {};
    const plugin = createPluginDefinition(harness.dependencies, {
      terminalGraceMs: 0,
      queueCapacityBytes: 1,
    });
    plugin.register({
      pluginConfig: {
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-secret-value',
        identityKey: 'identity-secret-value-which-is-long-enough',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'fixture',
      },
      on(name, handler) {
        handlers[name] = handler as Handler;
      },
      logger: { debug() {}, info() {}, warn() {}, error() {} },
      runtime: { version: '2026.5.5' },
    });

    const context = { runId: 'run-pressure', sessionId: 'session-pressure' };
    handlers.before_model_resolve!(
      { runId: 'run-pressure', prompt: 'start' },
      context,
    );
    handlers.before_prompt_build!(
      {
        runId: 'run-pressure',
        messages: [
          { role: 'user', content: 'ordinary evidence over capacity' },
        ],
      },
      context,
    );
    await handlers.agent_end!(
      {
        runId: 'run-pressure',
        success: true,
        messages: [{ role: 'assistant', content: 'done' }],
      },
      context,
    );

    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'lifecycle',
        phase: 'end',
        name: 'openclaw.agent.run',
      }),
    );
    expect(harness.enqueued).toEqual(['/sealed/run-a']);
  });

  it('bounds cyclic and huge native values before queue accounting and records the evidence loss', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const huge: Record<string, unknown> = {
      text: 'x'.repeat(400_000),
    };
    huge.self = huge;
    for (let index = 0; index < 700; index += 1) {
      huge[`field_${index}`] = `value_${index}`;
    }
    const context = { runId: 'run-bounded', sessionId: 'session-bounded' };

    handlers.before_model_resolve!(
      { runId: 'run-bounded', prompt: 'start' },
      context,
    );
    expect(() =>
      handlers.before_prompt_build!(
        {
          runId: 'run-bounded',
          messages: huge,
        },
        context,
      ),
    ).not.toThrow();
    await handlers.agent_end!({ runId: 'run-bounded', success: true }, context);

    const serialized = JSON.stringify(harness.records);
    expect(Buffer.byteLength(serialized, 'utf8')).toBeLessThan(2 * 1024 * 1024);
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        phase: 'gap',
        native: expect.objectContaining({
          reason: 'unsupported_native_value',
          type: 'openclaw.snapshot_limit',
          limits: expect.arrayContaining(['cycle', 'string', 'width']),
        }),
      }),
    );
  });

  it('captures an exactly identified response once across the 2026.5.5 message-write and terminal seams', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-reasoning',
      sessionId: 'session-reasoning',
      sessionKey: 'agent:main:reasoning',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          responseId: 'response-exposed-once',
          content: [
            {
              type: 'thinking',
              thinking: 'EXPOSED_ONCE',
              thinkingSignature: 'PRIVATE_SIGNATURE',
            },
          ],
        },
      },
      { sessionKey: context.sessionKey },
    );
    await handlers.agent_end!(
      {
        runId: context.runId,
        success: true,
        messages: [
          {
            role: 'assistant',
            responseId: 'response-exposed-once',
            content: [{ type: 'thinking', thinking: 'EXPOSED_ONCE' }],
          },
        ],
      },
      context,
    );

    const serialized = JSON.stringify(harness.records);
    expect(serialized.match(/EXPOSED_ONCE/gu)).toHaveLength(1);
    expect(serialized).toContain('PRIVATE_SIGNATURE');
    const assistant = harness.records.find(
      (record) => record.name === 'openclaw.assistant.message',
    );
    expect(
      JSON.stringify(
        (assistant?.semantic as Record<string, Record<string, unknown>>)?.value
          ?.reasoning,
      ),
    ).not.toContain('PRIVATE_SIGNATURE');
    expect(
      harness.records.filter(
        (record) =>
          record.semantic &&
          (record.semantic as Record<string, unknown>).state_type ===
            'openclaw.assistant_message',
      ),
    ).toHaveLength(1);
  });

  it('does not guess that idless live and terminal reasoning belong to the same response', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-idless-reasoning',
      sessionId: 'session-idless-reasoning',
      sessionKey: 'agent:main:idless-reasoning',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          content: [{ type: 'thinking', thinking: 'IDLESS_REASONING' }],
        },
      },
      context,
    );
    await handlers.agent_end!(
      {
        runId: context.runId,
        success: true,
        messages: [
          {
            role: 'assistant',
            content: [{ type: 'thinking', thinking: 'IDLESS_REASONING' }],
          },
        ],
      },
      context,
    );

    expect(JSON.stringify(harness.records).match(/IDLESS_REASONING/gu)).toHaveLength(
      2,
    );
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'state',
        name: 'openclaw.agent.final_reasoning',
        parentRecordId: 'root-0',
      }),
    );
  });

  it('omits cumulative terminal messages while retaining per-message evidence and the terminal outcome', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-terminal-history',
      sessionId: 'session-terminal-history',
      sessionKey: 'agent:main:terminal-history',
    };
    handlers.before_model_resolve!({ prompt: 'start' }, context);
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          content: [{ type: 'text', text: 'UNIQUE_PER_MESSAGE_EVIDENCE' }],
        },
      },
      { sessionKey: context.sessionKey },
    );
    await handlers.agent_end!(
      {
        runId: context.runId,
        success: true,
        durationMs: 42,
        messages: [
          { role: 'user', content: 'CUMULATIVE_TERMINAL_HISTORY' },
          { role: 'assistant', content: 'UNIQUE_PER_MESSAGE_EVIDENCE' },
        ],
      },
      context,
    );

    const serialized = JSON.stringify(harness.records);
    expect(serialized).toContain('UNIQUE_PER_MESSAGE_EVIDENCE');
    expect(serialized).not.toContain('CUMULATIVE_TERMINAL_HISTORY');
    expect(serialized).not.toContain('openclaw.agent.final_messages');
    expect(serialized).not.toContain('openclaw.final_messages');
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'lifecycle',
        phase: 'end',
        name: 'openclaw.agent.run',
        native: expect.objectContaining({ success: true, durationMs: 42 }),
        semantic: expect.objectContaining({
          type: 'agent.run',
          status: 'completed',
        }),
      }),
    );
  });

  it('keeps visible assistant content and normalized usage when no reasoning is exposed', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-visible',
      sessionId: 'session-visible',
      sessionKey: 'agent:main:visible',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          content: [{ type: 'text', text: 'VISIBLE_ANSWER' }],
          usage: { input: 7, output: 9 },
        },
      },
      { sessionKey: context.sessionKey },
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(harness.records).toContainEqual(
      expect.objectContaining({
        semantic: {
          type: 'state.transition',
          state_type: 'openclaw.assistant_message',
          value: {
            content: [{ type: 'text', text: 'VISIBLE_ANSWER' }],
            usage: { input_tokens: 7, output_tokens: 9 },
          },
        },
      }),
    );
  });

  it('does not assign assistant reasoning when a session has multiple active run owners', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const sessionKey = 'agent:main:shared';
    for (const runId of ['run-one', 'run-two']) {
      handlers.before_model_resolve!(
        { runId, prompt: 'start' },
        {
          runId,
          sessionId: 'session-shared',
          sessionKey,
        },
      );
    }
    handlers.before_message_write!(
      {
        sessionKey,
        message: {
          role: 'assistant',
          content: [{ type: 'thinking', thinking: 'AMBIGUOUS_REASONING' }],
        },
      },
      { sessionKey },
    );
    await Promise.all(
      ['run-one', 'run-two'].map((runId) =>
        handlers.agent_end!(
          { runId, success: true },
          {
            runId,
            sessionId: 'session-shared',
            sessionKey,
          },
        ),
      ),
    );

    expect(JSON.stringify(harness.records)).not.toContain(
      'AMBIGUOUS_REASONING',
    );
    expect(
      harness.records.filter(
        (record) =>
          record.kind === 'unknown' &&
          (record.native as Record<string, unknown>).type ===
            'openclaw.assistant_message_session_correlation',
      ),
    ).toHaveLength(2);
  });

  it('places exposed reasoning and usage on the response selected by an exact call ID', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = { runId: 'run-exact', sessionId: 'session-exact' };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'call-exact', model: 'model' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'call-other', model: 'model' },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        callId: 'call-exact',
        assistantTexts: ['answer'],
        usage: { input: 11, output: 22 },
        lastAssistant: {
          content: [{ type: 'thinking', thinking: 'EXACT_REASONING' }],
        },
      },
      context,
    );
    handlers.model_call_ended!(
      { runId: context.runId, callId: 'call-exact', outcome: 'completed' },
      context,
    );
    handlers.model_call_ended!(
      { runId: context.runId, callId: 'call-other', outcome: 'completed' },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'model',
        phase: 'end',
        semantic: expect.objectContaining({
          type: 'model.response',
          reasoning: [{ type: 'text', text: 'EXACT_REASONING' }],
          usage: { input_tokens: 11, output_tokens: 22 },
        }),
      }),
    );
  });

  it('preserves exact response correlation when model_call_ended arrives before rich output', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-late-output',
      sessionId: 'session-late-output',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'call-late-output' },
      context,
    );
    handlers.model_call_ended!(
      {
        runId: context.runId,
        callId: 'call-late-output',
        outcome: 'completed',
        finishReason: 'stop',
      },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        callId: 'call-late-output',
        responseId: 'response-late-output',
        assistantTexts: ['answer'],
        usage: { input: 13, output: 21 },
        lastAssistant: {
          content: [{ type: 'thinking', thinking: 'LATE_EXACT_REASONING' }],
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    const responses = harness.records.filter(
      (record) =>
        record.kind === 'model' &&
        (record.semantic as Record<string, unknown>).type === 'model.response',
    );
    expect(responses).toHaveLength(1);
    expect(responses[0]).toEqual(
      expect.objectContaining({
        parentRecordId: expect.any(String),
        native: expect.objectContaining({
          modelCallEnded: expect.objectContaining({
            callId: 'call-late-output',
            finishReason: 'stop',
          }),
        }),
        semantic: expect.objectContaining({
          call_id: 'call-late-output',
          finish_reason: 'stop',
          reasoning: [{ type: 'text', text: 'LATE_EXACT_REASONING' }],
          usage: { input_tokens: 13, output_tokens: 21 },
        }),
      }),
    );
  });

  it('never lets reasoning from one assistant response suppress a later model response', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-multiple-responses',
      sessionId: 'session-multiple-responses',
      sessionKey: 'agent:main:multiple-responses',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          responseId: 'response-one',
          content: [{ type: 'thinking', thinking: 'FIRST_RESPONSE_REASONING' }],
        },
      },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'call-two' },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        callId: 'call-two',
        responseId: 'response-two',
        lastAssistant: {
          responseId: 'response-two',
          content: [
            { type: 'thinking', thinking: 'SECOND_RESPONSE_REASONING' },
          ],
        },
      },
      context,
    );
    handlers.model_call_ended!(
      { runId: context.runId, callId: 'call-two', outcome: 'completed' },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    const serialized = JSON.stringify(harness.records);
    expect(serialized).toContain('FIRST_RESPONSE_REASONING');
    expect(serialized).toContain('SECOND_RESPONSE_REASONING');
  });

  it('keeps rich output unlinked when one open request exists but no exact call ID is exposed', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = { runId: 'run-unlinked', sessionId: 'session-unlinked' };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'only-open-call' },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        lastAssistant: {
          content: [{ type: 'thinking', thinking: 'UNLINKED_REASONING' }],
        },
        usage: { input: 3, output: 5 },
      },
      context,
    );
    handlers.model_call_ended!(
      { runId: context.runId, callId: 'only-open-call', outcome: 'completed' },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'state',
        name: 'openclaw.llm.output',
        parentRecordId: 'root-0',
        semantic: expect.objectContaining({
          type: 'state.transition',
          value: expect.objectContaining({
            reasoning: [{ type: 'text', text: 'UNLINKED_REASONING' }],
            usage: { input_tokens: 3, output_tokens: 5 },
          }),
        }),
      }),
    );
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        native: expect.objectContaining({
          type: 'model.output_call_correlation',
        }),
      }),
    );
  });

  it('correlates message-write content, reasoning, and usage when the host exposes an exact call ID', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-message-exact',
      sessionId: 'session-message-exact',
      sessionKey: 'agent:main:message-exact',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'message-call' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'other-call' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          callId: 'message-call',
          responseId: 'provider-response',
          content: [
            { type: 'thinking', thinking: 'MESSAGE_EXACT_REASONING' },
            { type: 'text', text: 'MESSAGE_EXACT_ANSWER' },
          ],
          usage: { input: 13, output: 21 },
        },
      },
      context,
    );
    for (const callId of ['message-call', 'other-call']) {
      handlers.model_call_ended!(
        { runId: context.runId, callId, outcome: 'completed' },
        context,
      );
    }
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'model',
        phase: 'end',
        nativeIdentity: 'message-call',
        parentRecordId: 'record-2',
        semantic: expect.objectContaining({
          type: 'model.response',
          call_id: 'message-call',
          response_id: 'provider-response',
          content: [{ type: 'text', text: 'MESSAGE_EXACT_ANSWER' }],
          reasoning: [{ type: 'text', text: 'MESSAGE_EXACT_REASONING' }],
          usage: { input_tokens: 13, output_tokens: 21 },
        }),
      }),
    );
  });

  it('preserves provider-exposed raw reasoning and summaries in order, including repetitions', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-reasoning-shapes',
      sessionId: 'session-reasoning-shapes',
      sessionKey: 'agent:main:reasoning-shapes',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          content: [
            { type: 'thinking', thinking: 'RAW_A' },
            { type: 'summary', text: 'SUMMARY_A' },
            { type: 'thinking', thinking: 'RAW_A' },
            {
              type: 'reasoning',
              summary: [{ type: 'summary_text', text: 'OPENAI_SUMMARY' }],
            },
            { type: 'text', thought: true, text: 'GOOGLE_THOUGHT' },
          ],
          reasoning_details: [
            { type: 'reasoning.summary', summary: 'OPENROUTER_SUMMARY' },
            { type: 'reasoning.text', text: 'OPENROUTER_RAW' },
          ],
          reasoning_content: 'DEEPSEEK_RAW',
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    const assistant = harness.records.find(
      (record) =>
        (record.semantic as Record<string, unknown>)?.state_type ===
        'openclaw.assistant_message',
    );
    expect(
      (assistant?.semantic as Record<string, Record<string, unknown>>).value
        .reasoning,
    ).toEqual([
      { type: 'text', text: 'RAW_A' },
      { type: 'summary', text: 'SUMMARY_A' },
      { type: 'text', text: 'RAW_A' },
      { type: 'summary', text: 'OPENAI_SUMMARY' },
      { type: 'text', text: 'GOOGLE_THOUGHT' },
      { type: 'summary', text: 'OPENROUTER_SUMMARY' },
      { type: 'text', text: 'OPENROUTER_RAW' },
      { type: 'text', text: 'DEEPSEEK_RAW' },
    ]);
  });

  it('keeps opaque reasoning native, omits redacted readable text, and records bounded losses', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-opaque-reasoning',
      sessionId: 'session-opaque-reasoning',
      sessionKey: 'agent:main:opaque-reasoning',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          content: [
            { type: 'thinking', text: 'REDACTED_SECRET', redacted: true },
            { type: 'reasoning.encrypted', data: 'ENCRYPTED_SECRET' },
            { type: 'reasoning', summary: { invalid: true } },
            { type: 'thinking', thinking: 'SAFE_EXPOSED_REASONING' },
          ],
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    const serialized = JSON.stringify(harness.records);
    expect(serialized).toContain('SAFE_EXPOSED_REASONING');
    expect(serialized).not.toContain('REDACTED_SECRET');
    expect(serialized).toContain('ENCRYPTED_SECRET');
    const assistant = harness.records.find(
      (record) => record.name === 'openclaw.assistant.message',
    );
    expect(JSON.stringify(assistant?.semantic)).not.toContain('ENCRYPTED_SECRET');
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        native: expect.objectContaining({
          type: 'openclaw.reasoning_redacted',
        }),
      }),
    );
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        native: expect.objectContaining({
          type: 'openclaw.reasoning_encrypted',
        }),
      }),
    );
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        native: expect.objectContaining({
          type: 'openclaw.reasoning_malformed',
        }),
      }),
    );
  });

  it('deduplicates only an exact repeated response callback while retaining repeated blocks inside it', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-delivery-dedup',
      sessionId: 'session-delivery-dedup',
      sessionKey: 'agent:main:delivery-dedup',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    const event = {
      sessionKey: context.sessionKey,
      message: {
        role: 'assistant',
        responseId: 'same-response-delivery',
        content: [
          { type: 'thinking', thinking: 'INTENTIONAL_REPEAT' },
          { type: 'thinking', thinking: 'INTENTIONAL_REPEAT' },
        ],
      },
    };
    handlers.before_message_write!(event, context);
    handlers.before_message_write!(event, context);
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(
      JSON.stringify(harness.records).match(/INTENTIONAL_REPEAT/gu),
    ).toHaveLength(2);
    expect(
      harness.records.filter(
        (record) => record.name === 'openclaw.assistant.message',
      ),
    ).toHaveLength(1);
  });

  it('deduplicates an exact repeated llm_output delivery without text-based guessing', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = { runId: 'run-llm-dedup', sessionId: 'session-llm-dedup' };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'dedup-call' },
      context,
    );
    const event = {
      runId: context.runId,
      callId: 'dedup-call',
      responseId: 'dedup-response',
      lastAssistant: {
        responseId: 'dedup-response',
        content: [{ type: 'thinking', thinking: 'LLM_DELIVERY_ONCE' }],
      },
    };
    handlers.llm_output!(event, context);
    handlers.llm_output!(event, context);
    handlers.model_call_ended!(
      { runId: context.runId, callId: 'dedup-call', outcome: 'completed' },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(
      JSON.stringify(harness.records).match(/LLM_DELIVERY_ONCE/gu),
    ).toHaveLength(1);
    expect(
      harness.records.filter((record) => record.name === 'openclaw.llm.output'),
    ).toHaveLength(1);
  });

  it.each(['2026.5.5', '2026.7.1-2'])(
    'deduplicates reasoning observed at both response seams on %s only with an exact shared response ID',
    async (hostVersion) => {
      const harness = captureHarness();
      const { handlers } = registerHarness(harness.dependencies, hostVersion);
      const context = {
        runId: 'run-cross-seam-dedup',
        sessionId: 'session-cross-seam-dedup',
        sessionKey: 'agent:main:cross-seam-dedup',
      };
      handlers.before_model_resolve!(
        { runId: context.runId, prompt: 'start' },
        context,
      );
      handlers.llm_output!(
        {
          runId: context.runId,
          responseId: 'shared-response',
          lastAssistant: {
            responseId: 'shared-response',
            content: [{ type: 'thinking', thinking: 'SHARED_REASONING' }],
          },
        },
        context,
      );
      handlers.before_message_write!(
        {
          sessionKey: context.sessionKey,
          message: {
            role: 'assistant',
            responseId: 'shared-response',
            content: [{ type: 'thinking', thinking: 'SHARED_REASONING' }],
          },
        },
        context,
      );
      await handlers.agent_end!(
        { runId: context.runId, success: true },
        context,
      );

      expect(
        JSON.stringify(harness.records).match(/SHARED_REASONING/gu),
      ).toHaveLength(1);
    },
  );

  it('retains identical reasoning from responses with different exact identities', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-distinct-response-identities',
      sessionId: 'session-distinct-response-identities',
      sessionKey: 'agent:main:distinct-response-identities',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        responseId: 'response-one',
        lastAssistant: {
          content: [{ type: 'thinking', thinking: 'SAME_READABLE_REASONING' }],
        },
      },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          responseId: 'response-two',
          content: [{ type: 'thinking', thinking: 'SAME_READABLE_REASONING' }],
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(
      JSON.stringify(harness.records).match(/SAME_READABLE_REASONING/gu),
    ).toHaveLength(2);
  });

  it('does not let a response observation without reasoning suppress later readable reasoning for that response', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-late-response-reasoning',
      sessionId: 'session-late-response-reasoning',
      sessionKey: 'agent:main:late-response-reasoning',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          responseId: 'late-reasoning-response',
          content: [{ type: 'text', text: 'answer' }],
        },
      },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        responseId: 'late-reasoning-response',
        lastAssistant: {
          content: [{ type: 'thinking', thinking: 'LATE_READABLE_REASONING' }],
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(JSON.stringify(harness.records)).toContain('LATE_READABLE_REASONING');
  });

  it('merges richer later reasoning for the same exact response without repeating shared blocks', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-richer-response-reasoning',
      sessionId: 'session-richer-response-reasoning',
      sessionKey: 'agent:main:richer-response-reasoning',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        responseId: 'richer-response',
        lastAssistant: {
          content: [{ type: 'thinking', thinking: 'SHARED_BLOCK' }],
        },
      },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          responseId: 'richer-response',
          content: [
            { type: 'thinking', thinking: 'SHARED_BLOCK' },
            { type: 'summary', text: 'LATER_RICHER_BLOCK' },
          ],
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    const serialized = JSON.stringify(harness.records);
    expect(serialized.match(/SHARED_BLOCK/gu)).toHaveLength(1);
    expect(serialized.match(/LATER_RICHER_BLOCK/gu)).toHaveLength(1);
  });

  it('adds only missing repeated occurrences from a richer later observation', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-richer-repeated-reasoning',
      sessionId: 'session-richer-repeated-reasoning',
      sessionKey: 'agent:main:richer-repeated-reasoning',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        responseId: 'repeated-response',
        lastAssistant: {
          content: [{ type: 'thinking', thinking: 'REPEATED_BLOCK' }],
        },
      },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          responseId: 'repeated-response',
          content: [
            { type: 'thinking', thinking: 'REPEATED_BLOCK' },
            { type: 'thinking', thinking: 'REPEATED_BLOCK' },
          ],
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(
      JSON.stringify(harness.records).match(/REPEATED_BLOCK/gu),
    ).toHaveLength(2);
  });

  it('captures agent-end reasoning as uncorrelated state only when earlier reasoning seams captured none', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-terminal-reasoning-fallback',
      sessionId: 'session-terminal-reasoning-fallback',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'terminal-model-call' },
      context,
    );
    handlers.model_call_ended!(
      {
        runId: context.runId,
        callId: 'terminal-model-call',
        outcome: 'completed',
      },
      context,
    );
    await handlers.agent_end!(
      {
        runId: context.runId,
        success: true,
        messages: [
          {
            role: 'assistant',
            content: [{ type: 'thinking', thinking: 'TERMINAL_FALLBACK' }],
          },
        ],
      },
      context,
    );

    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'state',
        name: 'openclaw.agent.final_reasoning',
        parentRecordId: 'root-0',
        semantic: {
          type: 'state.transition',
          state_type: 'openclaw.agent_end_reasoning',
          value: {
            reasoning: [{ type: 'text', text: 'TERMINAL_FALLBACK' }],
          },
        },
      }),
    );
    expect(
      harness.records.some(
        (record) =>
          record.kind === 'model' &&
          JSON.stringify(record.semantic).includes('TERMINAL_FALLBACK'),
      ),
    ).toBe(false);
  });

  it('captures a later terminal-only response after an earlier response exposed reasoning', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-later-terminal-response',
      sessionId: 'session-later-terminal-response',
      sessionKey: 'agent:main:later-terminal-response',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          responseId: 'earlier-response',
          content: [{ type: 'thinking', thinking: 'EARLIER_REASONING' }],
        },
      },
      context,
    );
    await handlers.agent_end!(
      {
        runId: context.runId,
        success: true,
        messages: [
          {
            role: 'assistant',
            responseId: 'earlier-response',
            content: [
              { type: 'thinking', thinking: 'EARLIER_REASONING' },
              { type: 'summary', text: 'EARLIER_TERMINAL_DETAIL' },
            ],
          },
          {
            role: 'assistant',
            responseId: 'later-response',
            content: [{ type: 'thinking', thinking: 'LATER_REASONING' }],
          },
        ],
      },
      context,
    );

    const serialized = JSON.stringify(harness.records);
    expect(serialized.match(/EARLIER_REASONING/gu)).toHaveLength(1);
    expect(serialized.match(/EARLIER_TERMINAL_DETAIL/gu)).toHaveLength(1);
    expect(serialized.match(/LATER_REASONING/gu)).toHaveLength(1);
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'state',
        name: 'openclaw.agent.final_reasoning',
        parentRecordId: 'root-0',
        semantic: expect.objectContaining({
          value: expect.objectContaining({ response_id: 'later-response' }),
        }),
      }),
    );
  });

  it('keeps state and a correlation loss when message-write owns an exact response but omits callId', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies, '2026.7.1-2');
    const context = {
      runId: 'run-message-first',
      sessionId: 'session-message-first',
      sessionKey: 'agent:main:message-first',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.model_call_started!(
      { runId: context.runId, callId: 'open-model-call' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          responseId: 'message-first-response',
          content: [{ type: 'thinking', thinking: 'MESSAGE_FIRST_REASONING' }],
        },
      },
      context,
    );
    handlers.llm_output!(
      {
        runId: context.runId,
        responseId: 'message-first-response',
        lastAssistant: {
          responseId: 'message-first-response',
          content: [{ type: 'thinking', thinking: 'MESSAGE_FIRST_REASONING' }],
        },
      },
      context,
    );
    handlers.model_call_ended!(
      { runId: context.runId, callId: 'open-model-call', outcome: 'completed' },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'state',
        name: 'openclaw.assistant.message',
        parentRecordId: 'root-0',
      }),
    );
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        native: expect.objectContaining({
          type: 'model.output_call_correlation',
          hook: 'before_message_write',
        }),
      }),
    );
  });

  it('retains normalized opaque carriers as bounded opaque evidence with an explicit loss', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-opaque-carrier-keys',
      sessionId: 'session-opaque-carrier-keys',
      sessionKey: 'agent:main:opaque-carrier-keys',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          content: [
            {
              type: 'thinking',
              thinking: 'READABLE_WITH_OPAQUE_CARRIER',
              encryptedContent: 'OPAQUE_CARRIER_VALUE',
              thinkingSignature: 'OPAQUE_SIGNATURE_VALUE',
            },
          ],
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    const serialized = JSON.stringify(harness.records);
    expect(serialized).toContain('READABLE_WITH_OPAQUE_CARRIER');
    expect(serialized).toContain('OPAQUE_CARRIER_VALUE');
    expect(serialized).toContain('OPAQUE_SIGNATURE_VALUE');
    const assistant = harness.records.find(
      (record) => record.name === 'openclaw.assistant.message',
    );
    const projected = JSON.stringify(assistant?.semantic);
    expect(projected).not.toContain('OPAQUE_CARRIER_VALUE');
    expect(projected).not.toContain('OPAQUE_SIGNATURE_VALUE');
    expect(harness.records).toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        native: expect.objectContaining({
          type: 'openclaw.reasoning_encrypted',
        }),
      }),
    );
  });

  it('does not classify unrelated signature-shaped metadata as reasoning evidence', async () => {
    const harness = captureHarness();
    const { handlers } = registerHarness(harness.dependencies);
    const context = {
      runId: 'run-unrelated-signature',
      sessionId: 'session-unrelated-signature',
      sessionKey: 'agent:main:unrelated-signature',
    };
    handlers.before_model_resolve!(
      { runId: context.runId, prompt: 'start' },
      context,
    );
    handlers.before_message_write!(
      {
        sessionKey: context.sessionKey,
        message: {
          role: 'assistant',
          content: [
            {
              type: 'text',
              text: 'answer',
              signature: 'UNRELATED_TEXT_SIGNATURE',
            },
          ],
          authentication: {
            signature: 'UNRELATED_AUTH_SIGNATURE',
            encryptedContent: 'UNRELATED_AUTH_PAYLOAD',
          },
        },
      },
      context,
    );
    await handlers.agent_end!({ runId: context.runId, success: true }, context);

    const assistant = harness.records.find(
      (record) => record.name === 'openclaw.assistant.message',
    );
    expect(JSON.stringify(assistant?.native)).not.toContain(
      'UNRELATED_AUTH_SIGNATURE',
    );
    expect(JSON.stringify(assistant?.native)).not.toContain(
      'UNRELATED_AUTH_PAYLOAD',
    );
    expect(JSON.stringify(assistant?.native)).not.toContain(
      'UNRELATED_TEXT_SIGNATURE',
    );
    expect(harness.records).not.toContainEqual(
      expect.objectContaining({
        kind: 'unknown',
        native: expect.objectContaining({
          type: 'openclaw.reasoning_encrypted',
        }),
      }),
    );
  });
});

function registerHarness(
  dependencies: PluginDependencies,
  hostVersion = '2026.5.5',
  unavailableHook?: string,
) {
  const handlers: Partial<Record<string, Handler>> = {};
  const errors: string[] = [];
  let gatewayHandler: GatewayHandler | undefined;
  const plugin = createPluginDefinition(dependencies, { terminalGraceMs: 0 });
  plugin.register({
    pluginConfig: {
      endpoint: 'https://ingest.example.test',
      ingestKey: 'ingest-secret-value',
      identityKey: 'identity-secret-value-which-is-long-enough',
      installationId: 'install_0123456789abcdef0123456789abcdef',
      serviceName: 'fixture-openclaw',
    },
    on(name, handler) {
      if (name === unavailableHook) throw new Error('fixture unavailable hook');
      handlers[name] = handler as Handler;
    },
    registerGatewayMethod(name, handler, options) {
      expect(name).toBe('semantic-layer.correlation.bind');
      expect(options).toEqual({ scope: 'operator.admin' });
      gatewayHandler = handler as unknown as GatewayHandler;
    },
    logger: {
      debug() {},
      info() {},
      warn() {},
      error(message) {
        errors.push(message);
      },
    },
    runtime: { version: hostVersion },
  });
  return {
    handlers,
    errors,
    bindCorrelation(params: Record<string, unknown>): unknown {
      let payload: unknown;
      gatewayHandler?.({
        params,
        respond(_ok, value) { payload = value; },
      });
      return payload;
    },
  };
}

function captureHarness(
  behavior: {
    pressure?: 'ok' | 'full';
    enqueueState?: 'pending' | 'awaiting_spool_admission';
    enqueueError?: Error;
  } = {},
) {
  const captures: unknown[] = [];
  const captureOptions: Array<Record<string, unknown>> = [];
  const sourceMetadata: Array<Record<string, unknown>> = [];
  const opened: Array<Record<string, unknown>> = [];
  const records: Array<Record<string, unknown>> = [];
  const enqueued: string[] = [];
  const shutdowns: string[] = [];
  const uploaderOptions: Array<Record<string, unknown>> = [];
  let captureNumber = 0;
  const dependencies: PluginDependencies = {
    createRunCapture(options) {
      captureOptions.push(options);
      const number = captureNumber++;
      const artifactPath = number === 0 ? '/sealed/run-a' : '/sealed/run-b';
      const capture = {
        installSource(source: CaptureSource) {
          sourceMetadata.push(source.metadata as Record<string, unknown>);
          const sink: SourceSink = {
            openTrace(input) {
              opened.push(input);
              return accepted(`root-${number}`, {
                runId: String(input.nativeIdentity),
                traceId: `trace-${number}`,
              });
            },
            record(input) {
              records.push(input);
              return accepted(`record-${records.length}`);
            },
          };
          source.install(sink);
          return { deactivate() {}, drain() {} };
        },
        status: () => status(artifactPath),
        flush: async () => status(artifactPath),
        shutdown: async () => {
          shutdowns.push(artifactPath);
          return status(artifactPath);
        },
      };
      captures.push(capture);
      return capture;
    },
    createUploader(options) {
      uploaderOptions.push(options);
      return {
        enqueueArtifact: async (path: string) => {
          enqueued.push(path);
          if (behavior.enqueueError) throw behavior.enqueueError;
          return {
            bundleId: path,
            bundleDigest: 'digest',
            state: behavior.enqueueState ?? ('pending' as const),
          };
        },
        flush: async () => ({
          pendingBundles: 0,
          uploadedBundles: 0,
          timedOut: false,
        }),
        status: () => ({
          lifecycle: 'running',
          ...(behavior.pressure ? { pressure: behavior.pressure } : {}),
        }),
        shutdown: async () => {},
      };
    },
  };
  return {
    dependencies,
    captures,
    captureOptions,
    sourceMetadata,
    opened,
    records,
    enqueued,
    shutdowns,
    uploaderOptions,
  };
}

function accepted(
  recordId: string,
  identity?: { runId: string; traceId: string },
) {
  return {
    accepted: true as const,
    recordId,
    settled: Promise.resolve(),
    ...(identity ? { identity } : {}),
  };
}

function status(artifactPath: string) {
  return {
    state: 'closed' as const,
    artifactPath,
    admitted: 1,
    persisted: 1,
    rejected: 0,
    losses: {},
    lastError: null,
  };
}
