import { cp, lstat, mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createCapture, validateArtifact } from 'semantic-layer-capture';
import { describe, expect, it } from 'vitest';
import { createPluginDefinition } from '../src/plugin.js';

type Handler = (
  event: Record<string, unknown>,
  context: Record<string, unknown>,
) => unknown;
type GatewayHandler = (input: {
  params: Record<string, unknown>;
  respond(ok: boolean, payload?: unknown): void;
}) => unknown;

describe('persisted OpenClaw capture', () => {
  it('protects native inbound run correlation without a Gateway binding', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-layer-openclaw-inbound-'));
    const handlers: Partial<Record<string, Handler>> = {};
    const identityKey = 'identity-secret-value-which-is-long-enough';
    const runId = 'private-native-inbound-run-id';
    let artifactPath = '';
    try {
      const plugin = createPluginDefinition(
        {
          createRunCapture: createCapture,
          createUploader: () => ({
            async enqueueArtifact(path: string) {
              artifactPath = path;
              return { bundleId: 'bundle', bundleDigest: 'digest', state: 'pending' as const };
            },
            async flush() { return { timedOut: false, uploadedBundles: 0 }; },
            status() { return { lifecycle: 'running', pressure: 'ok' }; },
            async shutdown() {},
          }),
        },
        { terminalGraceMs: 0 },
      );
      plugin.register({
        pluginConfig: {
          endpoint: 'https://ingest.example.test',
          ingestKey: 'ingest-secret-value',
          identityKey,
          installationId: 'install_0123456789abcdef0123456789abcdef',
          serviceName: 'openclaw-native-inbound-correlation',
          outputDirectory: output,
        },
        on(name, handler) { handlers[name] = handler as Handler; },
        logger: { debug() {}, info() {}, warn() {}, error() {} },
        runtime: { version: '2026.5.5' },
      });
      const context = { runId, sessionId: 'private-native-inbound-session' };
      handlers.message_received!({
        from: 'trusted-inbound-sender',
        content: 'synthetic inbound message',
        sessionKey: 'agent:main:slack:dm:synthetic',
      }, {
        channelId: 'slack',
        sessionKey: 'agent:main:slack:dm:synthetic',
      });
      handlers.before_model_resolve!({ prompt: 'synthetic inbound message' }, context);
      await handlers.agent_end!({ runId, success: true }, context);

      const trace = await readFile(join(artifactPath, 'trace.jsonl'), 'utf8');
      const start = trace.trim().split('\n').map((line) => JSON.parse(line))
        .find((row) => row.kind === 'run.start');
      expect(start.data.correlation).toMatchObject({
        task_id: expect.stringMatching(/^task_[a-f0-9]{64}$/u),
        execution: {
          system: 'openclaw',
          run_id: expect.stringMatching(/^exec_[a-f0-9]{64}$/u),
          root_run_id: expect.stringMatching(/^exec_[a-f0-9]{64}$/u),
        },
      });
      expect(start.data.correlation.task_id).not.toContain(runId);
      expect(start.data.correlation.execution.run_id).not.toContain(runId);
      expect(start.data.turn_id).toContain(runId);
      const workerCapture = createCapture({
        output,
        serviceName: 'inbound-worker-correlation',
        identityKey,
      });
      await workerCapture.observe('worker-attempt', {
        correlation: {
          taskId: runId,
          execution: {
            system: 'job-runner',
            runId: 'worker-run-for-native-inbound',
          },
        },
      }, async () => 'complete');
      const workerClosed = await workerCapture.shutdown();
      const workerTrace = await readFile(
        join(workerClosed.artifactPath, 'trace.jsonl'),
        'utf8',
      );
      const workerStart = workerTrace.trim().split('\n')
        .map((line) => JSON.parse(line))
        .find((row) => row.kind === 'run.start');
      expect(start.data.correlation.task_id)
        .toBe(workerStart.data.correlation.task_id);
      await expect(validateArtifact(artifactPath, { secretValues: [identityKey] }))
        .resolves.toMatchObject({ valid: true, issues: [] });
    } finally {
      await rm(output, { recursive: true, force: true });
    }
  });

  it('protects trusted run-start correlation without retaining the raw ID', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-layer-openclaw-correlation-'));
    const handlers: Partial<Record<string, Handler>> = {};
    const identityKey = 'identity-secret-value-which-is-long-enough';
    const taskId = 'private-research-task-id';
    let artifactPath = '';
    let bindCorrelation: GatewayHandler | undefined;
    try {
      const plugin = createPluginDefinition(
        {
          createRunCapture: createCapture,
          createUploader: () => ({
            async enqueueArtifact(path: string) {
              artifactPath = path;
              return { bundleId: 'bundle', bundleDigest: 'digest', state: 'pending' as const };
            },
            async flush() { return { timedOut: false, uploadedBundles: 0 }; },
            status() { return { lifecycle: 'running', pressure: 'ok' }; },
            async shutdown() {},
          }),
        },
        { terminalGraceMs: 0 },
      );
      plugin.register({
        pluginConfig: {
          endpoint: 'https://ingest.example.test',
          ingestKey: 'ingest-secret-value',
          identityKey,
          installationId: 'install_0123456789abcdef0123456789abcdef',
          serviceName: 'openclaw-correlation',
          outputDirectory: output,
        },
        on(name, handler) { handlers[name] = handler as Handler; },
        registerGatewayMethod(_name, handler) {
          bindCorrelation = handler as unknown as GatewayHandler;
        },
        logger: { debug() {}, info() {}, warn() {}, error() {} },
        runtime: { version: '2026.5.5' },
      });
      const context = {
        runId: 'openclaw-run-correlated',
        sessionId: 'openclaw-session-correlated',
      };
      let bindResult: unknown;
      bindCorrelation?.({
        params: { runId: context.runId, taskId },
        respond(_ok, payload) { bindResult = payload; },
      });
      expect(bindResult).toEqual({ accepted: true });
      handlers.before_model_resolve!({ prompt: 'hello' }, context);
      await handlers.agent_end!({ runId: context.runId, success: true }, context);

      const trace = await readFile(join(artifactPath, 'trace.jsonl'), 'utf8');
      const start = trace.trim().split('\n').map((line) => JSON.parse(line))
        .find((row) => row.kind === 'run.start');
      expect(start.data.correlation).toMatchObject({
        task_id: expect.stringMatching(/^task_[a-f0-9]{64}$/u),
        execution: {
          system: 'openclaw',
          run_id: expect.stringMatching(/^exec_[a-f0-9]{64}$/u),
        },
      });
      expect(trace).not.toContain(taskId);
      expect(trace).not.toContain(identityKey);
      const workerCapture = createCapture({
        output,
        serviceName: 'worker-correlation',
        identityKey,
      });
      await workerCapture.observe('worker-attempt', {
        correlation: {
          taskId,
          execution: {
            system: 'job-runner',
            runId: 'worker-run-correlated',
          },
        },
      }, async () => 'complete');
      const workerClosed = await workerCapture.shutdown();
      const workerTrace = await readFile(
        join(workerClosed.artifactPath, 'trace.jsonl'),
        'utf8',
      );
      const workerStart = workerTrace.trim().split('\n')
        .map((line) => JSON.parse(line))
        .find((row) => row.kind === 'run.start');
      expect(start.data.correlation.task_id)
        .toBe(workerStart.data.correlation.task_id);
      await expect(validateArtifact(artifactPath, { secretValues: [taskId, identityKey] }))
        .resolves.toMatchObject({ valid: true, issues: [] });
    } finally {
      await rm(output, { recursive: true, force: true });
    }
  });

  it('uploads a sealed trace when one hook payload is safely omitted', async () => {
    const output = await mkdtemp(
      join(tmpdir(), 'semantic-layer-openclaw-safe-omission-'),
    );
    const handlers: Partial<Record<string, Handler>> = {};
    const logs: string[] = [];
    let artifactPath = '';
    let enqueued = 0;
    const privateSessionId = 'private-session-value-for-final-scan';
    try {
      const plugin = createPluginDefinition(
        {
          createRunCapture: createCapture,
          createUploader: () => ({
            async enqueueArtifact(path: string) {
              artifactPath = path;
              enqueued += 1;
              return {
                bundleId: 'bundle',
                bundleDigest: 'digest',
                state: 'pending' as const,
              };
            },
            async flush() {
              return { timedOut: false, uploadedBundles: 0 };
            },
            status() {
              return { lifecycle: 'running', pressure: 'ok' };
            },
            async shutdown() {},
          }),
        },
        { terminalGraceMs: 0 },
      );
      plugin.register({
        pluginConfig: {
          endpoint: 'https://ingest.example.test',
          ingestKey: 'ingest-secret-value',
          identityKey: 'identity-secret-value-which-is-long-enough',
          installationId: 'install_0123456789abcdef0123456789abcdef',
          serviceName: 'openclaw-safe-omission',
          outputDirectory: output,
        },
        on(name, handler) {
          handlers[name] = handler as Handler;
        },
        logger: {
          debug(message) { logs.push(String(message)); },
          info(message) { logs.push(String(message)); },
          warn(message) { logs.push(String(message)); },
          error(message) { logs.push(String(message)); },
        },
        runtime: { version: '2026.5.5' },
      });
      const context = {
        runId: 'safe-omission-run',
        sessionId: privateSessionId,
      };
      handlers.before_model_resolve!({ prompt: 'hello' }, context);
      handlers.message_sent!(
        {
          runId: context.runId,
          messageId: privateSessionId,
          content: 'safe answer',
        },
        context,
      );
      await handlers.agent_end!(
        { runId: context.runId, success: true, messages: [] },
        context,
      );

      expect(enqueued).toBe(1);
      expect(artifactPath).not.toBe('');
      const trace = await readFile(join(artifactPath, 'trace.jsonl'), 'utf8');
      expect(trace).not.toContain(privateSessionId);
      expect(trace).toContain('scrubber_failure_payload_omitted');
      expect(trace).not.toContain('unsupported_semantic_projection');
      expect(logs.join('\n')).not.toContain(privateSessionId);
      await expect(
        validateArtifact(artifactPath, { secretValues: [privateSessionId] }),
      ).resolves.toMatchObject({ valid: true, issues: [] });
    } finally {
      await rm(output, { recursive: true, force: true });
    }
  });

  it.each(['awaiting', 'error'] as const)(
    'retains the sealed source when durable staging returns %s',
    async (outcome) => {
      const output = await mkdtemp(
        join(tmpdir(), `semantic-layer-openclaw-retained-${outcome}-`),
      );
      let sourceArtifactPath = '';
      const handlers: Partial<Record<string, Handler>> = {};
      try {
        const plugin = createPluginDefinition(
          {
            createRunCapture: createCapture,
            createUploader: () => ({
              async enqueueArtifact(path: string) {
                sourceArtifactPath = path;
                if (outcome === 'error') throw new Error('staging failed');
                return {
                  bundleId: 'bundle',
                  bundleDigest: 'digest',
                  state: 'awaiting_spool_admission' as const,
                };
              },
              async flush() {
                return { timedOut: false, uploadedBundles: 0 };
              },
              status() {
                return { lifecycle: 'running', pressure: 'full' };
              },
              async shutdown() {},
            }),
          },
          { terminalGraceMs: 0 },
        );
        plugin.register({
          pluginConfig: {
            endpoint: 'https://ingest.example.test',
            ingestKey: 'ingest-secret-value',
            identityKey: 'identity-secret-value-which-is-long-enough',
            installationId: 'install_0123456789abcdef0123456789abcdef',
            serviceName: 'openclaw-retention-test',
            outputDirectory: output,
          },
          on(name, handler) {
            handlers[name] = handler as Handler;
          },
          logger: { debug() {}, info() {}, warn() {}, error() {} },
          runtime: { version: '2026.5.5' },
        });
        const context = {
          runId: `retained-${outcome}`,
          sessionId: `session-${outcome}`,
        };
        handlers.before_model_resolve!({ prompt: 'hello' }, context);
        await handlers.agent_end!(
          { runId: context.runId, success: true, messages: [] },
          context,
        );

        expect(sourceArtifactPath).not.toBe('');
        expect((await lstat(sourceArtifactPath)).isDirectory()).toBe(true);
      } finally {
        await rm(output, { recursive: true, force: true });
      }
    },
  );

  it('seals a valid bundle with exact tool/scope pairs and native-only opaque reasoning evidence', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-layer-openclaw-'));
    const staged = await mkdtemp(
      join(tmpdir(), 'semantic-layer-openclaw-staged-'),
    );
    let artifactPath = '';
    let sourceArtifactPath = '';
    const handlers: Partial<Record<string, Handler>> = {};
    try {
      const plugin = createPluginDefinition(
        {
          createRunCapture: createCapture,
          createUploader: () => ({
            async enqueueArtifact(
              path: string,
              options?: { removeSourceAfterAdmissionFrom?: string },
            ) {
              sourceArtifactPath = path;
              artifactPath = join(staged, 'bundle');
              await cp(path, artifactPath, { recursive: true });
              if (options?.removeSourceAfterAdmissionFrom === output)
                await rm(path, { recursive: true, force: true });
              return {
                bundleId: 'bundle',
                bundleDigest: 'digest',
                state: 'pending' as const,
              };
            },
            async flush() {
              return { timedOut: false, uploadedBundles: 0 };
            },
            status() {
              return { lifecycle: 'running', pressure: 'ok' };
            },
            async shutdown() {},
          }),
        },
        { terminalGraceMs: 0 },
      );
      plugin.register({
        pluginConfig: {
          endpoint: 'https://ingest.example.test',
          ingestKey: 'ingest-secret-value',
          identityKey: 'identity-secret-value-which-is-long-enough',
          installationId: 'install_0123456789abcdef0123456789abcdef',
          serviceName: 'openclaw-integration',
          outputDirectory: output,
        },
        on(name, handler) {
          handlers[name] = handler as Handler;
        },
        logger: { debug() {}, info() {}, warn() {}, error() {} },
        runtime: { version: '2026.5.5' },
      });

      const context = {
        runId: 'parent-run',
        sessionId: 'customer-session',
        sessionKey: 'agent:main:parent-session',
      };
      handlers.before_model_resolve!({ prompt: 'hello' }, context);
      handlers.model_call_started!(
        {
          runId: 'parent-run',
          callId: 'model-1',
          provider: 'openrouter',
          model: 'deepseek/deepseek-chat-v3-0324',
          sessionId: 'customer-session',
        },
        context,
      );
      handlers.llm_input!(
        {
          runId: 'parent-run',
          sessionId: 'customer-session',
          provider: 'openrouter',
          model: 'deepseek/deepseek-chat-v3-0324',
          prompt: 'hello',
          historyMessages: [],
          imagesCount: 0,
        },
        context,
      );
      handlers.before_tool_call!(
        {
          runId: 'parent-run',
          toolCallId: 'tool-1',
          toolName: 'lookup',
          params: { query: 'safe' },
        },
        { ...context, toolCallId: 'tool-1', toolName: 'lookup' },
      );
      handlers.after_tool_call!(
        {
          runId: 'parent-run',
          toolCallId: 'tool-1',
          toolName: 'lookup',
          params: { query: 'safe' },
          result: { answer: 42 },
        },
        { ...context, toolCallId: 'tool-1', toolName: 'lookup' },
      );
      const subagentContext = {
        runId: 'child-run',
        childSessionKey: 'agent:researcher:child-session',
        requesterSessionKey: context.sessionKey,
      };
      handlers.subagent_spawned!(
        {
          runId: 'child-run',
          childSessionKey: subagentContext.childSessionKey,
          agentId: 'researcher',
          mode: 'run',
          threadRequested: false,
        },
        subagentContext,
      );
      handlers.subagent_ended!(
        {
          runId: 'child-run',
          targetSessionKey: subagentContext.childSessionKey,
          targetKind: 'subagent',
          reason: 'done',
          outcome: 'ok',
        },
        subagentContext,
      );
      handlers.model_call_ended!(
        {
          runId: 'parent-run',
          callId: 'model-1',
          provider: 'openrouter',
          model: 'deepseek/deepseek-chat-v3-0324',
          outcome: 'completed',
          durationMs: 12,
        },
        context,
      );
      handlers.before_message_write!(
        {
          sessionKey: context.sessionKey,
          message: {
            role: 'assistant',
            provider: 'openrouter',
            model: 'deepseek/deepseek-chat-v3-0324',
            responseId: 'provider-response-1',
            stopReason: 'stop',
            usage: { input: 10, output: 20, totalTokens: 30 },
            content: [
              {
                type: 'thinking',
                thinking: 'EXPOSED_MODEL_REASONING',
                thinkingSignature: 'PRIVATE_THINKING_SIGNATURE',
                encryptedReasoning: 'PRIVATE_ENCRYPTED_REASONING',
                encryptedContent: 'ingest-secret-value',
              },
              { type: 'summary', text: 'Checked the lookup result.' },
              {
                type: 'thinking',
                text: 'PRIVATE_REDACTED_REASONING',
                redacted: true,
              },
              {
                type: 'toolCall',
                arguments: { thinking: 'PRIVATE_TOOL_ARGUMENT' },
              },
              { type: 'text', text: 'answer' },
            ],
          },
        },
        { sessionKey: context.sessionKey },
      );
      handlers.llm_output!(
        {
          runId: 'parent-run',
          sessionId: 'customer-session',
          provider: 'openrouter',
          model: 'deepseek/deepseek-chat-v3-0324',
          responseId: 'provider-response-1',
          assistantTexts: ['answer'],
          lastAssistant: {
            responseId: 'provider-response-1',
            reasoning: [
              {
                type: 'thinking',
                text: 'EXPOSED_MODEL_REASONING',
                thinkingSignature: 'PRIVATE_MODEL_SIGNATURE',
              },
              { type: 'summary', text: 'Checked the lookup result.' },
            ],
          },
        },
        context,
      );
      await handlers.agent_end!(
        {
          runId: 'parent-run',
          success: true,
          messages: [
            {
              role: 'assistant',
              responseId: 'provider-response-1',
              content: [
                {
                  type: 'thinking',
                  thinking: 'EXPOSED_MODEL_REASONING',
                  thinkingSignature: 'PRIVATE_THINKING_SIGNATURE',
                },
                { type: 'text', text: 'answer' },
              ],
            },
          ],
        },
        context,
      );

      expect(artifactPath).not.toBe('');
      await expect(lstat(sourceArtifactPath)).rejects.toMatchObject({
        code: 'ENOENT',
      });
      const validation = await validateArtifact(artifactPath, {
        profile: 'structural',
      });
      expect(validation.valid, validation.issues.join('\n')).toBe(true);
      const manifest = JSON.parse(
        await readFile(join(artifactPath, 'manifest.json'), 'utf8'),
      );
      expect(manifest).toMatchObject({
        schema: 'semantic_trace_manifest_v2',
        installation_id: 'install_0123456789abcdef0123456789abcdef',
        sources: expect.arrayContaining([
          expect.objectContaining({
            name: 'openclaw',
            version: '2026.5.5',
            qualification: {
              status: 'exact_qualified',
              profile: 'openclaw.plugin.typed-hooks',
            },
          }),
        ]),
      });
      const trace = await readFile(join(artifactPath, 'trace.jsonl'), 'utf8');
      const records = trace
        .trim()
        .split('\n')
        .map((line) => JSON.parse(line) as Record<string, unknown>);
      expect(records.some((record) => record.kind === 'tool.call')).toBe(true);
      expect(records.some((record) => record.kind === 'tool.result')).toBe(
        true,
      );
      expect(records.filter((record) => record.kind === 'scope')).toHaveLength(
        2,
      );
      expect(trace).toContain(
        'This OpenClaw llm_input hook did not expose resolved tool definitions.',
      );
      expect(trace).toContain('before_agent_run');
      expect(trace).toContain('did not expose a finish reason');
      expect(trace).toContain('Checked the lookup result.');
      const response = records.find(
        (record) => record.kind === 'model.response',
      );
      expect(
        (response?.data as Record<string, unknown>)?.reasoning,
      ).toBeUndefined();
      const assistantMessage = records.find(
        (record) =>
          record.kind === 'state' &&
          (record.data as Record<string, unknown>)?.type ===
            'openclaw.assistant_message',
      );
      expect(
        (assistantMessage?.data as Record<string, unknown>)?.value,
      ).toMatchObject({
        provider: 'openrouter',
        model: 'deepseek/deepseek-chat-v3-0324',
        response_id: 'provider-response-1',
        stop_reason: 'stop',
        reasoning: [
          { type: 'text', text: 'EXPOSED_MODEL_REASONING' },
          { type: 'summary', text: 'Checked the lookup result.' },
        ],
        usage: { input_tokens: 10, output_tokens: 20, total_tokens: 30 },
      });
      expect(trace).toContain('EXPOSED_MODEL_REASONING');
      expect(trace.match(/EXPOSED_MODEL_REASONING/gu)).toHaveLength(1);
      expect(trace).not.toContain('PRIVATE_MODEL_SIGNATURE');
      expect(trace).not.toContain('PRIVATE_THINKING_SIGNATURE');
      expect(trace).not.toContain('PRIVATE_ENCRYPTED_REASONING');
      expect(trace).toContain('credential_redaction');
      expect(
        JSON.stringify(
          (
            (assistantMessage?.data as Record<string, unknown>)
              ?.value as Record<string, unknown>
          )?.reasoning,
        ),
      ).not.toContain('PRIVATE_THINKING_SIGNATURE');
      expect(trace).not.toContain('PRIVATE_REDACTED_REASONING');
      expect(trace).not.toContain('PRIVATE_TOOL_ARGUMENT');
      expect(trace).not.toContain('customer-session');
      expect(trace).not.toContain('agent:main:parent-session');
      expect(trace).not.toContain('agent:researcher:child-session');
      expect(trace).not.toContain('identity-secret-value');
      expect(trace).not.toContain('ingest-secret-value');
    } finally {
      await rm(output, { recursive: true, force: true });
      await rm(staged, { recursive: true, force: true });
    }
  });

  it('seals when OpenClaw exposes the same native run and session identity', async () => {
    const output = await mkdtemp(join(tmpdir(), 'semantic-layer-openclaw-shared-id-'));
    const staged = await mkdtemp(
      join(tmpdir(), 'semantic-layer-openclaw-shared-id-staged-'),
    );
    let artifactPath = '';
    let sourceArtifactPath = '';
    const handlers: Partial<Record<string, Handler>> = {};
    try {
      const plugin = createPluginDefinition(
        {
          createRunCapture: createCapture,
          createUploader: () => ({
            async enqueueArtifact(
              path: string,
              options?: { removeSourceAfterAdmissionFrom?: string },
            ) {
              sourceArtifactPath = path;
              artifactPath = join(staged, 'bundle');
              await cp(path, artifactPath, { recursive: true });
              if (options?.removeSourceAfterAdmissionFrom === output)
                await rm(path, { recursive: true, force: true });
              return {
                bundleId: 'bundle',
                bundleDigest: 'digest',
                state: 'pending' as const,
              };
            },
            async flush() {
              return { timedOut: false, uploadedBundles: 0 };
            },
            status() {
              return { lifecycle: 'running', pressure: 'ok' };
            },
            async shutdown() {},
          }),
        },
        { terminalGraceMs: 0 },
      );
      plugin.register({
        pluginConfig: {
          endpoint: 'https://ingest.example.test',
          ingestKey: 'ingest-secret-value',
          identityKey: 'identity-secret-value-which-is-long-enough',
          installationId: 'install_0123456789abcdef0123456789abcdef',
          serviceName: 'openclaw-shared-native-identity',
          outputDirectory: output,
        },
        on(name, handler) {
          handlers[name] = handler as Handler;
        },
        logger: { debug() {}, info() {}, warn() {}, error() {} },
        runtime: { version: '2026.5.5' },
      });

      const context = {
        runId: 'shared-native-run-session-id',
        sessionId: 'shared-native-run-session-id',
        sessionKey: 'agent:main:shared-native-session',
      };
      handlers.before_model_resolve!({ prompt: 'hello' }, context);
      await handlers.agent_end!(
        { runId: context.runId, success: true, messages: [] },
        context,
      );

      expect(artifactPath).not.toBe('');
      await expect(lstat(sourceArtifactPath)).rejects.toMatchObject({
        code: 'ENOENT',
      });
      const validation = await validateArtifact(artifactPath, {
        profile: 'structural',
      });
      expect(validation.valid, validation.issues.join('\n')).toBe(true);
      const trace = await readFile(join(artifactPath, 'trace.jsonl'), 'utf8');
      expect(trace).toContain('shared-native-run-session-id');
      expect(trace).not.toContain('agent:main:shared-native-session');
      expect(trace).not.toContain('scrubber_failure_payload_omitted');
    } finally {
      await rm(output, { recursive: true, force: true });
      await rm(staged, { recursive: true, force: true });
    }
  });
});
