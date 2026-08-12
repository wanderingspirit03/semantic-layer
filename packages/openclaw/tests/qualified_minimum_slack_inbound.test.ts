import { describe, expect, it } from 'vitest';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import {
  deriveInboundMessageHookContext,
  toPluginMessageContext,
  toPluginMessageReceivedEvent,
} from 'openclaw/plugin-sdk/hook-runtime';
import {
  createPluginDefinition,
  type PluginDependencies,
} from '../src/plugin.js';
import type { CaptureSource, SourceSink } from '../src/contracts.js';

type Handler = (
  event: Record<string, unknown>,
  context: Record<string, unknown>,
) => unknown;

describe('exact OpenClaw 2026.5.5 Slack inbound correlation', () => {
  it('uses the later trusted native run ID without a chat.send binding', async () => {
    const slack = await installedSlackTestApi();
    const prepared = await slack.prepareSlackMessage({
      ctx: slack.createInboundSlackTestContext({
        cfg: {
          agents: { defaults: { workspace: '/tmp' } },
          channels: { slack: { enabled: true } },
        },
        appClient: {
          conversations: {
            info: async () => ({ channel: { id: 'D123', is_im: true } }),
          },
          users: {
            info: async () => ({
              user: { id: 'U123', name: 'synthetic-user' },
            }),
          },
        },
      }),
      account: {
        accountId: 'default',
        enabled: true,
        botToken: 'token',
        appToken: 'app-token',
        config: {},
      },
      message: {
        type: 'message',
        channel: 'D123',
        user: 'U123',
        text: 'synthetic inbound',
        ts: '1723456789.000100',
        event_ts: '1723456789.000100',
      },
      opts: {},
    });
    expect(prepared).not.toBeNull();
    const canonical = deriveInboundMessageHookContext(prepared!.ctxPayload);
    const messageContext = toPluginMessageContext(canonical);
    const messageEvent = toPluginMessageReceivedEvent(canonical);
    expect(messageContext).not.toHaveProperty('runId');
    expect(messageEvent.runId).toBeUndefined();

    const opened: Array<Record<string, unknown>> = [];
    const handlers: Partial<Record<string, Handler>> = {};
    const plugin = createPluginDefinition(dependencies(opened), {
      terminalGraceMs: 0,
    });
    plugin.register({
      pluginConfig: {
        endpoint: 'https://ingest.example.test',
        ingestKey: 'ingest-secret-value',
        identityKey: 'identity-secret-value-which-is-long-enough',
        installationId: 'install_0123456789abcdef0123456789abcdef',
        serviceName: 'qualified-minimum-slack',
      },
      on(name, handler) { handlers[name] = handler as Handler; },
      logger: { debug() {}, info() {}, warn() {}, error() {} },
      runtime: { version: '2026.5.5' },
    });

    handlers.message_received!(
      messageEvent as unknown as Record<string, unknown>,
      messageContext as unknown as Record<string, unknown>,
    );
    const runContext = {
      runId: 'native-slack-agent-run',
      sessionId: 'native-slack-agent-session',
      sessionKey: messageContext.sessionKey,
      messageProvider: 'slack',
    };
    handlers.before_model_resolve!({ prompt: 'synthetic inbound' }, runContext);
    await handlers.agent_end!({ success: true }, runContext);

    expect(opened).toContainEqual(expect.objectContaining({
      correlation: {
        taskId: runContext.runId,
        execution: {
          system: 'openclaw',
          runId: runContext.runId,
          rootRunId: runContext.runId,
        },
      },
    }));
  });
});

type SlackTestApi = {
  createInboundSlackTestContext(params: Record<string, unknown>): unknown;
  prepareSlackMessage(params: Record<string, unknown>): Promise<{
    ctxPayload: Parameters<typeof deriveInboundMessageHookContext>[0];
  } | null>;
};

async function installedSlackTestApi(): Promise<SlackTestApi> {
  const openclawEntry = pathToFileURL(
    createRequire(import.meta.url).resolve('openclaw'),
  ).href;
  return await import(
    new URL('./extensions/slack/inbound-contract-test-api.js', openclawEntry)
      .href
  ) as SlackTestApi;
}

function dependencies(opened: Array<Record<string, unknown>>): PluginDependencies {
  return {
    createRunCapture() {
      return {
        installSource(source: CaptureSource) {
          const sink: SourceSink = {
            openTrace(record) {
              opened.push(record as unknown as Record<string, unknown>);
              return accepted({ traceId: 'trace-a', spanId: 'span-root' }, 'record-root');
            },
            record() { return accepted(undefined, 'record-event'); },
            openScope() { return accepted(undefined, 'record-scope'); },
            closeScope() { return accepted(undefined, 'record-scope-end'); },
            closeTrace() { return accepted(undefined, 'record-outcome'); },
            recordLoss() { return accepted(undefined, 'record-loss'); },
          } as SourceSink;
          const installed = source.install(sink);
          return {
            deactivate: installed.deactivate,
            drain: installed.drain ?? (async () => {}),
          };
        },
        status() { return { state: 'open' as const, losses: {}, rejected: 0 }; },
        async flush() {},
        async shutdown() {
          return {
            state: 'closed' as const,
            artifactPath: '/sealed/native-slack-agent-run',
            losses: {},
            rejected: 0,
          };
        },
      };
    },
    createUploader() {
      return {
        async enqueueArtifact() {
          return { bundleId: 'bundle', bundleDigest: 'digest', state: 'pending' as const };
        },
        async flush() { return { timedOut: false, uploadedBundles: 0 }; },
        status() { return { lifecycle: 'running' as const, pressure: 'ok' as const }; },
        async shutdown() {},
      };
    },
  };
}

function accepted(identity: unknown, recordId: string) {
  return {
    accepted: true as const,
    recordId,
    ...(identity ? { identity } : {}),
    settled: Promise.resolve(),
  };
}
