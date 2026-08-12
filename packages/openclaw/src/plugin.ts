import { createHmac } from 'node:crypto';
import type {
  OpenClawPluginApi,
  OpenClawPluginDefinition,
} from 'openclaw/plugin-sdk/plugin-entry';
import type {
  CaptureHandleLike,
  CloudUploaderLike,
  PluginDependencies,
  ResolvedPluginConfig,
} from './contracts.js';
import {
  createRunSource,
  type RunSource,
  type RunSourceEvent,
} from './source.js';

export const REQUIRED_HOOKS = [
  'before_model_resolve',
  'before_prompt_build',
  'agent_end',
  'model_call_started',
  'model_call_ended',
  'llm_input',
  'llm_output',
  'before_message_write',
  'before_tool_call',
  'after_tool_call',
  'message_received',
  'message_sent',
  'subagent_spawned',
  'subagent_ended',
  'gateway_start',
  'gateway_stop',
] as const;

export type RequiredHookName = (typeof REQUIRED_HOOKS)[number];
export type { PluginDependencies } from './contracts.js';

type Logger = Pick<
  OpenClawPluginApi['logger'],
  'debug' | 'info' | 'warn' | 'error'
>;

export type PluginApi = {
  pluginConfig?: Record<string, unknown>;
  logger: Logger;
  on: OpenClawPluginApi['on'];
  registerTool?: OpenClawPluginApi['registerTool'];
  registerGatewayMethod?: OpenClawPluginApi['registerGatewayMethod'];
  runtime?: { version?: string };
  registrationMode?: string;
};

export type PluginDefinition = OpenClawPluginDefinition & {
  id: 'semantic-layer-openclaw';
  name: 'Semantic Layer';
  version: '0.1.0-pilot.5';
  register(api: PluginApi): void;
};

export type PluginRuntimeOptions = {
  terminalGraceMs?: number;
  shutdownDeadlineMs?: number;
  queueCapacityBytes?: number;
};

type Run = {
  runId: string;
  sessionKeys: Set<string>;
  source: RunSource;
  capture: CaptureHandleLike;
  lifecycle: ReturnType<CaptureHandleLike['installSource']>;
  tail: Promise<void>;
  queuedBytes: number;
  closing: boolean;
  terminalQueued: boolean;
  closed: boolean;
  terminal?: Promise<void>;
  modelRequests: Map<string, string>;
  modelContentResponses: Set<string>;
  completedModelRequests: Map<string, CompletedModelRequest>;
  toolCalls: Map<string, string>;
  subagentScopes: Map<string, string>;
  deliveryIds: Set<string>;
  reasoningBlocksByResponse: Map<string, Map<string, number>>;
  gaps: Set<string>;
};

type CompletedModelRequest = {
  requestRecordId: string;
  terminal: Record<string, unknown>;
  richResponseCaptured: boolean;
};

const DEFAULT_QUEUE_BYTES = 4 * 1024 * 1024;
const DEFAULT_SHUTDOWN_MS = 10_000;
const MINIMUM_HOST_VERSION = '2026.5.5';
const QUALIFIED_HOST_VERSIONS = new Set(['2026.5.5', '2026.7.1-2']);
const QUALIFICATION_PROFILE = 'openclaw.plugin.typed-hooks';
const SNAPSHOT_MAX_DEPTH = 12;
const SNAPSHOT_MAX_NODES = 8_192;
const SNAPSHOT_MAX_WIDTH = 512;
const SNAPSHOT_MAX_STRING_BYTES = 256_000;
const SNAPSHOT_MAX_BYTES = 512 * 1024;
const MAX_PENDING_CORRELATIONS = 1_024;
const CORRELATION_BINDING_TTL_MS = 5 * 60 * 1_000;
const PRIVATE_SNAPSHOT_KEYS = new Set([
  'thinking',
  'reasoning',
  'reasoningcontent',
  'chainofthought',
  'encryptedcontent',
  'encryptedreasoning',
  'authorization',
  'apikey',
]);
type ExposedReasoningBlock = { type: 'text' | 'summary'; text: string };
type ReasoningOmission = 'encrypted' | 'malformed' | 'redacted';
type ExposedReasoning = {
  blocks: ExposedReasoningBlock[];
  truncated: boolean;
  omissions: ReasoningOmission[];
};

type SnapshotLimit =
  'bytes' | 'cycle' | 'depth' | 'nodes' | 'string' | 'width' | 'unavailable';

export function createPluginDefinition(
  dependencies: PluginDependencies,
  options: PluginRuntimeOptions = {},
): PluginDefinition {
  return {
    id: 'semantic-layer-openclaw',
    name: 'Semantic Layer',
    description:
      'Capture OpenClaw runs as semantic traces and enqueue sealed bundles for upload.',
    version: '0.1.0-pilot.5',
    register(api) {
      const runtime = new CaptureRuntime(api, dependencies, options);
      runtime.registerCorrelationGatewayMethod();
      runtime.registerHooks();
    },
  };
}

class CaptureRuntime {
  private readonly runs = new Map<string, Run>();
  private readonly pendingCorrelations = new Map<
    string,
    { taskId: string; expiresAt: number }
  >();
  private readonly consumedCorrelations = new Map<string, number>();
  private readonly sessionOwners = new Map<string, Set<string>>();
  private readonly unavailableHooks = new Set<string>();
  private readonly diagnosticKeys = new Set<string>();
  private readonly terminalGraceMs: number;
  private readonly shutdownDeadlineMs: number;
  private readonly queueCapacityBytes: number;
  private readonly config: ResolvedPluginConfig | undefined;
  private uploader: CloudUploaderLike | undefined;

  constructor(
    private readonly api: PluginApi,
    private readonly dependencies: PluginDependencies,
    options: PluginRuntimeOptions,
  ) {
    this.terminalGraceMs = options.terminalGraceMs ?? 25;
    this.shutdownDeadlineMs = options.shutdownDeadlineMs ?? DEFAULT_SHUTDOWN_MS;
    this.queueCapacityBytes = options.queueCapacityBytes ?? DEFAULT_QUEUE_BYTES;
    const hostVersion = api.runtime?.version;
    const hostBelowMinimum = Boolean(
      hostVersion && compareHostVersions(hostVersion, MINIMUM_HOST_VERSION) < 0,
    );
    this.config = hostBelowMinimum
      ? undefined
      : resolveConfig(api.pluginConfig);
    if (hostBelowMinimum) {
      this.logOnce(
        'unsupported-host',
        'error',
        `Semantic Layer capture is disabled: the plugin requires OpenClaw ${MINIMUM_HOST_VERSION} or newer (host=${hostVersion}).`,
      );
    } else if (!this.config) {
      this.logOnce(
        'invalid-config',
        'error',
        'Semantic Layer capture is disabled: configuration is incomplete. Run the setup doctor.',
      );
    } else if (
      hostVersion &&
      compareHostVersions(hostVersion, MINIMUM_HOST_VERSION) >= 0 &&
      !QUALIFIED_HOST_VERSIONS.has(hostVersion)
    ) {
      this.logOnce(
        'unqualified-host',
        'warn',
        `Semantic Layer capture is active on in-range but unqualified OpenClaw ${hostVersion}; the exact qualified build is ${[...QUALIFIED_HOST_VERSIONS].join(', ')}. Run the setup doctor for the overall capability check.`,
      );
    }
  }

  registerCorrelationGatewayMethod(): void {
    if (!this.api.registerGatewayMethod) return;
    this.api.registerGatewayMethod(
      'semantic-layer.correlation.bind',
      ({ params, respond }) => {
        if (!this.config) {
          respond(true, { accepted: false, reason: 'capture_disabled' });
          return;
        }
        this.pruneExpiredCorrelations();
        if (!validCorrelationBindingParams(params)) {
          respond(true, { accepted: false, reason: 'invalid_request' });
          return;
        }
        const { runId, taskId } = params;
        if (this.runs.has(runId)) {
          respond(true, { accepted: false, reason: 'run_active' });
          return;
        }
        if (this.consumedCorrelations.has(runId)) {
          respond(true, { accepted: false, reason: 'run_consumed' });
          return;
        }
        const existing = this.pendingCorrelations.get(runId);
        if (existing) {
          respond(true, existing.taskId === taskId
            ? { accepted: true }
            : { accepted: false, reason: 'conflict' });
          return;
        }
        if (this.pendingCorrelations.size >= MAX_PENDING_CORRELATIONS) {
          respond(true, { accepted: false, reason: 'capacity_reached' });
          return;
        }
        this.pendingCorrelations.set(runId, {
          taskId,
          expiresAt: Date.now() + CORRELATION_BINDING_TTL_MS,
        });
        respond(true, { accepted: true });
      },
      { scope: 'operator.admin' },
    );
  }

  registerHooks(): void {
    this.attempt('before_model_resolve', () =>
      this.api.on(
        'before_model_resolve',
        (event, ctx) => {
          this.beforeAgentStart(event, ctx);
        },
        { timeoutMs: 1_000 },
      ),
    );
    this.attempt('before_prompt_build', () =>
      this.api.on(
        'before_prompt_build',
        (event, ctx) => {
          this.beforePromptBuild(event, ctx);
        },
        { timeoutMs: 1_000 },
      ),
    );
    this.attempt('agent_end', () =>
      this.api.on('agent_end', (event, ctx) => this.agentEnd(event, ctx), {
        timeoutMs: this.shutdownDeadlineMs,
      }),
    );
    this.attempt('model_call_started', () =>
      this.api.on('model_call_started', (event, ctx) => {
        this.modelCallStarted(event, ctx);
      }),
    );
    this.attempt('model_call_ended', () =>
      this.api.on('model_call_ended', (event, ctx) => {
        this.modelCallEnded(event, ctx);
      }),
    );
    this.attempt('llm_input', () =>
      this.api.on('llm_input', (event, ctx) => {
        this.llmInput(event, ctx);
      }),
    );
    this.attempt('llm_output', () =>
      this.api.on('llm_output', (event, ctx) => {
        this.llmOutput(event, ctx);
      }),
    );
    this.attempt('before_message_write', () =>
      this.api.on('before_message_write', (event, ctx) => {
        this.beforeMessageWrite(event, ctx);
      }),
    );
    this.attempt('before_tool_call', () =>
      this.api.on('before_tool_call', (event, ctx) => {
        this.beforeToolCall(event, ctx);
      }),
    );
    this.attempt('after_tool_call', () =>
      this.api.on('after_tool_call', (event, ctx) => {
        this.afterToolCall(event, ctx);
      }),
    );
    this.attempt('message_received', () =>
      this.api.on('message_received', (event, ctx) => {
        this.messageReceived(event, ctx);
      }),
    );
    this.attempt('message_sent', () =>
      this.api.on('message_sent', (event, ctx) => {
        this.messageSent(event, ctx);
      }),
    );
    this.attempt('subagent_spawned', () =>
      this.api.on('subagent_spawned', (event, ctx) => {
        this.subagentSpawned(event, ctx);
      }),
    );
    this.attempt('subagent_ended', () =>
      this.api.on('subagent_ended', (event, ctx) => {
        this.subagentEnded(event, ctx);
      }),
    );
    this.attempt('gateway_start', () =>
      this.api.on('gateway_start', () => {
        if (this.config) {
          try {
            this.getUploader();
          } catch (error) {
            this.logError(
              'durable uploader startup failed; pending bundles remain on disk',
              error,
            );
          }
        }
        this.api.logger.info(
          `Semantic Layer OpenClaw capture hooks active (host=${this.api.runtime?.version ?? 'unknown'}, unavailable=${[...this.unavailableHooks].join(',') || 'none'})`,
        );
      }),
    );
    this.attempt('gateway_stop', () =>
      this.api.on(
        'gateway_stop',
        async () => {
          await Promise.allSettled(
            [...this.runs.values()].map((run) =>
              this.finishRun(run, {
                success: false,
                error: 'gateway_stopped_before_agent_end',
              }),
            ),
          );
          await this.uploader?.shutdown().catch((error: unknown) => {
            this.logError('uploader shutdown failed', error);
          });
        },
        { timeoutMs: this.shutdownDeadlineMs },
      ),
    );
  }

  private attempt(name: RequiredHookName, register: () => void): void {
    try {
      register();
    } catch (error) {
      this.unavailableHooks.add(name);
      this.logError(
        `OpenClaw hook ${name} is unavailable; capture will record a gap`,
        error,
      );
    }
  }

  private beforeAgentStart(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const run = this.ensureRun(data, context, {
      prompt: this.snapshot(undefined, data?.prompt),
      messages: this.snapshot(undefined, data?.messages),
    });
    if (!run) return;
  }

  private beforePromptBuild(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const run = this.ensureRun(data, context, {
      prompt: this.snapshot(undefined, data.prompt),
    });
    if (!run) return;
    this.enqueue(run, data, () => {
      run.source.event({
        kind: 'state',
        phase: 'event',
        name: 'openclaw.prompt.context',
        native: this.snapshot(run, pick(data, ['messages'])),
        semantic: {
          type: 'state.transition',
          state_type: 'openclaw.prompt_messages',
          value: this.snapshot(run, data.messages),
        },
      });
    });
  }

  private modelCallStarted(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const run = this.ensureRun(data, context);
    if (!run) return;
    const callId = text(data?.callId);
    if (!callId) {
      this.gapOnce(run, 'model-start-id', 'unsupported_native_value', {
        type: 'model.call_id',
        hook: 'model_call_started',
      });
      return;
    }
    this.enqueue(run, data, () => {
      const receipt = run.source.event({
        kind: 'model',
        phase: 'start',
        name: 'openclaw.model.request',
        nativeIdentity: callId,
        native: this.snapshot(
          run,
          pick(data, [
            'runId',
            'callId',
            'provider',
            'model',
            'api',
            'transport',
          ]),
        ),
        semantic: {
          type: 'model.request',
          call_id: callId,
          ...strings(data, ['provider', 'model', 'api', 'transport']),
        },
      });
      if (receipt.accepted) run.modelRequests.set(callId, receipt.recordId);
    });
  }

  private modelCallEnded(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const run = this.ensureRun(data, context);
    if (!run) return;
    const callId = text(data?.callId);
    this.enqueue(run, data, () => {
      const parentRecordId = callId ? run.modelRequests.get(callId) : undefined;
      const finishReason = text(data.finishReason);
      if (!callId || !parentRecordId)
        this.gapOnce(
          run,
          `model-end:${callId ?? 'missing'}`,
          'turn_order_ambiguous',
          { type: 'model.request_response', hook: 'model_call_ended', callId },
        );
      if (!finishReason)
        run.source.gap('unsupported_native_value', {
          type: 'model.finish_reason',
          hook: 'model_call_ended',
          ...(callId ? { callId } : {}),
          detail:
            'This OpenClaw model_call_ended event did not expose a finish reason.',
        });
      const contentAlreadyCaptured = Boolean(
        callId && run.modelContentResponses.has(callId),
      );
      if (callId && parentRecordId && !contentAlreadyCaptured) {
        run.completedModelRequests.set(callId, {
          requestRecordId: parentRecordId,
          terminal: data,
          richResponseCaptured: false,
        });
        run.modelRequests.delete(callId);
        return;
      }
      this.emitModelCallTerminal(
        run,
        data,
        callId,
        parentRecordId,
        contentAlreadyCaptured,
      );
      if (callId) {
        run.modelRequests.delete(callId);
        run.modelContentResponses.delete(callId);
      }
    });
  }

  private emitModelCallTerminal(
    run: Run,
    data: Record<string, unknown>,
    callId: string | undefined,
    parentRecordId: string | undefined,
    contentAlreadyCaptured: boolean,
  ): void {
    const finishReason = text(data.finishReason);
    run.source.event({
      kind: contentAlreadyCaptured ? 'state' : 'model',
      phase: contentAlreadyCaptured
        ? 'event'
        : data?.outcome === 'error'
          ? 'error'
          : 'end',
      name: contentAlreadyCaptured
        ? 'openclaw.model.telemetry'
        : 'openclaw.model.response',
      ...(callId ? { nativeIdentity: callId } : {}),
      ...(parentRecordId ? { parentRecordId } : {}),
      native: this.snapshot(
        run,
        pick(data, [
          'runId',
          'callId',
          'provider',
          'model',
          'api',
          'transport',
          'durationMs',
          'outcome',
          'errorCategory',
          'failureKind',
          'requestPayloadBytes',
          'responseStreamBytes',
          'timeToFirstByteMs',
          'upstreamRequestIdHash',
        ]),
      ),
      semantic: contentAlreadyCaptured
        ? {
            type: 'state.transition',
            state_type: 'openclaw.model_telemetry',
            value: this.snapshot(run, data),
          }
        : {
            type: 'model.response',
            status: data?.outcome === 'error' ? 'failed' : 'completed',
            ...(callId ? { call_id: callId } : {}),
            ...(finishReason ? { finish_reason: finishReason } : {}),
            ...strings(data, [
              'provider',
              'model',
              'api',
              'transport',
              'outcome',
              'errorCategory',
              'failureKind',
            ]),
            ...numbers(data, [
              'durationMs',
              'requestPayloadBytes',
              'responseStreamBytes',
              'timeToFirstByteMs',
            ]),
          },
    });
  }

  private llmInput(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const run = this.ensureRun(data, context);
    if (!run) return;
    this.enqueue(run, data, () => {
      const request =
        run.modelRequests.size === 1
          ? [...run.modelRequests.values()][0]
          : undefined;
      run.source.event({
        kind: 'state',
        phase: 'event',
        name: 'openclaw.llm.input',
        ...(request ? { parentRecordId: request } : {}),
        native: this.snapshot(
          run,
          pick(data, [
            'runId',
            'provider',
            'model',
            'systemPrompt',
            'prompt',
            'historyMessages',
            'imagesCount',
          ]),
        ),
        semantic: {
          type: 'state.transition',
          state_type: 'openclaw.llm_input',
          value: {
            ...strings(data, ['provider', 'model']),
            prompt: this.snapshot(run, data?.prompt),
            system_prompt: this.snapshot(run, data?.systemPrompt),
            history: this.snapshot(run, data?.historyMessages),
          },
        },
      });
      if (!request)
        this.gapOnce(run, 'llm-input-call-id', 'turn_order_ambiguous', {
          type: 'model.input_call_correlation',
          detail: `llm_input does not expose callId and ${run.modelRequests.size} model calls were open; no model-call edge was invented.`,
        });
      if (Array.isArray(data.tools)) {
        run.source.event({
          kind: 'state',
          phase: 'event',
          name: 'openclaw.model.tool_definitions',
          native: { count: data.tools.length },
          semantic: {
            type: 'state.transition',
            state_type: 'model.tool_definitions',
            value: this.snapshot(run, data.tools),
          },
        });
      } else {
        this.gapOnce(run, 'tool-definitions', 'unsupported_native_value', {
          type: 'model.tool_definitions',
          detail:
            'This OpenClaw llm_input hook did not expose resolved tool definitions.',
        });
      }
    });
  }

  private llmOutput(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const ctx = object(context);
    const run = this.ensureRun(data, context);
    if (!run) return;
    const reasoning = extractExposedReasoning(data?.lastAssistant);
    const opaqueReasoning = this.opaqueReasoningEvidence(
      run,
      data.lastAssistant,
    );
    const usage = normalizeUsage(data?.usage);
    const exactCallId = text(data.callId) ?? text(ctx?.callId);
    const lastAssistant = object(data.lastAssistant);
    const responseId = text(data.responseId) ?? text(lastAssistant?.responseId);
    const deliveryId = responseId ? `llm_output:${responseId}` : undefined;
    const deliveryAlreadyCaptured = Boolean(
      deliveryId && run.deliveryIds.has(deliveryId),
    );
    const reservedReasoning = reserveReasoningBlocks(
      run.reasoningBlocksByResponse,
      responseId,
      reasoning.blocks,
    );
    if (
      deliveryAlreadyCaptured &&
      reservedReasoning.blocks.length === 0 &&
      !opaqueReasoning
    )
      return;
    if (deliveryId && !deliveryAlreadyCaptured) run.deliveryIds.add(deliveryId);
    const capturedReasoning = { ...reasoning, blocks: reservedReasoning.blocks };
    const queued = this.enqueue(run, data, () => {
      if (reasoning.truncated) this.reasoningLimitGap(run);
      this.reasoningOmissionGaps(run, reasoning.omissions);
      const completed = exactCallId
        ? run.completedModelRequests.get(exactCallId)
        : undefined;
      const request = exactCallId
        ? (run.modelRequests.get(exactCallId) ?? completed?.requestRecordId)
        : undefined;
      const terminal = completed?.terminal;
      const receipt = run.source.event({
        kind: request ? 'model' : 'state',
        phase: request ? 'end' : 'event',
        name: 'openclaw.llm.output',
        ...(exactCallId ? { nativeIdentity: exactCallId } : {}),
        ...(request ? { parentRecordId: request } : {}),
        native: this.snapshot(run, {
          ...pick(data, [
            'runId',
            'callId',
            'responseId',
            'provider',
            'model',
            'resolvedRef',
            'harnessId',
            'assistantTexts',
            'lastAssistant',
            'usage',
          ]),
          ...(opaqueReasoning ? { opaqueReasoning } : {}),
          ...(terminal
            ? { modelCallEnded: modelTerminalEvidence(terminal) }
            : {}),
        }),
        semantic: request
          ? {
              type: 'model.response',
              status: terminal?.outcome === 'error' ? 'failed' : 'completed',
              call_id: exactCallId,
              ...(responseId ? { response_id: responseId } : {}),
              ...(text(terminal?.finishReason)
                ? { finish_reason: text(terminal?.finishReason) }
                : {}),
              ...strings(terminal, [
                'provider',
                'model',
                'api',
                'transport',
                'outcome',
                'errorCategory',
                'failureKind',
              ]),
              ...numbers(terminal, [
                'durationMs',
                'requestPayloadBytes',
                'responseStreamBytes',
                'timeToFirstByteMs',
              ]),
              ...strings(data, [
                'provider',
                'model',
                'resolvedRef',
                'harnessId',
              ]),
              content: this.snapshot(run, data?.assistantTexts),
              ...(usage ? { usage } : {}),
              ...(capturedReasoning.blocks.length > 0
                ? { reasoning: capturedReasoning.blocks }
                : {}),
            }
          : {
              type: 'state.transition',
              state_type: 'openclaw.llm_output',
              value: {
                ...strings(data, [
                  'provider',
                  'model',
                  'resolvedRef',
                  'harnessId',
                ]),
                ...(responseId ? { response_id: responseId } : {}),
                content: this.snapshot(run, data?.assistantTexts),
                ...(usage ? { usage } : {}),
                ...(capturedReasoning.blocks.length > 0
                  ? { reasoning: capturedReasoning.blocks }
                  : {}),
              },
            },
      });
      if (request && exactCallId && receipt.accepted) {
        if (completed) completed.richResponseCaptured = true;
        else run.modelContentResponses.add(exactCallId);
      }
      if (!receipt.accepted)
        releaseReasoningBlocks(
          run.reasoningBlocksByResponse,
          responseId,
          reservedReasoning.keys,
        );
      if (!request)
        this.gapOnce(run, 'llm-output-call-id', 'turn_order_ambiguous', {
          type: 'model.output_call_correlation',
          ...(exactCallId ? { callId: exactCallId } : {}),
          detail: exactCallId
            ? `llm_output exposed callId ${exactCallId}, but it did not match an open model request; no model-call edge was invented.`
            : `llm_output did not expose callId while ${run.modelRequests.size} model calls were open; no model-call edge was invented.`,
        });
    });
    if (!queued) {
      if (deliveryId && !deliveryAlreadyCaptured)
        run.deliveryIds.delete(deliveryId);
      releaseReasoningBlocks(
        run.reasoningBlocksByResponse,
        responseId,
        reservedReasoning.keys,
      );
    }
  }

  private beforeMessageWrite(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const ctx = object(context);
    const message = object(data.message);
    if (message?.role !== 'assistant') return;
    const reasoning = extractExposedReasoning(message);
    const sessionKey = text(data.sessionKey) ?? text(ctx?.sessionKey);
    const owners = sessionKey ? this.sessionOwners.get(sessionKey) : undefined;
    if (!owners || owners.size !== 1) {
      for (const runId of owners ?? []) {
        const run = this.runs.get(runId);
        if (run)
          this.gapOnce(run, 'assistant-message-owner', 'turn_order_ambiguous', {
            type: 'openclaw.assistant_message_session_correlation',
            detail: `before_message_write matched ${owners?.size ?? 0} active runs; assistant evidence was not assigned by guesswork.`,
          });
      }
      this.logOnce(
        `assistant-message-owner:${sessionKey ?? 'missing'}`,
        'warn',
        'OpenClaw before_message_write assistant evidence had no unique active run owner; evidence was not reassigned.',
      );
      return;
    }
    const runId = [...owners][0];
    if (!runId) return;
    const run = this.runs.get(runId);
    if (!run) return;
    const opaqueReasoning = this.opaqueReasoningEvidence(run, message);
    if (reasoning.truncated) this.reasoningLimitGap(run);
    this.reasoningOmissionGaps(run, reasoning.omissions);
    const responseId = text(message.responseId);
    const deliveryId = responseId
      ? `before_message_write:${responseId}`
      : undefined;
    const deliveryAlreadyCaptured = Boolean(
      deliveryId && run.deliveryIds.has(deliveryId),
    );
    const reservedReasoning = reserveReasoningBlocks(
      run.reasoningBlocksByResponse,
      responseId,
      reasoning.blocks,
    );
    if (
      deliveryAlreadyCaptured &&
      reservedReasoning.blocks.length === 0 &&
      !opaqueReasoning
    )
      return;
    if (deliveryId && !deliveryAlreadyCaptured) run.deliveryIds.add(deliveryId);
    const capturedReasoning = { ...reasoning, blocks: reservedReasoning.blocks };
    const exactCallId =
      text(message.callId) ?? text(data.callId) ?? text(ctx?.callId);
    const stopReason = text(message.stopReason);
    const usage = normalizeUsage(message.usage);
    const captured = {
      ...strings(message, ['provider', 'model']),
      ...(responseId ? { response_id: responseId } : {}),
      ...(stopReason ? { stop_reason: stopReason } : {}),
      content: this.snapshot(run, visibleAssistantContent(message.content)),
      ...(capturedReasoning.blocks.length > 0
        ? { reasoning: capturedReasoning.blocks }
        : {}),
      ...(usage ? { usage } : {}),
    };
    const queued = this.enqueue(
      run,
      [message.content, reasoning.blocks.map((block) => block.text)],
      () => {
        const completed = exactCallId
          ? run.completedModelRequests.get(exactCallId)
          : undefined;
        const request = exactCallId
          ? (run.modelRequests.get(exactCallId) ?? completed?.requestRecordId)
          : undefined;
        const terminal = completed?.terminal;
        const receipt = run.source.event({
          kind: request ? 'model' : 'state',
          phase: request ? 'end' : 'event',
          name: 'openclaw.assistant.message',
          ...(exactCallId ? { nativeIdentity: exactCallId } : {}),
          ...(request ? { parentRecordId: request } : {}),
          native: {
            seam: 'before_message_write',
            ...(exactCallId ? { callId: exactCallId } : {}),
            ...(responseId ? { responseId } : {}),
            ...(opaqueReasoning ? { opaqueReasoning } : {}),
            ...(terminal
              ? { modelCallEnded: modelTerminalEvidence(terminal) }
              : {}),
          },
          semantic: request
            ? {
                type: 'model.response',
                status: terminal?.outcome === 'error' ? 'failed' : 'completed',
                call_id: exactCallId,
                ...(text(terminal?.finishReason)
                  ? { finish_reason: text(terminal?.finishReason) }
                  : {}),
                ...strings(terminal, [
                  'provider',
                  'model',
                  'api',
                  'transport',
                  'outcome',
                  'errorCategory',
                  'failureKind',
                ]),
                ...numbers(terminal, [
                  'durationMs',
                  'requestPayloadBytes',
                  'responseStreamBytes',
                  'timeToFirstByteMs',
                ]),
                ...captured,
              }
            : {
                type: 'state.transition',
                state_type: 'openclaw.assistant_message',
                value: captured,
              },
        });
        if (request && exactCallId && receipt.accepted) {
          if (completed) completed.richResponseCaptured = true;
          else run.modelContentResponses.add(exactCallId);
        }
        if (!receipt.accepted)
          releaseReasoningBlocks(
            run.reasoningBlocksByResponse,
            responseId,
            reservedReasoning.keys,
          );
        if (!request)
          this.gapOnce(
            run,
            `assistant-message-call-id:${exactCallId ?? 'missing'}`,
            'turn_order_ambiguous',
            {
              type: 'model.output_call_correlation',
              ...(exactCallId ? { callId: exactCallId } : {}),
              hook: 'before_message_write',
              detail: exactCallId
                ? `before_message_write exposed callId ${exactCallId}, but it did not match an open model request; no model-call edge was invented.`
                : 'before_message_write did not expose callId; no model-call edge was invented.',
            },
          );
      },
    );
    if (!queued) {
      if (deliveryId && !deliveryAlreadyCaptured)
        run.deliveryIds.delete(deliveryId);
      releaseReasoningBlocks(
        run.reasoningBlocksByResponse,
        responseId,
        reservedReasoning.keys,
      );
    }
  }

  private beforeToolCall(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const ctx = object(context);
    const run = this.ensureRun(data, ctx);
    if (!run) return;
    const callId = text(data?.toolCallId) ?? text(ctx?.toolCallId);
    const name = text(data?.toolName) ?? text(ctx?.toolName) ?? 'unknown';
    this.enqueue(run, data, () => {
      if (!callId)
        this.gapOnce(run, `tool-start:${name}`, 'unsupported_native_value', {
          type: 'tool.call_id',
          hook: 'before_tool_call',
          tool: name,
        });
      const receipt = run.source.event({
        kind: 'tool',
        phase: 'start',
        name,
        ...(callId ? { nativeIdentity: callId } : {}),
        native: this.snapshot(
          run,
          pick(data, ['toolName', 'toolCallId', 'params', 'runId']),
        ),
        semantic: {
          type: 'tool.execution',
          name,
          input: this.snapshot(run, data?.params),
          ...(callId ? { call_id: callId } : {}),
        },
      });
      if (callId && receipt.accepted)
        run.toolCalls.set(callId, receipt.recordId);
    });
  }

  private afterToolCall(event: unknown, context: unknown): void {
    const data = object(event) ?? {};
    const ctx = object(context);
    const run = this.ensureRun(data, ctx);
    if (!run) return;
    const callId = text(data?.toolCallId) ?? text(ctx?.toolCallId);
    const name = text(data?.toolName) ?? text(ctx?.toolName) ?? 'unknown';
    this.enqueue(run, data, () => {
      const parentRecordId = callId ? run.toolCalls.get(callId) : undefined;
      if (!callId || !parentRecordId)
        this.gapOnce(
          run,
          `tool-end:${callId ?? name}`,
          'turn_order_ambiguous',
          {
            type: 'tool.call_result',
            hook: 'after_tool_call',
            callId,
            tool: name,
          },
        );
      run.source.event({
        kind: 'tool',
        phase: data?.error ? 'error' : 'end',
        name,
        ...(callId ? { nativeIdentity: callId } : {}),
        ...(parentRecordId ? { parentRecordId } : {}),
        native: this.snapshot(
          run,
          pick(data, [
            'toolName',
            'toolCallId',
            'result',
            'error',
            'durationMs',
            'runId',
          ]),
        ),
        semantic: {
          type: 'tool.result',
          name,
          status: data?.error ? 'failed' : 'succeeded',
          ...(callId ? { call_id: callId } : {}),
          output: this.snapshot(run, data?.result),
          ...(data?.error
            ? { error: this.snapshot(run, structuredError(data.error)) }
            : {}),
        },
      });
      if (callId) run.toolCalls.delete(callId);
    });
  }

  private messageReceived(event: unknown, context: unknown): void {
    this.message(event, context, 'user', 'openclaw.message.received');
  }

  private messageSent(event: unknown, context: unknown): void {
    this.message(event, context, 'assistant', 'openclaw.message.sent');
  }

  private message(
    event: unknown,
    context: unknown,
    role: 'user' | 'assistant',
    name: string,
  ): void {
    const data = object(event) ?? {};
    const run = this.existingRun(data, context);
    if (!run) return;
    this.enqueue(run, data, () => {
      const messageId = text(data.messageId);
      run.source.event({
        kind: 'model',
        phase: 'event',
        name,
        ...(messageId ? { nativeIdentity: messageId } : {}),
        native: this.snapshot(
          run,
          pick(data, [
            'content',
            'messageId',
            'threadId',
            'success',
            'error',
            'runId',
          ]),
        ),
        semantic: {
          type: 'message',
          role,
          content: this.snapshot(run, data.content),
          ...booleans(data, ['success']),
        },
      });
    });
  }

  private subagentSpawned(event: unknown, context: unknown): void {
    this.subagent(event, context, 'start', 'openclaw.subagent.spawned');
  }

  private subagentEnded(event: unknown, context: unknown): void {
    this.subagent(event, context, 'end', 'openclaw.subagent.ended');
  }

  private subagent(
    event: unknown,
    context: unknown,
    phase: 'start' | 'end',
    name: string,
  ): void {
    const data = object(event) ?? {};
    const ctx = object(context) ?? {};
    const requesterSessionKey = text(ctx.requesterSessionKey);
    const owners = requesterSessionKey
      ? this.sessionOwners.get(requesterSessionKey)
      : undefined;
    const parentRunId = owners?.size === 1 ? [...owners][0] : undefined;
    const run = parentRunId
      ? this.runs.get(parentRunId)
      : requesterSessionKey
        ? undefined
        : this.existingRun(data, context);
    if (!run) {
      this.logOnce(
        `orphan-subagent:${phase}`,
        'warn',
        'OpenClaw subagent evidence had no unique active requester run; no parent relation was invented.',
      );
      return;
    }
    this.enqueue(run, data, () => {
      const childRunId = text(data.runId);
      const childSessionKey =
        text(data.childSessionKey) ?? text(data.targetSessionKey);
      const nativeIdentity =
        childRunId ??
        (childSessionKey && this.config
          ? pseudonymizeSession(childSessionKey, this.config.identityKey)
          : undefined);
      if (!nativeIdentity) {
        this.gapOnce(run, `subagent:${phase}`, 'unsupported_native_value', {
          type: 'agent.scope',
          detail: `subagent ${phase} omitted an exact child identity`,
        });
        return;
      }
      const parentRecordId =
        phase === 'end' ? run.subagentScopes.get(nativeIdentity) : undefined;
      const receipt = run.source.event({
        kind: 'lifecycle',
        phase,
        name,
        nativeIdentity,
        ...(parentRecordId ? { parentRecordId } : {}),
        native: this.snapshot(run, data),
        semantic: {
          type: 'agent.scope',
          scope_type: 'agent',
          scope_id: nativeIdentity,
          status:
            phase === 'start' ? 'unknown' : normalizeScopeStatus(data.outcome),
          ...(data.error
            ? { error: this.snapshot(run, structuredError(data.error)) }
            : {}),
        },
      });
      if (receipt.accepted && phase === 'start')
        run.subagentScopes.set(nativeIdentity, receipt.recordId);
      if (phase === 'end') run.subagentScopes.delete(nativeIdentity);
    });
  }

  private agentEnd(
    event: unknown,
    context: unknown,
  ): Promise<void> | undefined {
    const data = object(event) ?? {};
    const run = this.ensureRun(data, context);
    if (!run) return undefined;
    return this.finishRun(run, data ?? {});
  }

  private finishRun(
    run: Run,
    terminal: Record<string, unknown>,
  ): Promise<void> {
    if (run.terminal) return run.terminal;
    run.closing = true;
    run.terminal = this.finishRunOnce(run, terminal).catch((error: unknown) => {
      this.logError(`capture finalization failed for run ${run.runId}`, error);
    });
    return run.terminal;
  }

  private async finishRunOnce(
    run: Run,
    terminal: Record<string, unknown>,
  ): Promise<void> {
    if (this.terminalGraceMs > 0) await delay(this.terminalGraceMs);
    run.terminalQueued = true;
    this.enqueue(
      run,
      terminal,
      () => {
        this.captureTerminalReasoningFallback(run, terminal);
        for (const [callId, completed] of run.completedModelRequests) {
          if (!completed.richResponseCaptured) {
            this.emitModelCallTerminal(
              run,
              completed.terminal,
              callId,
              completed.requestRecordId,
              false,
            );
          }
        }
        run.completedModelRequests.clear();
        for (const [callId] of run.modelRequests)
          run.source.gap('uncertain_tail', {
            type: 'model.response',
            callId,
            detail: 'run ended before model_call_ended',
          });
        for (const [callId] of run.toolCalls)
          run.source.gap('uncertain_tail', {
            type: 'tool.result',
            callId,
            detail: 'run ended before after_tool_call',
          });
        run.source.event({
          kind: 'lifecycle',
          phase:
            terminal.success === true
              ? 'end'
              : terminal.error
                ? 'error'
                : 'cancelled',
          name: 'openclaw.agent.run',
          nativeIdentity: run.runId,
          native: this.snapshot(
            run,
            pick(terminal, ['runId', 'success', 'error', 'durationMs']),
          ),
          semantic: {
            type: 'agent.run',
            status:
              terminal.success === true
                ? 'completed'
                : terminal.error
                  ? 'failed'
                  : 'cancelled',
            ...(terminal.error
              ? { error: this.snapshot(run, structuredError(terminal.error)) }
              : {}),
          },
        });
      },
      true,
    );

    await withDeadline(
      (async () => {
        await run.tail;
        const status = await run.capture.shutdown();
        if (
          status.state !== 'closed' ||
          status.rejected !== 0 ||
          status.lastError ||
          !status.artifactPath
        ) {
          throw new Error(
            `capture did not seal cleanly (state=${status.state}, rejected=${status.rejected}, lastError=${status.lastError ?? 'none'})`,
          );
        }
        try {
          const receipt = await this.getUploader().enqueueArtifact(
            status.artifactPath,
            this.config?.outputDirectory
              ? {
                  removeSourceAfterAdmissionFrom:
                    this.config.outputDirectory,
                }
              : undefined,
          );
          if (receipt.state === 'awaiting_spool_admission') {
            this.logOnce(
              `spool-admission:${run.runId}`,
              'error',
              `Semantic Layer spool is full; the sealed capture for run ${run.runId} is awaiting local spool admission. The agent was unaffected.`,
            );
          }
        } catch (error) {
          this.logError(
            `sealed capture could not be staged for run ${run.runId}; the local artifact was retained and the agent was unaffected`,
            error,
          );
        }
      })(),
      this.shutdownDeadlineMs,
      `run ${run.runId} capture shutdown`,
    );
    run.closed = true;
    this.runs.delete(run.runId);
    for (const sessionKey of run.sessionKeys) {
      const owners = this.sessionOwners.get(sessionKey);
      owners?.delete(run.runId);
      if (owners?.size === 0) this.sessionOwners.delete(sessionKey);
    }
  }

  private reasoningLimitGap(run: Run): void {
    this.gapOnce(run, 'reasoning-capture-limit', 'unsupported_native_value', {
      type: 'openclaw.reasoning_capture_limit',
      detail:
        'Provider-exposed reasoning exceeded a bounded capture limit; retained reasoning is explicitly truncated.',
    });
  }

  private opaqueReasoningEvidence(
    run: Run,
    value: unknown,
  ): unknown | undefined {
    const carriers = extractOpaqueReasoningCarriers(value);
    if (carriers.truncated) this.reasoningLimitGap(run);
    return carriers.values.length > 0
      ? this.snapshot(run, carriers.values)
      : undefined;
  }

  private captureTerminalReasoningFallback(
    run: Run,
    terminal: Record<string, unknown>,
  ): void {
    if (!Array.isArray(terminal.messages)) return;
    let capturedFallback = false;
    for (const candidate of terminal.messages) {
      const message = object(candidate);
      if (message?.role !== 'assistant') continue;
      const reasoning = extractExposedReasoning(message);
      if (reasoning.truncated) this.reasoningLimitGap(run);
      this.reasoningOmissionGaps(run, reasoning.omissions);
      const responseId = text(message.responseId);
      const opaqueReasoning = this.opaqueReasoningEvidence(run, message);
      const reservedReasoning = reserveReasoningBlocks(
        run.reasoningBlocksByResponse,
        responseId,
        reasoning.blocks,
      );
      if (reservedReasoning.blocks.length === 0 && !opaqueReasoning) continue;
      const receipt = run.source.event({
        kind: 'state',
        phase: 'event',
        name: 'openclaw.agent.final_reasoning',
        native: {
          seam: 'agent_end.messages',
          ...(responseId ? { responseId } : {}),
          ...(opaqueReasoning ? { opaqueReasoning } : {}),
        },
        semantic: {
          type: 'state.transition',
          state_type: 'openclaw.agent_end_reasoning',
          value: {
            ...strings(message, ['provider', 'model']),
            ...(responseId ? { response_id: responseId } : {}),
            ...(reservedReasoning.blocks.length > 0
              ? { reasoning: reservedReasoning.blocks }
              : {}),
          },
        },
      });
      if (receipt.accepted) {
        capturedFallback = true;
      } else
        releaseReasoningBlocks(
          run.reasoningBlocksByResponse,
          responseId,
          reservedReasoning.keys,
        );
    }
    if (capturedFallback)
      this.gapOnce(run, 'agent-end-reasoning-call-id', 'turn_order_ambiguous', {
        type: 'model.output_call_correlation',
        hook: 'agent_end',
        detail:
          'agent_end.messages exposed fallback reasoning without a model call identity; no model-call edge was invented.',
      });
  }

  private reasoningOmissionGaps(
    run: Run,
    omissions: ReasoningOmission[],
  ): void {
    for (const omission of omissions) {
      this.gapOnce(run, `reasoning-${omission}`, 'unsupported_native_value', {
        type: `openclaw.reasoning_${omission}`,
        detail:
          omission === 'redacted'
            ? 'OpenClaw exposed an explicitly redacted reasoning block; its body was omitted.'
            : omission === 'encrypted'
              ? 'OpenClaw exposed opaque reasoning; bounded carrier evidence was retained only in native data and omitted from readable reasoning.'
              : 'OpenClaw exposed a malformed reasoning block; safe response evidence was retained without inventing reasoning text.',
      });
    }
  }

  private ensureRun(
    event: Record<string, unknown> | undefined,
    context: unknown,
    input?: unknown,
  ): Run | undefined {
    const ctx = object(context);
    const runId = text(event?.runId) ?? text(ctx?.runId);
    if (!runId) {
      this.logOnce(
        'missing-run-id',
        'warn',
        'OpenClaw event omitted runId; event was not attached to another run.',
      );
      return undefined;
    }
    const existing = this.runs.get(runId);
    if (existing) {
      this.rememberSessionKeys(existing, event, ctx);
      return existing;
    }
    if (!this.config) return undefined;
    const sessionId = text(event?.sessionId) ?? text(ctx?.sessionId);
    if (!sessionId) {
      this.logOnce(
        `missing-session:${runId}`,
        'warn',
        `OpenClaw run ${runId} omitted sessionId; capture was skipped to avoid inventing conversation identity.`,
      );
      return undefined;
    }
    try {
      const initialSessionKeys = [
        text(event?.sessionKey),
        text(ctx?.sessionKey),
      ].filter((value): value is string => Boolean(value));
      this.reportSpoolPressure();
      const conversationId = pseudonymizeSession(
        sessionId,
        this.config.identityKey,
      );
      // OpenClaw 2026.5.5 may expose one value as both runId and sessionId.
      // The native run identity is structural trace evidence and cannot be scrubbed.
      const privateSessionValues = [sessionId, ...initialSessionKeys].filter(
        (value) => value !== runId,
      );
      const capture = this.dependencies.createRunCapture({
        serviceName: this.config.serviceName,
        installationId: this.config.installationId,
        identityKey: this.config.identityKey,
        ...(this.config.outputDirectory
          ? { output: this.config.outputDirectory }
          : {}),
        secretValues: [
          this.config.ingestKey,
          this.config.identityKey,
          ...privateSessionValues,
        ],
        identityMode: 'raw',
        shutdownDeadlineMs: this.shutdownDeadlineMs,
        queueCapacityBytes: 64 * 1024 * 1024,
      });
      const correlationTaskId = this.pendingCorrelation(runId);
      const source = createRunSource({
        runId,
        conversationId,
        hostVersion: this.api.runtime?.version ?? 'unknown',
        qualification: hostQualification(
          this.api.runtime?.version,
          this.unavailableHooks,
        ),
        ...(correlationTaskId
          ? {
              correlation: {
                taskId: correlationTaskId,
                execution: { system: 'openclaw', runId, rootRunId: runId },
              },
            }
          : {}),
        input: this.snapshot(undefined, input),
        unavailableHooks: [...this.unavailableHooks],
      });
      const lifecycle = capture.installSource(source.source);
      const started = source.start();
      if (!started.accepted) {
        throw new Error('OpenClaw run root was not admitted by capture.');
      }
      if (correlationTaskId) {
        this.markCorrelationConsumed(runId, correlationTaskId);
      }
      const run: Run = {
        runId,
        sessionKeys: new Set(),
        source,
        capture,
        lifecycle,
        tail: Promise.resolve(),
        queuedBytes: 0,
        closing: false,
        terminalQueued: false,
        closed: false,
        modelRequests: new Map(),
        modelContentResponses: new Set(),
        completedModelRequests: new Map(),
        toolCalls: new Map(),
        subagentScopes: new Map(),
        deliveryIds: new Set(),
        reasoningBlocksByResponse: new Map(),
        gaps: new Set(),
      };
      this.runs.set(runId, run);
      this.rememberSessionKeys(run, event, ctx);
      if ((this.api.runtime?.version ?? '') === '2026.5.5') {
        source.gap('unsupported_native_value', {
          type: 'openclaw.hook.unavailable',
          hook: 'before_agent_run',
          substitute: 'before_model_resolve',
          detail:
            'OpenClaw 2026.5.5 does not expose before_agent_run; before_model_resolve was used as the run-start seam.',
        });
      }
      return run;
    } catch (error) {
      this.logError(`failed to initialize capture for run ${runId}`, error);
      return undefined;
    }
  }

  private pendingCorrelation(runId: string): string | undefined {
    this.pruneExpiredCorrelations();
    return this.pendingCorrelations.get(runId)?.taskId;
  }

  private markCorrelationConsumed(runId: string, taskId: string): void {
    const binding = this.pendingCorrelations.get(runId);
    if (!binding || binding.taskId !== taskId) return;
    this.pendingCorrelations.delete(runId);
    if (this.consumedCorrelations.size >= MAX_PENDING_CORRELATIONS) {
      const oldest = this.consumedCorrelations.keys().next().value;
      if (typeof oldest === 'string') this.consumedCorrelations.delete(oldest);
    }
    this.consumedCorrelations.set(
      runId,
      Date.now() + CORRELATION_BINDING_TTL_MS,
    );
  }

  private pruneExpiredCorrelations(): void {
    const now = Date.now();
    for (const [runId, binding] of this.pendingCorrelations) {
      if (binding.expiresAt <= now) this.pendingCorrelations.delete(runId);
    }
    for (const [runId, expiresAt] of this.consumedCorrelations) {
      if (expiresAt <= now) this.consumedCorrelations.delete(runId);
    }
  }

  private existingRun(
    event: Record<string, unknown> | undefined,
    context: unknown,
  ): Run | undefined {
    const ctx = object(context);
    const runId = text(event?.runId) ?? text(ctx?.runId);
    if (!runId) {
      this.logOnce(
        'orphan-event',
        'warn',
        'OpenClaw event had no runId; capture emitted no guessed relation.',
      );
      return undefined;
    }
    const run = this.runs.get(runId);
    if (!run)
      this.logOnce(
        `orphan-run:${runId}`,
        'warn',
        `OpenClaw event referenced inactive run ${runId}; capture emitted no guessed relation.`,
      );
    return run;
  }

  private rememberSessionKeys(
    run: Run,
    event: Record<string, unknown> | undefined,
    context: Record<string, unknown> | undefined,
  ): void {
    for (const sessionKey of [
      text(event?.sessionKey),
      text(context?.sessionKey),
    ]) {
      if (!sessionKey) continue;
      run.sessionKeys.add(sessionKey);
      const owners = this.sessionOwners.get(sessionKey) ?? new Set<string>();
      owners.add(run.runId);
      this.sessionOwners.set(sessionKey, owners);
    }
  }

  private enqueue(
    run: Run,
    value: unknown,
    operation: () => void | Promise<void>,
    terminal = false,
  ): boolean {
    if (run.closed || (run.terminalQueued && !terminal)) {
      this.logOnce(
        `late:${run.runId}`,
        'warn',
        `OpenClaw emitted an event after terminal capture for run ${run.runId}; it was not reassigned.`,
      );
      return false;
    }
    const bytes = estimateBytes(this.snapshot(run, value));
    if (!terminal && run.queuedBytes + bytes > this.queueCapacityBytes) {
      this.gapOnce(
        run,
        'queue-overflow',
        'queue_backpressure_drop',
        {
          type: 'openclaw.hook_event',
          bytes,
        },
        true,
      );
      return false;
    }
    run.queuedBytes += bytes;
    run.tail = run.tail
      .then(operation)
      .catch((error: unknown) => {
        this.logError(`capture event failed for run ${run.runId}`, error);
      })
      .finally(() => {
        run.queuedBytes = Math.max(0, run.queuedBytes - bytes);
      });
    return true;
  }

  private gapOnce(
    run: Run,
    key: string,
    reason: string,
    detail: Record<string, unknown>,
    _immediate = false,
  ): void {
    if (run.gaps.has(key)) return;
    run.gaps.add(key);
    run.source.gap(
      reason,
      this.snapshot(undefined, detail) as Record<string, unknown>,
    );
  }

  private snapshot(run: Run | undefined, value: unknown): unknown {
    const snapshot = boundedSnapshot(value, this.config?.identityKey);
    if (run && snapshot.limits.length > 0) {
      this.gapOnce(
        run,
        `snapshot:${snapshot.limits.join(',')}`,
        'unsupported_native_value',
        {
          type: 'openclaw.snapshot_limit',
          limits: snapshot.limits,
          detail:
            'OpenClaw evidence exceeded the bounded capture snapshot and was truncated or omitted.',
        },
      );
    }
    return snapshot.value;
  }

  private getUploader(): CloudUploaderLike {
    if (this.uploader) return this.uploader;
    if (!this.config) throw new Error('plugin configuration is unresolved');
    this.uploader = this.dependencies.createUploader({
      endpoint: this.config.endpoint,
      ingestKey: this.config.ingestKey,
      installationId: this.config.installationId,
      ...(this.config.spoolDirectory
        ? { spoolDirectory: this.config.spoolDirectory }
        : {}),
      ...(this.config.maxSpoolBytes
        ? { maxSpoolBytes: this.config.maxSpoolBytes }
        : {}),
    });
    return this.uploader;
  }

  private reportSpoolPressure(): void {
    try {
      const spool = this.getUploader().status();
      if ('pressure' in spool && spool.pressure === 'full') {
        this.logOnce(
          'spool-full',
          'error',
          'Semantic Layer spool is full; local run capture continues and sealed bundles remain available for later admission. The agent is unaffected.',
        );
      }
    } catch (error) {
      this.logError(
        'cloud spool status could not be read; local run capture continues',
        error,
      );
    }
  }

  private logOnce(key: string, level: 'warn' | 'error', message: string): void {
    if (this.diagnosticKeys.has(key)) return;
    this.diagnosticKeys.add(key);
    this.api.logger[level](message);
  }

  private logError(message: string, error: unknown): void {
    this.api.logger.error(`${message}: ${errorMessage(error)}`);
  }
}

function validCorrelationBindingParams(
  value: unknown,
): value is { runId: string; taskId: string } {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false;
  if (Object.keys(value).sort().join(',') !== 'runId,taskId') return false;
  const input = value as Record<string, unknown>;
  return validCorrelationText(input.runId) && validCorrelationText(input.taskId);
}

function validCorrelationText(value: unknown): value is string {
  return typeof value === 'string'
    && value.length > 0
    && value.trim() === value
    && [...value].length <= 512
    && !value.includes('\0');
}

export function pseudonymizeSession(
  sessionId: string,
  identityKey: string,
): string {
  return `hmac-sha256:${createHmac('sha256', identityKey).update(sessionId, 'utf8').digest('hex')}`;
}

export function resolveConfig(
  value: unknown,
): ResolvedPluginConfig | undefined {
  const config = object(value);
  const endpoint = text(config?.endpoint);
  const ingestKey = text(config?.ingestKey);
  const identityKey = text(config?.identityKey);
  const installationId = text(config?.installationId);
  const serviceName = text(config?.serviceName);
  if (
    !endpoint ||
    !ingestKey ||
    !identityKey ||
    identityKey.length < 32 ||
    !installationId ||
    !/^install_[A-Za-z0-9_-]{22,128}$/u.test(installationId) ||
    !serviceName
  )
    return undefined;
  let url: URL;
  try {
    url = new URL(endpoint);
  } catch {
    return undefined;
  }
  if (url.protocol !== 'https:') return undefined;
  const outputDirectory = text(config?.outputDirectory);
  const spoolDirectory = text(config?.spoolDirectory);
  const maxSpoolBytes =
    typeof config?.maxSpoolBytes === 'number' &&
    Number.isSafeInteger(config.maxSpoolBytes)
      ? config.maxSpoolBytes
      : undefined;
  return {
    endpoint,
    ingestKey,
    identityKey,
    installationId,
    serviceName,
    ...(outputDirectory ? { outputDirectory } : {}),
    ...(spoolDirectory ? { spoolDirectory } : {}),
    ...(maxSpoolBytes ? { maxSpoolBytes } : {}),
  };
}

function boundedSnapshot(
  value: unknown,
  identityKey?: string,
): { value: unknown; limits: SnapshotLimit[] } {
  const limits = new Set<SnapshotLimit>();
  const ancestors = new WeakSet<object>();
  let nodes = 0;
  let stringBytes = 0;

  const mark = (limit: SnapshotLimit, placeholder: string): string => {
    limits.add(limit);
    return placeholder;
  };

  const boundedString = (
    input: string,
    perStringLimit = SNAPSHOT_MAX_STRING_BYTES,
  ): string => {
    const remaining = Math.max(0, SNAPSHOT_MAX_BYTES - stringBytes);
    const limit = Math.min(perStringLimit, remaining);
    if (limit === 0) return mark('bytes', '[byte-limited]');
    let end = Math.min(input.length, limit);
    let candidate = input.slice(0, end);
    while (Buffer.byteLength(candidate, 'utf8') > limit && end > 0) {
      end = Math.floor(end / 2);
      candidate = input.slice(0, end);
    }
    stringBytes += Buffer.byteLength(candidate, 'utf8');
    if (candidate.length < input.length) {
      limits.add(limit < perStringLimit ? 'bytes' : 'string');
      return `${candidate}[truncated]`;
    }
    return candidate;
  };

  const visit = (input: unknown, depth: number): unknown => {
    if (nodes >= SNAPSHOT_MAX_NODES) return mark('nodes', '[node-limited]');
    nodes += 1;
    if (depth > SNAPSHOT_MAX_DEPTH) return mark('depth', '[depth-limited]');
    if (
      input === null ||
      typeof input === 'boolean' ||
      typeof input === 'number'
    )
      return input;
    if (typeof input === 'string') return boundedString(input);
    if (typeof input === 'bigint') return boundedString(input.toString(), 128);
    if (typeof input === 'undefined') return '[undefined]';
    if (typeof input === 'symbol') return '[symbol]';
    if (typeof input === 'function') return '[function]';
    if (!input || typeof input !== 'object') return '[unsupported]';
    if (ancestors.has(input)) return mark('cycle', '[cycle]');
    ancestors.add(input);
    try {
      if (Array.isArray(input)) {
        if (input.length > SNAPSHOT_MAX_WIDTH) limits.add('width');
        const output: unknown[] = [];
        for (
          let index = 0;
          index < Math.min(input.length, SNAPSHOT_MAX_WIDTH);
          index += 1
        ) {
          if (stringBytes >= SNAPSHOT_MAX_BYTES) {
            output.push(mark('bytes', '[byte-limited]'));
            break;
          }
          const descriptor = Object.getOwnPropertyDescriptor(
            input,
            String(index),
          );
          if (!descriptor || !('value' in descriptor)) {
            output.push(mark('unavailable', '[accessor-omitted]'));
            continue;
          }
          output.push(visit(descriptor.value, depth + 1));
        }
        return output;
      }

      let keys: string[];
      try {
        keys = Object.keys(input);
      } catch {
        return mark('unavailable', '[object-unavailable]');
      }
      if (keys.length > SNAPSHOT_MAX_WIDTH) limits.add('width');
      const output: Record<string, unknown> = {};
      for (const originalKey of keys.slice(0, SNAPSHOT_MAX_WIDTH)) {
        if (stringBytes >= SNAPSHOT_MAX_BYTES) {
          output['[byte-limited]'] = true;
          limits.add('bytes');
          break;
        }
        const key = boundedString(originalKey, 1_024);
        const normalized = normalizeKey(originalKey);
        if (
          PRIVATE_SNAPSHOT_KEYS.has(normalized) ||
          normalized.endsWith('signature')
        )
          continue;
        let descriptor: PropertyDescriptor | undefined;
        try {
          descriptor = Object.getOwnPropertyDescriptor(input, originalKey);
        } catch {
          output[key] = mark('unavailable', '[property-unavailable]');
          continue;
        }
        if (!descriptor || !('value' in descriptor)) {
          output[key] = mark('unavailable', '[accessor-omitted]');
          continue;
        }
        const item = descriptor.value;
        if (
          identityKey &&
          isSessionIdentityKey(normalized) &&
          typeof item === 'string'
        ) {
          const sessionInput =
            item.length <= 16_384
              ? item
              : `${item.slice(0, 16_384)}[length:${item.length}]`;
          if (sessionInput !== item) limits.add('string');
          output[key] = pseudonymizeSession(sessionInput, identityKey);
          continue;
        }
        output[key] = visit(item, depth + 1);
      }
      return output;
    } finally {
      ancestors.delete(input);
    }
  };

  let snapshotValue: unknown;
  try {
    snapshotValue = visit(value, 0);
  } catch {
    snapshotValue = mark('unavailable', '[snapshot-unavailable]');
  }
  return { value: snapshotValue, limits: [...limits].sort() };
}

function isSessionIdentityKey(normalizedKey: string): boolean {
  return (
    normalizedKey === 'session_id' ||
    normalizedKey === 'sessionid' ||
    normalizedKey === 'session_key' ||
    normalizedKey === 'sessionkey' ||
    normalizedKey === 'child_session_key' ||
    normalizedKey === 'childsessionkey' ||
    normalizedKey === 'target_session_key' ||
    normalizedKey === 'targetsessionkey' ||
    normalizedKey === 'requester_session_key' ||
    normalizedKey === 'requestersessionkey'
  );
}

function extractExposedReasoning(value: unknown): ExposedReasoning {
  const output: ExposedReasoningBlock[] = [];
  const omissions = new Set<ReasoningOmission>();
  const ancestors = new WeakSet<object>();
  let nodes = 0;
  let bytes = 0;
  let truncated = false;
  const add = (type: 'text' | 'summary', candidate: unknown): void => {
    if (typeof candidate !== 'string' || candidate.length === 0) return;
    if (output.length >= SNAPSHOT_MAX_WIDTH || bytes >= SNAPSHOT_MAX_BYTES) {
      truncated = true;
      return;
    }
    const remaining = Math.min(
      SNAPSHOT_MAX_STRING_BYTES,
      SNAPSHOT_MAX_BYTES - bytes,
    );
    const encoded = Buffer.from(candidate, 'utf8');
    let retained = candidate;
    if (encoded.byteLength > remaining) {
      retained = encoded
        .subarray(0, remaining)
        .toString('utf8')
        .replace(/\uFFFD$/u, '');
      truncated = true;
    }
    if (retained.length === 0) return;
    bytes += Buffer.byteLength(retained, 'utf8');
    output.push({ type, text: retained });
  };
  const addObserved = (type: 'text' | 'summary', candidate: unknown): void => {
    if (typeof candidate !== 'string' || candidate.length === 0) {
      omissions.add('malformed');
      return;
    }
    add(type, candidate);
  };
  const own = (input: object, key: string): unknown => {
    try {
      const descriptor = Object.getOwnPropertyDescriptor(input, key);
      return descriptor && 'value' in descriptor ? descriptor.value : undefined;
    } catch {
      truncated = true;
      return undefined;
    }
  };
  const readBlock = (input: unknown): void => {
    if (!input || typeof input !== 'object' || ancestors.has(input)) return;
    if (nodes >= SNAPSHOT_MAX_NODES) {
      truncated = true;
      return;
    }
    nodes += 1;
    ancestors.add(input);
    try {
      if (own(input, 'redacted') === true) {
        omissions.add('redacted');
        return;
      }
      const blockType = own(input, 'type');
      const reasoningShaped = reasoningShapedBlock(
        typeof blockType === 'string' ? blockType : undefined,
        own(input, 'thought') === true,
      );
      if (
        (typeof blockType === 'string' && blockType.includes('encrypted')) ||
        hasOpaqueReasoningCarrier(
          input,
          typeof blockType === 'string' ? blockType : undefined,
          reasoningShaped,
        )
      ) {
        omissions.add('encrypted');
        if (typeof blockType === 'string' && blockType.includes('encrypted'))
          return;
      }
      if (
        blockType === 'summary' ||
        blockType === 'reasoning_summary' ||
        blockType === 'reasoning.summary' ||
        blockType === 'summary_text'
      ) {
        addObserved('summary', own(input, 'text') ?? own(input, 'summary'));
      } else if (
        blockType === 'thinking' ||
        blockType === 'reasoning.text' ||
        (blockType === 'text' && own(input, 'thought') === true)
      ) {
        addObserved(
          'text',
          own(input, 'text') ??
            own(input, 'thinking') ??
            own(input, 'reasoning'),
        );
      } else if (blockType === 'reasoning') {
        const initialBlocks = output.length;
        const summary = own(input, 'summary');
        if (Array.isArray(summary)) {
          if (summary.length > SNAPSHOT_MAX_WIDTH) truncated = true;
          for (const block of summary.slice(0, SNAPSHOT_MAX_WIDTH))
            readBlock(block);
        } else if (summary !== undefined) {
          add('summary', summary);
        }
        const content = own(input, 'content');
        if (Array.isArray(content)) {
          if (content.length > SNAPSHOT_MAX_WIDTH) truncated = true;
          for (const block of content.slice(0, SNAPSHOT_MAX_WIDTH))
            readBlock(block);
        } else {
          const raw = own(input, 'text') ?? own(input, 'reasoning');
          if (raw !== undefined) add('text', raw);
        }
        if (output.length === initialBlocks && !truncated)
          omissions.add('malformed');
      }
    } finally {
      ancestors.delete(input);
    }
  };
  if (!value || typeof value !== 'object') {
    return { blocks: output, truncated, omissions: [...omissions].sort() };
  }
  const containers = [
    own(value, 'content'),
    own(value, 'reasoning'),
    own(value, 'reasoning_details'),
    own(value, 'reasoningDetails'),
  ];
  for (const container of containers) {
    if (!Array.isArray(container)) continue;
    if (container.length > SNAPSHOT_MAX_WIDTH) truncated = true;
    for (const block of container.slice(0, SNAPSHOT_MAX_WIDTH))
      readBlock(block);
  }
  add(
    'text',
    typeof own(value, 'reasoning') === 'string'
      ? own(value, 'reasoning')
      : undefined,
  );
  add(
    'text',
    own(value, 'reasoning_content') ?? own(value, 'reasoningContent'),
  );
  add('summary', own(value, 'reasoningSummary'));
  return { blocks: output, truncated, omissions: [...omissions].sort() };
}

function reserveReasoningBlocks(
  captured: Map<string, Map<string, number>>,
  responseId: string | undefined,
  blocks: ExposedReasoningBlock[],
): { blocks: ExposedReasoningBlock[]; keys: string[] } {
  if (!responseId) return { blocks, keys: [] };
  const seen = captured.get(responseId) ?? new Map<string, number>();
  const observed = new Map<string, number>();
  const reservedBlocks: ExposedReasoningBlock[] = [];
  const keys: string[] = [];
  for (const block of blocks) {
    const key = `${block.type}\u0000${block.text}`;
    const occurrence = (observed.get(key) ?? 0) + 1;
    observed.set(key, occurrence);
    const previousCount = seen.get(key) ?? 0;
    if (occurrence <= previousCount) continue;
    seen.set(key, previousCount + 1);
    keys.push(key);
    reservedBlocks.push(block);
  }
  if (seen.size > 0) captured.set(responseId, seen);
  return { blocks: reservedBlocks, keys };
}

function releaseReasoningBlocks(
  captured: Map<string, Map<string, number>>,
  responseId: string | undefined,
  keys: string[],
): void {
  if (!responseId || keys.length === 0) return;
  const seen = captured.get(responseId);
  if (!seen) return;
  for (const key of keys) {
    const count = seen.get(key) ?? 0;
    if (count <= 1) seen.delete(key);
    else seen.set(key, count - 1);
  }
  if (seen.size === 0) captured.delete(responseId);
}

function extractOpaqueReasoningCarriers(value: unknown): {
  values: Array<{ key: string; value: unknown }>;
  truncated: boolean;
} {
  const values: Array<{ key: string; value: unknown }> = [];
  const ancestors = new WeakSet<object>();
  let nodes = 0;
  let truncated = false;
  const visit = (input: unknown, depth: number): void => {
    if (!input || typeof input !== 'object' || ancestors.has(input)) return;
    if (
      depth > SNAPSHOT_MAX_DEPTH ||
      nodes >= SNAPSHOT_MAX_NODES ||
      values.length >= SNAPSHOT_MAX_WIDTH
    ) {
      truncated = true;
      return;
    }
    nodes += 1;
    ancestors.add(input);
    try {
      let keys: string[];
      try {
        keys = Object.keys(input);
      } catch {
        truncated = true;
        return;
      }
      if (keys.length > SNAPSHOT_MAX_WIDTH) truncated = true;
      let typeDescriptor: PropertyDescriptor | undefined;
      try {
        typeDescriptor = Object.getOwnPropertyDescriptor(input, 'type');
      } catch {
        truncated = true;
      }
      const blockType =
        typeDescriptor &&
        'value' in typeDescriptor &&
        typeof typeDescriptor.value === 'string'
          ? typeDescriptor.value
          : undefined;
      let thought = false;
      try {
        const thoughtDescriptor = Object.getOwnPropertyDescriptor(
          input,
          'thought',
        );
        thought = Boolean(
          thoughtDescriptor &&
            'value' in thoughtDescriptor &&
            thoughtDescriptor.value === true,
        );
      } catch {
        truncated = true;
      }
      const reasoningShaped = reasoningShapedBlock(blockType, thought);
      const dataIsOpaque = Boolean(
        blockType?.includes('encrypted') || blockType === 'redacted_thinking',
      );
      for (const key of keys.slice(0, SNAPSHOT_MAX_WIDTH)) {
        let descriptor: PropertyDescriptor | undefined;
        try {
          descriptor = Object.getOwnPropertyDescriptor(input, key);
        } catch {
          truncated = true;
          continue;
        }
        if (!descriptor || !('value' in descriptor)) {
          truncated = true;
          continue;
        }
        const normalized = normalizeKey(key);
        if (
          isOpaqueReasoningCarrierKey(
            normalized,
            reasoningShaped,
            blockType,
          ) ||
          (dataIsOpaque && normalized === 'data')
        ) {
          if (values.length >= SNAPSHOT_MAX_WIDTH) {
            truncated = true;
            return;
          }
          values.push({ key, value: descriptor.value });
          continue;
        }
        visit(descriptor.value, depth + 1);
      }
    } finally {
      ancestors.delete(input);
    }
  };
  visit(value, 0);
  return { values, truncated };
}

function isOpaqueReasoningCarrierKey(
  normalizedKey: string,
  reasoningShaped: boolean,
  blockType: string | undefined,
): boolean {
  if (
    normalizedKey === 'encryptedreasoning' ||
    normalizedKey === 'reasoningsignature' ||
    normalizedKey === 'thinkingsignature' ||
    normalizedKey === 'textsignature'
  )
    return true;
  if (normalizedKey === 'thoughtsignature')
    return reasoningShaped || blockType === 'toolCall';
  if (normalizedKey === 'signature' || normalizedKey === 'encryptedcontent')
    return reasoningShaped;
  return false;
}

function reasoningShapedBlock(
  blockType: string | undefined,
  thought: boolean,
): boolean {
  return (
    blockType === 'thinking' ||
    blockType === 'redacted_thinking' ||
    blockType === 'reasoning' ||
    blockType?.startsWith('reasoning.') === true ||
    (blockType === 'text' && thought)
  );
}

function hasOpaqueReasoningCarrier(
  value: object,
  blockType: string | undefined,
  reasoningShaped: boolean,
): boolean {
  try {
    const dataIsOpaque = Boolean(
      blockType?.includes('encrypted') || blockType === 'redacted_thinking',
    );
    return Object.keys(value).some((key) => {
      const normalized = normalizeKey(key);
      return (
        isOpaqueReasoningCarrierKey(
          normalized,
          reasoningShaped,
          blockType,
        ) ||
        (dataIsOpaque && normalized === 'data')
      );
    });
  } catch {
    return false;
  }
}

function normalizeKey(value: string): string {
  return value.toLowerCase().replaceAll(/[^a-z0-9]/gu, '');
}

function normalizeUsage(value: unknown): Record<string, number> | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const number = (keys: string[]): number | undefined => {
    for (const key of keys) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (
        descriptor &&
        'value' in descriptor &&
        typeof descriptor.value === 'number' &&
        Number.isFinite(descriptor.value) &&
        descriptor.value >= 0
      )
        return Math.floor(descriptor.value);
    }
    return undefined;
  };
  const normalized = {
    input_tokens: number(['input_tokens', 'inputTokens', 'input']),
    output_tokens: number(['output_tokens', 'outputTokens', 'output']),
    cache_read_tokens: number([
      'cache_read_tokens',
      'cacheReadTokens',
      'cacheRead',
    ]),
    cache_write_tokens: number([
      'cache_write_tokens',
      'cacheWriteTokens',
      'cacheWrite',
    ]),
    total_tokens: number(['total_tokens', 'totalTokens', 'total']),
  };
  const retained = Object.fromEntries(
    Object.entries(normalized).filter(
      (entry): entry is [string, number] => entry[1] !== undefined,
    ),
  );
  return Object.keys(retained).length > 0 ? retained : undefined;
}

function visibleAssistantContent(value: unknown): unknown {
  if (!Array.isArray(value)) return value;
  return value.filter((block) => {
    if (!block || typeof block !== 'object') return true;
    const descriptor = Object.getOwnPropertyDescriptor(block, 'type');
    if (
      !descriptor ||
      !('value' in descriptor) ||
      typeof descriptor.value !== 'string'
    )
      return true;
    const type = descriptor.value.toLowerCase();
    return (
      !type.includes('thinking') &&
      !type.includes('reasoning') &&
      type !== 'summary'
    );
  });
}

function object(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

function text(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function pick(
  value: Record<string, unknown>,
  keys: readonly string[],
): Record<string, unknown> {
  return Object.fromEntries(
    keys
      .filter((key) => Object.prototype.hasOwnProperty.call(value, key))
      .map((key) => [key, value[key]]),
  );
}

function modelTerminalEvidence(
  value: Record<string, unknown>,
): Record<string, unknown> {
  return pick(value, [
    'runId',
    'callId',
    'provider',
    'model',
    'api',
    'transport',
    'durationMs',
    'outcome',
    'finishReason',
    'errorCategory',
    'failureKind',
    'requestPayloadBytes',
    'responseStreamBytes',
    'timeToFirstByteMs',
    'upstreamRequestIdHash',
  ]);
}

function strings(
  value: Record<string, unknown> | undefined,
  keys: readonly string[],
): Record<string, string> {
  if (!value) return {};
  return Object.fromEntries(
    keys.flatMap((key) =>
      typeof value[key] === 'string' ? [[key, value[key] as string]] : [],
    ),
  );
}

function numbers(
  value: Record<string, unknown> | undefined,
  keys: readonly string[],
): Record<string, number> {
  if (!value) return {};
  return Object.fromEntries(
    keys.flatMap((key) =>
      typeof value[key] === 'number' ? [[key, value[key] as number]] : [],
    ),
  );
}

function booleans(
  value: Record<string, unknown>,
  keys: readonly string[],
): Record<string, boolean> {
  return Object.fromEntries(
    keys.flatMap((key) =>
      typeof value[key] === 'boolean' ? [[key, value[key] as boolean]] : [],
    ),
  );
}

function estimateBytes(value: unknown): number {
  try {
    return Buffer.byteLength(JSON.stringify(value), 'utf8');
  } catch {
    return 1024;
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function structuredError(error: unknown): Record<string, unknown> {
  const value = object(error);
  return {
    type: text(value?.name) ?? text(value?.type) ?? 'openclaw_error',
    message: (text(value?.message) ?? String(error)).slice(0, 4_096),
    recoverable: value?.recoverable === true,
    ...(text(value?.code) ? { code: text(value?.code) } : {}),
  };
}

function normalizeScopeStatus(
  value: unknown,
): 'completed' | 'failed' | 'cancelled' | 'interrupted' | 'unknown' {
  if (value === 'ok') return 'completed';
  if (value === 'error') return 'failed';
  if (value === 'killed' || value === 'reset' || value === 'deleted')
    return 'cancelled';
  if (value === 'timeout') return 'interrupted';
  return 'unknown';
}

function hostQualification(
  hostVersion: string | undefined,
  unavailableHooks?: ReadonlySet<string>,
): RunSourceOptionsQualification {
  if (
    (unavailableHooks?.size ?? 0) > 0 ||
    !hostVersion ||
    !QUALIFIED_HOST_VERSIONS.has(hostVersion)
  ) {
    return { status: 'unknown', profile: QUALIFICATION_PROFILE };
  }
  return {
    status: 'exact_qualified',
    profile: QUALIFICATION_PROFILE,
  };
}

type RunSourceOptionsQualification = {
  status: 'exact_qualified' | 'capability_checked_unqualified' | 'unknown';
  profile: string;
};

function compareHostVersions(left: string, right: string): number {
  const parts = (version: string): number[] =>
    version
      .replace(/^v/u, '')
      .split(/[.-]/u)
      .map((part) => Number.parseInt(part, 10) || 0);
  const leftParts = parts(left);
  const rightParts = parts(right);
  for (
    let index = 0;
    index < Math.max(leftParts.length, rightParts.length);
    index += 1
  ) {
    const difference = (leftParts[index] ?? 0) - (rightParts[index] ?? 0);
    if (difference !== 0) return difference < 0 ? -1 : 1;
  }
  return 0;
}

function delay(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function withDeadline<T>(
  promise: Promise<T>,
  milliseconds: number,
  label: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(
          () =>
            reject(new Error(`${label} exceeded ${milliseconds}ms deadline`)),
          milliseconds,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}
