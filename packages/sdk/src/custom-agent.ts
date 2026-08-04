import type {
  AdmissionReceipt,
  CaptureSource,
  SourceSink,
  TraceIdentity,
} from './v1/types.js';

export type CustomAgentError = Readonly<{
  type: string;
  message: string;
  recoverable: boolean;
  code?: string;
  details?: unknown;
}>;

export type CustomAgentReasoningBlock = Readonly<{
  type: 'text' | 'summary';
  text: string;
}>;

export type CustomAgentEvent =
  | Readonly<{
      type: 'run.start';
      runId: string;
      name: string;
      input?: unknown;
      conversationId?: string;
      turnId?: string;
      turnIndex?: number;
      previousTurnId?: string;
    }>
  | Readonly<{
      type: 'run.outcome';
      runId: string;
      status: 'completed' | 'failed' | 'cancelled' | 'unknown';
      output?: unknown;
      error?: CustomAgentError;
    }>
  | Readonly<{
      type: 'message';
      runId: string;
      messageId: string;
      role: 'system' | 'developer' | 'user' | 'assistant' | 'tool';
      content: unknown;
      name?: string;
      callId?: string;
    }>
  | Readonly<{
      type: 'model.request';
      runId: string;
      callId: string;
      model?: string;
      tools?: readonly string[];
      messageIds?: readonly string[];
    }>
  | Readonly<{
      type: 'model.response';
      runId: string;
      callId: string;
      status: 'completed' | 'incomplete' | 'failed' | 'cancelled';
      model?: string;
      content?: unknown;
      reasoning?: readonly CustomAgentReasoningBlock[];
      finishReason?: string;
      usage?: Readonly<{ inputTokens?: number; outputTokens?: number }>;
      error?: CustomAgentError;
    }>
  | Readonly<{
      type: 'tool.proposal';
      runId: string;
      callId: string;
      name: string;
      input: unknown;
    }>
  | Readonly<{
      type: 'tool.call';
      runId: string;
      callId: string;
      name: string;
      input: unknown;
    }>
  | Readonly<{
      type: 'tool.result';
      runId: string;
      callId: string;
      status: 'succeeded' | 'failed' | 'cancelled';
      output?: unknown;
      error?: CustomAgentError;
    }>;

export type CustomAgentBridge = Readonly<{
  source: CaptureSource;
  record(event: CustomAgentEvent): AdmissionReceipt;
}>;

export type CustomAgentSourceOptions = Readonly<{
  name: string;
  version?: string;
  seam?: string;
}>;

type OpenRun = {
  identity: TraceIdentity;
  startRecordId: string;
  name: string;
};

const settled = Promise.resolve();

/**
 * Maps one app-owned callback stream into the shared semantic source contract.
 * It does not execute agent work, tools, retries, or model calls.
 */
export function createCustomAgentSource(
  options: CustomAgentSourceOptions,
): CustomAgentBridge {
  let sink: SourceSink | undefined;
  let active = false;
  const runs = new Map<string, OpenRun>();
  const messages = new Map<string, string>();
  const models = new Map<string, string>();
  const collidedModels = new Set<string>();
  const settledModels = new Set<string>();
  const modelRequestNotObservedRuns = new Set<string>();
  const proposals = new Set<string>();
  const tools = new Map<string, string>();
  const collidedTools = new Set<string>();
  const settledTools = new Set<string>();

  const source: CaptureSource = {
    metadata: {
      name: options.name,
      seam: options.seam ?? 'custom-agent.events',
      identityDomain: 'custom-agent.event',
      coverage: [],
      ...(options.version ? { version: options.version } : {}),
    },
    install(installedSink) {
      if (active) throw new Error('Custom agent source is already installed');
      sink = installedSink;
      active = true;
      return {
        deactivate() {
          if (!sink) return;
          for (const [runId, run] of runs) {
            recordOpenOperationGaps(sink, runId, run, models, tools);
            gap(sink, run, { type: 'run.outcome', runId },
              'run_terminal_not_observed');
            sink.record({
              kind: 'lifecycle',
              phase: 'end',
              name: run.name,
              trace: run.identity,
              nativeIdentity: runId,
              parentRecordId: run.startRecordId,
              native: { reason: 'source_deactivated' },
              semantic: { type: 'agent.run', status: 'unknown' },
            });
          }
          runs.clear();
          messages.clear();
          models.clear();
          collidedModels.clear();
          settledModels.clear();
          modelRequestNotObservedRuns.clear();
          proposals.clear();
          tools.clear();
          collidedTools.clear();
          settledTools.clear();
          active = false;
        },
        drain() {},
      };
    },
  };

  return Object.freeze({
    source,
    record(event: CustomAgentEvent): AdmissionReceipt {
      if (!active || !sink) return rejected('source_not_installed');
      const eventType = eventField(event, 'type');
      const runId = eventField(event, 'runId');
      if (!exactIdentity(runId)) return rejected('invalid_run_id');
      if (!isCustomAgentEventType(eventType)) {
        const run = runs.get(runId);
        return run
          ? gap(sink, run, event, 'unknown_event_type')
          : rejected('unknown_event_type');
      }

      if (eventType === 'run.start') {
        const current = event as Extract<CustomAgentEvent, { type: 'run.start' }>;
        if (typeof current.name !== 'string' || !current.name.trim()) {
          return rejected('invalid_run_name');
        }
        const existing = runs.get(runId);
        if (existing) return gap(sink, existing, current, 'duplicate_run_start');
        const opened = sink.openTrace({
          name: current.name,
          nativeIdentity: runId,
          ...(current.conversationId ? { conversationId: current.conversationId } : {}),
          ...(current.turnId ? { turnId: current.turnId } : {}),
          ...(current.turnIndex === undefined ? {} : { turnIndex: current.turnIndex }),
          ...(current.previousTurnId ? { previousTurnId: current.previousTurnId } : {}),
          native: current,
          semantic: {
            type: 'agent.run',
            name: current.name,
            ...(current.input === undefined ? {} : { input: current.input }),
          },
        });
        if (opened.accepted) {
          runs.set(runId, {
            identity: opened.identity,
            startRecordId: opened.recordId,
            name: current.name,
          });
        }
        return opened;
      }

      const run = runs.get(runId);
      if (!run) return rejected('unknown_run_id');

      if (eventType === 'run.outcome') {
        const current = event as Extract<CustomAgentEvent, { type: 'run.outcome' }>;
        const status = eventField(current, 'status');
        if (!isRunStatus(status)) {
          return gap(sink, run, current, 'invalid_status');
        }
        if (!consistentTerminalError(status, current.error)) {
          return gap(sink, run, current, 'contradictory_terminal_error');
        }
        recordOpenOperationGaps(sink, runId, run, models, tools);
        const receipt = sink.record({
          kind: 'lifecycle',
          phase: status === 'failed'
            ? 'error'
            : status === 'cancelled' ? 'cancelled' : 'end',
          name: run.name,
          trace: run.identity,
          nativeIdentity: runId,
          parentRecordId: run.startRecordId,
          native: current,
          semantic: {
            type: 'agent.run',
            status,
            ...(current.output === undefined ? {} : { output: current.output }),
            ...(current.error ? { error: current.error } : {}),
          },
        });
        runs.delete(runId);
        clearRunValues(runId, messages);
        clearRunValues(runId, models);
        clearRunKeys(runId, collidedModels);
        clearRunKeys(runId, settledModels);
        modelRequestNotObservedRuns.delete(runId);
        clearRunKeys(runId, proposals);
        clearRunValues(runId, tools);
        clearRunKeys(runId, collidedTools);
        clearRunKeys(runId, settledTools);
        return receipt;
      }

      if (eventType === 'message') {
        const current = event as Extract<CustomAgentEvent, { type: 'message' }>;
        if (!exactIdentity(current.messageId)) {
          return gap(sink, run, current, 'invalid_message_id');
        }
        if (!isMessageRole(current.role)
          || !Object.prototype.hasOwnProperty.call(current, 'content')) {
          return gap(sink, run, current, 'invalid_message');
        }
        const key = operationKey(runId, current.messageId);
        if (messages.has(key)) return gap(sink, run, current, 'duplicate_message');
        const invalidCallId = current.callId !== undefined && !exactIdentity(current.callId);
        const receipt = sink.record({
          kind: 'model',
          phase: 'event',
          name: 'message',
          trace: run.identity,
          nativeIdentity: current.messageId,
          parentRecordId: run.startRecordId,
          native: current,
          semantic: {
            type: 'message',
            role: current.role,
            content: current.content,
            ...(current.name ? { name: current.name } : {}),
            ...(exactIdentity(current.callId) ? { call_id: current.callId } : {}),
          },
        });
        if (receipt.accepted) {
          messages.set(key, receipt.recordId);
          if (invalidCallId) gap(sink, run, current, 'invalid_call_id', receipt.recordId);
        }
        return receipt;
      }

      const operation = event as Exclude<
        CustomAgentEvent,
        { type: 'run.start' | 'run.outcome' | 'message' }
      >;
      if (!exactIdentity(operation.callId)) {
        return gap(sink, run, operation, 'invalid_call_id');
      }
      const key = operationKey(runId, operation.callId);

      if (eventType === 'model.request') {
        const current = event as Extract<CustomAgentEvent, { type: 'model.request' }>;
        if (models.has(key)) {
          const request = models.get(key);
          models.delete(key);
          collidedModels.add(key);
          return gap(
            sink,
            run,
            event,
            'duplicate_model_request',
            request ?? run.startRecordId,
          );
        }
        if (collidedModels.has(key) || settledModels.has(key)) {
          return gap(sink, run, event, 'duplicate_model_request');
        }
        let contextRefs: string[] | undefined;
        const hasMessageIds = Object.prototype.hasOwnProperty.call(current, 'messageIds');
        const messageIds = eventField(current, 'messageIds');
        let invalidMessageId = false;
        let unknownMessageId = false;
        if (hasMessageIds && !Array.isArray(messageIds)) {
          gap(sink, run, current, 'invalid_message_ids');
        } else if (hasMessageIds && Array.isArray(messageIds)) {
          contextRefs = [];
        }
        const tools = current.tools === undefined
          ? undefined
          : Array.isArray(current.tools)
              && current.tools.every((tool) => typeof tool === 'string')
            ? [...current.tools]
            : undefined;
        if (current.tools !== undefined && tools === undefined) {
          gap(sink, run, current, 'invalid_tools');
        }
        for (const messageId of hasMessageIds && Array.isArray(messageIds) ? messageIds : []) {
          if (!exactIdentity(messageId)) {
            invalidMessageId = true;
            continue;
          }
          const message = messages.get(operationKey(runId, messageId));
          if (!message) {
            unknownMessageId = true;
            continue;
          }
          contextRefs?.push(message);
        }
        if (invalidMessageId) {
          gap(sink, run, current, 'invalid_message_id');
        }
        if (unknownMessageId) {
          gap(sink, run, current, 'unknown_message_id');
        }
        if (invalidMessageId || unknownMessageId) {
          contextRefs = undefined;
        }
        const receipt = sink.record({
          kind: 'model',
          phase: 'start',
          name: 'model.request',
          trace: run.identity,
          nativeIdentity: current.callId,
          parentRecordId: run.startRecordId,
          native: current,
          semantic: {
            type: 'model.request',
            ...(current.model ? { model: current.model } : {}),
            ...(tools ? { tools } : {}),
            ...(contextRefs === undefined ? {} : { context_refs: contextRefs }),
          },
        });
        if (receipt.accepted) models.set(key, receipt.recordId);
        return receipt;
      }

      if (eventType === 'model.response') {
        const current = event as Extract<CustomAgentEvent, { type: 'model.response' }>;
        if (collidedModels.has(key)) {
          collidedModels.delete(key);
          settledModels.add(key);
          return gap(sink, run, current, 'ambiguous_model_response');
        }
        if (settledModels.has(key)) {
          return gap(sink, run, current, 'duplicate_model_response');
        }
        const request = models.get(key);
        const status = eventField(current, 'status');
        if (!isModelStatus(status)) {
          return gap(sink, run, current, 'invalid_status');
        }
        if (!consistentTerminalError(status, current.error)) {
          return gap(sink, run, current, 'contradictory_terminal_error');
        }
        const missingContent = (
          (status === 'completed' || status === 'incomplete')
          && current.content === undefined
        );
        const reasoning = validReasoning(current.reasoning);
        const invalidReasoning = current.reasoning !== undefined && !reasoning;
        const receipt = sink.record({
          kind: 'model',
          phase: status === 'failed'
            ? 'error'
            : status === 'cancelled' ? 'cancelled' : 'end',
          name: 'model.response',
          trace: run.identity,
          nativeIdentity: current.callId,
          parentRecordId: request ?? run.startRecordId,
          native: current,
          semantic: {
            type: 'model.response',
            status,
            ...(current.model ? { model: current.model } : {}),
            ...(current.content === undefined ? {} : { content: current.content }),
            ...(reasoning ? { reasoning } : {}),
            ...(current.finishReason ? { finish_reason: current.finishReason } : {}),
            ...(current.usage ? { usage: {
              ...(current.usage.inputTokens === undefined
                ? {} : { input_tokens: current.usage.inputTokens }),
              ...(current.usage.outputTokens === undefined
                ? {} : { output_tokens: current.usage.outputTokens }),
            } } : {}),
            ...(current.error ? { error: current.error } : {}),
          },
        });
        if (receipt.accepted && current.error) {
          sink.record({
            kind: 'error',
            phase: 'error',
            name: 'model.response.error',
            trace: run.identity,
            nativeIdentity: current.callId,
            parentRecordId: receipt.recordId,
            native: current.error,
            semantic: { type: 'agent.error', error: current.error },
          });
        }
        if (receipt.accepted) {
          if (missingContent) {
            gap(sink, run, current, 'model_content_not_captured', receipt.recordId);
          }
          if (invalidReasoning) {
            gap(sink, run, current, 'invalid_reasoning', receipt.recordId);
          }
          if (!request && !modelRequestNotObservedRuns.has(runId)) {
            modelRequestNotObservedRuns.add(runId);
            gap(
              sink,
              run,
              { type: current.type, runId, callId: current.callId },
              'model_request_not_observed',
              receipt.recordId,
            );
          }
        }
        models.delete(key);
        settledModels.add(key);
        return receipt;
      }

      if (eventType === 'tool.proposal') {
        const current = event as Extract<CustomAgentEvent, { type: 'tool.proposal' }>;
        const invalidTool = invalidToolOperationReason(current);
        if (invalidTool) {
          return gap(sink, run, current, invalidTool);
        }
        if (proposals.has(key)) return gap(sink, run, event, 'duplicate_tool_proposal');
        const receipt = sink.record({
          kind: 'tool',
          phase: 'event',
          name: current.name,
          trace: run.identity,
          nativeIdentity: current.callId,
          parentRecordId: run.startRecordId,
          native: current,
          semantic: {
            type: 'tool.proposal',
            name: current.name,
            input: current.input,
          },
        });
        if (receipt.accepted) proposals.add(key);
        return receipt;
      }

      if (eventType === 'tool.call') {
        const current = event as Extract<CustomAgentEvent, { type: 'tool.call' }>;
        const invalidTool = invalidToolOperationReason(current);
        if (invalidTool) {
          return gap(sink, run, current, invalidTool);
        }
        if (tools.has(key)) {
          const call = tools.get(key);
          tools.delete(key);
          collidedTools.add(key);
          return gap(
            sink,
            run,
            event,
            'duplicate_tool_call',
            call ?? run.startRecordId,
          );
        }
        if (collidedTools.has(key) || settledTools.has(key)) {
          return gap(sink, run, event, 'duplicate_tool_call');
        }
        const receipt = sink.record({
          kind: 'tool',
          phase: 'start',
          name: current.name,
          trace: run.identity,
          nativeIdentity: current.callId,
          parentRecordId: run.startRecordId,
          native: current,
          semantic: {
            type: 'tool.execution',
            name: current.name,
            input: current.input,
          },
        });
        if (receipt.accepted) tools.set(key, receipt.recordId);
        return receipt;
      }

      if (eventType !== 'tool.result') {
        return gap(sink, run, event, 'unknown_event_type');
      }
      const current = event as Extract<CustomAgentEvent, { type: 'tool.result' }>;
      if (collidedTools.has(key)) {
        collidedTools.delete(key);
        settledTools.add(key);
        return gap(sink, run, current, 'ambiguous_tool_result');
      }
      if (settledTools.has(key)) {
        return gap(sink, run, current, 'duplicate_tool_result');
      }
      const call = tools.get(key);
      if (!call) return gap(sink, run, current, 'tool_result_without_call');
      const status = eventField(current, 'status');
      if (!isToolStatus(status)) {
        return gap(sink, run, current, 'invalid_status');
      }
      if (!consistentTerminalError(status, current.error)) {
        return gap(sink, run, current, 'contradictory_terminal_error');
      }
      const missingOutput = status === 'succeeded' && current.output === undefined;
      const receipt = sink.record({
        kind: 'tool',
        phase: status === 'failed'
          ? 'error'
          : status === 'cancelled' ? 'cancelled' : 'end',
        name: 'tool.result',
        trace: run.identity,
        nativeIdentity: current.callId,
        parentRecordId: call,
        native: current,
        semantic: {
          type: status === 'failed' ? 'tool.error' : 'tool.result',
          status,
          ...(current.output === undefined ? {} : { output: current.output }),
          ...(current.error ? { error: current.error } : {}),
        },
      });
      if (receipt.accepted && missingOutput) {
        gap(sink, run, current, 'tool_output_not_captured', receipt.recordId);
      }
      tools.delete(key);
      settledTools.add(key);
      return receipt;
    },
  });
}

function recordOpenOperationGaps(
  sink: SourceSink,
  runId: string,
  run: OpenRun,
  models: Map<string, string>,
  tools: Map<string, string>,
): void {
  for (const [key, recordId] of [...models]) {
    if (!key.startsWith(`${runId}\u0000`)) continue;
    gap(sink, run, { type: 'model.request', runId, callId: key.slice(runId.length + 1) },
      'model_request_without_response', recordId);
    models.delete(key);
  }
  for (const [key, recordId] of [...tools]) {
    if (!key.startsWith(`${runId}\u0000`)) continue;
    gap(sink, run, { type: 'tool.call', runId, callId: key.slice(runId.length + 1) },
      'tool_call_without_result', recordId);
    tools.delete(key);
  }
}

function gap(
  sink: SourceSink,
  run: OpenRun,
  event: unknown,
  reason: string,
  parentRecordId = run.startRecordId,
): AdmissionReceipt {
  return sink.record({
    kind: 'unknown',
    phase: 'gap',
    name: 'custom-agent.gap',
    trace: run.identity,
    parentRecordId,
    native: { reason, event },
    semantic: {
      type: 'capture.gap',
      reason,
      detail: `Custom agent callback gap: ${reason}.`,
    },
  });
}

function exactIdentity(value: unknown): value is string {
  return typeof value === 'string'
    && value.trim().length > 0
    && [...value].length <= 256
    && !value.includes('\u0000');
}

function operationKey(runId: string, callId: string): string {
  return `${runId}\u0000${callId}`;
}

function clearRunValues(runId: string, values: Map<string, string>): void {
  for (const key of values.keys()) {
    if (key.startsWith(`${runId}\u0000`)) values.delete(key);
  }
}

function clearRunKeys(runId: string, values: Set<string>): void {
  for (const key of values) {
    if (key.startsWith(`${runId}\u0000`)) values.delete(key);
  }
}

function consistentTerminalError(
  status: string,
  error: CustomAgentError | undefined,
): boolean {
  return error === undefined || status === 'failed';
}

function eventField(input: unknown, field: string): unknown {
  try {
    return input && typeof input === 'object' ? Reflect.get(input, field) : undefined;
  } catch {
    return undefined;
  }
}

function isCustomAgentEventType(value: unknown): value is CustomAgentEvent['type'] {
  return typeof value === 'string' && [
    'run.start',
    'run.outcome',
    'message',
    'model.request',
    'model.response',
    'tool.proposal',
    'tool.call',
    'tool.result',
  ].includes(value);
}

function isRunStatus(value: unknown): value is Extract<
  CustomAgentEvent,
  { type: 'run.outcome' }
>['status'] {
  return typeof value === 'string'
    && ['completed', 'failed', 'cancelled', 'unknown'].includes(value);
}

function isModelStatus(value: unknown): value is Extract<
  CustomAgentEvent,
  { type: 'model.response' }
>['status'] {
  return typeof value === 'string'
    && ['completed', 'incomplete', 'failed', 'cancelled'].includes(value);
}

function isToolStatus(value: unknown): value is Extract<
  CustomAgentEvent,
  { type: 'tool.result' }
>['status'] {
  return typeof value === 'string'
    && ['succeeded', 'failed', 'cancelled'].includes(value);
}

function isMessageRole(
  value: unknown,
): value is 'system' | 'developer' | 'user' | 'assistant' | 'tool' {
  return typeof value === 'string'
    && ['system', 'developer', 'user', 'assistant', 'tool'].includes(value);
}

function invalidToolOperationReason(
  value: unknown,
): 'invalid_tool_name' | 'tool_input_not_captured' | undefined {
  const name = eventField(value, 'name');
  if (
    typeof name !== 'string'
    || !name.trim()
    || [...name].length > 256
  ) return 'invalid_tool_name';
  if (
    value === null
    || typeof value !== 'object'
    || !Object.prototype.hasOwnProperty.call(value, 'input')
    || eventField(value, 'input') === undefined
  ) return 'tool_input_not_captured';
  return undefined;
}

function validReasoning(value: unknown): readonly CustomAgentReasoningBlock[] | undefined {
  if (!Array.isArray(value)) return undefined;
  const blocks: CustomAgentReasoningBlock[] = [];
  for (const block of value) {
    const type = eventField(block, 'type');
    const text = eventField(block, 'text');
    if (
      !block
      || typeof block !== 'object'
      || (type !== 'text' && type !== 'summary')
      || typeof text !== 'string'
    ) return undefined;
    blocks.push({ type, text });
  }
  return blocks;
}

function rejected(reason: string): AdmissionReceipt {
  return { accepted: false, reason, settled };
}
