import type {
  AdmissionReceipt,
  CaptureSource,
  SourceSink,
  TraceIdentity,
} from '../v1/types.js';
import { trustOfficialSource } from '../v1/source-ownership.js';
import { snapshotNative, snapshotRecord } from './native-snapshot.js';

type EventConstructor = abstract new (...args: never[]) => object;
type HookCallback = (event: Record<PropertyKey, unknown>) => void | Promise<void>;
type HookSubject = {
  addHook(event: EventConstructor, callback: HookCallback, options?: unknown): () => void;
};
type StrandsModule = Record<string, unknown>;
type StrandsAdapterOptions = { version?: string; sdk: StrandsModule };
const AGENT_INVOCATION_COVERAGE = Object.freeze({
  operation: 'agent-invocation', domain: 'strands.agent',
});
const TOOL_EXECUTION_COVERAGE = Object.freeze({
  operation: 'tool-execution', domain: 'gen-ai.tool-call',
});
type TokenUsage = { input_tokens?: number; output_tokens?: number };
type OpenOperation = {
  trace: TraceIdentity;
  name: string;
  start: AdmissionReceipt;
  attempt: number;
  usage?: TokenUsage;
  reasoning: Array<{ type: 'text' | 'summary'; text: string }>;
};
type FailedOperation = { receipt: AdmissionReceipt; attempt: number };
type MessageRole = 'system' | 'user' | 'assistant';
type OpenInvocation = {
  identity: TraceIdentity;
  after?: Record<string, unknown>;
  systemContext?: ContextEvidence;
  contextGapRecorded: boolean;
};
type ContextEvidence = {
  role: MessageRole;
  content: unknown;
  recordIds: string[];
};
type ToolResultEvidence = {
  nativeCallId: string;
  result: unknown;
  recordId: string;
};

const REQUIRED_EVENTS = [
  'BeforeInvocationEvent', 'AfterInvocationEvent', 'MessageAddedEvent',
  'BeforeModelCallEvent', 'AfterModelCallEvent', 'ModelStreamUpdateEvent',
  'ContentBlockEvent', 'ModelMessageEvent', 'BeforeToolCallEvent',
  'AfterToolCallEvent', 'ToolResultEvent', 'ToolStreamUpdateEvent',
  'AgentResultEvent', 'InterruptEvent',
] as const;

/** Captures the documented Strands hook stream without wrapping invocation or consuming streams. */
export function strandsAdapter(options: StrandsAdapterOptions): {
  createSource(agent: object): CaptureSource;
} {
  const events = requireEvents(options.sdk);
  return {
    createSource(agent) {
      if (!isHookSubject(agent)) throw new TypeError('Strands agent must expose addHook');
      return createStrandsSource(agent, events, options.version);
    },
  };
}

function createStrandsSource(
  subject: HookSubject,
  events: Map<string, EventConstructor>,
  version: string | undefined,
): CaptureSource {
  return trustOfficialSource({
    metadata: {
      name: 'official:strands-js',
      seam: 'HookProvider/hooks',
      identityDomain: 'strands.agent',
      ...(version ? { version } : {}),
      qualification: version === undefined
        ? { status: 'unknown' }
        : ['1.9.0', '1.8.0'].includes(version)
          ? { status: 'exact_qualified' }
          : { status: 'capability_checked_unqualified', profile: 'strands-hook-provider-v1' },
      official: true,
      coverage: [
        { ...AGENT_INVOCATION_COVERAGE, role: 'owner' },
        { ...TOOL_EXECUTION_COVERAGE, role: 'owner' },
      ],
    },
    install(sink) {
      const traces = new WeakMap<object, OpenInvocation>();
      const openInvocations = new Set<OpenInvocation>();
      const models = new WeakMap<object, OpenOperation>();
      const failedModels = new WeakMap<object, FailedOperation>();
      const tools = new Map<string, OpenOperation>();
      const failedTools = new Map<string, FailedOperation>();
      const contextByMessage = new WeakMap<object, ContextEvidence>();
      const toolResultsByBlock = new WeakMap<object, ToolResultEvidence>();
      const cleanups: Array<() => void> = [];
      let active = true;

      const register = (name: string, callback: HookCallback) => {
        const cleanup = subject.addHook(events.get(name)!, callback, { order: Number.MAX_SAFE_INTEGER });
        if (typeof cleanup !== 'function') throw new TypeError(`Strands addHook(${name}) must return cleanup`);
        cleanups.push(cleanup);
      };

      try {
        register('BeforeInvocationEvent', (event) => {
          if (!active) return;
          const state = invocationState(event);
          if (!state) return;
          const opened = sink.openTrace({
            name: 'strands.agent.invocation',
            coverage: AGENT_INVOCATION_COVERAGE,
            nativeIdentity: stringField(state, 'traceId') ?? stringField(state, 'requestId'),
            conversationId: stringField(state, 'conversationId') ?? stringField(state, 'sessionId'),
            turnId: stringField(state, 'turnId'),
            turnIndex: numberField(state, 'turnIndex'),
            previousTurnId: stringField(state, 'previousTurnId'),
            native: snapshot(event),
            semantic: {
              type: 'agent.run',
              framework: 'strands',
              name: 'strands.agent.invocation',
            },
          });
          if (!opened.accepted) return;
          const invocation = { identity: opened.identity, contextGapRecorded: false };
          traces.set(state, invocation);
          openInvocations.add(invocation);
          sink.record({
            kind: 'correlation', phase: 'event', name: 'strands.invocation.native',
            trace: opened.identity, native: snapshot(event),
            semantic: { type: 'capture.redundant', framework: 'strands' },
          });
        });
        register('AfterInvocationEvent', (event) => {
          const trace = traceFor(event, traces);
          if (!active || !trace) return;
          const state = invocationState(event);
          const invocation = state ? traces.get(state) : undefined;
          if (invocation) invocation.after = snapshot(event);
        });
        register('MessageAddedEvent', (event) => {
          const trace = traceFor(event, traces);
          const message = recordField(event, 'message');
          const role = message ? messageRole(message) : undefined;
          const content = message ? ownValue(message, 'content') : undefined;
          if (!trace || (role !== 'user' && role !== 'assistant') || content === undefined) return;
          const trackingId = stringField(message!, 'trackingId');
          const known = contextByMessage.get(message!);
          if (known && unchangedMessage(known, role, content)) return;
          const toolResultRefs = exactToolResultRefs(content, toolResultsByBlock);
          if (toolResultRefs) {
            contextByMessage.set(message!, {
              role,
              content: snapshotNative(content),
              recordIds: toolResultRefs,
            });
            return;
          }
          const receipt = sink.record({
            kind: 'state', phase: 'event', name: 'strands.message.added', trace,
            ...(trackingId ? { nativeIdentity: trackingId } : {}),
            native: snapshot(event),
            semantic: {
              type: 'message',
              framework: 'strands',
              role,
              content: snapshotNative(content),
            },
          });
          if (receipt.accepted) {
            contextByMessage.set(message!, {
              role,
              content: snapshotNative(content),
              recordIds: [receipt.recordId],
            });
          }
        });
        register('BeforeModelCallEvent', (event) => {
          const state = invocationState(event);
          const invocation = state ? traces.get(state) : undefined;
          const trace = invocation?.identity;
          if (!active || !state || !invocation || !trace) return;
          const modelFailure = failedModels.get(state);
          const recovery = modelFailure ? sink.record({
            kind: 'state', phase: 'event', name: 'strands.model.recovery', trace,
            native: { attempt: modelFailure.attempt + 1, event: snapshot(event) },
            ...(modelFailure.receipt.accepted
              ? { parentRecordId: modelFailure.receipt.recordId }
              : {}),
            semantic: {
              type: 'state.retry',
              state_type: 'model.retry',
              framework: 'strands',
              value: { attempt: modelFailure.attempt + 1 },
            },
          }) : undefined;
          if (modelFailure) failedModels.delete(state);
          const contextRefs = captureModelContext(
            sink,
            trace,
            event,
            contextByMessage,
            invocation,
          );
          const model = modelIdentifier(event);
          const declaredTools = toolNames(event);
          recordPostMiddlewareContextGap(sink, invocation);
          const start = sink.record({
            kind: 'model', phase: 'start', name: 'strands.model.call', trace,
            native: snapshot(event),
            ...(recovery?.accepted ? { parentRecordId: recovery.recordId } : {}),
            semantic: {
              type: 'model.request',
              framework: 'strands',
              context_refs: contextRefs,
              ...(model ? { model } : {}),
              ...(declaredTools.length ? { tools: declaredTools } : {}),
            },
          });
          models.set(state, {
            trace,
            name: 'strands.model.call',
            start,
            attempt: modelFailure?.attempt ? modelFailure.attempt + 1 : 1,
            reasoning: [],
          });
        });
        register('AfterModelCallEvent', (event) => {
          const state = invocationState(event);
          const open = state ? models.get(state) : undefined;
          if (!active || !state || !open) return;
          const error = event.error;
          const failed = error !== undefined && error !== null;
          const cancelled = failed && isCancellationError(error);
          const stopData = recordField(event, 'stopData');
          const message = stopData ? recordField(stopData, 'message') : undefined;
          const content = message ? ownValue(message, 'content') : undefined;
          const response = strandsResponseEvidence(content);
          const reasoning = response.reasoning.length
            ? response.reasoning
            : open.reasoning;
          const finishReason = stopData ? stringField(stopData, 'stopReason') : undefined;
          const attempt = integerField(event, 'attemptCount') ?? open.attempt;
          const semantic = failed
            ? {
                type: 'model.response',
                framework: 'strands',
                status: cancelled ? 'cancelled' : 'failed',
                ...(open.reasoning.length ? { reasoning: open.reasoning } : {}),
                error: semanticError('model.error', error, event.retry === true),
              }
            : {
                type: 'model.response',
                framework: 'strands',
                status: 'completed',
                ...(response.content === undefined ? {} : { content: snapshotNative(response.content) }),
                ...(reasoning.length ? { reasoning } : {}),
                ...(finishReason ? { finish_reason: finishReason } : {}),
                ...(open.usage ? { usage: open.usage } : {}),
              };
          const ended = sink.record({
            kind: 'model', phase: cancelled ? 'cancelled' : failed ? 'error' : 'end', name: open.name, trace: open.trace,
            native: snapshot(event), ...(open.start.accepted ? { parentRecordId: open.start.recordId } : {}),
            semantic,
          });
          if (failed && isContextOverflowError(error)) {
            sink.record({
              kind: 'unknown', phase: 'gap', name: 'strands.context.reduction.unobserved',
              trace: open.trace, native: { error: errorSnapshot(error) },
              ...(ended.accepted ? { parentRecordId: ended.recordId } : {}),
              semantic: {
                type: 'capture.gap',
                framework: 'strands',
                reason: 'strands_context_reduction_unobserved',
                count: 1,
                detail: 'Strands may reduce context after a context-window overflow; built-in summarization calls its model outside the public hook stream.',
              },
            });
          }
          if (!failed && response.unavailableReasoning > 0) {
            sink.record({
              kind: 'unknown',
              phase: 'gap',
              name: 'strands.model.reasoning.unavailable',
              trace: open.trace,
              native: { unavailable_reasoning_blocks: response.unavailableReasoning },
              ...(ended.accepted ? { parentRecordId: ended.recordId } : {}),
              semantic: {
                type: 'capture.gap',
                framework: 'strands',
                reason: 'reasoning_unavailable',
                count: response.unavailableReasoning,
                detail: 'Strands exposed only redacted or signature-only reasoning blocks.',
              },
            });
          }
          const role = message ? messageRole(message) : undefined;
          if (
            !failed
            && ended.accepted
            && role
            && content !== undefined
          ) {
            contextByMessage.set(message!, {
              role,
              content: snapshotNative(content),
              recordIds: [ended.recordId],
            });
          }
          if (failed) {
            const nativeError = errorSnapshot(error);
            const structuredError = findStructuredError(nativeError, new WeakSet<object>());
            const failure = sink.record({
              kind: 'error', phase: 'event', name: 'strands.model.error', trace: open.trace,
              native: {
                event: snapshot(event), error: nativeError,
                ...(structuredError === undefined ? {} : { structured_error: snapshotNative(structuredError) }),
              },
              ...(ended.accepted ? { parentRecordId: ended.recordId } : {}),
              semantic: { type: 'capture.redundant', framework: 'strands' },
            });
            failedModels.set(state, { receipt: failure, attempt });
          }
          models.delete(state);
        });
        register('ModelStreamUpdateEvent', (event) => {
          const inner = recordField(event, 'event');
          if (inner?.type === 'modelContentBlockDeltaEvent') {
            const state = invocationState(event);
            const open = state ? models.get(state) : undefined;
            const delta = recordField(inner, 'delta');
            const reasoning = delta ? recordField(delta, 'reasoningContent') : undefined;
            const text = reasoning ? stringField(reasoning, 'text') : undefined;
            if (open && text) open.reasoning.push({ type: 'text', text });
          }
          if (inner?.type === 'modelMetadataEvent' && inner.usage !== undefined) {
            const state = invocationState(event);
            const open = state ? models.get(state) : undefined;
            const usage = tokenUsage(inner.usage);
            if (open && usage) open.usage = usage;
            if (open) sink.record({
              kind: 'model', phase: 'event', name: 'strands.model.usage', trace: open.trace,
              native: { ...snapshot(event), usage: snapshotNative(inner.usage) },
              semantic: { type: 'capture.redundant', framework: 'strands' },
            });
            return;
          }
          recordEvent(
            sink, traces, event, 'stream', 'event',
            'strands.model.stream.delta', 'capture.redundant',
          );
        });
        register('ContentBlockEvent', (event) => {
          const block = recordField(event, 'contentBlock');
          const toolUse = block ? toolUseFrom(block) : undefined;
          const trace = traceFor(event, traces);
          if (trace && toolUse) {
            const state = invocationState(event);
            const open = state ? models.get(state) : undefined;
            sink.record({
              kind: 'tool', phase: 'event', name: 'strands.content.block', trace,
              nativeIdentity: toolUse.id,
              native: snapshot(event),
              ...(open?.start.accepted ? { parentRecordId: open.start.recordId } : {}),
              semantic: {
                type: 'tool.proposal',
                framework: 'strands',
                call_id: toolUse.id,
                native_call_id: toolUse.id,
                name: toolUse.name,
                input: snapshotNative(toolUse.input),
              },
            });
            return;
          }
          recordEvent(
            sink, traces, event, 'model', 'event',
            'strands.content.block', 'capture.redundant',
          );
        });
        register('ModelMessageEvent', (event) => {
          recordEvent(
            sink, traces, event, 'model', 'event',
            'strands.model.message', 'capture.redundant',
          );
        });
        register('BeforeToolCallEvent', (event) => {
          const trace = traceFor(event, traces);
          const toolUse = recordField(event, 'toolUse');
          const id = toolUse ? stringField(toolUse, 'toolUseId') : undefined;
          const toolName = toolUse ? stringField(toolUse, 'name') : undefined;
          const toolInput = toolUse ? ownValue(toolUse, 'input') : undefined;
          if (!active || !trace || !toolUse || !id || !toolName || toolInput === undefined) return;
          const key = operationKey(trace, id);
          const name = `strands.tool.${toolName}`;
          const toolFailure = failedTools.get(key);
          const recovery = toolFailure ? sink.record({
            kind: 'state', phase: 'event', name: 'strands.tool.recovery', trace,
            nativeIdentity: id, coverage: TOOL_EXECUTION_COVERAGE,
            native: { attempt: toolFailure.attempt + 1, event: snapshot(event) },
            ...(toolFailure.receipt.accepted
              ? { parentRecordId: toolFailure.receipt.recordId }
              : {}),
            semantic: {
              type: 'state.retry',
              state_type: 'tool.retry',
              framework: 'strands',
              value: { attempt: toolFailure.attempt + 1, call_id: id },
            },
          }) : undefined;
          if (toolFailure) failedTools.delete(key);
          const start = sink.record({
            kind: 'tool', phase: 'start', name, trace, nativeIdentity: id,
            coverage: TOOL_EXECUTION_COVERAGE,
            native: snapshot(event),
            ...(recovery?.accepted ? { parentRecordId: recovery.recordId } : {}),
            semantic: {
              type: 'tool.execution',
              framework: 'strands',
              call_id: id,
              native_call_id: id,
              name: toolName,
              input: snapshotNative(toolInput),
            },
          });
          tools.set(key, {
            trace,
            name,
            start,
            attempt: toolFailure?.attempt ? toolFailure.attempt + 1 : 1,
            reasoning: [],
          });
        });
        register('ToolStreamUpdateEvent', (event) => {
          recordEvent(
            sink, traces, event, 'stream', 'event',
            'strands.tool.stream.delta', 'capture.redundant',
          );
        });
        register('AfterToolCallEvent', (event) => {
          const toolUse = recordField(event, 'toolUse');
          const id = toolUse ? stringField(toolUse, 'toolUseId') : undefined;
          const trace = traceFor(event, traces);
          const key = trace && id ? operationKey(trace, id) : undefined;
          const open = key ? tools.get(key) : undefined;
          if (!active || !id || !key || !open) return;
          const error = event.error;
          const result = recordField(event, 'result');
          const resultStatus = result ? stringField(result, 'status') : undefined;
          const failed = (error !== undefined && error !== null) || resultStatus === 'error';
          const output = result ? ownValue(result, 'content') : undefined;
          const ended = sink.record({
            kind: 'tool', phase: failed ? 'error' : 'end', name: open.name, trace: open.trace,
            nativeIdentity: id, coverage: TOOL_EXECUTION_COVERAGE, native: snapshot(event),
            ...(open.start.accepted ? { parentRecordId: open.start.recordId } : {}),
            semantic: {
              type: failed ? 'tool.error' : 'tool.result',
              framework: 'strands',
              call_id: id,
              native_call_id: id,
              status: failed ? 'failed' : 'succeeded',
              ...(output === undefined ? {} : { output: snapshotNative(output) }),
              ...(failed
                ? { error: semanticError('tool.error', error ?? ownValue(result!, 'error'), event.retry === true) }
                : {}),
            },
          });
          if (result && ended.accepted) {
            toolResultsByBlock.set(result, {
              nativeCallId: id,
              result: snapshotNative(result),
              recordId: ended.recordId,
            });
          }
          if (failed) {
            const failure = sink.record({
              kind: 'error', phase: 'event', name: 'strands.tool.error', trace: open.trace,
              nativeIdentity: id, coverage: TOOL_EXECUTION_COVERAGE,
              native: { event: snapshot(event), error: errorSnapshot(error) },
              ...(ended.accepted ? { parentRecordId: ended.recordId } : {}),
              semantic: { type: 'capture.redundant', framework: 'strands' },
            });
            failedTools.set(key, { receipt: failure, attempt: open.attempt });
          }
          tools.delete(key);
        });
        register('ToolResultEvent', (event) => {
          recordEvent(
            sink, traces, event, 'tool', 'event',
            'strands.tool.result', 'capture.redundant',
          );
        });
        register('AgentResultEvent', (event) => {
          recordEvent(
            sink, traces, event, 'stream', 'end',
            'strands.agent.stream.terminal', 'capture.redundant',
          );
          const state = invocationState(event);
          if (state) {
            const invocation = traces.get(state);
            if (invocation) {
              const result = recordField(event, 'result');
              closeInvocation(sink, invocation, snapshot(event), result);
            }
            failedModels.delete(state);
            traces.delete(state);
            if (invocation) openInvocations.delete(invocation);
          }
        });
        register('InterruptEvent', (event) => {
          const trace = traceFor(event, traces);
          if (!trace) return;
          const interrupt = ownValue(event, 'interrupt');
          sink.record({
            kind: 'state', phase: 'event', name: 'strands.agent.interrupt', trace,
            native: snapshot(event),
            semantic: {
              type: 'state.interrupt',
              state_type: 'agent.interrupt',
              framework: 'strands',
              ...(interrupt === undefined ? {} : { value: snapshotNative(interrupt) }),
            },
          });
        });
      } catch (error) {
        active = false;
        runCleanups(cleanups);
        throw error;
      }

      return {
        deactivate() {
          if (!active) return;
          active = false;
          const errors = runCleanups(cleanups);
          if (errors.length > 0) throw new AggregateError(errors, 'Strands hook cleanup failed');
        },
        drain() {
          for (const invocation of openInvocations) {
            closeInvocation(
              sink,
              invocation,
              invocation.after ?? { terminal: 'source-drain' },
            );
          }
          openInvocations.clear();
          tools.clear();
          failedTools.clear();
        },
      };
    },
  }, 'deep');
}

function strandsResponseEvidence(value: unknown): {
  content?: unknown;
  reasoning: Array<{ type: 'text' | 'summary'; text: string }>;
  unavailableReasoning: number;
} {
  if (!Array.isArray(value)) {
    return {
      ...(value !== undefined ? { content: value } : {}),
      reasoning: [],
      unavailableReasoning: 0,
    };
  }
  const content: unknown[] = [];
  const reasoning: Array<{ type: 'text' | 'summary'; text: string }> = [];
  let unavailableReasoning = 0;
  for (const block of value) {
    if (block && typeof block === 'object'
      && ownValue(block as Record<PropertyKey, unknown>, 'type') === 'reasoningBlock') {
      const record = block as Record<PropertyKey, unknown>;
      const text = ownValue(record, 'text');
      if (typeof text === 'string' && text.length) reasoning.push({ type: 'text', text });
      else if (ownValue(record, 'redactedContent') !== undefined
        || ownValue(record, 'signature') !== undefined) unavailableReasoning += 1;
    } else {
      content.push(block);
    }
  }
  return {
    ...(content.length ? { content } : {}),
    reasoning,
    unavailableReasoning,
  };
}

function recordEvent(
  sink: SourceSink,
  traces: WeakMap<object, OpenInvocation>,
  event: Record<PropertyKey, unknown>,
  kind: 'state' | 'model' | 'tool' | 'stream',
  phase: 'event' | 'end',
  name: string,
  type: string,
): void {
  const trace = traceFor(event, traces);
  if (!trace) return;
  sink.record({ kind, phase, name, trace, native: snapshot(event), semantic: { type, framework: 'strands' } });
}

function recordPostMiddlewareContextGap(
  sink: SourceSink,
  invocation: OpenInvocation,
): void {
  if (invocation.contextGapRecorded) return;
  invocation.contextGapRecorded = true;
  sink.record({
    kind: 'unknown',
    phase: 'gap',
    name: 'strands.model.context.post_middleware_unavailable',
    trace: invocation.identity,
    native: {
      exposed_context: ['agent.messages', 'agent.systemPrompt', 'agent.toolRegistry'],
      missing_context: 'provider_visible_post_middleware',
    },
    semantic: {
      type: 'capture.gap',
      framework: 'strands',
      reason: 'strands_post_middleware_context_unavailable',
      count: 1,
      detail: 'Strands hooks do not expose the exact provider-visible context after middleware.',
    },
  });
}

function captureModelContext(
  sink: SourceSink,
  trace: TraceIdentity,
  event: Record<PropertyKey, unknown>,
  contextByMessage: WeakMap<object, ContextEvidence>,
  invocation: OpenInvocation,
): string[] {
  const agent = recordField(event, 'agent');
  const messages = agent ? ownValue(agent, 'messages') : undefined;
  const refs: string[] = [];
  const systemPrompt = agent ? ownValue(agent, 'systemPrompt') : undefined;
  if (systemPrompt !== undefined) {
    const known = invocation.systemContext;
    if (known && unchangedMessage(known, 'system', systemPrompt)) {
      appendUnique(refs, known.recordIds);
    } else {
      const receipt = sink.record({
        kind: 'state',
        phase: 'event',
        name: 'strands.model.context.system',
        trace,
        native: { system_prompt: snapshotNative(systemPrompt) },
        semantic: {
          type: 'message',
          framework: 'strands',
          origin: 'context',
          role: 'system',
          content: snapshotNative(systemPrompt),
        },
      });
      if (receipt.accepted) {
        refs.push(receipt.recordId);
        invocation.systemContext = {
          role: 'system',
          content: snapshotNative(systemPrompt),
          recordIds: [receipt.recordId],
        };
      }
    }
  }
  if (!Array.isArray(messages)) return refs;
  for (const value of messages) {
    if (!value || typeof value !== 'object') continue;
    const message = value as Record<PropertyKey, unknown>;
    const role = messageRole(message);
    const content = ownValue(message, 'content');
    const trackingId = stringField(message, 'trackingId');
    if (!role || content === undefined) continue;
    const known = contextByMessage.get(message);
    if (known && unchangedMessage(known, role, content)) {
      appendUnique(refs, known.recordIds);
      continue;
    }
    const receipt = sink.record({
      kind: 'state',
      phase: 'event',
      name: 'strands.model.context.message',
      trace,
      ...(trackingId ? { nativeIdentity: trackingId } : {}),
      native: { message: snapshotNative(message) },
      semantic: {
        type: 'message',
        framework: 'strands',
        origin: 'context',
        role,
        content: snapshotNative(content),
      },
    });
    if (!receipt.accepted) continue;
    refs.push(receipt.recordId);
    contextByMessage.set(message, {
      role,
      content: snapshotNative(content),
      recordIds: [receipt.recordId],
    });
  }
  return refs;
}

function exactToolResultRefs(
  content: unknown,
  toolResultsByBlock: WeakMap<object, ToolResultEvidence>,
): string[] | undefined {
  if (!Array.isArray(content) || content.length === 0) return undefined;
  const refs: string[] = [];
  for (const value of content) {
    if (!value || typeof value !== 'object') return undefined;
    const block = value as Record<PropertyKey, unknown>;
    const evidence = toolResultsByBlock.get(block);
    const nativeCallId = stringField(block, 'toolUseId');
    if (
      !evidence
      || nativeCallId !== evidence.nativeCallId
      || !sameSnapshot(snapshotNative(block), evidence.result)
    ) return undefined;
    appendUnique(refs, [evidence.recordId]);
  }
  return refs;
}

function unchangedMessage(
  known: ContextEvidence,
  role: MessageRole,
  content: unknown,
): boolean {
  return known.role === role
    && sameSnapshot(known.content, snapshotNative(content));
}

function modelIdentifier(event: Record<PropertyKey, unknown>): string | undefined {
  const model = recordField(event, 'model');
  return model ? stringField(model, 'modelId') : undefined;
}

function toolNames(event: Record<PropertyKey, unknown>): string[] {
  const agent = recordField(event, 'agent');
  const registry = agent ? recordField(agent, 'toolRegistry') : undefined;
  const list = registry?.list;
  if (typeof list !== 'function') return [];
  let tools: unknown;
  try {
    tools = Reflect.apply(list, registry, []);
  } catch {
    return [];
  }
  if (!Array.isArray(tools)) return [];
  const names: string[] = [];
  for (const tool of tools) {
    if (!tool || (typeof tool !== 'object' && typeof tool !== 'function')) continue;
    const spec = (tool as Record<PropertyKey, unknown>).toolSpec;
    if (!spec || (typeof spec !== 'object' && typeof spec !== 'function')) continue;
    const name = stringField(spec as Record<PropertyKey, unknown>, 'name');
    if (name && !names.includes(name)) names.push(name);
  }
  return names;
}

function sameSnapshot(left: unknown, right: unknown): boolean {
  try {
    return JSON.stringify(left) === JSON.stringify(right);
  } catch {
    return false;
  }
}

function appendUnique(target: string[], values: string[]): void {
  for (const value of values) {
    if (!target.includes(value)) target.push(value);
  }
}

function traceFor(event: Record<PropertyKey, unknown>, traces: WeakMap<object, OpenInvocation>): TraceIdentity | undefined {
  const state = invocationState(event);
  return state ? traces.get(state)?.identity : undefined;
}

function closeInvocation(
  sink: SourceSink,
  invocation: OpenInvocation,
  native: Record<string, unknown>,
  result?: Record<PropertyKey, unknown>,
): void {
  const stopReason = result ? stringField(result, 'stopReason') : undefined;
  const structuredOutput = result ? ownValue(result, 'structuredOutput') : undefined;
  sink.record({
    kind: 'lifecycle', phase: 'end', name: 'strands.agent.invocation',
    trace: invocation.identity, native,
    semantic: {
      type: 'agent.run',
      framework: 'strands',
      status: stopReason === 'endTurn'
        ? 'succeeded'
        : stopReason === 'cancelled'
          ? 'cancelled'
          : 'unknown',
      ...(structuredOutput === undefined
        ? {}
        : { output: snapshotNative(structuredOutput) }),
    },
  });
}

function invocationState(event: Record<PropertyKey, unknown>): Record<PropertyKey, unknown> | undefined {
  return recordField(event, 'invocationState');
}

function snapshot(event: Record<PropertyKey, unknown>): Record<string, unknown> {
  const value: Record<string, unknown> = {};
  let keys: PropertyKey[];
  try { keys = Reflect.ownKeys(event); } catch {
    return { $semantic_layer_omitted: 'descriptors_unavailable' };
  }
  for (const key of keys) {
    if (typeof key !== 'string' || key === 'agent' || key === 'invocationState' || key === 'model' || key === 'tool') continue;
    let descriptor: PropertyDescriptor | undefined;
    try { descriptor = Object.getOwnPropertyDescriptor(event, key); } catch {
      value[key] = { $semantic_layer_omitted: 'descriptor_unavailable' };
      continue;
    }
    value[key] = descriptor && 'value' in descriptor
      ? descriptor.value
      : { $semantic_layer_omitted: 'accessor' };
  }
  const state = Object.getOwnPropertyDescriptor(event, 'invocationState');
  value.invocation_state = state && 'value' in state ? state.value ?? null : { $semantic_layer_omitted: 'accessor' };
  const agent = Object.getOwnPropertyDescriptor(event, 'agent');
  if (agent && 'value' in agent && agent.value && typeof agent.value === 'object') {
    const messages = Object.getOwnPropertyDescriptor(agent.value, 'messages');
    value.agent_messages = messages && 'value' in messages
      ? messages.value ?? null
      : { $semantic_layer_omitted: 'accessor_or_missing' };
  }
  return snapshotRecord(value);
}

function errorSnapshot(value: unknown): unknown {
  return snapshotNative(value);
}

function isCancellationError(value: unknown): boolean {
  return value instanceof Error
    && ['AbortError', 'CancelledError', 'CanceledError'].includes(value.name);
}

function isContextOverflowError(value: unknown): boolean {
  return value instanceof Error && value.name === 'ContextWindowOverflowError';
}

function findStructuredError(value: unknown, seen: WeakSet<object>): unknown {
  if (typeof value === 'string') {
    const start = value.indexOf('{');
    if (start >= 0) {
      try { return findStructuredError(JSON.parse(value.slice(start)), seen); } catch { return undefined; }
    }
    return undefined;
  }
  if (!value || typeof value !== 'object' || seen.has(value)) return undefined;
  seen.add(value);
  if (Array.isArray(value)) {
    for (const child of value) {
      const found = findStructuredError(child, seen);
      if (found !== undefined) return found;
    }
    return undefined;
  }
  const record = value as Record<string, unknown>;
  const custom = record.custom as Record<string, unknown> | undefined;
  if (custom?.code === 'SEMANTIC_LAYER_CAPTURE_FAILURE_V1') return value;
  for (const child of Object.values(record)) {
    const found = findStructuredError(child, seen);
    if (found !== undefined) return found;
  }
  return undefined;
}

function operationKey(trace: TraceIdentity, nativeIdentity: string): string {
  return `${trace.traceId}:${nativeIdentity}`;
}

function runCleanups(cleanups: Array<() => void>): unknown[] {
  const errors: unknown[] = [];
  for (const cleanup of cleanups.splice(0).reverse()) {
    try { cleanup(); } catch (error) { errors.push(error); }
  }
  return errors;
}

function requireEvents(sdk: StrandsModule): Map<string, EventConstructor> {
  const events = new Map<string, EventConstructor>();
  for (const name of REQUIRED_EVENTS) {
    const value = sdk[name];
    if (typeof value !== 'function') throw new TypeError(`Strands SDK must export ${name}`);
    events.set(name, value as EventConstructor);
  }
  return events;
}

function isHookSubject(value: object): value is HookSubject {
  return typeof (value as Partial<HookSubject>).addHook === 'function';
}

function recordField(value: Record<PropertyKey, unknown>, key: string): Record<PropertyKey, unknown> | undefined {
  const field = value[key];
  return field !== null && typeof field === 'object' ? field as Record<PropertyKey, unknown> : undefined;
}

function stringField(value: Record<PropertyKey, unknown>, key: string): string | undefined {
  return typeof value[key] === 'string' ? value[key] : undefined;
}

function messageRole(
  value: Record<PropertyKey, unknown>,
): 'user' | 'assistant' | undefined {
  const role = stringField(value, 'role');
  return role === 'user' || role === 'assistant' ? role : undefined;
}

function numberField(value: Record<PropertyKey, unknown>, key: string): number | undefined {
  return typeof value[key] === 'number' && Number.isInteger(value[key]) ? value[key] : undefined;
}

function integerField(value: Record<PropertyKey, unknown>, key: string): number | undefined {
  const field = ownValue(value, key);
  return typeof field === 'number' && Number.isSafeInteger(field) ? field : undefined;
}

function ownValue(value: Record<PropertyKey, unknown>, key: string): unknown {
  let descriptor: PropertyDescriptor | undefined;
  try { descriptor = Object.getOwnPropertyDescriptor(value, key); } catch { return undefined; }
  return descriptor && 'value' in descriptor ? descriptor.value : undefined;
}

function tokenUsage(value: unknown): TokenUsage | undefined {
  if (!value || typeof value !== 'object') return undefined;
  const usage = value as Record<PropertyKey, unknown>;
  const input = integerField(usage, 'inputTokens');
  const output = integerField(usage, 'outputTokens');
  if ((input === undefined || input < 0) && (output === undefined || output < 0)) return undefined;
  return {
    ...(input !== undefined && input >= 0 ? { input_tokens: input } : {}),
    ...(output !== undefined && output >= 0 ? { output_tokens: output } : {}),
  };
}

function toolUseFrom(
  block: Record<PropertyKey, unknown>,
): { id: string; name: string; input: unknown } | undefined {
  const direct = stringField(block, 'type') === 'toolUseBlock'
    ? block
    : recordField(block, 'toolUse');
  if (!direct) return undefined;
  const id = stringField(direct, 'toolUseId');
  const name = stringField(direct, 'name');
  const input = ownValue(direct, 'input');
  return id && name && input !== undefined ? { id, name, input } : undefined;
}

function semanticError(
  type: 'model.error' | 'tool.error',
  error: unknown,
  recoverable: boolean,
): Record<string, unknown> {
  const copied = errorSnapshot(error);
  const record = copied && typeof copied === 'object' && !Array.isArray(copied)
    ? copied as Record<string, unknown>
    : {};
  return {
    type,
    message: typeof record.message === 'string' ? record.message : 'Strands operation failed.',
    recoverable,
  };
}
