import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  AcceptedReceipt,
  AdmissionReceipt,
  CaptureSource,
  OpenTraceReceipt,
  SourceSink,
  TraceIdentity,
} from '../v1/types.js';
import { trustOfficialSource } from '../v1/source-ownership.js';
import { snapshotNative, snapshotRecord } from './native-snapshot.js';

type AISDKAdapterOptions = { version?: string };
type Telemetry = Record<string, ((event: unknown) => unknown) | undefined>;
type AISDKSubject = {
  registerTelemetry(...integrations: Telemetry[]): void;
  streamText(...args: unknown[]): unknown;
};
type CallTerminal = { phase: 'end' | 'error' | 'cancelled'; event: unknown };
type ModelContext = {
  snapshots: unknown[];
  refs: Array<string | undefined>;
};
type OpenCall = {
  identity: TraceIdentity;
  name: string;
  start: AcceptedReceipt;
  invocation?: Invocation;
  activeModel?: AdmissionReceipt;
  modelSequence: number;
  modelContext?: ModelContext;
};
type OpenTool = { trace: TraceIdentity; name: string; toolName: string; start: AdmissionReceipt };
type FailedToolCall = { receipt: AdmissionReceipt; toolName: string };
export type AISDKRunIdentity = Readonly<{
  conversationId: string;
  turnId: string;
  turnIndex: number;
  previousTurnId?: string;
}>;
type Invocation = {
  identity?: AISDKRunIdentity;
  sequence: number;
  retained: boolean;
  overflowTracked: boolean;
  trace?: TraceIdentity;
  callId?: string;
  finish?: (phase: 'end' | 'error' | 'cancelled', event: unknown) => void;
  streamObserved: boolean;
  streamTerminal?: CallTerminal;
  telemetryTerminal?: CallTerminal;
  pending: Array<(trace: TraceIdentity) => void>;
};
type RecordValue = Record<PropertyKey, unknown>;
type Bridge = {
  active: boolean;
  installed: boolean;
  failed: boolean;
  invocations: AsyncLocalStorage<Invocation>;
  sink?: SourceSink;
  subject?: AISDKSubject;
  telemetry?: Telemetry;
  nextSequence: number;
  missingTelemetry: Map<number, Invocation>;
  missingTelemetryOverflow: number;
};

const MAX_PENDING_TELEMETRY_INVOCATIONS = 256;
const QUALIFIED_AI_SDK_VERSIONS = new Set(['7.0.22', '7.0.21']);

export type AISDKCaptureAdapter = {
  createSource(client: object): CaptureSource;
  streamText(...args: unknown[]): unknown;
  streamTextWithIdentity(identity: AISDKRunIdentity, ...args: unknown[]): unknown;
};

/**
 * AI SDK adapter for the real module containing official registerTelemetry/streamText exports.
 * It never reads result promises or creates a second stream consumer.
 */
export function aiSDKAdapter(options: AISDKAdapterOptions = {}): AISDKCaptureAdapter {
  const bridge: Bridge = {
    active: false,
    installed: false,
    failed: false,
    invocations: new AsyncLocalStorage<Invocation>(),
    nextSequence: 0,
    missingTelemetry: new Map(),
    missingTelemetryOverflow: 0,
  };
  const streamTextInvocation = (
    identity: AISDKRunIdentity | undefined,
    args: unknown[],
  ): unknown => {
    if (!bridge.subject || !bridge.installed) {
      throw new Error('AI SDK adapter must be installed before streamText');
    }
    const subject = bridge.subject;
    if (!bridge.active || !bridge.sink) return Reflect.apply(subject.streamText, undefined, args);
    const sink = bridge.sink;
    const invocation = retainInvocation(bridge, identity);
    return bridge.invocations.run(invocation, () => {
      const result = Reflect.apply(
        subject.streamText,
        undefined,
        composePerCallTelemetry(args, bridge.telemetry),
      );
      return instrumentResult(result, sink, invocation);
    });
  };
  return Object.freeze({
    createSource(client) {
      if (!isSubject(client)) {
        throw new TypeError('AI SDK module must export registerTelemetry and streamText');
      }
      if (bridge.subject && bridge.subject !== client) throw new Error('AI SDK adapter is already bound');
      bridge.subject = client;
      return createAISDKSource(client, options.version, bridge);
    },
    streamText(...args: unknown[]) {
      return streamTextInvocation(undefined, args);
    },
    streamTextWithIdentity(identity: AISDKRunIdentity, ...args: unknown[]) {
      return streamTextInvocation(validateRunIdentity(identity), args);
    },
  });
}

function createAISDKSource(
  subject: AISDKSubject,
  version: string | undefined,
  bridge: Bridge,
): CaptureSource {
  return trustOfficialSource({
    metadata: {
      name: 'official:ai-sdk',
      seam: 'registerTelemetry/fullStream/tool.execute',
      identityDomain: 'ai-sdk.generation',
      ...(version ? { version } : {}),
      qualification: version === undefined
        ? { status: 'unknown' }
        : QUALIFIED_AI_SDK_VERSIONS.has(version)
          ? { status: 'exact_qualified' }
          : { status: 'capability_checked_unqualified', profile: 'ai-sdk-telemetry-v1' },
      official: true,
      coverage: [{ operation: 'generation', domain: 'ai-sdk.generation', role: 'owner' }],
    },
    install(sink) {
      if (bridge.active || bridge.sink || bridge.installed || bridge.failed) {
        throw new Error('AI SDK capture adapter is already installed');
      }
      const calls = new Map<string, OpenCall>();
      const tools = new Map<string, OpenTool>();
      const failedCalls = new Map<string, FailedToolCall>();
      let health: OpenTraceReceipt | undefined;
      const telemetry: Telemetry = {
        onStart(event) {
          if (!bridge.active || !isRecord(event)) return;
          const invocation = bridge.invocations.getStore();
          if (invocation) releaseInvocation(bridge, invocation);
          const callId = stringField(event, 'callId');
          if (!callId) {
            if (health?.accepted) sink.record({
              kind: 'unknown', phase: 'gap', name: 'ai_sdk.telemetry.invalid', trace: health.identity,
              native: { reason: 'missing_call_id', event: snapshotNative(event) },
              semantic: { coverage: 'missing', framework: 'ai-sdk' },
            });
            return;
          }
          const operation = stringField(event, 'operationId') ?? 'ai.streamText';
          const name = `ai_sdk.${operation}`;
          const opened = sink.openTrace({
            name,
            nativeIdentity: callId,
            ...(invocation?.identity ? {
              conversationId: invocation.identity.conversationId,
              turnId: invocation.identity.turnId,
              turnIndex: invocation.identity.turnIndex,
              ...(invocation.identity.previousTurnId
                ? { previousTurnId: invocation.identity.previousTurnId } : {}),
            } : {}),
            native: snapshotRecord(event),
            semantic: generationStartSemantic(event, name),
          });
          if (!opened.accepted) return;
          calls.set(callId, {
            identity: opened.identity, name, start: opened,
            modelSequence: 0,
            ...(invocation ? { invocation } : {}),
          });
          if (invocation) {
            invocation.trace = opened.identity;
            invocation.callId = callId;
            invocation.finish = (phase, terminalEvent) => {
              const terminal = { phase, event: terminalEvent };
              invocation.streamTerminal = terminal;
              if (invocation.telemetryTerminal) {
                finishCall(sink, calls, invocation.telemetryTerminal.event, invocation.telemetryTerminal.phase);
              } else if (phase !== 'end') {
                finishCall(sink, calls, { callId, terminalEvent }, phase);
              }
            };
            for (const record of invocation.pending.splice(0)) record(opened.identity);
          }
          sink.record({
            kind: 'correlation', phase: 'event', name: 'ai_sdk.operation.start',
            trace: opened.identity, nativeIdentity: callId, native: snapshotRecord(event),
            parentRecordId: opened.recordId,
            semantic: { type: 'capture.redundant', framework: 'ai-sdk' },
          });
        },
        onLanguageModelCallStart(event) {
          if (!bridge.active) return;
          if (!isRecord(event)) return;
          const call = callFor(calls, event);
          if (!call) return;
          const contextRefs = recordModelContext(sink, call, event);
          call.activeModel = sink.record({
            kind: 'model', phase: 'event', name: 'ai_sdk.model.request',
            trace: call.identity, nativeIdentity: stringField(event, 'callId'),
            native: snapshotRecord(event),
            parentRecordId: call.start.recordId,
            semantic: modelRequestSemantic(event, contextRefs),
          });
        },
        onLanguageModelCallEnd(event) {
          if (!bridge.active || !isRecord(event)) return;
          const call = callFor(calls, event);
          if (!call) return;
          const response = sink.record({
            kind: 'model', phase: 'event', name: 'ai_sdk.model.response',
            trace: call.identity, nativeIdentity: stringField(event, 'callId'),
            native: snapshotRecord(event),
            parentRecordId: acceptedRecordId(call.activeModel)
              ?? call.start.recordId,
            semantic: modelResponseSemantic(event),
          });
          call.activeModel = undefined;
          if (event.usage !== undefined) {
            sink.record({
              kind: 'model', phase: 'event', name: 'ai_sdk.usage',
              trace: call.identity, nativeIdentity: stringField(event, 'callId'),
              native: snapshotRecord(event),
              parentRecordId: acceptedRecordId(response) ?? call.start.recordId,
              semantic: { type: 'capture.redundant', framework: 'ai-sdk' },
            });
          }
          recordToolProposals(sink, call, event, response);
        },
        onToolExecutionStart(event) {
          if (!bridge.active || !isRecord(event)) return;
          const callId = stringField(event, 'callId') ?? '';
          const call = calls.get(callId);
          const toolCall = nestedRecord(event, 'toolCall') ?? event;
          const toolCallId = stringField(toolCall, 'toolCallId');
          if (!call || !toolCallId) return;
          const toolName = stringField(toolCall, 'toolName') ?? stringField(event, 'toolName') ?? 'execute';
          const name = `ai_sdk.tool.${toolName}`;
          const failure = failedCalls.get(callId);
          const recovery = failure?.toolName === toolName ? sink.record({
            kind: 'state', phase: 'event', name: 'ai_sdk.tool.recovery', trace: call.identity,
            nativeIdentity: toolCallId,
            native: snapshotRecord({ attempt: 1, retry_tool: toolName, event }),
            ...(failure.receipt.accepted ? { parentRecordId: failure.receipt.recordId } : {}),
            semantic: {
              type: 'state.transition',
              framework: 'ai-sdk',
              state_type: 'tool.retry',
              value: { tool: toolName },
            },
          }) : undefined;
          if (recovery) failedCalls.delete(callId);
          const start = sink.record({
            kind: 'tool', phase: 'start', name, trace: call.identity,
            nativeIdentity: toolCallId, native: snapshotRecord(event),
            parentRecordId: acceptedRecordId(recovery)
              ?? call.start.recordId,
            semantic: toolCallSemantic(toolCall, toolName, toolCallId),
          });
          tools.set(toolKey(callId, toolCallId), { trace: call.identity, name, toolName, start });
        },
        onToolExecutionEnd(event) {
          if (!bridge.active || !isRecord(event)) return;
          const callId = stringField(event, 'callId') ?? '';
          const toolCall = nestedRecord(event, 'toolCall') ?? event;
          const toolOutput = nestedRecord(event, 'toolOutput');
          const toolCallId = stringField(toolCall, 'toolCallId')
            ?? (toolOutput ? stringField(toolOutput, 'toolCallId') : undefined);
          const key = toolCallId ? toolKey(callId, toolCallId) : undefined;
          const open = key ? tools.get(key) : undefined;
          if (!open || !toolCallId) return;
          const failed = toolOutput?.type === 'tool-error' || event.success === false;
          const ended = sink.record({
            kind: 'tool', phase: failed ? 'error' : 'end', name: open.name, trace: open.trace,
            nativeIdentity: toolCallId, native: snapshotRecord(event),
            ...(open.start.accepted ? { parentRecordId: open.start.recordId } : {}),
            semantic: toolResultSemantic(event, toolOutput, toolCallId, failed),
          });
          if (failed) {
            const failure = sink.record({
              kind: 'error', phase: 'event', name: 'ai_sdk.tool.error', trace: open.trace,
              nativeIdentity: toolCallId, native: snapshotRecord(event),
              ...(ended.accepted ? { parentRecordId: ended.recordId } : {}),
              semantic: { type: 'capture.redundant', framework: 'ai-sdk' },
            });
            failedCalls.set(callId, { receipt: failure, toolName: open.toolName });
          }
          tools.delete(key!);
        },
        onEnd(event) {
          if (!bridge.active) return;
          finishTelemetryCall(sink, calls, event, 'end');
          if (isRecord(event)) failedCalls.delete(stringField(event, 'callId') ?? '');
        },
        onAbort(event) {
          if (!bridge.active) return;
          finishTelemetryCall(sink, calls, event, 'cancelled');
        },
        onError(event) {
          if (!bridge.active) return;
          const call = isRecord(event) ? calls.get(stringField(event, 'callId') ?? '') : undefined;
          const trace = call?.identity;
          if (!trace) return;
          sink.record({
            kind: 'error', phase: 'event', name: 'ai_sdk.error', trace,
            native: directErrorEvidence(isRecord(event) ? event.error : event),
            semantic: {
              type: 'model.error', framework: 'ai-sdk', capture_callback: 'onError',
              error: semanticError(isRecord(event) ? event.error : event, 'model_error'),
              ...(isRecord(event) && stringField(event, 'callId')
                ? { call_id: stringField(event, 'callId') } : {}),
            },
          });
          if (call) finishTelemetryCall(sink, calls, event, 'error');
        },
      };
      try {
        subject.registerTelemetry(telemetry);
        bridge.telemetry = telemetry;
        health = sink.openTrace({
          name: 'ai_sdk.adapter.health',
          native: null,
          semantic: { type: 'capture.redundant', framework: 'ai-sdk' },
        });
      } catch (error) {
        bridge.failed = true;
        throw error;
      }
      bridge.active = true;
      bridge.installed = true;
      bridge.sink = sink;
      return {
        deactivate() {
          bridge.active = false;
        },
        async drain() {
          await drainMissingTelemetry(bridge, sink, health?.accepted ? health.identity : undefined);
          if (health?.accepted) {
            await sink.record({
              kind: 'lifecycle', phase: 'end', name: 'ai_sdk.adapter.health', trace: health.identity,
              native: null,
              semantic: { type: 'capture.redundant', framework: 'ai-sdk' },
            }).settled;
          }
          bridge.sink = undefined;
          failedCalls.clear();
        },
      };
    },
  }, 'deep');
}

function composePerCallTelemetry(args: unknown[], integration: Telemetry | undefined): unknown[] {
  const input = args[0];
  if (!integration || !isRecord(input)) return args;
  const primary = input.telemetry;
  const key = primary !== undefined ? 'telemetry' : 'experimental_telemetry';
  const options = primary !== undefined ? primary : input.experimental_telemetry;
  if (!isRecord(options) || options.integrations == null) return args;
  const integrations = Array.isArray(options.integrations)
    ? [...options.integrations, integration]
    : [options.integrations, integration];
  return [{
    ...input,
    [key]: { ...options, integrations },
  }, ...args.slice(1)];
}

function retainInvocation(bridge: Bridge, identity?: AISDKRunIdentity): Invocation {
  const sequence = ++bridge.nextSequence;
  const retained = bridge.missingTelemetry.size < MAX_PENDING_TELEMETRY_INVOCATIONS;
  const invocation: Invocation = {
    ...(identity ? { identity } : {}),
    sequence,
    retained,
    overflowTracked: !retained,
    streamObserved: false,
    pending: [],
  };
  if (retained) bridge.missingTelemetry.set(sequence, invocation);
  else bridge.missingTelemetryOverflow += 1;
  return invocation;
}

function validateRunIdentity(value: AISDKRunIdentity): AISDKRunIdentity {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError('AI SDK run identity must be an object');
  }
  const conversationId = identityString(value, 'conversationId');
  const turnId = identityString(value, 'turnId');
  const turnIndexValue = ownDataValue(value, 'turnIndex');
  if (!Number.isSafeInteger(turnIndexValue) || Number(turnIndexValue) < 0) {
    throw new TypeError('AI SDK run identity turnIndex must be a non-negative safe integer');
  }
  const previousValue = ownDataValue(value, 'previousTurnId');
  const previousTurnId = previousValue === undefined
    ? undefined
    : identityString(value, 'previousTurnId');
  return Object.freeze({
    conversationId,
    turnId,
    turnIndex: Number(turnIndexValue),
    ...(previousTurnId ? { previousTurnId } : {}),
  });
}

function identityString(
  value: AISDKRunIdentity,
  key: 'conversationId' | 'turnId' | 'previousTurnId',
): string {
  const observed = ownDataValue(value, key);
  if (typeof observed !== 'string' || observed.trim().length === 0) {
    throw new TypeError(`AI SDK run identity ${key} must be a non-empty string`);
  }
  return observed;
}

function ownDataValue(value: object, key: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor && 'value' in descriptor ? descriptor.value : undefined;
}

function releaseInvocation(bridge: Bridge, invocation: Invocation): void {
  if (invocation.retained) {
    bridge.missingTelemetry.delete(invocation.sequence);
    invocation.retained = false;
  }
  if (invocation.overflowTracked) {
    bridge.missingTelemetryOverflow = Math.max(0, bridge.missingTelemetryOverflow - 1);
    invocation.overflowTracked = false;
  }
}

async function drainMissingTelemetry(
  bridge: Bridge,
  sink: SourceSink,
  healthTrace: TraceIdentity | undefined,
): Promise<void> {
  const pending = [...bridge.missingTelemetry.values()];
  bridge.missingTelemetry.clear();
  const overflow = bridge.missingTelemetryOverflow;
  bridge.missingTelemetryOverflow = 0;
  if (!healthTrace) return;
  const settled: Promise<void>[] = [];
  for (const invocation of pending) {
    invocation.retained = false;
    settled.push(...recordMissingTelemetryGap(sink, healthTrace, invocation, 1, false));
  }
  if (overflow > 0) {
    settled.push(...recordMissingTelemetryGap(sink, healthTrace, undefined, overflow, true));
  }
  await Promise.allSettled(settled);
}

function recordMissingTelemetryGap(
  sink: SourceSink,
  healthTrace: TraceIdentity,
  invocation: Invocation | undefined,
  count: number,
  overflow: boolean,
): Promise<void>[] {
  const nativeIdentity = invocation ? `adapter-invocation-${invocation.sequence}` : undefined;
  if (invocation) {
    invocation.trace = healthTrace;
    for (const record of invocation.pending.splice(0)) record(healthTrace);
  }
  const gap = sink.record({
    kind: 'unknown', phase: 'gap', name: 'ai_sdk.telemetry.missing', trace: healthTrace,
    ...(nativeIdentity ? { nativeIdentity } : {}),
    native: {
      reason: 'official_telemetry_not_emitted',
      count,
      bounded_overflow: overflow,
      retained_limit: MAX_PENDING_TELEMETRY_INVOCATIONS,
    },
    semantic: { coverage: 'missing', framework: 'ai-sdk' },
  });
  return [gap.settled];
}

function instrumentResult(result: unknown, sink: SourceSink, invocation: Invocation): unknown {
  if (!isRecord(result)) return result;
  if (!Object.isExtensible(result)) {
    recordStreamGap(sink, invocation, 'result', 'non_extensible_result_surface');
    return result;
  }
  const wrappedStreams = new WeakMap<object, object>();
  return new Proxy(result, {
    get(target, property) {
      // Keep official final-value getters on the real result so their internal stream reads do
      // not make a final-only application call look like an application-consumed stream.
      if (!isStreamProperty(property)) return Reflect.get(target, property, target);
      invocation.streamObserved = true;
      // Invoke official getters against their real instance. Passing the proxy as receiver can
      // re-enter nested result getters and observes one application read more than once.
      const value = Reflect.get(target, property, target);
      if (!isRecord(value)) return value;
      const existing = wrappedStreams.get(value);
      if (existing) return existing;
      const wrapped = observeAsyncIterable(value, property, sink, invocation);
      wrappedStreams.set(value, wrapped);
      return wrapped;
    },
  });
}

function observeAsyncIterable(
  stream: RecordValue,
  property: string,
  sink: SourceSink,
  invocation: Invocation,
): RecordValue {
  const factory = stream[Symbol.asyncIterator];
  if (typeof factory !== 'function') {
    recordStreamGap(sink, invocation, property, 'not_async_iterable');
    return stream;
  }
  const ownFactory = Object.getOwnPropertyDescriptor(stream, Symbol.asyncIterator);
  if (ownFactory && ownFactory.configurable === false && 'value' in ownFactory && ownFactory.writable === false) {
    recordStreamGap(sink, invocation, property, 'non_extensible_stream_surface');
    return stream;
  }
  return new Proxy(stream, {
    get(target, key, receiver) {
      if (key !== Symbol.asyncIterator) return Reflect.get(target, key, receiver);
      return function observedAsyncIterator(this: unknown) {
        const iterator = Reflect.apply(factory, target, []);
        return observeIterator(iterator, property, sink, invocation);
      };
    },
  });
}

function observeIterator(
  iterator: AsyncIterator<unknown>,
  property: string,
  sink: SourceSink,
  invocation: Invocation,
): AsyncIterator<unknown> {
  return new Proxy(iterator, {
    get(target, key, receiver) {
      if (key === 'next') return (...args: [] | [unknown]) => Promise.resolve(target.next(...args)).then(
        (part) => {
          recordStreamPart(sink, invocation, property, part);
          if (part.done) invocation.finish?.('end', { surface: property, consumed: true });
          return part;
        },
        (error: unknown) => {
          recordStreamError(sink, invocation, property, 'next', error);
          invocation.finish?.('error', { surface: property, control: 'next', error });
          throw error;
        },
      );
      if (key === 'return' && typeof target.return === 'function') {
        return (...args: [] | [unknown]) => Promise.resolve(Reflect.apply(target.return!, target, args)).then(
          (part) => {
            withInvocationTrace(invocation, (identity) => sink.record({
              kind: 'stream', phase: 'event', name: 'ai_sdk.stream.control', trace: identity,
              ...(invocation.callId ? { nativeIdentity: invocation.callId } : {}),
              native: snapshotRecord({ surface: property, control: 'return', arguments: args, result: part }),
              semantic: { type: 'capture.redundant', framework: 'ai-sdk' },
            }));
            if (!part.done) recordStreamPart(sink, invocation, property, part);
            else {
              withInvocationTrace(invocation, (identity) => sink.record({
                kind: 'stream', phase: 'cancelled', name: 'ai_sdk.stream.cancelled', trace: identity,
                ...(invocation.callId ? { nativeIdentity: invocation.callId } : {}),
                native: snapshotRecord({ surface: property, consumer_cancelled: true, arguments: args, result: part }),
                semantic: { type: 'capture.redundant', framework: 'ai-sdk' },
              }));
              invocation.finish?.('cancelled', {
                surface: property, control: 'return', arguments: args, result: part,
              });
            }
            return part;
          },
          (error: unknown) => {
            recordStreamError(sink, invocation, property, 'return', error);
            invocation.finish?.('error', { surface: property, control: 'return', error });
            throw error;
          },
        );
      }
      if (key === 'throw' && typeof target.throw === 'function') {
        return (...args: [] | [unknown]) => Promise.resolve(Reflect.apply(target.throw!, target, args)).then(
          (part) => {
            withInvocationTrace(invocation, (identity) => sink.record({
              kind: 'stream', phase: 'event', name: 'ai_sdk.stream.control', trace: identity,
              ...(invocation.callId ? { nativeIdentity: invocation.callId } : {}),
              native: snapshotRecord({ surface: property, control: 'throw', arguments: args, result: part }),
              semantic: { type: 'capture.redundant', framework: 'ai-sdk' },
            }));
            if (!part.done) recordStreamPart(sink, invocation, property, part);
            else invocation.finish?.('error', {
              surface: property, control: 'throw', arguments: args, result: part,
            });
            return part;
          },
          (thrown: unknown) => {
            recordStreamError(sink, invocation, property, 'throw', thrown);
            invocation.finish?.('error', { surface: property, control: 'throw', error: thrown });
            throw thrown;
          },
        );
      }
      if (key === Symbol.asyncIterator) return () => receiver as AsyncIterator<unknown>;
      const value = Reflect.get(target as object, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function recordStreamPart(
  sink: SourceSink,
  invocation: Invocation,
  property: string,
  part: IteratorResult<unknown>,
): void {
  const stateTransition = !part.done && isRecord(part.value)
    && (part.value.type === 'start-step' || part.value.type === 'finish-step');
  withInvocationTrace(invocation, (identity) => sink.record({
    kind: stateTransition ? 'state' : 'stream', phase: part.done ? 'end' : 'event',
    name: part.done ? 'ai_sdk.stream.terminal'
      : stateTransition ? 'ai_sdk.step.transition' : 'ai_sdk.stream.delta',
    trace: identity,
    ...(invocation.callId ? { nativeIdentity: invocation.callId } : {}),
    native: part.done
      ? snapshotRecord({ surface: property, consumed: true, result: part })
      : snapshotRecord(part.value),
    semantic: {
      type: 'capture.redundant',
      framework: 'ai-sdk',
    },
  }));
}

function recordStreamError(
  sink: SourceSink,
  invocation: Invocation,
  property: string,
  control: 'next' | 'return' | 'throw',
  error: unknown,
): void {
  withInvocationTrace(invocation, (identity) => sink.record({
    kind: 'error', phase: 'error', name: 'ai_sdk.stream.error', trace: identity,
    ...(invocation.callId ? { nativeIdentity: invocation.callId } : {}),
    ...(typeof error === 'object' && error !== null ? { errorIdentity: error } : {}),
    // Keep provider/framework error fields directly addressable in canonical evidence.
    // Nesting the only lossless error snapshot below an adapter wrapper makes rich
    // cause/group/custom fields needlessly framework-specific to navigate.
    native: directErrorEvidence(error),
    semantic: {
      type: 'stream.error', framework: 'ai-sdk',
      capture_surface: property, capture_control: control,
      error: semanticError(error, 'stream_error'),
    },
  }));
}

function directErrorEvidence(error: unknown): RecordValue {
  const native = snapshotNative(error);
  return isRecord(native) ? native : { error: native };
}

function isStreamProperty(property: PropertyKey): property is 'fullStream' | 'stream' | 'textStream' {
  return property === 'fullStream' || property === 'stream' || property === 'textStream';
}

function recordStreamGap(
  sink: SourceSink,
  invocation: Invocation,
  property: string,
  reason: string,
): void {
  withInvocationTrace(invocation, (identity) => sink.record({
    kind: 'stream', phase: 'gap', name: 'ai_sdk.stream.unobservable', trace: identity,
    ...(invocation.callId ? { nativeIdentity: invocation.callId } : {}),
    native: { surface: property, reason },
    semantic: { type: 'stream.gap', framework: 'ai-sdk' },
  }));
}

function withInvocationTrace(invocation: Invocation, record: (trace: TraceIdentity) => void): void {
  if (invocation.trace) record(invocation.trace);
  else invocation.pending.push(record);
}

function recordToolProposals(
  sink: SourceSink,
  call: OpenCall,
  event: RecordValue,
  response: AdmissionReceipt,
): void {
  const content = Array.isArray(event.content) ? event.content : [];
  for (const item of content) {
    if (!isRecord(item) || !String(item.type).includes('tool')) continue;
    const toolCallId = stringField(item, 'toolCallId');
    const toolName = stringField(item, 'toolName');
    if (!toolCallId || !toolName || item.input === undefined) continue;
    sink.record({
      kind: 'tool', phase: 'event', name: 'ai_sdk.tool.proposed', trace: call.identity,
      nativeIdentity: toolCallId, native: snapshotRecord(item),
      parentRecordId: acceptedRecordId(response) ?? call.start.recordId,
      semantic: {
        type: 'tool.proposal',
        framework: 'ai-sdk',
        call_id: toolCallId,
        name: toolName,
        input: snapshotNative(item.input),
      },
    });
  }
}

function finishCall(
  sink: SourceSink,
  calls: Map<string, OpenCall>,
  event: unknown,
  phase: 'end' | 'error' | 'cancelled',
): void {
  if (!isRecord(event)) return;
  const callId = stringField(event, 'callId');
  const call = callId ? calls.get(callId) : undefined;
  if (!call) return;
  sink.record({
    kind: 'lifecycle', phase, name: call.name, trace: call.identity,
    nativeIdentity: callId, native: snapshotRecord(event),
    parentRecordId: call.start.recordId,
    semantic: generationOutcomeSemantic(event, phase),
  });
  calls.delete(callId!);
}

function finishTelemetryCall(
  sink: SourceSink,
  calls: Map<string, OpenCall>,
  event: unknown,
  phase: 'end' | 'error' | 'cancelled',
): void {
  if (!isRecord(event)) return;
  const callId = stringField(event, 'callId');
  const call = callId ? calls.get(callId) : undefined;
  if (call?.invocation?.streamObserved) {
    if (call.invocation.streamTerminal) {
      finishCall(sink, calls, event, phase);
    } else {
      call.invocation.telemetryTerminal = { phase, event };
    }
    return;
  }
  finishCall(sink, calls, event, phase);
}

function callFor(
  calls: Map<string, OpenCall>,
  event: RecordValue,
): OpenCall | undefined {
  const callId = stringField(event, 'callId');
  return callId ? calls.get(callId) : undefined;
}

function acceptedRecordId(receipt: AdmissionReceipt | undefined): string | undefined {
  return receipt?.accepted ? receipt.recordId : undefined;
}

function generationStartSemantic(
  event: RecordValue,
  name: string,
): Record<string, unknown> {
  const input: Record<string, unknown> = {};
  if (event.system !== undefined) input.system = snapshotNative(event.system);
  if (event.messages !== undefined) input.messages = snapshotNative(event.messages);
  return {
    type: 'agent.run',
    framework: 'ai-sdk',
    name,
    input,
  };
}

function generationOutcomeSemantic(
  event: RecordValue,
  phase: 'end' | 'error' | 'cancelled',
): Record<string, unknown> {
  const output = exactTerminalOutput(event);
  return {
    type: 'agent.run',
    framework: 'ai-sdk',
    status: phase === 'end' ? 'succeeded' : phase === 'cancelled' ? 'cancelled' : 'failed',
    ...(phase === 'end' && output !== undefined ? { output } : {}),
    ...(phase === 'error'
      ? { error: semanticError(event.error ?? event, 'generation_error') }
      : {}),
  };
}

function exactTerminalOutput(event: RecordValue): unknown {
  const output = ownDataValue(event, 'output');
  if (output !== undefined) return snapshotNative(output);
  const text = ownDataValue(event, 'text');
  return text === undefined ? undefined : snapshotNative(text);
}

function modelRequestSemantic(
  event: RecordValue,
  contextRefs: string[],
): Record<string, unknown> {
  const tools = Array.isArray(event.tools)
    ? event.tools.flatMap((tool) => (
      isRecord(tool) && stringField(tool, 'name') ? [stringField(tool, 'name')!] : []
    ))
    : [];
  return {
    type: 'model.request',
    framework: 'ai-sdk',
    context_refs: contextRefs,
    ...(modelName(event) ? { model: modelName(event) } : {}),
    ...(tools.length ? { tools } : {}),
  };
}

function recordModelContext(
  sink: SourceSink,
  call: OpenCall,
  event: RecordValue,
): string[] {
  const rows = preparedModelContext(event);
  const sequence = call.modelSequence++;
  const snapshots = rows.map((message) => snapshotNative(message));
  const refs: Array<string | undefined> = [];
  const previous = call.modelContext;
  let unchangedPrefix = previous !== undefined;
  rows.forEach((message, index) => {
    if (
      unchangedPrefix
      && previous
      && previous.refs[index] !== undefined
      && sameFiniteSnapshot(previous.snapshots[index], snapshots[index])
    ) {
      refs[index] = previous.refs[index];
      return;
    }
    unchangedPrefix = false;
    const role = messageRole(message.role);
    if (!role || !Object.prototype.hasOwnProperty.call(message, 'content')) return;
    const receipt = sink.record({
      kind: 'model',
      phase: 'event',
      name: 'ai_sdk.model.context',
      trace: call.identity,
      nativeIdentity: `${call.start.recordId}:context:${sequence}:${index}`,
      native: snapshotRecord({ message: snapshots[index] }),
      parentRecordId: call.start.recordId,
      semantic: {
        type: 'message',
        framework: 'ai-sdk',
        origin: 'context',
        role,
        content: snapshotNative(message.content),
        ...(stringField(message, 'name') ? { name: stringField(message, 'name') } : {}),
      },
    });
    if (receipt.accepted) refs[index] = receipt.recordId;
  });
  call.modelContext = { snapshots, refs };
  return refs.filter((ref): ref is string => ref !== undefined);
}

function sameFiniteSnapshot(left: unknown, right: unknown): boolean {
  try {
    return JSON.stringify(left) === JSON.stringify(right);
  } catch {
    return false;
  }
}

function preparedModelContext(event: RecordValue): RecordValue[] {
  const rows: RecordValue[] = [];
  const instructions = event.instructions;
  if (typeof instructions === 'string') {
    rows.push({ role: 'system', content: instructions });
  } else if (Array.isArray(instructions)) {
    for (const instruction of instructions) {
      if (isRecord(instruction)) rows.push(instruction);
    }
  } else if (isRecord(instructions)) {
    rows.push(instructions);
  }
  if (Array.isArray(event.messages)) {
    for (const message of event.messages) {
      if (isRecord(message)) rows.push(message);
    }
  }
  return rows;
}

function messageRole(
  value: unknown,
): 'system' | 'developer' | 'user' | 'assistant' | 'tool' | undefined {
  return value === 'system'
    || value === 'developer'
    || value === 'user'
    || value === 'assistant'
    || value === 'tool'
    ? value
    : undefined;
}

function modelResponseSemantic(event: RecordValue): Record<string, unknown> {
  const usage = semanticUsage(event.usage);
  const response = aiSDKResponseContent(event.content);
  return {
    type: 'model.response',
    framework: 'ai-sdk',
    status: 'completed',
    ...(modelName(event) ? { model: modelName(event) } : {}),
    ...(response.content !== undefined ? { content: snapshotNative(response.content) } : {}),
    ...(response.reasoning.length ? { reasoning: response.reasoning } : {}),
    ...(stringField(event, 'finishReason')
      ? { finish_reason: stringField(event, 'finishReason') }
      : {}),
    ...(usage ? { usage } : {}),
  };
}

function aiSDKResponseContent(value: unknown): {
  content?: unknown;
  reasoning: Array<{ type: 'text'; text: string }>;
} {
  if (!Array.isArray(value)) {
    return {
      ...(value !== undefined ? { content: value } : {}),
      reasoning: [],
    };
  }
  const content: unknown[] = [];
  const reasoning: Array<{ type: 'text'; text: string }> = [];
  for (const part of value) {
    if (isRecord(part) && part.type === 'reasoning' && typeof part.text === 'string'
      && part.text.length) {
      reasoning.push({ type: 'text', text: part.text });
    } else {
      content.push(part);
    }
  }
  return {
    ...(content.length ? { content } : {}),
    reasoning,
  };
}

function modelName(event: RecordValue): string | undefined {
  const provider = stringField(event, 'provider');
  const modelId = stringField(event, 'modelId');
  if (provider && modelId) return `${provider}/${modelId}`;
  return modelId ?? provider;
}

function semanticUsage(value: unknown): Record<string, number> | undefined {
  if (!isRecord(value)) return undefined;
  const inputTokens = nonnegativeIntegerField(value, 'inputTokens');
  const outputTokens = nonnegativeIntegerField(value, 'outputTokens');
  if (inputTokens === undefined && outputTokens === undefined) return undefined;
  return {
    ...(inputTokens !== undefined ? { input_tokens: inputTokens } : {}),
    ...(outputTokens !== undefined ? { output_tokens: outputTokens } : {}),
  };
}

function toolCallSemantic(
  toolCall: RecordValue,
  toolName: string,
  toolCallId: string,
): Record<string, unknown> {
  return {
    type: 'tool.execution',
    framework: 'ai-sdk',
    call_id: toolCallId,
    name: toolName,
    input: snapshotNative(toolCall.input ?? null),
  };
}

function toolResultSemantic(
  event: RecordValue,
  toolOutput: RecordValue | undefined,
  toolCallId: string,
  failed: boolean,
): Record<string, unknown> {
  const evidence = toolOutput ?? event;
  const error = evidence.error ?? event.error;
  return {
    type: failed ? 'tool.error' : 'tool.result',
    framework: 'ai-sdk',
    call_id: toolCallId,
    status: failed ? 'failed' : 'succeeded',
    ...(failed
      ? { error: semanticError(error, 'tool_error') }
      : { output: snapshotNative(evidence.output ?? null) }),
  };
}

function semanticError(value: unknown, fallbackType: string): Record<string, unknown> {
  const evidence = snapshotNative(value);
  const record = isRecord(evidence) ? evidence : undefined;
  return {
    type: fallbackType,
    message: record && typeof record.message === 'string'
      ? record.message
      : typeof evidence === 'string'
        ? evidence
        : fallbackType.replaceAll('_', ' '),
    recoverable: false,
  };
}

function nonnegativeIntegerField(
  value: RecordValue,
  key: PropertyKey,
): number | undefined {
  const candidate = value[key];
  return typeof candidate === 'number'
    && Number.isSafeInteger(candidate)
    && candidate >= 0
    ? candidate
    : undefined;
}

function stringField(value: RecordValue, key: PropertyKey): string | undefined {
  return typeof value[key] === 'string' ? value[key] : undefined;
}

function nestedRecord(value: RecordValue, key: PropertyKey): RecordValue | undefined {
  return isRecord(value[key]) ? value[key] : undefined;
}

function toolKey(callId: string, toolCallId: string): string {
  return `${callId}:${toolCallId}`;
}

function isRecord(value: unknown): value is RecordValue {
  return (typeof value === 'object' && value !== null) || typeof value === 'function';
}

function isSubject(value: object): value is AISDKSubject {
  const candidate = value as Partial<AISDKSubject>;
  return typeof candidate.registerTelemetry === 'function' && typeof candidate.streamText === 'function';
}
