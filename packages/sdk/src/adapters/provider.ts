import { AsyncLocalStorage } from 'node:async_hooks';
import { createHash } from 'node:crypto';
import type {
  CaptureSource,
  SourceLifecycle,
  SourceSink,
  TraceIdentity,
} from '../v1/types.js';
import { trustOfficialSource } from '../v1/source-ownership.js';
import { snapshotNative } from './native-snapshot.js';

type ProviderName = 'openai' | 'openrouter' | 'anthropic' | 'gemini';
const PROVIDER_REQUEST_COVERAGE = Object.freeze({
  operation: 'model-call', domain: 'provider.request',
});
const OPENAI_RESPONSE_COVERAGE = Object.freeze({
  operation: 'model-response', domain: 'openai.response',
});
const PROVIDER_SEAMS: Record<ProviderName, string> = {
  openai: 'responses.create/chat.completions.create',
  anthropic: 'messages.create/beta.messages.create',
  gemini: 'models.generateContentInternal/generateContentStreamInternal',
  openrouter: 'OpenAI-compatible responses.create/chat.completions.create',
};
const QUALIFIED_PROVIDER_VERSIONS: Readonly<Record<ProviderName, ReadonlySet<string>>> = {
  openai: new Set(['6.46.0', '6.45.0']),
  openrouter: new Set(['6.46.0', '6.45.0']),
  anthropic: new Set(['0.111.0', '0.110.0']),
  gemini: new Set(['2.11.0', '2.10.0']),
};
const PROVIDER_CAPABILITY_PROFILES: Readonly<Record<ProviderName, string>> = {
  openai: 'openai-compatible-responses-chat-v1',
  openrouter: 'openai-compatible-responses-chat-v1',
  anthropic: 'anthropic-messages-v1',
  gemini: 'gemini-generate-content-v1',
};
type ProviderAdapterOptions = {
  version?: string;
  provider?: ProviderName;
};
type MethodTarget = Record<PropertyKey, unknown>;
type Restore = () => void;
const MAX_GEMINI_PENDING_TOOL_TURNS = 1_024;
const MAX_PROVIDER_CONTEXT_TRACES = 1_024;
const MAX_PROVIDER_STREAM_RETAINED_BYTES = 256 * 1024;
const MAX_PROVIDER_STREAM_RETAINED_NODES = 4_096;
const MAX_PROVIDER_STREAM_RETAINED_ITEMS = 4_096;
type GeminiToolSlot = Readonly<{
  tool: MethodTarget;
  candidateIndex: number;
  partIndex: number;
}>;
type GeminiPendingToolCalls = Readonly<{
  callIds: readonly string[];
  hasUnidentified: boolean;
}>;
type GeminiResultCorrelation = Readonly<{
  callIds: ReadonlyMap<MethodTarget, string>;
  unpaired: boolean;
}>;
type GeminiToolCorrelation = Readonly<{
  proposalCallIds(
    trace: TraceIdentity,
    context: ProviderCaptureContext | undefined,
    slots: readonly GeminiToolSlot[],
    observationIndex: number,
  ): readonly (string | undefined)[];
  resultCallIds(
    context: ProviderCaptureContext | undefined,
    request: unknown,
  ): GeminiResultCorrelation;
  clear(): void;
}>;
type ProviderContextHistory = {
  messageCount: number;
  digest: string;
  eligible: boolean;
  requestRef?: string;
};
type ProviderContextPlan = {
  messageCount: number;
  digest?: string;
  contextRefs?: string[];
  contextBaseRef?: string;
  complete: boolean;
};
type ProviderMessageExtraction = Readonly<{
  messages: ProviderMessage[];
  complete: boolean;
  skippedCount: number;
}>;
export type ProviderCaptureContext = Readonly<{
  conversationId?: string;
  turnId?: string;
  turnIndex?: number;
  previousTurnId?: string;
}>;
const providerContext = new AsyncLocalStorage<ProviderCaptureContext>();

/** Attach causal turn identity to provider roots without adding fields to the provider request. */
export function withProviderCaptureContext<T>(
  context: ProviderCaptureContext,
  run: () => T,
): T {
  return providerContext.run(Object.freeze({ ...context }), run);
}

const OPENAI_METHODS = [
  ['responses', 'create'],
  ['chat', 'completions', 'create'],
] as const;
const ANTHROPIC_METHODS = [
  ['messages', 'create'],
  ['beta', 'messages', 'create'],
] as const;
const GEMINI_METHODS = [
  ['models', 'generateContentInternal'],
  ['models', 'generateContentStreamInternal'],
] as const;

/** Explicit-client OpenAI adapter. OpenRouter uses the same owned seam with distinct provenance. */
export function openAIProviderAdapter(options: ProviderAdapterOptions = {}): {
  createSource(client: object): CaptureSource;
} {
  const provider = options.provider ?? 'openai';
  if (provider !== 'openai' && provider !== 'openrouter') {
    throw new TypeError('OpenAI-compatible adapter provider must be openai or openrouter');
  }
  return {
    createSource(client) {
      return providerSource(provider, options.version, client, OPENAI_METHODS);
    },
  };
}

/** Explicit-client Anthropic adapter using GA/beta messages.create and official stream values. */
export function anthropicProviderAdapter(options: ProviderAdapterOptions = {}): {
  createSource(client: object): CaptureSource;
} {
  return {
    createSource(client) {
      return providerSource('anthropic', options.version, client, ANTHROPIC_METHODS);
    },
  };
}

/** Explicit-client Gemini adapter using Models response and official iterator values. */
export function geminiProviderAdapter(options: ProviderAdapterOptions = {}): {
  createSource(client: object): CaptureSource;
} {
  return {
    createSource(client) {
      return providerSource('gemini', options.version, client, GEMINI_METHODS, true);
    },
  };
}

function providerSource(
  provider: ProviderName,
  version: string | undefined,
  client: object,
  paths: readonly (readonly string[])[],
  observeInternalPromise = false,
): CaptureSource {
  return trustOfficialSource({
    metadata: {
      name: `provider:${provider}`,
      seam: PROVIDER_SEAMS[provider],
      identityDomain: 'provider.request',
      ...(version ? { version } : {}),
      qualification: version === undefined
        ? { status: 'unknown' }
        : QUALIFIED_PROVIDER_VERSIONS[provider].has(version)
          ? { status: 'exact_qualified' }
          : {
            status: 'capability_checked_unqualified',
            profile: PROVIDER_CAPABILITY_PROFILES[provider],
          },
      official: true,
      coverage: provider === 'openai' || provider === 'openrouter'
        ? [
          { ...PROVIDER_REQUEST_COVERAGE, role: 'owner' },
          { ...OPENAI_RESPONSE_COVERAGE, role: 'owner' },
        ]
        : [{ ...PROVIDER_REQUEST_COVERAGE, role: 'owner' }],
    },
    install(sink) {
      const restores: Restore[] = [];
      const contextHistories = new Map<string, ProviderContextHistory>();
      const geminiToolCorrelation = provider === 'gemini' ? createGeminiToolCorrelation() : undefined;
      try {
        for (const path of paths) {
          const restore = patchMethod(
            client, path, provider, sink, observeInternalPromise, contextHistories,
            geminiToolCorrelation,
          );
          if (!restore) {
            throw new TypeError(`frozen ${provider} seam is missing: ${path.join('.')}`);
          }
          restores.push(restore);
        }
      } catch (error) {
        for (const installed of restores.reverse()) installed();
        throw error;
      }
      let active = true;
      return {
        deactivate() {
          if (!active) return;
          active = false;
          contextHistories.clear();
          geminiToolCorrelation?.clear();
          for (const restore of restores.reverse()) restore();
        },
        drain() {},
      } satisfies SourceLifecycle;
    },
  }, 'provider');
}

function patchMethod(
  client: object,
  path: readonly string[],
  provider: ProviderName,
  sink: SourceSink,
  observeInternalPromise: boolean,
  contextHistories: Map<string, ProviderContextHistory>,
  geminiToolCorrelation?: GeminiToolCorrelation,
): Restore | undefined {
  const resolved = resolveMethod(client, path);
  if (!resolved) return undefined;
  const { target, key, original } = resolved;
  const operation = path.slice(0, -1).join('.');
  const wrapper = function providerCapture(this: unknown, ...args: unknown[]): unknown {
    const request = args[0] ?? null;
    const context = providerContext.getStore();
    const opened = sink.openTrace({
      name: `${provider}.${operation}`,
      coverage: PROVIDER_REQUEST_COVERAGE,
      nativeIdentity: nativeId(request),
      ...context,
      native: { provider, operation },
      semantic: {
        type: 'agent.run',
        name: `${provider}.${operation}`,
        input: { provider, operation },
      },
    });
    if (!opened.accepted) return original.apply(this, args);
    const trace = opened.identity;
    const geminiResultCorrelation = provider === 'gemini'
      ? geminiToolCorrelation?.resultCallIds(context, request)
      : undefined;
    if (geminiResultCorrelation?.unpaired) {
      sink.record({
        kind: 'correlation',
        phase: 'gap',
        name: 'gemini.function_responses.unpaired',
        trace,
        native: { provider, operation },
        semantic: {
          type: 'capture.gap',
          provider,
          reason: 'source_rejection',
          detail: 'gemini.function_responses.unpaired',
        },
      });
    }
    const geminiResultCallIds = geminiResultCorrelation?.callIds;
    const orderedGeminiResultCallIds = geminiResultCallIds?.size
      ? [...geminiResultCallIds.values()]
      : undefined;
    // Planning, admissions, and history commit are synchronous on one JS turn.
    // Concurrent calls therefore cannot observe a half-committed context plan.
    const history = providerContextHistory(contextHistories, trace.traceId);
    const contextPlan = recordRequestMessages(
      provider,
      operation,
      request,
      sink,
      trace,
      history,
    );
    const requestReceipt = sink.record({
      kind: 'model', phase: 'event', name: `${provider}.request`, trace,
      native: {
        provider,
        operation,
        request: providerRequestNative(provider, request, contextPlan.messageCount),
      },
      semantic: {
        type: 'model.request', provider,
        ...modelRequestFields(
          provider,
          request,
          contextPlan.contextRefs,
          contextPlan.contextBaseRef,
        ),
        ...(orderedGeminiResultCallIds ? { call_ids: orderedGeminiResultCallIds } : {}),
      },
    });
    if (requestReceipt.accepted && contextPlan.complete && contextPlan.digest) {
      history.messageCount = contextPlan.messageCount;
      history.digest = contextPlan.digest;
      history.eligible = true;
      history.requestRef = requestReceipt.recordId;
    }
    const requestRecordId = requestReceipt.accepted ? requestReceipt.recordId : undefined;
    let result: unknown;
    try {
      result = original.apply(this, args);
    } catch (error) {
      finishError(provider, operation, sink, trace, error, requestRecordId);
      throw error;
    }
    if (!isThenable(result)) {
      settleResult(
        provider, operation, sink, trace, result, requestRecordId, geminiToolCorrelation, context,
      );
      return result;
    }
    if (observeInternalPromise) {
      return observeAdapterInternalPromise(
        result, provider, operation, sink, trace, requestRecordId, geminiToolCorrelation, context,
      );
    }
    if (isLazyProviderPromise(result) && instrumentLazyProviderPromise(
      result, provider, operation, sink, trace, requestRecordId,
    )) {
      return result;
    }
    promiseObservationUnavailable(provider, operation, sink, trace);
    return result;
  };
  const restore = replaceMethod(target, key, wrapper, resolved.ownDescriptor, resolved.descriptor);
  if (!restore) return undefined;
  return () => {
    if (target[key] === wrapper) restore();
  };
}

function observeAdapterInternalPromise(
  result: PromiseLike<unknown>,
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
  requestRecordId?: string,
  geminiToolCorrelation?: GeminiToolCorrelation,
  context?: ProviderCaptureContext,
): Promise<unknown> {
  return Promise.resolve(result).then(
    (value) => {
      settleResult(
        provider, operation, sink, trace, value, requestRecordId, geminiToolCorrelation, context,
      );
      return value;
    },
    (error: unknown) => {
      finishError(provider, operation, sink, trace, error, requestRecordId);
      throw error;
    },
  );
}

function promiseObservationUnavailable(
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
): void {
  sink.record({
    kind: 'stream', phase: 'gap', name: `${provider}.promise.unobservable`, trace,
    native: { provider, operation, reason: 'provider_promise_unobservable' },
    semantic: {
      type: 'capture.gap',
      provider,
      reason: 'unsupported_native_value',
      detail: 'provider_promise_unobservable',
    },
  });
  sink.record({
    kind: 'lifecycle', phase: 'end', name: `${provider}.${operation}`, trace,
    native: { provider, operation, promise_observation: 'unavailable' },
    semantic: { type: 'agent.run', status: 'unknown' },
  });
}

function isLazyProviderPromise(result: PromiseLike<unknown>): boolean {
  const candidate = result as unknown as MethodTarget;
  return typeof candidate.then === 'function'
    && typeof candidate.catch === 'function'
    && typeof candidate.finally === 'function'
    && typeof candidate.parse === 'function'
    && typeof candidate.asResponse === 'function'
    && typeof candidate.withResponse === 'function'
    && typeof candidate._thenUnwrap === 'function';
}

function instrumentLazyProviderPromise(
  result: PromiseLike<unknown>,
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
  requestRecordId?: string,
): boolean {
  const candidate = result as unknown as MethodTarget;
  const originalThen = candidate.then;
  const originalCatch = candidate.catch;
  const originalFinally = candidate.finally;
  const originalWithResponse = candidate.withResponse;
  if (typeof originalThen !== 'function') return false;
  const descriptors = new Map<PropertyKey, PropertyDescriptor | undefined>();
  for (const key of ['then', 'catch', 'finally', 'withResponse'] as const) {
    descriptors.set(key, Object.getOwnPropertyDescriptor(candidate, key));
  }
  const restoreDescriptors = (): void => {
    for (const [key, descriptor] of descriptors) {
      if (descriptor) Object.defineProperty(candidate, key, descriptor);
      else if (Object.prototype.hasOwnProperty.call(candidate, key)) delete candidate[key];
    }
  };
  let completed = false;
  const success = (value: unknown): unknown => {
    if (!completed) {
      completed = true;
      restoreDescriptors();
      settleResult(provider, operation, sink, trace, value, requestRecordId);
    }
    return value;
  };
  const failure = (error: unknown): never => {
    if (!completed) {
      completed = true;
      restoreDescriptors();
      finishError(provider, operation, sink, trace, error, requestRecordId);
    }
    throw error;
  };
  try {
    Object.defineProperty(candidate, 'then', {
      configurable: true,
      value(this: unknown, onfulfilled?: (value: unknown) => unknown, onrejected?: (error: unknown) => unknown) {
        return Reflect.apply(originalThen, this, [
          (value: unknown) => onfulfilled ? onfulfilled(success(value)) : success(value),
          (error: unknown) => {
            try { failure(error); } catch (observed) {
              if (onrejected) return onrejected(observed);
              throw observed;
            }
          },
        ]);
      },
    });
    if (typeof originalCatch === 'function') Object.defineProperty(candidate, 'catch', {
      configurable: true,
      value(this: unknown, onrejected?: (error: unknown) => unknown) {
        return Reflect.apply(originalThen, this, [
          success,
          (error: unknown) => {
            try { failure(error); } catch (observed) {
              if (onrejected) return onrejected(observed);
              throw observed;
            }
          },
        ]);
      },
    });
    if (typeof originalFinally === 'function') Object.defineProperty(candidate, 'finally', {
      configurable: true,
      value(this: unknown, onfinally?: () => unknown) {
        const observed = Reflect.apply(originalThen, this, [success, failure]);
        return Reflect.apply(Promise.prototype.finally, observed, [onfinally]);
      },
    });
    if (typeof originalWithResponse === 'function') Object.defineProperty(candidate, 'withResponse', {
      configurable: true,
      value(this: unknown, ...args: unknown[]) {
        const response = Reflect.apply(originalWithResponse, this, args) as PromiseLike<unknown>;
        return Promise.resolve(response).then(
          (value) => {
            if (isObject(value) && 'data' in value) success(value.data);
            return value;
          },
          failure,
        );
      },
    });
    return true;
  } catch {
    restoreDescriptors();
    return false;
  }
}

function settleResult(
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
  value: unknown,
  requestRecordId?: string,
  geminiToolCorrelation?: GeminiToolCorrelation,
  context?: ProviderCaptureContext,
): void {
  if (instrumentStream(
    value, provider, operation, sink, trace, requestRecordId, geminiToolCorrelation, context,
  )) return;
  const responseId = exactOpenAIResponseId(provider, operation, value);
  const responseReceipt = sink.record({
    kind: 'model', phase: 'event', name: `${provider}.response`, trace,
    ...(responseId ? { nativeIdentity: responseId, coverage: OPENAI_RESPONSE_COVERAGE } : {}),
    ...(requestRecordId ? { parentRecordId: requestRecordId } : {}),
    native: { provider, operation, response: providerResponseNative(provider, value) },
    semantic: { type: 'model.response', provider, ...modelResponseFields(provider, value) },
  });
  if (responseReceipt.accepted && providerReasoningUnavailable(provider, value)) {
    recordReasoningUnavailable(provider, sink, trace, responseReceipt.recordId);
  }
  recordUsageAndTools(provider, sink, trace, value, geminiToolCorrelation, context);
  recordProviderState(provider, operation, sink, trace, 'completed');
  sink.record({
    kind: 'lifecycle', phase: 'end', name: `${provider}.${operation}`, trace,
    native: { provider, operation },
    semantic: { type: 'agent.run', status: 'succeeded' },
  });
}

function instrumentStream(
  value: unknown,
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
  requestRecordId?: string,
  geminiToolCorrelation?: GeminiToolCorrelation,
  context?: ProviderCaptureContext,
): boolean {
  if (!isObject(value)) return false;
  const candidate = value as MethodTarget & { iterator?: unknown };
  let completed = false;
  let lastPart: unknown;
  let responseId: string | undefined;
  let streamPartIndex = 0;
  const seenGeminiNativeToolIds = new Set<string>();
  const aggregate = createStreamAggregate(provider, () => {
    sink.record({
      kind: 'unknown',
      phase: 'gap',
      name: `${provider}.stream.retention_truncated`,
      trace,
      ...(requestRecordId ? { parentRecordId: requestRecordId } : {}),
      native: {
        provider,
        operation,
        byte_limit: MAX_PROVIDER_STREAM_RETAINED_BYTES,
        node_limit: MAX_PROVIDER_STREAM_RETAINED_NODES,
        item_limit: MAX_PROVIDER_STREAM_RETAINED_ITEMS,
      },
      semantic: {
        type: 'capture.gap',
        provider,
        reason: 'serialization_failure',
        detail: `${provider}.stream.retention_truncated`,
      },
    });
  });
  const finish = (phase: 'end' | 'cancelled' | 'error', native: unknown): void => {
    if (completed) return;
    completed = true;
    const terminal = phase === 'end' ? { provider, operation, response: lastPart } : native;
    sink.record({
      kind: 'stream', phase, name: `${provider}.stream.${phase === 'end' ? 'terminal' : phase}`,
      trace, native: terminal,
      semantic: { type: 'capture.redundant', provider },
    });
    if (phase === 'end' || phase === 'cancelled') {
      const semanticTerminal = aggregate.terminal(lastPart);
      const responseFields = modelResponseFields(provider, semanticTerminal);
      const responseReceipt = sink.record({
        kind: 'model', phase: 'event', name: `${provider}.response`, trace,
        ...(responseId ? { nativeIdentity: responseId, coverage: OPENAI_RESPONSE_COVERAGE } : {}),
        ...(requestRecordId ? { parentRecordId: requestRecordId } : {}),
        native: terminal,
        semantic: {
          type: 'model.response',
          provider,
          ...responseFields,
          ...(phase === 'cancelled' ? { status: 'cancelled' } : {}),
        },
      });
      if (responseReceipt.accepted && (aggregate.reasoningUnavailable()
        || providerReasoningUnavailable(provider, semanticTerminal))) {
        recordReasoningUnavailable(provider, sink, trace, responseReceipt.recordId);
      }
      if (phase === 'end' && provider !== 'gemini') {
        recordUsageAndTools(
          provider, sink, trace, semanticTerminal, geminiToolCorrelation, context, streamPartIndex,
        );
      }
    } else if (phase === 'error') {
      const streamError = isObject(native) && 'error' in native ? native.error : native;
      recordModelError(provider, operation, sink, trace, streamError, requestRecordId);
    }
    recordProviderState(provider, operation, sink, trace, phase === 'end' ? 'completed' : phase);
    sink.record({
      kind: 'lifecycle', phase, name: `${provider}.${operation}`, trace, native: terminal,
      semantic: {
        type: 'agent.run',
        status: phase === 'end' ? 'succeeded' : phase === 'cancelled' ? 'cancelled' : 'failed',
      },
    });
  };
  const observePart = (part: unknown): void => {
    lastPart = providerResponseNative(provider, part);
    aggregate.observe(part);
    if (provider === 'gemini') {
      recordUsageAndTools(
        provider,
        sink,
        trace,
        part,
        geminiToolCorrelation,
        context,
        streamPartIndex,
        seenGeminiNativeToolIds,
      );
    }
    responseId = exactOpenAIStreamResponseId(provider, operation, part) ?? responseId;
    sink.record({
      kind: 'stream', phase: 'event', name: `${provider}.stream.delta`, trace,
      native: {
        provider,
        operation,
        delta: providerResponseNative(provider, part),
        part: providerResponseNative(provider, part),
      },
      semantic: { type: 'capture.redundant', provider },
    });
    streamPartIndex += 1;
  };
  if (isIterator(candidate.iterator)) {
    try {
      candidate.iterator = observedIterator(candidate.iterator, observePart, finish);
    } catch {
      streamUnavailable(provider, operation, sink, trace);
    }
    return true;
  }
  const iteratorFactory = candidate[Symbol.asyncIterator];
  if (typeof iteratorFactory !== 'function') return false;
  const wrappedFactory = function observedAsyncIterator(this: unknown): AsyncIterator<unknown> {
    return observedIterator(Reflect.apply(iteratorFactory, this, []), observePart, finish);
  };
  try {
    candidate[Symbol.asyncIterator] = wrappedFactory;
    return true;
  } catch {
    streamUnavailable(provider, operation, sink, trace);
    return true;
  }
}

function recordProviderState(
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
  state: 'completed' | 'cancelled' | 'error',
): void {
  sink.record({
    kind: 'state', phase: 'event', name: `${provider}.operation.state`, trace,
    native: { provider, operation, state },
    semantic: { type: 'capture.redundant', provider },
  });
}

function exactOpenAIResponseId(
  provider: ProviderName,
  operation: string,
  value: unknown,
): string | undefined {
  if ((provider !== 'openai' && provider !== 'openrouter') || operation !== 'responses' || !isObject(value)) {
    return undefined;
  }
  if (value.object !== 'response') return undefined;
  return typeof value.id === 'string' && value.id.trim().length > 0 ? value.id : undefined;
}

function exactOpenAIStreamResponseId(
  provider: ProviderName,
  operation: string,
  part: unknown,
): string | undefined {
  if ((provider !== 'openai' && provider !== 'openrouter') || operation !== 'responses' || !isObject(part)) {
    return undefined;
  }
  if (part.type !== 'response.completed'
    && part.type !== 'response.failed'
    && part.type !== 'response.incomplete') return undefined;
  const response = part.response;
  if (!isObject(response)) return undefined;
  if (response.object !== 'response') return undefined;
  return typeof response.id === 'string' && response.id.trim().length > 0 ? response.id : undefined;
}

function streamUnavailable(
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
): void {
  sink.record({
    kind: 'stream', phase: 'gap', name: `${provider}.stream.unobservable`, trace,
    native: { provider, operation, reason: 'non_extensible_stream_surface' },
    semantic: { type: 'stream.gap', provider },
  });
  sink.record({
    kind: 'lifecycle', phase: 'end', name: `${provider}.${operation}`, trace,
    native: { provider, operation, stream_observation: 'unavailable' },
    semantic: { type: 'agent.run', status: 'unknown' },
  });
}

function observedIterator(
  iterator: AsyncIterator<unknown>,
  observePart: (part: unknown) => void,
  finish: (phase: 'end' | 'cancelled' | 'error', native: unknown) => void,
): AsyncIterator<unknown> {
  return new Proxy(iterator, {
    get(target, property, receiver) {
      if (property === 'next') return (...args: [] | [unknown]) => Promise.resolve(target.next(...args)).then(
        (result) => {
          if (result.done) finish('end', { consumed: true });
          else observePart(result.value);
          return result;
        },
        (error: unknown) => {
          finish('error', { error });
          throw error;
        },
      );
      if (property === 'return' && typeof target.return === 'function') {
        return (value?: unknown) => Promise.resolve(target.return!(value)).then((result) => {
          finish('cancelled', { consumer_cancelled: true });
          return result;
        });
      }
      if (property === 'throw' && typeof target.throw === 'function') {
        return (error?: unknown) => Promise.resolve(target.throw!(error)).then(
          (result) => result,
          (thrown: unknown) => {
            finish('error', { error: thrown });
            throw thrown;
          },
        );
      }
      if (property === Symbol.asyncIterator) return () => receiver as AsyncIterator<unknown>;
      const item = Reflect.get(target as object, property, target);
      return typeof item === 'function' ? item.bind(target) : item;
    },
  });
}

function recordUsageAndTools(
  provider: ProviderName,
  sink: SourceSink,
  trace: TraceIdentity,
  value: unknown,
  geminiToolCorrelation?: GeminiToolCorrelation,
  context?: ProviderCaptureContext,
  observationIndex = 0,
  seenGeminiNativeToolIds?: Set<string>,
): void {
  if (!isObject(value)) return;
  const observed = isObject(value.response)
    && /response\.(completed|failed|incomplete)/.test(String(value.type))
    ? value.response
    : value;
  const usage = observed.usage ?? observed.usageMetadata;
  if (usage !== undefined) sink.record({
    kind: 'model', phase: 'event', name: `${provider}.usage`, trace,
    native: { provider, usage: snapshotNative(usage) },
    semantic: { type: 'capture.redundant', provider },
  });
  const geminiSlots = provider === 'gemini'
    ? geminiToolSlots(observed).filter((slot) => {
      if (!exactIdentity(slot.tool.name) || slot.tool.args === undefined) return false;
      const nativeCallId = exactIdentity(slot.tool.id);
      return nativeCallId === undefined || !seenGeminiNativeToolIds?.has(nativeCallId);
    })
    : [];
  const tools = provider === 'gemini' ? geminiSlots.map((slot) => slot.tool) : toolCalls(observed);
  const geminiCallIds = geminiToolCorrelation?.proposalCallIds(
    trace, context, geminiSlots, observationIndex,
  ) ?? [];
  for (const [index, tool] of tools.entries()) {
    const proposal = toolProposalFields(provider, tool, geminiCallIds[index]);
    if (!proposal) continue;
    if (typeof proposal.native_call_id === 'string') {
      seenGeminiNativeToolIds?.add(proposal.native_call_id);
    }
    sink.record({
      kind: 'tool', phase: 'event', name: `${provider}.tool.proposed`, trace,
      native: { provider, tool: snapshotNative(tool) },
      semantic: { type: 'tool.proposal', provider, ...proposal },
    });
  }
}

type StreamTool = {
  id?: string;
  name?: string;
  input?: unknown;
  fragments: string[];
};

type StreamAggregate = Readonly<{
  observe(value: unknown): void;
  terminal(lastPart: unknown): unknown;
  reasoningUnavailable(): boolean;
}>;
type ExposedReasoningBlock = Readonly<{
  type: 'text' | 'summary';
  text: string;
}>;
type OrderedReasoning = Readonly<{
  append(type: ExposedReasoningBlock['type'], text: unknown, nativeBlockId?: string): void;
  separate(): void;
  blocks(): ExposedReasoningBlock[];
}>;

function createStreamAggregate(
  provider: ProviderName,
  onRetentionTruncated: () => void,
): StreamAggregate {
  const text: string[] = [];
  const tools = new Map<string, StreamTool>();
  const retention = createStreamRetentionBudget(onRetentionTruncated);
  const reasoning = createOrderedReasoning(retention);
  let model: string | undefined;
  let finishReason: string | undefined;
  let usage: MethodTarget | undefined;
  let unavailableReasoning = false;
  const append = (target: string[], value: unknown): void => {
    if (typeof value === 'string' && retention.retain(value)) target.push(value);
  };
  const retainIdentity = (value: unknown, current: string | undefined): string | undefined => {
    const candidate = exactIdentity(value);
    if (!candidate || candidate === current) return current;
    return retention.retain(candidate) ? candidate : current;
  };
  const retainUsage = (value: unknown): void => {
    if (!isObject(value)) return;
    const copied = snapshotNative(value);
    if (!isObject(copied) || !retention.retain(copied)) return;
    usage = { ...(usage ?? {}), ...copied };
  };
  const observeTool = (
    key: string,
    id: unknown,
    name: unknown,
    input: unknown,
    fragment = false,
  ): void => {
    let current = tools.get(key);
    if (!current) {
      if (!retention.retain(key)) return;
      current = { fragments: [] };
      tools.set(key, current);
    }
    current.id = retainIdentity(id, current.id);
    current.name = retainIdentity(name, current.name);
    if (fragment) append(current.fragments, input);
    else if (input !== undefined) {
      const copied = snapshotNative(input);
      if (retention.retain(copied)) current.input = copied;
    }
  };
  return {
    observe(value) {
      if (retention.exhausted()) return;
      if (!isObject(value)) return;
      if (isObject(value.response)
        && /response\.(completed|failed|incomplete)/.test(String(value.type))) return;
      model = retainIdentity(value.model, model);
      model = retainIdentity(value.modelVersion, model);
      const directUsage = value.usage ?? value.usageMetadata;
      retainUsage(directUsage);
      if (provider === 'openai' || provider === 'openrouter') {
        if (isObject(value.item) && openAIReasoningItemUnavailable(value.item)) {
          unavailableReasoning = true;
          reasoning.separate();
        }
        if (value.type === 'response.reasoning_summary_text.delta') {
          reasoning.append('summary', value.delta, openAIResponsesReasoningBlockId(value));
          return;
        }
        if (value.type === 'response.reasoning_text.delta') {
          reasoning.append('text', value.delta, openAIResponsesReasoningBlockId(value));
          return;
        }
        if (!Array.isArray(value.choices)) return;
        for (const choice of value.choices) {
          if (!isObject(choice)) continue;
          finishReason = retainIdentity(choice.finish_reason, finishReason);
          const delta = isObject(choice.delta) ? choice.delta : undefined;
          if (!delta) continue;
          append(text, delta.content);
          let detailedReasoning = false;
          if (Array.isArray(delta.reasoning_details)) {
            for (const detail of delta.reasoning_details) {
              if (!isObject(detail)) continue;
              const block = openRouterReasoningDetail(detail);
              if (!block) {
                if (typeof detail.type === 'string' && detail.type.startsWith('reasoning.')) {
                  reasoning.separate();
                  if (detail.type === 'reasoning.encrypted') unavailableReasoning = true;
                }
                continue;
              }
              detailedReasoning = true;
              reasoning.append(
                block.type,
                block.text,
                openRouterReasoningDetailId(choice.index, detail),
              );
            }
          }
          if (!detailedReasoning) {
            reasoning.append(
              'text',
              delta.reasoning_content ?? delta.reasoning,
              `choice:${String(choice.index ?? 0)}:reasoning`,
            );
          }
          if (!Array.isArray(delta.tool_calls)) continue;
          for (const [position, tool] of delta.tool_calls.entries()) {
            if (!isObject(tool)) continue;
            const index = typeof tool.index === 'number' ? tool.index : position;
            const fn = isObject(tool.function) ? tool.function : undefined;
            observeTool(
              `choice:${String(choice.index ?? 0)}:tool:${index}`,
              tool.id,
              fn?.name,
              fn?.arguments,
              true,
            );
          }
        }
        return;
      }
      if (provider === 'anthropic') {
        if (value.type === 'message_start' && isObject(value.message)) {
          model = retainIdentity(value.message.model, model);
          retainUsage(value.message.usage);
        }
        if (value.type === 'content_block_start' && isObject(value.content_block)) {
          const block = value.content_block;
          if (block.type === 'text') append(text, block.text);
          if (block.type === 'thinking' && typeof block.thinking === 'string') {
            reasoning.append('summary', block.thinking, anthropicReasoningBlockId(value.index));
          } else if (block.type === 'redacted_thinking') {
            reasoning.separate();
            unavailableReasoning = true;
          }
          if (block.type === 'tool_use') observeTool(
            `content:${String(value.index ?? tools.size)}`,
            block.id,
            block.name,
            block.input,
          );
        }
        if (value.type === 'content_block_delta' && isObject(value.delta)) {
          append(text, value.delta.text);
          if (value.delta.type === 'thinking_delta') {
            reasoning.append(
              'summary',
              value.delta.thinking,
              anthropicReasoningBlockId(value.index),
            );
          }
          if (typeof value.delta.partial_json === 'string') observeTool(
            `content:${String(value.index ?? tools.size)}`,
            undefined,
            undefined,
            value.delta.partial_json,
            true,
          );
        }
        if (value.type === 'message_delta' && isObject(value.delta)) {
          finishReason = retainIdentity(value.delta.stop_reason, finishReason);
        }
        return;
      }
      if (!Array.isArray(value.candidates)) return;
      for (const [candidateIndex, candidate] of value.candidates.entries()) {
        if (!isObject(candidate) || !isObject(candidate.content)
          || !Array.isArray(candidate.content.parts)) continue;
        finishReason = retainIdentity(candidate.finishReason, finishReason);
        for (const [partIndex, part] of candidate.content.parts.entries()) {
          if (!isObject(part)) continue;
          if (typeof part.text === 'string') {
            if (part.thought === true) reasoning.append('summary', part.text);
            else append(text, part.text);
          }
          if (isObject(part.functionCall)) observeTool(
            `candidate:${candidateIndex}:part:${partIndex}`,
            part.functionCall.id,
            part.functionCall.name,
            part.functionCall.args,
          );
        }
      }
    },
    terminal(lastPart) {
      if (isObject(lastPart) && isObject(lastPart.response)
        && /response\.(completed|failed|incomplete)/.test(String(lastPart.type))) {
        return lastPart;
      }
      const normalizedTools = [...tools.values()].flatMap((tool) => {
        if (!tool.name) return [];
        // Streaming protocols commonly announce an empty input container and
        // deliver the authoritative value in later JSON fragments.
        const input = tool.fragments.length
          ? tool.fragments.join('')
          : tool.input;
        return input === undefined ? [] : [{
          ...(tool.id ? { id: tool.id } : {}),
          name: tool.name,
          arguments: input,
        }];
      });
      if (provider === 'anthropic') return {
        ...(model ? { model } : {}),
        ...(reasoning.blocks().length ? { reasoning: reasoning.blocks() } : {}),
        content: [
          ...(text.length ? [{ type: 'text', text: text.join('') }] : []),
          ...normalizedTools.map((tool) => ({
            type: 'tool_use',
            ...(tool.id ? { id: tool.id } : {}),
            name: tool.name,
            input: parseExactJson(tool.arguments),
          })),
        ],
        ...(finishReason ? { stop_reason: finishReason } : {}),
        ...(usage ? { usage } : {}),
      };
      if (provider === 'gemini') return {
        ...(model ? { modelVersion: model } : {}),
        ...(reasoning.blocks().length ? { reasoning: reasoning.blocks() } : {}),
        candidates: [{
          content: {
            role: 'model',
            parts: [
              ...(text.length ? [{ text: text.join('') }] : []),
              ...normalizedTools.map((tool) => ({
                functionCall: { name: tool.name, args: parseExactJson(tool.arguments) },
              })),
            ],
          },
          ...(finishReason ? { finishReason } : {}),
        }],
        ...(usage ? { usageMetadata: usage } : {}),
      };
      return {
        ...(model ? { model } : {}),
        ...(reasoning.blocks().length ? { reasoning: reasoning.blocks() } : {}),
        choices: [{
          index: 0,
          message: {
            role: 'assistant',
            ...(text.length ? { content: text.join('') } : {}),
            ...(normalizedTools.length ? {
              tool_calls: normalizedTools.map((tool) => ({
                ...(tool.id ? { id: tool.id } : {}),
                type: 'function',
                function: { name: tool.name, arguments: tool.arguments },
              })),
            } : {}),
          },
          ...(finishReason ? { finish_reason: finishReason } : {}),
        }],
        ...(usage ? { usage } : {}),
      };
    },
    reasoningUnavailable() {
      return unavailableReasoning;
    },
  };
}

function createOrderedReasoning(retention: StreamRetentionBudget): OrderedReasoning {
  const retained: Array<{
    type: ExposedReasoningBlock['type'];
    text: string;
    nativeBlockId?: string;
  }> = [];
  let separated = false;
  return {
    append(type, text, nativeBlockId) {
      if (typeof text !== 'string' || text.length === 0 || !retention.retain(text)) return;
      const previous = retained.at(-1);
      if (nativeBlockId !== undefined
        && !separated
        && previous?.nativeBlockId === nativeBlockId
        && previous.type === type) {
        previous.text += text;
        return;
      }
      retained.push({ type, text, ...(nativeBlockId ? { nativeBlockId } : {}) });
      separated = false;
    },
    separate() {
      separated = true;
    },
    blocks() {
      return retained.map(({ type, text }) => ({ type, text }));
    },
  };
}

function openAIResponsesReasoningBlockId(value: MethodTarget): string | undefined {
  const itemId = exactIdentity(value.item_id);
  const outputIndex = nonnegativeInteger(value.output_index);
  const summaryIndex = nonnegativeInteger(value.summary_index);
  const contentIndex = nonnegativeInteger(value.content_index);
  const item = itemId ? `item:${itemId}` : outputIndex !== undefined ? `output:${outputIndex}` : undefined;
  if (!item) return undefined;
  const child = summaryIndex !== undefined
    ? `summary:${summaryIndex}`
    : contentIndex !== undefined ? `content:${contentIndex}` : undefined;
  return child ? `${item}:${child}` : item;
}

function anthropicReasoningBlockId(value: unknown): string | undefined {
  const index = nonnegativeInteger(value);
  return index === undefined ? undefined : `content:${index}`;
}

function openRouterReasoningDetailId(
  choiceIndex: unknown,
  detail: MethodTarget,
): string | undefined {
  const choice = nonnegativeInteger(choiceIndex) ?? 0;
  const id = exactIdentity(detail.id);
  if (id) return `choice:${choice}:reasoning-detail:id:${id}`;
  const index = nonnegativeInteger(detail.index);
  return index === undefined ? undefined : `choice:${choice}:reasoning-detail:index:${index}`;
}

type StreamRetentionCost = { bytes: number; nodes: number; items: number };
type StreamRetentionBudget = Readonly<{
  retain(value: unknown): boolean;
  exhausted(): boolean;
}>;

function createStreamRetentionBudget(onTruncated: () => void): StreamRetentionBudget {
  let retainedBytes = 0;
  let retainedNodes = 0;
  let retainedItems = 0;
  let truncated = false;
  const exhaust = (): false => {
    if (!truncated) {
      truncated = true;
      try { onTruncated(); } catch { /* capture must not change provider stream behavior */ }
    }
    return false;
  };
  return {
    retain(value) {
      if (truncated) return false;
      const cost = retainedStreamValueCost(
        value,
        MAX_PROVIDER_STREAM_RETAINED_BYTES - retainedBytes,
        MAX_PROVIDER_STREAM_RETAINED_NODES - retainedNodes,
        MAX_PROVIDER_STREAM_RETAINED_ITEMS - retainedItems,
      );
      if (!cost) return exhaust();
      retainedBytes += cost.bytes;
      retainedNodes += cost.nodes;
      retainedItems += cost.items;
      return true;
    },
    exhausted: () => truncated,
  };
}

function retainedStreamValueCost(
  value: unknown,
  byteLimit: number,
  nodeLimit: number,
  itemLimit: number,
): StreamRetentionCost | undefined {
  if (byteLimit < 0 || nodeLimit <= 0 || itemLimit <= 0) return undefined;
  const stack: unknown[] = [value];
  const seen = new WeakSet<object>();
  let bytes = 0;
  let nodes = 0;
  let items = 0;
  const addBytes = (amount: number): boolean => {
    if (!Number.isSafeInteger(amount) || amount < 0 || amount > byteLimit - bytes) return false;
    bytes += amount;
    return true;
  };
  try {
    while (stack.length) {
      const current = stack.pop();
      nodes += 1;
      items += 1;
      if (nodes > nodeLimit || items > itemLimit) return undefined;
      if (current === null || typeof current === 'boolean'
        || typeof current === 'number' || typeof current === 'bigint') {
        if (!addBytes(16)) return undefined;
        continue;
      }
      if (typeof current === 'string') {
        if (!addBytes(current.length * 4)) return undefined;
        continue;
      }
      if (current === undefined || typeof current === 'function' || typeof current === 'symbol') {
        if (!addBytes(16)) return undefined;
        continue;
      }
      if (typeof current !== 'object') {
        if (!addBytes(16)) return undefined;
        continue;
      }
      if (seen.has(current)) {
        if (!addBytes(8)) return undefined;
        continue;
      }
      seen.add(current);
      if (current instanceof Uint8Array) {
        if (!addBytes(current.byteLength)) return undefined;
        continue;
      }
      if (Array.isArray(current)) {
        for (let index = 0; index < current.length; index += 1) {
          const descriptor = Object.getOwnPropertyDescriptor(current, String(index));
          if (descriptor && 'value' in descriptor) stack.push(descriptor.value);
        }
        continue;
      }
      for (const key in current) {
        const descriptor = Object.getOwnPropertyDescriptor(current, key);
        if (!descriptor || !('value' in descriptor)) continue;
        if (!addBytes(key.length * 4)) return undefined;
        stack.push(descriptor.value);
      }
    }
  } catch {
    return undefined;
  }
  return { bytes, nodes, items };
}

type ProviderMessage = Readonly<{
  role: 'system' | 'developer' | 'user' | 'assistant' | 'tool';
  content: unknown;
  native: unknown;
  name?: string;
  callId?: string;
}>;

function recordRequestMessages(
  provider: ProviderName,
  operation: string,
  request: unknown,
  sink: SourceSink,
  trace: TraceIdentity,
  history: ProviderContextHistory,
): ProviderContextPlan {
  const fullDigest = createHash('sha256');
  const prefixDigest = createHash('sha256');
  const extraction = requestMessages(provider, operation, request);
  const messages = extraction.messages.map((message, index) => {
    const native = snapshotNative(message.native);
    const prepared = {
      ...message,
      content: canonicalProviderMessageContent(provider, message.content),
      native,
      exact: exactProviderSnapshot(native),
    };
    const key = canonicalProviderMessageKey(prepared);
    updateProviderContextDigest(fullDigest, key);
    if (index < history.messageCount) updateProviderContextDigest(prefixDigest, key);
    return prepared;
  });
  const inexactCount = messages.filter((message) => !message.exact).length;
  const exactContext = extraction.complete && inexactCount === 0;
  const digest = exactContext ? fullDigest.digest('hex') : undefined;
  const matchesPriorContext = exactContext
    && history.eligible
    && history.requestRef !== undefined
    && messages.length >= history.messageCount
    && prefixDigest.digest('hex') === history.digest;
  const refs: string[] = [];
  let complete = exactContext;
  for (const message of messages.slice(matchesPriorContext ? history.messageCount : 0)) {
    const receipt = sink.record({
      kind: 'model',
      phase: 'event',
      name: `${provider}.context.message`,
      trace,
      native: { provider, message: message.native },
      semantic: {
        type: 'message',
        origin: 'context',
        role: message.role,
        content: message.content,
        ...(message.name ? { name: message.name } : {}),
        ...(message.callId ? { call_id: message.callId } : {}),
      },
    });
    if (receipt.accepted) {
      refs.push(receipt.recordId);
    } else {
      complete = false;
    }
  }
  if (!exactContext) {
    const skippedItems = extraction.skippedCount > 0;
    sink.record({
      kind: 'correlation',
      phase: 'gap',
      name: skippedItems
        ? `${provider}.context.item_unrecognized`
        : `${provider}.context.message_not_exact`,
      trace,
      native: {
        provider,
        count: extraction.skippedCount + inexactCount,
      },
      semantic: {
        type: 'capture.gap',
        provider,
        reason: 'unsupported_native_value',
        count: extraction.skippedCount + inexactCount,
        detail: skippedItems
          ? `${provider}.context.item_unrecognized`
          : `${provider}.context.message_not_exact`,
      },
    });
  }
  const admittedCount = messages.length - (matchesPriorContext ? history.messageCount : 0);
  complete &&= refs.length === admittedCount;
  return {
    messageCount: messages.length,
    ...(digest ? { digest } : {}),
    ...(complete ? { contextRefs: refs } : {}),
    ...(complete && matchesPriorContext ? { contextBaseRef: history.requestRef } : {}),
    complete,
  };
}

function providerContextHistory(
  histories: Map<string, ProviderContextHistory>,
  traceId: string,
): ProviderContextHistory {
  const existing = histories.get(traceId);
  if (existing) {
    histories.delete(traceId);
    histories.set(traceId, existing);
    return existing;
  }
  while (histories.size >= MAX_PROVIDER_CONTEXT_TRACES) {
    const oldest = histories.keys().next().value;
    if (oldest === undefined) break;
    histories.delete(oldest);
  }
  const created: ProviderContextHistory = {
    messageCount: 0,
    digest: createHash('sha256').digest('hex'),
    eligible: false,
  };
  histories.set(traceId, created);
  return created;
}

function canonicalProviderMessageKey(message: ProviderMessage): string {
  return createHash('sha256').update(canonicalProviderJson({
    role: message.role,
    native: message.native,
    ...(message.name ? { name: message.name } : {}),
    ...(message.callId ? { callId: message.callId } : {}),
  })).digest('hex');
}

function updateProviderContextDigest(
  digest: ReturnType<typeof createHash>,
  messageKey: string,
): void {
  digest.update(messageKey.length.toString(16));
  digest.update(':');
  digest.update(messageKey);
}

function exactProviderSnapshot(value: unknown): boolean {
  const stack = [value];
  while (stack.length) {
    const current = stack.pop();
    if (Array.isArray(current)) {
      stack.push(...current);
      continue;
    }
    if (!isObject(current)) continue;
    if ('$semantic_layer_omitted' in current || '$semantic_layer_cycle' in current) {
      return false;
    }
    stack.push(...Object.values(current));
  }
  return true;
}

function canonicalProviderJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalProviderJson).join(',')}]`;
  if (isObject(value)) {
    return `{${Object.keys(value).sort().map((key) => (
      `${JSON.stringify(key)}:${canonicalProviderJson(value[key])}`
    )).join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

function canonicalProviderMessageContent(
  provider: ProviderName,
  content: unknown,
): unknown {
  const copied = snapshotNative(content);
  if (!Array.isArray(copied)) return copied;
  if (provider === 'gemini') {
    return copied.map((part) => {
      if (!isObject(part)) return part;
      const { thoughtSignature: _thoughtSignature, ...material } = part;
      return material;
    });
  }
  if (provider !== 'anthropic') return copied;
  return copied.map((block) => {
    if (!isObject(block)) return block;
    if (block.type === 'thinking') {
      const { signature: _signature, ...material } = block;
      return material;
    }
    if (block.type === 'redacted_thinking') return { type: 'redacted_thinking' };
    return block;
  });
}

function requestMessages(
  provider: ProviderName,
  operation: string,
  request: unknown,
): ProviderMessageExtraction {
  if (!isObject(request)) return { messages: [], complete: false, skippedCount: 1 };
  if (provider === 'gemini') return geminiRequestMessages(request);
  const messages: ProviderMessage[] = [];
  let skippedCount = 0;
  if (provider === 'anthropic' && request.system !== undefined) {
    messages.push({ role: 'system', content: request.system, native: request.system });
  }
  let candidates: unknown[] = [];
  if (provider === 'openai' || provider === 'openrouter') {
    if (operation === 'responses' && request.instructions != null) {
      if (typeof request.instructions === 'string') {
        messages.push({
          role: 'system',
          content: request.instructions,
          native: request.instructions,
        });
      } else {
        skippedCount += 1;
      }
    }
    if (operation === 'responses' && request.previous_response_id != null) {
      skippedCount += 1;
    }
    if (operation === 'responses' && request.conversation != null) {
      skippedCount += 1;
    }
    if (operation === 'responses' && request.prompt != null) {
      skippedCount += 1;
    }
    const context = operation === 'responses' ? request.input : request.messages;
    if (Array.isArray(context)) {
      candidates = context;
    } else if (operation === 'responses' && typeof context === 'string') {
      messages.push({ role: 'user', content: context, native: context });
    } else if (context !== undefined) {
      skippedCount += 1;
    }
  } else if (Array.isArray(request.messages)) {
    candidates = request.messages;
  } else if (request.messages !== undefined) {
    skippedCount += 1;
  }
  for (const candidate of candidates) {
    if (!isObject(candidate)) {
      skippedCount += 1;
      continue;
    }
    if (
      (provider === 'openai' || provider === 'openrouter')
      && candidate.type === 'function_call_output'
      && candidate.output !== undefined
    ) {
      messages.push({
        role: 'tool',
        content: candidate.output,
        native: candidate,
        ...(exactIdentity(candidate.call_id) ? { callId: exactIdentity(candidate.call_id)! } : {}),
      });
      continue;
    }
    if (
      (provider === 'openai' || provider === 'openrouter')
      && candidate.type === 'function_call'
    ) {
      messages.push({
        role: 'assistant',
        content: snapshotNative(candidate),
        native: candidate,
        ...(exactIdentity(candidate.name) ? { name: exactIdentity(candidate.name)! } : {}),
        ...(exactIdentity(candidate.call_id) ? { callId: exactIdentity(candidate.call_id)! } : {}),
      });
      continue;
    }
    const role = providerMessageRole(candidate.role);
    if (!role || candidate.content === undefined) {
      skippedCount += 1;
      continue;
    }
    messages.push({
      role,
      content: candidate.content,
      native: candidate,
      ...(exactIdentity(candidate.name) ? { name: exactIdentity(candidate.name)! } : {}),
      ...(exactIdentity(candidate.tool_call_id)
        ? { callId: exactIdentity(candidate.tool_call_id)! }
        : {}),
    });
  }
  return { messages, complete: skippedCount === 0, skippedCount };
}

function geminiRequestMessages(request: MethodTarget): ProviderMessageExtraction {
  const messages: ProviderMessage[] = [];
  let skippedCount = 0;
  const config = isObject(request.config) ? request.config : undefined;
  const instructionCandidates = [
    config?.systemInstruction,
    config?.system_instruction,
    request.system_instruction,
  ].filter((value) => value != null);
  if (instructionCandidates.length === 1) {
    const instruction = instructionCandidates[0];
    if (typeof instruction === 'string' || Array.isArray(instruction) || isObject(instruction)) {
      messages.push({ role: 'system', content: instruction, native: instruction });
    } else {
      skippedCount += 1;
    }
  } else if (instructionCandidates.length > 1) {
    skippedCount += instructionCandidates.length;
  }
  if (request.contents === undefined) {
    return { messages, complete: skippedCount === 0, skippedCount };
  }
  if (typeof request.contents === 'string') {
    messages.push({ role: 'user', content: request.contents, native: request.contents });
    return { messages, complete: skippedCount === 0, skippedCount };
  }
  if (!Array.isArray(request.contents)) {
    return { messages, complete: false, skippedCount: skippedCount + 1 };
  }
  for (const content of request.contents) {
    if (!isObject(content) || !Array.isArray(content.parts)) {
      skippedCount += 1;
      continue;
    }
    const role = content.role === 'model'
      ? 'assistant'
      : content.role === 'user' ? 'user' : undefined;
    if (!role) {
      skippedCount += 1;
      continue;
    }
    messages.push({ role, content: content.parts, native: content });
  }
  return { messages, complete: skippedCount === 0, skippedCount };
}

function providerMessageRole(value: unknown): ProviderMessage['role'] | undefined {
  return value === 'system'
    || value === 'developer'
    || value === 'user'
    || value === 'assistant'
    || value === 'tool'
    ? value
    : undefined;
}

function modelRequestFields(
  provider: ProviderName,
  request: unknown,
  contextRefs?: readonly string[],
  contextBaseRef?: string,
): Record<string, unknown> {
  if (!isObject(request)) {
    return {
      ...(contextRefs ? { context_refs: [...contextRefs] } : {}),
      ...(contextBaseRef ? { context_base_ref: contextBaseRef } : {}),
    };
  }
  const model = exactIdentity(request.model);
  const tools = requestToolNames(provider, request);
  return {
    ...(contextRefs ? { context_refs: [...contextRefs] } : {}),
    ...(contextBaseRef ? { context_base_ref: contextBaseRef } : {}),
    ...(model ? { model } : {}),
    ...(tools.length ? { tools } : {}),
  };
}

function providerRequestNative(
  provider: ProviderName,
  request: unknown,
  messageCount: number,
): unknown {
  if (!isObject(request)) return snapshotNative(request);
  const contextFields = provider === 'gemini'
    ? new Set(['contents', 'system_instruction'])
    : provider === 'anthropic'
      ? new Set(['messages', 'system'])
      : new Set(['input', 'messages', 'instructions']);
  const metadata = snapshotProviderRequestMetadata(request, contextFields);
  if (provider === 'gemini' && isObject(metadata.config)) {
    const config = Object.fromEntries(Object.entries(metadata.config).filter(
      ([key]) => key !== 'systemInstruction' && key !== 'system_instruction',
    ));
    if (Object.keys(config).length) {
      metadata.config = config;
    } else {
      delete metadata.config;
    }
  }
  return {
    message_count: messageCount,
    metadata,
  };
}

/** Remove only provider-defined opaque continuation material from persisted evidence. */
function providerResponseNative(provider: ProviderName, value: unknown): unknown {
  const copied = snapshotNative(value);
  if (!isObject(copied)) return copied;
  sanitizeProviderResponseNative(provider, copied);
  return copied;
}

function sanitizeProviderResponseNative(provider: ProviderName, value: MethodTarget): void {
  if (isObject(value.response)) sanitizeProviderResponseNative(provider, value.response);
  if (provider === 'openai' || provider === 'openrouter') {
    if (isObject(value.item)) sanitizeOpenAIReasoningItem(value.item);
    if (Array.isArray(value.output)) {
      for (const item of value.output) {
        if (isObject(item)) sanitizeOpenAIReasoningItem(item);
      }
    }
    if (Array.isArray(value.choices)) {
      for (const choice of value.choices) {
        if (!isObject(choice)) continue;
        const message = isObject(choice.message)
          ? choice.message
          : isObject(choice.delta) ? choice.delta : undefined;
        if (!message) continue;
        delete message.encrypted_reasoning;
        if (!Array.isArray(message.reasoning_details)) continue;
        message.reasoning_details = message.reasoning_details.map((detail) => {
          if (!isObject(detail)) return detail;
          if (detail.type === 'reasoning.encrypted') {
            const { data: _data, ...metadata } = detail;
            return metadata;
          }
          if (detail.type === 'reasoning.text') {
            const { signature: _signature, ...readable } = detail;
            return readable;
          }
          return detail;
        });
      }
    }
    return;
  }
  if (provider === 'anthropic') {
    if (Array.isArray(value.content)) value.content = value.content.map(sanitizeAnthropicThinkingBlock);
    if (isObject(value.content_block)) {
      value.content_block = sanitizeAnthropicThinkingBlock(value.content_block);
    }
    if (isObject(value.delta) && value.delta.type === 'signature_delta') {
      value.delta = { type: 'signature_delta' };
    }
    return;
  }
  if (!Array.isArray(value.candidates)) return;
  for (const candidate of value.candidates) {
    if (!isObject(candidate) || !isObject(candidate.content)
      || !Array.isArray(candidate.content.parts)) continue;
    candidate.content.parts = candidate.content.parts.map((part) => {
      if (!isObject(part)) return part;
      const { thoughtSignature: _thoughtSignature, ...readable } = part;
      return readable;
    });
  }
}

function sanitizeOpenAIReasoningItem(item: MethodTarget): void {
  if (item.type === 'compaction') {
    delete item.encrypted_content;
    return;
  }
  if (item.type !== 'reasoning') return;
  delete item.encrypted_content;
  delete item.encrypted_reasoning;
}

function sanitizeAnthropicThinkingBlock(block: unknown): unknown {
  if (!isObject(block)) return block;
  if (block.type === 'redacted_thinking') return { type: 'redacted_thinking' };
  if (block.type !== 'thinking') return block;
  const { signature: _signature, ...readable } = block;
  return readable;
}

function providerReasoningUnavailable(provider: ProviderName, value: unknown): boolean {
  if (!isObject(value)) return false;
  const response = isObject(value.response) ? value.response : value;
  if (provider === 'openai' || provider === 'openrouter') {
    if (isObject(response.item) && openAIReasoningItemUnavailable(response.item)) return true;
    if (Array.isArray(response.output)
      && response.output.some((item) => isObject(item) && openAIReasoningItemUnavailable(item))) {
      return true;
    }
    if (!Array.isArray(response.choices)) return false;
    return response.choices.some((choice) => {
      if (!isObject(choice)) return false;
      const message = isObject(choice.message)
        ? choice.message
        : isObject(choice.delta) ? choice.delta : undefined;
      return Array.isArray(message?.reasoning_details) && message.reasoning_details.some(
        (detail) => isObject(detail) && detail.type === 'reasoning.encrypted',
      );
    });
  }
  if (provider !== 'anthropic' || !Array.isArray(response.content)) return false;
  return response.content.some((block) => (
    isObject(block) && (block.type === 'redacted_thinking'
      || (block.type === 'thinking' && block.thinking === '' && exactIdentity(block.signature)))
  ));
}

function openAIReasoningItemUnavailable(item: MethodTarget): boolean {
  return item.type === 'reasoning'
    && typeof item.encrypted_content === 'string'
    && item.encrypted_content.length > 0;
}

function recordReasoningUnavailable(
  provider: ProviderName,
  sink: SourceSink,
  trace: TraceIdentity,
  responseRecordId: string,
): void {
  sink.record({
    kind: 'model', phase: 'gap', name: `${provider}.reasoning.opaque_unavailable`, trace,
    parentRecordId: responseRecordId,
    native: { provider, reasoning: 'opaque_unavailable' },
    semantic: {
      type: 'capture.gap',
      provider,
      reason: 'unsupported_native_value',
      detail: `${provider}.reasoning.opaque_unavailable`,
    },
  });
}

function snapshotProviderRequestMetadata(
  request: MethodTarget,
  contextFields: ReadonlySet<string>,
): Record<string, unknown> {
  const metadata: Record<string, unknown> = {};
  try {
    for (const key in request) {
      if (contextFields.has(key)) continue;
      const descriptor = Object.getOwnPropertyDescriptor(request, key);
      if (!descriptor || !('value' in descriptor)) continue;
      metadata[key] = snapshotNative(descriptor.value);
    }
  } catch {
    return metadata;
  }
  return metadata;
}

function requestToolNames(provider: ProviderName, request: MethodTarget): string[] {
  const names: string[] = [];
  const add = (value: unknown): void => {
    const name = exactIdentity(value);
    if (name && !names.includes(name)) names.push(name);
  };
  if (Array.isArray(request.tools)) {
    for (const tool of request.tools) {
      if (!isObject(tool)) continue;
      if (provider === 'openai' || provider === 'openrouter') {
        add(isObject(tool.function) ? tool.function.name : tool.name);
      } else if (provider === 'anthropic') {
        add(tool.name);
      } else if (provider === 'gemini' && Array.isArray(tool.functionDeclarations)) {
        for (const declaration of tool.functionDeclarations) {
          if (isObject(declaration)) add(declaration.name);
        }
      }
    }
  }
  const config = request.config;
  if (provider === 'gemini' && isObject(config) && Array.isArray(config.tools)) {
    for (const tool of config.tools) {
      if (!isObject(tool) || !Array.isArray(tool.functionDeclarations)) continue;
      for (const declaration of tool.functionDeclarations) {
        if (isObject(declaration)) add(declaration.name);
      }
    }
  }
  return names;
}

function modelResponseFields(
  provider: ProviderName,
  value: unknown,
): Record<string, unknown> {
  if (!isObject(value)) return { status: 'completed' };
  const response = isObject(value.response) && /response\.(completed|failed|incomplete)/.test(
    String(value.type),
  ) ? value.response : value;
  const model = exactIdentity(response.model) ?? exactIdentity(response.modelVersion);
  const content = responseContent(provider, response);
  const reasoning = responseReasoning(provider, response);
  const finishReason = responseFinishReason(provider, response);
  const usage = normalizedUsage(response.usage ?? response.usageMetadata);
  const nativeStatus = exactIdentity(response.status);
  const status = nativeStatus === 'failed'
    ? 'failed'
    : nativeStatus === 'incomplete'
      ? 'incomplete'
      : nativeStatus === 'cancelled' ? 'cancelled' : 'completed';
  return {
    status,
    ...(model ? { model } : {}),
    ...(content !== undefined ? { content } : {}),
    ...(reasoning.length ? { reasoning } : {}),
    ...(finishReason ? { finish_reason: finishReason } : {}),
    ...(usage ? { usage } : {}),
  };
}

function responseContent(provider: ProviderName, value: MethodTarget): unknown {
  if (typeof value.output_text === 'string') return value.output_text;
  if (provider === 'anthropic' && Array.isArray(value.content)) {
    const text = value.content
      .filter((part) => isObject(part) && part.type === 'text' && typeof part.text === 'string')
      .map((part) => (part as MethodTarget).text as string)
      .join('');
    return text.length ? text : undefined;
  }
  if (provider === 'gemini' && Array.isArray(value.candidates)) {
    const text = value.candidates.flatMap((candidate) => (
      isObject(candidate) && isObject(candidate.content) && Array.isArray(candidate.content.parts)
        ? candidate.content.parts
          .filter((part) => isObject(part) && part.thought !== true && typeof part.text === 'string')
          .map((part) => (part as MethodTarget).text as string)
        : []
    )).join('');
    return text.length ? text : undefined;
  }
  if (Array.isArray(value.choices)) {
    const first = value.choices[0];
    if (isObject(first)) {
      const message = isObject(first.message) ? first.message : isObject(first.delta) ? first.delta : undefined;
      if (message?.content !== undefined && message.content !== null) return message.content;
    }
  }
  if (Array.isArray(value.output)) {
    const text = value.output.flatMap((item) => {
      if (!isObject(item) || item.type !== 'message' || !Array.isArray(item.content)) return [];
      return item.content
        .filter((part) => isObject(part) && typeof part.text === 'string')
        .map((part) => (part as MethodTarget).text as string);
    }).join('');
    return text.length ? text : undefined;
  }
  return undefined;
}

function responseReasoning(
  provider: ProviderName,
  value: MethodTarget,
): Array<{ type: 'text' | 'summary'; text: string }> {
  const blocks: Array<{ type: 'text' | 'summary'; text: string }> = [];
  const add = (type: 'text' | 'summary', text: unknown): void => {
    if (typeof text !== 'string' || text.length === 0) return;
    blocks.push({ type, text });
  };
  if (Array.isArray(value.reasoning)) {
    for (const block of value.reasoning) {
      if (isObject(block) && (block.type === 'text' || block.type === 'summary')) {
        add(block.type, block.text);
      }
    }
    if (blocks.length) return blocks;
  }
  if (provider === 'anthropic' && Array.isArray(value.content)) {
    for (const part of value.content) {
      // Anthropic documents readable thinking blocks as summaries of private reasoning.
      if (isObject(part) && part.type === 'thinking') add('summary', part.thinking);
    }
  }
  if (provider === 'gemini' && Array.isArray(value.candidates)) {
    for (const candidate of value.candidates) {
      if (!isObject(candidate) || !isObject(candidate.content)
        || !Array.isArray(candidate.content.parts)) continue;
      for (const part of candidate.content.parts) {
        // GenerateContent's includeThoughts surface exposes thought summaries.
        if (isObject(part) && part.thought === true) add('summary', part.text);
      }
    }
  }
  if (Array.isArray(value.choices)) {
    const first = value.choices[0];
    if (isObject(first)) {
      const message = isObject(first.message) ? first.message : isObject(first.delta) ? first.delta : undefined;
      const detailed = openRouterReasoningDetails(message?.reasoning_details);
      if (detailed.length) blocks.push(...detailed);
      else add('text', message?.reasoning_content ?? message?.reasoning);
    }
  }
  if (Array.isArray(value.output)) {
    for (const item of value.output) {
      if (!isObject(item) || item.type !== 'reasoning') continue;
      if (Array.isArray(item.summary)) {
        for (const summary of item.summary) {
          if (typeof summary === 'string') add('summary', summary);
          else if (isObject(summary)) add('summary', summary.text);
        }
      }
      if (Array.isArray(item.content)) {
        for (const content of item.content) {
          if (isObject(content) && content.type === 'reasoning_text') add('text', content.text);
        }
      }
      add('text', item.text);
    }
  }
  return blocks;
}

function openRouterReasoningDetails(
  value: unknown,
): Array<{ type: 'text' | 'summary'; text: string }> {
  if (!Array.isArray(value)) return [];
  const blocks: Array<{ type: 'text' | 'summary'; text: string }> = [];
  for (const detail of value) {
    if (!isObject(detail)) continue;
    const block = openRouterReasoningDetail(detail);
    if (block) blocks.push(block);
  }
  return blocks;
}

function openRouterReasoningDetail(
  detail: MethodTarget,
): { type: 'text' | 'summary'; text: string } | undefined {
  if (detail.type === 'reasoning.summary'
    && typeof detail.summary === 'string' && detail.summary.length > 0) {
    return { type: 'summary', text: detail.summary };
  }
  if (detail.type === 'reasoning.text'
    && typeof detail.text === 'string' && detail.text.length > 0) {
    return { type: 'text', text: detail.text };
  }
  return undefined;
}

function responseFinishReason(provider: ProviderName, value: MethodTarget): string | undefined {
  const direct = provider === 'anthropic'
    ? exactIdentity(value.stop_reason)
    : exactIdentity(value.finishReason) ?? exactIdentity(value.finish_reason);
  if (direct) return direct;
  const candidates = provider === 'gemini' ? value.candidates : value.choices;
  if (!Array.isArray(candidates) || !isObject(candidates[0])) return undefined;
  return exactIdentity(candidates[0].finishReason)
    ?? exactIdentity(candidates[0].finish_reason)
    ?? exactIdentity(candidates[0].stop_reason);
}

function normalizedUsage(value: unknown): Record<string, number> | undefined {
  if (!isObject(value)) return undefined;
  const input = finiteNonnegative(
    value.input_tokens ?? value.prompt_tokens ?? value.promptTokenCount,
  );
  const output = finiteNonnegative(
    value.output_tokens ?? value.completion_tokens ?? value.candidatesTokenCount,
  );
  if (input === undefined && output === undefined) return undefined;
  return {
    ...(input !== undefined ? { input_tokens: input } : {}),
    ...(output !== undefined ? { output_tokens: output } : {}),
  };
}

function finiteNonnegative(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : undefined;
}

function nonnegativeInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function toolProposalFields(
  provider: ProviderName,
  tool: unknown,
  fallbackCallId?: string,
): Record<string, unknown> | undefined {
  if (!isObject(tool)) return undefined;
  const fn = isObject(tool.function) ? tool.function : tool;
  const name = exactIdentity(fn.name);
  if (!name) return undefined;
  const inputValue = fn.arguments ?? fn.input ?? fn.args;
  if (inputValue === undefined) return undefined;
  const nativeCallId = provider === 'gemini'
    ? fallbackCallId
    : exactIdentity(tool.id)
      ?? exactIdentity(tool.call_id)
      ?? exactIdentity(tool.callId);
  const callId = nativeCallId ?? fallbackCallId;
  const input = parseExactJson(inputValue);
  return {
    name,
    input,
    ...(callId ? { call_id: callId } : {}),
    ...(nativeCallId ? { native_call_id: nativeCallId } : {}),
  };
}

function parseExactJson(value: unknown): unknown {
  if (typeof value !== 'string') return snapshotNative(value);
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function normalizedError(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    return {
      type: normalizedErrorType(error.name),
      message: error.message || String(error),
      recoverable: false,
      ...errorCode(error),
    };
  }
  if (isObject(error)) {
    const type = exactIdentity(error.type) ?? exactIdentity(error.name) ?? 'error';
    const message = exactIdentity(error.message) ?? String(snapshotNative(error));
    return {
      type: normalizedErrorType(type),
      message,
      recoverable: false,
      ...errorCode(error),
    };
  }
  return { type: 'error', message: String(error), recoverable: false };
}

function normalizedErrorType(value: string): string {
  const normalized = value
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '_')
    .replace(/^[^a-z]+/, '');
  return /^[a-z][a-z0-9._-]{2,127}$/.test(normalized) ? normalized : 'provider_error';
}

function errorCode(error: unknown): Record<string, string> {
  if (!isObject(error)) return {};
  const value = exactIdentity(error.code)
    ?? (typeof error.status === 'number' ? String(error.status) : undefined);
  return value ? { code: value } : {};
}

function finishError(
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
  error: unknown,
  requestRecordId?: string,
): void {
  recordModelError(provider, operation, sink, trace, error, requestRecordId);
  sink.record({
    kind: 'lifecycle', phase: 'end', name: `${provider}.${operation}`, trace,
    native: { provider, operation, error: snapshotNative(error) },
    semantic: {
      type: 'agent.run',
      status: 'failed',
    },
  });
}

function recordModelError(
  provider: ProviderName,
  operation: string,
  sink: SourceSink,
  trace: TraceIdentity,
  error: unknown,
  requestRecordId?: string,
): void {
  const structuredError = structuredErrorEvidence(error);
  sink.record({
    kind: 'error', phase: 'error', name: `${provider}.error`, trace,
    ...(requestRecordId ? { parentRecordId: requestRecordId } : {}),
    ...(typeof error === 'object' && error !== null ? { errorIdentity: error } : {}),
    native: { provider, operation, error: snapshotNative(error),
      ...(structuredError === undefined ? {} : { structured_error: structuredError }) },
    semantic: { type: 'model.error', provider, error: normalizedError(error) },
  });
}

function structuredErrorEvidence(error: unknown): unknown {
  return findStructuredError(snapshotNative(error), new WeakSet<object>());
}

function findStructuredError(value: unknown, seen: WeakSet<object>): unknown {
  if (typeof value === 'string') {
    const start = value.indexOf('{');
    if (start >= 0) {
      try { return findStructuredError(JSON.parse(value.slice(start)), seen); } catch { return undefined; }
    }
    return undefined;
  }
  if (!value || typeof value !== 'object') return undefined;
  if (seen.has(value)) return undefined;
  seen.add(value);
  if (!Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    const custom = record.custom as Record<string, unknown> | undefined;
    if (custom?.code === 'SEMANTIC_LAYER_CAPTURE_FAILURE_V1') return value;
    for (const child of Object.values(record)) {
      const found = findStructuredError(child, seen);
      if (found !== undefined) return found;
    }
  } else {
    for (const child of value) {
      const found = findStructuredError(child, seen);
      if (found !== undefined) return found;
    }
  }
  return undefined;
}

function resolveMethod(root: object, path: readonly string[]): {
  target: MethodTarget;
  key: string;
  original: (...args: unknown[]) => unknown;
  ownDescriptor: PropertyDescriptor | undefined;
  descriptor: PropertyDescriptor;
} | undefined {
  let current: unknown = root;
  for (const segment of path.slice(0, -1)) {
    if (!isObject(current)) return undefined;
    current = current[segment];
  }
  if (!isObject(current)) return undefined;
  const key = path.at(-1)!;
  const original = current[key];
  if (typeof original !== 'function') return undefined;
  const descriptor = propertyDescriptor(current, key);
  if (!descriptor || !('value' in descriptor) || descriptor.writable === false) return undefined;
  return {
    target: current,
    key,
    original: original as (...args: unknown[]) => unknown,
    ownDescriptor: Object.getOwnPropertyDescriptor(current, key),
    descriptor,
  };
}

function propertyDescriptor(target: MethodTarget, key: PropertyKey): PropertyDescriptor | undefined {
  let current: object | null = target;
  while (current !== null) {
    const descriptor = Object.getOwnPropertyDescriptor(current, key);
    if (descriptor) return descriptor;
    current = Object.getPrototypeOf(current) as object | null;
  }
  return undefined;
}

function replaceMethod(
  target: MethodTarget,
  key: PropertyKey,
  wrapper: (...args: unknown[]) => unknown,
  ownDescriptor: PropertyDescriptor | undefined,
  descriptor: PropertyDescriptor,
): Restore | undefined {
  try {
    Object.defineProperty(target, key, ownDescriptor
      ? { ...ownDescriptor, value: wrapper }
      : { value: wrapper, writable: true, enumerable: descriptor.enumerable, configurable: true });
  } catch {
    return undefined;
  }
  return () => {
    if (ownDescriptor) Object.defineProperty(target, key, ownDescriptor);
    else delete target[key];
  };
}

export function createGeminiToolCorrelation(
  maxPendingTurns = MAX_GEMINI_PENDING_TOOL_TURNS,
): GeminiToolCorrelation {
  const pendingByTurn = new Map<string, GeminiPendingToolCalls>();
  return {
    proposalCallIds(trace, context, slots, observationIndex) {
      if (slots.length === 0) return [];
      void trace;
      void observationIndex;
      const callIds = slots.map((slot) => exactIdentity(slot.tool.id));
      const counts = new Map<string, number>();
      for (const callId of callIds) {
        if (callId) counts.set(callId, (counts.get(callId) ?? 0) + 1);
      }
      const usableCallIds = callIds.map((callId) => (
        callId && counts.get(callId) === 1 ? callId : undefined
      ));
      const exactCallIds = usableCallIds.filter(
        (callId): callId is string => callId !== undefined,
      );
      const turnKey = geminiTurnKey(context?.conversationId, context?.turnId);
      if (!turnKey) return usableCallIds;
      const existing = pendingByTurn.get(turnKey);
      pendingByTurn.set(turnKey, {
        callIds: [...(existing?.callIds ?? []), ...exactCallIds],
        hasUnidentified: (existing?.hasUnidentified ?? false)
          || exactCallIds.length !== slots.length,
      });
      while (pendingByTurn.size > maxPendingTurns) {
        const oldest = pendingByTurn.keys().next().value;
        if (oldest === undefined) break;
        pendingByTurn.delete(oldest);
      }
      return usableCallIds;
    },
    resultCallIds(context, request) {
      const results = exactTerminalGeminiFunctionResponses(request);
      if (!results) return { callIds: new Map(), unpaired: false };
      const parentTurnKey = geminiTurnKey(context?.conversationId, context?.previousTurnId);
      if (!parentTurnKey) return { callIds: new Map(), unpaired: true };
      const pending = pendingByTurn.get(parentTurnKey);
      pendingByTurn.delete(parentTurnKey);
      if (!pending) {
        return { callIds: new Map(), unpaired: true };
      }
      const resultIds = results.map((result) => exactIdentity(result.id));
      const expected = new Set(pending.callIds);
      const counts = new Map<string, number>();
      for (const callId of resultIds) {
        if (callId) counts.set(callId, (counts.get(callId) ?? 0) + 1);
      }
      const matched = new Map<MethodTarget, string>();
      for (const [index, result] of results.entries()) {
        const callId = resultIds[index];
        if (callId && counts.get(callId) === 1 && expected.has(callId)) {
          matched.set(result, callId);
        }
      }
      const fullyPaired = !pending.hasUnidentified
        && matched.size === results.length
        && matched.size === pending.callIds.length;
      return {
        callIds: matched,
        unpaired: !fullyPaired,
      };
    },
    clear() {
      pendingByTurn.clear();
    },
  };
}

function geminiTurnKey(conversationId: unknown, turnId: unknown): string | undefined {
  const conversation = exactIdentity(conversationId);
  const turn = exactIdentity(turnId);
  return conversation && turn ? JSON.stringify([conversation, turn]) : undefined;
}

function exactIdentity(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim().length > 0 ? value : undefined;
}

function exactTerminalGeminiFunctionResponses(request: unknown): MethodTarget[] | undefined {
  if (!isObject(request) || !Array.isArray(request.contents) || request.contents.length === 0) {
    return undefined;
  }
  const terminalContent = request.contents.at(-1);
  if (!isObject(terminalContent) || !Array.isArray(terminalContent.parts)
    || terminalContent.parts.length === 0) return undefined;
  const results: MethodTarget[] = [];
  let reachedUserText = false;
  for (const part of terminalContent.parts) {
    if (!isObject(part)) return undefined;
    if (isObject(part.functionResponse)) {
      if (reachedUserText) return undefined;
      results.push(part.functionResponse);
      continue;
    }
    if (typeof part.text === 'string' && part.text.length > 0 && results.length > 0) {
      reachedUserText = true;
      continue;
    }
    return undefined;
  }
  return results.length > 0 ? results : undefined;
}

function geminiToolSlots(value: MethodTarget): GeminiToolSlot[] {
  if (!Array.isArray(value.candidates)) return [];
  return value.candidates.flatMap((candidate, candidateIndex) => {
    if (!isObject(candidate) || !isObject(candidate.content)
      || !Array.isArray(candidate.content.parts)) return [];
    return candidate.content.parts.flatMap((part, partIndex) => (
      isObject(part) && isObject(part.functionCall)
        ? [{ tool: part.functionCall, candidateIndex, partIndex }]
        : []
    ));
  });
}

function toolCalls(value: MethodTarget): unknown[] {
  if (isObject(value.content_block) && /tool/.test(String(value.content_block.type))) {
    return [value.content_block];
  }
  const direct = value.tool_calls;
  if (Array.isArray(direct)) return direct;
  const output = value.output;
  if (Array.isArray(output)) return output.filter((item) => isObject(item) && /tool|function/.test(String(item.type)));
  const content = value.content;
  if (Array.isArray(content)) return content.filter((item) => isObject(item) && /tool|function/.test(String(item.type)));
  const candidates = value.candidates;
  if (Array.isArray(candidates)) return candidates.flatMap((candidate) => {
    if (!isObject(candidate) || !isObject(candidate.content) || !Array.isArray(candidate.content.parts)) return [];
    return candidate.content.parts.flatMap((part) => (
      isObject(part) && isObject(part.functionCall) ? [part.functionCall] : []
    ));
  });
  const choices = value.choices;
  if (!Array.isArray(choices)) return [];
  return choices.flatMap((choice) => {
    if (!isObject(choice)) return [];
    const message = isObject(choice.message) ? choice.message : isObject(choice.delta) ? choice.delta : undefined;
    return message && Array.isArray(message.tool_calls) ? message.tool_calls : [];
  });
}

function nativeId(value: unknown): string | undefined {
  return isObject(value) && typeof value.id === 'string' ? value.id : undefined;
}

function isObject(value: unknown): value is MethodTarget {
  return (typeof value === 'object' && value !== null) || typeof value === 'function';
}

function isThenable(value: unknown): value is PromiseLike<unknown> {
  return isObject(value) && typeof value.then === 'function';
}

function isIterator(value: unknown): value is AsyncIterator<unknown> {
  return isObject(value) && typeof value.next === 'function';
}
