import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  AdmissionReceipt,
  CaptureSource,
  SourceEventKind,
  SourceSink,
  TraceIdentity,
} from '../v1/types.js';
import { trustOfficialSource } from '../v1/source-ownership.js';
import { snapshotNative, snapshotRecord } from './native-snapshot.js';

type LangGraphAdapterOptions = { version?: string };
type CallbackHandler = { name: string; [key: string]: unknown };
type CallbackManager = {
  copy(): CallbackManager;
  addHandler(handler: CallbackHandler, inherit?: boolean): void;
};
type RunnableConfig = Record<string, unknown> & { callbacks?: unknown };
type RunnableSubject = {
  invoke(input: unknown, config?: RunnableConfig): unknown;
  stream(input: unknown, config?: RunnableConfig): unknown;
  streamEvents?: (input: unknown, config?: RunnableConfig, options?: unknown) => unknown;
};
type StateTracker = {
  seenMessages: WeakMap<object, string>;
  lastModelDelivery?: {
    content: unknown;
    receipt: AdmissionReceipt;
  };
  lastStateReceipt?: AdmissionReceipt;
  lastValue?: unknown;
};
type GraphStreamInvocation = {
  trace?: TraceIdentity;
  nativeIdentity?: string;
  finish?: () => void;
  consumerTerminal: boolean;
  stateTracker: StateTracker;
  pending: Array<(trace: TraceIdentity, nativeIdentity?: string) => void>;
};
type Bridge = {
  active: boolean;
  installed: boolean;
  streams: AsyncLocalStorage<GraphStreamInvocation>;
  sink?: SourceSink;
  handler?: CallbackHandler;
};
type OpenRun = {
  trace: TraceIdentity;
  name: string;
  pairIdentity: string;
  rootPairIdentity: string;
  start?: AdmissionReceipt;
  kind: SourceEventKind;
  parentRunId?: string;
  threadId?: string;
  callId?: string;
  stateTracker: StateTracker;
  toolCorrelation?: ToolCorrelation;
};
type InterruptedRun = { trace: TraceIdentity; receipt: AdmissionReceipt; pairIdentity: string };
type ToolCorrelation = {
  key: string;
  fingerprint: string;
  open: OpenRun;
  runIds: Set<string>;
  paused: boolean;
  terminalFingerprint?: string;
};

export type LangGraphCaptureAdapter = {
  createSource(graph: object): CaptureSource;
  invoke(input: unknown, config?: RunnableConfig): unknown;
  stream(input: unknown, config?: RunnableConfig): unknown;
  streamEvents(input: unknown, config?: RunnableConfig, options?: unknown): unknown;
};

/** Injects a documented LangChain callback handler into LangGraph runnable configuration. */
export function langGraphAdapter(options: LangGraphAdapterOptions = {}): LangGraphCaptureAdapter {
  const bridge: Bridge = { active: false, installed: false, streams: new AsyncLocalStorage() };
  let subject: RunnableSubject | undefined;
  return Object.freeze({
    createSource(graph: object) {
      if (!isRunnableSubject(graph)) throw new TypeError('LangGraph subject must expose invoke and stream');
      if (subject && subject !== graph) throw new Error('LangGraph adapter is already bound');
      subject = graph;
      return createLangGraphSource(bridge, options.version);
    },
    invoke(input: unknown, config?: RunnableConfig) {
      const runnable = requireBound(subject, bridge);
      return Reflect.apply(runnable.invoke, runnable, [
        input,
        bridge.active && bridge.handler ? injectCallbacks(config, bridge.handler) : config,
      ]);
    },
    stream(input: unknown, config?: RunnableConfig) {
      const runnable = requireBound(subject, bridge);
      if (!bridge.active || !bridge.handler || !bridge.sink) {
        return Reflect.apply(runnable.stream, runnable, [input, config]);
      }
      const invocation: GraphStreamInvocation = {
        consumerTerminal: false,
        stateTracker: { seenMessages: new WeakMap() },
        pending: [],
      };
      const result = bridge.streams.run(invocation, () => Reflect.apply(runnable.stream, runnable, [
        input, injectCallbacks(config, bridge.handler!),
      ]));
      return observeGraphStreamResult(result, bridge, invocation);
    },
    streamEvents(input: unknown, config?: RunnableConfig, options?: unknown) {
      const runnable = requireBound(subject, bridge);
      if (typeof runnable.streamEvents !== 'function') {
        throw new TypeError('LangGraph subject must expose streamEvents');
      }
      if (!bridge.active || !bridge.handler || !bridge.sink) {
        return Reflect.apply(runnable.streamEvents, runnable, [input, config, options]);
      }
      const invocation: GraphStreamInvocation = {
        consumerTerminal: false,
        stateTracker: { seenMessages: new WeakMap() },
        pending: [],
      };
      const result = bridge.streams.run(invocation, () => Reflect.apply(runnable.streamEvents!, runnable, [
        input, injectCallbacks(config, bridge.handler!), options,
      ]));
      return observeGraphStreamResult(result, bridge, invocation, recordGraphNativeEvent);
    },
  });
}

function createLangGraphSource(bridge: Bridge, version: string | undefined): CaptureSource {
  return trustOfficialSource({
    metadata: {
      name: 'official:langgraph-js',
      seam: 'RunnableConfig.callbacks/streamEvents',
      identityDomain: 'langgraph.run',
      ...(version ? { version } : {}),
      qualification: version === undefined
        ? { status: 'unknown' }
        : ['1.4.7', '1.4.6'].includes(version)
          ? { status: 'exact_qualified' }
          : { status: 'capability_checked_unqualified', profile: 'langgraph-runnable-callbacks-v1' },
      official: true,
      coverage: [{ operation: 'facade-callback-events', domain: 'langgraph.run', role: 'owner' }],
    },
    install(sink) {
      if (bridge.active || bridge.sink) throw new Error('LangGraph adapter is already installed');
      const runs = new Map<string, OpenRun>();
      const interrupts = new Map<string, InterruptedRun>();
      const handler = createCallbackHandler(
        sink, runs, interrupts, () => bridge.active, () => bridge.streams.getStore(),
      );
      bridge.sink = sink;
      bridge.handler = handler;
      bridge.active = true;
      bridge.installed = true;
      return {
        deactivate() { bridge.active = false; },
        drain() {
          bridge.sink = undefined;
          bridge.handler = undefined;
          runs.clear();
          interrupts.clear();
        },
      };
    },
  }, 'deep');
}

function observeGraphStreamResult(
  result: unknown,
  bridge: Bridge,
  invocation: GraphStreamInvocation,
  recordValue: typeof recordGraphChunk = recordGraphChunk,
): unknown {
  if (isPromiseLike(result)) {
    return Promise.resolve(result).then((stream) => (
      observeGraphStream(stream, bridge, invocation, recordValue)
    ));
  }
  return observeGraphStream(result, bridge, invocation, recordValue);
}

function observeGraphStream(
  stream: unknown,
  bridge: Bridge,
  invocation: GraphStreamInvocation,
  recordValue: typeof recordGraphChunk,
): unknown {
  if (!stream || (typeof stream !== 'object' && typeof stream !== 'function')) return stream;
  const source = stream as Record<PropertyKey, unknown>;
  const factory = source[Symbol.asyncIterator];
  if (typeof factory !== 'function') return stream;
  return new Proxy(source, {
    get(target, key) {
      if (key === Symbol.asyncIterator) {
        return function observedGraphIterator() {
          const iterator = Reflect.apply(factory, target, []) as AsyncIterator<unknown>;
          return observeGraphIterator(iterator, bridge, invocation, recordValue);
        };
      }
      const value = Reflect.get(target, key, target);
      if (key === 'getReader' && typeof value === 'function') {
        return (...args: unknown[]) => observeGraphReader(
          Reflect.apply(value, target, args), bridge, invocation, recordValue,
        );
      }
      return typeof value === 'function' && key !== 'constructor' ? value.bind(target) : value;
    },
  });
}

function observeGraphReader(
  reader: unknown,
  bridge: Bridge,
  invocation: GraphStreamInvocation,
  recordValue: typeof recordGraphChunk,
): unknown {
  if (!reader || (typeof reader !== 'object' && typeof reader !== 'function')) return reader;
  const target = reader as Record<PropertyKey, unknown>;
  return new Proxy(target, {
    get(inner, key) {
      const value = Reflect.get(inner, key, inner);
      if (key === 'read' && typeof value === 'function') {
        return (...args: unknown[]) => bridge.streams.run(
          invocation,
          () => Promise.resolve(Reflect.apply(value, inner, args)),
        ).then(
          (part: { done: boolean; value?: unknown }) => {
            if (!part.done) recordValue(bridge, invocation, part.value);
            else finishGraphStream(invocation);
            return part;
          },
          (error: unknown) => {
            recordGraphControlError(bridge, invocation, 'next', error);
            finishGraphStream(invocation);
            throw error;
          },
        );
      }
      if (key === 'cancel' && typeof value === 'function') {
        return (...args: unknown[]) => bridge.streams.run(
          invocation,
          () => Promise.resolve(Reflect.apply(value, inner, args)),
        ).then(
          (result) => {
            recordGraphReaderCancel(bridge, invocation, args, result);
            finishGraphStream(invocation);
            return result;
          },
          (error: unknown) => {
            recordGraphControlError(bridge, invocation, 'return', error);
            finishGraphStream(invocation);
            throw error;
          },
        );
      }
      return typeof value === 'function' ? value.bind(inner) : value;
    },
  });
}

function observeGraphIterator(
  iterator: AsyncIterator<unknown>,
  bridge: Bridge,
  invocation: GraphStreamInvocation,
  recordValue: typeof recordGraphChunk,
): AsyncIterator<unknown> {
  return new Proxy(iterator, {
    get(target, key, receiver) {
      if (key === 'next') return (...args: [] | [unknown]) => bridge.streams.run(
        invocation,
        () => Promise.resolve(Reflect.apply(target.next, target, args)),
      ).then(
        (part) => {
          if (!part.done) recordValue(bridge, invocation, part.value);
          else finishGraphStream(invocation);
          return part;
        },
        (error: unknown) => {
          recordGraphControlError(bridge, invocation, 'next', error);
          finishGraphStream(invocation);
          throw error;
        },
      );
      if (key === 'return' && typeof target.return === 'function') {
        return (...args: [] | [unknown]) => bridge.streams.run(
          invocation,
          () => Promise.resolve(Reflect.apply(target.return!, target, args)),
        ).then(
          (part) => {
            if (!part.done) recordValue(bridge, invocation, part.value);
            else {
              recordGraphControl(bridge, invocation, 'return', args, part);
              finishGraphStream(invocation);
            }
            return part;
          },
          (error: unknown) => {
            recordGraphControlError(bridge, invocation, 'return', error);
            finishGraphStream(invocation);
            throw error;
          },
        );
      }
      if (key === 'throw' && typeof target.throw === 'function') {
        return (...args: [] | [unknown]) => bridge.streams.run(
          invocation,
          () => Promise.resolve(Reflect.apply(target.throw!, target, args)),
        ).then(
          (part) => {
            if (!part.done) recordValue(bridge, invocation, part.value);
            else {
              recordGraphControl(bridge, invocation, 'throw', args, part);
              finishGraphStream(invocation);
            }
            return part;
          },
          (error: unknown) => {
            recordGraphControlError(bridge, invocation, 'throw', error);
            finishGraphStream(invocation);
            throw error;
          },
        );
      }
      if (key === Symbol.asyncIterator) return () => receiver as AsyncIterator<unknown>;
      const value = Reflect.get(target as object, key, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function recordGraphChunk(bridge: Bridge, invocation: GraphStreamInvocation, chunk: unknown): void {
  if (!bridge.active || !bridge.sink) return;
  recordGraphStream(invocation, (trace, nativeIdentity) => {
    const evidence = snapshotRecord(chunk);
    const compacted = compactLangGraphState(chunk, invocation.stateTracker.seenMessages);
    const value = compacted === undefined ? undefined : snapshotNative(compacted);
    if (value === undefined) return;
    invocation.stateTracker.lastValue = value;
    const receipt = bridge.sink?.record({
      kind: 'state', phase: 'event', name: 'langgraph.graph.stream.chunk', trace,
      ...(nativeIdentity ? { nativeIdentity } : {}),
      native: evidence,
      semantic: {
        type: 'state.transition',
        framework: 'langgraph',
        state_type: 'langgraph.stream.update',
        value,
      },
    });
    if (receipt?.accepted) invocation.stateTracker.lastStateReceipt = receipt;
  });
}

function recordGraphNativeEvent(bridge: Bridge, invocation: GraphStreamInvocation, event: unknown): void {
  if (!bridge.active || !bridge.sink) return;
  recordGraphStream(invocation, (trace, nativeIdentity) => bridge.sink?.record({
    kind: 'state', phase: 'event', name: 'langgraph.native.event', trace,
    ...(nativeIdentity ? { nativeIdentity } : {}),
    native: snapshotRecord({ event }),
    semantic: { type: 'capture.redundant', framework: 'langgraph' },
  }));
}

function recordGraphControl(
  bridge: Bridge,
  invocation: GraphStreamInvocation,
  control: 'return' | 'throw',
  args: unknown[],
  result: IteratorResult<unknown>,
): void {
  if (!bridge.active || !bridge.sink) return;
  recordGraphStream(invocation, (trace, nativeIdentity) => bridge.sink?.record({
    kind: control === 'return' ? 'stream' : 'state', phase: control === 'return' ? 'cancelled' : 'end',
    name: `langgraph.graph.stream.${control}`, trace,
    ...(nativeIdentity ? { nativeIdentity } : {}),
    native: snapshotRecord({ control, arguments: args, result }),
    semantic: { type: control === 'return' ? 'stream.cancelled' : 'state.transition', framework: 'langgraph' },
  }));
}

function recordGraphControlError(
  bridge: Bridge,
  invocation: GraphStreamInvocation,
  control: 'next' | 'return' | 'throw',
  error: unknown,
): void {
  if (!bridge.active || !bridge.sink) return;
  const capturedError = snapshotRecord(error);
  const exactError = semanticError(capturedError);
  recordGraphStream(invocation, (trace, nativeIdentity) => bridge.sink?.record({
    kind: 'error', phase: 'error', name: 'langgraph.graph.stream.error', trace,
    ...(nativeIdentity ? { nativeIdentity } : {}),
    ...(typeof error === 'object' && error !== null ? { errorIdentity: error } : {}),
    native: snapshotRecord({ control, error }),
    semantic: {
      type: 'stream.error',
      framework: 'langgraph',
      ...(exactError ? { error: exactError } : {}),
    },
  }));
}

function recordGraphReaderCancel(
  bridge: Bridge,
  invocation: GraphStreamInvocation,
  args: unknown[],
  result: unknown,
): void {
  if (!bridge.active || !bridge.sink) return;
  recordGraphStream(invocation, (trace, nativeIdentity) => bridge.sink?.record({
    kind: 'stream', phase: 'cancelled', name: 'langgraph.graph.stream.cancel', trace,
    ...(nativeIdentity ? { nativeIdentity } : {}),
    native: snapshotRecord({ control: 'reader.cancel', arguments: args, result }),
    semantic: { type: 'stream.cancelled', framework: 'langgraph' },
  }));
}

function recordGraphStream(
  invocation: GraphStreamInvocation,
  record: (trace: TraceIdentity, nativeIdentity?: string) => void,
): void {
  if (invocation.trace) record(invocation.trace, invocation.nativeIdentity);
  else invocation.pending.push(record);
}

function finishGraphStream(invocation: GraphStreamInvocation): void {
  invocation.consumerTerminal = true;
  const finish = invocation.finish;
  invocation.finish = undefined;
  finish?.();
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return !!value && (typeof value === 'object' || typeof value === 'function')
    && typeof (value as { then?: unknown }).then === 'function';
}

function createCallbackHandler(
  sink: SourceSink,
  runs: Map<string, OpenRun>,
  interrupts: Map<string, InterruptedRun>,
  active: () => boolean,
  currentStream: () => GraphStreamInvocation | undefined,
): CallbackHandler {
  const contextByTrace = new Map<string, WeakMap<object, AdmissionReceipt>>();
  const toolCorrelations = new Map<string, ToolCorrelation[]>();
  const startRun = (
    family: 'chain' | 'model' | 'tool',
    native: Record<string, unknown>,
    runId: string,
    parentRunId: string | undefined,
    name: string,
    callId?: string,
  ) => {
    if (!active()) return;
    const evidence = snapshotRecord(native);
    const metadata = isRecord(native.metadata) ? native.metadata : {};
    const threadId = stringField(metadata, 'thread_id') ?? stringField(metadata, 'conversation_id');
    const parentRun = parentRunId ? runs.get(parentRunId) : undefined;
    let trace = parentRun?.trace;
    let resumeReceipt: AdmissionReceipt | undefined;
    let rootStart: AdmissionReceipt | undefined;
    const priorInterrupt = !parentRunId && family === 'chain' && isResumeCommand(native.inputs) && threadId
      ? interrupts.get(threadId)
      : undefined;
    if (!trace && priorInterrupt) {
      trace = priorInterrupt.trace;
      resumeReceipt = sink.record({
        kind: 'state', phase: 'event', name: 'langgraph.interrupt.resume', trace,
        nativeIdentity: runId, native: evidence,
        ...(priorInterrupt.receipt.accepted ? { parentRecordId: priorInterrupt.receipt.recordId } : {}),
        semantic: {
          type: 'state.resume',
          framework: 'langgraph',
          ...(Object.prototype.hasOwnProperty.call(evidence, 'inputs')
            ? { value: evidence.inputs }
            : {}),
        },
      });
      interrupts.delete(threadId!);
    }
    if (!trace) {
      const opened = sink.openTrace({
        name: family === 'chain' ? name : 'langgraph.run',
        nativeIdentity: runId,
        conversationId: stringField(metadata, 'thread_id') ?? stringField(metadata, 'conversation_id'),
        turnId: stringField(metadata, 'turn_id'),
        turnIndex: numberField(metadata, 'turn_index'),
        previousTurnId: stringField(metadata, 'previous_turn_id'),
        native: evidence,
        semantic: {
          type: 'agent.run',
          framework: 'langgraph',
          name: family === 'chain' ? name : 'langgraph.run',
          ...(Object.prototype.hasOwnProperty.call(evidence, 'inputs')
            ? { input: evidence.inputs }
            : {}),
        },
      });
      if (!opened.accepted) return;
      trace = opened.identity;
      rootStart = opened;
      const invocation = !parentRunId ? currentStream() : undefined;
      if (invocation && !invocation.trace) {
        invocation.trace = trace;
        invocation.nativeIdentity = runId;
        for (const pending of invocation.pending.splice(0)) pending(trace, runId);
      }
      sink.record({
        kind: 'correlation', phase: 'event', name: 'langgraph.run.native', trace,
        nativeIdentity: runId, native: evidence,
        semantic: { type: 'capture.redundant', framework: 'langgraph' },
      });
    }
    const kind = family === 'model'
      ? 'model'
      : family === 'tool'
        ? 'tool'
        : (parentRunId ? 'state' : 'lifecycle');
    const eventName = `langgraph.${family}.${name}`;
    if (family === 'chain' && isResumeCommand(native.inputs) && !priorInterrupt) sink.record({
      kind: 'state', phase: 'event', name: 'langgraph.interrupt.resume', trace,
      nativeIdentity: runId, native: evidence,
      semantic: {
        type: 'state.resume',
        framework: 'langgraph',
        ...(Object.prototype.hasOwnProperty.call(evidence, 'inputs')
          ? { value: evidence.inputs }
          : {}),
      },
    });
    if (!parentRunId && kind === 'lifecycle') {
      const pairIdentity = priorInterrupt?.pairIdentity ?? runId;
      const invocation = currentStream();
      runs.set(runId, {
        trace, name, kind, pairIdentity, rootPairIdentity: pairIdentity,
        stateTracker: invocation?.stateTracker ?? { seenMessages: new WeakMap() },
        ...(rootStart ? { start: rootStart } : {}),
        ...(threadId ? { threadId } : {}),
      });
      return;
    }
    const toolKey = family === 'tool' && callId
      ? toolCorrelationKey(trace, callId)
      : undefined;
    const toolFingerprint = toolKey
      ? exactToolExecutionFingerprint(name, evidence.input)
      : undefined;
    if (toolKey && toolFingerprint) {
      const candidates = toolCorrelations.get(toolKey) ?? [];
      const exact = candidates.find((candidate) => (
        candidate.fingerprint === toolFingerprint
        && candidate.terminalFingerprint === undefined
        && provenToolCorrelationReuse(candidate, parentRunId, runs)
      ));
      if (exact) {
        exact.paused = false;
        exact.runIds.add(runId);
        runs.set(runId, exact.open);
        return;
      }
      if (candidates.length) {
        recordToolCorrelationGap(
          sink,
          trace,
          runId,
          evidence,
          parentRun?.start,
          'LangGraph reused a tool call ID without paused-resume or callback-ancestry proof; both executions were retained as ambiguous.',
        );
      }
    }
    const contextRefs = family === 'model'
      ? recordModelContext(
        sink,
        trace,
        runId,
        native.messages,
        contextByTrace,
        parentRun?.start,
      )
      : [];
    const start = sink.record({
      kind, phase: 'start', name: eventName, trace, nativeIdentity: runId, native: evidence,
      ...(resumeReceipt?.accepted
        ? { parentRecordId: resumeReceipt.recordId }
        : parentRun?.start?.accepted
          ? { parentRecordId: parentRun.start.recordId }
          : {}),
      semantic: startSemantic(family, name, evidence, callId, contextRefs),
    });
    const open: OpenRun = {
      trace, name: eventName, pairIdentity: runId,
      rootPairIdentity: parentRun?.rootPairIdentity ?? runId,
      stateTracker: parentRun?.stateTracker ?? { seenMessages: new WeakMap() },
      start, kind,
      ...(parentRunId ? { parentRunId } : {}),
      ...(threadId ? { threadId } : {}),
      ...(callId ? { callId } : {}),
    };
    if (toolKey && toolFingerprint) {
      const correlation: ToolCorrelation = {
        key: toolKey,
        fingerprint: toolFingerprint,
        open,
        runIds: new Set([runId]),
        paused: false,
      };
      open.toolCorrelation = correlation;
      const candidates = toolCorrelations.get(toolKey) ?? [];
      candidates.push(correlation);
      toolCorrelations.set(toolKey, candidates);
    }
    runs.set(runId, open);
  };

  const endRun = (runId: string, parentRunId: string | undefined, native: Record<string, unknown>, failed: boolean) => {
    if (!active()) return;
    const open = runs.get(runId);
    if (!open) return;
    const pausedRoot = !parentRunId && open.kind === 'lifecycle'
      && !!open.threadId && interrupts.has(open.threadId);
    if (pausedRoot) {
      runs.delete(runId);
      return;
    }
    const evidence = snapshotRecord(native);
    const correlation = open.toolCorrelation;
    if (correlation) {
      const fingerprint = exactToolTerminalFingerprint(failed, evidence);
      if (correlation.terminalFingerprint !== undefined) {
        if (correlation.terminalFingerprint !== fingerprint) {
          recordToolCorrelationGap(
            sink,
            open.trace,
            runId,
            evidence,
            open.start,
            'Exactly correlated LangGraph tool callbacks ended with divergent results; the first result was retained as authoritative.',
          );
        }
        correlation.runIds.delete(runId);
        runs.delete(runId);
        cleanupToolCorrelation(toolCorrelations, correlation);
        return;
      }
      correlation.terminalFingerprint = fingerprint;
      correlation.paused = false;
    }
    const finish = () => {
      let delivery = open.stateTracker.lastStateReceipt;
      if (!parentRunId && open.kind === 'lifecycle' && !failed
        && Object.prototype.hasOwnProperty.call(native, 'outputs')) {
        const compacted = compactLangGraphState(
          native.outputs,
          open.stateTracker.seenMessages,
        );
        const finalValue = compacted === undefined ? undefined : snapshotNative(compacted);
        if (finalValue !== undefined
          && JSON.stringify(finalValue) !== JSON.stringify(open.stateTracker.lastValue)) {
          const receipt = sink.record({
            kind: 'state',
            phase: 'event',
            name: 'langgraph.run.final_state',
            trace: open.trace,
            nativeIdentity: runId,
            native: evidence,
            ...(open.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
            semantic: {
              type: 'state.transition',
              framework: 'langgraph',
              state_type: 'langgraph.final_state',
              value: finalValue,
            },
          });
          if (receipt.accepted) {
            open.stateTracker.lastStateReceipt = receipt;
            delivery = receipt;
          }
        }
      }
      const finalMessageContent = rootFinalMessageContent(native.outputs);
      if (
        open.stateTracker.lastModelDelivery
        && finalMessageContent !== undefined
        && JSON.stringify(finalMessageContent)
          === JSON.stringify(open.stateTracker.lastModelDelivery.content)
      ) {
        delivery = open.stateTracker.lastModelDelivery.receipt;
      }
      if (!parentRunId && !failed) sink.record({
        kind: 'stream', phase: 'end', name: 'langgraph.graph.terminal', trace: open.trace,
        nativeIdentity: runId, native: evidence,
        semantic: { type: 'capture.redundant', framework: 'langgraph' },
      });
      const semantic = terminalSemantic(
        open,
        failed,
        evidence,
        native,
        !failed && delivery?.accepted ? delivery.recordId : undefined,
      );
      const receipt = sink.record({
        kind: open.kind, phase: failed ? 'error' : 'end', name: open.name, trace: open.trace,
        nativeIdentity: open.pairIdentity, native: evidence,
        ...(open.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
        semantic,
      });
      if (receipt.accepted && semantic.type === 'state.transition') {
        open.stateTracker.lastStateReceipt = receipt;
      }
      if (semantic.type === 'model.response') {
        const unavailableReasoning = langGraphUnavailableReasoning(evidence.output);
        if (unavailableReasoning > 0) sink.record({
          kind: 'unknown',
          phase: 'gap',
          name: 'langgraph.model.reasoning.unavailable',
          trace: open.trace,
          native: { run_id: runId, unavailable_reasoning_blocks: unavailableReasoning },
          ...(receipt.accepted ? { parentRecordId: receipt.recordId } : {}),
          semantic: {
            type: 'capture.gap',
            framework: 'langgraph',
            reason: 'reasoning_unavailable',
            count: unavailableReasoning,
            detail: 'LangGraph exposed only encrypted reasoning detail blocks.',
          },
        });
      }
      if (
        receipt.accepted
        && semantic.type === 'model.response'
        && Object.prototype.hasOwnProperty.call(semantic, 'content')
      ) {
        open.stateTracker.lastModelDelivery = {
          content: semantic.content,
          receipt,
        };
      }
      if (!parentRunId && open.kind === 'lifecycle') {
        contextByTrace.delete(open.trace.traceId);
      }
    };
    const stream = !parentRunId && open.kind === 'lifecycle' ? currentStream() : undefined;
    if (stream?.consumerTerminal) finish();
    else if (stream) stream.finish = finish;
    else finish();
    runs.delete(runId);
    if (correlation) {
      correlation.runIds.delete(runId);
      cleanupToolCorrelation(toolCorrelations, correlation);
    }
  };

  return {
    name: 'semantic-layer-langgraph',
    awaitHandlers: true,
    // Core 1.2.2's declaration lists a different order. This is the manager.js runtime order
    // used by both pinned LangGraph versions.
    handleChainStart(chain: unknown, inputs: unknown, runId: string, parentRunId?: string, tags?: string[], metadata?: Record<string, unknown>, runType?: string, runName?: string) {
      const name = runName ?? serializedName(chain) ?? runType ?? 'chain';
      startRun('chain', { chain, inputs, run_type: runType, tags, metadata, run_name: runName, parent_run_id: parentRunId }, runId, parentRunId, name);
    },
    handleChainEnd(outputs: unknown, runId: string, parentRunId?: string, tags?: string[]) {
      endRun(runId, parentRunId, { outputs, tags, parent_run_id: parentRunId }, false);
    },
    handleChainError(error: unknown, runId: string, parentRunId?: string, tags?: string[], kwargs?: unknown) {
      if (isGraphInterrupt(error)) {
        const open = runs.get(runId);
        if (!active() || !open) return;
        const interrupt = sink.record({
          kind: 'state', phase: 'event', name: 'langgraph.interrupt', trace: open.trace,
          nativeIdentity: runId, native: snapshotRecord({ error, tags, kwargs, parent_run_id: parentRunId }),
          ...(open.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
          semantic: {
            type: 'state.interrupt',
            framework: 'langgraph',
            value: snapshotNative(error),
          },
        });
        if (open.threadId) interrupts.set(open.threadId, {
          trace: open.trace, receipt: interrupt, pairIdentity: open.rootPairIdentity,
        });
        sink.record({
          kind: 'state', phase: 'cancelled', name: 'langgraph.interrupt.pause', trace: open.trace,
          nativeIdentity: runId, native: snapshotRecord({ error, tags, kwargs, parent_run_id: parentRunId }),
          ...(open.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
          semantic: { type: 'capture.redundant', framework: 'langgraph' },
        });
        runs.delete(runId);
        return;
      }
      endRun(runId, parentRunId, { error, tags, kwargs, parent_run_id: parentRunId }, true);
    },
    handleLLMStart(llm: unknown, prompts: unknown, runId: string, parentRunId?: string, extra?: unknown, tags?: string[], metadata?: Record<string, unknown>, runName?: string) {
      startRun('model', { llm, prompts, extra, tags, metadata, run_name: runName, parent_run_id: parentRunId }, runId, parentRunId, runName ?? serializedName(llm) ?? 'llm');
    },
    handleChatModelStart(llm: unknown, messages: unknown, runId: string, parentRunId?: string, extra?: unknown, tags?: string[], metadata?: Record<string, unknown>, runName?: string) {
      startRun('model', { llm, messages, extra, tags, metadata, run_name: runName, parent_run_id: parentRunId }, runId, parentRunId, runName ?? serializedName(llm) ?? 'chat_model');
    },
    handleLLMNewToken(token: string, indices: unknown, runId: string, parentRunId?: string, tags?: string[], fields?: unknown) {
      const open = runs.get(runId);
      if (!active() || !open) return;
      sink.record({
        kind: 'stream', phase: 'event', name: 'langgraph.model.stream.delta', trace: open.trace,
        nativeIdentity: runId, native: snapshotRecord({ token, indices, fields, tags, parent_run_id: parentRunId }),
        semantic: { type: 'capture.redundant', framework: 'langgraph' },
      });
    },
    handleLLMEnd(output: unknown, runId: string, parentRunId?: string, tags?: string[], extra?: unknown) {
      const open = runs.get(runId);
      const evidence = snapshotRecord({ output, extra, tags, parent_run_id: parentRunId });
      const usage = usageFromOutput(evidence.output);
      if (active() && open) sink.record({
        kind: 'stream', phase: 'end', name: 'langgraph.model.stream.terminal', trace: open.trace,
        nativeIdentity: runId, native: evidence,
        semantic: { type: 'capture.redundant', framework: 'langgraph' },
      });
      if (active() && open && hasToolProposal(evidence.output)) {
        const proposal = exactGenerationToolProposal(evidence.output);
        sink.record({
          kind: 'tool', phase: 'event', name: 'langgraph.tool.proposal', trace: open.trace,
          nativeIdentity: runId, native: evidence,
          ...(open.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
          semantic: {
            type: 'tool.proposal', framework: 'langgraph',
            ...(proposal ? {
              call_id: proposal.callId,
              name: proposal.name,
              input: proposal.input,
            } : {}),
          },
        });
      }
      if (active() && open && usage) sink.record({
        kind: 'model', phase: 'event', name: 'langgraph.model.usage', trace: open.trace,
        nativeIdentity: runId,
        native: snapshotRecord({ usage, ...evidence }),
        semantic: { type: 'capture.redundant', framework: 'langgraph' },
      });
      endRun(runId, parentRunId, evidence, false);
    },
    handleLLMError(error: unknown, runId: string, parentRunId?: string, tags?: string[], extra?: unknown) {
      endRun(runId, parentRunId, { error, extra, tags, parent_run_id: parentRunId }, true);
    },
    handleToolStart(tool: unknown, input: unknown, runId: string, parentRunId?: string, tags?: string[], metadata?: Record<string, unknown>, runName?: string, toolCallId?: string) {
      startRun(
        'tool',
        {
          tool_definition: tool,
          input,
          tags,
          metadata,
          run_name: runName,
          tool_call_id: toolCallId,
          parent_run_id: parentRunId,
        },
        runId,
        parentRunId,
        runName ?? serializedName(tool) ?? 'tool',
        exactIdentifier(toolCallId),
      );
    },
    handleToolEvent(chunk: unknown, runId: string, parentRunId?: string, tags?: string[]) {
      const open = runs.get(runId);
      if (!active() || !open) return;
      sink.record({
        kind: 'stream', phase: 'event', name: 'langgraph.tool.stream.delta', trace: open.trace,
        nativeIdentity: runId, native: snapshotRecord({ chunk, tags, parent_run_id: parentRunId }),
        semantic: { type: 'capture.redundant', framework: 'langgraph' },
      });
    },
    handleToolEnd(output: unknown, runId: string, parentRunId?: string, tags?: string[]) {
      endRun(runId, parentRunId, { output, tags, parent_run_id: parentRunId }, false);
    },
    handleToolError(error: unknown, runId: string, parentRunId?: string, tags?: string[]) {
      if (isGraphInterrupt(error)) {
        const open = runs.get(runId);
        if (!active() || !open) return;
        if (open.toolCorrelation) {
          open.toolCorrelation.paused = true;
          open.toolCorrelation.runIds.delete(runId);
        }
        runs.delete(runId);
        return;
      }
      endRun(runId, parentRunId, { error, tags, parent_run_id: parentRunId }, true);
    },
    handleAgentAction(action: unknown, runId: string, parentRunId?: string, tags?: string[]) {
      const open = runs.get(runId) ?? (parentRunId ? runs.get(parentRunId) : undefined);
      if (!active() || !open) return;
      sink.record({
        kind: 'tool', phase: 'event', name: 'langgraph.tool.proposal', trace: open.trace,
        nativeIdentity: runId, native: snapshotRecord({ action, tags, parent_run_id: parentRunId }),
        semantic: { type: 'tool.proposal', framework: 'langgraph' },
      });
    },
    handleAgentEnd(action: unknown, runId: string, parentRunId?: string, tags?: string[]) {
      const open = runs.get(runId) ?? (parentRunId ? runs.get(parentRunId) : undefined);
      if (!active() || !open) return;
      sink.record({
        kind: 'stream', phase: 'end', name: 'langgraph.agent.stream.terminal', trace: open.trace,
        nativeIdentity: runId, native: snapshotRecord({ action, tags, parent_run_id: parentRunId }),
        semantic: { type: 'capture.redundant', framework: 'langgraph' },
      });
    },
  } as CallbackHandler;
}

function injectCallbacks(config: RunnableConfig | undefined, handler: CallbackHandler): RunnableConfig {
  const next = { ...(config ?? {}) };
  const callbacks = config?.callbacks;
  if (callbacks === undefined) next.callbacks = [handler];
  else if (Array.isArray(callbacks)) next.callbacks = [...callbacks, handler];
  else if (isCallbackManager(callbacks)) {
    const copy = callbacks.copy();
    copy.addHandler(handler, true);
    next.callbacks = copy;
  }
  else throw new TypeError('LangGraph RunnableConfig.callbacks must be an array or CallbackManager');
  return next;
}

function requireBound(subject: RunnableSubject | undefined, bridge: Bridge): RunnableSubject {
  if (!subject || !bridge.installed) throw new Error('LangGraph adapter must be installed before invocation');
  return subject;
}

function startSemantic(
  family: 'chain' | 'model' | 'tool',
  name: string,
  evidence: Record<string, unknown>,
  callId?: string,
  contextRefs: string[] = [],
): Record<string, unknown> {
  if (family === 'model') {
    return {
      type: 'model.request',
      framework: 'langgraph',
      model: name,
      context_refs: contextRefs,
    };
  }
  if (family === 'tool') {
    return {
      type: 'tool.execution',
      framework: 'langgraph',
      name,
      input: exactToolInput(evidence.input),
      ...(callId ? { call_id: callId } : {}),
    };
  }
  return {
    type: 'capture.redundant',
    framework: 'langgraph',
  };
}

function recordModelContext(
  sink: SourceSink,
  trace: TraceIdentity,
  runId: string,
  value: unknown,
  contextByTrace: Map<string, WeakMap<object, AdmissionReceipt>>,
  parent?: AdmissionReceipt,
): string[] {
  if (!Array.isArray(value) || value.length !== 1 || !Array.isArray(value[0])) return [];
  const priorContext = contextByTrace.get(trace.traceId);
  const known = priorContext ?? new WeakMap<object, AdmissionReceipt>();
  if (!priorContext) contextByTrace.set(trace.traceId, known);
  const refs: string[] = [];
  value[0].forEach((message, index) => {
    if (!isRecord(message)) return;
    const prior = known.get(message);
    if (prior?.accepted) {
      refs.push(prior.recordId);
      return;
    }
    const evidence = snapshotRecord(message);
    if (!Object.prototype.hasOwnProperty.call(evidence, 'content')) return;
    const role = langGraphMessageRole(evidence.type);
    if (!role) return;
    const receipt = sink.record({
      kind: 'model',
      phase: 'event',
      name: 'langgraph.model.context',
      trace,
      nativeIdentity: `${runId}:context:${index}`,
      native: { message: evidence },
      ...(parent?.accepted ? { parentRecordId: parent.recordId } : {}),
      semantic: {
        type: 'message',
        framework: 'langgraph',
        origin: 'context',
        role,
        content: evidence.content,
        ...(exactIdentifier(evidence.name) ? { name: exactIdentifier(evidence.name)! } : {}),
        ...(exactIdentifier(evidence.tool_call_id)
          ? { call_id: exactIdentifier(evidence.tool_call_id)! }
          : {}),
      },
    });
    if (receipt.accepted) {
      known.set(message, receipt);
      refs.push(receipt.recordId);
    }
  });
  return refs;
}

function langGraphMessageRole(
  value: unknown,
): 'system' | 'developer' | 'user' | 'assistant' | 'tool' | undefined {
  if (value === 'human') return 'user';
  if (value === 'ai') return 'assistant';
  return value === 'system' || value === 'developer' || value === 'tool'
    ? value
    : undefined;
}

function rootFinalMessageContent(value: unknown): unknown {
  if (!isRecord(value) || !Array.isArray(value.messages) || value.messages.length === 0) {
    return undefined;
  }
  const message = value.messages.at(-1);
  if (!isRecord(message) || !Object.prototype.hasOwnProperty.call(message, 'content')) {
    return undefined;
  }
  return snapshotNative(message.content);
}

function terminalSemantic(
  open: OpenRun,
  failed: boolean,
  evidence: Record<string, unknown>,
  native: Record<string, unknown>,
  outputRef?: string,
): Record<string, unknown> {
  if (open.kind === 'lifecycle') {
    const error = semanticError(evidence.error);
    return {
      type: failed ? 'agent.error' : 'agent.run',
      framework: 'langgraph',
      status: failed ? 'failed' : 'succeeded',
      ...(!failed && outputRef ? { output_ref: outputRef } : {}),
      ...(error ? { error } : {}),
    };
  }
  if (open.kind === 'model') {
    const error = semanticError(evidence.error);
    if (failed) {
      return {
        type: 'model.error',
        framework: 'langgraph',
        ...(error ? { error } : {}),
      };
    }
    const content = modelResponseContent(evidence.output);
    const reasoning = modelResponseReasoning(evidence.output);
    const usage = usageFromOutput(evidence.output);
    return {
      type: 'model.response',
      framework: 'langgraph',
      status: 'completed',
      ...(content !== undefined ? { content } : {}),
      ...(reasoning.length ? { reasoning } : {}),
      ...(usage ? { usage } : {}),
    };
  }
  if (open.kind === 'tool') {
    const error = semanticError(evidence.error);
    return {
      type: failed ? 'tool.error' : 'tool.result',
      framework: 'langgraph',
      status: failed ? 'failed' : 'succeeded',
      ...(open.callId ? { call_id: open.callId } : {}),
      ...(Object.prototype.hasOwnProperty.call(evidence, 'output')
        ? { output: toolResultOutput(evidence.output) }
        : {}),
      ...(error ? { error } : {}),
    };
  }
  if (failed) {
    const error = semanticError(evidence.error);
    return {
      type: 'agent.error',
      framework: 'langgraph',
      ...(error ? { error } : {}),
    };
  }
  const value = Object.prototype.hasOwnProperty.call(native, 'outputs')
    ? compactLangGraphState(native.outputs, open.stateTracker.seenMessages)
    : Object.prototype.hasOwnProperty.call(native, 'error')
      ? compactLangGraphState(native.error, open.stateTracker.seenMessages)
      : undefined;
  if (value === undefined) {
    return { type: 'capture.redundant', framework: 'langgraph' };
  }
  open.stateTracker.lastValue = snapshotNative(value);
  return {
    type: 'state.transition',
    framework: 'langgraph',
    state_type: 'langgraph.step.end',
    value,
  };
}

function compactLangGraphState(value: unknown, seenMessages: WeakMap<object, string>): unknown {
  if (Array.isArray(value)) {
    const compacted = value
      .map((item) => compactLangGraphState(item, seenMessages))
      .filter((item) => item !== undefined);
    return compacted.length ? compacted : undefined;
  }
  if (!isRecord(value)) return value;
  const compacted: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    if (key === 'lc_serializable') continue;
    if (key === 'messages') {
      const messages = Array.isArray(item) ? item : [item];
      const retained = messages.flatMap((message) => {
        const compactedMessage = compactLangGraphMessage(message);
        if (!message || typeof message !== 'object') return [compactedMessage];
        const fingerprint = JSON.stringify(compactedMessage);
        if (seenMessages.get(message) === fingerprint) return [];
        seenMessages.set(message, fingerprint);
        return [compactedMessage];
      });
      if (retained.length) compacted[key] = Array.isArray(item) ? retained : retained[0];
      continue;
    }
    compacted[key] = item;
  }
  return Object.keys(compacted).length ? compacted : undefined;
}

function compactLangGraphMessage(value: unknown): unknown {
  if (!isRecord(value)) return value;
  const snapshot = snapshotRecord(value);
  const message: Record<string, unknown> = {};
  for (const key of [
    'type',
    'role',
    'content',
    'name',
    'tool_call_id',
    'tool_calls',
    'additional_kwargs',
    'response_metadata',
    'artifact',
  ]) {
    if (!Object.prototype.hasOwnProperty.call(snapshot, key)) continue;
    const retained = withoutSnapshotOmissions(snapshot[key]);
    if (retained !== undefined) message[key] = retained;
  }
  return Object.keys(message).length ? message : { type: 'unknown_message' };
}

function withoutSnapshotOmissions(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(withoutSnapshotOmissions).filter((item) => item !== undefined);
  }
  if (!isRecord(value)) return value;
  if ('$semantic_layer_omitted' in value || '$semantic_layer_cycle' in value) return undefined;
  const retained = Object.fromEntries(
    Object.entries(value)
      .map(([key, item]) => [key, withoutSnapshotOmissions(item)] as const)
      .filter((entry): entry is readonly [string, unknown] => entry[1] !== undefined),
  );
  return retained;
}

function exactToolInput(value: unknown): unknown {
  if (typeof value !== 'string') return value ?? null;
  try {
    return JSON.parse(value) as unknown;
  } catch {
    return value;
  }
}

function toolCorrelationKey(trace: TraceIdentity, callId: string): string {
  return `${trace.traceId}\0${callId}`;
}

function exactToolExecutionFingerprint(name: string, input: unknown): string {
  return JSON.stringify({ name, input: exactToolInput(input) });
}

function provenToolCorrelationReuse(
  correlation: ToolCorrelation,
  parentRunId: string | undefined,
  runs: Map<string, OpenRun>,
): boolean {
  if (correlation.paused && correlation.runIds.size === 0) return true;
  let ancestor = parentRunId;
  const seen = new Set<string>();
  while (ancestor && !seen.has(ancestor)) {
    if (correlation.runIds.has(ancestor)) return true;
    seen.add(ancestor);
    ancestor = runs.get(ancestor)?.parentRunId;
  }
  return false;
}

function exactToolTerminalFingerprint(
  failed: boolean,
  evidence: Record<string, unknown>,
): string {
  return JSON.stringify({
    failed,
    ...(Object.prototype.hasOwnProperty.call(evidence, 'output')
      ? { output: toolResultOutput(evidence.output) }
      : {}),
    ...(Object.prototype.hasOwnProperty.call(evidence, 'error')
      ? { error: evidence.error }
      : {}),
  });
}

function recordToolCorrelationGap(
  sink: SourceSink,
  trace: TraceIdentity,
  runId: string,
  native: Record<string, unknown>,
  parent: AdmissionReceipt | undefined,
  detail: string,
): void {
  sink.record({
    kind: 'unknown',
    phase: 'gap',
    name: 'langgraph.tool.correlation.ambiguous',
    trace,
    nativeIdentity: runId,
    native,
    ...(parent?.accepted ? { parentRecordId: parent.recordId } : {}),
    semantic: {
      type: 'capture.gap',
      framework: 'langgraph',
      reason: 'ambiguous_tool_correlation',
      count: 1,
      detail,
    },
  });
}

function cleanupToolCorrelation(
  correlations: Map<string, ToolCorrelation[]>,
  correlation: ToolCorrelation,
): void {
  if (correlation.paused || correlation.runIds.size > 0) return;
  const candidates = correlations.get(correlation.key);
  if (!candidates) return;
  const retained = candidates.filter((candidate) => candidate !== correlation);
  if (retained.length) correlations.set(correlation.key, retained);
  else correlations.delete(correlation.key);
}

function toolResultOutput(value: unknown): unknown {
  return isRecord(value) && Object.prototype.hasOwnProperty.call(value, 'content')
    ? value.content
    : value;
}

function serializedName(value: unknown): string | undefined {
  if (!isRecord(value)) return undefined;
  if (typeof value.name === 'string') return value.name;
  const id = value.id;
  return Array.isArray(id) && typeof id.at(-1) === 'string' ? id.at(-1) as string : undefined;
}

function semanticError(value: unknown): Record<string, unknown> | undefined {
  if (!isRecord(value) || typeof value.message !== 'string') return undefined;
  return {
    type: 'langgraph_error',
    message: value.message,
    recoverable: false,
    ...(typeof value.code === 'string' ? { code: value.code } : {}),
    ...(Object.prototype.hasOwnProperty.call(value, 'detail')
      ? { details: value.detail }
      : {}),
  };
}

function modelResponseContent(value: unknown): unknown {
  if (!isRecord(value) || !Array.isArray(value.generations)) return undefined;
  const contents: unknown[] = [];
  for (const batch of value.generations) {
    if (!Array.isArray(batch)) return undefined;
    for (const generation of batch) {
      if (!isRecord(generation)) return undefined;
      const message = isRecord(generation.message) ? generation.message : undefined;
      if (message && Object.prototype.hasOwnProperty.call(message, 'content')) {
        const visible = visibleLangGraphContent(message.content);
        if (visible !== undefined) contents.push(visible);
        else if (Object.prototype.hasOwnProperty.call(generation, 'text')) contents.push(generation.text);
      } else if (Object.prototype.hasOwnProperty.call(generation, 'text')) {
        contents.push(generation.text);
      }
    }
  }
  if (contents.length === 0) return undefined;
  return contents.length === 1 ? contents[0] : contents;
}

function visibleLangGraphContent(value: unknown): unknown {
  if (!Array.isArray(value)) return value;
  const visible = value.filter((block) => !(
    isRecord(block)
    && ['reasoning', 'reasoning_summary', 'summary', 'thinking', 'redacted-reasoning']
      .includes(String(block.type))
  ));
  return visible.length ? visible : undefined;
}

function modelResponseReasoning(
  value: unknown,
): Array<{ type: 'text' | 'summary'; text: string }> {
  if (!isRecord(value) || !Array.isArray(value.generations)) return [];
  const blocks: Array<{ type: 'text' | 'summary'; text: string }> = [];
  const add = (type: 'text' | 'summary', text: unknown): void => {
    if (typeof text === 'string' && text.length) blocks.push({ type, text });
  };
  const readBlocks = (values: unknown[]): Array<{ type: 'text' | 'summary'; text: string }> => {
    const retained: Array<{ type: 'text' | 'summary'; text: string }> = [];
    const retain = (type: 'text' | 'summary', text: unknown): void => {
      if (typeof text === 'string' && text.length) retained.push({ type, text });
    };
    for (const block of values) {
      if (!isRecord(block)) continue;
      if (block.type === 'reasoning_summary' || block.type === 'summary') {
        retain('summary', block.text ?? block.summary);
      } else if (block.type === 'reasoning' || block.type === 'thinking') {
        retain('text', block.text ?? block.reasoning ?? block.thinking);
      }
    }
    return retained;
  };
  for (const batch of value.generations) {
    if (!Array.isArray(batch)) continue;
    for (const generation of batch) {
      if (!isRecord(generation) || !isRecord(generation.message)) continue;
      const message = generation.message;
      const additional = isRecord(message.additional_kwargs)
        ? message.additional_kwargs
        : isRecord(message.additionalKwargs)
          ? message.additionalKwargs
          : undefined;
      const contentBlocks = Array.isArray(message.content_blocks)
        ? message.content_blocks
        : Array.isArray(message.contentBlocks)
          ? message.contentBlocks
          : [];
      const details = Array.isArray(additional?.reasoning_details)
        ? additional.reasoning_details
        : [];
      const normalizedDetails: Array<{ type: 'text' | 'summary'; text: string }> = [];
      for (const detail of details) {
        if (!isRecord(detail)) continue;
        const type = String(detail.type ?? '');
        if (type.includes('summary')) {
          const text = detail.summary ?? detail.text;
          if (typeof text === 'string' && text.length) {
            normalizedDetails.push({ type: 'summary', text });
          }
        } else if (type.includes('text') && typeof detail.text === 'string' && detail.text.length) {
          normalizedDetails.push({ type: 'text', text: detail.text });
        }
      }
      const nativeReasoning = isRecord(additional?.reasoning) ? additional.reasoning : undefined;
      const nativeSummary = nativeReasoning && Array.isArray(nativeReasoning.summary)
        ? nativeReasoning.summary.flatMap((part) => (
            isRecord(part) && typeof part.text === 'string' && part.text.length
              ? [{ type: 'summary' as const, text: part.text }]
              : []
          ))
        : [];
      const normalized = normalizedDetails.length
        ? normalizedDetails
        : nativeSummary.length ? nativeSummary : readBlocks(contentBlocks);
      if (normalized.length) blocks.push(...normalized);
      else if (Array.isArray(message.content)) {
        const contentReasoning = readBlocks(message.content);
        if (contentReasoning.length) blocks.push(...contentReasoning);
        else add('text', additional?.reasoning_content ?? additional?.reasoning);
      } else {
        add('text', additional?.reasoning_content ?? additional?.reasoning);
      }
    }
  }
  return blocks;
}

function langGraphUnavailableReasoning(value: unknown): number {
  if (!isRecord(value) || !Array.isArray(value.generations)) return 0;
  let unavailable = 0;
  for (const batch of value.generations) {
    if (!Array.isArray(batch)) continue;
    for (const generation of batch) {
      if (!isRecord(generation) || !isRecord(generation.message)) continue;
      const additional = isRecord(generation.message.additional_kwargs)
        ? generation.message.additional_kwargs
        : isRecord(generation.message.additionalKwargs)
          ? generation.message.additionalKwargs
          : undefined;
      if (!Array.isArray(additional?.reasoning_details)) continue;
      unavailable += additional.reasoning_details.filter((detail) => (
        isRecord(detail) && String(detail.type ?? '').includes('encrypted')
      )).length;
    }
  }
  return unavailable;
}

function usageFromOutput(value: unknown): Record<string, number> | undefined {
  if (!isRecord(value)) return undefined;
  const direct = numericUsage(value.usage) ?? numericUsage(value.tokenUsage);
  if (direct) return direct;
  const llmOutput = isRecord(value.llmOutput) ? value.llmOutput : {};
  const tokenUsage = numericUsage(llmOutput.tokenUsage);
  if (tokenUsage) return tokenUsage;
  return nestedUsage(value.generations);
}

function nestedUsage(value: unknown, seen = new WeakSet<object>()): Record<string, number> | undefined {
  if (!value || typeof value !== 'object' || seen.has(value)) return undefined;
  seen.add(value);
  if (Array.isArray(value)) {
    for (const child of value) {
      const usage = nestedUsage(child, seen);
      if (usage) return usage;
    }
    return undefined;
  }
  const record = value as Record<string, unknown>;
  const usage = numericUsage(record.usage_metadata) ?? numericUsage(record.usageMetadata);
  if (usage) return usage;
  for (const child of Object.values(record)) {
    const nested = nestedUsage(child, seen);
    if (nested) return nested;
  }
  return undefined;
}

function numericUsage(value: unknown): Record<string, number> | undefined {
  if (!isRecord(value)) return undefined;
  const input = numberField(value, 'input_tokens') ?? numberField(value, 'promptTokens')
    ?? numberField(value, 'inputTokens');
  const output = numberField(value, 'output_tokens') ?? numberField(value, 'completionTokens')
    ?? numberField(value, 'outputTokens');
  const total = numberField(value, 'total_tokens') ?? numberField(value, 'totalTokens');
  if (input === undefined || output === undefined) return undefined;
  return { input_tokens: input, output_tokens: output, total_tokens: total ?? input + output };
}

function exactGenerationToolProposal(value: unknown): {
  callId: string;
  name: string;
  input: unknown;
} | undefined {
  if (!isRecord(value) || !Array.isArray(value.generations)) return undefined;
  const proposals: Array<{ callId: string; name: string; input: unknown }> = [];
  for (const batch of value.generations) {
    if (!Array.isArray(batch)) return undefined;
    for (const generation of batch) {
      if (!isRecord(generation)) return undefined;
      const message = generation.message;
      if (!isRecord(message)) continue;
      const aliases = ['tool_calls', 'toolCalls']
        .filter((key) => Object.prototype.hasOwnProperty.call(message, key));
      if (aliases.length === 0) continue;
      let exact: Array<{ callId: string; name: string; input: unknown }> | undefined;
      for (const alias of aliases) {
        const observed = exactToolProposalArray(message[alias]);
        if (!observed || (exact && !sameToolProposals(exact, observed))) return undefined;
        exact = observed;
      }
      proposals.push(...(exact ?? []));
    }
  }
  return proposals.length === 1 ? proposals[0] : undefined;
}

function exactToolProposalArray(
  value: unknown,
): Array<{ callId: string; name: string; input: unknown }> | undefined {
  if (!Array.isArray(value)) return undefined;
  const proposals: Array<{ callId: string; name: string; input: unknown }> = [];
  for (const call of value) {
    if (!isRecord(call)) return undefined;
    const aliases = ['id', 'toolCallId', 'tool_call_id', 'callId', 'call_id']
      .filter((key) => Object.prototype.hasOwnProperty.call(call, key));
    if (aliases.length === 0) return undefined;
    const observed = aliases.map((key) => exactIdentifier(call[key]));
    if (observed.some((identity) => !identity)) return undefined;
    const distinct = new Set(observed);
    if (distinct.size !== 1) return undefined;
    const name = exactIdentifier(call.name);
    const hasArgs = Object.prototype.hasOwnProperty.call(call, 'args');
    const hasArguments = Object.prototype.hasOwnProperty.call(call, 'arguments');
    if (!name || (!hasArgs && !hasArguments)) return undefined;
    const input = hasArgs ? call.args : call.arguments;
    if (hasArgs && hasArguments && JSON.stringify(call.args) !== JSON.stringify(call.arguments)) {
      return undefined;
    }
    proposals.push({ callId: observed[0]!, name, input });
  }
  return proposals;
}

function sameToolProposals(
  left: ReadonlyArray<{ callId: string; name: string; input: unknown }>,
  right: ReadonlyArray<{ callId: string; name: string; input: unknown }>,
): boolean {
  return left.length === right.length && left.every((proposal, index) => (
    proposal.callId === right[index]?.callId
    && proposal.name === right[index]?.name
    && JSON.stringify(proposal.input) === JSON.stringify(right[index]?.input)
  ));
}

function exactIdentifier(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 && value.trim() === value
    ? value
    : undefined;
}

function hasToolProposal(value: unknown, seen = new WeakSet<object>()): boolean {
  if (!value || typeof value !== 'object' || seen.has(value)) return false;
  seen.add(value);
  if (Array.isArray(value)) return value.some((item) => hasToolProposal(item, seen));
  const record = value as Record<string, unknown>;
  return ['tool_calls', 'toolCalls', 'tool_call_chunks'].some((key) => (
    Array.isArray(record[key]) && record[key].length > 0
  ))
    || Object.values(record).some((item) => hasToolProposal(item, seen));
}

function isGraphInterrupt(value: unknown): boolean {
  return isRecord(value) && (value.name === 'GraphInterrupt' || Array.isArray(value.interrupts));
}

function isResumeCommand(value: unknown): boolean {
  return isRecord(value) && value.lg_name === 'Command' && value.resume !== undefined;
}

function isCallbackManager(value: unknown): value is CallbackManager {
  return isRecord(value) && typeof value.copy === 'function' && typeof value.addHandler === 'function';
}

function isRunnableSubject(value: object): value is RunnableSubject {
  const subject = value as Partial<RunnableSubject>;
  return typeof subject.invoke === 'function' && typeof subject.stream === 'function';
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}

function stringField(value: Record<string, unknown>, key: string): string | undefined {
  return typeof value[key] === 'string' ? value[key] : undefined;
}

function numberField(value: Record<string, unknown>, key: string): number | undefined {
  return typeof value[key] === 'number' && Number.isInteger(value[key]) ? value[key] : undefined;
}
