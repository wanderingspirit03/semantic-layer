import { AsyncLocalStorage } from 'node:async_hooks';
import type {
  AcceptedReceipt,
  AdmissionReceipt,
  CaptureSource,
  SourceSink,
  TraceIdentity,
} from '../v1/types.js';
import { trustOfficialSource } from '../v1/source-ownership.js';
import { snapshotNative } from './native-snapshot.js';

type AgentsAdapterOptions = { version?: string };
const AGENT_RUN_COVERAGE = Object.freeze({ operation: 'agent-run', domain: 'openai-agents.trace' });
const OPENAI_RESPONSE_COVERAGE = Object.freeze({
  operation: 'model-response', domain: 'openai.response',
});
const QUALIFIED_OPENAI_AGENTS_VERSIONS = new Set(['0.13.2', '0.13.1']);
type AgentsSubject = {
  addTraceProcessor(processor: AgentsProcessor): void;
  run(...args: unknown[]): Promise<unknown>;
  StreamedRunResult?: abstract new (...args: never[]) => object;
};
type NativeTrace = {
  traceId: string;
  name?: string;
  groupId?: string | null;
  metadata?: unknown;
};
type NativeSpan = {
  traceId: string;
  spanId: string;
  parentId?: string | null;
  spanData: Record<string, unknown>;
  error?: unknown;
  startedAt?: string | null;
  endedAt?: string | null;
};
type AgentsProcessor = {
  onTraceStart(trace: NativeTrace): void | Promise<void>;
  onTraceEnd(trace: NativeTrace): void | Promise<void>;
  onSpanStart(span: NativeSpan): void | Promise<void>;
  onSpanEnd(span: NativeSpan): void | Promise<void>;
  forceFlush(): void | Promise<void>;
  shutdown(): void | Promise<void>;
};
type OpenTrace = {
  identity: TraceIdentity;
  name: string;
  invocation?: Invocation;
  contextRecords: Map<string, string[]>;
};
type OpenSpan = {
  trace: TraceIdentity;
  name: string;
  type: string;
  start: AdmissionReceipt;
};
type FailedOperation = { receipt: AdmissionReceipt; operation: string };
type ToolProposalFacet = Readonly<{
  callId: string;
  toolName: string;
  input: unknown;
  pointer: string;
  native: Record<string, unknown>;
}>;
type InvocationSettlement =
  | Readonly<{ status: 'completed'; result: unknown }>
  | Readonly<{ status: 'failed'; error: unknown }>
  | Readonly<{ status: 'cancelled' }>;
export type OpenAIAgentsRunIdentity = Readonly<{
  conversationId: string;
  turnId: string;
  turnIndex: number;
  previousTurnId?: string;
}>;
type Invocation = {
  identity?: OpenAIAgentsRunIdentity;
  trace?: TraceIdentity;
  streaming: boolean;
  modelOperations: AcceptedReceipt[];
  nextStreamOperation: number;
  streamOperation?: AcceptedReceipt;
  awaitingStreamOperation: boolean;
  pendingStream: Array<(trace: TraceIdentity, parentRecordId?: string) => void>;
  pending: Array<(trace: TraceIdentity) => void>;
  proposalCallIds: Set<string>;
  proposals: Map<string, Readonly<{ toolName: string; input: unknown }>>;
  executionGapRecorded: boolean;
  resultRecorded: boolean;
  result?: unknown;
  resultReady: boolean;
  streamCancellationRequested: boolean;
  activeStreamResponseIds: Set<string>;
  streamReasoning: Map<string, StreamReasoningResponse>;
  streamReasoningCorrelationGapRecorded: boolean;
  partialStreamReasoningRecorded: boolean;
  openGenerationSpans: number;
  settlement?: InvocationSettlement;
  complete?: (settlement: InvocationSettlement) => void;
};
type StreamReasoningResponse = {
  responseId: string;
  blocks: Map<string, ReasoningBlock & { itemId: string; deltaObserved: boolean }>;
  nativeEvents: unknown[];
};
type AgentsBridge = {
  active: boolean;
  installed: boolean;
  subject?: AgentsSubject;
  sink?: SourceSink;
  invocations: AsyncLocalStorage<Invocation>;
  restorers: Set<() => void>;
};

export type OpenAIAgentsCaptureAdapter = {
  createSource(client: object): CaptureSource;
  run(...args: unknown[]): Promise<unknown>;
  runWithIdentity(identity: OpenAIAgentsRunIdentity, ...args: unknown[]): Promise<unknown>;
};

/** Additive OpenAI Agents tracing processor. The upstream registry has no removal hook. */
export function openAIAgentsAdapter(options: AgentsAdapterOptions = {}): OpenAIAgentsCaptureAdapter {
  const bridge: AgentsBridge = {
    active: false,
    installed: false,
    invocations: new AsyncLocalStorage(),
    restorers: new Set(),
  };
  const runInvocation = (
    identity: OpenAIAgentsRunIdentity | undefined,
    args: unknown[],
  ): Promise<unknown> => {
    if (!bridge.subject || !bridge.installed) {
      throw new Error('OpenAI Agents adapter must be installed before run');
    }
    if (!bridge.active || !bridge.sink) {
      return Reflect.apply(bridge.subject.run, bridge.subject, args) as Promise<unknown>;
    }
    const invocation: Invocation = {
      ...(identity ? { identity } : {}),
      streaming: false,
      modelOperations: [], nextStreamOperation: 0,
      awaitingStreamOperation: false, pendingStream: [], pending: [],
      proposalCallIds: new Set(), proposals: new Map(),
      executionGapRecorded: false, resultRecorded: false, resultReady: false,
      streamCancellationRequested: false,
      activeStreamResponseIds: new Set(),
      streamReasoning: new Map(), streamReasoningCorrelationGapRecorded: false,
      partialStreamReasoningRecorded: false,
      openGenerationSpans: 0,
    };
    return bridge.invocations.run(invocation, async () => {
      try {
        const result = await Reflect.apply(bridge.subject!.run, bridge.subject, args);
        invocation.result = result;
        invocation.resultReady = true;
        const observed = instrumentStreamedResult(result, bridge, invocation);
        if (!invocation.streaming) {
          recordInvocation(bridge, invocation, (trace) => {
            if (invocation.resultRecorded || !bridge.sink) return;
            invocation.resultRecorded = true;
            recordNonStreamResult(bridge.sink, trace, result, invocation);
          });
          settleInvocation(invocation, { status: 'completed', result });
        }
        return observed;
      } catch (error) {
        settleInvocation(invocation, { status: 'failed', error });
        throw error;
      }
    });
  };
  return Object.freeze({
    createSource(client) {
      if (!isAgentsSubject(client)) throw new TypeError('OpenAI Agents subject must expose addTraceProcessor');
      if (bridge.subject && bridge.subject !== client) throw new Error('OpenAI Agents adapter is already bound');
      bridge.subject = client;
      return createAgentsSource(client, options.version, bridge);
    },
    run(...args: unknown[]): Promise<unknown> {
      return runInvocation(undefined, args);
    },
    runWithIdentity(identity: OpenAIAgentsRunIdentity, ...args: unknown[]): Promise<unknown> {
      return runInvocation(validateRunIdentity(identity), args);
    },
  });
}

function createAgentsSource(
  subject: AgentsSubject,
  version: string | undefined,
  bridge: AgentsBridge,
): CaptureSource {
  return trustOfficialSource({
    metadata: {
      name: 'official:openai-agents-js',
      seam: 'addTraceProcessor/TracingProcessor + Runner.run/StreamedRunResult iterator proxy',
      identityDomain: 'openai-agents.trace',
      ...(version ? { version } : {}),
      qualification: version === undefined
        ? { status: 'unknown' }
        : QUALIFIED_OPENAI_AGENTS_VERSIONS.has(version)
          ? { status: 'exact_qualified' }
          : {
            status: 'capability_checked_unqualified',
            profile: 'openai-agents-tracing-processor-v1',
          },
      official: true,
      coverage: [
        { ...AGENT_RUN_COVERAGE, role: 'owner' },
        { ...OPENAI_RESPONSE_COVERAGE, role: 'owner' },
      ],
    },
    install(sink) {
      if (bridge.active || bridge.sink) throw new Error('OpenAI Agents adapter is already installed');
      const traces = new Map<string, OpenTrace>();
      const spans = new Map<string, OpenSpan>();
      const failedTraces = new Map<string, FailedOperation>();
      let active = true;
      const processor: AgentsProcessor = {
        onTraceStart(trace) {
          if (!active) return;
          const invocation = bridge.invocations.getStore();
          const opened = openTrace(trace, sink, traces, invocation);
          if (opened && invocation) {
            bindInvocation(opened, invocation);
          }
        },
        onTraceEnd(trace) {
          if (!active) return;
          const open = traces.get(trace.traceId);
          if (!open) return;
          const native = traceSnapshot(trace);
          if (open.invocation) {
            const invocation = open.invocation;
            invocation.complete = (settlement) => {
              if (!active || traces.get(trace.traceId) !== open) return;
              const finalOutput = settlement.status === 'completed'
                ? exactRunFinalOutput(settlement.result)
                : undefined;
              const semanticError = settlement.status === 'failed'
                ? exactErrorSemantic(errorSnapshot(settlement.error), 'agent_run_error')
                : undefined;
              sink.record({
                kind: 'lifecycle',
                phase: settlement.status === 'completed'
                  ? 'end'
                  : settlement.status === 'failed' ? 'error' : 'cancelled',
                name: open.name,
                trace: open.identity,
                nativeIdentity: trace.traceId,
                native: {
                  ...native,
                  ...(finalOutput !== undefined
                    ? { final_output: snapshotNative(finalOutput) } : {}),
                  ...(settlement.status === 'failed'
                    ? { error: errorSnapshot(settlement.error) } : {}),
                },
                semantic: {
                  type: 'agent.run',
                  framework: 'openai-agents',
                  name: open.name,
                  status: settlement.status,
                  ...(finalOutput !== undefined ? { output: snapshotNative(finalOutput) } : {}),
                  ...(semanticError ? { error: semanticError } : {}),
                },
              });
              failedTraces.delete(trace.traceId);
              traces.delete(trace.traceId);
              delete invocation.complete;
              delete invocation.settlement;
              invocation.activeStreamResponseIds.clear();
              invocation.streamReasoning.clear();
            };
            if (invocation.settlement) {
              invocation.complete(invocation.settlement);
            } else if (invocation.streamCancellationRequested) {
              settleInvocation(invocation, { status: 'cancelled' });
            } else if (invocation.streaming && invocation.resultReady) {
              settleInvocation(invocation, { status: 'completed', result: invocation.result });
            }
            return;
          }
          sink.record({
            kind: 'lifecycle', phase: 'end', name: open.name, trace: open.identity,
            nativeIdentity: trace.traceId,
            native,
            semantic: {
              type: 'agent.run',
              framework: 'openai-agents',
              name: open.name,
              status: 'unknown',
            },
          });
          failedTraces.delete(trace.traceId);
          traces.delete(trace.traceId);
        },
        onSpanStart(span) {
          if (!active) return;
          const invocation = bridge.invocations.getStore();
          const root = traces.get(span.traceId) ?? openImplicitTrace(
            span.traceId, sink, traces, invocation,
          );
          if (!root) return;
          if (invocation && root.invocation !== invocation) bindInvocation(root, invocation);
          const kind = spanKind(span.spanData.type);
          const name = `openai_agents.${String(span.spanData.type ?? 'unknown')}`;
          const native = spanSnapshot(span);
          const spanData = snapshotSpanData(native);
          const contextRefs = spanData.type === 'generation'
            ? recordModelContext(sink, root, span.spanId, spanData.input)
            : [];
          const priorError = failedTraces.get(span.traceId);
          const operation = spanOperation(span.spanData);
          const recovery = priorError?.operation === operation ? sink.record({
            kind: 'state', phase: 'event', name: 'openai_agents.recovery', trace: root.identity,
            nativeIdentity: span.spanId,
            native: {
              attempt: 1, retry_operation: operation, recovering_span_id: span.spanId,
              recovering_span_data: spanData,
            },
            ...(priorError.receipt.accepted ? { parentRecordId: priorError.receipt.recordId } : {}),
            semantic: { type: 'recovery.retry', framework: 'openai-agents' },
          }) : undefined;
          if (recovery) failedTraces.delete(span.traceId);
          const nativeParent = span.parentId ? spans.get(span.parentId) : undefined;
          const parentRecordId = recovery?.accepted
            ? recovery.recordId
            : nativeParent?.start.accepted
              ? nativeParent.start.recordId
              : undefined;
          const start = sink.record({
            kind, phase: 'start', name, trace: root.identity,
            nativeIdentity: span.spanId,
            native,
            ...(parentRecordId ? { parentRecordId } : {}),
            semantic: semanticForSpan(
              spanData,
              'start',
              native.error,
              contextRefs,
            ),
          });
          if (isModelOperationSpan(span.spanData.type) && root.invocation) {
            if (start.accepted) {
              root.invocation.modelOperations.push(start);
              fulfillStreamOperation(root.invocation);
            }
          }
          if (span.spanData.type === 'generation' && root.invocation) {
            root.invocation.openGenerationSpans += 1;
          }
          spans.set(span.spanId, {
            trace: root.identity,
            name,
            type: String(spanData.type ?? 'unknown'),
            start,
          });
        },
        onSpanEnd(span) {
          if (!active) return;
          const open = spans.get(span.spanId);
          if (!open) return;
          const failed = span.error !== undefined && span.error !== null;
          const responseId = exactResponseId(span.spanData);
          const native = spanSnapshot(span);
          const spanData = snapshotSpanData(native);
          const invocation = traces.get(span.traceId)?.invocation;
          if (spanData.type === 'generation' && invocation) {
            invocation.openGenerationSpans = Math.max(0, invocation.openGenerationSpans - 1);
          }
          const cancelled = !failed
            && invocation?.streamCancellationRequested === true
            && ['agent', 'generation', 'response'].includes(String(spanData.type));
          const semanticPhase = failed ? 'error' : cancelled ? 'cancelled' : 'end';
          const streamedReasoning = exactStreamReasoningForSpan(
            invocation,
            spanData,
            responseId,
          );
          if (streamedReasoning && semanticPhase !== 'end') {
            invocation!.partialStreamReasoningRecorded = true;
          }
          const terminalPhase = cancelled
            ? 'cancelled'
            : failed && spanData.type !== 'generation' ? 'error' : 'end';
          const ended = sink.record({
            kind: spanKind(span.spanData.type), phase: terminalPhase, name: open.name,
            trace: open.trace,
            nativeIdentity: span.spanId,
            native: {
              ...(responseId ? spanLifecycleSnapshot(native, responseId) : native),
              ...(streamedReasoning
                ? { consumed_reasoning_events: streamedReasoning.nativeEvents } : {}),
            },
            ...(open.start.accepted ? { parentRecordId: open.start.recordId } : {}),
            semantic: semanticForSpan(
              spanData,
              semanticPhase,
              native.error,
              undefined,
              streamedReasoning?.blocks,
            ),
          });
          const response = responseId ? sink.record({
            kind: 'model', phase: 'event', name: open.name, trace: open.trace,
            nativeIdentity: responseId,
            coverage: OPENAI_RESPONSE_COVERAGE,
            native,
            ...(open.start.accepted ? { parentRecordId: open.start.recordId } : {}),
            semantic: modelResponseSemantic(spanData, semanticPhase, streamedReasoning?.blocks),
          }) : undefined;
          const unavailableReasoning = openAIAgentsUnavailableReasoning(spanData.output);
          if (unavailableReasoning > 0) sink.record({
            kind: 'unknown',
            phase: 'gap',
            name: 'openai_agents.model.reasoning.unavailable',
            trace: open.trace,
            native: { span_id: span.spanId, unavailable_reasoning_blocks: unavailableReasoning },
            ...(response?.accepted
              ? { parentRecordId: response.recordId }
              : ended.accepted ? { parentRecordId: ended.recordId } : {}),
            semantic: {
              type: 'capture.gap',
              framework: 'openai-agents',
              reason: 'reasoning_unavailable',
              count: unavailableReasoning,
              detail: 'OpenAI Agents exposed only encrypted reasoning content.',
            },
          });
          if (failed && spanData.type !== 'agent') {
            const semanticError = exactErrorSemantic(native.error, 'framework_span_error');
            const error = sink.record({
            kind: 'error', phase: 'event', name: 'openai_agents.span.error', trace: open.trace,
            nativeIdentity: span.spanId,
            native: { error: native.error, span_data: spanData },
            ...(ended.accepted ? { parentRecordId: ended.recordId } : {}),
            semantic: spanData.type === 'function'
              ? redundantSemantic()
              : semanticError
                ? { type: 'error', framework: 'openai-agents', error: semanticError }
                : { coverage: 'missing', framework: 'openai-agents' },
            });
            failedTraces.set(span.traceId, { receipt: error, operation: spanOperation(span.spanData) });
          }
          if (!failed && !cancelled && span.spanData.type === 'generation') {
            const proposals = exactToolProposalFacets(spanData.output);
            if (proposals === null) {
              sink.record({
                kind: 'unknown',
                phase: 'gap',
                name: 'openai_agents.tool.proposal.ambiguous',
                trace: open.trace,
                native: {
                  generation_span_id: span.spanId,
                  reason: 'ambiguous_tool_proposal',
                },
                semantic: { coverage: 'missing', framework: 'openai-agents' },
              });
            }
            for (const proposal of proposals ?? []) {
              const invocation = traces.get(span.traceId)?.invocation;
              if (invocation?.proposalCallIds.has(proposal.callId)) continue;
              invocation?.proposalCallIds.add(proposal.callId);
              invocation?.proposals.set(proposal.callId, {
                toolName: proposal.toolName,
                input: proposal.input,
              });
              sink.record({
                kind: 'tool', phase: 'event', name: 'openai_agents.tool.proposal', trace: open.trace,
                nativeIdentity: proposal.callId,
                native: {
                  generation_span_id: span.spanId,
                  proposal_pointer: proposal.pointer,
                  proposal: proposal.native,
                },
                semantic: {
                  type: 'tool.proposal', framework: 'openai-agents',
                  call_id: proposal.callId,
                  native_call_id: proposal.callId,
                  name: proposal.toolName,
                  tool_name: proposal.toolName,
                  ...(proposal.input !== undefined ? { input: proposal.input } : {}),
                },
              });
            }
          }
          if (spanData.type === 'generation'
            && semanticPhase !== 'end'
            && invocation
            && !streamedReasoning) {
            recordPartialStreamReasoning(
              bridge,
              invocation,
              semanticPhase === 'cancelled' ? 'cancelled' : 'incomplete',
            );
          }
          spans.delete(span.spanId);
        },
        forceFlush() {},
        shutdown() {},
      };
      try {
        subject.addTraceProcessor(processor);
      } catch (error) {
        active = false;
        throw error;
      }
      const cancelOutstanding = () => {
        for (const [spanId, open] of spans) {
          if (open.type === 'agent') {
            sink.record({
              kind: 'lifecycle',
              phase: 'cancelled',
              name: open.name,
              trace: open.trace,
              nativeIdentity: spanId,
              native: { reason: 'source_deactivated', span_id: spanId },
              ...(open.start.accepted ? { parentRecordId: open.start.recordId } : {}),
              semantic: {
                type: 'agent.scope',
                framework: 'openai-agents',
                status: 'cancelled',
              },
            });
            continue;
          }
          sink.record({
            kind: open.name === 'openai_agents.generation' ? 'model'
              : open.name === 'openai_agents.function' ? 'tool' : 'unknown',
            phase: 'cancelled', name: open.name, trace: open.trace,
            nativeIdentity: spanId,
            native: { reason: 'source_deactivated', span_id: spanId },
            ...(open.start.accepted ? { parentRecordId: open.start.recordId } : {}),
          });
        }
        spans.clear();
        for (const [traceId, open] of traces) {
          sink.record({
            kind: 'lifecycle', phase: 'cancelled', name: open.name, trace: open.identity,
            nativeIdentity: traceId,
            native: { reason: 'source_deactivated', trace_id: traceId },
          });
          open.invocation?.activeStreamResponseIds.clear();
          open.invocation?.streamReasoning.clear();
        }
        traces.clear();
        failedTraces.clear();
      };
      bridge.active = true;
      bridge.installed = true;
      bridge.sink = sink;
      return {
        deactivate() {
          cancelOutstanding();
          active = false;
          bridge.active = false;
          restoreStreamResults(bridge);
        },
        drain() {
          restoreStreamResults(bridge);
          bridge.sink = undefined;
          traces.clear();
          spans.clear();
          failedTraces.clear();
        },
      };
    },
  }, 'deep');
}

function instrumentStreamedResult(
  result: unknown,
  bridge: AgentsBridge,
  invocation: Invocation,
): unknown {
  if (!isOfficialStreamedResult(result, bridge.subject)) return result;
  invocation.streaming = true;
  const original = Reflect.get(result, Symbol.asyncIterator);
  if (typeof original !== 'function') return result;
  const previous = Object.getOwnPropertyDescriptor(result, Symbol.asyncIterator);
  if (!Object.isExtensible(result)) {
    recordInvocation(bridge, invocation, (trace, parentRecordId) => bridge.sink?.record({
      kind: 'unknown', phase: 'gap', name: 'openai_agents.stream.unobservable', trace,
      native: { reason: 'non_extensible_streamed_result' },
      ...(parentRecordId ? { parentRecordId } : {}),
      semantic: { coverage: 'missing', framework: 'openai-agents' },
    }));
    return result;
  }
  let restore = () => {};
  let observable = true;
  const wrapped = function (this: object): AsyncIterator<unknown> {
    const iterator = Reflect.apply(original, result, []) as AsyncIterator<unknown>;
    return observable ? observeIterator(iterator, bridge, invocation, () => restore()) : iterator;
  };
  try {
    Object.defineProperty(result, Symbol.asyncIterator, {
      configurable: true,
      writable: true,
      value: wrapped,
    });
  } catch (error) {
    recordInvocation(bridge, invocation, (trace, parentRecordId) => bridge.sink?.record({
      kind: 'unknown', phase: 'gap', name: 'openai_agents.stream.unobservable', trace,
      native: { reason: 'streamed_result_attachment_failed', error: errorSnapshot(error) },
      ...(parentRecordId ? { parentRecordId } : {}),
      semantic: { coverage: 'missing', framework: 'openai-agents' },
    }));
    return result;
  }
  restore = () => {
    observable = false;
    try {
      const current = Object.getOwnPropertyDescriptor(result, Symbol.asyncIterator);
      if (current?.value !== wrapped) return;
      if (previous) Object.defineProperty(result, Symbol.asyncIterator, previous);
      else Reflect.deleteProperty(result, Symbol.asyncIterator);
    } catch {
      // The application may freeze or otherwise lock the official result after run(). The
      // installed wrapper becomes an inert pass-through rather than changing native control flow.
    } finally {
      bridge.restorers.delete(restore);
    }
  };
  bridge.restorers.add(restore);
  return result;
}

function settleInvocation(invocation: Invocation, settlement: InvocationSettlement): void {
  if (invocation.complete) {
    invocation.complete(settlement);
    return;
  }
  invocation.settlement = settlement;
}

function bindInvocation(open: OpenTrace, invocation: Invocation): void {
  open.invocation = invocation;
  invocation.trace = open.identity;
  for (const pending of invocation.pending.splice(0)) pending(open.identity);
}

function recordNonStreamResult(
  sink: SourceSink,
  trace: TraceIdentity,
  result: unknown,
  invocation: Invocation,
): void {
  const items = publicRunResultValue(result, 'newItems');
  if (!Array.isArray(items)) return;
  for (const item of items) {
    const tool = exactRunResultToolFacet(item);
    if (!tool) continue;
    const native = snapshotNative(item);
    if (tool.stage === 'proposal') {
      invocation.proposals.set(tool.callId, {
        toolName: tool.toolName,
        input: tool.value,
      });
      if (invocation.proposalCallIds.has(tool.callId)) continue;
      invocation.proposalCallIds.add(tool.callId);
      sink.record({
        kind: 'tool',
        phase: 'event',
        name: 'openai_agents.tool.proposal',
        trace,
        nativeIdentity: tool.callId,
        native,
        semantic: {
          type: 'tool.proposal',
          framework: 'openai-agents',
          call_id: tool.callId,
          native_call_id: tool.callId,
          name: tool.toolName,
          tool_name: tool.toolName,
          ...(tool.value !== undefined ? { input: tool.value } : {}),
        },
      });
      continue;
    }
    recordObservedToolOutput(sink, trace, invocation, tool, native);
  }
}

function recordObservedToolOutput(
  sink: SourceSink,
  trace: TraceIdentity,
  invocation: Invocation,
  tool: Readonly<{ callId: string; toolName: string; value: unknown }>,
  native: unknown,
  parentRecordId?: string,
): void {
  const proposal = invocation.proposals.get(tool.callId);
  if (!proposal || proposal.toolName !== tool.toolName || proposal.input === undefined) {
    sink.record({
      kind: 'tool',
      phase: 'event',
      name: 'openai_agents.tool.output',
      trace,
      nativeIdentity: tool.callId,
      native,
      ...(parentRecordId ? { parentRecordId } : {}),
      semantic: {
        type: 'message',
        framework: 'openai-agents',
        role: 'tool',
        call_id: tool.callId,
        content: snapshotNative(tool.value),
      },
    });
    if (invocation.executionGapRecorded) return;
    invocation.executionGapRecorded = true;
    sink.record({
      kind: 'unknown',
      phase: 'gap',
      name: 'openai_agents.tool.execution.unproven',
      trace,
      native: { reason: 'openai_agents_tool_execution_unproven' },
      ...(parentRecordId ? { parentRecordId } : {}),
      semantic: {
        type: 'capture.gap',
        framework: 'openai-agents',
        reason: 'unsupported_native_value',
        detail: 'openai_agents_tool_execution_unproven',
      },
    });
    return;
  }

  const call = sink.record({
    kind: 'tool',
    phase: 'start',
    name: 'openai_agents.tool.execution',
    trace,
    nativeIdentity: tool.callId,
    native: {
      call_id: tool.callId,
      name: tool.toolName,
      input: snapshotNative(proposal.input),
    },
    ...(parentRecordId ? { parentRecordId } : {}),
    semantic: {
      type: 'tool.execution',
      framework: 'openai-agents',
      call_id: tool.callId,
      native_call_id: tool.callId,
      name: tool.toolName,
      input: snapshotNative(proposal.input),
    },
  });
  sink.record({
    kind: 'tool',
    phase: 'end',
    name: 'openai_agents.tool.execution',
    trace,
    nativeIdentity: tool.callId,
    native,
    ...(call.accepted
      ? { parentRecordId: call.recordId }
      : parentRecordId ? { parentRecordId } : {}),
    semantic: {
      type: 'tool.result',
      framework: 'openai-agents',
      call_id: tool.callId,
      native_call_id: tool.callId,
      status: 'succeeded',
      output: snapshotNative(tool.value),
    },
  });
  invocation.proposals.delete(tool.callId);
}

function exactRunFinalOutput(result: unknown): unknown {
  return publicRunResultValue(result, 'finalOutput');
}

function publicRunResultValue(result: unknown, key: 'newItems' | 'finalOutput'): unknown {
  if (!isRecord(result)) return undefined;
  try {
    return Reflect.get(result, key);
  } catch {
    return undefined;
  }
}

function observeIterator(
  iterator: AsyncIterator<unknown>,
  bridge: AgentsBridge,
  invocation: Invocation,
  restore: () => void,
): AsyncIterator<unknown> {
  let terminal = false;
  const finish = (phase: 'end' | 'error' | 'cancelled', _native: unknown) => {
    if (terminal) return;
    terminal = true;
    if (phase === 'end' && invocation.resultReady) {
      settleInvocation(invocation, { status: 'completed', result: invocation.result });
    } else if (phase === 'cancelled') {
      settleInvocation(invocation, { status: 'cancelled' });
    }
    flushUnboundStreamOperation(bridge, invocation);
    restore();
  };
  const proxy: AsyncIterableIterator<unknown> = {
    [Symbol.asyncIterator]() { return this; },
    async next(value?: unknown) {
      try {
        const result = await iterator.next(value);
        if (result.done) finish('end', { reason: 'natural_exhaustion', result });
        else recordStreamEvent(bridge, invocation, result.value);
        return result;
      } catch (error) {
        recordStreamError(bridge, invocation, error);
        recordPartialStreamReasoning(bridge, invocation, 'incomplete');
        settleInvocation(invocation, { status: 'failed', error });
        finish('error', { reason: 'upstream_error', error: errorSnapshot(error) });
        throw error;
      }
    },
  };
  if (typeof iterator.return === 'function') proxy.return = async (value?: unknown) => {
    invocation.streamCancellationRequested = true;
    try {
      const result = await iterator.return!(value);
      recordPartialStreamReasoning(bridge, invocation, 'cancelled');
      if (result.done) finish('cancelled', { reason: 'application_return', result });
      else recordStreamEvent(bridge, invocation, result.value);
      return result;
    } catch (error) {
      recordStreamError(bridge, invocation, error);
      recordPartialStreamReasoning(bridge, invocation, 'incomplete');
      settleInvocation(invocation, { status: 'failed', error });
      finish('error', { reason: 'return_error', error: errorSnapshot(error) });
      throw error;
    }
  };
  if (typeof iterator.throw === 'function') proxy.throw = async (error?: unknown) => {
    try {
      const result = await iterator.throw!(error);
      if (result.done) {
        recordPartialStreamReasoning(bridge, invocation, 'incomplete');
        settleInvocation(invocation, { status: 'failed', error });
        finish('error', { reason: 'application_throw', error: errorSnapshot(error), result });
      }
      else recordStreamEvent(bridge, invocation, result.value);
      return result;
    } catch (thrown) {
      recordStreamError(bridge, invocation, thrown);
      recordPartialStreamReasoning(bridge, invocation, 'incomplete');
      settleInvocation(invocation, { status: 'failed', error: thrown });
      finish('error', { reason: 'application_throw', error: errorSnapshot(thrown) });
      throw thrown;
    }
  };
  return proxy;
}

function recordPartialStreamReasoning(
  bridge: AgentsBridge,
  invocation: Invocation,
  status: 'incomplete' | 'cancelled',
): void {
  if (invocation.partialStreamReasoningRecorded) return;
  if (invocation.openGenerationSpans > 0 || invocation.activeStreamResponseIds.size !== 1) return;
  const responseId = [...invocation.activeStreamResponseIds][0];
  const accumulated = responseId ? invocation.streamReasoning.get(responseId) : undefined;
  if (!responseId || !accumulated || accumulated.blocks.size === 0) return;
  invocation.partialStreamReasoningRecorded = true;
  const reasoning = [...accumulated.blocks.values()].map(({ type, text }) => ({ type, text }));
  const record = (trace: TraceIdentity) => {
    const state = bridge.sink?.record({
      kind: 'state', phase: 'event', name: 'openai_agents.stream.reasoning.partial', trace,
      nativeIdentity: responseId,
      native: {
        response_id: responseId,
        consumed_reasoning_events: accumulated.nativeEvents,
      },
      semantic: {
        type: 'state.transition', framework: 'openai-agents',
        state_type: 'openai_agents.stream.reasoning_partial',
        value: { status, response_id: responseId, reasoning },
      },
    });
    bridge.sink?.record({
      kind: 'unknown', phase: 'gap', name: 'openai_agents.stream.reasoning_request_unresolved',
      trace,
      native: { response_id: responseId, status },
      ...(state?.accepted ? { parentRecordId: state.recordId } : {}),
      semantic: {
        type: 'capture.gap', framework: 'openai-agents',
        reason: 'unsupported_native_value',
        detail: 'openai_agents_reasoning_model_request_correlation_not_captured',
      },
    });
  };
  if (invocation.trace) record(invocation.trace);
  else invocation.pending.push(record);
}

function recordStreamEvent(
  bridge: AgentsBridge,
  invocation: Invocation,
  event: unknown,
): void {
  const native = snapshotNative(event);
  advanceStreamOperation(invocation, native);
  accumulateStreamReasoning(bridge, invocation, native);
  const tool = exactRunItemToolFacet(native);
  if (tool) {
    if (tool.stage === 'proposal') {
      invocation.proposals.set(tool.callId, {
        toolName: tool.toolName,
        input: tool.value,
      });
      if (invocation.proposalCallIds.has(tool.callId)) return;
      invocation.proposalCallIds.add(tool.callId);
      recordInvocation(bridge, invocation, (trace, parentRecordId) => bridge.sink?.record({
        kind: 'tool', phase: 'event', name: 'openai_agents.tool.proposal', trace,
        nativeIdentity: tool.callId,
        native,
        ...(parentRecordId ? { parentRecordId } : {}),
        semantic: {
          type: 'tool.proposal',
          framework: 'openai-agents',
          call_id: tool.callId,
          native_call_id: tool.callId,
          name: tool.toolName,
          tool_name: tool.toolName,
          input: tool.value,
        },
      }));
      return;
    }
    recordInvocation(bridge, invocation, (trace, parentRecordId) => {
      if (bridge.sink) {
        recordObservedToolOutput(
          bridge.sink,
          trace,
          invocation,
          tool,
          native,
          parentRecordId,
        );
      }
    });
    return;
  }
}

function accumulateStreamReasoning(
  bridge: AgentsBridge,
  invocation: Invocation,
  event: unknown,
): void {
  if (!isRecord(event) || ownDataValue(event, 'type') !== 'raw_model_stream_event') return;
  const data = ownDataValue(event, 'data');
  if (!isRecord(data)) return;
  const raw = ownDataValue(data, 'type') === 'model'
    ? ownDataValue(data, 'event')
    : undefined;
  if (isRecord(raw) && ownDataValue(raw, 'type') === 'response.created') {
    const response = ownDataValue(raw, 'response');
    const responseId = isRecord(response) ? exactString(ownDataValue(response, 'id')) : undefined;
    if (responseId) openStreamResponse(invocation, responseId);
    return;
  }
  if (ownDataValue(data, 'type') === 'response_started') {
    const providerData = ownDataValue(data, 'providerData');
    const response = isRecord(providerData) ? ownDataValue(providerData, 'response') : undefined;
    const responseId = isRecord(response) ? exactString(ownDataValue(response, 'id')) : undefined;
    if (!responseId) return;
    openStreamResponse(invocation, responseId);
    return;
  }
  if (ownDataValue(data, 'type') === 'response_done') {
    const response = ownDataValue(data, 'response');
    const responseId = isRecord(response) ? exactString(ownDataValue(response, 'id')) : undefined;
    if (responseId) invocation.activeStreamResponseIds.delete(responseId);
    return;
  }
  if (!isRecord(raw)) return;
  const eventType = ownDataValue(raw, 'type');
  if (['response.completed', 'response.failed', 'response.incomplete'].includes(String(eventType))) {
    const response = ownDataValue(raw, 'response');
    const responseId = isRecord(response) ? exactString(ownDataValue(response, 'id')) : undefined;
    if (responseId) invocation.activeStreamResponseIds.delete(responseId);
    return;
  }
  const kind = eventType === 'response.reasoning_text.delta'
    || eventType === 'response.reasoning_text.done'
    ? 'text'
    : eventType === 'response.reasoning_summary_text.delta'
      || eventType === 'response.reasoning_summary_text.done'
      ? 'summary'
      : undefined;
  if (!kind) return;
  const itemId = exactString(ownDataValue(raw, 'item_id'));
  const responseId = invocation.activeStreamResponseIds.size === 1
    ? [...invocation.activeStreamResponseIds][0]
    : undefined;
  if (!itemId || !responseId) {
    if (!invocation.streamReasoningCorrelationGapRecorded) {
      invocation.streamReasoningCorrelationGapRecorded = true;
      recordInvocation(bridge, invocation, (trace, parentRecordId) => bridge.sink?.record({
        kind: 'unknown', phase: 'gap', name: 'openai_agents.stream.reasoning_uncorrelated', trace,
        native: { event },
        ...(parentRecordId ? { parentRecordId } : {}),
        semantic: {
          type: 'capture.gap', framework: 'openai-agents',
          reason: 'unsupported_native_value',
          detail: 'openai_agents_reasoning_response_correlation_not_captured',
        },
      }));
    }
    return;
  }
  const accumulated = invocation.streamReasoning.get(responseId);
  if (!accumulated) return;
  const partIndex = kind === 'summary'
    ? ownDataValue(raw, 'summary_index')
    : ownDataValue(raw, 'content_index');
  if (typeof partIndex !== 'number' || !Number.isInteger(partIndex) || partIndex < 0) return;
  const key = JSON.stringify([itemId, kind, partIndex]);
  const existing = accumulated.blocks.get(key);
  const deltaValue = ownDataValue(raw, 'delta');
  const doneValue = ownDataValue(raw, 'text');
  const delta = typeof deltaValue === 'string' && deltaValue.length > 0 ? deltaValue : undefined;
  const doneText = typeof doneValue === 'string' && doneValue.length > 0 ? doneValue : undefined;
  if (delta) {
    accumulated.blocks.set(key, {
      type: kind,
      itemId,
      text: `${existing?.text ?? ''}${delta}`,
      deltaObserved: true,
    });
  } else if (doneText && !existing?.deltaObserved) {
    accumulated.blocks.set(key, {
      type: kind, itemId, text: doneText, deltaObserved: false,
    });
  }
  accumulated.nativeEvents.push(event);
}

function openStreamResponse(invocation: Invocation, responseId: string): void {
  invocation.activeStreamResponseIds.add(responseId);
  if (!invocation.streamReasoning.has(responseId)) {
    invocation.streamReasoning.set(responseId, {
      responseId,
      blocks: new Map(),
      nativeEvents: [],
    });
  }
}

function exactStreamReasoningForSpan(
  invocation: Invocation | undefined,
  data: Record<string, unknown>,
  responseId: string | undefined,
): Readonly<{ blocks: ReasoningBlock[]; nativeEvents: unknown[] }> | undefined {
  if (!invocation || data.type !== 'generation') return undefined;
  let accumulated = responseId ? invocation.streamReasoning.get(responseId) : undefined;
  if (!accumulated && data.type === 'generation') {
    const itemIds = exactReasoningItemIds(data.output);
    const matches = [...invocation.streamReasoning.values()].filter((candidate) => (
      [...candidate.blocks.values()].some((block) => itemIds.has(block.itemId))
    ));
    if (matches.length === 1) accumulated = matches[0];
  }
  if (!accumulated || accumulated.blocks.size === 0) return undefined;
  return {
    blocks: [...accumulated.blocks.values()].map(({ type, text }) => ({ type, text })),
    nativeEvents: accumulated.nativeEvents,
  };
}

function exactReasoningItemIds(value: unknown): Set<string> {
  const result = new Set<string>();
  if (!Array.isArray(value)) return result;
  for (const item of value) {
    if (!isRecord(item) || ownDataValue(item, 'type') !== 'reasoning') continue;
    const id = exactString(ownDataValue(item, 'id'));
    if (id) result.add(id);
  }
  return result;
}

function exactRunItemToolFacet(event: unknown):
  | Readonly<{
    stage: 'proposal';
    callId: string;
    toolName: string;
    value: unknown;
  }>
  | Readonly<{
    stage: 'output';
    callId: string;
    toolName: string;
    value: unknown;
  }>
  | undefined {
  if (!isRecord(event) || ownDataValue(event, 'type') !== 'run_item_stream_event') return undefined;
  const name = ownDataValue(event, 'name');
  const stage = name === 'tool_called' ? 'proposal' : name === 'tool_output' ? 'output' : undefined;
  if (!stage) return undefined;
  const item = ownDataValue(event, 'item');
  if (!isRecord(item)) return undefined;
  const expectedItem = stage === 'proposal' ? 'tool_call_item' : 'tool_call_output_item';
  const expectedRaw = stage === 'proposal' ? 'function_call' : 'function_call_result';
  const rawItem = ownDataValue(item, 'rawItem');
  if (ownDataValue(item, 'type') !== expectedItem
    || !isRecord(rawItem)
    || ownDataValue(rawItem, 'type') !== expectedRaw) return undefined;
  const identity = inspectToolCallId(rawItem, false);
  const toolName = exactString(ownDataValue(rawItem, 'name'));
  const value = stage === 'proposal'
    ? ownDataValue(rawItem, 'arguments') ?? ownDataValue(rawItem, 'input')
    : ownDataValue(item, 'output') ?? ownDataValue(rawItem, 'output');
  if (identity.status !== 'exact' || !toolName || value === undefined) return undefined;
  return { stage, callId: identity.value, toolName, value };
}

function exactRunResultToolFacet(item: unknown): Readonly<{
  stage: 'proposal' | 'output';
  callId: string;
  toolName: string;
  value: unknown;
}> | undefined {
  if (!isRecord(item)) return undefined;
  const itemType = ownDataValue(item, 'type');
  const stage = itemType === 'tool_call_item'
    ? 'proposal'
    : itemType === 'tool_call_output_item'
      ? 'output'
      : undefined;
  if (!stage) return undefined;
  const rawItem = ownDataValue(item, 'rawItem');
  const expectedRaw = stage === 'proposal' ? 'function_call' : 'function_call_result';
  if (!isRecord(rawItem) || ownDataValue(rawItem, 'type') !== expectedRaw) return undefined;
  const identity = inspectToolCallId(rawItem, false);
  const toolName = exactString(ownDataValue(rawItem, 'name'));
  const value = stage === 'proposal'
    ? ownDataValue(rawItem, 'arguments') ?? ownDataValue(rawItem, 'input')
    : Object.prototype.hasOwnProperty.call(item, 'output')
      ? ownDataValue(item, 'output')
      : ownDataValue(rawItem, 'output');
  return identity.status === 'exact' && toolName && value !== undefined
    ? { stage, callId: identity.value, toolName, value }
    : undefined;
}

function recordStreamError(bridge: AgentsBridge, invocation: Invocation, error: unknown): void {
  const native = directErrorEvidence(error);
  const semanticError = exactErrorSemantic(native, 'stream_error');
  recordInvocation(bridge, invocation, (trace, parentRecordId) => bridge.sink?.record({
    kind: 'error', phase: 'event', name: 'openai_agents.stream.error', trace,
    ...(typeof error === 'object' && error !== null ? { errorIdentity: error } : {}),
    // Keep provider/framework error fields directly addressable in canonical evidence.
    native,
    ...(parentRecordId ? { parentRecordId } : {}),
    semantic: semanticError
      ? { type: 'error', framework: 'openai-agents', error: semanticError }
      : { coverage: 'missing', framework: 'openai-agents' },
  }));
}

function spanOperation(data: Record<string, unknown>): string {
  const type = typeof data.type === 'string' ? data.type : 'unknown';
  const name = typeof data.name === 'string' ? data.name : '';
  return `${type}:${name}`;
}

function recordInvocation(
  bridge: AgentsBridge,
  invocation: Invocation,
  record: (trace: TraceIdentity, parentRecordId?: string) => void,
): void {
  if (!bridge.active) return;
  if (invocation.awaitingStreamOperation) {
    invocation.pendingStream.push(record);
    return;
  }
  const parentRecordId = invocation.streamOperation?.accepted
    ? invocation.streamOperation.recordId
    : undefined;
  if (invocation.trace) record(invocation.trace, parentRecordId);
  else invocation.pending.push(record);
}

function advanceStreamOperation(invocation: Invocation, event?: unknown): void {
  const responseStarted = isRecord(event)
    && ownDataValue(event, 'type') === 'raw_model_stream_event'
    && isRecord(ownDataValue(event, 'data'))
    && ownDataValue(ownDataValue(event, 'data') as Record<string, unknown>, 'type')
      === 'response_started';
  if (!responseStarted) return;
  const next = invocation.modelOperations[invocation.nextStreamOperation];
  if (!next) {
    delete invocation.streamOperation;
    invocation.awaitingStreamOperation = true;
    return;
  }
  invocation.streamOperation = next;
  invocation.nextStreamOperation += 1;
}

function fulfillStreamOperation(invocation: Invocation): void {
  if (!invocation.awaitingStreamOperation) return;
  const next = invocation.modelOperations[invocation.nextStreamOperation];
  if (!next) return;
  invocation.streamOperation = next;
  invocation.nextStreamOperation += 1;
  invocation.awaitingStreamOperation = false;
  const pending = invocation.pendingStream.splice(0);
  if (invocation.trace) {
    for (const record of pending) record(invocation.trace, next.recordId);
    return;
  }
  for (const record of pending) {
    invocation.pending.push((trace) => record(trace, next.recordId));
  }
}

function flushUnboundStreamOperation(bridge: AgentsBridge, invocation: Invocation): void {
  if (!invocation.awaitingStreamOperation) return;
  invocation.awaitingStreamOperation = false;
  const pending = invocation.pendingStream.splice(0);
  const recordGap = (trace: TraceIdentity) => bridge.sink?.record({
    kind: 'unknown', phase: 'gap', name: 'openai_agents.stream.operation_unresolved', trace,
    native: {
      reason: 'response_started_without_official_model_operation',
      pending_event_count: pending.length,
    },
    semantic: { coverage: 'missing', framework: 'openai-agents' },
  });
  if (invocation.trace) {
    for (const record of pending) record(invocation.trace);
    recordGap(invocation.trace);
    return;
  }
  for (const record of pending) invocation.pending.push(record);
  invocation.pending.push(recordGap);
}

function restoreStreamResults(bridge: AgentsBridge): void {
  for (const restore of [...bridge.restorers]) restore();
}

function isOfficialStreamedResult(value: unknown, subject: AgentsSubject | undefined): value is object {
  if (!value || typeof value !== 'object') return false;
  const constructor = subject?.StreamedRunResult;
  return typeof constructor === 'function'
    ? value instanceof constructor
    : (value as { constructor?: { name?: string } }).constructor?.name === 'StreamedRunResult';
}

function errorSnapshot(value: unknown): unknown {
  return snapshotNative(value);
}

function openTrace(
  trace: NativeTrace,
  sink: SourceSink,
  traces: Map<string, OpenTrace>,
  invocation?: Invocation,
): OpenTrace | undefined {
  const name = trace.name?.trim() || 'openai_agents.trace';
  const native = traceSnapshot(trace);
  const receipt = sink.openTrace({
    name,
    coverage: AGENT_RUN_COVERAGE,
    nativeIdentity: trace.traceId,
    conversationId: invocation?.identity?.conversationId ?? trace.groupId ?? undefined,
    ...(invocation?.identity ? {
      turnId: invocation.identity.turnId,
      turnIndex: invocation.identity.turnIndex,
      ...(invocation.identity.previousTurnId
        ? { previousTurnId: invocation.identity.previousTurnId } : {}),
    } : {}),
    native,
    semantic: {
      type: 'agent.run',
      framework: 'openai-agents',
      name,
    },
  });
  if (!receipt.accepted) return undefined;
  const open: OpenTrace = {
    identity: receipt.identity,
    name,
    contextRecords: new Map(),
    ...(invocation ? { invocation } : {}),
  };
  traces.set(trace.traceId, open);
  sink.record({
    kind: 'correlation', phase: 'event', name: 'openai_agents.trace.native', trace: open.identity,
    nativeIdentity: trace.traceId, native,
    semantic: redundantSemantic(),
  });
  return open;
}

function exactResponseId(data: Record<string, unknown>): string | undefined {
  if (data.type !== 'response') return undefined;
  const value = data.response_id;
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function openImplicitTrace(
  traceId: string,
  sink: SourceSink,
  traces: Map<string, OpenTrace>,
  invocation?: Invocation,
): OpenTrace | undefined {
  return openTrace({ traceId, name: 'openai_agents.trace' }, sink, traces, invocation);
}

function validateRunIdentity(value: OpenAIAgentsRunIdentity): OpenAIAgentsRunIdentity {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError('OpenAI Agents run identity must be an object');
  }
  const conversationId = identityString(value, 'conversationId');
  const turnId = identityString(value, 'turnId');
  const turnIndexValue = ownDataValue(value, 'turnIndex');
  if (!Number.isSafeInteger(turnIndexValue) || Number(turnIndexValue) < 0) {
    throw new TypeError('OpenAI Agents run identity turnIndex must be a non-negative safe integer');
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
  value: OpenAIAgentsRunIdentity,
  key: 'conversationId' | 'turnId' | 'previousTurnId',
): string {
  const observed = ownDataValue(value, key);
  if (typeof observed !== 'string' || observed.trim().length === 0) {
    throw new TypeError(`OpenAI Agents run identity ${key} must be a non-empty string`);
  }
  return observed;
}

function ownDataValue(value: object, key: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(value, key);
  return descriptor && 'value' in descriptor ? descriptor.value : undefined;
}

function traceSnapshot(trace: NativeTrace): Record<string, unknown> {
  return {
    trace_id: trace.traceId,
    name: trace.name ?? null,
    group_id: trace.groupId ?? null,
    metadata: snapshotNative(trace.metadata) ?? null,
  };
}

function spanSnapshot(span: NativeSpan): Record<string, unknown> {
  return {
    trace_id: span.traceId,
    span_id: span.spanId,
    parent_id: span.parentId ?? null,
    started_at: span.startedAt ?? null,
    ended_at: span.endedAt ?? null,
    error: snapshotNative(span.error) ?? null,
    span_data: snapshotNative(span.spanData),
  };
}

function spanLifecycleSnapshot(
  snapshot: Record<string, unknown>,
  responseId: string,
): Record<string, unknown> {
  const data = snapshotSpanData(snapshot);
  return {
    trace_id: snapshot.trace_id,
    span_id: snapshot.span_id,
    parent_id: snapshot.parent_id,
    started_at: snapshot.started_at,
    ended_at: snapshot.ended_at,
    error: snapshot.error,
    span_data: { type: data.type ?? null, response_id: responseId },
  };
}

function snapshotSpanData(snapshot: Record<string, unknown>): Record<string, unknown> {
  return isRecord(snapshot.span_data) ? snapshot.span_data : {};
}

function spanKind(type: unknown): 'lifecycle' | 'model' | 'tool' | 'state' | 'unknown' {
  if (type === 'agent') return 'lifecycle';
  if (['generation', 'response', 'transcription', 'speech'].includes(String(type))) return 'model';
  if (['function', 'mcp_tools'].includes(String(type))) return 'tool';
  if (['handoff', 'guardrail'].includes(String(type))) return 'state';
  return 'unknown';
}

function isModelOperationSpan(type: unknown): boolean {
  return type === 'generation' || type === 'response';
}

function semanticForSpan(
  data: Record<string, unknown>,
  phase: 'start' | 'end' | 'error' | 'cancelled',
  error: unknown,
  contextRefs: string[] = [],
  streamedReasoning?: ReasoningBlock[],
): Record<string, unknown> {
  if (data.type === 'agent') {
    const name = exactString(data.name);
    const semanticError = phase === 'error'
      ? exactErrorSemantic(error, 'agent_span_error')
      : undefined;
    return {
      type: 'agent.scope',
      framework: 'openai-agents',
      ...(phase === 'start'
        ? { scope_type: 'agent', ...(name ? { name } : {}) }
        : {
            status: phase === 'error'
              ? 'failed'
              : phase === 'cancelled' ? 'cancelled' : 'completed',
            ...(semanticError ? { error: semanticError } : {}),
          }),
    };
  }
  if (data.type === 'function') {
    return redundantSemantic();
  }
  if (data.type === 'handoff') {
    if (phase === 'start') return redundantSemantic();
    const fromAgent = exactString(data.from_agent);
    const toAgent = exactString(data.to_agent);
    return {
      type: 'state.transition',
      framework: 'openai-agents',
      state_type: 'agent.handoff',
      value: {
        status: phase === 'error' ? 'failed' : 'completed',
        ...(fromAgent ? { from_agent: fromAgent } : {}),
        ...(toAgent ? { to_agent: toAgent } : {}),
      },
    };
  }
  if (data.type === 'generation') {
    return phase === 'start'
      ? modelRequestSemantic(data, contextRefs)
      : modelResponseSemantic(data, phase, streamedReasoning);
  }
  if (data.type === 'response') {
    return phase === 'start' ? modelRequestSemantic(data) : redundantSemantic();
  }
  return { type: 'native.event', framework: 'openai-agents' };
}

function modelRequestSemantic(
  data: Record<string, unknown>,
  contextRefs: string[] = [],
): Record<string, unknown> {
  const model = exactString(data.model);
  return {
    type: 'model.request',
    framework: 'openai-agents',
    context_refs: contextRefs,
    ...(model ? { model } : {}),
  };
}

function recordModelContext(
  sink: SourceSink,
  trace: OpenTrace,
  spanId: string,
  input: unknown,
): string[] {
  if (!Array.isArray(input)) return [];
  const refs: string[] = [];
  const occurrences = new Map<string, number>();
  input.forEach((message, index) => {
    if (!isRecord(message)) return;
    const role = ownDataValue(message, 'role');
    if (!['system', 'developer', 'user', 'assistant', 'tool'].includes(String(role))) return;
    if (!Object.prototype.hasOwnProperty.call(message, 'content')) return;
    const content = safeContextContent(ownDataValue(message, 'content'));
    if (content === undefined) return;
    const callIdentity = inspectToolCallId(message, false);
    if (role === 'assistant' && content === null && callIdentity.status !== 'exact') return;
    const name = exactString(ownDataValue(message, 'name'));
    const fingerprint = JSON.stringify({
      role,
      content,
      ...(callIdentity.status === 'exact' ? { call_id: callIdentity.value } : {}),
      ...(name ? { name } : {}),
    });
    const occurrence = occurrences.get(fingerprint) ?? 0;
    occurrences.set(fingerprint, occurrence + 1);
    const retained = trace.contextRecords.get(fingerprint) ?? [];
    const existing = retained[occurrence];
    if (existing) {
      refs.push(existing);
      return;
    }
    const receipt = sink.record({
      kind: 'model',
      phase: 'event',
      name: 'openai_agents.model.context',
      trace: trace.identity,
      nativeIdentity: `${spanId}:context:${index}`,
      native: { message },
      semantic: {
        type: 'message',
        framework: 'openai-agents',
        origin: 'context',
        role,
        content,
        ...(callIdentity.status === 'exact' ? { call_id: callIdentity.value } : {}),
        ...(name ? { name } : {}),
      },
    });
    if (receipt.accepted) {
      retained.push(receipt.recordId);
      trace.contextRecords.set(fingerprint, retained);
      refs.push(receipt.recordId);
    }
  });
  return refs;
}

function modelResponseSemantic(
  data: Record<string, unknown>,
  phase: 'end' | 'error' | 'cancelled',
  streamedReasoning?: ReasoningBlock[],
): Record<string, unknown> {
  const model = exactString(data.model);
  const usage = exactUsage(data.usage) ?? exactOutputUsage(data.output);
  const content = safeModelResponseContent(data.output);
  const finalReasoning = openAIAgentsResponseReasoning(data.output);
  const reasoning = finalReasoning.length ? finalReasoning : streamedReasoning ?? [];
  const partialStream = finalReasoning.length === 0 && reasoning.length > 0;
  return {
    type: 'model.response',
    framework: 'openai-agents',
    status: phase === 'error'
      ? partialStream ? 'incomplete' : 'failed'
      : phase === 'cancelled' ? 'cancelled' : 'completed',
    ...(phase === 'cancelled'
      ? { finish_reason: 'application_return' }
      : phase === 'error' && partialStream ? { finish_reason: 'stream_error' } : {}),
    ...(model ? { model } : {}),
    ...(content !== undefined ? { content } : {}),
    ...(reasoning.length ? { reasoning } : {}),
    ...(usage ? { usage } : {}),
  };
}

function safeContextContent(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(safeContextContent);
  if (!isRecord(value)) return value;
  const output: Record<string, unknown> = {};
  for (const [key, item] of Object.entries(value)) {
    if (key === 'encrypted_reasoning' || key === 'encrypted_content' || key === 'signature') {
      continue;
    }
    output[key] = safeContextContent(item);
  }
  return output;
}

function safeModelResponseContent(value: unknown): unknown {
  if (!Array.isArray(value)) return undefined;
  const text: string[] = [];
  const append = (content: unknown) => {
    if (typeof content === 'string' && content.length > 0) {
      text.push(content);
      return;
    }
    if (!Array.isArray(content)) return;
    for (const part of content) {
      if (typeof part === 'string' && part.length > 0) {
        text.push(part);
        continue;
      }
      if (!isRecord(part)) continue;
      const type = ownDataValue(part, 'type');
      const partText = ownDataValue(part, 'text');
      if (['text', 'output_text'].includes(String(type))
        && typeof partText === 'string'
        && partText.length > 0) {
        text.push(partText);
      }
    }
  };
  for (const item of value) {
    if (!isRecord(item)) continue;
    if (ownDataValue(item, 'type') === 'message') append(ownDataValue(item, 'content'));
    const choices = ownDataValue(item, 'choices');
    if (!Array.isArray(choices)) continue;
    for (const choice of choices) {
      if (!isRecord(choice)) continue;
      const message = ownDataValue(choice, 'message');
      if (isRecord(message)) append(ownDataValue(message, 'content'));
    }
  }
  return text.length === 0 ? undefined : text.length === 1 ? text[0] : text;
}

type ReasoningBlock = { type: 'text' | 'summary'; text: string };

function openAIAgentsResponseReasoning(value: unknown): ReasoningBlock[] {
  if (!Array.isArray(value)) return [];
  const blocks: ReasoningBlock[] = [];
  for (const item of value) {
    if (!isRecord(item)) continue;
    if (ownDataValue(item, 'type') === 'reasoning') {
      appendReasoningParts(blocks, ownDataValue(item, 'rawContent'), 'text');
      appendReasoningParts(blocks, ownDataValue(item, 'content'), 'summary');
      appendReasoningParts(blocks, ownDataValue(item, 'summary'), 'summary');
    }
    const choices = ownDataValue(item, 'choices');
    if (!Array.isArray(choices)) continue;
    for (const choice of choices) {
      if (!isRecord(choice)) continue;
      const message = ownDataValue(choice, 'message');
      if (!isRecord(message)) continue;
      appendReasoningText(blocks, 'text', ownDataValue(message, 'reasoning'));
      const details = ownDataValue(message, 'reasoning_details');
      if (!Array.isArray(details)) continue;
      for (const detail of details) {
        if (!isRecord(detail)) continue;
        const type = String(ownDataValue(detail, 'type') ?? '');
        if (type.includes('summary')) {
          appendReasoningText(
            blocks,
            'summary',
            ownDataValue(detail, 'summary') ?? ownDataValue(detail, 'text'),
          );
        } else if (type.includes('text')) {
          appendReasoningText(blocks, 'text', ownDataValue(detail, 'text'));
        }
      }
    }
  }
  return blocks;
}

function openAIAgentsUnavailableReasoning(value: unknown): number {
  if (!Array.isArray(value)) return 0;
  return value.filter((item) => (
    isRecord(item)
    && ownDataValue(item, 'type') === 'reasoning'
    && openAIAgentsResponseReasoning([item]).length === 0
    && hasOpaqueReasoningCarrier(item)
  )).length;
}

function hasOpaqueReasoningCarrier(value: unknown, seen = new WeakSet<object>()): boolean {
  if (!value || typeof value !== 'object' || seen.has(value)) return false;
  seen.add(value);
  if (Array.isArray(value)) return value.some((item) => hasOpaqueReasoningCarrier(item, seen));
  for (const [key, item] of Object.entries(value)) {
    if (['encrypted_content', 'encryptedContent', 'encrypted_reasoning'].includes(key)
      && item !== undefined) return true;
    if (hasOpaqueReasoningCarrier(item, seen)) return true;
  }
  return false;
}

function appendReasoningParts(
  blocks: ReasoningBlock[],
  value: unknown,
  type: ReasoningBlock['type'],
): void {
  if (!Array.isArray(value)) return;
  for (const part of value) {
    if (typeof part === 'string') appendReasoningText(blocks, type, part);
    if (isRecord(part)) appendReasoningText(blocks, type, ownDataValue(part, 'text'));
  }
}

function appendReasoningText(
  blocks: ReasoningBlock[],
  type: ReasoningBlock['type'],
  value: unknown,
): void {
  if (typeof value === 'string' && value.length > 0) blocks.push({ type, text: value });
}

function exactOutputUsage(value: unknown): Record<string, number> | undefined {
  if (!Array.isArray(value)) return undefined;
  for (const item of value) {
    if (!isRecord(item)) continue;
    const usage = exactUsage(ownDataValue(item, 'usage'));
    if (usage) return usage;
  }
  return undefined;
}

function directErrorEvidence(error: unknown): Record<string, unknown> {
  const native = errorSnapshot(error);
  return isRecord(native) ? native : { error: native };
}

function exactToolProposalFacets(value: unknown): ToolProposalFacet[] | null {
  if (!Array.isArray(value)) return [];
  const facets: ToolProposalFacet[] = [];
  let rejected = false;
  value.forEach((item, outputIndex) => {
    if (!isRecord(item)) {
      if (hasToolProposal(item)) rejected = true;
      return;
    }
    if (['function_call', 'tool_call', 'tool_use'].includes(String(ownDataValue(item, 'type')))) {
      const identity = inspectToolCallId(item, true);
      const toolName = exactString(ownDataValue(item, 'name'));
      if (identity.status !== 'exact' || !toolName) {
        rejected = true;
        return;
      }
      facets.push({
        callId: identity.value,
        toolName,
        input: exactToolInput(item),
        pointer: `/output/${outputIndex}`,
        native: item,
      });
      return;
    }
    const choices = ownDataValue(item, 'choices');
    if (choices === undefined) {
      if (hasToolProposal(item)) rejected = true;
      return;
    }
    if (!Array.isArray(choices) || choices.length !== 1 || !isRecord(choices[0])) {
      rejected = true;
      return;
    }
    const message = ownDataValue(choices[0], 'message');
    if (!isRecord(message)) {
      rejected = true;
      return;
    }
    const calls = ownDataValue(message, 'tool_calls');
    if (!Array.isArray(calls) || calls.length === 0) {
      if (hasToolProposal(item)) rejected = true;
      return;
    }
    calls.forEach((call, callIndex) => {
      if (!isRecord(call) || ownDataValue(call, 'type') !== 'function') {
        rejected = true;
        return;
      }
      const identity = inspectToolCallId(call, true);
      const fn = ownDataValue(call, 'function');
      const toolName = isRecord(fn) ? exactString(ownDataValue(fn, 'name')) : undefined;
      if (identity.status !== 'exact' || !toolName) {
        rejected = true;
        return;
      }
      facets.push({
        callId: identity.value,
        toolName,
        input: exactToolInput(call),
        pointer: `/output/${outputIndex}/choices/0/message/tool_calls/${callIndex}`,
        native: call,
      });
    });
  });
  if (rejected) return null;
  if (facets.length === 0) return [];
  const callIds = facets.map((facet) => facet.callId);
  return new Set(callIds).size === callIds.length ? facets : null;
}

function exactToolInput(value: Record<string, unknown>): unknown {
  const direct = ownDataValue(value, 'arguments') ?? ownDataValue(value, 'input');
  if (direct !== undefined) return direct;
  const fn = ownDataValue(value, 'function');
  return isRecord(fn)
    ? ownDataValue(fn, 'arguments') ?? ownDataValue(fn, 'input')
    : undefined;
}

function exactUsage(value: unknown): Record<string, number> | undefined {
  if (!isRecord(value)) return undefined;
  const input = exactNonnegativeInteger(
    ownDataValue(value, 'input_tokens')
      ?? ownDataValue(value, 'inputTokens')
      ?? ownDataValue(value, 'prompt_tokens'),
  );
  const output = exactNonnegativeInteger(
    ownDataValue(value, 'output_tokens')
      ?? ownDataValue(value, 'outputTokens')
      ?? ownDataValue(value, 'completion_tokens'),
  );
  if (input === undefined && output === undefined) return undefined;
  return {
    ...(input !== undefined ? { input_tokens: input } : {}),
    ...(output !== undefined ? { output_tokens: output } : {}),
  };
}

function exactNonnegativeInteger(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
    ? value
    : undefined;
}

function exactErrorSemantic(
  value: unknown,
  type: string,
): Record<string, unknown> | undefined {
  if (!isRecord(value)) return undefined;
  const message = exactString(ownDataValue(value, 'message'));
  if (!message) return undefined;
  const data = ownDataValue(value, 'data');
  const code = isRecord(data) ? exactString(ownDataValue(data, 'code')) : undefined;
  return {
    type,
    message,
    recoverable: false,
    ...(code ? { code } : {}),
    ...(data !== undefined ? { details: data } : {}),
  };
}

function redundantSemantic(): Record<string, unknown> {
  return { type: 'capture.redundant', framework: 'openai-agents' };
}

function exactString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 && value.trim() === value
    ? value
    : undefined;
}

function inspectToolCallId(
  value: Record<string, unknown>,
  allowGenericId: boolean,
): Readonly<{ status: 'missing' | 'invalid' }> | Readonly<{ status: 'exact'; value: string }> {
  const specificKeys = ['callId', 'call_id', 'toolCallId', 'tool_call_id'] as const;
  const present = specificKeys.filter((key) => Object.prototype.hasOwnProperty.call(value, key));
  const keys: readonly string[] = present.length > 0
    ? present
    : allowGenericId && Object.prototype.hasOwnProperty.call(value, 'id') ? ['id'] : [];
  if (keys.length === 0) return { status: 'missing' };
  const identities = keys.map((key) => ownDataValue(value, key));
  if (identities.some((identity) => typeof identity !== 'string' || identity.trim().length === 0)) {
    return { status: 'invalid' };
  }
  const distinct = new Set(identities as string[]);
  return distinct.size === 1
    ? { status: 'exact', value: identities[0] as string }
    : { status: 'invalid' };
}

function hasToolProposal(value: unknown, seen = new WeakSet<object>()): boolean {
  if (!value || typeof value !== 'object' || seen.has(value)) return false;
  seen.add(value);
  if (Array.isArray(value)) return value.some((item) => hasToolProposal(item, seen));
  const record = value as Record<string, unknown>;
  return ['function_call', 'tool_call', 'tool_use'].includes(String(record.type))
    || 'tool_calls' in record
    || Object.values(record).some((item) => hasToolProposal(item, seen));
}

function isAgentsSubject(value: object): value is AgentsSubject {
  const subject = value as Partial<AgentsSubject>;
  return typeof subject.addTraceProcessor === 'function' && typeof subject.run === 'function';
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object';
}
