import type {
  AdmissionReceipt,
  CaptureSource,
  SourceEventKind,
  SourceSink,
  TraceIdentity,
} from '../v1/types.js';

import { trustOfficialSource } from '../v1/source-ownership.js';
import { snapshotNative, snapshotRecord } from './native-snapshot.js';

type MastraAdapterOptions = { version?: string };
const EXACT_ROOT_SEMANTICS = Object.freeze({
  agent_run: 'agent.run',
  workflow_run: 'workflow.run',
} as const);
const EXACT_TOOL_SPAN_TYPES = new Set([
  'tool_call',
  'mcp_tool_call',
  'client_tool_call',
]);
const EXACT_WORKFLOW_STATE_SPAN_TYPES = new Set([
  'workflow_step',
  'workflow_conditional',
  'workflow_conditional_eval',
  'workflow_parallel',
  'workflow_loop',
  'workflow_sleep',
  'workflow_wait_event',
]);
type ExportedSpan = {
  id: string;
  traceId: string;
  parentSpanId?: string;
  name: string;
  type: string;
  isRootSpan: boolean;
  isEvent: boolean;
  startTime: Date;
  endTime?: Date;
  attributes?: Record<string, unknown>;
  input?: unknown;
  output?: unknown;
  errorInfo?: unknown;
  [key: string]: unknown;
};
type TracingEvent = { type: 'span_started' | 'span_updated' | 'span_ended'; exportedSpan: ExportedSpan };
type MastraExporter = {
  name: string;
  exportTracingEvent(event: TracingEvent): Promise<void>;
  flush(): Promise<void>;
  shutdown(): Promise<void>;
};
type Bridge = { active: boolean; sink?: SourceSink; pending: Set<Promise<void>> };
type OpenSpan = {
  name: string;
  start?: AdmissionReceipt;
  modelStepsStarted?: number;
  modelStepsTerminal?: number;
  modelAttributes?: Record<string, unknown>;
  modelContext?: ModelContext;
  modelRequest?: AdmissionReceipt;
  modelStepId?: string;
  lastModelRequest?: AdmissionReceipt;
  lastModelResponse?: AdmissionReceipt;
  toolCallId?: string;
};
type ModelContext = {
  identities: Array<string | undefined>;
  snapshots: unknown[];
  refs: Array<string | undefined>;
};
type NativeMessageIdentityState = {
  next: number;
  tokens: WeakMap<object, string>;
};
type OpenedTrace = { trace: TraceIdentity; start: AdmissionReceipt };
type PendingToolCall = Readonly<{ callId: string; toolName: string; input?: unknown }>;
type ToolCorrelation = {
  proposalKeys: Set<string>;
  pendingProposalKeys: Set<string>;
  streamStarts: Map<string, AdmissionReceipt>;
};
export type MastraCaptureAdapter = {
  /** Add this documented exporter to a Mastra observability configuration. */
  readonly exporter: MastraExporter;
  /** Install before invoking Mastra so the exporter has a live local sink. */
  readonly source: CaptureSource;
  /** Preserve an error yielded by Mastra's application-consumed stream on its native trace. */
  recordStreamError(runId: string, error: unknown): AdmissionReceipt;
  /** Preserve a part from the application-consumed Mastra stream on its native trace. */
  recordStreamPart(runId: string, part: unknown): AdmissionReceipt;
};

/** Adapter for Mastra's documented ObservabilityExporter span lifecycle. */
export function mastraAdapter(options: MastraAdapterOptions = {}): MastraCaptureAdapter {
  const bridge: Bridge = { active: false, pending: new Set() };
  const traces = new Map<string, TraceIdentity>();
  const spans = new Map<string, OpenSpan>();
  const failures = new Map<string, AdmissionReceipt>();
  const tools: ToolCorrelation = {
    proposalKeys: new Set(),
    pendingProposalKeys: new Set(),
    streamStarts: new Map(),
  };
  const uncorrelatedReasoningIds = new Set<string>();
  const messageIdentities: NativeMessageIdentityState = {
    next: 0,
    tokens: new WeakMap(),
  };

  const exporter: MastraExporter = Object.freeze({
    name: 'semantic-layer-local-capture',
    async exportTracingEvent(event) {
      if (!bridge.active || !bridge.sink) return;
      const nativeMessageIdentities = exactNativeMessageIdentities(event, messageIdentities);
      const captured = snapshotRecord(event);
      if (!isTracingEvent(captured)) return;
      captureTracingEvent(
        bridge,
        traces,
        spans,
        failures,
        tools,
        uncorrelatedReasoningIds,
        captured,
        nativeMessageIdentities,
      );
    },
    async flush() { await settlePending(bridge); },
    async shutdown() {
      bridge.active = false;
      await settlePending(bridge);
    },
  });

  const source = trustOfficialSource(Object.freeze<CaptureSource>({
    metadata: {
      name: 'official:mastra',
      seam: 'observability tracing/tool execute',
      identityDomain: 'mastra.run',
      ...(options.version ? { version: options.version } : {}),
      qualification: options.version === undefined
        ? { status: 'unknown' }
        : ['1.50.1', '1.50.0'].includes(options.version)
          ? { status: 'exact_qualified' }
          : { status: 'capability_checked_unqualified', profile: 'mastra-observability-exporter-v1' },
      official: true,
      coverage: [{ operation: 'agent-or-workflow-run', domain: 'mastra.run', role: 'owner' }],
    },
    install(sink) {
      if (bridge.active || bridge.sink) throw new Error('Mastra capture adapter is already installed');
      bridge.sink = sink;
      bridge.active = true;
      return {
        deactivate() { bridge.active = false; },
        async drain() {
          await settlePending(bridge);
          bridge.sink = undefined;
          traces.clear();
          spans.clear();
          failures.clear();
          tools.proposalKeys.clear();
          tools.pendingProposalKeys.clear();
          tools.streamStarts.clear();
          uncorrelatedReasoningIds.clear();
        },
      };
    },
  }), 'deep');

  function recordStreamError(runId: string, error: unknown): AdmissionReceipt {
    const trace = traces.get(runId);
    if (!bridge.active || !bridge.sink || !trace) {
      return { accepted: false, reason: 'Mastra trace is not active', settled: Promise.resolve() };
    }
    const capturedError = snapshotRecord(error);
    const semanticError = exactErrorSemantic(capturedError, 'stream_error');
    const receipt = bridge.sink.record({
      kind: 'error', phase: 'event', name: 'mastra.stream.error', trace,
      nativeIdentity: runId, native: capturedError,
      ...(typeof error === 'object' && error !== null ? { errorIdentity: error } : {}),
      semantic: {
        type: 'agent.error',
        framework: 'mastra',
        ...(semanticError ? { error: semanticError } : {}),
      },
    });
    track(bridge, receipt);
    return receipt;
  }

  function recordStreamPart(runId: string, part: unknown): AdmissionReceipt {
    const trace = traces.get(runId);
    if (!bridge.active || !bridge.sink || !trace) {
      return { accepted: false, reason: 'Mastra trace is not active', settled: Promise.resolve() };
    }
    const partRecord = snapshotRecord(part);
    const payload = recordField(partRecord, 'payload');
    const reasoningPart = exactStreamReasoningPart(partRecord);
    if (reasoningPart) {
      const evidence = bridge.sink.record({
        kind: 'state', phase: 'event', name: 'mastra.model.reasoning.uncorrelated', trace,
        nativeIdentity: reasoningPart.id,
        native: partRecord,
        semantic: {
          type: 'state.model_reasoning_stream',
          framework: 'mastra',
          value: partRecord,
        },
      });
      track(bridge, evidence);
      const correlationKey = `${trace.traceId}\u0000${reasoningPart.id}`;
      if (!uncorrelatedReasoningIds.has(correlationKey)) {
        uncorrelatedReasoningIds.add(correlationKey);
        const gap = bridge.sink.record({
          kind: 'unknown', phase: 'gap', name: 'mastra.model.reasoning.uncorrelated', trace,
          nativeIdentity: reasoningPart.id,
          native: partRecord,
          ...(evidence.accepted ? { parentRecordId: evidence.recordId } : {}),
          semantic: {
            type: 'capture.gap',
            framework: 'mastra',
            reason: 'mastra_reasoning_correlation_unavailable',
            count: 1,
            detail: 'The consumed stream-part seam did not expose an exact model identity.',
          },
        });
        track(bridge, gap);
      }
      if (reasoningPart.type === 'redacted-reasoning') {
        const redacted = bridge.sink.record({
          kind: 'unknown', phase: 'gap', name: 'mastra.model.reasoning.unavailable', trace,
          nativeIdentity: reasoningPart.id,
          native: partRecord,
          ...(evidence.accepted ? { parentRecordId: evidence.recordId } : {}),
          semantic: {
            type: 'capture.gap',
            framework: 'mastra',
            reason: 'reasoning_unavailable',
            count: 1,
            detail: 'Mastra exposed only a redacted reasoning carrier.',
          },
        });
        track(bridge, redacted);
      }
      return evidence;
    }
    const candidateToolPart = exactStreamToolPart(partRecord);
    const candidateLifecycleKey = candidateToolPart
      ? toolProposalKey(trace.traceId, candidateToolPart.callId)
      : undefined;
    const toolPart = candidateToolPart?.type === 'tool-call'
      ? exactClientToolCarrier(payload) ? candidateToolPart : undefined
      : candidateLifecycleKey && tools.streamStarts.has(candidateLifecycleKey)
        ? candidateToolPart
        : undefined;
    const toolDelta = partRecord.type === 'tool-call-delta'
      && typeof payload.argsTextDelta === 'string' ? payload.argsTextDelta : undefined;
    const failed = toolPart?.type === 'tool-result'
      && payload.isError === true;
    const lifecycleKey = toolPart ? toolProposalKey(trace.traceId, toolPart.callId) : undefined;
    const start = toolPart?.type === 'tool-result' && lifecycleKey
      ? tools.streamStarts.get(lifecycleKey) : undefined;
    const toolError = failed
      ? exactErrorSemantic(capturedValue(payload, 'result'), 'tool_error')
      : undefined;
    const receipt = bridge.sink.record({
      kind: toolPart ? 'tool' : 'stream',
      phase: toolPart?.type === 'tool-call' ? 'start'
        : failed ? 'error' : toolPart?.type === 'tool-result' ? 'end' : 'event',
      name: toolPart ? 'mastra.tool_call' : 'mastra.stream.part',
      trace,
      nativeIdentity: toolPart?.callId ?? runId,
      native: toolDelta === undefined ? partRecord
        : snapshotRecord({ type: 'tool-input-delta', delta: toolDelta, part: partRecord }),
      ...(start?.accepted ? { parentRecordId: start.recordId } : {}),
      semantic: toolPart ? {
        type: toolPart.type === 'tool-call' ? 'tool.execution'
          : failed ? 'tool.error' : 'tool.result',
        framework: 'mastra',
        call_id: toolPart.callId,
        native_call_id: toolPart.callId,
        name: toolPart.toolName,
        ...(toolPart.type === 'tool-call'
          ? { input: capturedValue(payload, 'args') ?? null }
          : {
              status: failed ? 'failed' : 'succeeded',
              output: capturedValue(payload, 'result') ?? null,
              ...(toolError ? { error: toolError } : {}),
            }),
      } : {
        ...redundantSemantic(),
      },
    });
    if (toolPart?.type === 'tool-call' && lifecycleKey) {
      tools.streamStarts.set(lifecycleKey, receipt);
    } else if (toolPart?.type === 'tool-result' && lifecycleKey) {
      tools.streamStarts.delete(lifecycleKey);
    }
    track(bridge, receipt);
    return receipt;
  }

  return Object.freeze({ exporter, source, recordStreamError, recordStreamPart });
}

function captureTracingEvent(
  bridge: Bridge,
  traces: Map<string, TraceIdentity>,
  spans: Map<string, OpenSpan>,
  failures: Map<string, AdmissionReceipt>,
  tools: ToolCorrelation,
  uncorrelatedReasoningIds: Set<string>,
  event: TracingEvent,
  nativeMessageIdentities?: Array<string | undefined>,
): void {
  const sink = bridge.sink!;
  const span = event.exportedSpan;
  const opened = traces.has(span.traceId) ? undefined : openTrace(sink, traces, span);
  const trace = traces.get(span.traceId) ?? opened?.trace;
  if (!trace) return;

  if (event.type === 'span_started') {
    if (span.isRootSpan === true) {
      track(bridge, sink.record({
        kind: 'correlation', phase: 'event', name: 'mastra.trace.native', trace,
        nativeIdentity: span.traceId, native: nativeEvent(event),
        ...(opened?.start.accepted ? { parentRecordId: opened.start.recordId } : {}),
        semantic: redundantSemantic(),
      }));
      spans.set(span.id, { name: span.name, start: opened?.start });
      return;
    }
    if (span.isEvent === true) {
      recordEventSpan(bridge, sink, trace, spans, event);
      return;
    }
    const parent = exactParentReceipt(spans, span);
    const name = EXACT_TOOL_SPAN_TYPES.has(span.type)
      ? `mastra.${span.type}.observability`
      : `mastra.${span.type}`;
    const failure = failures.get(span.traceId);
    const attempt = retryAttempt(capturedValue(span, 'input'))
      ?? retryAttempt(capturedValue(span, 'attributes'));
    const recovery = failure && attempt ? sink.record({
      kind: 'state', phase: 'event', name: 'mastra.recovery', trace,
      nativeIdentity: span.id,
      native: { attempt, recovering_span: nativeEvent(event) },
      ...(failure.accepted ? { parentRecordId: failure.recordId } : {}),
      semantic: { type: 'recovery.retry', framework: 'mastra' },
    }) : undefined;
    if (recovery) track(bridge, recovery);
    if (recovery) failures.delete(span.traceId);

    if (span.type === 'model_generation') {
      const start = recordRedundantSpan(bridge, sink, trace, event, parent);
      spans.set(span.id, {
        name,
        start,
        modelStepsStarted: 0,
        modelStepsTerminal: 0,
        modelAttributes: recordField(span, 'attributes'),
      });
      return;
    }

    if (span.type === 'model_step') {
      const generation = parentSpan(spans, span);
      if (generation) {
        generation.modelStepsStarted = (generation.modelStepsStarted ?? 0) + 1;
      }
      const start = recordRedundantSpan(bridge, sink, trace, event, parent);
      spans.set(span.id, { name, start });
      return;
    }

    if (span.type === 'model_inference') {
      const step = parentSpan(spans, span);
      if (step) {
        step.modelAttributes = {
          ...step.modelAttributes,
          ...recordField(span, 'attributes'),
        };
      }
      const start = recordRedundantSpan(bridge, sink, trace, event, parent);
      spans.set(span.id, {
        name,
        start,
        modelStepId: stringField(span, 'parentSpanId'),
      });
      return;
    }
    const toolCallId = EXACT_TOOL_SPAN_TYPES.has(span.type)
      ? exactToolCallIdentity(span)
      : undefined;
    const start = sink.record({
      kind: startKindForSpan(span, parent), phase: 'start', name, trace,
      nativeIdentity: span.id, native: nativeEvent(event),
      ...(parent?.accepted ? { parentRecordId: parent.recordId }
        : recovery?.accepted ? { parentRecordId: recovery.recordId } : {}),
      semantic: semanticForSpan(span, 'start', Boolean(parent?.accepted), toolCallId),
    });
    if (EXACT_TOOL_SPAN_TYPES.has(span.type)) {
      recordToolCorrelationGap(bridge, sink, trace, tools, span, parent);
    }
    track(bridge, start);
    spans.set(span.id, { name, start, ...(toolCallId ? { toolCallId } : {}) });
    return;
  }

  if (event.type === 'span_updated') {
    const open = spans.get(span.id);
    if (span.type === 'model_generation') {
      if (open) {
        open.modelAttributes = {
          ...open.modelAttributes,
          ...recordField(span, 'attributes'),
        };
      }
      recordRedundantSpan(bridge, sink, trace, event, open?.start);
      return;
    }
    if (span.type === 'agent_run' || span.type === 'model_inference') {
      recordRedundantSpan(bridge, sink, trace, event, open?.start);
      return;
    }
    if (span.type === 'model_step') {
      if (!open?.modelRequest) {
        if (!modelMessages(span)) {
          recordRedundantSpan(bridge, sink, trace, event, open?.start);
          return;
        }
        const generation = parentSpan(spans, span);
        const context = recordModelContext(
          sink,
          trace,
          span,
          generation?.start,
          generation?.modelContext,
          nativeMessageIdentities,
        );
        if (generation) generation.modelContext = context.context;
        const requestSpan = withModelAttributes(span, {
          ...generation?.modelAttributes,
          ...open?.modelAttributes,
        });
        const request = sink.record({
          kind: 'model', phase: 'start', name: 'mastra.model.request', trace,
          nativeIdentity: span.id, native: nativeEvent(event),
          ...(generation?.start?.accepted
            ? { parentRecordId: generation.start.recordId }
            : {}),
          semantic: modelRequestSemantic(requestSpan, context.refs),
        });
        track(bridge, request);
        if (open) {
          open.start = request;
          open.modelRequest = request;
        }
        if (generation) generation.lastModelRequest = request;
      } else {
        recordRedundantSpan(bridge, sink, trace, event, open.modelRequest);
      }
      return;
    }
    const value = capturedValue(span, 'output');
    track(bridge, sink.record({
      kind: kindForSpan(span.type), phase: 'event', name: `mastra.${span.type}.updated`, trace,
      nativeIdentity: span.id, native: nativeEvent(event),
      ...(open?.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
      semantic: EXACT_WORKFLOW_STATE_SPAN_TYPES.has(span.type) && open?.start?.accepted
        ? {
            type: `state.${span.type}`,
            framework: 'mastra',
            ...(value !== undefined ? { value } : {}),
          }
        : semanticForSpan(span, 'update', false),
    }));
    return;
  }

  const open = spans.get(span.id);
  const errorEvidence = capturedValue(span, 'errorInfo');
  const outputEvidence = capturedValue(span, 'output');
  const failed = errorEvidence !== undefined && errorEvidence !== null;
  const root = span.isRootSpan === true;
  const name = open?.name ?? `mastra.${span.type}`;
  if (span.type === 'model_chunk' && outputEvidence !== undefined) {
    const output = recordField(span, 'output');
    track(bridge, sink.record({
      kind: 'stream', phase: 'event', name: 'mastra.model.stream.delta', trace,
      nativeIdentity: stringField(span, 'parentSpanId') ?? span.id,
      native: snapshotRecord({
        ...(typeof output.text === 'string' ? { text_delta: output.text } : {}),
        event: nativeEvent(event),
      }),
      ...(open?.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
      semantic: redundantSemantic(),
    }));
    const proposal = exactChunkToolCall(output);
    if (proposal && registerToolProposal(tools, span.traceId, proposal)) {
      const request = modelRequestForChunk(spans, span);
      track(bridge, sink.record({
        kind: 'tool', phase: 'event', name: 'mastra.tool.proposal', trace,
        nativeIdentity: proposal.callId, native: nativeEvent(event),
        ...(request?.accepted ? { parentRecordId: request.recordId } : {}),
        semantic: {
          type: 'tool.proposal',
          framework: 'mastra',
          call_id: proposal.callId,
          native_call_id: proposal.callId,
          name: proposal.toolName,
          input: proposal.input,
        },
      }));
    }
  }
  if (root) {
    const end = sink.record({
      kind: 'lifecycle',
      phase: failed ? 'error' : 'end',
      name,
      trace,
      nativeIdentity: span.traceId,
      native: nativeEvent(event),
      ...(open?.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
      semantic: rootOutcomeSemantic(span, failed),
    });
    track(bridge, end);
    failures.delete(span.traceId);
    clearToolCorrelation(tools, span.traceId);
    clearUncorrelatedReasoning(uncorrelatedReasoningIds, trace.traceId);
    clearTraceAliases(traces, trace);
    spans.delete(span.id);
    return;
  }

  if (span.type === 'model_generation') {
    const started = open?.modelStepsStarted ?? 0;
    const terminal = open?.modelStepsTerminal ?? 0;
    if (started > 0 && terminal === started) {
      if (modelResponseStatus(span, failed) === 'failed'
        && open?.lastModelResponse) {
        recordModelError(
          bridge,
          sink,
          trace,
          event,
          span,
          failed,
          open.lastModelResponse,
        );
      }
      recordRedundantSpan(bridge, sink, trace, event, open?.start);
      spans.delete(span.id);
      return;
    }
    let request = open?.lastModelRequest;
    if (!request) {
      const context = recordModelContext(sink, trace, span, open?.start);
      request = sink.record({
        kind: 'model', phase: 'start', name: 'mastra.model.request', trace,
        nativeIdentity: span.id, native: nativeEvent(event),
        ...(open?.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
        semantic: modelRequestSemantic(span, context.refs),
      });
      track(bridge, request);
    }
    const response = recordModelResponse(
      bridge,
      sink,
      trace,
      event,
      span,
      failed,
      request,
    );
    recordModelError(bridge, sink, trace, event, span, failed, response);
    spans.delete(span.id);
    return;
  }

  if (span.type === 'model_inference') {
    recordRedundantSpan(bridge, sink, trace, event, open?.start);
    spans.delete(span.id);
    return;
  }

  if (span.type === 'model_step') {
    let request = open?.modelRequest;
    if (!request) {
      const generation = parentSpan(spans, span);
      const context = recordModelContext(
        sink,
        trace,
        span,
        generation?.start,
        generation?.modelContext,
        nativeMessageIdentities,
      );
      if (generation) generation.modelContext = context.context;
      const requestSpan = withModelAttributes(span, {
        ...generation?.modelAttributes,
        ...open?.modelAttributes,
      });
      request = sink.record({
        kind: 'model', phase: 'start', name: 'mastra.model.request', trace,
        nativeIdentity: span.id, native: nativeEvent(event),
        ...(generation?.start?.accepted
          ? { parentRecordId: generation.start.recordId }
          : {}),
        semantic: modelRequestSemantic(requestSpan, context.refs),
      });
      track(bridge, request);
      if (generation) generation.lastModelRequest = request;
    }
    const generation = parentSpan(spans, span);
    const responseSpan = withModelAttributes(span, {
      ...generation?.modelAttributes,
      ...open?.modelAttributes,
    });
    const response = recordModelResponse(
      bridge,
      sink,
      trace,
      event,
      responseSpan,
      failed,
      request,
    );
    recordModelError(bridge, sink, trace, event, responseSpan, failed, response);
    if (generation) {
      generation.modelStepsTerminal = (generation.modelStepsTerminal ?? 0) + 1;
      generation.lastModelResponse = response;
    }
    spans.delete(span.id);
    return;
  }

  const end = sink.record({
    kind: endKindForSpan(span, open), phase: failed ? 'error' : 'end', name, trace,
    nativeIdentity: span.id, native: nativeEvent(event),
    ...(open?.start?.accepted ? { parentRecordId: open.start.recordId } : {}),
    semantic: failed
      ? errorSemanticForSpan(span, Boolean(open?.start?.accepted), open?.toolCallId)
      : semanticForSpan(span, 'end', Boolean(open?.start?.accepted), open?.toolCallId),
  });
  track(bridge, end);
  if (failed && !root) {
    const errorInfo = recordField(span, 'errorInfo');
    const failure = sink.record({
      kind: 'error', phase: 'event', name: 'mastra.span.error', trace,
      nativeIdentity: span.id,
      native: snapshotRecord({ ...errorInfo, event: nativeEvent(event), error: errorEvidence }),
      ...(end.accepted ? { parentRecordId: end.recordId } : {}),
      semantic: EXACT_TOOL_SPAN_TYPES.has(span.type)
        || EXACT_WORKFLOW_STATE_SPAN_TYPES.has(span.type)
        ? redundantSemantic()
        : {
            type: 'agent.error',
            framework: 'mastra',
            ...(exactErrorSemantic(errorEvidence, 'agent_error')
              ? { error: exactErrorSemantic(errorEvidence, 'agent_error') }
              : {}),
          },
    });
    track(bridge, failure);
    failures.set(span.traceId, failure);
  }
  spans.delete(span.id);
}

function recordEventSpan(
  bridge: Bridge,
  sink: SourceSink,
  trace: TraceIdentity,
  spans: Map<string, OpenSpan>,
  event: TracingEvent,
): void {
  const span = event.exportedSpan;
  const stream = span.type === 'model_chunk';
  const parent = exactParentReceipt(spans, span);
  track(bridge, sink.record({
    kind: stream ? 'stream' : kindForSpan(span.type), phase: 'event',
    name: stream ? 'mastra.model.stream.delta' : `mastra.${span.type}`,
    trace, nativeIdentity: stream ? stringField(span, 'parentSpanId') ?? span.id : span.id,
    native: nativeEvent(event),
    ...(parent?.accepted ? { parentRecordId: parent.recordId } : {}),
    semantic: stream ? redundantSemantic() : { type: 'native.event', framework: 'mastra' },
  }));
}

function recordRedundantSpan(
  bridge: Bridge,
  sink: SourceSink,
  trace: TraceIdentity,
  event: TracingEvent,
  parent?: AdmissionReceipt,
): AdmissionReceipt {
  const receipt = sink.record({
    kind: kindForSpan(event.exportedSpan.type),
    phase: event.type === 'span_started' ? 'start'
      : event.type === 'span_ended' ? 'end' : 'event',
    name: `mastra.${event.exportedSpan.type}`,
    trace,
    nativeIdentity: event.exportedSpan.id,
    native: nativeEvent(event),
    ...(parent?.accepted ? { parentRecordId: parent.recordId } : {}),
    semantic: redundantSemantic(),
  });
  track(bridge, receipt);
  return receipt;
}

function recordModelResponse(
  bridge: Bridge,
  sink: SourceSink,
  trace: TraceIdentity,
  event: TracingEvent,
  span: ExportedSpan,
  failed: boolean,
  request?: AdmissionReceipt,
): AdmissionReceipt {
  const status = modelResponseStatus(span, failed);
  const response = sink.record({
    kind: 'model',
    phase: status === 'failed' ? 'error'
      : status === 'cancelled' ? 'cancelled' : 'end',
    name: 'mastra.model.response',
    trace,
    nativeIdentity: span.id,
    native: nativeEvent(event),
    ...(request?.accepted ? { parentRecordId: request.recordId } : {}),
    semantic: modelResponseSemantic(span, failed),
  });
  track(bridge, response);
  const unavailableReasoning = mastraResponseEvidence(capturedValue(span, 'output'))
    .unavailableReasoning;
  if (unavailableReasoning > 0) {
    track(bridge, sink.record({
      kind: 'unknown',
      phase: 'gap',
      name: 'mastra.model.reasoning.unavailable',
      trace,
      native: { model_span_id: span.id, unavailable_reasoning_blocks: unavailableReasoning },
      ...(response.accepted ? { parentRecordId: response.recordId } : {}),
      semantic: {
        type: 'capture.gap',
        framework: 'mastra',
        reason: 'reasoning_unavailable',
        count: unavailableReasoning,
        detail: 'Mastra exposed only redacted reasoning blocks.',
      },
    }));
  }
  return response;
}

function recordModelError(
  bridge: Bridge,
  sink: SourceSink,
  trace: TraceIdentity,
  event: TracingEvent,
  span: ExportedSpan,
  failed: boolean,
  response: AdmissionReceipt,
): void {
  const error = failed
    ? exactErrorSemantic(capturedValue(span, 'errorInfo'), 'model_error')
    : undefined;
  if (!error) return;
  track(bridge, sink.record({
    kind: 'error',
    phase: 'event',
    name: 'mastra.model.error',
    trace,
    nativeIdentity: span.id,
    native: nativeEvent(event),
    ...(response.accepted ? { parentRecordId: response.recordId } : {}),
    semantic: { type: 'agent.error', framework: 'mastra', error },
  }));
}

function openTrace(
  sink: SourceSink,
  traces: Map<string, TraceIdentity>,
  span: ExportedSpan,
): OpenedTrace | undefined {
  const attributes = recordField(span, 'attributes');
  const metadata = recordField(span, 'metadata');
  const rootSemantic = rootSemanticForSpan(span);
  const opened = sink.openTrace({
    name: span.isRootSpan === true ? span.name : 'mastra.trace',
    nativeIdentity: span.traceId,
    conversationId: stringField(metadata, 'conversationId')
      ?? stringField(attributes, 'conversationId') ?? stringField(metadata, 'threadId'),
    turnId: stringField(metadata, 'turnId'),
    turnIndex: integerField(metadata, 'turnIndex'),
    previousTurnId: stringField(metadata, 'previousTurnId'),
    native: snapshotRecord({ span }),
    ...(rootSemantic ? { semantic: rootSemantic } : {}),
  });
  if (!opened.accepted) return undefined;
  traces.set(span.traceId, opened.identity);
  const runId = stringField(metadata, 'runId');
  if (runId) traces.set(runId, opened.identity);
  return { trace: opened.identity, start: opened };
}

function rootSemanticForSpan(span: ExportedSpan): Record<string, unknown> | undefined {
  if (span.isRootSpan !== true) return undefined;
  const type = exactRootType(span.type);
  const input = capturedValue(span, 'input');
  return type ? {
    type,
    framework: 'mastra',
    name: span.name,
    ...(input !== undefined ? { input } : {}),
  } : undefined;
}

function kindForSpan(type: string): SourceEventKind {
  if (['model_generation', 'model_step', 'model_inference'].includes(type)) return 'model';
  if (['tool_call', 'mcp_tool_call', 'client_tool_call'].includes(type)) return 'tool';
  if (type === 'model_chunk') return 'stream';
  if (type === 'agent_run') return 'lifecycle';
  return 'state';
}

function startKindForSpan(span: ExportedSpan, parent?: AdmissionReceipt): SourceEventKind {
  return EXACT_WORKFLOW_STATE_SPAN_TYPES.has(span.type) && parent?.accepted
    ? 'lifecycle'
    : kindForSpan(span.type);
}

function endKindForSpan(span: ExportedSpan, open?: OpenSpan): SourceEventKind {
  return EXACT_WORKFLOW_STATE_SPAN_TYPES.has(span.type) && open?.start?.accepted
    ? 'lifecycle'
    : kindForSpan(span.type);
}

function semanticForSpan(
  span: ExportedSpan,
  phase: 'start' | 'update' | 'end',
  exactParent: boolean,
  toolCallId?: string,
): Record<string, unknown> {
  if (EXACT_TOOL_SPAN_TYPES.has(span.type)) {
    return phase === 'start'
      ? toolCallSemantic(span, toolCallId)
      : toolResultSemantic(span, false, toolCallId);
  }
  if (span.type === 'model_chunk') return redundantSemantic();
  if (EXACT_WORKFLOW_STATE_SPAN_TYPES.has(span.type) && exactParent) {
    return {
      type: 'workflow.step',
      framework: 'mastra',
      scope_type: 'step',
      scope_id: `mastra:${span.id}`,
      name: span.name,
      ...(phase === 'end' ? { status: exactSpanStatus(span) } : {}),
    };
  }
  if (span.type === 'memory_operation') {
    const value = phase === 'start'
      ? capturedValue(span, 'input')
      : capturedValue(span, 'output');
    return {
      type: 'state.transition',
      framework: 'mastra',
      ...(value !== undefined ? { value } : {}),
    };
  }
  return { type: 'native.event', framework: 'mastra' };
}

function errorSemanticForSpan(
  span: ExportedSpan,
  exactParent: boolean,
  toolCallId?: string,
): Record<string, unknown> {
  if (EXACT_TOOL_SPAN_TYPES.has(span.type)) return toolResultSemantic(span, true, toolCallId);
  if (EXACT_WORKFLOW_STATE_SPAN_TYPES.has(span.type) && exactParent) {
    const error = exactErrorSemantic(capturedValue(span, 'errorInfo'), 'workflow_step_error');
    return {
      type: 'workflow.step',
      framework: 'mastra',
      scope_type: 'step',
      scope_id: `mastra:${span.id}`,
      status: 'failed',
      ...(error ? { error } : {}),
    };
  }
  return { type: 'agent.error', framework: 'mastra' };
}

function recordModelContext(
  sink: SourceSink,
  trace: TraceIdentity,
  span: ExportedSpan,
  parent?: AdmissionReceipt,
  previous?: ModelContext,
  nativeMessageIdentities?: Array<string | undefined>,
): { refs: string[]; context: ModelContext } {
  const messages = modelMessages(span);
  if (!Array.isArray(messages)) {
    return {
      refs: [],
      context: previous ?? { identities: [], snapshots: [], refs: [] },
    };
  }
  const identities = nativeMessageIdentities ?? messages.map(() => undefined);
  const snapshots = messages.map((message) => snapshotNative(message));
  const refs: Array<string | undefined> = [];
  let unchangedPrefix = previous !== undefined;
  messages.forEach((message, index) => {
    if (unchangedPrefix
      && previous
      && identities[index] !== undefined
      && identities[index] === previous.identities[index]
      && sameFiniteSnapshot(snapshots[index], previous.snapshots[index])
      && previous.refs[index] !== undefined) {
      refs[index] = previous.refs[index];
      return;
    }
    unchangedPrefix = false;
    if (!message || typeof message !== 'object' || Array.isArray(message)) return;
    const captured = message as Record<string, unknown>;
    const role = captured.role;
    if (!['system', 'developer', 'user', 'assistant', 'tool'].includes(String(role))
      || !Object.prototype.hasOwnProperty.call(captured, 'content')) return;
    const receipt = sink.record({
      kind: 'model',
      phase: 'event',
      name: 'mastra.model.context',
      trace,
      nativeIdentity: `${span.id}:context:${index}`,
      native: captured,
      ...(parent?.accepted ? { parentRecordId: parent.recordId } : {}),
      semantic: {
        type: 'message',
        framework: 'mastra',
        role,
        content: captured.content,
      },
    });
    if (receipt.accepted) refs[index] = receipt.recordId;
  });
  return {
    refs: refs.filter((ref): ref is string => ref !== undefined),
    context: { identities, snapshots, refs },
  };
}

function sameFiniteSnapshot(left: unknown, right: unknown): boolean {
  try {
    return JSON.stringify(left) === JSON.stringify(right);
  } catch {
    return false;
  }
}

function exactNativeMessageIdentities(
  event: TracingEvent,
  state: NativeMessageIdentityState,
): Array<string | undefined> | undefined {
  const exportedSpan = ownDataValue(event, 'exportedSpan');
  if (!exportedSpan || typeof exportedSpan !== 'object') return undefined;
  const messages = nativeMessages(exportedSpan as ExportedSpan);
  if (!messages) return undefined;
  return Array.from({ length: messages.length }, (_, index) => {
    const message = ownDataValue(messages, String(index));
    if (!message || typeof message !== 'object') return undefined;
    const existing = state.tokens.get(message);
    if (existing) return existing;
    const identity = `message_${state.next}`;
    state.next += 1;
    state.tokens.set(message, identity);
    return identity;
  });
}

function nativeMessages(span: ExportedSpan): unknown[] | undefined {
  const input = ownDataValue(span, 'input');
  const messages = Array.isArray(input)
    ? input
    : ownDataValue(input, 'messages');
  return Array.isArray(messages) ? messages : undefined;
}

function ownDataValue(value: unknown, key: string): unknown {
  if (!value || typeof value !== 'object') return undefined;
  try {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && 'value' in descriptor ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

function modelMessages(span: ExportedSpan): unknown[] | undefined {
  const input = capturedValue(span, 'input');
  const messages = input && typeof input === 'object' && !Array.isArray(input)
    ? capturedValue(input as Record<string, unknown>, 'messages')
    : Array.isArray(input) ? input : undefined;
  return Array.isArray(messages) ? messages : undefined;
}

function modelRequestSemantic(
  span: ExportedSpan,
  contextRefs: string[],
): Record<string, unknown> {
  const attributes = recordField(span, 'attributes');
  const tools = exactStringArray(capturedValue(attributes, 'availableTools'));
  return {
    type: 'model.request',
    framework: 'mastra',
    context_refs: contextRefs,
    ...(nonemptyString(attributes.model) ? { model: attributes.model } : {}),
    ...(tools.length ? { tools } : {}),
  };
}

function modelResponseSemantic(
  span: ExportedSpan,
  failed: boolean,
): Record<string, unknown> {
  const attributes = recordField(span, 'attributes');
  const output = capturedValue(span, 'output');
  const response = mastraResponseEvidence(output);
  const usage = exactUsage(capturedValue(attributes, 'usage'));
  return {
    type: 'model.response',
    framework: 'mastra',
    status: modelResponseStatus(span, failed),
    ...(nonemptyString(attributes.responseModel)
      ? { model: attributes.responseModel }
      : nonemptyString(attributes.model) ? { model: attributes.model } : {}),
    ...(response.content !== undefined ? { content: response.content } : {}),
    ...(response.reasoning.length ? { reasoning: response.reasoning } : {}),
    ...(nonemptyString(attributes.finishReason)
      ? { finish_reason: attributes.finishReason }
      : {}),
    ...(usage ? { usage } : {}),
  };
}

function mastraResponseEvidence(value: unknown): {
  content?: unknown;
  reasoning: Array<{ type: 'text' | 'summary'; text: string }>;
  unavailableReasoning: number;
} {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    return {
      ...(value !== undefined ? { content: value } : {}),
      reasoning: [],
      unavailableReasoning: 0,
    };
  }
  const record = value as Record<string, unknown>;
  const content = Object.fromEntries(Object.entries(record).filter(([key]) => (
    !['reasoning', 'reasoningText', 'reasoningDetails'].includes(key)
  )));
  const reasoning: Array<{ type: 'text' | 'summary'; text: string }> = [];
  let unavailableReasoning = 0;
  const details = Array.isArray(record.reasoningDetails)
    ? record.reasoningDetails
    : Array.isArray(record.reasoning) ? record.reasoning : [];
  for (const detail of details) {
    if (!detail || typeof detail !== 'object' || Array.isArray(detail)) continue;
    const block = detail as Record<string, unknown>;
    const payload = recordField(block, 'payload');
    if (block.type === 'reasoning' && typeof payload.text === 'string' && payload.text.length) {
      reasoning.push({ type: 'text', text: payload.text });
    } else if (block.type === 'redacted-reasoning') unavailableReasoning += 1;
    else if (block.type === 'text' && typeof block.text === 'string' && block.text.length) {
      reasoning.push({ type: 'text', text: block.text });
    }
  }
  if (!reasoning.length) {
    const text = typeof record.reasoning === 'string'
      ? record.reasoning
      : typeof record.reasoningText === 'string' ? record.reasoningText : undefined;
    if (text?.length) reasoning.push({ type: 'text', text });
  }
  return {
    ...(Object.keys(content).length ? { content } : {}),
    reasoning,
    unavailableReasoning,
  };
}

function modelResponseStatus(
  span: ExportedSpan,
  failed: boolean,
): 'completed' | 'incomplete' | 'failed' | 'cancelled' {
  if (failed) return 'failed';
  const finishReason = capturedValue(recordField(span, 'attributes'), 'finishReason');
  if (finishReason === 'error') return 'failed';
  if (finishReason === 'length' || finishReason === 'content-filter') return 'incomplete';
  if (finishReason === 'abort' || finishReason === 'aborted') return 'cancelled';
  return 'completed';
}

function toolCallSemantic(
  span: ExportedSpan,
  exactCallId?: string,
): Record<string, unknown> {
  const callId = exactCallId ?? exactToolCallIdentity(span);
  return {
    type: 'tool.execution',
    framework: 'mastra',
    call_id: callId,
    native_call_id: callId,
    name: exactToolName(span),
    input: capturedValue(span, 'input') ?? null,
  };
}

function toolResultSemantic(
  span: ExportedSpan,
  failed: boolean,
  exactCallId?: string,
): Record<string, unknown> {
  const callId = exactCallId ?? exactToolCallIdentity(span);
  const output = capturedValue(span, 'output');
  const error = failed
    ? exactErrorSemantic(capturedValue(span, 'errorInfo'), 'tool_error')
    : undefined;
  return {
    type: failed ? 'tool.error' : 'tool.result',
    framework: 'mastra',
    call_id: callId,
    native_call_id: callId,
    status: failed ? 'failed' : exactToolStatus(span),
    ...(output !== undefined ? { output } : {}),
    ...(error ? { error } : {}),
  };
}

function exactToolCallIdentity(span: ExportedSpan): string {
  const attributes = recordField(span, 'attributes');
  const candidates = [
    capturedValue(span, 'toolCallId'),
    capturedValue(span, 'tool_call_id'),
    capturedValue(attributes, 'toolCallId'),
    capturedValue(attributes, 'tool_call_id'),
  ];
  return candidates.find(nonemptyString) ?? span.id;
}

function rootOutcomeSemantic(
  span: ExportedSpan,
  failed: boolean,
): Record<string, unknown> {
  const output = capturedValue(span, 'output');
  const error = failed
    ? exactErrorSemantic(capturedValue(span, 'errorInfo'), 'agent_error')
    : undefined;
  return {
    type: exactRootType(span.type) ?? 'agent.run',
    framework: 'mastra',
    status: failed ? 'failed' : exactRootStatus(span),
    ...(output !== undefined ? { output } : {}),
    ...(error ? { error } : {}),
  };
}

function exactRootType(type: string): typeof EXACT_ROOT_SEMANTICS[keyof typeof EXACT_ROOT_SEMANTICS]
  | undefined {
  return Object.prototype.hasOwnProperty.call(EXACT_ROOT_SEMANTICS, type)
    ? EXACT_ROOT_SEMANTICS[type as keyof typeof EXACT_ROOT_SEMANTICS]
    : undefined;
}

function exactParentReceipt(
  spans: Map<string, OpenSpan>,
  span: ExportedSpan,
): AdmissionReceipt | undefined {
  const parentId = stringField(span, 'parentSpanId');
  return parentId ? spans.get(parentId)?.start : undefined;
}

function parentSpan(
  spans: Map<string, OpenSpan>,
  span: ExportedSpan,
): OpenSpan | undefined {
  const parentId = stringField(span, 'parentSpanId');
  return parentId ? spans.get(parentId) : undefined;
}

function modelRequestForChunk(
  spans: Map<string, OpenSpan>,
  span: ExportedSpan,
): AdmissionReceipt | undefined {
  const inference = parentSpan(spans, span);
  return inference?.modelStepId
    ? spans.get(inference.modelStepId)?.modelRequest
    : undefined;
}

function withModelAttributes(
  span: ExportedSpan,
  attributes?: Record<string, unknown>,
): ExportedSpan {
  return {
    ...span,
    attributes: {
      ...attributes,
      ...recordField(span, 'attributes'),
    },
  };
}

function exactToolName(span: ExportedSpan): string {
  return nonemptyString(span.entityName)
    ? span.entityName
    : nonemptyString(span.entityId) ? span.entityId : span.name;
}

function exactToolStatus(span: ExportedSpan): 'succeeded' | 'failed' {
  return capturedValue(recordField(span, 'attributes'), 'success') === false
    ? 'failed'
    : 'succeeded';
}

function exactSpanStatus(
  span: ExportedSpan,
): 'succeeded' | 'failed' | 'cancelled' | 'interrupted' | 'unknown' {
  const status = capturedValue(recordField(span, 'attributes'), 'status');
  if (status === 'success' || status === 'succeeded') return 'succeeded';
  if (status === 'failed' || status === 'tripwire' || status === 'bailed') return 'failed';
  if (status === 'canceled' || status === 'cancelled') return 'cancelled';
  if (status === 'suspended' || status === 'paused') return 'interrupted';
  if (status !== undefined) return 'unknown';
  return 'succeeded';
}

function exactRootStatus(
  span: ExportedSpan,
): 'succeeded' | 'failed' | 'cancelled' | 'unknown' {
  const status = exactSpanStatus(span);
  return status === 'interrupted' ? 'unknown' : status;
}

function exactUsage(value: unknown): Record<string, number> | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const usage = value as Record<string, unknown>;
  const input = exactNonnegativeInteger(usage.inputTokens)
    ?? exactNonnegativeInteger(usage.input_tokens);
  const output = exactNonnegativeInteger(usage.outputTokens)
    ?? exactNonnegativeInteger(usage.output_tokens);
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

function exactStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.filter((item): item is string => nonemptyString(item));
}

function exactErrorSemantic(
  value: unknown,
  fallbackType: string,
): Record<string, unknown> | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const error = value as Record<string, unknown>;
  if (!nonemptyString(error.message)) return undefined;
  return {
    type: normalizedErrorType(error.name, fallbackType),
    message: error.message,
    recoverable: false,
    ...(nonemptyString(error.id)
      ? { code: error.id }
      : nonemptyString(error.code) ? { code: error.code } : {}),
    ...(error.details !== undefined ? { details: error.details } : {}),
  };
}

function normalizedErrorType(value: unknown, fallback: string): string {
  if (!nonemptyString(value)) return fallback;
  const normalized = value
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '_')
    .replace(/^[^a-z]+/, '')
    .slice(0, 128);
  return /^[a-z][a-z0-9._-]{2,127}$/.test(normalized) ? normalized : fallback;
}

function redundantSemantic(): Record<string, unknown> {
  return { type: 'capture.redundant', framework: 'mastra' };
}

function nativeEvent(event: TracingEvent): Record<string, unknown> {
  return snapshotRecord({ type: event.type, exported_span: event.exportedSpan });
}

function exactChunkToolCall(value: Record<string, unknown>): PendingToolCall | undefined {
  const callId = nonemptyString(value.toolCallId) ? value.toolCallId : undefined;
  const toolName = nonemptyString(value.toolName) ? value.toolName : undefined;
  const input = capturedValue(value, 'toolInput');
  return callId && toolName && input !== undefined ? { callId, toolName, input } : undefined;
}

function registerToolProposal(
  tools: ToolCorrelation,
  traceId: string,
  proposal: PendingToolCall,
): boolean {
  const key = toolProposalKey(traceId, proposal.callId);
  if (tools.proposalKeys.has(key)) return false;
  tools.proposalKeys.add(key);
  tools.pendingProposalKeys.add(key);
  return true;
}

function recordToolCorrelationGap(
  bridge: Bridge,
  sink: SourceSink,
  trace: TraceIdentity,
  tools: ToolCorrelation,
  span: ExportedSpan,
  parent?: AdmissionReceipt,
): void {
  const callId = exactToolCallIdentity(span);
  const exactKey = toolProposalKey(span.traceId, callId);
  if (tools.pendingProposalKeys.delete(exactKey)) return;
  const prefix = `${span.traceId}\u0000`;
  const pending = [...tools.pendingProposalKeys].filter((key) => key.startsWith(prefix));
  if (pending.length === 0) return;
  track(bridge, sink.record({
    kind: 'unknown',
    phase: 'gap',
    name: 'mastra.tool.correlation.unavailable',
    trace,
    nativeIdentity: span.id,
    native: {
      tool_span_id: span.id,
      exact_call_id: callId,
      pending_proposal_count: pending.length,
    },
    ...(parent?.accepted ? { parentRecordId: parent.recordId } : {}),
    semantic: {
      type: 'capture.gap',
      framework: 'mastra',
      reason: 'mastra_tool_correlation_unavailable',
      count: 1,
      detail: 'Mastra exposed no exact shared identifier between the tool proposal and tool span.',
    },
  }));
}

function clearToolCorrelation(tools: ToolCorrelation, traceId: string): void {
  const prefix = `${traceId}\u0000`;
  for (const key of tools.proposalKeys) {
    if (key.startsWith(prefix)) tools.proposalKeys.delete(key);
  }
  for (const key of tools.pendingProposalKeys) {
    if (key.startsWith(prefix)) tools.pendingProposalKeys.delete(key);
  }
  for (const key of tools.streamStarts.keys()) {
    if (key.startsWith(prefix)) tools.streamStarts.delete(key);
  }
}

function clearTraceAliases(
  traces: Map<string, TraceIdentity>,
  trace: TraceIdentity,
): void {
  for (const [key, candidate] of traces) {
    if (candidate === trace) traces.delete(key);
  }
}

function toolProposalKey(traceId: string, callId: string): string {
  return `${traceId}\u0000${callId}`;
}

function exactStreamToolPart(
  part: Record<string, unknown>,
): (PendingToolCall & { type: 'tool-call' | 'tool-result' }) | undefined {
  if (part.type !== 'tool-call' && part.type !== 'tool-result') return undefined;
  const payload = recordField(part, 'payload');
  const callId = nonemptyString(payload.toolCallId) ? payload.toolCallId : undefined;
  const toolName = nonemptyString(payload.toolName) ? payload.toolName : undefined;
  if (!callId || !toolName) return undefined;
  if (part.type === 'tool-result'
    && !Object.prototype.hasOwnProperty.call(payload, 'result')) return undefined;
  return { type: part.type, callId, toolName };
}

function exactStreamReasoningPart(
  part: Record<string, unknown>,
):
  | { type: 'reasoning-start' | 'reasoning-end' | 'redacted-reasoning'; id: string }
  | { type: 'reasoning-delta'; id: string; text: string }
  | undefined {
  if (!['reasoning-start', 'reasoning-delta', 'reasoning-end', 'redacted-reasoning']
    .includes(String(part.type))) return undefined;
  const payload = recordField(part, 'payload');
  const id = nonemptyString(payload.id) ? payload.id : undefined;
  if (!id) return undefined;
  if (part.type === 'reasoning-delta') {
    return typeof payload.text === 'string'
      ? { type: 'reasoning-delta', id, text: payload.text }
      : undefined;
  }
  return { type: part.type as 'reasoning-start' | 'reasoning-end' | 'redacted-reasoning', id };
}

function clearUncorrelatedReasoning(ids: Set<string>, traceId: string): void {
  const prefix = `${traceId}\u0000`;
  for (const id of ids) {
    if (id.startsWith(prefix)) ids.delete(id);
  }
}

function exactClientToolCarrier(payload: Record<string, unknown>): boolean {
  const carrier = capturedValue(payload, 'observability');
  return carrier !== null && typeof carrier === 'object' && !Array.isArray(carrier);
}

function nonemptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function isTracingEvent(value: unknown): value is TracingEvent {
  if (!value || typeof value !== 'object') return false;
  const event = value as Partial<TracingEvent>;
  return ['span_started', 'span_updated', 'span_ended'].includes(String(event.type))
    && !!event.exportedSpan
    && typeof event.exportedSpan.id === 'string'
    && typeof event.exportedSpan.traceId === 'string'
    && typeof event.exportedSpan.name === 'string'
    && typeof event.exportedSpan.type === 'string'
    && typeof event.exportedSpan.isRootSpan === 'boolean'
    && typeof event.exportedSpan.isEvent === 'boolean';
}

function track(bridge: Bridge, receipt: AdmissionReceipt): void {
  bridge.pending.add(receipt.settled);
  void receipt.settled.finally(() => bridge.pending.delete(receipt.settled));
}

async function settlePending(bridge: Bridge): Promise<void> {
  await Promise.allSettled([...bridge.pending]);
}

function stringField(value: Record<string, unknown>, key: string): string | undefined {
  return typeof value[key] === 'string' ? value[key] : undefined;
}

function integerField(value: Record<string, unknown>, key: string): number | undefined {
  return Number.isSafeInteger(value[key]) ? value[key] as number : undefined;
}

function recordField(value: Record<string, unknown>, key: string): Record<string, unknown> {
  const child = capturedValue(value, key);
  return child && typeof child === 'object' && !Array.isArray(child)
    ? child as Record<string, unknown> : {};
}

function capturedValue(value: Record<string, unknown>, key: string): unknown {
  const child = value[key];
  if (!child || typeof child !== 'object' || Array.isArray(child)) return child;
  const omitted = (child as Record<string, unknown>).$semantic_layer_omitted;
  return typeof omitted === 'string' ? undefined : child;
}

function retryAttempt(value: unknown, seen = new WeakSet<object>()): number | undefined {
  if (!value || typeof value !== 'object' || seen.has(value)) return undefined;
  seen.add(value);
  for (const [key, child] of Object.entries(value)) {
    const normalized = key.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toLowerCase();
    if (['attempt', 'retry', 'retry_count', 'attempt_index'].includes(normalized)
      && Number.isInteger(child) && (child as number) > 0) return child as number;
    const nested = retryAttempt(child, seen);
    if (nested) return nested;
  }
  return undefined;
}
