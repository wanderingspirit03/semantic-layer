import type {
  AdmissionReceipt,
  CaptureSource,
  SourceLifecycle,
  SourceSink,
  TraceIdentity,
} from '../v1/types.js';
import { trustOfficialSource } from '../v1/source-ownership.js';

type OTelOptions = { version?: string };
const GEN_AI_SCHEMA_URL = 'https://opentelemetry.io/schemas/gen-ai/1.42.0';
const OTEL_OBSERVATION_COVERAGE = Object.freeze({
  operation: 'otel-observation', domain: 'otel.trace-span',
});
const TOOL_EXECUTION_COVERAGE = Object.freeze({
  operation: 'tool-execution', domain: 'gen-ai.tool-call',
});
const MODEL_INPUT_ROLES = new Set(['system', 'developer', 'user', 'assistant', 'tool']);
type OTelContext = unknown;
type OTelSpan = {
  spanContext(): { traceId: string; spanId: string; traceFlags?: number; traceState?: unknown };
  parentSpanContext?: { spanId?: string; traceId?: string };
  name?: string;
  attributes?: Record<string, unknown>;
  events?: unknown[];
  links?: unknown[];
  status?: unknown;
  resource?: { attributes?: Record<string, unknown> };
  instrumentationScope?: unknown;
  droppedAttributesCount?: number;
  droppedEventsCount?: number;
  droppedLinksCount?: number;
};
type OTelLogRecord = {
  spanContext?: { traceId?: string; spanId?: string; traceFlags?: number };
  hrTime?: unknown;
  hrTimeObserved?: unknown;
  severityNumber?: number;
  severityText?: string;
  body?: unknown;
  attributes?: Record<string, unknown>;
  resource?: { attributes?: Record<string, unknown> };
  instrumentationScope?: unknown;
  droppedAttributesCount?: number;
};
type OTelSpanProcessor = {
  onStart(span: OTelSpan, parentContext: OTelContext): void;
  onEnd(span: OTelSpan): void;
  forceFlush(): Promise<void>;
  shutdown(): Promise<void>;
};
type OTelLogRecordProcessor = {
  onEmit(record: OTelLogRecord, context?: OTelContext): void;
  forceFlush(): Promise<void>;
  shutdown(): Promise<void>;
};
type NormalizedModelMessage = {
  key: string;
  index: number;
  message: Record<string, unknown>;
  role: string;
  content: unknown[];
  name?: string;
};
type CachedModelInput = { key: string; receipt: AdmissionReceipt };
type OpenOTelTrace = {
  identity: TraceIdentity;
  start: AdmissionReceipt;
  name: string;
  openSpans: number;
  modelInput: CachedModelInput[];
  agentRootSpanId?: string;
  agentInputCaptured?: boolean;
  terminalAgentSpan?: OTelSpan;
  failed?: boolean;
};
type OpenOTelSpan = {
  traceId: string;
  trace: TraceIdentity;
  start: AdmissionReceipt;
  kind: 'agent-root' | 'agent-scope' | 'model' | 'tool' | 'unknown';
  claimed: boolean;
  semanticParent: boolean;
  modelInputCaptured?: boolean;
  toolInputCaptured?: boolean;
  suppressTerminal?: boolean;
};

export interface OpenTelemetrySource extends CaptureSource {
  readonly spanProcessor: OTelSpanProcessor;
  readonly logRecordProcessor: OTelLogRecordProcessor;
}

/** Additive in-process OTel processors. Include them alongside existing processors at provider creation. */
export function createOpenTelemetrySource(options: OTelOptions = {}): OpenTelemetrySource {
  let sink: SourceSink | undefined;
  let active = false;
  const traces = new Map<string, OpenOTelTrace>();
  const spans = new Map<string, OpenOTelSpan>();
  const ignoredSpans = new Set<string>();

  const spanProcessor: OTelSpanProcessor = {
    onStart(span) {
      if (!active || !sink) return;
      const context = readSpanContext(span);
      if (!isValidContext(context)) {
        if (hasGenAISignal(span.attributes)) {
          recordContextGap(sink, 'invalid_span_context', 'span', safeSpanSnapshot(span));
        }
        return;
      }
      const classified = classifySpan(span.attributes);
      const existingRoot = traces.get(context.traceId);
      if (!existingRoot && classified === 'unknown' && !hasGenAISignal(span.attributes)) {
        ignoredSpans.add(spanKey(context.traceId, context.spanId));
        return;
      }
      const root = existingRoot ?? openTrace(sink, traces, context, span, classified);
      if (!root) return;
      root.openSpans += 1;
      if (classified === 'agent'
        && root.agentRootSpanId === undefined
        && existingRoot === undefined) {
        const start = sink.record({
          kind: 'unknown',
          phase: 'start',
          name: 'otel.span',
          trace: root.identity,
          nativeIdentity: context.spanId,
          coverage: OTEL_OBSERVATION_COVERAGE,
          native: spanSnapshot(span),
          semantic: { type: 'capture.redundant', route: 'otel' },
        });
        spans.set(spanKey(context.traceId, context.spanId), {
          traceId: context.traceId,
          trace: root.identity,
          start,
          kind: 'unknown',
          claimed: false,
          semanticParent: false,
          suppressTerminal: true,
        });
        recordSchemaGap(sink, root.identity, start, span);
        return;
      }
      if (classified === 'agent' && root.agentRootSpanId === context.spanId) {
        spans.set(spanKey(context.traceId, context.spanId), {
          traceId: context.traceId,
          trace: root.identity,
          start: root.start,
          kind: 'agent-root',
          claimed: true,
          semanticParent: true,
        });
        return;
      }
      const parentSpanId = span.parentSpanContext?.spanId;
      const parent = parentSpanId ? spans.get(spanKey(context.traceId, parentSpanId)) : undefined;
      const toolCallId = exactToolCallId(span.attributes);
      const kind = classified === 'agent' ? 'agent-scope' : classified;
      const claimed = hasGenAISignal(span.attributes);
      const modelInputCaptured = kind === 'model'
        ? hasContent(span.attributes, 'gen_ai.input.messages')
        : undefined;
      const toolInputCaptured = kind === 'tool'
        ? jsonAttribute(span.attributes, 'gen_ai.tool.call.arguments').state === 'valid'
        : undefined;
      const contextRefs = kind === 'model'
        ? recordModelInput(sink, root, span, context.spanId)
        : [];
      const start = sink.record({
        kind: kind === 'agent-scope' ? 'lifecycle' : kind,
        phase: 'start', name: 'otel.span', trace: root.identity,
        nativeIdentity: toolCallId ?? context.spanId,
        coverage: toolCallId ? TOOL_EXECUTION_COVERAGE : OTEL_OBSERVATION_COVERAGE,
        native: spanSnapshot(span),
        ...(parent?.start.accepted && parent.kind !== 'unknown'
          ? { parentRecordId: parent.start.recordId }
          : {}),
        semantic: kind === 'agent-scope'
          ? agentScopeSemantic(span, 'start')
          : kind === 'tool' && !toolInputCaptured
            ? { type: 'capture.redundant', route: 'otel' }
            : spanSemantic(span, kind, 'start', claimed, contextRefs),
      });
      spans.set(spanKey(context.traceId, context.spanId), {
        traceId: context.traceId,
        trace: root.identity,
        start,
        kind,
        claimed,
        semanticParent: kind !== 'agent-scope'
          || Boolean(parent && parent.kind !== 'unknown'),
        ...(modelInputCaptured !== undefined ? { modelInputCaptured } : {}),
        ...(toolInputCaptured !== undefined ? { toolInputCaptured } : {}),
      });
      if (kind === 'tool' && invalidToolCallId(span.attributes)) {
        recordContentGap(sink, root.identity, start, 'invalid_tool_call_id');
      }
      if (kind === 'agent-scope' && parent && parent.kind !== 'unknown') {
        if (hasContent(span.attributes, 'gen_ai.input.messages')) {
          recordAgentMessages(
            sink,
            root,
            span,
            context.spanId,
            'gen_ai.input.messages',
            'context',
            start,
          );
        } else {
          recordContentGap(
            sink,
            root.identity,
            start,
            'nested_agent_input_not_captured',
          );
        }
      }
      if (claimed && parentSpanId && !parent
        && !ignoredSpans.has(spanKey(context.traceId, parentSpanId))) {
        sink.record({
          kind: 'unknown', phase: 'gap', name: 'otel.context.gap', trace: root.identity,
          nativeIdentity: context.spanId,
          native: { reason: 'unobserved_parent_span', native_type: 'span', parent_span_id: parentSpanId },
          semantic: {
            type: 'capture.gap',
            reason: 'unobserved_parent_span',
            detail: 'The OpenTelemetry parent span was not observed by this source.',
          },
        });
      }
    },
    onEnd(span) {
      if (!active || !sink) return;
      const context = readSpanContext(span);
      if (!isValidContext(context)) {
        if (hasGenAISignal(span.attributes)) {
          recordContextGap(sink, 'invalid_span_context', 'span', safeSpanSnapshot(span));
        }
        return;
      }
      const key = spanKey(context.traceId, context.spanId);
      const open = spans.get(key);
      if (!open) {
        if (ignoredSpans.delete(key)) return;
        if (hasGenAISignal(span.attributes)) {
          recordContextGap(sink, 'span_end_without_observed_start', 'span', spanSnapshot(span));
        }
        return;
      }
      const failed = isErrorStatus(span.status);
      const toolCallId = exactToolCallId(span.attributes);
      const kind = open.kind;
      const claimed = open.claimed || hasGenAISignal(span.attributes);
      const root = traces.get(open.traceId);
      const semanticError = failed ? normalizedSpanError(span) : undefined;
      let terminal: AdmissionReceipt = open.start;
      let terminalParent = open.start;
      let toolProjectable = true;
      if (root && kind === 'model' && !open.modelInputCaptured) {
        const lateInput = messageArrayAttribute(span, 'gen_ai.input.messages');
        if (lateInput.state === 'valid') {
          recordModelInput(sink, root, span, context.spanId);
          recordContentGap(
            sink,
            root.identity,
            open.start,
            'model_input_late_unlinked',
          );
        } else {
          recordContentGap(
            sink,
            root.identity,
            open.start,
            lateInput.state === 'malformed'
              ? 'model_input_malformed'
              : 'model_input_not_captured',
          );
        }
      }
      if (root && kind === 'tool' && !open.toolInputCaptured) {
        const lateInput = jsonAttribute(span.attributes, 'gen_ai.tool.call.arguments');
        if (lateInput.state === 'valid') {
          terminalParent = sink.record({
            kind: 'tool',
            phase: 'start',
            name: 'otel.span.late_input',
            trace: root.identity,
            nativeIdentity: toolCallId ?? context.spanId,
            coverage: toolCallId ? TOOL_EXECUTION_COVERAGE : OTEL_OBSERVATION_COVERAGE,
            native: spanSnapshot(span),
            semantic: spanSemantic(span, 'tool', 'start', claimed),
          });
        } else {
          toolProjectable = false;
          recordContentGap(
            sink,
            root.identity,
            open.start,
            lateInput.state === 'malformed'
              ? 'tool_input_malformed'
              : 'tool_input_not_captured',
          );
        }
      }
      if (kind === 'agent-root') {
        if (root) {
          root.terminalAgentSpan = span;
          root.failed = failed;
          if (!root.agentInputCaptured) {
            const lateInput = messageArrayAttribute(span, 'gen_ai.input.messages');
            if (lateInput.state === 'valid') {
              recordAgentMessages(
                sink,
                root,
                span,
                context.spanId,
                'gen_ai.input.messages',
                'context',
                root.start,
              );
              root.agentInputCaptured = true;
            } else {
              recordContentGap(
                sink,
                root.identity,
                root.start,
                lateInput.state === 'malformed'
                  ? 'agent_input_malformed'
                  : 'agent_input_not_captured',
              );
              root.agentInputCaptured = true;
            }
          }
        }
      } else {
        terminal = sink.record({
          kind: kind === 'agent-scope' ? 'lifecycle' : kind,
          phase: failed ? 'error' : 'end', name: 'otel.span',
          trace: open.trace, nativeIdentity: toolCallId ?? context.spanId,
          coverage: toolCallId ? TOOL_EXECUTION_COVERAGE : OTEL_OBSERVATION_COVERAGE,
          native: spanSnapshot(span),
          ...(terminalParent.accepted && open.kind !== 'unknown'
            ? { parentRecordId: terminalParent.recordId }
            : {}),
          semantic: kind === 'agent-scope'
            ? agentScopeSemantic(span, failed ? 'error' : 'end', semanticError)
            : kind === 'tool' && !toolProjectable
              ? { type: 'capture.redundant', route: 'otel' }
              : kind === 'unknown' && open.suppressTerminal
                ? { type: 'capture.redundant', route: 'otel' }
              : spanSemantic(
                  span,
                  kind,
                  failed ? 'error' : 'end',
                  claimed,
                  [],
                  semanticError,
                ),
        });
      }
      if (root && kind === 'model') {
        const modelOutput = messageArrayAttribute(span, 'gen_ai.output.messages');
        appendModelOutput(root, span, terminal);
        if (modelOutput.state === 'malformed') {
          recordContentGap(sink, root.identity, terminal, 'model_output_malformed');
        } else if (!failed && modelOutput.state === 'missing') {
          recordContentGap(sink, root.identity, terminal, 'model_output_not_captured');
        }
      }
      if (root && kind === 'tool' && toolProjectable && !failed) {
        const toolOutput = jsonAttribute(span.attributes, 'gen_ai.tool.call.result');
        if (toolOutput.state !== 'valid') {
          recordContentGap(
            sink,
            root.identity,
            terminal,
            toolOutput.state === 'malformed'
              ? 'tool_output_malformed'
              : 'tool_output_not_captured',
          );
        }
      }
      if (root && kind === 'agent-scope' && open.semanticParent) {
        if (hasContent(span.attributes, 'gen_ai.output.messages')) {
          recordAgentMessages(
            sink,
            root,
            span,
            context.spanId,
            'gen_ai.output.messages',
            'observed',
            open.start,
          );
        } else if (!failed) {
          recordContentGap(
            sink,
            root.identity,
            open.start,
            'nested_agent_output_not_captured',
          );
        }
      }
      if (failed && kind === 'model') sink.record({
        kind: 'error', phase: 'event', name: 'otel.span.error', trace: open.trace,
        nativeIdentity: toolCallId ?? context.spanId,
        coverage: OTEL_OBSERVATION_COVERAGE,
        native: { status: span.status, events: span.events ?? [] },
        ...(terminal.accepted ? { parentRecordId: terminal.recordId } : {}),
        semantic: { type: 'model.error', route: 'otel', error: semanticError },
      });
      spans.delete(key);
      if (!root) return;
      root.openSpans -= 1;
      if (root.openSpans === 0) closeTrace(sink, traces, open.traceId, root);
    },
    async forceFlush() {},
    async shutdown() {},
  };

  const logRecordProcessor: OTelLogRecordProcessor = {
    onEmit(record) {
      if (!active || !sink) return;
      const claimed = hasGenAISignal(record.attributes);
      const context = record.spanContext;
      if (context === undefined) {
        if (claimed) recordContextGap(sink, 'missing_log_context', 'log', logSnapshot(record));
        return;
      }
      if (!isValidContext(context)) {
        if (claimed) recordContextGap(sink, 'invalid_log_context', 'log', logSnapshot(record));
        return;
      }
      const open = spans.get(spanKey(context.traceId, context.spanId));
      const root = traces.get(context.traceId);
      if (!open || !root) {
        if (claimed) recordContextGap(sink, 'orphan_log_context', 'log', logSnapshot(record));
        return;
      }
      if (!claimed) return;
      sink.record({
        kind: 'log', phase: 'event', name: 'otel.log', trace: root.identity,
        nativeIdentity: context.spanId,
        coverage: OTEL_OBSERVATION_COVERAGE,
        ...(open.start.accepted ? { parentRecordId: open.start.recordId } : {}),
        native: logSnapshot(record), semantic: coverageUnknown('log'),
      });
    },
    async forceFlush() {},
    async shutdown() {},
  };

  return trustOfficialSource({
    metadata: {
      name: 'generic:otel',
      seam: 'SpanProcessor/LogRecordProcessor',
      identityDomain: 'otel.trace-span',
      ...(options.version ? { version: options.version } : {}),
      official: true,
      coverage: [
        { ...OTEL_OBSERVATION_COVERAGE, role: 'evidence' },
        { ...TOOL_EXECUTION_COVERAGE, role: 'evidence' },
      ],
    },
    spanProcessor,
    logRecordProcessor,
    install(installedSink): SourceLifecycle {
      if (active) throw new Error('OpenTelemetry source is already installed');
      sink = installedSink;
      active = true;
      return {
        deactivate() { active = false; },
        drain() {},
      };
    },
  }, 'otel');
}

function openTrace(
  sink: SourceSink,
  traces: Map<string, OpenOTelTrace>,
  context: { traceId: string; spanId: string; traceFlags?: number },
  span: OTelSpan,
  kind: 'agent' | 'model' | 'tool' | 'unknown',
): OpenOTelTrace | undefined {
  const nativeTraceId = context.traceId;
  const schemaUrl = instrumentationSchemaUrl(span);
  const agentRoot = kind === 'agent' && schemaUrl === GEN_AI_SCHEMA_URL;
  const agentInput = agentRoot
    ? messageArrayAttribute(span, 'gen_ai.input.messages')
    : { state: 'missing' as const };
  const name = agentName(span);
  const opened = sink.openTrace({
    name, nativeIdentity: nativeTraceId,
    coverage: OTEL_OBSERVATION_COVERAGE,
    parentContext: { traceparent: toTraceparent(context) },
    native: kind === 'agent' ? spanSnapshot(span) : null,
    semantic: agentRoot ? {
      type: 'agent.run',
      name,
      ...(agentInput.state === 'valid' ? { input: agentInput.value } : {}),
      route: 'otel',
      semconv: schemaUrl,
    } : { type: 'capture.control', route: 'otel' },
  });
  if (!opened.accepted) return undefined;
  const root = {
    identity: opened.identity,
    start: opened,
    name,
    openSpans: 0,
    modelInput: [],
    ...(agentRoot ? {
      agentRootSpanId: context.spanId,
      agentInputCaptured: agentInput.state !== 'missing',
    } : {}),
  };
  traces.set(nativeTraceId, root);
  if (agentRoot && agentInput.state === 'malformed') {
    recordContentGap(sink, root.identity, root.start, 'agent_input_malformed');
  }
  return root;
}

function exactToolCallId(attributes: Record<string, unknown> | undefined): string | undefined {
  if (attributes?.['gen_ai.operation.name'] !== 'execute_tool') return undefined;
  const toolName = attributes['gen_ai.tool.name'];
  if (typeof toolName !== 'string' || toolName.trim().length === 0) return undefined;
  const value = attributes?.['gen_ai.tool.call.id'];
  return typeof value === 'string'
    && value.trim().length > 0
    && [...value].length <= 256
    && !value.includes('\u0000')
    ? value
    : undefined;
}

function invalidToolCallId(attributes: Record<string, unknown> | undefined): boolean {
  return attributes?.['gen_ai.operation.name'] === 'execute_tool'
    && Object.prototype.hasOwnProperty.call(attributes, 'gen_ai.tool.call.id')
    && exactToolCallId(attributes) === undefined;
}

function toTraceparent(context: { traceId: string; spanId: string; traceFlags?: number }): string {
  const flags = (context.traceFlags ?? 0) & 0xff;
  return `00-${context.traceId.toLowerCase()}-${context.spanId.toLowerCase()}-${flags.toString(16).padStart(2, '0')}`;
}

function closeTrace(
  sink: SourceSink,
  traces: Map<string, OpenOTelTrace>,
  nativeTraceId: string,
  root: OpenOTelTrace,
): void {
  const terminal = root.terminalAgentSpan;
  const agentOutput = terminal
    ? messageArrayAttribute(terminal, 'gen_ai.output.messages')
    : { state: 'missing' as const };
  if (root.agentRootSpanId && terminal) {
    if (agentOutput.state === 'malformed') {
      recordContentGap(sink, root.identity, root.start, 'agent_output_malformed');
    } else if (!root.failed && agentOutput.state === 'missing') {
      recordContentGap(sink, root.identity, root.start, 'agent_output_not_captured');
    }
  }
  sink.record({
    kind: 'lifecycle',
    phase: root.failed ? 'error' : 'end',
    name: root.name,
    trace: root.identity,
    nativeIdentity: nativeTraceId,
    ...(root.start.accepted ? { parentRecordId: root.start.recordId } : {}),
    native: terminal ? spanSnapshot(terminal) : null,
    semantic: root.agentRootSpanId ? {
      type: 'agent.run',
      status: root.failed ? 'failed' : terminal ? 'completed' : 'unknown',
      ...(agentOutput.state === 'valid' ? { output: agentOutput.value } : {}),
      ...(root.failed && terminal ? { error: normalizedSpanError(terminal) } : {}),
      route: 'otel',
    } : { type: 'capture.control', route: 'otel' },
  });
  traces.delete(nativeTraceId);
}

function spanSnapshot(span: OTelSpan): Record<string, unknown> {
  const context = span.spanContext();
  return {
    name: span.name ?? null,
    trace_id: context.traceId,
    span_id: context.spanId,
    trace_flags: context.traceFlags ?? null,
    trace_state: context.traceState ?? null,
    parent_span_context: span.parentSpanContext
      ? spanContextSnapshot(span.parentSpanContext)
      : null,
    attributes: span.attributes ?? {},
    events: span.events ?? [],
    links: span.links ?? [],
    status: span.status ?? null,
    resource: span.resource?.attributes ?? {},
    instrumentation_scope: instrumentationScopeSnapshot(span.instrumentationScope),
    dropped_attributes_count: span.droppedAttributesCount ?? 0,
    dropped_events_count: span.droppedEventsCount ?? 0,
    dropped_links_count: span.droppedLinksCount ?? 0,
  };
}

function spanContextSnapshot(context: {
  traceId?: string;
  spanId?: string;
  traceFlags?: number;
  traceState?: unknown;
}): Record<string, unknown> {
  return {
    trace_id: context.traceId ?? null,
    span_id: context.spanId ?? null,
    trace_flags: context.traceFlags ?? null,
    trace_state: context.traceState ?? null,
  };
}

function instrumentationScopeSnapshot(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== 'object') return null;
  const scope = value as {
    name?: unknown;
    version?: unknown;
    schemaUrl?: unknown;
    attributes?: unknown;
  };
  return {
    name: scope.name ?? null,
    version: scope.version ?? null,
    schema_url: scope.schemaUrl ?? null,
    attributes: scope.attributes ?? {},
  };
}

function safeSpanSnapshot(span: OTelSpan): Record<string, unknown> {
  try {
    return spanSnapshot(span);
  } catch (error) {
    return { name: span.name ?? null, context_error: error };
  }
}

function logSnapshot(record: OTelLogRecord): Record<string, unknown> {
  return {
    trace_id: record.spanContext?.traceId ?? null,
    span_id: record.spanContext?.spanId ?? null,
    trace_flags: record.spanContext?.traceFlags ?? null,
    timestamp: record.hrTime ?? null,
    observed_timestamp: record.hrTimeObserved ?? null,
    severity_number: record.severityNumber ?? null,
    severity_text: record.severityText ?? null,
    body: record.body ?? null,
    attributes: record.attributes ?? {},
    resource: record.resource?.attributes ?? {},
    instrumentation_scope: record.instrumentationScope ?? null,
    dropped_attributes_count: record.droppedAttributesCount ?? 0,
  };
}

function classifySpan(
  attributes: Record<string, unknown> | undefined,
): 'agent' | 'model' | 'tool' | 'unknown' {
  if (!attributes) return 'unknown';
  if (attributes['gen_ai.operation.name'] === 'invoke_agent') return 'agent';
  if (attributes['gen_ai.operation.name'] === 'execute_tool') return 'tool';
  if (['chat', 'text_completion', 'generate_content'].includes(
    String(attributes['gen_ai.operation.name']),
  )) return 'model';
  return 'unknown';
}

function hasGenAISignal(attributes: Record<string, unknown> | undefined): boolean {
  return Object.keys(attributes ?? {}).some((name) => name.startsWith('gen_ai.'));
}

function spanSemantic(
  span: OTelSpan,
  kind: 'model' | 'tool' | 'unknown',
  phase: 'start' | 'end' | 'error',
  claimed: boolean,
  contextRefs: readonly string[] = [],
  error?: Record<string, unknown>,
): Record<string, unknown> {
  const attributes = span.attributes ?? {};
  if (kind === 'model') {
    const output = messageArrayAttribute(span, 'gen_ai.output.messages');
    const usage = {
      ...numberField(attributes['gen_ai.usage.input_tokens'], 'input_tokens'),
      ...numberField(attributes['gen_ai.usage.output_tokens'], 'output_tokens'),
    };
    return {
      type: phase === 'start' ? 'model.request' : 'model.response',
      route: 'otel',
      ...(phase === 'start' ? { context_refs: [...contextRefs] } : {
        status: phase === 'error' ? 'failed' : 'completed',
        ...(output.state === 'valid' ? { content: output.value } : {}),
        ...(Object.keys(usage).length ? { usage } : {}),
        ...(phase === 'error' ? {} : { origin: 'inferred' }),
      }),
      ...(typeof (attributes['gen_ai.response.model'] ?? attributes['gen_ai.request.model']) === 'string'
        ? { model: attributes['gen_ai.response.model'] ?? attributes['gen_ai.request.model'] }
        : {}),
    };
  }
  if (kind === 'tool') {
    const name = attributes['gen_ai.tool.name'];
    const input = jsonAttribute(attributes, 'gen_ai.tool.call.arguments');
    const output = jsonAttribute(attributes, 'gen_ai.tool.call.result');
    return {
      type: phase === 'start'
        ? 'tool.execution'
        : phase === 'error' ? 'tool.error' : 'tool.result',
      route: 'otel',
      ...(typeof name === 'string' && name.trim() ? { name } : {}),
      ...(phase === 'start'
        ? (input.state === 'valid' ? { input: input.value } : {})
        : {
            status: phase === 'error' ? 'failed' : 'succeeded',
            ...(output.state === 'valid' ? { output: output.value } : {}),
            ...(error ? { error } : {}),
            ...(phase === 'error' ? {} : { origin: 'inferred' }),
          }),
    };
  }
  return phase === 'start' || !claimed
    ? { type: 'capture.redundant', route: 'otel' }
    : {
        type: 'capture.gap',
        reason: 'unsupported_genai_operation',
        detail: 'The OpenTelemetry span contained GenAI attributes for an unsupported operation.',
      };
}

function agentScopeSemantic(
  span: OTelSpan,
  phase: 'start' | 'end' | 'error',
  error?: Record<string, unknown>,
): Record<string, unknown> {
  return {
    type: 'scope',
    scope_type: 'agent',
    name: agentName(span),
    ...(phase === 'start' ? {} : { status: phase === 'error' ? 'failed' : 'completed' }),
    ...(error ? { error } : {}),
    route: 'otel',
  };
}

function agentName(span: OTelSpan): string {
  const name = span.attributes?.['gen_ai.agent.name'];
  return typeof name === 'string' && name.trim() ? name : span.name ?? 'otel.agent';
}

function hasContent(
  attributes: Record<string, unknown> | undefined,
  name: string,
): boolean {
  return attributes !== undefined
    && Object.prototype.hasOwnProperty.call(attributes, name)
    && attributes[name] !== undefined;
}

type MessageArrayAttribute =
  | { state: 'missing' }
  | { state: 'malformed' }
  | { state: 'valid'; value: unknown[] };

function messageArrayAttribute(
  span: OTelSpan,
  name: 'gen_ai.input.messages' | 'gen_ai.output.messages',
): MessageArrayAttribute {
  if (!hasContent(span.attributes, name)) return { state: 'missing' };
  const value = structuredAttribute(span.attributes?.[name]);
  if (!Array.isArray(value)) return { state: 'malformed' };
  const normalized = normalizeModelMessages(value);
  return normalized.discarded === 0
    ? { state: 'valid', value }
    : { state: 'malformed' };
}

type JsonAttribute =
  | { state: 'missing' }
  | { state: 'malformed' }
  | { state: 'valid'; value: unknown };

function jsonAttribute(
  attributes: Record<string, unknown> | undefined,
  name: string,
): JsonAttribute {
  if (!attributes || !Object.prototype.hasOwnProperty.call(attributes, name)) {
    return { state: 'missing' };
  }
  const raw = attributes[name];
  let value = raw;
  if (typeof raw === 'string') {
    try {
      value = JSON.parse(raw);
    } catch {
      return { state: 'malformed' };
    }
  }
  return isJsonValue(value)
    ? { state: 'valid', value }
    : { state: 'malformed' };
}

function instrumentationSchemaUrl(span: OTelSpan): string | undefined {
  if (!span.instrumentationScope || typeof span.instrumentationScope !== 'object') {
    return undefined;
  }
  const value = (span.instrumentationScope as { schemaUrl?: unknown }).schemaUrl;
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function recordSchemaGap(
  sink: SourceSink,
  trace: TraceIdentity,
  affected: AdmissionReceipt,
  span: OTelSpan,
): void {
  const observed = instrumentationSchemaUrl(span);
  const reason = observed ? 'unsupported_genai_schema' : 'missing_genai_schema';
  sink.record({
    kind: 'unknown',
    phase: 'gap',
    name: 'otel.schema.gap',
    trace,
    ...(affected.accepted ? { parentRecordId: affected.recordId } : {}),
    native: {
      reason,
      observed_schema: observed ?? null,
      supported_schema: GEN_AI_SCHEMA_URL,
    },
    semantic: {
      type: 'capture.gap',
      reason,
      detail: observed
        ? `OpenTelemetry GenAI schema ${observed} is not supported for rich agent projection.`
        : 'OpenTelemetry did not expose an instrumentation scope schema for rich agent projection.',
    },
  });
}

function recordContentGap(
  sink: SourceSink,
  trace: TraceIdentity,
  affected: AdmissionReceipt,
  reason: string,
): void {
  sink.record({
    kind: 'unknown',
    phase: 'gap',
    name: 'otel.content.gap',
    trace,
    ...(affected.accepted ? { parentRecordId: affected.recordId } : {}),
    native: { reason },
    semantic: {
      type: 'capture.gap',
      reason,
      detail: `OpenTelemetry GenAI evidence was incomplete: ${reason}.`,
    },
  });
}

function recordModelInput(
  sink: SourceSink,
  root: OpenOTelTrace,
  span: OTelSpan,
  spanId: string,
): string[] {
  const attributes = span.attributes ?? {};
  if (!Object.prototype.hasOwnProperty.call(attributes, 'gen_ai.input.messages')) return [];
  const input = structuredAttribute(attributes['gen_ai.input.messages']);
  if (!Array.isArray(input)) {
    root.modelInput.length = 0;
    recordModelInputLoss(sink, root.identity, spanId, 1);
    return [];
  }

  const { messages: normalized, discarded } = normalizeModelMessages(input);

  const refs: string[] = [];
  let prefix = 0;
  while (prefix < normalized.length
    && prefix < root.modelInput.length
    && normalized[prefix]?.key === root.modelInput[prefix]?.key) {
    const receipt = root.modelInput[prefix]!.receipt;
    appendReceiptRef(refs, receipt);
    prefix += 1;
  }
  root.modelInput.splice(prefix);
  normalized.slice(prefix).forEach((message) => {
    const receipt = sink.record({
      kind: 'model',
      phase: 'event',
      name: 'otel.model.context',
      trace: root.identity,
      nativeIdentity: `${spanId}:input:${message.index}`,
      coverage: OTEL_OBSERVATION_COVERAGE,
      native: {
        attribute: 'gen_ai.input.messages',
        index: message.index,
        message: message.message,
      },
      semantic: {
        type: 'message',
        route: 'otel',
        origin: 'context',
        role: message.role,
        content: message.content,
        ...(message.name ? { name: message.name } : {}),
      },
    });
    root.modelInput.push({ key: message.key, receipt });
    appendReceiptRef(refs, receipt);
  });
  if (discarded > 0) recordModelInputLoss(sink, root.identity, spanId, discarded);
  return refs;
}

function recordAgentMessages(
  sink: SourceSink,
  root: OpenOTelTrace,
  span: OTelSpan,
  spanId: string,
  attribute: 'gen_ai.input.messages' | 'gen_ai.output.messages',
  origin: 'context' | 'observed',
  parent: AdmissionReceipt,
): void {
  const input = structuredAttribute(span.attributes?.[attribute]);
  if (!Array.isArray(input)) {
    recordContentGap(
      sink,
      root.identity,
      parent,
      attribute === 'gen_ai.input.messages'
        ? 'agent_input_malformed'
        : 'agent_output_malformed',
    );
    return;
  }
  const { messages, discarded } = normalizeModelMessages(input);
  messages.forEach((message) => {
    sink.record({
      kind: 'model',
      phase: 'event',
      name: 'otel.agent.message',
      trace: root.identity,
      nativeIdentity: `${spanId}:${attribute}:${message.index}`,
      coverage: OTEL_OBSERVATION_COVERAGE,
      ...(parent.accepted ? { parentRecordId: parent.recordId } : {}),
      native: {
        attribute,
        index: message.index,
        message: message.message,
      },
      semantic: {
        type: 'message',
        route: 'otel',
        origin,
        role: message.role,
        content: message.content,
        ...(message.name ? { name: message.name } : {}),
      },
    });
  });
  if (discarded > 0) {
    recordContentGap(
      sink,
      root.identity,
      parent,
      attribute === 'gen_ai.input.messages'
        ? 'agent_input_partially_malformed'
        : 'agent_output_partially_malformed',
    );
  }
}

function appendModelOutput(
  root: OpenOTelTrace,
  span: OTelSpan,
  receipt: AdmissionReceipt,
): void {
  if (!receipt.accepted) return;
  const attributes = span.attributes ?? {};
  if (!Object.prototype.hasOwnProperty.call(attributes, 'gen_ai.output.messages')) return;
  const output = structuredAttribute(attributes['gen_ai.output.messages']);
  if (!Array.isArray(output)) return;
  const normalized = normalizeModelMessages(output);
  if (normalized.discarded > 0) return;
  normalized.messages.forEach((message) => {
    root.modelInput.push({ key: message.key, receipt });
  });
}

function normalizeModelMessages(input: unknown[]): {
  messages: NormalizedModelMessage[];
  discarded: number;
} {
  let discarded = 0;
  const messages: NormalizedModelMessage[] = [];
  input.forEach((message, index) => {
    const rawName = isRecord(message) ? message.name : undefined;
    if (!isRecord(message)
      || !MODEL_INPUT_ROLES.has(String(message.role))
      || !Array.isArray(message.parts)
      || !isJsonValue(message.parts)
      || (rawName !== undefined && (typeof rawName !== 'string' || !rawName.trim()))) {
      discarded += 1;
      return;
    }
    const name = typeof rawName === 'string' ? boundedParticipantName(rawName) : undefined;
    const normalizedMessage = {
      role: String(message.role),
      content: message.parts,
      ...(name ? { name } : {}),
    };
    messages.push({
      key: canonicalJson(normalizedMessage),
      index,
      message,
      role: normalizedMessage.role,
      content: normalizedMessage.content,
      ...(name ? { name } : {}),
    });
  });
  return { messages, discarded };
}

function appendReceiptRef(refs: string[], receipt: AdmissionReceipt): void {
  if (receipt.accepted && !refs.includes(receipt.recordId)) refs.push(receipt.recordId);
}

function recordModelInputLoss(
  sink: SourceSink,
  trace: TraceIdentity,
  spanId: string,
  discarded: number,
): void {
  sink.record({
    kind: 'unknown',
    phase: 'gap',
    name: 'otel.model.input',
    trace,
    nativeIdentity: `${spanId}:input:loss`,
    coverage: OTEL_OBSERVATION_COVERAGE,
    native: {
      attribute: 'gen_ai.input.messages',
      discarded_messages: discarded,
    },
    semantic: {
      type: 'capture.gap',
      reason: 'model_input_messages_discarded',
      count: discarded,
      detail: 'OpenTelemetry model input contained messages that could not be represented.',
    },
  });
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function boundedParticipantName(value: string): string {
  return [...value].slice(0, 256).join('');
}

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (isRecord(value)) {
    return `{${Object.keys(value).sort().map((key) => (
      `${JSON.stringify(key)}:${canonicalJson(value[key])}`
    )).join(',')}}`;
  }
  return JSON.stringify(value) ?? 'null';
}

function isJsonValue(value: unknown, seen = new Set<object>(), depth = 0): boolean {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  if (typeof value !== 'object' || depth >= 32 || seen.has(value)) return false;
  seen.add(value);
  const valid = Array.isArray(value)
    ? value.every((item) => isJsonValue(item, seen, depth + 1))
    : Object.getPrototypeOf(value) === Object.prototype
      && Object.values(value).every((item) => isJsonValue(item, seen, depth + 1));
  seen.delete(value);
  return valid;
}

function structuredAttribute(value: unknown): unknown {
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

function numberField(value: unknown, name: string): Record<string, number> {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0
    ? { [name]: value }
    : {};
}

function coverageUnknown(nativeType: string): Record<string, unknown> {
  return { coverage: 'unknown', native_type: nativeType, route: 'otel' };
}

function isErrorStatus(status: unknown): boolean {
  if (!status || typeof status !== 'object') return false;
  return (status as { code?: unknown }).code === 2;
}

function normalizedSpanError(span: OTelSpan): Record<string, unknown> {
  const status = isRecord(span.status) ? span.status : {};
  const exception = (span.events ?? []).find((event) => (
    isRecord(event) && event.name === 'exception'
  ));
  const exceptionAttributes = isRecord(exception)
    && isRecord(exception.attributes)
    ? exception.attributes
    : {};
  const spanAttributes = span.attributes ?? {};
  const observedType = stringValue(spanAttributes['error.type'])
    ?? stringValue(exceptionAttributes['exception.type']);
  const observedMessage = stringValue(exceptionAttributes['exception.message'])
    ?? stringValue(status.description);
  const stacktrace = stringValue(exceptionAttributes['exception.stacktrace']);
  return {
    type: normalizedErrorType(observedType ?? 'otel_span_error'),
    message: observedMessage ?? 'OpenTelemetry span ended with error status.',
    recoverable: false,
    details: {
      status_code: status.code === 2 ? 2 : null,
      ...(stringValue(status.description)
        ? { status_description: stringValue(status.description) }
        : {}),
      ...(observedType ? { exception_type: observedType } : {}),
      ...(stacktrace ? { exception_stacktrace: stacktrace } : {}),
    },
  };
}

function normalizedErrorType(value: string): string {
  const normalized = value
    .replace(/([a-z0-9])([A-Z])/g, '$1_$2')
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, '_')
    .replace(/^[^a-z]+/, '')
    .slice(0, 128);
  return /^[a-z][a-z0-9._-]{2,127}$/.test(normalized)
    ? normalized
    : 'otel_span_error';
}

function stringValue(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0
    ? [...value].slice(0, 4096).join('')
    : undefined;
}

function readSpanContext(span: OTelSpan): ReturnType<OTelSpan['spanContext']> | undefined {
  try {
    return span.spanContext();
  } catch {
    return undefined;
  }
}

function isValidContext(
  context: { traceId?: string; spanId?: string } | undefined,
): context is { traceId: string; spanId: string } {
  return context !== undefined
    && /^[0-9a-f]{32}$/i.test(context.traceId ?? '')
    && !/^0{32}$/.test(context.traceId ?? '')
    && /^[0-9a-f]{16}$/i.test(context.spanId ?? '')
    && !/^0{16}$/.test(context.spanId ?? '');
}

function spanKey(traceId: string, spanId: string): string {
  return `${traceId}:${spanId}`;
}

function recordContextGap(
  sink: SourceSink,
  reason: string,
  nativeType: 'span' | 'log',
  snapshot: Record<string, unknown>,
): void {
  const opened = sink.openTrace({
    name: 'otel.observability_gap',
    coverage: OTEL_OBSERVATION_COVERAGE,
    native: null,
    semantic: { type: 'capture.control', route: 'otel' },
  });
  if (!opened.accepted) return;
  sink.record({
    kind: 'unknown', phase: 'gap', name: 'otel.context.gap', trace: opened.identity,
    coverage: OTEL_OBSERVATION_COVERAGE,
    native: { reason, native_type: nativeType, snapshot },
    semantic: {
      type: 'capture.gap',
      reason,
      detail: `OpenTelemetry ${nativeType} context could not be correlated.`,
    },
  });
  sink.record({
    kind: 'lifecycle', phase: 'end', name: 'otel.observability_gap', trace: opened.identity,
    coverage: OTEL_OBSERVATION_COVERAGE,
    native: null,
    semantic: { type: 'capture.control', route: 'otel' },
  });
}
