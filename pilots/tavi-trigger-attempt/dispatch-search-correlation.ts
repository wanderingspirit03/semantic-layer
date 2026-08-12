import type { OpenClawToolCorrelationBridge } from './openclaw-tool-correlation.js';

export async function dispatchSearchWithTrustedCorrelation<T>(input: Readonly<{
  bridge: OpenClawToolCorrelationBridge;
  toolCallId: string;
  modelParams: Record<string, unknown>;
  signal?: AbortSignal;
  dispatch(
    params: Record<string, unknown>,
    context: Readonly<{
      signal?: AbortSignal;
      client_request_id?: string;
    }>,
  ): Promise<T>;
}>): Promise<T> {
  const researchId = input.bridge.consume(input.toolCallId);
  const trustedParams = withoutCorrelationFields(input.modelParams);
  try {
    return await input.dispatch(trustedParams, {
      ...(input.signal ? { signal: input.signal } : {}),
      ...(researchId ? { client_request_id: researchId } : {}),
    });
  } finally {
    input.bridge.discard(input.toolCallId);
  }
}

function withoutCorrelationFields(
  params: Record<string, unknown>,
): Record<string, unknown> {
  return sanitizeObject(params);
}

function sanitizeObject(value: Record<string, unknown>): Record<string, unknown> {
  const clean: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(value)) {
    if (isCorrelationCarrier(key)) continue;
    clean[key] = sanitizeValue(child);
  }
  return clean;
}

function sanitizeValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(sanitizeValue);
  if (isPlainObject(value)) return sanitizeObject(value);
  return value;
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function isCorrelationCarrier(key: string): boolean {
  const normalized = key.normalize('NFKC').replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
  return normalized === 'correlation'
    || normalized === 'traceparent'
    || normalized === 'tracestate'
    || normalized === 'baggage'
    || normalized === 'sendercontextid'
    || normalized.endsWith('requestid')
    || normalized.endsWith('researchid')
    || normalized.endsWith('taskid')
    || normalized.endsWith('runid')
    || normalized.endsWith('executionid');
}
