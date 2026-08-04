import { createRequire } from 'node:module';
import type { ParentContext } from './types.js';

export type ResolvedParentContext = Readonly<{
  traceparent?: string;
  gap?: 'required_parent_context_missing' | 'invalid_traceparent' | 'parent_context_unreadable' | 'parent_context_conflict';
  error?: unknown;
}>;

type OTelApi = {
  context?: { active(): unknown };
  trace?: {
    getSpanContext(value: unknown): {
      traceId?: unknown;
      spanId?: unknown;
      traceFlags?: unknown;
      isRemote?: unknown;
    } | undefined;
  };
};

const require = createRequire(import.meta.url);
let otelApi: OTelApi | null | undefined;

/** Resolve explicit, inherited, or active-OTel context without installing global instrumentation. */
export function resolveParentContext(
  explicit: ParentContext | undefined,
  inherited?: string,
): ResolvedParentContext {
  const value = readParentContext(explicit);
  if (value.unreadable) return { ...withGap(inherited, 'parent_context_unreadable'), error: value.error };
  if (value.traceparent !== undefined) {
    if (!isValidTraceparent(value.traceparent)) return withGap(inherited, 'invalid_traceparent');
    if (inherited !== undefined && value.traceparent !== inherited) {
      return { traceparent: inherited, gap: 'parent_context_conflict' };
    }
    return { traceparent: inherited ?? value.traceparent };
  }
  if (inherited !== undefined) return { traceparent: inherited };
  const active = activeOTelTraceparent();
  if (active !== undefined) return { traceparent: active };
  return value.required ? { gap: 'required_parent_context_missing' } : {};
}

/** Read the public option without letting a hostile getter interrupt customer code. */
export function resolveParentContextOption(
  options: unknown,
  inherited?: string,
): ResolvedParentContext {
  try {
    if (!options || typeof options !== 'object') return resolveParentContext(undefined, inherited);
    return resolveParentContext(Reflect.get(options, 'parentContext') as ParentContext | undefined, inherited);
  } catch (error) {
    return { ...withGap(inherited, 'parent_context_unreadable'), error };
  }
}

export function isValidTraceparent(value: unknown): value is string {
  if (typeof value !== 'string' || value.length > 512) return false;
  const match = /^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})(.*)$/.exec(value);
  if (!match) return false;
  const [, version, traceId, parentId, , future] = match;
  if (version === 'ff' || /^0+$/.test(traceId) || /^0+$/.test(parentId)) return false;
  if (version === '00') return future.length === 0;
  return future.length === 0 || future.startsWith('-');
}

function readParentContext(value: ParentContext | undefined): {
  traceparent?: unknown;
  required: boolean;
  unreadable: boolean;
  error?: unknown;
} {
  if (value === undefined) return { required: false, unreadable: false };
  if (!value || typeof value !== 'object') return { traceparent: value, required: false, unreadable: false };
  try {
    return {
      traceparent: Reflect.get(value, 'traceparent'),
      required: Reflect.get(value, 'required') === true,
      unreadable: false,
    };
  } catch (error) {
    return { required: false, unreadable: true, error };
  }
}

function withGap(inherited: string | undefined, gap: NonNullable<ResolvedParentContext['gap']>): ResolvedParentContext {
  return inherited === undefined ? { gap } : { traceparent: inherited, gap };
}

function activeOTelTraceparent(): string | undefined {
  try {
    if (otelApi === undefined) otelApi = require('@opentelemetry/api') as OTelApi;
    if (otelApi === null) return undefined;
    const active = otelApi.context?.active();
    const span = active === undefined ? undefined : otelApi.trace?.getSpanContext(active);
    const traceId = span?.traceId;
    const spanId = span?.spanId;
    const traceFlags = span?.traceFlags;
    if (typeof traceId !== 'string' || typeof spanId !== 'string') return undefined;
    const flags = typeof traceFlags === 'number' ? (traceFlags & 0xff).toString(16).padStart(2, '0') : '00';
    const candidate = `00-${traceId}-${spanId}-${flags}`;
    return isValidTraceparent(candidate) ? candidate : undefined;
  } catch {
    otelApi = null;
    return undefined;
  }
}
