import type { OpenClawPluginApi } from 'openclaw/plugin-sdk/plugin-entry';

const DEFAULT_MAX_BINDINGS = 1_024;
const DEFAULT_TTL_MS = 5 * 60 * 1_000;

export type TrustedOpenClawToolEvent = Readonly<{
  toolName: string;
  toolCallId?: string;
  runId?: string;
}>;

export type TrustedOpenClawToolContext = Readonly<{
  runId?: string;
  toolCallId?: string;
}>;

export type OpenClawToolCorrelationBridge = Readonly<{
  remember(
    event: TrustedOpenClawToolEvent,
    context: TrustedOpenClawToolContext,
  ): void;
  consume(toolCallId: string): string | undefined;
  discard(toolCallId: string): void;
}>;

export function registerOpenClawToolCorrelation(
  api: Pick<OpenClawPluginApi, 'on'>,
  bridge: OpenClawToolCorrelationBridge,
): void {
  api.on('before_tool_call', (event, context) => {
    bridge.remember(event, context);
  });
}

export function createOpenClawToolCorrelationBridge(options: Readonly<{
  toolName?: string;
  maxBindings?: number;
  ttlMs?: number;
  now?: () => number;
}> = {}): OpenClawToolCorrelationBridge {
  const toolName = options.toolName ?? 'dispatch_search';
  const maxBindings = positiveInteger(options.maxBindings, DEFAULT_MAX_BINDINGS);
  const ttlMs = positiveInteger(options.ttlMs, DEFAULT_TTL_MS);
  const now = options.now ?? Date.now;
  const bindings = new Map<string, { runId: string | null; expiresAt: number }>();

  const prune = () => {
    const current = now();
    for (const [toolCallId, binding] of bindings) {
      if (binding.expiresAt <= current) bindings.delete(toolCallId);
    }
  };

  return {
    remember(event, context) {
      if (event.toolName !== toolName) return;
      prune();
      const eventToolCallId = trustedId(event.toolCallId);
      const contextToolCallId = trustedId(context.toolCallId);
      if (
        eventToolCallId
        && contextToolCallId
        && eventToolCallId !== contextToolCallId
      ) return;
      const toolCallId = eventToolCallId ?? contextToolCallId;
      const eventRunId = trustedId(event.runId);
      const contextRunId = trustedId(context.runId);
      if (eventRunId && contextRunId && eventRunId !== contextRunId) return;
      const runId = eventRunId ?? contextRunId;
      if (!toolCallId || !runId) return;
      const existing = bindings.get(toolCallId);
      if (existing) {
        if (existing.runId !== runId) {
          bindings.set(toolCallId, { runId: null, expiresAt: now() + ttlMs });
        }
        return;
      }
      if (bindings.size >= maxBindings) return;
      bindings.set(toolCallId, { runId, expiresAt: now() + ttlMs });
    },
    consume(toolCallId) {
      prune();
      const key = trustedId(toolCallId);
      if (!key) return undefined;
      const binding = bindings.get(key);
      bindings.delete(key);
      return binding?.runId ?? undefined;
    },
    discard(toolCallId) {
      const key = trustedId(toolCallId);
      if (key) bindings.delete(key);
    },
  };
}

function trustedId(value: string | undefined): string | undefined {
  if (typeof value !== 'string') return undefined;
  if (
    value.length === 0
    || [...value].length > 512
    || value.trim() !== value
    || value.includes('\0')
  ) return undefined;
  return value;
}

function positiveInteger(value: number | undefined, fallback: number): number {
  return Number.isInteger(value) && value! > 0 ? value! : fallback;
}
