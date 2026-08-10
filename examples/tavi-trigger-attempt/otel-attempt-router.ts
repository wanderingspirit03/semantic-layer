import { AsyncLocalStorage } from 'node:async_hooks';
import type { Context, Span } from '@opentelemetry/api';
import type { ReadableSpan, SpanProcessor } from '@opentelemetry/sdk-trace-base';
import type { OpenTelemetrySource } from 'semantic-layer-capture';

export type TaviOpenTelemetryAttemptRouter = Readonly<{
  /** Add this processor to Tavi's existing provider exactly once. */
  spanProcessor: SpanProcessor;
  /** Keep one attempt source routable until capture finalization finishes. */
  registerSource(source: OpenTelemetrySource): () => void;
  /** Run one enrolled attempt with its own Semantic Layer source. */
  runWithSource<T>(source: OpenTelemetrySource, task: () => T): T;
}>;

/**
 * Route each span from a shared provider to the one enrolled attempt that
 * started it. Spans outside an enrolled attempt remain visible to the other
 * provider processors and are ignored by Semantic Layer.
 */
export function createTaviOpenTelemetryAttemptRouter(): TaviOpenTelemetryAttemptRouter {
  const currentSource = new AsyncLocalStorage<OpenTelemetrySource>();
  const owners = new Map<string, OpenTelemetrySource>();
  const activeSources = new Set<OpenTelemetrySource>();

  const spanProcessor: SpanProcessor = {
    onStart(span: Span, parentContext: Context): void {
      const source = currentSource.getStore();
      if (!source || !activeSources.has(source)) return;
      const key = spanKey(span);
      if (!key || owners.has(key)) return;
      owners.set(key, source);
      try {
        source.spanProcessor.onStart(span, parentContext);
      } catch {
        owners.delete(key);
      }
    },
    onEnd(span: ReadableSpan): void {
      const key = spanKey(span);
      if (!key) return;
      const source = owners.get(key);
      if (!source) return;
      owners.delete(key);
      try {
        source.spanProcessor.onEnd(span);
      } catch {
        // Telemetry must never change the application span lifecycle.
      }
    },
    async forceFlush(): Promise<void> {
      await settleSources(activeSources, 'forceFlush');
    },
    async shutdown(): Promise<void> {
      owners.clear();
      await settleSources(activeSources, 'shutdown');
      activeSources.clear();
    },
  };

  return {
    spanProcessor,
    registerSource(source: OpenTelemetrySource): () => void {
      if (activeSources.has(source)) {
        throw new Error('OpenTelemetry attempt source is already registered');
      }
      activeSources.add(source);
      let registered = true;
      return () => {
        if (!registered) return;
        registered = false;
        activeSources.delete(source);
        for (const [key, owner] of owners) {
          if (owner === source) owners.delete(key);
        }
      };
    },
    runWithSource<T>(source: OpenTelemetrySource, task: () => T): T {
      return currentSource.run(source, task);
    },
  };
}

function spanKey(span: Pick<Span, 'spanContext'>): string | undefined {
  try {
    const context = span.spanContext();
    return context.traceId && context.spanId
      ? `${context.traceId}:${context.spanId}`
      : undefined;
  } catch {
    return undefined;
  }
}

async function settleSources(
  sources: Iterable<OpenTelemetrySource>,
  operation: 'forceFlush' | 'shutdown',
): Promise<void> {
  await Promise.allSettled([...sources].map(async (source) => {
    await source.spanProcessor[operation]();
  }));
}
