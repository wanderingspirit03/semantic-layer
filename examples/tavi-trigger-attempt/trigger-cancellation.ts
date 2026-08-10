export type TaviTriggerCancellation = Readonly<{
  signal: AbortSignal;
  completeTelemetry(): void;
  release(): void;
}>;

export type TaviTriggerCancellationRegistry = Readonly<{
  register(runId: string): TaviTriggerCancellation;
  cancel(runId: string): Promise<void>;
}>;

/** Join Trigger's task-local cancellation hook to one attempt's finalizer. */
export function createTaviTriggerCancellationRegistry(): TaviTriggerCancellationRegistry {
  const attempts = new Map<string, {
    controller: AbortController;
    telemetryDone: Promise<void>;
    resolveTelemetryDone: () => void;
  }>();

  return {
    register(runId: string): TaviTriggerCancellation {
      if (!runId || attempts.has(runId)) {
        throw new Error('Trigger run ID must identify one active attempt');
      }
      const controller = new AbortController();
      let resolveTelemetryDone!: () => void;
      const telemetryDone = new Promise<void>((resolve) => {
        resolveTelemetryDone = resolve;
      });
      const entry = { controller, telemetryDone, resolveTelemetryDone };
      attempts.set(runId, entry);
      let released = false;
      return {
        signal: controller.signal,
        completeTelemetry() {
          resolveTelemetryDone();
        },
        release() {
          if (released) return;
          released = true;
          resolveTelemetryDone();
          if (attempts.get(runId) === entry) attempts.delete(runId);
        },
      };
    },
    async cancel(runId: string): Promise<void> {
      const entry = attempts.get(runId);
      if (!entry) return;
      entry.controller.abort();
      await entry.telemetryDone;
    },
  };
}
