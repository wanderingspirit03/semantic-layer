import type {
  AdmissionReceipt,
  CaptureSource,
  SourceSink,
  TraceIdentity,
} from 'semantic-layer-capture';

const SETTLED = Promise.resolve();

export type RunSourceOptions = {
  runId: string;
  conversationId: string;
  hostVersion: string;
  qualification: {
    status: 'exact_qualified' | 'capability_checked_unqualified' | 'unknown';
    profile: string;
  };
  input?: unknown;
  unavailableHooks?: readonly string[];
};

export type RunSource = {
  source: CaptureSource;
  start(): AdmissionReceipt;
  event(input: RunSourceEvent): AdmissionReceipt;
  gap(reason: string, detail: Record<string, unknown>): AdmissionReceipt;
};

export type RunSourceEvent = {
  kind:
    | 'lifecycle'
    | 'model'
    | 'tool'
    | 'state'
    | 'log'
    | 'error'
    | 'stream'
    | 'correlation'
    | 'unknown';
  phase: 'start' | 'event' | 'end' | 'error' | 'cancelled' | 'gap';
  name: string;
  native: unknown;
  semantic: Record<string, unknown>;
  nativeIdentity?: string;
  parentRecordId?: string;
};

export function createRunSource(options: RunSourceOptions): RunSource {
  let sink: SourceSink | undefined;
  let identity: TraceIdentity | undefined;
  let rootRecordId: string | undefined;
  const pending = new Set<Promise<void>>();

  const source: CaptureSource = {
    metadata: {
      name: 'openclaw',
      version: options.hostVersion,
      seam: 'openclaw.plugin.typed-hooks',
      identityDomain: 'openclaw.run',
      coverage: [
        { operation: 'agent.run', domain: 'openclaw.run', role: 'owner' },
      ],
      qualification: options.qualification,
    },
    install(installedSink) {
      if (sink) throw new Error('OpenClaw run source is already installed');
      sink = installedSink;
      return {
        deactivate() {
          sink = undefined;
        },
        async drain() {
          await Promise.allSettled([...pending]);
        },
      };
    },
  };

  const track = (receipt: AdmissionReceipt): AdmissionReceipt => {
    pending.add(receipt.settled);
    void receipt.settled.finally(() => pending.delete(receipt.settled));
    return receipt;
  };

  return {
    source,
    start() {
      if (!sink) return rejected('source_not_installed');
      if (identity) return rejected('duplicate_run_start');
      const opened = sink.openTrace({
        name: 'openclaw.agent.run',
        nativeIdentity: options.runId,
        conversationId: options.conversationId,
        turnId: options.runId,
        native: { runId: options.runId },
        semantic: {
          type: 'agent.run',
          name: 'openclaw.agent.run',
          ...(options.input === undefined ? {} : { input: options.input }),
        },
      });
      if (opened.accepted) {
        identity = opened.identity;
        rootRecordId = opened.recordId;
        track(opened);
        for (const hook of options.unavailableHooks ?? []) {
          this.gap('unsupported_native_value', {
            type: 'openclaw.hook.unavailable',
            hook,
          });
        }
      }
      return opened;
    },
    event(input) {
      if (!sink || !identity) return rejected('run_not_started');
      const parentRecordId = input.parentRecordId ?? rootRecordId;
      return track(
        sink.record({
          kind: input.kind,
          phase: input.phase,
          name: input.name,
          trace: identity,
          ...(input.nativeIdentity
            ? { nativeIdentity: input.nativeIdentity }
            : {}),
          ...(parentRecordId ? { parentRecordId } : {}),
          native: input.native,
          semantic: input.semantic,
        }),
      );
    },
    gap(reason, detail) {
      if (!sink || !identity) return rejected('run_not_started');
      const description = gapDescription(detail);
      return track(
        sink.record({
          kind: 'unknown',
          phase: 'gap',
          name: 'openclaw.capture.gap',
          trace: identity,
          ...(rootRecordId ? { parentRecordId: rootRecordId } : {}),
          native: { reason, ...detail },
          semantic: { type: 'capture.gap', reason, detail: description },
        }),
      );
    },
  };
}

function gapDescription(detail: Record<string, unknown>): string {
  if (typeof detail.detail === 'string') return detail.detail.slice(0, 4_096);
  try {
    return JSON.stringify(detail).slice(0, 4_096);
  } catch {
    return 'OpenClaw evidence was unavailable.';
  }
}

function rejected(reason: string): AdmissionReceipt {
  return { accepted: false, reason, settled: SETTLED };
}
