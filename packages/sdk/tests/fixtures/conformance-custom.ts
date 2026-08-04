import type { CaptureSource, SourceSink, TraceIdentity } from '../../src/v1/types.js';

type Event = { type: string; value?: unknown };
type Listener = (event: Event) => void;

export class ConformanceSubject {
  private readonly listeners = new Set<Listener>();
  subscribe(listener: Listener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }
  emit(type: string, value?: unknown): unknown {
    const event = { type, value };
    for (const listener of this.listeners) listener(event);
    return value;
  }
}

export function createSubject(): ConformanceSubject {
  return new ConformanceSubject();
}

export const expectations = Object.freeze({
  lifecycle: 'lifecycle-ok', stream: 'stream-ok', error: 'error-ok',
  unknown: 'unknown-ok', rejection: 'rejection-ok', shutdown: 'shutdown-ok',
});

/** Repository-only source fixture used to verify adapter conformance. */
export function createSource({ subject }: { subject: ConformanceSubject }): CaptureSource {
  return {
    metadata: {
      name: 'conformance:custom-source', seam: 'subject.subscribe',
      identityDomain: 'conformance.operation', coverage: [], version: '1',
    },
    install(sink) {
      let open: TraceIdentity | undefined;
      const unsubscribe = subject.subscribe((event) => { open = handle(event, sink, open); });
      return { deactivate: unsubscribe, drain() {} };
    },
  };
}

function handle(event: Event, sink: SourceSink, current?: TraceIdentity): TraceIdentity | undefined {
  if (event.type.endsWith('.start')) {
    const name = event.type.slice(0, -6);
    const opened = sink.openTrace({
      name,
      native: event,
      semantic: { type: 'agent.run', name },
    });
    return opened.accepted ? opened.identity : undefined;
  }
  const trace = current ?? openFallback(sink, event.type);
  if (!trace) return current;
  if (event.type === 'stream.delta') sink.record({
    kind: 'stream',
    phase: 'event',
    name: event.type,
    trace,
    native: event,
    semantic: { type: 'message', role: 'assistant', content: event.value ?? null },
  });
  else if (event.type.endsWith('.error')) {
    const name = event.type.slice(0, -6);
    const error = { type: 'error', message: 'fixture failure', recoverable: false };
    sink.record({
      kind: 'error',
      phase: 'error',
      name: event.type,
      trace,
      native: event,
      semantic: { type: 'agent.error', error },
    });
    sink.record({
      kind: 'lifecycle',
      phase: 'error',
      name,
      trace,
      native: event,
      semantic: { type: 'agent.run', status: 'failed', error },
    });
    return undefined;
  } else if (event.type.endsWith('.end')) {
    sink.record({
      kind: 'lifecycle',
      phase: 'end',
      name: event.type.slice(0, -4),
      trace,
      native: event,
      semantic: { type: 'agent.run', status: 'succeeded' },
    });
    return undefined;
  } else sink.record({ kind: 'unknown', phase: 'event', name: event.type, trace, native: event });
  return trace;
}

function openFallback(sink: SourceSink, name: string): TraceIdentity | undefined {
  const opened = sink.openTrace({
    name,
    native: { implicit: true },
    semantic: { type: 'agent.run', name },
  });
  return opened.accepted ? opened.identity : undefined;
}

export async function lifecycle(subject: ConformanceSubject): Promise<string> {
  subject.emit('lifecycle.start', { input: true });
  subject.emit('lifecycle.end', { output: true });
  return 'lifecycle-ok';
}

export async function unknown(subject: ConformanceSubject): Promise<string> {
  subject.emit('unknown.start');
  subject.emit('unknown.event', { future: true });
  subject.emit('unknown.end');
  return 'unknown-ok';
}

export async function stream(subject: ConformanceSubject): Promise<string> {
  subject.emit('stream.start');
  subject.emit('stream.delta', { text: 'one' });
  subject.emit('stream.end');
  return 'stream-ok';
}

export async function error(subject: ConformanceSubject): Promise<string> {
  subject.emit('failure.start');
  subject.emit('failure.error', { cause: 'fixture' });
  return 'error-ok';
}

export async function rejection(subject: ConformanceSubject): Promise<string> {
  subject.emit('rejection.start');
  subject.emit('rejection.event', {
    payload: 'semantic-layer-conformance-secret-value',
  });
  subject.emit('rejection.end');
  return 'rejection-ok';
}

export async function shutdown(subject: ConformanceSubject): Promise<string> {
  subject.emit('shutdown.start', { active_at_shutdown: true });
  return 'shutdown-ok';
}
