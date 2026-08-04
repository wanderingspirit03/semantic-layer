import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { isDeepStrictEqual, types as utilTypes } from 'node:util';
import { initialize, resetCaptureForTests } from '../../src/v1/runtime.js';
import { validateArtifact } from '../../src/v1/validation.js';
import type { CaptureSource, SourceRecord, SourceSink } from '../../src/v1/types.js';

const CASES = ['lifecycle', 'stream', 'error', 'unknown', 'rejection', 'shutdown'] as const;
const SECRET = 'semantic-layer-conformance-secret-value';
const primordialReflectApply = Reflect.apply;
const primordialSymbolKeyFor = Symbol.keyFor;

type AdapterModule = { createSource(input: { subject: unknown }): CaptureSource };
type DriverModule = {
  createSubject(): unknown | Promise<unknown>;
  expectations: Record<string, unknown>;
} & Record<string, unknown>;

export type ConformanceReport = { valid: boolean; cases: number; issues: string[] };

export async function verifyAdapterConformance(
  adapter: AdapterModule,
  driver: DriverModule,
): Promise<ConformanceReport> {
  const issues: string[] = [];
  const output = await mkdtemp(join(tmpdir(), 'semantic-layer-adapter-'));
  try {
    const controlSubject = await driver.createSubject();
    const runners = new Map<typeof CASES[number], (...args: unknown[]) => unknown>();
    const controls = new Map<typeof CASES[number], CaseOutcome>();
    for (const name of CASES) {
      const run = driver[name];
      if (typeof run !== 'function' || !(name in driver.expectations)) {
        issues.push(`CASE_CONTRACT_MISSING:${name}`);
        continue;
      }
      const caseRunner = run as (...args: unknown[]) => unknown;
      runners.set(name, caseRunner);
      controls.set(name, await observeCase(caseRunner, driver, controlSubject));
    }
    const subject = await driver.createSubject();
    const capture = initialize({ output, serviceName: 'adapter-conformance', secretValues: [SECRET] });
    const source = adapter.createSource({ subject });
    const sourceName = source.metadata.name;
    if (typeof sourceName !== 'string' || sourceName.length === 0) {
      issues.push('SOURCE_METADATA_NAME_MISSING');
    }
    capture.installSource(source);
    let probeSink: SourceSink | undefined;
    capture.installSource({
      metadata: {
        name: 'conformance:source-input-probe', seam: 'conformance.runtime-probe',
        identityDomain: 'conformance.runtime-probe', coverage: [],
      },
      install(sink) {
        probeSink = sink;
        return { deactivate() {}, drain() {} };
      },
    });
    const probeOpened = probeSink?.openTrace({ name: 'conformance.source-input-probe' });
    if (!probeOpened || !probeOpened.accepted) {
      issues.push('SOURCE_INPUT_PROBE_OPEN_REJECTED');
    } else {
      const identitySnapshot = { ...probeOpened.identity };
      let immutable = Object.isFrozen(probeOpened.identity);
      try {
        for (const key of ['runId', 'traceId', 'operationId'] as const) {
          if (Reflect.set(probeOpened.identity, key, `tampered-${key}`)) immutable = false;
        }
      } catch {
        immutable = false;
      }
      if (!immutable || !isDeepStrictEqual(probeOpened.identity, identitySnapshot)) {
        issues.push('SOURCE_TRACE_IDENTITY_MUTABLE');
      }
      const forgedIdentity = probeSink!.record({
        kind: 'log', phase: 'event', name: 'conformance.forged-identity',
        trace: { ...probeOpened.identity, traceId: 'trace_conformance_forged' }, native: null,
      });
      if (forgedIdentity.accepted || forgedIdentity.reason !== 'invalid_record') {
        issues.push('SOURCE_TRACE_IDENTITY_FORGERY_NOT_REJECTED');
      }
      await forgedIdentity.settled;
      const forgedLoss = probeSink!.record({
        kind: 'loss', phase: 'gap', name: 'conformance.forged-loss',
        trace: probeOpened.identity, native: null,
        loss: { reason: 'serialization_failure', stage: 'source', recoverable: false },
      } as unknown as SourceRecord);
      if (forgedLoss.accepted || forgedLoss.reason !== 'invalid_record') {
        issues.push('SOURCE_LOSS_KIND_NOT_REJECTED');
      }
      const probeEnded = probeSink!.record({
        kind: 'lifecycle', phase: 'end', name: 'conformance.source-input-probe',
        trace: probeOpened.identity, native: null,
      });
      if (!probeEnded.accepted) issues.push('SOURCE_INPUT_PROBE_END_REJECTED');
    }
    for (const name of CASES) {
      const caseRunner = runners.get(name);
      const control = controls.get(name);
      if (!caseRunner || !control) continue;
      const observed = await observeCase(caseRunner, driver, subject);
      if (control.status === 'unsupported' || observed.status === 'unsupported') {
        issues.push(`CASE_OUTCOME_UNSUPPORTED:${name}`);
        continue;
      }
      if (!isDeepStrictEqual(observed, control)) issues.push(`CASE_PARITY_MISMATCH:${name}`);
      if (observed.status === 'returned'
        && observed.protocol === undefined
        && !isDeepStrictEqual(observed.value, comparable(driver.expectations[name]))) {
        issues.push(`CASE_RETURN_MISMATCH:${name}`);
      }
    }
    const status = await capture.shutdown();
    const artifact = await validateArtifact(status.artifactPath);
    if (!artifact.valid) issues.push(...artifact.issues.map((issue) => `ARTIFACT:${issue}`));
    const text = await readFile(join(status.artifactPath, 'trace.jsonl'), 'utf8');
    const rows = text.trim().split('\n').filter(Boolean).map((line) => JSON.parse(line)) as Array<Record<string, any>>;
    const manifest = JSON.parse(
      await readFile(join(status.artifactPath, 'manifest.json'), 'utf8'),
    ) as { sources?: Array<{ id?: string; name?: string }> };
    const sourceId = manifest.sources?.find((source) => source.name === sourceName)?.id;
    if (!rows.some((row) => row.kind === 'loss'
      && row.data?.reason === 'source_rejection')) {
      issues.push('SOURCE_TRACE_IDENTITY_REJECTION_EVIDENCE_MISSING');
    }
    if (!rows.some((row) => row.kind === 'loss'
      && row.data?.reason === 'serialization_failure'
      && row.data?.path === '/event_kind')) {
      issues.push('SOURCE_LOSS_REJECTION_EVIDENCE_MISSING');
    }
    const sourceRows = rows.filter((row) => row.source === sourceId);
    if (sourceRows.length === 0) issues.push('SOURCE_EVIDENCE_MISSING');
    const starts = sourceRows.filter((row) => row.kind === 'run.start');
    const outcomes = sourceRows.filter((row) => row.kind === 'run.outcome');
    if (starts.length === 0) issues.push('LIFECYCLE_START_MISSING');
    if (!outcomes.some((row) => row.data?.status === 'completed')) issues.push('LIFECYCLE_END_MISSING');
    if (!sourceRows.some((row) => row.kind === 'message')) issues.push('STREAM_EVIDENCE_MISSING');
    if (!sourceRows.some((row) => row.kind === 'error')) issues.push('ERROR_EVIDENCE_MISSING');
    if (!sourceRows.some((row) => row.kind === 'loss'
      && row.data?.reason === 'unsupported_semantic_projection')) {
      issues.push('UNKNOWN_EVIDENCE_MISSING');
    }
    if (!outcomes.some((row) => row.data?.status === 'cancelled')) issues.push('ACTIVE_SHUTDOWN_MISSING');
    const startIds = new Set(starts.map((row) => row.id));
    if (outcomes.some((row) => !startIds.has(row.parent))) issues.push('SOURCE_PARENTAGE_INVALID');
    if (status.queue.pendingBytes !== 0 || status.queue.pendingControlBytes !== 0) {
      issues.push('ADMITTED_RECORDS_UNSETTLED');
    }
    if (!rows.some((row) => row.kind === 'loss'
      && ['credential_redaction', 'scrubber_failure_payload_omitted'].includes(row.data?.reason))) {
      issues.push('SECRET_REDACTION_LOSS_MISSING');
    }
    if (text.includes(SECRET)) issues.push('SECRET_PERSISTED');
    return { valid: issues.length === 0, cases: CASES.length, issues };
  } catch (error) {
    return {
      valid: false,
      cases: CASES.length,
      issues: [`ADAPTER_CONFORMANCE_FAILED:${error instanceof Error ? `${error.name}:${error.message}` : String(error)}`],
    };
  } finally {
    await resetCaptureForTests();
    await rm(output, { recursive: true, force: true });
  }
}

type CaseOutcome = {
  status: 'returned' | 'threw' | 'unsupported';
  value?: unknown;
  error?: unknown;
  protocol?: unknown;
};

async function observeCase(
  run: (...args: unknown[]) => unknown,
  receiver: unknown,
  subject: unknown,
): Promise<CaseOutcome> {
  let value: unknown;
  try {
    value = await Reflect.apply(run, receiver, [subject]);
  } catch (error) {
    try {
      return { status: 'threw', error: comparableError(error) };
    } catch (observationError) {
      return { status: 'unsupported', error: observationReason(observationError) };
    }
  }
  try {
    const iteratorFactory = dataMethod(value, Symbol.asyncIterator);
    if (iteratorFactory) {
      return { status: 'returned', protocol: await observeAsyncProtocol(value as object, iteratorFactory) };
    }
    return { status: 'returned', value: comparable(value) };
  } catch (error) {
    return { status: 'unsupported', error: observationReason(error) };
  }
}

async function observeAsyncProtocol(
  iterable: object,
  factory: (...args: unknown[]) => unknown,
): Promise<unknown> {
  const values: unknown[] = [];
  const iterator = Reflect.apply(factory, iterable, []) as object;
  const next = requiredDataMethod(iterator, 'next');
  for (let index = 0; index < 64; index += 1) {
    const part = await Reflect.apply(next, iterator, []) as IteratorResult<unknown>;
    values.push(comparable(part));
    if (part.done) break;
    if (index === 63) values.push({ bounded: true });
  }
  const returnSentinel = { conformance: 'return-sentinel' };
  const returnIterator = Reflect.apply(factory, iterable, []) as object;
  const returnMethod = dataMethod(returnIterator, 'return');
  const returned = returnMethod
    ? await settleProtocol(
      () => Promise.resolve(Reflect.apply(returnMethod, returnIterator, [returnSentinel])) as Promise<IteratorResult<unknown>>,
      returnSentinel,
    )
    : { supported: false };
  const throwSentinel = new Error('conformance-throw-sentinel');
  const throwIterator = Reflect.apply(factory, iterable, []) as object;
  const throwMethod = dataMethod(throwIterator, 'throw');
  const thrown = throwMethod
    ? await settleProtocol(
      () => Promise.resolve(Reflect.apply(throwMethod, throwIterator, [throwSentinel])) as Promise<IteratorResult<unknown>>,
      throwSentinel,
    )
    : { supported: false };
  return { values, returned, thrown };
}

async function settleProtocol(run: () => Promise<IteratorResult<unknown>>, sentinel: unknown): Promise<unknown> {
  let result: IteratorResult<unknown>;
  try {
    result = await run();
  } catch (error) {
    return { supported: true, status: 'threw', error: comparableError(error), sentinel: error === sentinel };
  }
  return { supported: true, status: 'returned', result: comparable(result), sentinel: result.value === sentinel };
}

function comparable(value: unknown, seen = new WeakSet<object>()): unknown {
  if (value === null || value === undefined || ['string', 'number', 'boolean'].includes(typeof value)) {
    return value;
  }
  if (typeof value === 'bigint') return { bigint: value.toString() };
  if (typeof value === 'symbol') return symbolSnapshot(value);
  if ((typeof value !== 'object' && typeof value !== 'function') || utilTypes.isProxy(value)) {
    throw new ConformanceObservationError('unsupported or proxied return value');
  }
  if (typeof value === 'function') throw new ConformanceObservationError('function return value');
  if (seen.has(value)) return { circular: true };
  seen.add(value);
  if (utilTypes.isMap(value)) {
    return { prototype: prototypeIdentity(Object.getPrototypeOf(value)), map: [...Map.prototype.entries.call(value) as Iterable<[unknown, unknown]>]
      .map(([key, item]) => [comparable(key, seen), comparable(item, seen)]) };
  }
  if (utilTypes.isSet(value)) {
    return { prototype: prototypeIdentity(Object.getPrototypeOf(value)), set: [...Set.prototype.values.call(value) as Iterable<unknown>]
      .map((item) => comparable(item, seen)) };
  }
  const descriptors = Object.getOwnPropertyDescriptors(value);
  const keys = Reflect.ownKeys(descriptors)
    .filter((key) => !(utilTypes.isNativeError(value) && key === 'stack'))
    .sort((left, right) => propertyLabel(left).localeCompare(propertyLabel(right)));
  const prototype = Object.getPrototypeOf(value) as object | null;
  if (keys.length === 0 && prototype !== Object.prototype && prototype !== null) {
    throw new ConformanceObservationError('opaque return value');
  }
  return {
    prototype: prototypeIdentity(prototype),
    object: keys.map((key) => {
      const descriptor = descriptors[key as keyof typeof descriptors]!;
      if (!('value' in descriptor)) {
        throw new ConformanceObservationError(`accessor return property: ${propertyLabel(key)}`);
      }
      return {
        key: propertySnapshot(key), value: comparable(descriptor.value, seen),
        enumerable: descriptor.enumerable, configurable: descriptor.configurable,
        writable: descriptor.writable,
      };
    }),
  };
}

function comparableError(error: unknown): unknown {
  return comparable(error);
}

class ConformanceObservationError extends Error {}

const prototypeIdentities = new WeakMap<object, number>();
let nextPrototypeIdentity = 0;

function prototypeIdentity(prototype: object | null): number {
  if (prototype === null) return 0;
  const existing = prototypeIdentities.get(prototype);
  if (existing !== undefined) return existing;
  const assigned = ++nextPrototypeIdentity;
  prototypeIdentities.set(prototype, assigned);
  return assigned;
}

function observationReason(error: unknown): string {
  return error instanceof ConformanceObservationError ? error.message : 'observation failed';
}

function propertyLabel(key: PropertyKey): string {
  if (typeof key !== 'symbol') return `string:${key}`;
  const snapshot = symbolSnapshot(key);
  return snapshot.registry === 'global'
    ? `symbol:global:${snapshot.key}`
    : 'symbol:local';
}

function symbolSnapshot(value: symbol):
  | { symbol: true; registry: 'global'; key: string }
  | { symbol: true; registry: 'local'; identity: symbol } {
  const key = primordialReflectApply(
    primordialSymbolKeyFor,
    Symbol,
    [value],
  ) as string | undefined;
  if (key !== undefined) return { symbol: true, registry: 'global', key };
  return { symbol: true, registry: 'local', identity: value };
}

function propertySnapshot(key: PropertyKey): unknown {
  return typeof key === 'symbol'
    ? symbolSnapshot(key)
    : { property: 'string', value: key };
}

function dataMethod(value: unknown, key: PropertyKey): ((...args: unknown[]) => unknown) | undefined {
  if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return undefined;
  if (utilTypes.isProxy(value)) throw new ConformanceObservationError('proxied protocol surface');
  let current: object | null = value;
  while (current !== null) {
    if (utilTypes.isProxy(current)) throw new ConformanceObservationError('proxied protocol prototype');
    const descriptor = Object.getOwnPropertyDescriptor(current, key);
    if (descriptor) {
      if (!('value' in descriptor)) {
        throw new ConformanceObservationError(`accessor protocol property: ${propertyLabel(key)}`);
      }
      if (descriptor.value === undefined) return undefined;
      if (typeof descriptor.value !== 'function') {
        throw new ConformanceObservationError(`non-callable protocol property: ${propertyLabel(key)}`);
      }
      return descriptor.value as (...args: unknown[]) => unknown;
    }
    current = Object.getPrototypeOf(current) as object | null;
  }
  return undefined;
}

function requiredDataMethod(value: unknown, key: PropertyKey): (...args: unknown[]) => unknown {
  const method = dataMethod(value, key);
  if (!method) throw new ConformanceObservationError(`missing protocol property: ${propertyLabel(key)}`);
  return method;
}
