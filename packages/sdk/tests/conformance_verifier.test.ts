import { afterEach, expect, it } from 'vitest';
import * as Driver from './fixtures/conformance-custom.js';
import { verifyAdapterConformance } from './helpers/conformance-verify.js';
import { resetCaptureForTests } from '../src/index.js';

afterEach(async () => resetCaptureForTests());

it('proves lifecycle, evidence, parity, loss, privacy, parentage, and active shutdown', async () => {
  const report = await verifyAdapterConformance(Driver, Driver);
  expect(report).toEqual({ valid: true, cases: 6, issues: [] });
});

it('rejects a validly-shaped no-op source', async () => {
  const noOp = {
    createSource() {
      return {
        metadata: {
          name: 'conformance:custom-source', seam: 'noop',
          identityDomain: 'conformance.operation', coverage: [],
        },
        install() { return { deactivate() {}, drain() {} }; },
      };
    },
  };
  const report = await verifyAdapterConformance(noOp, Driver);
  expect(report.valid).toBe(false);
  expect(report.issues).toEqual(expect.arrayContaining([
    'SOURCE_EVIDENCE_MISSING', 'STREAM_EVIDENCE_MISSING', 'ERROR_EVIDENCE_MISSING',
    'UNKNOWN_EVIDENCE_MISSING', 'ACTIVE_SHUTDOWN_MISSING',
    'SECRET_REDACTION_LOSS_MISSING',
  ]));
});

it('derives source identity from the adapter instead of the bundled fixture name', async () => {
  const alternate = {
    createSource(input: { subject: unknown }) {
      const source = Driver.createSource(input as Parameters<typeof Driver.createSource>[0]);
      return { ...source, metadata: { ...source.metadata, name: 'conformance:alternate-source' } };
    },
  };
  await expect(verifyAdapterConformance(alternate, Driver)).resolves.toEqual({
    valid: true, cases: 6, issues: [],
  });
});

it('rejects stream/control behavior changed by installation despite self-expectations', async () => {
  type Subject = ReturnType<typeof Driver.createSubject> & { parityValue: string };
  const driver = {
    ...Driver,
    expectations: { ...Driver.expectations, lifecycle: 'mutated' },
    createSubject() {
      const subject = Driver.createSubject() as Subject;
      subject.parityValue = 'control';
      return subject;
    },
    async lifecycle(subject: Subject) {
      await Driver.lifecycle(subject);
      return {
        async *[Symbol.asyncIterator]() {
          try {
            yield `value:${subject.parityValue}`;
          } catch {
            yield `caught:${subject.parityValue}`;
          }
          return `terminal:${subject.parityValue}`;
        },
      };
    },
  };
  const mutating = {
    createSource({ subject }: { subject: unknown }) {
      (subject as Subject).parityValue = 'mutated';
      return Driver.createSource({ subject: subject as Subject });
    },
  };
  const report = await verifyAdapterConformance(mutating, driver);
  expect(report.valid).toBe(false);
  expect(report.issues).toContain('CASE_PARITY_MISMATCH:lifecycle');
});

it('rejects accessor outcomes without invoking the getter', async () => {
  let getterCalls = 0;
  const driver = {
    ...Driver,
    async lifecycle(subject: ReturnType<typeof Driver.createSubject>) {
      await Driver.lifecycle(subject);
      return Object.defineProperty({}, 'hostile', {
        enumerable: true,
        get() {
          getterCalls += 1;
          throw new Error('getter must not run');
        },
      });
    },
  };
  const report = await verifyAdapterConformance(Driver, driver);
  expect(report.valid).toBe(false);
  expect(report.issues).toContain('CASE_OUTCOME_UNSUPPORTED:lifecycle');
  expect(getterCalls).toBe(0);
});

it('snapshots control outcomes before adapter creation can mutate a shared prototype', async () => {
  class SharedSubject extends Driver.ConformanceSubject {
    behavior() { return 'control'; }
  }
  const driver = {
    ...Driver,
    expectations: { ...Driver.expectations, lifecycle: 'observed' },
    createSubject: () => new SharedSubject(),
    async lifecycle(subject: SharedSubject) {
      await Driver.lifecycle(subject);
      return subject.behavior();
    },
  };
  const mutating = {
    createSource({ subject }: { subject: unknown }) {
      SharedSubject.prototype.behavior = () => 'observed';
      return Driver.createSource({ subject: subject as SharedSubject });
    },
  };
  const report = await verifyAdapterConformance(mutating, driver);
  expect(report.valid).toBe(false);
  expect(report.issues).toContain('CASE_PARITY_MISMATCH:lifecycle');
});

it('binds native Error class identity without reading getters', async () => {
  type Subject = ReturnType<typeof Driver.createSubject> & { ErrorType: ErrorConstructor };
  const driver = {
    ...Driver,
    expectations: { ...Driver.expectations, lifecycle: new Error('same') },
    createSubject() {
      const subject = Driver.createSubject() as Subject;
      subject.ErrorType = Error;
      return subject;
    },
    async lifecycle(subject: Subject) {
      await Driver.lifecycle(subject);
      return new subject.ErrorType('same');
    },
  };
  const mutating = {
    createSource({ subject }: { subject: unknown }) {
      (subject as Subject).ErrorType = TypeError;
      return Driver.createSource({ subject: subject as Subject });
    },
  };
  const report = await verifyAdapterConformance(mutating, driver);
  expect(report.valid).toBe(false);
  expect(report.issues).toContain('CASE_PARITY_MISMATCH:lifecycle');
});

it('distinguishes global registry symbols from local symbols with the same description', async () => {
  type Subject = ReturnType<typeof Driver.createSubject> & { symbolValue: symbol };
  const local = Symbol('same');
  const driver = {
    ...Driver,
    expectations: { ...Driver.expectations, lifecycle: local },
    createSubject() {
      const subject = Driver.createSubject() as Subject;
      subject.symbolValue = Symbol.for('same');
      return subject;
    },
    async lifecycle(subject: Subject) {
      await Driver.lifecycle(subject);
      return subject.symbolValue;
    },
  };
  const mutating = {
    createSource({ subject }: { subject: unknown }) {
      (subject as Subject).symbolValue = local;
      return Driver.createSource({ subject: subject as Subject });
    },
  };
  const report = await verifyAdapterConformance(mutating, driver);
  expect(report.valid).toBe(false);
  expect(report.issues).toContain('CASE_PARITY_MISMATCH:lifecycle');
});

it('supports only exact shared identity for driver-local symbols', async () => {
  type Subject = ReturnType<typeof Driver.createSubject> & { symbolValue: symbol };
  const shared = Symbol('driver-local');
  const sharedDriver = {
    ...Driver,
    expectations: { ...Driver.expectations, lifecycle: shared },
    createSubject() {
      const subject = Driver.createSubject() as Subject;
      subject.symbolValue = shared;
      return subject;
    },
    async lifecycle(subject: Subject) {
      await Driver.lifecycle(subject);
      return subject.symbolValue;
    },
  };
  await expect(verifyAdapterConformance(Driver, sharedDriver)).resolves.toMatchObject({ valid: true });

  const freshDriver = {
    ...sharedDriver,
    expectations: { ...Driver.expectations, lifecycle: Symbol('driver-local') },
    createSubject() {
      const subject = Driver.createSubject() as Subject;
      subject.symbolValue = Symbol('driver-local');
      return subject;
    },
  };
  const freshReport = await verifyAdapterConformance(Driver, freshDriver);
  expect(freshReport.valid).toBe(false);
  expect(freshReport.issues).toContain('CASE_PARITY_MISMATCH:lifecycle');
});

it('uses Symbol primordials captured before adapter installation', async () => {
  type Subject = ReturnType<typeof Driver.createSubject> & { symbolValue: symbol };
  const keyForDescriptor = Object.getOwnPropertyDescriptor(Symbol, 'keyFor')!;
  const descriptionDescriptor = Object.getOwnPropertyDescriptor(Symbol.prototype, 'description')!;
  const originalKeyFor = keyForDescriptor.value as (symbol: symbol) => string | undefined;
  const originalDescription = descriptionDescriptor.get!;
  const apply = Reflect.apply;
  let keyForCalls = 0;
  let descriptionCalls = 0;

  const run = async (control: symbol, observed: symbol) => {
    const driver = {
      ...Driver,
      expectations: { ...Driver.expectations, lifecycle: observed },
      createSubject() {
        const subject = Driver.createSubject() as Subject;
        subject.symbolValue = control;
        return subject;
      },
      async lifecycle(subject: Subject) {
        await Driver.lifecycle(subject);
        return subject.symbolValue;
      },
    };
    const poisoning = {
      createSource({ subject }: { subject: unknown }) {
        const source = Driver.createSource({ subject: subject as Subject });
        return {
          ...source,
          install(sink: Parameters<typeof source.install>[0]) {
            (subject as Subject).symbolValue = observed;
            Object.defineProperty(Symbol, 'keyFor', {
              ...keyForDescriptor,
              value(value: symbol) {
                keyForCalls += 1;
                return apply(originalKeyFor, Symbol, [value]) as string | undefined;
              },
            });
            Object.defineProperty(Symbol.prototype, 'description', {
              ...descriptionDescriptor,
              get(this: symbol) {
                descriptionCalls += 1;
                return apply(originalDescription, this, []) as string | undefined;
              },
            });
            return source.install(sink);
          },
        };
      },
    };
    try {
      return await verifyAdapterConformance(poisoning, driver);
    } finally {
      Object.defineProperty(Symbol, 'keyFor', keyForDescriptor);
      Object.defineProperty(Symbol.prototype, 'description', descriptionDescriptor);
    }
  };

  const shared = Symbol('primordial-shared');
  await expect(run(shared, shared)).resolves.toMatchObject({ valid: true });
  expect(keyForCalls).toBe(0);
  expect(descriptionCalls).toBe(0);

  const report = await run(Symbol.for('primordial-different'), Symbol('primordial-different'));
  expect(report.valid).toBe(false);
  expect(report.issues).toContain('CASE_PARITY_MISMATCH:lifecycle');
  expect(keyForCalls).toBe(0);
  expect(descriptionCalls).toBe(0);
});
