import { describe, expect, it } from 'vitest';

import { snapshotRecord } from '../src/adapters/native-snapshot.js';
import { safeSerialize } from '../src/v1/error-evidence.js';

describe('framework native snapshots', () => {
  it('preserves unknown own data and encodes unsupported values without artifact loss', () => {
    const native = Object.create({ prototypeMethod() { return 'interface behavior'; } }) as Record<string, unknown>;
    native.futureFrameworkField = { nested: 7 };
    native.missing = undefined;
    native.callback = () => 'not payload data';
    native.marker = Symbol('native-marker');

    const copied = snapshotRecord(native);
    expect(copied.futureFrameworkField).toEqual({ nested: 7 });
    expect(copied.missing).toEqual({ $semantic_layer_omitted: 'undefined' });
    expect(copied.callback).toEqual({ $semantic_layer_omitted: 'function' });
    expect(copied.marker).toEqual({ $semantic_layer_omitted: 'symbol' });
    expect(copied).not.toHaveProperty('prototypeMethod');
    expect(safeSerialize(copied).losses).toEqual([]);
  });

  it('never invokes hostile accessors and retains diagnostic error fields losslessly', () => {
    let reads = 0;
    const cause = new Error('root cause');
    const native = Object.create(null) as Record<string, unknown>;
    Object.defineProperty(native, 'hostile', {
      enumerable: true,
      get() {
        reads += 1;
        throw new Error('must not run');
      },
    });
    Object.defineProperty(native, 'failure', {
      enumerable: true,
      value: Object.assign(new Error('native failure', { cause }), { code: 'NATIVE_FAILURE' }),
    });
    const hostileError = new Error('hostile error');
    Object.defineProperty(hostileError, 'stack', {
      configurable: true,
      get() {
        reads += 1;
        throw new Error('hostile error stack must not run');
      },
    });
    native.hostileError = hostileError;

    const copied = snapshotRecord(native);
    expect(reads).toBe(0);
    expect(copied.hostile).toEqual({ $semantic_layer_omitted: 'accessor' });
    expect(copied.failure).toMatchObject({
      name: 'Error', message: 'native failure', code: 'NATIVE_FAILURE',
      cause: { name: 'Error', message: 'root cause' },
    });
    expect(copied.hostileError).toMatchObject({ message: 'hostile error', stack: '' });
    expect(safeSerialize(copied).losses).toEqual([]);
  });

  it('preserves the exact-runtime intrinsic Error stack without blessing custom accessors', () => {
    const cause = new Error('intrinsic cause');
    const error = Object.assign(new Error('intrinsic outer', { cause }), { code: 'E_INTRINSIC' });
    const serialized = safeSerialize({ error });
    expect(serialized.losses).toEqual([]);
    expect(serialized.value).toMatchObject({ error: {
      name: 'Error', message: 'intrinsic outer', code: 'E_INTRINSIC',
      stack: expect.stringContaining('Error: intrinsic outer'),
      cause: { message: 'intrinsic cause', stack: expect.stringContaining('Error: intrinsic cause') },
    } });
  });

  it('fails soft on revoked or descriptor-hostile framework objects', () => {
    const revoked = Proxy.revocable({ future: 'field' }, {});
    revoked.revoke();
    const hostile = new Proxy({}, {
      ownKeys() { throw new Error('descriptor trap'); },
    });

    expect(() => snapshotRecord(revoked.proxy)).not.toThrow();
    expect(snapshotRecord(hostile)).toEqual({ $semantic_layer_omitted: 'descriptors_unavailable' });
    expect(safeSerialize(snapshotRecord(hostile)).losses).toEqual([]);
  });

  it('preserves shared acyclic data while marking only true recursion cycles', () => {
    const shared = { toolCallId: 'shared-call', output: { ok: true } };
    const cycle: Record<string, unknown> = { name: 'cycle' };
    cycle.self = cycle;

    const copied = snapshotRecord({ input: shared, output: shared, cycle });
    expect(copied.input).toEqual(shared);
    expect(copied.output).toEqual(shared);
    expect(copied.cycle).toEqual({ name: 'cycle', self: { $semantic_layer_cycle: true } });
    expect(safeSerialize(copied).losses).toEqual([]);
  });

  it('stops wide framework containers before allocating an unbounded snapshot', () => {
    const wide = new Array(500_000);
    const copied = snapshotRecord({ wide });

    expect(Array.isArray(copied.wide)).toBe(true);
    expect((copied.wide as unknown[]).length).toBeLessThan(25_000);
    expect(copied.wide).toContainEqual({ $semantic_layer_omitted: 'resource_limit' });
  });

  it('does not copy byte buffers larger than the snapshot budget', () => {
    const copied = snapshotRecord({ bytes: new Uint8Array(9 * 1024 * 1024) });
    expect(copied.bytes).toEqual({ $semantic_layer_omitted: 'resource_limit' });
  });

  it('bounds strings and error diagnostics created or retained by snapshots', () => {
    const oversized = 'x'.repeat(9 * 1024 * 1024);
    const copiedText = snapshotRecord({ text: oversized });
    const copiedError = snapshotRecord({ error: new Error(oversized) });

    expect(copiedText.text).toEqual({ $semantic_layer_omitted: 'resource_limit' });
    expect(copiedError.error).toMatchObject({
      message: { $semantic_layer_omitted: 'resource_limit' },
    });
  });

  it('copies typed arrays without consulting custom iterators', () => {
    const bytes = new Uint8Array([1, 2, 3]);
    Object.defineProperty(bytes, Symbol.iterator, {
      value() { throw new Error('iterator must not run'); },
    });

    const copied = snapshotRecord({ bytes });
    expect(copied.bytes).toEqual(new Uint8Array([1, 2, 3]));
    const serialized = safeSerialize({ bytes });
    expect(serialized.losses).toEqual([]);
    expect(serialized.blobs[0]?.bytes).toEqual(new Uint8Array([1, 2, 3]));
  });

  it('copies typed arrays without consulting a hostile Symbol.species', () => {
    class HostileBytes extends Uint8Array {
      static get [Symbol.species](): Uint8ArrayConstructor {
        throw new Error('species must not run');
      }
    }
    const bytes = new HostileBytes([4, 5, 6]);

    const copied = snapshotRecord({ bytes });
    expect(copied.bytes).toEqual(new Uint8Array([4, 5, 6]));
    const serialized = safeSerialize({ bytes });
    expect(serialized.losses).toEqual([]);
    expect(serialized.blobs[0]?.bytes).toEqual(new Uint8Array([4, 5, 6]));
  });

  it('turns native snapshot resource markers into one explicit serialization loss', () => {
    const copied = snapshotRecord({
      first: new Array(500_000),
      second: new Array(500_000),
    });
    const serialized = safeSerialize(copied);

    expect(serialized.losses.filter((loss) => loss.nativeSnapshotResourceLimit))
      .toEqual([expect.objectContaining({ reason: 'serialization_failure' })]);
  });

  it('stops copying wide object properties at the snapshot budget', () => {
    const wide: Record<string, number> = {};
    for (let index = 0; index < 50_000; index += 1) wide[`field_${index}`] = index;

    const copied = snapshotRecord(wide);
    expect(Object.keys(copied).length).toBeLessThan(25_000);
    expect(copied.$semantic_layer_omitted).toBe('resource_limit');
  });
});
