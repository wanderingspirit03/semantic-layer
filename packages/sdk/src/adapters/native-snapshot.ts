import { NATIVE_SNAPSHOT_RESOURCE_LIMIT } from '../v1/error-evidence.js';

/**
 * Copies framework-owned evidence into values the capture artifact can persist losslessly.
 * Accessors are never invoked, errors retain their diagnostic fields, and unsupported runtime
 * values are represented structurally before they reach the artifact serializer.
 */
const INTRINSIC_ERROR_STACK_GETTER = Object.getOwnPropertyDescriptor(new Error(), 'stack')?.get;
const INTRINSIC_TYPED_ARRAY_BYTE_LENGTH_GETTER = Object.getOwnPropertyDescriptor(
  Object.getPrototypeOf(Uint8Array.prototype) as object,
  'byteLength',
)?.get;
const MAX_SNAPSHOT_NODES = 20_000;
const MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024;
const MAX_SNAPSHOT_BIGINT = 1n << 262_144n;
const RESOURCE_LIMIT = Object.freeze(markResourceLimit({
  $semantic_layer_omitted: 'resource_limit',
}));
const ERROR_DATA_KEYS = [
  'cause', 'code', 'status', 'statusCode', 'request_id', 'requestId', 'body', 'errors',
] as const;
type SnapshotBudget = { nodes: number; bytes: number; exhausted: boolean };

export function snapshotNative(value: unknown): unknown {
  return snapshot(
    value,
    new WeakSet<object>(),
    0,
    { nodes: 0, bytes: 0, exhausted: false },
  );
}

export function snapshotRecord(value: unknown): Record<string, unknown> {
  const copied = snapshotNative(value);
  return copied && typeof copied === 'object' && !Array.isArray(copied)
    ? copied as Record<string, unknown>
    : { value: copied };
}

function snapshot(
  value: unknown,
  seen: WeakSet<object>,
  depth: number,
  budget: SnapshotBudget,
): unknown {
  if (!retain(budget, 32)) return RESOURCE_LIMIT;
  if (value === null || typeof value === 'boolean') return value;
  if (typeof value === 'string') return snapshotString(value, budget);
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : { $semantic_layer_value: String(value) };
  }
  if (typeof value === 'bigint') {
    if (value >= MAX_SNAPSHOT_BIGINT || value <= -MAX_SNAPSHOT_BIGINT) {
      budget.exhausted = true;
      return RESOURCE_LIMIT;
    }
    const text = snapshotString(value.toString(), budget);
    return text === RESOURCE_LIMIT ? RESOURCE_LIMIT : { $semantic_layer_bigint: text };
  }
  if (value === undefined || typeof value === 'function' || typeof value === 'symbol') {
    return { $semantic_layer_omitted: typeof value };
  }
  if (typeof value !== 'object') return snapshotString(String(value), budget);
  if (isInstance(value, Error)) return snapshotError(value, seen, depth, budget);
  if (isInstance(value, Date)) {
    try {
      const milliseconds = Date.prototype.getTime.call(value);
      return Number.isFinite(milliseconds)
        ? snapshotString(new Date(milliseconds).toISOString(), budget)
        : { $semantic_layer_value: 'Invalid Date' };
    } catch {
      return { $semantic_layer_omitted: 'invalid_date' };
    }
  }
  if (isInstance(value, URL)) {
    try {
      return snapshotString(URL.prototype.toString.call(value), budget);
    } catch {
      return { $semantic_layer_omitted: 'invalid_url' };
    }
  }
  if (isInstance(value, Uint8Array)) {
    try {
      if (!INTRINSIC_TYPED_ARRAY_BYTE_LENGTH_GETTER) {
        return { $semantic_layer_omitted: 'invalid_bytes' };
      }
      const byteLength = Reflect.apply(INTRINSIC_TYPED_ARRAY_BYTE_LENGTH_GETTER, value, []) as number;
      if (!retain(budget, byteLength)) return RESOURCE_LIMIT;
      // Allocate the exact intrinsic type and copy without consulting iterator or species hooks.
      const copied = new Uint8Array(byteLength);
      Uint8Array.prototype.set.call(copied, value);
      return copied;
    } catch {
      return { $semantic_layer_omitted: 'invalid_bytes' };
    }
  }
  if (depth > 24) return { $semantic_layer_omitted: 'depth_limit' };
  if (seen.has(value)) return { $semantic_layer_cycle: true };
  seen.add(value);
  if (safeArray(value)) {
    try { return snapshotArray(value, seen, depth, budget); }
    finally { seen.delete(value); }
  }
  if (isInstance(value, Map)) {
    try {
      const entries: unknown[] = [];
      const iterator = Map.prototype.entries.call(value) as MapIterator<[unknown, unknown]>;
      for (const [key, child] of iterator) {
        if (budget.exhausted) break;
        entries.push([
          snapshot(key, seen, depth + 1, budget),
          snapshot(child, seen, depth + 1, budget),
        ]);
      }
      if (budget.exhausted) entries.push(RESOURCE_LIMIT);
      return { $semantic_layer_map: entries };
    } catch {
      return { $semantic_layer_omitted: 'invalid_map' };
    } finally {
      seen.delete(value);
    }
  }
  if (isInstance(value, Set)) {
    try {
      const values: unknown[] = [];
      const iterator = Set.prototype.values.call(value) as SetIterator<unknown>;
      for (const item of iterator) {
        if (budget.exhausted) break;
        values.push(snapshot(item, seen, depth + 1, budget));
      }
      if (budget.exhausted) values.push(RESOURCE_LIMIT);
      return { $semantic_layer_set: values };
    } catch {
      return { $semantic_layer_omitted: 'invalid_set' };
    } finally {
      seen.delete(value);
    }
  }

  const output: Record<string, unknown> = {};
  try {
    // JavaScript has no bounded own-key iterator. `for...in` is the only incremental
    // enumeration surface; budget before each descriptor lookup and keep only own data.
    for (const key in value) {
      if (!retain(budget, key.length * 2 + 16)) {
        output.$semantic_layer_omitted = 'resource_limit';
        markResourceLimit(output);
        break;
      }
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor) continue;
      if (!('value' in descriptor)) {
        output[key] = { $semantic_layer_omitted: 'accessor' };
        continue;
      }
      const child = snapshot(descriptor.value, seen, depth + 1, budget);
      if (child !== undefined) output[key] = child;
    }
  } catch {
    seen.delete(value);
    return { $semantic_layer_omitted: 'descriptors_unavailable' };
  }
  seen.delete(value);
  return output;
}

function snapshotArray(
  value: unknown[],
  seen: WeakSet<object>,
  depth: number,
  budget: SnapshotBudget,
): unknown[] | Record<string, unknown> {
  let lengthDescriptor: PropertyDescriptor | undefined;
  try {
    lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length');
  } catch {
    return { $semantic_layer_omitted: 'descriptors_unavailable' };
  }
  const length = lengthDescriptor && 'value' in lengthDescriptor
    && Number.isSafeInteger(lengthDescriptor.value) && lengthDescriptor.value >= 0
    ? lengthDescriptor.value as number : 0;
  const output: unknown[] = [];
  for (let index = 0; index < length; index += 1) {
    if (budget.exhausted || !retain(budget, 32)) {
      output.push(RESOURCE_LIMIT);
      break;
    }
    let descriptor: PropertyDescriptor | undefined;
    try { descriptor = Object.getOwnPropertyDescriptor(value, String(index)); } catch {
      output.push({ $semantic_layer_omitted: 'descriptors_unavailable' });
      continue;
    }
    if (!descriptor) {
      output.push({ $semantic_layer_omitted: 'array_hole' });
    } else if (!('value' in descriptor)) {
      output.push({ $semantic_layer_omitted: 'accessor' });
    } else {
      output.push(snapshot(descriptor.value, seen, depth + 1, budget));
    }
  }
  return output;
}

function snapshotError(
  error: Error,
  seen: WeakSet<object>,
  depth: number,
  budget: SnapshotBudget,
): Record<string, unknown> {
  const result: Record<string, unknown> = {
    name: snapshotString(safeErrorText(error, 'name', 'Error'), budget),
    message: snapshotString(safeErrorText(error, 'message', ''), budget),
    stack: snapshotString(safeErrorText(error, 'stack', ''), budget),
  };
  if (seen.has(error)) return { $semantic_layer_cycle: true };
  seen.add(error);
  const copied = new Set<string>();
  try {
    for (const key of ERROR_DATA_KEYS) {
      const descriptor = Object.getOwnPropertyDescriptor(error, key);
      if (!descriptor || !('value' in descriptor)) continue;
      if (!retain(budget, key.length * 2 + 16)) {
        result.$semantic_layer_omitted = 'resource_limit';
        markResourceLimit(result);
        break;
      }
      copied.add(key);
      const child = snapshot(descriptor.value, seen, depth + 1, budget);
      if (child !== undefined) result[key] = child;
    }
    if (!budget.exhausted) {
      for (const key in error) {
        if (copied.has(key) || ['name', 'message', 'stack'].includes(key)) continue;
        if (!retain(budget, key.length * 2 + 16)) {
          result.$semantic_layer_omitted = 'resource_limit';
          markResourceLimit(result);
          break;
        }
        const descriptor = Object.getOwnPropertyDescriptor(error, key);
        if (!descriptor || !('value' in descriptor)) continue;
        const child = snapshot(descriptor.value, seen, depth + 1, budget);
        if (child !== undefined) result[key] = child;
      }
    }
  } catch {
    result.$semantic_layer_omitted = 'descriptors_unavailable';
  }
  seen.delete(error);
  return result;
}

function safeErrorText(error: Error, key: 'name' | 'message' | 'stack', fallback: string): string {
  let current: object | null = error;
  while (current) {
    let descriptor: PropertyDescriptor | undefined;
    try { descriptor = Object.getOwnPropertyDescriptor(current, key); } catch { return fallback; }
    if (descriptor) {
      if ('value' in descriptor) return typeof descriptor.value === 'string' ? descriptor.value : fallback;
      if (key === 'stack' && descriptor.get === INTRINSIC_ERROR_STACK_GETTER && descriptor.get) {
        try {
          const stack = Reflect.apply(descriptor.get, error, []);
          return typeof stack === 'string' ? stack : fallback;
        } catch {
          return fallback;
        }
      }
      return fallback;
    }
    try { current = Object.getPrototypeOf(current) as object | null; } catch { return fallback; }
  }
  return fallback;
}

function isInstance<T extends abstract new (...args: never[]) => object>(
  value: object,
  constructor: T,
): value is InstanceType<T> {
  try { return value instanceof constructor; } catch { return false; }
}

function safeArray(value: object): value is unknown[] {
  try { return Array.isArray(value); } catch { return false; }
}

function retain(budget: SnapshotBudget, bytes: number): boolean {
  if (budget.exhausted) return false;
  if (
    budget.nodes >= MAX_SNAPSHOT_NODES
    || !Number.isSafeInteger(bytes)
    || bytes < 0
    || bytes > MAX_SNAPSHOT_BYTES - budget.bytes
  ) {
    budget.exhausted = true;
    return false;
  }
  budget.nodes += 1;
  budget.bytes += bytes;
  return true;
}

function snapshotString(value: string, budget: SnapshotBudget): string | typeof RESOURCE_LIMIT {
  return retain(budget, value.length * 2) ? value : RESOURCE_LIMIT;
}

function markResourceLimit<T extends object>(value: T): T {
  Object.defineProperty(value, NATIVE_SNAPSHOT_RESOURCE_LIMIT, {
    configurable: false,
    enumerable: false,
    value: true,
    writable: false,
  });
  return value;
}
