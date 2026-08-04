import { createHash } from 'node:crypto';
import type { JsonValue, LossReason } from './types.js';

// V8 exposes Error.stack through this intrinsic accessor in exact Node 24.1.0.
// It is the one accessor the contract permits us to invoke: identity comparison
// prevents an application-defined getter from running during capture.
const INTRINSIC_ERROR_STACK_GETTER = Object.getOwnPropertyDescriptor(new Error(), 'stack')?.get;

// Serialization happens before queue admission, so it needs its own tighter bound. Two maximum-
// sized snapshots still leave half of the fixed 64 MiB queue for record metadata and encoding.
// The structural ceiling independently bounds maps, paths, and object bookkeeping when values are
// tiny. These limits are deliberately well above the observed ~450 KiB LangGraph/Zod tool schema.
const MAX_SERIALIZATION_RETAINED_BYTES = 8 * 1024 * 1024;
const MAX_SERIALIZATION_NODES = 250_000;
// JSON-pointer paths are transient traversal bookkeeping, not retained row bytes. Bound their
// cumulative construction independently so repeated deep prefixes cannot consume the evidence
// budget while traversal remains finite under its own path, node, and depth ceilings.
const MAX_SERIALIZATION_TRAVERSAL_PATH_BYTES = 64 * 1024 * 1024;
const RETAINED_NODE_BYTES = 32;
const RESOURCE_LIMIT_SENTINEL_BYTES = 64;
const RESOURCE_LIMIT_SENTINEL = Object.freeze({ $semantic_layer_omitted: 'resource_limit' });
const MAX_SERIALIZABLE_BIGINT = 1n << 262_144n;
const INTRINSIC_TYPED_ARRAY_BYTE_LENGTH_GETTER = Object.getOwnPropertyDescriptor(
  Object.getPrototypeOf(Uint8Array.prototype) as object,
  'byteLength',
)?.get;

export type CapturedBlob = { bytes: Uint8Array; digest: string; path: string; mimeType: string };
export const NATIVE_SNAPSHOT_RESOURCE_LIMIT = Symbol('semantic-layer.native-snapshot-resource-limit');
export type SerializationLoss = {
  reason: LossReason;
  path: string;
  nativeSnapshotResourceLimit?: true;
};
export type SerializationResult = {
  value: JsonValue;
  losses: SerializationLoss[];
  blobs: CapturedBlob[];
};

export function safeSerialize(value: unknown): SerializationResult {
  const losses: SerializationLoss[] = [];
  const blobs: CapturedBlob[] = [];
  const seen = new Map<object, string>();
  let nodes = 0;
  let retainedBytes = 0;
  let traversalPathBytes = 0;
  let resourceLimitReached = false;
  let nativeSnapshotResourceLimitRecorded = false;

  const resourceLimit = (path: string): JsonValue => {
    if (!resourceLimitReached) {
      resourceLimitReached = true;
      losses.push({ reason: 'serialization_failure', path });
    }
    return RESOURCE_LIMIT_SENTINEL;
  };

  const retain = (bytes: number, path: string): boolean => {
    if (resourceLimitReached) return false;
    // Keep one sentinel outside the working budget so the first truncated child can always carry
    // an inline explanation without taking the result beyond the hard retained-byte ceiling.
    const workingLimit = MAX_SERIALIZATION_RETAINED_BYTES - RESOURCE_LIMIT_SENTINEL_BYTES;
    if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > workingLimit - retainedBytes) {
      resourceLimit(path);
      return false;
    }
    retainedBytes += bytes;
    return true;
  };

  const traverse = (path: string): boolean => {
    if (resourceLimitReached) return false;
    const bytes = path.length * 2;
    if (!Number.isSafeInteger(bytes) || bytes < 0
      || bytes > MAX_SERIALIZATION_TRAVERSAL_PATH_BYTES - traversalPathBytes) return false;
    traversalPathBytes += bytes;
    return true;
  };

  const retainString = (input: string, path: string): JsonValue => {
    const remaining = MAX_SERIALIZATION_RETAINED_BYTES - RESOURCE_LIMIT_SENTINEL_BYTES - retainedBytes;
    const bytes = boundedJsonStringBytes(input, remaining);
    return retain(bytes, path) ? input : RESOURCE_LIMIT_SENTINEL;
  };

  const visit = (input: unknown, path: string, depth: number): JsonValue => {
    if (resourceLimitReached) return RESOURCE_LIMIT_SENTINEL;
    nodes += 1;
    if (nodes > MAX_SERIALIZATION_NODES) return resourceLimit(path);
    if (!traverse(path)) return resourceLimit(path);
    if (!retain(RETAINED_NODE_BYTES, path)) return RESOURCE_LIMIT_SENTINEL;
    if (depth > 48) {
      losses.push({ reason: 'serialization_failure', path });
      if (!retain(RESOURCE_LIMIT_SENTINEL_BYTES, path)) return RESOURCE_LIMIT_SENTINEL;
      return { $semantic_layer_omitted: 'resource_limit' };
    }
    if (input === null || typeof input === 'boolean') return input;
    if (typeof input === 'string') return retainString(input, path);
    if (typeof input === 'number') {
      if (Number.isFinite(input)) {
        return retain(String(input).length, path) ? input : RESOURCE_LIMIT_SENTINEL;
      }
      losses.push({ reason: 'unsupported_native_value', path });
      return retain(RESOURCE_LIMIT_SENTINEL_BYTES, path)
        ? { $semantic_layer_value: String(input) }
        : RESOURCE_LIMIT_SENTINEL;
    }
    if (typeof input === 'bigint') {
      // Avoid creating an unbounded decimal string before its byte cost can be measured.
      if (input >= MAX_SERIALIZABLE_BIGINT || input <= -MAX_SERIALIZABLE_BIGINT) {
        return resourceLimit(path);
      }
      const text = input.toString();
      return retain(boundedJsonStringBytes(text, MAX_SERIALIZATION_RETAINED_BYTES - retainedBytes), path)
        ? { $semantic_layer_bigint: text }
        : RESOURCE_LIMIT_SENTINEL;
    }
    if (input === undefined || typeof input === 'function' || typeof input === 'symbol') {
      losses.push({ reason: 'unsupported_native_value', path });
      return retain(RESOURCE_LIMIT_SENTINEL_BYTES, path)
        ? { $semantic_layer_omitted: typeof input }
        : RESOURCE_LIMIT_SENTINEL;
    }
    if (typeof input !== 'object') return retainString(String(input), path);
    if (!nativeSnapshotResourceLimitRecorded
      && hasNativeSnapshotResourceLimit(input)) {
      nativeSnapshotResourceLimitRecorded = true;
      losses.push({
        reason: 'serialization_failure',
        path,
        nativeSnapshotResourceLimit: true,
      });
    }
    if (seen.has(input)) {
      losses.push({ reason: 'unsupported_native_value', path });
      const reference = seen.get(input) ?? '';
      return retain(boundedJsonStringBytes(reference, MAX_SERIALIZATION_RETAINED_BYTES - retainedBytes), path)
        ? { $semantic_layer_ref: reference }
        : RESOURCE_LIMIT_SENTINEL;
    }
    // `seen` is the active recursion stack, not a global object-identity cache. JSON can
    // faithfully duplicate a shared acyclic value at each path; only a reference to an
    // active ancestor is a cycle that needs an explicit ref and loss record.
    seen.set(input, path);
    try {
    if (isInstance(input, Date, path, losses)) {
      try {
        const milliseconds = Date.prototype.getTime.call(input);
        if (Number.isFinite(milliseconds)) return retainString(new Date(milliseconds).toISOString(), path);
        losses.push({ reason: 'unsupported_native_value', path });
        return retain(RESOURCE_LIMIT_SENTINEL_BYTES, path)
          ? { $semantic_layer_value: 'Invalid Date' }
          : RESOURCE_LIMIT_SENTINEL;
      } catch {
        losses.push({ reason: 'serialization_failure', path });
        return retain(RESOURCE_LIMIT_SENTINEL_BYTES, path)
          ? { $semantic_layer_omitted: 'hostile_date' }
          : RESOURCE_LIMIT_SENTINEL;
      }
    }
    if (isInstance(input, URL, path, losses)) {
      try { return retainString(URL.prototype.toString.call(input), path); } catch {
        losses.push({ reason: 'serialization_failure', path });
        return retain(RESOURCE_LIMIT_SENTINEL_BYTES, path)
          ? { $semantic_layer_omitted: 'hostile_url' }
          : RESOURCE_LIMIT_SENTINEL;
      }
    }
    if (isInstance(input, Uint8Array, path, losses)) {
      try {
        if (!INTRINSIC_TYPED_ARRAY_BYTE_LENGTH_GETTER) {
          throw new TypeError('typed array byteLength intrinsic unavailable');
        }
        const byteLength = Reflect.apply(
          INTRINSIC_TYPED_ARRAY_BYTE_LENGTH_GETTER,
          input,
          [],
        ) as number;
        if (!retain(byteLength + 256, path)) return RESOURCE_LIMIT_SENTINEL;
        // Allocate the exact intrinsic type and copy without consulting iterator or species hooks.
        const bytes = new Uint8Array(byteLength);
        Uint8Array.prototype.set.call(bytes, input);
        const digest = createHash('sha256').update(bytes).digest('hex');
        blobs.push({ bytes, digest, path, mimeType: 'application/octet-stream' });
        return { $semantic_layer_binary: { byte_length: bytes.byteLength, digest, inline_omitted: true } };
      } catch {
        losses.push({ reason: 'serialization_failure', path });
        return { $semantic_layer_omitted: 'hostile_bytes' };
      }
    }
    if (isInstance(input, Error, path, losses)) {
      return serializeError(
        input, path, depth, visit, losses, retain, () => resourceLimitReached,
      );
    }
    if (Array.isArray(input)) {
      const values: JsonValue[] = [];
      for (let index = 0; index < input.length && !resourceLimitReached; index += 1) {
        const childPath = `${path}/${index}`;
        if (!retain(1, childPath)) break;
        let descriptor: PropertyDescriptor | undefined;
        try { descriptor = Object.getOwnPropertyDescriptor(input, String(index)); } catch {
          losses.push({ reason: 'serialization_failure', path });
          return { $semantic_layer_omitted: 'descriptors_unavailable' };
        }
        if (!descriptor) {
          nodes += 1;
          if (nodes > MAX_SERIALIZATION_NODES || !traverse(childPath)
            || !retain(RETAINED_NODE_BYTES, childPath)) {
            resourceLimit(childPath);
            break;
          }
          values.length += 1;
        } else if (!('value' in descriptor)) {
          nodes += 1;
          if (nodes > MAX_SERIALIZATION_NODES || !traverse(childPath)
            || !retain(RETAINED_NODE_BYTES + RESOURCE_LIMIT_SENTINEL_BYTES, childPath)) {
            resourceLimit(childPath);
            values.push(RESOURCE_LIMIT_SENTINEL);
            break;
          }
          losses.push({ reason: 'unsafe_getter_avoided', path: childPath });
          values.push({ $semantic_layer_omitted: 'accessor' });
        } else {
          values.push(visit(descriptor.value, childPath, depth + 1));
        }
      }
      return values;
    }
    if (isInstance(input, Map, path, losses)) {
      const entries: JsonValue[] = [];
      try {
        const iterator = Map.prototype.entries.call(input) as MapIterator<[unknown, unknown]>;
        for (const [mapKey, mapValue] of iterator) {
          if (resourceLimitReached || !retain(3, path)) break;
          const index = entries.length;
          entries.push([
            visit(mapKey, `${path}/$semantic_layer_map/${index}/0`, depth + 1),
            visit(mapValue, `${path}/$semantic_layer_map/${index}/1`, depth + 1),
          ]);
        }
        return { $semantic_layer_map: entries };
      } catch {
        losses.push({ reason: 'serialization_failure', path });
        return { $semantic_layer_omitted: 'hostile_map' };
      }
    }
    if (isInstance(input, Set, path, losses)) {
      const values: JsonValue[] = [];
      try {
        const iterator = Set.prototype.values.call(input) as SetIterator<unknown>;
        for (const setValue of iterator) {
          if (resourceLimitReached || !retain(1, path)) break;
          values.push(visit(setValue, `${path}/$semantic_layer_set/${values.length}`, depth + 1));
        }
        return { $semantic_layer_set: values };
      } catch {
        losses.push({ reason: 'serialization_failure', path });
        return { $semantic_layer_omitted: 'hostile_set' };
      }
    }

    const result: Record<string, JsonValue> = {};
    let keys: string[];
    try {
      // Fetch only the key list up front. A descriptor map would fully duplicate every property
      // before the traversal budget had a chance to stop an unusually wide object.
      keys = Object.getOwnPropertyNames(input);
    } catch {
      losses.push({ reason: 'serialization_failure', path });
      return { $semantic_layer_omitted: 'descriptors_unavailable' };
    }
    for (const key of keys) {
      if (resourceLimitReached) break;
      const keyBytes = boundedJsonStringBytes(key, MAX_SERIALIZATION_RETAINED_BYTES - retainedBytes);
      if (!retain(keyBytes + 8, path)) break;
      const childPath = `${path}/${escapePointer(key)}`;
      let descriptor: PropertyDescriptor | undefined;
      try { descriptor = Object.getOwnPropertyDescriptor(input, key); } catch {
        losses.push({ reason: 'serialization_failure', path: childPath });
        return { $semantic_layer_omitted: 'descriptors_unavailable' };
      }
      if (!descriptor) continue;
      if (!('value' in descriptor)) {
        losses.push({ reason: 'unsafe_getter_avoided', path: childPath });
        result[key] = { $semantic_layer_omitted: 'accessor' };
        continue;
      }
      if (['toJSON', 'export', 'to_dict', 'model_dump', 'finalResponse', 'finalMessage'].includes(key) && typeof descriptor.value === 'function') {
        losses.push({ reason: 'unsafe_helper_avoided', path: childPath });
        continue;
      }
      result[key] = visit(descriptor.value, childPath, depth + 1);
    }
    return result;
    } finally {
      seen.delete(input);
    }
  };

  return { value: visit(value, '', 0), losses, blobs };
}

function hasNativeSnapshotResourceLimit(value: object): boolean {
  try {
    return Object.getOwnPropertyDescriptor(value, NATIVE_SNAPSHOT_RESOURCE_LIMIT)?.value === true;
  } catch {
    return false;
  }
}

function serializeError(
  error: Error,
  path: string,
  depth: number,
  visit: (value: unknown, path: string, depth: number) => JsonValue,
  losses: SerializationLoss[],
  retain: (bytes: number, path: string) => boolean,
  resourceLimitReached: () => boolean,
): JsonValue {
  const name = safeErrorText(error, 'name', 'Error', path, losses);
  const message = safeErrorText(error, 'message', '', path, losses);
  const stack = safeErrorText(error, 'stack', '', path, losses);
  if (!retain(
    boundedJsonStringBytes(name, MAX_SERIALIZATION_RETAINED_BYTES)
      + boundedJsonStringBytes(message, MAX_SERIALIZATION_RETAINED_BYTES)
      + boundedJsonStringBytes(stack, MAX_SERIALIZATION_RETAINED_BYTES)
      + 32,
    path,
  )) return RESOURCE_LIMIT_SENTINEL;
  const result: Record<string, JsonValue> = {
    name, message, stack,
  };
  let keys: string[];
  try { keys = Object.getOwnPropertyNames(error); } catch {
    losses.push({ reason: 'serialization_failure', path });
    return result;
  }
  for (const key of keys) {
    if (resourceLimitReached()) break;
    if (['name', 'message', 'stack'].includes(key) || (key === 'errors' && error instanceof AggregateError)) continue;
    const childPath = `${path}/${escapePointer(key)}`;
    if (!retain(boundedJsonStringBytes(key, MAX_SERIALIZATION_RETAINED_BYTES) + 8, childPath)) break;
    let descriptor: PropertyDescriptor | undefined;
    try { descriptor = Object.getOwnPropertyDescriptor(error, key); } catch {
      losses.push({ reason: 'serialization_failure', path: childPath });
      return result;
    }
    if (!descriptor) continue;
    if (!('value' in descriptor)) {
      losses.push({ reason: 'unsafe_getter_avoided', path: childPath });
      result[key] = { $semantic_layer_omitted: 'accessor' };
    } else if (descriptor.value === undefined) {
      // Optional Error fields such as APIError.param commonly use undefined to
      // mean absent. Omitting them is lossless and avoids a false capture gap.
      continue;
    } else {
      result[key] = visit(descriptor.value, childPath, depth + 1);
    }
  }
  if (error instanceof AggregateError) {
    try {
      const own = Object.getOwnPropertyDescriptor(error, 'errors');
      if (own && 'value' in own && Array.isArray(own.value)) {
        result.errors = visit(own.value, `${path}/errors`, depth + 1);
      }
    } catch {
      losses.push({ reason: 'serialization_failure', path: `${path}/errors` });
    }
  }
  return result;
}

function boundedJsonStringBytes(value: string, ceiling: number): number {
  let bytes = 2; // surrounding JSON quotes
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code === 0x22 || code === 0x5c) bytes += 2;
    else if (code <= 0x1f) bytes += 6;
    else if (code <= 0x7f) bytes += 1;
    else if (code <= 0x7ff) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next >= 0xdc00 && next <= 0xdfff) {
        bytes += 4;
        index += 1;
      } else {
        bytes += 6;
      }
    } else if (code >= 0xdc00 && code <= 0xdfff) bytes += 6;
    else bytes += 3;
    if (bytes > ceiling) return ceiling + 1;
  }
  return bytes;
}

function safeErrorText(
  error: Error,
  key: 'name' | 'message' | 'stack',
  fallback: string,
  path: string,
  losses: SerializationLoss[],
): string {
  let current: object | null = error;
  while (current) {
    const descriptor = Object.getOwnPropertyDescriptor(current, key);
    if (descriptor) {
      if ('value' in descriptor) return typeof descriptor.value === 'string' ? descriptor.value : fallback;
      if (key === 'stack' && descriptor.get === INTRINSIC_ERROR_STACK_GETTER && descriptor.get) {
        try {
          const stack = Reflect.apply(descriptor.get, error, []);
          return typeof stack === 'string' ? stack : fallback;
        } catch {
          losses.push({ reason: 'serialization_failure', path: `${path}/${key}` });
          return fallback;
        }
      }
      losses.push({ reason: 'unsafe_getter_avoided', path: `${path}/${key}` });
      return fallback;
    }
    try { current = Object.getPrototypeOf(current) as object | null; } catch {
      losses.push({ reason: 'serialization_failure', path: `${path}/${key}` });
      return fallback;
    }
  }
  return fallback;
}

function escapePointer(value: string): string {
  return value.replaceAll('~', '~0').replaceAll('/', '~1');
}

function isInstance<T extends abstract new (...args: any[]) => any>(
  value: object,
  constructor: T,
  path: string,
  losses: SerializationLoss[],
): value is InstanceType<T> {
  try { return value instanceof constructor; } catch {
    losses.push({ reason: 'serialization_failure', path });
    return false;
  }
}
