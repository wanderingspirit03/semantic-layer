import type { KeyRegistryEntry } from './http.js';
import { validId, validInstallationId } from './protocol.js';

export function parseKeyRegistry(serialized: string): Record<string, KeyRegistryEntry> {
  const value = JSON.parse(serialized) as unknown;
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('SEMANTIC_LAYER_KEY_REGISTRY_JSON must be an object');
  const entries = Object.entries(value as Record<string, unknown>);
  const parsed: Array<[string, KeyRegistryEntry]> = entries.map(([hash, entry]) => {
    if (!/^[0-9a-f]{64}$/u.test(hash)) throw new Error('key registry keys must be lowercase SHA-256 hashes');
    if (typeof entry === 'string') {
      if (!validId(entry)) throw new Error('legacy key registry tenant ID is invalid');
      return [hash, entry];
    }
    if (!entry || typeof entry !== 'object' || Array.isArray(entry)) throw new Error('key registry entry is invalid');
    const candidate = entry as Record<string, unknown>;
    if (Object.keys(candidate).sort().join(',') !== 'installation_id,status,tenant_id'
      || !validId(candidate.tenant_id)
      || !validInstallationId(candidate.installation_id)
      || (candidate.status !== 'active' && candidate.status !== 'revoked')) {
      throw new Error('managed key registry entry is invalid');
    }
    return [hash, {
      tenant_id: candidate.tenant_id,
      installation_id: candidate.installation_id,
      status: candidate.status,
    }];
  });
  return Object.fromEntries(parsed);
}
