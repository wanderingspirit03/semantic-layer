import { expect, it } from 'vitest';
import { parseKeyRegistry } from '../src/registry.js';

const hash = 'a'.repeat(64);
const managed = {
  tenant_id: 'tenant_a1',
  installation_id: 'install_AAAAAAAAAAAAAAAAAAAAAA',
  status: 'active',
};

it('strictly parses managed and explicit legacy registry entries', () => {
  expect(parseKeyRegistry(JSON.stringify({ [hash]: managed }))).toEqual({ [hash]: managed });
  expect(parseKeyRegistry(JSON.stringify({ [hash]: 'tenant_a1' }))).toEqual({ [hash]: 'tenant_a1' });
});

it('rejects malformed hashes, fields, installations, and status values', () => {
  expect(() => parseKeyRegistry(JSON.stringify({ plaintext: managed }))).toThrow(/lowercase SHA-256/u);
  expect(() => parseKeyRegistry(JSON.stringify({ [hash]: { ...managed, unexpected: true } }))).toThrow(/entry is invalid/u);
  expect(() => parseKeyRegistry(JSON.stringify({ [hash]: { ...managed, installation_id: 'install_short' } }))).toThrow(/entry is invalid/u);
  expect(() => parseKeyRegistry(JSON.stringify({ [hash]: { ...managed, status: 'paused' } }))).toThrow(/entry is invalid/u);
});
