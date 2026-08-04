import { describe, expect, it } from 'vitest';
import { checkCompatibility } from '../src/preflight.js';

describe('OpenClaw compatibility preflight', () => {
  it.each([
    ['2026.5.5', '22.14.0'],
    ['2026.5.6', '22.14.0'],
    ['2026.5.7', '22.14.0'],
    ['2026.5.12', '22.16.0'],
    ['2026.5.18', '22.19.0'],
    ['2026.5.19', '22.19.0'],
    ['2026.5.20', '22.19.0'],
    ['2026.5.22', '22.19.0'],
    ['2026.5.26', '22.19.0'],
    ['2026.5.27', '22.19.0'],
    ['2026.5.28', '22.19.0'],
    ['2026.6.1', '22.19.0'],
    ['2026.6.5', '22.19.0'],
    ['2026.6.6', '22.19.0'],
    ['2026.6.8', '22.19.0'],
    ['2026.6.9', '22.19.0'],
    ['2026.6.10', '22.19.0'],
    ['2026.6.11', '22.19.0'],
    ['2026.6.33', '22.19.0'],
    ['2026.7.1', '22.22.3'],
    ['2026.7.1-1', '22.22.3'],
    ['2026.7.1-2', '22.22.3'],
  ])('accepts stable OpenClaw %s on Node %s', (hostVersion, nodeVersion) => {
    expect(checkCompatibility(hostVersion, nodeVersion).ok).toBe(true);
  });

  it('accepts the exact 2026.5.5 customer baseline on its declared Node floor', () => {
    expect(checkCompatibility('2026.5.5', '22.14.0')).toEqual({
      ok: true,
      qualification: 'exact_qualified',
      errors: [],
      warnings: [],
    });
  });

  it('uses the OpenClaw 2026.5.12 Node floor', () => {
    expect(checkCompatibility('2026.5.12', '22.15.0').ok).toBe(false);
    expect(checkCompatibility('2026.5.12', '22.16.0').ok).toBe(true);
  });

  it('uses the OpenClaw 2026.5.18 Node floor through 2026.6.33', () => {
    expect(checkCompatibility('2026.5.18', '22.18.0').ok).toBe(false);
    expect(checkCompatibility('2026.5.18', '22.19.0').ok).toBe(true);
    expect(checkCompatibility('2026.6.33', '22.18.0').ok).toBe(false);
    expect(checkCompatibility('2026.6.33', '22.19.0').ok).toBe(true);
  });

  it('accepts the qualified current build on its Node floors', () => {
    expect(checkCompatibility('2026.7.1-2', '24.15.0')).toEqual({
      ok: true,
      qualification: 'exact_qualified',
      errors: [],
      warnings: [],
    });
    expect(checkCompatibility('2026.7.1-2', '24.1.0')).toMatchObject({
      ok: false,
    });
    expect(checkCompatibility('2026.7.1-2', '22.22.2')).toMatchObject({
      ok: false,
    });
  });

  it('uses the current Node floors for every OpenClaw 2026.7.1 correction', () => {
    for (const hostVersion of ['2026.7.1', '2026.7.1-1', '2026.7.1-2']) {
      expect(checkCompatibility(hostVersion, '22.22.2').ok).toBe(false);
      expect(checkCompatibility(hostVersion, '22.22.3').ok).toBe(true);
      expect(checkCompatibility(hostVersion, '24.14.9').ok).toBe(false);
      expect(checkCompatibility(hostVersion, '24.15.0').ok).toBe(true);
    }
  });

  it('accepts the supported Node lines', () => {
    expect(checkCompatibility('2026.5.5', '24.0.0').ok).toBe(true);
    expect(checkCompatibility('2026.5.5', '23.11.0').ok).toBe(false);
    expect(checkCompatibility('2026.5.5', '25.9.0').ok).toBe(true);
    expect(checkCompatibility('2026.7.1-2', '25.8.0').ok).toBe(false);
    expect(checkCompatibility('2026.7.1-2', '25.9.0').ok).toBe(true);
    expect(checkCompatibility('2026.7.1-2', '26.0.0').ok).toBe(true);
  });

  it('rejects hosts below 2026.5.5 and flags unqualified versions in-range', () => {
    expect(checkCompatibility('2026.5.4', '22.22.3').ok).toBe(false);
    expect(checkCompatibility('2026.6.0', '22.22.3')).toMatchObject({
      ok: true,
      qualification: 'unknown',
      warnings: [expect.stringContaining('in-range but unqualified')],
    });
  });

  it('rejects named prereleases while accepting numeric correction releases', () => {
    expect(checkCompatibility('2026.7.2-beta.7', '22.22.3').ok).toBe(false);
    expect(checkCompatibility('2026.7.1-2', '22.22.3').ok).toBe(true);
  });
});
