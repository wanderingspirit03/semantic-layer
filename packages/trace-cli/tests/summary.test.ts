import { describe, expect, test } from 'vitest';
import { contentRecords, summarize } from '../src/summary.js';

describe('trace summary recovery evidence', () => {
  test('counts retry state records and includes state evidence in a private view', () => {
    const records = [
      { kind: 'state', data: { type: 'model.retry', value: { attempt: 2 } } },
      { kind: 'state', data: { type: 'recovery.retry', value: { attempt: 1 } } },
      { kind: 'state', data: { type: 'agent.interrupt' } },
    ];
    const summary = summarize(
      { tenant: 'tenant_test', installation: 'install_0123456789abcdef0123456789abcdef', bundle: 'bundle_test' },
      { sources: [], trace: { losses: 0 } },
      records,
      { valid: true, profile: 'structural', issues: [], rows: 3, secretMatches: 0, sourceActivity: [] },
    );
    expect(summary.recoveries).toBe(2);
    expect(summary.tokenUsageObserved).toBe(false);
    expect(summary.lossReasons).toEqual([]);
    expect(contentRecords(records)).toEqual(records);
  });
});
