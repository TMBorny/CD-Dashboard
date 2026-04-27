import { describe, expect, it } from 'vitest';
import { getClientHealthScore, getClientHealthStatusLabel, getMergeSuccessRate } from './clientHealth';

describe('client health helpers', () => {
  it('calculates success rates with finished-with-issues weighted at half credit', () => {
    expect(getMergeSuccessRate({
      total: 10,
      succeeded: 6,
      finishedWithIssues: 2,
      noData: 2,
    })).toBe(87.5);
  });

  it('returns null when there is no valid merge denominator', () => {
    expect(getMergeSuccessRate({
      total: 3,
      succeeded: 0,
      finishedWithIssues: 0,
      noData: 3,
    })).toBeNull();
  });

  it('calculates the same composite health score used in client health views', () => {
    expect(getClientHealthScore({
      merges: {
        nightly: { total: 10, succeeded: 8, failed: 0, finishedWithIssues: 1, noData: 1, halted: 0, mergeTimeMs: 0 },
        realtime: { total: 4, succeeded: 2, failed: 1, finishedWithIssues: 1, noData: 0 },
        manual: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
      },
      mergeErrorsCount: 3,
      activeUsers24h: 12,
    })).toBeCloseTo(72.4722222222, 6);
  });

  it('maps health scores into the expected status bands', () => {
    expect(getClientHealthStatusLabel(92)).toBe('Healthy');
    expect(getClientHealthStatusLabel(70)).toBe('Warning');
    expect(getClientHealthStatusLabel(50)).toBe('At Risk');
    expect(getClientHealthStatusLabel(null)).toBe('Unknown');
  });
});
