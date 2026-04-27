import type { ClientHealthSnapshot } from '@/types/clientHealth';

type MergeBucketLike = Pick<
  ClientHealthSnapshot['merges']['nightly'],
  'total' | 'succeeded' | 'finishedWithIssues' | 'noData'
>;

export type ClientHealthStatus = 'Healthy' | 'Warning' | 'At Risk';

export function getMergeSuccessRate(bucket: MergeBucketLike | null | undefined): number | null {
  if (!bucket) {
    return null;
  }

  const validTotal = bucket.total - bucket.noData;
  if (validTotal <= 0) {
    return null;
  }

  return ((bucket.succeeded + (bucket.finishedWithIssues * 0.5)) / validTotal) * 100;
}

export function getClientHealthScore(
  snapshot: Pick<ClientHealthSnapshot, 'merges' | 'mergeErrorsCount' | 'activeUsers24h'>,
): number {
  const rates = [
    getMergeSuccessRate(snapshot.merges.nightly),
    getMergeSuccessRate(snapshot.merges.realtime),
  ].filter((value): value is number => value !== null);

  const baseSuccess = rates.length > 0
    ? rates.reduce((sum, value) => sum + value, 0) / rates.length
    : 0;

  const openErrors = snapshot.mergeErrorsCount ?? 0;
  const errorPenalty = openErrors > 0 ? Math.min(20, Math.log2(openErrors + 1) * 4) : 0;
  const activeUsers = snapshot.activeUsers24h;
  const activityAdjustment = activeUsers >= 20 ? 4 : activeUsers >= 5 ? 2 : activeUsers >= 1 ? 0 : -3;

  return Math.max(0, Math.min(100, baseSuccess - errorPenalty + activityAdjustment));
}

export function getClientHealthStatusLabel(score: number | null | undefined): ClientHealthStatus | 'Unknown' {
  if (score == null) {
    return 'Unknown';
  }

  if (score >= 85) {
    return 'Healthy';
  }

  if (score >= 65) {
    return 'Warning';
  }

  return 'At Risk';
}
