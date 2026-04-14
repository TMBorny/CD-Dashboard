import { formatLocalDateTime } from '@/utils/dateTime';

export type JobRun = {
  jobId: string;
  school?: string | null;
  scope: string;
  status: string;
  snapshotDate?: string | null;
  schoolsProcessed?: number;
  totalSchools?: number;
  attemptedAt?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  startDate?: string | null;
  endDate?: string | null;
  dateCount?: number | null;
  lastHeartbeatAt?: string | null;
  lastProgressAt?: string | null;
  currentSchool?: string | null;
  currentSnapshotDate?: string | null;
  completedUnits?: number;
  failedUnits?: number;
  skippedUnits?: number;
  statusDetail?: string | null;
  failureReason?: string | null;
  failedUnitsSample?: Array<{
    school?: string;
    snapshotDate?: string;
    attemptCount?: number;
    error?: string;
  }>;
  checkpointState?: {
    currentSchool?: string | null;
    currentSnapshotDate?: string | null;
    remainingUnits?: number;
  } | null;
  stallThresholdSeconds?: number;
  errors?: string[];
  errorCount?: number;
  timing?: {
    totalSec?: number;
    listSchoolsSec?: number;
    fetchHealthSec?: number;
    persistDbSec?: number;
  } | null;
  errorMessage?: string | null;
};

export type SyncRunsResponse = {
  syncRuns: JobRun[];
  totalCount: number;
  limit: number;
  offset: number;
};

export const getJobStatusTone = (status: string) => {
  switch (status.toLowerCase()) {
    case 'completed':
      return 'emerald' as const;
    case 'completed_with_failures':
      return 'amber' as const;
    case 'running':
      return 'slate' as const;
    case 'stalled':
      return 'amber' as const;
    case 'failed':
      return 'rose' as const;
    default:
      return 'amber' as const;
  }
};

export const formatJobScope = (scope: string) => scope.replaceAll('-', ' ');

export const formatJobRuntime = (run: JobRun) => {
  if (run.timing?.totalSec != null) return `${run.timing.totalSec.toFixed(1)}s`;
  if (!run.finishedAt || !run.startedAt) return null;
  const runtimeMs = new Date(run.finishedAt).getTime() - new Date(run.startedAt).getTime();
  return `${(runtimeMs / 1000).toFixed(1)}s`;
};

export const formatJobProgress = (run: JobRun) => {
  if (!run.totalSchools) return null;
  return `${run.schoolsProcessed ?? 0} / ${run.totalSchools}`;
};

export const formatBackfillRange = (run: JobRun) => {
  if (!run.startDate) return null;
  const end = run.endDate ?? run.startDate;
  const span = run.dateCount ? ` (${run.dateCount} day${run.dateCount === 1 ? '' : 's'})` : '';
  return `${run.startDate} to ${end}${span}`;
};

export const formatDateTime = (value?: string | null) => formatLocalDateTime(value, 'Unknown');

export const formatRelativeAge = (value?: string | null) => {
  if (!value) return 'Unknown';
  const ageSeconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1000));
  if (ageSeconds < 60) return `${ageSeconds}s ago`;
  if (ageSeconds < 3600) return `${Math.round(ageSeconds / 60)}m ago`;
  return `${Math.round(ageSeconds / 3600)}h ago`;
};

export const getJobCompletionRatio = (run: JobRun) => {
  if (!run.totalSchools || run.totalSchools <= 0) return null;
  return Math.min(1, Math.max(0, (run.schoolsProcessed ?? 0) / run.totalSchools));
};

export const formatJobCompletionPercent = (run: JobRun) => {
  const ratio = getJobCompletionRatio(run);
  return ratio == null ? null : `${(ratio * 100).toFixed(1)}%`;
};

export const getElapsedSeconds = (run: JobRun) => {
  if (run.timing?.totalSec != null) return run.timing.totalSec;
  if (!run.startedAt) return null;
  const startedMs = new Date(run.startedAt).getTime();
  const endMs = run.finishedAt ? new Date(run.finishedAt).getTime() : Date.now();
  if (Number.isNaN(startedMs) || Number.isNaN(endMs) || endMs <= startedMs) return null;
  return (endMs - startedMs) / 1000;
};

export const getJobThroughputPerMinute = (run: JobRun) => {
  const elapsedSeconds = getElapsedSeconds(run);
  const completedUnits = run.completedUnits ?? run.schoolsProcessed ?? 0;
  if (!elapsedSeconds || elapsedSeconds <= 0 || completedUnits <= 0) return null;
  return (completedUnits / elapsedSeconds) * 60;
};

export const getRemainingUnits = (run: JobRun) => {
  if (run.checkpointState?.remainingUnits != null) return run.checkpointState.remainingUnits;
  if (!run.totalSchools) return null;
  return Math.max(0, run.totalSchools - (run.schoolsProcessed ?? 0));
};

export const formatDuration = (seconds?: number | null) => {
  if (seconds == null || !Number.isFinite(seconds) || seconds < 0) return null;
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return remainingMinutes ? `${hours}h ${remainingMinutes}m` : `${hours}h`;
};

export const getEstimatedSecondsRemaining = (run: JobRun) => {
  const throughput = getJobThroughputPerMinute(run);
  const remainingUnits = getRemainingUnits(run);
  if (!throughput || throughput <= 0 || remainingUnits == null || remainingUnits <= 0) return null;
  return (remainingUnits / throughput) * 60;
};

export const formatEta = (run: JobRun) => {
  const secondsRemaining = getEstimatedSecondsRemaining(run);
  if (secondsRemaining == null) return null;
  return formatDuration(secondsRemaining);
};

export const canResumeJob = (run: JobRun) =>
  run.scope.includes('backfill') && ['stalled', 'failed'].includes(run.status);

export const canRetryFailuresForJob = (run: JobRun) =>
  run.scope.includes('backfill') && run.status === 'completed_with_failures' && (run.failedUnits ?? 0) > 0;
