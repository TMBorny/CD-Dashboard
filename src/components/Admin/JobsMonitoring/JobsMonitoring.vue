<script setup lang="ts">
import { computed } from 'vue';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { getSyncRuns, resumeHistoryBackfill, retryHistoryBackfillFailures } from '@/api';
import Badge from '@/components/ui/Badge.vue';

type SyncRun = {
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
  failedUnitsSample?: Array<{ school?: string; snapshotDate?: string; attemptCount?: number; error?: string }>;
  checkpointState?: {
    currentSchool?: string | null;
    currentSnapshotDate?: string | null;
    remainingUnits?: number;
  } | null;
  stallThresholdSeconds?: number;
  errors?: string[];
  errorCount?: number;
  timing?: { totalSec?: number } | null;
  errorMessage?: string | null;
};

const queryClient = useQueryClient();

const { data, isLoading, error } = useQuery({
  queryKey: ['syncRuns'],
  queryFn: () => getSyncRuns({ limit: 50 }).then((res) => res.data),
  refetchInterval: 5000,
});

const resumeMutation = useMutation({
  mutationFn: (jobId: string) => resumeHistoryBackfill(jobId),
  onSuccess: async () => {
    await queryClient.invalidateQueries({ queryKey: ['syncRuns'] });
  },
});

const retryFailuresMutation = useMutation({
  mutationFn: (jobId: string) => retryHistoryBackfillFailures(jobId),
  onSuccess: async () => {
    await queryClient.invalidateQueries({ queryKey: ['syncRuns'] });
  },
});

const mutationError = computed(() => {
  const resumeError = resumeMutation.error as { response?: { data?: { detail?: string } } ; message?: string } | null;
  const retryError = retryFailuresMutation.error as { response?: { data?: { detail?: string } } ; message?: string } | null;
  return resumeError?.response?.data?.detail || retryError?.response?.data?.detail || resumeError?.message || retryError?.message || null;
});

const getStatusTone = (status: string) => {
  switch (status.toLowerCase()) {
    case 'completed': return 'emerald';
    case 'completed_with_failures': return 'amber';
    case 'running': return 'slate';
    case 'stalled': return 'amber';
    case 'failed': return 'rose';
    default: return 'amber';
  }
};

const formatScope = (scope: string) => scope.replaceAll('-', ' ');

const formatRuntime = (run: SyncRun) => {
  if (run.timing?.totalSec != null) return `${run.timing.totalSec.toFixed(1)}s`;
  if (!run.finishedAt || !run.startedAt) return null;
  const runtimeMs = new Date(run.finishedAt).getTime() - new Date(run.startedAt).getTime();
  return `${(runtimeMs / 1000).toFixed(1)}s`;
};

const formatProgress = (run: SyncRun) => {
  if (!run.totalSchools) return null;
  return `${run.schoolsProcessed ?? 0} / ${run.totalSchools}`;
};

const formatBackfillRange = (run: SyncRun) => {
  if (!run.startDate) return null;
  const end = run.endDate ?? run.startDate;
  const span = run.dateCount ? ` (${run.dateCount} day${run.dateCount === 1 ? '' : 's'})` : '';
  return `${run.startDate} to ${end}${span}`;
};

const formatDateTime = (value?: string | null) => value ? new Date(value).toLocaleString() : 'Unknown';
const formatRelativeAge = (value?: string | null) => {
  if (!value) return 'Unknown';
  const ageSeconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1000));
  if (ageSeconds < 60) return `${ageSeconds}s ago`;
  if (ageSeconds < 3600) return `${Math.round(ageSeconds / 60)}m ago`;
  return `${Math.round(ageSeconds / 3600)}h ago`;
};
const canResume = (run: SyncRun) => run.scope.includes('backfill') && ['stalled', 'failed'].includes(run.status);
const canRetryFailures = (run: SyncRun) => run.scope.includes('backfill') && run.status === 'completed_with_failures' && (run.failedUnits ?? 0) > 0;
const activeResumeJobId = computed(() => resumeMutation.variables as unknown as string | undefined);
const activeRetryJobId = computed(() => retryFailuresMutation.variables as unknown as string | undefined);
const runActionPending = (run: SyncRun) =>
  (resumeMutation.isPending && activeResumeJobId.value === run.jobId)
  || (retryFailuresMutation.isPending && activeRetryJobId.value === run.jobId);
const triggerResume = async (run: SyncRun) => {
  await resumeMutation.mutateAsync(run.jobId);
};
const triggerRetryFailures = async (run: SyncRun) => {
  await retryFailuresMutation.mutateAsync(run.jobId);
};

const descriptorClass = 'group relative inline-flex items-center gap-2';
const descriptorButtonClass = 'flex h-5 w-5 items-center justify-center rounded-full border border-slate-300 bg-white text-[11px] font-semibold text-slate-500 transition hover:border-slate-400 hover:text-slate-700';
const descriptorPopoverClass = 'pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-64 -translate-x-1/2 rounded-2xl border border-slate-200 bg-white p-3 text-[11px] leading-5 text-slate-600 shadow-lg group-hover:block';
</script>

<template>
  <div class="px-8 py-8 w-full max-w-7xl mx-auto">
    <div class="mb-8 flex items-center justify-between">
      <div>
        <h1 class="text-2xl font-semibold tracking-tight text-slate-950">Background Jobs</h1>
        <p class="mt-1 text-sm text-slate-500">Monitor bulk school syncs and historical backfill queue status.</p>
      </div>
    </div>

    <div v-if="isLoading" class="text-sm text-slate-500">Loading jobs...</div>
    <div v-else-if="error" class="text-sm text-rose-500">Failed to load job history.</div>
    <div v-if="mutationError" class="mb-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
      {{ mutationError }}
    </div>
    
    <div v-else class="rounded-[24px] border border-slate-200 bg-white p-3 shadow-sm">
      <table class="w-full border-separate border-spacing-y-3 text-left text-sm">
        <thead class="text-xs uppercase tracking-[0.1em] text-slate-500">
          <tr>
            <th class="px-6 py-4 font-semibold text-slate-600">
              <div :class="descriptorClass">
                <span>ID / Date</span>
                <button class="cursor-help" :class="descriptorButtonClass" aria-label="Job identity and creation time">?</button>
                <div :class="descriptorPopoverClass">
                  Shows the background job id plus the time the run was first recorded by the backend.
                </div>
              </div>
            </th>
            <th class="px-6 py-4 font-semibold text-slate-600">
              <div :class="descriptorClass">
                <span>Scope</span>
                <button class="cursor-help" :class="descriptorButtonClass" aria-label="Job scope">?</button>
                <div :class="descriptorPopoverClass">
                  Identifies whether the job was a daily sync or a historical backfill, and whether it targeted one school or the full portfolio.
                </div>
              </div>
            </th>
            <th class="px-6 py-4 font-semibold text-slate-600">
              <div :class="descriptorClass">
                <span>Status</span>
                <button class="cursor-help" :class="descriptorButtonClass" aria-label="Job status and runtime">?</button>
                <div :class="descriptorPopoverClass">
                  Current lifecycle state for the run. Runtime is shown when enough timing data is available.
                </div>
              </div>
            </th>
            <th class="px-6 py-4 font-semibold text-slate-600">
              <div :class="descriptorClass">
                <span>Progress</span>
                <button class="cursor-help" :class="descriptorButtonClass" aria-label="Job progress">?</button>
                <div :class="descriptorPopoverClass">
                  For syncs this is schools completed out of schools queued. For backfills it is completed school-day snapshots out of the total work units.
                </div>
              </div>
            </th>
            <th class="px-6 py-4 font-semibold text-slate-600">
              <div :class="descriptorClass">
                <span>Range / Snapshot</span>
                <button class="cursor-help" :class="descriptorButtonClass" aria-label="Affected dates">?</button>
                <div :class="descriptorPopoverClass">
                  Backfills show the requested date range. Daily syncs show the snapshot date that the job wrote.
                </div>
              </div>
            </th>
            <th class="px-6 py-4 font-semibold text-slate-600">
              <div :class="descriptorClass">
                <span>Details</span>
                <button class="cursor-help" :class="descriptorButtonClass" aria-label="Captured issues">?</button>
                <div :class="descriptorPopoverClass">
                  Summarizes issues captured during execution. The surfaced error message is the first recorded failure or sync warning saved for the run.
                </div>
              </div>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="run in (data?.syncRuns as SyncRun[])"
            :key="run.jobId"
            class="group transition"
          >
            <td class="rounded-l-3xl border-y border-l border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div class="font-medium text-slate-900">{{ run.jobId.slice(0, 8) }}...</div>
              <div class="mt-1 text-xs text-slate-500">{{ formatDateTime(run.attemptedAt) }}</div>
            </td>
            <td class="border-y border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div class="font-medium text-slate-900 capitalize">{{ formatScope(run.scope) }}</div>
              <div v-if="run.school" class="mt-1 text-xs text-slate-500">Target: {{ run.school }}</div>
            </td>
            <td class="border-y border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <Badge :tone="getStatusTone(run.status)">{{ run.status }}</Badge>
              <div v-if="formatRuntime(run)" class="mt-2 text-xs text-slate-500">
                {{ formatRuntime(run) }}
              </div>
              <div v-if="run.statusDetail" class="mt-2 text-xs text-slate-500">
                {{ run.statusDetail.replaceAll('_', ' ') }}
              </div>
            </td>
            <td class="border-y border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div v-if="formatProgress(run)" class="font-medium text-slate-900" :title="run.scope.includes('backfill') ? 'Completed snapshots out of total school-day snapshots in this backfill' : 'Completed schools out of total schools in this sync'">{{ formatProgress(run) }}</div>
              <div v-else class="text-xs text-slate-400">No progress data</div>
              <div v-if="run.startedAt" class="mt-2 text-xs text-slate-500">
                Started {{ formatDateTime(run.startedAt) }}
              </div>
              <div v-if="run.scope.includes('backfill')" class="mt-2 space-y-1 text-xs text-slate-500">
                <div>Completed: {{ run.completedUnits ?? 0 }}</div>
                <div>Failed: {{ run.failedUnits ?? 0 }}</div>
                <div>Skipped: {{ run.skippedUnits ?? 0 }}</div>
              </div>
            </td>
            <td class="border-y border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div v-if="formatBackfillRange(run)" class="font-medium text-slate-900" title="Inclusive historical date range requested by the job">{{ formatBackfillRange(run) }}</div>
              <div v-else-if="run.snapshotDate" class="font-medium text-slate-900" title="Snapshot date written by this daily sync">{{ run.snapshotDate }}</div>
              <div v-else class="text-xs text-slate-400">No snapshot data</div>
              <div v-if="run.scope.includes('backfill')" class="mt-2 text-xs text-slate-500">Historical backfill</div>
              <div v-else class="mt-2 text-xs text-slate-500">Daily sync</div>
              <div v-if="run.currentSchool || run.currentSnapshotDate" class="mt-2 text-xs text-slate-500">
                Current: {{ run.currentSchool ?? 'Unknown school' }}<span v-if="run.currentSnapshotDate"> on {{ run.currentSnapshotDate }}</span>
              </div>
              <div v-if="run.lastHeartbeatAt" class="mt-2 text-xs text-slate-500">
                Heartbeat: {{ formatRelativeAge(run.lastHeartbeatAt) }}
              </div>
              <div v-if="run.lastProgressAt" class="mt-1 text-xs text-slate-500">
                Last progress: {{ formatRelativeAge(run.lastProgressAt) }}
              </div>
            </td>
            <td class="rounded-r-3xl border-y border-r border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div v-if="run.errorCount" class="mb-2 text-xs text-slate-600" title="Count of warnings and errors captured during execution">
                {{ run.errorCount }} captured issue{{ run.errorCount === 1 ? '' : 's' }}
              </div>
              <div v-if="run.failureReason" class="mb-2 text-xs text-amber-700">
                {{ run.failureReason }}
              </div>
              <div v-if="run.errorMessage" class="text-xs text-rose-600 font-mono bg-rose-50 p-2 rounded max-w-md overflow-x-auto">
                {{ run.errorMessage }}
              </div>
              <div v-else class="text-xs text-slate-400">No errors</div>
              <div v-if="run.failedUnitsSample?.length" class="mt-3 space-y-2">
                <div
                  v-for="sample in run.failedUnitsSample.slice(0, 3)"
                  :key="`${sample.school}-${sample.snapshotDate}-${sample.attemptCount}`"
                  class="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800"
                >
                  <div class="font-medium">{{ sample.school }} • {{ sample.snapshotDate }}</div>
                  <div v-if="sample.error" class="mt-1 break-words">{{ sample.error }}</div>
                </div>
              </div>
              <div v-if="canResume(run) || canRetryFailures(run)" class="mt-3 flex flex-wrap gap-2">
                <button
                  v-if="canResume(run)"
                  class="rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 transition hover:border-slate-400 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="runActionPending(run)"
                  @click="triggerResume(run)"
                >
                  {{ runActionPending(run) ? 'Working…' : 'Resume' }}
                </button>
                <button
                  v-if="canRetryFailures(run)"
                  class="rounded-full border border-amber-300 bg-amber-50 px-3 py-1 text-xs font-medium text-amber-800 transition hover:border-amber-400 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="runActionPending(run)"
                  @click="triggerRetryFailures(run)"
                >
                  {{ runActionPending(run) ? 'Working…' : 'Retry Failures' }}
                </button>
              </div>
            </td>
          </tr>
          <tr v-if="!data?.syncRuns?.length">
            <td colspan="6" class="px-6 py-8 text-center text-slate-500">No jobs found in history.</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
