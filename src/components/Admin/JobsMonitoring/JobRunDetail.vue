<script setup lang="ts">
import { computed } from 'vue';
import { RouterLink } from 'vue-router';
import { useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { getSyncRun, resumeHistoryBackfill, retryHistoryBackfillFailures } from '@/api';
import Badge from '@/components/ui/Badge.vue';
import Card from '@/components/ui/Card.vue';
import {
  canResumeJob,
  canRetryFailuresForJob,
  formatBackfillRange,
  formatDateTime,
  formatDuration,
  formatEta,
  formatJobCompletionPercent,
  formatJobProgress,
  formatJobRuntime,
  formatJobScope,
  formatRelativeAge,
  getElapsedSeconds,
  getJobStatusTone,
  getJobThroughputPerMinute,
  getRemainingUnits,
  type JobRun,
} from '@/types/jobRuns';

const props = defineProps<{
  jobId: string;
}>();

const queryClient = useQueryClient();

const { data, isLoading, error } = useQuery({
  queryKey: ['syncRun', props.jobId],
  queryFn: () => getSyncRun(props.jobId).then((res) => res.data),
  refetchInterval: 5000,
});

const hasSyncRunData = computed(() => Boolean(data.value?.syncRun));
const isNotFoundError = computed(() => {
  const queryError = error.value as { response?: { status?: number } } | null;
  return queryError?.response?.status === 404;
});

const run = computed<JobRun | null>(() => (data.value?.syncRun as JobRun | undefined) ?? null);

const resumeMutation = useMutation({
  mutationFn: (jobId: string) => resumeHistoryBackfill(jobId),
  onSuccess: async (_result, jobId) => {
    await queryClient.invalidateQueries({ queryKey: ['syncRuns'] });
    await queryClient.invalidateQueries({ queryKey: ['syncRun', jobId] });
  },
});

const retryFailuresMutation = useMutation({
  mutationFn: (jobId: string) => retryHistoryBackfillFailures(jobId),
  onSuccess: async (_result, jobId) => {
    await queryClient.invalidateQueries({ queryKey: ['syncRuns'] });
    await queryClient.invalidateQueries({ queryKey: ['syncRun', jobId] });
  },
});

const mutationError = computed(() => {
  const resumeError = resumeMutation.error as { response?: { data?: { detail?: string } }; message?: string } | null;
  const retryError = retryFailuresMutation.error as { response?: { data?: { detail?: string } }; message?: string } | null;
  return resumeError?.response?.data?.detail || retryError?.response?.data?.detail || resumeError?.message || retryError?.message || null;
});

const activeResumeJobId = computed(() => resumeMutation.variables as unknown as string | undefined);
const activeRetryJobId = computed(() => retryFailuresMutation.variables as unknown as string | undefined);
const runActionPending = computed(() =>
  Boolean(run.value) && (
    (resumeMutation.isPending && activeResumeJobId.value === run.value?.jobId)
    || (retryFailuresMutation.isPending && activeRetryJobId.value === run.value?.jobId)
  )
);

const detailItems = computed(() => {
  if (!run.value) return [];
  return [
    { label: 'Scope', value: formatJobScope(run.value.scope) },
    { label: 'Target', value: run.value.school ?? 'All schools' },
    { label: 'Attempted', value: formatDateTime(run.value.attemptedAt) },
    { label: 'Started', value: formatDateTime(run.value.startedAt) },
    { label: 'Finished', value: formatDateTime(run.value.finishedAt) },
    { label: 'Runtime', value: formatJobRuntime(run.value) ?? 'In progress' },
  ];
});

const progressInsights = computed(() => {
  if (!run.value) return [];
  const throughput = getJobThroughputPerMinute(run.value);
  const remainingUnits = getRemainingUnits(run.value);
  const eta = formatEta(run.value);
  const percentComplete = formatJobCompletionPercent(run.value);
  const elapsed = formatDuration(getElapsedSeconds(run.value));

  return [
    { label: 'Percent complete', value: percentComplete ?? 'Unknown' },
    { label: 'Units per minute', value: throughput != null ? `${throughput.toFixed(1)} / min` : 'Unknown' },
    { label: 'Remaining units', value: remainingUnits != null ? String(remainingUnits) : 'Unknown' },
    { label: 'Estimated time remaining', value: eta ?? 'Unknown' },
    { label: 'Elapsed time', value: elapsed ?? 'Unknown' },
    { label: 'Stall threshold', value: run.value.stallThresholdSeconds ? formatDuration(run.value.stallThresholdSeconds) ?? 'Unknown' : 'Not tracked' },
  ];
});

const triggerResume = async () => {
  if (!run.value) return;
  await resumeMutation.mutateAsync(run.value.jobId);
};

const triggerRetryFailures = async () => {
  if (!run.value) return;
  await retryFailuresMutation.mutateAsync(run.value.jobId);
};
</script>

<template>
  <div class="w-full max-w-6xl mx-auto px-8 py-8">
    <div class="mb-8 flex flex-wrap items-start justify-between gap-4">
      <div>
        <RouterLink
          to="/admin/jobs"
          class="inline-flex items-center rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-600 transition hover:border-slate-400 hover:bg-slate-50"
        >
          Back to jobs
        </RouterLink>
        <h1 class="mt-4 text-2xl font-semibold tracking-tight text-slate-950">Job details</h1>
        <p class="mt-1 text-sm text-slate-500">Inspect progress, failures, and recovery options for a single background run.</p>
      </div>
      <div v-if="run" class="rounded-[24px] border border-slate-200 bg-white px-5 py-4 shadow-sm">
        <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Job ID</p>
        <p class="mt-2 font-mono text-sm text-slate-900">{{ run.jobId }}</p>
      </div>
    </div>

    <div v-if="isLoading" class="text-sm text-slate-500">Loading job details...</div>
    <div v-else-if="isNotFoundError && !hasSyncRunData" class="rounded-[28px] border border-slate-200 bg-white p-8 text-sm text-slate-500 shadow-sm">
      This job could not be found.
    </div>
    <div v-else-if="error && !hasSyncRunData" class="text-sm text-rose-500">Failed to load job details.</div>
    <div v-else-if="!run" class="rounded-[28px] border border-slate-200 bg-white p-8 text-sm text-slate-500 shadow-sm">
      This job could not be found.
    </div>
    <template v-else>
      <div v-if="error" class="mb-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
        Live updates are temporarily unavailable. Showing the most recent job details we already loaded.
      </div>
      <div v-if="mutationError" class="mb-4 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
        {{ mutationError }}
      </div>

      <div class="grid gap-6 lg:grid-cols-[1.4fr_0.9fr]">
        <Card>
          <template #header>
            <div class="flex flex-wrap items-start justify-between gap-3">
              <div>
                <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Overview</p>
                <h3 class="mt-2 text-lg font-semibold text-slate-950">Run summary</h3>
              </div>
              <Badge :tone="getJobStatusTone(run.status)">{{ run.status }}</Badge>
            </div>
          </template>

          <div class="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
            <div
              v-for="item in detailItems"
              :key="item.label"
              class="rounded-2xl border border-slate-200 bg-slate-50/70 px-4 py-3"
            >
              <p class="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{{ item.label }}</p>
              <p class="mt-2 text-sm text-slate-900">{{ item.value }}</p>
            </div>
          </div>

          <div class="mt-6 grid gap-4 md:grid-cols-2">
            <div class="rounded-2xl border border-slate-200 bg-white px-4 py-4">
              <p class="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Progress</p>
              <p class="mt-2 text-lg font-semibold text-slate-950">{{ formatJobProgress(run) ?? 'No progress data' }}</p>
              <div v-if="run.scope.includes('backfill')" class="mt-3 space-y-1 text-sm text-slate-600">
                <div>Completed units: {{ run.completedUnits ?? 0 }}</div>
                <div>Failed units: {{ run.failedUnits ?? 0 }}</div>
                <div>Skipped units: {{ run.skippedUnits ?? 0 }}</div>
              </div>
              <div v-else class="mt-3 text-sm text-slate-600">
                Schools processed: {{ run.schoolsProcessed ?? 0 }}
              </div>
            </div>

            <div class="rounded-2xl border border-slate-200 bg-white px-4 py-4">
              <p class="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Date coverage</p>
              <p class="mt-2 text-sm text-slate-900">
                {{ formatBackfillRange(run) ?? run.snapshotDate ?? 'No snapshot data' }}
              </p>
              <div v-if="run.currentSchool || run.currentSnapshotDate" class="mt-3 text-sm text-slate-600">
                Current unit: {{ run.currentSchool ?? 'Unknown school' }}<span v-if="run.currentSnapshotDate"> on {{ run.currentSnapshotDate }}</span>
              </div>
              <div v-if="run.lastHeartbeatAt" class="mt-2 text-sm text-slate-600">
                Last heartbeat: {{ formatRelativeAge(run.lastHeartbeatAt) }}
              </div>
              <div v-if="run.lastProgressAt" class="mt-1 text-sm text-slate-600">
                Last progress: {{ formatRelativeAge(run.lastProgressAt) }}
              </div>
            </div>
          </div>

          <div class="mt-6 rounded-2xl border border-slate-200 bg-white px-4 py-4">
            <p class="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Live estimates</p>
            <div class="mt-3 grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
              <div
                v-for="item in progressInsights"
                :key="item.label"
                class="rounded-2xl border border-slate-200 bg-slate-50/70 px-3 py-3"
              >
                <p class="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-500">{{ item.label }}</p>
                <p class="mt-2 text-sm text-slate-900">{{ item.value }}</p>
              </div>
            </div>
          </div>
        </Card>

        <div class="space-y-6">
          <Card title="Recovery" subtitle="Actions">
            <div class="space-y-3 text-sm text-slate-600">
              <p v-if="run.statusDetail">Status detail: {{ run.statusDetail.replaceAll('_', ' ') }}</p>
              <p v-if="run.failureReason" class="text-amber-700">{{ run.failureReason }}</p>
              <p v-if="run.checkpointState?.remainingUnits != null">
                Remaining units: {{ run.checkpointState.remainingUnits }}
              </p>
              <p v-if="run.checkpointState?.currentSchool || run.checkpointState?.currentSnapshotDate">
                Checkpoint: {{ run.checkpointState?.currentSchool ?? 'Unknown school' }}<span v-if="run.checkpointState?.currentSnapshotDate"> on {{ run.checkpointState.currentSnapshotDate }}</span>
              </p>
            </div>

            <div v-if="canResumeJob(run) || canRetryFailuresForJob(run)" class="mt-4 flex flex-wrap gap-2">
              <button
                v-if="canResumeJob(run)"
                class="rounded-full border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:border-slate-400 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
                :disabled="runActionPending"
                @click="triggerResume"
              >
                {{ runActionPending ? 'Working…' : 'Resume backfill' }}
              </button>
              <button
                v-if="canRetryFailuresForJob(run)"
                class="rounded-full border border-amber-300 bg-amber-50 px-3 py-1.5 text-xs font-medium text-amber-800 transition hover:border-amber-400 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-50"
                :disabled="runActionPending"
                @click="triggerRetryFailures"
              >
                {{ runActionPending ? 'Working…' : 'Retry failed units' }}
              </button>
            </div>
            <p v-else class="mt-4 text-sm text-slate-500">No recovery action is available for this run.</p>
          </Card>

          <Card title="Issues" subtitle="Diagnostics">
            <div v-if="run.errorMessage" class="rounded-2xl border border-rose-200 bg-rose-50 p-3 font-mono text-xs text-rose-700">
              {{ run.errorMessage }}
            </div>
            <p v-else class="text-sm text-slate-500">No primary error message was recorded.</p>

            <div v-if="run.errors?.length" class="mt-4 space-y-2">
              <div
                v-for="entry in run.errors"
                :key="entry"
                class="rounded-2xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700"
              >
                {{ entry }}
              </div>
            </div>

            <div v-if="run.failedUnitsSample?.length" class="mt-4 space-y-2">
              <div
                v-for="sample in run.failedUnitsSample"
                :key="`${sample.school}-${sample.snapshotDate}-${sample.attemptCount}`"
                class="rounded-2xl border border-amber-200 bg-amber-50 px-3 py-3 text-xs text-amber-900"
              >
                <div class="font-semibold">
                  {{ sample.school ?? 'Unknown school' }}<span v-if="sample.snapshotDate"> on {{ sample.snapshotDate }}</span>
                </div>
                <div v-if="sample.attemptCount != null" class="mt-1">Attempt {{ sample.attemptCount }}</div>
                <div v-if="sample.error" class="mt-1 break-words">{{ sample.error }}</div>
              </div>
            </div>
          </Card>
        </div>
      </div>
    </template>
  </div>
</template>
