<script setup lang="ts">
import { computed, ref } from 'vue';
import { RouterLink } from 'vue-router';
import { keepPreviousData, useMutation, useQuery, useQueryClient } from '@tanstack/vue-query';
import { getSyncRuns, resumeHistoryBackfill, retryHistoryBackfillFailures } from '@/api';
import Badge from '@/components/ui/Badge.vue';
import { isStaticDataMode } from '@/config/runtime';
import {
  canResumeJob,
  canRetryFailuresForJob,
  formatBackfillRange,
  formatDateTime,
  formatJobProgress,
  formatJobRuntime,
  formatJobScope,
  formatRelativeAge,
  getJobStatusTone,
  type JobRun,
  type SyncRunsResponse,
} from '@/types/jobRuns';
import { getLocalTimeZoneLabel } from '@/utils/dateTime';

const queryClient = useQueryClient();
const PAGE_SIZE = 25;
const currentPage = ref(0);
const currentOffset = computed(() => currentPage.value * PAGE_SIZE);

const { data, isLoading, error } = useQuery({
  queryKey: computed(() => ['syncRuns', { limit: PAGE_SIZE, offset: currentOffset.value }]),
  queryFn: () => getSyncRuns({ limit: PAGE_SIZE, offset: currentOffset.value }).then((res) => res.data as SyncRunsResponse),
  refetchInterval: isStaticDataMode ? false : 5000,
  placeholderData: keepPreviousData,
});

const hasSyncRunsData = computed(() => Array.isArray(data.value?.syncRuns) && data.value.syncRuns.length > 0);
const totalCount = computed(() => data.value?.totalCount ?? 0);
const pageCount = computed(() => Math.max(1, Math.ceil(totalCount.value / PAGE_SIZE)));
const showingFrom = computed(() => (totalCount.value === 0 ? 0 : currentOffset.value + 1));
const showingTo = computed(() => Math.min(currentOffset.value + (data.value?.syncRuns?.length ?? 0), totalCount.value));
const canGoBack = computed(() => currentPage.value > 0);
const canGoForward = computed(() => currentPage.value + 1 < pageCount.value);

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

const activeResumeJobId = computed(() => resumeMutation.variables as unknown as string | undefined);
const activeRetryJobId = computed(() => retryFailuresMutation.variables as unknown as string | undefined);
const runActionPending = (run: JobRun) =>
  (resumeMutation.isPending && activeResumeJobId.value === run.jobId)
  || (retryFailuresMutation.isPending && activeRetryJobId.value === run.jobId);
const triggerResume = async (run: JobRun) => {
  await resumeMutation.mutateAsync(run.jobId);
};
const triggerRetryFailures = async (run: JobRun) => {
  await retryFailuresMutation.mutateAsync(run.jobId);
};

const goToPreviousPage = () => {
  if (!canGoBack.value) return;
  currentPage.value -= 1;
};

const goToNextPage = () => {
  if (!canGoForward.value) return;
  currentPage.value += 1;
};

const descriptorClass = 'group relative inline-flex items-center gap-2';
const descriptorButtonClass = 'flex h-5 w-5 items-center justify-center rounded-full border border-slate-300 bg-white text-[11px] font-semibold text-slate-500 transition hover:border-slate-400 hover:text-slate-700';
const descriptorPopoverClass = 'pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-64 -translate-x-1/2 rounded-2xl border border-slate-200 bg-white p-3 text-[11px] leading-5 text-slate-600 shadow-lg group-hover:block';
const localTimeZoneLabel = getLocalTimeZoneLabel();
</script>

<template>
  <div class="px-8 py-8 w-full max-w-78xl mx-auto">
    <div class="mb-8 flex items-center justify-between">
      <div>
        <h1 class="text-2xl font-semibold tracking-tight text-slate-950">Background Jobs</h1>
        <p class="mt-1 text-sm text-slate-500">Monitor bulk school syncs and historical backfill queue status.</p>
        <p class="mt-2 text-xs text-slate-400">Times shown in {{ localTimeZoneLabel }}.</p>
      </div>
    </div>

    <div v-if="isLoading" class="text-sm text-slate-500">Loading jobs...</div>
    <div v-else-if="error && !hasSyncRunsData" class="text-sm text-rose-500">Failed to load job history.</div>
    <div v-else class="space-y-4">
      <div v-if="error" class="rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-base text-amber-800">
        {{ isStaticDataMode ? 'Showing the exported snapshot of job history from the static build.' : 'Live updates are temporarily unavailable. Showing the most recent job history we already loaded.' }}
      </div>
      <div v-if="isStaticDataMode" class="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-base text-slate-700">
        This hosted view is read-only. Resume and retry actions stay available in the live dashboard only.
      </div>
      <div v-if="mutationError" class="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-base text-rose-700">
        {{ mutationError }}
      </div>
      <div class="flex flex-wrap items-center justify-between gap-3 px-1 text-base text-slate-500">
        <p>
          Showing {{ showingFrom }}-{{ showingTo }} of {{ totalCount }} job{{ totalCount === 1 ? '' : 's' }}.
        </p>
        <div class="flex items-center gap-2">
          <button
            class="rounded-full border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:border-slate-400 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
            :disabled="!canGoBack"
            @click="goToPreviousPage"
          >
            Previous
          </button>
          <span class="text-xs font-medium text-slate-500">Page {{ currentPage + 1 }} of {{ pageCount }}</span>
          <button
            class="rounded-full border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:border-slate-400 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
            :disabled="!canGoForward"
            @click="goToNextPage"
          >
            Next
          </button>
        </div>
      </div>

      <div class="rounded-[24px] border border-slate-200 bg-white p-3 shadow-sm">
      <table class="w-full border-separate border-spacing-y-3 text-left text-[1.02rem]">
        <thead class="text-xs uppercase tracking-[0.1em] text-slate-500">
          <tr>
            <th class="px-6 py-4 font-semibold text-slate-600">
              <div :class="descriptorClass">
                <span>ID / Local Time</span>
                <button class="cursor-help" :class="descriptorButtonClass" aria-label="Job identity and local creation time">?</button>
                <div :class="descriptorPopoverClass">
                  Shows the background job id plus the time the run was first recorded by the backend, displayed in your local timezone.
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
            v-for="run in (data?.syncRuns as JobRun[])"
            :key="run.jobId"
            class="group transition"
          >
            <td class="rounded-l-3xl border-y border-l border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div class="font-medium text-slate-900">{{ run.jobId.slice(0, 8) }}...</div>
              <div class="mt-1 text-sm text-slate-500">{{ formatDateTime(run.attemptedAt) }}</div>
            </td>
            <td class="border-y border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div class="font-medium text-slate-900 capitalize">{{ formatJobScope(run.scope) }}</div>
              <div v-if="run.school" class="mt-1 text-sm text-slate-500">Target: {{ run.school }}</div>
            </td>
            <td class="border-y border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <Badge :tone="getJobStatusTone(run.status)">{{ run.status }}</Badge>
              <div v-if="formatJobRuntime(run)" class="mt-2 text-sm text-slate-500">
                {{ formatJobRuntime(run) }}
              </div>
              <div v-if="run.statusDetail" class="mt-2 text-sm text-slate-500">
                {{ run.statusDetail.replaceAll('_', ' ') }}
              </div>
            </td>
            <td class="border-y border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div v-if="formatJobProgress(run)" class="font-medium text-slate-900" :title="run.scope.includes('backfill') ? 'Completed snapshots out of total school-day snapshots in this backfill' : 'Completed schools out of total schools in this sync'">{{ formatJobProgress(run) }}</div>
              <div v-else class="text-sm text-slate-400">No progress data</div>
              <div v-if="run.startedAt" class="mt-2 text-sm text-slate-500">
                Started {{ formatDateTime(run.startedAt) }}
              </div>
              <div v-if="run.scope.includes('backfill')" class="mt-2 space-y-1 text-sm text-slate-500">
                <div>Completed: {{ run.completedUnits ?? 0 }}</div>
                <div>Failed: {{ run.failedUnits ?? 0 }}</div>
                <div>Skipped: {{ run.skippedUnits ?? 0 }}</div>
              </div>
            </td>
            <td class="border-y border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <div v-if="formatBackfillRange(run)" class="font-medium text-slate-900" title="Inclusive historical date range requested by the job">{{ formatBackfillRange(run) }}</div>
              <div v-else-if="run.snapshotDate" class="font-medium text-slate-900" title="Snapshot date written by this daily sync">{{ run.snapshotDate }}</div>
              <div v-else class="text-sm text-slate-400">No snapshot data</div>
              <div v-if="run.scope.includes('backfill')" class="mt-2 text-sm text-slate-500">Historical backfill</div>
              <div v-else class="mt-2 text-sm text-slate-500">Daily sync</div>
              <div v-if="run.currentSchool || run.currentSnapshotDate" class="mt-2 text-sm text-slate-500">
                Current: {{ run.currentSchool ?? 'Unknown school' }}<span v-if="run.currentSnapshotDate"> on {{ run.currentSnapshotDate }}</span>
              </div>
              <div v-if="run.lastHeartbeatAt" class="mt-2 text-sm text-slate-500">
                <div>Heartbeat: {{ formatRelativeAge(run.lastHeartbeatAt) }}</div>
                <div class="mt-1 text-slate-400">{{ formatDateTime(run.lastHeartbeatAt) }}</div>
              </div>
              <div v-if="run.lastProgressAt" class="mt-1 text-sm text-slate-500">
                <div>Last progress: {{ formatRelativeAge(run.lastProgressAt) }}</div>
                <div class="mt-1 text-slate-400">{{ formatDateTime(run.lastProgressAt) }}</div>
              </div>
            </td>
            <td class="rounded-r-3xl border-y border-r border-slate-200 bg-slate-50/70 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300 group-hover:bg-white">
              <RouterLink
                :to="`/admin/jobs/${run.jobId}`"
                class="mb-3 inline-flex items-center rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 transition hover:border-slate-400 hover:bg-slate-100"
              >
                View details
              </RouterLink>
              <div v-if="run.errorCount" class="mb-2 text-sm text-slate-600" title="Count of warnings and errors captured during execution">
                {{ run.errorCount }} captured issue{{ run.errorCount === 1 ? '' : 's' }}
              </div>
              <div v-if="run.failureReason" class="mb-2 text-sm text-amber-700">
                {{ run.failureReason }}
              </div>
              <div v-if="run.errorMessage" class="text-sm text-rose-600 font-mono bg-rose-50 p-2 rounded max-w-md overflow-x-auto">
                {{ run.errorMessage }}
              </div>
              <div v-else class="text-sm text-slate-400">No errors</div>
              <div v-if="run.failedUnitsSample?.length" class="mt-3 space-y-2">
                <div
                  v-for="sample in run.failedUnitsSample.slice(0, 3)"
                  :key="`${sample.school}-${sample.snapshotDate}-${sample.attemptCount}`"
                  class="rounded-xl border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-800"
                >
                  <div class="font-medium">{{ sample.school }} • {{ sample.snapshotDate }}</div>
                  <div v-if="sample.error" class="mt-1 break-words">{{ sample.error }}</div>
                </div>
              </div>
              <div v-if="canResumeJob(run) || canRetryFailuresForJob(run)" class="mt-3 flex flex-wrap gap-2">
                <button
                  v-if="canResumeJob(run)"
                  class="rounded-full border border-slate-300 px-3 py-1 text-xs font-medium text-slate-700 transition hover:border-slate-400 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="isStaticDataMode || runActionPending(run)"
                  @click="triggerResume(run)"
                >
                  {{ isStaticDataMode ? 'Unavailable on static site' : (runActionPending(run) ? 'Working…' : 'Resume') }}
                </button>
                <button
                  v-if="canRetryFailuresForJob(run)"
                  class="rounded-full border border-amber-300 bg-amber-50 px-3 py-1 text-xs font-medium text-amber-800 transition hover:border-amber-400 hover:bg-amber-100 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="isStaticDataMode || runActionPending(run)"
                  @click="triggerRetryFailures(run)"
                >
                  {{ isStaticDataMode ? 'Unavailable on static site' : (runActionPending(run) ? 'Working…' : 'Retry Failures') }}
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
  </div>
</template>
