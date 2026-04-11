<script setup lang="ts">
import { computed, ref } from 'vue';
import { useRoute } from 'vue-router';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { getClientHealthHistory, getClientHealthActiveUsers, getClientHealthSyncMetadata, getSyncStatus, triggerHistoryBackfill, triggerSync } from '@/api';
import type { FailedMerge } from '@/types/clientHealth';
import VueApexCharts from 'vue3-apexcharts';
import Card from '@/components/ui/Card.vue';
import { useChartOptions, useStackedBarChartOptions } from '@/composables/useChartOptions';
import { formatSchoolLabel } from '@/utils/schoolNames';

const route = useRoute();
const queryClient = useQueryClient();
const school = route.params.school as string;
const schoolLabel = computed(() => formatSchoolLabel(school));
const backfillDays = ref(7);

const isSyncing = ref(false);
const syncError = ref<string | null>(null);
const syncResult = ref<{
  status?: string;
  totalSec?: number;
  schoolsProcessed?: number;
  totalSchools?: number;
  errors?: string[];
  alreadyRunning?: boolean;
} | null>(null);
const isBackfilling = ref(false);
const backfillError = ref<string | null>(null);
const backfillResult = ref<{
  status?: string;
  totalSec?: number;
  schoolsProcessed?: number;
  totalSchools?: number;
  errors?: string[];
  alreadyRunning?: boolean;
} | null>(null);



const { data: history, isLoading: isLoadingHistory, error: historyError } = useQuery({
  queryKey: ['clientHealthHistory', school, { days: 30 }],
  queryFn: () => getClientHealthHistory({ school, days: 30 }).then((res) => res.data),
});

const { data: activeUsers, isLoading: isLoadingUsers, error: usersError } = useQuery({
  queryKey: ['clientHealthActiveUsers', school],
  queryFn: () => getClientHealthActiveUsers({ school }).then((res) => res.data),
});

const { data: syncMetadata, isLoading: isLoadingSyncMetadata } = useQuery({
  queryKey: ['clientHealthSyncMetadata', school],
  queryFn: () => getClientHealthSyncMetadata({ school }).then((res) => res.data),
});

const loading = computed(() => isLoadingHistory.value || isLoadingUsers.value || isLoadingSyncMetadata.value);
const error = computed(() => historyError.value || usersError.value ? 'Failed to load data' : null);
const snapshotCount = computed(() => history.value?.snapshots?.length ?? 0);
const latestSnapshot = computed(() => {
  const snapshots = history.value?.snapshots ?? [];
  return snapshots.length > 0 ? snapshots[snapshots.length - 1] : null;
});
const lastSuccessfulSyncLabel = computed(() => {
  const createdAt = syncMetadata.value?.lastSuccessfulSync?.createdAt;
  if (!createdAt) return 'No successful sync yet';
  return new Date(createdAt).toLocaleString();
});
const lastAttemptedSyncLabel = computed(() => {
  const attemptedAt = syncMetadata.value?.lastAttemptedSync?.attemptedAt;
  if (!attemptedAt) return 'No attempted sync yet';
  return new Date(attemptedAt).toLocaleString();
});
const lastAttemptStatusLabel = computed(() => {
  const status = syncMetadata.value?.lastAttemptedSync?.status;
  if (!status) return null;
  return status.charAt(0).toUpperCase() + status.slice(1);
});

const nightlySuccessChartSeries = computed(() => {
  if (!history.value) return [];
  const succeeded = history.value.snapshots.map((s: any) => s.merges.nightly.succeeded);
  const issues = history.value.snapshots.map((s: any) => s.merges.nightly.finishedWithIssues);
  const noData = history.value.snapshots.map((s: any) => s.merges.nightly.noData);
  const failed = history.value.snapshots.map((s: any) => s.merges.nightly.failed);
  
  return [
    { name: 'Succeeded', data: succeeded },
    { name: 'Finished With Issues', data: issues },
    { name: 'No Data', data: noData },
    { name: 'Failed', data: failed }
  ];
});

const nightlySuccessChartOptions = computed(() => useStackedBarChartOptions({
  categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
}));

const realtimeSuccessChartSeries = computed(() => {
  if (!history.value) return [];
  const succeeded = history.value.snapshots.map((s: any) => s.merges.realtime.succeeded);
  const issues = history.value.snapshots.map((s: any) => s.merges.realtime.finishedWithIssues);
  const noData = history.value.snapshots.map((s: any) => s.merges.realtime.noData);
  const failed = history.value.snapshots.map((s: any) => s.merges.realtime.failed);

  return [
    { name: 'Succeeded', data: succeeded },
    { name: 'Finished With Issues', data: issues },
    { name: 'No Data', data: noData },
    { name: 'Failed', data: failed }
  ];
});

const realtimeSuccessChartOptions = computed(() => useStackedBarChartOptions({
  categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
}));

const mergeErrorsChartSeries = computed(() => {
  if (!history.value) return [];
  const data = history.value.snapshots.map((s: any) => s.mergeErrorsCount);
  return [{ name: 'Open Merge Errors per Snapshot', data }];
});

const mergeErrorsChartOptions = computed(() => useChartOptions({
  colors: ['#ffcd56'],
  categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
}));

const activeUsersChartSeries = computed(() => {
  if (!history.value) return [];
  const data = history.value.snapshots.map((s: any) => s.activeUsers24h);
  return [{ name: 'Distinct Active Users (24h)', data }];
});

const activeUsersChartOptions = computed(() => useChartOptions({
  colors: ['#36a2eb'],
  categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
}));

async function pollSyncJob(jobId: string) {
  while (true) {
    const status = await getSyncStatus(jobId);
    syncResult.value = {
      status: status.status,
      totalSec: status.timing?.totalSec,
      schoolsProcessed: status.schoolsProcessed,
      totalSchools: status.totalSchools,
      errors: status.errors,
      alreadyRunning: status.alreadyRunning,
    };

    if (status.status === 'completed') {
      await queryClient.invalidateQueries({ queryKey: ['clientHealth'] });
      await queryClient.invalidateQueries({ queryKey: ['clientHealthHistory', school, { days: 30 }] });
      await queryClient.invalidateQueries({ queryKey: ['clientHealthActiveUsers', school] });
      await queryClient.invalidateQueries({ queryKey: ['clientHealthSyncMetadata', school] });
      break;
    }

    if (status.status === 'failed') {
      syncError.value = status.error || 'Sync failed';
      break;
    }

    await new Promise((resolve) => window.setTimeout(resolve, 1500));
  }
}

async function handleSchoolSync() {
  isSyncing.value = true;
  syncError.value = null;
  syncResult.value = null;

  try {
    const result = await triggerSync({ school });
    await pollSyncJob(result.jobId);
  } catch (e: any) {
    syncError.value = e?.response?.data?.detail || e?.message || 'Sync failed';
  } finally {
    isSyncing.value = false;
  }
}

async function pollBackfillJob(jobId: string) {
  while (true) {
    const status = await getSyncStatus(jobId);
    backfillResult.value = {
      status: status.status,
      totalSec: status.timing?.totalSec,
      schoolsProcessed: status.schoolsProcessed,
      totalSchools: status.totalSchools,
      errors: status.errors,
      alreadyRunning: status.alreadyRunning,
    };

    if (status.status === 'completed') {
      await queryClient.invalidateQueries({ queryKey: ['clientHealth'] });
      await queryClient.invalidateQueries({ queryKey: ['clientHealthHistory', school, { days: 30 }] });
      await queryClient.invalidateQueries({ queryKey: ['clientHealthSyncMetadata', school] });
      break;
    }

    if (status.status === 'failed') {
      backfillError.value = status.error || 'Backfill failed';
      break;
    }

    await new Promise((resolve) => window.setTimeout(resolve, 1500));
  }
}

async function handleHistoryBackfill() {
  isBackfilling.value = true;
  backfillError.value = null;
  backfillResult.value = null;

  try {
    const result = await triggerHistoryBackfill({ school, days: backfillDays.value });
    await pollBackfillJob(result.jobId);
  } catch (e: any) {
    backfillError.value = e?.response?.data?.detail || e?.message || 'Backfill failed';
  } finally {
    isBackfilling.value = false;
  }
}
</script>

<template>
  <div class="min-h-screen bg-slate-50">
    <div class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      <div class="mb-8 rounded-[28px] border border-slate-200 bg-white/95 p-8 shadow-sm">
        <div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">School Details</p>
            <h1 class="mt-3 text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">{{ schoolLabel }}</h1>
            <p class="mt-4 text-base leading-7 text-slate-600">30-day local snapshot history with explicit metric windows and current activity.</p>
            <p class="mt-3 text-sm text-slate-500">Last successful sync: {{ lastSuccessfulSyncLabel }}</p>
            <p class="mt-1 text-sm text-slate-500">Last attempted sync: {{ lastAttemptedSyncLabel }}</p>
            <p v-if="lastAttemptStatusLabel" class="mt-1 text-xs" :class="syncMetadata?.lastAttemptedSync?.status === 'failed' ? 'text-rose-500' : 'text-slate-400'">
              Last attempt status: {{ lastAttemptStatusLabel }}
            </p>
            <p v-if="latestSnapshot?.snapshotDate" class="mt-1 text-xs text-slate-400">Latest successful snapshot date: {{ latestSnapshot.snapshotDate }}</p>
            <p v-if="snapshotCount === 1" class="mt-2 text-xs text-amber-600">Only one local snapshot is available right now, so the charts will show a single point instead of a trend line.</p>
            <p class="mt-2 max-w-2xl text-xs text-slate-500">Nightly success uses Coursedog's upstream 48-hour health window. Realtime success and active users use the last 24 hours. Merge errors reflect the current open-error count on synced days; backfilled history uses a failed-merge proxy.</p>
          </div>
          <div class="flex flex-col items-end gap-2">
            <button
              @click="handleSchoolSync"
              :disabled="isSyncing || isBackfilling"
              class="rounded-full bg-slate-900 px-5 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span v-if="isSyncing">Syncing {{ school }}…</span>
              <span v-else>Sync This School</span>
            </button>
            <button
              @click="handleHistoryBackfill"
              :disabled="isBackfilling || isSyncing"
              class="rounded-full border border-slate-300 bg-white px-5 py-2.5 text-sm font-semibold text-slate-900 shadow-sm transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              <span v-if="isBackfilling">Backfilling {{ backfillDays }} Days…</span>
              <span v-else>Backfill Past {{ backfillDays }} Days</span>
            </button>
            <div class="flex items-center gap-3">
              <label for="backfill-days" class="text-xs font-medium uppercase tracking-[0.18em] text-slate-500">Backfill Days</label>
              <select id="backfill-days" v-model.number="backfillDays" :disabled="isBackfilling || isSyncing" class="rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-900 focus:border-slate-400 focus:outline-none disabled:opacity-50">
                <option :value="7">7</option>
                <option :value="14">14</option>
                <option :value="30">30</option>
              </select>
            </div>
            <p v-if="syncResult" class="text-xs text-emerald-600">
              <span v-if="syncResult.status === 'completed'">✓ {{ school }} synced in {{ syncResult.totalSec }}s</span>
              <span v-else-if="syncResult.status === 'queued'">Queued school sync</span>
              <span v-else-if="syncResult.status === 'running'">School sync in progress</span>
            </p>
            <p v-if="syncResult?.errors?.length" class="max-w-sm text-right text-xs text-amber-500">
              {{ syncResult.errors.slice(0, 2).join(' | ') }}
            </p>
            <p v-if="syncError" class="text-xs text-rose-500">{{ syncError }}</p>
            <p v-if="backfillResult" class="text-xs text-blue-600">
              <span v-if="backfillResult.status === 'completed'">✓ Backfilled {{ backfillResult.schoolsProcessed }} days in {{ backfillResult.totalSec }}s</span>
              <span v-else-if="backfillResult.status === 'queued'">Queued {{ backfillDays }}-day backfill</span>
              <span v-else-if="backfillResult.status === 'running'">Historical backfill in progress</span>
            </p>
            <p v-if="backfillResult?.errors?.length" class="max-w-sm text-right text-xs text-amber-500">
              {{ backfillResult.errors.slice(0, 2).join(' | ') }}
            </p>
            <p v-if="backfillError" class="text-xs text-rose-500">{{ backfillError }}</p>
          </div>
        </div>
      </div>
      <div v-if="loading" class="rounded-[28px] border border-slate-200 bg-white p-8 text-slate-700 shadow-sm">Loading...</div>
      <div v-else-if="error" class="rounded-[28px] border border-rose-200 bg-rose-50 p-8 text-rose-700 shadow-sm">{{ error }}</div>
      <div v-else class="space-y-8">
        <div class="grid grid-cols-1 gap-6 md:grid-cols-2">
        <Card subtitle="Performance" title="Nightly Merge Activity (48h Upstream Window)">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="bar" :options="nightlySuccessChartOptions" :series="nightlySuccessChartSeries" />
          </div>
        </Card>
        <Card subtitle="Performance" title="Realtime Merge Activity (Last 24h)">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="bar" :options="realtimeSuccessChartOptions" :series="realtimeSuccessChartSeries" />
          </div>
        </Card>
        <Card subtitle="Issues" title="Open Merge Errors Across Local Snapshots">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="line" :options="mergeErrorsChartOptions" :series="mergeErrorsChartSeries" />
          </div>
        </Card>
        <Card subtitle="Activity" title="Distinct Active Users (Last 24h)">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="line" :options="activeUsersChartOptions" :series="activeUsersChartSeries" />
          </div>
        </Card>
        <Card subtitle="Users" title="Active Users in Last 24h">
          <p class="mt-2 text-3xl font-semibold text-slate-950">{{ activeUsers?.count }}</p>
          <p v-if="activeUsers?.error" class="mt-2 text-xs text-amber-500">{{ activeUsers.error }}</p>
          <div class="mt-6 space-y-2">
            <div v-for="user in activeUsers?.users" :key="user" class="flex items-center gap-3 rounded-lg bg-slate-50 p-3">
              <div class="h-2 w-2 rounded-full bg-emerald-500"></div>
              <span class="text-sm text-slate-900">{{ user }}</span>
            </div>
            <div v-if="!activeUsers?.users?.length" class="text-sm text-slate-500">No active users</div>
          </div>
        </Card>
        <Card subtitle="Issues" title="Recent Failed Merges">
          <div class="mt-6 space-y-2">
            <div
              v-for="merge in (latestSnapshot?.recentFailedMerges as FailedMerge[] | undefined)"
              :key="merge.id"
              class="flex items-start gap-3 rounded-lg bg-slate-50 p-3"
            >
              <div class="mt-1 h-2 w-2 rounded-full bg-rose-500 flex-shrink-0"></div>
              <div class="flex flex-col text-sm">
                <span class="font-medium text-slate-900">{{ merge.id }}</span>
                <span v-if="merge.type" class="text-xs text-slate-500">Type: {{ merge.type }}</span>
                <span v-if="merge.scheduleType" class="text-xs text-slate-500 capitalize">{{ merge.scheduleType }} merge</span>
                <span v-if="merge.timestampEnd" class="text-xs text-slate-400">
                  {{ new Date(merge.timestampEnd).toLocaleString() }}
                </span>
              </div>
            </div>
            <div v-if="!latestSnapshot?.recentFailedMerges?.length" class="text-sm text-slate-500">No recent failed merges</div>
          </div>
        </Card>
        </div>
      </div>
    </div>
  </div>
</template>
