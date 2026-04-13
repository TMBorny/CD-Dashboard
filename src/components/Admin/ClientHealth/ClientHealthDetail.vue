<script setup lang="ts">
import { computed } from 'vue';
import { useRoute } from 'vue-router';
import { useQuery } from '@tanstack/vue-query';
import { getClientHealthHistory, getClientHealthActiveUsers, getClientHealthSyncMetadata } from '@/api';
import type { FailedMerge } from '@/types/clientHealth';
import VueApexCharts from 'vue3-apexcharts';
import Card from '@/components/ui/Card.vue';
import { useChartOptions, useStackedBarChartOptions } from '@/composables/useChartOptions';
import { formatSchoolLabel } from '@/utils/schoolNames';

const route = useRoute();
const school = computed(() => String(route.params.school ?? ''));
const coursedogBaseUrl = (import.meta.env.VITE_COURSEDOG_PRD_URL?.trim() || 'https://app.coursedog.com').replace(/\/+$/, '');
const integrationHubUrl = computed(() => `${coursedogBaseUrl}/#/int/${school.value}`);
const mergeReportsUrl = computed(() => `${coursedogBaseUrl}/#/int/${school.value}/merge-history`);

const { data: history, isLoading: isLoadingHistory, error: historyError } = useQuery({
  queryKey: computed(() => ['clientHealthHistory', school.value, { days: 30 }]),
  queryFn: () => getClientHealthHistory({ school: school.value, days: 30 }).then((res) => res.data),
  enabled: computed(() => school.value.length > 0),
});

const { data: activeUsers, isLoading: isLoadingUsers, error: usersError } = useQuery({
  queryKey: computed(() => ['clientHealthActiveUsers', school.value]),
  queryFn: () => getClientHealthActiveUsers({ school: school.value }).then((res) => res.data),
  enabled: computed(() => school.value.length > 0),
});

const { data: syncMetadata, isLoading: isLoadingSyncMetadata } = useQuery({
  queryKey: computed(() => ['clientHealthSyncMetadata', school.value]),
  queryFn: () => getClientHealthSyncMetadata({ school: school.value }).then((res) => res.data),
  enabled: computed(() => school.value.length > 0),
});

const loading = computed(() => isLoadingHistory.value || isLoadingUsers.value || isLoadingSyncMetadata.value);
const error = computed(() => historyError.value || usersError.value ? 'Failed to load data' : null);
const snapshotCount = computed(() => history.value?.snapshots?.length ?? 0);
const latestSnapshot = computed(() => {
  const snapshots = history.value?.snapshots ?? [];
  return snapshots.length > 0 ? snapshots[snapshots.length - 1] : null;
});
const schoolLabel = computed(() => formatSchoolLabel(school.value, latestSnapshot.value?.displayName));
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

const nightlyDurationChartSeries = computed(() => {
  if (!history.value) return [];
  const data = history.value.snapshots.map((s: any) => parseFloat(((s.merges.nightly.mergeTimeMs || 0) / 3600000).toFixed(2)));
  return [{ name: 'Nightly Duration (hours)', data }];
});

const nightlyDurationChartOptions = computed(() => useChartOptions({
  colors: ['#8b5cf6'],
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
  const data = history.value.snapshots.map((s: any) => s.mergeErrorsCount ?? null);
  return [{ name: 'Open Merge Errors per Snapshot', data }];
});

const mergeErrorsChartOptions = computed(() => ({
  ...useChartOptions({
    colors: ['#ffcd56'],
    categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
  }),
  yaxis: {
    labels: {
      style: { colors: '#64748b', fontSize: '12px' },
      formatter: (value: number) => `${Math.round(value)}`,
    },
  },
  tooltip: {
    theme: 'light',
    y: {
      formatter: (value: number | undefined) =>
        typeof value === 'number' ? `${Math.round(value)}` : '',
    },
  },
}));

const activeUsersChartSeries = computed(() => {
  if (!history.value) return [];
  const data = history.value.snapshots.map((s: any) => s.activeUsers24h);
  return [{ name: 'Distinct Active Users (24h)', data }];
});

const activeUsersChartOptions = computed(() => ({
  ...useChartOptions({
    colors: ['#36a2eb'],
    categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
  }),
  yaxis: {
    labels: {
      style: { colors: '#64748b', fontSize: '12px' },
      formatter: (value: number) => `${Math.round(value)}`,
    },
  },
  tooltip: {
    theme: 'light',
    y: {
      formatter: (value: number | undefined) =>
        typeof value === 'number' ? `${Math.round(value)}` : '',
    },
  },
}));
</script>

<template>
  <div class="min-h-screen bg-slate-50">
    <div class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      <div class="mb-8 rounded-[28px] border border-slate-200 bg-white/95 p-8 shadow-sm">
        <div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">School Details</p>
            <div class="mt-4 flex flex-wrap items-center gap-3">
              <router-link
                :to="{ name: 'AdminClientHealth' }"
                class="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:bg-white hover:text-slate-950"
              >
                ← Back to Client Health
              </router-link>
              <a
                :href="integrationHubUrl"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
              >
                Integration Hub ↗
              </a>
              <a
                :href="mergeReportsUrl"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
              >
                Merge Reports ↗
              </a>
            </div>
            <h1 class="mt-3 text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">{{ schoolLabel }}</h1>
            <p class="mt-4 text-base leading-7 text-slate-600">30-day local snapshot history with explicit metric windows and current activity.</p>
            <p class="mt-3 text-sm text-slate-500">Last successful sync: {{ lastSuccessfulSyncLabel }}</p>
            <p class="mt-1 text-sm text-slate-500">Last attempted sync: {{ lastAttemptedSyncLabel }}</p>
            <p v-if="lastAttemptStatusLabel" class="mt-1 text-xs" :class="syncMetadata?.lastAttemptedSync?.status === 'failed' ? 'text-rose-500' : 'text-slate-400'">
              Last attempt status: {{ lastAttemptStatusLabel }}
            </p>
            <p v-if="latestSnapshot?.snapshotDate" class="mt-1 text-xs text-slate-400">Latest successful snapshot date: {{ latestSnapshot.snapshotDate }}</p>
            <p v-if="snapshotCount === 1" class="mt-2 text-xs text-amber-600">Only one local snapshot is available right now, so the charts will show a single point instead of a trend line.</p>
            <p class="mt-2 max-w-2xl text-xs text-slate-500">Nightly success uses Coursedog's upstream 48-hour health window. Realtime success and active users use the last 24 hours. Open merge errors are shown only for days where that count was captured directly during a local sync.</p>
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
        <Card subtitle="Duration" title="Nightly Merge Duration Trend">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="line" :options="nightlyDurationChartOptions" :series="nightlyDurationChartSeries" />
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
        <Card subtitle="Activity" title="Distinct Active Users (per day)">
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
