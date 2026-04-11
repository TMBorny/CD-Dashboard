<script setup lang="ts">
import { ref, computed } from 'vue';
import { useRouter } from 'vue-router';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { getClientHealth, getClientHealthHistory, getSyncStatus, triggerSync } from '@/api';
import type { ClientHealthSnapshot } from '@/types/clientHealth';
import { useChartOptions } from '@/composables/useChartOptions';
import { formatSchoolLabel } from '@/utils/schoolNames';
import ClientHealthSummaryCards from './ClientHealthSummaryCards.vue';
import ClientHealthTable from './ClientHealthTable.vue';
import VueApexCharts from 'vue3-apexcharts';

const router = useRouter();
const queryClient = useQueryClient();
const selectedSchool = ref<string>('all');

// Sync state
const isSyncing = ref(false);
const syncJobId = ref<string | null>(null);
const syncResult = ref<{
  status?: string;
  totalSec?: number;
  schoolsProcessed?: number;
  totalSchools?: number;
  errors?: string[];
  alreadyRunning?: boolean;
  limit?: number;
} | null>(null);
const syncError = ref<string | null>(null);

const { data: healthResponse, isLoading: isLoadingHealth, error: healthError } = useQuery({
  queryKey: ['clientHealth'],
  queryFn: () => getClientHealth().then((res) => res.data),
});

const { data: historyResponse, isLoading: isLoadingHistory, error: historyError } = useQuery({
  queryKey: ['clientHealthHistory', { days: 30 }],
  queryFn: () => getClientHealthHistory({ days: 30 }).then((res) => res.data.snapshots),
});

const loading = computed(() => isLoadingHealth.value || isLoadingHistory.value);
const error = computed(() => healthError.value || historyError.value ? 'Failed to load client health data' : null);

const data = computed(() => healthResponse.value);
const history = computed(() => historyResponse.value ?? []);

const schoolOptions = computed(() => {
  return data.value ? [{ value: 'all', label: 'All schools' }, ...data.value.schools.map((school: ClientHealthSnapshot) => ({ value: school.school, label: formatSchoolLabel(school.school) }))] : [{ value: 'all', label: 'All schools' }];
});

const filteredHistory = computed(() => {
  if (selectedSchool.value === 'all') {
    return history.value;
  }
  return history.value.filter((snapshot: ClientHealthSnapshot) => snapshot.school === selectedSchool.value);
});

const mergeErrorsChartSeries = computed(() => {
  const grouped = new Map<string, number>();
  filteredHistory.value.forEach((snapshot: ClientHealthSnapshot) => {
    const count = grouped.get(snapshot.snapshotDate) ?? 0;
    grouped.set(snapshot.snapshotDate, count + snapshot.mergeErrorsCount);
  });
  const sortedDates = Array.from(grouped.keys()).sort();
  return [
    {
      name: selectedSchool.value === 'all' ? 'Total Merge Errors' : `Errors for ${filteredHistory.value[0] ? formatSchoolLabel(filteredHistory.value[0].school) : 'Selected School'}`,
      data: sortedDates.map((date) => grouped.get(date) ?? 0),
    },
  ];
});

const mergeErrorsChartOptions = computed(() => useChartOptions({
  colors: ['#f14a4c'],
  categories: filteredHistory.value.filter((_: ClientHealthSnapshot, i: number) => i % Math.ceil(filteredHistory.value.length / 10) === 0).map((s: ClientHealthSnapshot) => s.snapshotDate),
}));

const handleRowClick = (school: ClientHealthSnapshot) => {
  router.push({ name: 'AdminClientHealthDetail', params: { school: school.school } });
};

async function pollSyncJob(jobId: string) {
  syncJobId.value = jobId;

  while (true) {
    const status = await getSyncStatus(jobId);
    syncResult.value = {
      status: status.status,
      totalSec: status.timing?.totalSec,
      schoolsProcessed: status.schoolsProcessed,
      totalSchools: status.totalSchools,
      errors: status.errors,
      alreadyRunning: status.alreadyRunning,
      limit: status.limit,
    };

    if (status.status === 'completed') {
      await queryClient.invalidateQueries({ queryKey: ['clientHealth'] });
      await queryClient.invalidateQueries({ queryKey: ['clientHealthHistory'] });
      break;
    }

    if (status.status === 'failed') {
      syncError.value = status.error || 'Sync failed';
      break;
    }

    await new Promise((resolve) => window.setTimeout(resolve, 1500));
  }
}

async function handleSync() {
  isSyncing.value = true;
  syncError.value = null;
  syncResult.value = null;

  try {
    const result = await triggerSync();
    await pollSyncJob(result.jobId);
  } catch (e: any) {
    syncError.value = e?.response?.data?.detail || e?.message || 'Sync failed';
  } finally {
    isSyncing.value = false;
  }
}
</script>

<template>
  <div class="w-full bg-slate-50 text-slate-900">
    <div class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      <div class="mb-8 rounded-[28px] border border-slate-200 bg-white/95 p-8 shadow-sm">
        <div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Client Health</p>
            <h1 class="mt-3 text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">Client Health Dashboard</h1>
            <p class="mt-4 max-w-3xl text-base leading-7 text-slate-600">Track nightly success from the upstream 48-hour health window, realtime success and active users from the last 24 hours, and current open merge errors from the latest local sync.</p>
          </div>
          <div class="flex flex-col items-end gap-2">
            <button
              @click="handleSync"
              :disabled="isSyncing"
              class="rounded-full bg-slate-900 px-5 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <span v-if="isSyncing">Syncing…</span>
              <span v-else>⟳ Sync Now</span>
            </button>
            <p v-if="isSyncing && syncResult?.totalSchools" class="text-xs text-slate-500">
              {{ syncResult.schoolsProcessed ?? 0 }} / {{ syncResult.totalSchools }} schools processed
            </p>
            <p v-if="syncResult?.limit" class="text-xs text-slate-400">
              Dev mode: bulk sync is temporarily limited to {{ syncResult.limit }} schools
            </p>
            <p v-if="syncResult" class="text-xs text-emerald-600">
              <span v-if="syncResult.status === 'completed'">✓ {{ syncResult.schoolsProcessed }} schools synced in {{ syncResult.totalSec }}s</span>
              <span v-else-if="syncResult.status === 'queued'">Queued sync job{{ syncResult.alreadyRunning ? ' (joining active run)' : '' }}</span>
              <span v-else-if="syncResult.status === 'running'">Sync job in progress</span>
            </p>
            <p v-if="syncResult?.errors?.length" class="text-xs text-amber-600">
              ⚠ {{ syncResult.errors.length }} errors
            </p>
            <div v-if="syncResult?.errors?.length" class="max-w-sm space-y-1 text-right">
              <p v-for="message in syncResult.errors.slice(0, 3)" :key="message" class="text-xs text-amber-500">
                {{ message }}
              </p>
            </div>
            <p v-if="syncError" class="text-xs text-rose-500">
              ✗ {{ syncError }}
            </p>
            <p v-if="data?.snapshotDate" class="text-xs text-slate-400">
              Last sync: {{ data.snapshotDate }}
            </p>
          </div>
        </div>
      </div>

      <div v-if="loading" class="rounded-[28px] border border-slate-200 bg-white p-8 text-slate-700 shadow-sm">Loading client health data…</div>
      <div v-else-if="error" class="rounded-[28px] border border-rose-200 bg-rose-50 p-8 text-rose-700 shadow-sm">{{ error }}</div>
      <div v-else-if="!data?.schools?.length" class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
        <div class="text-center">
          <p class="text-lg font-semibold text-slate-700">No data yet</p>
          <p class="mt-2 text-sm text-slate-500">Click "Sync Now" to fetch health data for all schools from Coursedog.</p>
        </div>
      </div>
      <div v-else class="space-y-6">
        <ClientHealthSummaryCards :schools="data!.schools" />
        <div class="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
          <div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div>
              <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Merge Analysis</p>
              <h2 class="mt-2 text-xl font-semibold text-slate-950">Open merge errors across local snapshots</h2>
              <p class="mt-2 max-w-xl text-sm leading-6 text-slate-600">The latest sync stores the current open-error count from Integrations Hub. Historical backfills approximate older points from failed merges on that day.</p>
            </div>
            <div class="flex items-center gap-3">
              <label class="text-sm font-medium text-slate-700">School</label>
              <select v-model="selectedSchool" class="rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-900 focus:border-slate-400 focus:outline-none">
                <option v-for="option in schoolOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
              </select>
            </div>
          </div>
          <div class="mt-6 min-h-[320px] rounded-[24px] border border-slate-200 bg-slate-50 p-6">
            <VueApexCharts type="line" :options="mergeErrorsChartOptions" :series="mergeErrorsChartSeries" />
          </div>
        </div>
        <ClientHealthTable :schools="data!.schools" @row-click="handleRowClick" />
      </div>
    </div>
  </div>
</template>
