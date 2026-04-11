<script setup lang="ts">
import { ref, computed, watch } from 'vue';
import { useRouter } from 'vue-router';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { getClientHealth, getClientHealthHistory, getSyncStatus, triggerSync } from '@/api';
import type { ClientHealthSnapshot } from '@/types/clientHealth';
import { useChartOptions, useStackedBarChartOptions } from '@/composables/useChartOptions';
import { formatSchoolLabel } from '@/utils/schoolNames';
import ClientHealthSummaryCards from './ClientHealthSummaryCards.vue';
import ClientHealthTable from './ClientHealthTable.vue';
import VueApexCharts from 'vue3-apexcharts';

const router = useRouter();
const queryClient = useQueryClient();
const selectedSchool = ref<string>('all');
const selectedSis = ref<string>('all');

// Reset school filter when SIS changes (to avoid stale cross-filter state)
watch(selectedSis, () => { selectedSchool.value = 'all'; });

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
  const base = data.value?.schools ?? [];
  const sisFiltered = selectedSis.value === 'all'
    ? base
    : base.filter((s: ClientHealthSnapshot) => s.sisPlatform === selectedSis.value);
  return [
    { value: 'all', label: 'All schools' },
    ...sisFiltered.map((school: ClientHealthSnapshot) => ({ value: school.school, label: formatSchoolLabel(school.school) })),
  ];
});

// Schools filtered by SIS (and optionally school) for the table + summary cards
const filteredSchools = computed(() => {
  const base = data.value?.schools ?? [];
  return base.filter((s: ClientHealthSnapshot) => {
    const sisMatch = selectedSis.value === 'all' || s.sisPlatform === selectedSis.value;
    const schoolMatch = selectedSchool.value === 'all' || s.school === selectedSchool.value;
    return sisMatch && schoolMatch;
  });
});

// Unique SIS platforms from history for the filter dropdown
const sisOptions = computed(() => {
  const platforms = new Set<string>();
  history.value.forEach((s: ClientHealthSnapshot) => {
    if (s.sisPlatform) platforms.add(s.sisPlatform);
  });
  return [
    { value: 'all', label: 'All SIS Platforms' },
    ...Array.from(platforms).sort().map((p) => ({ value: p, label: p })),
  ];
});

const filteredHistory = computed(() => {
  return history.value.filter((snapshot: ClientHealthSnapshot) => {
    const schoolMatch = selectedSchool.value === 'all' || snapshot.school === selectedSchool.value;
    const sisMatch = selectedSis.value === 'all' || snapshot.sisPlatform === selectedSis.value;
    return schoolMatch && sisMatch;
  });
});

// Stacked area chart: nightly success breakdown over time
const nightlyBreakdownSeries = computed(() => {
  // Aggregate counts by date across all filtered schools
  const byDate = new Map<string, { succeeded: number; issues: number; noData: number; failed: number }>();
  filteredHistory.value.forEach((s: ClientHealthSnapshot) => {
    const d = s.snapshotDate;
    const existing = byDate.get(d) ?? { succeeded: 0, issues: 0, noData: 0, failed: 0 };
    const n = s.merges.nightly;
    byDate.set(d, {
      succeeded: existing.succeeded + n.succeeded,
      issues: existing.issues + (n.finishedWithIssues || 0),
      noData: existing.noData + (n.noData || 0),
      failed: existing.failed + n.failed,
    });
  });
  const sortedDates = Array.from(byDate.keys()).sort();
  return [
    { name: 'Success',            data: sortedDates.map((d) => byDate.get(d)!.succeeded) },
    { name: 'Finished w/ Issues', data: sortedDates.map((d) => byDate.get(d)!.issues) },
    { name: 'No Data',            data: sortedDates.map((d) => byDate.get(d)!.noData) },
    { name: 'Failed',             data: sortedDates.map((d) => byDate.get(d)!.failed) },
  ];
});

const nightlyBreakdownDates = computed(() => {
  const dates = new Set<string>();
  filteredHistory.value.forEach((s: ClientHealthSnapshot) => dates.add(s.snapshotDate));
  return Array.from(dates).sort();
});

const nightlyBreakdownOptions = computed(() => useStackedBarChartOptions({
  categories: nightlyBreakdownDates.value,
  colors: ['#10b981', '#f59e0b', '#94a3b8', '#ef4444'],
}));

// Helper: aggregate a single numeric field per date across filtered history
const aggregateByDate = (field: (s: ClientHealthSnapshot) => number) => {
  const byDate = new Map<string, number>();
  filteredHistory.value.forEach((s: ClientHealthSnapshot) => {
    byDate.set(s.snapshotDate, (byDate.get(s.snapshotDate) ?? 0) + field(s));
  });
  return Array.from(byDate.entries()).sort(([a], [b]) => a.localeCompare(b));
};

const chartDates = computed(() => nightlyBreakdownDates.value);

// Open merge errors over time
const mergeErrorsSeries = computed(() => {
  const entries = aggregateByDate((s) => s.mergeErrorsCount);
  return [{ name: 'Open Merge Errors', data: entries.map(([, v]) => v) }];
});
const mergeErrorsOptions = computed(() => useChartOptions({
  colors: ['#ef4444'],
  categories: chartDates.value,
}));

// Active users over time
const activeUsersSeries = computed(() => {
  const entries = aggregateByDate((s) => s.activeUsers24h);
  return [{ name: 'Active Users (24h)', data: entries.map(([, v]) => v) }];
});
const activeUsersOptions = computed(() => useChartOptions({
  colors: ['#36a2eb'],
  categories: chartDates.value,
}));

// Schools at risk per day (health score < 65)
const atRiskSeries = computed(() => {
  const byDate = new Map<string, number>();
  filteredHistory.value.forEach((s: ClientHealthSnapshot) => {
    const n = s.merges.nightly;
    const validTotal = n.total - (n.noData || 0);
    const rate = validTotal > 0 ? ((n.succeeded + (n.finishedWithIssues || 0) * 0.5) / validTotal) * 100 : 0;
    const score = Math.max(0, Math.min(100, rate - Math.min(20, Math.log2(s.mergeErrorsCount + 1) * 4)));
    if (score < 65) byDate.set(s.snapshotDate, (byDate.get(s.snapshotDate) ?? 0) + 1);
  });
  return [{ name: 'Schools at Risk', data: chartDates.value.map((d) => byDate.get(d) ?? 0) }];
});
const atRiskOptions = computed(() => useChartOptions({
  colors: ['#f59e0b'],
  categories: chartDates.value,
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
        <ClientHealthSummaryCards :schools="filteredSchools" />
        <div class="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
          <div class="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
            <div>
              <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Fleet Health</p>
              <h2 class="mt-2 text-xl font-semibold text-slate-950">Nightly merge outcomes — 30-day trend</h2>
              <p class="mt-2 max-w-xl text-sm leading-6 text-slate-600">Aggregate nightly merge results across all schools in the selected filter, broken down by outcome. Backfilled from merge history; current-day data uses the integrations health endpoint.</p>
            </div>
            <div class="flex flex-wrap items-center gap-3">
              <div class="flex items-center gap-2">
                <label class="text-sm font-medium text-slate-700">SIS</label>
                <select v-model="selectedSis" class="rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-900 focus:border-slate-400 focus:outline-none">
                  <option v-for="option in sisOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                </select>
              </div>
              <div class="flex items-center gap-2">
                <label class="text-sm font-medium text-slate-700">School</label>
                <select v-model="selectedSchool" class="rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-900 focus:border-slate-400 focus:outline-none">
                  <option v-for="option in schoolOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                </select>
              </div>
            </div>
          </div>
          <div class="mt-6 min-h-[320px] rounded-[24px] border border-slate-200 bg-slate-50 p-6">
            <VueApexCharts type="bar" :options="nightlyBreakdownOptions" :series="nightlyBreakdownSeries" />
          </div>
        </div>

        <!-- Secondary trend charts -->
        <div class="grid grid-cols-1 gap-6 md:grid-cols-3">
          <div class="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Errors</p>
            <h3 class="mt-2 text-base font-semibold text-slate-950">Open Merge Errors</h3>
            <p class="mt-1 text-xs text-slate-500">Total open errors captured at each sync, aggregated across filtered schools.</p>
            <div class="mt-4 min-h-[200px]">
              <VueApexCharts type="line" :options="mergeErrorsOptions" :series="mergeErrorsSeries" />
            </div>
          </div>
          <div class="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Engagement</p>
            <h3 class="mt-2 text-base font-semibold text-slate-950">Active Users (24h)</h3>
            <p class="mt-1 text-xs text-slate-500">Distinct active users per snapshot day — drops here correlate with integration impact.</p>
            <div class="mt-4 min-h-[200px]">
              <VueApexCharts type="line" :options="activeUsersOptions" :series="activeUsersSeries" />
            </div>
          </div>
          <div class="rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Fleet Risk</p>
            <h3 class="mt-2 text-base font-semibold text-slate-950">Schools at Risk</h3>
            <p class="mt-1 text-xs text-slate-500">Count of schools with a computed health score below 65 (Warning or At Risk threshold).</p>
            <div class="mt-4 min-h-[200px]">
              <VueApexCharts type="line" :options="atRiskOptions" :series="atRiskSeries" />
            </div>
          </div>
        </div>

        <ClientHealthTable :schools="filteredSchools" @row-click="handleRowClick" />
      </div>
    </div>
  </div>
</template>
