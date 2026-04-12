<script setup lang="ts">
import { ref, computed, watch } from 'vue';
import { useRouter } from 'vue-router';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { getClientHealth, getClientHealthHistory, getSyncStatus, triggerHistoryBackfill, triggerSync } from '@/api';
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
const backfillStartDate = ref('2026-01-01');

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
const isBackfilling = ref(false);
const backfillResult = ref<{
  status?: string;
  totalSec?: number;
  schoolsProcessed?: number;
  totalSchools?: number;
  errors?: string[];
  alreadyRunning?: boolean;
  scope?: string;
} | null>(null);
const backfillError = ref<string | null>(null);

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
    ...sisFiltered.map((school: ClientHealthSnapshot) => ({ value: school.school, label: formatSchoolLabel(school.school, school.displayName) })),
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

// Stacked bar chart: nightly success breakdown as % rates over time
// Using percentages so days with different numbers of schools contributing
// are directly comparable (4/10 had 508 schools, other days have ~7-11).
const nightlyBreakdownSeries = computed(() => {
  const byDate = new Map<string, { succeeded: number; issues: number; noData: number; failed: number; total: number }>();
  filteredHistory.value.forEach((s: ClientHealthSnapshot) => {
    const d = s.snapshotDate;
    const existing = byDate.get(d) ?? { succeeded: 0, issues: 0, noData: 0, failed: 0, total: 0 };
    const n = s.merges.nightly;
    byDate.set(d, {
      succeeded: existing.succeeded + n.succeeded,
      issues:    existing.issues    + (n.finishedWithIssues || 0),
      noData:    existing.noData    + (n.noData || 0),
      failed:    existing.failed    + n.failed,
      total:     existing.total     + n.total,
    });
  });
  const sortedDates = Array.from(byDate.keys()).sort();
  const pct = (val: number, total: number) => total > 0 ? parseFloat(((val / total) * 100).toFixed(1)) : 0;
  return [
    { name: 'Success',            data: sortedDates.map((d) => pct(byDate.get(d)!.succeeded, byDate.get(d)!.total)) },
    { name: 'Finished w/ Issues', data: sortedDates.map((d) => pct(byDate.get(d)!.issues,    byDate.get(d)!.total)) },
    { name: 'No Data',            data: sortedDates.map((d) => pct(byDate.get(d)!.noData,    byDate.get(d)!.total)) },
    { name: 'Failed',             data: sortedDates.map((d) => pct(byDate.get(d)!.failed,    byDate.get(d)!.total)) },
  ];
});

const nightlyBreakdownDates = computed(() => {
  const dates = new Set<string>();
  filteredHistory.value.forEach((s: ClientHealthSnapshot) => dates.add(s.snapshotDate));
  return Array.from(dates).sort();
});

const nightlyBreakdownOptions = computed(() => ({
  ...useStackedBarChartOptions({
    categories: nightlyBreakdownDates.value,
    colors: ['#10b981', '#f59e0b', '#94a3b8', '#ef4444'],
  }),
  yaxis: {
    min: 0,
    max: 100,
    labels: {
      style: { colors: '#64748b', fontSize: '12px' },
      formatter: (v: number) => `${v}%`,
    },
  },
  tooltip: {
    theme: 'light',
    y: { formatter: (v: number) => `${v}%` },
  },
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
      scope: status.scope,
    };

    if (status.status === 'completed') {
      await queryClient.invalidateQueries({ queryKey: ['clientHealth'] });
      await queryClient.invalidateQueries({ queryKey: ['clientHealthHistory'] });
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
    const result = await triggerHistoryBackfill({ startDate: backfillStartDate.value });
    await pollBackfillJob(result.jobId);
  } catch (e: any) {
    backfillError.value = e?.response?.data?.detail || e?.message || 'Backfill failed';
  } finally {
    isBackfilling.value = false;
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
              :disabled="isSyncing || isBackfilling"
              class="rounded-full bg-slate-900 px-5 py-2.5 text-sm font-semibold text-white shadow-sm transition hover:bg-slate-700 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <span v-if="isSyncing">Syncing…</span>
              <span v-else>⟳ Sync Now</span>
            </button>
            <button
              @click="handleHistoryBackfill"
              :disabled="isSyncing || isBackfilling"
              class="rounded-full border border-slate-300 bg-white px-5 py-2.5 text-sm font-semibold text-slate-900 shadow-sm transition hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <span v-if="isBackfilling">Backfilling From {{ backfillStartDate }}…</span>
              <span v-else>Backfill All Schools From {{ backfillStartDate }}</span>
            </button>
            <div class="flex items-center gap-3">
              <label for="bulk-backfill-start-date" class="text-xs font-medium uppercase tracking-[0.18em] text-slate-500">Start Date</label>
              <input
                id="bulk-backfill-start-date"
                v-model="backfillStartDate"
                type="date"
                :disabled="isSyncing || isBackfilling"
                class="rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-900 focus:border-slate-400 focus:outline-none disabled:opacity-50"
              />
            </div>
            <p v-if="isSyncing && syncResult?.totalSchools" class="text-xs text-slate-500">
              {{ syncResult.schoolsProcessed ?? 0 }} / {{ syncResult.totalSchools }} schools processed
            </p>
            <p v-if="isBackfilling && backfillResult?.totalSchools" class="text-xs text-slate-500">
              {{ backfillResult.schoolsProcessed ?? 0 }} / {{ backfillResult.totalSchools }} snapshots processed
            </p>
            <p v-if="syncResult" class="text-xs text-emerald-600">
              <span v-if="syncResult.status === 'completed'">✓ {{ syncResult.schoolsProcessed }} schools synced in {{ syncResult.totalSec }}s</span>
              <span v-else-if="syncResult.status === 'queued'">Queued sync job{{ syncResult.alreadyRunning ? ' (joining active run)' : '' }}</span>
              <span v-else-if="syncResult.status === 'running'">Sync job in progress</span>
            </p>
            <p v-if="backfillResult" class="text-xs text-blue-600">
              <span v-if="backfillResult.status === 'completed'">✓ Backfilled {{ backfillResult.schoolsProcessed }} snapshots in {{ backfillResult.totalSec }}s</span>
              <span v-else-if="backfillResult.status === 'queued'">Queued bulk backfill from {{ backfillStartDate }}</span>
              <span v-else-if="backfillResult.status === 'running'">Bulk historical backfill in progress</span>
            </p>
            <p v-if="syncResult?.errors?.length" class="text-xs text-amber-600">
              ⚠ {{ syncResult.errors.length }} errors
            </p>
            <div v-if="syncResult?.errors?.length" class="max-w-sm space-y-1 text-right">
              <p v-for="message in syncResult.errors.slice(0, 3)" :key="message" class="text-xs text-amber-500">
                {{ message }}
              </p>
            </div>
            <div v-if="backfillResult?.errors?.length" class="max-w-sm space-y-1 text-right">
              <p v-for="message in backfillResult.errors.slice(0, 3)" :key="message" class="text-xs text-amber-500">
                {{ message }}
              </p>
            </div>
            <p v-if="syncError" class="text-xs text-rose-500">
              ✗ {{ syncError }}
            </p>
            <p v-if="backfillError" class="text-xs text-rose-500">
              ✗ {{ backfillError }}
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
        <!-- Filters Section -->
        <div class="flex flex-col gap-4 md:flex-row md:items-end md:justify-between py-2">
          <div>
            <h2 class="text-lg font-semibold text-slate-950">Trend Analytics</h2>
            <p class="mt-1 text-sm text-slate-500">30-day historical trends for the selected cohort.</p>
          </div>
          <div class="flex flex-wrap items-center gap-3">
            <div class="flex items-center gap-2">
              <label class="text-sm font-medium text-slate-700">SIS</label>
              <select v-model="selectedSis" class="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                <option v-for="option in sisOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
              </select>
            </div>
            <div class="flex items-center gap-2">
              <label class="text-sm font-medium text-slate-700">School</label>
              <select v-model="selectedSchool" class="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                <option v-for="option in schoolOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
              </select>
            </div>
          </div>
        </div>

        <!-- 4-Up Chart Grid -->
        <div class="grid grid-cols-1 gap-6 xl:grid-cols-2">
          <!-- Chart 1: Merge Outcomes -->
          <div class="flex flex-col rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Fleet Health</p>
            <h3 class="mt-2 text-base font-semibold text-slate-950">Nightly merge outcomes</h3>
            <p class="mt-1 text-xs text-slate-500">Percentage breakdown normalized across valid schools.</p>
            <div class="mt-4 flex-1 min-h-[250px]">
              <VueApexCharts type="bar" :options="nightlyBreakdownOptions" :series="nightlyBreakdownSeries" height="100%" />
            </div>
          </div>
          <!-- Chart 2: Open Errors -->
          <div class="flex flex-col rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Errors</p>
            <h3 class="mt-2 text-base font-semibold text-slate-950">Open Merge Errors</h3>
            <p class="mt-1 text-xs text-slate-500">Total open errors captured at each sync, across filtered scope.</p>
            <div class="mt-4 flex-1 min-h-[250px]">
              <VueApexCharts type="line" :options="mergeErrorsOptions" :series="mergeErrorsSeries" height="100%" />
            </div>
          </div>
          <!-- Chart 3: Active Users -->
          <div class="flex flex-col rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Engagement</p>
            <h3 class="mt-2 text-base font-semibold text-slate-950">Active Users (24h)</h3>
            <p class="mt-1 text-xs text-slate-500">Distinct active users per snapshot day.</p>
            <div class="mt-4 flex-1 min-h-[250px]">
              <VueApexCharts type="line" :options="activeUsersOptions" :series="activeUsersSeries" height="100%" />
            </div>
          </div>
          <!-- Chart 4: Schools at Risk -->
          <div class="flex flex-col rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Fleet Risk</p>
            <h3 class="mt-2 text-base font-semibold text-slate-950">Schools at Risk</h3>
            <p class="mt-1 text-xs text-slate-500">Count of schools with a computed health score below 65.</p>
            <div class="mt-4 flex-1 min-h-[250px]">
              <VueApexCharts type="line" :options="atRiskOptions" :series="atRiskSeries" height="100%" />
            </div>
          </div>
        </div>

        <ClientHealthTable :schools="filteredSchools" @row-click="handleRowClick" />
      </div>
    </div>
  </div>
</template>
