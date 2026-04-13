<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { useQuery } from '@tanstack/vue-query';
import VueApexCharts from 'vue3-apexcharts';
import { getErrorAnalysis } from '@/api';
import Card from '@/components/ui/Card.vue';
import { useChartOptions } from '@/composables/useChartOptions';
import type {
  ErrorAnalysisResponse,
  ErrorBreakdownRow,
  ErrorSignatureCluster,
  ResolutionHint,
} from '@/types/errorAnalysis';
import { formatLocalDateTime, getLocalTimeZoneLabel } from '@/utils/dateTime';
import { formatSchoolLabel } from '@/utils/schoolNames';

type ErrorViewMode = 'aggregate' | 'school' | 'sis';
type WindowOption = '7' | '30' | 'all';

const selectedWindow = ref<WindowOption>('7');
const selectedSchool = ref('all');
const selectedSis = ref('all');
const activeView = ref<ErrorViewMode>('aggregate');
const localTimeZoneLabel = getLocalTimeZoneLabel();
const coursedogBaseUrl = (import.meta.env.VITE_COURSEDOG_PRD_URL?.trim() || 'https://app.coursedog.com').replace(/\/+$/, '');

const daysParam = computed<number | undefined>(() => {
  if (selectedWindow.value === 'all') return undefined;
  return Number(selectedWindow.value);
});

const { data, isLoading, error } = useQuery({
  queryKey: computed(() => [
    'errorAnalysis',
    {
      days: daysParam.value ?? 'all',
      school: selectedSchool.value,
      sisPlatform: selectedSis.value,
    },
  ]),
  queryFn: () =>
    getErrorAnalysis({
      days: daysParam.value,
      school: selectedSchool.value === 'all' ? undefined : selectedSchool.value,
      sisPlatform: selectedSis.value === 'all' ? undefined : selectedSis.value,
    }).then((res) => res.data as ErrorAnalysisResponse),
});

const response = computed(() => data.value);

const sisOptions = computed(() => [
  { value: 'all', label: 'All SIS Platforms' },
  ...(response.value?.filterOptions.sisPlatforms ?? []).map((value) => ({
    value,
    label: value,
  })),
]);

const schoolOptions = computed(() => {
  const schools = response.value?.filterOptions.schools ?? [];
  const filtered = selectedSis.value === 'all'
    ? schools
    : schools.filter((school) => school.sisPlatform === selectedSis.value);

  return [
    { value: 'all', label: 'All schools' },
    ...filtered.map((school) => ({
      value: school.value,
      label: formatSchoolLabel(school.value, school.label),
    })),
  ];
});

watch([selectedSis, response], () => {
  if (selectedSchool.value === 'all') return;
  const exists = schoolOptions.value.some((option) => option.value === selectedSchool.value);
  if (!exists) {
    selectedSchool.value = 'all';
  }
}, { immediate: true });

const hasCapturedData = computed(() => response.value?.metadata.hasCapturedData ?? false);
const hasFilteredRows = computed(() => (response.value?.summary.totalErrorInstances ?? 0) > 0);

const summaryCards = computed(() => ([
  {
    key: 'totalErrors',
    label: 'Open Error Instances',
    value: response.value?.summary.totalErrorInstances ?? 0,
    detail: 'Grouped open merge errors counted across captured snapshots.',
  },
  {
    key: 'signatures',
    label: 'Recurring Signatures',
    value: response.value?.summary.distinctSignatures ?? 0,
    detail: 'Distinct normalized error patterns in the current filtered view.',
  },
  {
    key: 'schools',
    label: 'Affected Schools',
    value: response.value?.summary.affectedSchools ?? 0,
    detail: 'Unique schools represented by the current error selection.',
  },
  {
    key: 'days',
    label: 'Captured Days',
    value: response.value?.summary.captureDays ?? 0,
    detail: 'Days with detailed error captures available for this filter set.',
  },
]));

const trendCategories = computed(() => response.value?.trends.map((point) => point.snapshotDate) ?? []);

const trendSeries = computed(() => ([
  {
    name: 'Open errors',
    data: response.value?.trends.map((point) => point.totalErrors) ?? [],
  },
  {
    name: 'Distinct signatures',
    data: response.value?.trends.map((point) => point.distinctSignatures) ?? [],
  },
]));

const trendOptions = computed(() => ({
  ...useChartOptions({
    categories: trendCategories.value,
    colors: ['#dc2626', '#0f766e'],
  }),
  stroke: {
    curve: 'smooth' as const,
    width: [3, 2],
  },
  markers: {
    size: 4,
    hover: {
      size: 6,
    },
  },
}));

const topSignatures = computed(() => response.value?.signatures.slice(0, 12) ?? []);
const schoolRows = computed(() => response.value?.schoolBreakdowns ?? []);
const sisRows = computed(() => response.value?.sisBreakdowns ?? []);

const viewOptions: Array<{ value: ErrorViewMode; label: string }> = [
  { value: 'aggregate', label: 'Aggregate' },
  { value: 'school', label: 'By School' },
  { value: 'sis', label: 'By SIS' },
];

const windowOptions: Array<{ value: WindowOption; label: string }> = [
  { value: '7', label: '7d' },
  { value: '30', label: '30d' },
  { value: 'all', label: 'All captured' },
];

const getIntegrationHubUrl = (school: string) => `${coursedogBaseUrl}/#/int/${school}`;

const formatTheme = (value?: string | null) => {
  if (!value) return 'Mixed';
  return value.replaceAll('_', ' ');
};

const getResolutionToneClass = (hint: ResolutionHint) => {
  switch (hint.bucket) {
    case 'missing_reference':
      return 'bg-amber-100 text-amber-800';
    case 'duplicate_conflict':
      return 'bg-rose-100 text-rose-700';
    case 'validation_data_shape':
      return 'bg-sky-100 text-sky-700';
    case 'configuration_auth':
      return 'bg-violet-100 text-violet-700';
    default:
      return 'bg-slate-100 text-slate-700';
  }
};

const getSchoolLabel = (school?: string | null) => {
  if (!school) return 'Unknown school';
  const schoolMatch = response.value?.filterOptions.schools.find((option) => option.value === school);
  return formatSchoolLabel(school, schoolMatch?.label);
};

const getDominantDrilldownLabel = (signature: ErrorSignatureCluster) => {
  if (signature.dominantSchool) {
    return getSchoolLabel(signature.dominantSchool);
  }
  return signature.dominantSisPlatform || 'Mixed schools';
};

const formatCount = (value?: number | null) => `${value ?? 0}`;

const schoolRoute = (school: string) => ({
  name: 'AdminClientHealthDetail',
  params: { school },
});

const sortSchoolRows = computed(() => [...schoolRows.value].sort((a, b) => b.totalErrors - a.totalErrors || a.label.localeCompare(b.label)));
const sortSisRows = computed(() => [...sisRows.value].sort((a, b) => b.totalErrors - a.totalErrors || a.label.localeCompare(b.label)));

const hasAnyRows = computed(() =>
  topSignatures.value.length > 0 || sortSchoolRows.value.length > 0 || sortSisRows.value.length > 0
);

const headerStatus = computed(() => {
  if (!response.value?.metadata.lastCapturedAt) return 'No detailed capture yet';
  return `Last detailed capture: ${formatLocalDateTime(response.value.metadata.lastCapturedAt)}`;
});

const historyStatus = computed(() => {
  if (!response.value?.metadata.historyStartsOn) return 'Detailed error history will begin with the next sync.';
  return `Detailed error history starts on ${response.value.metadata.historyStartsOn}.`;
});

const isEmptyState = computed(() => !isLoading.value && !hasAnyRows.value);

const coerceSchoolSpecificRow = (row: ErrorBreakdownRow) => row.sisPlatform || 'Unknown';
</script>

<template>
  <div class="w-full bg-slate-50 text-slate-900">
    <div class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      <div class="mb-8 rounded-[28px] border border-slate-200 bg-white/95 p-8 shadow-sm">
        <div class="flex flex-col gap-6 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Admin Analytics</p>
            <h1 class="mt-3 text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">Error Analysis</h1>
            <p class="mt-4 max-w-3xl text-base leading-7 text-slate-600">
              Aggregate open merge errors into recurring signatures, compare patterns across schools and SIS platforms,
              and surface likely next steps from captured trends.
            </p>
            <p class="mt-3 text-sm text-slate-500">{{ headerStatus }}</p>
            <p class="mt-1 text-sm text-slate-500">{{ historyStatus }}</p>
            <p class="mt-2 text-xs text-slate-400">Times shown in {{ localTimeZoneLabel }}.</p>
          </div>

          <div class="rounded-[24px] border border-slate-200 bg-slate-50 p-4">
            <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">View</p>
            <div class="mt-3 flex flex-wrap gap-2" data-testid="view-toggle">
              <button
                v-for="option in viewOptions"
                :key="option.value"
                class="rounded-full px-4 py-2 text-sm font-semibold transition"
                :class="activeView === option.value ? 'bg-slate-900 text-white' : 'bg-white text-slate-700 hover:bg-slate-100'"
                @click="activeView = option.value"
              >
                {{ option.label }}
              </button>
            </div>
          </div>
        </div>
      </div>

      <div class="mb-6 rounded-[28px] border border-slate-200 bg-white p-6 shadow-sm">
        <div class="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div>
            <h2 class="text-lg font-semibold text-slate-950">Filters</h2>
            <p class="mt-1 text-sm text-slate-500">Slice recurring error patterns by capture window, school, or SIS cohort.</p>
          </div>

          <div class="flex flex-wrap items-center gap-3">
            <div class="flex items-center gap-2">
              <label class="text-sm font-medium text-slate-700">Window</label>
              <div class="flex rounded-full border border-slate-200 bg-slate-50 p-1">
                <button
                  v-for="option in windowOptions"
                  :key="option.value"
                  class="rounded-full px-3 py-1.5 text-sm font-medium transition"
                  :class="selectedWindow === option.value ? 'bg-slate-900 text-white' : 'text-slate-600 hover:text-slate-900'"
                  @click="selectedWindow = option.value"
                >
                  {{ option.label }}
                </button>
              </div>
            </div>

            <div class="flex items-center gap-2">
              <label class="text-sm font-medium text-slate-700">SIS</label>
              <select v-model="selectedSis" data-testid="sis-filter" class="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                <option v-for="option in sisOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
              </select>
            </div>

            <div class="flex items-center gap-2">
              <label class="text-sm font-medium text-slate-700">School</label>
              <select v-model="selectedSchool" data-testid="school-filter" class="rounded-full border border-slate-200 bg-white px-4 py-2 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                <option v-for="option in schoolOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
              </select>
            </div>
          </div>
        </div>
      </div>

      <div v-if="isLoading" class="rounded-[28px] border border-slate-200 bg-white p-8 text-slate-700 shadow-sm">Loading detailed error analysis...</div>
      <div v-else-if="error" class="rounded-[28px] border border-rose-200 bg-rose-50 p-8 text-rose-700 shadow-sm">Failed to load error analysis data.</div>
      <div v-else-if="isEmptyState && !hasCapturedData" class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm" data-testid="error-analysis-empty">
        <div class="max-w-3xl">
          <p class="text-sm font-semibold uppercase tracking-[0.25em] text-slate-500">Detailed History Unavailable</p>
          <h2 class="mt-3 text-2xl font-semibold text-slate-950">The next sync starts detailed error analysis.</h2>
          <p class="mt-3 text-base leading-7 text-slate-600">
            Client Health already tracks open-error counts, but this page only shows detailed error clustering after the new
            capture pipeline has written grouped signatures into the local database.
          </p>
        </div>
      </div>
      <div v-else-if="isEmptyState" class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
        <p class="text-lg font-semibold text-slate-950">No matching captured errors</p>
        <p class="mt-2 text-sm text-slate-500">Try widening the window or clearing the school/SIS filters.</p>
      </div>
      <div v-else class="space-y-6">
        <div class="grid gap-6 sm:grid-cols-2 xl:grid-cols-4">
          <Card v-for="card in summaryCards" :key="card.key" :subtitle="card.label" bgClass="bg-white">
            <p class="mt-4 text-4xl font-semibold text-slate-950">{{ card.value }}</p>
            <p class="mt-2 text-xs leading-5 text-slate-500">{{ card.detail }}</p>
          </Card>
        </div>

        <template v-if="activeView === 'aggregate'">
          <div class="grid gap-6 xl:grid-cols-[minmax(0,1.3fr)_minmax(0,1fr)]">
            <Card subtitle="Trend" title="Open error intensity over time">
              <p class="mt-1 text-xs text-slate-500">Track captured open-error volume against the number of recurring signatures seen on each day.</p>
              <div class="mt-6 min-h-[300px]">
                <VueApexCharts type="line" :options="trendOptions" :series="trendSeries" height="300" />
              </div>
            </Card>

            <Card subtitle="Coverage" title="Captured scope">
              <div class="space-y-4">
                <div class="rounded-2xl bg-slate-50 p-4">
                  <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Latest snapshot</p>
                  <p class="mt-2 text-2xl font-semibold text-slate-950">{{ response?.summary.latestSnapshotDate || 'Unavailable' }}</p>
                </div>
                <div class="rounded-2xl bg-slate-50 p-4">
                  <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Filter result</p>
                  <p class="mt-2 text-base text-slate-700">{{ response?.metadata.filteredGroupCount }} grouped rows across {{ response?.summary.captureDays }} captured days.</p>
                </div>
                <div class="rounded-2xl bg-slate-50 p-4">
                  <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Trend note</p>
                  <p class="mt-2 text-sm leading-6 text-slate-600">
                    Historical detail begins on {{ response?.metadata.historyStartsOn || 'the next sync' }}. Earlier dates remain intentionally blank rather than inferred.
                  </p>
                </div>
              </div>
            </Card>
          </div>

          <Card subtitle="Recurring Patterns" title="Top error signatures">
            <div class="overflow-x-auto">
              <table class="min-w-full border-separate border-spacing-y-3 text-left text-sm text-slate-600">
                <thead class="text-xs uppercase tracking-[0.12em] text-slate-500">
                  <tr>
                    <th class="px-4 py-2 font-semibold">Signature</th>
                    <th class="px-4 py-2 font-semibold">Impact</th>
                    <th class="px-4 py-2 font-semibold">Dominant source</th>
                    <th class="px-4 py-2 font-semibold">Resolution</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="signature in topSignatures" :key="signature.signatureKey" data-testid="signature-row">
                    <td class="rounded-l-3xl border-y border-l border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <div class="flex flex-wrap items-center gap-2">
                        <span class="rounded-full bg-slate-900 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-white">
                          {{ signature.entityType || 'Unknown entity' }}
                        </span>
                        <span v-if="signature.errorCode" class="rounded-full bg-slate-200 px-2.5 py-1 text-[11px] font-semibold text-slate-700">
                          {{ signature.errorCode }}
                        </span>
                      </div>
                      <p class="mt-3 font-semibold text-slate-950">{{ signature.sampleMessage }}</p>
                      <p class="mt-2 text-xs leading-5 text-slate-500">{{ signature.normalizedMessage }}</p>
                      <p v-if="signature.termCodes.length" class="mt-2 text-xs text-slate-500">Terms: {{ signature.termCodes.join(', ') }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="text-lg font-semibold text-slate-950">{{ formatCount(signature.totalCount) }}</p>
                      <p class="mt-2 text-xs text-slate-500">{{ signature.recurrenceDays }} captured day{{ signature.recurrenceDays === 1 ? '' : 's' }}</p>
                      <p class="mt-1 text-xs text-slate-500">{{ signature.affectedSchools }} school{{ signature.affectedSchools === 1 ? '' : 's' }}</p>
                      <p class="mt-1 text-xs text-slate-500">Last seen {{ signature.lastSeen }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="font-medium text-slate-900">{{ getDominantDrilldownLabel(signature) }}</p>
                      <p class="mt-1 text-xs text-slate-500">Primary SIS: {{ signature.dominantSisPlatform || 'Mixed' }}</p>
                      <div v-if="signature.dominantSchool" class="mt-3 flex flex-wrap gap-2">
                        <router-link :to="schoolRoute(signature.dominantSchool)" class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700">
                          School detail
                        </router-link>
                        <a :href="getIntegrationHubUrl(signature.dominantSchool)" target="_blank" rel="noopener noreferrer" class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950">
                          Integration Hub
                        </a>
                      </div>
                    </td>
                    <td class="rounded-r-3xl border-y border-r border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <span class="inline-flex rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.08em]" :class="getResolutionToneClass(signature.resolutionHint)">
                        {{ signature.resolutionHint.title }}
                      </span>
                      <p class="mt-3 text-sm font-medium leading-6 text-slate-900">{{ signature.resolutionHint.action }}</p>
                      <p class="mt-2 text-xs leading-5 text-slate-500">{{ signature.resolutionHint.rationale }}</p>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </Card>
        </template>

        <template v-else-if="activeView === 'school'">
          <Card subtitle="School Comparison" title="Where recurring errors concentrate">
            <div class="overflow-x-auto">
              <table class="min-w-full border-separate border-spacing-y-3 text-left text-sm text-slate-600">
                <thead class="text-xs uppercase tracking-[0.12em] text-slate-500">
                  <tr>
                    <th class="px-4 py-2 font-semibold">School</th>
                    <th class="px-4 py-2 font-semibold">SIS</th>
                    <th class="px-4 py-2 font-semibold">Impact</th>
                    <th class="px-4 py-2 font-semibold">Dominant signature</th>
                    <th class="px-4 py-2 font-semibold">Likely next step</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in sortSchoolRows" :key="row.key">
                    <td class="rounded-l-3xl border-y border-l border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <router-link :to="schoolRoute(row.key)" class="font-semibold text-slate-950 hover:text-blue-700">
                        {{ formatSchoolLabel(row.key, row.label) }}
                      </router-link>
                      <p class="mt-1 text-xs text-slate-500">Last seen {{ row.lastSeen }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="font-medium text-slate-900">{{ coerceSchoolSpecificRow(row) }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="text-lg font-semibold text-slate-950">{{ row.totalErrors }}</p>
                      <p class="mt-2 text-xs text-slate-500">{{ row.distinctSignatures }} signatures</p>
                      <p class="mt-1 text-xs text-slate-500">{{ row.recurrenceDays || 0 }} captured day{{ row.recurrenceDays === 1 ? '' : 's' }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="font-medium text-slate-900">{{ row.dominantSignature || 'No dominant signature yet' }}</p>
                      <p class="mt-2 text-xs text-slate-500">Theme: {{ formatTheme(row.topResolutionTheme) }}</p>
                    </td>
                    <td class="rounded-r-3xl border-y border-r border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="text-sm leading-6 text-slate-700">{{ row.likelyNextStep || 'Inspect the latest Integration Hub samples for this school.' }}</p>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </Card>
        </template>

        <template v-else>
          <Card subtitle="SIS Comparison" title="Recurring patterns by SIS">
            <div class="overflow-x-auto">
              <table class="min-w-full border-separate border-spacing-y-3 text-left text-sm text-slate-600">
                <thead class="text-xs uppercase tracking-[0.12em] text-slate-500">
                  <tr>
                    <th class="px-4 py-2 font-semibold">SIS</th>
                    <th class="px-4 py-2 font-semibold">Affected schools</th>
                    <th class="px-4 py-2 font-semibold">Impact</th>
                    <th class="px-4 py-2 font-semibold">Dominant signature</th>
                    <th class="px-4 py-2 font-semibold">Common theme</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in sortSisRows" :key="row.key">
                    <td class="rounded-l-3xl border-y border-l border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="font-semibold text-slate-950">{{ row.label }}</p>
                      <p class="mt-1 text-xs text-slate-500">Last seen {{ row.lastSeen }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="text-lg font-semibold text-slate-950">{{ row.affectedSchools || 0 }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="text-lg font-semibold text-slate-950">{{ row.totalErrors }}</p>
                      <p class="mt-2 text-xs text-slate-500">{{ row.distinctSignatures }} signatures</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="font-medium text-slate-900">{{ row.dominantSignature || 'No dominant signature yet' }}</p>
                    </td>
                    <td class="rounded-r-3xl border-y border-r border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="text-sm leading-6 text-slate-700">{{ formatTheme(row.commonResolutionTheme) }}</p>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </Card>
        </template>

        <div v-if="!hasFilteredRows" class="rounded-[28px] border border-amber-200 bg-amber-50 px-6 py-4 text-sm text-amber-800">
          No grouped error rows match the current filters yet. Captured history remains available starting on {{ response?.metadata.historyStartsOn || 'the next sync' }}.
        </div>
      </div>
    </div>
  </div>
</template>
