<script setup lang="ts">
import { ref, computed, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import type { ClientHealthSnapshot } from '@/types/clientHealth';
import Badge from '@/components/ui/Badge.vue';
import { formatSchoolLabel } from '@/utils/schoolNames';
import { getClientHealthScore, getClientHealthStatusLabel, getMergeSuccessRate } from '@/utils/clientHealth';

interface Props {
  schools: ClientHealthSnapshot[];
}

const props = defineProps<Props>();
const emit = defineEmits<{
  rowClick: [school: ClientHealthSnapshot];
}>();

const route = useRoute();
const router = useRouter();

const validSortBy = [
  'displayName', 'sisPlatform', 'nightlySuccess', 'nightlyTotals',
  'nightlyHalts', 'nightlyDuration', 'realtimeSuccess', 'realtimeTotals',
  'mergeErrors', 'activeUsers', 'health'
] as const;

type SortByKey = typeof validSortBy[number];

const initialSortBy = route.query.sortBy as string;
const initialSortOrder = route.query.sortOrder as string;

const sortBy = ref<SortByKey>(
  validSortBy.includes(initialSortBy as SortByKey) ? (initialSortBy as SortByKey) : 'health'
);
const sortOrder = ref<'asc' | 'desc'>(
  initialSortOrder === 'asc' ? 'asc' : 'desc'
);
const searchQuery = ref('');

watch([sortBy, sortOrder], ([newSortBy, newSortOrder]) => {
  router.replace({
    query: {
      ...route.query,
      sortBy: newSortBy,
      sortOrder: newSortOrder,
    },
  });
});

const sortLabels: Record<typeof sortBy.value, string> = {
  displayName: 'School',
  sisPlatform: 'SIS',
  nightlySuccess: 'Nightly %',
  nightlyTotals: 'Nightly Totals',
  nightlyHalts: 'Merge Halts',
  nightlyDuration: 'Nightly Duration',
  realtimeSuccess: 'Realtime %',
  realtimeTotals: 'Realtime Totals',
  mergeErrors: 'Open Errors',
  activeUsers: 'Active Users (24h)',
  health: 'Health',
};

const healthScoreHelpText = [
  'Health score = clamp(0, 100, baseSuccess - errorPenalty + activityAdjustment).',
  'baseSuccess = average(nightlyRate, realtimeRate), using only rates with data.',
  'nightlyRate/realtimeRate = ((succeeded + 0.5 * finishedWithIssues) / (total - noData)) * 100.',
  'errorPenalty = min(20, log2(openErrors + 1) * 4).',
  'activityAdjustment = +4 for 20+ active users, +2 for 5-19, 0 for 1-4, and -3 for 0.',
];

const formatDuration = (ms?: number) => {
  if (!ms) return 'N/A';
  const totalMinutes = Math.floor(ms / 60000);
  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;

  if (hours === 0) return `${minutes}m`;
  if (minutes === 0) return `${hours}h`;
  return `${hours}h ${minutes}m`;
};

const nightlyRate = (school: ClientHealthSnapshot) => getMergeSuccessRate(school.merges.nightly);
const realtimeRate = (school: ClientHealthSnapshot) => getMergeSuccessRate(school.merges.realtime);
const getHealthScoreValue = (school: ClientHealthSnapshot) => getClientHealthScore(school);

const filteredSchools = computed(() => {
  const q = searchQuery.value.trim().toLowerCase();
  if (!q) return props.schools;
  return props.schools.filter((school) => {
    const displayName = formatSchoolLabel(school.school, school.displayName).toLowerCase();
    const rawId = school.school.toLowerCase();
    return displayName.includes(q) || rawId.includes(q);
  });
});

const sortedSchools = computed(() => {
  return [...filteredSchools.value].sort((a, b) => {
    const nightlyA = nightlyRate(a) ?? 0;
    const nightlyB = nightlyRate(b) ?? 0;
    const nightlyTotalA = a.merges.nightly.total;
    const nightlyTotalB = b.merges.nightly.total;
    const realtimeA = realtimeRate(a) ?? 0;
    const realtimeB = realtimeRate(b) ?? 0;
    const realtimeTotalA = a.merges.realtime.total;
    const realtimeTotalB = b.merges.realtime.total;
    const nightlyHaltsA = a.merges.nightly.halted ?? 0;
    const nightlyHaltsB = b.merges.nightly.halted ?? 0;
    const healthA = getHealthScoreValue(a);
    const healthB = getHealthScoreValue(b);
    const durationA = a.merges.nightly.mergeTimeMs ?? 0;
    const durationB = b.merges.nightly.mergeTimeMs ?? 0;

    let aVal: number | string = '';
    let bVal: number | string = '';

    switch (sortBy.value) {
      case 'displayName':
        aVal = formatSchoolLabel(a.school, a.displayName);
        bVal = formatSchoolLabel(b.school, b.displayName);
        break;
      case 'nightlySuccess':
        aVal = nightlyA;
        bVal = nightlyB;
        break;
      case 'nightlyTotals':
        aVal = nightlyTotalA;
        bVal = nightlyTotalB;
        break;
      case 'nightlyHalts':
        aVal = nightlyHaltsA;
        bVal = nightlyHaltsB;
        break;
      case 'nightlyDuration':
        aVal = durationA;
        bVal = durationB;
        break;
      case 'sisPlatform':
        aVal = a.sisPlatform || 'Unknown';
        bVal = b.sisPlatform || 'Unknown';
        break;
      case 'realtimeSuccess':
        aVal = realtimeA;
        bVal = realtimeB;
        break;
      case 'realtimeTotals':
        aVal = realtimeTotalA;
        bVal = realtimeTotalB;
        break;
      case 'mergeErrors':
        aVal = a.mergeErrorsCount ?? 0;
        bVal = b.mergeErrorsCount ?? 0;
        break;
      case 'activeUsers':
        aVal = a.activeUsers24h;
        bVal = b.activeUsers24h;
        break;
      case 'health':
        aVal = healthA;
        bVal = healthB;
        break;
    }

    if (typeof aVal === 'string') {
      return sortOrder.value === 'asc' ? aVal.localeCompare(bVal as string) : (bVal as string).localeCompare(aVal);
    }

    return sortOrder.value === 'asc' ? (aVal as number) - (bVal as number) : (bVal as number) - (aVal as number);
  });
});

const handleSort = (field: typeof sortBy.value) => {
  if (sortBy.value === field) {
    sortOrder.value = sortOrder.value === 'asc' ? 'desc' : 'asc';
  } else {
    sortBy.value = field;
    sortOrder.value = 'desc';
  }
};

const updateSortBy = (field: typeof sortBy.value) => {
  sortBy.value = field;
};

const getStatusBadge = (school: ClientHealthSnapshot) => {
  const status = getClientHealthStatusLabel(getHealthScoreValue(school));

  if (status === 'Healthy') return { label: status, tone: 'emerald' as const };
  if (status === 'Warning') return { label: status, tone: 'amber' as const };
  return { label: 'At Risk', tone: 'rose' as const };
};

const getHealthScore = (school: ClientHealthSnapshot) => {
  return getHealthScoreValue(school).toFixed(1);
};

const getRowTone = (school: ClientHealthSnapshot) => {
  const status = getStatusBadge(school).tone;

  if (status === 'rose') return 'bg-rose-50/45';
  if (status === 'amber') return 'bg-amber-50/45';
  return 'bg-emerald-50/35';
};

const escapeCsvValue = (value: string | number) => {
  const stringValue = String(value);
  if (/[",\n]/.test(stringValue)) {
    return `"${stringValue.replace(/"/g, '""')}"`;
  }

  return stringValue;
};

const buildExportRows = () => {
  return sortedSchools.value.map((school) => ({
    schoolId: school.school,
    schoolName: formatSchoolLabel(school.school, school.displayName),
    sisPlatform: school.sisPlatform || 'Unknown',
    status: getStatusBadge(school).label,
    healthScore: getHealthScore(school),
    nightlySuccessPct: nightlyRate(school) === null ? 'N/A' : nightlyRate(school)!.toFixed(1),
    nightlySucceeded: school.merges.nightly.succeeded,
    nightlyTotal: school.merges.nightly.total,
    nightlyIssues: school.merges.nightly.finishedWithIssues,
    nightlyNoData: school.merges.nightly.noData,
    nightlyHalts: school.merges.nightly.halted ?? 0,
    nightlyDuration: formatDuration(school.merges.nightly.mergeTimeMs),
    realtimeSuccessPct: realtimeRate(school) === null ? 'N/A' : realtimeRate(school)!.toFixed(1),
    realtimeSucceeded: school.merges.realtime.succeeded,
    realtimeTotal: school.merges.realtime.total,
    realtimeIssues: school.merges.realtime.finishedWithIssues,
    realtimeNoData: school.merges.realtime.noData,
    openErrors: school.mergeErrorsCount ?? 0,
    recentFailures: school.recentFailedMerges.length,
    activeUsers24h: school.activeUsers24h,
    products: school.products.join(', '),
  }));
};

const exportSchools = () => {
  const rows = buildExportRows();
  const headers = Object.keys(rows[0] ?? {
    schoolId: '',
    schoolName: '',
    sisPlatform: '',
    status: '',
    healthScore: '',
    nightlySuccessPct: '',
    nightlySucceeded: '',
    nightlyTotal: '',
    nightlyIssues: '',
    nightlyNoData: '',
    nightlyHalts: '',
    nightlyDuration: '',
    realtimeSuccessPct: '',
    realtimeSucceeded: '',
    realtimeTotal: '',
    realtimeIssues: '',
    realtimeNoData: '',
    openErrors: '',
    recentFailures: '',
    activeUsers24h: '',
    products: '',
  });

  const csvContent = [
    headers.join(','),
    ...rows.map((row) => headers.map((header) => escapeCsvValue(row[header as keyof typeof row] ?? '')).join(',')),
  ].join('\n');

  const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  const dateStamp = new Date().toISOString().slice(0, 10);

  link.href = url;
  link.download = `client-health-export-${dateStamp}.csv`;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
};
</script>

<template>
  <div class="rounded-[32px] border border-slate-200 bg-white shadow-sm">
    <div class="flex flex-col gap-4 border-b border-slate-200 px-6 py-5 sm:flex-row sm:items-center sm:justify-between">
      <div>
        <p class="text-xs font-semibold uppercase tracking-[0.32em] text-slate-500">Client list</p>
        <h2 class="mt-2 text-xl font-semibold tracking-tight text-slate-950">Client health by school</h2>
      </div>
      <div class="flex flex-wrap items-center gap-3">
        <!-- Search input -->
        <div class="relative">
          <svg class="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor">
            <path fill-rule="evenodd" d="M9 3.5a5.5 5.5 0 1 0 0 11 5.5 5.5 0 0 0 0-11ZM2 9a7 7 0 1 1 12.452 4.391l3.328 3.329a.75.75 0 1 1-1.06 1.06l-3.329-3.328A7 7 0 0 1 2 9Z" clip-rule="evenodd" />
          </svg>
          <input
            v-model="searchQuery"
            type="text"
            placeholder="Filter by school or ID…"
            class="h-8 w-52 rounded-full border border-slate-200 bg-slate-50 pl-8 pr-8 text-[11px] font-medium text-slate-700 placeholder-slate-400 outline-none ring-0 transition focus:border-slate-400 focus:bg-white focus:ring-2 focus:ring-slate-200"
          />
          <button
            v-if="searchQuery"
            @click="searchQuery = ''"
            class="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-700 transition"
            aria-label="Clear search"
          >
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="h-3.5 w-3.5">
              <path d="M6.28 5.22a.75.75 0 0 0-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 1 0 1.06 1.06L10 11.06l3.72 3.72a.75.75 0 1 0 1.06-1.06L11.06 10l3.72-3.72a.75.75 0 0 0-1.06-1.06L10 8.94 6.28 5.22Z" />
            </svg>
          </button>
        </div>
        <div class="relative">
          <select
            :value="sortBy"
            @change="updateSortBy(($event.target as HTMLSelectElement).value as typeof sortBy)"
            class="h-8 appearance-none rounded-full border border-slate-200 bg-slate-50 pl-4 pr-10 text-[11px] font-medium tracking-[0.04em] text-slate-600 outline-none transition focus:border-slate-400 focus:bg-white focus:ring-2 focus:ring-slate-200"
            aria-label="Sort schools by"
          >
            <option
              v-for="(label, value) in sortLabels"
              :key="value"
              :value="value"
            >
              Sorted by {{ label }}
            </option>
          </select>
          <svg
            class="pointer-events-none absolute right-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400"
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 20 20"
            fill="currentColor"
          >
            <path fill-rule="evenodd" d="M5.23 7.21a.75.75 0 0 1 1.06.02L10 10.94l3.71-3.71a.75.75 0 1 1 1.06 1.06l-4.24 4.24a.75.75 0 0 1-1.06 0L5.21 8.29a.75.75 0 0 1 .02-1.08Z" clip-rule="evenodd" />
          </svg>
        </div>
        <button
          type="button"
          @click="sortOrder = sortOrder === 'asc' ? 'desc' : 'asc'"
          class="rounded-full bg-slate-950 px-3 py-1.5 text-[11px] font-medium tracking-[0.04em] text-white transition hover:bg-slate-800"
          :aria-label="`Toggle sort order for ${sortLabels[sortBy]}`"
        >
         {{ sortOrder === 'asc' ? '↑' : '↓' }}
        </button>
        <button
          type="button"
          @click="exportSchools"
          class="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-medium tracking-[0.04em] text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
          aria-label="Export visible client table data as CSV"
        >
          Export CSV
        </button>
      </div>
    </div>

    <div class="w-full max-h-[85vh] overflow-x-auto overflow-y-auto">
      <table class="client-health-table relative min-w-[1500px] w-full border-separate border-spacing-y-3 px-3 text-left text-sm text-slate-600">
        <thead class="bg-slate-50 border-b border-slate-200 text-xs uppercase tracking-[0.1em] text-slate-500">
          <tr>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('displayName')" class="flex items-center gap-2 hover:text-slate-950 transition">
                School <span v-if="sortBy === 'displayName'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('sisPlatform')" class="flex items-center gap-2 hover:text-slate-950 transition">
                SIS <span v-if="sortBy === 'sisPlatform'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <div class="flex items-start gap-2">
                <button @click="handleSort('health')" class="flex items-center gap-2 hover:text-slate-950 transition">
                  Status <span v-if="sortBy === 'health'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
                </button>
                <div class="group relative mt-0.5 normal-case tracking-normal">
                  <button
                    type="button"
                    class="flex h-5 w-5 items-center justify-center rounded-full border border-slate-300 bg-white text-[11px] font-semibold text-slate-500 transition hover:border-slate-400 hover:text-slate-700"
                    :aria-label="healthScoreHelpText.join(' ')"
                  >
                    i
                  </button>
                  <div class="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-80 -translate-x-1/2 rounded-2xl border border-slate-200 bg-white p-3 text-[11px] leading-5 text-slate-600 shadow-lg group-hover:block">
                    <p
                      v-for="line in healthScoreHelpText"
                      :key="line"
                      class="mt-2 first:mt-0"
                    >
                      {{ line }}
                    </p>
                  </div>
                </div>
              </div>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('nightlySuccess')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Nightly % <span v-if="sortBy === 'nightlySuccess'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('nightlyTotals')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Nightly Totals <span v-if="sortBy === 'nightlyTotals'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('nightlyHalts')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Merge Halts <span v-if="sortBy === 'nightlyHalts'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('nightlyDuration')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Nightly Duration <span v-if="sortBy === 'nightlyDuration'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('realtimeSuccess')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Realtime % <span v-if="sortBy === 'realtimeSuccess'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('realtimeTotals')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Realtime Totals <span v-if="sortBy === 'realtimeTotals'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('mergeErrors')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Open Errors <span v-if="sortBy === 'mergeErrors'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              <button @click="handleSort('activeUsers')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Active Users (24h) <span v-if="sortBy === 'activeUsers'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="sticky top-0 z-20 bg-slate-50 px-6 py-4 text-right font-semibold shadow-[inset_0_-1px_0_0_rgba(226,232,240,1)]">
              Actions
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="sortedSchools.length === 0">
            <td colspan="12" class="px-6 py-10 text-center text-sm text-slate-400">
              No schools match <span class="font-semibold text-slate-600">"{{ searchQuery }}"</span>.
            </td>
          </tr>
          <tr 
            v-for="school in sortedSchools" 
            :key="school.school"
            @click="emit('rowClick', school)"
            class="group cursor-pointer transition"
          >
            <td :class="['whitespace-nowrap border-y border-l border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school), 'rounded-l-3xl']">
              <div class="text-base font-semibold text-slate-950">{{ formatSchoolLabel(school.school, school.displayName) }}</div>
              <div class="mt-1 text-xs font-medium uppercase tracking-[0.14em] text-slate-400">{{ school.school }}</div>
              <div class="mt-3 flex flex-wrap gap-2">
                <span
                  v-for="product in school.products"
                  :key="product"
                  class="rounded-full border border-slate-200 bg-white/80 px-2.5 py-1 text-[11px] font-semibold text-slate-600"
                >
                  {{ product }}
                </span>
                <span v-if="!school.products.length" class="rounded-full border border-dashed border-slate-200 bg-white/70 px-2.5 py-1 text-[11px] font-medium text-slate-400">
                  No products listed
                </span>
              </div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-sm font-semibold text-slate-900">{{ school.sisPlatform || 'Unknown' }}</div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <Badge :tone="getStatusBadge(school).tone">{{ getStatusBadge(school).label }}</Badge>
              <div class="mt-3 text-2xl font-semibold leading-none text-slate-950">{{ getHealthScore(school) }}</div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-lg font-semibold leading-none text-slate-950">
                {{ nightlyRate(school) === null ? 'N/A' : `${nightlyRate(school)!.toFixed(0)}%` }}
              </div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-lg font-semibold leading-none text-slate-950">{{ school.merges.nightly.succeeded }} / {{ school.merges.nightly.total }}</div>
              <div class="mt-3 flex flex-wrap gap-2">
                <div v-if="school.merges.nightly.finishedWithIssues > 0" class="rounded-full bg-amber-100 px-2 py-1 text-[11px] font-semibold text-amber-700" title="Finished With Issues">{{ school.merges.nightly.finishedWithIssues }} Issues</div>
                <div v-if="school.merges.nightly.noData > 0" class="rounded-full bg-slate-200/80 px-2 py-1 text-[11px] font-semibold text-slate-600" title="No Data">{{ school.merges.nightly.noData }} No Data</div>
                <div v-if="school.merges.nightly.halted > 0" class="rounded-full bg-yellow-100 px-2 py-1 text-[11px] font-semibold text-yellow-800" title="Halted: Change Threshold Exceeded">{{ school.merges.nightly.halted }} Halted</div>
              </div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-lg font-semibold leading-none text-slate-950">{{ school.merges.nightly.halted || 0 }}</div>
              <div v-if="school.merges.nightly.halted > 0" class="mt-3 rounded-full bg-yellow-100 px-2 py-1 text-[11px] font-semibold text-yellow-800" title="Halted: Change Threshold Exceeded">
                Threshold hit
              </div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-lg font-semibold leading-none text-slate-950">{{ formatDuration(school.merges.nightly.mergeTimeMs) }}</div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-lg font-semibold leading-none text-slate-950">
                {{ realtimeRate(school) === null ? 'N/A' : `${realtimeRate(school)!.toFixed(0)}%` }}
              </div>
             
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-lg font-semibold leading-none text-slate-950">{{ school.merges.realtime.succeeded }} / {{ school.merges.realtime.total }}</div>
              <div class="mt-3 flex flex-wrap gap-2">
                <div v-if="school.merges.realtime.finishedWithIssues > 0" class="rounded-full bg-amber-100 px-2 py-1 text-[11px] font-semibold text-amber-700" title="Finished With Issues">{{ school.merges.realtime.finishedWithIssues }} Issues</div>
                <div v-if="school.merges.realtime.noData > 0" class="rounded-full bg-slate-200/80 px-2 py-1 text-[11px] font-semibold text-slate-600" title="No Data">{{ school.merges.realtime.noData }} No Data</div>
              </div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-lg font-semibold leading-none text-slate-950">{{ school.mergeErrorsCount ?? 0 }}</div>
              <div v-if="school.recentFailedMerges.length > 0" class="mt-3 rounded-full bg-rose-100 px-2 py-1 text-[11px] font-semibold tracking-[0.08em] text-rose-700">{{ school.recentFailedMerges.length }} recent failures</div>
            </td>
            <td :class="['whitespace-nowrap border-y border-slate-200 px-6 py-5 align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school)]">
              <div class="text-lg font-semibold leading-none text-slate-950">{{ school.activeUsers24h }}</div>
            </td>
            <td :class="['whitespace-nowrap border-y border-r border-slate-200 px-6 py-5 text-right align-top shadow-sm transition group-hover:border-slate-300', getRowTone(school), 'rounded-r-3xl']">
              <div class="flex flex-col items-end gap-2">
                <router-link :to="{ name: 'AdminClientHealthDetail', params: { school: school.school } }" class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-sm font-semibold text-white transition hover:bg-slate-700">View Details</router-link>
                <a :href="`https://app.coursedog.com/#/int/${school.school}`" target="_blank" rel="noopener noreferrer" class="inline-flex items-center text-xs font-medium text-slate-500 hover:text-blue-600 transition">Int Hub ↗</a>
                <a :href="`https://app.coursedog.com/#/int/${school.school}/merge-history`" target="_blank" rel="noopener noreferrer" class="inline-flex items-center text-xs font-medium text-slate-500 hover:text-blue-600 transition">Reports ↗</a>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.client-health-table thead th {
  position: sticky;
  top: 0;
  z-index: 20;
  background: #f8fafc;
  box-shadow: inset 0 -1px 0 rgba(226, 232, 240, 1);
}
</style>
