<script setup lang="ts">
import { ref, computed } from 'vue';
import type { ClientHealthSnapshot } from '@/types/clientHealth';
import Badge from '@/components/ui/Badge.vue';
import { formatSchoolLabel } from '@/utils/schoolNames';

interface Props {
  schools: ClientHealthSnapshot[];
}

const props = defineProps<Props>();
const emit = defineEmits<{
  rowClick: [school: ClientHealthSnapshot];
}>();

const sortBy = ref<'displayName' | 'sisPlatform' | 'nightlySuccess' | 'nightlyDuration' | 'realtimeSuccess' | 'mergeErrors' | 'activeUsers' | 'health'>('health');
const sortOrder = ref<'asc' | 'desc'>('desc');
const searchQuery = ref('');

const sortLabels: Record<typeof sortBy.value, string> = {
  displayName: 'School',
  sisPlatform: 'SIS',
  nightlySuccess: 'Nightly (48h)',
  nightlyDuration: 'Nightly Duration',
  realtimeSuccess: 'Realtime (24h)',
  mergeErrors: 'Open Errors',
  activeUsers: 'Active Users (24h)',
  health: 'Health',
};

const healthScoreHelpText =
  'Score = average nightly and realtime success, minus a modest open-error penalty, plus a small activity confidence adjustment.';

const formatDuration = (ms?: number) => {
  if (!ms) return 'N/A';
  const mins = Math.floor(ms / 60000);
  const secs = Math.floor((ms % 60000) / 1000);
  if (mins === 0) return `${secs}s`;
  return `${mins}m ${secs}s`;
};

const nightlyRate = (school: ClientHealthSnapshot) => {
  const { total, succeeded, noData, finishedWithIssues } = school.merges.nightly;
  const validTotal = total - noData;
  return validTotal > 0 ? ((succeeded + (finishedWithIssues * 0.5)) / validTotal) * 100 : null;
};

const realtimeRate = (school: ClientHealthSnapshot) => {
  const { total, succeeded, noData, finishedWithIssues } = school.merges.realtime;
  const validTotal = total - noData;
  return validTotal > 0 ? ((succeeded + (finishedWithIssues * 0.5)) / validTotal) * 100 : null;
};

const getBaseSuccessScore = (school: ClientHealthSnapshot) => {
  const rates = [nightlyRate(school), realtimeRate(school)].filter(
    (value): value is number => value !== null,
  );

  if (rates.length === 0) {
    return 0;
  }

  return rates.reduce((sum, value) => sum + value, 0) / rates.length;
};

const getErrorPenalty = (school: ClientHealthSnapshot) => {
  if (school.mergeErrorsCount <= 0) {
    return 0;
  }

  // Use a capped logarithmic penalty so errors matter without overwhelming the score.
  return Math.min(20, Math.log2(school.mergeErrorsCount + 1) * 4);
};

const getActivityAdjustment = (school: ClientHealthSnapshot) => {
  if (school.activeUsers24h >= 20) {
    return 4;
  }

  if (school.activeUsers24h >= 5) {
    return 2;
  }

  if (school.activeUsers24h >= 1) {
    return 0;
  }

  return -3;
};

const getHealthScoreValue = (school: ClientHealthSnapshot) => {
  const score =
    getBaseSuccessScore(school) - getErrorPenalty(school) + getActivityAdjustment(school);
  return Math.max(0, Math.min(100, score));
};

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
    const realtimeA = realtimeRate(a) ?? 0;
    const realtimeB = realtimeRate(b) ?? 0;
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
      case 'mergeErrors':
        aVal = a.mergeErrorsCount;
        bVal = b.mergeErrorsCount;
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

const getStatusBadge = (school: ClientHealthSnapshot) => {
  const score = getHealthScoreValue(school);

  if (score >= 85) return { label: 'Healthy', tone: 'emerald' as const };
  if (score >= 65) return { label: 'Warning', tone: 'amber' as const };
  return { label: 'At Risk', tone: 'rose' as const };
};

const getHealthScore = (school: ClientHealthSnapshot) => {
  return getHealthScoreValue(school).toFixed(1);
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
            class="h-9 w-56 rounded-full border border-slate-200 bg-slate-50 pl-8 pr-8 text-xs font-medium text-slate-700 placeholder-slate-400 outline-none ring-0 transition focus:border-slate-400 focus:bg-white focus:ring-2 focus:ring-slate-200"
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
        <!-- Sort indicator -->
        <span class="rounded-full border border-slate-200 bg-slate-50 px-3 py-2 text-xs font-semibold uppercase tracking-[0.24em] text-slate-600">Sorted by</span>
        <span class="rounded-full bg-slate-950 px-3 py-2 text-xs font-semibold uppercase tracking-[0.24em] text-white">{{ sortLabels[sortBy] }} {{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
      </div>
    </div>

    <div class="overflow-x-auto w-full">
      <table class="w-full text-left text-sm text-slate-600">
        <thead class="bg-slate-50 border-b border-slate-200 text-xs uppercase tracking-[0.1em] text-slate-500">
          <tr>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <button @click="handleSort('displayName')" class="flex items-center gap-2 hover:text-slate-950 transition">
                School <span v-if="sortBy === 'displayName'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <button @click="handleSort('sisPlatform')" class="flex items-center gap-2 hover:text-slate-950 transition">
                SIS <span v-if="sortBy === 'sisPlatform'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <div class="flex items-start gap-2">
                <button @click="handleSort('health')" class="flex items-center gap-2 hover:text-slate-950 transition">
                  Status <span v-if="sortBy === 'health'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
                </button>
                <div class="group relative mt-0.5 normal-case tracking-normal">
                  <button
                    type="button"
                    class="flex h-5 w-5 items-center justify-center rounded-full border border-slate-300 bg-white text-[11px] font-semibold text-slate-500 transition hover:border-slate-400 hover:text-slate-700"
                    :aria-label="healthScoreHelpText"
                  >
                    i
                  </button>
                  <div class="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-64 -translate-x-1/2 rounded-2xl border border-slate-200 bg-white p-3 text-[11px] leading-5 text-slate-600 shadow-lg group-hover:block">
                    {{ healthScoreHelpText }}
                  </div>
                </div>
              </div>
            </th>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <button @click="handleSort('nightlySuccess')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Nightly (48h) <span v-if="sortBy === 'nightlySuccess'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <button @click="handleSort('nightlyDuration')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Nightly Duration <span v-if="sortBy === 'nightlyDuration'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <button @click="handleSort('realtimeSuccess')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Realtime (24h) <span v-if="sortBy === 'realtimeSuccess'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <button @click="handleSort('mergeErrors')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Open Errors <span v-if="sortBy === 'mergeErrors'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <button @click="handleSort('activeUsers')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Active Users (24h) <span v-if="sortBy === 'activeUsers'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="px-6 py-4 py-5 text-right font-semibold">
              Actions
            </th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100 bg-white">
          <tr v-if="sortedSchools.length === 0">
            <td colspan="8" class="px-6 py-10 text-center text-sm text-slate-400">
              No schools match <span class="font-semibold text-slate-600">"{{ searchQuery }}"</span>.
            </td>
          </tr>
          <tr 
            v-for="school in sortedSchools" 
            :key="school.school"
            @click="emit('rowClick', school)"
            class="group cursor-pointer transition hover:bg-slate-50"
          >
            <td class="whitespace-nowrap px-6 py-5">
              <div class="text-base font-semibold text-slate-950">{{ formatSchoolLabel(school.school, school.displayName) }}</div>
              <div class="mt-1 text-xs text-slate-500">{{ school.products.join(', ') || 'No products listed' }}</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <div class="text-sm font-semibold text-slate-900">{{ school.sisPlatform || 'Unknown' }}</div>
              <div class="mt-1 text-xs text-slate-500">Integration configuration source</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <Badge :tone="getStatusBadge(school).tone">{{ getStatusBadge(school).label }}</Badge>
              <div class="mt-2 text-xs font-semibold text-slate-400">Score: <span class="text-slate-600">{{ getHealthScore(school) }}</span></div>
              <div class="mt-1 text-xs text-slate-500">Uses success, open errors, and active-user confidence.</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <div class="flex items-center gap-2">
                <div class="text-sm font-semibold text-slate-900">{{ school.merges.nightly.succeeded }} / {{ school.merges.nightly.total }}</div>
                <div v-if="school.merges.nightly.finishedWithIssues > 0" class="text-xs font-semibold text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded-md" title="Finished With Issues">{{ school.merges.nightly.finishedWithIssues }} Issues</div>
                <div v-if="school.merges.nightly.noData > 0" class="text-xs font-semibold text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded-md" title="No Data">{{ school.merges.nightly.noData }} No Data</div>
              </div>
              <div class="mt-1 text-xs text-slate-500">{{ (nightlyRate(school) ?? 0).toFixed(0) }}% score in upstream 48h window</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <div class="text-sm font-semibold text-slate-900">{{ formatDuration(school.merges.nightly.mergeTimeMs) }}</div>
              <div class="mt-1 text-xs text-slate-500">Total min/max timespan</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <div class="flex items-center gap-2">
                <div class="text-sm font-semibold text-slate-900">{{ school.merges.realtime.succeeded }} / {{ school.merges.realtime.total }}</div>
                <div v-if="school.merges.realtime.finishedWithIssues > 0" class="text-xs font-semibold text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded-md" title="Finished With Issues">{{ school.merges.realtime.finishedWithIssues }} Issues</div>
                <div v-if="school.merges.realtime.noData > 0" class="text-xs font-semibold text-slate-500 bg-slate-100 px-1.5 py-0.5 rounded-md" title="No Data">{{ school.merges.realtime.noData }} No Data</div>
              </div>
              <div class="mt-1 text-xs text-slate-500">{{ (realtimeRate(school) ?? 0).toFixed(0) }}% score in last 24h</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <div class="text-sm font-semibold text-slate-900">{{ school.mergeErrorsCount }}</div>
              <div class="mt-1 text-xs text-slate-500">Current open-error count</div>
              <div v-if="school.recentFailedMerges.length > 0" class="mt-1 text-xs font-medium text-rose-500 tracking-wide">{{ school.recentFailedMerges.length }} RECENT FAILED MERGE(S)</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <div class="text-sm font-semibold text-slate-900">{{ school.activeUsers24h }}</div>
              <div class="mt-1 text-xs text-slate-500">Distinct emails in the last 24h</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5 text-right">
              <div class="flex flex-col items-end gap-2">
                <router-link :to="{ name: 'AdminClientHealthDetail', params: { school: school.school } }" class="inline-flex items-center text-sm font-semibold text-slate-500 hover:text-blue-600 transition">View Details →</router-link>
                <a :href="`https://app.coursedog.com/#/int/${school.school}`" target="_blank" rel="noopener noreferrer" class="inline-flex items-center text-xs font-medium text-slate-400 hover:text-blue-600 transition">Int Hub ↗</a>
                <a :href="`https://app.coursedog.com/#/int/${school.school}/merge-history`" target="_blank" rel="noopener noreferrer" class="inline-flex items-center text-xs font-medium text-slate-400 hover:text-blue-600 transition">Reports ↗</a>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
