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

const sortBy = ref<'displayName' | 'sisPlatform' | 'nightlySuccess' | 'realtimeSuccess' | 'mergeErrors' | 'activeUsers' | 'health'>('health');
const sortOrder = ref<'asc' | 'desc'>('desc');

const sortLabels: Record<typeof sortBy.value, string> = {
  displayName: 'School',
  sisPlatform: 'SIS',
  nightlySuccess: 'Nightly (48h)',
  realtimeSuccess: 'Realtime (24h)',
  mergeErrors: 'Open Errors',
  activeUsers: 'Active Users (24h)',
  health: 'Health',
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

const sortedSchools = computed(() => {
  return [...props.schools].sort((a, b) => {
    const nightlyA = nightlyRate(a) ?? 0;
    const nightlyB = nightlyRate(b) ?? 0;
    const realtimeA = realtimeRate(a) ?? 0;
    const realtimeB = realtimeRate(b) ?? 0;
    const healthA = getHealthScoreValue(a);
    const healthB = getHealthScoreValue(b);

    let aVal: number | string = '';
    let bVal: number | string = '';

    switch (sortBy.value) {
      case 'displayName':
        aVal = formatSchoolLabel(a.school);
        bVal = formatSchoolLabel(b.school);
        break;
      case 'nightlySuccess':
        aVal = nightlyA;
        bVal = nightlyB;
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
      <div class="flex flex-wrap items-center gap-2">
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
              <button @click="handleSort('health')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Status <span v-if="sortBy === 'health'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
              </button>
            </th>
            <th scope="col" class="px-6 py-4 py-5 font-semibold">
              <button @click="handleSort('nightlySuccess')" class="flex items-center gap-2 hover:text-slate-950 transition">
                Nightly (48h) <span v-if="sortBy === 'nightlySuccess'" class="text-slate-900">{{ sortOrder === 'asc' ? '↑' : '↓' }}</span>
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
          <tr 
            v-for="school in sortedSchools" 
            :key="school.school"
            @click="emit('rowClick', school)"
            class="group cursor-pointer transition hover:bg-slate-50"
          >
            <td class="whitespace-nowrap px-6 py-5">
              <div class="text-base font-semibold text-slate-950">{{ formatSchoolLabel(school.school) }}</div>
              <div class="mt-1 text-xs text-slate-500">{{ school.products.join(', ') || 'No products listed' }}</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <div class="text-sm font-semibold text-slate-900">{{ school.sisPlatform || 'Unknown' }}</div>
              <div class="mt-1 text-xs text-slate-500">Integration configuration source</div>
            </td>
            <td class="whitespace-nowrap px-6 py-5">
              <Badge :tone="getStatusBadge(school).tone">{{ getStatusBadge(school).label }}</Badge>
              <div class="mt-2 text-xs font-semibold text-slate-400">Score: <span class="text-slate-600">{{ getHealthScore(school) }}</span></div>
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
