<script setup lang="ts">
import { computed } from 'vue';
import type { ClientHealthSnapshot } from '@/types/clientHealth';
import Card from '@/components/ui/Card.vue';

interface Props {
  schools: ClientHealthSnapshot[];
}

const props = defineProps<Props>();

// Helpers for calculations
const calcSplits = (schools: ClientHealthSnapshot[], type: 'nightly' | 'realtime') => {
  const total = schools.reduce((sum, s) => sum + s.merges[type].total, 0);
  const succeeded = schools.reduce((sum, s) => sum + s.merges[type].succeeded, 0);
  // Optional chaining in case older DB rows don't have these properties yet
  const issues = schools.reduce((sum, s) => sum + (s.merges[type].finishedWithIssues || 0), 0);
  const noData = schools.reduce((sum, s) => sum + (s.merges[type].noData || 0), 0);
  const failed = schools.reduce((sum, s) => sum + s.merges[type].failed, 0);
  
  if (total === 0) return { succeeded: 0, issues: 0, noData: 0, failed: 0 };
  
  return {
    succeeded: (succeeded / total) * 100,
    issues: (issues / total) * 100,
    noData: (noData / total) * 100,
    failed: (failed / total) * 100,
  };
};

const nightlySplits = computed(() => calcSplits(props.schools, 'nightly'));
const realtimeSplits = computed(() => calcSplits(props.schools, 'realtime'));

// Computed properties for the template
const totalSchools = computed(() => props.schools.length);

const totalMergeErrors = computed(() => props.schools.reduce((sum, s) => sum + (s.mergeErrorsCount ?? 0), 0));

const totalActiveUsers = computed(() => props.schools.reduce((sum, s) => sum + s.activeUsers24h, 0));
</script>

<template>
  <div class="grid gap-6 sm:grid-cols-2 xl:grid-cols-5">
    <Card subtitle="Total Schools" bgClass="bg-slate-50">
      <p class="mt-4 text-4xl font-semibold text-slate-950">{{ totalSchools }}</p>
    </Card>
    
    <Card subtitle="Nightly Success (48h Upstream)" bgClass="bg-slate-50">
      <p class="mt-4 text-3xl font-semibold text-slate-950">{{ nightlySplits.succeeded.toFixed(1) }}%</p>
      <div class="mt-3 flex h-1.5 w-full overflow-hidden rounded-full bg-rose-500">
        <div :style="{ width: `${nightlySplits.succeeded}%` }" class="bg-emerald-500"></div>
        <div :style="{ width: `${nightlySplits.issues}%` }" class="bg-amber-400"></div>
        <div :style="{ width: `${nightlySplits.noData}%` }" class="bg-slate-400"></div>
      </div>
      <p class="mt-2 text-[10px] text-slate-500 flex gap-2">
        <span class="text-emerald-600">■ Success</span>
        <span class="text-amber-500">■ Issues</span>
        <span class="text-slate-500">■ No Data</span>
        <span class="text-rose-600">■ Fail</span>
      </p>
    </Card>

    <Card subtitle="Realtime Success (Last 24h)" bgClass="bg-slate-50">
      <p class="mt-4 text-3xl font-semibold text-slate-950">{{ realtimeSplits.succeeded.toFixed(1) }}%</p>
      <div class="mt-3 flex h-1.5 w-full overflow-hidden rounded-full bg-rose-500">
        <div :style="{ width: `${realtimeSplits.succeeded}%` }" class="bg-emerald-500"></div>
        <div :style="{ width: `${realtimeSplits.issues}%` }" class="bg-amber-400"></div>
        <div :style="{ width: `${realtimeSplits.noData}%` }" class="bg-slate-400"></div>
      </div>
      <p class="mt-2 text-[10px] text-slate-500 flex gap-2">
        <span class="text-emerald-600">■ Success</span>
        <span class="text-amber-500">■ Issues</span>
        <span class="text-slate-500">■ No Data</span>
        <span class="text-rose-600">■ Fail</span>
      </p>
    </Card>

    <Card subtitle="Open Merge Errors (Latest Sync)" bgClass="bg-slate-50">
      <p class="mt-4 text-4xl font-semibold text-slate-950">{{ totalMergeErrors }}</p>
      <p class="mt-2 text-xs text-slate-500">Current Integrations Hub error count captured during the most recent local sync.</p>
    </Card>
    
    <Card subtitle="Active Users (24h)" bgClass="bg-slate-50">
      <p class="mt-4 text-4xl font-semibold text-slate-950">{{ totalActiveUsers }}</p>
      <p class="mt-2 text-xs text-slate-500">Distinct user emails seen in activity over the last 24 hours.</p>
    </Card>
  </div>
</template>
