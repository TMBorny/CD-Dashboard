<script setup lang="ts">
import { computed } from 'vue';
import type { ClientHealthSnapshot } from '@/types/clientHealth';
import Card from '@/components/ui/Card.vue';

interface Props {
  schools: ClientHealthSnapshot[];
}

const props = defineProps<Props>();

// Helpers for calculations
const calcNightlySuccessRate = (schools: ClientHealthSnapshot[]) => {
  const total = schools.reduce((sum, s) => sum + s.merges.nightly.total, 0);
  const succeeded = schools.reduce((sum, s) => sum + s.merges.nightly.succeeded, 0);
  return total > 0 ? (succeeded / total) * 100 : 0;
};

const calcRealtimeSuccessRate = (schools: ClientHealthSnapshot[]) => {
  const total = schools.reduce((sum, s) => sum + s.merges.realtime.total, 0);
  const succeeded = schools.reduce((sum, s) => sum + s.merges.realtime.succeeded, 0);
  return total > 0 ? (succeeded / total) * 100 : 0;
};

// Computed properties for the template
const totalSchools = computed(() => props.schools.length);

const nightlySuccessRate = computed(() => calcNightlySuccessRate(props.schools));

const realtimeSuccessRate = computed(() => calcRealtimeSuccessRate(props.schools));

const totalMergeErrors = computed(() => props.schools.reduce((sum, s) => sum + s.mergeErrorsCount, 0));

const totalActiveUsers = computed(() => props.schools.reduce((sum, s) => sum + s.activeUsers24h, 0));
</script>

<template>
  <div class="grid gap-6 sm:grid-cols-2 xl:grid-cols-5">
    <Card subtitle="Total Schools" bgClass="bg-slate-50">
      <p class="mt-4 text-4xl font-semibold text-slate-950">{{ totalSchools }}</p>
    </Card>
    <Card subtitle="Nightly Success (48h Upstream Window)" bgClass="bg-slate-50">
      <p class="mt-4 text-4xl font-semibold text-slate-950">{{ nightlySuccessRate.toFixed(1) }}%</p>
      <p class="mt-2 text-xs text-slate-500">Aggregated from the upstream integrations health snapshot.</p>
    </Card>
    <Card subtitle="Realtime Success (Last 24h)" bgClass="bg-slate-50">
      <p class="mt-4 text-4xl font-semibold text-slate-950">{{ realtimeSuccessRate.toFixed(1) }}%</p>
      <p class="mt-2 text-xs text-slate-500">Computed from completed realtime merge reports in the last 24 hours.</p>
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
