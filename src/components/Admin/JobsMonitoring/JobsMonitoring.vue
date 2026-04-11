<script setup lang="ts">
import { useQuery } from '@tanstack/vue-query';
import { getSyncRuns } from '@/api';
import Badge from '@/components/ui/Badge.vue';

const { data, isLoading, error } = useQuery({
  queryKey: ['syncRuns'],
  queryFn: () => getSyncRuns({ limit: 50 }).then((res) => res.data),
  refetchInterval: 5000,
});

const getStatusTone = (status: string) => {
  switch (status.toLowerCase()) {
    case 'completed': return 'emerald';
    case 'running': return 'blue';
    case 'failed': return 'rose';
    default: return 'amber';
  }
};
</script>

<template>
  <div class="px-8 py-8 w-full max-w-7xl mx-auto">
    <div class="mb-8 flex items-center justify-between">
      <div>
        <h1 class="text-2xl font-semibold tracking-tight text-slate-950">Background Jobs</h1>
        <p class="mt-1 text-sm text-slate-500">Monitor bulk school syncs and historical backfill queue status.</p>
      </div>
    </div>

    <div v-if="isLoading" class="text-sm text-slate-500">Loading jobs...</div>
    <div v-else-if="error" class="text-sm text-rose-500">Failed to load job history.</div>
    
    <div v-else class="rounded-[24px] border border-slate-200 bg-white overflow-hidden shadow-sm">
      <table class="w-full text-left text-sm">
        <thead class="bg-slate-50 border-b border-slate-200">
          <tr>
            <th class="px-6 py-4 font-semibold text-slate-600">ID / Date</th>
            <th class="px-6 py-4 font-semibold text-slate-600">Scope</th>
            <th class="px-6 py-4 font-semibold text-slate-600">Status</th>
            <th class="px-6 py-4 font-semibold text-slate-600">Details</th>
          </tr>
        </thead>
        <tbody class="divide-y divide-slate-100">
          <tr v-for="run in data?.syncRuns" :key="run.jobId" class="hover:bg-slate-50 transition">
            <td class="px-6 py-4">
              <div class="font-medium text-slate-900">{{ run.jobId.slice(0, 8) }}...</div>
              <div class="text-xs text-slate-500 mt-1">{{ new Date(run.attemptedAt).toLocaleString() }}</div>
            </td>
            <td class="px-6 py-4">
              <div class="font-medium text-slate-900 capitalize">{{ run.scope.replace('-', ' ') }}</div>
              <div v-if="run.school" class="text-xs text-slate-500 mt-1">Target: {{ run.school }}</div>
            </td>
            <td class="px-6 py-4">
              <Badge :tone="getStatusTone(run.status)">{{ run.status }}</Badge>
              <div v-if="run.finishedAt" class="text-xs text-slate-500 mt-1 pl-1">
                {{ ((new Date(run.finishedAt).getTime() - new Date(run.attemptedAt).getTime()) / 1000).toFixed(1) }}s
              </div>
            </td>
            <td class="px-6 py-4">
              <div v-if="run.errorMessage" class="text-xs text-rose-600 font-mono bg-rose-50 p-2 rounded max-w-md overflow-x-auto">
                {{ run.errorMessage }}
              </div>
              <div v-else class="text-xs text-slate-400">No errors</div>
            </td>
          </tr>
          <tr v-if="!data?.syncRuns?.length">
            <td colspan="4" class="px-6 py-8 text-center text-slate-500">No jobs found in history.</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
