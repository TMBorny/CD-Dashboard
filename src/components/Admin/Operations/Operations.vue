<script setup lang="ts">
import { computed, ref } from 'vue';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { getSchools, getSyncStatus, triggerHistoryBackfill, triggerSync } from '@/api';
import type { SchoolOption } from '@/types/clientHealth';
import { formatSchoolLabel } from '@/utils/schoolNames';

type OperationMode = 'sync' | 'backfill';
type OperationJobStatus = {
  school: string | null;
  status: string;
  totalSec?: number;
  startedAt?: string | null;
  finishedAt?: string | null;
  schoolsProcessed?: number;
  totalSchools?: number;
  errors?: string[];
  alreadyRunning?: boolean;
};

const queryClient = useQueryClient();
const allSchoolsSelected = ref(true);
const selectedSchools = ref<string[]>([]);
const schoolSearch = ref('');
const startDate = ref('2026-01-01');
const endDate = ref(new Date().toISOString().slice(0, 10));
const activeMode = ref<OperationMode | null>(null);
const statusMessage = ref<string | null>(null);
const operationError = ref<string | null>(null);
const completedJobs = ref<OperationJobStatus[]>([]);
const currentJob = ref<OperationJobStatus | null>(null);
const queueProgress = ref<{ completed: number; total: number } | null>(null);

const { data, isLoading, error } = useQuery({
  queryKey: ['schools'],
  queryFn: () => getSchools().then((res) => res.data),
});

const schools = computed<SchoolOption[]>(() => data.value?.schools ?? []);
const sortedSchools = computed(() =>
  [...schools.value].sort((a, b) =>
    formatSchoolLabel(a.school, a.displayName).localeCompare(formatSchoolLabel(b.school, b.displayName))
  )
);
const filteredSchools = computed(() => {
  const search = schoolSearch.value.trim().toLowerCase();
  if (!search) {
    return sortedSchools.value;
  }

  return sortedSchools.value.filter((school) => {
    const label = formatSchoolLabel(school.school, school.displayName).toLowerCase();
    return label.includes(search) || school.school.toLowerCase().includes(search);
  });
});

const selectedSchoolLabels = computed(() => {
  if (allSchoolsSelected.value) {
    return 'All schools';
  }

  if (selectedSchools.value.length === 0) {
    return 'No schools selected';
  }

  return `${selectedSchools.value.length} school${selectedSchools.value.length === 1 ? '' : 's'} selected`;
});

const invalidDateRange = computed(() => endDate.value < startDate.value);
const isRunning = computed(() => activeMode.value !== null);
const canRunSync = computed(() => !isRunning.value && (allSchoolsSelected.value || selectedSchools.value.length > 0));
const canRunBackfill = computed(() =>
  !isRunning.value &&
  !invalidDateRange.value &&
  (allSchoolsSelected.value || selectedSchools.value.length > 0)
);

function formatTimestamp(value?: string | null) {
  if (!value) {
    return 'Not available';
  }

  return new Date(value).toLocaleString();
}

function getElapsedSeconds(job: OperationJobStatus) {
  if (typeof job.totalSec === 'number') {
    return job.totalSec;
  }

  if (job.startedAt && job.finishedAt) {
    const started = new Date(job.startedAt).getTime();
    const finished = new Date(job.finishedAt).getTime();
    return Number(((finished - started) / 1000).toFixed(1));
  }

  return null;
}

function onToggleAllSchools() {
  if (allSchoolsSelected.value) {
    selectedSchools.value = [];
  }
}

function toggleSchool(school: string, checked: boolean | undefined) {
  if (checked) {
    allSchoolsSelected.value = false;
    if (!selectedSchools.value.includes(school)) {
      selectedSchools.value = [...selectedSchools.value, school];
    }
    return;
  }

  selectedSchools.value = selectedSchools.value.filter((value) => value !== school);
  if (selectedSchools.value.length === 0) {
    allSchoolsSelected.value = true;
  }
}

function getTargetSchools() {
  if (allSchoolsSelected.value) {
    return [];
  }

  return [...selectedSchools.value];
}

async function pollJob(jobId: string, school: string | null) {
  while (true) {
    const status = await getSyncStatus(jobId);
    currentJob.value = {
      school,
      status: status.status,
      totalSec: status.timing?.totalSec,
      startedAt: status.startedAt,
      finishedAt: status.finishedAt,
      schoolsProcessed: status.schoolsProcessed,
      totalSchools: status.totalSchools,
      errors: status.errors,
      alreadyRunning: status.alreadyRunning,
    };

    if (status.status === 'completed') {
      completedJobs.value = [...completedJobs.value, currentJob.value];
      return;
    }

    if (status.status === 'failed') {
      throw new Error(status.error || 'Job failed');
    }

    await new Promise((resolve) => window.setTimeout(resolve, 1500));
  }
}

async function invalidateDataQueries() {
  await Promise.all([
    queryClient.invalidateQueries({ queryKey: ['clientHealth'] }),
    queryClient.invalidateQueries({ queryKey: ['clientHealthHistory'] }),
    queryClient.invalidateQueries({ queryKey: ['clientHealthSyncMetadata'] }),
    queryClient.invalidateQueries({ queryKey: ['clientHealthActiveUsers'] }),
    queryClient.invalidateQueries({ queryKey: ['syncRuns'] }),
  ]);
}

async function runOperation(mode: OperationMode) {
  activeMode.value = mode;
  statusMessage.value = null;
  operationError.value = null;
  completedJobs.value = [];
  currentJob.value = null;

  const targets = getTargetSchools();
  const schoolQueue = targets.length > 0 ? targets : [null];
  queueProgress.value = { completed: 0, total: schoolQueue.length };

  try {
    for (const school of schoolQueue) {
      const schoolLabel = school
        ? formatSchoolLabel(
            school,
            schools.value.find((candidate) => candidate.school === school)?.displayName
          )
        : 'all schools';

      statusMessage.value = mode === 'sync'
        ? `Starting sync for ${schoolLabel}`
        : `Starting backfill for ${schoolLabel}`;

      const result = mode === 'sync'
        ? await triggerSync(school ? { school } : undefined)
        : await triggerHistoryBackfill({
            startDate: startDate.value,
            endDate: endDate.value,
            ...(school ? { school } : {}),
          });

      await pollJob(result.jobId, school);
      queueProgress.value = {
        completed: queueProgress.value.completed + 1,
        total: schoolQueue.length,
      };
    }

    await invalidateDataQueries();
    statusMessage.value = mode === 'sync'
      ? 'Sync completed successfully.'
      : 'Backfill completed successfully.';
  } catch (e: any) {
    operationError.value = e?.response?.data?.detail || e?.message || 'Operation failed';
  } finally {
    activeMode.value = null;
    currentJob.value = null;
  }
}

async function handleSync() {
  await runOperation('sync');
}

async function handleBackfill() {
  if (invalidDateRange.value) {
    operationError.value = 'End date must be on or after the start date.';
    return;
  }

  await runOperation('backfill');
}
</script>

<template>
  <div class="w-full bg-slate-50 text-slate-900">
    <div class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      <div class="mb-8 rounded-[28px] border border-slate-200 bg-white/95 p-8 shadow-sm">
        <div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Operations</p>
            <h1 class="mt-3 text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">Manual Sync & Backfill</h1>
            <p class="mt-4 max-w-3xl text-base leading-7 text-slate-600">
              Run manual syncs or historical backfills without cluttering the analytics views. Bulk runs target all schools;
              custom runs process selected schools one at a time.
            </p>
          </div>
          <div class="rounded-2xl border border-slate-200 bg-slate-50 px-5 py-4 text-sm text-slate-600">
            <p class="font-medium text-slate-900">{{ selectedSchoolLabels }}</p>
            <p class="mt-1">Backfill range: {{ startDate }} to {{ endDate }}</p>
          </div>
        </div>
      </div>

      <div class="grid gap-8 lg:grid-cols-[1.1fr_0.9fr]">
        <section class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
          <div class="flex flex-col gap-6">
            <div>
              <h2 class="text-xl font-semibold text-slate-950">Scope</h2>
              <p class="mt-2 text-sm text-slate-500">Choose all schools or build a custom selection for targeted sync and backfill runs.</p>
            </div>

            <label class="flex items-center gap-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
              <input
                v-model="allSchoolsSelected"
                type="checkbox"
                class="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-500"
                :disabled="isRunning"
                @change="onToggleAllSchools"
              />
              <span class="font-medium text-slate-900">All schools</span>
            </label>

            <div class="rounded-2xl border border-slate-200">
              <div class="border-b border-slate-200 px-4 py-3">
                <h3 class="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">School selection</h3>
              </div>
              <div class="border-b border-slate-200 px-4 py-3">
                <label for="school-search" class="sr-only">Search schools</label>
                <input
                  id="school-search"
                  v-model="schoolSearch"
                  type="search"
                  placeholder="Search by school name or slug"
                  :disabled="isRunning"
                  class="w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-900 focus:border-slate-400 focus:outline-none disabled:opacity-50"
                />
              </div>
              <div v-if="isLoading" class="px-4 py-6 text-sm text-slate-500">Loading schools...</div>
              <div v-else-if="error" class="px-4 py-6 text-sm text-rose-500">Failed to load schools.</div>
              <div v-else class="max-h-[420px] space-y-2 overflow-y-auto px-4 py-4">
                <label
                  v-for="school in filteredSchools"
                  :key="school.school"
                  class="flex items-start gap-3 rounded-xl border border-slate-100 px-3 py-3 transition hover:border-slate-200 hover:bg-slate-50"
                >
                  <input
                    :checked="selectedSchools.includes(school.school)"
                    type="checkbox"
                    class="mt-1 h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-500"
                    :disabled="allSchoolsSelected || isRunning"
                    @change="toggleSchool(school.school, ($event.target as HTMLInputElement).checked)"
                  />
                  <div class="min-w-0">
                    <p class="font-medium text-slate-900">{{ formatSchoolLabel(school.school, school.displayName) }}</p>
                    <p class="mt-1 text-xs text-slate-500">{{ school.school }}</p>
                  </div>
                </label>
                <p v-if="filteredSchools.length === 0" class="rounded-xl border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">
                  No schools match that search.
                </p>
              </div>
            </div>
          </div>
        </section>

        <section class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
          <div class="flex flex-col gap-6">
            <div>
              <h2 class="text-xl font-semibold text-slate-950">Actions</h2>
              <p class="mt-2 text-sm text-slate-500">Sync refreshes the latest snapshot. Backfill writes historical daily snapshots for the selected inclusive range.</p>
            </div>

            <div class="grid gap-4 sm:grid-cols-2">
              <div>
                <label for="operations-start-date" class="text-xs font-medium uppercase tracking-[0.18em] text-slate-500">Start Date</label>
                <input
                  id="operations-start-date"
                  v-model="startDate"
                  type="date"
                  :disabled="isRunning"
                  class="mt-2 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-900 focus:border-slate-400 focus:outline-none disabled:opacity-50"
                />
              </div>
              <div>
                <label for="operations-end-date" class="text-xs font-medium uppercase tracking-[0.18em] text-slate-500">End Date</label>
                <input
                  id="operations-end-date"
                  v-model="endDate"
                  type="date"
                  :disabled="isRunning"
                  class="mt-2 w-full rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-900 focus:border-slate-400 focus:outline-none disabled:opacity-50"
                />
              </div>
            </div>

            <p v-if="invalidDateRange" class="text-sm text-rose-500">End date must be on or after the start date.</p>

            <div class="flex flex-col gap-3 sm:flex-row">
              <div class="group relative">
                <button
                  type="button"
                  :disabled="!canRunSync"
                  class="rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white shadow-sm transition hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-50"
                  @click="handleSync"
                >
                  <span v-if="activeMode === 'sync'">Running Sync…</span>
                  <span v-else>Run Sync</span>
                </button>
                <div class="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-72 -translate-x-1/2 rounded-2xl border border-slate-200 bg-white p-3 text-[11px] leading-5 text-slate-600 shadow-lg group-hover:block">
                  Refreshes the latest snapshot only. Use this when you want current-state data for today without rebuilding older history.
                </div>
              </div>
              <div class="group relative">
                <button
                  type="button"
                  :disabled="!canRunBackfill"
                  class="rounded-full border border-slate-300 bg-white px-5 py-3 text-sm font-semibold text-slate-900 shadow-sm transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
                  @click="handleBackfill"
                >
                  <span v-if="activeMode === 'backfill'">Running Backfill…</span>
                  <span v-else>Run Backfill</span>
                </button>
                <div class="pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-72 -translate-x-1/2 rounded-2xl border border-slate-200 bg-white p-3 text-[11px] leading-5 text-slate-600 shadow-lg group-hover:block">
                  Writes historical daily snapshots across the selected inclusive date range. Use this to populate or repair trend history.
                </div>
              </div>
            </div>

            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-5">
              <h3 class="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">Run status</h3>
              <p v-if="statusMessage" class="mt-3 text-sm text-slate-700">{{ statusMessage }}</p>
              <p v-if="queueProgress" class="mt-2 text-sm text-slate-500">
                Queue progress: {{ queueProgress.completed }} / {{ queueProgress.total }}
              </p>
              <div v-if="currentJob" class="mt-4 rounded-xl border border-slate-200 bg-white p-4">
                <p class="font-medium text-slate-900">
                  {{ currentJob.school ? formatSchoolLabel(currentJob.school) : 'All schools' }}
                </p>
                <p class="mt-1 text-sm text-slate-600">Status: {{ currentJob.status }}</p>
                <p class="mt-1 text-xs text-slate-500">Started: {{ formatTimestamp(currentJob.startedAt) }}</p>
                <p class="mt-1 text-xs text-slate-500">Ended: {{ formatTimestamp(currentJob.finishedAt) }}</p>
                <p v-if="getElapsedSeconds(currentJob) !== null" class="mt-1 text-xs text-slate-500">
                  Total time: {{ getElapsedSeconds(currentJob) }}s
                </p>
                <p v-if="currentJob.totalSchools" class="mt-1 text-xs text-slate-500">
                  {{ currentJob.schoolsProcessed ?? 0 }} / {{ currentJob.totalSchools }} processed
                </p>
                <p v-if="currentJob.errors?.length" class="mt-2 text-xs text-amber-600">
                  {{ currentJob.errors.slice(0, 2).join(' | ') }}
                </p>
              </div>
              <div v-if="completedJobs.length" class="mt-4 space-y-3">
                <div
                  v-for="job in completedJobs"
                  :key="`${job.school ?? 'all'}-${job.totalSec ?? 0}-${job.status}`"
                  class="rounded-xl border border-emerald-200 bg-emerald-50 p-4 text-sm"
                >
                  <p class="font-medium text-emerald-800">
                    {{ job.school ? formatSchoolLabel(job.school) : 'All schools' }}
                  </p>
                  <p class="mt-1 text-emerald-700">Started: {{ formatTimestamp(job.startedAt) }}</p>
                  <p class="mt-1 text-emerald-700">Ended: {{ formatTimestamp(job.finishedAt) }}</p>
                  <p v-if="getElapsedSeconds(job) !== null" class="mt-1 text-emerald-700">
                    Total time: {{ getElapsedSeconds(job) }}s
                  </p>
                </div>
              </div>
              <p v-if="operationError" class="mt-4 text-sm text-rose-500">{{ operationError }}</p>
            </div>
          </div>
        </section>
      </div>
    </div>
  </div>
</template>
