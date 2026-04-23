<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { useQuery, useQueryClient } from '@tanstack/vue-query';
import { addSchoolExclusion, getSchedulerSettings, getSchools, getSyncStatus, removeSchoolExclusion, triggerHistoryBackfill, triggerSync, updateSchedulerSettings } from '@/api';
import Badge from '@/components/ui/Badge.vue';
import { isStaticDataMode } from '@/config/runtime';
import type { ExcludedSchoolOption, SchoolOption } from '@/types/clientHealth';
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
const defaultEndDate = new Date();
const defaultStartDate = new Date(defaultEndDate);
defaultStartDate.setDate(defaultStartDate.getDate() - 13);
const allSchoolsSelected = ref(true);
const selectedSchools = ref<string[]>([]);
const schoolSearch = ref('');
const startDate = ref(defaultStartDate.toISOString().slice(0, 10));
const endDate = ref(defaultEndDate.toISOString().slice(0, 10));
const activeMode = ref<OperationMode | null>(null);
const statusMessage = ref<string | null>(null);
const operationError = ref<string | null>(null);
const completedJobs = ref<OperationJobStatus[]>([]);
const currentJob = ref<OperationJobStatus | null>(null);
const queueProgress = ref<{ completed: number; total: number } | null>(null);
const exclusionError = ref<string | null>(null);
const exclusionMutationSchool = ref<string | null>(null);
const schedulerSaveState = ref<'idle' | 'saving' | 'saved'>('idle');
const schedulerMessage = ref<string | null>(null);
const schedulerError = ref<string | null>(null);
const schedulerEnabled = ref(true);
const schedulerTime = ref('07:30');

const { data, isLoading, error } = useQuery({
  queryKey: ['schools'],
  queryFn: () => getSchools().then((res) => res.data),
  enabled: true,
});

const { data: schedulerSettings, isLoading: isLoadingSchedulerSettings } = useQuery({
  queryKey: ['schedulerSettings'],
  queryFn: () => getSchedulerSettings().then((res) => res.data),
  enabled: !isStaticDataMode,
});

const schools = computed<SchoolOption[]>(() => data.value?.schools ?? []);
const excludedSchools = computed<ExcludedSchoolOption[]>(() => data.value?.excludedSchools ?? []);
const excludedTerms = computed<string[]>(() => data.value?.excludedTerms ?? []);
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
const manualExcludedSchools = computed(() =>
  excludedSchools.value.filter((school) => school.reason === 'Manually excluded in Operations')
);
const termExcludedSchools = computed(() =>
  excludedSchools.value.filter((school) => school.reason !== 'Manually excluded in Operations')
);
const schedulerStatusTone = computed<'emerald' | 'slate'>(() => (schedulerEnabled.value ? 'emerald' : 'slate'));
const schedulerStatusLabel = computed(() => (schedulerEnabled.value ? 'Enabled' : 'Disabled'));
const hasSchedulerChanges = computed(() => (
  schedulerEnabled.value !== (schedulerSettings.value?.syncEnabled ?? true) ||
  schedulerTime.value !== (schedulerSettings.value?.syncTime ?? '07:30')
));

watch(
  schedulerSettings,
  (value) => {
    if (!value) return;
    schedulerEnabled.value = value.syncEnabled;
    schedulerTime.value = value.syncTime;
  },
  { immediate: true }
);

function getCompactReason(reason?: string | null) {
  if (!reason) {
    return null;
  }

  return reason === 'Manually excluded in Operations' ? 'Manual exclusion' : reason;
}

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

async function saveSchedulerConfiguration() {
  schedulerError.value = null;
  schedulerMessage.value = null;
  schedulerSaveState.value = 'saving';

  try {
    const result = await updateSchedulerSettings({
      syncEnabled: schedulerEnabled.value,
      syncTime: schedulerTime.value,
    });
    schedulerEnabled.value = result.data.syncEnabled;
    schedulerTime.value = result.data.syncTime;
    await queryClient.invalidateQueries({ queryKey: ['schedulerSettings'] });
    schedulerMessage.value = result.data.syncEnabled
      ? `Daily sync enabled for ${result.data.syncTime} America/New_York.`
      : 'Daily sync disabled.';
    schedulerSaveState.value = 'saved';
  } catch (e: any) {
    schedulerError.value = e?.response?.data?.detail || e?.message || 'Failed to save daily sync settings';
    schedulerSaveState.value = 'idle';
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

async function refreshSchoolCatalog() {
  await queryClient.invalidateQueries({ queryKey: ['schools'] });
}

async function excludeSchool(school: string) {
  exclusionError.value = null;
  exclusionMutationSchool.value = school;
  try {
    await addSchoolExclusion({ school });
    selectedSchools.value = selectedSchools.value.filter((value) => value !== school);
    if (selectedSchools.value.length === 0) {
      allSchoolsSelected.value = true;
    }
    await refreshSchoolCatalog();
  } catch (e: any) {
    exclusionError.value = e?.response?.data?.detail || e?.message || 'Failed to exclude school';
  } finally {
    exclusionMutationSchool.value = null;
  }
}

async function unexcludeSchool(school: string) {
  exclusionError.value = null;
  exclusionMutationSchool.value = school;
  try {
    await removeSchoolExclusion(school);
    await refreshSchoolCatalog();
  } catch (e: any) {
    exclusionError.value = e?.response?.data?.detail || e?.message || 'Failed to remove school exclusion';
  } finally {
    exclusionMutationSchool.value = null;
  }
}
</script>

<template>
  <div class="w-full bg-slate-50 text-slate-900">
    <div class="w-full px-4 py-6 sm:px-6 lg:px-8 xl:px-10 2xl:px-12">
      <div v-if="isStaticDataMode" class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
        <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Operations</p>
        <h1 class="mt-3 text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">Live admin controls stay in the private dashboard</h1>
        <p class="mt-4 max-w-3xl text-base leading-7 text-slate-600">
          This GitHub Pages build is a read-only snapshot. Sync triggers, historical backfills, scheduler changes,
          and school exclusion management require the live backend and are intentionally disabled here.
        </p>
        <div class="mt-6 rounded-2xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-900">
          Use the live internal deployment when you need to run or manage operations.
        </div>
      </div>
      <div v-if="isStaticDataMode" class="mt-8 grid gap-6 2xl:grid-cols-[minmax(0,1.35fr)_minmax(20rem,0.65fr)]">
        <section class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
          <div class="flex flex-col gap-6">
            <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div>
                <h2 class="text-xl font-semibold text-slate-950">Excluded schools</h2>
                <p class="mt-2 text-sm text-slate-500">
                  Default term filters: {{ excludedTerms.join(', ') }}.
                  The static snapshot includes the current excluded-school list for reference.
                </p>
              </div>
              <p class="text-sm font-medium text-slate-900">{{ excludedSchools.length }} excluded</p>
            </div>

            <div class="grid gap-4 xl:grid-cols-2 2xl:grid-cols-3">
              <div
                v-for="school in manualExcludedSchools"
                :key="`static-manual-${school.school}`"
                class="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4"
              >
                <p class="truncate text-sm font-semibold text-slate-900">{{ formatSchoolLabel(school.school, school.displayName) }}</p>
                <p class="mt-1 truncate text-xs text-slate-500">
                  <span class="font-medium text-slate-600">{{ school.school }}</span>
                  <span v-if="getCompactReason(school.reason)"> · {{ getCompactReason(school.reason) }}</span>
                </p>
              </div>

              <p v-if="manualExcludedSchools.length === 0" class="rounded-2xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
                No manual exclusions are present in the static snapshot.
              </p>
            </div>

            <div>
              <p class="text-xs font-medium uppercase tracking-[0.18em] text-slate-500">Excluded by default rules</p>
              <div class="mt-3 grid max-h-80 gap-2 overflow-y-auto pr-1">
                <div
                  v-for="school in termExcludedSchools"
                  :key="`static-rule-${school.school}`"
                  class="rounded-xl border border-slate-200 bg-white px-4 py-3"
                >
                  <p class="truncate text-sm font-semibold text-slate-900">{{ formatSchoolLabel(school.school, school.displayName) }}</p>
                  <p class="mt-1 truncate text-xs text-slate-500">
                    <span class="font-medium text-slate-600">{{ school.school }}</span>
                    <span v-if="getCompactReason(school.reason)"> · {{ getCompactReason(school.reason) }}</span>
                  </p>
                </div>
                <p v-if="termExcludedSchools.length === 0" class="rounded-xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
                  No schools currently match the default exclusion rules.
                </p>
              </div>
            </div>
          </div>
        </section>

        <aside class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
          <h2 class="text-xl font-semibold text-slate-950">Read-only snapshot</h2>
          <p class="mt-2 text-sm leading-7 text-slate-600">
            The static site mirrors the current exclusion list for visibility only. To add or remove exclusions,
            use the private Operations dashboard.
          </p>
        </aside>
      </div>
      <template v-else>
      <div class="mb-8 rounded-[28px] border border-slate-200 bg-white/95 p-8 shadow-sm">
        <div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">Operations</p>
            <h1 class="mt-3 text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">Manual Sync & Backfill</h1>
            <p class="mt-4 max-w-3xl text-base leading-7 text-slate-600">
              Run manual syncs or historical backfills without cluttering the analytics views. Bulk runs target all schools;
              custom runs process selected schools one at a time. Excluded schools are skipped from the selectable list and bulk runs.
            </p>
          </div>
          <div class="rounded-2xl border border-slate-200 bg-slate-50 px-5 py-4 text-sm text-slate-600">
            <p class="font-medium text-slate-900">{{ selectedSchoolLabels }}</p>
            <p class="mt-1">Backfill range: {{ startDate }} to {{ endDate }}</p>
          </div>
        </div>
      </div>

      <div class="grid gap-8 2xl:grid-cols-[minmax(0,1.15fr)_minmax(24rem,0.85fr)]">
        <section class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
          <div class="flex flex-col gap-6">
            <div>
              <h2 class="text-xl font-semibold text-slate-950">Scope</h2>
              <p class="mt-2 text-sm text-slate-500">Choose all schools or build a custom selection for targeted sync and backfill runs.</p>
            </div>

            <label class="flex items-center gap-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
              <input
                id="operations-all-schools"
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
                  class="flex items-start justify-between gap-3 rounded-xl border border-slate-100 px-3 py-3 transition hover:border-slate-200 hover:bg-slate-50"
                >
                  <div class="flex min-w-0 items-start gap-3">
                    <input
                      :id="`school-select-${school.school}`"
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
                  </div>
                  <button
                    type="button"
                    class="rounded-full border border-slate-200 px-3 py-1.5 text-xs font-semibold text-slate-600 transition hover:border-slate-300 hover:bg-white disabled:cursor-not-allowed disabled:opacity-50"
                    :disabled="isRunning || exclusionMutationSchool === school.school"
                    @click.prevent="excludeSchool(school.school)"
                  >
                    <span v-if="exclusionMutationSchool === school.school">Excluding…</span>
                    <span v-else>Exclude</span>
                  </button>
                </label>
                <p v-if="filteredSchools.length === 0" class="rounded-xl border border-dashed border-slate-200 px-4 py-6 text-sm text-slate-500">
                  No schools match that search.
                </p>
              </div>
            </div>

            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-5">
              <div class="flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
                <div>
                  <h3 class="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">Excluded schools</h3>
                  <p class="mt-2 text-sm text-slate-600">
                    Default term filters: {{ excludedTerms.join(', ') }}.
                    Additional exclusions are stored locally and skipped by bulk operations.
                  </p>
                </div>
                <p class="text-sm font-medium text-slate-900">{{ excludedSchools.length }} excluded</p>
              </div>

              <div class="mt-4 grid gap-4 2xl:grid-cols-[minmax(0,1.35fr)_minmax(20rem,0.65fr)]">
                <div>
                  <p class="text-xs font-medium uppercase tracking-[0.18em] text-slate-500">Manual exclusions</p>
                  <div class="mt-3 grid gap-2 xl:grid-cols-2 2xl:grid-cols-3">
                    <div
                      v-for="school in manualExcludedSchools"
                      :key="`manual-${school.school}`"
                      class="flex items-start justify-between gap-3 rounded-xl border border-slate-200 bg-white px-4 py-3"
                    >
                      <div class="min-w-0 flex-1">
                        <p class="truncate text-sm font-semibold text-slate-900">{{ formatSchoolLabel(school.school, school.displayName) }}</p>
                        <p class="mt-1 truncate text-xs text-slate-500">
                          <span class="font-medium text-slate-600">{{ school.school }}</span>
                          <span v-if="getCompactReason(school.reason)"> · {{ getCompactReason(school.reason) }}</span>
                        </p>
                      </div>
                      <button
                        type="button"
                        class="shrink-0 rounded-full border border-slate-200 px-3 py-1.5 text-xs font-semibold text-slate-600 transition hover:border-slate-300 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
                        :disabled="isRunning || exclusionMutationSchool === school.school"
                        @click="unexcludeSchool(school.school)"
                      >
                        <span v-if="exclusionMutationSchool === school.school">Updating…</span>
                        <span v-else>Unexclude</span>
                      </button>
                    </div>
                    <p v-if="manualExcludedSchools.length === 0" class="rounded-xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
                      No manual exclusions yet. Use the Exclude button next to a school to add one.
                    </p>
                  </div>
                </div>

                <div>
                  <p class="text-xs font-medium uppercase tracking-[0.18em] text-slate-500">Excluded by default rules</p>
                  <div class="mt-3 grid max-h-80 gap-2 overflow-y-auto pr-1">
                    <div
                      v-for="school in termExcludedSchools"
                      :key="`rule-${school.school}`"
                      class="rounded-xl border border-slate-200 bg-white px-4 py-3"
                    >
                      <p class="truncate text-sm font-semibold text-slate-900">{{ formatSchoolLabel(school.school, school.displayName) }}</p>
                      <p class="mt-1 truncate text-xs text-slate-500">
                        <span class="font-medium text-slate-600">{{ school.school }}</span>
                        <span v-if="getCompactReason(school.reason)"> · {{ getCompactReason(school.reason) }}</span>
                      </p>
                    </div>
                    <p v-if="termExcludedSchools.length === 0" class="rounded-xl border border-dashed border-slate-200 px-4 py-5 text-sm text-slate-500">
                      No schools currently match the default exclusion rules.
                    </p>
                  </div>
                </div>
              </div>

              <p v-if="exclusionError" class="mt-4 text-sm text-rose-500">{{ exclusionError }}</p>
            </div>
          </div>
        </section>

        <section class="rounded-[28px] border border-slate-200 bg-white p-8 shadow-sm">
          <div class="flex flex-col gap-6">
            <div>
              <h2 class="text-xl font-semibold text-slate-950">Actions</h2>
              <p class="mt-2 text-sm text-slate-500">Sync refreshes the latest snapshot. Backfill writes historical daily snapshots for the selected inclusive range.</p>
            </div>

            <div class="rounded-2xl border border-slate-200 bg-slate-50 p-5">
              <div class="flex flex-col gap-5">
                <div class="flex flex-wrap items-center gap-2">
                  <h3 class="text-sm font-semibold uppercase tracking-[0.2em] text-slate-500">Daily sync</h3>
                  <Badge :tone="schedulerStatusTone" :label="schedulerStatusLabel" />
                </div>
                <p class="text-sm text-slate-600">Control the automatic daily sync schedule for the backend service in America/New_York.</p>

                <div v-if="isLoadingSchedulerSettings" class="text-sm text-slate-500">Loading daily sync settings...</div>
                <div v-else class="grid gap-4 sm:grid-cols-[minmax(0,1fr)_12rem_auto] sm:items-end">
                  <label class="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white px-4 py-3">
                    <input
                      id="daily-sync-enabled"
                      v-model="schedulerEnabled"
                      type="checkbox"
                      class="h-4 w-4 rounded border-slate-300 text-slate-900 focus:ring-slate-500"
                      :disabled="schedulerSaveState === 'saving' || isRunning"
                    />
                    <span class="font-medium text-slate-900">Enable daily sync</span>
                  </label>

                  <div>
                    <label for="daily-sync-time" class="text-xs font-medium uppercase tracking-[0.18em] text-slate-500">Time</label>
                    <input
                      id="daily-sync-time"
                      v-model="schedulerTime"
                      type="time"
                      step="60"
                      :disabled="schedulerSaveState === 'saving' || isRunning"
                      class="mt-2 w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-900 focus:border-slate-400 focus:outline-none disabled:opacity-50"
                    />
                  </div>

                  <button
                    type="button"
                    :disabled="schedulerSaveState === 'saving' || isRunning || !hasSchedulerChanges"
                    class="rounded-full bg-slate-900 px-5 py-3 text-sm font-semibold text-white shadow-sm transition hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-50"
                    @click="saveSchedulerConfiguration"
                  >
                    <span v-if="schedulerSaveState === 'saving'">Saving…</span>
                    <span v-else>Save</span>
                  </button>
                </div>
              </div>

              <p v-if="schedulerMessage" class="mt-4 text-sm text-slate-600">{{ schedulerMessage }}</p>
              <p v-if="schedulerError" class="mt-2 text-sm text-rose-500">{{ schedulerError }}</p>
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
      </template>
    </div>
  </div>
</template>
