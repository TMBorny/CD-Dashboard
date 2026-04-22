<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { useRoute } from 'vue-router';
import { useQuery } from '@tanstack/vue-query';
import { getClientHealthHistory, getClientHealthActiveUsers, getClientHealthSyncMetadata, getErrorAnalysis, getErrorAnalysisErrors } from '@/api';
import type { FailedMerge } from '@/types/clientHealth';
import type { ErrorAnalysisResponse, ErrorDetailRow, ErrorDetailTableResponse, ErrorSignatureCluster, MergeReportReference, ResolutionHint } from '@/types/errorAnalysis';
import VueApexCharts from 'vue3-apexcharts';
import Card from '@/components/ui/Card.vue';
import { useChartOptions, useStackedBarChartOptions } from '@/composables/useChartOptions';
import { formatLocalDateTime, getLocalTimeZoneLabel } from '@/utils/dateTime';
import { formatSchoolLabel } from '@/utils/schoolNames';

const route = useRoute();
const school = computed(() => String(route.params.school ?? ''));
const coursedogBaseUrl = (import.meta.env.VITE_COURSEDOG_PRD_URL?.trim() || 'https://app.coursedog.com').replace(/\/+$/, '');
const integrationHubUrl = computed(() => `${coursedogBaseUrl}/#/int/${school.value}`);
const mergeReportsUrl = computed(() => `${coursedogBaseUrl}/#/int/${school.value}/merge-history`);
const localTimeZoneLabel = getLocalTimeZoneLabel();

const { data: history, isLoading: isLoadingHistory, error: historyError } = useQuery({
  queryKey: computed(() => ['clientHealthHistory', school.value]),
  queryFn: () => getClientHealthHistory({ school: school.value }).then((res) => res.data),
  enabled: computed(() => school.value.length > 0),
});

const { data: activeUsers, isLoading: isLoadingUsers, error: usersError } = useQuery({
  queryKey: computed(() => ['clientHealthActiveUsers', school.value]),
  queryFn: () => getClientHealthActiveUsers({ school: school.value }).then((res) => res.data),
  enabled: computed(() => school.value.length > 0),
});

const { data: syncMetadata, isLoading: isLoadingSyncMetadata } = useQuery({
  queryKey: computed(() => ['clientHealthSyncMetadata', school.value]),
  queryFn: () => getClientHealthSyncMetadata({ school: school.value }).then((res) => res.data),
  enabled: computed(() => school.value.length > 0),
});

const { data: currentErrorAnalysis, isLoading: isLoadingCurrentErrorAnalysis, error: currentErrorAnalysisError } = useQuery({
  queryKey: computed(() => ['clientHealthCurrentErrorAnalysis', school.value]),
  queryFn: () =>
    getErrorAnalysis({
      school: school.value,
      latestOnly: true,
    }).then((res) => res.data as ErrorAnalysisResponse),
  enabled: computed(() => school.value.length > 0),
});

const currentErrorPage = ref(1);
const currentErrorPageSize = 20;
const currentErrorCategoryFilter = ref('');
const currentErrorEntityTypeFilter = ref('');
const currentErrorSignatureFilter = ref('');

const { data: currentErrorRows, isLoading: isLoadingCurrentErrorRows, error: currentErrorRowsError } = useQuery({
  queryKey: computed(() => [
    'clientHealthCurrentErrorRows',
    school.value,
    currentErrorPage.value,
    currentErrorCategoryFilter.value,
    currentErrorEntityTypeFilter.value,
    currentErrorSignatureFilter.value,
  ]),
  queryFn: () =>
    getErrorAnalysisErrors({
      school: school.value,
      latestOnly: true,
      category: currentErrorCategoryFilter.value || undefined,
      entityType: currentErrorEntityTypeFilter.value || undefined,
      signature: currentErrorSignatureFilter.value || undefined,
      page: currentErrorPage.value,
      pageSize: currentErrorPageSize,
      sortBy: 'signatureLabel',
      sortDir: 'asc',
    }).then((res) => res.data as ErrorDetailTableResponse),
  enabled: computed(() => school.value.length > 0),
});

const loading = computed(() => isLoadingHistory.value || isLoadingUsers.value || isLoadingSyncMetadata.value);
const error = computed(() => historyError.value || usersError.value ? 'Failed to load data' : null);
const snapshotCount = computed(() => history.value?.snapshots?.length ?? 0);
const latestSnapshot = computed(() => {
  const snapshots = history.value?.snapshots ?? [];
  return snapshots.length > 0 ? snapshots[snapshots.length - 1] : null;
});
const schoolLabel = computed(() => formatSchoolLabel(school.value, latestSnapshot.value?.displayName));
const lastSuccessfulSyncLabel = computed(() => {
  const createdAt = syncMetadata.value?.lastSuccessfulSync?.createdAt;
  if (!createdAt) return 'No successful sync yet';
  return formatLocalDateTime(createdAt);
});
const lastAttemptedSyncLabel = computed(() => {
  const attemptedAt = syncMetadata.value?.lastAttemptedSync?.attemptedAt;
  if (!attemptedAt) return 'No attempted sync yet';
  return formatLocalDateTime(attemptedAt);
});
const lastAttemptStatusLabel = computed(() => {
  const status = syncMetadata.value?.lastAttemptedSync?.status;
  if (!status) return null;
  return status.charAt(0).toUpperCase() + status.slice(1);
});
const latestSnapshotCapturedAtLabel = computed(() => {
  const createdAt = latestSnapshot.value?.createdAt;
  return createdAt ? formatLocalDateTime(createdAt) : null;
});
const currentErrorSnapshotDate = computed(() => currentErrorAnalysis.value?.metadata.resolvedSnapshotDate ?? null);
const currentErrorSnapshotLabel = computed(() => {
  if (!currentErrorSnapshotDate.value) return 'No current detailed error snapshot captured yet.';
  return `Current detailed error snapshot: ${currentErrorSnapshotDate.value}`;
});
const currentErrorSignatures = computed(() => currentErrorAnalysis.value?.signatures ?? []);
const currentErrorDetailRows = computed(() => currentErrorRows.value?.rows ?? []);
const currentErrorDetailTotal = computed(() => currentErrorRows.value?.total ?? 0);
const currentErrorTotalPages = computed(() => Math.max(1, Math.ceil(currentErrorDetailTotal.value / currentErrorPageSize)));
const currentErrorTotals = computed(() => currentErrorAnalysis.value?.summary.totalErrorInstances ?? 0);
const currentErrorTab = ref<'categories' | 'signatures' | 'rows' | 'recent-failures'>('rows');
const selectedErrorDetail = ref<{
  title: string;
  signatureLabel: string;
  fullErrorText: string;
  entityType?: string | null;
  errorCode?: string | null;
  school?: string | null;
  schoolLabel?: string | null;
  sisPlatform?: string | null;
  termCode?: string | null;
  scheduleType?: string | null;
  entityDisplayName?: string | null;
  mergeReport?: MergeReportReference | null;
  rawPayload?: string | null;
  resolutionHint?: ResolutionHint | null;
} | null>(null);
const currentErrorCategories = computed(() => {
  const categories = new Map<string, { key: string; title: string; action: string; bucket: string; count: number; signatures: number }>();
  currentErrorSignatures.value.forEach((signature) => {
    const hint = signature.resolutionHint;
    const existing = categories.get(hint.title);
    if (existing) {
      existing.count += signature.totalCount;
      existing.signatures += 1;
      return;
    }
    categories.set(hint.title, {
      key: hint.title,
      title: hint.title,
      action: hint.action,
      bucket: hint.bucket,
      count: signature.totalCount,
      signatures: 1,
    });
  });

  return [...categories.values()].sort((left, right) => right.count - left.count || left.title.localeCompare(right.title));
});
const currentErrorCategoryOptions = computed(() =>
  currentErrorCategories.value.map((category) => ({
    value: category.bucket,
    label: category.title,
  })),
);
const currentErrorEntityTypeOptions = computed(() => {
  const entityTypes = new Map<string, string>();
  currentErrorSignatures.value.forEach((signature) => {
    const value = signature.entityType?.trim();
    if (!value || entityTypes.has(value)) return;
    entityTypes.set(value, value);
  });
  return [...entityTypes.entries()]
    .map(([value, label]) => ({ value, label }))
    .sort((left, right) => left.label.localeCompare(right.label));
});
const currentErrorSignatureOptions = computed(() =>
  currentErrorSignatures.value
    .map((signature) => ({
      value: signature.signatureKey,
      count: signature.totalCount,
      label: `(${signature.totalCount}) ${buildSignatureHeadline(signature.signatureLabel)}`,
    }))
    .sort((left, right) => right.count - left.count || left.label.localeCompare(right.label)),
);
const hasCurrentErrorContent = computed(() => currentErrorSignatures.value.length > 0 || currentErrorDetailRows.value.length > 0);
const recentFailedMerges = computed(() => (latestSnapshot.value?.recentFailedMerges as FailedMerge[] | undefined) ?? []);
const hasIssueTabsContent = computed(() => hasCurrentErrorContent.value || recentFailedMerges.value.length > 0);
const currentErrorTabs = computed(() => [
  { key: 'rows', label: `Errors (${currentErrorTotals.value})` },
  { key: 'categories', label: `Categories (${currentErrorCategories.value.length})` },
  { key: 'signatures', label: `Signatures (${currentErrorSignatures.value.length})` },
  { key: 'recent-failures', label: `Recent Failed Merges (${recentFailedMerges.value.length})` },
] as const);

const nightlySuccessChartSeries = computed(() => {
  if (!history.value) return [];
  const succeeded = history.value.snapshots.map((s: any) => s.merges.nightly.succeeded);
  const issues = history.value.snapshots.map((s: any) => s.merges.nightly.finishedWithIssues);
  const noData = history.value.snapshots.map((s: any) => s.merges.nightly.noData);
  const halted = history.value.snapshots.map((s: any) => s.merges.nightly.halted || 0);
  const failed = history.value.snapshots.map((s: any) => s.merges.nightly.failed);
  
  return [
    { name: 'Succeeded', data: succeeded },
    { name: 'Finished With Issues', data: issues },
    { name: 'No Data', data: noData },
    { name: 'Halted', data: halted },
    { name: 'Failed', data: failed }
  ];
});

const nightlySuccessChartOptions = computed(() => useStackedBarChartOptions({
  categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
  colors: ['#10b981', '#f59e0b', '#94a3b8', '#a16207', '#ef4444'],
}));

const nightlyDurationChartSeries = computed(() => {
  if (!history.value) return [];
  const data = history.value.snapshots.map((s: any) => parseFloat(((s.merges.nightly.mergeTimeMs || 0) / 3600000).toFixed(2)));
  return [{ name: 'Nightly Duration (hours)', data }];
});

const nightlyDurationChartOptions = computed(() => useChartOptions({
  colors: ['#8b5cf6'],
  categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
}));

const realtimeSuccessChartSeries = computed(() => {
  if (!history.value) return [];
  const succeeded = history.value.snapshots.map((s: any) => s.merges.realtime.succeeded);
  const issues = history.value.snapshots.map((s: any) => s.merges.realtime.finishedWithIssues);
  const noData = history.value.snapshots.map((s: any) => s.merges.realtime.noData);
  const failed = history.value.snapshots.map((s: any) => s.merges.realtime.failed);

  return [
    { name: 'Succeeded', data: succeeded },
    { name: 'Finished With Issues', data: issues },
    { name: 'No Data', data: noData },
    { name: 'Failed', data: failed }
  ];
});

const realtimeSuccessChartOptions = computed(() => useStackedBarChartOptions({
  categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
}));

const mergeErrorsChartSeries = computed(() => {
  if (!history.value) return [];
  const data = history.value.snapshots.map((s: any) => s.mergeErrorsCount ?? null);
  return [{ name: 'Open Merge Errors per Snapshot', data }];
});

const mergeErrorsChartOptions = computed(() => ({
  ...useChartOptions({
    colors: ['#ffcd56'],
    categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
  }),
  yaxis: {
    labels: {
      style: { colors: '#64748b', fontSize: '12px' },
      formatter: (value: number) => `${Math.round(value)}`,
    },
  },
  tooltip: {
    theme: 'light',
    y: {
      formatter: (value: number | undefined) =>
        typeof value === 'number' ? `${Math.round(value)}` : '',
    },
  },
}));

const activeUsersChartSeries = computed(() => {
  if (!history.value) return [];
  const data = history.value.snapshots.map((s: any) => s.activeUsers24h);
  return [{ name: 'Daily Active Sessions', data }];
});

const activeUsersChartOptions = computed(() => ({
  ...useChartOptions({
    colors: ['#36a2eb'],
    categories: history.value?.snapshots.map((s: any) => s.snapshotDate),
  }),
  yaxis: {
    labels: {
      style: { colors: '#64748b', fontSize: '12px' },
      formatter: (value: number) => `${Math.round(value)}`,
    },
  },
  tooltip: {
    theme: 'light',
    y: {
      formatter: (value: number | undefined) =>
        typeof value === 'number' ? `${Math.round(value)}` : '',
    },
  },
}));

const buildSignatureHeadline = (signatureLabel?: string | null) => {
  const normalized = (signatureLabel || '').replace(/\s+/g, ' ').trim();
  if (!normalized) return 'Unknown signature';
  const segments = normalized.split('|').map((segment) => segment.trim()).filter(Boolean);
  const [entity, errorCode, ...rest] = segments;
  const message = rest.join(' | ');
  return [entity, errorCode, message].filter(Boolean).join(' | ');
};

const getResolutionToneClass = (hint: ResolutionHint | { bucket: string }) => {
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

const getMergeReportUrl = (reference: { school: string; mergeReportId: string }) =>
  `${coursedogBaseUrl}/#/int/${reference.school}/merge-history/${reference.mergeReportId}`;

const getRecentFailedMergeReportUrl = (merge: FailedMerge) => {
  if (!merge.id) return null;
  return `${coursedogBaseUrl}/#/int/${school.value}/merge-history/${merge.id}`;
};

const getSignatureSummary = (signature: ErrorSignatureCluster) => {
  return [signature.entityType, signature.errorCode, signature.termCodes[0]].filter(Boolean).join(' • ');
};

const getRowSummary = (row: ErrorDetailRow) => {
  return [row.entityType, row.errorCode, row.termCodes[0], row.entityDisplayName].filter(Boolean).join(' • ');
};

const schoolRoute = (schoolId: string) => ({
  name: 'AdminClientHealthDetail',
  params: { school: schoolId },
});

const getIntegrationHubSchoolUrl = (schoolId: string) => `${coursedogBaseUrl}/#/int/${schoolId}`;

const stringifyPayload = (value: unknown) => {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return null;
  }
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  Boolean(value) && typeof value === 'object' && !Array.isArray(value);

const collectErrorStrings = (value: unknown, sink: string[]) => {
  if (typeof value === 'string') {
    const trimmed = value.trim();
    if (trimmed) sink.push(trimmed);
    return;
  }

  if (Array.isArray(value)) {
    value.forEach((entry) => collectErrorStrings(entry, sink));
    return;
  }

  if (!isRecord(value)) return;

  ['message', 'detail', 'description', 'title', 'reason'].forEach((key) => {
    const candidate = value[key];
    if (typeof candidate === 'string' && candidate.trim()) sink.push(candidate.trim());
  });

  if ('body' in value) collectErrorStrings(value.body, sink);
  if ('errors' in value) collectErrorStrings(value.errors, sink);
  if ('error' in value) collectErrorStrings(value.error, sink);
  if ('originalError' in value) collectErrorStrings(value.originalError, sink);
};

const dedupeStrings = (values: string[]) => [...new Set(values.map((value) => value.trim()).filter(Boolean))];

const getBestSamplePayload = (sampleErrors: Record<string, unknown>[]) => sampleErrors.find((entry) => isRecord(entry)) ?? null;

const extractBestErrorText = (sampleErrors: Record<string, unknown>[], fallback?: string | null) => {
  const matches: string[] = [];
  sampleErrors.forEach((entry) => collectErrorStrings(entry, matches));
  const deduped = dedupeStrings(matches);
  if (deduped.length > 0) return deduped.join('\n\n');
  return fallback?.trim() || 'No captured upstream error text is available for this sample.';
};

const extractSampleTermCode = (sampleErrors: Record<string, unknown>[]) => {
  const sample = getBestSamplePayload(sampleErrors);
  if (!sample) return null;
  if (typeof sample.termCode === 'string' && sample.termCode.trim()) return sample.termCode;
  const term = sample.term;
  if (isRecord(term) && typeof term.code === 'string' && term.code.trim()) return term.code;
  return null;
};

const extractSampleScheduleType = (sampleErrors: Record<string, unknown>[], mergeReport?: MergeReportReference | null) => {
  const sample = getBestSamplePayload(sampleErrors);
  if (sample) {
    const nestedMergeReport = sample.mergeReport;
    if (isRecord(nestedMergeReport) && typeof nestedMergeReport.scheduleType === 'string' && nestedMergeReport.scheduleType.trim()) {
      return nestedMergeReport.scheduleType;
    }
  }
  return mergeReport?.scheduleType || null;
};

const extractSampleEntityDisplayName = (sampleErrors: Record<string, unknown>[], mergeReport?: MergeReportReference | null) => {
  const sample = getBestSamplePayload(sampleErrors);
  if (sample && typeof sample.entityDisplayName === 'string' && sample.entityDisplayName.trim()) return sample.entityDisplayName;
  return mergeReport?.entityDisplayName || null;
};

const openSignatureDetail = (signature: ErrorSignatureCluster) => {
  selectedErrorDetail.value = {
    title: 'Representative sample error',
    signatureLabel: signature.signatureLabel,
    fullErrorText: extractBestErrorText(signature.sampleErrors, signature.sampleMessage),
    entityType: signature.entityType,
    errorCode: signature.errorCode,
    school: signature.dominantSchool,
    schoolLabel: signature.dominantSchool ? formatSchoolLabel(signature.dominantSchool, schoolLabel.value) : null,
    sisPlatform: signature.dominantSisPlatform,
    termCode: extractSampleTermCode(signature.sampleErrors) || signature.termCodes[0] || null,
    scheduleType: extractSampleScheduleType(signature.sampleErrors, signature.dominantSchoolMergeReport || signature.latestMergeReport),
    entityDisplayName: extractSampleEntityDisplayName(signature.sampleErrors, signature.dominantSchoolMergeReport || signature.latestMergeReport),
    mergeReport: signature.dominantSchoolMergeReport || signature.latestMergeReport || null,
    rawPayload: stringifyPayload(getBestSamplePayload(signature.sampleErrors)),
    resolutionHint: signature.resolutionHint,
  };
};

const openDetailRowError = (row: ErrorDetailRow) => {
  selectedErrorDetail.value = {
    title: `${formatSchoolLabel(row.school, row.displayName)} error`,
    signatureLabel: row.signatureLabel,
    fullErrorText: row.fullErrorText,
    entityType: row.entityType,
    errorCode: row.errorCode,
    school: row.school,
    schoolLabel: formatSchoolLabel(row.school, row.displayName),
    sisPlatform: row.sisPlatform,
    termCode: row.termCodes[0] || null,
    scheduleType: row.mergeReport?.scheduleType || null,
    entityDisplayName: row.entityDisplayName,
    mergeReport: row.mergeReport || null,
    rawPayload: stringifyPayload(row.rawError),
    resolutionHint: null,
  };
};

const closeErrorDetail = () => {
  selectedErrorDetail.value = null;
};

watch(school, () => {
  currentErrorPage.value = 1;
  currentErrorCategoryFilter.value = '';
  currentErrorEntityTypeFilter.value = '';
  currentErrorSignatureFilter.value = '';
});

watch(currentErrorRows, (value) => {
  if (!value) return;
  const totalPages = Math.max(1, Math.ceil((value.total ?? 0) / currentErrorPageSize));
  if (currentErrorPage.value > totalPages) {
    currentErrorPage.value = totalPages;
  }
});

watch([currentErrorCategoryFilter, currentErrorEntityTypeFilter, currentErrorSignatureFilter], () => {
  currentErrorPage.value = 1;
});
</script>

<template>
  <div class="min-h-screen bg-slate-50">
    <div class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      <div class="mb-8 rounded-[28px] border border-slate-200 bg-white/95 p-8 shadow-sm">
        <div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.3em] text-slate-500">School Details</p>
            <div class="mt-4 flex flex-wrap items-center gap-3">
              <router-link
                :to="{ name: 'AdminClientHealth' }"
                class="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:bg-white hover:text-slate-950"
              >
                ← Back to Client Health
              </router-link>

              <a
                :href="mergeReportsUrl"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
              >
                Merge Reports ↗
              </a>
            </div>
            <h1 class="mt-3 text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">{{ schoolLabel }}</h1>
            <p class="mt-4 text-base leading-7 text-slate-600">Full local snapshot history with explicit metric windows and current activity.</p>
            <p class="mt-3 text-xs text-slate-400">Times shown in {{ localTimeZoneLabel }}.</p>
            <p class="mt-3 text-sm text-slate-500">Last successful sync: {{ lastSuccessfulSyncLabel }}</p>
            <p class="mt-1 text-sm text-slate-500">Last attempted sync: {{ lastAttemptedSyncLabel }}</p>
            <p v-if="lastAttemptStatusLabel" class="mt-1 text-xs" :class="syncMetadata?.lastAttemptedSync?.status === 'failed' ? 'text-rose-500' : 'text-slate-400'">
              Last attempt status: {{ lastAttemptStatusLabel }}
            </p>
            <p v-if="latestSnapshot?.snapshotDate" class="mt-1 text-xs text-slate-400">Latest successful snapshot date: {{ latestSnapshot.snapshotDate }}</p>
            <p v-if="latestSnapshotCapturedAtLabel" class="mt-1 text-xs text-slate-400">Latest snapshot captured locally: {{ latestSnapshotCapturedAtLabel }}</p>
            <p v-if="snapshotCount === 1" class="mt-2 text-xs text-amber-600">Only one local snapshot is available right now, so the charts will show a single point instead of a trend line.</p>
            <p class="mt-2 max-w-2xl text-xs text-slate-500">Nightly success uses Coursedog's upstream 48-hour health window. Realtime success and active users use the last 24 hours. Open merge errors are shown only for days where that count was captured directly during a local sync. Halted nightly merges are broken out when the Sync Coursedog Updates with SIS stage reports a change-threshold halt.</p>
          </div>
         
        </div>
      </div>
      <div v-if="loading" class="rounded-[28px] border border-slate-200 bg-white p-8 text-slate-700 shadow-sm">Loading...</div>
      <div v-else-if="error" class="rounded-[28px] border border-rose-200 bg-rose-50 p-8 text-rose-700 shadow-sm">{{ error }}</div>
      <div v-else class="space-y-8">
        <div class="grid grid-cols-1 gap-6 md:grid-cols-2">
        <Card subtitle="Performance" title="Nightly Merge Activity (48h Upstream Window)">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="bar" :options="nightlySuccessChartOptions" :series="nightlySuccessChartSeries" />
          </div>
        </Card>
        <Card subtitle="Duration" title="Nightly Merge Duration Trend">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="line" :options="nightlyDurationChartOptions" :series="nightlyDurationChartSeries" />
          </div>
        </Card>
        <Card subtitle="Performance" title="Realtime Merge Activity (Last 24h)">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="bar" :options="realtimeSuccessChartOptions" :series="realtimeSuccessChartSeries" />
          </div>
        </Card>
        <Card subtitle="Issues" title="Open Merge Errors Across Local Snapshots">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="line" :options="mergeErrorsChartOptions" :series="mergeErrorsChartSeries" />
          </div>
        </Card>
        <Card subtitle="Activity" title="Daily Active Sessions">
          <div class="mt-6 min-h-[260px]">
            <VueApexCharts type="line" :options="activeUsersChartOptions" :series="activeUsersChartSeries" />
          </div>
        </Card>
        <Card class="flex flex-col overflow-hidden" subtitle="Users" title="Active Users in Last 24h">
          <p class="mt-2 text-3xl font-semibold text-slate-950">{{ activeUsers?.count }}</p>
          <p v-if="activeUsers?.error" class="mt-2 text-xs text-amber-500">{{ activeUsers.error }}</p>
          <div class="mt-6 max-h-[28rem] space-y-2 overflow-y-auto pr-2" data-testid="active-users-list">
            <div v-for="user in activeUsers?.users" :key="user" class="flex items-center gap-3 rounded-lg bg-slate-50 p-3">
              <div class="h-2 w-2 rounded-full bg-emerald-500"></div>
              <span class="text-sm text-slate-900">{{ user }}</span>
            </div>
            <div v-if="!activeUsers?.users?.length" class="text-sm text-slate-500">No active users</div>
          </div>
        </Card>
        </div>

        <Card subtitle="Current Errors" title="Current Error Details">
          <div class="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <p class="text-sm text-slate-500">Grouped from the latest captured detailed error snapshot for this school only.</p>
              <p class="mt-2 text-xs text-slate-400">{{ currentErrorSnapshotLabel }}</p>
            </div>
            <p v-if="hasCurrentErrorContent" class="text-sm font-medium text-slate-700">{{ currentErrorTotals }} current captured error{{ currentErrorTotals === 1 ? '' : 's' }}</p>
          </div>

          <div v-if="hasIssueTabsContent" class="mt-6 flex flex-wrap gap-2 border-t border-slate-200 pt-5" data-testid="current-error-tabs">
            <button
              v-for="tab in currentErrorTabs"
              :key="tab.key"
              type="button"
              class="rounded-full px-4 py-2 text-sm font-semibold transition"
              :class="currentErrorTab === tab.key ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-700 hover:bg-slate-200'"
              @click="currentErrorTab = tab.key"
            >
              {{ tab.label }}
            </button>
          </div>

          <div v-if="isLoadingCurrentErrorAnalysis" class="mt-6 rounded-2xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">
            Loading current error categories...
          </div>
          <div v-else-if="currentErrorAnalysisError" class="mt-6 rounded-2xl border border-rose-200 bg-rose-50 p-5 text-sm text-rose-700">
            Failed to load current error categories.
          </div>
          <div v-else-if="!hasIssueTabsContent" class="mt-6 rounded-2xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">
            No detailed captured errors or recent failed merges are present in the latest local snapshot for this school yet.
          </div>
          <div v-else class="mt-6">
            <div v-if="currentErrorTab === 'categories'" class="grid gap-4 lg:grid-cols-2" data-testid="current-error-panel-categories">
              <div v-for="category in currentErrorCategories" :key="category.key" class="rounded-3xl border border-slate-200 bg-slate-50 p-5" data-testid="current-error-category">
                <div class="flex items-start justify-between gap-4">
                  <div>
                    <span class="inline-flex rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.08em]" :class="getResolutionToneClass(category)">
                      {{ category.title }}
                    </span>
                    <p class="mt-3 text-sm leading-6 text-slate-600">{{ category.action }}</p>
                  </div>
                  <div class="text-right">
                    <p class="text-2xl font-semibold text-slate-950">{{ category.count }}</p>
                    <p class="text-xs text-slate-500">{{ category.signatures }} signature{{ category.signatures === 1 ? '' : 's' }}</p>
                  </div>
                </div>
              </div>
            </div>

            <div v-else-if="currentErrorTab === 'signatures'" class="space-y-3" data-testid="current-error-panel-signatures">
              <div v-if="!currentErrorSignatures.length" class="rounded-2xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">
                No current captured error signatures for this school.
              </div>
              <div v-for="signature in currentErrorSignatures" :key="signature.signatureKey" class="rounded-3xl border border-slate-200 bg-slate-50 p-5" data-testid="current-error-signature">
                <div class="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                  <div class="min-w-0">
                    <p class="font-semibold leading-6 text-slate-950">{{ buildSignatureHeadline(signature.signatureLabel) }}</p>
                    <p v-if="getSignatureSummary(signature)" class="mt-1 text-xs text-slate-500">{{ getSignatureSummary(signature) }}</p>
                    <p class="mt-3 text-sm leading-6 text-slate-600">{{ signature.resolutionHint.action }}</p>
                    <p class="mt-3 text-xs leading-5 text-slate-500">{{ signature.sampleMessage }}</p>
                  </div>
                  <div class="flex flex-col items-start gap-2 lg:items-end">
                    <span class="inline-flex rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.08em]" :class="getResolutionToneClass(signature.resolutionHint)">
                      {{ signature.resolutionHint.title }}
                    </span>
                    <p class="text-2xl font-semibold text-slate-950">{{ signature.totalCount }}</p>
                    <button
                      type="button"
                      class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700"
                      data-testid="signature-detail-button"
                      @click="openSignatureDetail(signature)"
                    >
                      View full error
                    </button>
                    <a
                      v-if="signature.latestMergeReport"
                      :href="getMergeReportUrl(signature.latestMergeReport)"
                      target="_blank"
                      rel="noopener noreferrer"
                      class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                    >
                      Merge report ↗
                    </a>
                  </div>
                </div>
              </div>
            </div>

            <div v-else-if="currentErrorTab === 'rows'" data-testid="current-error-panel-rows">
              <div v-if="isLoadingCurrentErrorRows" class="rounded-2xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">
                Loading current captured errors...
              </div>
              <div v-else-if="currentErrorRowsError" class="rounded-2xl border border-rose-200 bg-rose-50 p-5 text-sm text-rose-700">
                Failed to load current captured errors.
              </div>
              <div v-else class="space-y-3">
                <div class="grid gap-3 rounded-3xl border border-slate-200 bg-white p-4 md:grid-cols-3" data-testid="current-error-row-filters">
                  <label class="flex flex-col gap-2 text-sm text-slate-600">
                    <span class="font-medium text-slate-700">Category</span>
                    <select
                      v-model="currentErrorCategoryFilter"
                      class="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-slate-400 focus:bg-white"
                      data-testid="current-error-category-filter"
                    >
                      <option value="">All categories</option>
                      <option v-for="option in currentErrorCategoryOptions" :key="option.value" :value="option.value">
                        {{ option.label }}
                      </option>
                    </select>
                  </label>
                  <label class="flex flex-col gap-2 text-sm text-slate-600">
                    <span class="font-medium text-slate-700">Entity Type</span>
                    <select
                      v-model="currentErrorEntityTypeFilter"
                      class="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-slate-400 focus:bg-white"
                      data-testid="current-error-entity-type-filter"
                    >
                      <option value="">All entity types</option>
                      <option v-for="option in currentErrorEntityTypeOptions" :key="option.value" :value="option.value">
                        {{ option.label }}
                      </option>
                    </select>
                  </label>
                  <label class="flex flex-col gap-2 text-sm text-slate-600">
                    <span class="font-medium text-slate-700">Signature</span>
                    <select
                      v-model="currentErrorSignatureFilter"
                      class="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-slate-400 focus:bg-white"
                      data-testid="current-error-signature-filter"
                    >
                      <option value="">All signatures</option>
                      <option v-for="option in currentErrorSignatureOptions" :key="option.value" :value="option.value">
                        {{ option.label }}
                      </option>
                    </select>
                  </label>
                </div>
                <div
                  v-if="currentErrorDetailRows.length"
                  class="flex flex-col gap-3 border-t border-slate-200 pt-4 text-sm text-slate-600 sm:flex-row sm:items-center sm:justify-between"
                  data-testid="current-error-pagination-top"
                >
                  <p>{{ currentErrorDetailTotal }} error row{{ currentErrorDetailTotal === 1 ? '' : 's' }} total</p>
                  <span data-testid="current-error-page-label-top">Page {{ currentErrorPage }} of {{ currentErrorTotalPages }}</span>
                </div>
                <div v-if="!currentErrorDetailRows.length" class="rounded-2xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">
                  No captured errors match the selected category, entity type, and signature filters.
                </div>
                <div
                  v-for="row in currentErrorDetailRows"
                  :key="row.id"
                  class="cursor-pointer rounded-3xl border border-slate-200 bg-slate-50 p-5 transition hover:border-slate-300 hover:bg-white"
                  data-testid="current-error-row"
                  role="button"
                  tabindex="0"
                  @click="openDetailRowError(row)"
                  @keydown.enter.prevent="openDetailRowError(row)"
                  @keydown.space.prevent="openDetailRowError(row)"
                >
                  <div class="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                    <div class="min-w-0">
                      <p class="font-semibold leading-6 text-slate-950">{{ buildSignatureHeadline(row.signatureLabel) }}</p>
                      <p v-if="getRowSummary(row)" class="mt-1 text-xs text-slate-500">{{ getRowSummary(row) }}</p>
                      <p class="mt-3 text-sm leading-6 text-slate-600">{{ row.fullErrorText }}</p>
                    </div>
                    <div class="flex flex-col items-start gap-2 lg:items-end">
                      <p class="text-xs text-slate-500">{{ row.snapshotDate }}</p>
                      <button
                        type="button"
                        class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700"
                        data-testid="row-detail-button"
                        @click.stop="openDetailRowError(row)"
                      >
                        View full error
                      </button>
                      <a
                        v-if="row.mergeReport"
                        :href="getMergeReportUrl(row.mergeReport)"
                        target="_blank"
                        rel="noopener noreferrer"
                        class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                        @click.stop
                      >
                        Merge report ↗
                      </a>
                    </div>
                  </div>
                </div>
                <div v-if="currentErrorDetailRows.length" class="flex flex-col gap-3 border-t border-slate-200 pt-4 text-sm text-slate-600 sm:flex-row sm:items-center sm:justify-between">
                  <p>{{ currentErrorDetailTotal }} error row{{ currentErrorDetailTotal === 1 ? '' : 's' }} total</p>
                  <div class="flex items-center gap-3">
                    <button
                      type="button"
                      class="rounded-full border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                      :disabled="currentErrorPage <= 1"
                      data-testid="current-error-page-prev"
                      @click="currentErrorPage -= 1"
                    >
                      Previous
                    </button>
                    <span data-testid="current-error-page-label">Page {{ currentErrorPage }} of {{ currentErrorTotalPages }}</span>
                    <button
                      type="button"
                      class="rounded-full border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                      :disabled="currentErrorPage >= currentErrorTotalPages"
                      data-testid="current-error-page-next"
                      @click="currentErrorPage += 1"
                    >
                      Next
                    </button>
                  </div>
                </div>
              </div>
            </div>

            <div v-else class="space-y-3" data-testid="current-error-panel-recent-failures">
              <div
                v-for="merge in recentFailedMerges"
                :key="merge.id"
                class="flex items-start gap-3 rounded-3xl border border-slate-200 bg-slate-50 p-5"
                data-testid="recent-failed-merge-row"
              >
                <div class="mt-1 h-2 w-2 rounded-full bg-rose-500 flex-shrink-0"></div>
                <div class="flex flex-col text-sm">
                  <a
                    v-if="getRecentFailedMergeReportUrl(merge)"
                    :href="getRecentFailedMergeReportUrl(merge) || undefined"
                    target="_blank"
                    rel="noopener noreferrer"
                    class="w-fit font-medium text-slate-900 underline decoration-slate-300 underline-offset-4 transition hover:text-slate-700 hover:decoration-slate-500"
                    data-testid="recent-failed-merge-id-link"
                  >
                    {{ merge.id }}
                  </a>
                  <span v-else class="font-medium text-slate-900">{{ merge.id }}</span>
                  <span v-if="merge.type" class="text-xs text-slate-500">Type: {{ merge.type }}</span>
                  <span v-if="merge.scheduleType" class="text-xs text-slate-500 capitalize">{{ merge.scheduleType }} merge</span>
                  <span v-if="merge.haltReason || merge.statusDetail" class="text-xs font-medium text-yellow-700">{{ merge.haltReason || merge.statusDetail }}</span>
                  <span v-if="merge.timestampEnd" class="text-xs text-slate-400">
                    {{ formatLocalDateTime(merge.timestampEnd) }}
                  </span>
                </div>
              </div>
              <div v-if="!recentFailedMerges.length" class="rounded-2xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">
                No recent failed merges.
              </div>
            </div>
          </div>
        </Card>

        <div
          v-if="selectedErrorDetail"
          class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-8"
          data-testid="current-error-detail-modal"
          @click.self="closeErrorDetail"
        >
          <div class="max-h-[90vh] w-full max-w-4xl overflow-y-auto rounded-[28px] border border-slate-200 bg-white p-6 shadow-2xl sm:p-8">
            <div class="flex items-start justify-between gap-4">
              <div>
                <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">{{ selectedErrorDetail.title }}</p>
                <h2 class="mt-2 text-2xl font-semibold text-slate-950">Full upstream error</h2>
                <p class="mt-3 text-sm leading-6 text-slate-600">{{ selectedErrorDetail.signatureLabel }}</p>
              </div>
              <button
                type="button"
                class="inline-flex h-10 w-10 items-center justify-center rounded-full border border-slate-200 text-lg text-slate-500 transition hover:border-slate-300 hover:text-slate-900"
                aria-label="Close full error modal"
                @click="closeErrorDetail"
              >
                ×
              </button>
            </div>

            <div class="mt-6 flex flex-wrap gap-2 text-xs text-slate-600">
              <span v-if="selectedErrorDetail.entityType" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedErrorDetail.entityType }}</span>
              <span v-if="selectedErrorDetail.errorCode" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedErrorDetail.errorCode }}</span>
              <span v-if="selectedErrorDetail.schoolLabel" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedErrorDetail.schoolLabel }}</span>
              <span v-if="selectedErrorDetail.sisPlatform" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedErrorDetail.sisPlatform }}</span>
              <span v-if="selectedErrorDetail.termCode" class="rounded-full bg-slate-100 px-3 py-1.5">Term {{ selectedErrorDetail.termCode }}</span>
              <span v-if="selectedErrorDetail.scheduleType" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedErrorDetail.scheduleType }}</span>
            </div>

            <div class="mt-6 grid gap-4 lg:items-start lg:grid-cols-[minmax(0,1.5fr)_minmax(260px,0.9fr)]">
              <div class="self-start rounded-3xl border border-slate-200 bg-slate-50 p-5">
                <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Captured upstream message</p>
                <pre class="mt-3 whitespace-pre-wrap break-words font-sans text-sm leading-6 text-slate-800">{{ selectedErrorDetail.fullErrorText }}</pre>
              </div>

              <div class="space-y-4">
                <div class="rounded-3xl border border-slate-200 bg-white p-5">
                  <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Context</p>
                  <dl class="mt-3 space-y-3 text-sm text-slate-700">
                    <div v-if="selectedErrorDetail.entityDisplayName">
                      <dt class="text-xs uppercase tracking-[0.1em] text-slate-500">Entity</dt>
                      <dd class="mt-1 text-slate-900">{{ selectedErrorDetail.entityDisplayName }}</dd>
                    </div>
                    <div v-if="selectedErrorDetail.resolutionHint">
                      <dt class="text-xs uppercase tracking-[0.1em] text-slate-500">Likely next step</dt>
                      <dd class="mt-1 text-slate-900">{{ selectedErrorDetail.resolutionHint.action }}</dd>
                    </div>
                  </dl>
                </div>

                <div class="rounded-3xl border border-slate-200 bg-white p-5">
                  <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Links</p>
                  <div class="mt-3 flex flex-wrap gap-2">
                    <router-link
                      v-if="selectedErrorDetail.school"
                      :to="schoolRoute(selectedErrorDetail.school)"
                      class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700"
                    >
                      School detail
                    </router-link>

                    <a
                      v-if="selectedErrorDetail.mergeReport"
                      :href="getMergeReportUrl(selectedErrorDetail.mergeReport)"
                      target="_blank"
                      rel="noopener noreferrer"
                      class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                    >
                      Merge report ↗
                    </a>
                  </div>
                </div>
              </div>
            </div>

            <details v-if="selectedErrorDetail.rawPayload" class="mt-6 rounded-3xl border border-slate-200 bg-slate-50 p-5">
              <summary class="cursor-pointer text-sm font-semibold text-slate-900">Raw sample payload</summary>
              <pre class="mt-4 overflow-x-auto whitespace-pre-wrap break-words text-xs leading-5 text-slate-700">{{ selectedErrorDetail.rawPayload }}</pre>
            </details>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
