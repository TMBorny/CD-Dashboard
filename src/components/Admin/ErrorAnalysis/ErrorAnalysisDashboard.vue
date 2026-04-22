<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { keepPreviousData, useQuery } from '@tanstack/vue-query';
import VueApexCharts from 'vue3-apexcharts';
import { downloadErrorAnalysisDetailedExport, downloadErrorAnalysisExport, getErrorAnalysis, getErrorAnalysisErrors } from '@/api';
import Card from '@/components/ui/Card.vue';
import { useChartOptions } from '@/composables/useChartOptions';
import type {
  ErrorAnalysisResponse,
  ErrorBreakdownRow,
  ErrorDetailRow,
  ErrorDetailTableResponse,
  ErrorSignatureCluster,
  MergeReportReference,
  ResolutionHint,
} from '@/types/errorAnalysis';
import { formatLocalDateTime, getLocalTimeZoneLabel } from '@/utils/dateTime';
import { formatSchoolLabel } from '@/utils/schoolNames';

type ErrorViewMode = 'aggregate' | 'all' | 'school' | 'sis';
type WindowOption = '7' | '30' | 'all';
type ErrorSortDir = 'asc' | 'desc';
type SisSortKey = 'label' | 'affectedSchools' | 'totalErrors' | 'dominantSignature' | 'commonResolutionTheme';

interface ErrorDetailContext {
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
  impactedSchools?: ErrorSignatureCluster['impactedSchools'];
  exampleMergeReports?: ErrorSignatureCluster['exampleMergeReports'];
  rawPayload?: string | null;
  resolutionHint?: ResolutionHint | null;
}

interface SisSignatureContext {
  label: string;
  associatedSignatures: NonNullable<ErrorBreakdownRow['associatedSignatures']>;
}

const selectedWindow = ref<WindowOption>('7');
const selectedSis = ref('all');
const activeView = ref<ErrorViewMode>('aggregate');
const selectedErrorDetail = ref<ErrorDetailContext | null>(null);
const selectedSisSignatureContext = ref<SisSignatureContext | null>(null);
const isExporting = ref(false);
const detailSearch = ref('');
const detailPage = ref(1);
const detailPageSize = 50;
const detailSortBy = ref('snapshotDate');
const detailSortDir = ref<ErrorSortDir>('desc');
const sisSortBy = ref<SisSortKey>('totalErrors');
const sisSortDir = ref<ErrorSortDir>('desc');
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
      sisPlatform: selectedSis.value,
    },
  ]),
  queryFn: () =>
    getErrorAnalysis({
      days: daysParam.value,
      sisPlatform: selectedSis.value === 'all' ? undefined : selectedSis.value,
    }).then((res) => res.data as ErrorAnalysisResponse),
  placeholderData: keepPreviousData,
});

const response = computed(() => data.value);

const { data: detailData, isLoading: isLoadingDetails } = useQuery({
  queryKey: computed(() => [
    'errorAnalysisDetails',
    {
      days: daysParam.value ?? 'all',
      sisPlatform: selectedSis.value,
      q: detailSearch.value,
      page: detailPage.value,
      pageSize: detailPageSize,
      sortBy: detailSortBy.value,
      sortDir: detailSortDir.value,
    },
  ]),
  queryFn: () =>
    getErrorAnalysisErrors({
      days: daysParam.value,
      sisPlatform: selectedSis.value === 'all' ? undefined : selectedSis.value,
      q: detailSearch.value || undefined,
      page: detailPage.value,
      pageSize: detailPageSize,
      sortBy: detailSortBy.value,
      sortDir: detailSortDir.value,
    }).then((res) => res.data as ErrorDetailTableResponse),
  placeholderData: keepPreviousData,
});

const detailResponse = computed(() => detailData.value);

const formatCountLabel = (value?: number | null) => new Intl.NumberFormat('en-US').format(value ?? 0);

const sisOptions = computed(() => {
  const countsBySis = new Map(
    (response.value?.sisBreakdowns ?? []).map((row) => [row.key, row.totalErrors] as const),
  );

  const sisPlatforms = response.value?.filterOptions.sisPlatforms ?? [];
  const sortedSisPlatforms = [...sisPlatforms].sort((left, right) => {
    const countDiff = (countsBySis.get(right) ?? 0) - (countsBySis.get(left) ?? 0);
    if (countDiff !== 0) return countDiff;
    return left.localeCompare(right, undefined, { sensitivity: 'base' });
  });

  return [
    { value: 'all', label: 'All SIS Platforms' },
    ...sortedSisPlatforms.map((value) => ({
      value,
      label: `${value} (${formatCountLabel(countsBySis.get(value))})`,
    })),
  ];
});

watch([selectedWindow, selectedSis, detailSearch], () => {
  detailPage.value = 1;
});

const hasCapturedData = computed(() => response.value?.metadata.hasCapturedData ?? false);
const hasFilteredRows = computed(() => (response.value?.summary.totalErrorInstances ?? 0) > 0);

const summaryCards = computed(() => ([
  {
    key: 'totalErrors',
    label: 'Open Error Instances',
    value: response.value?.summary.totalErrorInstances ?? 0,
    detail: 'Open merge-error instances in the latest captured snapshot for the current filters.',
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

const allSignatures = computed(() => response.value?.signatures ?? []);
const topSignatures = computed(() => allSignatures.value.slice(0, 12));
const schoolRows = computed(() => response.value?.schoolBreakdowns ?? []);
const sisRows = computed(() => response.value?.sisBreakdowns ?? []);
const detailRows = computed(() => detailResponse.value?.rows ?? []);
const detailTotal = computed(() => detailResponse.value?.total ?? 0);
const detailTotalPages = computed(() => Math.max(1, Math.ceil(detailTotal.value / detailPageSize)));
const hasDetailRows = computed(() => detailRows.value.length > 0);

const viewOptions: Array<{ value: ErrorViewMode; label: string }> = [
  { value: 'aggregate', label: 'Signatures' },
  { value: 'all', label: 'All Errors' },
  { value: 'school', label: 'By School' },
  { value: 'sis', label: 'By SIS' },
];

const windowOptions: Array<{ value: WindowOption; label: string }> = [
  { value: '7', label: '7d' },
  { value: '30', label: '30d' },
  { value: 'all', label: 'All captured' },
];

const activeViewDescription = computed(() => {
  switch (activeView.value) {
    case 'aggregate':
      return 'Groups recurring merge errors into normalized signatures so you can spot the biggest patterns quickly.';
    case 'all':
      return 'Shows each captured merge-error row individually, including search, sorting, and pagination.';
    case 'school':
      return 'Rolls grouped error patterns up by school so you can compare concentration and dominant signatures.';
    case 'sis':
      return 'Rolls grouped error patterns up by SIS platform to compare impact and shared themes.';
    default:
      return '';
  }
});

const isSignaturePatternDetail = computed(() =>
  Boolean(selectedErrorDetail.value?.impactedSchools?.length || selectedErrorDetail.value?.exampleMergeReports?.length),
);

const errorDetailHeading = computed(() =>
  isSignaturePatternDetail.value ? 'Signature pattern' : 'Captured error',
);

const errorDetailMessageLabel = computed(() =>
  isSignaturePatternDetail.value ? 'Sample upstream message' : 'Captured upstream message',
);

const errorDetailContextLabel = computed(() =>
  isSignaturePatternDetail.value ? 'Pattern context' : 'Context',
);

const errorDetailLinksLabel = computed(() =>
  isSignaturePatternDetail.value ? 'Sample links' : 'Links',
);

const getIntegrationHubUrl = (school: string) => `${coursedogBaseUrl}/#/int/${school}`;
const getMergeReportUrl = (reference: MergeReportReference) =>
  `${coursedogBaseUrl}/#/int/${reference.school}/merge-history/${reference.mergeReportId}`;

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

const formatCount = (value?: number | null) => formatCountLabel(value);

const schoolRoute = (school: string) => ({
  name: 'AdminClientHealthDetail',
  params: { school },
});

const sortSchoolRows = computed(() => [...schoolRows.value].sort((a, b) => b.totalErrors - a.totalErrors || a.label.localeCompare(b.label)));
const sortSisRows = computed(() => [...sisRows.value].sort((a, b) => {
  const direction = sisSortDir.value === 'asc' ? 1 : -1;
  const compareStrings = (left?: string | null, right?: string | null) =>
    (left || '').localeCompare(right || '', undefined, { sensitivity: 'base' });

  switch (sisSortBy.value) {
    case 'label': {
      const result = compareStrings(a.label, b.label);
      return result === 0 ? b.totalErrors - a.totalErrors : result * direction;
    }
    case 'affectedSchools': {
      const result = (a.affectedSchools || 0) - (b.affectedSchools || 0);
      return result === 0 ? compareStrings(a.label, b.label) : result * direction;
    }
    case 'dominantSignature': {
      const result = compareStrings(a.dominantSignature, b.dominantSignature);
      return result === 0 ? b.totalErrors - a.totalErrors : result * direction;
    }
    case 'commonResolutionTheme': {
      const result = compareStrings(formatTheme(a.commonResolutionTheme), formatTheme(b.commonResolutionTheme));
      return result === 0 ? b.totalErrors - a.totalErrors : result * direction;
    }
    case 'totalErrors':
    default: {
      const result = a.totalErrors - b.totalErrors;
      return result === 0 ? compareStrings(a.label, b.label) : result * direction;
    }
  }
}));

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

const normalizeInlineText = (value?: string | null) => (value || '').replace(/\s+/g, ' ').trim();

const truncateText = (value: string, limit: number) => {
  if (value.length <= limit) return value;
  return `${value.slice(0, Math.max(0, limit - 1)).trimEnd()}…`;
};

const buildSignatureHeadline = (signatureLabel?: string | null) => {
  const normalized = normalizeInlineText(signatureLabel);
  if (!normalized) return 'Unknown signature';
  const segments = normalized.split('|').map((segment) => segment.trim()).filter(Boolean);
  const [entity, errorCode, ...rest] = segments;
  const message = rest.join(' | ');

  if (!message) {
    return truncateText([entity, errorCode].filter(Boolean).join(' | '), 120);
  }

  const compactMessage = truncateText(message, 110);
  return [entity, errorCode, compactMessage].filter(Boolean).join(' | ');
};

const buildSignatureSubline = (signatureLabel?: string | null) => {
  const normalized = normalizeInlineText(signatureLabel);
  if (!normalized) return null;
  if (normalized.length <= 120) return null;
  return truncateText(normalized, 220);
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

  const preferredKeys = ['message', 'detail', 'description', 'title', 'reason'];
  preferredKeys.forEach((key) => {
    const candidate = value[key];
    if (typeof candidate === 'string') {
      const trimmed = candidate.trim();
      if (trimmed) sink.push(trimmed);
    }
  });

  if ('body' in value) collectErrorStrings(value.body, sink);
  if ('errors' in value) collectErrorStrings(value.errors, sink);
  if ('error' in value) collectErrorStrings(value.error, sink);
  if ('originalError' in value) collectErrorStrings(value.originalError, sink);
}

const dedupeStrings = (values: string[]) => [...new Set(values.map((value) => value.trim()).filter(Boolean))];

const getBestSamplePayload = (sampleErrors: Record<string, unknown>[]) => sampleErrors.find((entry) => isRecord(entry)) ?? null;

const extractBestErrorText = (sampleErrors: Record<string, unknown>[], fallback?: string | null) => {
  const matches: string[] = [];
  sampleErrors.forEach((entry) => collectErrorStrings(entry, matches));
  const deduped = dedupeStrings(matches);
  if (deduped.length > 0) {
    return deduped.join('\n\n');
  }
  return fallback?.trim() || 'No captured upstream error text is available for this sample.';
};

const extractSampleTermCode = (sampleErrors: Record<string, unknown>[]) => {
  const sample = getBestSamplePayload(sampleErrors);
  if (!sample) return null;
  if (typeof sample.termCode === 'string' && sample.termCode.trim()) return sample.termCode;
  const term = sample.term;
  if (isRecord(term) && typeof term.code === 'string' && term.code.trim()) {
    return term.code;
  }
  return null;
};

const extractSampleScheduleType = (
  sampleErrors: Record<string, unknown>[],
  mergeReport?: MergeReportReference | null,
) => {
  const sample = getBestSamplePayload(sampleErrors);
  if (sample) {
    const nestedMergeReport = sample.mergeReport;
    if (isRecord(nestedMergeReport) && typeof nestedMergeReport.scheduleType === 'string' && nestedMergeReport.scheduleType.trim()) {
      return nestedMergeReport.scheduleType;
    }
  }
  return mergeReport?.scheduleType || null;
};

const extractSampleEntityDisplayName = (
  sampleErrors: Record<string, unknown>[],
  mergeReport?: MergeReportReference | null,
) => {
  const sample = getBestSamplePayload(sampleErrors);
  if (sample && typeof sample.entityDisplayName === 'string' && sample.entityDisplayName.trim()) {
    return sample.entityDisplayName;
  }
  return mergeReport?.entityDisplayName || null;
};

const stringifyPayload = (value: unknown) => {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return null;
  }
};

const buildSignatureErrorDetail = (signature: ErrorSignatureCluster): ErrorDetailContext => ({
  title: 'Representative sample error',
  signatureLabel: signature.signatureLabel,
  fullErrorText: extractBestErrorText(signature.sampleErrors, signature.sampleMessage),
  entityType: signature.entityType,
  errorCode: signature.errorCode,
  school: signature.dominantSchool,
  schoolLabel: getSchoolLabel(signature.dominantSchool),
  sisPlatform: signature.dominantSisPlatform,
  termCode: extractSampleTermCode(signature.sampleErrors) || signature.termCodes[0] || null,
  scheduleType: extractSampleScheduleType(signature.sampleErrors, signature.dominantSchoolMergeReport || signature.latestMergeReport),
  entityDisplayName: extractSampleEntityDisplayName(signature.sampleErrors, signature.dominantSchoolMergeReport || signature.latestMergeReport),
  mergeReport: signature.dominantSchoolMergeReport || signature.latestMergeReport || null,
  impactedSchools: signature.impactedSchools,
  exampleMergeReports: signature.exampleMergeReports,
  rawPayload: stringifyPayload(getBestSamplePayload(signature.sampleErrors)),
  resolutionHint: signature.resolutionHint,
});

const findDominantSignature = (row: ErrorBreakdownRow) => {
  if (!row.dominantSignature) return null;
  return allSignatures.value.find((signature) =>
    signature.signatureLabel === row.dominantSignature && (!row.key || signature.dominantSchool === row.key),
  ) || allSignatures.value.find((signature) => signature.signatureLabel === row.dominantSignature) || null;
};

const buildSchoolErrorDetail = (row: ErrorBreakdownRow): ErrorDetailContext | null => {
  const signature = findDominantSignature(row);
  if (!signature) return null;
  return {
    ...buildSignatureErrorDetail(signature),
    title: `${formatSchoolLabel(row.key, row.label)} sample error`,
    school: row.key,
    schoolLabel: formatSchoolLabel(row.key, row.label),
    sisPlatform: row.sisPlatform || signature.dominantSisPlatform || null,
    mergeReport: row.latestMergeReport || signature.dominantSchoolMergeReport || signature.latestMergeReport || null,
    scheduleType: extractSampleScheduleType(signature.sampleErrors, row.latestMergeReport || signature.dominantSchoolMergeReport || signature.latestMergeReport),
    entityDisplayName: extractSampleEntityDisplayName(signature.sampleErrors, row.latestMergeReport || signature.dominantSchoolMergeReport || signature.latestMergeReport),
  };
};

const buildDetailRowErrorContext = (row: ErrorDetailRow): ErrorDetailContext => ({
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
});

const openSignatureDetail = (signature: ErrorSignatureCluster) => {
  selectedErrorDetail.value = buildSignatureErrorDetail(signature);
};

const openSchoolErrorDetail = (row: ErrorBreakdownRow) => {
  selectedErrorDetail.value = buildSchoolErrorDetail(row);
};

const openDetailRowError = (row: ErrorDetailRow) => {
  selectedErrorDetail.value = buildDetailRowErrorContext(row);
};

const openSisSignatures = (row: ErrorBreakdownRow) => {
  selectedSisSignatureContext.value = {
    label: row.label,
    associatedSignatures: row.associatedSignatures ?? [],
  };
};

const closeErrorDetail = () => {
  selectedErrorDetail.value = null;
};

const closeSisSignatures = () => {
  selectedSisSignatureContext.value = null;
};

const openSignatureDetailByKey = (signatureKey: string) => {
  const signature = allSignatures.value.find((item) => item.signatureKey === signatureKey);
  if (!signature) return;
  selectedSisSignatureContext.value = null;
  openSignatureDetail(signature);
};

const toggleDetailSort = (column: string) => {
  if (detailSortBy.value === column) {
    detailSortDir.value = detailSortDir.value === 'asc' ? 'desc' : 'asc';
    return;
  }
  detailSortBy.value = column;
  detailSortDir.value = 'asc';
};

const toggleSisSort = (column: SisSortKey) => {
  if (sisSortBy.value === column) {
    sisSortDir.value = sisSortDir.value === 'asc' ? 'desc' : 'asc';
    return;
  }
  sisSortBy.value = column;
  sisSortDir.value = column === 'label' || column === 'dominantSignature' || column === 'commonResolutionTheme'
    ? 'asc'
    : 'desc';
};

const handleExport = async () => {
  if (isExporting.value) return;
  isExporting.value = true;
  try {
    const exportParams = {
      days: daysParam.value,
      sisPlatform: selectedSis.value === 'all' ? undefined : selectedSis.value,
    };
    const { blob, filename } = activeView.value === 'all'
      ? await downloadErrorAnalysisDetailedExport({
        ...exportParams,
        q: detailSearch.value || undefined,
      })
      : await downloadErrorAnalysisExport(exportParams);
    const objectUrl = window.URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = objectUrl;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.URL.revokeObjectURL(objectUrl);
  } finally {
    isExporting.value = false;
  }
};
</script>

<template>
  <div class="w-full bg-slate-50 text-slate-900">
    <div class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
      <div class="mb-6 rounded-[28px] border border-slate-200 bg-white/95 p-8 shadow-sm">
        <div class="max-w-4xl">
          <h1 class="text-4xl font-semibold tracking-tight text-slate-950 sm:text-5xl">Error Analysis</h1>
          <p class="mt-4 text-base leading-7 text-slate-600">
            Find recurring open merge-error patterns, compare them across schools and SIS platforms,
            and surface likely next steps from captured trends.
          </p>
          <div class="mt-5 flex flex-wrap gap-2 text-sm">
            <span class="inline-flex rounded-full bg-slate-100 px-3 py-1.5 text-slate-700">{{ headerStatus }}</span>
            <span class="inline-flex rounded-full bg-slate-100 px-3 py-1.5 text-slate-700">{{ historyStatus }}</span>
            <span class="inline-flex rounded-full bg-slate-100 px-3 py-1.5 text-slate-500">Times shown in {{ localTimeZoneLabel }}</span>
          </div>
        </div>

        <div class="mt-6 flex justify-start sm:justify-end">
          <button
            type="button"
            class="inline-flex items-center rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-60"
            data-testid="error-analysis-export"
            :disabled="isLoading || isExporting"
            @click="handleExport"
          >
            {{ isExporting ? 'Saving export...' : 'Save export' }}
          </button>
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

        <template v-if="activeView === 'aggregate'">
          <Card subtitle="Filters" title="Refine this view">
            <div class="grid gap-4 xl:grid-cols-[minmax(0,1.4fr)_max-content_minmax(220px,1fr)]">
              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">View</label>
                <div class="flex flex-wrap gap-2" data-testid="view-toggle">
                  <button
                    v-for="option in viewOptions"
                    :key="option.value"
                    class="rounded-full px-4 py-2 text-sm font-semibold transition"
                    :class="activeView === option.value ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-700 hover:bg-slate-200'"
                    @click="activeView = option.value"
                  >
                    {{ option.label }}
                  </button>
                </div>
                <p class="mt-3 max-w-2xl text-sm leading-6 text-slate-500">{{ activeViewDescription }}</p>
              </div>

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Window</label>
                <div class="inline-flex flex-wrap rounded-full border border-slate-200 bg-slate-50 p-1">
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

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">SIS</label>
                <select v-model="selectedSis" data-testid="sis-filter" class="w-full rounded-full border border-slate-200 bg-white px-4 py-2.5 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                  <option v-for="option in sisOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                </select>
              </div>
            </div>
          </Card>

          <Card subtitle="Recurring Patterns" title="Top error signatures">
            <p class="mb-4 text-sm text-slate-500">Use the actions column to inspect a representative error without crowding the table with raw payload text.</p>
            <div class="overflow-x-auto">
              <table class="min-w-full table-fixed border-separate border-spacing-y-3 text-left text-sm text-slate-600">
                <colgroup>
                  <col class="w-[40%]">
                  <col class="w-[16%]">
                  <col class="w-[28%]">
                  <col class="w-[16%]">
                </colgroup>
                <thead class="text-xs uppercase tracking-[0.12em] text-slate-500">
                  <tr>
                    <th class="px-4 py-2 font-semibold">Pattern</th>
                    <th class="px-4 py-2 font-semibold">Impact</th>
                    <th class="px-4 py-2 font-semibold">Likely next step</th>
                    <th class="px-4 py-2 font-semibold">Actions</th>
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
                      <p class="mt-3 font-semibold leading-6 text-slate-950 break-words" :title="signature.signatureLabel">
                        {{ buildSignatureHeadline(signature.signatureLabel) }}
                      </p>
                      <p
                        v-if="buildSignatureSubline(signature.signatureLabel)"
                        class="mt-1 text-xs leading-5 text-slate-500 break-words"
                        :title="signature.signatureLabel"
                      >
                        {{ buildSignatureSubline(signature.signatureLabel) }}
                      </p>
                      <p class="mt-2 text-xs leading-5 text-slate-500">
                        Source: {{ getDominantDrilldownLabel(signature) }}<span v-if="signature.dominantSisPlatform"> · {{ signature.dominantSisPlatform }}</span>
                      </p>
                      <p v-if="signature.termCodes.length" class="mt-1 text-xs text-slate-500">Terms: {{ signature.termCodes.join(', ') }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="text-lg font-semibold text-slate-950">{{ formatCount(signature.totalCount) }}</p>
                      <p class="mt-2 text-xs leading-5 text-slate-500">{{ signature.recurrenceDays }} captured day{{ signature.recurrenceDays === 1 ? '' : 's' }}</p>
                      <p class="mt-1 text-xs leading-5 text-slate-500">{{ signature.affectedSchools }} school{{ signature.affectedSchools === 1 ? '' : 's' }}</p>
                      <p class="mt-1 text-xs leading-5 text-slate-500">Last seen {{ signature.lastSeen }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <span class="inline-flex rounded-full px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.08em]" :class="getResolutionToneClass(signature.resolutionHint)">
                        {{ signature.resolutionHint.title }}
                      </span>
                      <p class="mt-3 text-sm leading-6 text-slate-900">{{ signature.resolutionHint.action }}</p>
                    </td>
                    <td class="rounded-r-3xl border-y border-r border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <div class="flex flex-col items-start gap-2">
                        <button
                          type="button"
                          class="inline-flex w-full items-center justify-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700 sm:w-auto"
                          @click="openSignatureDetail(signature)"
                        >
                          View full error
                        </button>
                        <router-link
                          v-if="signature.dominantSchool"
                          :to="schoolRoute(signature.dominantSchool)"
                          class="inline-flex w-full items-center justify-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950 sm:w-auto"
                        >
                          School detail
                        </router-link>
                        <a
                          v-if="signature.dominantSchoolMergeReport"
                          :href="getMergeReportUrl(signature.dominantSchoolMergeReport)"
                          target="_blank"
                          rel="noopener noreferrer"
                          class="inline-flex w-full items-center justify-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950 sm:w-auto"
                        >
                          Merge report ↗
                        </a>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </Card>
        </template>

        <template v-else-if="activeView === 'all'">
          <Card subtitle="Filters" title="Refine this view">
            <div class="grid gap-4 xl:grid-cols-[minmax(0,1.4fr)_max-content_minmax(220px,1fr)]">
              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">View</label>
                <div class="flex flex-wrap gap-2" data-testid="view-toggle">
                  <button
                    v-for="option in viewOptions"
                    :key="option.value"
                    class="rounded-full px-4 py-2 text-sm font-semibold transition"
                    :class="activeView === option.value ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-700 hover:bg-slate-200'"
                    @click="activeView = option.value"
                  >
                    {{ option.label }}
                  </button>
                </div>
                <p class="mt-3 max-w-2xl text-sm leading-6 text-slate-500">{{ activeViewDescription }}</p>
              </div>

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Window</label>
                <div class="inline-flex flex-wrap rounded-full border border-slate-200 bg-slate-50 p-1">
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

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">SIS</label>
                <select v-model="selectedSis" data-testid="sis-filter" class="w-full rounded-full border border-slate-200 bg-white px-4 py-2.5 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                  <option v-for="option in sisOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                </select>
              </div>
            </div>
          </Card>

          <Card subtitle="Detailed Search" title="All captured merge errors">
            <div class="mb-4 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div class="max-w-xl">
                <p class="text-sm text-slate-500">Search across error text, signature, school, SIS, entity, and merge-report metadata. Results are paginated server-side.</p>
              </div>
              <input
                v-model="detailSearch"
                type="search"
                placeholder="Search all captured errors"
                data-testid="error-detail-search"
                class="w-full rounded-full border border-slate-200 bg-white px-4 py-2.5 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none lg:max-w-sm"
              >
            </div>

            <div v-if="isLoadingDetails" class="rounded-2xl border border-slate-200 bg-slate-50 p-6 text-sm text-slate-600">
              Loading captured error rows...
            </div>
            <div v-else-if="!hasDetailRows" class="rounded-2xl border border-slate-200 bg-slate-50 p-6 text-sm text-slate-600">
              No individual captured errors match the current filters yet. Detailed per-error history starts with the new persistence pipeline.
            </div>
            <div v-else class="overflow-x-auto">
              <table class="min-w-full table-fixed border-separate border-spacing-y-3 text-left text-sm text-slate-600">
                <colgroup>
                  <col class="w-[10%]">
                  <col class="w-[15%]">
                  <col class="w-[10%]">
                  <col class="w-[8%]">
                  <col class="w-[9%]">
                  <col class="w-[18%]">
                  <col class="w-[20%]">
                  <col class="w-[5%]">
                  <col class="w-[5%]">
                </colgroup>
                <thead class="text-xs uppercase tracking-[0.12em] text-slate-500">
                  <tr>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleDetailSort('snapshotDate')">Date</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleDetailSort('displayName')">School</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleDetailSort('sisPlatform')">SIS</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleDetailSort('entityType')">Entity</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleDetailSort('errorCode')">Code</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleDetailSort('signatureLabel')">Signature</button></th>
                    <th class="px-4 py-2 font-semibold">Error text</th>
                    <th class="px-4 py-2 font-semibold">Term</th>
                    <th class="px-4 py-2 font-semibold">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in detailRows" :key="row.id" data-testid="detail-row">
                    <td class="rounded-l-3xl border-y border-l border-slate-200 bg-slate-50 px-4 py-4 align-top text-xs text-slate-500">{{ row.snapshotDate }}</td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="font-medium text-slate-900">{{ formatSchoolLabel(row.school, row.displayName) }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top text-xs text-slate-600">{{ row.sisPlatform || 'Unknown' }}</td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top text-xs text-slate-600">{{ row.entityType || 'Unknown' }}</td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top text-xs text-slate-600">{{ row.errorCode || '—' }}</td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="font-medium leading-6 text-slate-900 break-words" :title="row.signatureLabel">{{ buildSignatureHeadline(row.signatureLabel) }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="line-clamp-3 break-words text-xs leading-5 text-slate-600" :title="row.fullErrorText">{{ row.fullErrorText }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top text-xs text-slate-600">{{ row.termCodes[0] || '—' }}</td>
                    <td class="rounded-r-3xl border-y border-r border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <div class="flex flex-col items-start gap-2">
                        <button
                          type="button"
                          class="inline-flex w-full items-center justify-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700 sm:w-auto"
                          @click="openDetailRowError(row)"
                        >
                          View
                        </button>
                        <a
                          v-if="row.mergeReport"
                          :href="getMergeReportUrl(row.mergeReport)"
                          target="_blank"
                          rel="noopener noreferrer"
                          class="inline-flex w-full items-center justify-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950 sm:w-auto"
                        >
                          Report ↗
                        </a>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>

            <div class="mt-4 flex flex-col gap-3 border-t border-slate-200 pt-4 text-sm text-slate-600 sm:flex-row sm:items-center sm:justify-between">
              <p>{{ detailTotal }} error row{{ detailTotal === 1 ? '' : 's' }} total</p>
              <div class="flex items-center gap-3">
                <button
                  type="button"
                  class="rounded-full border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="detailPage <= 1"
                  @click="detailPage -= 1"
                >
                  Previous
                </button>
                <span>Page {{ detailPage }} of {{ detailTotalPages }}</span>
                <button
                  type="button"
                  class="rounded-full border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="detailPage >= detailTotalPages"
                  @click="detailPage += 1"
                >
                  Next
                </button>
              </div>
            </div>
          </Card>
        </template>

        <template v-else-if="activeView === 'school'">
          <Card subtitle="Filters" title="Refine this view">
            <div class="grid gap-4 xl:grid-cols-[minmax(0,1.4fr)_max-content_minmax(220px,1fr)]">
              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">View</label>
                <div class="flex flex-wrap gap-2" data-testid="view-toggle">
                  <button
                    v-for="option in viewOptions"
                    :key="option.value"
                    class="rounded-full px-4 py-2 text-sm font-semibold transition"
                    :class="activeView === option.value ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-700 hover:bg-slate-200'"
                    @click="activeView = option.value"
                  >
                    {{ option.label }}
                  </button>
                </div>
                <p class="mt-3 max-w-2xl text-sm leading-6 text-slate-500">{{ activeViewDescription }}</p>
              </div>

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Window</label>
                <div class="inline-flex flex-wrap rounded-full border border-slate-200 bg-slate-50 p-1">
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

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">SIS</label>
                <select v-model="selectedSis" data-testid="sis-filter" class="w-full rounded-full border border-slate-200 bg-white px-4 py-2.5 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                  <option v-for="option in sisOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                </select>
              </div>
            </div>
          </Card>

          <Card subtitle="School Comparison" title="Where recurring errors concentrate">
            <div class="overflow-x-auto">
              <table class="min-w-full table-fixed border-separate border-spacing-y-3 text-left text-sm text-slate-600">
                <colgroup>
                  <col class="w-[26%]">
                  <col class="w-[14%]">
                  <col class="w-[38%]">
                  <col class="w-[22%]">
                </colgroup>
                <thead class="text-xs uppercase tracking-[0.12em] text-slate-500">
                  <tr>
                    <th class="px-4 py-2 font-semibold">School</th>
                    <th class="px-4 py-2 font-semibold">Impact</th>
                    <th class="px-4 py-2 font-semibold">Dominant signature</th>
                    <th class="px-4 py-2 font-semibold">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="row in sortSchoolRows" :key="row.key">
                    <td class="rounded-l-3xl border-y border-l border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <router-link :to="schoolRoute(row.key)" class="font-semibold text-slate-950 hover:text-blue-700">
                        {{ formatSchoolLabel(row.key, row.label) }}
                      </router-link>
                      <p class="mt-1 text-xs leading-5 text-slate-500">{{ coerceSchoolSpecificRow(row) }}</p>
                      <p class="mt-1 text-xs leading-5 text-slate-500">Last seen {{ row.lastSeen }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p class="text-lg font-semibold text-slate-950">{{ row.totalErrors }}</p>
                      <p class="mt-2 text-xs leading-5 text-slate-500">{{ row.distinctSignatures }} signatures</p>
                      <p class="mt-1 text-xs leading-5 text-slate-500">{{ row.recurrenceDays || 0 }} captured day{{ row.recurrenceDays === 1 ? '' : 's' }}</p>
                    </td>
                    <td class="border-y border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <p
                        class="font-medium leading-6 text-slate-900 break-words"
                        :title="row.dominantSignature || undefined"
                      >
                        {{ buildSignatureHeadline(row.dominantSignature || undefined) }}
                      </p>
                      <p
                        v-if="buildSignatureSubline(row.dominantSignature || undefined)"
                        class="mt-1 text-xs leading-5 text-slate-500 break-words"
                        :title="row.dominantSignature || undefined"
                      >
                        {{ buildSignatureSubline(row.dominantSignature || undefined) }}
                      </p>
                      <p class="mt-2 text-xs text-slate-500">Theme: {{ formatTheme(row.topResolutionTheme) }}</p>
                      <p class="mt-2 text-xs leading-5 text-slate-500">{{ row.likelyNextStep || 'Inspect the latest Integration Hub samples for this school.' }}</p>
                    </td>
                    <td class="rounded-r-3xl border-y border-r border-slate-200 bg-slate-50 px-4 py-4 align-top">
                      <div class="flex flex-col items-start gap-2">
                        <button
                          v-if="findDominantSignature(row)"
                          type="button"
                          class="inline-flex w-full items-center justify-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700 sm:w-auto"
                          @click="openSchoolErrorDetail(row)"
                        >
                          View full error
                        </button>
                        <a
                          v-if="row.latestMergeReport"
                          :href="getMergeReportUrl(row.latestMergeReport)"
                          target="_blank"
                          rel="noopener noreferrer"
                          class="inline-flex w-full items-center justify-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950 sm:w-auto"
                        >
                          Latest merge report ↗
                        </a>
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </Card>
        </template>

        <template v-else>
          <Card subtitle="Filters" title="Refine this view">
            <div class="grid gap-4 xl:grid-cols-[minmax(0,1.4fr)_max-content_minmax(220px,1fr)]">
              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">View</label>
                <div class="flex flex-wrap gap-2" data-testid="view-toggle">
                  <button
                    v-for="option in viewOptions"
                    :key="option.value"
                    class="rounded-full px-4 py-2 text-sm font-semibold transition"
                    :class="activeView === option.value ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-700 hover:bg-slate-200'"
                    @click="activeView = option.value"
                  >
                    {{ option.label }}
                  </button>
                </div>
                <p class="mt-3 max-w-2xl text-sm leading-6 text-slate-500">{{ activeViewDescription }}</p>
              </div>

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Window</label>
                <div class="inline-flex flex-wrap rounded-full border border-slate-200 bg-slate-50 p-1">
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

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">SIS</label>
                <select v-model="selectedSis" data-testid="sis-filter" class="w-full rounded-full border border-slate-200 bg-white px-4 py-2.5 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                  <option v-for="option in sisOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                </select>
              </div>
            </div>
          </Card>

          <Card subtitle="SIS Comparison" title="Recurring patterns by SIS">
            <div class="overflow-x-auto">
              <table class="min-w-full border-separate border-spacing-y-3 text-left text-sm text-slate-600">
                <thead class="text-xs uppercase tracking-[0.12em] text-slate-500">
                  <tr>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleSisSort('label')">SIS</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleSisSort('affectedSchools')">Affected schools</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleSisSort('totalErrors')">Impact</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleSisSort('dominantSignature')">Dominant signature</button></th>
                    <th class="px-4 py-2 font-semibold"><button type="button" class="hover:text-slate-900" @click="toggleSisSort('commonResolutionTheme')">Common theme</button></th>
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
                      <button
                        v-if="row.associatedSignatures?.length"
                        type="button"
                        class="mt-3 inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                        @click="openSisSignatures(row)"
                      >
                        View all {{ row.associatedSignatures.length }} signature{{ row.associatedSignatures.length === 1 ? '' : 's' }}
                      </button>
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

    <div
      v-if="selectedSisSignatureContext"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-8"
      data-testid="sis-signatures-modal"
      @click.self="closeSisSignatures"
    >
      <div class="max-h-[90vh] w-full max-w-3xl overflow-y-auto rounded-[28px] border border-slate-200 bg-white p-6 shadow-2xl sm:p-8">
        <div class="flex items-start justify-between gap-4">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">SIS signatures</p>
            <h2 class="mt-2 text-2xl font-semibold text-slate-950">{{ selectedSisSignatureContext.label }}</h2>
            <p class="mt-2 text-sm text-slate-600">All associated signatures captured for this SIS in the current filter window.</p>
          </div>
          <button
            type="button"
            class="inline-flex h-10 w-10 items-center justify-center rounded-full border border-slate-200 text-lg text-slate-500 transition hover:border-slate-300 hover:text-slate-900"
            aria-label="Close SIS signatures modal"
            @click="closeSisSignatures"
          >
            ×
          </button>
        </div>

        <div class="mt-6 space-y-3">
          <div
            v-for="signature in selectedSisSignatureContext.associatedSignatures"
            :key="signature.signatureKey"
            class="rounded-3xl border border-slate-200 bg-slate-50 p-4"
          >
            <div class="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
              <div class="min-w-0">
                <div class="flex flex-wrap items-center gap-2">
                  <span v-if="signature.entityType" class="rounded-full bg-slate-900 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.12em] text-white">
                    {{ signature.entityType }}
                  </span>
                  <span v-if="signature.errorCode" class="rounded-full bg-slate-200 px-2.5 py-1 text-[11px] font-semibold text-slate-700">
                    {{ signature.errorCode }}
                  </span>
                  <span class="rounded-full bg-white px-2.5 py-1 text-[11px] font-semibold text-slate-700">
                    {{ signature.count }} occurrence{{ signature.count === 1 ? '' : 's' }}
                  </span>
                </div>
                <p class="mt-3 font-medium leading-6 text-slate-900 break-words">{{ buildSignatureHeadline(signature.signatureLabel) }}</p>
                <p v-if="signature.sampleMessage" class="mt-2 text-xs leading-5 text-slate-500 break-words">{{ signature.sampleMessage }}</p>
                <p v-if="signature.resolutionTitle" class="mt-2 text-xs font-medium uppercase tracking-[0.08em] text-slate-500">{{ signature.resolutionTitle }}</p>
              </div>
              <div class="flex shrink-0 flex-wrap gap-2">
                <button
                  type="button"
                  class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700"
                  @click="openSignatureDetailByKey(signature.signatureKey)"
                >
                  View full error
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="selectedErrorDetail"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-8"
      data-testid="error-detail-modal"
      @click.self="closeErrorDetail"
    >
      <div class="max-h-[90vh] w-full max-w-4xl overflow-y-auto rounded-[28px] border border-slate-200 bg-white p-6 shadow-2xl sm:p-8">
        <div class="flex items-start justify-between gap-4">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">{{ selectedErrorDetail.title }}</p>
            <h2 class="mt-2 text-2xl font-semibold text-slate-950">{{ errorDetailHeading }}</h2>
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
          <span v-if="selectedErrorDetail.sisPlatform" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedErrorDetail.sisPlatform }}</span>
          <span v-if="selectedErrorDetail.termCode" class="rounded-full bg-slate-100 px-3 py-1.5">Term {{ selectedErrorDetail.termCode }}</span>
          <span v-if="selectedErrorDetail.scheduleType" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedErrorDetail.scheduleType }}</span>
        </div>

        <div class="mt-6 grid gap-4 lg:items-start lg:grid-cols-[minmax(0,1.5fr)_minmax(260px,0.9fr)]">
          <div class="self-start rounded-3xl border border-slate-200 bg-slate-50 p-5">
            <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">{{ errorDetailMessageLabel }}</p>
            <pre class="mt-3 whitespace-pre-wrap break-words font-sans text-sm leading-6 text-slate-800">{{ selectedErrorDetail.fullErrorText }}</pre>
          </div>

          <div class="space-y-4">
            <div class="rounded-3xl border border-slate-200 bg-white p-5">
              <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">{{ errorDetailContextLabel }}</p>
              <dl class="mt-3 space-y-3 text-sm text-slate-700">
                <div v-if="selectedErrorDetail.schoolLabel">
                  <dt class="text-xs uppercase tracking-[0.1em] text-slate-500">{{ isSignaturePatternDetail ? 'Sample school' : 'School' }}</dt>
                  <dd class="mt-1 text-slate-900">{{ selectedErrorDetail.schoolLabel }}</dd>
                </div>
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
              <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">{{ errorDetailLinksLabel }}</p>
              <div class="mt-3 flex flex-wrap gap-2">
                <router-link
                  v-if="selectedErrorDetail.school"
                  :to="schoolRoute(selectedErrorDetail.school)"
                  class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700"
                >
                  School detail
                </router-link>
                <a
                  v-if="selectedErrorDetail.school"
                  :href="getIntegrationHubUrl(selectedErrorDetail.school)"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                >
                  Integration Hub
                </a>
                <a
                  v-if="selectedErrorDetail.mergeReport && !selectedErrorDetail.exampleMergeReports?.length"
                  :href="getMergeReportUrl(selectedErrorDetail.mergeReport)"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                >
                  Merge report ↗
                </a>
              </div>
            </div>

            <div v-if="selectedErrorDetail.impactedSchools?.length" class="rounded-3xl border border-slate-200 bg-white p-5">
              <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Impacted schools</p>
              <div class="mt-3 max-h-56 overflow-y-auto pr-1">
                <div class="flex flex-wrap gap-2">
                  <span
                    v-for="school in selectedErrorDetail.impactedSchools"
                    :key="school.school"
                    class="inline-flex rounded-full bg-slate-100 px-3 py-1.5 text-xs font-semibold text-slate-700"
                  >
                    {{ formatSchoolLabel(school.school, school.label) }} · {{ school.count }}
                  </span>
                </div>
              </div>
            </div>

            <div v-if="selectedErrorDetail.exampleMergeReports?.length" class="rounded-3xl border border-slate-200 bg-white p-5">
              <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Example merge reports</p>
              <div class="mt-3 flex flex-wrap gap-2">
                <a
                  v-for="report in selectedErrorDetail.exampleMergeReports"
                  :key="`${report.school}-${report.mergeReportId}`"
                  :href="getMergeReportUrl(report)"
                  target="_blank"
                  rel="noopener noreferrer"
                  class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                >
                  {{ getSchoolLabel(report.school) }} ↗
                </a>
              </div>
            </div>
          </div>
        </div>

        <details v-if="selectedErrorDetail.rawPayload" class="mt-6 rounded-3xl border border-slate-200 bg-slate-50 p-5">
          <summary class="cursor-pointer text-sm font-semibold text-slate-900">{{ isSignaturePatternDetail ? 'Raw sample payload' : 'Raw captured payload' }}</summary>
          <pre class="mt-4 overflow-x-auto whitespace-pre-wrap break-words text-xs leading-5 text-slate-700">{{ selectedErrorDetail.rawPayload }}</pre>
        </details>
      </div>
    </div>
  </div>
</template>
