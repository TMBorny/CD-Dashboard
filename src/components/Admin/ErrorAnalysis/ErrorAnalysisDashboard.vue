<script setup lang="ts">
import { computed, ref, watch } from 'vue';
import { keepPreviousData, useQuery } from '@tanstack/vue-query';
import VueApexCharts from 'vue3-apexcharts';
import { useRoute, useRouter } from 'vue-router';
import {
  downloadErrorAnalysisDetailedExport,
  downloadErrorAnalysisExport,
  getErrorAnalysis,
  getErrorAnalysisErrors,
  getErrorAnalysisSignatureExplorer,
} from '@/api';
import Card from '@/components/ui/Card.vue';
import { useChartOptions } from '@/composables/useChartOptions';
import { isStaticDataMode } from '@/config/runtime';
import type {
  ErrorAnalysisResponse,
  ErrorBreakdownRow,
  ErrorDetailRow,
  ErrorDetailTableResponse,
  ErrorSignatureExplorerBucket,
  ErrorSignatureExplorerGroupBy,
  ErrorSignatureExplorerSchoolCount,
  ErrorSignatureExplorerResponse,
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
type SignatureExplorerContext = ErrorSignatureCluster;
type ErrorAnalysisModalKind = 'sis-signatures' | 'signature-explorer' | 'error-detail' | 'signature-explorer-schools';
type ErrorDetailRouteState =
  | { origin: 'all'; rowId: number }
  | { origin: 'signature-explorer'; rowId: number; signatureKey: string }
  | { origin: 'signature-sample'; signatureKey: string }
  | { origin: 'school-sample'; schoolKey: string };

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
  routeState?: ErrorDetailRouteState;
}

interface SisSignatureContext {
  key: string;
  label: string;
  associatedSignatures: NonNullable<ErrorBreakdownRow['associatedSignatures']>;
}

interface SignatureExplorerSchoolsContext {
  rowId: number;
  fullErrorText: string;
  instanceCount: number;
  schools: ErrorSignatureExplorerSchoolCount[];
  sisPlatform?: string | null;
  termCode?: string | null;
}

const route = useRoute();
const router = useRouter();

const defaultWindow: WindowOption = '7';
const defaultView: ErrorViewMode = 'aggregate';
const defaultDetailSortBy = 'snapshotDate';
const defaultDetailSortDir: ErrorSortDir = 'desc';
const defaultSisSortBy: SisSortKey = 'totalErrors';
const defaultSisSortDir: ErrorSortDir = 'desc';
const defaultSignatureExplorerGroupBy: ErrorSignatureExplorerGroupBy = 'sis';
const validViews: ErrorViewMode[] = ['aggregate', 'all', 'school', 'sis'];
const validWindows: WindowOption[] = ['7', '30', 'all'];
const validSortDirections: ErrorSortDir[] = ['asc', 'desc'];
const validDetailSortColumns = ['snapshotDate', 'displayName', 'sisPlatform', 'entityType', 'errorCode', 'signatureLabel'] as const;
const validSisSortKeys: SisSortKey[] = ['label', 'affectedSchools', 'totalErrors', 'dominantSignature', 'commonResolutionTheme'];
const validSignatureExplorerGroupBy: ErrorSignatureExplorerGroupBy[] = ['sis', 'school', 'term'];
const validModalKinds: ErrorAnalysisModalKind[] = ['sis-signatures', 'signature-explorer', 'error-detail', 'signature-explorer-schools'];
const validErrorDetailOrigins: ErrorDetailRouteState['origin'][] = ['all', 'signature-explorer', 'signature-sample', 'school-sample'];
const managedQueryKeys = [
  'view',
  'window',
  'sis',
  'school',
  'q',
  'signaturePage',
  'detailPage',
  'detailSortBy',
  'detailSortDir',
  'sisSortBy',
  'sisSortDir',
  'signature',
  'signatureGroupBy',
  'signatureBucket',
  'signatureExplorerPage',
  'modal',
  'modalSis',
  'modalSchool',
  'modalRowId',
  'detailOrigin',
  'detailId',
] as const;

const readQueryValue = (value: unknown) => {
  if (typeof value === 'string') return value;
  if (Array.isArray(value)) return typeof value[0] === 'string' ? value[0] : undefined;
  return undefined;
};

const parsePositiveInt = (value: string | undefined, fallback = 1) => {
  const parsed = Number.parseInt(value || '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const coerceViewQuery = (value: string | undefined): ErrorViewMode =>
  validViews.includes(value as ErrorViewMode) ? (value as ErrorViewMode) : defaultView;

const coerceWindowQuery = (value: string | undefined): WindowOption =>
  validWindows.includes(value as WindowOption) ? (value as WindowOption) : defaultWindow;

const coerceSortDirQuery = (value: string | undefined, fallback: ErrorSortDir): ErrorSortDir =>
  validSortDirections.includes(value as ErrorSortDir) ? (value as ErrorSortDir) : fallback;

const coerceDetailSortByQuery = (value: string | undefined) =>
  validDetailSortColumns.includes(value as (typeof validDetailSortColumns)[number]) ? value : defaultDetailSortBy;

const coerceSisSortByQuery = (value: string | undefined): SisSortKey =>
  validSisSortKeys.includes(value as SisSortKey) ? (value as SisSortKey) : defaultSisSortBy;

const coerceSignatureExplorerGroupByQuery = (value: string | undefined): ErrorSignatureExplorerGroupBy =>
  validSignatureExplorerGroupBy.includes(value as ErrorSignatureExplorerGroupBy)
    ? (value as ErrorSignatureExplorerGroupBy)
    : defaultSignatureExplorerGroupBy;

const coerceModalQuery = (value: string | undefined): ErrorAnalysisModalKind | null =>
  validModalKinds.includes(value as ErrorAnalysisModalKind) ? (value as ErrorAnalysisModalKind) : null;

const coerceDetailOriginQuery = (value: string | undefined): ErrorDetailRouteState['origin'] | null =>
  validErrorDetailOrigins.includes(value as ErrorDetailRouteState['origin'])
    ? (value as ErrorDetailRouteState['origin'])
    : null;

const normalizeQueryRecord = (query: Record<string, unknown>) =>
  Object.fromEntries(
    Object.entries(query)
      .map(([key, value]) => [key, readQueryValue(value)])
      .filter((entry): entry is [string, string] => typeof entry[1] === 'string' && entry[1].length > 0),
  );

const areQueryRecordsEqual = (left: Record<string, unknown>, right: Record<string, unknown>) => {
  const normalizedLeft = normalizeQueryRecord(left);
  const normalizedRight = normalizeQueryRecord(right);
  const keys = new Set([...Object.keys(normalizedLeft), ...Object.keys(normalizedRight)]);

  for (const key of keys) {
    if (normalizedLeft[key] !== normalizedRight[key]) {
      return false;
    }
  }

  return true;
};

const selectedWindow = ref<WindowOption>(coerceWindowQuery(readQueryValue(route.query.window)));
const selectedSchool = ref(readQueryValue(route.query.school) || 'all');
const selectedSis = ref(readQueryValue(route.query.sis) || 'all');
const activeView = ref<ErrorViewMode>(coerceViewQuery(readQueryValue(route.query.view)));
const selectedSignatureExplorer = ref<SignatureExplorerContext | null>(null);
const signatureExplorerGroupBy = ref<ErrorSignatureExplorerGroupBy>(
  coerceSignatureExplorerGroupByQuery(readQueryValue(route.query.signatureGroupBy)),
);
const signatureExplorerBucket = ref<string | null>(readQueryValue(route.query.signatureBucket) ?? null);
const signatureExplorerPage = ref(parsePositiveInt(readQueryValue(route.query.signatureExplorerPage)));
const signatureExplorerPageSize = 25;
const signaturePage = ref(parsePositiveInt(readQueryValue(route.query.signaturePage)));
const signaturePageSize = 12;
const selectedErrorDetail = ref<ErrorDetailContext | null>(null);
const selectedSisSignatureContext = ref<SisSignatureContext | null>(null);
const selectedSignatureExplorerSchools = ref<SignatureExplorerSchoolsContext | null>(null);
const isExporting = ref(false);
const detailSearch = ref(readQueryValue(route.query.q) ?? '');
const detailPage = ref(parsePositiveInt(readQueryValue(route.query.detailPage)));
const detailPageSize = 50;
const detailSortBy = ref(coerceDetailSortByQuery(readQueryValue(route.query.detailSortBy)));
const detailSortDir = ref<ErrorSortDir>(coerceSortDirQuery(readQueryValue(route.query.detailSortDir), defaultDetailSortDir));
const sisSortBy = ref<SisSortKey>(coerceSisSortByQuery(readQueryValue(route.query.sisSortBy)));
const sisSortDir = ref<ErrorSortDir>(coerceSortDirQuery(readQueryValue(route.query.sisSortDir), defaultSisSortDir));
const localTimeZoneLabel = getLocalTimeZoneLabel();
const coursedogBaseUrl = (import.meta.env.VITE_COURSEDOG_PRD_URL?.trim() || 'https://app.coursedog.com').replace(/\/+$/, '');
const signaturesTooltipTitle = 'What are signatures?';
const signaturesTooltipBody = 'Signatures group repeat merge errors into normalized patterns by combining the entity type, error code, and a cleaned version of the message with IDs and other one-off values stripped out.';
const signaturesTooltipDevelopment = 'They are developed from captured merge-error rows in the latest snapshot so similar failures roll up together and recurring issues are easier to spot.';
const descriptorClass = 'group relative inline-flex items-center gap-2 align-middle';
const descriptorButtonClass = 'flex h-5 w-5 items-center justify-center rounded-full border border-slate-300 bg-white text-[11px] font-semibold text-slate-500 transition hover:border-slate-400 hover:text-slate-700 focus-visible:border-slate-500 focus-visible:text-slate-700 focus-visible:outline-none';
const descriptorPopoverClass = 'pointer-events-none absolute left-1/2 top-full z-20 mt-2 hidden w-72 -translate-x-1/2 rounded-2xl border border-slate-200 bg-white p-3 text-left text-[11px] normal-case leading-5 text-slate-600 shadow-lg group-hover:block group-focus-within:block';
const interactiveRowClass = 'cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-400 focus-visible:ring-offset-2';
const isApplyingRouteState = ref(false);

const daysParam = computed<number | undefined>(() => {
  if (selectedWindow.value === 'all') return undefined;
  return Number(selectedWindow.value);
});

const { data, isLoading, error } = useQuery({
  queryKey: computed(() => [
      'errorAnalysis',
      {
        days: daysParam.value ?? 'all',
        school: selectedSchool.value,
        sisPlatform: selectedSis.value,
      },
  ]),
  queryFn: () =>
    getErrorAnalysis({
      days: daysParam.value,
      school: selectedSchool.value === 'all' ? undefined : selectedSchool.value,
      sisPlatform: selectedSis.value === 'all' ? undefined : selectedSis.value,
    }).then((res) => res.data as ErrorAnalysisResponse),
  placeholderData: keepPreviousData,
});

const response = computed(() => data.value);

const { data: sisCountsData } = useQuery({
  queryKey: computed(() => [
    'errorAnalysisSisCounts',
    {
      days: daysParam.value ?? 'all',
    },
  ]),
  queryFn: () =>
    getErrorAnalysis({
      days: daysParam.value,
    }).then((res) => res.data as ErrorAnalysisResponse),
  placeholderData: keepPreviousData,
});

const sisCountsResponse = computed(() => sisCountsData.value);

const { data: detailData, isLoading: isLoadingDetails } = useQuery({
  queryKey: computed(() => [
      'errorAnalysisDetails',
      {
        days: daysParam.value ?? 'all',
        school: selectedSchool.value,
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
      school: selectedSchool.value === 'all' ? undefined : selectedSchool.value,
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

const { data: signatureExplorerData, isLoading: isLoadingSignatureExplorer } = useQuery({
  queryKey: computed(() => [
    'errorAnalysisSignatureExplorer',
    {
      days: daysParam.value ?? 'all',
      school: selectedSchool.value,
      sisPlatform: selectedSis.value,
      signature: selectedSignatureExplorer.value?.signatureKey ?? null,
      groupBy: signatureExplorerGroupBy.value,
      bucket: signatureExplorerBucket.value,
      page: signatureExplorerPage.value,
      pageSize: signatureExplorerPageSize,
    },
  ]),
  queryFn: () =>
    getErrorAnalysisSignatureExplorer({
      days: daysParam.value,
      school: selectedSchool.value === 'all' ? undefined : selectedSchool.value,
      sisPlatform: selectedSis.value === 'all' ? undefined : selectedSis.value,
      latestOnly: true,
      signature: selectedSignatureExplorer.value!.signatureKey,
      groupBy: signatureExplorerGroupBy.value,
      bucket: signatureExplorerBucket.value ?? undefined,
      page: signatureExplorerPage.value,
      pageSize: signatureExplorerPageSize,
    }).then((res) => res.data as ErrorSignatureExplorerResponse),
  enabled: computed(() => Boolean(selectedSignatureExplorer.value?.signatureKey)),
  placeholderData: keepPreviousData,
});

const signatureExplorerResponse = computed(() => signatureExplorerData.value);

const formatCountLabel = (value?: number | null) => new Intl.NumberFormat('en-US').format(value ?? 0);

watch(selectedSis, () => {
  if (isApplyingRouteState.value) return;
  selectedSchool.value = 'all';
});

watch(selectedSignatureExplorer, () => {
  if (isApplyingRouteState.value) return;
  signatureExplorerGroupBy.value = defaultSignatureExplorerGroupBy;
  signatureExplorerBucket.value = null;
  signatureExplorerPage.value = 1;
});

watch(signatureExplorerGroupBy, () => {
  if (isApplyingRouteState.value) return;
  signatureExplorerBucket.value = null;
  signatureExplorerPage.value = 1;
});

watch(signatureExplorerBucket, () => {
  if (isApplyingRouteState.value) return;
  signatureExplorerPage.value = 1;
});

const sisOptions = computed(() => {
  const optionSource = sisCountsResponse.value ?? response.value;
  const countsBySis = new Map(
    (optionSource?.sisBreakdowns ?? []).map((row) => [row.key, row.totalErrors] as const),
  );

  const schoolSisPlatforms = new Set(
    (optionSource?.filterOptions.schools ?? [])
      .map((option) => option.sisPlatform)
      .filter((sisPlatform): sisPlatform is string => Boolean(sisPlatform)),
  );

  const sisPlatforms = (optionSource?.filterOptions.sisPlatforms ?? []).filter((sisPlatform) =>
    schoolSisPlatforms.has(sisPlatform),
  );
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

watch(sisOptions, (options) => {
  if (options.some((option) => option.value === selectedSis.value)) {
    return;
  }

  selectedSis.value = 'all';
});

const schoolOptions = computed(() => {
  const filteredSchools = (response.value?.filterOptions.schools ?? []).filter((option) =>
    selectedSis.value === 'all' || option.sisPlatform === selectedSis.value,
  );

  return [
    { value: 'all', label: 'All schools' },
    ...filteredSchools
      .slice()
      .sort((left, right) => left.label.localeCompare(right.label))
      .map((option) => ({
        value: option.value,
        label: formatSchoolLabel(option.value, option.label),
      })),
  ];
});

watch([selectedWindow, selectedSchool, selectedSis], () => {
  if (isApplyingRouteState.value) return;
  signaturePage.value = 1;
  detailPage.value = 1;
});

watch(detailSearch, () => {
  if (isApplyingRouteState.value) return;
  detailPage.value = 1;
});

const buildRouteQuery = () => {
  const nextQuery: Record<string, string | undefined> = {};

  if (activeView.value !== defaultView) nextQuery.view = activeView.value;
  if (selectedWindow.value !== defaultWindow) nextQuery.window = selectedWindow.value;
  if (selectedSis.value !== 'all') nextQuery.sis = selectedSis.value;
  if (selectedSchool.value !== 'all') nextQuery.school = selectedSchool.value;
  if (detailSearch.value.trim()) nextQuery.q = detailSearch.value.trim();
  if (activeView.value === 'aggregate' && signaturePage.value > 1) nextQuery.signaturePage = String(signaturePage.value);
  if (activeView.value === 'all' && detailPage.value > 1) nextQuery.detailPage = String(detailPage.value);
  if (detailSortBy.value !== defaultDetailSortBy) nextQuery.detailSortBy = detailSortBy.value;
  if (detailSortDir.value !== defaultDetailSortDir) nextQuery.detailSortDir = detailSortDir.value;
  if (sisSortBy.value !== defaultSisSortBy) nextQuery.sisSortBy = sisSortBy.value;
  if (sisSortDir.value !== defaultSisSortDir) nextQuery.sisSortDir = sisSortDir.value;

  if (selectedSignatureExplorer.value?.signatureKey) {
    nextQuery.signature = selectedSignatureExplorer.value.signatureKey;
    if (signatureExplorerGroupBy.value !== defaultSignatureExplorerGroupBy) {
      nextQuery.signatureGroupBy = signatureExplorerGroupBy.value;
    }
    if (signatureExplorerBucket.value) nextQuery.signatureBucket = signatureExplorerBucket.value;
    if (signatureExplorerPage.value > 1) nextQuery.signatureExplorerPage = String(signatureExplorerPage.value);
  }

  if (selectedSignatureExplorerSchools.value) {
    nextQuery.modal = 'signature-explorer-schools';
    nextQuery.modalRowId = String(selectedSignatureExplorerSchools.value.rowId);
    return nextQuery;
  }

  if (selectedErrorDetail.value?.routeState) {
    nextQuery.modal = 'error-detail';
    nextQuery.detailOrigin = selectedErrorDetail.value.routeState.origin;

    switch (selectedErrorDetail.value.routeState.origin) {
      case 'all':
        nextQuery.detailId = String(selectedErrorDetail.value.routeState.rowId);
        break;
      case 'signature-explorer':
        nextQuery.detailId = String(selectedErrorDetail.value.routeState.rowId);
        nextQuery.signature = selectedErrorDetail.value.routeState.signatureKey;
        if (signatureExplorerGroupBy.value !== defaultSignatureExplorerGroupBy) {
          nextQuery.signatureGroupBy = signatureExplorerGroupBy.value;
        }
        if (signatureExplorerBucket.value) nextQuery.signatureBucket = signatureExplorerBucket.value;
        if (signatureExplorerPage.value > 1) nextQuery.signatureExplorerPage = String(signatureExplorerPage.value);
        break;
      case 'signature-sample':
        nextQuery.signature = selectedErrorDetail.value.routeState.signatureKey;
        break;
      case 'school-sample':
        nextQuery.modalSchool = selectedErrorDetail.value.routeState.schoolKey;
        break;
    }

    return nextQuery;
  }

  if (selectedSisSignatureContext.value) {
    nextQuery.modal = 'sis-signatures';
    nextQuery.modalSis = selectedSisSignatureContext.value.key;
    return nextQuery;
  }

  if (selectedSignatureExplorer.value) {
    nextQuery.modal = 'signature-explorer';
  }

  return nextQuery;
};

const buildFullRouteQuery = () => {
  const preservedQuery = Object.fromEntries(
    Object.entries(route.query).filter(([key]) => !managedQueryKeys.includes(key as (typeof managedQueryKeys)[number])),
  );

  return {
    ...preservedQuery,
    ...buildRouteQuery(),
  };
};

const syncRouteQuery = () => {
  if (isApplyingRouteState.value) return;
  const nextQuery = buildFullRouteQuery();
  if (areQueryRecordsEqual(route.query as Record<string, unknown>, nextQuery)) return;
  router.replace({ query: nextQuery });
};

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
const signatureTotal = computed(() => allSignatures.value.length);
const signatureTotalPages = computed(() => Math.max(1, Math.ceil(signatureTotal.value / signaturePageSize)));
const topSignatures = computed(() => {
  const start = (signaturePage.value - 1) * signaturePageSize;
  return allSignatures.value.slice(start, start + signaturePageSize);
});
const schoolRows = computed(() => response.value?.schoolBreakdowns ?? []);
const sisRows = computed(() => response.value?.sisBreakdowns ?? []);
const detailRows = computed(() => detailResponse.value?.rows ?? []);
const detailTotal = computed(() => detailResponse.value?.total ?? 0);
const detailTotalPages = computed(() => Math.max(1, Math.ceil(detailTotal.value / detailPageSize)));
const hasDetailRows = computed(() => detailRows.value.length > 0);
const signatureExplorerTabs: Array<{ value: ErrorSignatureExplorerGroupBy; label: string }> = [
  { value: 'sis', label: 'SIS' },
  { value: 'school', label: 'School' },
  { value: 'term', label: 'Term' },
];
const signatureExplorerBuckets = computed(() =>
  signatureExplorerResponse.value?.breakdowns[signatureExplorerGroupBy.value] ?? [],
);
const signatureExplorerRows = computed(() => signatureExplorerResponse.value?.rows ?? []);
const signatureExplorerTotal = computed(() => signatureExplorerResponse.value?.total ?? 0);
const signatureExplorerBucketTotal = computed(() => signatureExplorerResponse.value?.metadata.bucketTotal ?? 0);
const signatureExplorerTotalPages = computed(() =>
  Math.max(1, Math.ceil(signatureExplorerTotal.value / signatureExplorerPageSize)),
);
const hasSignatureExplorerRows = computed(() => signatureExplorerRows.value.length > 0);
const selectedSignatureExplorerBucketSummary = computed(() =>
  signatureExplorerBuckets.value.find((bucket) => bucket.key === signatureExplorerBucket.value) ?? null,
);

watch(signatureTotalPages, (pages) => {
  if (signaturePage.value > pages) {
    signaturePage.value = pages;
  }
});

watch(signatureExplorerBuckets, (buckets) => {
  if (!selectedSignatureExplorer.value) return;
  if (!buckets.length) {
    if (signatureExplorerBucket.value !== null) {
      signatureExplorerBucket.value = null;
    }
    return;
  }

  if (signatureExplorerBucket.value && buckets.some((bucket) => bucket.key === signatureExplorerBucket.value)) {
    return;
  }

  signatureExplorerBucket.value = buckets[0].key;
});

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

const errorDetailHeading = computed(() => 'Full upstream error');

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

const formatShare = (value?: number | null) => `${Math.round((value ?? 0) * 100)}%`;

const formatSignatureExplorerBucketLabel = (bucket: ErrorSignatureExplorerBucket) => {
  if (signatureExplorerGroupBy.value === 'school') {
    return formatSchoolLabel(bucket.key, bucket.label);
  }
  return bucket.label;
};

const formatSignatureExplorerSchoolCount = (school: ErrorSignatureExplorerSchoolCount) => {
  const label = school.label === 'Unknown' ? 'Unknown' : formatSchoolLabel(school.school, school.label);
  if (school.count <= 1) {
    return label;
  }
  return `${label} · ${formatCount(school.count)}`;
};

const sortSignatureExplorerSchools = (schools: ErrorSignatureExplorerSchoolCount[]) =>
  [...schools].sort((left, right) => {
    const countDiff = right.count - left.count;
    if (countDiff !== 0) return countDiff;
    return formatSignatureExplorerSchoolCount(left).localeCompare(formatSignatureExplorerSchoolCount(right), undefined, {
      sensitivity: 'base',
    });
  });

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
  routeState: {
    origin: 'signature-sample',
    signatureKey: signature.signatureKey,
  },
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
    routeState: {
      origin: 'school-sample',
      schoolKey: row.key,
    },
  };
};

const buildDetailRowErrorContext = (
  row: ErrorDetailRow,
  routeState: Extract<ErrorDetailRouteState, { origin: 'all' | 'signature-explorer' }>,
): ErrorDetailContext => ({
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
  routeState,
});

const openSignatureExplorer = (signature: ErrorSignatureCluster) => {
  selectedSignatureExplorer.value = signature;
};

const openSchoolErrorDetail = (row: ErrorBreakdownRow) => {
  selectedErrorDetail.value = buildSchoolErrorDetail(row);
};

const openDetailRowError = (row: ErrorDetailRow) => {
  selectedErrorDetail.value = buildDetailRowErrorContext(row, {
    origin: 'all',
    rowId: row.id,
  });
};

const openSignatureExplorerDetailRow = (row: (typeof signatureExplorerRows.value)[number]) => {
  if (!selectedSignatureExplorer.value) return;
  selectedErrorDetail.value = buildDetailRowErrorContext(row, {
    origin: 'signature-explorer',
    rowId: row.id,
    signatureKey: selectedSignatureExplorer.value.signatureKey,
  });
};

const openSignatureExplorerSchools = (row: (typeof signatureExplorerRows.value)[number]) => {
  selectedSignatureExplorerSchools.value = {
    rowId: row.id,
    fullErrorText: row.fullErrorText,
    instanceCount: row.instanceCount,
    schools: sortSignatureExplorerSchools(row.schools),
    sisPlatform: row.sisPlatform,
    termCode: row.termCodes[0] || null,
  };
};

const openSisSignatures = (row: ErrorBreakdownRow) => {
  selectedSisSignatureContext.value = {
    key: row.key,
    label: row.label,
    associatedSignatures: row.associatedSignatures ?? [],
  };
};

const openDominantSignature = (row: ErrorBreakdownRow) => {
  const signature = findDominantSignature(row);
  if (!signature) return;
  openSignatureExplorer(signature);
};

const shouldIgnoreRowActivation = (target: EventTarget | null) =>
  target instanceof Element && Boolean(target.closest('a, button, input, select, textarea, summary'));

const activateRow = (event: MouseEvent | KeyboardEvent, action?: (() => void) | null) => {
  if (!action || shouldIgnoreRowActivation(event.target)) return;
  if (event instanceof KeyboardEvent) {
    event.preventDefault();
  }
  action();
};

const handleSignatureRowActivation = (event: MouseEvent | KeyboardEvent, signature: ErrorSignatureCluster) => {
  activateRow(event, () => openSignatureExplorer(signature));
};

const handleDetailRowActivation = (event: MouseEvent | KeyboardEvent, row: ErrorDetailRow) => {
  activateRow(event, () => openDetailRowError(row));
};

const hasSchoolRowModal = (row: ErrorBreakdownRow) => Boolean(findDominantSignature(row));

const handleSchoolRowActivation = (event: MouseEvent | KeyboardEvent, row: ErrorBreakdownRow) => {
  activateRow(event, hasSchoolRowModal(row) ? () => openSchoolErrorDetail(row) : null);
};

const hasSisRowModal = (row: ErrorBreakdownRow) => Boolean(row.associatedSignatures?.length || findDominantSignature(row));

const handleSisRowActivation = (event: MouseEvent | KeyboardEvent, row: ErrorBreakdownRow) => {
  activateRow(event, hasSisRowModal(row)
    ? () => {
      if (row.associatedSignatures?.length) {
        openSisSignatures(row);
        return;
      }
      openDominantSignature(row);
    }
    : null);
};

const handleSignatureExplorerRowActivation = (
  event: MouseEvent | KeyboardEvent,
  row: (typeof signatureExplorerRows.value)[number],
) => {
  activateRow(event, () => openSignatureExplorerDetailRow(row));
};

const openSignatureRepresentativeSample = () => {
  if (!selectedSignatureExplorer.value) return;
  selectedErrorDetail.value = buildSignatureErrorDetail(selectedSignatureExplorer.value);
};

const closeSignatureExplorer = () => {
  selectedSignatureExplorer.value = null;
};

const closeErrorDetail = () => {
  selectedErrorDetail.value = null;
};

const closeSignatureExplorerSchools = () => {
  selectedSignatureExplorerSchools.value = null;
};

const closeSisSignatures = () => {
  selectedSisSignatureContext.value = null;
};

const openSignatureDetailByKey = (signatureKey: string) => {
  const signature = allSignatures.value.find((item) => item.signatureKey === signatureKey);
  if (!signature) return;
  selectedSisSignatureContext.value = null;
  openSignatureExplorer(signature);
};

const selectSignatureExplorerBucket = (bucketKey: string) => {
  if (signatureExplorerBucket.value === bucketKey) return;
  signatureExplorerBucket.value = bucketKey;
};

const changeSignatureExplorerPage = (nextPage: number) => {
  const boundedPage = Math.min(Math.max(nextPage, 1), signatureExplorerTotalPages.value);
  if (boundedPage === signatureExplorerPage.value) return;
  signatureExplorerPage.value = boundedPage;
};

const changeSignaturePage = (nextPage: number) => {
  const boundedPage = Math.min(Math.max(nextPage, 1), signatureTotalPages.value);
  if (boundedPage === signaturePage.value) return;
  signaturePage.value = boundedPage;
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

const applyStateFromRouteQuery = () => {
  selectedWindow.value = coerceWindowQuery(readQueryValue(route.query.window));
  selectedSis.value = readQueryValue(route.query.sis) || 'all';
  selectedSchool.value = readQueryValue(route.query.school) || 'all';
  activeView.value = coerceViewQuery(readQueryValue(route.query.view));
  detailSearch.value = readQueryValue(route.query.q) ?? '';
  signaturePage.value = parsePositiveInt(readQueryValue(route.query.signaturePage));
  detailPage.value = parsePositiveInt(readQueryValue(route.query.detailPage));
  detailSortBy.value = coerceDetailSortByQuery(readQueryValue(route.query.detailSortBy));
  detailSortDir.value = coerceSortDirQuery(readQueryValue(route.query.detailSortDir), defaultDetailSortDir);
  sisSortBy.value = coerceSisSortByQuery(readQueryValue(route.query.sisSortBy));
  sisSortDir.value = coerceSortDirQuery(readQueryValue(route.query.sisSortDir), defaultSisSortDir);
  signatureExplorerGroupBy.value = coerceSignatureExplorerGroupByQuery(readQueryValue(route.query.signatureGroupBy));
  signatureExplorerBucket.value = readQueryValue(route.query.signatureBucket) ?? null;
  signatureExplorerPage.value = parsePositiveInt(readQueryValue(route.query.signatureExplorerPage));
};

const restoreModalStateFromRouteQuery = () => {
  const modal = coerceModalQuery(readQueryValue(route.query.modal));
  const signatureKey = readQueryValue(route.query.signature);
  const detailOrigin = coerceDetailOriginQuery(readQueryValue(route.query.detailOrigin));
  const detailId = parsePositiveInt(readQueryValue(route.query.detailId), 0);
  const modalSisKey = readQueryValue(route.query.modalSis);
  const modalSchoolKey = readQueryValue(route.query.modalSchool);
  const modalRowId = parsePositiveInt(readQueryValue(route.query.modalRowId), 0);
  const signature = signatureKey
    ? allSignatures.value.find((item) => item.signatureKey === signatureKey) ?? null
    : null;

  if (!modal) {
    selectedSisSignatureContext.value = null;
    selectedSignatureExplorerSchools.value = null;
    selectedErrorDetail.value = null;
    selectedSignatureExplorer.value = null;
    return;
  }

  switch (modal) {
    case 'sis-signatures': {
      const row = modalSisKey ? sortSisRows.value.find((item) => item.key === modalSisKey) ?? null : null;
      selectedSignatureExplorer.value = null;
      selectedSignatureExplorerSchools.value = null;
      selectedErrorDetail.value = null;
      selectedSisSignatureContext.value = row
        ? {
          key: row.key,
          label: row.label,
          associatedSignatures: row.associatedSignatures ?? [],
        }
        : null;
      return;
    }
    case 'signature-explorer': {
      selectedSisSignatureContext.value = null;
      selectedSignatureExplorerSchools.value = null;
      selectedErrorDetail.value = null;
      selectedSignatureExplorer.value = signature;
      return;
    }
    case 'signature-explorer-schools': {
      selectedSisSignatureContext.value = null;
      selectedErrorDetail.value = null;
      selectedSignatureExplorer.value = signature;

      const row = modalRowId
        ? signatureExplorerRows.value.find((item) => item.id === modalRowId) ?? null
        : null;

      selectedSignatureExplorerSchools.value = row
        ? {
          rowId: row.id,
          fullErrorText: row.fullErrorText,
          instanceCount: row.instanceCount,
          schools: sortSignatureExplorerSchools(row.schools),
          sisPlatform: row.sisPlatform,
          termCode: row.termCodes[0] || null,
        }
        : null;
      return;
    }
    case 'error-detail': {
      selectedSisSignatureContext.value = null;
      selectedSignatureExplorerSchools.value = null;

      switch (detailOrigin) {
        case 'all': {
          const row = detailId ? detailRows.value.find((item) => item.id === detailId) ?? null : null;
          selectedSignatureExplorer.value = null;
          selectedErrorDetail.value = row
            ? buildDetailRowErrorContext(row, {
              origin: 'all',
              rowId: row.id,
            })
            : null;
          return;
        }
        case 'school-sample': {
          const row = modalSchoolKey ? sortSchoolRows.value.find((item) => item.key === modalSchoolKey) ?? null : null;
          selectedSignatureExplorer.value = null;
          selectedErrorDetail.value = row ? buildSchoolErrorDetail(row) : null;
          return;
        }
        case 'signature-sample': {
          selectedSignatureExplorer.value = signature;
          selectedErrorDetail.value = signature ? buildSignatureErrorDetail(signature) : null;
          return;
        }
        case 'signature-explorer': {
          const row = detailId ? signatureExplorerRows.value.find((item) => item.id === detailId) ?? null : null;
          selectedSignatureExplorer.value = signature;
          selectedErrorDetail.value = row && signature
            ? buildDetailRowErrorContext(row, {
              origin: 'signature-explorer',
              rowId: row.id,
              signatureKey: signature.signatureKey,
            })
            : null;
          return;
        }
        default:
          selectedSignatureExplorer.value = null;
          selectedErrorDetail.value = null;
          return;
      }
    }
  }
};

watch(
  () => route.query,
  () => {
    isApplyingRouteState.value = true;
    applyStateFromRouteQuery();
    restoreModalStateFromRouteQuery();
    isApplyingRouteState.value = false;
  },
  { immediate: true, deep: true },
);

watch([response, detailResponse, signatureExplorerResponse], () => {
  if (!coerceModalQuery(readQueryValue(route.query.modal))) return;
  isApplyingRouteState.value = true;
  restoreModalStateFromRouteQuery();
  isApplyingRouteState.value = false;
});

watch(
  () => buildRouteQuery(),
  () => {
    syncRouteQuery();
  },
  { deep: true },
);

const handleExport = async () => {
  if (isExporting.value) return;
  isExporting.value = true;
  try {
    const exportParams = {
      days: daysParam.value,
      school: selectedSchool.value === 'all' ? undefined : selectedSchool.value,
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
          <span v-if="isStaticDataMode" class="inline-flex rounded-full bg-amber-100 px-3 py-1.5 text-amber-800">Read-only static snapshot</span>
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
                  <div
                    v-for="option in viewOptions"
                    :key="option.value"
                    class="inline-flex items-center gap-2"
                  >
                    <button
                      class="rounded-full px-4 py-2 text-sm font-semibold transition"
                      :class="activeView === option.value ? 'bg-slate-900 text-white' : 'bg-slate-100 text-slate-700 hover:bg-slate-200'"
                      @click="activeView = option.value"
                    >
                      {{ option.label }}
                    </button>
                    <span
                      v-if="option.value === 'aggregate'"
                      :class="descriptorClass"
                      data-testid="error-analysis-signatures-tooltip"
                    >
                      <button
                        type="button"
                        class="cursor-help"
                        :class="descriptorButtonClass"
                        :aria-label="signaturesTooltipTitle"
                        @click.stop
                      >
                        ?
                      </button>
                      <div :class="descriptorPopoverClass">
                        <p class="font-semibold text-slate-900">{{ signaturesTooltipTitle }}</p>
                        <p class="mt-1">{{ signaturesTooltipBody }}</p>
                        <p class="mt-2">{{ signaturesTooltipDevelopment }}</p>
                      </div>
                    </span>
                  </div>
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

              <div>
                <label class="mb-2 block text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">School</label>
                <select v-model="selectedSchool" data-testid="school-filter" class="w-full rounded-full border border-slate-200 bg-white px-4 py-2.5 text-sm text-slate-900 shadow-sm focus:border-slate-400 focus:outline-none">
                  <option v-for="option in schoolOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                </select>
              </div>
            </div>
          </Card>

          <Card subtitle="Recurring Patterns" title="Top error signatures">
            <div class="mt-4 flex flex-col gap-3 border-b border-slate-200 pb-4 text-sm text-slate-600 sm:flex-row sm:items-center sm:justify-between">
              <p>{{ formatCount(signatureTotal) }} signature pattern{{ signatureTotal === 1 ? '' : 's' }} total</p>
              <div class="flex items-center gap-3">
                <button
                  type="button"
                  class="rounded-full border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="signaturePage <= 1"
                  @click="changeSignaturePage(signaturePage - 1)"
                >
                  Previous
                </button>
                <span>Page {{ signaturePage }} of {{ signatureTotalPages }}</span>
                <button
                  type="button"
                  class="rounded-full border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="signaturePage >= signatureTotalPages"
                  @click="changeSignaturePage(signaturePage + 1)"
                >
                  Next
                </button>
              </div>
            </div>
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
                  <tr
                    v-for="signature in topSignatures"
                    :key="signature.signatureKey"
                    :class="interactiveRowClass"
                    data-testid="signature-row"
                    tabindex="0"
                    @click="handleSignatureRowActivation($event, signature)"
                    @keydown.enter="handleSignatureRowActivation($event, signature)"
                    @keydown.space="handleSignatureRowActivation($event, signature)"
                  >
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
                          @click="openSignatureExplorer(signature)"
                        >
                          Explore signature
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

            <div class="mt-4 flex flex-col gap-3 border-t border-slate-200 pt-4 text-sm text-slate-600 sm:flex-row sm:items-center sm:justify-between">
              <p>{{ formatCount(signatureTotal) }} signature pattern{{ signatureTotal === 1 ? '' : 's' }} total</p>
              <div class="flex items-center gap-3">
                <button
                  type="button"
                  class="rounded-full border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="signaturePage <= 1"
                  @click="changeSignaturePage(signaturePage - 1)"
                >
                  Previous
                </button>
                <span>Page {{ signaturePage }} of {{ signatureTotalPages }}</span>
                <button
                  type="button"
                  class="rounded-full border border-slate-200 px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="signaturePage >= signatureTotalPages"
                  @click="changeSignaturePage(signaturePage + 1)"
                >
                  Next
                </button>
              </div>
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
                  <tr
                    v-for="row in detailRows"
                    :key="row.id"
                    :class="interactiveRowClass"
                    data-testid="detail-row"
                    tabindex="0"
                    @click="handleDetailRowActivation($event, row)"
                    @keydown.enter="handleDetailRowActivation($event, row)"
                    @keydown.space="handleDetailRowActivation($event, row)"
                  >
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
                  <tr
                    v-for="row in sortSchoolRows"
                    :key="row.key"
                    :class="hasSchoolRowModal(row) ? interactiveRowClass : ''"
                    data-testid="school-row"
                    :tabindex="hasSchoolRowModal(row) ? 0 : undefined"
                    @click="handleSchoolRowActivation($event, row)"
                    @keydown.enter="handleSchoolRowActivation($event, row)"
                    @keydown.space="handleSchoolRowActivation($event, row)"
                  >
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
                      <p class="mt-2 text-xs leading-5 text-slate-500">{{ row.likelyNextStep || 'Inspect the latest merge error samples for this school.' }}</p>
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
                  <tr
                    v-for="row in sortSisRows"
                    :key="row.key"
                    :class="hasSisRowModal(row) ? interactiveRowClass : ''"
                    data-testid="sis-row"
                    :tabindex="hasSisRowModal(row) ? 0 : undefined"
                    @click="handleSisRowActivation($event, row)"
                    @keydown.enter="handleSisRowActivation($event, row)"
                    @keydown.space="handleSisRowActivation($event, row)"
                  >
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
                  Open signature
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div
      v-if="selectedSignatureExplorer"
      class="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/45 px-4 py-8"
      data-testid="signature-explorer-modal"
      @click.self="closeSignatureExplorer"
    >
      <div class="max-h-[90vh] w-full max-w-6xl overflow-y-auto rounded-[28px] border border-slate-200 bg-white p-6 shadow-2xl sm:p-8">
        <div class="flex items-start justify-between gap-4">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Signature explorer</p>
            <h2 class="mt-2 text-2xl font-semibold text-slate-950">{{ buildSignatureHeadline(selectedSignatureExplorer.signatureLabel) }}</h2>
            <p class="mt-3 text-sm leading-6 text-slate-600">{{ selectedSignatureExplorer.signatureLabel }}</p>
          </div>
          <button
            type="button"
            class="inline-flex h-10 w-10 items-center justify-center rounded-full border border-slate-200 text-lg text-slate-500 transition hover:border-slate-300 hover:text-slate-900"
            aria-label="Close signature explorer modal"
            @click="closeSignatureExplorer"
          >
            ×
          </button>
        </div>

        <div class="mt-6 flex flex-wrap gap-2 text-xs text-slate-600">
          <span v-if="selectedSignatureExplorer.entityType" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedSignatureExplorer.entityType }}</span>
          <span v-if="selectedSignatureExplorer.errorCode" class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedSignatureExplorer.errorCode }}</span>
          <span class="rounded-full bg-slate-100 px-3 py-1.5">{{ formatCount(selectedSignatureExplorer.totalCount) }} captured occurrences</span>
          <span class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedSignatureExplorer.recurrenceDays }} captured day{{ selectedSignatureExplorer.recurrenceDays === 1 ? '' : 's' }}</span>
          <span class="rounded-full bg-slate-100 px-3 py-1.5">{{ selectedSignatureExplorer.affectedSchools }} school{{ selectedSignatureExplorer.affectedSchools === 1 ? '' : 's' }}</span>
        </div>

        <div class="mt-6 grid gap-4 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,0.8fr)]">
          <div class="rounded-3xl border border-slate-200 bg-slate-50 p-5">
            <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Pattern summary</p>
            <p class="mt-3 text-sm leading-6 text-slate-900">{{ selectedSignatureExplorer.resolutionHint.action }}</p>
            <div class="mt-4 flex flex-wrap gap-2">
              <button
                type="button"
                class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700"
                @click="openSignatureRepresentativeSample"
              >
                Representative sample
              </button>
              <router-link
                v-if="selectedSignatureExplorer.dominantSchool"
                :to="schoolRoute(selectedSignatureExplorer.dominantSchool)"
                class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
              >
                School detail
              </router-link>
              <a
                v-if="selectedSignatureExplorer.dominantSchoolMergeReport"
                :href="getMergeReportUrl(selectedSignatureExplorer.dominantSchoolMergeReport)"
                target="_blank"
                rel="noopener noreferrer"
                class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
              >
                Merge report ↗
              </a>
            </div>
          </div>

          <div class="rounded-3xl border border-slate-200 bg-white p-5">
            <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Current-state drilldown</p>
            <p class="mt-3 text-sm leading-6 text-slate-600">
              Uses the latest captured snapshot for this signature and the current page filters.
            </p>
            <div class="mt-4 grid gap-3 sm:grid-cols-2">
              <div class="rounded-2xl bg-slate-50 p-4">
                <p class="text-xs uppercase tracking-[0.08em] text-slate-500">Latest snapshot</p>
                <p class="mt-2 text-lg font-semibold text-slate-950">{{ signatureExplorerResponse?.metadata.resolvedSnapshotDate || 'Unavailable' }}</p>
              </div>
              <div class="rounded-2xl bg-slate-50 p-4">
                <p class="text-xs uppercase tracking-[0.08em] text-slate-500">Open rows now</p>
                <p class="mt-2 text-lg font-semibold text-slate-950">{{ formatCount(signatureExplorerResponse?.metadata.signatureTotal) }}</p>
              </div>
            </div>
          </div>
        </div>

        <div class="mt-6 rounded-3xl border border-slate-200 bg-white p-5">
          <div class="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Within this signature</p>
              <p class="mt-2 text-sm leading-6 text-slate-600">Pivot the currently open rows by SIS, school, or term, then inspect the captured errors for the selected bucket.</p>
            </div>
            <div class="inline-flex flex-wrap rounded-full border border-slate-200 bg-slate-50 p-1">
              <button
                v-for="tab in signatureExplorerTabs"
                :key="tab.value"
                type="button"
                class="rounded-full px-3 py-1.5 text-sm font-medium transition"
                :class="signatureExplorerGroupBy === tab.value ? 'bg-slate-900 text-white' : 'text-slate-600 hover:text-slate-900'"
                :data-testid="`signature-explorer-tab-${tab.value}`"
                @click="signatureExplorerGroupBy = tab.value"
              >
                {{ tab.label }}
              </button>
            </div>
          </div>

          <div v-if="isLoadingSignatureExplorer" class="mt-6 rounded-3xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">
            Loading latest signature drilldown...
          </div>

          <div
            v-else-if="(signatureExplorerResponse?.metadata.signatureTotal ?? 0) === 0"
            class="mt-6 rounded-3xl border border-amber-200 bg-amber-50 p-5 text-sm text-amber-800"
          >
            No currently open rows match this signature in the latest captured snapshot for the active filters.
          </div>

          <div v-else class="mt-6 grid gap-5 xl:grid-cols-[minmax(260px,0.9fr)_minmax(0,2.1fr)]">
            <div class="rounded-3xl border border-slate-200 bg-slate-50 p-4">
              <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Buckets</p>
              <div class="mt-3 space-y-2">
                <button
                  v-for="bucket in signatureExplorerBuckets"
                  :key="bucket.key"
                  type="button"
                  class="flex w-full items-center justify-between rounded-2xl border px-3 py-3 text-left transition"
                  :class="signatureExplorerBucket === bucket.key ? 'border-slate-900 bg-slate-900 text-white' : 'border-slate-200 bg-white text-slate-800 hover:border-slate-300'"
                  data-testid="signature-explorer-bucket"
                  @click="selectSignatureExplorerBucket(bucket.key)"
                >
                  <span class="min-w-0 pr-3">
                    <span class="block truncate text-sm font-semibold">{{ formatSignatureExplorerBucketLabel(bucket) }}</span>
                    <span class="mt-1 block text-xs" :class="signatureExplorerBucket === bucket.key ? 'text-slate-200' : 'text-slate-500'">
                      {{ formatCount(bucket.count) }} rows · {{ formatShare(bucket.share) }}
                    </span>
                  </span>
                  <span class="shrink-0 text-xs font-semibold">{{ formatShare(bucket.share) }}</span>
                </button>
              </div>
            </div>

            <div class="rounded-3xl border border-slate-200 bg-white p-4">
              <div class="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                <div>
                  <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Matching errors</p>
                  <p class="mt-1 text-sm text-slate-600">
                    Showing {{ formatCount(signatureExplorerRows.length) }} of {{ formatCount(signatureExplorerTotal) }} grouped error{{ signatureExplorerTotal === 1 ? '' : 's' }}
                    covering {{ formatCount(signatureExplorerBucketTotal) }} row{{ signatureExplorerBucketTotal === 1 ? '' : 's' }}
                    <span v-if="selectedSignatureExplorerBucketSummary">
                      for {{ formatSignatureExplorerBucketLabel(selectedSignatureExplorerBucketSummary) }}
                    </span>
                  </p>
                </div>
                <p class="text-xs text-slate-500">Page {{ signatureExplorerPage }} of {{ signatureExplorerTotalPages }}</p>
              </div>

              <div v-if="hasSignatureExplorerRows" class="mt-4 overflow-x-auto">
                <table class="min-w-full text-left text-sm text-slate-700">
                  <thead class="text-xs uppercase tracking-[0.12em] text-slate-500">
                    <tr>
                      <th class="px-3 py-2 font-semibold">Affected schools</th>
                      <th class="px-3 py-2 font-semibold">SIS</th>
                      <th class="px-3 py-2 font-semibold">Term</th>
                      <th class="px-3 py-2 font-semibold">Count</th>
                      <th class="px-3 py-2 font-semibold">Error</th>
                      <th class="px-3 py-2 font-semibold">Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr
                      v-for="row in signatureExplorerRows"
                      :key="row.key"
                      class="border-t border-slate-200"
                      :class="interactiveRowClass"
                      data-testid="signature-explorer-row"
                      tabindex="0"
                      @click="handleSignatureExplorerRowActivation($event, row)"
                      @keydown.enter="handleSignatureExplorerRowActivation($event, row)"
                      @keydown.space="handleSignatureExplorerRowActivation($event, row)"
                    >
                      <td class="px-3 py-3 align-top">
                        <div class="min-w-0">
                          <p class="font-semibold text-slate-900">
                            {{ formatCount(row.schools.length) }} school{{ row.schools.length === 1 ? '' : 's' }}
                          </p>
                          <button
                            v-if="row.schools.length > 1"
                            type="button"
                            class="mt-3 inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                            data-testid="signature-explorer-school-summary-trigger"
                            @click="openSignatureExplorerSchools(row)"
                          >
                            View school list
                          </button>
                        </div>
                      </td>
                      <td class="px-3 py-3 align-top">{{ row.sisPlatform || 'Unknown' }}</td>
                      <td class="px-3 py-3 align-top">{{ row.termCodes[0] || 'Unknown' }}</td>
                      <td class="px-3 py-3 align-top font-semibold text-slate-900">{{ formatCount(row.instanceCount) }}</td>
                      <td class="px-3 py-3 align-top">
                        <p class="font-medium leading-6 text-slate-900 break-words">{{ truncateText(row.fullErrorText, 180) }}</p>
                        <p v-if="row.entityDisplayName" class="mt-1 text-xs text-slate-500">{{ row.entityDisplayName }}</p>
                      </td>
                      <td class="px-3 py-3 align-top">
                        <div class="flex flex-wrap gap-2">
                          <button
                            type="button"
                            class="inline-flex items-center rounded-full bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white transition hover:bg-slate-700"
                            data-testid="signature-explorer-error-trigger"
                            @click="openSignatureExplorerDetailRow(row)"
                          >
                            View full error
                          </button>
                          <a
                            v-if="row.mergeReport"
                            :href="getMergeReportUrl(row.mergeReport)"
                            target="_blank"
                            rel="noopener noreferrer"
                            class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950"
                          >
                            Merge report ↗
                          </a>
                        </div>
                      </td>
                    </tr>
                  </tbody>
                </table>
              </div>

              <div v-else class="mt-4 rounded-3xl border border-slate-200 bg-slate-50 p-5 text-sm text-slate-600">
                No matching rows for the selected bucket.
              </div>

              <div class="mt-4 flex items-center justify-between">
                <button
                  type="button"
                  class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="signatureExplorerPage <= 1"
                  @click="changeSignatureExplorerPage(signatureExplorerPage - 1)"
                >
                  Previous
                </button>
                <button
                  type="button"
                  class="inline-flex items-center rounded-full border border-slate-200 bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                  :disabled="signatureExplorerPage >= signatureExplorerTotalPages"
                  @click="changeSignatureExplorerPage(signatureExplorerPage + 1)"
                >
                  Next
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

        <div class="mt-6 space-y-4">
          <div class="rounded-3xl border border-slate-200 bg-slate-50 p-5">
            <p class="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">{{ errorDetailMessageLabel }}</p>
            <pre class="mt-3 whitespace-pre-wrap break-words font-sans text-sm leading-6 text-slate-800">{{ selectedErrorDetail.fullErrorText }}</pre>
          </div>

          <div class="grid gap-4 lg:grid-cols-2">
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
                  Integration Hub ↗
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

            <div v-if="selectedErrorDetail.impactedSchools?.length" class="rounded-3xl border border-slate-200 bg-white p-5 lg:col-span-2">
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

            <div v-if="selectedErrorDetail.exampleMergeReports?.length" class="rounded-3xl border border-slate-200 bg-white p-5 lg:col-span-2">
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

    <div
      v-if="selectedSignatureExplorerSchools"
      class="fixed inset-0 z-[60] flex items-center justify-center bg-slate-950/45 px-4 py-8"
      data-testid="signature-explorer-schools-modal"
      @click.self="closeSignatureExplorerSchools"
    >
      <div class="max-h-[85vh] w-full max-w-2xl overflow-y-auto rounded-[28px] border border-slate-200 bg-white p-6 shadow-2xl sm:p-8">
        <div class="flex items-start justify-between gap-4">
          <div>
            <p class="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Affected schools</p>
            <h2 class="mt-2 text-2xl font-semibold text-slate-950">
              {{ formatCount(selectedSignatureExplorerSchools.schools.length) }} school{{ selectedSignatureExplorerSchools.schools.length === 1 ? '' : 's' }}
            </h2>
            <p class="mt-3 text-sm leading-6 text-slate-600">
              {{ truncateText(selectedSignatureExplorerSchools.fullErrorText, 180) }}
            </p>
          </div>
          <button
            type="button"
            class="inline-flex h-10 w-10 items-center justify-center rounded-full border border-slate-200 text-lg text-slate-500 transition hover:border-slate-300 hover:text-slate-900"
            aria-label="Close affected schools modal"
            @click="closeSignatureExplorerSchools"
          >
            ×
          </button>
        </div>

        <div class="mt-6 flex flex-wrap gap-2 text-xs text-slate-600">
          <span v-if="selectedSignatureExplorerSchools.sisPlatform" class="rounded-full bg-slate-100 px-3 py-1.5">
            {{ selectedSignatureExplorerSchools.sisPlatform }}
          </span>
          <span v-if="selectedSignatureExplorerSchools.termCode" class="rounded-full bg-slate-100 px-3 py-1.5">
            Term {{ selectedSignatureExplorerSchools.termCode }}
          </span>
          <span class="rounded-full bg-slate-100 px-3 py-1.5">
            {{ formatCount(selectedSignatureExplorerSchools.instanceCount) }} open row{{ selectedSignatureExplorerSchools.instanceCount === 1 ? '' : 's' }}
          </span>
        </div>

        <div class="mt-6 space-y-3">
          <div
            v-for="school in selectedSignatureExplorerSchools.schools"
            :key="school.school"
            class="flex items-center justify-between gap-4 rounded-3xl border border-slate-200 bg-slate-50 px-4 py-3"
          >
            <div class="min-w-0">
              <p class="font-semibold text-slate-900">{{ school.label === 'Unknown' ? 'Unknown' : formatSchoolLabel(school.school, school.label) }}</p>
              <p class="mt-1 text-xs text-slate-500">{{ formatCount(school.count) }} matching row{{ school.count === 1 ? '' : 's' }}</p>
            </div>
            <span class="rounded-full bg-white px-3 py-1.5 text-xs font-semibold text-slate-700">
              {{ formatCount(school.count) }}
            </span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>
