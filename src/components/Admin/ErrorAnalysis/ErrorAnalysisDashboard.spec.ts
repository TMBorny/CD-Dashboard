import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { enableAutoUnmount, mount, RouterLinkStub } from '@vue/test-utils';
import { computed, nextTick, ref } from 'vue';
import type { VueWrapper } from '@vue/test-utils';
import type {
  ErrorAnalysisResponse,
  ErrorDetailTableResponse,
  ErrorSignatureExplorerResponse,
} from '@/types/errorAnalysis';

const queryData = ref<ErrorAnalysisResponse | null>(null);
const sisCountsQueryData = ref<ErrorAnalysisResponse | null>(null);
const detailQueryData = ref<ErrorDetailTableResponse | null>(null);
const signatureExplorerQueryData = ref<ErrorSignatureExplorerResponse | null>(null);
const findViewToggleButton = (wrapper: VueWrapper<any>, label: string) =>
  wrapper.get('[data-testid="view-toggle"]').findAll('button').find((button) => button.text().trim() === label);
const queryLoading = ref(false);
const queryError = ref<Error | null>(null);
const downloadErrorAnalysisExportMock = vi.fn().mockResolvedValue({
  blob: new Blob(['{"ok":true}'], { type: 'application/json' }),
  filename: 'error-analysis-export.json',
});
const downloadErrorAnalysisDetailedExportMock = vi.fn().mockResolvedValue({
  blob: new Blob(['{"ok":true}'], { type: 'application/json' }),
  filename: 'error-analysis-all-errors.json',
});
const route = {
  query: {} as Record<string, unknown>,
};
const replace = vi.fn().mockImplementation(({ query }: { query: Record<string, unknown> }) => {
  route.query = { ...query };
  return Promise.resolve();
});
const push = vi.fn();

vi.mock('@tanstack/vue-query', () => ({
  keepPreviousData: Symbol('keepPreviousData'),
  useQuery: vi.fn((options) => {
    const queryRef = computed(() => options.queryKey?.value ?? options.queryKey);
    const queryName = computed(() => {
      const keyValue = queryRef.value;
      return Array.isArray(keyValue) ? keyValue[0] : null;
    });
    if (queryName.value === 'errorAnalysisDetails') {
      return {
        data: detailQueryData,
        isLoading: queryLoading,
        error: queryError,
      };
    }
    if (queryName.value === 'errorAnalysisSignatureExplorer') {
      return {
        data: signatureExplorerQueryData,
        isLoading: queryLoading,
        error: queryError,
      };
    }
    if (queryName.value === 'errorAnalysisSisCounts') {
      return {
        data: sisCountsQueryData,
        isLoading: queryLoading,
        error: queryError,
      };
    }
    return {
      data: queryData,
      isLoading: queryLoading,
      error: queryError,
    };
  }),
}));

vi.mock('@/api', () => ({
  getErrorAnalysis: vi.fn().mockResolvedValue({ data: {} }),
  getErrorAnalysisErrors: vi.fn().mockResolvedValue({ data: {} }),
  getErrorAnalysisSignatureExplorer: vi.fn().mockResolvedValue({ data: {} }),
  downloadErrorAnalysisExport: downloadErrorAnalysisExportMock,
  downloadErrorAnalysisDetailedExport: downloadErrorAnalysisDetailedExportMock,
}));

vi.mock('vue3-apexcharts', () => ({
  default: {
    name: 'VueApexCharts',
    template: '<div class="chart-stub" />',
  },
}));

vi.mock('vue-router', async () => {
  const actual = await vi.importActual<typeof import('vue-router')>('vue-router');
  return {
    ...actual,
    useRoute: () => route,
    useRouter: () => ({ replace, push }),
  };
});

enableAutoUnmount(afterEach);

const buildResponse = (): ErrorAnalysisResponse => ({
  metadata: {
    historyStartsOn: '2026-04-12',
    lastCapturedAt: '2026-04-13T12:00:00Z',
    appliedFilters: {
      days: 7,
      school: null,
      sisPlatform: null,
      latestOnly: false,
    },
    hasCapturedData: true,
    filteredGroupCount: 3,
    resolvedSnapshotDate: null,
  },
  filterOptions: {
    schools: [
      { value: 'bar01', label: 'Baruch College', sisPlatform: 'Banner' },
      { value: 'foo01', label: 'Foo State', sisPlatform: 'PeopleSoftDirect' },
    ],
    sisPlatforms: ['Banner', 'PeopleSoftDirect'],
  },
  summary: {
    totalGroupedErrors: 3,
    totalErrorInstances: 7,
    distinctSignatures: 2,
    affectedSchools: 2,
    affectedSisPlatforms: 2,
    captureDays: 2,
    latestSnapshotDate: '2026-04-13',
  },
  trends: [
    { snapshotDate: '2026-04-12', totalErrors: 2, distinctSignatures: 1, affectedSchools: 1 },
    { snapshotDate: '2026-04-13', totalErrors: 5, distinctSignatures: 2, affectedSchools: 2 },
  ],
  signatures: [
    {
      signatureKey: 'sig-b',
      entityType: 'courses',
      errorCode: 'duplicate_course',
      signatureLabel: 'courses | duplicate_course | duplicate course <num>',
      normalizedMessage: 'duplicate course <num>',
      sampleMessage: 'Duplicate course 12345',
      totalCount: 4,
      affectedSchools: 1,
      affectedSisPlatforms: 1,
      firstSeen: '2026-04-13',
      lastSeen: '2026-04-13',
      recurrenceDays: 1,
      dominantSchool: 'foo01',
      dominantSisPlatform: 'PeopleSoftDirect',
      sampleErrors: [
        {
          entityDisplayName: 'Duplicate course 12345',
          termCode: '202505',
          mergeReport: { scheduleType: 'realtime' },
          errors: [
            {
              originalError: {
                body: {
                  errors: [
                    {
                      code: 'duplicate_course',
                      message: 'Duplicate course 12345 already exists in CourseDog',
                    },
                  ],
                },
              },
            },
          ],
        },
      ],
      termCodes: ['202505'],
      latestMergeReport: {
        school: 'bar01',
        mergeReportId: 'report-b2',
        scheduleType: 'nightly',
        entityDisplayName: 'Duplicate course 99999',
        snapshotDate: '2026-04-14',
      },
      dominantSchoolMergeReport: {
        school: 'foo01',
        mergeReportId: 'report-b1',
        scheduleType: 'realtime',
        entityDisplayName: 'Duplicate course 12345',
        snapshotDate: '2026-04-13',
      },
      impactedSchools: [
        { school: 'foo01', label: 'Foo State', count: 4 },
      ],
      exampleMergeReports: [
        {
          school: 'foo01',
          mergeReportId: 'report-b1',
          scheduleType: 'realtime',
          entityDisplayName: 'Duplicate course 12345',
          snapshotDate: '2026-04-13',
        },
      ],
      resolutionHint: {
        bucket: 'duplicate_conflict',
        title: 'Duplicate or conflicting record',
        action: 'Check for duplicate source records or conflicting identifiers and resolve the collision before rerunning the sync.',
        rationale: 'The signature points to a duplicate, uniqueness, or conflicting-write condition.',
        confidence: 0.79,
      },
    },
    {
      signatureKey: 'sig-a',
      entityType: 'sections',
      errorCode: 'missing_course',
      signatureLabel: 'sections | missing_course | course <num> missing dependency <num>',
      normalizedMessage: 'course <num> missing dependency <num>',
      sampleMessage: 'Course 202602 missing dependency 987654',
      totalCount: 3,
      affectedSchools: 1,
      affectedSisPlatforms: 1,
      firstSeen: '2026-04-12',
      lastSeen: '2026-04-13',
      recurrenceDays: 2,
      dominantSchool: 'bar01',
      dominantSisPlatform: 'Banner',
      sampleErrors: [
        {
          entityDisplayName: 'Section BIO-101-01',
          term: { code: '202602' },
          mergeReport: { scheduleType: 'nightly' },
          errors: [
            {
              originalError: {
                body: {
                  errors: [
                    {
                      code: 'missing_course',
                      message: 'Course 202602 missing dependency 987654',
                    },
                    {
                      detail: 'Dependent course must sync first.',
                    },
                  ],
                },
              },
            },
          ],
        },
      ],
      termCodes: ['202505', '202602'],
      latestMergeReport: {
        school: 'bar01',
        mergeReportId: 'report-a2',
        scheduleType: 'nightly',
        entityDisplayName: 'Course 202602 missing dependency 987654',
        snapshotDate: '2026-04-13',
      },
      dominantSchoolMergeReport: {
        school: 'bar01',
        mergeReportId: 'report-a2',
        scheduleType: 'nightly',
        entityDisplayName: 'Course 202602 missing dependency 987654',
        snapshotDate: '2026-04-13',
      },
      impactedSchools: [
        { school: 'bar01', label: 'Baruch College', count: 3 },
      ],
      exampleMergeReports: [
        {
          school: 'bar01',
          mergeReportId: 'report-a2',
          scheduleType: 'nightly',
          entityDisplayName: 'Course 202602 missing dependency 987654',
          snapshotDate: '2026-04-13',
        },
      ],
      resolutionHint: {
        bucket: 'missing_reference',
        title: 'Missing dependency or reference',
        action: 'Verify the referenced records exist in the SIS and are synced before retrying dependent entities.',
        rationale: 'The signature reads like a missing upstream dependency or lookup reference.',
        confidence: 0.82,
      },
    },
  ],
  schoolBreakdowns: [
    {
      key: 'foo01',
      label: 'Foo State',
      sisPlatform: 'PeopleSoftDirect',
      totalErrors: 4,
      distinctSignatures: 1,
      dominantSignature: 'courses | duplicate_course | duplicate course <num>',
      lastSeen: '2026-04-13',
      recurrenceDays: 1,
      likelyNextStep: 'Check for duplicate source records or conflicting identifiers and resolve the collision before rerunning the sync.',
      topResolutionTheme: 'duplicate_conflict',
      latestMergeReport: {
        school: 'foo01',
        mergeReportId: 'report-b1',
        scheduleType: 'realtime',
        entityDisplayName: 'Duplicate course 12345',
        snapshotDate: '2026-04-13',
      },
    },
    {
      key: 'bar01',
      label: 'Baruch College',
      sisPlatform: 'Banner',
      totalErrors: 3,
      distinctSignatures: 1,
      dominantSignature: 'sections | missing_course | course <num> missing dependency <num>',
      lastSeen: '2026-04-13',
      recurrenceDays: 2,
      likelyNextStep: 'Verify the referenced records exist in the SIS and are synced before retrying dependent entities.',
      topResolutionTheme: 'missing_reference',
      latestMergeReport: {
        school: 'bar01',
        mergeReportId: 'report-a2',
        scheduleType: 'nightly',
        entityDisplayName: 'Course 202602 missing dependency 987654',
        snapshotDate: '2026-04-13',
      },
    },
  ],
  sisBreakdowns: [
    {
      key: 'PeopleSoftDirect',
      label: 'PeopleSoftDirect',
      totalErrors: 4,
      distinctSignatures: 1,
      dominantSignature: 'courses | duplicate_course | duplicate course <num>',
      lastSeen: '2026-04-13',
      affectedSchools: 1,
      commonResolutionTheme: 'duplicate_conflict',
      associatedSignatures: [
        {
          signatureKey: 'sig-b',
          signatureLabel: 'courses | duplicate_course | duplicate course <num>',
          count: 4,
          entityType: 'courses',
          errorCode: 'duplicate_course',
          resolutionTitle: 'Duplicate or conflicting record',
          sampleMessage: 'Duplicate course 12345',
        },
      ],
    },
    {
      key: 'Banner',
      label: 'Banner',
      totalErrors: 3,
      distinctSignatures: 1,
      dominantSignature: 'sections | missing_course | course <num> missing dependency <num>',
      lastSeen: '2026-04-13',
      affectedSchools: 1,
      commonResolutionTheme: 'missing_reference',
      associatedSignatures: [
        {
          signatureKey: 'sig-a',
          signatureLabel: 'sections | missing_course | course <num> missing dependency <num>',
          count: 3,
          entityType: 'sections',
          errorCode: 'missing_course',
          resolutionTitle: 'Missing dependency or reference',
          sampleMessage: 'Course 202602 missing dependency 987654',
        },
      ],
    },
  ],
});

const buildDetailResponse = (): ErrorDetailTableResponse => ({
  rows: [
    {
      id: 1,
      snapshotDate: '2026-04-13',
      school: 'foo01',
      displayName: 'Foo State',
      sisPlatform: 'PeopleSoftDirect',
      entityType: 'courses',
      errorCode: 'duplicate_course',
      signatureKey: 'sig-b-detail',
      signatureLabel: 'courses | duplicate_course | duplicate course <num>',
      normalizedMessage: 'duplicate course <num>',
      fullErrorText: 'Duplicate course 12345 already exists in CourseDog',
      entityDisplayName: 'Course 12345',
      mergeReport: {
        school: 'foo01',
        mergeReportId: 'report-b1',
        scheduleType: 'realtime',
        entityDisplayName: 'Course 12345',
        snapshotDate: '2026-04-13',
      },
      termCodes: ['202505'],
      rawError: { message: 'Duplicate course 12345 already exists in CourseDog' },
      createdAt: '2026-04-13T12:00:00Z',
    },
    {
      id: 2,
      snapshotDate: '2026-04-13',
      school: 'bar01',
      displayName: 'Baruch College',
      sisPlatform: 'Banner',
      entityType: 'sections',
      errorCode: 'missing_course',
      signatureKey: 'sig-a-detail',
      signatureLabel: 'sections | missing_course | course <num> missing dependency <num>',
      normalizedMessage: 'course <num> missing dependency <num>',
      fullErrorText: 'Course 202602 missing dependency 987654',
      entityDisplayName: 'BIO-101-01',
      mergeReport: {
        school: 'bar01',
        mergeReportId: 'report-a2',
        scheduleType: 'nightly',
        entityDisplayName: 'BIO-101-01',
        snapshotDate: '2026-04-13',
      },
      termCodes: ['202602'],
      rawError: { message: 'Course 202602 missing dependency 987654' },
      createdAt: '2026-04-13T12:00:00Z',
    },
  ],
  total: 2,
  page: 1,
  pageSize: 50,
  sortBy: 'snapshotDate',
  sortDir: 'desc',
  metadata: {
    appliedFilters: {
      days: 7,
      school: null,
      sisPlatform: null,
      latestOnly: false,
      q: null,
    },
    resolvedSnapshotDate: null,
  },
});

const buildSignatureExplorerResponse = (): ErrorSignatureExplorerResponse => ({
  rows: [
    {
      key: 'Duplicate course 12345 already exists in CourseDog',
      id: 11,
      snapshotDate: '2026-04-13',
      school: 'foo01',
      displayName: 'Foo State',
      sisPlatform: 'PeopleSoftDirect',
      entityType: 'courses',
      errorCode: 'duplicate_course',
      signatureKey: 'sig-b',
      signatureLabel: 'courses | duplicate_course | duplicate course <num>',
      normalizedMessage: 'duplicate course <num>',
      fullErrorText: 'Duplicate course 12345 already exists in CourseDog',
      entityDisplayName: 'Course 12345',
      mergeReport: {
        school: 'foo01',
        mergeReportId: 'report-b1',
        scheduleType: 'realtime',
        entityDisplayName: 'Course 12345',
        snapshotDate: '2026-04-13',
      },
      termCodes: ['202505'],
      rawError: { message: 'Duplicate course 12345 already exists in CourseDog' },
      createdAt: '2026-04-13T12:00:00Z',
      instanceCount: 3,
      schools: [
        { school: 'bar01', label: 'Baruch College', count: 1 },
        { school: 'foo01', label: 'Foo State', count: 2 },
      ],
    },
  ],
  total: 1,
  page: 1,
  pageSize: 25,
  metadata: {
    appliedFilters: {
      days: 7,
      school: null,
      sisPlatform: null,
      latestOnly: true,
      signature: 'sig-b',
      groupBy: 'sis',
      bucket: 'PeopleSoftDirect',
    },
    resolvedSnapshotDate: '2026-04-13',
    signatureTotal: 3,
    bucketTotal: 3,
  },
  breakdowns: {
    sis: [
      { key: 'PeopleSoftDirect', label: 'PeopleSoftDirect', count: 3, share: 1 },
    ],
    school: [
      { key: 'foo01', label: 'Foo State', count: 2, share: 2 / 3 },
      { key: 'bar01', label: 'Baruch College', count: 1, share: 1 / 3 },
    ],
    term: [
      { key: '202505', label: '202505', count: 3, share: 1 },
    ],
  },
});

const buildPaginatedSignatures = (count: number) =>
  Array.from({ length: count }, (_, index) => {
    const baseSignature = buildResponse().signatures[index % buildResponse().signatures.length];
    const itemNumber = index + 1;
    const itemLabel = String(itemNumber).padStart(2, '0');

    return {
      ...baseSignature,
      signatureKey: `sig-${itemNumber}`,
      signatureLabel: `${baseSignature.entityType} | ${baseSignature.errorCode} | recurring pattern ${itemLabel}`,
      normalizedMessage: `recurring pattern ${itemLabel}`,
      sampleMessage: `Recurring pattern ${itemLabel}`,
      totalCount: count - index,
      recurrenceDays: (index % 3) + 1,
      termCodes: [`2025${String(itemNumber).padStart(2, '0')}`],
    };
  });

describe('ErrorAnalysisDashboard', () => {
  beforeEach(() => {
    route.query = {};
    replace.mockClear();
    push.mockClear();
    queryLoading.value = false;
    queryError.value = null;
    queryData.value = buildResponse();
    sisCountsQueryData.value = buildResponse();
    detailQueryData.value = buildDetailResponse();
    signatureExplorerQueryData.value = buildSignatureExplorerResponse();
    downloadErrorAnalysisExportMock.mockClear();
    downloadErrorAnalysisDetailedExportMock.mockClear();
    window.URL.createObjectURL = vi.fn(() => 'blob:download-url');
    window.URL.revokeObjectURL = vi.fn();
  });

  it('renders the empty state before any detailed capture exists', async () => {
    queryData.value = {
      ...buildResponse(),
      metadata: {
        historyStartsOn: null,
        lastCapturedAt: null,
        appliedFilters: { days: 7, school: null, sisPlatform: null, latestOnly: false },
        hasCapturedData: false,
        filteredGroupCount: 0,
        resolvedSnapshotDate: null,
      },
      summary: {
        totalGroupedErrors: 0,
        totalErrorInstances: 0,
        distinctSignatures: 0,
        affectedSchools: 0,
        affectedSisPlatforms: 0,
        captureDays: 0,
        latestSnapshotDate: null,
      },
      trends: [],
      signatures: [],
      schoolBreakdowns: [],
      sisBreakdowns: [],
    };
    detailQueryData.value = { ...buildDetailResponse(), rows: [], total: 0 };

    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    expect(wrapper.get('[data-testid="error-analysis-empty"]').text()).toContain('The next sync starts detailed error analysis.');
  });

  it('renders recurring signatures and supports toggling to the school view', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    expect(wrapper.text()).toContain('Top error signatures');
    expect(wrapper.get('[data-testid="error-analysis-signatures-tooltip"]').text()).toContain('What are signatures?');
    expect(wrapper.get('[data-testid="error-analysis-signatures-tooltip"]').text()).toContain('normalized patterns');
    expect(wrapper.get('[data-testid="error-analysis-signatures-tooltip"]').text()).toContain('captured merge-error rows');
    expect(wrapper.text()).toContain('Duplicate or conflicting record');
    expect(wrapper.text()).toContain('Explore signature');
    expect(wrapper.html()).toContain('/#/int/foo01/merge-history/report-b1');
    expect(wrapper.html()).not.toContain('/#/int/bar01/merge-history/report-b2');

    const bySchoolButton = findViewToggleButton(wrapper, 'By School');
    expect(bySchoolButton).toBeTruthy();
    await bySchoolButton!.trigger('click');

    expect(wrapper.text()).toContain('Where recurring errors concentrate');
    expect(wrapper.text()).toContain('Foo State');
    expect(wrapper.text()).toContain('Check for duplicate source records or conflicting identifiers');
    expect(wrapper.html()).toContain('/#/int/foo01/merge-history/report-b1');
  });

  it('paginates recurring signatures and resets the page when filters change', async () => {
    const paginatedSignatures = buildPaginatedSignatures(13);
    queryData.value = {
      ...buildResponse(),
      signatures: paginatedSignatures,
      summary: {
        ...buildResponse().summary,
        totalGroupedErrors: paginatedSignatures.length,
        distinctSignatures: paginatedSignatures.length,
        totalErrorInstances: paginatedSignatures.reduce((sum, signature) => sum + signature.totalCount, 0),
      },
      schoolBreakdowns: [],
      sisBreakdowns: [],
    };

    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    expect(wrapper.findAll('[data-testid="signature-row"]')).toHaveLength(12);
    expect(wrapper.text()).toContain('13 signature patterns total');
    expect(wrapper.text()).toContain('Page 1 of 2');
    expect(wrapper.text()).toContain('recurring pattern 01');
    expect(wrapper.text()).not.toContain('recurring pattern 13');

    await wrapper.findAll('button').find((button) => button.text().trim() === 'Next')!.trigger('click');

    expect(wrapper.findAll('[data-testid="signature-row"]')).toHaveLength(1);
    expect(wrapper.text()).toContain('Page 2 of 2');
    expect(wrapper.text()).toContain('recurring pattern 13');
    expect(wrapper.text()).not.toContain('recurring pattern 01');

    await wrapper.findAll('button').find((button) => button.text().trim() === '30d')!.trigger('click');

    expect(wrapper.text()).toContain('Page 1 of 2');
    expect(wrapper.findAll('[data-testid="signature-row"]')).toHaveLength(12);
    expect(wrapper.text()).toContain('recurring pattern 01');
    expect(wrapper.text()).not.toContain('recurring pattern 13');
  });

  it('restores the current view, page, and modal from the url query', async () => {
    const paginatedSignatures = buildPaginatedSignatures(13);
    queryData.value = {
      ...buildResponse(),
      signatures: paginatedSignatures,
      summary: {
        ...buildResponse().summary,
        totalGroupedErrors: paginatedSignatures.length,
        distinctSignatures: paginatedSignatures.length,
        totalErrorInstances: paginatedSignatures.reduce((sum, signature) => sum + signature.totalCount, 0),
      },
      schoolBreakdowns: [],
      sisBreakdowns: [],
    };
    route.query = {
      signaturePage: '2',
      modal: 'signature-explorer',
      signature: paginatedSignatures[12].signatureKey,
    };

    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    expect(wrapper.text()).toContain('Page 2 of 2');
    expect(wrapper.text()).toContain('recurring pattern 13');
    expect(wrapper.get('[data-testid="signature-explorer-modal"]').text()).toContain('Signature explorer');
  });

  it('writes the current page and open modal into the url query', async () => {
    const paginatedSignatures = buildPaginatedSignatures(13);
    queryData.value = {
      ...buildResponse(),
      signatures: paginatedSignatures,
      summary: {
        ...buildResponse().summary,
        totalGroupedErrors: paginatedSignatures.length,
        distinctSignatures: paginatedSignatures.length,
        totalErrorInstances: paginatedSignatures.reduce((sum, signature) => sum + signature.totalCount, 0),
      },
      schoolBreakdowns: [],
      sisBreakdowns: [],
    };

    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.findAll('button').find((button) => button.text().trim() === 'Next')!.trigger('click');
    await wrapper.get('[data-testid="signature-row"]').trigger('click');
    await nextTick();

    expect(replace).toHaveBeenCalled();
    expect(replace).toHaveBeenLastCalledWith({
      query: {
        signaturePage: '2',
        modal: 'signature-explorer',
        signature: paginatedSignatures[12].signatureKey,
      },
    });
  });

  it('opens the signature explorer and lets analysts inspect matching rows', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.get('[data-testid="signature-row"]').trigger('click');

    const modal = wrapper.get('[data-testid="signature-explorer-modal"]');
    expect(modal.text()).toContain('Signature explorer');
    expect(modal.text()).toContain('Check for duplicate source records or conflicting identifiers');
    expect(modal.text()).toContain('Uses the latest captured snapshot');
    expect(modal.text()).toContain('PeopleSoftDirect');
    expect(modal.text()).toContain('100%');
    expect(modal.text()).toContain('Showing 1 of 1 grouped error covering 3 rows');
    expect(modal.text()).toContain('Affected schools');
    expect(modal.text()).toContain('Count');
    expect(modal.text()).toContain('2 schools');
    expect(modal.text()).toContain('View school list');
    expect(modal.text()).toContain('Duplicate course 12345 already exists in CourseDog');
    expect(modal.text()).toContain('Representative sample');

    await modal.get('[data-testid="signature-explorer-row"]').trigger('click');

    const errorModal = wrapper.get('[data-testid="error-detail-modal"]');
    expect(errorModal.text()).toContain('Full upstream error');
    expect(errorModal.text()).toContain('Duplicate course 12345 already exists in CourseDog');
    expect(errorModal.text()).toContain('Foo State (foo01)');
    expect(errorModal.text()).toContain('PeopleSoftDirect');
    expect(errorModal.text()).toContain('Term 202505');
    expect(errorModal.text()).toContain('realtime');
    expect(errorModal.html()).toContain('/#/int/foo01/merge-history/report-b1');
  });

  it('opens the affected schools modal from a compact school summary', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.get('[data-testid="signature-row"]').trigger('click');
    await wrapper.get('[data-testid="signature-explorer-school-summary-trigger"]').trigger('click');

    const modal = wrapper.get('[data-testid="signature-explorer-schools-modal"]');
    expect(modal.text()).toContain('Affected schools');
    expect(modal.text()).toContain('2 schools');
    expect(modal.text()).toContain('Duplicate course 12345 already exists in CourseDog');
    expect(modal.text()).toContain('PeopleSoftDirect');
    expect(modal.text()).toContain('Term 202505');
    expect(modal.text()).toContain('3 open rows');
    expect(modal.text()).toContain('Foo State (foo01)');
    expect(modal.text()).toContain('2 matching rows');
    expect(modal.text()).toContain('Baruch College (CUNY) (bar01)');
    expect(modal.text()).toContain('1 matching row');
  });

  it('switches signature explorer tabs and resets to the first bucket', async () => {
    signatureExplorerQueryData.value = {
      ...buildSignatureExplorerResponse(),
      metadata: {
        ...buildSignatureExplorerResponse().metadata,
        appliedFilters: {
          ...buildSignatureExplorerResponse().metadata.appliedFilters,
          groupBy: 'school',
          bucket: 'foo01',
        },
      },
      breakdowns: {
        sis: [{ key: 'PeopleSoftDirect', label: 'PeopleSoftDirect', count: 1, share: 1 }],
        school: [{ key: 'foo01', label: 'Foo State', count: 1, share: 1 }],
        term: [{ key: '202505', label: '202505', count: 1, share: 1 }],
      },
    };

    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.get('[data-testid="signature-row"]').trigger('click');
    await wrapper.get('[data-testid="signature-explorer-tab-school"]').trigger('click');
    await nextTick();

    const modal = wrapper.get('[data-testid="signature-explorer-modal"]');
    expect(modal.text()).toContain('Foo State (foo01)');

    await wrapper.get('[data-testid="signature-explorer-tab-term"]').trigger('click');
    await nextTick();

    expect(modal.text()).toContain('202505');
  });

  it('opens a representative sample from the signature explorer', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.get('[data-testid="signature-row"]').trigger('click');
    await wrapper.get('[data-testid="signature-explorer-modal"]').findAll('button')
      .find((button) => button.text() === 'Representative sample')!
      .trigger('click');

    const modal = wrapper.get('[data-testid="error-detail-modal"]');
    expect(modal.text()).toContain('courses');
    expect(modal.text()).toContain('duplicate_course');
    expect(modal.text()).toContain('Foo State (foo01)');
    expect(modal.text()).toContain('PeopleSoftDirect');
    expect(modal.text()).toContain('Term 202505');
    expect(modal.text()).toContain('realtime');
    expect(modal.text()).toContain('Impacted schools');
    expect(modal.text()).toContain('Foo State (foo01) · 4');
    expect(modal.text()).toContain('Example merge reports');
    expect(modal.html()).toContain('/#/int/foo01/merge-history/report-b1');
    expect(modal.text()).toContain('Integration Hub');
  });

  it('opens a school-specific full error modal from the school view', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    const bySchoolButton = findViewToggleButton(wrapper, 'By School');
    expect(bySchoolButton).toBeTruthy();
    await bySchoolButton!.trigger('click');
    await wrapper.get('[data-testid="school-row"]').trigger('click');

    const modal = wrapper.get('[data-testid="error-detail-modal"]');
    expect(modal.text()).toContain('Foo State (foo01) sample error');
    expect(modal.text()).toContain('Duplicate course 12345 already exists in CourseDog');
    expect(modal.html()).toContain('/#/int/foo01/merge-history/report-b1');
  });

  it('narrows school options when the SIS filter changes', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.get('[data-testid="sis-filter"]').setValue('PeopleSoftDirect');

    const schoolOptions = wrapper.get('[data-testid="school-filter"]').findAll('option').map((option) => option.text());
    expect(schoolOptions).toEqual(['All schools', 'Foo State (foo01)']);
  });

  it('hides SIS platforms that have no associated schools', async () => {
    queryData.value = {
      ...buildResponse(),
      filterOptions: {
        ...buildResponse().filterOptions,
        sisPlatforms: ['Banner', 'ColumbiaCollegeChicago', 'PeopleSoftDirect'],
      },
      sisBreakdowns: [
        buildResponse().sisBreakdowns[0],
        buildResponse().sisBreakdowns[1],
      ],
    };
    sisCountsQueryData.value = queryData.value;

    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    const sisOptions = wrapper.get('[data-testid="sis-filter"]').findAll('option').map((option) => option.text());
    expect(sisOptions).toEqual([
      'All SIS Platforms',
      'PeopleSoftDirect (4)',
      'Banner (3)',
    ]);
  });

  it('downloads an export using the current filters', async () => {
    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});

    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.get('[data-testid="sis-filter"]').setValue('PeopleSoftDirect');
    await wrapper.get('[data-testid="school-filter"]').setValue('foo01');
    await wrapper.get('[data-testid="error-analysis-export"]').trigger('click');

    expect(downloadErrorAnalysisExportMock).toHaveBeenCalledWith({
      days: 7,
      school: 'foo01',
      sisPlatform: 'PeopleSoftDirect',
    });
    expect(window.URL.createObjectURL).toHaveBeenCalled();
    expect(clickSpy).toHaveBeenCalled();

    clickSpy.mockRestore();
  });

  it('renders and searches the all errors table', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    const allErrorsButton = findViewToggleButton(wrapper, 'All Errors');
    expect(allErrorsButton).toBeTruthy();
    await allErrorsButton!.trigger('click');

    expect(wrapper.text()).toContain('All captured merge errors');
    expect(wrapper.text()).toContain('Duplicate course 12345 already exists in CourseDog');
    expect(wrapper.findAll('[data-testid="detail-row"]')).toHaveLength(2);

    await wrapper.get('[data-testid="error-detail-search"]').setValue('duplicate');
    expect(wrapper.get('[data-testid="error-detail-search"]').element).toHaveProperty('value', 'duplicate');
  });

  it('opens a modal from the all errors table and exports detailed rows', async () => {
    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});

    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    const allErrorsButton = findViewToggleButton(wrapper, 'All Errors');
    expect(allErrorsButton).toBeTruthy();
    await allErrorsButton!.trigger('click');
    await wrapper.get('[data-testid="detail-row"]').trigger('click');

    const modal = wrapper.get('[data-testid="error-detail-modal"]');
    expect(modal.text()).toContain('Duplicate course 12345 already exists in CourseDog');
    expect(modal.html()).toContain('/#/int/foo01/merge-history/report-b1');

    await wrapper.get('[data-testid="error-analysis-export"]').trigger('click');
    expect(downloadErrorAnalysisDetailedExportMock).toHaveBeenCalledWith({
      days: 7,
      school: undefined,
      sisPlatform: undefined,
      q: undefined,
    });

    clickSpy.mockRestore();
  });

  it('opens associated signatures from the SIS view', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    const bySisButton = findViewToggleButton(wrapper, 'By SIS');
    expect(bySisButton).toBeTruthy();
    await bySisButton!.trigger('click');
    expect(wrapper.text()).toContain('Recurring patterns by SIS');

    await wrapper.get('[data-testid="sis-row"]').trigger('click');

    const modal = wrapper.get('[data-testid="sis-signatures-modal"]');
    expect(modal.text()).toContain('PeopleSoftDirect');
    expect(modal.text()).toContain('courses | duplicate_course | duplicate course <num>');

    await modal.findAll('button').find((button) => button.text() === 'Open signature')!.trigger('click');
    expect(wrapper.get('[data-testid="signature-explorer-modal"]').text()).toContain('Signature explorer');
  });

  it('sorts the SIS table when a header is clicked', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    const bySisButton = findViewToggleButton(wrapper, 'By SIS');
    expect(bySisButton).toBeTruthy();
    await bySisButton!.trigger('click');

    const before = wrapper.findAll('tbody tr td:first-child p.font-semibold').map((node) => node.text());
    expect(before[0]).toBe('PeopleSoftDirect');

    const headerButtons = wrapper.findAll('thead button');
    await headerButtons[0].trigger('click');

    const after = wrapper.findAll('tbody tr td:first-child p.font-semibold').map((node) => node.text());
    expect(after[0]).toBe('Banner');
  });

  it('keeps SIS option counts visible after selecting a SIS filter', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.get('[data-testid="sis-filter"]').setValue('PeopleSoftDirect');

    queryData.value = {
      ...buildResponse(),
      metadata: {
        ...buildResponse().metadata,
        appliedFilters: {
          days: 7,
          school: null,
          sisPlatform: 'PeopleSoftDirect',
          latestOnly: false,
        },
      },
      summary: {
        ...buildResponse().summary,
        totalGroupedErrors: 1,
        totalErrorInstances: 4,
        distinctSignatures: 1,
        affectedSchools: 1,
        affectedSisPlatforms: 1,
      },
      trends: [
        { snapshotDate: '2026-04-13', totalErrors: 4, distinctSignatures: 1, affectedSchools: 1 },
      ],
      signatures: [buildResponse().signatures[0]],
      schoolBreakdowns: [buildResponse().schoolBreakdowns[0]],
      sisBreakdowns: [buildResponse().sisBreakdowns[0]],
    };
    await nextTick();

    const sisOptions = wrapper.get('[data-testid="sis-filter"]').findAll('option').map((option) => option.text());
    expect(sisOptions).toEqual([
      'All SIS Platforms',
      'PeopleSoftDirect (4)',
      'Banner (3)',
    ]);
  });
});
