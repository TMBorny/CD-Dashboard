import { beforeEach, describe, expect, it, vi } from 'vitest';
import { mount, RouterLinkStub } from '@vue/test-utils';
import { ref } from 'vue';
import type { ErrorAnalysisResponse } from '@/types/errorAnalysis';

const queryData = ref<ErrorAnalysisResponse | null>(null);
const queryLoading = ref(false);
const queryError = ref<Error | null>(null);
const downloadErrorAnalysisExportMock = vi.fn().mockResolvedValue({
  blob: new Blob(['{"ok":true}'], { type: 'application/json' }),
  filename: 'error-analysis-export.json',
});

vi.mock('@tanstack/vue-query', () => ({
  useQuery: vi.fn(() => ({
    data: queryData,
    isLoading: queryLoading,
    error: queryError,
  })),
}));

vi.mock('@/api', () => ({
  getErrorAnalysis: vi.fn().mockResolvedValue({ data: {} }),
  downloadErrorAnalysisExport: downloadErrorAnalysisExportMock,
}));

vi.mock('vue3-apexcharts', () => ({
  default: {
    name: 'VueApexCharts',
    template: '<div class="chart-stub" />',
  },
}));

const buildResponse = (): ErrorAnalysisResponse => ({
  metadata: {
    historyStartsOn: '2026-04-12',
    lastCapturedAt: '2026-04-13T12:00:00Z',
    appliedFilters: {
      days: 7,
      school: null,
      sisPlatform: null,
    },
    hasCapturedData: true,
    filteredGroupCount: 3,
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
    },
  ],
});

describe('ErrorAnalysisDashboard', () => {
  beforeEach(() => {
    queryLoading.value = false;
    queryError.value = null;
    queryData.value = buildResponse();
    downloadErrorAnalysisExportMock.mockClear();
    window.URL.createObjectURL = vi.fn(() => 'blob:download-url');
    window.URL.revokeObjectURL = vi.fn();
  });

  it('renders the empty state before any detailed capture exists', async () => {
    queryData.value = {
      ...buildResponse(),
      metadata: {
        historyStartsOn: null,
        lastCapturedAt: null,
        appliedFilters: { days: 7, school: null, sisPlatform: null },
        hasCapturedData: false,
        filteredGroupCount: 0,
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
    expect(wrapper.text()).toContain('Duplicate or conflicting record');
    expect(wrapper.text()).toContain('View full error');
    expect(wrapper.html()).toContain('/#/int/foo01/merge-history/report-b1');
    expect(wrapper.html()).not.toContain('/#/int/bar01/merge-history/report-b2');

    await wrapper.get('[data-testid="view-toggle"]').findAll('button')[1].trigger('click');

    expect(wrapper.text()).toContain('Where recurring errors concentrate');
    expect(wrapper.text()).toContain('Foo State');
    expect(wrapper.text()).toContain('Check for duplicate source records or conflicting identifiers');
    expect(wrapper.html()).toContain('/#/int/foo01/merge-history/report-b1');
  });

  it('opens a full error modal with nested upstream text and links', async () => {
    const { default: ErrorAnalysisDashboard } = await import('./ErrorAnalysisDashboard.vue');
    const wrapper = mount(ErrorAnalysisDashboard, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          VueApexCharts: { template: '<div class="chart-stub" />' },
        },
      },
    });

    await wrapper.get('[data-testid="signature-row"] button').trigger('click');

    const modal = wrapper.get('[data-testid="error-detail-modal"]');
    expect(modal.text()).toContain('Full upstream error');
    expect(modal.text()).toContain('Duplicate course 12345 already exists in CourseDog');
    expect(modal.text()).toContain('courses');
    expect(modal.text()).toContain('duplicate_course');
    expect(modal.text()).toContain('Foo State (foo01)');
    expect(modal.text()).toContain('PeopleSoftDirect');
    expect(modal.text()).toContain('Term 202505');
    expect(modal.text()).toContain('realtime');
    expect(modal.html()).toContain('/#/int/foo01/merge-history/report-b1');
    expect(modal.text()).toContain('Integration Hub');

    await modal.get('button[aria-label="Close full error modal"]').trigger('click');
    expect(wrapper.find('[data-testid="error-detail-modal"]').exists()).toBe(false);
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

    await wrapper.get('[data-testid="view-toggle"]').findAll('button')[1].trigger('click');
    const schoolButtons = wrapper.findAll('button').filter((button) => button.text() === 'View full error');
    await schoolButtons[0].trigger('click');

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
});
