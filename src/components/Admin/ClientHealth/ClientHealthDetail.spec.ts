import { ref } from 'vue';
import { describe, expect, it, vi } from 'vitest';
import { mount, RouterLinkStub } from '@vue/test-utils';

vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { school: 'bar01' } }),
}));

vi.mock('@tanstack/vue-query', () => ({
  useQuery: vi.fn(({ queryKey }) => {
    const resolvedQueryKey = Array.isArray(queryKey) ? queryKey : queryKey?.value ?? [];
    const key = resolvedQueryKey[0];

    if (key === 'clientHealthHistory') {
      return {
        data: ref({
          snapshots: [
            {
              snapshotDate: '2026-04-12',
              school: 'bar01',
              displayName: 'Baruch College',
              sisPlatform: 'Banner',
              products: [],
              merges: {
                nightly: { total: 2, succeeded: 1, failed: 0, finishedWithIssues: 0, noData: 0, halted: 1, mergeTimeMs: 0 },
                realtime: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
                manual: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
              },
              recentFailedMerges: [
                {
                  id: 'nightly-3',
                  scheduleType: 'nightly',
                  status: 'finishedWithIssues',
                  haltReason: 'Halted: Change Threshold Exceeded',
                },
              ],
              mergeErrorsCount: 0,
              activeUsers24h: 3,
              createdAt: new Date('2026-04-12T00:00:00Z'),
            },
          ],
        }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    if (key === 'clientHealthActiveUsers') {
      return {
        data: ref({ count: 3, users: ['a@example.com'] }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    if (key === 'clientHealthSyncMetadata') {
      return {
        data: ref({
          lastAttemptedSync: null,
          lastSuccessfulSync: null,
        }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    if (key === 'clientHealthCurrentErrorAnalysis') {
      return {
        data: ref({
          metadata: {
            historyStartsOn: '2026-04-12',
            lastCapturedAt: '2026-04-12T00:00:00Z',
            appliedFilters: {
              days: null,
              school: 'bar01',
              sisPlatform: null,
              latestOnly: true,
            },
            hasCapturedData: true,
            filteredGroupCount: 2,
            resolvedSnapshotDate: '2026-04-12',
          },
          filterOptions: {
            schools: [],
            sisPlatforms: [],
          },
          summary: {
            totalGroupedErrors: 2,
            totalErrorInstances: 3,
            distinctSignatures: 2,
            affectedSchools: 1,
            affectedSisPlatforms: 1,
            captureDays: 1,
            latestSnapshotDate: '2026-04-12',
          },
          trends: [],
          signatures: [
            {
              signatureKey: 'sig-a',
              entityType: 'sections',
              errorCode: 'missing_course',
              signatureLabel: 'sections | missing_course | course <num> missing dependency <num>',
              normalizedMessage: 'course <num> missing dependency <num>',
              sampleMessage: 'Course 202602 missing dependency 987654',
              totalCount: 2,
              affectedSchools: 1,
              affectedSisPlatforms: 1,
              firstSeen: '2026-04-12',
              lastSeen: '2026-04-12',
              recurrenceDays: 1,
              dominantSchool: 'bar01',
              dominantSisPlatform: 'Banner',
              sampleErrors: [],
              termCodes: ['202602'],
              resolutionHint: {
                bucket: 'missing_reference',
                title: 'Missing reference',
                action: 'Verify the related dependency exists before rerunning the merge.',
                rationale: 'Dependency is missing.',
                confidence: 0.9,
              },
              latestMergeReport: {
                school: 'bar01',
                mergeReportId: 'report-a1',
                scheduleType: 'nightly',
                snapshotDate: '2026-04-12',
              },
              dominantSchoolMergeReport: {
                school: 'bar01',
                mergeReportId: 'report-a1',
                scheduleType: 'nightly',
                snapshotDate: '2026-04-12',
              },
              impactedSchools: [],
              exampleMergeReports: [],
            },
            {
              signatureKey: 'sig-b',
              entityType: 'courses',
              errorCode: 'duplicate_course',
              signatureLabel: 'courses | duplicate_course | duplicate course <num>',
              normalizedMessage: 'duplicate course <num>',
              sampleMessage: 'Duplicate course 12345',
              totalCount: 1,
              affectedSchools: 1,
              affectedSisPlatforms: 1,
              firstSeen: '2026-04-12',
              lastSeen: '2026-04-12',
              recurrenceDays: 1,
              dominantSchool: 'bar01',
              dominantSisPlatform: 'Banner',
              sampleErrors: [],
              termCodes: ['202602'],
              resolutionHint: {
                bucket: 'duplicate_conflict',
                title: 'Duplicate conflict',
                action: 'Resolve the duplicate upstream record before retrying.',
                rationale: 'Duplicate detected.',
                confidence: 0.85,
              },
              latestMergeReport: null,
              dominantSchoolMergeReport: null,
              impactedSchools: [],
              exampleMergeReports: [],
            },
          ],
          schoolBreakdowns: [],
          sisBreakdowns: [],
        }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    if (key === 'clientHealthCurrentErrorRows') {
      return {
        data: ref({
          rows: [
            {
              id: 1,
              snapshotDate: '2026-04-12',
              school: 'bar01',
              displayName: 'Baruch College',
              sisPlatform: 'Banner',
              entityType: 'sections',
              errorCode: 'missing_course',
              signatureKey: 'sig-a',
              signatureLabel: 'sections | missing_course | course <num> missing dependency <num>',
              normalizedMessage: 'course <num> missing dependency <num>',
              fullErrorText: 'Course 202602 missing dependency 987654',
              entityDisplayName: 'BIO-101-01',
              mergeReport: {
                school: 'bar01',
                mergeReportId: 'report-a1',
                scheduleType: 'nightly',
                snapshotDate: '2026-04-12',
              },
              termCodes: ['202602'],
              rawError: { message: 'Course 202602 missing dependency 987654' },
              createdAt: '2026-04-12T00:00:00Z',
            },
          ],
          total: 1,
          page: 1,
          pageSize: 8,
          sortBy: 'signatureLabel',
          sortDir: 'asc',
          metadata: {
            appliedFilters: {
              days: null,
              school: 'bar01',
              sisPlatform: null,
              latestOnly: true,
              q: null,
            },
            resolvedSnapshotDate: '2026-04-12',
          },
        }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    return {
      data: ref(null),
      isLoading: ref(false),
      error: ref(null),
    };
  }),
}));

vi.mock('@/api', () => ({
  getClientHealthHistory: vi.fn().mockResolvedValue({ data: { snapshots: [] } }),
  getClientHealthActiveUsers: vi.fn().mockResolvedValue({ data: { count: 0, users: [] } }),
  getClientHealthSyncMetadata: vi.fn().mockResolvedValue({ data: { lastAttemptedSync: null, lastSuccessfulSync: null } }),
  getErrorAnalysis: vi.fn().mockResolvedValue({ data: {} }),
  getErrorAnalysisErrors: vi.fn().mockResolvedValue({ data: {} }),
}));

vi.mock('vue3-apexcharts', () => ({
  default: {
    props: ['series'],
    template: '<div class="chart-stub">{{ series.map((item) => item.name).join(",") }}</div>',
  },
}));

const CardStub = {
  template: '<section><slot /></section>',
};

describe('ClientHealthDetail', () => {
  it('removes sync and backfill controls from the detail view', async () => {
    const { default: ClientHealthDetail } = await import('./ClientHealthDetail.vue');
    const wrapper = mount(ClientHealthDetail, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          Card: CardStub,
        },
      },
    });

    expect(wrapper.text()).toContain('Times shown in');
    expect(wrapper.text()).not.toContain('Sync This School');
    expect(wrapper.text()).not.toContain('Backfill From');
    expect(wrapper.find('#backfill-start-date').exists()).toBe(false);
  });

  it('shows halted nightly merges in charts and recent merge details', async () => {
    const { default: ClientHealthDetail } = await import('./ClientHealthDetail.vue');
    const wrapper = mount(ClientHealthDetail, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          Card: CardStub,
        },
      },
    });

    expect(wrapper.findAll('.chart-stub').some((node) => node.text().includes('Halted'))).toBe(true);
    expect(wrapper.text()).toContain('Halted: Change Threshold Exceeded');
  });

  it('renders current error categories, signatures, and captured rows from the latest snapshot', async () => {
    const { default: ClientHealthDetail } = await import('./ClientHealthDetail.vue');
    const wrapper = mount(ClientHealthDetail, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          Card: CardStub,
        },
      },
    });

    expect(wrapper.text()).toContain('Current detailed error snapshot: 2026-04-12');
    expect(wrapper.text()).toContain('Missing reference');
    expect(wrapper.text()).toContain('Duplicate conflict');
    expect(wrapper.findAll('[data-testid="current-error-signature"]')).toHaveLength(2);
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(1);
    expect(wrapper.text()).toContain('Course 202602 missing dependency 987654');
  });
});
