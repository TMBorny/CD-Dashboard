import { computed, nextTick, ref } from 'vue';
import { describe, expect, it, vi } from 'vitest';
import { mount, RouterLinkStub } from '@vue/test-utils';

const paginatedCurrentErrorRows = Array.from({ length: 21 }, (_, index) => {
  const isMissingDependency = index < 11;

  return {
    id: index + 1,
    snapshotDate: '2026-04-12',
    school: 'bar01',
    displayName: 'Baruch College',
    sisPlatform: 'Banner',
    entityType: isMissingDependency ? 'sections' : 'courses',
    errorCode: isMissingDependency ? 'missing_course' : 'duplicate_course',
    signatureKey: isMissingDependency ? 'sig-a' : 'sig-b',
    signatureLabel: isMissingDependency
      ? `sections | missing_course | course <num> missing dependency <num> | ${index + 1}`
      : `courses | duplicate_course | duplicate course <num> | ${index + 1}`,
    normalizedMessage: isMissingDependency ? 'course <num> missing dependency <num>' : 'duplicate course <num>',
    fullErrorText: isMissingDependency
      ? `Course 202602 missing dependency ${987654 + index}`
      : `Duplicate course ${123450 + index} already exists in CourseDog`,
    entityDisplayName: isMissingDependency
      ? `BIO-101-${String(index + 1).padStart(2, '0')}`
      : `Course-${String(index + 1).padStart(2, '0')}`,
    mergeReport: {
      school: 'bar01',
      mergeReportId: `${isMissingDependency ? 'report-a' : 'report-b'}${index + 1}`,
      scheduleType: 'nightly',
      snapshotDate: '2026-04-12',
    },
    termCodes: ['202602'],
    rawError: {
      message: isMissingDependency
        ? `Course 202602 missing dependency ${987654 + index}`
        : `Duplicate course ${123450 + index} already exists in CourseDog`,
    },
    createdAt: '2026-04-12T00:00:00Z',
  };
});

vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { school: 'bar01' } }),
}));

const getResolvedQueryKey = (queryKey: unknown) => (Array.isArray(queryKey) ? queryKey : (queryKey as { value?: unknown[] } | undefined)?.value ?? []);

vi.mock('@tanstack/vue-query', () => ({
  useQuery: vi.fn(({ queryKey }) => {
    const resolvedQueryKey = getResolvedQueryKey(queryKey);
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
                  mergeReportId: 'nightly-3',
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
            totalErrorInstances: 21,
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
      const pageSize = 20;

      return {
        data: computed(() => {
          const activeQueryKey = getResolvedQueryKey(queryKey);
          const page = Number(activeQueryKey[2] ?? 1);
          const category = String(activeQueryKey[3] ?? '');
          const entityType = String(activeQueryKey[4] ?? '');
          const signature = String(activeQueryKey[5] ?? '');
          const start = (page - 1) * pageSize;
          const filteredRows = paginatedCurrentErrorRows.filter((row) => {
            const matchesCategory = !category
              || (category === 'missing_reference' && row.signatureKey === 'sig-a')
              || (category === 'duplicate_conflict' && row.signatureKey === 'sig-b');
            const matchesEntityType = !entityType || row.entityType === entityType;
            const matchesSignature = !signature || row.signatureKey === signature;
            return matchesCategory && matchesEntityType && matchesSignature;
          });

          return {
            rows: filteredRows.slice(start, start + pageSize),
            total: filteredRows.length,
            page,
            pageSize,
            sortBy: 'signatureLabel',
            sortDir: 'asc',
            metadata: {
              appliedFilters: {
                days: null,
                school: 'bar01',
                sisPlatform: null,
                latestOnly: true,
                category: category || null,
                entityType: entityType || null,
                signature: signature || null,
                q: null,
              },
              resolvedSnapshotDate: '2026-04-12',
            },
          };
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
    const recentFailuresTab = wrapper.get('[data-testid="current-error-tabs"]').findAll('button').find((node) => node.text() === 'Recent Failed Merges (1)');
    expect(recentFailuresTab).toBeTruthy();
    await recentFailuresTab!.trigger('click');
    expect(wrapper.find('[data-testid="current-error-panel-recent-failures"]').exists()).toBe(true);
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
    expect(wrapper.get('[data-testid="current-error-tabs"]').text()).toContain('Categories (2)');
    expect(wrapper.get('[data-testid="current-error-tabs"]').text()).toContain('Signatures (2)');
    expect(wrapper.get('[data-testid="current-error-tabs"]').text()).toContain('Errors (21)');
    expect(wrapper.get('[data-testid="current-error-tabs"]').text()).toContain('Recent Failed Merges (1)');
    expect(wrapper.find('[data-testid="current-error-panel-rows"]').exists()).toBe(true);
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(20);
    await wrapper.get('[data-testid="current-error-row"]').trigger('click');
    expect(wrapper.find('[data-testid="current-error-detail-modal"]').exists()).toBe(true);
    expect(wrapper.text()).toContain('Course 202602 missing dependency 987654');

    const categoriesTab = wrapper.get('[data-testid="current-error-tabs"]').findAll('button').find((node) => node.text() === 'Categories (2)');
    expect(categoriesTab).toBeTruthy();
    await categoriesTab!.trigger('click');
    expect(wrapper.find('[data-testid="current-error-panel-categories"]').exists()).toBe(true);
    expect(wrapper.text()).toContain('Missing reference');
    expect(wrapper.text()).toContain('Duplicate conflict');

    const capturedErrorsTab = wrapper.get('[data-testid="current-error-tabs"]').findAll('button').find((node) => node.text() === 'Errors (21)');
    expect(capturedErrorsTab).toBeTruthy();
    await capturedErrorsTab!.trigger('click');
    expect(wrapper.find('[data-testid="current-error-panel-rows"]').exists()).toBe(true);
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(20);
    expect(wrapper.get('[data-testid="current-error-pagination-top"]').text()).toContain('21 error rows total');
    expect(wrapper.get('[data-testid="current-error-page-label-top"]').text()).toBe('Page 1 of 2');
    expect(wrapper.get('[data-testid="current-error-page-label"]').text()).toBe('Page 1 of 2');
    await wrapper.get('[data-testid="current-error-row"]').trigger('click');
    expect(wrapper.find('[data-testid="current-error-detail-modal"]').exists()).toBe(true);
    expect(wrapper.text()).toContain('Course 202602 missing dependency 987654');
    await wrapper.get('[data-testid="current-error-page-next"]').trigger('click');
    await nextTick();
    expect(wrapper.get('[data-testid="current-error-page-label-top"]').text()).toBe('Page 2 of 2');
    expect(wrapper.get('[data-testid="current-error-page-label"]').text()).toBe('Page 2 of 2');
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(1);
    expect(wrapper.text()).toContain('Duplicate course 123470 already exists in CourseDog');

    const recentFailuresTab = wrapper.get('[data-testid="current-error-tabs"]').findAll('button').find((node) => node.text() === 'Recent Failed Merges (1)');
    expect(recentFailuresTab).toBeTruthy();
    await recentFailuresTab!.trigger('click');
    expect(wrapper.find('[data-testid="current-error-panel-recent-failures"]').exists()).toBe(true);
    expect(wrapper.findAll('[data-testid="recent-failed-merge-row"]')).toHaveLength(1);
    expect(wrapper.get('[data-testid="recent-failed-merge-id-link"]').text()).toBe('nightly-3');
    expect(wrapper.get('[data-testid="recent-failed-merge-id-link"]').attributes('href')).toContain('/#/int/bar01/merge-history/nightly-3');
  });

  it('filters captured errors by category, entity type, and signature', async () => {
    const { default: ClientHealthDetail } = await import('./ClientHealthDetail.vue');
    const wrapper = mount(ClientHealthDetail, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          Card: CardStub,
        },
      },
    });

    const capturedErrorsTab = wrapper.get('[data-testid="current-error-tabs"]').findAll('button').find((node) => node.text() === 'Errors (21)');
    expect(capturedErrorsTab).toBeTruthy();
    await capturedErrorsTab!.trigger('click');

    const categoryFilter = wrapper.get('[data-testid="current-error-category-filter"]');
    const entityTypeFilter = wrapper.get('[data-testid="current-error-entity-type-filter"]');
    const signatureFilter = wrapper.get('[data-testid="current-error-signature-filter"]');

    const signatureOptions = signatureFilter.findAll('option').map((option) => option.text());
    expect(signatureOptions).toContain('(2) sections | missing_course | course <num> missing dependency <num>');
    expect(signatureOptions).toContain('(1) courses | duplicate_course | duplicate course <num>');

    await categoryFilter.setValue('duplicate_conflict');
    await nextTick();
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(10);
    expect(wrapper.text()).toContain('Duplicate course 123461 already exists in CourseDog');
    expect(wrapper.get('[data-testid="current-error-pagination-top"]').text()).toContain('10 error rows total');
    expect(wrapper.get('[data-testid="current-error-page-label-top"]').text()).toBe('Page 1 of 1');
    expect(wrapper.get('[data-testid="current-error-page-label"]').text()).toBe('Page 1 of 1');

    await entityTypeFilter.setValue('sections');
    await nextTick();
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(0);
    expect(wrapper.text()).toContain('No captured errors match the selected category, entity type, and signature filters.');

    await categoryFilter.setValue('');
    await entityTypeFilter.setValue('sections');
    await nextTick();
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(11);
    expect(wrapper.get('[data-testid="current-error-pagination-top"]').text()).toContain('11 error rows total');

    await signatureFilter.setValue('sig-a');
    await nextTick();
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(11);
    expect(wrapper.text()).toContain('Course 202602 missing dependency 987654');
    expect(wrapper.get('[data-testid="current-error-page-label"]').text()).toBe('Page 1 of 1');

    await categoryFilter.setValue('missing_reference');
    await entityTypeFilter.setValue('courses');
    await nextTick();
    expect(wrapper.findAll('[data-testid="current-error-row"]')).toHaveLength(0);
    expect(wrapper.text()).toContain('No captured errors match the selected category, entity type, and signature filters.');
  });
});
