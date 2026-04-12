import { beforeEach, describe, expect, it, vi } from 'vitest';
import { flushPromises, mount } from '@vue/test-utils';
import { ref } from 'vue';

const invalidateQueries = vi.fn().mockResolvedValue(undefined);
const triggerHistoryBackfill = vi.fn();
const getSyncStatus = vi.fn();
const triggerSync = vi.fn();

vi.mock('vue-router', () => ({
  useRoute: () => ({ params: { school: 'bar01' } }),
}));

vi.mock('@tanstack/vue-query', () => ({
  useQueryClient: () => ({ invalidateQueries }),
  useQuery: vi.fn(({ queryKey }) => {
    const key = queryKey[0];

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
                nightly: { total: 1, succeeded: 1, failed: 0, finishedWithIssues: 0, noData: 0, mergeTimeMs: 0 },
                realtime: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
                manual: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
              },
              recentFailedMerges: [],
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
  getSyncStatus,
  triggerHistoryBackfill,
  triggerSync,
}));

describe('ClientHealthDetail', () => {
  beforeEach(() => {
    invalidateQueries.mockClear();
    triggerHistoryBackfill.mockReset();
    getSyncStatus.mockReset();
    triggerSync.mockReset();
  });

  it('starts a school backfill with the default 2026-01-01 start date', async () => {
    triggerHistoryBackfill.mockResolvedValue({ jobId: 'school-job-1' });
    getSyncStatus.mockResolvedValue({
      status: 'completed',
      timing: { totalSec: 0.75 },
      schoolsProcessed: 1,
      totalSchools: 1,
      errors: [],
      scope: 'history-backfill-single',
    });

    const { default: ClientHealthDetail } = await import('./ClientHealthDetail.vue');
    const wrapper = mount(ClientHealthDetail, {
      global: {
        stubs: {
          Card: true,
          VueApexCharts: true,
        },
      },
    });

    const input = wrapper.get('#backfill-start-date');
    expect((input.element as HTMLInputElement).value).toBe('2026-01-01');

    const button = wrapper
      .findAll('button')
      .find((candidate) => candidate.text().includes('Backfill From'));

    expect(button).toBeTruthy();
    await button!.trigger('click');
    await flushPromises();

    expect(triggerHistoryBackfill).toHaveBeenCalledWith({
      school: 'bar01',
      startDate: '2026-01-01',
    });
  });
});
