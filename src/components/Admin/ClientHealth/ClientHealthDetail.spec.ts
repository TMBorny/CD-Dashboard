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
}));

describe('ClientHealthDetail', () => {
  it('removes sync and backfill controls from the detail view', async () => {
    const { default: ClientHealthDetail } = await import('./ClientHealthDetail.vue');
    const wrapper = mount(ClientHealthDetail, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          Card: true,
          VueApexCharts: true,
        },
      },
    });

    expect(wrapper.text()).toContain('Times shown in');
    expect(wrapper.text()).not.toContain('Sync This School');
    expect(wrapper.text()).not.toContain('Backfill From');
    expect(wrapper.find('#backfill-start-date').exists()).toBe(false);
  });
});
