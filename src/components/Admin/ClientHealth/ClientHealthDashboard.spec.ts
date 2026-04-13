import { beforeEach, describe, expect, it, vi } from 'vitest';
import { mount } from '@vue/test-utils';
import { ref } from 'vue';

const push = vi.fn();

vi.mock('vue-router', () => ({
  useRouter: () => ({ push }),
}));

vi.mock('@tanstack/vue-query', () => ({
  useQuery: vi.fn(({ queryKey }) => {
    const key = queryKey[0];

    if (key === 'clientHealth') {
      return {
        data: ref({
          snapshotDate: '2026-04-12',
          schools: [
            {
              snapshotDate: '2026-04-12',
              school: 'bar01',
              displayName: 'Baruch College',
              sisPlatform: 'Banner',
              products: [],
              merges: {
                nightly: { total: 3, succeeded: 1, failed: 0, finishedWithIssues: 0, noData: 1, halted: 1, mergeTimeMs: 0 },
                realtime: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
                manual: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
              },
              recentFailedMerges: [{ id: 'nightly-3', haltReason: 'Halted: Change Threshold Exceeded' }],
              mergeErrorsCount: 0,
              activeUsers24h: 0,
              createdAt: new Date('2026-04-12T00:00:00Z'),
            },
          ],
        }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    if (key === 'clientHealthHistory') {
      return {
        data: ref([
          {
            snapshotDate: '2026-04-12',
            school: 'bar01',
            displayName: 'Baruch College',
            sisPlatform: 'Banner',
            products: [],
            merges: {
              nightly: { total: 3, succeeded: 1, failed: 0, finishedWithIssues: 0, noData: 1, halted: 1, mergeTimeMs: 0 },
              realtime: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
              manual: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
            },
            recentFailedMerges: [{ id: 'nightly-3', haltReason: 'Halted: Change Threshold Exceeded' }],
            mergeErrorsCount: 0,
            activeUsers24h: 0,
            createdAt: new Date('2026-04-12T00:00:00Z'),
          },
        ]),
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
  getClientHealth: vi.fn().mockResolvedValue({ data: { snapshotDate: '2026-04-12', schools: [] } }),
  getClientHealthHistory: vi.fn().mockResolvedValue({ data: { snapshots: [] } }),
}));

vi.mock('vue3-apexcharts', () => ({
  default: {
    props: ['series'],
    template: '<div class="chart-stub">{{ series.map((item) => item.name).join(",") }}</div>',
  },
}));

describe('ClientHealthDashboard', () => {
  beforeEach(() => {
    push.mockReset();
  });

  it('removes sync and backfill controls from the dashboard header', async () => {
    const { default: ClientHealthDashboard } = await import('./ClientHealthDashboard.vue');
    const wrapper = mount(ClientHealthDashboard, {
      global: {
        stubs: {
          ClientHealthSummaryCards: true,
          ClientHealthTable: true,
        },
      },
    });

    expect(wrapper.text()).not.toContain('Sync Now');
    expect(wrapper.text()).not.toContain('Backfill All Schools');
    expect(wrapper.find('#bulk-backfill-start-date').exists()).toBe(false);
  });

  it('includes a halted nightly outcome series and updated copy', async () => {
    const { default: ClientHealthDashboard } = await import('./ClientHealthDashboard.vue');
    const wrapper = mount(ClientHealthDashboard, {
      global: {
        stubs: {
          ClientHealthSummaryCards: true,
          ClientHealthTable: true,
        },
      },
    });

    expect(wrapper.text()).toContain('were halted by change threshold');
    expect(wrapper.findAll('.chart-stub').some((node) => node.text().includes('Halted'))).toBe(true);
  });
});
