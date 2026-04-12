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
        data: ref({ snapshotDate: '2026-04-12', schools: [] }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    if (key === 'clientHealthHistory') {
      return {
        data: ref([]),
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
          VueApexCharts: true,
        },
      },
    });

    expect(wrapper.text()).not.toContain('Sync Now');
    expect(wrapper.text()).not.toContain('Backfill All Schools');
    expect(wrapper.find('#bulk-backfill-start-date').exists()).toBe(false);
  });
});
