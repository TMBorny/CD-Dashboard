import { beforeEach, describe, expect, it, vi } from 'vitest';
import { flushPromises, mount } from '@vue/test-utils';
import { ref } from 'vue';

const push = vi.fn();
const invalidateQueries = vi.fn().mockResolvedValue(undefined);
const triggerHistoryBackfill = vi.fn();
const getSyncStatus = vi.fn();
const triggerSync = vi.fn();

vi.mock('vue-router', () => ({
  useRouter: () => ({ push }),
}));

vi.mock('@tanstack/vue-query', () => ({
  useQueryClient: () => ({ invalidateQueries }),
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
  getSyncStatus,
  triggerHistoryBackfill,
  triggerSync,
}));

describe('ClientHealthDashboard', () => {
  beforeEach(() => {
    push.mockReset();
    invalidateQueries.mockClear();
    triggerHistoryBackfill.mockReset();
    getSyncStatus.mockReset();
    triggerSync.mockReset();
  });

  it('defaults the bulk backfill date to 2026-01-01', async () => {
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

    const input = wrapper.get('#bulk-backfill-start-date');
    expect((input.element as HTMLInputElement).value).toBe('2026-01-01');
  });

  it('starts a bulk backfill using the absolute start date', async () => {
    triggerHistoryBackfill.mockResolvedValue({ jobId: 'bulk-job-1' });
    getSyncStatus.mockResolvedValue({
      status: 'completed',
      timing: { totalSec: 1.23 },
      schoolsProcessed: 12,
      totalSchools: 12,
      errors: [],
      scope: 'history-backfill-bulk',
    });

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

    const button = wrapper
      .findAll('button')
      .find((candidate) => candidate.text().includes('Backfill All Schools'));

    expect(button).toBeTruthy();
    await button!.trigger('click');
    await flushPromises();

    expect(triggerHistoryBackfill).toHaveBeenCalledWith({ startDate: '2026-01-01' });
  });
});
