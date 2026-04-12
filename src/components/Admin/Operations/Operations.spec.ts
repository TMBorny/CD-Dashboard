import { beforeEach, describe, expect, it, vi } from 'vitest';
import { flushPromises, mount } from '@vue/test-utils';
import { ref } from 'vue';

const invalidateQueries = vi.fn().mockResolvedValue(undefined);
const getSyncStatus = vi.fn();
const getSchools = vi.fn();
const triggerHistoryBackfill = vi.fn();
const triggerSync = vi.fn();

vi.mock('@tanstack/vue-query', () => ({
  useQueryClient: () => ({ invalidateQueries }),
  useQuery: vi.fn(({ queryKey, queryFn }) => {
    if (queryKey[0] === 'schools') {
      return {
        data: ref({
          schools: [
            { school: 'bar01', displayName: 'Baruch College', products: [] },
            { school: 'bcc01', displayName: 'Bronx Community College', products: [] },
          ],
        }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    return queryFn ? queryFn() : { data: ref(null), isLoading: ref(false), error: ref(null) };
  }),
}));

vi.mock('@/api', () => ({
  getSchools,
  getSyncStatus,
  triggerHistoryBackfill,
  triggerSync,
}));

describe('Operations', () => {
  beforeEach(() => {
    invalidateQueries.mockClear();
    getSchools.mockReset();
    getSyncStatus.mockReset();
    triggerHistoryBackfill.mockReset();
    triggerSync.mockReset();
  });

  it('shows school options and validates date ranges before backfill', async () => {
    const { default: Operations } = await import('./Operations.vue');
    const wrapper = mount(Operations);

    expect(wrapper.text()).toContain('Baruch College');
    expect(wrapper.text()).toContain('Bronx Community College');

    await wrapper.get('#operations-start-date').setValue('2026-02-10');
    await wrapper.get('#operations-end-date').setValue('2026-02-01');
    const backfillButton = wrapper
      .findAll('button')
      .find((candidate) => candidate.text().includes('Run Backfill'));

    expect(backfillButton).toBeTruthy();
    await backfillButton!.trigger('click');

    expect(triggerHistoryBackfill).not.toHaveBeenCalled();
    expect(wrapper.text()).toContain('End date must be on or after the start date.');
  });

  it('filters the school list from the type search', async () => {
    const { default: Operations } = await import('./Operations.vue');
    const wrapper = mount(Operations);

    await wrapper.get('#school-search').setValue('bronx');

    expect(wrapper.text()).toContain('Bronx Community College');
    expect(wrapper.text()).not.toContain('Baruch College');

    await wrapper.get('#school-search').setValue('missing-school');

    expect(wrapper.text()).toContain('No schools match that search.');
  });

  it('runs multi-school sync sequentially and invalidates dependent queries', async () => {
    triggerSync
      .mockResolvedValueOnce({ jobId: 'job-1' })
      .mockResolvedValueOnce({ jobId: 'job-2' });

    getSyncStatus
      .mockResolvedValueOnce({
        status: 'completed',
        timing: { totalSec: 1.25 },
        startedAt: '2026-04-12T12:00:00Z',
        finishedAt: '2026-04-12T12:00:01Z',
        schoolsProcessed: 1,
        totalSchools: 1,
        errors: [],
      })
      .mockResolvedValueOnce({
        status: 'completed',
        timing: { totalSec: 1.5 },
        startedAt: '2026-04-12T12:01:00Z',
        finishedAt: '2026-04-12T12:01:02Z',
        schoolsProcessed: 1,
        totalSchools: 1,
        errors: [],
      });

    const { default: Operations } = await import('./Operations.vue');
    const wrapper = mount(Operations);

    await wrapper.get('input[type="checkbox"]').setValue(false);
    const schoolBoxes = wrapper.findAll('input[type="checkbox"]');
    await schoolBoxes[1].setValue(true);
    await schoolBoxes[2].setValue(true);

    const buttons = wrapper.findAll('button');
    await buttons[0].trigger('click');
    await flushPromises();

    expect(triggerSync).toHaveBeenNthCalledWith(1, { school: 'bar01' });
    expect(triggerSync).toHaveBeenNthCalledWith(2, { school: 'bcc01' });
    expect(getSyncStatus).toHaveBeenCalledTimes(2);
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['clientHealth'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['clientHealthHistory'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['clientHealthSyncMetadata'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['clientHealthActiveUsers'] });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['syncRuns'] });
    expect(wrapper.text()).toContain('Queue progress: 2 / 2');
    expect(wrapper.text()).toContain('Total time: 1.25s');
  });

  it('runs all-school backfill with start and end dates', async () => {
    triggerHistoryBackfill.mockResolvedValue({ jobId: 'bulk-backfill-1' });
    getSyncStatus.mockResolvedValue({
      status: 'completed',
      timing: { totalSec: 2.1 },
      schoolsProcessed: 12,
      totalSchools: 12,
      errors: [],
    });

    const { default: Operations } = await import('./Operations.vue');
    const wrapper = mount(Operations);

    await wrapper.get('#operations-start-date').setValue('2026-01-01');
    await wrapper.get('#operations-end-date').setValue('2026-01-07');

    const buttons = wrapper.findAll('button');
    await buttons[1].trigger('click');
    await flushPromises();

    expect(triggerHistoryBackfill).toHaveBeenCalledWith({
      startDate: '2026-01-01',
      endDate: '2026-01-07',
    });
  });
});
