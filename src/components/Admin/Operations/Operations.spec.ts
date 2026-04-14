import { beforeEach, describe, expect, it, vi } from 'vitest';
import { flushPromises, mount } from '@vue/test-utils';
import { ref } from 'vue';

const invalidateQueries = vi.fn().mockResolvedValue(undefined);
const getSyncStatus = vi.fn();
const getSchools = vi.fn();
const getSchedulerSettings = vi.fn();
const updateSchedulerSettings = vi.fn();
const addSchoolExclusion = vi.fn();
const removeSchoolExclusion = vi.fn();
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
          excludedSchools: [
            { school: 'demo01', displayName: 'Demo School', products: [], reason: 'Matches excluded term: demo' },
          ],
          excludedTerms: ['demo', 'test', 'sandbox', 'baseline'],
        }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    if (queryKey[0] === 'schedulerSettings') {
      return {
        data: ref({
          syncEnabled: true,
          syncTime: '07:30',
        }),
        isLoading: ref(false),
        error: ref(null),
      };
    }

    return queryFn ? queryFn() : { data: ref(null), isLoading: ref(false), error: ref(null) };
  }),
}));

vi.mock('@/api', () => ({
  addSchoolExclusion,
  getSchedulerSettings,
  getSchools,
  getSyncStatus,
  removeSchoolExclusion,
  triggerHistoryBackfill,
  triggerSync,
  updateSchedulerSettings,
}));

describe('Operations', () => {
  beforeEach(() => {
    invalidateQueries.mockClear();
    getSchools.mockReset();
    getSyncStatus.mockReset();
    addSchoolExclusion.mockReset();
    removeSchoolExclusion.mockReset();
    getSchedulerSettings.mockReset();
    updateSchedulerSettings.mockReset();
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

  it('shows excluded schools and allows adding a manual exclusion', async () => {
    addSchoolExclusion.mockResolvedValue({ data: { school: 'bar01' } });

    const { default: Operations } = await import('./Operations.vue');
    const wrapper = mount(Operations);

    expect(wrapper.text()).toContain('Demo School');
    expect(wrapper.text()).toContain('Matches excluded term: demo');

    const excludeButton = wrapper
      .findAll('button')
      .find((candidate) => candidate.text().includes('Exclude'));

    expect(excludeButton).toBeTruthy();
    await excludeButton!.trigger('click');
    await flushPromises();

    expect(addSchoolExclusion).toHaveBeenCalledWith({ school: 'bar01' });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['schools'] });
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

    await wrapper.get('#operations-all-schools').setValue(false);
    await wrapper.get('#school-select-bar01').setValue(true);
    await wrapper.get('#school-select-bcc01').setValue(true);

    const syncButton = wrapper
      .findAll('button')
      .find((candidate) => candidate.text().includes('Run Sync'));

    expect(syncButton).toBeTruthy();
    await syncButton!.trigger('click');
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

    const backfillButton = wrapper
      .findAll('button')
      .find((candidate) => candidate.text().includes('Run Backfill'));

    expect(backfillButton).toBeTruthy();
    await backfillButton!.trigger('click');
    await flushPromises();

    expect(triggerHistoryBackfill).toHaveBeenCalledWith({
      startDate: '2026-01-01',
      endDate: '2026-01-07',
    });
  });

  it('saves daily sync scheduler settings', async () => {
    updateSchedulerSettings.mockResolvedValue({
      data: {
        syncEnabled: false,
        syncTime: '09:15',
      },
    });

    const { default: Operations } = await import('./Operations.vue');
    const wrapper = mount(Operations);

    const schedulerToggle = wrapper.get('#daily-sync-enabled');
    (schedulerToggle.element as HTMLInputElement).checked = false;
    await schedulerToggle.trigger('change');
    await wrapper.get('#daily-sync-time').setValue('09:15');

    const saveButton = wrapper
      .findAll('button')
      .find((candidate) => candidate.text().includes('Save'));

    expect(saveButton).toBeTruthy();
    await saveButton!.trigger('click');
    await flushPromises();

    expect(updateSchedulerSettings).toHaveBeenCalledWith({
      syncEnabled: false,
      syncTime: '09:15',
    });
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['schedulerSettings'] });
    expect(wrapper.text()).toContain('Daily sync disabled.');
  });
});
