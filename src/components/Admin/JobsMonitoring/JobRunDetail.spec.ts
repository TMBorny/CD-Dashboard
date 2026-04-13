import { beforeEach, describe, expect, it, vi } from 'vitest';
import { flushPromises, mount } from '@vue/test-utils';
import { ref } from 'vue';

const invalidateQueries = vi.fn().mockResolvedValue(undefined);
const getSyncRun = vi.fn();
const resumeHistoryBackfill = vi.fn();
const retryHistoryBackfillFailures = vi.fn();
const mockUseQuery = vi.fn();

vi.mock('@tanstack/vue-query', () => ({
  useQueryClient: () => ({ invalidateQueries }),
  useQuery: mockUseQuery.mockImplementation(({ queryFn }) => {
    const state = ref<{ syncRun: unknown } | null>(null);
    const error = ref<unknown>(null);
    queryFn()
      .then((value: { syncRun: unknown }) => {
        state.value = value;
      })
      .catch((err: unknown) => {
        error.value = err;
      });
    return {
      data: state,
      isLoading: ref(false),
      error,
    };
  }),
  useMutation: vi.fn(({ mutationFn, onSuccess }) => ({
    isPending: false,
    variables: undefined,
    error: null,
    mutateAsync: async (value: string) => {
      const result = await mutationFn(value);
      await onSuccess?.(result, value);
      return result;
    },
  })),
}));

vi.mock('@/api', () => ({
  getSyncRun,
  resumeHistoryBackfill,
  retryHistoryBackfillFailures,
}));

describe('JobRunDetail', () => {
  beforeEach(() => {
    invalidateQueries.mockClear();
    getSyncRun.mockReset();
    resumeHistoryBackfill.mockReset();
    retryHistoryBackfillFailures.mockReset();
    mockUseQuery.mockClear();
  });

  it('renders a single run with diagnostics and recovery actions', async () => {
    getSyncRun.mockResolvedValue({
      data: {
        syncRun: {
          jobId: 'job-123',
          scope: 'history-backfill-bulk',
          status: 'completed_with_failures',
          attemptedAt: '2026-04-12T12:00:00Z',
          startedAt: '2026-04-12T12:00:05Z',
          finishedAt: '2026-04-12T12:05:00Z',
          startDate: '2026-01-01',
          endDate: '2026-01-03',
          dateCount: 3,
          schoolsProcessed: 12,
          totalSchools: 14,
          completedUnits: 10,
          failedUnits: 2,
          skippedUnits: 2,
          currentSchool: 'bar01',
          currentSnapshotDate: '2026-01-03',
          lastHeartbeatAt: '2026-04-12T12:04:30Z',
          lastProgressAt: '2026-04-12T12:04:00Z',
          statusDetail: 'completed_with_failures',
          failureReason: '2 work units failed after retries',
          errorMessage: 'sample failure',
          errors: ['bar01 2026-01-02 failed', 'bcc01 2026-01-03 failed'],
          failedUnitsSample: [
            { school: 'bar01', snapshotDate: '2026-01-02', attemptCount: 2, error: 'timeout' },
          ],
          checkpointState: { remainingUnits: 0 },
          timing: { totalSec: 295.0 },
        },
      },
    });
    retryHistoryBackfillFailures.mockResolvedValue({ ok: true });

    const { default: JobRunDetail } = await import('./JobRunDetail.vue');
    const wrapper = mount(JobRunDetail, {
      props: { jobId: 'job-123' },
      global: {
        stubs: {
          RouterLink: {
            props: ['to'],
            template: '<a :href="typeof to === \'string\' ? to : to.path"><slot /></a>',
          },
        },
      },
    });

    await flushPromises();

    expect(wrapper.text()).toContain('Job details');
    expect(wrapper.text()).toContain('history backfill bulk');
    expect(wrapper.text()).toContain('completed_with_failures');
    expect(wrapper.text()).toContain('2 work units failed after retries');
    expect(wrapper.text()).toContain('bar01 on 2026-01-02');
    expect(wrapper.text()).toContain('Percent complete');
    expect(wrapper.text()).toContain('Remaining units');

    const retryButton = wrapper
      .findAll('button')
      .find((candidate) => candidate.text().includes('Retry failed units'));

    expect(retryButton).toBeTruthy();
    await retryButton!.trigger('click');
    await flushPromises();

    expect(retryHistoryBackfillFailures).toHaveBeenCalledWith('job-123');
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['syncRuns'] });
  });

  it('shows a not-found message when the requested run returns 404', async () => {
    getSyncRun.mockRejectedValue({ response: { status: 404 } });

    const { default: JobRunDetail } = await import('./JobRunDetail.vue');
    const wrapper = mount(JobRunDetail, {
      props: { jobId: 'missing-job' },
      global: {
        stubs: {
          RouterLink: {
            props: ['to'],
            template: '<a :href="typeof to === \'string\' ? to : to.path"><slot /></a>',
          },
        },
      },
    });

    await flushPromises();

    expect(wrapper.text()).toContain('This job could not be found.');
  });

  it('shows an error message when the request fails for another reason', async () => {
    getSyncRun.mockRejectedValue(new Error('network down'));

    const { default: JobRunDetail } = await import('./JobRunDetail.vue');
    const wrapper = mount(JobRunDetail, {
      props: { jobId: 'missing-job' },
      global: {
        stubs: {
          RouterLink: {
            props: ['to'],
            template: '<a :href="typeof to === \'string\' ? to : to.path"><slot /></a>',
          },
        },
      },
    });

    await flushPromises();

    expect(wrapper.text()).toContain('Failed to load job details.');
  });

  it('keeps showing loaded job details when a background refetch errors', async () => {
    mockUseQuery.mockImplementationOnce(() => ({
      data: ref({
        syncRun: {
          jobId: 'job-123',
          scope: 'history-backfill-bulk',
          status: 'running',
        },
      }),
      isLoading: ref(false),
      error: ref(new Error('network down')),
    }));

    const { default: JobRunDetail } = await import('./JobRunDetail.vue');
    const wrapper = mount(JobRunDetail, {
      props: { jobId: 'job-123' },
      global: {
        stubs: {
          RouterLink: {
            props: ['to'],
            template: '<a :href="typeof to === \'string\' ? to : to.path"><slot /></a>',
          },
        },
      },
    });

    await flushPromises();

    expect(wrapper.text()).toContain('Live updates are temporarily unavailable.');
    expect(wrapper.text()).toContain('history backfill bulk');
    expect(wrapper.text()).not.toContain('Failed to load job details.');
  });
});
