import { beforeEach, describe, expect, it, vi } from 'vitest';
import { flushPromises, mount } from '@vue/test-utils';
import { ref, watchEffect } from 'vue';

const invalidateQueries = vi.fn().mockResolvedValue(undefined);
const getSyncRuns = vi.fn();
const resumeHistoryBackfill = vi.fn();
const retryHistoryBackfillFailures = vi.fn();
const mockUseQuery = vi.fn();

vi.mock('@tanstack/vue-query', () => ({
  keepPreviousData: Symbol('keepPreviousData'),
  useQueryClient: () => ({ invalidateQueries }),
  useQuery: mockUseQuery.mockImplementation(({ queryFn }) => {
    const data = ref<unknown>(null);
    const error = ref<unknown>(null);

    watchEffect(() => {
      error.value = null;
      void Promise.resolve(queryFn())
        .then((value) => {
          data.value = value;
        })
        .catch((err) => {
          error.value = err;
        });
    });

    return {
      data,
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
  getSyncRuns,
  resumeHistoryBackfill,
  retryHistoryBackfillFailures,
}));

const makeRun = (jobId: string, attemptedAt: string) => ({
  jobId,
  scope: 'history-backfill-bulk',
  status: 'completed',
  attemptedAt,
  completedUnits: 1,
  failedUnits: 0,
  skippedUnits: 0,
});

describe('JobsMonitoring', () => {
  beforeEach(() => {
    invalidateQueries.mockClear();
    getSyncRuns.mockReset();
    resumeHistoryBackfill.mockReset();
    retryHistoryBackfillFailures.mockReset();
    mockUseQuery.mockClear();
  });

  it('shows paged job history and fetches the next page', async () => {
    const firstPageRuns = Array.from({ length: 25 }, (_, index) =>
      makeRun(`job-${30 - index}`, `2026-04-12T${String(index).padStart(2, '0')}:00:00Z`),
    );
    const secondPageRuns = Array.from({ length: 5 }, (_, index) =>
      makeRun(`job-${5 - index}`, `2026-04-11T${String(index).padStart(2, '0')}:00:00Z`),
    );

    getSyncRuns.mockImplementation(({ limit, offset }: { limit: number; offset: number }) =>
      Promise.resolve({
        data: {
          syncRuns: offset === 0 ? firstPageRuns : secondPageRuns,
          totalCount: 30,
          limit,
          offset,
        },
      }),
    );

    const { default: JobsMonitoring } = await import('./JobsMonitoring.vue');
    const wrapper = mount(JobsMonitoring, {
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

    expect(wrapper.text()).toContain('Showing 1-25 of 30 jobs.');
    expect(wrapper.text()).toContain('Page 1 of 2');
    expect(wrapper.text()).toContain('Times shown in');
    expect(getSyncRuns).toHaveBeenCalledWith({ limit: 25, offset: 0 });

    const nextButton = wrapper
      .findAll('button')
      .find((candidate) => candidate.text() === 'Next');

    expect(nextButton).toBeTruthy();
    await nextButton!.trigger('click');
    await flushPromises();

    expect(getSyncRuns).toHaveBeenCalledWith({ limit: 25, offset: 25 });
    expect(wrapper.text()).toContain('Showing 26-30 of 30 jobs.');
    expect(wrapper.text()).toContain('Page 2 of 2');
    expect(wrapper.text()).toContain('job-5...');
  });

  it('keeps showing loaded job history when a background refetch errors', async () => {
    mockUseQuery.mockImplementationOnce(() => ({
      data: ref({
        syncRuns: [makeRun('job-123', '2026-04-12T12:00:00Z')],
        totalCount: 1,
        limit: 25,
        offset: 0,
      }),
      isLoading: ref(false),
      error: ref(new Error('network down')),
    }));

    const { default: JobsMonitoring } = await import('./JobsMonitoring.vue');
    const wrapper = mount(JobsMonitoring, {
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
    expect(wrapper.text()).toContain('Showing 1-1 of 1 job.');
    expect(wrapper.text()).toContain('job-123...');
    expect(wrapper.text()).toContain('Times shown in');
    expect(wrapper.text()).not.toContain('Failed to load job history.');
  });
});
