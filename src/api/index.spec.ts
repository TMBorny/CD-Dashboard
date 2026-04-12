import { beforeEach, describe, expect, it, vi } from 'vitest';

const post = vi.fn();
const get = vi.fn();

vi.mock('axios', () => ({
  default: {
    create: vi.fn(() => ({ post, get })),
  },
}));

describe('client health api helpers', () => {
  beforeEach(() => {
    post.mockReset();
    get.mockReset();
  });

  it('fetches the available schools list', async () => {
    get.mockResolvedValue({ data: { schools: [] } });
    const { getSchools } = await import('./index');

    await getSchools();

    expect(get).toHaveBeenCalledWith('/schools');
  });

  it('sends bulk history backfill with an absolute start date', async () => {
    post.mockResolvedValue({ data: { jobId: 'bulk-1' } });
    const { triggerHistoryBackfill } = await import('./index');

    await triggerHistoryBackfill({ startDate: '2026-01-01' });

    expect(post).toHaveBeenCalledWith('/client-health/history/backfill', null, {
      params: {
        startDate: '2026-01-01',
      },
    });
  });

  it('sends single-school history backfill with school and date range', async () => {
    post.mockResolvedValue({ data: { jobId: 'school-1' } });
    const { triggerHistoryBackfill } = await import('./index');

    await triggerHistoryBackfill({
      school: 'bar01',
      startDate: '2026-01-01',
      endDate: '2026-01-07',
    });

    expect(post).toHaveBeenCalledWith('/client-health/history/backfill', null, {
      params: {
        school: 'bar01',
        startDate: '2026-01-01',
        endDate: '2026-01-07',
      },
    });
  });
});
