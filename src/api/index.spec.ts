import { beforeEach, describe, expect, it, vi } from 'vitest';

const post = vi.fn();
const get = vi.fn();
const del = vi.fn();
const put = vi.fn();
const create = vi.fn(() => ({ post, get, put, delete: del }));
const fetchMock = vi.fn();

vi.mock('axios', () => ({
  default: {
    create,
  },
}));

describe('client health api helpers', () => {
  beforeEach(() => {
    post.mockReset();
    get.mockReset();
    del.mockReset();
    put.mockReset();
    create.mockClear();
    fetchMock.mockReset();
    vi.resetModules();
    vi.unstubAllEnvs();
    vi.stubGlobal('fetch', fetchMock);
  });

  it('fetches the available schools list', async () => {
    get.mockResolvedValue({ data: { schools: [] } });
    const { getSchools } = await import('./index');

    await getSchools();

    expect(get).toHaveBeenCalledWith('/schools');
  });

  it('configures the backend client with the internal api key header when present', async () => {
    vi.stubEnv('VITE_INTERNAL_API_KEY', 'test-key');

    await import('./index');

    expect(create).toHaveBeenCalledWith({
      baseURL: '/backend/api',
      headers: {
        'X-Internal-API-Key': 'test-key',
      },
    });
  });

  it('adds an explicit school exclusion', async () => {
    post.mockResolvedValue({ data: { school: 'bar01' } });
    const { addSchoolExclusion } = await import('./index');

    await addSchoolExclusion({ school: 'bar01' });

    expect(post).toHaveBeenCalledWith('/schools/exclusions', {
      school: 'bar01',
    });
  });

  it('removes an explicit school exclusion', async () => {
    del.mockResolvedValue({ data: { school: 'bar01' } });
    const { removeSchoolExclusion } = await import('./index');

    await removeSchoolExclusion('bar01');

    expect(del).toHaveBeenCalledWith('/schools/exclusions/bar01');
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

  it('resumes a historical backfill by job id', async () => {
    post.mockResolvedValue({ data: { jobId: 'bulk-1' } });
    const { resumeHistoryBackfill } = await import('./index');

    await resumeHistoryBackfill('bulk-1');

    expect(post).toHaveBeenCalledWith('/client-health/history/backfill/bulk-1/resume');
  });

  it('retries failed historical backfill units by job id', async () => {
    post.mockResolvedValue({ data: { jobId: 'bulk-1' } });
    const { retryHistoryBackfillFailures } = await import('./index');

    await retryHistoryBackfillFailures('bulk-1');

    expect(post).toHaveBeenCalledWith('/client-health/history/backfill/bulk-1/retry-failures');
  });

  it('fetches paged sync runs', async () => {
    get.mockResolvedValue({ data: { syncRuns: [], totalCount: 0, limit: 25, offset: 25 } });
    const { getSyncRuns } = await import('./index');

    await getSyncRuns({ limit: 25, offset: 25 });

    expect(get).toHaveBeenCalledWith('/client-health/sync-runs', {
      params: {
        limit: 25,
        offset: 25,
      },
    });
  });

  it('fetches scheduler settings', async () => {
    get.mockResolvedValue({ data: { syncEnabled: true, syncTime: '07:30' } });
    const { getSchedulerSettings } = await import('./index');

    await getSchedulerSettings();

    expect(get).toHaveBeenCalledWith('/client-health/scheduler-settings');
  });

  it('updates scheduler settings', async () => {
    put.mockResolvedValue({ data: { syncEnabled: false, syncTime: '09:15' } });
    const { updateSchedulerSettings } = await import('./index');

    await updateSchedulerSettings({ syncEnabled: false, syncTime: '09:15' });

    expect(put).toHaveBeenCalledWith('/client-health/scheduler-settings', {
      syncEnabled: false,
      syncTime: '09:15',
    });
  });

  it('fetches full client health history when no day window is provided', async () => {
    get.mockResolvedValue({ data: { snapshots: [] } });
    const { getClientHealthHistory } = await import('./index');

    await getClientHealthHistory({});

    expect(get).toHaveBeenCalledWith('/client-health/history', {
      params: {},
    });
  });

  it('fetches a single sync run by job id', async () => {
    get.mockResolvedValue({ data: { syncRun: { jobId: 'bulk-1' } } });
    const { getSyncRun } = await import('./index');

    await getSyncRun('bulk-1');

    expect(get).toHaveBeenCalledWith('/client-health/sync-runs/bulk-1');
  });

  it('loads latest client health from static json when static mode is enabled', async () => {
    vi.stubEnv('VITE_DATA_MODE', 'static');
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        snapshotDate: '2026-04-22',
        schools: [],
        excludedSchools: [{ school: 'demo01', displayName: 'Demo School', products: [], reason: 'Matches excluded term: demo' }],
        excludedTerms: ['demo', 'test'],
        additionalExcludedSchools: ['demo01'],
      }),
    });
    const { getClientHealth } = await import('./index');

    const response = await getClientHealth();

    expect(fetchMock).toHaveBeenCalledWith('/static-data/client-health/latest.json', {
      headers: { Accept: 'application/json' },
    });
    expect(response.data.snapshotDate).toBe('2026-04-22');
  });

  it('loads excluded schools from static json when fetching schools in static mode', async () => {
    vi.stubEnv('VITE_DATA_MODE', 'static');
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        snapshotDate: '2026-04-22',
        schools: [
          { snapshotDate: '2026-04-22', school: 'bar01', displayName: 'Baruch College', products: [] },
        ],
        excludedSchools: [
          { school: 'demo01', displayName: 'Demo School', products: [], reason: 'Matches excluded term: demo' },
        ],
        excludedTerms: ['demo', 'test'],
        additionalExcludedSchools: ['demo01'],
      }),
    });
    const { getSchools } = await import('./index');

    const response = await getSchools();

    expect(fetchMock).toHaveBeenCalledWith('/static-data/client-health/latest.json', {
      headers: { Accept: 'application/json' },
    });
    expect(response.data.excludedSchools).toEqual([
      { school: 'demo01', displayName: 'Demo School', products: [], reason: 'Matches excluded term: demo' },
    ]);
    expect(response.data.excludedTerms).toEqual(['demo', 'test']);
    expect(response.data.additionalExcludedSchools).toEqual(['demo01']);
  });

  it('loads sync runs from static json when static mode is enabled', async () => {
    vi.stubEnv('VITE_DATA_MODE', 'static');
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        syncRuns: [{ jobId: 'job-1' }, { jobId: 'job-2' }],
        totalCount: 2,
        limit: 2,
        offset: 0,
      }),
    });
    const { getSyncRuns } = await import('./index');

    const response = await getSyncRuns({ limit: 1, offset: 1 });

    expect(fetchMock).toHaveBeenCalledWith('/static-data/jobs/sync-runs.json', {
      headers: { Accept: 'application/json' },
    });
    expect(response.data.syncRuns).toEqual([{ jobId: 'job-2' }]);
    expect(response.data.totalCount).toBe(2);
  });

  it('builds example merge report links from compact static error summary references', async () => {
    vi.stubEnv('VITE_DATA_MODE', 'static');
    fetchMock.mockResolvedValue({
      ok: true,
      json: async () => ({
        metadata: {
          historyStartsOn: '2026-04-12',
          lastCapturedAt: '2026-04-13T12:00:00Z',
          exportedAt: '2026-04-15T00:00:00Z',
        },
        groups: [
          {
            snapshotDate: '2026-04-13',
            school: 'foo01',
            displayName: 'Foo State',
            sisPlatform: 'PeopleSoftDirect',
            entityType: 'courses',
            errorCode: 'duplicate_course',
            signatureKey: 'sig-b',
            normalizedMessage: 'duplicate course <num>',
            sampleMessage: 'Duplicate course 12345',
            count: 4,
            sampleErrors: [],
            latestMergeReport: {
              school: 'foo01',
              mergeReportId: 'report-b1',
              scheduleType: 'realtime',
              entityDisplayName: 'Duplicate course 12345',
              snapshotDate: '2026-04-13',
            },
            termCodes: ['202505'],
          },
        ],
      }),
    });
    const { getErrorAnalysis } = await import('./index');

    const response = await getErrorAnalysis({});

    expect(fetchMock).toHaveBeenCalledWith('/static-data/error-analysis/summary.json', {
      headers: { Accept: 'application/json' },
    });
    expect(response.data.signatures[0].latestMergeReport?.mergeReportId).toBe('report-b1');
    expect(response.data.signatures[0].dominantSchoolMergeReport?.mergeReportId).toBe('report-b1');
    expect(response.data.signatures[0].exampleMergeReports).toEqual([
      {
        school: 'foo01',
        mergeReportId: 'report-b1',
        scheduleType: 'realtime',
        entityDisplayName: 'Duplicate course 12345',
        snapshotDate: '2026-04-13',
      },
    ]);
  });
});
