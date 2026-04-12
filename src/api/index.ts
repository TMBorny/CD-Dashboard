import axios from 'axios';

const internalApiKey = import.meta.env.VITE_INTERNAL_API_KEY?.trim();

// --- Backend API (cached data from SQLite) ---
const backend = axios.create({
  baseURL: '/backend/api',
  ...(internalApiKey
    ? {
        headers: {
          'X-Internal-API-Key': internalApiKey,
        },
      }
    : {}),
});

export async function getSchools() {
  const res = await backend.get('/schools');
  return { data: res.data };
}

export async function addSchoolExclusion(params: { school: string }) {
  const res = await backend.post('/schools/exclusions', {
    school: params.school,
  });
  return { data: res.data };
}

export async function removeSchoolExclusion(school: string) {
  const res = await backend.delete(`/schools/exclusions/${school}`);
  return { data: res.data };
}

/**
 * Get the latest health snapshot for all schools.
 * This hits our local backend which serves pre-cached data from SQLite.
 */
export async function getClientHealth() {
  const res = await backend.get('/client-health');
  return { data: res.data };
}

/**
 * Trigger a sync: the backend fetches fresh data from Coursedog for ALL
 * schools and persists it to SQLite. Returns timing information.
 */
export async function triggerSync(params?: { school?: string }) {
  const res = await backend.post('/client-health/sync', null, {
    params: params?.school ? { school: params.school } : {},
  });
  return res.data;
}

export async function getSyncStatus(jobId: string) {
  const res = await backend.get(`/client-health/sync/${jobId}`);
  return res.data;
}

export async function triggerHistoryBackfill(params: { startDate: string; endDate?: string; school?: string }) {
  const res = await backend.post('/client-health/history/backfill', null, {
    params: {
      startDate: params.startDate,
      ...(params.endDate ? { endDate: params.endDate } : {}),
      ...(params.school ? { school: params.school } : {}),
    },
  });
  return res.data;
}

export async function getClientHealthSyncMetadata(params?: { school?: string }) {
  const res = await backend.get('/client-health/sync-metadata', {
    params: params?.school ? { school: params.school } : {},
  });
  return { data: res.data };
}


// ---------------------------------------------------------------------------
// Detail page data — fetched live from Coursedog via direct API
// ---------------------------------------------------------------------------

export async function getClientHealthHistory(params: { days?: number; school?: string }) {
  const res = await backend.get('/client-health/history', {
    params: {
      days: params.days ?? 30,
      ...(params.school ? { school: params.school } : {}),
    },
  });

  return { data: res.data };
}

/**
 * Get active users for a specific school via the local backend.
 */
export async function getClientHealthActiveUsers(params: { school?: string }) {
  const schoolId = params.school;
  if (!schoolId) {
    throw new Error('school is required');
  }

  const after = String(Date.now() - 24 * 60 * 60 * 1000);
  const res = await backend.get('/client-health/active-users', {
    params: {
      school: schoolId,
      after,
    },
  });
  return { data: res.data };
}

export async function getSyncRuns(params?: { limit?: number; offset?: number }) {
  const res = await backend.get('/client-health/sync-runs', {
    params: {
      limit: params?.limit ?? 50,
      offset: params?.offset ?? 0,
    },
  });
  return { data: res.data };
}

export { backend };
