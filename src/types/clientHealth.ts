/** A single failed merge entry as returned by the integrations-hub health endpoint */
export interface FailedMerge {
  id: string;
  type?: string;
  scheduleType?: string;
  timestampEnd?: string;
  status?: string;
}

/** A single entry from /integrations-hub/merge-history */
export interface MergeHistoryEntry {
  id: string;
  scheduleType: 'nightly' | 'realtime' | 'manual' | string;
  status: 'SUCCESS' | 'ERROR' | 'RUNNING' | string;
  timestampEnd: string;
  type?: string;
}

/** A raw user activity record from /:school/userActivity */
export interface UserActivityRecord {
  email?: string;
  userId?: string;
  timestamp?: string;
  [key: string]: unknown;
}

export interface ClientHealthSnapshot {
  snapshotDate: string;
  school: string;
  displayName: string;
  sisPlatform?: string | null;
  products: string[];
  merges: {
    nightly: { total: number; succeeded: number; failed: number; finishedWithIssues: number; noData: number; mergeTimeMs?: number; };
    realtime: { total: number; succeeded: number; failed: number; finishedWithIssues: number; noData: number; };
    manual: { total: number; succeeded: number; failed: number; finishedWithIssues: number; noData: number; };
  };
  recentFailedMerges: FailedMerge[];
  mergeErrorsCount: number | null;
  activeUsers24h: number;
  createdAt: Date;
}

export interface ClientHealthResponse {
  snapshotDate: string;
  schools: ClientHealthSnapshot[];
}

export interface ClientHealthHistoryResponse {
  snapshots: ClientHealthSnapshot[];
}

export interface ActiveUsersResponse {
  count: number;
  users: string[];
  error?: string;
}

export interface SchoolOption {
  school: string;
  displayName: string;
  products: string[];
}

export interface ExcludedSchoolOption extends SchoolOption {
  reason: string;
}
