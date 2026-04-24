export interface ResolutionHint {
  bucket: string;
  title: string;
  action: string;
  rationale: string;
  confidence: number;
}

export interface MergeReportReference {
  school: string;
  mergeReportId: string;
  scheduleType?: string | null;
  entityDisplayName?: string | null;
  snapshotDate: string;
}

export interface ErrorTrendPoint {
  snapshotDate: string;
  totalErrors: number;
  distinctSignatures: number;
  affectedSchools: number;
}

export interface ErrorSignatureCluster {
  signatureKey: string;
  entityType?: string | null;
  errorCode?: string | null;
  signatureLabel: string;
  normalizedMessage: string;
  sampleMessage: string;
  totalCount: number;
  affectedSchools: number;
  affectedSisPlatforms: number;
  firstSeen: string;
  lastSeen: string;
  recurrenceDays: number;
  dominantSchool?: string | null;
  dominantSisPlatform?: string | null;
  sampleErrors: Record<string, unknown>[];
  termCodes: string[];
  resolutionHint: ResolutionHint;
  latestMergeReport?: MergeReportReference | null;
  dominantSchoolMergeReport?: MergeReportReference | null;
  impactedSchools?: Array<{
    school: string;
    label: string;
    count: number;
  }>;
  exampleMergeReports?: MergeReportReference[];
}

export interface ErrorBreakdownRow {
  key: string;
  label: string;
  sisPlatform?: string | null;
  totalErrors: number;
  distinctSignatures: number;
  dominantSignature?: string | null;
  lastSeen: string;
  recurrenceDays?: number;
  likelyNextStep?: string | null;
  topResolutionTheme?: string | null;
  resolutionBuckets?: Record<string, number>;
  affectedSchools?: number;
  commonResolutionTheme?: string | null;
  latestMergeReport?: MergeReportReference | null;
  associatedSignatures?: Array<{
    signatureKey: string;
    signatureLabel: string;
    count: number;
    entityType?: string | null;
    errorCode?: string | null;
    resolutionTitle?: string | null;
    sampleMessage?: string | null;
  }>;
}

export interface ErrorAnalysisFilterOption {
  value: string;
  label: string;
  sisPlatform?: string | null;
}

export interface ErrorDetailRow {
  id: number;
  snapshotDate: string;
  school: string;
  displayName: string;
  sisPlatform?: string | null;
  entityType?: string | null;
  errorCode?: string | null;
  signatureKey: string;
  signatureLabel: string;
  normalizedMessage: string;
  fullErrorText: string;
  entityDisplayName?: string | null;
  mergeReport?: MergeReportReference | null;
  termCodes: string[];
  rawError?: Record<string, unknown> | null;
  createdAt?: string | null;
}

export interface ErrorDetailFilterOption {
  value: string;
  label: string;
}

export interface ErrorDetailTableResponse {
  rows: ErrorDetailRow[];
  total: number;
  page: number;
  pageSize: number;
  sortBy: string;
  sortDir: 'asc' | 'desc';
  metadata: {
    appliedFilters: {
      days: number | null;
      school: string | null;
      sisPlatform: string | null;
      latestOnly: boolean;
      category?: string | null;
      entityType?: string | null;
      signature?: string | null;
      q: string | null;
    };
    resolvedSnapshotDate: string | null;
    filterOptions?: {
      categories?: ErrorDetailFilterOption[];
      entityTypes?: ErrorDetailFilterOption[];
      signatures?: ErrorDetailFilterOption[];
    };
  };
}

export type ErrorSignatureExplorerGroupBy = 'sis' | 'school' | 'term';

export interface ErrorSignatureExplorerBucket {
  key: string;
  label: string;
  count: number;
  share: number;
}

export interface ErrorSignatureExplorerSchoolCount {
  school: string;
  label: string;
  count: number;
}

export interface ErrorSignatureExplorerRow extends ErrorDetailRow {
  key: string;
  instanceCount: number;
  schools: ErrorSignatureExplorerSchoolCount[];
}

export interface ErrorSignatureExplorerResponse {
  rows: ErrorSignatureExplorerRow[];
  total: number;
  page: number;
  pageSize: number;
  metadata: {
    appliedFilters: {
      days: number | null;
      school: string | null;
      sisPlatform: string | null;
      latestOnly: boolean;
      signature: string;
      groupBy: ErrorSignatureExplorerGroupBy;
      bucket: string | null;
    };
    resolvedSnapshotDate: string | null;
    signatureTotal: number;
    bucketTotal: number;
  };
  breakdowns: Record<ErrorSignatureExplorerGroupBy, ErrorSignatureExplorerBucket[]>;
}

export interface ErrorAnalysisResponse {
  metadata: {
    historyStartsOn: string | null;
    lastCapturedAt: string | null;
    appliedFilters: {
      days: number | null;
      school: string | null;
      sisPlatform: string | null;
      latestOnly: boolean;
    };
    hasCapturedData: boolean;
    filteredGroupCount: number;
    resolvedSnapshotDate: string | null;
  };
  filterOptions: {
    schools: ErrorAnalysisFilterOption[];
    sisPlatforms: string[];
  };
  summary: {
    totalGroupedErrors: number;
    totalErrorInstances: number;
    distinctSignatures: number;
    affectedSchools: number;
    affectedSisPlatforms: number;
    captureDays: number;
    latestSnapshotDate: string | null;
  };
  trends: ErrorTrendPoint[];
  signatures: ErrorSignatureCluster[];
  schoolBreakdowns: ErrorBreakdownRow[];
  sisBreakdowns: ErrorBreakdownRow[];
}
