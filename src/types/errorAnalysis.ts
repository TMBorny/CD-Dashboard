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
  affectedSchools?: number;
  commonResolutionTheme?: string | null;
  latestMergeReport?: MergeReportReference | null;
}

export interface ErrorAnalysisFilterOption {
  value: string;
  label: string;
  sisPlatform?: string | null;
}

export interface ErrorAnalysisResponse {
  metadata: {
    historyStartsOn: string | null;
    lastCapturedAt: string | null;
    appliedFilters: {
      days: number | null;
      school: string | null;
      sisPlatform: string | null;
    };
    hasCapturedData: boolean;
    filteredGroupCount: number;
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
