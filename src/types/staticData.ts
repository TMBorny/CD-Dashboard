import type { ErrorDetailRow, MergeReportReference } from '@/types/errorAnalysis';

export interface StaticExportFileManifestEntry {
  path: string;
  sha256: string;
  sizeBytes: number;
}

export interface StaticExportManifest {
  exportedAt: string;
  latestSnapshotDate: string | null;
  referenceDate: string | null;
  dataMode: 'static';
  files: Record<string, StaticExportFileManifestEntry>;
}

export interface StaticErrorGroupRow {
  snapshotDate: string;
  school: string;
  displayName: string;
  sisPlatform?: string | null;
  entityType?: string | null;
  errorCode?: string | null;
  canonicalErrorCode?: string | null;
  canonicalCodeSource?: string | null;
  operationName?: string | null;
  signatureKey: string;
  legacySignatureKey?: string | null;
  signatureVersion?: string | null;
  signatureStrategy?: string | null;
  signatureConfidence?: number | null;
  messageTemplate?: string | null;
  normalizedMessage: string;
  sampleMessage: string;
  count: number;
  sampleErrors: Record<string, unknown>[];
  latestMergeReport?: MergeReportReference | null;
  termCodes: string[];
  createdAt?: string | null;
}

export interface StaticErrorSummaryExport {
  metadata: {
    historyStartsOn: string | null;
    lastCapturedAt: string | null;
    exportedAt: string;
  };
  groups: StaticErrorGroupRow[];
}

export interface StaticErrorDetailExport {
  metadata: {
    historyStartsOn: string | null;
    lastCapturedAt: string | null;
    exportedAt: string;
  };
  rows: ErrorDetailRow[];
}
