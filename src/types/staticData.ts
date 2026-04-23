import type { ErrorDetailRow } from '@/types/errorAnalysis';

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
  signatureKey: string;
  normalizedMessage: string;
  sampleMessage: string;
  count: number;
  sampleErrors: Record<string, unknown>[];
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
