import type {
  ActiveUsersResponse,
  ClientHealthHistoryResponse,
  ClientHealthResponse,
} from '@/types/clientHealth';
import type {
  ErrorAnalysisFilterOption,
  ErrorAnalysisResponse,
  ErrorBreakdownRow,
  ErrorDetailRow,
  ErrorDetailTableResponse,
  ErrorSignatureCluster,
  ErrorTrendPoint,
  MergeReportReference,
  ResolutionHint,
} from '@/types/errorAnalysis';
import type { SyncRunsResponse } from '@/types/jobRuns';
import type {
  StaticErrorDetailExport,
  StaticErrorSummaryExport,
  StaticExportManifest,
} from '@/types/staticData';
import { resolveSitePath } from '@/config/runtime';

const STATIC_MODE_ERROR = 'This action is unavailable in the static hosted dashboard.';
const fileCache = new Map<string, Promise<unknown>>();

type StaticSyncMetadataResponse = {
  lastAttemptedSync: Record<string, unknown> | null;
  lastSuccessfulSync: Record<string, unknown> | null;
};

const loadStaticJson = async <T>(relativePath: string): Promise<T> => {
  const cacheKey = relativePath.replace(/^\/+/, '');
  if (!fileCache.has(cacheKey)) {
    fileCache.set(
      cacheKey,
      fetch(resolveSitePath(cacheKey), {
        headers: { Accept: 'application/json' },
      }).then(async (response) => {
        if (!response.ok) {
          throw new Error(`Failed to load static data from ${cacheKey} (${response.status})`);
        }
        return response.json();
      }),
    );
  }

  return (await fileCache.get(cacheKey)) as T;
};

const getManifest = () => loadStaticJson<StaticExportManifest>('static-data/manifest.json');
const getLatestClientHealth = () => loadStaticJson<ClientHealthResponse>('static-data/client-health/latest.json');
const getFullClientHealthHistory = () => loadStaticJson<ClientHealthHistoryResponse>('static-data/client-health/history.json');
const getStaticErrorSummaryExport = () => loadStaticJson<StaticErrorSummaryExport>('static-data/error-analysis/summary.json');
const getStaticErrorDetailExport = () => loadStaticJson<StaticErrorDetailExport>('static-data/error-analysis/errors.json');
const getStaticSyncRunsExport = () => loadStaticJson<SyncRunsResponse>('static-data/jobs/sync-runs.json');

const getReferenceDate = async () => {
  const manifest = await getManifest();
  return manifest.referenceDate ?? manifest.latestSnapshotDate ?? manifest.exportedAt.slice(0, 10);
};

const parseDateKey = (value?: string | null) => {
  if (!value) return null;
  const timestamp = Date.parse(`${value}T00:00:00Z`);
  return Number.isNaN(timestamp) ? null : timestamp;
};

const buildCutoffDate = (referenceDate: string, days: number) => {
  const referenceTimestamp = parseDateKey(referenceDate);
  if (referenceTimestamp == null) {
    return null;
  }

  const cutoff = new Date(referenceTimestamp);
  cutoff.setUTCDate(cutoff.getUTCDate() - days);
  return cutoff.toISOString().slice(0, 10);
};

const applySnapshotWindow = async <T extends { snapshotDate: string }>(rows: T[], days?: number) => {
  if (typeof days !== 'number') {
    return rows;
  }

  const referenceDate = await getReferenceDate();
  const cutoff = buildCutoffDate(referenceDate, days);
  if (!cutoff) {
    return rows;
  }

  return rows.filter((row) => row.snapshotDate >= cutoff);
};

const firstNonEmptyString = (...values: Array<unknown>) => {
  for (const value of values) {
    if (typeof value === 'string' && value.trim()) {
      return value.trim();
    }
  }
  return null;
};

const buildErrorSignatureLabel = (
  entityType: string | null | undefined,
  errorCode: string | null | undefined,
  normalizedMessage: string,
) => {
  const parts = [entityType || 'unknown-entity'];
  if (errorCode) {
    parts.push(errorCode);
  }
  parts.push(normalizedMessage);
  return parts.join(' | ');
};

const buildResolutionHint = (
  normalizedMessage: string,
  entityType?: string | null,
  errorCode?: string | null,
): ResolutionHint => {
  const haystack = [normalizedMessage, entityType || '', errorCode || ''].join(' ').toLowerCase();

  if (['missing', 'not found', 'not.found', 'unknown', 'reference', 'dependency', 'prerequisite', 'does not exist']
    .some((token) => haystack.includes(token))) {
    return {
      bucket: 'missing_reference',
      title: 'Missing dependency or reference',
      action: 'Verify the referenced records exist in the SIS and are synced before retrying dependent entities.',
      rationale: 'The signature reads like a missing upstream dependency or lookup reference.',
      confidence: 0.82,
    };
  }

  if (['duplicate', 'already exists', 'conflict', 'unique', 'overlap', 'overlapping']
    .some((token) => haystack.includes(token))) {
    return {
      bucket: 'duplicate_conflict',
      title: 'Duplicate or conflicting record',
      action: 'Check for duplicate source records or conflicting identifiers and resolve the collision before rerunning the sync.',
      rationale: 'The signature points to a duplicate, uniqueness, or conflicting-write condition.',
      confidence: 0.79,
    };
  }

  if (['invalid', 'validation', 'required', 'malformed', 'format', 'parse', 'cannot parse', 'type mismatch', 'schema', 'null']
    .some((token) => haystack.includes(token))) {
    return {
      bucket: 'validation_data_shape',
      title: 'Validation or data-shape issue',
      action: 'Review the source payload for missing required fields, invalid formats, or schema mismatches before the next sync.',
      rationale: 'The signature suggests the payload shape or field values do not satisfy validation.',
      confidence: 0.77,
    };
  }

  if (['auth', 'permission', 'unauthorized', 'forbidden', 'token', 'credential', 'config', 'mapping', 'disabled']
    .some((token) => haystack.includes(token))) {
    return {
      bucket: 'configuration_auth',
      title: 'Configuration or access problem',
      action: 'Review integration credentials, permissions, and mapping/configuration settings for the affected entity type.',
      rationale: 'The signature looks tied to authentication, permissions, or configuration drift.',
      confidence: 0.74,
    };
  }

  return {
    bucket: 'generic_investigation',
    title: 'General investigation recommended',
    action: 'Inspect a sample error in Integration Hub, compare recent changes for the entity type, and confirm whether the issue is isolated to one school or SIS.',
    rationale: 'The signature is not specific enough for a stronger automatic recommendation.',
    confidence: 0.52,
  };
};

const extractMergeReportReference = (sampleErrors: Array<Record<string, unknown>>): MergeReportReference | null => {
  for (const sampleError of sampleErrors) {
    const mergeReport = typeof sampleError.mergeReport === 'object' && sampleError.mergeReport !== null
      ? sampleError.mergeReport as Record<string, unknown>
      : null;
    const mergeReportId = firstNonEmptyString(
      sampleError.lastSyncMergeReportId,
      sampleError.mergeReportId,
      mergeReport?.id,
    );

    if (!mergeReportId) {
      continue;
    }

    return {
      school: '',
      mergeReportId,
      scheduleType: typeof mergeReport?.scheduleType === 'string' ? mergeReport.scheduleType : null,
      entityDisplayName: firstNonEmptyString(sampleError.entityDisplayName, sampleError.entityId),
      snapshotDate: '',
    };
  }

  return null;
};

const getDominantEntry = (counter: Map<string, number>) => {
  let bestKey: string | null = null;
  let bestCount = -1;

  for (const [key, count] of counter.entries()) {
    if (count > bestCount || (count === bestCount && bestKey !== null && key < bestKey)) {
      bestKey = key;
      bestCount = count;
    }
  }

  return bestKey;
};

const getLatestSnapshotDate = <T extends { snapshotDate: string }>(rows: T[]) => {
  if (!rows.length) {
    return null;
  }

  return rows.reduce((latest, row) => (row.snapshotDate > latest ? row.snapshotDate : latest), rows[0].snapshotDate);
};

const normalizeDetailSearch = (row: ErrorDetailRow) => {
  return [
    row.fullErrorText,
    row.signatureLabel,
    row.normalizedMessage,
    row.entityType,
    row.errorCode,
    row.school,
    row.displayName,
    row.sisPlatform,
    row.entityDisplayName,
    row.mergeReport?.mergeReportId,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
};

const sortDetailRows = (rows: ErrorDetailRow[], sortBy: string, sortDir: 'asc' | 'desc') => {
  const multiplier = sortDir === 'asc' ? 1 : -1;

  return [...rows].sort((left, right) => {
    const leftValue = (() => {
      switch (sortBy) {
        case 'school':
          return left.school;
        case 'displayName':
          return left.displayName;
        case 'sisPlatform':
          return left.sisPlatform ?? '';
        case 'entityType':
          return left.entityType ?? '';
        case 'errorCode':
          return left.errorCode ?? '';
        case 'signatureLabel':
          return left.signatureLabel;
        case 'mergeReportId':
          return left.mergeReport?.mergeReportId ?? '';
        case 'snapshotDate':
        default:
          return left.snapshotDate;
      }
    })();

    const rightValue = (() => {
      switch (sortBy) {
        case 'school':
          return right.school;
        case 'displayName':
          return right.displayName;
        case 'sisPlatform':
          return right.sisPlatform ?? '';
        case 'entityType':
          return right.entityType ?? '';
        case 'errorCode':
          return right.errorCode ?? '';
        case 'signatureLabel':
          return right.signatureLabel;
        case 'mergeReportId':
          return right.mergeReport?.mergeReportId ?? '';
        case 'snapshotDate':
        default:
          return right.snapshotDate;
      }
    })();

    if (leftValue < rightValue) return -1 * multiplier;
    if (leftValue > rightValue) return 1 * multiplier;
    return ((sortDir === 'desc' ? right.id - left.id : left.id - right.id) || 0);
  });
};

const buildErrorAnalysisResponse = async (
  params: { days?: number; school?: string; sisPlatform?: string; latestOnly?: boolean },
): Promise<ErrorAnalysisResponse> => {
  const [summaryExport, latestClientHealth] = await Promise.all([
    getStaticErrorSummaryExport(),
    getLatestClientHealth(),
  ]);

  const latestRows = latestClientHealth.schools ?? [];
  const schoolOptions: ErrorAnalysisFilterOption[] = [...latestRows]
    .sort((left, right) => (left.displayName || left.school).localeCompare(right.displayName || right.school))
    .map((row) => ({
      value: row.school,
      label: row.displayName || row.school,
      sisPlatform: row.sisPlatform,
    }));
  const sisValues = new Set(
    latestRows
      .map((row) => row.sisPlatform)
      .filter((value): value is string => Boolean(value)),
  );

  const rawGroups = await applySnapshotWindow(summaryExport.groups, params.days);
  const filteredByParams = rawGroups.filter((group) => {
    if (params.school && group.school !== params.school) return false;
    if (params.sisPlatform && group.sisPlatform !== params.sisPlatform) return false;
    return true;
  });

  const resolvedSnapshotDate = params.latestOnly ? getLatestSnapshotDate(filteredByParams) : null;
  const groups = resolvedSnapshotDate
    ? filteredByParams.filter((group) => group.snapshotDate === resolvedSnapshotDate)
    : filteredByParams;

  const response: ErrorAnalysisResponse = {
    metadata: {
      historyStartsOn: summaryExport.metadata.historyStartsOn,
      lastCapturedAt: summaryExport.metadata.lastCapturedAt,
      appliedFilters: {
        days: params.days ?? null,
        school: params.school ?? null,
        sisPlatform: params.sisPlatform ?? null,
        latestOnly: Boolean(params.latestOnly),
      },
      hasCapturedData: Boolean(summaryExport.metadata.historyStartsOn),
      filteredGroupCount: groups.length,
      resolvedSnapshotDate,
    },
    filterOptions: {
      schools: schoolOptions,
      sisPlatforms: [...sisValues].sort(),
    },
    summary: {
      totalGroupedErrors: groups.length,
      totalErrorInstances: 0,
      distinctSignatures: 0,
      affectedSchools: 0,
      affectedSisPlatforms: 0,
      captureDays: 0,
      latestSnapshotDate: null,
    },
    trends: [],
    signatures: [],
    schoolBreakdowns: [],
    sisBreakdowns: [],
  };

  if (!groups.length) {
    return response;
  }

  const trends = new Map<string, { totalErrors: number; distinctSignatures: Set<string>; affectedSchools: Set<string> }>();
  const signatures = new Map<string, {
    signatureKey: string;
    entityType?: string | null;
    errorCode?: string | null;
    signatureLabel: string;
    normalizedMessage: string;
    sampleMessage: string;
    totalCount: number;
    affectedSchools: Set<string>;
    affectedSisPlatforms: Set<string>;
    firstSeen: string;
    lastSeen: string;
    recurrenceDays: Set<string>;
    countsBySchool: Map<string, number>;
    countsBySis: Map<string, number>;
    schoolLabels: Map<string, string>;
    latestMergeReportBySchool: Map<string, MergeReportReference>;
    exampleMergeReports: MergeReportReference[];
    sampleErrors: Record<string, unknown>[];
    termCodes: Set<string>;
    resolutionHint: ResolutionHint;
    latestMergeReport: MergeReportReference | null;
  }>();
  const schoolBreakdowns = new Map<string, {
    key: string;
    label: string;
    sisPlatform?: string | null;
    totalErrors: number;
    distinctSignatures: Set<string>;
    countsBySignature: Map<string, number>;
    resolutionBuckets: Map<string, number>;
    lastSeen: string;
    recurrenceDays: Set<string>;
    latestMergeReport: MergeReportReference | null;
  }>();
  const sisBreakdowns = new Map<string, {
    key: string;
    label: string;
    schoolCountSet: Set<string>;
    totalErrors: number;
    distinctSignatures: Set<string>;
    countsBySignature: Map<string, number>;
    resolutionBuckets: Map<string, number>;
    lastSeen: string;
  }>();
  const affectedSchools = new Set<string>();
  const affectedSis = new Set<string>();
  const captureDays = new Set<string>();

  for (const group of groups) {
    const resolutionHint = buildResolutionHint(group.normalizedMessage, group.entityType, group.errorCode);
    const sisLabel = group.sisPlatform || 'Unknown';
    const mergeReportReference = extractMergeReportReference(group.sampleErrors);

    affectedSchools.add(group.school);
    affectedSis.add(sisLabel);
    captureDays.add(group.snapshotDate);

    const trend = trends.get(group.snapshotDate) ?? {
      totalErrors: 0,
      distinctSignatures: new Set<string>(),
      affectedSchools: new Set<string>(),
    };
    trend.totalErrors += group.count;
    trend.distinctSignatures.add(group.signatureKey);
    trend.affectedSchools.add(group.school);
    trends.set(group.snapshotDate, trend);

    const signature = signatures.get(group.signatureKey) ?? {
      signatureKey: group.signatureKey,
      entityType: group.entityType,
      errorCode: group.errorCode,
      signatureLabel: buildErrorSignatureLabel(group.entityType, group.errorCode, group.normalizedMessage),
      normalizedMessage: group.normalizedMessage,
      sampleMessage: group.sampleMessage,
      totalCount: 0,
      affectedSchools: new Set<string>(),
      affectedSisPlatforms: new Set<string>(),
      firstSeen: group.snapshotDate,
      lastSeen: group.snapshotDate,
      recurrenceDays: new Set<string>(),
      countsBySchool: new Map<string, number>(),
      countsBySis: new Map<string, number>(),
      schoolLabels: new Map<string, string>(),
      latestMergeReportBySchool: new Map<string, MergeReportReference>(),
      exampleMergeReports: [],
      sampleErrors: group.sampleErrors,
      termCodes: new Set<string>(group.termCodes),
      resolutionHint,
      latestMergeReport: null,
    };

    signature.totalCount += group.count;
    signature.affectedSchools.add(group.school);
    signature.affectedSisPlatforms.add(sisLabel);
    signature.firstSeen = signature.firstSeen < group.snapshotDate ? signature.firstSeen : group.snapshotDate;
    signature.lastSeen = signature.lastSeen > group.snapshotDate ? signature.lastSeen : group.snapshotDate;
    signature.recurrenceDays.add(group.snapshotDate);
    signature.countsBySchool.set(group.school, (signature.countsBySchool.get(group.school) ?? 0) + group.count);
    signature.countsBySis.set(sisLabel, (signature.countsBySis.get(sisLabel) ?? 0) + group.count);
    signature.schoolLabels.set(group.school, group.displayName || group.school);
    group.termCodes.forEach((termCode) => signature.termCodes.add(termCode));

    if (mergeReportReference) {
      const normalizedReference = {
        ...mergeReportReference,
        school: group.school,
        snapshotDate: group.snapshotDate,
      };

      if (!signature.latestMergeReport || group.snapshotDate >= signature.latestMergeReport.snapshotDate) {
        signature.latestMergeReport = normalizedReference;
      }

      const existingSchoolReference = signature.latestMergeReportBySchool.get(group.school);
      if (!existingSchoolReference || group.snapshotDate >= existingSchoolReference.snapshotDate) {
        signature.latestMergeReportBySchool.set(group.school, normalizedReference);
        signature.exampleMergeReports = [...signature.latestMergeReportBySchool.values()]
          .sort((left, right) =>
            right.snapshotDate.localeCompare(left.snapshotDate)
            || left.school.localeCompare(right.school)
            || left.mergeReportId.localeCompare(right.mergeReportId),
          )
          .slice(0, 5);
      }
    }

    signatures.set(group.signatureKey, signature);

    const schoolBreakdown = schoolBreakdowns.get(group.school) ?? {
      key: group.school,
      label: group.displayName || group.school,
      sisPlatform: group.sisPlatform,
      totalErrors: 0,
      distinctSignatures: new Set<string>(),
      countsBySignature: new Map<string, number>(),
      resolutionBuckets: new Map<string, number>(),
      lastSeen: group.snapshotDate,
      recurrenceDays: new Set<string>(),
      latestMergeReport: null,
    };
    schoolBreakdown.totalErrors += group.count;
    schoolBreakdown.distinctSignatures.add(group.signatureKey);
    schoolBreakdown.countsBySignature.set(group.signatureKey, (schoolBreakdown.countsBySignature.get(group.signatureKey) ?? 0) + group.count);
    schoolBreakdown.resolutionBuckets.set(
      resolutionHint.bucket,
      (schoolBreakdown.resolutionBuckets.get(resolutionHint.bucket) ?? 0) + group.count,
    );
    schoolBreakdown.lastSeen = schoolBreakdown.lastSeen > group.snapshotDate ? schoolBreakdown.lastSeen : group.snapshotDate;
    schoolBreakdown.recurrenceDays.add(group.snapshotDate);
    if (mergeReportReference) {
      const normalizedReference = {
        ...mergeReportReference,
        school: group.school,
        snapshotDate: group.snapshotDate,
      };
      if (!schoolBreakdown.latestMergeReport || group.snapshotDate >= schoolBreakdown.latestMergeReport.snapshotDate) {
        schoolBreakdown.latestMergeReport = normalizedReference;
      }
    }
    schoolBreakdowns.set(group.school, schoolBreakdown);

    const sisBreakdown = sisBreakdowns.get(sisLabel) ?? {
      key: sisLabel,
      label: sisLabel,
      schoolCountSet: new Set<string>(),
      totalErrors: 0,
      distinctSignatures: new Set<string>(),
      countsBySignature: new Map<string, number>(),
      resolutionBuckets: new Map<string, number>(),
      lastSeen: group.snapshotDate,
    };
    sisBreakdown.schoolCountSet.add(group.school);
    sisBreakdown.totalErrors += group.count;
    sisBreakdown.distinctSignatures.add(group.signatureKey);
    sisBreakdown.countsBySignature.set(group.signatureKey, (sisBreakdown.countsBySignature.get(group.signatureKey) ?? 0) + group.count);
    sisBreakdown.resolutionBuckets.set(
      resolutionHint.bucket,
      (sisBreakdown.resolutionBuckets.get(resolutionHint.bucket) ?? 0) + group.count,
    );
    sisBreakdown.lastSeen = sisBreakdown.lastSeen > group.snapshotDate ? sisBreakdown.lastSeen : group.snapshotDate;
    sisBreakdowns.set(sisLabel, sisBreakdown);
  }

  const serializedTrends: ErrorTrendPoint[] = [...trends.entries()]
    .map(([snapshotDate, trend]) => ({
      snapshotDate,
      totalErrors: trend.totalErrors,
      distinctSignatures: trend.distinctSignatures.size,
      affectedSchools: trend.affectedSchools.size,
    }))
    .sort((left, right) => left.snapshotDate.localeCompare(right.snapshotDate));

  const serializedSignatures: ErrorSignatureCluster[] = [...signatures.values()]
    .map((signature) => {
      const dominantSchool = getDominantEntry(signature.countsBySchool);
      const dominantSisPlatform = getDominantEntry(signature.countsBySis);
      return {
        signatureKey: signature.signatureKey,
        entityType: signature.entityType,
        errorCode: signature.errorCode,
        signatureLabel: signature.signatureLabel,
        normalizedMessage: signature.normalizedMessage,
        sampleMessage: signature.sampleMessage,
        totalCount: signature.totalCount,
        affectedSchools: signature.affectedSchools.size,
        affectedSisPlatforms: signature.affectedSisPlatforms.size,
        firstSeen: signature.firstSeen,
        lastSeen: signature.lastSeen,
        recurrenceDays: signature.recurrenceDays.size,
        dominantSchool,
        dominantSisPlatform,
        sampleErrors: signature.sampleErrors,
        termCodes: [...signature.termCodes].sort(),
        resolutionHint: signature.resolutionHint,
        latestMergeReport: signature.latestMergeReport,
        dominantSchoolMergeReport: dominantSchool ? signature.latestMergeReportBySchool.get(dominantSchool) ?? null : null,
        impactedSchools: [...signature.countsBySchool.entries()]
          .sort((left, right) => (right[1] - left[1]) || left[0].localeCompare(right[0]))
          .map(([school, count]) => ({
            school,
            label: signature.schoolLabels.get(school) ?? school,
            count,
          })),
        exampleMergeReports: signature.exampleMergeReports,
      };
    })
    .sort((left, right) =>
      (right.totalCount - left.totalCount)
      || (right.recurrenceDays - left.recurrenceDays)
      || left.sampleMessage.localeCompare(right.sampleMessage),
    );

  const serializedSchoolBreakdowns: ErrorBreakdownRow[] = [...schoolBreakdowns.values()]
    .map((schoolBreakdown) => {
      const dominantSignatureKey = getDominantEntry(schoolBreakdown.countsBySignature);
      const dominantSignature = dominantSignatureKey ? signatures.get(dominantSignatureKey) : null;
      const dominantBucket = getDominantEntry(schoolBreakdown.resolutionBuckets);
      return {
        key: schoolBreakdown.key,
        label: schoolBreakdown.label,
        sisPlatform: schoolBreakdown.sisPlatform,
        totalErrors: schoolBreakdown.totalErrors,
        distinctSignatures: schoolBreakdown.distinctSignatures.size,
        dominantSignature: dominantSignature?.signatureLabel ?? null,
        lastSeen: schoolBreakdown.lastSeen,
        recurrenceDays: schoolBreakdown.recurrenceDays.size,
        likelyNextStep: dominantSignature?.resolutionHint.action ?? null,
        topResolutionTheme: dominantBucket,
        resolutionBuckets: Object.fromEntries(schoolBreakdown.resolutionBuckets),
        latestMergeReport: schoolBreakdown.latestMergeReport,
      };
    })
    .sort((left, right) => (right.totalErrors - left.totalErrors) || left.label.localeCompare(right.label));

  const serializedSisBreakdowns: ErrorBreakdownRow[] = [...sisBreakdowns.values()]
    .map((sisBreakdown) => {
      const dominantSignatureKey = getDominantEntry(sisBreakdown.countsBySignature);
      const dominantSignature = dominantSignatureKey ? signatures.get(dominantSignatureKey) : null;
      const dominantBucket = getDominantEntry(sisBreakdown.resolutionBuckets);
      return {
        key: sisBreakdown.key,
        label: sisBreakdown.label,
        affectedSchools: sisBreakdown.schoolCountSet.size,
        totalErrors: sisBreakdown.totalErrors,
        distinctSignatures: sisBreakdown.distinctSignatures.size,
        dominantSignature: dominantSignature?.signatureLabel ?? null,
        lastSeen: sisBreakdown.lastSeen,
        commonResolutionTheme: dominantBucket,
        associatedSignatures: [...sisBreakdown.countsBySignature.entries()]
          .sort((left, right) => (right[1] - left[1]) || left[0].localeCompare(right[0]))
          .flatMap(([signatureKey, count]) => {
            const signature = signatures.get(signatureKey);
            if (!signature) return [];
            return [{
              signatureKey: signature.signatureKey,
              signatureLabel: signature.signatureLabel,
              count,
              entityType: signature.entityType,
              errorCode: signature.errorCode,
              resolutionTitle: signature.resolutionHint.title,
              sampleMessage: signature.sampleMessage,
            }];
          }),
      };
    })
    .sort((left, right) => (right.totalErrors - left.totalErrors) || left.label.localeCompare(right.label));

  const latestSnapshotDate = [...captureDays].sort().at(-1) ?? null;
  const latestSnapshotBySchool = new Map<string, string>();
  for (const group of groups) {
    const existing = latestSnapshotBySchool.get(group.school);
    if (!existing || group.snapshotDate > existing) {
      latestSnapshotBySchool.set(group.school, group.snapshotDate);
    }
  }

  const latestSnapshotTotalErrorInstances = groups.reduce((total, group) => {
    return total + (latestSnapshotBySchool.get(group.school) === group.snapshotDate ? group.count : 0);
  }, 0);

  response.summary = {
    totalGroupedErrors: groups.length,
    totalErrorInstances: latestSnapshotTotalErrorInstances,
    distinctSignatures: signatures.size,
    affectedSchools: affectedSchools.size,
    affectedSisPlatforms: affectedSis.size,
    captureDays: captureDays.size,
    latestSnapshotDate,
  };
  response.trends = serializedTrends;
  response.signatures = serializedSignatures;
  response.schoolBreakdowns = serializedSchoolBreakdowns;
  response.sisBreakdowns = serializedSisBreakdowns;

  return response;
};

const buildErrorDetailTableResponse = async (params: {
  days?: number;
  school?: string;
  sisPlatform?: string;
  latestOnly?: boolean;
  category?: string;
  entityType?: string;
  signature?: string;
  q?: string;
  sortBy?: string;
  sortDir?: 'asc' | 'desc';
  page?: number;
  pageSize?: number;
}): Promise<ErrorDetailTableResponse> => {
  const detailExport = await getStaticErrorDetailExport();
  const rowsWithWindow = await applySnapshotWindow(detailExport.rows, params.days);
  const search = params.q?.trim().toLowerCase();
  const filtered = rowsWithWindow.filter((row) => {
    if (params.school && row.school !== params.school) return false;
    if (params.sisPlatform && row.sisPlatform !== params.sisPlatform) return false;
    if (params.entityType && row.entityType !== params.entityType) return false;
    if (params.signature && row.signatureKey !== params.signature) return false;
    if (params.category) {
      const hint = buildResolutionHint(row.normalizedMessage, row.entityType, row.errorCode);
      if (hint.bucket !== params.category) return false;
    }
    if (search && !normalizeDetailSearch(row).includes(search)) return false;
    return true;
  });

  const resolvedSnapshotDate = params.latestOnly ? getLatestSnapshotDate(filtered) : null;
  const latestFiltered = resolvedSnapshotDate
    ? filtered.filter((row) => row.snapshotDate === resolvedSnapshotDate)
    : filtered;
  const sortedRows = sortDetailRows(latestFiltered, params.sortBy ?? 'snapshotDate', params.sortDir ?? 'desc');
  const page = params.page ?? 1;
  const pageSize = params.pageSize ?? 50;
  const start = (page - 1) * pageSize;
  const rows = sortedRows.slice(start, start + pageSize);

  return {
    rows,
    total: sortedRows.length,
    page,
    pageSize,
    sortBy: params.sortBy ?? 'snapshotDate',
    sortDir: params.sortDir ?? 'desc',
    metadata: {
      appliedFilters: {
        days: params.days ?? null,
        school: params.school ?? null,
        sisPlatform: params.sisPlatform ?? null,
        latestOnly: Boolean(params.latestOnly),
        category: params.category ?? null,
        entityType: params.entityType ?? null,
        signature: params.signature ?? null,
        q: params.q ?? null,
      },
      resolvedSnapshotDate,
    },
  };
};

const makeJsonDownload = <T extends Record<string, unknown>>(payload: T, filename: string) => ({
  blob: new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' }),
  filename,
});

const unsupportedStaticAction = async (...args: unknown[]) => {
  void args;
  throw new Error(STATIC_MODE_ERROR);
};

export const getStaticManifest = getManifest;

export async function getSchools() {
  const latest = await getLatestClientHealth();
  const schools = [...latest.schools]
    .map((school) => ({
      school: school.school,
      displayName: school.displayName,
      products: school.products,
    }))
    .sort((left, right) => left.school.localeCompare(right.school));

  return {
    data: {
      schools,
      excludedSchools: [],
      excludedTerms: [],
    },
  };
}

export const addSchoolExclusion = unsupportedStaticAction;
export const removeSchoolExclusion = unsupportedStaticAction;

export async function getClientHealth() {
  return { data: await getLatestClientHealth() };
}

export const triggerSync = unsupportedStaticAction;
export const getSyncStatus = unsupportedStaticAction;
export const triggerHistoryBackfill = unsupportedStaticAction;
export const resumeHistoryBackfill = unsupportedStaticAction;
export const retryHistoryBackfillFailures = unsupportedStaticAction;

export async function getClientHealthSyncMetadata(params?: { school?: string }) {
  const school = params?.school;
  const suffix = school ? encodeURIComponent(school) : 'all';
  const data = await loadStaticJson<StaticSyncMetadataResponse>(`static-data/client-health/sync-metadata/${suffix}.json`);
  return { data };
}

export async function getSchedulerSettings() {
  return {
    data: {
      syncEnabled: false,
      syncTime: '07:30',
      unavailableReason: STATIC_MODE_ERROR,
    },
  };
}

export const updateSchedulerSettings = unsupportedStaticAction;

export async function getClientHealthHistory(params: { days?: number; school?: string }) {
  const history = await getFullClientHealthHistory();
  const withWindow = await applySnapshotWindow(history.snapshots, params.days);
  const snapshots = params.school
    ? withWindow.filter((snapshot) => snapshot.school === params.school)
    : withWindow;
  return { data: { snapshots } };
}

export async function getErrorAnalysis(params: { days?: number; school?: string; sisPlatform?: string; latestOnly?: boolean }) {
  return { data: await buildErrorAnalysisResponse(params) };
}

export async function downloadErrorAnalysisExport(params: { days?: number; school?: string; sisPlatform?: string }) {
  const summary = await getErrorAnalysis({
    days: params.days,
    school: params.school,
    sisPlatform: params.sisPlatform,
  });
  const manifest = await getManifest();
  const filenameParts = ['error-analysis'];
  if (params.school) filenameParts.push(params.school);
  if (params.sisPlatform) filenameParts.push(params.sisPlatform.toLowerCase().replaceAll(' ', '-'));
  if (typeof params.days === 'number') filenameParts.push(`${params.days}d`);
  filenameParts.push('static.json');

  return makeJsonDownload({
    metadata: {
      historyStartsOn: summary.data.metadata.historyStartsOn,
      lastCapturedAt: summary.data.metadata.lastCapturedAt,
      appliedFilters: {
        days: params.days ?? null,
        school: params.school ?? null,
        sisPlatform: params.sisPlatform ?? null,
        category: null,
        entityType: null,
        signature: null,
        q: null,
        view: 'grouped',
      },
      exportedAt: manifest.exportedAt,
      groupCount: summary.data.summary.totalGroupedErrors,
      totalErrorInstances: summary.data.summary.totalErrorInstances,
    },
    groups: (await getStaticErrorSummaryExport()).groups.filter((group) => {
      if (params.school && group.school !== params.school) return false;
      if (params.sisPlatform && group.sisPlatform !== params.sisPlatform) return false;
      if (typeof params.days === 'number') {
        const referenceDate = manifest.referenceDate ?? manifest.latestSnapshotDate;
        if (referenceDate) {
          const cutoff = buildCutoffDate(referenceDate, params.days);
          if (cutoff && group.snapshotDate < cutoff) return false;
        }
      }
      return true;
    }),
  }, filenameParts.join('-'));
}

export async function getErrorAnalysisErrors(params: {
  days?: number;
  school?: string;
  sisPlatform?: string;
  latestOnly?: boolean;
  category?: string;
  entityType?: string;
  signature?: string;
  q?: string;
  sortBy?: string;
  sortDir?: 'asc' | 'desc';
  page?: number;
  pageSize?: number;
}) {
  return { data: await buildErrorDetailTableResponse(params) };
}

export async function downloadErrorAnalysisDetailedExport(params: {
  days?: number;
  school?: string;
  sisPlatform?: string;
  q?: string;
}) {
  const detailResponse = await buildErrorDetailTableResponse({
    days: params.days,
    school: params.school,
    sisPlatform: params.sisPlatform,
    q: params.q,
    page: 1,
    pageSize: Number.MAX_SAFE_INTEGER,
  });
  const manifest = await getManifest();
  const filenameParts = ['error-analysis', 'all-errors'];
  if (params.school) filenameParts.push(params.school);
  if (params.sisPlatform) filenameParts.push(params.sisPlatform.toLowerCase().replaceAll(' ', '-'));
  if (typeof params.days === 'number') filenameParts.push(`${params.days}d`);
  filenameParts.push('static.json');

  return makeJsonDownload({
    metadata: {
      historyStartsOn: (await getStaticErrorDetailExport()).metadata.historyStartsOn,
      lastCapturedAt: (await getStaticErrorDetailExport()).metadata.lastCapturedAt,
      appliedFilters: {
        days: params.days ?? null,
        school: params.school ?? null,
        sisPlatform: params.sisPlatform ?? null,
        category: null,
        entityType: null,
        signature: null,
        q: params.q ?? null,
        view: 'all-errors',
      },
      exportedAt: manifest.exportedAt,
      rowCount: detailResponse.total,
      totalErrorInstances: detailResponse.total,
    },
    rows: detailResponse.rows,
  }, filenameParts.join('-'));
}

export async function getClientHealthActiveUsers(params: { school?: string }) {
  if (!params.school) {
    throw new Error('school is required');
  }

  const path = `static-data/client-health/active-users/${encodeURIComponent(params.school)}.json`;
  const data = await loadStaticJson<ActiveUsersResponse>(path).catch(async () => {
    const latest = await getLatestClientHealth();
    const school = latest.schools.find((entry) => entry.school === params.school);
    return {
      count: school?.activeUsers24h ?? 0,
      users: [],
      error: 'The static site includes the latest cached count only. Individual active-user lists require the live backend.',
    };
  });

  return { data };
}

export async function getSyncRuns(params?: { limit?: number; offset?: number }) {
  const data = await getStaticSyncRunsExport();
  const limit = params?.limit ?? data.limit ?? 50;
  const offset = params?.offset ?? data.offset ?? 0;
  return {
    data: {
      syncRuns: data.syncRuns.slice(offset, offset + limit),
      totalCount: data.totalCount,
      limit,
      offset,
    },
  };
}

export async function getSyncRun(jobId: string) {
  const data = await getStaticSyncRunsExport();
  const syncRun = data.syncRuns.find((run) => run.jobId === jobId);
  if (!syncRun) {
    const error = new Error(`Static job ${jobId} was not found.`);
    Object.assign(error, { response: { status: 404 } });
    throw error;
  }

  return { data: { syncRun } };
}
