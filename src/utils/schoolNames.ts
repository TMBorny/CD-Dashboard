const SCHOOL_NAME_ALIASES: Record<string, string> = {
  bar01: 'Baruch',
};

const SIS_SUFFIXES = new Set([
  'anthology',
  'banner',
  'colleague',
  'direct',
  'ethos',
  'jenzabar',
  'n2n',
  'peoplesoft',
  'sql',
  'workday',
]);

const TOKEN_ALIASES: Record<string, string> = {
  cc: 'CC',
  cuny: 'CUNY',
  lcad: 'LCAD',
  ncccs: 'NCCCS',
  smu: 'SMU',
  suny: 'SUNY',
  uc: 'UC',
  unc: 'UNC',
};

function toTitleCaseToken(token: string): string {
  const lower = token.toLowerCase();
  if (TOKEN_ALIASES[lower]) return TOKEN_ALIASES[lower];
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

function humanizeSchoolSlug(schoolId: string): string {
  const alias = SCHOOL_NAME_ALIASES[schoolId.toLowerCase()];
  if (alias) return alias;

  const parts = schoolId
    .split(/[_-]+/)
    .filter(Boolean);

  while (parts.length > 1 && SIS_SUFFIXES.has(parts[parts.length - 1].toLowerCase())) {
    parts.pop();
  }

  return parts.map(toTitleCaseToken).join(' ');
}

export function formatSchoolLabel(schoolId: string): string {
  return `${humanizeSchoolSlug(schoolId)} (${schoolId})`;
}
