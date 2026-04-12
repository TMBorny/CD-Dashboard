/**
 * Derives the best possible human-readable school name from the school slug.
 *
 * The Coursedog admin API does not expose a displayName field for schools —
 * the school slug is the only identifier available. We strip well-known SIS/
 * integration suffixes and apply a curated set of aliases and acronym rules
 * to produce a clean label.
 */

// ------------------------------------------------------------------
// Full-slug overrides: exact matches take priority over all other logic
// ------------------------------------------------------------------
const SCHOOL_NAME_ALIASES: Record<string, string> = {
  // CUNY
  bar01: 'Baruch College (CUNY)',
  bcc01: 'Bronx Community College (CUNY)',
  bkl01: 'Brooklyn College (CUNY)',
  csi01: 'College of Staten Island (CUNY)',
  cty01: 'City College (CUNY)',
  grd01: 'The Graduate Center (CUNY)',
  hos01: 'Hostos Community College (CUNY)',
  htc01: 'Hunter College (CUNY)',
  jjc01: 'John Jay College (CUNY)',
  kcc01: 'Kingsborough Community College (CUNY)',
  lag01: 'LaGuardia Community College (CUNY)',
  leh01: 'Lehman College (CUNY)',
  med01: 'CUNY School of Medicine',
  mec01: 'Medgar Evers College (CUNY)',
  ncc01: 'New York City College of Technology (CUNY)',
  nyt01: 'New York City Tech (CUNY)',
  qcc01: 'Queensborough Community College (CUNY)',
  qns01: 'Queens College (CUNY)',
  slu01: 'St. Louis University',
  soj01: 'School of Journalism (CUNY)',
  sph01: 'School of Public Health (CUNY)',
  sps01: 'School of Professional Studies (CUNY)',
  yrk01: 'York College (CUNY)',
  // Well-known standalones
  stanford: 'Stanford University',
  pomona: 'Pomona College',
  rutgers: 'Rutgers University',
  wagner: 'Wagner College',
  wilkes: 'Wilkes University',
  winthrop: 'Winthrop University',
  weber: 'Weber State University',
  westcliff: 'Westcliff University',
  trocaire: 'Trocaire College',
  tacomacc: 'Tacoma Community College',
  swc: 'Southwestern College',
  sbccd: 'San Bernardino CCD',
  scsu: 'Southern Connecticut State University',
  ucsb: 'UC Santa Barbara',
  ucsc: 'UC Santa Cruz',
  ucci: 'UC Irvine',
  umsi: 'University of Michigan School of Information',
  osu: 'Oregon State University',
  sou: 'Southern Oregon University',
  ozarks: 'University of the Ozarks',
  parker: 'Parker University',
  richmond: 'University of Richmond',
  risd: 'Rhode Island School of Design',
  olin_washu_workday: 'Olin Business School (WashU)',
  pointu: 'Point University',
  pointloma_workday: 'Point Loma Nazarene University',
  // Misc short codes
  jcu: 'John Carroll University',
  lcad: 'Laguna College of Art & Design',
  stu: 'St. Thomas University',
  tctc: 'Tri-County Technical College',
  cazc: 'Central Arizona College',
  lccc: 'Lorain County Community College',
};

// ------------------------------------------------------------------
// Token-level overrides: applied after slug splitting and SIS stripping
// ------------------------------------------------------------------
const TOKEN_ALIASES: Record<string, string> = {
  // Acronyms that should be all-caps
  cc: 'CC',
  ccd: 'CCD',
  cuny: 'CUNY',
  jcc: 'JCC',
  lcad: 'LCAD',
  ncccs: 'NCCCS',
  rmu: 'RMU',
  rmcad: 'RMCAD',
  saic: 'SAIC',
  sbccd: 'SBCCD',
  smu: 'SMU',
  sou: 'SOU',
  suny: 'SUNY',
  tcu: 'TCU',
  uc: 'UC',
  ucr: 'UCR',
  ucc: 'UCC',
  unc: 'UNC',
  ufl: 'UFL',
  uah: 'UAH',
  ucw: 'UCW',
  usu: 'USU',
  vuu: 'VUU',
  wfu: 'WFU',
  wne: 'WNE',
  wsu: 'WSU',
  // Compound tokens
  'stmarys': "St. Mary's",
  'stthomas': 'St. Thomas',
  'stjohns': "St. John's",
  'southernmethodist': 'Southern Methodist',
  'thenewschool': 'The New School',
  'ucberkeley': 'UC Berkeley',
  'ucalgary': 'University of Calgary',
  'ucla': 'UCLA',
  'ufv': 'UFV',
  'uagm': 'UAGM',
  'udst': 'UDST',
  'umgc': 'UMGC',
  'ummc': 'UMMC',
  'unthsc': 'UNTHSC',
  'uwi': 'UWI',
  'uwrf': 'UWRF',
  'uwsp': 'UWSP',
  'udm': 'University of Detroit Mercy',
  'aamu': 'Alabama A&M University',
  'acu': 'Abilene Christian University',
  'alcorn': 'Alcorn State University',
  'asub': 'Arkansas State University-Beebe',
  'bhcc': 'Bunker Hill Community College',
  'ccsj': 'Calumet College of St. Joseph',
  'csustan': 'CSU Stanislaus',
  'csusm': 'CSU San Marcos',
  'cvcc': 'Central Virginia Community College',
  'dsu': 'Delaware State University',
  'ecok': 'East Central University',
  'famu': 'Florida A&M University',
  'fpu': 'Fresno Pacific University',
  'fscj': 'Florida State College Jacksonville',
  'gu': 'Gonzaga University',
  'hsus': 'Hardin-Simmons University',
  'jsu': 'Jacksonville State University',
  'kwu': 'Kansas Wesleyan University',
  'mhu': 'Mars Hill University',
  'mga': 'Middle Georgia State University',
  'ndsu': 'North Dakota State University',
  'njcu': 'New Jersey City University',
  'nmt': 'New Mexico Tech',
  'nscc': 'North Shore Community College',
  'oxy': 'Occidental College',
  'pfw': 'Purdue Fort Wayne',
  'ptc': 'Piedmont Technical College',
  'rcbc': 'Rowan College at Burlington County',
  'rcsj': 'Rowan College of South Jersey',
  'rdpolytech': 'Red Deer Polytechnic',
  'rsu': 'Rogers State University',
  'sbts': 'Southern Baptist Theological Seminary',
  'scsc': 'Southern Connecticut State University',
  'scuhs': 'Southern California University of Health Sciences',
  'sebts': 'Southeastern Baptist Theological Seminary',
  'senmc': 'Southeast New Mexico College',
  'seu': 'Southeastern University',
  'siu': 'Southern Illinois University',
  'siue': 'Southern Illinois University Edwardsville',
  'sju': "Saint Joseph's University",
  'smcvt': "Saint Michael's College",
  'svu': 'Southern Virginia University',
  'swlaw': 'Southwestern Law School',
  'tccd': 'Tarrant County College District',
  'trentu': 'Trent University',
  'tru': 'Thompson Rivers University',
  'ttu': 'Texas Tech University',
  'tvcc': 'Trinity Valley Community College',
  'uclawsf': 'UC Law SF',
  'unf': 'University of North Florida',
  'upike': 'University of Pikeville',
  'uscga': 'US Coast Guard Academy',
  'utulsa': 'University of Tulsa',
  'waynecc': 'Wayne Community College',
  'wctc': 'Waukesha County Technical College',
  'wcu': 'West Coast University',
  'wnc': 'Western Nevada College',
  'wsc': 'Wayne State College',
  'wsutech': 'WSU Tech',
};

// Suffixes to strip from slug (SIS platform / integration identifiers)
const SIS_SUFFIXES = new Set([
  'anthology',
  'banner',
  'campusnexus',
  'campus',
  'nexus',
  'colleague',
  'csv',
  'direct',
  'ethos',
  'homegrown',
  'jenzabar',
  'n2n',
  'peoplesoft',
  'powercampus',
  'powercampus',
  'sql',
  'workday',
]);

function toTitleCaseToken(token: string): string {
  const lower = token.toLowerCase();
  if (TOKEN_ALIASES[lower]) return TOKEN_ALIASES[lower];
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

function humanizeSchoolSlug(schoolId: string): string {
  const alias = SCHOOL_NAME_ALIASES[schoolId.toLowerCase()];
  if (alias) return alias;

  const parts = schoolId.split(/[_-]+/).filter(Boolean);

  // Strip trailing SIS suffixes (keep stripping until we hit a non-suffix)
  while (parts.length > 1 && SIS_SUFFIXES.has(parts[parts.length - 1].toLowerCase())) {
    parts.pop();
  }

  return parts.map(toTitleCaseToken).join(' ');
}

export function formatSchoolLabel(schoolId: string, displayName?: string | null): string {
  const name = humanizeSchoolSlug(schoolId);

  // If the Coursedog API provides a displayName but it's just a short acronym (like "Fscj") 
  // while our local heuristic expanded it to a full name via aliases ("Florida State..."), we prefer our heuristic.
  if (displayName && displayName !== schoolId && displayName.length >= name.length) {
    return `${displayName} (${schoolId})`;
  }

  // If the name would just be the slug in title case with no improvement, show it with the id
  // Otherwise show "Name (slug)" so the slug is still searchable
  return `${name} (${schoolId})`;
}
