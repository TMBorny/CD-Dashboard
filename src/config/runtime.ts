const normalizeDataMode = (value: string | undefined): 'live' | 'static' => {
  return value?.trim().toLowerCase() === 'static' ? 'static' : 'live';
};

const ensureLeadingSlash = (value: string) => (value.startsWith('/') ? value : `/${value}`);

const ensureTrailingSlash = (value: string) => (value.endsWith('/') ? value : `${value}/`);

const normalizeBasePath = (value: string | undefined) => {
  const fallback = '/';
  if (!value || !value.trim()) {
    return fallback;
  }

  const val = value.trim();
  if (val === './' || val === '.') {
    return './';
  }

  return ensureTrailingSlash(ensureLeadingSlash(val));
};

export const dataMode = normalizeDataMode(import.meta.env.VITE_DATA_MODE);
export const isStaticDataMode = dataMode === 'static';
export const siteBase = normalizeBasePath(import.meta.env.BASE_URL || import.meta.env.VITE_SITE_BASE);

export const resolveSitePath = (relativePath: string) => {
  const normalizedPath = relativePath.replace(/^\/+/, '');
  return `${siteBase}${normalizedPath}`;
};
