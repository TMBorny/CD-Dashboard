const getValidDate = (value?: string | Date | null) => {
  if (!value) return null;

  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

export const getLocalTimeZoneLabel = () =>
  Intl.DateTimeFormat().resolvedOptions().timeZone || 'your local timezone';

export const formatLocalDateTime = (
  value?: string | Date | null,
  fallback = 'Unknown',
) => {
  const date = getValidDate(value);
  if (!date) return fallback;

  return new Intl.DateTimeFormat(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    timeZoneName: 'short',
  }).format(date);
};
