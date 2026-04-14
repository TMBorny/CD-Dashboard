import { describe, it, expect } from 'vitest';
import { useChartOptions } from './useChartOptions';

describe('useChartOptions', () => {
  it('returns default options', () => {
    const options = useChartOptions();
    expect(options.colors).toEqual(['#4b8bbf']);
    expect(options.xaxis?.categories).toEqual([]);
    expect(options.chart?.type).toBe('line');
    expect(options.chart?.zoom?.enabled).toBe(true);
  });

  it('accepts options overrides', () => {
    const options = useChartOptions({
      colors: ['#000000'],
      categories: ['Jan', 'Feb'],
    });
    expect(options.colors).toEqual(['#000000']);
    expect(options.xaxis?.categories).toEqual(['Jan', 'Feb']);
  });

  it('uses adaptive datetime axes for ISO date categories', () => {
    const options = useChartOptions({
      categories: ['2026-01-01', '2026-02-01', '2026-03-01', '2026-04-01'],
    });

    expect(options.xaxis?.type).toBe('datetime');
    expect(options.xaxis?.tickAmount).toBe(4);

    const labels = Array.isArray(options.xaxis) ? options.xaxis[0]?.labels : options.xaxis?.labels;
    expect(labels?.rotate).toBe(0);
    expect(labels?.showDuplicates).toBe(false);
  });

  it('formats axis labels without long floating point tails', () => {
    const options = useChartOptions();
    const yaxis = Array.isArray(options.yaxis) ? options.yaxis[0] : options.yaxis;
    const formatter = yaxis?.labels?.formatter;
    expect(formatter?.(100, { seriesIndex: 0, dataPointIndex: 0, w: {} as any })).toBe('100');
    expect(formatter?.(94.25, { seriesIndex: 0, dataPointIndex: 0, w: {} as any })).toBe('94.3');
  });
});
