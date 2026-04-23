import type { ApexOptions } from 'apexcharts';

function formatLegendLabel(seriesName: string): string {
  if (seriesName === 'Finished With Issues' || seriesName === 'Finished w/ Issues') {
    return 'Issues';
  }

  return seriesName;
}

function formatChartValue(value: number): string {
  if (Number.isInteger(value)) {
    return String(value);
  }

  return value.toFixed(1).replace(/\.0$/, '');
}

function usesDateCategories(categories: string[]): boolean {
  return categories.length > 0 && categories.every((category) => /^\d{4}-\d{2}-\d{2}$/.test(category));
}

function getAdaptiveTickAmount(categoryCount: number): number | undefined {
  if (categoryCount <= 1) return categoryCount || undefined;
  if (categoryCount <= 7) return categoryCount;
  if (categoryCount <= 21) return 7;
  if (categoryCount <= 90) return 8;
  return 10;
}

function buildXAxis(categories: string[]): ApexOptions['xaxis'] {
  const isDateAxis = usesDateCategories(categories);

  return {
    categories,
    type: isDateAxis ? 'datetime' : 'category',
    tickAmount: isDateAxis ? getAdaptiveTickAmount(categories.length) : undefined,
    axisBorder: { show: false },
    axisTicks: { show: false },
    labels: {
      style: { colors: '#64748b', fontSize: '12px' },
      hideOverlappingLabels: true,
      showDuplicates: false,
      datetimeUTC: false,
      trim: !isDateAxis,
      rotate: isDateAxis ? 0 : -45,
    },
  };
}

export function useChartOptions(opts: { categories?: string[]; colors?: string[] } = {}): ApexOptions {
  const categories = opts.categories || [];

  return {
    chart: {
      type: 'line',
      toolbar: {
        show: true,
        tools: {
          download: true,
          selection: true,
          zoom: true,
          zoomin: true,
          zoomout: true,
          pan: true,
          reset: true,
        },
      },
      zoom: {
        enabled: true,
        type: 'x',
        autoScaleYaxis: true,
      },
      fontFamily: 'Inter, ui-sans-serif, system-ui',
    },
    markers: {
      size: 4,
      strokeWidth: 0,
      hover: {
        size: 6,
      },
    },
    stroke: {
      curve: 'smooth',
      width: 2,
    },
    colors: opts.colors || ['#4b8bbf'],
    fill: {
      type: 'gradient',
      gradient: {
        shadeIntensity: 1,
        opacityFrom: 0.45,
        opacityTo: 0.05,
        stops: [20, 100, 100, 100],
      },
    },
    xaxis: buildXAxis(categories),
    yaxis: {
      labels: {
        style: { colors: '#64748b', fontSize: '12px' },
        formatter: (value: number) => formatChartValue(value),
      },
    },
    grid: {
      borderColor: '#e2e8f0',
      strokeDashArray: 4,
    },
    tooltip: {
      theme: 'light',
      x: { format: 'MMM d, yyyy' },
      y: {
        formatter: (value: number | undefined) =>
          typeof value === 'number' ? formatChartValue(value) : '',
      },
    },
    legend: {
      position: 'top',
      horizontalAlign: 'left',
      formatter: formatLegendLabel,
      itemMargin: {
        horizontal: 10,
        vertical: 6,
      },
      labels: { colors: '#334155' },
    },
    noData: {
      text: 'No history available yet',
      align: 'center',
      verticalAlign: 'middle',
      style: {
        color: '#64748b',
        fontSize: '14px',
      },
    },
  };
}

export function useStackedBarChartOptions(opts: { categories?: string[]; colors?: string[] } = {}): ApexOptions {
  const categories = opts.categories || [];

  return {
    chart: {
      type: 'bar',
      stacked: true,
      toolbar: {
        show: true,
        tools: {
          download: true,
          selection: true,
          zoom: true,
          zoomin: true,
          zoomout: true,
          pan: true,
          reset: true,
        },
      },
      zoom: {
        enabled: true,
        type: 'x',
        autoScaleYaxis: true,
      },
      fontFamily: 'Inter, ui-sans-serif, system-ui',
    },
    colors: opts.colors || ['#10b981', '#f59e0b', '#64748b', '#ef4444'], // Success, Issues, No Data, Failed by default
    plotOptions: {
      bar: {
        horizontal: false,
        columnWidth: '50%',
      },
    },
    xaxis: buildXAxis(categories),
    yaxis: {
      labels: {
        style: { colors: '#64748b', fontSize: '12px' },
        formatter: (value: number) => formatChartValue(value),
      },
    },
    grid: {
      borderColor: '#e2e8f0',
      strokeDashArray: 4,
    },
    tooltip: {
      theme: 'light',
      shared: true,
      intersect: false,
      x: { format: 'MMM d, yyyy' },
      y: {
        formatter: (value: number | undefined) =>
          typeof value === 'number' ? formatChartValue(value) : '',
      },
    },
    legend: {
      position: 'top',
      horizontalAlign: 'left',
      formatter: formatLegendLabel,
      itemMargin: {
        horizontal: 10,
        vertical: 6,
      },
      labels: { colors: '#334155' },
    },
    dataLabels: {
      enabled: false,
    },
    noData: {
      text: 'No history available yet',
      align: 'center',
      verticalAlign: 'middle',
      style: {
        color: '#64748b',
        fontSize: '14px',
      },
    },
  };
}
