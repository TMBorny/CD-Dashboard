import type { ApexOptions } from 'apexcharts';

function formatChartValue(value: number): string {
  if (Number.isInteger(value)) {
    return String(value);
  }

  return value.toFixed(1).replace(/\.0$/, '');
}

export function useChartOptions(opts: { categories?: string[]; colors?: string[] } = {}): ApexOptions {
  return {
    chart: {
      type: 'line',
      toolbar: { show: true },
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
    xaxis: {
      categories: opts.categories || [],
      axisBorder: { show: false },
      axisTicks: { show: false },
      labels: { style: { colors: '#64748b', fontSize: '12px' } },
    },
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
      x: { format: 'yyyy-MM-dd' },
      y: {
        formatter: (value: number | undefined) =>
          typeof value === 'number' ? formatChartValue(value) : '',
      },
    },
    legend: {
      position: 'top',
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
  return {
    chart: {
      type: 'bar',
      stacked: true,
      toolbar: { show: true },
      fontFamily: 'Inter, ui-sans-serif, system-ui',
    },
    colors: opts.colors || ['#10b981', '#f59e0b', '#64748b', '#ef4444'], // Success, Issues, No Data, Failed by default
    plotOptions: {
      bar: {
        horizontal: false,
        columnWidth: '50%',
      },
    },
    xaxis: {
      categories: opts.categories || [],
      axisBorder: { show: false },
      axisTicks: { show: false },
      labels: { style: { colors: '#64748b', fontSize: '12px' } },
    },
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
      y: {
        formatter: (value: number | undefined) =>
          typeof value === 'number' ? formatChartValue(value) : '',
      },
    },
    legend: {
      position: 'top',
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
