import { beforeEach, describe, expect, it, vi } from 'vitest';
import { mount, RouterLinkStub } from '@vue/test-utils';

describe('ClientHealthTable', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('renders halted nightly badges and keeps halted merges out of the success numerator', async () => {
    const { default: ClientHealthTable } = await import('./ClientHealthTable.vue');
    const wrapper = mount(ClientHealthTable, {
      props: {
        schools: [
          {
            snapshotDate: '2026-04-12',
            school: 'bar01',
            displayName: 'Baruch College',
            sisPlatform: 'Banner',
            products: [],
            merges: {
              nightly: { total: 2, succeeded: 1, failed: 0, finishedWithIssues: 0, noData: 0, halted: 1, mergeTimeMs: 0 },
              realtime: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
              manual: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
            },
            recentFailedMerges: [],
            mergeErrorsCount: 0,
            activeUsers24h: 0,
            createdAt: new Date('2026-04-12T00:00:00Z'),
          },
        ],
      },
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          Badge: {
            template: '<span><slot /></span>',
          },
        },
      },
    });

    expect(wrapper.text()).toContain('1 Halted');
    expect(wrapper.text()).toContain('Merge Halts');
    expect(wrapper.text()).toContain('Threshold hit');
    expect(wrapper.text()).toContain('50%');
  });

  it('exports the currently visible rows as a csv', async () => {
    const originalCreateObjectURL = URL.createObjectURL;
    const originalRevokeObjectURL = URL.revokeObjectURL;
    const OriginalBlob = globalThis.Blob;
    class FakeBlob {
      parts: unknown[];
      type: string;

      constructor(parts: unknown[], options?: BlobPropertyBag) {
        this.parts = parts;
        this.type = options?.type ?? '';
      }

      text() {
        return Promise.resolve(this.parts.join(''));
      }
    }

    globalThis.Blob = FakeBlob as unknown as typeof Blob;
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      writable: true,
      value: vi.fn(),
    });
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      writable: true,
      value: vi.fn(),
    });

    const createObjectURL = vi.mocked(URL.createObjectURL).mockReturnValue('blob:client-health');
    const revokeObjectURL = vi.mocked(URL.revokeObjectURL).mockImplementation(() => {});
    const clickSpy = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {});
    let exportedBlob: FakeBlob | undefined;
    createObjectURL.mockImplementation((blob) => {
      exportedBlob = blob as unknown as FakeBlob;
      return 'blob:client-health';
    });

    const { default: ClientHealthTable } = await import('./ClientHealthTable.vue');
    const wrapper = mount(ClientHealthTable, {
      props: {
        schools: [
          {
            snapshotDate: '2026-04-12',
            school: 'bar01',
            displayName: 'Baruch College',
            sisPlatform: 'Banner',
            products: ['Catalog'],
            merges: {
              nightly: { total: 2, succeeded: 1, failed: 0, finishedWithIssues: 0, noData: 0, halted: 1, mergeTimeMs: 0 },
              realtime: { total: 3, succeeded: 3, failed: 0, finishedWithIssues: 0, noData: 0 },
              manual: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
            },
            recentFailedMerges: [],
            mergeErrorsCount: 2,
            activeUsers24h: 8,
            createdAt: new Date('2026-04-12T00:00:00Z'),
          },
          {
            snapshotDate: '2026-04-12',
            school: 'foo99',
            displayName: 'Foo University',
            sisPlatform: 'Colleague',
            products: ['Curriculum'],
            merges: {
              nightly: { total: 5, succeeded: 5, failed: 0, finishedWithIssues: 0, noData: 0, halted: 0, mergeTimeMs: 3600000 },
              realtime: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
              manual: { total: 0, succeeded: 0, failed: 0, finishedWithIssues: 0, noData: 0 },
            },
            recentFailedMerges: [],
            mergeErrorsCount: 0,
            activeUsers24h: 25,
            createdAt: new Date('2026-04-12T00:00:00Z'),
          },
        ],
      },
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
          Badge: {
            template: '<span><slot /></span>',
          },
        },
      },
    });

    await wrapper.get('input[placeholder="Filter by school or ID…"]').setValue('bar');
    await wrapper.get('button[aria-label="Export visible client table data as CSV"]').trigger('click');

    expect(clickSpy).toHaveBeenCalledTimes(1);
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:client-health');
    expect(exportedBlob?.type).toBe('text/csv;charset=utf-8;');

    const csv = await exportedBlob!.text();
    expect(csv).toContain('schoolId,schoolName,sisPlatform,status,healthScore');
    expect(csv).toContain('bar01,Baruch College (CUNY) (bar01),Banner');
    expect(csv).toContain('Catalog');
    expect(csv).not.toContain('foo99');

    globalThis.Blob = OriginalBlob;
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      writable: true,
      value: originalCreateObjectURL,
    });
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      writable: true,
      value: originalRevokeObjectURL,
    });
  });
});
