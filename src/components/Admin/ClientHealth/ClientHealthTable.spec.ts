import { describe, expect, it } from 'vitest';
import { mount, RouterLinkStub } from '@vue/test-utils';

describe('ClientHealthTable', () => {
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
});
