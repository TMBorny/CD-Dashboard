import { describe, expect, it } from 'vitest';
import router from './index';

describe('router', () => {
  it('registers the error analysis admin route', () => {
    const route = router.getRoutes().find((entry) => entry.name === 'AdminErrorAnalysis');

    expect(route?.path).toBe('/admin/error-analysis');
  });
});
