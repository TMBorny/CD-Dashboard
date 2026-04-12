import { describe, expect, it } from 'vitest';
import { formatSchoolLabel } from './schoolNames';

describe('formatSchoolLabel', () => {
  it('uses explicit aliases for known shorthand school ids', () => {
    expect(formatSchoolLabel('bar01')).toBe('Baruch College (CUNY) (bar01)');
  });

  it('strips SIS suffixes from school ids', () => {
    expect(formatSchoolLabel('duke_peoplesoft')).toBe('Duke (duke_peoplesoft)');
  });

  it('humanizes plain school ids', () => {
    expect(formatSchoolLabel('stanford')).toBe('Stanford University (stanford)');
  });
});
