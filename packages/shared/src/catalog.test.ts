import { describe, expect, it } from 'vitest';
import { catalog } from './catalog.ts';
import { render } from './template.ts';

describe('catalog', () => {
  for (const [type, route] of Object.entries(catalog)) {
    it(`${type}: sample is valid and fully renders`, () => {
      const data = route.schema.parse(route.sample);
      expect(route.recipients(data).length).toBeGreaterThan(0);
      for (const tpl of [route.template.title, route.template.body]) {
        expect(tpl).toMatch(/\{\{/);
        // Every placeholder must resolve to a non-empty value for the sample payload.
        for (const [, path] of tpl.matchAll(/\{\{\s*([\w.]+)\s*\}\}/g)) {
          expect(render(`{{${path}}}`, data), `${type} ${path}`).not.toBe('');
        }
      }
    });
  }

  it('dedupes mention recipients', () => {
    const r = catalog['comment.mentioned']!;
    expect(
      r.recipients(r.schema.parse({ userIds: ['a', 'a', 'b'], author: 'x', snippet: 'y' })),
    ).toEqual(['a', 'b']);
  });
});
