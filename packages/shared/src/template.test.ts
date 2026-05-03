import { describe, expect, it } from 'vitest';
import { render } from './template.ts';

describe('render', () => {
  it('substitutes flat and nested variables', () => {
    expect(
      render('Order {{ orderId }} via {{carrier.name}}', {
        orderId: 'A1',
        carrier: { name: 'UPS' },
      }),
    ).toBe('Order A1 via UPS');
  });

  it('renders missing variables as empty and leaves other text intact', () => {
    expect(render('Hi {{name}}! {{a.b.c}}', {})).toBe('Hi ! ');
  });

  it('stringifies numbers', () => {
    expect(render('{{amount}} {{currency}}', { amount: 12.5, currency: 'EUR' })).toBe('12.5 EUR');
  });
});
