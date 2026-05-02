import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    projects: [
      {
        test: { name: 'unit', include: ['packages/*/src/**/*.test.ts', 'apps/*/src/**/*.test.ts'] },
      },
      {
        test: {
          name: 'integration',
          include: ['tests/integration/**/*.test.ts'],
          testTimeout: 60_000,
          hookTimeout: 60_000,
          // Integration tests share one Redis/Postgres; run files serially.
          fileParallelism: false,
        },
      },
    ],
  },
});
