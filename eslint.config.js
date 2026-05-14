import js from '@eslint/js';
import tseslint from 'typescript-eslint';
import globals from 'globals';

export default tseslint.config(
  { ignores: ['**/node_modules', '**/dist', 'loadtest/results'] },
  js.configs.recommended,
  ...tseslint.configs.strict,
  {
    languageOptions: { globals: { ...globals.node, ...globals.browser } },
    rules: {
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_' }],
      // noUncheckedIndexedAccess makes `!` the explicit, reviewed escape hatch for known-present keys.
      '@typescript-eslint/no-non-null-assertion': 'off',
    },
  },
);
