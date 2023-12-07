/* eslint-env node */
module.exports = {
  extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended'],
  parser: '@typescript-eslint/parser',
  plugins: ['@typescript-eslint'],
  root: true,
  "rules": {
    "@typescript-eslint/no-explicit-any": "off",
    "@typescript-eslint/no-extra-semi": "error",
    "@typescript-eslint/semi": ["error", "never"]
  },
  "ignorePatterns": ["jest.config.js", "dist/**/*.js", "dist/**/*.d.ts"],
}
