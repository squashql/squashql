module.exports = {
  preset: 'ts-jest',
  testMatch: [
    "<rootDir>/src/__tests__/**/*.test.ts"
  ],
  testPathIgnorePatterns: [
    "<rootDir>/dist"
  ],
  transform: {
    "^.+\\.(ts|tsx)?$": "ts-jest"
  },
};
