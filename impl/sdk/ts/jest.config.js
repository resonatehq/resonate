/** @type {import('ts-jest').JestConfigWithTsJest} */
export default {
  collectCoverage: true,
  coverageDirectory: "coverage",
  preset: "ts-jest",
  testEnvironment: "node",
  testPathIgnorePatterns: ["dist/"],
};
