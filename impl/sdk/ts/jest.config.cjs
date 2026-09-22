module.exports = {
  collectCoverage: true,
  coverageDirectory: "coverage",
  preset: "ts-jest/presets/default-esm",
  testEnvironment: "node",
  testPathIgnorePatterns: ["dist/"],
  forceExit: true,
  extensionsToTreatAsEsm: [".ts"],
  moduleNameMapper: {
    "^(\\.{1,2}/.*)\\.js$": "$1",
  },
  transform: {
    "^.+\\.ts$": ["ts-jest", { 
      useESM: true,
      tsconfig: {
        module: "ESNext",
        moduleResolution: "Bundler",
        allowImportingTsExtensions: false,
        esModuleInterop: true
      }
    }],
  },
};
