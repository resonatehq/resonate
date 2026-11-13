// eventsource >= 5 (and its eventsource-parser dependency) ship ESM only. Jest
// runs these tests as CommonJS, and on Node < 24.9 it cannot require() an ES
// module, so those two packages are compiled to CommonJS by ts-jest as well.
const esmOnlyDeps = ["eventsource", "eventsource-parser"];

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
  transformIgnorePatterns: [`/node_modules/(?!(${esmOnlyDeps.join("|")})/)`],
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
    [`/node_modules/(${esmOnlyDeps.join("|")})/.+\\.js$`]: ["ts-jest", {
      useESM: true,
      tsconfig: {
        allowJs: true,
        module: "ESNext",
        moduleResolution: "Bundler",
      }
    }],
  },
};
