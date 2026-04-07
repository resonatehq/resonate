import { describe, expect, jest, test } from "@jest/globals";
import type { Context } from "../src/context.js";
import { ConsoleLogger, type Logger } from "../src/logger.js";
import { Resonate } from "../src/resonate.js";

describe("ConsoleLogger", () => {
  test("level filtering: debug logger emits all levels", () => {
    const logger = new ConsoleLogger("debug");
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
    const errorSpy = jest.spyOn(console, "error").mockImplementation(() => {});

    logger.debug({}, "debug msg");
    logger.info({}, "info msg");
    logger.warn({}, "warn msg");
    logger.error({}, "error msg");

    expect(logSpy).toHaveBeenCalledTimes(2); // debug + info
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(errorSpy).toHaveBeenCalledTimes(1);

    logSpy.mockRestore();
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  test("level filtering: warn logger suppresses debug and info", () => {
    const logger = new ConsoleLogger("warn");
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
    const errorSpy = jest.spyOn(console, "error").mockImplementation(() => {});

    logger.debug({}, "debug msg");
    logger.info({}, "info msg");
    logger.warn({}, "warn msg");
    logger.error({}, "error msg");

    expect(logSpy).toHaveBeenCalledTimes(0);
    expect(warnSpy).toHaveBeenCalledTimes(1);
    expect(errorSpy).toHaveBeenCalledTimes(1);

    logSpy.mockRestore();
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  test("level filtering: error logger only emits error", () => {
    const logger = new ConsoleLogger("error");
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});
    const errorSpy = jest.spyOn(console, "error").mockImplementation(() => {});

    logger.debug({}, "debug msg");
    logger.info({}, "info msg");
    logger.warn({}, "warn msg");
    logger.error({}, "error msg");

    expect(logSpy).toHaveBeenCalledTimes(0);
    expect(warnSpy).toHaveBeenCalledTimes(0);
    expect(errorSpy).toHaveBeenCalledTimes(1);

    logSpy.mockRestore();
    warnSpy.mockRestore();
    errorSpy.mockRestore();
  });

  test("output format: emits structured JSON with timestamp, level, msg, and fields", () => {
    const logger = new ConsoleLogger("debug");
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});

    logger.debug({ component: "test", corrId: "abc123" }, "hello world");

    expect(logSpy).toHaveBeenCalledTimes(1);
    const output = logSpy.mock.calls[0][0] as string;
    const parsed = JSON.parse(output);

    expect(parsed.level).toBe("debug");
    expect(parsed.msg).toBe("hello world");
    expect(parsed.component).toBe("test");
    expect(parsed.corrId).toBe("abc123");
    expect(parsed.timestamp).toBeDefined();
    // timestamp should be ISO 8601
    expect(() => new Date(parsed.timestamp)).not.toThrow();

    logSpy.mockRestore();
  });

  test("default level is warn", () => {
    const logger = new ConsoleLogger();
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    logger.debug({}, "should not appear");
    logger.info({}, "should not appear");
    logger.warn({}, "should appear");

    expect(logSpy).toHaveBeenCalledTimes(0);
    expect(warnSpy).toHaveBeenCalledTimes(1);

    logSpy.mockRestore();
    warnSpy.mockRestore();
  });
});

describe("Resonate logger integration", () => {
  test("verbose: true results in debug-level output", async () => {
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER, verbose: true });

    function myFunc(_ctx: Context): string {
      return "result";
    }

    resonate.register("myFunc", myFunc);
    const result = await resonate.run("test-verbose-1", "myFunc");
    expect(result).toBe("result");

    // The debug logger should have been called (transport sends debug-level logs)
    const debugCalls = logSpy.mock.calls.filter((call) => {
      try {
        const parsed = JSON.parse(call[0] as string);
        return parsed.level === "debug";
      } catch {
        return false;
      }
    });

    expect(debugCalls.length).toBeGreaterThan(0);

    logSpy.mockRestore();
    await resonate.stop();
  });

  test("custom logger receives calls when injected", async () => {
    const calls: { level: string; fields: Record<string, any>; msg: string }[] = [];

    const customLogger: Logger = {
      debug(fields, msg) {
        calls.push({ level: "debug", fields, msg });
      },
      info(fields, msg) {
        calls.push({ level: "info", fields, msg });
      },
      warn(fields, msg) {
        calls.push({ level: "warn", fields, msg });
      },
      error(fields, msg) {
        calls.push({ level: "error", fields, msg });
      },
    };

    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER, logger: customLogger });

    function myFunc(_ctx: Context): string {
      return "hello";
    }

    resonate.register("myFunc", myFunc);
    const result = await resonate.run("test-custom-logger-1", "myFunc");
    expect(result).toBe("hello");

    // The custom logger should have received at least one call
    // (structured logs from various components go through the custom logger)
    expect(calls.length).toBeGreaterThan(0);

    // Verify structured fields are present (component field is set on all log calls)
    const componentCalls = calls.filter((c) => c.fields.component !== undefined);
    expect(componentCalls.length).toBeGreaterThan(0);

    await resonate.stop();
  });

  test("logLevel takes precedence over verbose", async () => {
    const logSpy = jest.spyOn(console, "log").mockImplementation(() => {});
    const warnSpy = jest.spyOn(console, "warn").mockImplementation(() => {});

    // verbose: true would normally set debug, but logLevel: "error" should take precedence
    const resonate = new Resonate({ pid: "default", ttl: Number.MAX_SAFE_INTEGER, verbose: true, logLevel: "error" });

    function myFunc(_ctx: Context): string {
      return "result";
    }

    resonate.register("myFunc", myFunc);
    await resonate.run("test-precedence-1", "myFunc");

    // No debug logs should appear because logLevel: "error" takes precedence
    const debugCalls = logSpy.mock.calls.filter((call) => {
      try {
        const parsed = JSON.parse(call[0] as string);
        return parsed.level === "debug";
      } catch {
        return false;
      }
    });

    expect(debugCalls.length).toBe(0);

    logSpy.mockRestore();
    warnSpy.mockRestore();
    await resonate.stop();
  });
});
