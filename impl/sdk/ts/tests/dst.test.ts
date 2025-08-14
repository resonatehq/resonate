import { run } from "../sim/main";

describe("run dst", () => {
  test("should execute simulation and not ", () => {
    expect(() =>
      run({
        seed: Math.floor(Math.random() * 1000000),
        steps: 1000,
      }),
    ).not.toThrow();
  });
});
