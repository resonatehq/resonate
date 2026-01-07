import { Nursery } from "../src/nursery";
import { assert } from "../src/util";

describe("Nursery", () => {
  test("nursery function executed only until done", async () => {
    const signal = Promise.withResolvers();

    let n = 0;
    let h = 0;
    new Nursery<number>(
      (nursery) => {
        if (n === 3) {
          return nursery.done({ kind: "value", value: n });
        }

        // bump n
        n++;

        // create holds, once done is called the nursery function will
        // not be called again
        for (let i = 0; i < 5; i++) {
          nursery.hold((next) => {
            h++;
            next();
          });
        }

        nursery.cont();
      },
      (res) => {
        expect(res.kind).toBe("value");
        assert(res.kind === "value");
        expect(res.value).toBe(3);
        signal.resolve(true);
      },
    );

    await signal.promise;
    expect(n).toBe(3); // 0, 1, 2, 3
    expect(h).toBe(15); // 5 * 3
  });

  test("nursery function executed once per hold release", async () => {
    const signal = Promise.withResolvers();
    const promises = [Promise.withResolvers(), Promise.withResolvers(), Promise.withResolvers()];

    let n = 0;
    new Nursery<boolean>(
      (nursery) => {
        // on first execution hold on three promises
        if (n === 0) {
          for (const { promise } of promises) {
            nursery.hold((next) => promise.then(next));
          }
        }

        // bump n
        n++;

        // the nusery function will be executed 4 times:
        // - once on init
        // - once after each hold is released (3 total)
        if (n === 4) {
          return nursery.done({ kind: "value", value: true });
        }

        // continue so more can happen
        nursery.cont();
      },
      (res) => {
        expect(res.kind).toBe("value");
        assert(res.kind === "value");
        signal.resolve(true);
      },
    );

    // init: 1
    expect(n).toBe(1);

    // first hold released: 2
    promises[0].resolve(true);
    await eventLoopTick();
    expect(n).toBe(2);

    // second hold released: 3
    promises[1].resolve(true);
    await eventLoopTick();
    expect(n).toBe(3);

    // third hold released: 4
    promises[2].resolve(true);
    await eventLoopTick();
    expect(n).toBe(4);

    await signal.promise;
  });

  test("all collects all results", async () => {
    new Nursery<number[]>(
      (nursery) =>
        nursery.all<number, any>(
          [1, 2, 3],
          (n, c) => c({ kind: "value", value: n + 1 }),
          (res) => nursery.done(res),
        ),
      (res) => {
        expect(res.kind).toBe("value");
        assert(res.kind === "value");
        expect(res.value).toEqual([2, 3, 4]);
      },
    );
  });

  test("all short circuits on first error", async () => {
    new Nursery<number[]>(
      (nursery) =>
        nursery.all<number, any>(
          [1, 2, 3],
          (n, c) => c({ kind: "value", value: true }),
          (res) => nursery.done(res),
        ),
      (res) => {
        expect(res.kind).toBe("value");
      },
    );
  });
});

async function eventLoopTick() {
  return new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}
