import { Constant } from "../src/retries";
import { Exponential } from "../src/retries";
import { Linear } from "../src/retries";
import { Never } from "../src/retries";

describe("RetryPolicy delay progression", () => {
  const cases: [Never | Constant | Linear | Exponential, (number | null)[] | null][] = [
    [new Never(), null],
    [new Constant({ delay: 1000, maxRetries: 2 }), [1000, 1000, null]],
    [new Linear({ delay: 1000, maxRetries: 2 }), [1000, 2000, null]],
    [new Exponential({ delay: 1000, factor: 2, maxRetries: 5, maxDelay: 8000 }), [2000, 4000, 8000, 8000, 8000, null]],
  ];

  test.each(cases)("policy %p progression", (policy, progression) => {
    if (policy instanceof Never) {
      // Never only returns 0 on attempt 0, so no progression
      return;
    }

    let i = 1;
    const delays: (number | null)[] = [];

    while (true) {
      const nextDelay = policy.next(i);
      delays.push(nextDelay);
      i += 1;

      if (nextDelay === null) {
        break;
      }
    }

    expect(delays).toEqual(progression);
  });
});
