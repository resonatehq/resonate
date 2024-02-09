import { describe, test, expect } from "@jest/globals";
import { Resonate, Context } from "../../lib/resonate";
import { ResonateTestCrash } from "../../lib/core/error";
import seedrandom from "seedrandom";

import { primes } from "./programs/primes";
import { recurse } from "./programs/recurse";

const tests = [
  { name: "primes", func: primes },
  { name: "recurse", func: recurse },
];

// equivalency algorithm
function isEquivalent(received: Context, expected: Context): boolean {
  const getIdSuffix = (id: string) => id.split("/").slice(2).join("/");

  const otherChildren = expected.children.map((child) => getIdSuffix(child.id));
  const contextChildren = received.children.map((child) => getIdSuffix(child.id));

  if (contextChildren.length === 0) {
    return true;
  }

  if (otherChildren.every((child) => !contextChildren.includes(child))) {
    return false;
  }

  for (const child of received.children) {
    const matchingChild = expected.children.find((otherChild) => getIdSuffix(otherChild.id) === getIdSuffix(child.id));

    if (matchingChild && !isEquivalent(child, matchingChild)) {
      return false;
    }
  }

  return true;
}

// extend jest with custom matcher
// this helps formatting when printing failure messages

declare module "expect" {
  interface Matchers<R> {
    toBeEquivalentTo(expected: Context): R;
  }
}

expect.extend({
  toBeEquivalentTo(received: Context, expected: Context) {
    return {
      pass: isEquivalent(received, expected),
      message: () => `
Expected:
${prettyTree(received)}

Received:
${prettyTree(expected)}`,
    };
  },
});

function prettyTree(tree: Context, indent: string = ""): string {
  let output = "";

  // Append the current node information to the output string
  // output += indent + `id: ${tree.id}, func: ${tree.func}, args: ${JSON.stringify(tree.args)}\n`;
  output += indent + `id: ${tree.id}, func: "", args: []\n`;

  if (tree.children.length > 0) {
    for (let i = 0; i < tree.children.length - 1; i++) {
      output += indent + "├── \n";
      output += prettyTree(tree.children[i], indent + "│   ");
    }
    output += indent + "└── \n";
    output += prettyTree(tree.children[tree.children.length - 1], indent + "    ");
  }

  return output;
}

describe("DST", () => {
  const seed = process.env.SEED || randInt().toString();

  // Loop through all provided programs
  for (const { name, func } of tests) {
    test(`${name}(seed=${seed})`, async () => {
      const generator = seedrandom(seed);
      const test = {
        p: 0.5,
        generator,
      };

      const resonate = new Resonate({ timeout: Number.MAX_SAFE_INTEGER });
      resonate.register("baseline", func);
      resonate.register("test", func, resonate.options({ test }));

      const { context: baselineContext, promise: baselinePromise } = resonate.runWithContext("baseline", "a");
      await baselinePromise;

      const testContext = await runUntilSuccess(resonate, "test", "b");
      expect(testContext).toBeEquivalentTo(baselineContext);
    });
  }
});

async function runUntilSuccess(resonate: Resonate, name: string, id: string, max: number = 1000): Promise<Context> {
  for (let i = 0; i < max; i++) {
    try {
      const { context, promise } = resonate.runWithContext(name, id);
      await promise;

      return context;
    } catch (e) {
      // ResonateTestCrash is a simlated failure and expected,
      // any other error is unexpected
      if (!(e instanceof ResonateTestCrash)) {
        throw e;
      }
    }
  }

  // TODO: run with a 0% chance of failure
  throw new Error("Max iterations reached");
}

// Utils

function randInt(min: number = 0, max: number = Number.MAX_SAFE_INTEGER - 1): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}
