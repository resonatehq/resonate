import { describe, test, expect } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { ResonateTestCrash } from "../lib/core/error";
import seedrandom from "seedrandom";

// trace tree
type TraceTree = {
  id: string;
  func: string;
  args: any[];
  children: TraceTree[];
};

function traces(ctx: Context): TraceTree {
  return {
    id: ctx.id,
    func: "",
    args: [],
    children: ctx.children.map(traces),
  };
}

async function test1(ctx: Context) {
  await ctx.run(foo);
  await ctx.run(foo);
}

async function foo(ctx: Context) {
  await ctx.run(bar);
  await ctx.run(bar);
}

async function bar(ctx: Context) {}

// prime number generator
async function test2(ctx: Context) {
  await ctx.run(prime, 2);
  await ctx.run(prime, 3);
  await ctx.run(prime, 5);
  await ctx.run(prime, 7);
}

async function prime(ctx: Context, n: number) {
  for (let i = 2; i < n; i++) {
    if (n % i === 0) {
      return false;
    }
  }
  return true;
}

function isSubsetTree(context: Context, other: Context): boolean {
  const getIdSuffix = (id: string) => id.split("/").slice(2).join("/");

  const otherChildren = other.children.map((child) => getIdSuffix(child.id));
  const contextChildren = context.children.map((child) => getIdSuffix(child.id));

  if (contextChildren.length === 0) {
    return true;
  }

  if (otherChildren.every((child) => !contextChildren.includes(child))) {
    return false;
  }

  for (const child of context.children) {
    const matchingChild = other.children.find((otherChild) => getIdSuffix(otherChild.id) === getIdSuffix(child.id));

    if (matchingChild && !isSubsetTree(child, matchingChild)) {
      return false;
    }
  }

  return true;
}

function printTree(tree: TraceTree, indent: string = ""): void {
  // use something from jest to print the tree
  console.log(indent + `id: ${tree.id}, func: ${tree.func}, args: ${JSON.stringify(tree.args)}`);

  if (tree.children.length > 0) {
    for (let i = 0; i < tree.children.length - 1; i++) {
      console.log(indent + "├── ");
      printTree(tree.children[i], indent + "│   ");
    }
    console.log(indent + "└── ");
    printTree(tree.children[tree.children.length - 1], indent + "    ");
  }
}

describe("Simulate failures", () => {
  const seed = process.env.SEED || Math.random().toString();
  for (const func of [test1, test2]) {
    test("Simulate failure 1", async () => {
      const resonate = new Resonate();
      resonate.register("baseline", func);
      resonate.register("withFailure", func, resonate.options({ test: { p: 0.5, generator: seedrandom(seed) } }));

      const { context: context1, promise: promise1 } = resonate.runWithContext("baseline", "baseline");

      await promise1;

      const tree1 = traces(context1);
      printTree(tree1);

      let context2: Context | undefined = undefined;
      while (!context2) {
        // Create a new resonate instance with opts
        try {
          const currentResult = resonate.runWithContext("withFailure", "withFailure");
          const currentContext = currentResult.context;
          const currentPromise = currentResult.promise;

          await currentPromise;
          // Break the loop if the current context was successful
          const tree2 = traces(currentContext);
          printTree(tree2);

          context2 = currentContext;
          break;
        } catch (e) {
          if (e !== ResonateTestCrash) {
            console.log("Failed to run test, trying again! ", e);
          }
        }
      }
      expect(isSubsetTree(context2, context1)).toBe(true);
    });
  }
});
