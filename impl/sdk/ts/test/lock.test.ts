import { jest, describe, test, expect } from "@jest/globals";
import { Resonate, Context } from "../lib/resonate";
import { LocalStore } from "../lib/core/stores/local";

// Set a larger timeout for hooks (e.g., 10 seconds)
jest.setTimeout(10000);

const sharedResource: string[] = [];

function write(context: Context, id: string, final: boolean) {
  return new Promise((resolve) => {
    sharedResource.push(id);

    if (final) {
      resolve(sharedResource);
    }
  });
}

describe("Lock", () => {
  const store = new LocalStore();
  const r1 = new Resonate({ store });
  const r2 = new Resonate({ store });

  r1.register("write", write, r1.options({ eid: "a", timeout: 5000 }));
  r2.register("write", write, r2.options({ eid: "b", timeout: 5000 }));

  test("Lock guards shared resource", async () => {
    r1.run("write", "id", "a", false);
    const p2 = r2.run("write", "id", "b", true);

    while (sharedResource.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(sharedResource.length).toBe(1);

    // release lock so p2 can run
    await store.locks.release("write/id", sharedResource[0]);

    const r = await p2;

    expect(sharedResource.length).toBe(2);
    expect(sharedResource).toContain("a");
    expect(sharedResource).toContain("b");
    expect(sharedResource).toEqual(r);
  });
});
