import { Promises } from "../src/promises";

describe("State Transition Tests", () => {
  test("Test Case 0: transitions from Init to Pending via Create", async () => {
    const promises = new Promises();
    const promise = await promises.create("id0", Number.MAX_SAFE_INTEGER, {});

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id0");
  });

  test("Test Case 1: transitions from Init to Init via Resolve", async () => {
    const promises = new Promises();
    await expect(promises.resolve("id1", {})).rejects.toThrow();
  });

  test("Test Case 2: transitions from Init to Init via Reject", async () => {
    const promises = new Promises();
    await expect(promises.reject("id2", {})).rejects.toThrow();
  });

  test("Test Case 3: transitions from Init to Init via Cancel", async () => {
    const promises = new Promises();
    await expect(promises.cancel("id3", {})).rejects.toThrow();
  });

  test("Test Case 4: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id4", Number.MAX_SAFE_INTEGER, {});

    const promise = await promises.create("id4", Number.MAX_SAFE_INTEGER, {});

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id4");
  });

  test("Test Case 5: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id5", Number.MAX_SAFE_INTEGER, {});

    const promise = await promises.resolve("id5", {});

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id5");
  });

  test("Test Case 6: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id6", Number.MAX_SAFE_INTEGER, {});

    const promise = await promises.reject("id6", {});

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id6");
  });

  test("Test Case 7: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id7", Number.MAX_SAFE_INTEGER, {});

    const promise = await promises.cancel("id7", {});

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id7");
  });

  test("Test Case 8: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id8", Number.MAX_SAFE_INTEGER, {});
    await promises.resolve("id8", {});

    const promise = await promises.create("id8", Number.MAX_SAFE_INTEGER, {});

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id8");
  });

  test("Test Case 9: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id9", Number.MAX_SAFE_INTEGER, {});
    await promises.resolve("id9", {});

    const promise = await promises.resolve("id9", {});

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id9");
  });

  test("Test Case 10: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id10", Number.MAX_SAFE_INTEGER, {});
    await promises.resolve("id10", {});

    const promise = await promises.reject("id10", {});

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id10");
  });

  test("Test Case 11: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id11", Number.MAX_SAFE_INTEGER, {});
    await promises.resolve("id11", {});

    const promise = await promises.cancel("id11", {});

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id11");
  });

  test("Test Case 12: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id12", Number.MAX_SAFE_INTEGER, {});
    await promises.reject("id12", {});

    const promise = await promises.create("id12", Number.MAX_SAFE_INTEGER, {});

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id12");
  });

  test("Test Case 13: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id13", Number.MAX_SAFE_INTEGER, {});
    await promises.reject("id13", {});

    const promise = await promises.resolve("id13", {});

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id13");
  });

  test("Test Case 14: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id14", Number.MAX_SAFE_INTEGER, {});
    await promises.reject("id14", {});

    const promise = await promises.reject("id14", {});

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id14");
  });

  test("Test Case 15: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id15", Number.MAX_SAFE_INTEGER, {});
    await promises.reject("id15", {});

    const promise = await promises.cancel("id15", {});

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id15");
  });

  test("Test Case 16: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id16", Number.MAX_SAFE_INTEGER, {});
    await promises.cancel("id16", {});

    const promise = await promises.create("id16", Number.MAX_SAFE_INTEGER, {});

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id16");
  });

  test("Test Case 17: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id17", Number.MAX_SAFE_INTEGER, {});
    await promises.cancel("id17", {});

    const promise = await promises.resolve("id17", {});

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id17");
  });

  test("Test Case 18: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id18", Number.MAX_SAFE_INTEGER, {});
    await promises.cancel("id18", {});

    const promise = await promises.reject("id18", {});

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id18");
  });

  test("Test Case 19: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id19", Number.MAX_SAFE_INTEGER, {});
    await promises.cancel("id19", {});

    const promise = await promises.cancel("id19", {});

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id19");
  });

  test("Test Case 20: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id20", 0, {});

    const promise = await promises.create("id20", Number.MAX_SAFE_INTEGER, {});

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id20");
  });

  test("Test Case 21: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id21", 0, {});

    const promise = await promises.resolve("id21", {});

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id21");
  });

  test("Test Case 22: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id22", 0, {});

    const promise = await promises.reject("id22", {});

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id22");
  });

  test("Test Case 23: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id23", 0, {});

    const promise = await promises.cancel("id23", {});

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id23");
  });
});
