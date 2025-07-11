import { Promises } from "../src/promises";
describe("State Transition Tests", () => {
  test("Test Case 0: transitions from Init to Pending via Create", async () => {
    const promises = new Promises();
    const promise = await promises.create("id0", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {});

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id0");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 1: transitions from Init to Pending via Create", async () => {
    const promises = new Promises();
    const promise = await promises.create("id1", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id1");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 2: transitions from Init to Pending via Create", async () => {
    const promises = new Promises();
    const promise = await promises.create("id2", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {});

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id2");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 3: transitions from Init to Pending via Create", async () => {
    const promises = new Promises();
    const promise = await promises.create("id3", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id3");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 4: transitions from Init to Init via Resolve", async () => {
    const promises = new Promises();
    await expect(promises.resolve("id4", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 5: transitions from Init to Init via Resolve", async () => {
    const promises = new Promises();
    await expect(promises.resolve("id5", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 6: transitions from Init to Init via Resolve", async () => {
    const promises = new Promises();
    await expect(promises.resolve("id6", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 7: transitions from Init to Init via Resolve", async () => {
    const promises = new Promises();
    await expect(promises.resolve("id7", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 8: transitions from Init to Init via Reject", async () => {
    const promises = new Promises();
    await expect(promises.reject("id8", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 9: transitions from Init to Init via Reject", async () => {
    const promises = new Promises();
    await expect(promises.reject("id9", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 10: transitions from Init to Init via Reject", async () => {
    const promises = new Promises();
    await expect(promises.reject("id10", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 11: transitions from Init to Init via Reject", async () => {
    const promises = new Promises();
    await expect(promises.reject("id11", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 12: transitions from Init to Init via Cancel", async () => {
    const promises = new Promises();
    await expect(promises.cancel("id12", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 13: transitions from Init to Init via Cancel", async () => {
    const promises = new Promises();
    await expect(promises.cancel("id13", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 14: transitions from Init to Init via Cancel", async () => {
    const promises = new Promises();
    await expect(promises.cancel("id14", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 15: transitions from Init to Init via Cancel", async () => {
    const promises = new Promises();
    await expect(promises.cancel("id15", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 16: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id16", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    await expect(promises.create("id16", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 17: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id17", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    await expect(promises.create("id17", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 18: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id18", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    await expect(promises.create("id18", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 19: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id19", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    await expect(promises.create("id19", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 20: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id20", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.resolve("id20", undefined, true, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id20");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 21: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id21", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.resolve("id21", undefined, false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id21");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 22: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id22", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.resolve("id22", "iku", true, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id22");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 23: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id23", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.resolve("id23", "iku", false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id23");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 24: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id24", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.reject("id24", undefined, true, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id24");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 25: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id25", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.reject("id25", undefined, false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id25");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 26: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id26", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.reject("id26", "iku", true, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id26");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 27: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id27", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.reject("id27", "iku", false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id27");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 28: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id28", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.cancel("id28", undefined, true, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id28");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 29: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id29", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.cancel("id29", undefined, false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id29");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 30: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id30", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.cancel("id30", "iku", true, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id30");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 31: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id31", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});

    const promise = await promises.cancel("id31", "iku", false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id31");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 32: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id32", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    await expect(promises.create("id32", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 33: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id33", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    await expect(promises.create("id33", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 34: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id34", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.create("id34", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {});

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id34");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 35: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id35", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.create("id35", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id35");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 36: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id36", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    await expect(promises.create("id36", Number.MAX_SAFE_INTEGER, "ikc*", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 37: transitions from Pending to Pending via Create", async () => {
    const promises = new Promises();
    await promises.create("id37", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    await expect(promises.create("id37", Number.MAX_SAFE_INTEGER, "ikc*", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 38: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id38", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.resolve("id38", undefined, true, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id38");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 39: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id39", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.resolve("id39", undefined, false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id39");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 40: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id40", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.resolve("id40", "iku", true, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id40");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 41: transitions from Pending to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id41", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.resolve("id41", "iku", false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id41");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 42: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id42", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.reject("id42", undefined, true, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id42");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 43: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id43", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.reject("id43", undefined, false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id43");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 44: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id44", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.reject("id44", "iku", true, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id44");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 45: transitions from Pending to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id45", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.reject("id45", "iku", false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id45");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 46: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id46", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.cancel("id46", undefined, true, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id46");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 47: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id47", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.cancel("id47", undefined, false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id47");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 48: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id48", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.cancel("id48", "iku", true, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id48");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 49: transitions from Pending to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id49", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    const promise = await promises.cancel("id49", "iku", false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id49");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 50: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id50", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id50", undefined, false, undefined);

    await expect(promises.create("id50", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 51: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id51", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id51", undefined, false, undefined);

    await expect(promises.create("id51", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 52: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id52", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id52", undefined, false, undefined);

    await expect(promises.create("id52", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 53: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id53", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id53", undefined, false, undefined);

    await expect(promises.create("id53", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 54: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id54", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id54", undefined, false, undefined);

    await expect(promises.resolve("id54", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 55: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id55", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id55", undefined, false, undefined);

    await expect(promises.resolve("id55", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 56: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id56", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id56", undefined, false, undefined);

    await expect(promises.resolve("id56", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 57: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id57", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id57", undefined, false, undefined);

    await expect(promises.resolve("id57", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 58: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id58", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id58", undefined, false, undefined);

    await expect(promises.reject("id58", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 59: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id59", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id59", undefined, false, undefined);

    await expect(promises.reject("id59", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 60: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id60", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id60", undefined, false, undefined);

    await expect(promises.reject("id60", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 61: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id61", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id61", undefined, false, undefined);

    await expect(promises.reject("id61", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 62: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id62", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id62", undefined, false, undefined);

    await expect(promises.cancel("id62", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 63: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id63", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id63", undefined, false, undefined);

    await expect(promises.cancel("id63", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 64: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id64", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id64", undefined, false, undefined);

    await expect(promises.cancel("id64", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 65: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id65", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id65", undefined, false, undefined);

    await expect(promises.cancel("id65", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 66: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id66", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id66", "iku", false, undefined);

    await expect(promises.create("id66", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 67: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id67", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id67", "iku", false, undefined);

    await expect(promises.create("id67", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 68: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id68", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id68", "iku", false, undefined);

    await expect(promises.create("id68", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 69: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id69", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id69", "iku", false, undefined);

    await expect(promises.create("id69", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 70: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id70", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id70", "iku", false, undefined);

    await expect(promises.resolve("id70", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 71: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id71", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id71", "iku", false, undefined);

    await expect(promises.resolve("id71", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 72: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id72", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id72", "iku", false, undefined);

    const promise = await promises.resolve("id72", "iku", true, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id72");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 73: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id73", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id73", "iku", false, undefined);

    const promise = await promises.resolve("id73", "iku", false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id73");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 74: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id74", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id74", "iku", false, undefined);

    await expect(promises.resolve("id74", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 75: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id75", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id75", "iku", false, undefined);

    await expect(promises.resolve("id75", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 76: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id76", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id76", "iku", false, undefined);

    await expect(promises.reject("id76", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 77: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id77", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id77", "iku", false, undefined);

    await expect(promises.reject("id77", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 78: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id78", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id78", "iku", false, undefined);

    await expect(promises.reject("id78", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 79: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id79", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id79", "iku", false, undefined);

    const promise = await promises.reject("id79", "iku", false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id79");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 80: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id80", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id80", "iku", false, undefined);

    await expect(promises.reject("id80", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 81: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id81", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id81", "iku", false, undefined);

    await expect(promises.reject("id81", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 82: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id82", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id82", "iku", false, undefined);

    await expect(promises.cancel("id82", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 83: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id83", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id83", "iku", false, undefined);

    await expect(promises.cancel("id83", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 84: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id84", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id84", "iku", false, undefined);

    await expect(promises.cancel("id84", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 85: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id85", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id85", "iku", false, undefined);

    const promise = await promises.cancel("id85", "iku", false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id85");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 86: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id86", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id86", "iku", false, undefined);

    await expect(promises.cancel("id86", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 87: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id87", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.resolve("id87", "iku", false, undefined);

    await expect(promises.cancel("id87", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 88: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id88", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id88", undefined, false, undefined);

    await expect(promises.create("id88", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 89: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id89", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id89", undefined, false, undefined);

    await expect(promises.create("id89", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 90: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id90", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id90", undefined, false, undefined);

    await expect(promises.create("id90", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 91: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id91", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id91", undefined, false, undefined);

    const promise = await promises.create("id91", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id91");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 92: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id92", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id92", undefined, false, undefined);

    await expect(promises.create("id92", Number.MAX_SAFE_INTEGER, "ikc*", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 93: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id93", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id93", undefined, false, undefined);

    await expect(promises.create("id93", Number.MAX_SAFE_INTEGER, "ikc*", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 94: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id94", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id94", undefined, false, undefined);

    await expect(promises.resolve("id94", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 95: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id95", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id95", undefined, false, undefined);

    await expect(promises.resolve("id95", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 96: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id96", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id96", undefined, false, undefined);

    await expect(promises.resolve("id96", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 97: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id97", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id97", undefined, false, undefined);

    await expect(promises.resolve("id97", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 98: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id98", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id98", undefined, false, undefined);

    await expect(promises.reject("id98", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 99: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id99", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id99", undefined, false, undefined);

    await expect(promises.reject("id99", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 100: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id100", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id100", undefined, false, undefined);

    await expect(promises.reject("id100", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 101: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id101", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id101", undefined, false, undefined);

    await expect(promises.reject("id101", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 102: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id102", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id102", undefined, false, undefined);

    await expect(promises.cancel("id102", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 103: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id103", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id103", undefined, false, undefined);

    await expect(promises.cancel("id103", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 104: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id104", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id104", undefined, false, undefined);

    await expect(promises.cancel("id104", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 105: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id105", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id105", undefined, false, undefined);

    await expect(promises.cancel("id105", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 106: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id106", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id106", "iku", false, undefined);

    await expect(promises.create("id106", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 107: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id107", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id107", "iku", false, undefined);

    await expect(promises.create("id107", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 108: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id108", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id108", "iku", false, undefined);

    await expect(promises.create("id108", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 109: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id109", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id109", "iku", false, undefined);

    const promise = await promises.create("id109", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id109");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 110: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id110", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id110", "iku", false, undefined);

    await expect(promises.create("id110", Number.MAX_SAFE_INTEGER, "ikc*", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 111: transitions from Resolved to Resolved via Create", async () => {
    const promises = new Promises();
    await promises.create("id111", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id111", "iku", false, undefined);

    await expect(promises.create("id111", Number.MAX_SAFE_INTEGER, "ikc*", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 112: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id112", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id112", "iku", false, undefined);

    await expect(promises.resolve("id112", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 113: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id113", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id113", "iku", false, undefined);

    await expect(promises.resolve("id113", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 114: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id114", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id114", "iku", false, undefined);

    const promise = await promises.resolve("id114", "iku", true, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id114");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 115: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id115", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id115", "iku", false, undefined);

    const promise = await promises.resolve("id115", "iku", false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id115");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 116: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id116", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id116", "iku", false, undefined);

    await expect(promises.resolve("id116", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 117: transitions from Resolved to Resolved via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id117", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id117", "iku", false, undefined);

    await expect(promises.resolve("id117", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 118: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id118", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id118", "iku", false, undefined);

    await expect(promises.reject("id118", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 119: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id119", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id119", "iku", false, undefined);

    await expect(promises.reject("id119", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 120: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id120", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id120", "iku", false, undefined);

    await expect(promises.reject("id120", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 121: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id121", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id121", "iku", false, undefined);

    const promise = await promises.reject("id121", "iku", false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id121");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 122: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id122", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id122", "iku", false, undefined);

    await expect(promises.reject("id122", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 123: transitions from Resolved to Resolved via Reject", async () => {
    const promises = new Promises();
    await promises.create("id123", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id123", "iku", false, undefined);

    await expect(promises.reject("id123", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 124: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id124", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id124", "iku", false, undefined);

    await expect(promises.cancel("id124", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 125: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id125", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id125", "iku", false, undefined);

    await expect(promises.cancel("id125", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 126: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id126", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id126", "iku", false, undefined);

    await expect(promises.cancel("id126", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 127: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id127", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id127", "iku", false, undefined);

    const promise = await promises.cancel("id127", "iku", false, undefined);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id127");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 128: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id128", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id128", "iku", false, undefined);

    await expect(promises.cancel("id128", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 129: transitions from Resolved to Resolved via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id129", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.resolve("id129", "iku", false, undefined);

    await expect(promises.cancel("id129", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 130: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id130", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id130", undefined, false, undefined);

    await expect(promises.create("id130", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 131: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id131", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id131", undefined, false, undefined);

    await expect(promises.create("id131", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 132: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id132", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id132", undefined, false, undefined);

    await expect(promises.create("id132", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 133: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id133", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id133", undefined, false, undefined);

    await expect(promises.create("id133", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 134: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id134", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id134", undefined, false, undefined);

    await expect(promises.resolve("id134", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 135: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id135", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id135", undefined, false, undefined);

    await expect(promises.resolve("id135", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 136: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id136", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id136", undefined, false, undefined);

    await expect(promises.resolve("id136", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 137: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id137", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id137", undefined, false, undefined);

    await expect(promises.resolve("id137", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 138: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id138", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id138", undefined, false, undefined);

    await expect(promises.reject("id138", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 139: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id139", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id139", undefined, false, undefined);

    await expect(promises.reject("id139", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 140: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id140", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id140", undefined, false, undefined);

    await expect(promises.reject("id140", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 141: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id141", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id141", undefined, false, undefined);

    await expect(promises.reject("id141", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 142: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id142", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id142", undefined, false, undefined);

    await expect(promises.cancel("id142", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 143: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id143", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id143", undefined, false, undefined);

    await expect(promises.cancel("id143", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 144: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id144", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id144", undefined, false, undefined);

    await expect(promises.cancel("id144", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 145: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id145", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id145", undefined, false, undefined);

    await expect(promises.cancel("id145", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 146: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id146", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id146", "iku", false, undefined);

    await expect(promises.create("id146", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 147: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id147", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id147", "iku", false, undefined);

    await expect(promises.create("id147", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 148: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id148", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id148", "iku", false, undefined);

    await expect(promises.create("id148", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 149: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id149", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id149", "iku", false, undefined);

    await expect(promises.create("id149", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 150: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id150", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id150", "iku", false, undefined);

    await expect(promises.resolve("id150", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 151: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id151", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id151", "iku", false, undefined);

    await expect(promises.resolve("id151", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 152: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id152", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id152", "iku", false, undefined);

    await expect(promises.resolve("id152", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 153: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id153", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id153", "iku", false, undefined);

    const promise = await promises.resolve("id153", "iku", false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id153");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 154: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id154", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id154", "iku", false, undefined);

    await expect(promises.resolve("id154", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 155: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id155", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id155", "iku", false, undefined);

    await expect(promises.resolve("id155", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 156: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id156", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id156", "iku", false, undefined);

    await expect(promises.reject("id156", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 157: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id157", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id157", "iku", false, undefined);

    await expect(promises.reject("id157", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 158: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id158", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id158", "iku", false, undefined);

    const promise = await promises.reject("id158", "iku", true, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id158");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 159: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id159", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id159", "iku", false, undefined);

    const promise = await promises.reject("id159", "iku", false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id159");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 160: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id160", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id160", "iku", false, undefined);

    await expect(promises.reject("id160", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 161: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id161", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id161", "iku", false, undefined);

    await expect(promises.reject("id161", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 162: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id162", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id162", "iku", false, undefined);

    await expect(promises.cancel("id162", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 163: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id163", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id163", "iku", false, undefined);

    await expect(promises.cancel("id163", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 164: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id164", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id164", "iku", false, undefined);

    await expect(promises.cancel("id164", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 165: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id165", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id165", "iku", false, undefined);

    const promise = await promises.cancel("id165", "iku", false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id165");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 166: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id166", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id166", "iku", false, undefined);

    await expect(promises.cancel("id166", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 167: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id167", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.reject("id167", "iku", false, undefined);

    await expect(promises.cancel("id167", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 168: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id168", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id168", undefined, false, undefined);

    await expect(promises.create("id168", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 169: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id169", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id169", undefined, false, undefined);

    await expect(promises.create("id169", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 170: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id170", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id170", undefined, false, undefined);

    await expect(promises.create("id170", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 171: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id171", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id171", undefined, false, undefined);

    const promise = await promises.create("id171", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id171");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 172: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id172", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id172", undefined, false, undefined);

    await expect(promises.create("id172", Number.MAX_SAFE_INTEGER, "ikc*", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 173: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id173", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id173", undefined, false, undefined);

    await expect(promises.create("id173", Number.MAX_SAFE_INTEGER, "ikc*", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 174: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id174", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id174", undefined, false, undefined);

    await expect(promises.resolve("id174", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 175: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id175", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id175", undefined, false, undefined);

    await expect(promises.resolve("id175", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 176: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id176", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id176", undefined, false, undefined);

    await expect(promises.resolve("id176", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 177: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id177", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id177", undefined, false, undefined);

    await expect(promises.resolve("id177", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 178: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id178", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id178", undefined, false, undefined);

    await expect(promises.reject("id178", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 179: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id179", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id179", undefined, false, undefined);

    await expect(promises.reject("id179", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 180: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id180", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id180", undefined, false, undefined);

    await expect(promises.reject("id180", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 181: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id181", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id181", undefined, false, undefined);

    await expect(promises.reject("id181", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 182: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id182", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id182", undefined, false, undefined);

    await expect(promises.cancel("id182", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 183: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id183", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id183", undefined, false, undefined);

    await expect(promises.cancel("id183", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 184: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id184", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id184", undefined, false, undefined);

    await expect(promises.cancel("id184", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 185: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id185", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id185", undefined, false, undefined);

    await expect(promises.cancel("id185", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 186: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id186", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id186", "iku", false, undefined);

    await expect(promises.create("id186", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 187: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id187", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id187", "iku", false, undefined);

    await expect(promises.create("id187", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 188: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id188", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id188", "iku", false, undefined);

    await expect(promises.create("id188", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 189: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id189", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id189", "iku", false, undefined);

    const promise = await promises.create("id189", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id189");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 190: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id190", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id190", "iku", false, undefined);

    await expect(promises.create("id190", Number.MAX_SAFE_INTEGER, "ikc*", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 191: transitions from Rejected to Rejected via Create", async () => {
    const promises = new Promises();
    await promises.create("id191", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id191", "iku", false, undefined);

    await expect(promises.create("id191", Number.MAX_SAFE_INTEGER, "ikc*", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 192: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id192", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id192", "iku", false, undefined);

    await expect(promises.resolve("id192", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 193: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id193", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id193", "iku", false, undefined);

    await expect(promises.resolve("id193", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 194: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id194", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id194", "iku", false, undefined);

    await expect(promises.resolve("id194", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 195: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id195", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id195", "iku", false, undefined);

    const promise = await promises.resolve("id195", "iku", false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id195");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 196: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id196", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id196", "iku", false, undefined);

    await expect(promises.resolve("id196", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 197: transitions from Rejected to Rejected via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id197", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id197", "iku", false, undefined);

    await expect(promises.resolve("id197", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 198: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id198", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id198", "iku", false, undefined);

    await expect(promises.reject("id198", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 199: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id199", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id199", "iku", false, undefined);

    await expect(promises.reject("id199", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 200: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id200", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id200", "iku", false, undefined);

    const promise = await promises.reject("id200", "iku", true, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id200");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 201: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id201", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id201", "iku", false, undefined);

    const promise = await promises.reject("id201", "iku", false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id201");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 202: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id202", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id202", "iku", false, undefined);

    await expect(promises.reject("id202", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 203: transitions from Rejected to Rejected via Reject", async () => {
    const promises = new Promises();
    await promises.create("id203", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id203", "iku", false, undefined);

    await expect(promises.reject("id203", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 204: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id204", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id204", "iku", false, undefined);

    await expect(promises.cancel("id204", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 205: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id205", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id205", "iku", false, undefined);

    await expect(promises.cancel("id205", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 206: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id206", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id206", "iku", false, undefined);

    await expect(promises.cancel("id206", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 207: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id207", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id207", "iku", false, undefined);

    const promise = await promises.cancel("id207", "iku", false, undefined);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id207");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 208: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id208", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id208", "iku", false, undefined);

    await expect(promises.cancel("id208", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 209: transitions from Rejected to Rejected via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id209", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.reject("id209", "iku", false, undefined);

    await expect(promises.cancel("id209", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 210: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id210", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id210", undefined, false, undefined);

    await expect(promises.create("id210", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 211: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id211", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id211", undefined, false, undefined);

    await expect(promises.create("id211", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 212: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id212", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id212", undefined, false, undefined);

    await expect(promises.create("id212", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 213: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id213", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id213", undefined, false, undefined);

    await expect(promises.create("id213", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 214: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id214", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id214", undefined, false, undefined);

    await expect(promises.resolve("id214", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 215: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id215", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id215", undefined, false, undefined);

    await expect(promises.resolve("id215", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 216: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id216", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id216", undefined, false, undefined);

    await expect(promises.resolve("id216", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 217: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id217", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id217", undefined, false, undefined);

    await expect(promises.resolve("id217", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 218: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id218", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id218", undefined, false, undefined);

    await expect(promises.reject("id218", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 219: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id219", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id219", undefined, false, undefined);

    await expect(promises.reject("id219", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 220: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id220", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id220", undefined, false, undefined);

    await expect(promises.reject("id220", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 221: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id221", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id221", undefined, false, undefined);

    await expect(promises.reject("id221", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 222: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id222", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id222", undefined, false, undefined);

    await expect(promises.cancel("id222", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 223: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id223", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id223", undefined, false, undefined);

    await expect(promises.cancel("id223", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 224: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id224", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id224", undefined, false, undefined);

    await expect(promises.cancel("id224", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 225: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id225", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id225", undefined, false, undefined);

    await expect(promises.cancel("id225", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 226: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id226", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id226", "iku", false, undefined);

    await expect(promises.create("id226", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 227: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id227", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id227", "iku", false, undefined);

    await expect(promises.create("id227", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 228: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id228", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id228", "iku", false, undefined);

    await expect(promises.create("id228", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 229: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id229", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id229", "iku", false, undefined);

    await expect(promises.create("id229", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 230: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id230", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id230", "iku", false, undefined);

    await expect(promises.resolve("id230", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 231: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id231", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id231", "iku", false, undefined);

    await expect(promises.resolve("id231", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 232: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id232", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id232", "iku", false, undefined);

    await expect(promises.resolve("id232", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 233: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id233", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id233", "iku", false, undefined);

    const promise = await promises.resolve("id233", "iku", false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id233");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 234: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id234", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id234", "iku", false, undefined);

    await expect(promises.resolve("id234", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 235: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id235", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id235", "iku", false, undefined);

    await expect(promises.resolve("id235", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 236: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id236", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id236", "iku", false, undefined);

    await expect(promises.reject("id236", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 237: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id237", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id237", "iku", false, undefined);

    await expect(promises.reject("id237", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 238: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id238", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id238", "iku", false, undefined);

    await expect(promises.reject("id238", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 239: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id239", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id239", "iku", false, undefined);

    const promise = await promises.reject("id239", "iku", false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id239");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 240: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id240", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id240", "iku", false, undefined);

    await expect(promises.reject("id240", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 241: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id241", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id241", "iku", false, undefined);

    await expect(promises.reject("id241", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 242: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id242", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id242", "iku", false, undefined);

    await expect(promises.cancel("id242", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 243: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id243", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id243", "iku", false, undefined);

    await expect(promises.cancel("id243", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 244: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id244", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id244", "iku", false, undefined);

    const promise = await promises.cancel("id244", "iku", true, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id244");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 245: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id245", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id245", "iku", false, undefined);

    const promise = await promises.cancel("id245", "iku", false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id245");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 246: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id246", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id246", "iku", false, undefined);

    await expect(promises.cancel("id246", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 247: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id247", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {});
    await promises.cancel("id247", "iku", false, undefined);

    await expect(promises.cancel("id247", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 248: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id248", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id248", undefined, false, undefined);

    await expect(promises.create("id248", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 249: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id249", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id249", undefined, false, undefined);

    await expect(promises.create("id249", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 250: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id250", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id250", undefined, false, undefined);

    await expect(promises.create("id250", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 251: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id251", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id251", undefined, false, undefined);

    const promise = await promises.create("id251", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id251");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 252: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id252", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id252", undefined, false, undefined);

    await expect(promises.create("id252", Number.MAX_SAFE_INTEGER, "ikc*", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 253: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id253", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id253", undefined, false, undefined);

    await expect(promises.create("id253", Number.MAX_SAFE_INTEGER, "ikc*", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 254: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id254", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id254", undefined, false, undefined);

    await expect(promises.resolve("id254", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 255: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id255", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id255", undefined, false, undefined);

    await expect(promises.resolve("id255", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 256: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id256", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id256", undefined, false, undefined);

    await expect(promises.resolve("id256", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 257: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id257", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id257", undefined, false, undefined);

    await expect(promises.resolve("id257", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 258: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id258", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id258", undefined, false, undefined);

    await expect(promises.reject("id258", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 259: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id259", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id259", undefined, false, undefined);

    await expect(promises.reject("id259", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 260: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id260", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id260", undefined, false, undefined);

    await expect(promises.reject("id260", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 261: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id261", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id261", undefined, false, undefined);

    await expect(promises.reject("id261", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 262: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id262", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id262", undefined, false, undefined);

    await expect(promises.cancel("id262", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 263: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id263", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id263", undefined, false, undefined);

    await expect(promises.cancel("id263", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 264: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id264", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id264", undefined, false, undefined);

    await expect(promises.cancel("id264", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 265: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id265", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id265", undefined, false, undefined);

    await expect(promises.cancel("id265", "iku", false, undefined)).rejects.toThrow();
  });

  test("Test Case 266: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id266", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id266", "iku", false, undefined);

    await expect(promises.create("id266", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 267: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id267", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id267", "iku", false, undefined);

    await expect(promises.create("id267", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 268: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id268", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id268", "iku", false, undefined);

    await expect(promises.create("id268", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 269: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id269", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id269", "iku", false, undefined);

    const promise = await promises.create("id269", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id269");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 270: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id270", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id270", "iku", false, undefined);

    await expect(promises.create("id270", Number.MAX_SAFE_INTEGER, "ikc*", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 271: transitions from Canceled to Canceled via Create", async () => {
    const promises = new Promises();
    await promises.create("id271", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id271", "iku", false, undefined);

    await expect(promises.create("id271", Number.MAX_SAFE_INTEGER, "ikc*", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 272: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id272", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id272", "iku", false, undefined);

    await expect(promises.resolve("id272", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 273: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id273", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id273", "iku", false, undefined);

    await expect(promises.resolve("id273", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 274: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id274", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id274", "iku", false, undefined);

    await expect(promises.resolve("id274", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 275: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id275", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id275", "iku", false, undefined);

    const promise = await promises.resolve("id275", "iku", false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id275");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 276: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id276", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id276", "iku", false, undefined);

    await expect(promises.resolve("id276", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 277: transitions from Canceled to Canceled via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id277", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id277", "iku", false, undefined);

    await expect(promises.resolve("id277", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 278: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id278", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id278", "iku", false, undefined);

    await expect(promises.reject("id278", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 279: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id279", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id279", "iku", false, undefined);

    await expect(promises.reject("id279", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 280: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id280", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id280", "iku", false, undefined);

    await expect(promises.reject("id280", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 281: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id281", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id281", "iku", false, undefined);

    const promise = await promises.reject("id281", "iku", false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id281");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 282: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id282", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id282", "iku", false, undefined);

    await expect(promises.reject("id282", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 283: transitions from Canceled to Canceled via Reject", async () => {
    const promises = new Promises();
    await promises.create("id283", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id283", "iku", false, undefined);

    await expect(promises.reject("id283", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 284: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id284", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id284", "iku", false, undefined);

    await expect(promises.cancel("id284", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 285: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id285", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id285", "iku", false, undefined);

    await expect(promises.cancel("id285", undefined, false, undefined)).rejects.toThrow();
  });

  test("Test Case 286: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id286", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id286", "iku", false, undefined);

    const promise = await promises.cancel("id286", "iku", true, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id286");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 287: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id287", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id287", "iku", false, undefined);

    const promise = await promises.cancel("id287", "iku", false, undefined);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id287");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 288: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id288", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id288", "iku", false, undefined);

    await expect(promises.cancel("id288", "iku*", true, undefined)).rejects.toThrow();
  });

  test("Test Case 289: transitions from Canceled to Canceled via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id289", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});
    await promises.cancel("id289", "iku", false, undefined);

    await expect(promises.cancel("id289", "iku*", false, undefined)).rejects.toThrow();
  });

  test("Test Case 290: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id290", 0, undefined, false, undefined, {});

    await expect(promises.create("id290", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 291: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id291", 0, undefined, false, undefined, {});

    await expect(promises.create("id291", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 292: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id292", 0, undefined, false, undefined, {});

    await expect(promises.create("id292", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 293: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id293", 0, undefined, false, undefined, {});

    await expect(promises.create("id293", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 294: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id294", 0, undefined, false, undefined, {});

    await expect(promises.resolve("id294", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 295: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id295", 0, undefined, false, undefined, {});

    const promise = await promises.resolve("id295", undefined, false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id295");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 296: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id296", 0, undefined, false, undefined, {});

    await expect(promises.resolve("id296", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 297: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id297", 0, undefined, false, undefined, {});

    const promise = await promises.resolve("id297", "iku", false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id297");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 298: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id298", 0, undefined, false, undefined, {});

    await expect(promises.reject("id298", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 299: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id299", 0, undefined, false, undefined, {});

    const promise = await promises.reject("id299", undefined, false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id299");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 300: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id300", 0, undefined, false, undefined, {});

    await expect(promises.reject("id300", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 301: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id301", 0, undefined, false, undefined, {});

    const promise = await promises.reject("id301", "iku", false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id301");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 302: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id302", 0, undefined, false, undefined, {});

    await expect(promises.cancel("id302", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 303: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id303", 0, undefined, false, undefined, {});

    const promise = await promises.cancel("id303", undefined, false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id303");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 304: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id304", 0, undefined, false, undefined, {});

    await expect(promises.cancel("id304", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 305: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id305", 0, undefined, false, undefined, {});

    const promise = await promises.cancel("id305", "iku", false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id305");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 306: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id306", 0, "ikc", false, undefined, {});

    await expect(promises.create("id306", Number.MAX_SAFE_INTEGER, undefined, true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 307: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id307", 0, "ikc", false, undefined, {});

    await expect(promises.create("id307", Number.MAX_SAFE_INTEGER, undefined, false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 308: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id308", 0, "ikc", false, undefined, {});

    await expect(promises.create("id308", Number.MAX_SAFE_INTEGER, "ikc", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 309: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id309", 0, "ikc", false, undefined, {});

    const promise = await promises.create("id309", Number.MAX_SAFE_INTEGER, "ikc", false, undefined, {});

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id309");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 310: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id310", 0, "ikc", false, undefined, {});

    await expect(promises.create("id310", Number.MAX_SAFE_INTEGER, "ikc*", true, undefined, {})).rejects.toThrow();
  });

  test("Test Case 311: transitions from Timedout to Timedout via Create", async () => {
    const promises = new Promises();
    await promises.create("id311", 0, "ikc", false, undefined, {});

    await expect(promises.create("id311", Number.MAX_SAFE_INTEGER, "ikc*", false, undefined, {})).rejects.toThrow();
  });

  test("Test Case 312: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id312", 0, "ikc", false, undefined, {});

    await expect(promises.resolve("id312", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 313: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id313", 0, "ikc", false, undefined, {});

    const promise = await promises.resolve("id313", undefined, false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id313");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 314: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id314", 0, "ikc", false, undefined, {});

    await expect(promises.resolve("id314", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 315: transitions from Timedout to Timedout via Resolve", async () => {
    const promises = new Promises();
    await promises.create("id315", 0, "ikc", false, undefined, {});

    const promise = await promises.resolve("id315", "iku", false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id315");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 316: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id316", 0, "ikc", false, undefined, {});

    await expect(promises.reject("id316", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 317: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id317", 0, "ikc", false, undefined, {});

    const promise = await promises.reject("id317", undefined, false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id317");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 318: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id318", 0, "ikc", false, undefined, {});

    await expect(promises.reject("id318", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 319: transitions from Timedout to Timedout via Reject", async () => {
    const promises = new Promises();
    await promises.create("id319", 0, "ikc", false, undefined, {});

    const promise = await promises.reject("id319", "iku", false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id319");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 320: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id320", 0, "ikc", false, undefined, {});

    await expect(promises.cancel("id320", undefined, true, undefined)).rejects.toThrow();
  });

  test("Test Case 321: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id321", 0, "ikc", false, undefined, {});

    const promise = await promises.cancel("id321", undefined, false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id321");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 322: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id322", 0, "ikc", false, undefined, {});

    await expect(promises.cancel("id322", "iku", true, undefined)).rejects.toThrow();
  });

  test("Test Case 323: transitions from Timedout to Timedout via Cancel", async () => {
    const promises = new Promises();
    await promises.create("id323", 0, "ikc", false, undefined, {});

    const promise = await promises.cancel("id323", "iku", false, undefined);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id323");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });
});
