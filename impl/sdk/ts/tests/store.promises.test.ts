import { Promises } from "../src/promises";
describe("State Transition Tests", () => {
  test("Test Case 0: transitions from Init to Pending via Create", async () => {
    const p = new Promises();
    const promise = await p.create(
      "id0",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      true,
    );

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id0");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 1: transitions from Init to Pending via Create", async () => {
    const p = new Promises();
    const promise = await p.create(
      "id1",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id1");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 2: transitions from Init to Pending via Create", async () => {
    const p = new Promises();
    const promise = await p.create(
      "id2",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      true,
    );

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id2");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 3: transitions from Init to Pending via Create", async () => {
    const p = new Promises();
    const promise = await p.create(
      "id3",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id3");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 4: transitions from Init to Init via Resolve", async () => {
    const p = new Promises();
    await expect(
      p.resolve("id4", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 5: transitions from Init to Init via Resolve", async () => {
    const p = new Promises();
    await expect(
      p.resolve("id5", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 6: transitions from Init to Init via Resolve", async () => {
    const p = new Promises();
    await expect(p.resolve("id6", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 7: transitions from Init to Init via Resolve", async () => {
    const p = new Promises();
    await expect(p.resolve("id7", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 8: transitions from Init to Init via Reject", async () => {
    const p = new Promises();
    await expect(p.reject("id8", undefined, undefined, true)).rejects.toThrow();
  });

  test("Test Case 9: transitions from Init to Init via Reject", async () => {
    const p = new Promises();
    await expect(
      p.reject("id9", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 10: transitions from Init to Init via Reject", async () => {
    const p = new Promises();
    await expect(p.reject("id10", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 11: transitions from Init to Init via Reject", async () => {
    const p = new Promises();
    await expect(p.reject("id11", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 12: transitions from Init to Init via Cancel", async () => {
    const p = new Promises();
    await expect(
      p.cancel("id12", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 13: transitions from Init to Init via Cancel", async () => {
    const p = new Promises();
    await expect(
      p.cancel("id13", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 14: transitions from Init to Init via Cancel", async () => {
    const p = new Promises();
    await expect(p.cancel("id14", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 15: transitions from Init to Init via Cancel", async () => {
    const p = new Promises();
    await expect(p.cancel("id15", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 16: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id16",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    await expect(
      p.create("id16", Number.MAX_SAFE_INTEGER, undefined, {}, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 17: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id17",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    await expect(
      p.create(
        "id17",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 18: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id18",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    await expect(
      p.create("id18", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 19: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id19",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    await expect(
      p.create("id19", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", false),
    ).rejects.toThrow();
  });

  test("Test Case 20: transitions from Pending to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id20",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.resolve("id20", undefined, undefined, true);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id20");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 21: transitions from Pending to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id21",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.resolve("id21", undefined, undefined, false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id21");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 22: transitions from Pending to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id22",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.resolve("id22", undefined, "iku", true);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id22");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 23: transitions from Pending to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id23",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.resolve("id23", undefined, "iku", false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id23");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 24: transitions from Pending to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id24",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.reject("id24", undefined, undefined, true);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id24");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 25: transitions from Pending to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id25",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.reject("id25", undefined, undefined, false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id25");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 26: transitions from Pending to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id26",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.reject("id26", undefined, "iku", true);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id26");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 27: transitions from Pending to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id27",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.reject("id27", undefined, "iku", false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id27");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 28: transitions from Pending to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id28",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.cancel("id28", undefined, undefined, true);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id28");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 29: transitions from Pending to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id29",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.cancel("id29", undefined, undefined, false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id29");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 30: transitions from Pending to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id30",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.cancel("id30", undefined, "iku", true);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id30");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 31: transitions from Pending to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id31",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );

    const promise = await p.cancel("id31", undefined, "iku", false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id31");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 32: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id32",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    await expect(
      p.create("id32", Number.MAX_SAFE_INTEGER, undefined, {}, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 33: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id33",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    await expect(
      p.create(
        "id33",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 34: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id34",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.create(
      "id34",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      true,
    );

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id34");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 35: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id35",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.create(
      "id35",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id35");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 36: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id36",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    await expect(
      p.create("id36", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", true),
    ).rejects.toThrow();
  });

  test("Test Case 37: transitions from Pending to Pending via Create", async () => {
    const p = new Promises();
    await p.create(
      "id37",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    await expect(
      p.create("id37", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", false),
    ).rejects.toThrow();
  });

  test("Test Case 38: transitions from Pending to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id38",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.resolve("id38", undefined, undefined, true);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id38");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 39: transitions from Pending to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id39",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.resolve("id39", undefined, undefined, false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id39");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 40: transitions from Pending to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id40",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.resolve("id40", undefined, "iku", true);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id40");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 41: transitions from Pending to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id41",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.resolve("id41", undefined, "iku", false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id41");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 42: transitions from Pending to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id42",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.reject("id42", undefined, undefined, true);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id42");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 43: transitions from Pending to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id43",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.reject("id43", undefined, undefined, false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id43");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 44: transitions from Pending to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id44",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.reject("id44", undefined, "iku", true);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id44");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 45: transitions from Pending to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id45",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.reject("id45", undefined, "iku", false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id45");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 46: transitions from Pending to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id46",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.cancel("id46", undefined, undefined, true);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id46");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 47: transitions from Pending to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id47",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.cancel("id47", undefined, undefined, false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id47");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 48: transitions from Pending to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id48",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.cancel("id48", undefined, "iku", true);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id48");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 49: transitions from Pending to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id49",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    const promise = await p.cancel("id49", undefined, "iku", false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id49");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 50: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id50",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id50", undefined, undefined, false);

    await expect(
      p.create("id50", Number.MAX_SAFE_INTEGER, undefined, {}, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 51: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id51",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id51", undefined, undefined, false);

    await expect(
      p.create(
        "id51",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 52: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id52",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id52", undefined, undefined, false);

    await expect(
      p.create("id52", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 53: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id53",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id53", undefined, undefined, false);

    await expect(
      p.create("id53", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", false),
    ).rejects.toThrow();
  });

  test("Test Case 54: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id54",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id54", undefined, undefined, false);

    await expect(
      p.resolve("id54", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 55: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id55",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id55", undefined, undefined, false);

    await expect(
      p.resolve("id55", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 56: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id56",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id56", undefined, undefined, false);

    await expect(p.resolve("id56", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 57: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id57",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id57", undefined, undefined, false);

    await expect(p.resolve("id57", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 58: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id58",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id58", undefined, undefined, false);

    await expect(
      p.reject("id58", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 59: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id59",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id59", undefined, undefined, false);

    await expect(
      p.reject("id59", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 60: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id60",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id60", undefined, undefined, false);

    await expect(p.reject("id60", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 61: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id61",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id61", undefined, undefined, false);

    await expect(p.reject("id61", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 62: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id62",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id62", undefined, undefined, false);

    await expect(
      p.cancel("id62", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 63: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id63",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id63", undefined, undefined, false);

    await expect(
      p.cancel("id63", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 64: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id64",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id64", undefined, undefined, false);

    await expect(p.cancel("id64", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 65: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id65",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id65", undefined, undefined, false);

    await expect(p.cancel("id65", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 66: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id66",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id66", undefined, "iku", false);

    await expect(
      p.create("id66", Number.MAX_SAFE_INTEGER, undefined, {}, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 67: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id67",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id67", undefined, "iku", false);

    await expect(
      p.create(
        "id67",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 68: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id68",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id68", undefined, "iku", false);

    await expect(
      p.create("id68", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 69: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id69",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id69", undefined, "iku", false);

    await expect(
      p.create("id69", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", false),
    ).rejects.toThrow();
  });

  test("Test Case 70: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id70",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id70", undefined, "iku", false);

    await expect(
      p.resolve("id70", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 71: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id71",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id71", undefined, "iku", false);

    await expect(
      p.resolve("id71", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 72: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id72",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id72", undefined, "iku", false);

    const promise = await p.resolve("id72", undefined, "iku", true);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id72");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 73: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id73",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id73", undefined, "iku", false);

    const promise = await p.resolve("id73", undefined, "iku", false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id73");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 74: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id74",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id74", undefined, "iku", false);

    await expect(p.resolve("id74", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 75: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id75",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id75", undefined, "iku", false);

    await expect(p.resolve("id75", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 76: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id76",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id76", undefined, "iku", false);

    await expect(
      p.reject("id76", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 77: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id77",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id77", undefined, "iku", false);

    await expect(
      p.reject("id77", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 78: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id78",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id78", undefined, "iku", false);

    await expect(p.reject("id78", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 79: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id79",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id79", undefined, "iku", false);

    const promise = await p.reject("id79", undefined, "iku", false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id79");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 80: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id80",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id80", undefined, "iku", false);

    await expect(p.reject("id80", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 81: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id81",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id81", undefined, "iku", false);

    await expect(p.reject("id81", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 82: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id82",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id82", undefined, "iku", false);

    await expect(
      p.cancel("id82", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 83: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id83",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id83", undefined, "iku", false);

    await expect(
      p.cancel("id83", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 84: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id84",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id84", undefined, "iku", false);

    await expect(p.cancel("id84", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 85: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id85",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id85", undefined, "iku", false);

    const promise = await p.cancel("id85", undefined, "iku", false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id85");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 86: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id86",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id86", undefined, "iku", false);

    await expect(p.cancel("id86", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 87: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id87",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.resolve("id87", undefined, "iku", false);

    await expect(p.cancel("id87", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 88: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id88",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id88", undefined, undefined, false);

    await expect(
      p.create("id88", Number.MAX_SAFE_INTEGER, undefined, {}, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 89: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id89",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id89", undefined, undefined, false);

    await expect(
      p.create(
        "id89",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 90: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id90",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id90", undefined, undefined, false);

    await expect(
      p.create("id90", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 91: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id91",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id91", undefined, undefined, false);

    const promise = await p.create(
      "id91",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id91");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 92: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id92",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id92", undefined, undefined, false);

    await expect(
      p.create("id92", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", true),
    ).rejects.toThrow();
  });

  test("Test Case 93: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id93",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id93", undefined, undefined, false);

    await expect(
      p.create("id93", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", false),
    ).rejects.toThrow();
  });

  test("Test Case 94: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id94",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id94", undefined, undefined, false);

    await expect(
      p.resolve("id94", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 95: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id95",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id95", undefined, undefined, false);

    await expect(
      p.resolve("id95", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 96: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id96",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id96", undefined, undefined, false);

    await expect(p.resolve("id96", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 97: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id97",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id97", undefined, undefined, false);

    await expect(p.resolve("id97", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 98: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id98",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id98", undefined, undefined, false);

    await expect(
      p.reject("id98", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 99: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id99",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id99", undefined, undefined, false);

    await expect(
      p.reject("id99", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 100: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id100",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id100", undefined, undefined, false);

    await expect(p.reject("id100", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 101: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id101",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id101", undefined, undefined, false);

    await expect(p.reject("id101", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 102: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id102",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id102", undefined, undefined, false);

    await expect(
      p.cancel("id102", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 103: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id103",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id103", undefined, undefined, false);

    await expect(
      p.cancel("id103", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 104: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id104",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id104", undefined, undefined, false);

    await expect(p.cancel("id104", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 105: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id105",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id105", undefined, undefined, false);

    await expect(p.cancel("id105", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 106: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id106",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id106", undefined, "iku", false);

    await expect(
      p.create(
        "id106",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 107: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id107",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id107", undefined, "iku", false);

    await expect(
      p.create(
        "id107",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 108: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id108",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id108", undefined, "iku", false);

    await expect(
      p.create("id108", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 109: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id109",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id109", undefined, "iku", false);

    const promise = await p.create(
      "id109",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id109");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 110: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id110",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id110", undefined, "iku", false);

    await expect(
      p.create("id110", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", true),
    ).rejects.toThrow();
  });

  test("Test Case 111: transitions from Resolved to Resolved via Create", async () => {
    const p = new Promises();
    await p.create(
      "id111",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id111", undefined, "iku", false);

    await expect(
      p.create("id111", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", false),
    ).rejects.toThrow();
  });

  test("Test Case 112: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id112",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id112", undefined, "iku", false);

    await expect(
      p.resolve("id112", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 113: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id113",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id113", undefined, "iku", false);

    await expect(
      p.resolve("id113", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 114: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id114",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id114", undefined, "iku", false);

    const promise = await p.resolve("id114", undefined, "iku", true);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id114");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 115: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id115",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id115", undefined, "iku", false);

    const promise = await p.resolve("id115", undefined, "iku", false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id115");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 116: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id116",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id116", undefined, "iku", false);

    await expect(p.resolve("id116", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 117: transitions from Resolved to Resolved via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id117",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id117", undefined, "iku", false);

    await expect(
      p.resolve("id117", undefined, "iku*", false),
    ).rejects.toThrow();
  });

  test("Test Case 118: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id118",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id118", undefined, "iku", false);

    await expect(
      p.reject("id118", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 119: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id119",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id119", undefined, "iku", false);

    await expect(
      p.reject("id119", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 120: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id120",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id120", undefined, "iku", false);

    await expect(p.reject("id120", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 121: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id121",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id121", undefined, "iku", false);

    const promise = await p.reject("id121", undefined, "iku", false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id121");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 122: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id122",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id122", undefined, "iku", false);

    await expect(p.reject("id122", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 123: transitions from Resolved to Resolved via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id123",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id123", undefined, "iku", false);

    await expect(p.reject("id123", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 124: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id124",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id124", undefined, "iku", false);

    await expect(
      p.cancel("id124", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 125: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id125",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id125", undefined, "iku", false);

    await expect(
      p.cancel("id125", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 126: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id126",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id126", undefined, "iku", false);

    await expect(p.cancel("id126", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 127: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id127",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id127", undefined, "iku", false);

    const promise = await p.cancel("id127", undefined, "iku", false);

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id127");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 128: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id128",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id128", undefined, "iku", false);

    await expect(p.cancel("id128", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 129: transitions from Resolved to Resolved via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id129",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.resolve("id129", undefined, "iku", false);

    await expect(p.cancel("id129", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 130: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id130",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id130", undefined, undefined, false);

    await expect(
      p.create(
        "id130",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 131: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id131",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id131", undefined, undefined, false);

    await expect(
      p.create(
        "id131",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 132: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id132",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id132", undefined, undefined, false);

    await expect(
      p.create("id132", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 133: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id133",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id133", undefined, undefined, false);

    await expect(
      p.create("id133", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", false),
    ).rejects.toThrow();
  });

  test("Test Case 134: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id134",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id134", undefined, undefined, false);

    await expect(
      p.resolve("id134", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 135: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id135",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id135", undefined, undefined, false);

    await expect(
      p.resolve("id135", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 136: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id136",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id136", undefined, undefined, false);

    await expect(p.resolve("id136", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 137: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id137",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id137", undefined, undefined, false);

    await expect(p.resolve("id137", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 138: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id138",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id138", undefined, undefined, false);

    await expect(
      p.reject("id138", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 139: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id139",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id139", undefined, undefined, false);

    await expect(
      p.reject("id139", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 140: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id140",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id140", undefined, undefined, false);

    await expect(p.reject("id140", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 141: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id141",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id141", undefined, undefined, false);

    await expect(p.reject("id141", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 142: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id142",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id142", undefined, undefined, false);

    await expect(
      p.cancel("id142", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 143: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id143",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id143", undefined, undefined, false);

    await expect(
      p.cancel("id143", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 144: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id144",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id144", undefined, undefined, false);

    await expect(p.cancel("id144", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 145: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id145",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id145", undefined, undefined, false);

    await expect(p.cancel("id145", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 146: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id146",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id146", undefined, "iku", false);

    await expect(
      p.create(
        "id146",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 147: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id147",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id147", undefined, "iku", false);

    await expect(
      p.create(
        "id147",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 148: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id148",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id148", undefined, "iku", false);

    await expect(
      p.create("id148", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 149: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id149",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id149", undefined, "iku", false);

    await expect(
      p.create("id149", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", false),
    ).rejects.toThrow();
  });

  test("Test Case 150: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id150",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id150", undefined, "iku", false);

    await expect(
      p.resolve("id150", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 151: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id151",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id151", undefined, "iku", false);

    await expect(
      p.resolve("id151", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 152: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id152",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id152", undefined, "iku", false);

    await expect(p.resolve("id152", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 153: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id153",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id153", undefined, "iku", false);

    const promise = await p.resolve("id153", undefined, "iku", false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id153");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 154: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id154",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id154", undefined, "iku", false);

    await expect(p.resolve("id154", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 155: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id155",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id155", undefined, "iku", false);

    await expect(
      p.resolve("id155", undefined, "iku*", false),
    ).rejects.toThrow();
  });

  test("Test Case 156: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id156",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id156", undefined, "iku", false);

    await expect(
      p.reject("id156", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 157: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id157",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id157", undefined, "iku", false);

    await expect(
      p.reject("id157", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 158: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id158",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id158", undefined, "iku", false);

    const promise = await p.reject("id158", undefined, "iku", true);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id158");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 159: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id159",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id159", undefined, "iku", false);

    const promise = await p.reject("id159", undefined, "iku", false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id159");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 160: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id160",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id160", undefined, "iku", false);

    await expect(p.reject("id160", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 161: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id161",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id161", undefined, "iku", false);

    await expect(p.reject("id161", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 162: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id162",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id162", undefined, "iku", false);

    await expect(
      p.cancel("id162", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 163: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id163",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id163", undefined, "iku", false);

    await expect(
      p.cancel("id163", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 164: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id164",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id164", undefined, "iku", false);

    await expect(p.cancel("id164", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 165: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id165",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id165", undefined, "iku", false);

    const promise = await p.cancel("id165", undefined, "iku", false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id165");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 166: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id166",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id166", undefined, "iku", false);

    await expect(p.cancel("id166", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 167: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id167",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.reject("id167", undefined, "iku", false);

    await expect(p.cancel("id167", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 168: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id168",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id168", undefined, undefined, false);

    await expect(
      p.create(
        "id168",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 169: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id169",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id169", undefined, undefined, false);

    await expect(
      p.create(
        "id169",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 170: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id170",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id170", undefined, undefined, false);

    await expect(
      p.create("id170", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 171: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id171",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id171", undefined, undefined, false);

    const promise = await p.create(
      "id171",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id171");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 172: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id172",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id172", undefined, undefined, false);

    await expect(
      p.create("id172", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", true),
    ).rejects.toThrow();
  });

  test("Test Case 173: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id173",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id173", undefined, undefined, false);

    await expect(
      p.create("id173", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", false),
    ).rejects.toThrow();
  });

  test("Test Case 174: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id174",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id174", undefined, undefined, false);

    await expect(
      p.resolve("id174", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 175: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id175",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id175", undefined, undefined, false);

    await expect(
      p.resolve("id175", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 176: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id176",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id176", undefined, undefined, false);

    await expect(p.resolve("id176", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 177: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id177",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id177", undefined, undefined, false);

    await expect(p.resolve("id177", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 178: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id178",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id178", undefined, undefined, false);

    await expect(
      p.reject("id178", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 179: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id179",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id179", undefined, undefined, false);

    await expect(
      p.reject("id179", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 180: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id180",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id180", undefined, undefined, false);

    await expect(p.reject("id180", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 181: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id181",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id181", undefined, undefined, false);

    await expect(p.reject("id181", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 182: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id182",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id182", undefined, undefined, false);

    await expect(
      p.cancel("id182", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 183: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id183",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id183", undefined, undefined, false);

    await expect(
      p.cancel("id183", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 184: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id184",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id184", undefined, undefined, false);

    await expect(p.cancel("id184", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 185: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id185",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id185", undefined, undefined, false);

    await expect(p.cancel("id185", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 186: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id186",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id186", undefined, "iku", false);

    await expect(
      p.create(
        "id186",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 187: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id187",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id187", undefined, "iku", false);

    await expect(
      p.create(
        "id187",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 188: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id188",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id188", undefined, "iku", false);

    await expect(
      p.create("id188", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 189: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id189",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id189", undefined, "iku", false);

    const promise = await p.create(
      "id189",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id189");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 190: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id190",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id190", undefined, "iku", false);

    await expect(
      p.create("id190", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", true),
    ).rejects.toThrow();
  });

  test("Test Case 191: transitions from Rejected to Rejected via Create", async () => {
    const p = new Promises();
    await p.create(
      "id191",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id191", undefined, "iku", false);

    await expect(
      p.create("id191", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", false),
    ).rejects.toThrow();
  });

  test("Test Case 192: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id192",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id192", undefined, "iku", false);

    await expect(
      p.resolve("id192", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 193: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id193",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id193", undefined, "iku", false);

    await expect(
      p.resolve("id193", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 194: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id194",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id194", undefined, "iku", false);

    await expect(p.resolve("id194", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 195: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id195",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id195", undefined, "iku", false);

    const promise = await p.resolve("id195", undefined, "iku", false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id195");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 196: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id196",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id196", undefined, "iku", false);

    await expect(p.resolve("id196", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 197: transitions from Rejected to Rejected via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id197",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id197", undefined, "iku", false);

    await expect(
      p.resolve("id197", undefined, "iku*", false),
    ).rejects.toThrow();
  });

  test("Test Case 198: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id198",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id198", undefined, "iku", false);

    await expect(
      p.reject("id198", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 199: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id199",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id199", undefined, "iku", false);

    await expect(
      p.reject("id199", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 200: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id200",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id200", undefined, "iku", false);

    const promise = await p.reject("id200", undefined, "iku", true);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id200");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 201: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id201",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id201", undefined, "iku", false);

    const promise = await p.reject("id201", undefined, "iku", false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id201");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 202: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id202",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id202", undefined, "iku", false);

    await expect(p.reject("id202", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 203: transitions from Rejected to Rejected via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id203",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id203", undefined, "iku", false);

    await expect(p.reject("id203", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 204: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id204",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id204", undefined, "iku", false);

    await expect(
      p.cancel("id204", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 205: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id205",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id205", undefined, "iku", false);

    await expect(
      p.cancel("id205", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 206: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id206",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id206", undefined, "iku", false);

    await expect(p.cancel("id206", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 207: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id207",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id207", undefined, "iku", false);

    const promise = await p.cancel("id207", undefined, "iku", false);

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id207");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 208: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id208",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id208", undefined, "iku", false);

    await expect(p.cancel("id208", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 209: transitions from Rejected to Rejected via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id209",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.reject("id209", undefined, "iku", false);

    await expect(p.cancel("id209", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 210: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id210",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id210", undefined, undefined, false);

    await expect(
      p.create(
        "id210",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 211: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id211",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id211", undefined, undefined, false);

    await expect(
      p.create(
        "id211",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 212: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id212",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id212", undefined, undefined, false);

    await expect(
      p.create("id212", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 213: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id213",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id213", undefined, undefined, false);

    await expect(
      p.create("id213", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", false),
    ).rejects.toThrow();
  });

  test("Test Case 214: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id214",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id214", undefined, undefined, false);

    await expect(
      p.resolve("id214", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 215: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id215",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id215", undefined, undefined, false);

    await expect(
      p.resolve("id215", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 216: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id216",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id216", undefined, undefined, false);

    await expect(p.resolve("id216", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 217: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id217",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id217", undefined, undefined, false);

    await expect(p.resolve("id217", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 218: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id218",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id218", undefined, undefined, false);

    await expect(
      p.reject("id218", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 219: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id219",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id219", undefined, undefined, false);

    await expect(
      p.reject("id219", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 220: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id220",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id220", undefined, undefined, false);

    await expect(p.reject("id220", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 221: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id221",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id221", undefined, undefined, false);

    await expect(p.reject("id221", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 222: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id222",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id222", undefined, undefined, false);

    await expect(
      p.cancel("id222", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 223: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id223",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id223", undefined, undefined, false);

    await expect(
      p.cancel("id223", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 224: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id224",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id224", undefined, undefined, false);

    await expect(p.cancel("id224", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 225: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id225",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id225", undefined, undefined, false);

    await expect(p.cancel("id225", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 226: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id226",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id226", undefined, "iku", false);

    await expect(
      p.create(
        "id226",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 227: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id227",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id227", undefined, "iku", false);

    await expect(
      p.create(
        "id227",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 228: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id228",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id228", undefined, "iku", false);

    await expect(
      p.create("id228", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 229: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id229",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id229", undefined, "iku", false);

    await expect(
      p.create("id229", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", false),
    ).rejects.toThrow();
  });

  test("Test Case 230: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id230",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id230", undefined, "iku", false);

    await expect(
      p.resolve("id230", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 231: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id231",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id231", undefined, "iku", false);

    await expect(
      p.resolve("id231", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 232: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id232",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id232", undefined, "iku", false);

    await expect(p.resolve("id232", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 233: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id233",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id233", undefined, "iku", false);

    const promise = await p.resolve("id233", undefined, "iku", false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id233");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 234: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id234",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id234", undefined, "iku", false);

    await expect(p.resolve("id234", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 235: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id235",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id235", undefined, "iku", false);

    await expect(
      p.resolve("id235", undefined, "iku*", false),
    ).rejects.toThrow();
  });

  test("Test Case 236: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id236",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id236", undefined, "iku", false);

    await expect(
      p.reject("id236", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 237: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id237",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id237", undefined, "iku", false);

    await expect(
      p.reject("id237", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 238: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id238",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id238", undefined, "iku", false);

    await expect(p.reject("id238", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 239: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id239",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id239", undefined, "iku", false);

    const promise = await p.reject("id239", undefined, "iku", false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id239");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 240: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id240",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id240", undefined, "iku", false);

    await expect(p.reject("id240", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 241: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id241",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id241", undefined, "iku", false);

    await expect(p.reject("id241", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 242: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id242",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id242", undefined, "iku", false);

    await expect(
      p.cancel("id242", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 243: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id243",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id243", undefined, "iku", false);

    await expect(
      p.cancel("id243", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 244: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id244",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id244", undefined, "iku", false);

    const promise = await p.cancel("id244", undefined, "iku", true);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id244");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 245: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id245",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id245", undefined, "iku", false);

    const promise = await p.cancel("id245", undefined, "iku", false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id245");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 246: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id246",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id246", undefined, "iku", false);

    await expect(p.cancel("id246", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 247: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id247",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      undefined,
      false,
    );
    await p.cancel("id247", undefined, "iku", false);

    await expect(p.cancel("id247", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 248: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id248",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id248", undefined, undefined, false);

    await expect(
      p.create(
        "id248",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 249: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id249",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id249", undefined, undefined, false);

    await expect(
      p.create(
        "id249",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 250: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id250",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id250", undefined, undefined, false);

    await expect(
      p.create("id250", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 251: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id251",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id251", undefined, undefined, false);

    const promise = await p.create(
      "id251",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id251");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 252: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id252",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id252", undefined, undefined, false);

    await expect(
      p.create("id252", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", true),
    ).rejects.toThrow();
  });

  test("Test Case 253: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id253",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id253", undefined, undefined, false);

    await expect(
      p.create("id253", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", false),
    ).rejects.toThrow();
  });

  test("Test Case 254: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id254",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id254", undefined, undefined, false);

    await expect(
      p.resolve("id254", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 255: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id255",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id255", undefined, undefined, false);

    await expect(
      p.resolve("id255", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 256: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id256",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id256", undefined, undefined, false);

    await expect(p.resolve("id256", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 257: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id257",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id257", undefined, undefined, false);

    await expect(p.resolve("id257", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 258: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id258",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id258", undefined, undefined, false);

    await expect(
      p.reject("id258", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 259: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id259",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id259", undefined, undefined, false);

    await expect(
      p.reject("id259", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 260: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id260",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id260", undefined, undefined, false);

    await expect(p.reject("id260", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 261: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id261",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id261", undefined, undefined, false);

    await expect(p.reject("id261", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 262: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id262",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id262", undefined, undefined, false);

    await expect(
      p.cancel("id262", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 263: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id263",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id263", undefined, undefined, false);

    await expect(
      p.cancel("id263", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 264: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id264",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id264", undefined, undefined, false);

    await expect(p.cancel("id264", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 265: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id265",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id265", undefined, undefined, false);

    await expect(p.cancel("id265", undefined, "iku", false)).rejects.toThrow();
  });

  test("Test Case 266: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id266",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id266", undefined, "iku", false);

    await expect(
      p.create(
        "id266",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 267: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id267",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id267", undefined, "iku", false);

    await expect(
      p.create(
        "id267",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 268: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id268",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id268", undefined, "iku", false);

    await expect(
      p.create("id268", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 269: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id269",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id269", undefined, "iku", false);

    const promise = await p.create(
      "id269",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id269");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 270: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id270",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id270", undefined, "iku", false);

    await expect(
      p.create("id270", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", true),
    ).rejects.toThrow();
  });

  test("Test Case 271: transitions from Canceled to Canceled via Create", async () => {
    const p = new Promises();
    await p.create(
      "id271",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id271", undefined, "iku", false);

    await expect(
      p.create("id271", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", false),
    ).rejects.toThrow();
  });

  test("Test Case 272: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id272",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id272", undefined, "iku", false);

    await expect(
      p.resolve("id272", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 273: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id273",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id273", undefined, "iku", false);

    await expect(
      p.resolve("id273", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 274: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id274",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id274", undefined, "iku", false);

    await expect(p.resolve("id274", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 275: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id275",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id275", undefined, "iku", false);

    const promise = await p.resolve("id275", undefined, "iku", false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id275");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 276: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id276",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id276", undefined, "iku", false);

    await expect(p.resolve("id276", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 277: transitions from Canceled to Canceled via Resolve", async () => {
    const p = new Promises();
    await p.create(
      "id277",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id277", undefined, "iku", false);

    await expect(
      p.resolve("id277", undefined, "iku*", false),
    ).rejects.toThrow();
  });

  test("Test Case 278: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id278",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id278", undefined, "iku", false);

    await expect(
      p.reject("id278", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 279: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id279",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id279", undefined, "iku", false);

    await expect(
      p.reject("id279", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 280: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id280",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id280", undefined, "iku", false);

    await expect(p.reject("id280", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 281: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id281",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id281", undefined, "iku", false);

    const promise = await p.reject("id281", undefined, "iku", false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id281");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 282: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id282",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id282", undefined, "iku", false);

    await expect(p.reject("id282", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 283: transitions from Canceled to Canceled via Reject", async () => {
    const p = new Promises();
    await p.create(
      "id283",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id283", undefined, "iku", false);

    await expect(p.reject("id283", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 284: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id284",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id284", undefined, "iku", false);

    await expect(
      p.cancel("id284", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 285: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id285",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id285", undefined, "iku", false);

    await expect(
      p.cancel("id285", undefined, undefined, false),
    ).rejects.toThrow();
  });

  test("Test Case 286: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id286",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id286", undefined, "iku", false);

    const promise = await p.cancel("id286", undefined, "iku", true);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id286");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 287: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id287",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id287", undefined, "iku", false);

    const promise = await p.cancel("id287", undefined, "iku", false);

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id287");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 288: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id288",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id288", undefined, "iku", false);

    await expect(p.cancel("id288", undefined, "iku*", true)).rejects.toThrow();
  });

  test("Test Case 289: transitions from Canceled to Canceled via Cancel", async () => {
    const p = new Promises();
    await p.create(
      "id289",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );
    await p.cancel("id289", undefined, "iku", false);

    await expect(p.cancel("id289", undefined, "iku*", false)).rejects.toThrow();
  });

  test("Test Case 290: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id290", 0, undefined, {}, undefined, false);

    await expect(
      p.create(
        "id290",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 291: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id291", 0, undefined, {}, undefined, false);

    await expect(
      p.create(
        "id291",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 292: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id292", 0, undefined, {}, undefined, false);

    await expect(
      p.create("id292", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 293: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id293", 0, undefined, {}, undefined, false);

    await expect(
      p.create("id293", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", false),
    ).rejects.toThrow();
  });

  test("Test Case 294: transitions from Timedout to Timedout via Resolve", async () => {
    const p = new Promises();
    await p.create("id294", 0, undefined, {}, undefined, false);

    await expect(
      p.resolve("id294", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 295: transitions from Timedout to Timedout via Resolve", async () => {
    const p = new Promises();
    await p.create("id295", 0, undefined, {}, undefined, false);

    const promise = await p.resolve("id295", undefined, undefined, false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id295");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 296: transitions from Timedout to Timedout via Resolve", async () => {
    const p = new Promises();
    await p.create("id296", 0, undefined, {}, undefined, false);

    await expect(p.resolve("id296", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 297: transitions from Timedout to Timedout via Resolve", async () => {
    const p = new Promises();
    await p.create("id297", 0, undefined, {}, undefined, false);

    const promise = await p.resolve("id297", undefined, "iku", false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id297");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 298: transitions from Timedout to Timedout via Reject", async () => {
    const p = new Promises();
    await p.create("id298", 0, undefined, {}, undefined, false);

    await expect(
      p.reject("id298", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 299: transitions from Timedout to Timedout via Reject", async () => {
    const p = new Promises();
    await p.create("id299", 0, undefined, {}, undefined, false);

    const promise = await p.reject("id299", undefined, undefined, false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id299");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 300: transitions from Timedout to Timedout via Reject", async () => {
    const p = new Promises();
    await p.create("id300", 0, undefined, {}, undefined, false);

    await expect(p.reject("id300", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 301: transitions from Timedout to Timedout via Reject", async () => {
    const p = new Promises();
    await p.create("id301", 0, undefined, {}, undefined, false);

    const promise = await p.reject("id301", undefined, "iku", false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id301");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 302: transitions from Timedout to Timedout via Cancel", async () => {
    const p = new Promises();
    await p.create("id302", 0, undefined, {}, undefined, false);

    await expect(
      p.cancel("id302", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 303: transitions from Timedout to Timedout via Cancel", async () => {
    const p = new Promises();
    await p.create("id303", 0, undefined, {}, undefined, false);

    const promise = await p.cancel("id303", undefined, undefined, false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id303");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 304: transitions from Timedout to Timedout via Cancel", async () => {
    const p = new Promises();
    await p.create("id304", 0, undefined, {}, undefined, false);

    await expect(p.cancel("id304", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 305: transitions from Timedout to Timedout via Cancel", async () => {
    const p = new Promises();
    await p.create("id305", 0, undefined, {}, undefined, false);

    const promise = await p.cancel("id305", undefined, "iku", false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id305");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 306: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id306", 0, undefined, {}, "ikc", false);

    await expect(
      p.create(
        "id306",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        true,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 307: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id307", 0, undefined, {}, "ikc", false);

    await expect(
      p.create(
        "id307",
        Number.MAX_SAFE_INTEGER,
        undefined,
        {},
        undefined,
        false,
      ),
    ).rejects.toThrow();
  });

  test("Test Case 308: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id308", 0, undefined, {}, "ikc", false);

    await expect(
      p.create("id308", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc", true),
    ).rejects.toThrow();
  });

  test("Test Case 309: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id309", 0, undefined, {}, "ikc", false);

    const promise = await p.create(
      "id309",
      Number.MAX_SAFE_INTEGER,
      undefined,
      {},
      "ikc",
      false,
    );

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id309");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 310: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id310", 0, undefined, {}, "ikc", false);

    await expect(
      p.create("id310", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", true),
    ).rejects.toThrow();
  });

  test("Test Case 311: transitions from Timedout to Timedout via Create", async () => {
    const p = new Promises();
    await p.create("id311", 0, undefined, {}, "ikc", false);

    await expect(
      p.create("id311", Number.MAX_SAFE_INTEGER, undefined, {}, "ikc*", false),
    ).rejects.toThrow();
  });

  test("Test Case 312: transitions from Timedout to Timedout via Resolve", async () => {
    const p = new Promises();
    await p.create("id312", 0, undefined, {}, "ikc", false);

    await expect(
      p.resolve("id312", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 313: transitions from Timedout to Timedout via Resolve", async () => {
    const p = new Promises();
    await p.create("id313", 0, undefined, {}, "ikc", false);

    const promise = await p.resolve("id313", undefined, undefined, false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id313");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 314: transitions from Timedout to Timedout via Resolve", async () => {
    const p = new Promises();
    await p.create("id314", 0, undefined, {}, "ikc", false);

    await expect(p.resolve("id314", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 315: transitions from Timedout to Timedout via Resolve", async () => {
    const p = new Promises();
    await p.create("id315", 0, undefined, {}, "ikc", false);

    const promise = await p.resolve("id315", undefined, "iku", false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id315");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 316: transitions from Timedout to Timedout via Reject", async () => {
    const p = new Promises();
    await p.create("id316", 0, undefined, {}, "ikc", false);

    await expect(
      p.reject("id316", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 317: transitions from Timedout to Timedout via Reject", async () => {
    const p = new Promises();
    await p.create("id317", 0, undefined, {}, "ikc", false);

    const promise = await p.reject("id317", undefined, undefined, false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id317");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 318: transitions from Timedout to Timedout via Reject", async () => {
    const p = new Promises();
    await p.create("id318", 0, undefined, {}, "ikc", false);

    await expect(p.reject("id318", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 319: transitions from Timedout to Timedout via Reject", async () => {
    const p = new Promises();
    await p.create("id319", 0, undefined, {}, "ikc", false);

    const promise = await p.reject("id319", undefined, "iku", false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id319");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 320: transitions from Timedout to Timedout via Cancel", async () => {
    const p = new Promises();
    await p.create("id320", 0, undefined, {}, "ikc", false);

    await expect(
      p.cancel("id320", undefined, undefined, true),
    ).rejects.toThrow();
  });

  test("Test Case 321: transitions from Timedout to Timedout via Cancel", async () => {
    const p = new Promises();
    await p.create("id321", 0, undefined, {}, "ikc", false);

    const promise = await p.cancel("id321", undefined, undefined, false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id321");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 322: transitions from Timedout to Timedout via Cancel", async () => {
    const p = new Promises();
    await p.create("id322", 0, undefined, {}, "ikc", false);

    await expect(p.cancel("id322", undefined, "iku", true)).rejects.toThrow();
  });

  test("Test Case 323: transitions from Timedout to Timedout via Cancel", async () => {
    const p = new Promises();
    await p.create("id323", 0, undefined, {}, "ikc", false);

    const promise = await p.cancel("id323", undefined, "iku", false);

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id323");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });
});
