import { Server } from "../src/store";
describe("State Transition Tests", () => {
  test("Test Case 0: transitions from Init to Pending via Create", async () => {
    const server = new Server();
    const promise = server.process({
      kind: "createPromise",
      id: "id0",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: true,
    }).promise;

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id0");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 1: transitions from Init to Pending via Create", async () => {
    const server = new Server();
    const promise = server.process({
      kind: "createPromise",
      id: "id1",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id1");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 2: transitions from Init to Pending via Create", async () => {
    const server = new Server();
    const promise = server.process({
      kind: "createPromise",
      id: "id2",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: true,
    }).promise;

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id2");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 3: transitions from Init to Pending via Create", async () => {
    const server = new Server();
    const promise = server.process({
      kind: "createPromise",
      id: "id3",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id3");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 4: transitions from Init to Init via Resolve", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id4",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 5: transitions from Init to Init via Resolve", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id5",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 6: transitions from Init to Init via Resolve", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id6",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 7: transitions from Init to Init via Resolve", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id7",
          state: "resolved",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 8: transitions from Init to Init via Reject", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id8",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 9: transitions from Init to Init via Reject", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id9",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 10: transitions from Init to Init via Reject", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id10",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 11: transitions from Init to Init via Reject", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id11",
          state: "rejected",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 12: transitions from Init to Init via Cancel", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id12",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 13: transitions from Init to Init via Cancel", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id13",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 14: transitions from Init to Init via Cancel", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id14",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 15: transitions from Init to Init via Cancel", async () => {
    const server = new Server();
    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id15",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 16: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id16",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id16",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 17: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id17",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id17",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 18: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id18",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id18",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 19: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id19",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id19",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 20: transitions from Pending to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id20",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id20",
      state: "resolved",
      strict: true,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id20");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 21: transitions from Pending to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id21",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id21",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id21");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 22: transitions from Pending to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id22",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id22",
      state: "resolved",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id22");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 23: transitions from Pending to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id23",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id23",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id23");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 24: transitions from Pending to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id24",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id24",
      state: "rejected",
      strict: true,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id24");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 25: transitions from Pending to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id25",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id25",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id25");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 26: transitions from Pending to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id26",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id26",
      state: "rejected",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id26");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 27: transitions from Pending to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id27",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id27",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id27");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 28: transitions from Pending to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id28",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id28",
      state: "rejected_canceled",
      strict: true,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id28");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 29: transitions from Pending to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id29",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id29",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id29");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 30: transitions from Pending to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id30",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id30",
      state: "rejected_canceled",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id30");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 31: transitions from Pending to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id31",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id31",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id31");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 32: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id32",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id32",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 33: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id33",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id33",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 34: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id34",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id34",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: true,
    }).promise;

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id34");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 35: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id35",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id35",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("pending");
    expect(promise.id).toBe("id35");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 36: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id36",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id36",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 37: transitions from Pending to Pending via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id37",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id37",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 38: transitions from Pending to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id38",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id38",
      state: "resolved",
      strict: true,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id38");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 39: transitions from Pending to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id39",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id39",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id39");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 40: transitions from Pending to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id40",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id40",
      state: "resolved",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id40");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 41: transitions from Pending to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id41",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id41",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id41");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 42: transitions from Pending to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id42",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id42",
      state: "rejected",
      strict: true,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id42");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 43: transitions from Pending to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id43",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id43",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id43");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 44: transitions from Pending to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id44",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id44",
      state: "rejected",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id44");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 45: transitions from Pending to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id45",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id45",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id45");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 46: transitions from Pending to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id46",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id46",
      state: "rejected_canceled",
      strict: true,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id46");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 47: transitions from Pending to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id47",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id47",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id47");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 48: transitions from Pending to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id48",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id48",
      state: "rejected_canceled",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id48");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 49: transitions from Pending to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id49",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id49",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id49");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 50: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id50",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id50",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id50",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 51: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id51",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id51",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id51",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 52: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id52",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id52",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id52",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 53: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id53",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id53",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id53",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 54: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id54",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id54",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id54",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 55: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id55",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id55",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id55",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 56: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id56",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id56",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id56",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 57: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id57",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id57",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id57",
          state: "resolved",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 58: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id58",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id58",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id58",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 59: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id59",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id59",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id59",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 60: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id60",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id60",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id60",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 61: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id61",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id61",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id61",
          state: "rejected",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 62: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id62",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id62",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id62",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 63: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id63",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id63",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id63",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 64: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id64",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id64",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id64",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 65: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id65",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id65",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id65",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 66: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id66",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id66",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id66",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 67: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id67",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id67",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id67",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 68: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id68",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id68",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id68",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 69: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id69",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id69",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id69",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 70: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id70",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id70",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id70",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 71: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id71",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id71",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id71",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 72: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id72",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id72",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id72",
      state: "resolved",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id72");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 73: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id73",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id73",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id73",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id73");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 74: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id74",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id74",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id74",
          state: "resolved",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 75: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id75",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id75",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id75",
          state: "resolved",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 76: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id76",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id76",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id76",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 77: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id77",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id77",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id77",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 78: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id78",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id78",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id78",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 79: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id79",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id79",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id79",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id79");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 80: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id80",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id80",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id80",
          state: "rejected",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 81: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id81",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id81",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id81",
          state: "rejected",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 82: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id82",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id82",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id82",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 83: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id83",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id83",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id83",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 84: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id84",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id84",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id84",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 85: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id85",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id85",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id85",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id85");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 86: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id86",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id86",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id86",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 87: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id87",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id87",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id87",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 88: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id88",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id88",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id88",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 89: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id89",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id89",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id89",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 90: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id90",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id90",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id90",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 91: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id91",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id91",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id91",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id91");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 92: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id92",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id92",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id92",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 93: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id93",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id93",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id93",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 94: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id94",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id94",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id94",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 95: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id95",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id95",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id95",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 96: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id96",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id96",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id96",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 97: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id97",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id97",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id97",
          state: "resolved",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 98: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id98",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id98",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id98",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 99: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id99",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id99",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id99",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 100: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id100",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id100",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id100",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 101: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id101",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id101",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id101",
          state: "rejected",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 102: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id102",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id102",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id102",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 103: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id103",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id103",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id103",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 104: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id104",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id104",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id104",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 105: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id105",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id105",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id105",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 106: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id106",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id106",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id106",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 107: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id107",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id107",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id107",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 108: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id108",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id108",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id108",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 109: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id109",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id109",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id109",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id109");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 110: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id110",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id110",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id110",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 111: transitions from Resolved to Resolved via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id111",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id111",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id111",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 112: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id112",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id112",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id112",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 113: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id113",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id113",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id113",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 114: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id114",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id114",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id114",
      state: "resolved",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id114");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 115: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id115",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id115",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id115",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id115");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 116: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id116",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id116",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id116",
          state: "resolved",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 117: transitions from Resolved to Resolved via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id117",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id117",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id117",
          state: "resolved",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 118: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id118",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id118",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id118",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 119: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id119",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id119",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id119",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 120: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id120",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id120",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id120",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 121: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id121",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id121",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id121",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id121");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 122: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id122",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id122",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id122",
          state: "rejected",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 123: transitions from Resolved to Resolved via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id123",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id123",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id123",
          state: "rejected",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 124: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id124",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id124",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id124",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 125: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id125",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id125",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id125",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 126: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id126",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id126",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id126",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 127: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id127",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id127",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id127",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("resolved");
    expect(promise.id).toBe("id127");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 128: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id128",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id128",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id128",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 129: transitions from Resolved to Resolved via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id129",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id129",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id129",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 130: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id130",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id130",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id130",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 131: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id131",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id131",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id131",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 132: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id132",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id132",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id132",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 133: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id133",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id133",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id133",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 134: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id134",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id134",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id134",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 135: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id135",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id135",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id135",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 136: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id136",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id136",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id136",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 137: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id137",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id137",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id137",
          state: "resolved",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 138: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id138",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id138",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id138",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 139: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id139",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id139",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id139",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 140: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id140",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id140",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id140",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 141: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id141",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id141",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id141",
          state: "rejected",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 142: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id142",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id142",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id142",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 143: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id143",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id143",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id143",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 144: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id144",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id144",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id144",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 145: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id145",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id145",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id145",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 146: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id146",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id146",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id146",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 147: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id147",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id147",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id147",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 148: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id148",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id148",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id148",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 149: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id149",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id149",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id149",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 150: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id150",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id150",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id150",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 151: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id151",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id151",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id151",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 152: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id152",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id152",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id152",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 153: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id153",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id153",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id153",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id153");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 154: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id154",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id154",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id154",
          state: "resolved",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 155: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id155",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id155",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id155",
          state: "resolved",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 156: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id156",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id156",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id156",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 157: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id157",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id157",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id157",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 158: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id158",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id158",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id158",
      state: "rejected",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id158");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 159: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id159",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id159",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id159",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id159");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 160: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id160",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id160",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id160",
          state: "rejected",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 161: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id161",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id161",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id161",
          state: "rejected",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 162: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id162",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id162",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id162",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 163: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id163",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id163",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id163",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 164: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id164",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id164",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id164",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 165: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id165",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id165",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id165",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id165");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 166: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id166",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id166",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id166",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 167: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id167",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id167",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id167",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 168: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id168",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id168",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id168",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 169: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id169",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id169",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id169",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 170: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id170",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id170",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id170",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 171: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id171",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id171",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id171",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id171");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 172: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id172",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id172",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id172",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 173: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id173",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id173",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id173",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 174: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id174",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id174",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id174",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 175: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id175",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id175",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id175",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 176: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id176",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id176",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id176",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 177: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id177",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id177",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id177",
          state: "resolved",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 178: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id178",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id178",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id178",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 179: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id179",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id179",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id179",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 180: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id180",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id180",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id180",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 181: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id181",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id181",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id181",
          state: "rejected",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 182: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id182",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id182",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id182",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 183: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id183",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id183",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id183",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 184: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id184",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id184",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id184",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 185: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id185",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id185",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id185",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 186: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id186",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id186",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id186",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 187: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id187",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id187",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id187",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 188: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id188",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id188",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id188",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 189: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id189",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id189",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id189",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id189");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 190: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id190",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id190",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id190",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 191: transitions from Rejected to Rejected via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id191",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id191",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id191",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 192: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id192",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id192",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id192",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 193: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id193",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id193",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id193",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 194: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id194",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id194",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id194",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 195: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id195",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id195",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id195",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id195");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 196: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id196",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id196",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id196",
          state: "resolved",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 197: transitions from Rejected to Rejected via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id197",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id197",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id197",
          state: "resolved",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 198: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id198",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id198",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id198",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 199: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id199",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id199",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id199",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 200: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id200",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id200",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id200",
      state: "rejected",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id200");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 201: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id201",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id201",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id201",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id201");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 202: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id202",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id202",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id202",
          state: "rejected",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 203: transitions from Rejected to Rejected via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id203",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id203",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id203",
          state: "rejected",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 204: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id204",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id204",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id204",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 205: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id205",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id205",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id205",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 206: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id206",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id206",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id206",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 207: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id207",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id207",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id207",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected");
    expect(promise.id).toBe("id207");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 208: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id208",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id208",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id208",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 209: transitions from Rejected to Rejected via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id209",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id209",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id209",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 210: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id210",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id210",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id210",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 211: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id211",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id211",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id211",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 212: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id212",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id212",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id212",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 213: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id213",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id213",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id213",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 214: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id214",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id214",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id214",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 215: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id215",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id215",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id215",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 216: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id216",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id216",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id216",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 217: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id217",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id217",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id217",
          state: "resolved",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 218: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id218",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id218",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id218",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 219: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id219",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id219",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id219",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 220: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id220",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id220",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id220",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 221: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id221",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id221",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id221",
          state: "rejected",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 222: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id222",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id222",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id222",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 223: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id223",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id223",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id223",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 224: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id224",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id224",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id224",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 225: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id225",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id225",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id225",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 226: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id226",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id226",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id226",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 227: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id227",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id227",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id227",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 228: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id228",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id228",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id228",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 229: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id229",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id229",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id229",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 230: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id230",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id230",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id230",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 231: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id231",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id231",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id231",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 232: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id232",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id232",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id232",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 233: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id233",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id233",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id233",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id233");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 234: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id234",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id234",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id234",
          state: "resolved",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 235: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id235",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id235",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id235",
          state: "resolved",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 236: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id236",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id236",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id236",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 237: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id237",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id237",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id237",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 238: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id238",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id238",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id238",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 239: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id239",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id239",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id239",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id239");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 240: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id240",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id240",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id240",
          state: "rejected",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 241: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id241",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id241",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id241",
          state: "rejected",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 242: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id242",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id242",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id242",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 243: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id243",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id243",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id243",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 244: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id244",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id244",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id244",
      state: "rejected_canceled",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id244");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 245: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id245",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id245",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id245",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id245");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 246: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id246",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id246",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id246",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 247: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id247",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: undefined,
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id247",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id247",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 248: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id248",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id248",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id248",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 249: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id249",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id249",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id249",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 250: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id250",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id250",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id250",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 251: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id251",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id251",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id251",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id251");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 252: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id252",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id252",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id252",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 253: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id253",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id253",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id253",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 254: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id254",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id254",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id254",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 255: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id255",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id255",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id255",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 256: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id256",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id256",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id256",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 257: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id257",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id257",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id257",
          state: "resolved",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 258: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id258",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id258",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id258",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 259: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id259",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id259",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id259",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 260: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id260",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id260",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id260",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 261: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id261",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id261",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id261",
          state: "rejected",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 262: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id262",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id262",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id262",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 263: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id263",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id263",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id263",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 264: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id264",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id264",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id264",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 265: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id265",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id265",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id265",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 266: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id266",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id266",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id266",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 267: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id267",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id267",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id267",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 268: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id268",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id268",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id268",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 269: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id269",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id269",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id269",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id269");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 270: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id270",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id270",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id270",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 271: transitions from Canceled to Canceled via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id271",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id271",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id271",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 272: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id272",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id272",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id272",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 273: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id273",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id273",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id273",
          state: "resolved",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 274: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id274",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id274",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id274",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 275: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id275",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id275",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id275",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id275");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 276: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id276",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id276",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id276",
          state: "resolved",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 277: transitions from Canceled to Canceled via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id277",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id277",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id277",
          state: "resolved",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 278: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id278",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id278",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id278",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 279: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id279",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id279",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id279",
          state: "rejected",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 280: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id280",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id280",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id280",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 281: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id281",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id281",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id281",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id281");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 282: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id282",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id282",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id282",
          state: "rejected",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 283: transitions from Canceled to Canceled via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id283",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id283",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id283",
          state: "rejected",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 284: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id284",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id284",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id284",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 285: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id285",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id285",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id285",
          state: "rejected_canceled",
          strict: false,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 286: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id286",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id286",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id286",
      state: "rejected_canceled",
      strict: true,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id286");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 287: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id287",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id287",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id287",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_canceled");
    expect(promise.id).toBe("id287");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe("iku");
  });

  test("Test Case 288: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id288",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id288",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id288",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 289: transitions from Canceled to Canceled via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id289",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;
    server.process({
      kind: "completePromise",
      id: "id289",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id289",
          state: "rejected_canceled",
          strict: false,
          iKey: "iku*",
        }).promise,
    ).toThrow();
  });

  test("Test Case 290: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id290",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id290",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 291: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id291",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id291",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 292: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id292",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id292",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 293: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id293",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id293",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 294: transitions from Timedout to Timedout via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id294",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id294",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 295: transitions from Timedout to Timedout via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id295",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id295",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id295");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 296: transitions from Timedout to Timedout via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id296",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id296",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 297: transitions from Timedout to Timedout via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id297",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id297",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id297");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 298: transitions from Timedout to Timedout via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id298",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id298",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 299: transitions from Timedout to Timedout via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id299",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id299",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id299");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 300: transitions from Timedout to Timedout via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id300",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id300",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 301: transitions from Timedout to Timedout via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id301",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id301",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id301");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 302: transitions from Timedout to Timedout via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id302",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id302",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 303: transitions from Timedout to Timedout via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id303",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id303",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id303");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 304: transitions from Timedout to Timedout via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id304",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id304",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 305: transitions from Timedout to Timedout via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id305",
      timeout: 0,
      iKey: undefined,
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id305",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id305");
    expect(promise.iKeyForCreate).toBe(undefined);
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 306: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id306",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id306",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 307: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id307",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id307",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: undefined,
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 308: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id308",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id308",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 309: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id309",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "createPromise",
      id: "id309",
      timeout: Number.MAX_SAFE_INTEGER,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id309");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 310: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id310",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id310",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: true,
        }).promise,
    ).toThrow();
  });

  test("Test Case 311: transitions from Timedout to Timedout via Create", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id311",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "createPromise",
          id: "id311",
          timeout: Number.MAX_SAFE_INTEGER,
          iKey: "ikc*",
          strict: false,
        }).promise,
    ).toThrow();
  });

  test("Test Case 312: transitions from Timedout to Timedout via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id312",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id312",
          state: "resolved",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 313: transitions from Timedout to Timedout via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id313",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id313",
      state: "resolved",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id313");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 314: transitions from Timedout to Timedout via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id314",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id314",
          state: "resolved",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 315: transitions from Timedout to Timedout via Resolve", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id315",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id315",
      state: "resolved",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id315");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 316: transitions from Timedout to Timedout via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id316",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id316",
          state: "rejected",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 317: transitions from Timedout to Timedout via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id317",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id317",
      state: "rejected",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id317");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 318: transitions from Timedout to Timedout via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id318",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id318",
          state: "rejected",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 319: transitions from Timedout to Timedout via Reject", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id319",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id319",
      state: "rejected",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id319");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 320: transitions from Timedout to Timedout via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id320",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id320",
          state: "rejected_canceled",
          strict: true,
          iKey: undefined,
        }).promise,
    ).toThrow();
  });

  test("Test Case 321: transitions from Timedout to Timedout via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id321",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id321",
      state: "rejected_canceled",
      strict: false,
      iKey: undefined,
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id321");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });

  test("Test Case 322: transitions from Timedout to Timedout via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id322",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    expect(
      () =>
        server.process({
          kind: "completePromise",
          id: "id322",
          state: "rejected_canceled",
          strict: true,
          iKey: "iku",
        }).promise,
    ).toThrow();
  });

  test("Test Case 323: transitions from Timedout to Timedout via Cancel", async () => {
    const server = new Server();
    server.process({
      kind: "createPromise",
      id: "id323",
      timeout: 0,
      iKey: "ikc",
      strict: false,
    }).promise;

    const promise = server.process({
      kind: "completePromise",
      id: "id323",
      state: "rejected_canceled",
      strict: false,
      iKey: "iku",
    }).promise;

    expect(promise.state).toBe("rejected_timedout");
    expect(promise.id).toBe("id323");
    expect(promise.iKeyForCreate).toBe("ikc");
    expect(promise.iKeyForComplete).toBe(undefined);
  });
});
