import { LocalNetwork } from "../dev/network";
import { Server } from "../dev/server";

import { Schedules } from "../src/schedules";

let COUNTER = 0;

describe("schedule transitions", () => {
  let server: Server;
  let schedules: Schedules;
  let id: string;

  beforeAll(() => {
    server = new Server();
    schedules = new Schedules(new LocalNetwork(server));
  });

  beforeEach(async () => {
    id = `tid${COUNTER++}`;
  });

  afterEach(async () => {
    await schedules.delete(id);
  });

  test("test case 1: read and get", async () => {
    let schedule = await schedules.create(id, "0 * * * *", "foo", Number.MAX_SAFE_INTEGER);
    expect(await schedules.get(schedule.id)).toEqual(schedule);
  });

  test("test case 2: create twice without ikey", async () => {
    await schedules.create(id, "* * * * *", "foo", 10);
    await expect(schedules.create(id, "* * * * *", "foo", 10)).rejects.toThrow();
  });

  test("test case 3: create twich with ikey", async () => {
    let schedule = await schedules.create(id, "* * * * *", "foo", 10, "foo");
    await schedules.create(id, "* 2 * * *", "bar", 10, "foo");
    expect(await schedules.get(schedule.id)).toEqual(schedule);
  });
});
