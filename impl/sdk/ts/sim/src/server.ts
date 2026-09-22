import type { StepClock } from "../../src/clock.js";
import { type Change, Server } from "../../src/network/local.js";
import * as util from "../../src/util.js";
import { type Address, anycast, Message, Process, unicast } from "./simulator.js";

function extractOutgoing(changes: Change[]): Array<{ address: string; message: string }> {
  const out: Array<{ address: string; message: string }> = [];
  for (const c of changes) {
    if (c.kind === "message.send") {
      out.push({ address: c.address, message: JSON.stringify(c.message) });
    }
  }
  return out;
}

export class ServerProcess extends Process {
  private clock: StepClock;
  server: Server = new Server();

  constructor(
    clock: StepClock,
    public readonly iaddr: string,
  ) {
    super(iaddr);
    this.clock = clock;
  }

  tick(tick: number, messages: Message<string>[]): Message<string>[] {
    this.log(tick, "[recv]", messages);

    // Advance the clock so that server-side timeouts (e.g. PENDING_RETRY_TTL = 30000) can fire.
    this.clock.time += 100;

    const responses: Message<string>[] = [];
    const outgoing: Array<{ address: string; message: string }> = [];

    for (const message of messages) {
      util.assert(message.target.iaddr === this.iaddr);
      util.assert(typeof message.data === "string");
      if (message.isRequest()) {
        const data = JSON.parse(message.data);
        const result = this.server.apply(this.clock.time, data);
        outgoing.push(...extractOutgoing(result.changes));
        const response = result.response;
        responses.push(
          message.resp(
            JSON.stringify({
              kind: response.kind,
              head: { corrId: data.head.corrId, status: response.head.status, version: data.head.version },
              data: response.data,
            }),
          ),
        );
      }
    }

    for (const msg of outgoing) {
      const url = new URL(msg.address);
      let target: Address;
      if (url.username === "any") {
        target = url.pathname === "" ? anycast(url.hostname) : anycast(url.hostname, url.pathname.slice(1));
      } else if (url.username === "uni") {
        target = unicast(url.pathname.slice(1));
      } else {
        throw new Error(`not handled ${url}`);
      }

      responses.push(new Message<string>(unicast(this.iaddr), target, msg.message, { requ: true }));
    }

    this.log(tick, "[send]", responses);
    return responses;
  }
}
