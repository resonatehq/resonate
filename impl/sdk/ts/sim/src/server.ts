import { Server } from "../../dev/server";
import type { Message as NetworkMessage, Request, Response } from "../../src/network/network";
import { type Address, Message, Process, anycast, unicast } from "./simulator";

export class ServerProcess extends Process {
  server: Server = new Server();

  constructor(public readonly iaddr: string) {
    super(iaddr);
  }

  tick(time: number, messages: Message<Request>[]): Message<Response | NetworkMessage>[] {
    this.log(time, "[recv]", messages);

    const responses: Message<Response | NetworkMessage>[] = [];

    for (const message of messages) {
      if (message.isRequest()) {
        responses.push(message.resp(this.server.process(message.data, time)));
      }
    }

    for (const message of this.server.step(time)) {
      const url = new URL(message.recv);
      let target: Address;
      if (url.username === "any") {
        target = url.pathname === "" ? anycast(url.hostname) : anycast(url.hostname, url.pathname.slice(1));
      } else if (url.username === "uni") {
        target = unicast(url.pathname.slice(1));
      } else {
        throw new Error(`not handled ${url}`);
      }

      const msg = new Message<NetworkMessage>(unicast(this.iaddr), target, message.msg, { requ: true });
      responses.push(msg);
    }

    this.log(time, "[send]", responses);
    return responses;
  }
}
