import { Server } from "../dev/server";
import type { RecvMsg, RequestMsg, ResponseMsg } from "../src/network/network";
import { type Address, Message, Process, anycast, unicast } from "./simulator";

export class ServerProcess extends Process {
  server: Server = new Server();

  constructor(public readonly iaddr: string) {
    super(iaddr);
  }

  tick(time: number, messages: Message<RequestMsg>[]): Message<ResponseMsg | RecvMsg>[] {
    if (messages.length > 0) {
      this.log(time, messages);
    }

    const responses: Message<ResponseMsg | RecvMsg>[] = [];

    for (const message of messages) {
      if (message.isRequest()) {
        responses.push(message.resp(this.server.process(message.data, time)));
      }
    }
    const taskMgs = this.server.step(time);
    for (const message of taskMgs) {
      const url = new URL(message.recv);
      let target: Address;
      if (url.username === "any") {
        target = url.pathname === "" ? anycast(url.hostname) : anycast(url.hostname, url.pathname.slice(1));
      } else if (url.username === "uni") {
        target = unicast(url.pathname.slice(1));
      } else {
        throw new Error(`not handled ${url}`);
      }
      const msg = new Message<RecvMsg>(unicast(this.iaddr), target, message.msg, { requ: true });
      responses.push(msg);
    }

    return responses;
  }
}
