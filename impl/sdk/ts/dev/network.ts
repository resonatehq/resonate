import { Server } from "./server";

import type { Callback } from "types";
import type { Message, Network, Request, ResponseFor } from "../src/network/network";
import * as util from "../src/util";

export class LocalNetwork implements Network {
  private server: Server;
  private timeoutId: ReturnType<typeof setTimeout> | undefined;
  private shouldStop = false;
  private subscriptions: Array<(msg: Message) => void> = new Array();

  constructor(server: Server = new Server()) {
    this.server = server;
    this.timeoutId = undefined;
  }

  stop() {
    clearTimeout(this.timeoutId);
    this.shouldStop = true;
    this.timeoutId = undefined;
  }

  send<T extends Request>(req: Request, callback: Callback<ResponseFor<T>>): void {
    setTimeout(() => {
      try {
        const res = this.server.process(req, Date.now());
        util.assert(res.kind === req.kind, "res kind must match req kind");

        callback(false, res as ResponseFor<T>);
        this.enqueueNext();
      } catch (err) {
        callback(true);
      }
    });
  }

  recv(msg: Message): void {
    for (const callback of this.subscriptions) {
      callback(msg);
    }
  }

  subscribe(callback: (msg: Message) => void): void {
    this.subscriptions.push(callback);
  }

  private enqueueNext(): void {
    clearTimeout(this.timeoutId);

    const time = Date.now();
    const next = this.server.next(time);

    if (next !== undefined && !this.shouldStop) {
      this.timeoutId = setTimeout((): void => {
        for (const { msg } of this.server.step(time)) {
          this.recv(msg);
        }
        this.enqueueNext();
      }, next);
    }
  }
}
