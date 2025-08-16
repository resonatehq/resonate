import { Server } from "./server";

import type { Message, Network, Request, Response } from "../src/network/network";

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
    this.shouldStop = true;
    clearTimeout(this.timeoutId);
    this.timeoutId = undefined;
  }

  private enqueueNext(): void {
    const time = Date.now();
    clearTimeout(this.timeoutId);
    const n = this.server.next(time);

    if (n !== undefined && !this.shouldStop) {
      this.timeoutId = setTimeout((): void => {
        for (const { msg } of this.server.step(time)) {
          this.recv(msg);
        }
        this.enqueueNext();
      }, n);
    }
  }

  send(request: Request, callback: (timeout: boolean, response: Response) => void): void {
    setTimeout(() => {
      const response = this.server.process(request, Date.now());
      clearTimeout(this.timeoutId);
      this.enqueueNext();
      callback(false, response);
    });
  }

  recv(msg: Message): void {
    for (const callback of this.subscriptions) {
      callback(msg);
    }
  }

  public subscribe(callback: (msg: Message) => void): void {
    this.subscriptions.push(callback);
  }
}
