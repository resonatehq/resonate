import { Server } from "./server";

import type { Network, RecvMsg, RequestMsg, ResponseMsg } from "../src/network/network";
import type { CompResult } from "../src/types";

export class LocalNetwork implements Network {
  private server: Server;
  private timeoutId: ReturnType<typeof setTimeout> | undefined;
  private shouldStop = false;

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
        const msgs = this.server.step(time);
        this.enqueueNext();
        this.recv(msgs);
      }, n);
    }
  }

  send(request: RequestMsg, callback: (timeout: boolean, response: ResponseMsg) => void): void {
    setTimeout(() => {
      const response = this.server.process(request, Date.now());
      clearTimeout(this.timeoutId);
      this.enqueueNext();
      callback(false, response);
    });
  }

  recv(msg: any): void {
    const msgs = msg as { msg: RecvMsg; recv: string }[];
    for (const m of msgs) {
      this.onMessage?.(m.msg, () => {});
    }
  }

  public onMessage?: (msg: RecvMsg, cb: (res: CompResult) => void) => void;
}
