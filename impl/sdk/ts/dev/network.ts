import type { ResonateError } from "../src/exceptions";
import type { Message, MessageSource, Network, Request, ResponseFor } from "../src/network/network";
import * as util from "../src/util";
import { Server } from "./server";

export class LocalMessageSource implements MessageSource {
  private server: Server;
  private timeoutId: ReturnType<typeof setTimeout> | undefined;
  private shouldStop = false;
  private subscriptions: {
    invoke: Array<(msg: Message) => void>;
    resume: Array<(msg: Message) => void>;
    notify: Array<(msg: Message) => void>;
  } = { invoke: [], resume: [], notify: [] };

  constructor(server: Server) {
    this.server = server;
    this.timeoutId = undefined;
  }

  stop() {
    clearTimeout(this.timeoutId);
    this.shouldStop = true;
    this.timeoutId = undefined;
  }

  recv(msg: Message): void {
    for (const callback of this.subscriptions[msg.type]) {
      callback(msg);
    }
  }

  subscribe(type: "invoke" | "resume" | "notify", callback: (msg: Message) => void): void {
    this.subscriptions[type].push(callback);
  }

  enqueueNext(): void {
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

export class LocalNetwork implements Network {
  private server: Server;
  private messageSource: LocalMessageSource;

  constructor(server: Server = new Server()) {
    this.server = server;
    this.messageSource = new LocalMessageSource(server);
  }

  getMessageSource(): MessageSource {
    return this.messageSource;
  }

  stop() {
    // No-op for LocalNetwork, MessageSource handles polling cleanup
  }

  send<T extends Request>(req: Request, callback: (err?: ResonateError, res?: ResponseFor<T>) => void): void {
    setTimeout(() => {
      try {
        const res = this.server.process(req, Date.now());
        util.assert(res.kind === req.kind, "res kind must match req kind");

        callback(undefined, res as ResponseFor<T>);
        this.messageSource.enqueueNext();
      } catch (err) {
        callback(err as ResonateError);
      }
    });
  }
}
