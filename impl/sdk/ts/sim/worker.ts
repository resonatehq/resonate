import type { Network, RecvMsg, RequestMsg, ResponseMsg } from "../src/network/network";
import { ResonateInner } from "../src/resonate-inner";
import { type Address, Message, Process, anycast, unicast } from "./simulator";

class SimulatedNetwork implements Network {
  private correlationId = 1;
  private buffer: Message<RequestMsg>[] = [];
  private callbacks: Record<number, { callback: (timeout: boolean, response: ResponseMsg) => void; timeout: number }> =
    {};
  private currentTime = 0;

  constructor(
    public readonly source: Address,
    public readonly target: Address,
  ) {}
  send(request: RequestMsg, callback: (timeout: boolean, response: ResponseMsg) => void): void {
    const message = new Message<RequestMsg>(this.source, this.target, request, {
      requ: true,
      correlationId: this.correlationId++,
    });
    this.callbacks[message.head!.correlationId] = { callback, timeout: this.currentTime + 5000 };
    this.buffer.push(message);
  }

  recv(msg: Message<RecvMsg>): void {
    this.onMessage?.(msg.data);
  }

  onMessage?: (msg: RecvMsg) => void;

  time(time: number): void {
    this.currentTime = time;

    // Then, check for timed-out callbacks
    for (const key in this.callbacks) {
      const cb = this.callbacks[key];
      const hasTimedOut = cb.timeout < this.currentTime;
      if (hasTimedOut) {
        cb.callback(true, {
          kind: "error",
          code: "invalid_request",
          message: "timedout",
        });
        delete this.callbacks[key];
      }
    }
  }

  process(message: Message<ResponseMsg | RecvMsg>): void {
    if (message.isResponse()) {
      const correlationId = message.head?.correlationId;
      const entry = correlationId && this.callbacks[correlationId];
      if (entry) {
        entry.callback(false, message.data);
        delete this.callbacks[correlationId];
      }
    } else {
      this.recv(message as Message<RecvMsg>);
    }
  }
  flush(): Message<any>[] {
    // Finally, flush the buffer
    const flushed = this.buffer;
    this.buffer = [];
    return flushed;
  }
}

export class WorkerProcess extends Process {
  private network: SimulatedNetwork;
  resonate: ResonateInner;

  constructor(
    public readonly iaddr: string,
    public readonly gaddr: string,
  ) {
    super(iaddr, gaddr);
    this.network = new SimulatedNetwork(anycast(gaddr, iaddr), unicast("server"));
    this.resonate = new ResonateInner(this.network, { pid: iaddr, group: gaddr, ttl: 5000 });
  }

  tick(time: number, messages: Message<ResponseMsg | RecvMsg>[]): Message<RequestMsg>[] {
    if (messages.length > 0) {
      this.log(time, messages);
    }

    this.network.time(time);
    for (const message of messages) {
      this.network.process(message);
    }

    return this.network.flush();
  }
}
