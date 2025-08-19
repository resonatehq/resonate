import { NoHeartbeat } from "../../src/heartbeat";
import type { Network, Message as NetworkMessage, Request, Response } from "../../src/network/network";
import { ResonateInner } from "../../src/resonate-inner";
import { type Address, Message, Process, type Random, anycast, unicast } from "./simulator";

interface DeliveryOptions {
  charFlipProb?: number;
}

class SimulatedNetwork implements Network {
  private correlationId = 0;
  private currentTime = 0;

  private prng: Random;
  private deliveryOptions: Required<DeliveryOptions>;
  private buffer: Message<Request>[] = [];
  private callbacks: Record<number, { callback: (err: boolean, res: Response) => void; timeout: number }> = {};
  private subscriptions: ((msg: NetworkMessage) => void)[] = [];

  constructor(
    prng: Random,
    { charFlipProb = 0 }: DeliveryOptions,
    public readonly source: Address,
    public readonly target: Address,
  ) {
    this.prng = prng;
    this.deliveryOptions = { charFlipProb };
  }

  send(request: Request, callback: (err: boolean, res: Response) => void): void {
    const message = new Message<Request>(this.source, this.target, request, {
      requ: true,
      correlationId: this.correlationId++,
    });

    this.callbacks[message.head!.correlationId] = { callback, timeout: this.currentTime + 5000 };
    this.buffer.push(message);
  }

  recv(msg: NetworkMessage): void {
    for (const callback of this.subscriptions) {
      callback(this.maybeCorruptData(msg));
    }
  }

  subscribe(callback: (msg: NetworkMessage) => void): void {
    this.subscriptions.push(callback);
  }

  stop(): void {}

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

  process(message: Message<Response | NetworkMessage>): void {
    if (message.isResponse()) {
      const correlationId = message.head?.correlationId;
      const entry = correlationId && this.callbacks[correlationId];
      if (entry) {
        entry.callback(false, this.maybeCorruptData(message.data));
        delete this.callbacks[correlationId];
      }
    } else {
      this.recv((message as Message<NetworkMessage>).data);
    }
  }

  flush(): Message<any>[] {
    // Finally, flush the buffer
    const flushed = this.buffer;
    this.buffer = [];
    return flushed;
  }

  private maybeCorruptData(data: Response | NetworkMessage): any {
    // Serialize the data to a string
    let jsonStr = JSON.stringify(data);

    // Randomly decide whether to corrupt
    const shouldCorrupt = this.prng.next() < this.deliveryOptions.charFlipProb;
    if (!shouldCorrupt) return data;

    // Pick a random index to corrupt
    const idx = Math.floor(this.prng.next() * jsonStr.length);

    // Corrupt that character
    jsonStr = `${jsonStr.slice(0, idx)}X${jsonStr.slice(idx + 1)}`;

    // Return corrupted string, even if it's invalid JSON
    return jsonStr;
  }
}

export class WorkerProcess extends Process {
  private network: SimulatedNetwork;
  resonate: ResonateInner;

  constructor(
    prng: Random,
    { charFlipProb = 0 }: DeliveryOptions,
    public readonly iaddr: string,
    public readonly gaddr: string,
  ) {
    super(iaddr, gaddr);
    this.network = new SimulatedNetwork(prng, { charFlipProb: 0 }, anycast(gaddr, iaddr), unicast("server"));
    this.resonate = new ResonateInner(this.network, {
      pid: iaddr,
      group: gaddr,
      ttl: 5000,
      heartbeat: new NoHeartbeat(),
    });
  }

  tick(time: number, messages: Message<Response | NetworkMessage>[]): Message<Request>[] {
    this.log(time, "[recv]", messages);

    this.network.time(time);
    for (const message of messages) {
      this.network.process(message);
    }

    const responses = this.network.flush();

    this.log(time, "[send]", responses);

    return responses;
  }
}
