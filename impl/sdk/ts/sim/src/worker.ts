import { NoHeartbeat } from "../../src/heartbeat";
import type { Network, Message as NetworkMessage, Request, Response, ResponseFor } from "../../src/network/network";
import { ResonateInner } from "../../src/resonate-inner";
import type { Callback } from "../../src/types";
import * as util from "../../src/util";
import { type Address, Message, Process, type Random, unicast } from "./simulator";

interface DeliveryOptions {
  charFlipProb?: number;
}

class SimulatedNetwork implements Network {
  private correlationId = 1;
  private currentTime = 0;

  private prng: Random;
  private deliveryOptions: Required<DeliveryOptions>;
  private buffer: Message<Request>[] = [];
  private callbacks: Record<number, { callback: Callback<Response>; timeout: number }> = {};
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

  send<T extends Request>(req: T, cb: (err: boolean, res?: ResponseFor<T>) => void): void {
    const message = new Message<Request>(this.source, this.target, req, {
      requ: true,
      correlationId: this.correlationId++,
    });

    const callback = (err: boolean, res?: Response) => {
      util.assert(err || (res !== undefined && res.kind === req.kind), "res kind must match req kind");
      cb(err, res as ResponseFor<T>);
    };

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
        cb.callback(true);
        delete this.callbacks[key];
      }
    }
  }

  process(message: Message<{ err?: any; res?: Response } | NetworkMessage>): void {
    if (message.isResponse()) {
      util.assert(message.source === this.target);
      util.assert(message.target === this.source);
      const correlationId = message.head?.correlationId;
      const entry = correlationId && this.callbacks[correlationId];
      if (entry) {
        const msg = message as Message<{ err?: any; res?: Response }>;
        if (msg.data.err) {
          util.assert(msg.data.res === undefined);
          entry.callback(true);
        } else {
          util.assertDefined(msg.data.res);
          entry.callback(false, this.maybeCorruptData(msg.data.res));
        }
        delete this.callbacks[correlationId];
      }
    } else {
      util.assert(message.source.kind === this.target.kind && message.source.iaddr === this.target.iaddr);
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
    this.network = new SimulatedNetwork(prng, { charFlipProb: 0 }, unicast(iaddr), unicast("server"));
    this.resonate = new ResonateInner(this.network, {
      pid: iaddr,
      anycast: `sim://any@${gaddr}/${iaddr}`,
      unicast: `sim://uni@${gaddr}/${iaddr}`,
      ttl: 5000,
      heartbeat: new NoHeartbeat(),
    });
  }

  tick(time: number, messages: Message<{ err?: any; res?: Response } | NetworkMessage>[]): Message<Request>[] {
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
