import type { StepClock } from "../../src/clock.js";
import { Codec } from "../../src/codec.js";
import { Core } from "../../src/core.js";
import { NoopHeartbeat } from "../../src/heartbeat.js";
import { ConsoleLogger } from "../../src/logger.js";
import type { Network } from "../../src/network/network.js";
import { isResponse, type Message, type Request, type Response } from "../../src/network/types.js";

import { OptionsBuilder } from "../../src/options.js";
import type { Registry } from "../../src/registry.js";
import * as util from "../../src/util.js";
import { type Address, Process, type Random, Message as SimMessage, unicast } from "./simulator.js";

interface DeliveryOptions {
  charFlipProb?: number;
}

class SimulatedNetwork implements Network {
  readonly unicast: string;
  readonly anycast: string;

  private subscribers: Array<(msg: Message) => void> = [];
  private correlationId = 1;
  private currentTime = 0;

  private prng: Random;
  private deliveryOptions: Required<DeliveryOptions>;
  private buffer: SimMessage<string>[] = [];
  private callbacks: Record<
    number,
    {
      req: Request;
      resolve: (res: Response) => void;
      timeout: number;
    }
  > = {};

  constructor(
    iaddr: string,
    gaddr: string,
    prng: Random,
    { charFlipProb = 0 }: DeliveryOptions,
    public readonly source: Address,
    public readonly target: Address,
  ) {
    this.unicast = `sim://uni@${gaddr}/${iaddr}`;
    this.anycast = `sim://any@${gaddr}/${iaddr}`;
    this.prng = prng;
    this.deliveryOptions = { charFlipProb };
  }

  async init(): Promise<void> {}
  async stop(): Promise<void> {}

  match(target: string): string {
    return `sim://any@${target}`;
  }

  recv(callback: (msg: Message) => void): void {
    this.subscribers.push(callback);
  }

  send = async <K extends Request["kind"]>(
    req: Extract<Request, { kind: K }>,
  ): Promise<Extract<Response, { kind: K }>> => {
    const serialized = JSON.stringify(req);
    const message = new SimMessage<string>(this.source, this.target, serialized, {
      requ: true,
      correlationId: this.correlationId++,
    });

    return new Promise<Extract<Response, { kind: K }>>((resolve) => {
      this.callbacks[message.head!.correlationId] = {
        req,
        resolve: resolve as (res: Response) => void,
        timeout: this.currentTime + 2000,
      };
      this.buffer.push(message);
    });
  };

  time(time: number): void {
    this.currentTime = time;

    // Then, check for timed-out callbacks
    for (const key in this.callbacks) {
      const cb = this.callbacks[key];
      const hasTimedOut = cb.timeout < this.currentTime;
      if (hasTimedOut) {
        const req = cb.req;
        const res = {
          kind: req.kind,
          head: { corrId: req.head.corrId, version: req.head.version, status: 500 },
          data: "req timed out",
        } as Response;
        cb.resolve(res);
        delete this.callbacks[key];
      }
    }
  }

  process(message: SimMessage<string>): void {
    if (message.isResponse()) {
      util.assert(message.source === this.target);
      util.assert(message.target === this.source);
      const correlationId = message.head?.correlationId;
      const entry = correlationId && this.callbacks[correlationId];
      if (entry) {
        util.assertDefined(message.data);
        const dataStr = this.maybeCorruptData(message.data);
        let parsed: unknown;
        try {
          parsed = JSON.parse(dataStr);
        } catch {
          // Corrupted data - return a synthetic error response
          const res = {
            kind: entry.req.kind,
            head: { corrId: entry.req.head.corrId, version: entry.req.head.version, status: 500 },
            data: "corrupted response",
          } as Response;
          entry.resolve(res);
          delete this.callbacks[correlationId];
          return;
        }
        if (!isResponse(parsed)) {
          const res = {
            kind: entry.req.kind,
            head: { corrId: entry.req.head.corrId, version: entry.req.head.version, status: 500 },
            data: "invalid response",
          } as Response;
          entry.resolve(res);
          delete this.callbacks[correlationId];
          return;
        }
        entry.resolve(parsed);
        delete this.callbacks[correlationId];
      }
    } else {
      util.assert(message.source.kind === this.target.kind && message.source.iaddr === this.target.iaddr);
      const dataStr = this.maybeCorruptData(message.data);
      let parsed: unknown;
      try {
        parsed = JSON.parse(dataStr);
      } catch {
        return; // discard corrupted messages
      }
      // Validate it's a proper Message
      if (typeof parsed === "object" && parsed !== null && "kind" in parsed) {
        for (const callback of this.subscribers) {
          callback(parsed as Message);
        }
      }
    }
  }

  flush(): SimMessage<any>[] {
    // Finally, flush the buffer
    const flushed = this.buffer;
    this.buffer = [];
    return flushed;
  }

  private maybeCorruptData(data: string): string {
    // Randomly decide whether to corrupt
    const shouldCorrupt = this.prng.next() < this.deliveryOptions.charFlipProb;
    if (!shouldCorrupt) return data;

    // Pick a random index to corrupt
    const idx = Math.floor(this.prng.next() * data.length);

    // Corrupt that character
    return `${data.slice(0, idx)}X${data.slice(idx + 1)}`;
  }
}

export class WorkerProcess extends Process {
  private clock: StepClock;
  private network: SimulatedNetwork;

  constructor(
    prng: Random,
    clock: StepClock,
    registry: Registry,
    { charFlipProb }: DeliveryOptions,
    public readonly iaddr: string,
    public readonly gaddr: string,
  ) {
    super(iaddr, gaddr);
    this.clock = clock;
    this.network = new SimulatedNetwork(iaddr, gaddr, prng, { charFlipProb }, unicast(iaddr), unicast("server"));
    const logger = new ConsoleLogger("error");
    const core = new Core({
      pid: iaddr,
      ttl: 10000,
      clock: this.clock,
      send: this.network.send,
      codec: new Codec(),
      registry,
      heartbeat: new NoopHeartbeat(),
      dependencies: new Map(),
      optsBuilder: new OptionsBuilder({ match: (target: string) => `sim://any@${target}`, idPrefix: "" }),
      logger,
    });
    this.network.recv((msg) => {
      core.onMessage(msg).catch(() => {});
    });
  }

  tick(tick: number, messages: SimMessage<string>[]): SimMessage<string>[] {
    this.log(tick, "[recv]", messages);

    this.network.time(this.clock.time);
    for (const message of messages) {
      this.network.process(message);
    }

    const responses = this.network.flush();

    this.log(tick, "[send]", responses);

    return responses;
  }
}
