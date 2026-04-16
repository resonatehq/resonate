import type { Message, Request, Response } from "./types.js";

export type Send = <K extends Request["kind"]>(
  req: Extract<Request, { kind: K }>,
) => Promise<Extract<Response, { kind: K }>>;

export type Recv = (callback: (msg: Message) => void) => void;

export interface Network {
  readonly unicast: string;
  readonly anycast: string;

  match(target: string): string;

  init(): Promise<void>;
  stop(): Promise<void>;

  send: Send;
  recv: Recv;
}
