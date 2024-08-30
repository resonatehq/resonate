import assert from "assert";
import { IEncoder } from "../encoder";
import { ErrorCodes, ResonateError } from "../errors";

export type DurablePromiseRecord = {
  state: "PENDING" | "RESOLVED" | "REJECTED" | "REJECTED_CANCELED" | "REJECTED_TIMEDOUT";
  id: string;
  timeout: number;
  param: {
    headers: Record<string, string> | undefined;
    data: string | undefined;
  };
  value: {
    headers: Record<string, string> | undefined;
    data: string | undefined;
  };
  createdOn: number;
  completedOn: number | undefined;
  idempotencyKeyForCreate: string | undefined;
  idempotencyKeyForComplete: string | undefined;
  tags: Record<string, string> | undefined;
};

// This is an unsound type guard, we should be more strict in what we call a DurablePromise
export function isDurablePromiseRecord(p: unknown): p is DurablePromiseRecord {
  return (
    p !== null &&
    typeof p === "object" &&
    "state" in p &&
    typeof p.state === "string" &&
    ["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"].includes(p.state)
  );
}

export function isPendingPromise(p: DurablePromiseRecord): boolean {
  return p.state === "PENDING";
}

export function isResolvedPromise(p: DurablePromiseRecord): boolean {
  return p.state === "RESOLVED";
}

export function isRejectedPromise(p: DurablePromiseRecord): boolean {
  return p.state === "REJECTED";
}

export function isCanceledPromise(p: DurablePromiseRecord): boolean {
  return p.state === "REJECTED_CANCELED";
}

export function isTimedoutPromise(p: DurablePromiseRecord): boolean {
  return p.state === "REJECTED_TIMEDOUT";
}

export function isCompletedPromise(p: DurablePromiseRecord): boolean {
  return ["RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"].includes(p.state);
}

/**
 * Handles a completed durable promise and returns its result.
 *
 * @param p - The DurablePromiseRecord to handle.
 * @param encoder - An IEncoder instance used to decode the promise's data.
 * @returns The decoded result of the promise if resolved, or null if pending.
 * @throws {ResonateError} If the promise was rejected, canceled, or timed out.
 *
 * @remarks
 * Users must handle the null return case, which could indicate either:
 * 1. The promise is still pending, or
 * 2. The promise completed with a null value.
 * It's important to distinguish between these cases in the calling code if necessary.
 */
export function handleCompletedPromise<R>(p: DurablePromiseRecord, encoder: IEncoder<unknown, string | undefined>): R {
  assert(p.state !== "PENDING", "Promise was pending when trying to handle its completion");
  switch (p.state) {
    case "RESOLVED":
      return encoder.decode(p.value.data) as R;
    case "REJECTED":
      throw encoder.decode(p.value.data);
    case "REJECTED_CANCELED":
      throw new ResonateError("Resonate function canceled", ErrorCodes.CANCELED, encoder.decode(p.value.data));
    case "REJECTED_TIMEDOUT":
      throw new ResonateError(
        `Resonate function timedout at ${new Date(p.timeout).toISOString()}`,
        ErrorCodes.TIMEDOUT,
      );
  }
}
