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
