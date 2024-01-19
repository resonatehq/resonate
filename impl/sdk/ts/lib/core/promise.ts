export type DurablePromise = PendingPromise | ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise;

export type PendingPromise = {
  state: "PENDING";
  id: string;
  timeout: number;
  param: {
    headers: Record<string, string> | undefined;
    data: string | undefined;
  };
  value: {
    headers: undefined;
    data: undefined;
  };
  createdOn: number;
  completedOn: undefined;
  idempotencyKeyForCreate?: string;
  idempotencyKeyForComplete?: undefined;
  tags?: Record<string, string>;
};

export type ResolvedPromise = {
  state: "RESOLVED";
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
  completedOn: number;
  idempotencyKeyForCreate?: string;
  idempotencyKeyForComplete?: string;
  tags?: Record<string, string>;
};

export type RejectedPromise = {
  state: "REJECTED";
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
  completedOn: number;
  idempotencyKeyForCreate?: string;
  idempotencyKeyForComplete?: string;
  tags?: Record<string, string>;
};

export type CanceledPromise = {
  state: "REJECTED_CANCELED";
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
  completedOn: number;
  idempotencyKeyForCreate?: string;
  idempotencyKeyForComplete?: string;
  tags?: Record<string, string>;
};

export type TimedoutPromise = {
  state: "REJECTED_TIMEDOUT";
  id: string;
  timeout: number;
  param: {
    headers: Record<string, string> | undefined;
    data: string | undefined;
  };
  value: {
    headers: undefined;
    data: undefined;
  };
  createdOn: number;
  completedOn: number;
  idempotencyKeyForCreate?: string;
  idempotencyKeyForComplete?: undefined;
  tags?: Record<string, string>;
};

export function isDurablePromise(p: unknown): p is DurablePromise {
  if (
    p != null &&
    typeof p === "object" &&
    "state" in p &&
    typeof p.state === "string" &&
    ["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"].includes(p.state)
  ) {
    return true;
  }

  return false;
}

export function isPendingPromise(p: unknown): p is PendingPromise {
  return isDurablePromise(p) && p.state === "PENDING";
}

export function isResolvedPromise(p: unknown): p is ResolvedPromise {
  return isDurablePromise(p) && p.state === "RESOLVED";
}

export function isRejectedPromise(p: unknown): p is RejectedPromise {
  return isDurablePromise(p) && p.state === "REJECTED";
}

export function isCanceledPromise(p: unknown): p is CanceledPromise {
  return isDurablePromise(p) && p.state === "REJECTED_CANCELED";
}

export function isTimedoutPromise(p: unknown): p is TimedoutPromise {
  return isDurablePromise(p) && p.state === "REJECTED_TIMEDOUT";
}

export function isCompletedPromise(
  p: unknown,
): p is ResolvedPromise | RejectedPromise | CanceledPromise | TimedoutPromise {
  return isDurablePromise(p) && ["RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"].includes(p.state);
}

export function searchStates(state: string | undefined): string[] {
  if (state?.toLowerCase() == "pending") {
    return ["PENDING"];
  } else if (state?.toLowerCase() == "resolved") {
    return ["RESOLVED"];
  } else if (state?.toLowerCase() == "rejected") {
    return ["REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"];
  } else {
    return ["PENDING", "RESOLVED", "REJECTED", "REJECTED_CANCELED", "REJECTED_TIMEDOUT"];
  }
}
