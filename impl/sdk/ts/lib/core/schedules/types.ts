export type Schedule = {
  id: string;
  description: string | undefined;
  cron: string;
  tags: Record<string, string> | undefined;
  promiseId: string;
  promiseTimeout: number;
  promiseParam: {
    data: string | undefined;
    headers: Record<string, string> | undefined;
  };
  promiseTags: Record<string, string> | undefined;
  lastRunTime: number | undefined;
  nextRunTime: number;
  idempotencyKey: string | undefined;
  createdOn: number;
};

export function isSchedule(s: unknown): s is Schedule {
  return s !== null && typeof s === "object" && "id" in s;
}
