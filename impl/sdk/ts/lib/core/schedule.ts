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

export function isSchedule(obj: any): obj is Schedule {
  return (
    obj !== undefined &&
    typeof obj.id === "string" &&
    typeof obj.cron === "string" &&
    typeof obj.promiseId === "string" &&
    typeof obj.promiseTimeout === "number"
  );
}
