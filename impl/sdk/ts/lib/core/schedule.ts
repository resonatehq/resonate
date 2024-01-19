export type Schedule = {
  id: string;
  description?: string;
  cron: string;
  tags?: Record<string, string>;
  promiseId: string;
  promiseTimeout: number;
  promiseParam?: {
    data?: string;
    headers: Record<string, string>;
  };
  promiseTags?: Record<string, string>;
  lastRunTime?: number;
  nextRunTime?: number;
  idempotencyKey?: string;
  createdOn?: number;
};

// Function to check if the response matches the Schedule type
export function isSchedule(obj: any): obj is Schedule {
  // You may need to adjust this based on the actual structure of your Schedule type
  return (
    obj !== undefined &&
    typeof obj.id === "string" &&
    typeof obj.cron === "string" &&
    typeof obj.promiseId === "string" &&
    typeof obj.promiseTimeout === "number"
  );
}
