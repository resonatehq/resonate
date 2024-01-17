export interface ILock {
  tryAcquire(id: string, pid: string, eid: string): boolean;
  release(id: string, eid: string): void;
}
