import { ILock } from "../lock";

export class LocalLock implements ILock {
  private locks: Record<string, { pid: string; eid: string }> = {};

  tryAcquire(id: string, pid: string, eid: string): boolean {
    if (!this.locks[id] || (this.locks[id] && this.locks[id].eid === eid)) {
      // Lock is available, acquire it
      this.locks[id] = { pid, eid };
      return true;
    } else {
      // Lock is already acquired
      return false;
    }
  }

  release(id: string, eid: string): void {
    if (this.locks[id] && this.locks[id].eid === eid) {
      // Release the lock
      delete this.locks[id];
    } else {
      // Lock was not acquired
      throw new Error(`Lock with ID '${id}' not found.`);
    }
  }
}
