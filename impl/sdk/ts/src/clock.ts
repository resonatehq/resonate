export interface Clock {
  now(): number;
}

export class WallClock {
  now(): number {
    return Date.now();
  }
}
