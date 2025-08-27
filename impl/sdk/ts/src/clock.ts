export interface Clock {
  now(): number;
}

export class WallClock {
  now(): number {
    return Date.now();
  }
}

export class StepClock {
  public time = 0;
  now(): number {
    return this.time++;
  }
}
