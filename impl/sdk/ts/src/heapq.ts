/**
 * Heap queue algorithm (a.k.a. priority queue).
 *
 * Inspired by heapq.py from CPython.
 *
 * See https://github.com/python/cpython/blob/master/Lib/heapq.py
 */
export class Heapq<T> {
  heap: Array<T>;
  comparator: (a: T, b: T) => boolean;

  constructor(heap: Array<T> = [], comparator: (a: T, b: T) => boolean = (a: T, b: T) => a < b) {
    this.heap = heap;
    this.comparator = comparator;
    this.heapify();
  }

  /**
   * Pushes item to the heap queue.
   */
  push(item: T) {
    this.heap.push(item);
    this.siftdown(0, this.heap.length - 1);
  }

  /**
   * Pop current smallest item from heap queue.
   * Throw exception if heap queue is empty.
   */
  pop(): T {
    if (this.heap.length === 0) {
      throw new Error("Heap is empty");
    }

    const last = this.heap[this.heap.length - 1];
    const returnItem: T = this.heap[0];

    this.heap.pop();

    this.heap[0] = last;
    this.siftup(0);
    return returnItem;
  }

  /**
   * Pop the current smallest elemtn and add new item to heap queue.
   */
  replace(item: T): T {
    const returnItem: T = this.heap[0];
    this.heap[0] = item;
    this.siftup(0);

    return item;
  }

  /**
   * Pushes new item to heap queue and then pop the smallest one.
   */
  pushPop(item: T): T {
    if (this.heap.length && this.comparator(this.heap[0], item)) {
      [item, this.heap[0]] = [this.heap[0], item];
      this.siftup(0);
    }

    return item;
  }

  /**
   * Returns current smallest item in heap queue (does not pop it).
   */
  top(): T {
    return this.heap[0];
  }

  /**
   * Returns the length of the heap.
   */
  length(): number {
    return this.heap.length;
  }

  private heapify() {
    const n: number = this.heap.length;
    for (let i: number = n / 2; i >= 0; i--) {
      this.siftup(i);
    }
  }

  private siftdown(startPos: number, pos: number) {
    const newItem: T = this.heap[pos];
    if (newItem === undefined) {
      return;
    }

    while (pos > startPos) {
      let parentPos: number = (pos - 1) >> 1;
      let parent: T = this.heap[parentPos];
      if (this.comparator(newItem, parent)) {
        this.heap[pos] = parent;
        pos = parentPos;
        continue;
      }

      break;
    }

    this.heap[pos] = newItem;
  }

  private siftup(pos: number) {
    const endPos: number = this.heap.length;
    const startPos: number = pos;
    const newItem = this.heap[pos];
    if (newItem === undefined) {
      return;
    }

    let childPos: number = 2 * pos + 1;
    while (childPos < endPos) {
      let rightPos = childPos + 1;
      if (rightPos < endPos && !this.comparator(this.heap[childPos], this.heap[rightPos])) {
        childPos = rightPos;
      }

      this.heap[pos] = this.heap[childPos];
      pos = childPos;
      childPos = 2 * pos + 1;
    }

    this.heap[pos] = newItem;
    this.siftdown(startPos, pos);
  }
}
