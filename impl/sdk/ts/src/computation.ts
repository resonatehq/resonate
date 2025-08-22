import { type Clock, WallClock } from "./clock";
import { InnerContext } from "./context";
import { Coroutine, type LocalTodo, type RemoteTodo } from "./coroutine";
import { Handler } from "./handler";
import type { Heartbeat } from "./heartbeat";
import type { CallbackRecord, DurablePromiseRecord, Network } from "./network/network";
import { Nursery } from "./nursery";
import { AsyncProcessor, type Processor } from "./processor/processor";
import type { Registry } from "./registry";
import type { Callback, ClaimedTask, Func, Task } from "./types";
import * as util from "./util";

export type Status = Completed | Suspended;

export type Completed = {
  kind: "completed";
  promise: DurablePromiseRecord;
};

export type Suspended = {
  kind: "suspended";
  callbacks: CallbackRecord[];
};

export class Computation {
  private id: string;
  private pid: string;
  private ttl: number;
  private clock: Clock;
  private group: string;
  private network: Network;
  private handler: Handler;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private processor: Processor;

  private seen: Set<string> = new Set();
  private nurseries: Map<string, Nursery<boolean, Status>> = new Map();

  constructor(
    id: string,
    pid: string,
    ttl: number,
    group: string,
    network: Network,
    registry: Registry,
    heartbeat: Heartbeat,
    processor?: Processor,
    clock?: Clock,
  ) {
    this.id = id;
    this.pid = pid;
    this.ttl = ttl;
    this.group = group;
    this.network = network;
    this.handler = new Handler(network);
    this.registry = registry;
    this.heartbeat = heartbeat;
    this.processor = processor ?? new AsyncProcessor();
    this.clock = clock ?? new WallClock();
  }

  public process(task: Task, done: Callback<Status>) {
    switch (task.kind) {
      case "claimed":
        this.processClaimed(task, done);
        break;

      case "unclaimed":
        this.network.send(
          {
            kind: "claimTask",
            id: task.id,
            counter: task.counter,
            processId: this.pid,
            ttl: this.ttl,
          },
          (err, res) => {
            if (err) return done(true);
            util.assertDefined(res);

            const { root, leaf } = res.message.promises;
            util.assertDefined(root);

            if (leaf) {
              this.handler.updateCache(leaf.data);
            }

            this.processClaimed({ ...task, kind: "claimed", rootPromise: root.data }, done);
          },
        );
        break;
    }
  }

  private processClaimed(task: ClaimedTask, done: Callback<Status>) {
    util.assert(task.rootPromiseId === this.id, "task root promise id must match computation id");

    if (this.nurseries.has(task.rootPromise.id)) {
      // TODO: log something useful
      return done(true);
    }

    if (!("func" in task.rootPromise.param) || !("args" in task.rootPromise.param)) {
      // TODO: log something useful
      return done(true);
    }

    const registered = this.registry.get(task.rootPromise.param.func);
    if (!registered) {
      // TODO: log something useful
      return done(true);
    }

    const func = registered.func;
    const args = task.rootPromise.param.args;

    // TODO: investigate if it is possible to update a completed
    // promise with a pending promise
    this.handler.updateCache(task.rootPromise);

    // start heartbeat
    this.heartbeat.start();

    this.nurseries.set(
      task.rootPromise.id,
      new Nursery<boolean, Status>(
        (nursery) => {
          const done = (err: boolean, res?: Status) => {
            if (err) {
              this.nurseries.delete(task.rootPromise.id);
              return nursery.done(err);
            }

            this.network.send({ kind: "completeTask", id: task.id, counter: task.counter }, (err) => {
              this.nurseries.delete(task.rootPromise.id);
              nursery.done(err, res);
            });
          };

          const ctx = InnerContext.root(this.id, this.clock);
          if (util.isGeneratorFunction(func)) {
            this.processGenerator(nursery, ctx, func, args, done);
          } else {
            this.processFunction(this.id, ctx, func, args, (err, promise) => {
              if (err) return done(true);
              util.assertDefined(promise);

              done(false, { kind: "completed", promise });
            });
          }
        },
        (err, res) => {
          if (err) return done(true);
          util.assertDefined(res);
          done(false, res);
        },
      ),
    );
  }

  private processGenerator(
    nursery: Nursery<boolean, Status>,
    ctx: InnerContext,
    func: Func,
    args: any[],
    done: Callback<Status>,
  ) {
    Coroutine.exec(this.id, ctx, func, args, this.handler, (err, status) => {
      if (err) return done(err);
      util.assertDefined(status);

      switch (status.type) {
        case "completed":
          done(false, { kind: "completed", promise: status.promise });
          break;

        case "suspended":
          util.assert(status.todo.local.length > 0 || status.todo.remote.length > 0, "must be at least one todo");

          if (status.todo.local.length > 0) {
            this.processLocalTodo(nursery, status.todo.local, done);
          } else if (status.todo.remote.length > 0) {
            this.processRemoteTodo(nursery, status.todo.remote, done);
          }
          break;
      }
    });
  }

  private processFunction(
    id: string,
    ctx: InnerContext,
    func: Func,
    args: any[],
    done: Callback<DurablePromiseRecord>,
  ) {
    this.processor.process(
      id,
      async () => await func(ctx, ...args),
      (res) => this.handler.completePromise(id, res, done),
    );
  }

  private processLocalTodo(nursery: Nursery<boolean, Status>, todo: LocalTodo[], done: Callback<Status>) {
    for (const { id, ctx, func, args } of todo) {
      if (this.seen.has(id)) {
        continue;
      }

      this.seen.add(id);

      nursery.hold((next) => {
        this.processFunction(id, ctx, func, args, (err) => {
          if (err) return done(err);
          next();
        });
      });
    }

    // once all local todos are submitted we can call continue
    return nursery.cont();
  }

  private processRemoteTodo(nursery: Nursery<boolean, Status>, todo: RemoteTodo[], done: Callback<Status>) {
    nursery.all<
      RemoteTodo,
      { kind: "callback"; callback: CallbackRecord } | { kind: "promise"; promise: DurablePromiseRecord },
      boolean
    >(
      todo,
      ({ id }, done) =>
        this.handler.createCallback(id, this.id, Number.MAX_SAFE_INTEGER, `poll://any@${this.group}/${this.pid}`, done),
      (err, results) => {
        if (err) return done(err);
        util.assertDefined(results);

        const callbacks: CallbackRecord[] = [];

        for (const res of results) {
          switch (res.kind) {
            case "promise":
              nursery.hold((next) => next());
              return nursery.cont();

            case "callback":
              callbacks.push(res.callback);
              break;
          }
        }

        // once all callbacks are created we can call done
        return done(false, { kind: "suspended", callbacks });
      },
    );
  }
}
