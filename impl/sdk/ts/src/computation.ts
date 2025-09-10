import type { ClaimedTask, Task } from "resonate-inner";
import type { Clock } from "./clock";
import { InnerContext } from "./context";
import { Coroutine, type LocalTodo, type RemoteTodo } from "./coroutine";
import type { Handler } from "./handler";
import type { Heartbeat } from "./heartbeat";
import type { CallbackRecord, DurablePromiseRecord, Network } from "./network/network";
import { Nursery } from "./nursery";
import { AsyncProcessor, type Processor } from "./processor/processor";
import type { Registry } from "./registry";
import type { Callback, Func } from "./types";
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
  private unicast: string;
  private anycast: string;
  private network: Network;
  private handler: Handler;
  private registry: Registry;
  private dependencies: Map<string, any>;
  private heartbeat: Heartbeat;
  private processor: Processor;

  private seen: Set<string> = new Set();
  private processing = false;

  constructor(
    id: string,
    unicast: string,
    anycast: string,
    pid: string,
    ttl: number,
    clock: Clock,
    network: Network,
    handler: Handler,
    registry: Registry,
    heartbeat: Heartbeat,
    dependencies: Map<string, any>,
    processor?: Processor,
  ) {
    this.id = id;
    this.unicast = unicast;
    this.anycast = anycast;
    this.pid = pid;
    this.ttl = ttl;
    this.clock = clock;
    this.network = network;
    this.handler = handler;
    this.registry = registry;
    this.heartbeat = heartbeat;
    this.dependencies = dependencies;
    this.processor = processor ?? new AsyncProcessor();
  }

  public process(task: Task, done: Callback<Status>) {
    // If we are already processing there is nothing to do, the
    // caller will be notified via the promise handler
    if (this.processing) return done(true);
    this.processing = true;

    const doneProcessing = (err: boolean, res?: Status) => {
      this.processing = false;
      err ? done(err) : done(err, res!);
    };

    switch (task.kind) {
      case "claimed":
        this.processClaimed(task, doneProcessing);
        break;

      case "unclaimed":
        this.handler.claimTask(
          {
            kind: "claimTask",
            id: task.task.id,
            counter: task.task.counter,
            processId: this.pid,
            ttl: this.ttl,
          },
          (err, promise) => {
            if (err) return doneProcessing(true);
            util.assertDefined(promise);

            this.processClaimed({ kind: "claimed", task: task.task, rootPromise: promise }, doneProcessing);
          },
        );
        break;
    }
  }

  private processClaimed({ task, rootPromise }: ClaimedTask, done: Callback<Status>) {
    util.assert(task.rootPromiseId === this.id, "task root promise id must match computation id");

    const doneAndDropTaskIfErr = (err?: boolean, res?: Status) => {
      if (err) {
        return this.network.send({ kind: "dropTask", id: task.id, counter: task.counter }, () => {
          // ignore the drop task response, if the request failed the
          // task will eventually expire anyways
          done(true);
        });
      }

      done(false, res!);
    };

    if (!("func" in rootPromise.param) || !("args" in rootPromise.param)) {
      // TODO: log something useful
      return doneAndDropTaskIfErr(true);
    }

    const registered = this.registry.get(rootPromise.param.func);
    if (!registered) {
      // TODO: log something useful
      return doneAndDropTaskIfErr(true);
    }

    const func = registered.func;
    const args = rootPromise.param.args;

    // start heartbeat
    this.heartbeat.start();

    return new Nursery<boolean, Status>((nursery) => {
      const done = (err: boolean, res?: Status) => {
        if (err) {
          return nursery.done(err);
        }

        this.network.send({ kind: "completeTask", id: task.id, counter: task.counter }, (err) => {
          nursery.done(err, res);
        });
      };

      const ctx = InnerContext.root(this.id, rootPromise.timeout, this.clock, this.dependencies);

      if (util.isGeneratorFunction(func)) {
        this.processGenerator(nursery, ctx, func, args, done);
      } else {
        this.processFunction(this.id, ctx, func, args, (err, promise) => {
          if (err) return done(true);
          util.assertDefined(promise);

          done(false, { kind: "completed", promise });
        });
      }
    }, doneAndDropTaskIfErr);
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
            this.processRemoteTodo(nursery, status.todo.remote, ctx.timeout, done);
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
      (res) =>
        this.handler.completePromise(
          {
            kind: "completePromise",
            id: id,
            state: res.success ? "resolved" : "rejected",
            value: res.success ? res.value : res.error,
            iKey: id,
            strict: false,
          },
          done,
        ),
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

  private processRemoteTodo(
    nursery: Nursery<boolean, Status>,
    todo: RemoteTodo[],
    timeout: number,
    done: Callback<Status>,
  ) {
    nursery.all<
      RemoteTodo,
      { kind: "callback"; callback: CallbackRecord } | { kind: "promise"; promise: DurablePromiseRecord },
      boolean
    >(
      todo,
      ({ id }, done) =>
        this.handler.createCallback(
          {
            kind: "createCallback",
            promiseId: id,
            rootPromiseId: this.id,
            timeout: timeout,
            recv: this.anycast,
          },
          done,
        ),
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
