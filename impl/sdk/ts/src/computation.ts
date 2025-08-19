import type { Heartbeat } from "heartbeat";
import { Context } from "./context";
import { Coroutine, type LocalTodo, type RemoteTodo } from "./coroutine";
import { Handler } from "./handler";
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
  private group: string;
  private network: Network;
  private handler: Handler;
  private registry: Registry;
  private heartbeat: Heartbeat;
  private processor: Processor;

  private seen: Set<string> = new Set();
  private nurseries: Map<string, Nursery> = new Map();

  constructor(
    id: string,
    pid: string,
    ttl: number,
    group: string,
    network: Network,
    registry: Registry,
    heartbeat: Heartbeat,
    processor?: Processor,
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

            if (res.kind === "claimedtask") {
              const { root, leaf } = res.message.promises;
              util.assertDefined(root);

              if (leaf) {
                this.handler.updateCache(leaf.data);
              }

              this.processClaimed({ ...task, kind: "claimed", rootPromise: root.data }, done);
            }
          },
        );
        break;
    }
  }

  private processClaimed(task: ClaimedTask, done: Callback<Status>) {
    util.assert(
      task.rootPromiseId === this.id,
      "task root promise id must match computation id",
    );

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
    this.heartbeat.startHeartbeat(this.ttl / 2);

    this.nurseries.set(
      task.rootPromise.id,
      new Nursery((nursery) => {
        const done = (err: boolean, res?: any) => {
          if (err) {
            this.nurseries.delete(task.rootPromise.id);
            return nursery.done(err);
          }

          this.network.send({ kind: "completeTask", id: task.id, counter: task.counter }, (err) => {
            this.nurseries.delete(task.rootPromise.id);
            nursery.done(err, res);
          });
        };

        if (util.isGeneratorFunction(func)) {
          this.processGenerator(nursery, func, args, done);
        } else {
          this.processFunction(this.id, new Context(), func, args, done);
        }
      }, done),
    );
  }

  private processGenerator(nursery: Nursery, func: Func, args: any[], done: (err: boolean, res?: any) => void) {
    Coroutine.exec(this.id, func, args, this.handler, (err, status) => {
      if (err) return done(err);
      util.assertDefined(status);

      switch (status.type) {
        case "completed":
          done(false, { kind: "completed", promise: status.promise });
          break;

        case "suspended":
        util.assert(
          status.todo.local.length > 0 || status.todo.remote.length > 0,
          "must be at least one todo",
        );

          // local todos
          if (status.todo.local.length > 0) {
            this.processLocalTodo(nursery, status.todo.local, done);
          }

          // remote todos
          if (status.todo.remote.length > 0) {
            this.processRemoteTodo(nursery, status.todo.remote, done);
          }
          break;
      }
    });
  }

  private processFunction(id: string, ctx: Context, func: Func, args: any[], done: (err: boolean, res?: any) => void) {
    this.processor.process(
      id,
      async () => await func(ctx, ...args),
      (res) => this.handler.completePromise(id, res, done),
    );
  }

  private processLocalTodo(nursery: Nursery, todo: LocalTodo[], done: (err: boolean, res?: any) => void) {
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

  private processRemoteTodo(nursery: Nursery, todo: RemoteTodo[], done: (err: boolean, res?: any) => void) {
    all(
      todo,
      (
        { id },
        done: Callback<
          { kind: "callback"; callback: CallbackRecord } | { kind: "promise"; promise: DurablePromiseRecord }
        >,
      ) =>
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

function all<T, U>(list: U[], func: (item: U, done: Callback<T>) => void, done: Callback<T[]>) {
  const results: T[] = new Array(list.length);

  let remaining = list.length;
  let completed = false;

  const finalize = (err: boolean) => {
    if (completed) return;
    completed = true;
    err ? done(err) : done(err, results);
  };

  list.forEach((item, index) => {
    func(item, (err, res) => {
      if (completed) return;

      if (err) return finalize(err);
      util.assertDefined(res);

      results[index] = res;
      remaining--;

      if (remaining === 0) {
        finalize(false);
      }
    });
  });
}
