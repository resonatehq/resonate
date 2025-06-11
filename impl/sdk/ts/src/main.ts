import { invoke, rpc } from "./context";

import { Computation } from "./computation";
import { Handler } from "./handler";

function* foo(x: number) {
  const p1 = yield* invoke(bar, x);
  const p2 = yield* invoke(bar, x);
  const p3 = yield* invoke(baz, 3);
  const v1 = yield* p1;
  const v2 = yield* p2;
  const v3 = yield* p3;
  return v1 + v2 + v3;
}

function* bar(x: number) {
  const p = yield* rpc(baz, x);
  const b = yield* invoke(bazooka, 5);
  const v = yield* p;
  return v;
}

function* baz(x: number) {
  return x * 2;
}

function* bazooka(x: number) {
  return x * x;
}

const h = new Handler([]);

let c = Computation.exec("foo.1", foo, [1], h);

console.log("first run");
console.log(c, h);
if (c.type == "suspended") {
  c.todos.forEach((val) => h.resolvePromise(val.uuid, 5));
}

console.log("second run");
c = Computation.exec("foo.1", foo, [1], h);
console.log(c, h);
