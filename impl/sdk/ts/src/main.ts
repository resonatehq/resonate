import * as context from "./context";
import { ResonateInner } from "./resonate-inner";

function* bar(ctx: context.Context, name: string) {
  return `Hello World ${name}`;
}

function* foo(ctx: context.Context) {
  const p = yield* context.lfi((ctx) => {
    const a = 24;
    const b = 24;
    return a + b;
  });

  const p2 = yield* context.lfi((ctx) => {
    return 39;
  });

  const p3 = yield* context.rfi("bar", "Andres");

  const v1 = yield* p;
  const v3 = yield* p3;
  const v2 = yield* p2;

  return { v1, v2, v3 };
}

const resonate = ResonateInner.remote({});

const rfoo = resonate.register("foo", foo);
resonate.register("bar", bar);

rfoo.run("foo.1", [], async (iHandler) => {
  const res = await iHandler.result;
  console.log("got", res);
});
