
import { Context } from "../../../lib/resonate";

export async function foo(c : Context, f : number) : Promise<number> {

    const a = await c.run(bar, 1);
    const b = await c.run(baz, 2);

    return a + b;
}

export async function bar(c : Context, f : number) : Promise<number> {

    if(c.attempt < 2) {
        throw new Error("bar");
    }

    return f;
}

export async function baz(c : Context, f : number) : Promise<number> {

    if(c.attempt < 2) {
        throw new Error("baz");
    }

    return f;
}

export async function que(c : Context, f : number) : Promise<number> {

    if(c.attempt < 10000) {
        throw new Error("que");
    }

    return 42;
}
