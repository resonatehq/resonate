package io.resonatehq.examples.helloworld;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;

/**
 * hello is a minimal example of using the Resonate SDK.
 *
 * <p>It registers a function, invokes it durably against a Resonate server, and prints the result.
 *
 * <p>Start a Resonate server on localhost:8001 first ({@code resonate dev}), then:
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.helloworld.HelloWorld}</pre>
 */
public final class HelloWorld {
    private HelloWorld() {}

    public static String foo(Context ctx, String name) {
        return ctx.run(HelloWorld::bar, name).await();
    }

    public static String bar(Context ctx, String name) {
        return (String) ctx.rpc("baz", name).await();
    }

    public static String baz(Context ctx, String name) {
        System.out.println("foo");
        return "hello, " + name + "!";
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();
        r.register(HelloWorld::foo);
        r.register(HelloWorld::baz);
        try {
            String id = "hello-" + System.nanoTime();
            ResonateHandle<String> handle = r.run(id, HelloWorld::foo, "world");
            String ok = handle.result();
            assert ok.equals("hello, world!");
            System.out.println(ok);
        } finally {
            r.stop().join();
        }
    }
}
