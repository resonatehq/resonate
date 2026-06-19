package io.resonatehq.examples.fibonacci;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Context.ResonateFuture;
import io.resonatehq.resonate.Fn;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * fibonacci shows three ways to compose recursive durable invocations with the Resonate SDK:
 *
 * <ul>
 *   <li>{@code --mode rpc} every recursive call goes through {@code ctx.rpc} (server-dispatched, may
 *       execute on any worker in the group)
 *   <li>{@code --mode run} every recursive call goes through {@code ctx.run} (local task, same worker)
 *   <li>{@code --mode mix} one branch via rpc, the other via run
 * </ul>
 *
 * <p>Start a Resonate server on localhost:8001 first ({@code resonate dev}), then e.g.:
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.fibonacci.Fibonacci \
 *     -PexampleArgs="--mode rpc --n 10"}</pre>
 */
public final class Fibonacci {
    private Fibonacci() {}

    static int fib(int n) {
        if (n <= 1) {
            return n;
        }
        return fib(n - 1) + fib(n - 2);
    }

    public static int fibRpc(Context ctx, int n) {
        if (n < 2) {
            return n;
        }
        ResonateFuture<Object> f1 = ctx.rpc("fibRpc", n - 1);
        ResonateFuture<Object> f2 = ctx.rpc("fibRpc", n - 2);
        return (Integer) f1.await() + (Integer) f2.await();
    }

    public static int fibRun(Context ctx, int n) {
        if (n < 2) {
            return n;
        }
        ResonateFuture<Integer> f1 = ctx.run(Fibonacci::fibRun, n - 1);
        ResonateFuture<Integer> f2 = ctx.run(Fibonacci::fibRun, n - 2);
        return f1.await() + f2.await();
    }

    public static int fibMix(Context ctx, int n) {
        if (n < 2) {
            return n;
        }
        ResonateFuture<Object> f1 = ctx.rpc("fibMix", n - 1);
        ResonateFuture<Integer> f2 = ctx.run(Fibonacci::fibMix, n - 2);
        return (Integer) f1.await() + f2.await();
    }

    public static void main(String[] args) {
        String mode = argValue(args, "--mode", "run");
        int n = Integer.parseInt(argValue(args, "--n", "10"));

        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();
        r.register(Fibonacci::fibRpc);
        r.register(Fibonacci::fibRun);
        r.register(Fibonacci::fibMix);
        Map<String, Fn.F1<Integer, Integer>> fns = new LinkedHashMap<>();
        fns.put("rpc", Fibonacci::fibRpc);
        fns.put("run", Fibonacci::fibRun);
        fns.put("mix", Fibonacci::fibMix);

        try {
            String id = "fib-" + mode + "-" + n;
            ResonateHandle<Integer> handle = r.run(id, fns.get(mode), n);
            int out = handle.result();
            assert out == fib(n);
            ResonateHandle<Object> got = r.get(id).join();
            boolean done = got.done();
            assert done;
            System.out.printf("fib(%d) = %d  [mode=%s]%n", n, out, mode);
        } catch (RuntimeException e) {
            System.out.println("oops " + e);
        } finally {
            r.stop().join();
        }
    }

    /** Minimal {@code --flag value} parser. */
    private static String argValue(String[] args, String flag, String fallback) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return fallback;
    }
}
