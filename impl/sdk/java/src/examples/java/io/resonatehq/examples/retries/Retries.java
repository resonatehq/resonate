package io.resonatehq.examples.retries;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Context.Opts;
import io.resonatehq.resonate.Resonate;
import io.resonatehq.resonate.Retry.Constant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * retries shows Resonate retrying a flaky function until it succeeds.
 *
 * <p>The rule: Resonate retries <b>leaf</b> functions -- those that perform no durable op ({@code
 * ctx.run} / {@code ctx.rpc} / {@code ctx.sleep} / ...). A function that <i>does</i> perform one is a
 * workflow, recovered by replay, and is never retried.
 *
 * <p>{@code charge} is a leaf that fails twice before it succeeds. This example invokes it through
 * every entrypoint, and Resonate retries it every time:
 *
 * <ul>
 *   <li>{@code resonate.run(id, charge, ...)} -- top-level, as a root task.
 *   <li>{@code resonate.rpc(id, "charge", ...)} -- top-level, dispatched by name.
 *   <li>{@code ctx.run(charge, ...)} -- from a workflow, locally.
 *   <li>{@code ctx.rpc("charge", ...)} -- from a workflow, by name.
 * </ul>
 *
 * <p>Each path runs {@code charge} as a leaf, so each is retried. The {@code run} paths use the
 * policy {@code charge} was registered with, except {@code ctx.run} here, which sets a per-call
 * policy via {@code options}. The {@code checkout} workflow itself performs durable ops, so it is
 * never retried -- only the leaves it calls are.
 *
 * <p>Start a Resonate server on localhost:8001 first ({@code resonate dev}), then:
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.retries.Retries}</pre>
 */
public final class Retries {
    private Retries() {}

    // Retry up to 5 times with no delay between attempts (keeps the demo instant).
    static final Constant POLICY = new Constant(5, 0);

    /** A flaky service: fails the first two calls <i>per key</i>, then succeeds. */
    public static final class Gateway {
        private final Map<String, Integer> attempts = new ConcurrentHashMap<>();

        public String charge(String key, int amount) {
            int n = attempts.merge(key, 1, Integer::sum);
            System.out.printf("  [%s] attempt %d...%n", key, n);
            if (n <= 2) {
                throw new RuntimeException("timeout (attempt " + n + ")");
            }
            return "charged $" + amount;
        }
    }

    public static String charge(Context ctx, String key, int amount) {
        // A leaf -- no ctx.* durable op -- so Resonate retries it on failure.
        return ctx.getDependency(Gateway.class).charge(key, amount);
    }

    public static String checkout(Context ctx) {
        // checkout is a workflow (it calls ctx.run / ctx.rpc), so it is never retried -- but each
        // flaky leaf it invokes is.
        String viaRun = ctx.options(new Opts().withRetryPolicy(POLICY))
                .run(Retries::charge, "ctx.run", 300)
                .await();
        String viaRpc = (String) ctx.rpc("charge", "ctx.rpc", 400).await();
        return viaRun + " | " + viaRpc;
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();
        r.withDependency(new Gateway());
        r.register(Retries::checkout);
        // Registering with a policy is what makes the run-as-root-task paths (resonate.run,
        // resonate.rpc, ctx.rpc) retry: the executing worker reads it.
        r.register(Retries::charge, "charge", 1, POLICY);

        long ts = System.nanoTime();
        try {
            // Top-level: run / rpc the registered leaf directly. Each runs as a root task, retried
            // via the policy charge was registered with.
            System.out.println("resonate.run:");
            String a = r.run("retries-run-" + ts, Retries::charge, "resonate.run", 100)
                    .result();
            System.out.println("  -> " + a);

            System.out.println("resonate.rpc:");
            Object b = r.rpc("retries-rpc-" + ts, "charge", "resonate.rpc", 200).result();
            System.out.println("  -> " + b);

            // From inside a workflow: ctx.run / ctx.rpc the same leaf.
            System.out.println("ctx.run + ctx.rpc (inside the checkout workflow):");
            String c = r.run("retries-checkout-" + ts, Retries::checkout).result();
            System.out.println("  -> " + c);
        } finally {
            r.stop();
        }
    }
}
