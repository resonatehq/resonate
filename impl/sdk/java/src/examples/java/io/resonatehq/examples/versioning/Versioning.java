package io.resonatehq.examples.versioning;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Context.Opts;
import io.resonatehq.resonate.Resonate;

/**
 * versioning shows how to run several versions of one function side by side.
 *
 * <p>The registry is keyed on {@code (name, version)}, so the <i>same</i> function name can have
 * multiple implementations registered at once -- the bread and butter of a rolling deploy, where
 * in-flight work must keep running the version it started on while new work picks up the new code.
 *
 * <p>The version is <b>explicit, never "latest"</b>. That is what makes replay deterministic: a
 * durable promise records its version at create time, so when an orchestrator suspends and replays
 * -- or another worker picks the task up -- it resolves the <i>same</i> implementation every time,
 * no matter what has been registered since.
 *
 * <p>Two ways to pick the version, depending on how you dispatch:
 *
 * <ul>
 *   <li>{@code run(id, fn, ...)} takes a function <b>object</b>, so the version is whatever {@code fn}
 *       was registered as -- recovered by identity. {@code options(version=)} does NOT apply here; the
 *       object already implies the version.
 *   <li>{@code rpc(id, "name", ...)} dispatches by <b>name string</b>, so the version comes from
 *       {@code options(version=)} (default {@code 1}). This is also the path that runs a function that
 *       need not be registered in <i>this</i> process.
 * </ul>
 *
 * <p>Start a Resonate server on localhost:8001 first ({@code resonate dev}), then:
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.versioning.Versioning}</pre>
 */
public final class Versioning {
    private Versioning() {}

    public static double chargeV1(Context ctx, double amount) {
        // v1: charge the amount as-is.
        return amount;
    }

    public static double chargeV2(Context ctx, double amount) {
        // v2: the billing rules changed -- add a 3% processing fee.
        return Math.round(amount * 1.03 * 100.0) / 100.0;
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();

        // Same name "charge", two coexisting implementations.
        r.register(Versioning::chargeV1, "charge", 1);
        r.register(Versioning::chargeV2, "charge", 2);

        long ts = System.nanoTime();
        try {
            // run() -- version is taken from the function OBJECT you hand it.
            double v1 =
                    r.run("charge-run-v1-" + ts, Versioning::chargeV1, 100.0).result();
            double v2 =
                    r.run("charge-run-v2-" + ts, Versioning::chargeV2, 100.0).result();
            assert v1 == 100;
            assert v2 == 103;
            System.out.println("run  charge_v1(100) = " + v1); // 100.0
            System.out.println("run  charge_v2(100) = " + v2); // 103.0

            // rpc() -- dispatched by NAME; version comes from options (default 1).
            Object rpcV1 = r.rpc("charge-rpc-v1-" + ts, "charge", 100.0).result();
            Object rpcV2 = r.options(new Opts().withVersion(2))
                    .rpc("charge-rpc-v2-" + ts, "charge", 100.0)
                    .result();
            assert ((Number) rpcV1).doubleValue() == 100;
            assert ((Number) rpcV2).doubleValue() == 103;
            System.out.println("rpc  charge v1 (100) = " + rpcV1); // 100.0 -- default version 1
            System.out.println("rpc  charge v2 (100) = " + rpcV2); // 103.0 -- selected via options
        } finally {
            r.stop();
        }
    }
}
