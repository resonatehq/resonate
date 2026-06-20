package io.resonatehq.examples.polling;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * polling shows non-blocking progress tracking over many concurrent durable workflows using
 * {@link ResonateHandle#done()}.
 *
 * <p>Top-level {@code Resonate.run} returns <i>immediately</i> with a handle -- the durable
 * invocation runs in the background. That makes it trivial to fan several workflows out at once:
 * hand each its own id, collect the handles, and you have N independent executions in flight.
 *
 * <p>{@code handle.done()} returns a plain {@code boolean} -- synchronous, no scheduling point -- so
 * a single tick can scan every handle without committing to wait on any one of them. This example
 * dispatches three {@code renderFrame} workflows of different weights, then loops printing a
 * dashboard every 250ms until all three are done, then harvests results with {@code result()}.
 */
public final class Polling {
    private Polling() {}

    // -- Leaf steps (each prints once, settles once) ---------------------------

    public static String shade(Context ctx, String frame, int ms) {
        // Pretend this is GPU work; the sleep stands in for I/O or compute that takes a while.
        sleep(ms);
        System.out.printf("  [shade]    frame=%s ms=%d%n", frame, ms);
        return "shaded-" + frame;
    }

    public static String encode(Context ctx, String shaded) {
        sleep(50);
        System.out.println("  [encode]   " + shaded);
        return shaded.replace("shaded-", "encoded-");
    }

    // -- Orchestrator ----------------------------------------------------------

    public static String renderFrame(Context ctx, String frame, int ms) {
        String shaded = ctx.run(Polling::shade, frame, ms).await();
        return ctx.run(Polling::encode, shaded).await();
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();
        r.register(Polling::renderFrame);

        try {
            // Fan out: three workflows of different weights, all dispatched up front.
            long batch = System.nanoTime();
            Map<String, Integer> jobs = new LinkedHashMap<>();
            jobs.put("frame-1", 200);
            jobs.put("frame-2", 600);
            jobs.put("frame-3", 400);

            Map<String, ResonateHandle<String>> handles = new LinkedHashMap<>();
            jobs.forEach((frame, ms) ->
                    handles.put(frame, r.run("render-" + batch + "-" + frame, Polling::renderFrame, frame, ms)));
            System.out.println("[polling] dispatched " + handles.size() + " render workflows");

            // Non-blocking progress dashboard. handle.done() is synchronous, so a single tick can
            // scan every handle without committing to wait on any one of them.
            int tick = 0;
            while (true) {
                Map<String, Boolean> states = new LinkedHashMap<>();
                handles.forEach((frame, handle) -> states.put(frame, handle.done()));
                long doneCount =
                        states.values().stream().filter(Boolean::booleanValue).count();
                StringBuilder bar = new StringBuilder();
                states.forEach((frame, done) ->
                        bar.append(frame).append('=').append(done ? "✓" : "…").append(' '));
                System.out.printf(
                        "[polling] tick=%2d  %d/%d  %s%n",
                        tick, doneCount, handles.size(), bar.toString().trim());
                if (doneCount == handles.size()) {
                    break;
                }
                sleep(250);
                tick++;
            }

            // Every handle is done: harvest the results.
            for (Map.Entry<String, ResonateHandle<String>> e : handles.entrySet()) {
                String result = e.getValue().result();
                System.out.printf("[polling] %s -> %s%n", e.getKey(), result);
                assert result.equals("encoded-" + e.getKey());
            }
        } finally {
            r.stop();
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
