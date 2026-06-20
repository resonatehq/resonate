package io.resonatehq.examples.recovery;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Resonate;
import java.util.List;
import java.util.Map;

/**
 * recovery shows typed serialize/deserialize across the durability boundary.
 *
 * <p>Every value a durable function exchanges -- its arguments and its return -- is written to a
 * durable promise as JSON. The interesting claim is that <b>recovery needs no special path</b>: the
 * same (de)serialization runs on every invocation, so a value that is rebuilt after a crash is
 * rebuilt by the exact steps that ran the first time.
 *
 * <p>On every call the runtime coerces each argument to its declared parameter type, and the
 * top-level handle coerces the settled value to the function's declared return type. Whether the
 * input is a freshly-packed in-memory object (live) or the JSON builtins it round-tripped to (replay
 * / re-run / a different worker), the coercion is identical -- that is what keeps the live and
 * recovery paths in lock-step. A by-name {@code rpc} dispatch opts out (its handle decodes to {@code
 * Object}) and gets raw builtins.
 *
 * <p>This example is a plain typed checkout -- no crash, no forced suspend. It then:
 *
 * <ul>
 *   <li>re-runs with the same id, so the result is served from the durable promise (genuine
 *       recovery) and arrives as the <i>same</i> rebuilt record -- same code, no recovery-only branch;
 *       and
 *   <li>dispatches the same function by name via {@code rpc} (which is untyped) to show the other side
 *       of the coin: identical stored bytes, but a raw {@code Map} back, because nothing told the
 *       runtime how to rebuild it.
 * </ul>
 *
 * <p>Start a Resonate server on localhost:8001 first ({@code resonate dev}), then:
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.recovery.Recovery}</pre>
 *
 * <p>Note on replay: a durable orchestrator re-executes from the top each time it awaits a
 * not-yet-settled future, so any side effect (a {@code println}, an external call) belongs in a leaf
 * step -- which settles once and never re-runs -- not in {@code checkout} itself. That is why every
 * log line below lives in a step.
 */
public final class Recovery {
    private Recovery() {}

    // -- Domain types: what crosses the durability boundary --------------------

    public record Cart(List<String> items, int total) {}

    public record Receipt(Cart cart, boolean paid) {}

    // -- Leaf steps (each prints once, settles once) ---------------------------

    public static Cart summarize(Context ctx, List<String> items) {
        Cart cart = new Cart(items, items.size() * 10);
        System.out.println("  [summarize] " + items + " -> " + cart);
        return cart;
    }

    public static Receipt pay(Context ctx, Cart cart) {
        // cart is declared Cart, so the runtime coerces the argument to a Cart before this body runs
        // -- on every call. Passed a live Cart it is a no-op; handed the JSON map a recovery would
        // deliver it rebuilds the Cart. Either way the accessors below are valid.
        System.out.printf(
                "  [pay] charging %d for %d items%n", cart.total(), cart.items().size());
        return new Receipt(cart, true);
    }

    // -- Orchestrator ----------------------------------------------------------

    public static Receipt checkout(Context ctx, List<String> items) {
        Cart cart = ctx.run(Recovery::summarize, items).await();
        // The Cart is handed straight back across the boundary as an argument to the next step; pay
        // coerces it back to a Cart on the way in, and its Receipt return is coerced to a Receipt for
        // whoever awaits this workflow.
        return ctx.run(Recovery::pay, cart).await();
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();
        r.register(Recovery::checkout);

        List<String> items = List.of("apple", "pear", "plum");
        try {
            String id = "recovery-" + System.nanoTime();
            System.out.println("[checkout] run id=" + id);
            Receipt receipt = r.run(id, Recovery::checkout, items).result();

            // run is typed: the handle coerces the settled JSON to checkout's declared Receipt -- a
            // real record, nested Cart and all -- not a map.
            assert receipt.cart().total() == 30;
            assert receipt.paid();
            System.out.println("[checkout] typed result: " + receipt);

            // Re-run with the SAME id. Nothing re-executes (no leaf lines print); the value is served
            // from the durable promise -- genuine recovery -- and comes back through the very same
            // deserialize, yielding an equal record.
            System.out.println("[checkout] re-run id=" + id + " (served from the durable promise)");
            Receipt again = r.run(id, Recovery::checkout, items).result();
            assert again.equals(receipt);
            System.out.println("[checkout] recovered result equals the original: " + again.equals(receipt));

            // Same function, dispatched by NAME via rpc -- which is untyped (its handle decodes to
            // Object). Identical bytes on the wire, but no return annotation to rebuild from, so the
            // value comes back as a raw Map. Use the typed run() form to keep the type.
            String rpcId = "recovery-rpc-" + System.nanoTime();
            System.out.println("[checkout] rpc  id=" + rpcId + " (untyped dispatch by name)");
            Object untyped = r.rpc(rpcId, "checkout", items).result();
            assert untyped instanceof Map;
            assert Boolean.TRUE.equals(((Map<?, ?>) untyped).get("paid")); // value survived; only the type was lost
            System.out.println(
                    "[checkout] untyped (rpc) result is a " + untyped.getClass().getSimpleName() + ": " + untyped);

            // Re-run the rpc with the SAME id: like the run re-run, the promise is already settled, so
            // it is served from durable storage (no leaf lines) and decodes to the same raw map --
            // recovery is the same path here too, untyped on both the first call and the re-run.
            System.out.println("[checkout] re-run rpc id=" + rpcId + " (served from the durable promise)");
            Object untypedAgain = r.rpc(rpcId, "checkout", items).result();
            assert untypedAgain.equals(untyped);
            System.out.println(
                    "[checkout] recovered untyped result equals the original: " + untypedAgain.equals(untyped));
        } finally {
            r.stop();
        }
    }
}
