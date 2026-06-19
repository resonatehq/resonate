package io.resonatehq.examples.structuredconcurrency;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;

/**
 * structured-concurrency shows that the runtime never leaks an unawaited durable child.
 *
 * <pre>
 *     foo:
 *         f1 = ctx.run(bar, 1)   // spawned, never awaited
 *         f2 = ctx.run(bar, 2)   // spawned, never awaited
 *         return 5               // returns without touching f1 / f2
 * </pre>
 *
 * <p>{@code foo} fires off two {@code ctx.run(bar)} children and returns {@code 5} immediately, never
 * awaiting either future. A naive runtime would resolve {@code foo} and orphan the two children.
 * Resonate does not: structured concurrency guarantees a parent cannot settle while any child it
 * spawned is still in flight. Before {@code foo}'s promise resolves, the runtime joins every
 * eagerly-spawned local child, so both {@code bar} invocations run to completion regardless of
 * whether {@code foo} awaited them.
 *
 * <p>We prove it durably. {@code ctx.run} children get deterministic ids {@code {foo_id}.1} and
 * {@code {foo_id}.2}. After {@code foo} returns {@code 5} we attach to those two promises by id and
 * assert each one resolved -- evidence the never-awaited work was awaited <i>by the runtime</i> on
 * our behalf.
 *
 * <p>Start a Resonate server on localhost:8001 first ({@code resonate dev}), then:
 *
 * <pre>{@code ./gradlew runExample \
 *     -PmainClass=io.resonatehq.examples.structuredconcurrency.StructuredConcurrency}</pre>
 *
 * <p>Note on replay: a durable orchestrator re-executes from the top each time it awaits a
 * not-yet-settled future, so any side effect (a {@code println}) belongs in a leaf function -- which
 * settles once and never re-runs -- not in {@code foo} itself. That is why the log line below lives
 * in {@code bar}.
 */
public final class StructuredConcurrency {
    private StructuredConcurrency() {}

    public static int bar(Context ctx, int n) {
        // Leaf: prints once, settles once. If structured concurrency holds, both of foo's
        // never-awaited children land here even though foo returned first.
        System.out.println("  [bar] running child n=" + n);
        return n * 10;
    }

    public static int foo(Context ctx) {
        // Spawn two local children and walk away -- neither future is awaited.
        ctx.run(StructuredConcurrency::bar, 1);
        ctx.run(StructuredConcurrency::bar, 2);
        return 5;
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();
        r.register(StructuredConcurrency::foo);
        r.register(StructuredConcurrency::bar);

        try {
            String fooId = "structured-concurrency-" + System.nanoTime();
            System.out.println("[foo] starting workflow id=" + fooId);
            int out = r.run(fooId, StructuredConcurrency::foo).result();

            // foo returned 5 without awaiting either child.
            assert out == 5 : "expected 5, got " + out;
            System.out.println("[foo] OK: returned " + out + " (never awaited its two children)");

            // Structured concurrency: the runtime awaited the two never-awaited ctx.run children
            // before resolving foo. ctx.run assigns child ids in call order as {parent_id}.{seq}
            // (seq starts at 1), so foo's two children are {foo_id}.1 and {foo_id}.2. Attach to each
            // durable promise and confirm it resolved with bar's result.
            for (int seq = 1; seq <= 2; seq++) {
                String childId = fooId + "." + seq;
                ResonateHandle<Object> childHandle = r.get(childId).join();
                int childOut = ((Number) childHandle.result()).intValue();
                assert childOut == seq * 10 : "child " + childId + " resolved " + childOut + ", expected " + (seq * 10);
                System.out.println("[child] " + childId + " resolved " + childOut + " -- the runtime awaited it");
            }

            System.out.println("[ok] both never-awaited children completed: structured concurrency holds");
        } finally {
            r.stop().join();
        }
    }
}
