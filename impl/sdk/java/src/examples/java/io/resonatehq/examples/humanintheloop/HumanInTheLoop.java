package io.resonatehq.examples.humanintheloop;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Context.ResonateFuture;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;
import io.resonatehq.resonate.Types.Value;
import java.util.concurrent.CompletableFuture;

/**
 * human shows a human-in-the-loop durable workflow.
 *
 * <p>An order-fulfillment orchestrator does some prep work, then <i>suspends</i> on a durable promise
 * that some external party -- a reviewer, a webhook, a UI -- is expected to resolve. While the
 * workflow is suspended, the worker holds no state: the orchestrator can be restarted, the server can
 * be restarted, even days can pass. Whenever the external resolve eventually arrives, replay picks up
 * exactly where it left off and proceeds with the human's decision.
 *
 * <p>The mechanism is {@link Context#promise()}: a "dependency-injected" durable promise with a
 * global, externally addressable id. The orchestrator awaits it; anyone with the id can settle it
 * through the regular promise API ({@code r.promises.resolve(id, ...)}), the CLI, or HTTP.
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.humanintheloop.HumanInTheLoop                        # approve
 * ./gradlew runExample -PmainClass=io.resonatehq.examples.humanintheloop.HumanInTheLoop -PexampleArgs="--decision reject"}</pre>
 *
 * <p>Note on replay: the orchestrator re-executes from the top on every suspend, so any side effect
 * (printing the promise id, calling a notification service) belongs in a leaf -- {@code
 * notifyReviewer} here -- which settles once and never re-runs.
 */
public final class HumanInTheLoop {
    private HumanInTheLoop() {}

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // -- Reviewer inbox --------------------------------------------------------

    /**
     * Signal channel from the worker to a waiting reviewer.
     *
     * <p>{@code notifyReviewer} is the leaf that <i>publishes</i> the durable promise id (in a real
     * system: to Slack, email, a dashboard). Here it also completes {@code approvalId} so anything
     * awaiting an approval -- like the simulated reviewer in {@code main} -- learns the id the instant
     * the promise exists, rather than polling until it stops 404-ing.
     *
     * <p>Injected through {@link Resonate#withDependency(Object)} and fetched in the leaf via {@code
     * ctx.getDependency(ReviewerInbox.class)}.
     */
    public static final class ReviewerInbox {
        final CompletableFuture<String> approvalId = new CompletableFuture<>();

        public void publish(String approvalId) {
            // notifyReviewer is a leaf and settles once, but be defensive: complete only once.
            this.approvalId.complete(approvalId);
        }
    }

    // -- Domain types ----------------------------------------------------------

    public record Decision(boolean approve, String note) {}

    // -- Leaf functions (each prints once, settles once) -----------------------

    public static String notifyReviewer(Context ctx, String orderId, int amount, String approvalId) {
        // In a real system this would call Slack / email / a dashboard. The point is that the side
        // effect (and the promise id the reviewer must resolve) lives in a leaf, not in the
        // orchestrator -- so it happens exactly once.
        System.out.printf(
                "  [notify_reviewer] order %s ($%d) needs approval; resolve promise id: '%s'%n",
                orderId, amount, approvalId);
        // Signal any in-process listener (the simulated reviewer in main) that the durable promise now
        // exists and tell them its id. A real reviewer would learn the same id from the side effect.
        ctx.getDependency(ReviewerInbox.class).publish(approvalId);
        return approvalId;
    }

    public static String shipOrder(Context ctx, String orderId, String note) {
        System.out.printf("  [ship_order] shipping %s (note: '%s')%n", orderId, note);
        return "shipped-" + orderId;
    }

    public static String cancelOrder(Context ctx, String orderId, String note) {
        System.out.printf("  [cancel_order] canceling %s (reason: '%s')%n", orderId, note);
        return "canceled-" + orderId;
    }

    // -- Orchestrator ----------------------------------------------------------

    public static String fulfillOrder(Context ctx, String orderId, int amount) {
        // Open the human-decision promise first so its id is deterministic ({workflow_id}.1).
        // ctx.promise returns a future whose id() is awaitable; we publish that id through a leaf so a
        // real reviewer would know where to resolve.
        ResonateFuture<Object> approval = ctx.promise(); // inherit workflow timeout
        String approvalId = approval.id();

        ctx.run(HumanInTheLoop::notifyReviewer, orderId, amount, approvalId).await();

        // Suspend until the external party resolves the promise. The worker holds no state while
        // suspended; this can be seconds, hours, or days.
        Decision decision = MAPPER.convertValue(approval.await(), Decision.class);

        if (decision.approve()) {
            return ctx.run(HumanInTheLoop::shipOrder, orderId, decision.note()).await();
        }
        return ctx.run(HumanInTheLoop::cancelOrder, orderId, decision.note()).await();
    }

    // -- main ------------------------------------------------------------------

    /**
     * Stand in for an external system that eventually resolves the promise. Waits for {@code
     * notifyReviewer} to publish the durable promise id on the inbox -- exactly mirroring how a real
     * reviewer learns where to resolve -- then settles it. No polling, no hardcoded id.
     */
    private static void simulateReviewer(Resonate r, ReviewerInbox inbox, Decision decision) {
        String approvalId = inbox.approvalId.join();
        r.promises.resolve(approvalId, new Value(null, decision)).join();
        System.out.printf(
                "[reviewer] resolved %s -> approve=%b note='%s'%n", approvalId, decision.approve(), decision.note());
    }

    public static void main(String[] args) {
        String decisionArg = argValue(args, "--decision", "approve");
        String order = argValue(args, "--order", "order-42");
        int amount = Integer.parseInt(argValue(args, "--amount", "199"));

        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();
        // Hand the inbox to every context -- notifyReviewer will publish on it the moment the durable
        // approval promise is created.
        ReviewerInbox inbox = new ReviewerInbox();
        r.withDependency(inbox);
        r.register(HumanInTheLoop::fulfillOrder);

        try {
            String wid = "fulfill-" + System.nanoTime();
            System.out.println("[fulfill_order] starting workflow id=" + wid + " decision='" + decisionArg + "'");
            ResonateHandle<String> handle = r.run(wid, HumanInTheLoop::fulfillOrder, order, amount);

            boolean approve = decisionArg.equals("approve");
            Decision decision = new Decision(approve, approve ? "looks good" : "policy violation");
            Thread reviewer = new Thread(() -> simulateReviewer(r, inbox, decision));
            reviewer.start();

            String out = handle.result();
            try {
                reviewer.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            if (approve) {
                assert "shipped-order-42".equals(out);
            } else {
                assert "canceled-order-42".equals(out);
            }
            System.out.println("[fulfill_order] OK: " + out);
        } finally {
            r.stop().join();
        }
    }

    private static String argValue(String[] args, String flag, String fallback) {
        for (int i = 0; i + 1 < args.length; i++) {
            if (args[i].equals(flag)) {
                return args[i + 1];
            }
        }
        return fallback;
    }
}
