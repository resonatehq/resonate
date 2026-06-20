package io.resonatehq.examples.detached;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Context.ResonateFuture;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;
import java.nio.charset.StandardCharsets;

/**
 * detached shows fire-and-forget durable invocations whose lifetime is <b>decoupled</b> from the
 * parent.
 *
 * <pre>
 *     place_order:
 *         1. reserve_stock        (ctx.run -- parent waits)
 *         2. charge_card          (ctx.run -- parent waits)
 *         3. write_audit_log      (ctx.detached -- parent does NOT wait)
 * </pre>
 *
 * <p>The first two steps are ordinary durable children: their result feeds the orchestrator, so the
 * orchestrator awaits them. The audit log is different -- it is a side-effect workflow that must
 * <i>survive</i> the order completing.
 *
 * <p>{@code ctx.detached} is the tool. Unlike {@code ctx.run} / {@code ctx.rpc}, its future's
 * {@code id()} is the <b>promise id</b>, not the result. The parent never awaits the body, so the
 * audit's lifetime, timeout, and execution worker are independent of {@code placeOrder}.
 */
public final class Detached {
    private Detached() {}

    /** The triple returned by {@code placeOrder}. */
    public record OrderResult(String stockRef, String chargeRef, String auditId) {}

    // -- Order steps (leaves: each prints once, settles once) ------------------

    public static String reserveStock(Context ctx, String sku, int qty) {
        String ref = "STK-" + sku + "-" + qty;
        System.out.println("  [reserve_stock] reserved " + ref);
        return ref;
    }

    public static String chargeCard(Context ctx, String customer, int amount) {
        String ref = "CH-" + customer + "-" + amount;
        System.out.println("  [charge_card] charged " + ref);
        return ref;
    }

    // -- Audit workflow (a real, multi-step durable workflow on its own) -------

    public static String hashPayload(Context ctx, String customer, String sku, int amount) {
        String digest = hex((customer + ":" + sku + ":" + amount).getBytes(StandardCharsets.UTF_8));
        System.out.println("  [hash_payload] " + digest);
        return digest;
    }

    public static String shipToWarehouse(Context ctx, String digest) {
        System.out.println("  [ship_to_warehouse] persisted " + digest);
        return "audit-" + digest;
    }

    public static String writeAuditLog(
            Context ctx, String customer, String sku, int amount, String stockRef, String chargeRef) {
        String digest = ctx.run(Detached::hashPayload, customer, sku, amount).await();
        String location = ctx.run(Detached::shipToWarehouse, digest).await();
        System.out.printf("  [write_audit_log] stock=%s charge=%s -> %s%n", stockRef, chargeRef, location);
        return location;
    }

    // -- Orchestrator ----------------------------------------------------------

    public static OrderResult placeOrder(Context ctx, String customer, String sku, int qty, int amount) {
        // Foreground work -- the order needs both of these to commit.
        String stockRef = ctx.run(Detached::reserveStock, sku, qty).await();
        String chargeRef = ctx.run(Detached::chargeCard, customer, amount).await();

        // Fire-and-forget: dispatch the audit workflow by NAME and get back the durable id of its
        // root promise. The audit's body never runs in this task; placeOrder only awaits the id.
        ResonateFuture<String> auditFuture = ctx.detached("writeAuditLog", customer, sku, amount, stockRef, chargeRef);
        String auditId = auditFuture.id();
        System.out.println("  [place_order] dispatched audit id=" + auditId + " (not waiting on it)");

        // Return immediately. The audit may still be running -- that is fine.
        return new OrderResult(stockRef, chargeRef, auditId);
    }

    private static String hex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >>> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();

        // Register both the orchestrator and the audit workflow: ctx.detached dispatches by name
        // through the server, so the audit needs to be registered on whichever group will execute it.
        r.register(Detached::placeOrder);
        r.register(Detached::writeAuditLog);

        try {
            String orderId = "order-" + System.nanoTime();
            System.out.println("[place_order] starting workflow id=" + orderId);
            ResonateHandle<OrderResult> handle = r.run(orderId, Detached::placeOrder, "alice", "WIDGET-7", 2, 199);
            OrderResult result = handle.result();
            assert "STK-WIDGET-7-2".equals(result.stockRef());
            assert "CH-alice-199".equals(result.chargeRef());
            System.out.printf("[place_order] OK: stock=%s charge=%s%n", result.stockRef(), result.chargeRef());

            // The order has committed. Attach to the audit's durable promise by id and await it,
            // separately from the order. In a real system this could be a wholly different process.
            System.out.println("[audit] attaching to detached workflow id=" + result.auditId());
            ResonateHandle<Object> auditHandle = r.get(result.auditId()).join();
            String location = (String) auditHandle.result();
            System.out.println("[audit] OK: " + location);
            assert location.startsWith("audit-");
        } finally {
            r.stop();
        }
    }
}
