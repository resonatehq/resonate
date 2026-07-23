// human-in-the-loop shows a durable workflow that suspends on an external
// decision.
//
// An order-fulfillment orchestrator does some prep work, then *suspends* on a
// durable promise that some external party -- a reviewer, a webhook, a UI -- is
// expected to resolve. While suspended, the worker holds no state: the
// orchestrator, the server, even days, can pass. Whenever the external resolve
// arrives, replay picks up exactly where it left off.
//
// The mechanism is ctx.promise(): a durable promise with a global,
// externally-addressable id. The orchestrator awaits it; anyone with the id can
// settle it through the regular promise API (resonate.promises.resolve), the
// CLI, or HTTP.
//
// Start a Resonate server on localhost:8001 first (`resonate dev`), then:
//
//   npx tsx examples/generator/human-in-the-loop.ts                    # approve
//   npx tsx examples/generator/human-in-the-loop.ts --decision reject
//
// Note on replay: the orchestrator re-executes from the top on every suspend,
// so any side effect (publishing the promise id, calling a notifier) belongs in
// a leaf -- `notifyReviewer` here -- which settles once and never re-runs.

import { Codec, type Context, Resonate } from "../../src/index.js";

interface Decision {
  approve: boolean;
  note: string;
}

// Signal channel from the worker to a waiting reviewer. `notifyReviewer`
// publishes the durable promise id here; the simulated reviewer in main() awaits
// it, exactly mirroring how a real reviewer would learn where to resolve.
class ReviewerInbox {
  private resolve!: (id: string) => void;
  readonly approvalId = new Promise<string>((r) => {
    this.resolve = r;
  });
  private done = false;
  publish(id: string): void {
    if (!this.done) {
      this.done = true;
      this.resolve(id);
    }
  }
}

// -- Leaf functions (each prints once, settles once) ----------------------

async function notifyReviewer(ctx: Context, orderId: string, amount: number, approvalId: string): Promise<string> {
  // In a real system this would call Slack / email / a dashboard. The side
  // effect (and the promise id the reviewer must resolve) lives in a leaf.
  console.log(
    `  [notifyReviewer] order ${orderId} ($${amount}) needs approval; resolve promise id: ${JSON.stringify(approvalId)}`,
  );
  ctx.getDependency<ReviewerInbox>("inbox")!.publish(approvalId);
  return approvalId;
}

async function shipOrder(_ctx: Context, orderId: string, note: string): Promise<string> {
  console.log(`  [shipOrder] shipping ${orderId} (note: ${JSON.stringify(note)})`);
  return `shipped-${orderId}`;
}

async function cancelOrder(_ctx: Context, orderId: string, note: string): Promise<string> {
  console.log(`  [cancelOrder] canceling ${orderId} (reason: ${JSON.stringify(note)})`);
  return `canceled-${orderId}`;
}

// -- Orchestrator ---------------------------------------------------------

function* fulfillOrder(ctx: Context, orderId: string, amount: number): Generator<any, string, any> {
  // Open the human-decision promise first so its id is deterministic
  // (`{workflowId}.0`). The Future's `.id` is the address to resolve.
  const approval = yield* ctx.promise<Decision>();
  const approvalId = approval.id;

  yield* ctx.run(notifyReviewer, orderId, amount, approvalId);

  // Suspend until the external party resolves the promise. The worker holds no
  // state while suspended; this can be seconds, hours, or days.
  const decision: Decision = yield* approval;

  if (decision.approve) return yield* ctx.run(shipOrder, orderId, decision.note);
  return yield* ctx.run(cancelOrder, orderId, decision.note);
}

// -- main -----------------------------------------------------------------

function flag(name: string, fallback: string): string {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const approve = flag("decision", "approve") === "approve";

const resonate = new Resonate({ url: process.env.RESONATE_URL ?? "http://localhost:8001" });
const inbox = new ReviewerInbox();
resonate.setDependency("inbox", inbox);
resonate.register("fulfillOrder", fulfillOrder);

const codec = new Codec();

// Stand in for an external system that eventually resolves the promise: wait
// for notifyReviewer to publish the id, then settle it. No polling, no
// hardcoded id.
async function simulateReviewer(decision: Decision): Promise<void> {
  const approvalId = await inbox.approvalId;
  await resonate.promises.resolve(approvalId, { data: codec.encode(decision).data });
  console.log(`[reviewer] resolved ${approvalId} -> approve=${decision.approve} note=${JSON.stringify(decision.note)}`);
}

try {
  const wid = `fulfill-${Date.now()}`;
  console.log(`[fulfillOrder] starting workflow id=${wid} decision=${JSON.stringify(approve ? "approve" : "reject")}`);
  const decision: Decision = { approve, note: approve ? "looks good" : "policy violation" };
  const reviewer = simulateReviewer(decision);

  const out = await resonate.run(wid, fulfillOrder, "order-42", 199);
  await reviewer;
  const expected = approve ? "shipped-order-42" : "canceled-order-42";
  if (out !== expected) throw new Error(`expected ${expected}, got ${out}`);
  console.log(`[fulfillOrder] OK: ${out}`);
} finally {
  await resonate.stop();
}
