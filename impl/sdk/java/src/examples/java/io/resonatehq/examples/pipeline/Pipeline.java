package io.resonatehq.examples.pipeline;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Context.ResonateFuture;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;
import java.util.List;

/**
 * pipeline shows a multi-stage DAG-shaped durable workflow:
 *
 * <pre>
 *     download -&gt; parse -&gt; +- transform_a -+
 *                          +- transform_b -+- merge -&gt; emit
 * </pre>
 *
 * <p>transform_a and transform_b run in parallel (both calls are dispatched before either is
 * awaited); merge depends on both and synchronizes them with await. Every stage is a registered
 * function backed by a durable promise, so a crash mid-pipeline picks up at the first unsettled
 * stage without re-doing completed work.
 *
 * <p>Note on replay: a durable orchestrator re-executes from the top each time it awaits a
 * not-yet-settled future, so any side effect (a {@code println}, an external call) belongs in a leaf
 * stage function -- which settles once and never re-runs -- not in {@code runPipeline} itself. That
 * is why every log line below lives in a stage.
 */
public final class Pipeline {
    private Pipeline() {}

    /** The pair returned by {@code merge} and unpacked into {@code emit}. */
    public record Merged(int wordCount, String upper) {}

    public static String download(Context ctx, String url) {
        String body = "the quick brown fox jumps over " + url;
        System.out.printf("  [download] %s -> %d bytes%n", url, body.length());
        return body;
    }

    public static List<String> parse(Context ctx, String raw) {
        List<String> words = List.of(raw.split(" "));
        System.out.printf("  [parse] %d words%n", words.size());
        return words;
    }

    public static int transformA(Context ctx, List<String> p) {
        System.out.println("  [transform_a] counting words");
        return p.size();
    }

    public static String transformB(Context ctx, List<String> p) {
        String upper = String.join(" ", p).toUpperCase();
        System.out.printf("  [transform_b] uppercased %d chars%n", upper.length());
        return upper;
    }

    public static Merged merge(Context ctx, int wordCount, String upper) {
        System.out.println("  [merge] combining transforms");
        return new Merged(wordCount, upper);
    }

    public static String emit(Context ctx, int wordCount, String upper) {
        System.out.printf("  [emit] words=%d upper=%s%n", wordCount, upper);
        return "ok";
    }

    public static String runPipeline(Context ctx, String url) {
        String raw = ctx.run(Pipeline::download, url).await();
        List<String> parsed = ctx.run(Pipeline::parse, raw).await();

        // fan out: transform_a and transform_b dispatched before either is awaited
        ResonateFuture<Integer> fa = ctx.run(Pipeline::transformA, parsed);
        ResonateFuture<String> fb = ctx.run(Pipeline::transformB, parsed);

        // fan in
        int a = fa.await();
        String b = fb.await();

        Merged merged = ctx.run(Pipeline::merge, a, b).await();
        return ctx.run(Pipeline::emit, merged.wordCount(), merged.upper()).await();
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).build();
        r.register(Pipeline::runPipeline);

        try {
            String id = "pipeline-" + System.nanoTime();
            System.out.println("[run_pipeline] starting workflow id=" + id);
            ResonateHandle<String> handle = r.run(id, Pipeline::runPipeline, "example.com/doc");
            String out = handle.result();
            assert "ok".equals(out);
            System.out.println("[run_pipeline] OK: sent=" + out);
        } finally {
            r.stop().join();
        }
    }
}
