package io.resonatehq.examples.rpc;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Context.Opts;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;

/**
 * rpc shows one worker dispatching to another by group.
 *
 * <p>Two {@code Resonate} instances share a server but live in different <b>groups</b>:
 *
 * <ul>
 *   <li>{@code backend} registers {@code greet} and does the work.
 *   <li>{@code frontend} registers nothing -- it only <i>dispatches</i>.
 * </ul>
 *
 * <p>{@code rpc} dispatches by <b>name</b>, not by a local function object, so the caller need not
 * have the target registered at all. {@code options(target="backend")} routes the call to the
 * backend group's anycast address ({@code poll://any@backend}); the server hands the execute message
 * to a worker subscribed there, which runs {@code greet} and settles the promise. The frontend's
 * handle is woken by the resulting {@code unblock} and reads the value back -- the whole round trip
 * crossing the durability boundary, not an in-process call.
 *
 * <p>This is the building block for splitting a system into independently deployed services: each
 * owns its functions and group, and they invoke each other by name + target without sharing code.
 *
 * <p>Start a Resonate server on localhost:8001 first ({@code resonate dev}), then:
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.rpc.Rpc}</pre>
 */
public final class Rpc {
    private Rpc() {}

    public static String greet(Context ctx, String name) {
        // Runs on the backend worker -- the side effect lives in the leaf.
        System.out.println("backend: greeting " + name);
        return "hello from backend, " + name + "!";
    }

    public static void main(String[] args) {
        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");

        // The worker: owns greet and listens on the "backend" group.
        Resonate backend = Resonate.builder().url(url).group("backend").build();
        backend.register(Rpc::greet);

        // The caller: a different group, with greet deliberately NOT registered.
        Resonate frontend = Resonate.builder().url(url).group("frontend").build();

        try {
            String id = "rpc-" + System.nanoTime();
            // Dispatch by name + target to the backend group, then await the result.
            ResonateHandle<Object> handle =
                    frontend.options(new Opts().withTarget("backend")).rpc(id, "greet", "world");
            Object result = handle.result();
            assert "hello from backend, world!".equals(result);
            System.out.println("frontend: got " + result);
        } finally {
            frontend.stop().join();
            backend.stop().join();
        }
    }
}
