package io.resonatehq.examples.errorhandling;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Errors.ApplicationError;
import io.resonatehq.resonate.Resonate;
import io.resonatehq.resonate.Retry.Never;
import java.util.List;

/**
 * Error handling across the Resonate durability boundary.
 *
 * <p>When a durable function like {@code bar} fails, its exception does not propagate in-process the
 * way a normal call would. Resonate <b>encodes the failure, writes it to a durable promise, and
 * later decodes it</b> for whoever awaits the result -- the same process, a different worker (via
 * {@code ctx.rpc}), or a process that recovered after a crash.
 *
 * <p>The Java wire form carries a transport-safe {@code message} string plus a best-effort native
 * serialization of the original exception (the analogue of Python's pickle). So a same-runtime
 * awaiter that can load the class gets the <b>original type back</b> -- {@code foo} below catches
 * {@link UsernameTakenError} and {@link IllegalArgumentException} directly. When the serialized form
 * cannot round-trip, the awaiter falls back to a plain {@link ApplicationError} carrying the
 * message.
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.errorhandling.ErrorHandling \
 *     -PexampleArgs="--mode run --error taken"}</pre>
 */
public final class ErrorHandling {
    private ErrorHandling() {}

    /** Raised when a username is already registered. A {@link RuntimeException} so it round-trips by type. */
    public static final class UsernameTakenError extends RuntimeException {
        public UsernameTakenError(String message) {
            super(message);
        }
    }

    public static String bar(Context ctx, String username, int age) {
        List<String> existingUsers = List.of("admin", "coder123", "python_fan");

        if (age < 0) {
            throw new IllegalArgumentException("Age cannot be negative.");
        }
        if (existingUsers.contains(username.toLowerCase())) {
            throw new UsernameTakenError("The username '" + username + "' is already in use.");
        }
        return "Registration successful for " + username + "!";
    }

    public static String foo(Context ctx, String username, int age, String mode) {
        try {
            String v = (String)
                    (mode.equals("run")
                            ? ctx.run(ErrorHandling::bar, username, age).await()
                            : ctx.rpc("bar", username, age).await());
            System.out.println("🎉 Success: " + v);
            return v;
        } catch (UsernameTakenError e) {
            // The original domain type is reconstructed across the boundary, so we can catch it
            // directly instead of parsing a message prefix.
            System.out.println("⚠️ Business Rule Error: " + e.getMessage());
            System.out.println("💡 Suggestion: Please try adding numbers to your username.");
            return null;
        } catch (IllegalArgumentException e) {
            System.out.println("💥 Validation Error: " + e.getMessage());
            return null;
        } catch (ApplicationError e) {
            throw new AssertionError(
                    "ApplicationError is the fallback used when the raised error cannot be deserialized."
                            + " Not applicable in this example.",
                    e);
        }
    }

    public static void main(String[] args) {
        String mode = argValue(args, "--mode", "run");
        String error = argValue(args, "--error", "none");

        String username;
        int age;
        if (error.equals("taken")) {
            username = "admin"; // Triggers UsernameTakenError
            age = 25;
        } else if (error.equals("value")) {
            username = "alice"; // Triggers IllegalArgumentException
            age = -5;
        } else {
            username = "alice"; // Successful path
            age = 25;
        }

        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).retryPolicy(new Never()).build();
        // Registering both functions so RPC coordination works flawlessly.
        r.register(ErrorHandling::foo);
        r.register(ErrorHandling::bar);

        try {
            String id = "error-handling-" + System.nanoTime();
            r.run(id, ErrorHandling::foo, username, age, mode).result();
            r.get(id).join().done();
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
