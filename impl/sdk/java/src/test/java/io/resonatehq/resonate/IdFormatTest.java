package io.resonatehq.resonate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.resonatehq.resonate.Errors.InvalidIdError;
import io.resonatehq.resonate.Network.LocalNetwork;
import io.resonatehq.resonate.Retry.Never;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Compliance tests for the promise id format the server enforces. Mirrors {@code
 * resonate-sdk-py/tests/test_id_format.py}.
 *
 * <p>The server (resonatehq/resonate, {@code new promise id} / PR #1127) treats a promise id as
 * {@code <origin>:<lineage>}: the origin is everything before the first {@code :} and the lineage
 * segments below it are {@code .}-separated:
 *
 * <pre>{@code root -> root:1 -> root:1.1 -> root:1.1.1}</pre>
 *
 * <p>{@link #serverValidate} is a direct port of the server's {@code
 * validate_promise_create_data}, and {@link #serverOrigin} of its {@code origin()} helper. Every
 * promise the SDK creates is replayed through them here, so a drift in id minting fails locally
 * instead of as a 400 from a real server.
 */
class IdFormatTest {

    private final List<Resonate> instances = new ArrayList<>();

    @AfterEach
    void stopAll() {
        for (Resonate r : instances) {
            r.stop();
        }
        instances.clear();
    }

    private Resonate local() {
        Resonate r = Resonate.builder().retryPolicy(new Never()).build();
        instances.add(r);
        return r;
    }

    // ── The server's rules, ported ─────────────────────────────────────────

    /** The origin, per the server's {@code origin()}: text before the first {@code :}. */
    private static String serverOrigin(String id) {
        int sep = id.indexOf(':');
        return sep == -1 ? id : id.substring(0, sep);
    }

    /** Port of the server's {@code validate_promise_create_data}. */
    private static void serverValidate(String id, Map<String, String> tags) {
        assertFalse(id.indexOf('\0') >= 0, "null_bytes: id=" + id);

        String origin = tags.get("resonate:origin");
        if (origin != null) {
            assertFalse(origin.contains(":"), "colon_in_origin: origin=" + origin);
            assertTrue(
                    id.equals(origin) || id.startsWith(origin + ":"),
                    "origin_prefix: id=%s is not prefixed by origin=%s".formatted(id, origin));
        }
        for (String key : new String[] {"resonate:branch", "resonate:parent"}) {
            String ancestor = tags.get(key);
            if (ancestor != null) {
                // A bare root joins its first lineage segment with ':'; an ancestor that already
                // carries lineage joins deeper segments with '.'.
                String sep = ancestor.contains(":") ? "." : ":";
                assertTrue(
                        id.equals(ancestor) || id.startsWith(ancestor + sep),
                        "%s_prefix: id=%s is not prefixed by %s=%s".formatted(key, id, key, ancestor));
            }
        }
        String prefix = tags.get("resonate:prefix");
        if (prefix != null) {
            assertFalse(prefix.contains("."), "dot_in_prefix: prefix=" + prefix);
        }
    }

    // ── Workflow under test ────────────────────────────────────────────────

    static int leaf(Context ctx, int n) {
        return n;
    }

    static int grandchild(Context ctx, int n) {
        ctx.run(IdFormatTest::leaf, n).await();
        return n;
    }

    static int tail(Context ctx, int n) {
        return n;
    }

    static int detachesAgain(Context ctx, int n) {
        // A detached child that itself detaches -- the recursion-bounding case.
        ctx.detached("tail", n).await();
        return n;
    }

    static int mid(Context ctx, int n) {
        ctx.run(IdFormatTest::grandchild, n).await();
        // A global-scope (bare) timer promise: minted from the same seq as everything else. A zero
        // sleep is settled at create, so awaiting it does not suspend.
        ctx.sleep(Duration.ZERO).await();
        // Detached from a *nested* context: its id is minted off the origin, not off this context,
        // so its declared ancestors must be the origin too.
        ctx.detached("detachesAgain", n).await();
        return n;
    }

    static int top(Context ctx, int n) {
        ctx.run(IdFormatTest::mid, n).await();
        ctx.run(IdFormatTest::mid, n + 1).await();
        return n;
    }

    /** Run the workflow above and return the local server's promise table. */
    private Map<String, Map<String, String>> runWorkflow(String id) {
        Resonate r = local();
        r.register(IdFormatTest::top);
        r.register(IdFormatTest::mid);
        r.register(IdFormatTest::grandchild);
        r.register(IdFormatTest::leaf);
        r.register(IdFormatTest::detachesAgain);
        r.register(IdFormatTest::tail);
        r.run(id, IdFormatTest::top, 1).result();

        LocalNetwork net = (LocalNetwork) r.network;
        // Let the fire-and-forget detached children be dispatched and run: 2x mid each detach a
        // child that detaches again -> 4 detached promises, all settled.
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            synchronized (net.state) {
                long detached = net.state.promises.keySet().stream()
                        .filter(pid -> pid.startsWith(id + ":d"))
                        .count();
                boolean allSettled = net.state.promises.values().stream()
                        .noneMatch(p -> "pending".equals(p.state) && p.id.startsWith(id + ":d"));
                if (detached == 4 && allSettled) {
                    break;
                }
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }

        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        synchronized (net.state) {
            net.state.promises.forEach((pid, p) -> out.put(pid, p.tags));
        }
        return out;
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    @ParameterizedTest
    @ValueSource(strings = {"wf", "my.app.workflow"})
    void everyCreatedPromisePassesServerValidation(String root) {
        // A dotted root is a caller's prerogative: '.' is only read below the origin, so every id
        // minted under it still validates and still shares that origin.
        Map<String, Map<String, String>> promises = runWorkflow(root);
        assertTrue(promises.size() > 1);
        promises.forEach(IdFormatTest::serverValidate);
        promises.forEach((id, tags) -> assertEquals(root, serverOrigin(id), "id " + id));
    }

    @Test
    void wholeWorkflowSharesOneOrigin() {
        // The origin is the server's partition key and the unit both promise.register_callback and
        // task.suspend match on, so every promise a workflow creates -- detached children included
        // -- must share it.
        Map<String, Map<String, String>> promises = runWorkflow("wf");
        promises.forEach((id, tags) -> {
            assertEquals("wf", serverOrigin(id), "id " + id);
            assertEquals("wf", tags.get("resonate:origin"), "id " + id);
        });
    }

    @Test
    void childIdsAreColonThenDotSeparated() {
        Map<String, Map<String, String>> promises = runWorkflow("wf");
        assertTrue(promises.containsKey("wf"));
        // First level below the root joins with ':', deeper levels with '.'.
        assertTrue(promises.containsKey("wf:1"), promises.keySet().toString());
        assertTrue(promises.containsKey("wf:1.1"), promises.keySet().toString());
        assertTrue(promises.containsKey("wf:1.1.1"), promises.keySet().toString());
        // No id keeps the old all-'.' shape.
        for (String id : promises.keySet()) {
            assertFalse(id.startsWith("wf."), "old-shape id: " + id);
        }
    }

    @Test
    void detachedIdsStayBoundedBelowTheOrigin() {
        // Detached ids are {origin}:d{16 hex} -- one segment past the origin no matter how deep the
        // spawning context is, or how many times a detached child detaches again.
        Map<String, Map<String, String>> promises = runWorkflow("wf");
        Pattern shape = Pattern.compile("^wf:d[0-9a-f]{16}$");
        List<String> detached =
                promises.keySet().stream().filter(id -> id.startsWith("wf:d")).toList();
        assertEquals(4, detached.size(), detached.toString()); // 2x mid, each detaching a child that detaches
        for (String id : detached) {
            assertTrue(shape.matcher(id).matches(), "id " + id);
            assertEquals("wf", promises.get(id).get("resonate:parent"), "id " + id);
            assertEquals(id, promises.get(id).get("resonate:branch"), "id " + id);
        }
    }

    @Test
    void prefixTagIsNotEmitted() {
        Map<String, Map<String, String>> promises = runWorkflow("wf");
        promises.forEach((id, tags) -> assertFalse(tags.containsKey("resonate:prefix"), "id " + id + " tags " + tags));
    }

    @Test
    void joinIdMatchesTheServersSeparatorRule() {
        assertEquals("root:1", Ids.joinId("root", "1"));
        assertEquals("root:1.2", Ids.joinId("root:1", "2"));
        assertEquals("root:1.2.3", Ids.joinId("root:1.2", "3"));
        assertEquals("root:dbeef", Ids.joinId("root", "dbeef"));
    }

    @Test
    void originOfMatchesTheServersOrigin() {
        for (String id : new String[] {"root", "root:1", "root:1.2", "root:dbeef"}) {
            assertEquals(serverOrigin(id), Ids.originOf(id));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"a:b", "a.b:c", "", "a\u0000b"})
    void validateRootIdRejectsReservedSeparators(String id) {
        // ':' is the one reserved separator in a root id: it becomes the origin of its whole
        // lineage. See the test below for what it actually breaks.
        assertThrows(InvalidIdError.class, () -> Ids.validateRootId(id));
    }

    @Test
    void aDotInARootIdIsAccepted() {
        // '.' only separates lineage segments *below* the origin, which is read after the origin
        // has been split off at the first ':'. A dotted root is therefore unambiguous, and the
        // server takes it.
        assertEquals("my.app.workflow", Ids.validateRootId("my.app.workflow"));
        String id = Ids.joinId("my.app.workflow", "1");
        assertEquals("my.app.workflow:1", id);
        assertEquals("my.app.workflow", Ids.originOf(id));
        serverValidate(id, Map.of("resonate:origin", "my.app.workflow"));
    }

    @Test
    void aColonInARootIdIsRejectedByTheServer() {
        // ':' cannot create the root either: a root is its own origin, and the origin is everything
        // before an id's first ':', so an origin holding one is unrepresentable -- no id could ever
        // split back to it.
        try {
            serverValidate("a:b", Map.of("resonate:origin", "a:b"));
            fail("expected colon_in_origin");
        } catch (AssertionError exc) {
            assertTrue(exc.getMessage().contains("colon_in_origin"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", "a-b", "a_b", "a.b", "wf-1786636678653183000"})
    void validateRootIdAcceptsBareIds(String id) {
        assertEquals(id, Ids.validateRootId(id));
    }
}
