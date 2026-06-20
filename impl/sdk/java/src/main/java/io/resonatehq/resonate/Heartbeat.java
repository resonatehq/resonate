package io.resonatehq.resonate;

import io.resonatehq.resonate.Send.Sender;
import io.resonatehq.resonate.Send.TaskRef;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps task leases alive, mirroring {@code resonate.heartbeat} from the Python SDK.
 *
 * <p>Implementations track a set of active tasks and periodically send {@code task.heartbeat}
 * requests to the server for all of them. Python defines a {@code Heartbeat} {@code Protocol} plus a
 * {@link Noop} and an {@link Async} implementation; we mirror the whole module as this holder (the
 * same pattern as {@link Send} / {@link Types}), with the protocol as the nested {@link Heartbeat}
 * interface.
 */
public final class Heartbeat {

    private Heartbeat() {}

    /** Protocol for keeping task leases alive. Mirrors Python's {@code Heartbeat} protocol. */
    public interface Hb {
        /**
         * Add a task to the heartbeat set. Starts the heartbeat loop if this is the first tracked
         * task.
         */
        void start(String taskId, int taskVersion);

        /** Remove a task from the heartbeat set. Stops the heartbeat loop if no tasks remain. */
        void stop(String taskId);

        /**
         * Shut down the heartbeat entirely. Clears all tracked tasks and aborts the loop. Called on
         * graceful shutdown.
         */
        void shutdown();
    }

    /** No-op heartbeat for local mode. */
    public static final class Noop implements Hb {
        @Override
        public void start(String taskId, int taskVersion) {}

        @Override
        public void stop(String taskId) {}

        @Override
        public void shutdown() {}
    }

    /**
     * Sends {@code task.heartbeat} requests at regular intervals for tracked tasks.
     *
     * <p>Uses {@link Sender} (not the raw transport) so the request goes through the standard
     * protocol envelope with corrId, version header, etc.
     *
     * <p>Python runs the loop as a single asyncio task and relies on its single-threaded model to
     * avoid a lock. Java runs the loop on a single dedicated daemon platform thread, so
     * {@code activeTasks} is a {@link ConcurrentHashMap} and the loop lifecycle is guarded by
     * {@code this}. (A virtual thread would compete for the shared carrier pool, and the
     * synchronized lifecycle would pin carriers on JDK 21 -- starving its own join on stop.)
     */
    public static final class Async implements Hb {

        private static final Logger LOGGER = System.getLogger(Async.class.getName());

        private final String pid;
        private final long intervalMs;
        private final Sender sender;
        private final Map<String, Integer> activeTasks = new ConcurrentHashMap<>();

        private Thread loopThread;

        public Async(String pid, long intervalMs, Sender sender) {
            this.pid = pid;
            this.intervalMs = intervalMs;
            this.sender = sender;
        }

        /** The configured beat interval in milliseconds. Mirrors Python's reachable {@code interval_ms}. */
        public long intervalMs() {
            return intervalMs;
        }

        @Override
        public synchronized void start(String taskId, int taskVersion) {
            activeTasks.put(taskId, taskVersion);
            ensureLoopRunning();
        }

        @Override
        public synchronized void stop(String taskId) {
            activeTasks.remove(taskId);
            if (activeTasks.isEmpty()) {
                cancelLoop();
            }
        }

        @Override
        public synchronized void shutdown() {
            activeTasks.clear();
            cancelLoop();
        }

        /** Spawn the heartbeat loop if not already running. Sends first tick immediately, then every interval. */
        private void ensureLoopRunning() {
            if (loopThread != null) {
                return;
            }
            loopThread = Thread.ofPlatform().name("resonate-heartbeat").daemon().start(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    tick();
                    try {
                        Thread.sleep(intervalMs);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
        }

        private void cancelLoop() {
            if (loopThread != null) {
                loopThread.interrupt();
                // Wait for an in-flight tick to finish before returning so that, once stop/shutdown
                // returns, no further heartbeat can be sent. tick() never acquires this monitor, so
                // awaiting here cannot deadlock.
                try {
                    loopThread.join(intervalMs * 2 + 1000);
                    if (loopThread.isAlive()) {
                        LOGGER.log(Level.WARNING, "heartbeat loop did not terminate in time");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                loopThread = null;
            }
        }

        /** One heartbeat tick: snapshot tracked tasks and send a single heartbeat for all of them. */
        private void tick() {
            List<TaskRef> tasks = new ArrayList<>();
            for (Map.Entry<String, Integer> e : activeTasks.entrySet()) {
                tasks.add(new TaskRef(e.getKey(), e.getValue()));
            }
            if (tasks.isEmpty()) {
                return;
            }
            try {
                sender.taskHeartbeat(pid, tasks).join();
            } catch (Exception exc) {
                // log and keep heartbeating
                LOGGER.log(Level.WARNING, "heartbeat failed: {0}", exc);
            }
        }
    }
}
