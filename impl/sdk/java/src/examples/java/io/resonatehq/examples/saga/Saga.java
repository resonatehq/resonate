package io.resonatehq.examples.saga;

import io.resonatehq.resonate.Context;
import io.resonatehq.resonate.Handle.ResonateHandle;
import io.resonatehq.resonate.Resonate;
import io.resonatehq.resonate.Retry.Never;

/**
 * saga shows a multi-step durable workflow with compensation on failure (the canonical "distributed
 * transactions" pattern):
 *
 * <pre>
 *     book_trip:
 *         1. reserve_flight
 *         2. reserve_hotel   (on failure: release_flight)
 *         3. charge_card     (on failure: release_hotel, release_flight)
 * </pre>
 *
 * <p>Each step is its own registered function dispatched via {@code ctx.run}. Step settlement is
 * recorded in a durable promise, so if the worker crashes between two steps a restart skips the steps
 * that already settled and runs only the missing ones -- including the compensations.
 *
 * <p>Start a Resonate server on localhost:8001 first ({@code resonate dev}), then either of:
 *
 * <pre>{@code ./gradlew runExample -PmainClass=io.resonatehq.examples.saga.Saga                       # happy path
 * ./gradlew runExample -PmainClass=io.resonatehq.examples.saga.Saga -PexampleArgs="--fail charge" # both compensations run}</pre>
 *
 * <p>Steps throw <i>ordinary</i> exceptions ({@link BookingError} below) -- there is no need to
 * construct a Resonate {@code ApplicationError}. The orchestrator catches the domain exception by its
 * real type -- the SDK serializes it across the durability boundary and reconstructs it when the
 * awaiting worker can load the class -- with {@code ApplicationError} as the fallback for when it
 * cannot round-trip. Note the catch is deliberately <i>not</i> a bare {@code catch (Throwable)}: that
 * would also swallow the internal {@code Suspended} signal the durable replay machinery relies on.
 *
 * <p>Note on replay: a durable orchestrator re-executes from the top each time it awaits a
 * not-yet-settled future, so any side effect (a {@code println}, an external call) belongs in a leaf
 * step function -- which settles once and never re-runs -- not in {@code bookTrip} itself. That is why
 * every log line below lives in a step, never in the orchestrator.
 */
public final class Saga {
    private Saga() {}

    /**
     * A plain domain error -- deliberately NOT a Resonate error.
     *
     * <p>Demonstrates that step functions can throw any exception; the SDK settles the step's promise
     * {@code rejected} and reconstructs the original type for the orchestrator when it can round-trip
     * across the durability boundary (same runtime, loadable class), falling back to {@code
     * ApplicationError} otherwise.
     */
    public static class BookingError extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public BookingError(String message) {
            super(message);
        }
    }

    public static final class HotelReservationError extends BookingError {
        private static final long serialVersionUID = 1L;

        public HotelReservationError(String message) {
            super(message);
        }
    }

    public static final class ChargeCardError extends BookingError {
        private static final long serialVersionUID = 1L;

        public ChargeCardError(String message) {
            super(message);
        }
    }

    /** The triple returned by {@code bookTrip}. */
    public record TripResult(String flight, String hotel, String charge) {}

    // -- Step functions (leaves: each prints once, settles once) ---------------

    public static String reserveFlight(Context ctx, String customer, String frm, String to) {
        String ref = "FL-" + customer + "-" + frm + "-" + to;
        System.out.println("  [reserve_flight] reserved " + ref);
        return ref;
    }

    public static String reserveHotel(Context ctx, String customer, String city, boolean fail) {
        if (fail) {
            System.out.println("  [reserve_hotel] FAILED for " + customer + " in " + city);
            throw new HotelReservationError("no rooms available in " + city);
        }
        String ref = "HT-" + customer + "-" + city;
        System.out.println("  [reserve_hotel] reserved " + ref);
        return ref;
    }

    public static String chargeCard(Context ctx, String customer, int amount, boolean fail) {
        if (fail) {
            System.out.println("  [charge_card] FAILED for " + customer + " ($" + amount + ")");
            throw new ChargeCardError("card declined for $" + amount);
        }
        String ref = "CH-" + customer + "-" + amount;
        System.out.println("  [charge_card] charged " + ref);
        return ref;
    }

    public static String releaseFlight(Context ctx, String ref) {
        System.out.println("  [release_flight] released " + ref);
        return ref;
    }

    public static String releaseHotel(Context ctx, String ref) {
        System.out.println("  [release_hotel] released " + ref);
        return ref;
    }

    // -- Saga orchestrator -----------------------------------------------------

    public static TripResult bookTrip(Context ctx, String customer, String frm, String to, int amount, String failAt) {
        // Step 1: flight
        String flight = ctx.run(Saga::reserveFlight, customer, frm, to).await();

        // Step 2: hotel (compensate the flight on failure)
        String hotel;
        try {
            hotel = ctx.run(Saga::reserveHotel, customer, to, "hotel".equals(failAt))
                    .await();
        } catch (BookingError e) {
            compensate(ctx, "", flight);
            throw e;
        }

        // Step 3: charge (compensate hotel + flight on failure, reverse order)
        String charge;
        try {
            charge = ctx.run(Saga::chargeCard, customer, amount, "charge".equals(failAt))
                    .await();
        } catch (BookingError e) {
            compensate(ctx, hotel, flight);
            throw e;
        }

        return new TripResult(flight, hotel, charge);
    }

    /**
     * Run the inverse of any completed steps in reverse order. Empty refs are skipped. Each
     * compensation is its own durable promise, so a crash mid-rollback resumes at the first unsettled
     * one.
     */
    private static void compensate(Context ctx, String hotelRef, String flightRef) {
        if (!hotelRef.isEmpty()) {
            ctx.run(Saga::releaseHotel, hotelRef).await();
        }
        if (!flightRef.isEmpty()) {
            ctx.run(Saga::releaseFlight, flightRef).await();
        }
    }

    public static void main(String[] args) {
        String fail = argValue(args, "--fail", "");

        String url = System.getenv().getOrDefault("RESONATE_URL", "http://localhost:8001");
        Resonate r = Resonate.builder().url(url).retryPolicy(new Never()).build();
        r.register(Saga::bookTrip);
        r.register(Saga::reserveFlight);
        r.register(Saga::reserveHotel);
        r.register(Saga::chargeCard);
        r.register(Saga::releaseFlight);
        r.register(Saga::releaseHotel);

        try {
            String id = "saga-" + System.nanoTime();
            System.out.println("[book_trip] starting workflow id=" + id + " fail_at='" + fail + "'");
            ResonateHandle<TripResult> handle = r.run(id, Saga::bookTrip, "alice", "SFO", "JFK", 850, fail);
            TripResult trip;
            try {
                trip = handle.result();
            } catch (BookingError exc) {
                System.out.println("[book_trip] FAILED: " + exc.getClass().getSimpleName() + ": " + exc.getMessage());
                return;
            }
            System.out.printf(
                    "[book_trip] OK: flight=%s hotel=%s charge=%s%n", trip.flight(), trip.hotel(), trip.charge());
            assert "FL-alice-SFO-JFK".equals(trip.flight());
            assert "HT-alice-JFK".equals(trip.hotel());
            assert "CH-alice-850".equals(trip.charge());
        } finally {
            r.stop();
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
