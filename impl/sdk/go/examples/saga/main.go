// saga shows a multi-step durable workflow with compensation on failure
// (the canonical "distributed transactions" pattern):
//
//	bookTrip:
//	    1. reserveFlight
//	    2. reserveHotel   (on failure: releaseFlight)
//	    3. chargeCard     (on failure: releaseHotel, releaseFlight)
//
// Each step is its own registered function dispatched via ctx.RPC. Step
// settlement is recorded in a durable promise, so if the worker crashes
// between two steps a restart skips the steps that already settled and runs
// only the missing ones — including the compensations.
//
// Run a Resonate server on localhost:8001 first, then either of:
//
//	go run .                    # happy path
//	go run . -fail=charge       # forces chargeCard to fail; both
//	                            # compensations run in reverse order
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

// ── Domain types ────────────────────────────────────────────────────────

type TripArgs struct {
	Customer string `json:"customer"`
	From     string `json:"from"`
	To       string `json:"to"`
	Amount   int    `json:"amount"`
	FailAt   string `json:"fail_at"` // "" | "hotel" | "charge"
}

type TripResult struct {
	FlightRef string `json:"flight_ref"`
	HotelRef  string `json:"hotel_ref"`
	ChargeRef string `json:"charge_ref"`
}

type FlightArgs struct {
	Customer string `json:"customer"`
	From     string `json:"from"`
	To       string `json:"to"`
}

type HotelArgs struct {
	Customer string `json:"customer"`
	City     string `json:"city"`
	Fail     bool   `json:"fail"`
}

type ChargeArgs struct {
	Customer string `json:"customer"`
	Amount   int    `json:"amount"`
	Fail     bool   `json:"fail"`
}

type Reservation struct {
	Ref string `json:"ref"`
}

type Charge struct {
	Ref string `json:"ref"`
}

type ReleaseArgs struct {
	Ref string `json:"ref"`
}

type ReleaseResult struct {
	Released string `json:"released"`
}

// ── Step functions ──────────────────────────────────────────────────────

func reserveFlight(_ *resonate.Context, args FlightArgs) (Reservation, error) {
	ref := fmt.Sprintf("FL-%s-%s-%s", args.Customer, args.From, args.To)
	fmt.Printf("  [reserveFlight] reserved %s\n", ref)
	return Reservation{Ref: ref}, nil
}

func reserveHotel(_ *resonate.Context, args HotelArgs) (Reservation, error) {
	if args.Fail {
		fmt.Printf("  [reserveHotel] FAILED for %s in %s\n", args.Customer, args.City)
		return Reservation{}, fmt.Errorf("no rooms available in %s", args.City)
	}
	ref := fmt.Sprintf("HT-%s-%s", args.Customer, args.City)
	fmt.Printf("  [reserveHotel] reserved %s\n", ref)
	return Reservation{Ref: ref}, nil
}

func chargeCard(_ *resonate.Context, args ChargeArgs) (Charge, error) {
	if args.Fail {
		fmt.Printf("  [chargeCard] FAILED for %s ($%d)\n", args.Customer, args.Amount)
		return Charge{}, fmt.Errorf("card declined for $%d", args.Amount)
	}
	ref := fmt.Sprintf("CH-%s-%d", args.Customer, args.Amount)
	fmt.Printf("  [chargeCard] charged %s\n", ref)
	return Charge{Ref: ref}, nil
}

func releaseFlight(_ *resonate.Context, args ReleaseArgs) (ReleaseResult, error) {
	fmt.Printf("  [releaseFlight] released %s\n", args.Ref)
	return ReleaseResult{Released: args.Ref}, nil
}

func releaseHotel(_ *resonate.Context, args ReleaseArgs) (ReleaseResult, error) {
	fmt.Printf("  [releaseHotel] released %s\n", args.Ref)
	return ReleaseResult{Released: args.Ref}, nil
}

// ── Saga orchestrator ───────────────────────────────────────────────────

func bookTrip(ctx *resonate.Context, args TripArgs) (TripResult, error) {
	// Step 1: flight
	f1, err := ctx.RPC("reserveFlight", FlightArgs{
		Customer: args.Customer, From: args.From, To: args.To,
	})
	if err != nil {
		return TripResult{}, err
	}
	var flight Reservation
	if err := f1.Await(&flight); err != nil {
		return TripResult{}, fmt.Errorf("reserveFlight: %w", err)
	}

	// Step 2: hotel
	f2, err := ctx.RPC("reserveHotel", HotelArgs{
		Customer: args.Customer, City: args.To, Fail: args.FailAt == "hotel",
	})
	if err != nil {
		return TripResult{}, err
	}
	var hotel Reservation
	if err := f2.Await(&hotel); err != nil {
		compensate(ctx, "", flight.Ref)
		return TripResult{}, fmt.Errorf("reserveHotel: %w", err)
	}

	// Step 3: charge
	f3, err := ctx.RPC("chargeCard", ChargeArgs{
		Customer: args.Customer, Amount: args.Amount, Fail: args.FailAt == "charge",
	})
	if err != nil {
		return TripResult{}, err
	}
	var charge Charge
	if err := f3.Await(&charge); err != nil {
		compensate(ctx, hotel.Ref, flight.Ref)
		return TripResult{}, fmt.Errorf("chargeCard: %w", err)
	}

	return TripResult{FlightRef: flight.Ref, HotelRef: hotel.Ref, ChargeRef: charge.Ref}, nil
}

// compensate runs the inverse of any completed steps in reverse order. Empty
// refs are skipped. Compensation failures are logged but not surfaced — the
// saga has already failed; the goal is best-effort rollback.
func compensate(ctx *resonate.Context, hotelRef, flightRef string) {
	fmt.Println("  [bookTrip] running compensations...")
	if hotelRef != "" {
		if f, err := ctx.RPC("releaseHotel", ReleaseArgs{Ref: hotelRef}); err == nil {
			var r ReleaseResult
			if err := f.Await(&r); err != nil {
				fmt.Printf("  [bookTrip] releaseHotel failed: %v\n", err)
			}
		}
	}
	if flightRef != "" {
		if f, err := ctx.RPC("releaseFlight", ReleaseArgs{Ref: flightRef}); err == nil {
			var r ReleaseResult
			if err := f.Await(&r); err != nil {
				fmt.Printf("  [bookTrip] releaseFlight failed: %v\n", err)
			}
		}
	}
}

// ── main ────────────────────────────────────────────────────────────────

func main() {
	failAt := flag.String("fail", "", "force a step to fail: \"\" | \"hotel\" | \"charge\"")
	flag.Parse()

	r, err := resonate.New(resonate.Config{URL: "http://localhost:8001"})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	bookFn, err := resonate.Register(r, "bookTrip", bookTrip)
	if err != nil {
		log.Fatalf("Register bookTrip: %v", err)
	}
	if _, err := resonate.Register(r, "reserveFlight", reserveFlight); err != nil {
		log.Fatalf("Register reserveFlight: %v", err)
	}
	if _, err := resonate.Register(r, "reserveHotel", reserveHotel); err != nil {
		log.Fatalf("Register reserveHotel: %v", err)
	}
	if _, err := resonate.Register(r, "chargeCard", chargeCard); err != nil {
		log.Fatalf("Register chargeCard: %v", err)
	}
	if _, err := resonate.Register(r, "releaseFlight", releaseFlight); err != nil {
		log.Fatalf("Register releaseFlight: %v", err)
	}
	if _, err := resonate.Register(r, "releaseHotel", releaseHotel); err != nil {
		log.Fatalf("Register releaseHotel: %v", err)
	}

	ctx := context.Background()
	id := fmt.Sprintf("saga-%d", time.Now().UnixNano())
	args := TripArgs{
		Customer: "alice",
		From:     "SFO",
		To:       "JFK",
		Amount:   850,
		FailAt:   *failAt,
	}

	fmt.Printf("[bookTrip] starting workflow id=%s fail_at=%q\n", id, *failAt)
	h, err := bookFn.Run(ctx, id, args)
	if err != nil {
		log.Fatalf("Run: %v", err)
	}

	out, err := h.Result(ctx)
	if err != nil {
		fmt.Printf("[bookTrip] FAILED: %v\n", err)
		return
	}
	fmt.Printf("[bookTrip] OK: flight=%s hotel=%s charge=%s\n",
		out.FlightRef, out.HotelRef, out.ChargeRef)
}
