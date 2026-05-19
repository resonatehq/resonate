// fibonacci shows three ways to compose recursive durable invocations with
// the Resonate SDK:
//
//	-mode=rpc  every recursive call goes through ctx.RPC (server-dispatched,
//	           may execute on any worker in the group)
//	-mode=run  every recursive call goes through ctx.Run  (local goroutine,
//	           same worker)
//	-mode=mix  one branch via RPC, the other via Run
//
// Run a Resonate server on localhost:8001 first, then e.g.:
//
//	go run . -mode=rpc -n=10
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

type FibArgs struct {
	N int `json:"n"`
}

func fibRPC(ctx *resonate.Context, args FibArgs) (int, error) {
	if args.N < 2 {
		return args.N, nil
	}
	f1, err := ctx.RPC("fibRPC", FibArgs{N: args.N - 1})
	if err != nil {
		return 0, err
	}
	f2, err := ctx.RPC("fibRPC", FibArgs{N: args.N - 2})
	if err != nil {
		return 0, err
	}
	var a, b int
	if err := f1.Await(&a); err != nil {
		return 0, err
	}
	if err := f2.Await(&b); err != nil {
		return 0, err
	}
	return a + b, nil
}

func fibRun(ctx *resonate.Context, args FibArgs) (int, error) {
	if args.N < 2 {
		return args.N, nil
	}
	f1, err := ctx.Run(fibRun, FibArgs{N: args.N - 1})
	if err != nil {
		return 0, err
	}
	f2, err := ctx.Run(fibRun, FibArgs{N: args.N - 2})
	if err != nil {
		return 0, err
	}
	var a, b int
	if err := f1.Await(&a); err != nil {
		return 0, err
	}
	if err := f2.Await(&b); err != nil {
		return 0, err
	}
	return a + b, nil
}

func fibMix(ctx *resonate.Context, args FibArgs) (int, error) {
	if args.N < 2 {
		return args.N, nil
	}
	f1, err := ctx.RPC("fibMix", FibArgs{N: args.N - 1})
	if err != nil {
		return 0, err
	}
	f2, err := ctx.Run(fibMix, FibArgs{N: args.N - 2})
	if err != nil {
		return 0, err
	}
	var a, b int
	if err := f1.Await(&a); err != nil {
		return 0, err
	}
	if err := f2.Await(&b); err != nil {
		return 0, err
	}
	return a + b, nil
}

func main() {
	mode := flag.String("mode", "run", "rpc | run | mix")
	n := flag.Int("n", 10, "fibonacci input")
	flag.Parse()

	r, err := resonate.New(resonate.Config{
		URL: "http://localhost:8001",
	})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	rpcFn, err := resonate.Register(r, "fibRPC", fibRPC)
	if err != nil {
		log.Fatalf("Register fibRPC: %v", err)
	}
	runFn, err := resonate.Register(r, "fibRun", fibRun)
	if err != nil {
		log.Fatalf("Register fibRun: %v", err)
	}
	mixFn, err := resonate.Register(r, "fibMix", fibMix)
	if err != nil {
		log.Fatalf("Register fibMix: %v", err)
	}

	ctx := context.Background()
	id := fmt.Sprintf("fib-%s-%d-%d", *mode, *n, time.Now().UnixNano())
	args := FibArgs{N: *n}

	var (
		out  int
		rerr error
	)
	switch *mode {
	case "rpc":
		h, err := rpcFn.Run(ctx, id, args)
		if err != nil {
			log.Fatalf("Run: %v", err)
		}
		out, rerr = h.Result(ctx)
	case "run":
		h, err := runFn.Run(ctx, id, args)
		if err != nil {
			log.Fatalf("Run: %v", err)
		}
		out, rerr = h.Result(ctx)
	case "mix":
		h, err := mixFn.Run(ctx, id, args)
		if err != nil {
			log.Fatalf("Run: %v", err)
		}
		out, rerr = h.Result(ctx)
	default:
		log.Fatalf("unknown -mode %q (want rpc | run | mix)", *mode)
	}
	if rerr != nil {
		log.Fatalf("Result: %v", rerr)
	}
	fmt.Printf("fib(%d) = %d  [mode=%s]\n", *n, out, *mode)
}
