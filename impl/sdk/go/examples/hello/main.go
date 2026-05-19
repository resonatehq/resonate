// hello is a minimal example of using the Resonate SDK against an
// in-process local network. It registers a function, invokes it durably,
// and prints the result.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

type GreetArgs struct {
	Name string `json:"name"`
}

func greet(_ *resonate.Context, args GreetArgs) (string, error) {
	return fmt.Sprintf("hello, %s!", args.Name), nil
}

func main() {
	r, err := resonate.New(resonate.Config{
		URL: "http://localhost:8001",
	})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	greetFn, err := resonate.Register(r, "greet", greet)
	if err != nil {
		log.Fatalf("Register: %v", err)
	}

	ctx := context.Background()
	id := fmt.Sprintf("hello-%d", time.Now().UnixNano())
	h, err := greetFn.Run(ctx, id, GreetArgs{Name: "world"})
	if err != nil {
		log.Fatalf("Run: %v", err)
	}

	out, err := h.Result(ctx)
	if err != nil {
		log.Fatalf("Result: %v", err)
	}
	fmt.Println(out)
}
