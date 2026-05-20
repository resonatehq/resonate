// pipeline shows a multi-stage DAG-shaped durable workflow:
//
//	download → parse → ┬─ transformA ─┐
//	                   └─ transformB ─┴─ merge → emit
//
// transformA and transformB run in parallel (both RPCs are dispatched
// before either is awaited); merge depends on both and synchronizes them
// with Await. Every stage is a registered function backed by a durable
// promise, so a crash mid-pipeline picks up at the first unsettled stage
// without re-doing completed work.
//
// Run a Resonate server on localhost:8001 first, then:
//
//	go run .
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	resonate "github.com/resonatehq/resonate-sdk-go"
)

// ── Domain types ────────────────────────────────────────────────────────

type PipelineArgs struct {
	URL string `json:"url"`
}

type Raw struct {
	Body string `json:"body"`
}

type Parsed struct {
	Words []string `json:"words"`
}

type TransformAResult struct {
	WordCount int `json:"word_count"`
}

type TransformBResult struct {
	Upper string `json:"upper"`
}

type MergeArgs struct {
	A TransformAResult `json:"a"`
	B TransformBResult `json:"b"`
}

type Merged struct {
	WordCount int    `json:"word_count"`
	Upper     string `json:"upper"`
}

type EmitResult struct {
	Sent string `json:"sent"`
}

// ── Stage functions ─────────────────────────────────────────────────────

func download(_ *resonate.Context, args PipelineArgs) (Raw, error) {
	body := fmt.Sprintf("the quick brown fox jumps over %s", args.URL)
	fmt.Printf("  [download] %s -> %d bytes\n", args.URL, len(body))
	return Raw{Body: body}, nil
}

func parseStage(_ *resonate.Context, raw Raw) (Parsed, error) {
	words := strings.Fields(raw.Body)
	fmt.Printf("  [parse] %d words\n", len(words))
	return Parsed{Words: words}, nil
}

func transformA(_ *resonate.Context, p Parsed) (TransformAResult, error) {
	fmt.Printf("  [transformA] counting words\n")
	return TransformAResult{WordCount: len(p.Words)}, nil
}

func transformB(_ *resonate.Context, p Parsed) (TransformBResult, error) {
	upper := strings.ToUpper(strings.Join(p.Words, " "))
	fmt.Printf("  [transformB] uppercased %d chars\n", len(upper))
	return TransformBResult{Upper: upper}, nil
}

func merge(_ *resonate.Context, args MergeArgs) (Merged, error) {
	fmt.Printf("  [merge] combining transforms\n")
	return Merged{WordCount: args.A.WordCount, Upper: args.B.Upper}, nil
}

func emit(_ *resonate.Context, m Merged) (EmitResult, error) {
	fmt.Printf("  [emit] words=%d upper=%q\n", m.WordCount, m.Upper)
	return EmitResult{Sent: "ok"}, nil
}

// ── Pipeline orchestrator ───────────────────────────────────────────────

func runPipeline(ctx *resonate.Context, args PipelineArgs) (EmitResult, error) {
	// download
	fDownload, err := ctx.RPC("download", args)
	if err != nil {
		return EmitResult{}, err
	}
	var raw Raw
	if err := fDownload.Await(&raw); err != nil {
		return EmitResult{}, fmt.Errorf("download: %w", err)
	}

	// parse
	fParse, err := ctx.RPC("parse", raw)
	if err != nil {
		return EmitResult{}, err
	}
	var parsed Parsed
	if err := fParse.Await(&parsed); err != nil {
		return EmitResult{}, fmt.Errorf("parse: %w", err)
	}

	// fan out: transformA and transformB in parallel
	fA, err := ctx.RPC("transformA", parsed)
	if err != nil {
		return EmitResult{}, err
	}
	fB, err := ctx.RPC("transformB", parsed)
	if err != nil {
		return EmitResult{}, err
	}

	// fan in
	var a TransformAResult
	if err := fA.Await(&a); err != nil {
		return EmitResult{}, fmt.Errorf("transformA: %w", err)
	}
	var b TransformBResult
	if err := fB.Await(&b); err != nil {
		return EmitResult{}, fmt.Errorf("transformB: %w", err)
	}

	// merge
	fMerge, err := ctx.RPC("merge", MergeArgs{A: a, B: b})
	if err != nil {
		return EmitResult{}, err
	}
	var merged Merged
	if err := fMerge.Await(&merged); err != nil {
		return EmitResult{}, fmt.Errorf("merge: %w", err)
	}

	// emit
	fEmit, err := ctx.RPC("emit", merged)
	if err != nil {
		return EmitResult{}, err
	}
	var out EmitResult
	if err := fEmit.Await(&out); err != nil {
		return EmitResult{}, fmt.Errorf("emit: %w", err)
	}
	return out, nil
}

// ── main ────────────────────────────────────────────────────────────────

func main() {
	r, err := resonate.New(resonate.Config{URL: "http://localhost:8001"})
	if err != nil {
		log.Fatalf("resonate.New: %v", err)
	}
	defer func() { _ = r.Stop() }()

	runFn, err := resonate.Register(r, "runPipeline", runPipeline)
	if err != nil {
		log.Fatalf("Register runPipeline: %v", err)
	}
	if _, err := resonate.Register(r, "download", download); err != nil {
		log.Fatalf("Register download: %v", err)
	}
	if _, err := resonate.Register(r, "parse", parseStage); err != nil {
		log.Fatalf("Register parse: %v", err)
	}
	if _, err := resonate.Register(r, "transformA", transformA); err != nil {
		log.Fatalf("Register transformA: %v", err)
	}
	if _, err := resonate.Register(r, "transformB", transformB); err != nil {
		log.Fatalf("Register transformB: %v", err)
	}
	if _, err := resonate.Register(r, "merge", merge); err != nil {
		log.Fatalf("Register merge: %v", err)
	}
	if _, err := resonate.Register(r, "emit", emit); err != nil {
		log.Fatalf("Register emit: %v", err)
	}

	ctx := context.Background()
	id := fmt.Sprintf("pipeline-%d", time.Now().UnixNano())
	fmt.Printf("[runPipeline] starting workflow id=%s\n", id)

	h, err := runFn.Run(ctx, id, PipelineArgs{URL: "example.com/doc"})
	if err != nil {
		log.Fatalf("Run: %v", err)
	}
	out, err := h.Result(ctx)
	if err != nil {
		log.Fatalf("Result: %v", err)
	}
	fmt.Printf("[runPipeline] OK: %+v\n", out)
}
