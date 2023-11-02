package simulator

import "github.com/resonatehq/resonate/test/harness/src/store"

// Generator is responsible for telling the test what operations to perform
// on the implementation during the test. It outputs a sequence of operations.
type Generator struct{}

func NewGenerator() *Generator {
	return &Generator{}
}

func (g *Generator) Generate() []store.Operation {
	ops := make([]store.Operation, 0)
	return ops
}

// TODO: look at dst for inspiration
