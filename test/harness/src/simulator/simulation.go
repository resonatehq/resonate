package simulator

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/resonatehq/resonate/test/harness/src/checker"
	"github.com/resonatehq/resonate/test/harness/src/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

// Simulation is a client
type Simulation struct {
	suite.Suite

	System *System
}

func New(config string) *Simulation {
	// parse
	return &Simulation{}
}

func (s *Simulation) SetupSuite() {
	// responsible for setting up the environment for tests to target
	ctx := context.Background()

	var err error
	s.System, err = NewSystem(ctx)
	if err != nil {
		s.T().Fatal(err)
	}

	// readiness probe
	var ready bool
	for i := 0; i < 10; i++ {
		if s.System.IsReady() {
			ready = true
			break
		}

		time.Sleep(3 * time.Second)
	}

	if !ready {
		s.T().Fatal("server did not become ready in time.")
	}
}

func (s *Simulation) TearDownSuite() {}

func (s *Simulation) TestSingleClientCorrectness() {
	defer func() {
		if r := recover(); r != nil {
			s.TearDownSuite()
			panic(r)
		}
	}()

	s.T().Run("single client correctness", func(t *testing.T) {
		test := NewTest(
			WithClient(NewClient(s.System.HttpURL)),
			WithGenerator(NewGenerator()),
			WithChecker(checker.New()),
		)

		assert.Nil(t, test.Run()) // output: server logs, checker data
	})
}

type Test struct {
	Name      string
	Client    *Client
	Generator *Generator
	Checker   *checker.Checker
}

type TestOption func(*Test)

func NewTest(opts ...TestOption) *Test {
	test := &Test{}
	for _, opt := range opts {
		opt(test)
	}
	return test
}

func WithClient(c *Client) TestOption {
	return func(t *Test) {
		t.Client = c
	}
}

func WithGenerator(g *Generator) TestOption {
	return func(t *Test) {
		t.Generator = g
	}
}

func WithChecker(c *checker.Checker) TestOption {
	return func(t *Test) {
		t.Checker = c
	}
}

func (t *Test) Run() error {
	log.Println("running tests...")

	s := store.NewStore()

	// ops := t.Generator.Generate()
	ops := []store.Operation{
		{
			API:   store.Create,
			Value: []byte(`'{"param":{"data": "'"$(echo -n 'Durable Promise Created' | base64)"'"},"timeout": 2524608000000}'`),
		},
	}

	for _, op := range ops {
		s.Add(t.Client.Invoke(op))
	}

	return t.Checker.Check(s.History())
}
