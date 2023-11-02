package main

import (
	"testing"

	"github.com/resonatehq/resonate/test/harness/src/simulator"
	"github.com/stretchr/testify/suite"
)

// ref: https://github.com/jepsen-io/jepsen/tree/main
func TestMain(t *testing.T) {
	suite.Run(t, simulator.New("config"))
}
