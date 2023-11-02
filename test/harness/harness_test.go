package main

import (
	"testing"

	"github.com/resonatehq/resonate/test/harness/src/simulator"
	"github.com/stretchr/testify/suite"
)

// ref: https://github.com/jepsen-io/jepsen/tree/main
func TestMain(t *testing.T) {
	// config to point to already existing server or start resonate -- two modes
	// environment variable ??
	suite.Run(t, simulator.New("config"))
}
