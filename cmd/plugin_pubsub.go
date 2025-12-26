//go:build pubsub || gcp || all

package cmd

import (
	"github.com/resonatehq/resonate/internal/app/plugins/pubsub"
)

func init() {
	cfg.AIO.Plugins.Add("pubsub", false, &pubsub.Config{})
}
