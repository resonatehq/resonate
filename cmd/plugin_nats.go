//go:build nats || all

package cmd

import (
	natsPlugin "github.com/resonatehq/resonate/internal/app/plugins/nats"
)

func init() {
	cfg.AIO.Plugins.Add("nats", false, &natsPlugin.Config{})
}
