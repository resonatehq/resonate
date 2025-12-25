//go:build nats || all

package cmd

import (
	natsPlugin "github.com/resonatehq/resonate/internal/app/plugins/nats"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/nats"
)

func init() {
	cfg.API.Subsystems.Add("nats", false, &nats.Config{})
	cfg.AIO.Plugins.Add("nats", false, &natsPlugin.Config{})
}
