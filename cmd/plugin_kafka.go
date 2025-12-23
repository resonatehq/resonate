//go:build kafka || all

package cmd

import (
	kafkaPlugin "github.com/resonatehq/resonate/internal/app/plugins/kafka"
	"github.com/resonatehq/resonate/internal/app/subsystems/api/kafka"
)

func init() {
	cfg.API.Subsystems.Add("kafka", false, &kafka.Config{})
	cfg.AIO.Plugins.Add("kafka", false, &kafkaPlugin.Config{})
}
