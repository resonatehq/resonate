//go:build sqs || aws || all

package cmd

import "github.com/resonatehq/resonate/internal/app/plugins/sqs"

func init() {
	cfg.AIO.Plugins.Add("sqs", false, &sqs.Config{})
}
