package server

import (
	"github.com/rcrowley/go-metrics"
)

var (
	syncerEps = metrics.NewMeter()
	applyEps  = metrics.NewMeter()

	proposeChannelSize = metrics.NewGauge()
)

func init() {
	metrics.Register("syncer_eps", syncerEps)
	metrics.Register("apply_eps", applyEps)
	metrics.Register("propose_channel_size", proposeChannelSize)
}
