package storage

import (
	"github.com/flike/kingbus/utils"
	"github.com/rcrowley/go-metrics"
)

var (
	readTps        = metrics.NewMeter()
	readThroughput = metrics.NewMeter()
	readLatency    = metrics.NewHistogram(utils.NewUniformSample(8192))

	writeTps        = metrics.NewMeter()
	writeThroughput = metrics.NewMeter()
	writeLatency    = metrics.NewHistogram(utils.NewUniformSample(8192))
)

func init() {
	metrics.Register("read_tps", readTps)
	metrics.Register("read_throughput", readThroughput)
	metrics.Register("read_latency", readLatency)

	metrics.Register("write_tps", writeTps)
	metrics.Register("write_throughput", writeThroughput)
	metrics.Register("write_latency", writeLatency)
}
