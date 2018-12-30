package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/flike/kingbus/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rcrowley/go-metrics"
)

// PrometheusServer provides a container with config parameters for the
// Prometheus Exporter
type PrometheusServer struct {
	addr   string
	server *http.Server
	ctx    context.Context
	cancel context.CancelFunc

	namespace     string
	registry      metrics.Registry // Registry to be exported
	subsystem     string
	promRegistry  prometheus.Registerer //Prometheus registry
	flushInterval time.Duration         //interval to update prom metrics
	tick          *time.Timer
	gauges        map[string]prometheus.Gauge
}

// NewPrometheusServer returns a Provider that produces Prometheus metrics.
// Namespace and subsystem are applied to all produced metrics.
func NewPrometheusServer(addr string, r metrics.Registry,
	promRegistry prometheus.Registerer, FlushInterval time.Duration) *PrometheusServer {
	if len(addr) == 0 {
		return nil
	}

	p := &PrometheusServer{
		addr:          addr,
		namespace:     "kingbus",
		subsystem:     "metrics",
		registry:      r,
		promRegistry:  promRegistry,
		flushInterval: FlushInterval,
		tick:          time.NewTimer(FlushInterval),
		gauges:        make(map[string]prometheus.Gauge),
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	p.server = &http.Server{Addr: addr, Handler: mux}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return p
}

func (c *PrometheusServer) flattenKey(key string) string {
	key = strings.Replace(key, " ", "_", -1)
	key = strings.Replace(key, ".", "_", -1)
	key = strings.Replace(key, "-", "_", -1)
	key = strings.Replace(key, "=", "_", -1)
	return key
}

func (c *PrometheusServer) gaugeFromNameAndValue(name string, val float64) {
	key := fmt.Sprintf("%s_%s_%s", c.namespace, c.subsystem, name)
	g, ok := c.gauges[key]
	if !ok {
		g = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: c.flattenKey(c.namespace),
			Subsystem: c.flattenKey(c.subsystem),
			Name:      c.flattenKey(name),
			Help:      name,
		})
		c.promRegistry.MustRegister(g)
		c.gauges[key] = g
	}
	g.Set(val)
}

//Run prometheus server
func (c *PrometheusServer) Run() {
	go c.updatePrometheusMetrics()
	err := c.server.ListenAndServe()
	if err != nil {
		log.Log.Infof("PrometheusServer ListenAndServe error,err:%s", err)
	}
}

//Stop prometheus server
func (c *PrometheusServer) Stop() {
	c.cancel()
	c.tick.Stop()
	c.server.Shutdown(nil)
}

func (c *PrometheusServer) updatePrometheusMetrics() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.tick.C:
			log.Log.Infof("update Prometheus Metrics Once")
			c.updatePrometheusMetricsOnce()
		}
	}
}

func (c *PrometheusServer) updatePrometheusMetricsOnce() error {
	c.registry.Each(func(name string, i interface{}) {
		switch metric := i.(type) {
		case metrics.Counter:
			c.gaugeFromNameAndValue(name, float64(metric.Count()))
		case metrics.Gauge:
			c.gaugeFromNameAndValue(name, float64(metric.Value()))
		case metrics.GaugeFloat64:
			c.gaugeFromNameAndValue(name, float64(metric.Value()))
		case metrics.Histogram:
			snap := metric.Snapshot()
			c.gaugeFromNameAndValue(name+".mean", snap.Mean())
			c.gaugeFromNameAndValue(name+".p95", snap.Percentile(0.95))
			c.gaugeFromNameAndValue(name+".max", float64(snap.Max()))
		case metrics.Meter:
			lastSample := metric.Snapshot().Rate1()
			c.gaugeFromNameAndValue(name, float64(lastSample))
		case metrics.Timer:
			lastSample := metric.Snapshot().Rate1()
			c.gaugeFromNameAndValue(name, float64(lastSample))
		}
	})
	return nil
}
