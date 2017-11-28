package forcesleep

import (
	"net/http"
	"os"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MetricsServer struct {
	Config     MetricsConfig
	Controller *Sleeper
}

type MetricsConfig struct {
	BindAddr       string `json:"bindAddr" yaml:"bindAddr"`
	CollectRuntime bool   `json:"collectRuntime" yaml:"collectRuntime"`
	CollectCache   bool   `json:"collectCache" yaml:"collectCache"`
}

const MetricsEndpoint = "/metrics"

var CacheSizeMetric = prometheus.NewDesc(
	"forcesleep_cache_size",
	"Number of resources currently held in cache",
	[]string{},
	prometheus.Labels{},
)

func (s *MetricsServer) Serve() error {
	registry := prometheus.NewRegistry()

	if s.Config.CollectRuntime {
		if err := registry.Register(prometheus.NewGoCollector()); err != nil {
			return err
		}

		if err := registry.Register(prometheus.NewProcessCollector(os.Getpid(), "")); err != nil {
			return err
		}
	}

	if s.Config.CollectCache {
		if err := registry.Register(NewSizeCollector(s.Controller)); err != nil {
			return err
		}
	}

	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorLog: &logWrapper{},
	})

	mux := http.NewServeMux()
	mux.Handle(MetricsEndpoint, handler)
	server := &http.Server{
		Addr:    s.Config.BindAddr,
		Handler: mux,
	}

	return server.ListenAndServe()
}

type logWrapper struct{}

func (l *logWrapper) Println(v ...interface{}) {
	glog.V(0).Info(v)
}

type SizeCollector struct {
	controller *Sleeper
}

func NewSizeCollector(controller *Sleeper) *SizeCollector {
	return &SizeCollector{
		controller: controller,
	}
}

func (c *SizeCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- CacheSizeMetric
}

func (c *SizeCollector) Collect(ch chan<- prometheus.Metric) {
	size := float64(len(c.controller.resources.Indexer.ListKeys()))
	ch <- prometheus.MustNewConstMetric(
		CacheSizeMetric,
		prometheus.GaugeValue,
		size,
	)
}
