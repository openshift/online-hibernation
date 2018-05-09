package forcesleep

import (
	"net/http"
	"os"

	"github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/labels"
)

type MetricsServer struct {
	Config     MetricsConfig
	Controller *Sleeper
}

type MetricsConfig struct {
	CollectRuntime bool `json:"collectRuntime" yaml:"collectRuntime"`
	CollectCache   bool `json:"collectCache" yaml:"collectCache"`
}

var CacheSizeMetric = prometheus.NewDesc(
	"forcesleep_cache_size",
	"Number of resources currently held in cache",
	[]string{},
	prometheus.Labels{},
)

func (s *MetricsServer) Handler() (http.Handler, error) {
	registry := prometheus.NewRegistry()

	if s.Config.CollectRuntime {
		if err := registry.Register(prometheus.NewGoCollector()); err != nil {
			return nil, err
		}

		if err := registry.Register(prometheus.NewProcessCollector(os.Getpid(), "")); err != nil {
			return nil, err
		}
	}

	if s.Config.CollectCache {
		if err := registry.Register(NewSizeCollector(s.Controller)); err != nil {
			return nil, err
		}
	}

	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{
		ErrorLog: &logWrapper{},
	}), nil
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

// TODO: Fix size here...I have no idea what this should be
func (c *SizeCollector) Collect(ch chan<- prometheus.Metric) {
	projlist, err := c.controller.resourceStore.ProjectList.List(labels.Everything())
	if err != nil {
		glog.Errorf("Error: %s", err)
	}
	size := float64(len(projlist) * 10)
	ch <- prometheus.MustNewConstMetric(
		CacheSizeMetric,
		prometheus.GaugeValue,
		size,
	)
}
