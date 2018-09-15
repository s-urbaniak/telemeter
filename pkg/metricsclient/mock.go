package metricsclient

import (
	"context"
	"math/rand"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientmodel "github.com/prometheus/client_model/go"
)

type mock struct {
	gauge    prometheus.Gauge
	registry *prometheus.Registry
}

func NewMock() *mock {
	r := prometheus.NewRegistry()
	g := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "etcd_object_counts",
			Help: "This is a mock metric.",
		},
	)
	r.MustRegister(g)

	return &mock{gauge: g, registry: r}
}

func (m *mock) Retrieve(context.Context, *http.Request) ([]*clientmodel.MetricFamily, error) {
	m.gauge.Set(rand.Float64())
	families, err := m.registry.Gather()
	if err != nil {
		return nil, err
	}

	// Prom doesn't have an API for this.
	scrapeTimestamp := time.Now().UnixNano() / int64(time.Millisecond)

	for _, f := range families {
		for _, m := range f.Metric {
			m.TimestampMs = &scrapeTimestamp
		}
	}

	return families, nil
}
