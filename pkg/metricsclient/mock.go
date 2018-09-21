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
	gauges   []prometheus.Gauge
	registry *prometheus.Registry
}

func NewMock(metricNames []string) *mock {
	r := prometheus.NewRegistry()

	gauges := make([]prometheus.Gauge, len(metricNames))

	for i, name := range metricNames {
		gauges[i] = prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: name,
				Help: "mock",
			},
		)
		r.MustRegister(gauges[i])
	}

	return &mock{gauges: gauges, registry: r}
}

func (m *mock) Retrieve(_ context.Context, req *http.Request) ([]*clientmodel.MetricFamily, error) {
	for _, g := range m.gauges {
		g.Set(rand.Float64())
	}

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
