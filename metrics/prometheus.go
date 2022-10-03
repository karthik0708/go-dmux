package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

var (
	// initialize all the metrics here
	offsetMetrics = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "offset_metrics",
			Help: "The metric represent all offset related metrics for dmux",
		}, []string{"key"})
)

type PrometheusConfig struct {
	//metricPort to which the metrics would be sent
	metricPort int
}
type PrometheusRegistry struct {
	// stateless
}

func (p PrometheusRegistry) start(config interface{}) {
	pConfig, ok := config.(PrometheusConfig)
	if !ok {
		log.Println("error in starting metric ingestion - invalid config")
		return
	}

	//The metrics can be fetched by a Get request from the http://localhost:9999/metrics end point
	go func(config PrometheusConfig) {
		addr := flag.String("listen-address", ":"+strconv.Itoa(config.metricPort), "The address to listen on for HTTP requests.")
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(*addr, nil))
	}(pConfig)

	//register collector for offset metrics
	prometheus.MustRegister(offsetMetrics)
}

//Ingest metrics as and when events are received
func (p PrometheusRegistry) ingest(metric Metric) {
	switch metric.Type {
	case Offset:
		offsetMetrics.WithLabelValues(metric.Name).Set(float64(metric.Value))
	}
}
