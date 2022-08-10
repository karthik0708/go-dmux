package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

type PrometheusConfig struct {
	//metricPort to which the metrics would be sent
	metricPort int

	//metric collector
	offsetMetrics *prometheus.GaugeVec
}

func (p *PrometheusConfig) init(){
	//The metrics can be fetched by a Get request from the http://localhost:9999/metrics end point
	go func(config *PrometheusConfig) {
		addr := flag.String("listen-address", ":"+strconv.Itoa(config.metricPort), "The address to listen on for HTTP requests.")
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(*addr, nil))
	}(p)

	offsetMetrics := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "offset_metrics",
			Help: "The metric represent offset related metrics for dmux",
		}, []string{"key"})

	p.offsetMetrics = offsetMetrics

	//register collector for offset metrics
	prometheus.MustRegister(p.offsetMetrics)
}

//Ingest metrics as and when events are received from the channels
func (p *PrometheusConfig) ingest(metric Metric){
	switch metric.MetricType {
	case prometheus.GaugeValue:
		p.offsetMetrics.WithLabelValues(metric.MetricName).Set(float64(metric.MetricValue))
	}
}
