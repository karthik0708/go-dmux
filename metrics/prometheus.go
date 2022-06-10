package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

type PrometheusConfig struct {
	metricPort int
}

func (p *PrometheusConfig) init(){
	//The metrics can be fetched by a Get request from the http://localhost:9999/metrics end point
	go func(config *PrometheusConfig) {
		addr := flag.String("listen-address", ":"+strconv.Itoa(config.metricPort), "The address to listen on for HTTP requests.")
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(*addr, nil))
	}(p)
}

//Ingest metrics as and when events are received from the channels
func (p *PrometheusConfig) ingest(metric interface{}){
}

//Initialize all the metric collectors
func (p *PrometheusConfig) createMetrics(){
}

//Register the metric collectors
func (p *PrometheusConfig) registerMetrics() {
}