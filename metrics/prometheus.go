package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

type pusher interface {
	push(p *PrometheusMetrics)
}
var MetricPort int

type PrometheusMetrics struct {

}

func (p *PrometheusMetrics) Init(){
	go p.displayMetrics()
}

//Ingest metrics as and when events are received from the channels
func (p *PrometheusMetrics) Ingest(metric interface{}){
}

//The metrics can be fetched by a Get request from the http://localhost:9999/metrics end point
func  (p *PrometheusMetrics) displayMetrics() {
	addr := flag.String("listen-address", ":"+strconv.Itoa(MetricPort), "The address to listen on for HTTP requests.")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

//Initialize all the metric collectors
func (p *PrometheusMetrics) createMetrics(){
}

//Register the metric collectors
func (p *PrometheusMetrics) registerMetrics() {
}