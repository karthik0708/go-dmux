package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

var MetricPort int

type PrometheusMetrics struct {
	partitionOwned *prometheus.GaugeVec
}

func (p *PrometheusMetrics) Init(){
	go p.displayMetrics()
	p.createMetrics()
	p.registerMetrics()
}

func (p *PrometheusMetrics) Ingest(metric interface{}){
		switch metric.(type) {
		case PartitionInfo:
			info := metric.(PartitionInfo)
			p.partitionOwned.With(prometheus.Labels{"topic": info.Topic, "consumerId":info.ConsumerId, "partitionId":strconv.Itoa(int(info.PartitionId))}).SetToCurrentTime()
		}
}

func  (p *PrometheusMetrics) displayMetrics() {
	addr := flag.String("listen-address", ":"+strconv.Itoa(MetricPort), "The address to listen on for HTTP requests.")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func (p *PrometheusMetrics) createMetrics(){
	p.partitionOwned = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "partition_owned",
			Help: "Metric to represent the partitions owned",
		}, []string{"topic","consumerId", "partitionId"})
}

func (p *PrometheusMetrics) registerMetrics() {
	prometheus.MustRegister(p.partitionOwned)
}
