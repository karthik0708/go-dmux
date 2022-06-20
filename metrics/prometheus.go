package metrics

import (
	"flag"
	hystrix_metric "github.com/afex/hystrix-go/hystrix/metric_collector"
	prometheus_hystrix_go "github.com/gjbae1212/prometheus-hystrix-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

type PrometheusConfig struct {
	metricPort int
}

func (p *PrometheusConfig) init(){
	//Create a wrapper for collecting metrics from hystrix
	wrapper := prometheus_hystrix_go.NewPrometheusCollector("hystrix", map[string]string{"app": "circuit_breaker"})

	// register and initialize to hystrix prometheus
	hystrix_metric.Registry.Register(wrapper)

	//The metrics can be fetched by a Get request from the http://localhost:9999/metrics end point
	go func(config *PrometheusConfig) {
		addr := flag.String("listen-address", ":"+strconv.Itoa(config.metricPort), "The address to listen on for HTTP requests.")
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(*addr, nil))
	}(p)
}