package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
)


func Init() {
	go send()
}

func send() {
	addr := flag.String("listen-address", ":9999", "The address to listen on for HTTP requests.")

	http.Handle("/metrics", promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{}))
	promhttp.HandlerFor(prometheus.DefaultGatherer, promhttp.HandlerOpts{})

	log.Fatal(http.ListenAndServe(*addr, nil))
}
