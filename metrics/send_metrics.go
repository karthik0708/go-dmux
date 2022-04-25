package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)


func Init(port int) {
	go send(port)
}

func send(port int) {
	addr := flag.String("listen-address", ":"+strconv.Itoa(port), "The address to listen on for HTTP requests.")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}
