package main

import (
	hystrix_metric "github.com/afex/hystrix-go/hystrix/metric_collector"
	prometheus_hystrix_go "github.com/gjbae1212/prometheus-hystrix-go"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"os"

	"github.com/go-dmux/logging"
)

//

// **************** Bootstrap ***********

func main() {
	args := os.Args[1:]
	sz := len(args)

	var path string

	if sz == 1 {
		path = args[0]
	}

	dconf := DMuxConfigSetting{
		FilePath: path,
	}
	conf := dconf.GetDmuxConf()

	dmuxLogging := new(logging.DMuxLogging)
	dmuxLogging.Start(conf.Logging)

	c := Controller{config: conf}
	go c.start()

	log.Printf("config: %v", conf)

	for _, item := range conf.DMuxItems {
		go func(connType ConnectionType, connConf interface{}, logDebug bool, name string) {
			connType.Start(connConf, logDebug, name)
		}(item.ConnType, item.Connection, dmuxLogging.EnableDebug, item.Name)
	}

	wrapper := prometheus_hystrix_go.NewPrometheusCollector("hystrix", map[string]string{"app": "circuit"})

	// register and initialize to hystrix prometheus
	hystrix_metric.Registry.Register(wrapper)

	// start server
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":9999", nil)


	//main thread halts. TODO make changes to listen to kill and reboot
	select {}
}
