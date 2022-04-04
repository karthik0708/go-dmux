package main

import (
	"flag"
	"github.com/go-dmux/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/go-dmux/logging"
)

//

// **************** Bootstrap ***********

func main() {
	generateSampleMetric()
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
		go func(connType ConnectionType, connConf interface{}, logDebug bool) {
			connType.Start(connConf, logDebug)
		}(item.ConnType, item.Connection, dmuxLogging.EnableDebug)
	}

	//main thread halts. TODO make changes to listen to kill and reboot
	select {}
}

var (
	uniformDomain     = flag.Float64("uniform.domain", 0.0002, "The domain for the uniform distribution.")
	normDomain        = flag.Float64("normal.domain", 0.0002, "The domain for the normal distribution.")
	normMean          = flag.Float64("normal.mean", 0.00001, "The mean for the normal distribution.")
	oscillationPeriod = flag.Duration("oscillation-period", 10*time.Minute, "The duration of the rate oscillation period.")
)

func generateSampleMetric(){
		metrics.Init()
		rpcDurations := prometheus.NewSummaryVec(
			prometheus.SummaryOpts{
				Name:       "rpc_durations_seconds",
				Help:       "RPC latency distributions.",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			},
			[]string{"service"},
		)
		prometheus.MustRegister(rpcDurations)
		oscillationFactor := func() float64 {
			return 2 + math.Sin(math.Sin(2*math.Pi*float64(time.Since(time.Now()))/float64(*oscillationPeriod)))
		}

		go func() {
			for {
				v := rand.Float64() * *uniformDomain
				rpcDurations.WithLabelValues("uniform").Observe(v)
				time.Sleep(time.Duration(100*oscillationFactor()) * time.Millisecond)
			}
		}()

		go func() {
			for {
				v := rand.ExpFloat64() / 1e6
				rpcDurations.WithLabelValues("exponential").Observe(v)
				time.Sleep(time.Duration(50*oscillationFactor()) * time.Millisecond)
			}
		}()
}
