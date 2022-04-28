package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)


func Init(port int) {
	go send(port)
}

type OffsetInfo struct {
	Topic string
	Partition int32
	Offset int64
}
func send(port int) {
	addr := flag.String("listen-address", ":"+strconv.Itoa(port), "The address to listen on for HTTP requests.")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func PopulateMetrics(sourceCh <- chan OffsetInfo, sinkCh <- chan OffsetInfo){
	sourceOffsets := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "source_offset",
			Help: "Metric to represent the offset at source",
		}, []string{"topic","partition"})

	sinkOffests := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sink_offset",
			Help: "Metric to represent the offset at sink",
		}, []string{"topic","partition"})

	lag := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lag",
			Help: "Metric to represent the lag between source and sink",
		}, []string{"topic","partition"})

	prometheus.MustRegister(sinkOffests)
	prometheus.MustRegister(sourceOffsets)
	prometheus.MustRegister(lag)

	lastSourceDetail := make(map[string]map[int32]int64)
	lastSinkDetail := make(map[string]map[int32]int64)

	lastSourcePartition := make(map[int32]int64)
	lastSinkPartition := make(map[int32]int64)

	for{
		select {
		case info := <- sourceCh:
			sourceOffsets.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))

			lastSourcePartition[info.Partition] = info.Offset
			lastSourceDetail[info.Topic] = lastSourcePartition

			if lastOffset, ok :=lastSinkDetail[info.Topic][info.Partition]; ok == true{
				lag.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset - lastOffset))
			}
		case info := <- sinkCh:
			sinkOffests.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))

			lastSinkPartition[info.Partition] = info.Offset
			lastSinkDetail[info.Topic] = lastSinkPartition

			if lastOffset, ok := lastSourceDetail[info.Topic][info.Partition]; ok == true {
				lag.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(lastOffset - info.Offset))
			}
		}
	}
}
