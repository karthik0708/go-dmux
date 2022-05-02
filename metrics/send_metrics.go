package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"strconv"
)

type (
	SinkOffset OffsetInfo
	SourceOffset OffsetInfo
	LagOffset OffsetInfo
)

type Metrics interface {
	Init()
	displayMetrics()
	createMetrics()
	registerMetrics()
	TrackMetrics(sourceCh <- chan SourceOffset, sinkCh <- chan SinkOffset)
	pushMetric(metrics *PrometheusMetrics)
}

type OffsetInfo struct {
	Topic string
	Partition int32
	Offset int64
}

type PrometheusMetrics struct {
	LastSourceDetail map[string]map[int32]int64
	LastSinkDetail map[string]map[int32]int64

	sourceOffMetric *prometheus.GaugeVec
	sinkOffMetric *prometheus.GaugeVec
	lagOffMetric *prometheus.GaugeVec
}

var MetricPort int

func (metrics *PrometheusMetrics) Init() {
	go metrics.displayMetrics()
	metrics.createMetrics()
	metrics.registerMetrics()
}

func  (metrics *PrometheusMetrics) displayMetrics() {
	addr := flag.String("listen-address", ":"+strconv.Itoa(MetricPort), "The address to listen on for HTTP requests.")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

func (metrics *PrometheusMetrics) createMetrics(){
	metrics.sourceOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "source_offset",
			Help: "Metric to represent the offset at source",
		}, []string{"topic","partition"})

	metrics.sinkOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sink_offset",
			Help: "Metric to represent the offset at sink",
		}, []string{"topic","partition"})

	metrics.lagOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lag",
			Help: "Metric to represent the lag between source and sink",
		}, []string{"topic","partition"})
}

func (metrics *PrometheusMetrics) registerMetrics() {
	prometheus.MustRegister(metrics.sourceOffMetric)
	prometheus.MustRegister(metrics.sinkOffMetric)
	prometheus.MustRegister(metrics.lagOffMetric)
}

func (metrics *PrometheusMetrics)TrackMetrics(sourceCh <- chan SourceOffset, sinkCh <- chan SinkOffset){
	for{
		select {
		case info := <- sourceCh:
			metrics.pushMetric(info)

			if partitionDetail, ok := metrics.LastSourceDetail[info.Topic]; ok == true{
				partitionDetail[info.Partition] = info.Offset
				metrics.LastSourceDetail[info.Topic] = partitionDetail
			} else{
				partitionDetail = make(map[int32]int64)
				metrics.LastSourceDetail[info.Topic] = partitionDetail
			}

			if lastOffset, ok := metrics.LastSinkDetail[info.Topic][info.Partition]; ok == true{
				metrics.pushMetric(LagOffset{Topic: info.Topic, Partition: info.Partition, Offset: info.Offset - lastOffset})
			}
		case info := <- sinkCh:
			metrics.pushMetric(info)

			if partitionDetail, ok := metrics.LastSinkDetail[info.Topic]; ok == true{
				partitionDetail[info.Partition] = info.Offset
				metrics.LastSinkDetail[info.Topic] = partitionDetail
			} else{
				partitionDetail = make(map[int32]int64)
				metrics.LastSinkDetail[info.Topic] = partitionDetail
			}

			if lastOffset, ok := metrics.LastSourceDetail[info.Topic][info.Partition]; ok == true {
				metrics.pushMetric(LagOffset{Topic: info.Topic, Partition: info.Partition, Offset: lastOffset - info.Offset})
			}
		}
	}
}

func (metrics *PrometheusMetrics) pushMetric(metric interface{}){
	switch metric.(type) {
	case SourceOffset:
		info := metric.(SourceOffset)
		metrics.sourceOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	case SinkOffset:
		info := metric.(SinkOffset)
		metrics.sinkOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	case LagOffset:
		info := metric.(LagOffset)
		metrics.lagOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	}
}
