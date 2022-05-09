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
	LastSourceDetail map[string]map[int32]int64
	LastSinkDetail map[string]map[int32]int64

	sourceOffMetric *prometheus.GaugeVec
	sinkOffMetric *prometheus.GaugeVec
	lagOffMetric *prometheus.GaugeVec
	partitionOwned *prometheus.GaugeVec
}

func (p *PrometheusMetrics) Init(){
	go p.displayMetrics()
	p.LastSinkDetail = make(map[string]map[int32]int64)
	p.LastSourceDetail = make(map[string]map[int32]int64)
	p.createMetrics()
	p.registerMetrics()
}

func (p *PrometheusMetrics) Ingest(metric interface{}){
		switch metric.(type) {
		case SourceOffset:
			info := metric.(SourceOffset)
			p.sourceOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))

			if partitionDetail, ok := p.LastSourceDetail[info.Topic]; ok == true {
				partitionDetail[info.Partition] = info.Offset
				p.LastSourceDetail[info.Topic] = partitionDetail
			} else {
				partitionDetail = make(map[int32]int64)
				p.LastSourceDetail[info.Topic] = partitionDetail
			}
			if lastOffset, ok := p.LastSinkDetail[info.Topic][info.Partition]; ok == true{
				p.lagOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset - lastOffset))
			}

		case SinkOffset:
			info := metric.(SinkOffset)
			p.sinkOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))

			if partitionDetail, ok := p.LastSinkDetail[info.Topic]; ok == true{
				partitionDetail[info.Partition] = info.Offset
				p.LastSinkDetail[info.Topic] = partitionDetail
			} else{
				partitionDetail = make(map[int32]int64)
				p.LastSinkDetail[info.Topic] = partitionDetail
			}

			if lastOffset, ok := p.LastSourceDetail[info.Topic][info.Partition]; ok == true {
				p.lagOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(lastOffset - info.Offset))
			}
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

	p.sourceOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "source_offset",
			Help: "Metric to represent the offset at source",
		}, []string{"topic","partition"})

	p.sinkOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sink_offset",
			Help: "Metric to represent the offset at sink",
		}, []string{"topic","partition"})

	p.lagOffMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lag",
			Help: "Metric to represent the lag between source and sink",
		}, []string{"topic","partition"})

	p.partitionOwned = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "partition_owned",
			Help: "Metric to represent the partitions owned",
		}, []string{"topic","consumerId", "partitionId"})
}

func (p *PrometheusMetrics) registerMetrics() {
	prometheus.MustRegister(p.sourceOffMetric)
	prometheus.MustRegister(p.sinkOffMetric)
	prometheus.MustRegister(p.lagOffMetric)
	prometheus.MustRegister(p.partitionOwned)
}
