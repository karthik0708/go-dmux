package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"log"
	"math"
	"net/http"
	"strconv"
)

//pusher is an interface where the methods are implemented by the metric type
type pusher interface {
	push(p *PrometheusMetrics)
}

type PrometheusMetrics struct {
	//metric collectors
	sourceOffMetric *prometheus.GaugeVec
	sinkOffMetric *prometheus.GaugeVec
	lagOffMetric *prometheus.GaugeVec
	partitionOwned *prometheus.GaugeVec
}

func (p *PrometheusMetrics) Init(){
	go p.displayMetrics()

	p.createMetrics()
	p.registerMetrics()
}

//Ingest metrics as and when events are received from the channels
func (p *PrometheusMetrics) Ingest(metric interface{}){
		switch metric.(type) {
		case SourceOffset:
			m := metric.(SourceOffset)
			m.push(p)
		case SinkOffset:
			m := metric.(SinkOffset)
			m.push(p)
		case PartitionInfo:
			m := metric.(PartitionInfo)
			m.push(p)
		}
}

//The metrics are exposed to "http://localhost:<port>/metrics" endpoint
func  (p *PrometheusMetrics) displayMetrics() {
	addr := flag.String("listen-address", ":"+strconv.Itoa(MetricPort), "The address to listen on for HTTP requests.")
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))
}

//Initialize all the metric collectors
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

//Register the metric collectors
func (p *PrometheusMetrics) registerMetrics() {
	prometheus.MustRegister(p.sourceOffMetric)
	prometheus.MustRegister(p.sinkOffMetric)
	prometheus.MustRegister(p.lagOffMetric)
	prometheus.MustRegister(p.partitionOwned)
}

func (info *SourceOffset) push(p *PrometheusMetrics){
	//Push the latest source offset for the corresponding topic and partition
	p.sourceOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	p.updateLag(*info)
}

func (info *SinkOffset) push(p *PrometheusMetrics){
	//Push the latest sink offset for the corresponding topic and partition
	p.sinkOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	p.updateLag(*info)
}

func (info *PartitionInfo) push(p *PrometheusMetrics){
	//Push the time stamp at which the partition joined dmux
	p.partitionOwned.With(prometheus.Labels{"topic": info.Topic, "consumerId":info.ConsumerId, "partitionId":strconv.Itoa(int(info.PartitionId))}).SetToCurrentTime()
}

func (p *PrometheusMetrics) updateLag(newInfo interface{}) {
	switch newInfo.(type) {
	case SourceOffset:
		info := newInfo.(SourceOffset)
		if lastSinkOffset, err := p.sinkOffMetric.GetMetricWith(prometheus.Labels{"topic" : info.Topic, "partition":strconv.Itoa(int(info.Partition))});err==nil{
			//Push lag metric based on the last sink offset and the new source offset
			p.pushLag(lastSinkOffset, info.Topic, strconv.Itoa(int(info.Partition)), info.Offset)
		}
	case SinkOffset:
		info := newInfo.(SinkOffset)
		if lastSourceOffset, err := p.sourceOffMetric.GetMetricWith(prometheus.Labels{"topic" : info.Topic, "partition":strconv.Itoa(int(info.Partition))});err==nil{
			//Push lag metric based on the last source offset and the new sink offset
			p.pushLag(lastSourceOffset, info.Topic, strconv.Itoa(int(info.Partition)), info.Offset)
		}
	}
}

func (p *PrometheusMetrics) pushLag(lastOffsetGauge prometheus.Gauge, topic string, partition string, offset int64){
	var lastOffset = &dto.Metric{}
	lastOffsetGauge.Write(lastOffset)
	lastOffsetValue := *lastOffset.Gauge.Value
	//Compute the lag which is difference between source and sink offset
	lag := math.Abs(lastOffsetValue - float64(offset))
	//push lag
	p.lagOffMetric.WithLabelValues(topic, partition).Set(lag)
}
