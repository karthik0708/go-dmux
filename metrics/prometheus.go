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

type PrometheusConfig struct {
	//keep track of last source and sink details for lag calculation
	lastSourceDetail map[string]map[int32]int64
	lastSinkDetail map[string]map[int32]int64

	//metric collectors
	sourceOffMetric *prometheus.GaugeVec
	sinkOffMetric *prometheus.GaugeVec
	lagOffMetric *prometheus.GaugeVec
	partitionOwned *prometheus.GaugeVec

	//metricPort to which the metrics would be sent
	metricPort int

	//Max topics and max partitions per topic
	maxTopics int
	maxPartitions int
}

func (p *PrometheusConfig) init(){
	//The metrics can be fetched by a Get request from the http://localhost:9999/metrics end point
	go func(config *PrometheusConfig) {
		addr := flag.String("listen-address", ":"+strconv.Itoa(config.metricPort), "The address to listen on for HTTP requests.")
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(*addr, nil))
	}(p)

	p.lastSinkDetail = make(map[string]map[int32]int64, p.maxTopics)
	p.lastSourceDetail = make(map[string]map[int32]int64, p.maxTopics)

	p.createMetrics()
	p.registerMetrics()
}

//Initialize all the metric collectors
func (p *PrometheusConfig) createMetrics(){
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
func (p *PrometheusConfig) registerMetrics() {
	prometheus.MustRegister(p.sourceOffMetric)
	prometheus.MustRegister(p.sinkOffMetric)
	prometheus.MustRegister(p.lagOffMetric)
	prometheus.MustRegister(p.partitionOwned)
}

//Ingest metrics as and when events are received from the channels
func (p *PrometheusConfig) ingest(metric interface{}){
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

func (info *SourceOffset) push(p *PrometheusConfig){
	//Push the latest source offset for the corresponding topic and partition
	p.sourceOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	info.updateLag(p)
}

func (info *SinkOffset) push(p *PrometheusConfig){
	//Push the latest sink offset for the corresponding topic and partition
	p.sinkOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	info.updateLag(p)
}

func (info *PartitionInfo) push(p *PrometheusConfig){
	//Push the time stamp at which the partition joined dmux
	p.partitionOwned.With(prometheus.Labels{"topic": info.Topic, "consumerId":info.ConsumerId, "partitionId":strconv.Itoa(int(info.PartitionId))}).SetToCurrentTime()
}

func (info *SourceOffset) updateLag(p *PrometheusConfig){
	//Update the previous offset details at source
	partitionDetail, ok := p.lastSourceDetail[info.Topic]
	if len(p.lastSourceDetail) <= p.maxTopics {
		p.lastSourceDetail[info.Topic] = updateMap(ok, (*OffsetInfo)(info), partitionDetail, p.maxPartitions)
	}

	//Push lag metric based on the stored sink offset
	if lastOffset, ok := p.lastSinkDetail[info.Topic][info.Partition]; ok {
		p.pushLag((*OffsetInfo)(info), lastOffset)
	} else {
		//If offset detail is not found thn check if the overall map or the partition map is full and
		//fetch details from collector(this is computationally expensive and slow and is used only if
		//maps are full)
		if len(p.lastSinkDetail) >= p.maxTopics || len(p.lastSinkDetail[info.Topic]) >= p.maxPartitions{
			p.pushLag((*OffsetInfo)(info), p.fromCollector(info))
		}
	}
}

func (info *SinkOffset) updateLag(p *PrometheusConfig) {
	//Update the previous offset details at sink
	partitionDetail, ok := p.lastSinkDetail[info.Topic]
	//Check if the overall map is full
	if len(p.lastSinkDetail) <= p.maxTopics {
		p.lastSinkDetail[info.Topic] = updateMap(ok, (*OffsetInfo)(info), partitionDetail, p.maxPartitions)
	}

	//Push lag metric based on the stored source offset
	if lastOffset, ok := p.lastSourceDetail[info.Topic][info.Partition]; ok {
		p.pushLag((*OffsetInfo)(info), lastOffset)
	} else {
		//If offset detail is not found thn check if the overall map or the partition map is full and
		//fetch details from collector(this is computationally expensive and slow and is used only if
		//maps are full)
		if len(p.lastSourceDetail) >= p.maxTopics || len(p.lastSourceDetail[info.Topic]) >= p.maxPartitions{
			p.pushLag((*OffsetInfo)(info), p.fromCollector(info))
		}
	}
}

func (p *PrometheusConfig) pushLag(info *OffsetInfo, offset int64) {
	//calculate lag
	lag := math.Abs(float64(offset - info.Offset))

	//Push the lag which is last source offset minus the sink offset for the corresponding topic and partition
	p.lagOffMetric.WithLabelValues(info.Topic, strconv.Itoa(int(info.Partition))).Set(lag)
}

func updateMap(ok bool, info *OffsetInfo, detail map[int32]int64, partitions int) map[int32]int64 {
	//Add partitions only if the partition map is not full
	if len(detail) < partitions {
		if ok {
			detail[info.Partition] = info.Offset
		} else{
			detail = make(map[int32]int64, partitions)
			detail[info.Partition] = info.Offset
		}
	} else {
		//The value corresponding to existing key can be updated
		if _, ok1 := detail[info.Partition]; ok1 {
			detail[info.Partition] = info.Offset
		}
	}
	return detail
}

func (p* PrometheusConfig) fromCollector(newInfo interface{}) int64 {
	switch newInfo.(type) {
	case SourceOffset:
		info := newInfo.(SourceOffset)
		if lastSinkOffset, err := p.sinkOffMetric.GetMetricWith(prometheus.Labels{"topic" : info.Topic, "partition":strconv.Itoa(int(info.Partition))});err==nil{
			var lastOffset = &dto.Metric{}
			lastSinkOffset.Write(lastOffset)
			lastOffsetValue := *lastOffset.Gauge.Value
			return int64(lastOffsetValue)
		}
	case SinkOffset:
		info := newInfo.(SinkOffset)
		if lastSourceOffset, err := p.sourceOffMetric.GetMetricWith(prometheus.Labels{"topic" : info.Topic, "partition":strconv.Itoa(int(info.Partition))});err==nil{
			var lastOffset = &dto.Metric{}
			lastSourceOffset.Write(lastOffset)
			lastOffsetValue := *lastOffset.Gauge.Value
			return int64(lastOffsetValue)
		}
	}
	return 0
}
