package metrics

import (
	"flag"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"log"
	"net/http"
	"strconv"
)

type PrometheusConfig struct {
	//metricPort to which the metrics would be sent
	metricPort int

	//(MaxTopics )* (max partitions per topic)
	maxEntries int
}

//metric collectors
type metricCollector struct {
	sourceOffMetric *prometheus.GaugeVec
	sinkOffMetric *prometheus.GaugeVec
	lagOffMetric *prometheus.GaugeVec
	partitionOwned *prometheus.GaugeVec
	producerOffMetric *prometheus.GaugeVec
}

var (
	collectors *metricCollector

	//keep track of last source and sink details for lag calculation
	lastSourceDetail map[string]int64
	lastSinkDetail map[string]int64
)

func (p *PrometheusConfig) init(){
	//The metrics can be fetched by a Get request from the http://localhost:9999/metrics end point
	go func(config *PrometheusConfig) {
		addr := flag.String("listen-address", ":"+strconv.Itoa(config.metricPort), "The address to listen on for HTTP requests.")
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(*addr, nil))
	}(p)

	lastSinkDetail = make(map[string]int64, p.maxEntries)
	lastSourceDetail = make(map[string]int64, p.maxEntries)

	collectors = createMetrics()
	collectors.registerMetrics()
}

//Initialize all the metric collectors
func  createMetrics() *metricCollector{
	sourceOffMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "source_offset",
			Help: "Metric to represent the offset at source",
		}, []string{"connection", "topic","partition"})

	sinkOffMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sink_offset",
			Help: "Metric to represent the offset at sink",
		}, []string{"connection","topic","partition"})

	lagOffMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "lag",
			Help: "Metric to represent the lag between source and sink",
		}, []string{"connection","topic","partition"})

	partitionOwned := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "partition_owned",
			Help: "Metric to represent the partitions owned",
		}, []string{"connection", "topic","consumerId", "partitionId"})

	producerOffMetric := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "producer_offset",
			Help: "Metric to represent the offset that that was last processed at producer side",
		}, []string{"connection","topic","partition"})

	c := &metricCollector{
		sourceOffMetric: sourceOffMetric,
		sinkOffMetric:   sinkOffMetric,
		lagOffMetric:    lagOffMetric,
		partitionOwned:  partitionOwned,
		producerOffMetric: producerOffMetric,
	}

	return c
}

//Register the metric collectors
func (c *metricCollector) registerMetrics() {
	prometheus.MustRegister(c.sourceOffMetric)
	prometheus.MustRegister(c.sinkOffMetric)
	prometheus.MustRegister(c.lagOffMetric)
	prometheus.MustRegister(c.partitionOwned)
	prometheus.MustRegister(c.producerOffMetric)
}

//Ingest metrics as and when events are received from the channels
func (p *PrometheusConfig) ingest(metric interface{}){
	switch metric.(type) {
	case SourceOffset:
		m := metric.(SourceOffset)
		m.push(p.maxEntries)
	case SinkOffset:
		m := metric.(SinkOffset)
		m.push(p.maxEntries)
	case PartitionInfo:
		m := metric.(PartitionInfo)
		m.push()
	case ProducerOffset:
		m := metric.(ProducerOffset)
		m.push()
	}
}

func (info *SourceOffset) push(maxEntries int){
	//Push the latest source offset for the corresponding topic and partition
	collectors.sourceOffMetric.WithLabelValues(info.ConnectionName, info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	info.updateLag(maxEntries)
}

func (info *SinkOffset) push(maxEntries int){
	//Push the latest sink offset for the corresponding topic and partition
	collectors.sinkOffMetric.WithLabelValues(info.ConnectionName, info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
	info.updateLag(maxEntries)
}

func (info *ProducerOffset) push(){
	//Push the last processed producer offset for the corresponding topic and partition
	collectors.producerOffMetric.WithLabelValues(info.ConnectionName, info.Topic, strconv.Itoa(int(info.Partition))).Set(float64(info.Offset))
}

func (info *PartitionInfo) push(){
	//Push the time stamp at which the partition joined dmux
	collectors.partitionOwned.WithLabelValues(info.ConnectionName, info.Topic, info.ConsumerId, strconv.Itoa(int(info.PartitionId))).SetToCurrentTime()
}

func (info *SourceOffset) updateLag(maxEntries int){
	//Update the previous offset details at source
	//Check if the overall map is full
	topicPartition := info.ConnectionName + info.Topic + "_" + strconv.Itoa(int(info.Partition))
	if _, ok := lastSourceDetail[topicPartition]; ok || len(lastSourceDetail) < maxEntries {
		lastSourceDetail[topicPartition] = info.Offset
	}

	//Push lag metric based on the stored sink offset
	if lastOffset, ok := lastSinkDetail[topicPartition]; ok {
		pushLag(*info, lastOffset)
	} else if len(lastSinkDetail) >= maxEntries {
		//If offset detail is not found then check if the overall map is full and
		//fetch details from collector(this is computationally expensive and slow and is used only if
		//maps are full)
		pushLag(*info, fromCollector(*info))
	} else{
		//if the maps are not full then that implies that it is the first message from that partition processed by dmux
		pushLag(*info, 0)
	}
}

func (info *SinkOffset) updateLag(maxEntries int) {
	//Update the previous offset details at sink
	//Check if the overall map is full
	topicPartition := info.ConnectionName + info.Topic + "_" + strconv.Itoa(int(info.Partition))
	if _, ok := lastSinkDetail[topicPartition]; ok || len(lastSinkDetail) < maxEntries {
		lastSinkDetail[topicPartition] = info.Offset
	}

	//Push lag metric based on the stored source offset
	if lastOffset, ok := lastSourceDetail[topicPartition]; ok {
		pushLag(*info, lastOffset)
	} else {
		//If offset detail is not found thn check if the overall map fetch details
		//from collector(this is computationally expensive and slow and is used only if //maps are full)
		if len(lastSourceDetail) >= maxEntries {
			pushLag(*info, fromCollector(*info))
		}
	}
}

func  pushLag(info interface{}, lastOffset int64) {
	//if lastOffset is -1 then that means it was not fetched from collector in the right way
	if lastOffset == -1 {
		return
	}

	//calculate lag based on type of info
	switch info.(type) {
	case SourceOffset:
		srcOff := info.(SourceOffset)

		//Push the lag which is last source offset minus the sink offset for the corresponding topic and partition
		collectors.lagOffMetric.WithLabelValues(srcOff.ConnectionName, srcOff.Topic, strconv.Itoa(int(srcOff.Partition))).Set(float64(srcOff.Offset - lastOffset))
	case SinkOffset:
		skOff := info.(SinkOffset)

		//Push the lag which is last source offset minus the sink offset for the corresponding topic and partition only if lag is positive
		if lag := lastOffset - skOff.Offset; lag >= 0 {
			collectors.lagOffMetric.WithLabelValues(skOff.ConnectionName, skOff.Topic, strconv.Itoa(int(skOff.Partition))).Set(float64(lastOffset - skOff.Offset))
		}
	}
}

func fromCollector(newInfo interface{}) int64 {
	switch newInfo.(type) {
	case SourceOffset:
		info := newInfo.(SourceOffset)
		if lastSinkOffset, err := collectors.sinkOffMetric.GetMetricWith(prometheus.Labels{"connection":info.ConnectionName, "topic" : info.Topic, "partition":strconv.Itoa(int(info.Partition))});err==nil{
			var lastOffset = &dto.Metric{}
			lastSinkOffset.Write(lastOffset)
			lastOffsetValue := *lastOffset.Gauge.Value
			return int64(lastOffsetValue)
		}
	case SinkOffset:
		info := newInfo.(SinkOffset)
		if lastSourceOffset, err := collectors.sourceOffMetric.GetMetricWith(prometheus.Labels{"connection":info.ConnectionName, "topic" : info.Topic, "partition":strconv.Itoa(int(info.Partition))});err==nil{
			var lastOffset = &dto.Metric{}
			lastSourceOffset.Write(lastOffset)
			lastOffsetValue := *lastOffset.Gauge.Value
			return int64(lastOffsetValue)
		}
	}
	return -1
}
