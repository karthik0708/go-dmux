package metrics

var Reg *Registry

const (
	defaultTopics int = 20
	defaultPartitions int = 200
	defaultMetricPort int = 9999
)

type Limits struct{
	MaxTopics  int			`json:"topics"`
	MaxPart    int			`json:"partitions_per_topic"`
}

type MetricConf struct {
	MetricPort  		int		`json:"metric_port"`
	OffsetTrackerLimits	Limits	`json:"offset_tracker_limits"`
}

type Registry struct {
	provider RegistryProvider

	SourceCh chan SourceOffset
	SinkCh chan SinkOffset
	PartitionCh chan PartitionInfo
}

type OffsetInfo struct {
	Topic string
	Partition int32
	Offset int64
}

type PartitionInfo struct {
	PartitionId int32
	ConsumerId string
	Topic string
}

type (
	SinkOffset OffsetInfo
	SourceOffset OffsetInfo
)

// RegistryProvider interface that implements metric registry types
type RegistryProvider interface {
	init()
	ingest(interface{})
}

//Start creates a registry and initializes the metrics based on the registry type and implementation and returns the created registry
func Start(metricConf MetricConf)  {
	partitions := metricConf.OffsetTrackerLimits.MaxPart
	topics := metricConf.OffsetTrackerLimits.MaxTopics
	port := metricConf.MetricPort

	if partitions <= 0 {
		partitions = defaultPartitions
	}

	if topics <= 0 {
		topics = defaultTopics
	}

	if port <= 0 {
		port = defaultMetricPort
	}

	config := &PrometheusConfig{metricPort: port, maxEntries: topics*partitions}

	Reg = &Registry{provider: config, SourceCh: make(chan SourceOffset), SinkCh: make(chan SinkOffset), PartitionCh: make(chan PartitionInfo)}
	Reg.provider.init()

	//Start tracking metrics
	go Reg.TrackMetrics()
}

//TrackMetrics reads from various channels and calls ingest method updating the metrics
func (reg *Registry) TrackMetrics(){
	for{
		select {
		case info := <- reg.SourceCh:
			reg.ingest(info)
		case info := <- reg.SinkCh:
			reg.ingest(info)
		case info := <- reg.PartitionCh:
			reg.ingest(info)
		}
	}
}

//Ingest calls the ingest method of the provider which is implementation by a metric registry type and forwards the metric
func (reg *Registry) ingest(metric interface{}){
	reg.provider.ingest(metric)
}