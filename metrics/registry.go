package metrics

var (
	Reg *Registry
	MetricPort int
	MaxTopics int
	MaxPartitions int
)

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
func Start(metricPort int) *Registry {
	config := &PrometheusConfig{metricPort: metricPort}

	reg := &Registry{provider: config}
	reg.provider.init()
	reg.TrackMetrics()
	return reg
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