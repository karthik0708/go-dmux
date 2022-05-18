package metrics

type registry struct {
	provider Provider
	SourceCh chan SourceOffset
	SinkCh chan SinkOffset
	PartitionCh chan PartitionInfo
}

type (
	SinkOffset OffsetInfo
	SourceOffset OffsetInfo
)

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

type Provider interface {
	Init()
	Ingest(interface{})
}

var Registry registry

func Init() registry{
	pm := PrometheusMetrics{}

	reg := registry{provider: &pm, SourceCh: make(chan SourceOffset, 1), SinkCh: make(chan SinkOffset, 1), PartitionCh: make(chan PartitionInfo)}
	reg.provider.Init()
	return reg
}

func (reg *registry) TrackMetrics(){
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

func (reg *registry) ingest(metric interface{}){
	reg.provider.Ingest(metric)
}
