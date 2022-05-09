package metrics

type registry struct {
	provider Provider
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

func Init() registry{
	pm := PrometheusMetrics{}

	reg := registry{provider: &pm}
	reg.provider.Init()
	return reg
}

func (reg *registry) TrackMetrics(sourceCh <- chan SourceOffset, sinkCh <- chan SinkOffset, partitionCh <- chan PartitionInfo){
	for{
		select {
		case info := <- sourceCh:
			reg.ingest(info)
		case info := <- sinkCh:
			reg.ingest(info)
		case info := <- partitionCh:
			reg.ingest(info)
		}
	}
}

func (reg *registry) ingest(metric interface{}){
	reg.provider.Ingest(metric)
}
