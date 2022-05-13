package metrics

type registry struct {
	provider Provider
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

func (reg *registry) TrackMetrics(partitionCh <- chan PartitionInfo){
	for{
		select {
		case info := <- partitionCh:
			reg.ingest(info)
		}
	}
}

func (reg *registry) ingest(metric interface{}){
	switch metric.(type) {
	case PartitionInfo:
		reg.provider.Ingest(metric)
	}
}
