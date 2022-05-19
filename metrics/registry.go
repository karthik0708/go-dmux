package metrics

type registry struct {
	provider Provider
	PartitionCh chan PartitionInfo
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

	reg := registry{provider: &pm, PartitionCh: make(chan PartitionInfo)}
	reg.provider.Init()
	return reg
}

func (reg *registry) TrackMetrics(){
	for{
		select {
		case info := <- reg.PartitionCh:
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
