package metrics

type registry struct {
	provider Provider
}

//Provider interface that implements metric registry types
type Provider interface {
	Init()
	Ingest(interface{})
}

var Registry registry

//Init creates a registry and initializes the metrics based on the registry type and implementation and returns the created registry
func Init() registry{
	pm := PrometheusMetrics{}

	reg := registry{provider: &pm}
	reg.provider.Init()
	return reg
}

//TrackMetrics reads from various channels and calls ingest method updating the metrics
func (reg *registry) TrackMetrics(){

}

//Ingest calls the ingest method of the provider which is implementation by a metric registry type and forwards the metric
func (reg *registry) ingest(metric interface{}){
}
