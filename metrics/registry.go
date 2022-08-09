package metrics

var Reg *Registry

const (
	defaultMetricPort int = 9999
)


type MetricConf struct {
	MetricPort  		int		`json:"metric_port"`
}

type Registry struct {
	provider RegistryProvider
}

// RegistryProvider interface that implements metric registry types
type RegistryProvider interface {
	init()
	ingest(interface{})
}

//Start creates a registry and initializes the metrics based on the registry type and implementation and returns the created registry
func Start(metricConf MetricConf)  {
	port := metricConf.MetricPort

	if port <= 0 {
		port = defaultMetricPort
	}

	config := &PrometheusConfig{metricPort: port}

	Reg = &Registry{
		provider: config,
	}
	Reg.provider.init()

}

//Ingest calls the ingest method of the provider which is implementation by a metric registry type and forwards the metric
func (reg *Registry) Ingest(metric interface{}){
	reg.provider.ingest(metric)
}