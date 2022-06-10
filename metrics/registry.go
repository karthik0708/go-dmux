package metrics

type Registry struct {
	provider RegistryProvider
}

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
	return reg
}

//TrackMetrics reads from various channels and calls ingest method updating the metrics
func (reg *Registry) TrackMetrics() {

}

//Ingest calls the ingest method of the provider which is implementation by a metric registry type and forwards the metric
func (reg *Registry) Ingest(metric interface{}) {
}
