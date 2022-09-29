package metrics

var Reg *Registry

const (
	defaultMetricPort int = 9999
	COUNTER           int = 0
	GAUGE             int = 1
	SUMMARY           int = 2
	UNTYPED           int = 3
	HISTOGRAM         int = 4
)

//generic metric structure
type Metric struct {
	MetricType  int
	MetricName  string
	MetricValue int64
}

type Registry struct {
	provider RegistryProvider
}

// RegistryProvider interface that implements metric registry types
type RegistryProvider interface {
	init()
	ingest(metric Metric)
}

//Start creates a registry and initializes the metrics based on the registry type and implementation and returns the created registry
func Start(metricPort int) {

	if metricPort <= 0 {
		metricPort = defaultMetricPort
	}

	config := &PrometheusConfig{metricPort: metricPort}

	Reg = &Registry{
		provider: config,
	}
	Reg.provider.init()

}

//Ingest calls the ingest method of the provider which is implementation by a metric registry type and forwards the metric
func (reg *Registry) Ingest(metric Metric) {
	reg.provider.ingest(metric)
}
