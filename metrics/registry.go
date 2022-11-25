package metrics

type MetricType int64

const (
	defaultMetricPort int        = 9999
	Offset            MetricType = iota
)

//generic metric structure
type Metric struct {
	Type  MetricType
	Name  string
	Value int64
}

type Registry interface {
	start(conf interface{})
	ingest(metric Metric)
}

var registry PrometheusRegistry

//Start creates a registry and initializes the metrics based on the registry type and implementation and returns the created registry
func Start(metricPort int) {

	if metricPort <= 0 {
		metricPort = defaultMetricPort
	}

	config := PrometheusConfig{metricPort: metricPort}

	registry = PrometheusRegistry{}
	registry.start(config)

}

//Ingest calls the ingest method of the provider which is implementation by a metric registry type and forwards the metric
func Ingest(metric Metric) {
	registry.ingest(metric)
}
