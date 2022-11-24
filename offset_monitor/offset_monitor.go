package offset_monitor

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/go-dmux/core"
	consumergroup "github.com/go-dmux/kafka/consumer-group"
	"github.com/go-dmux/metrics"
	"strconv"
	"time"
)

type OffMonitor struct {
	SourceSinkMonitorEnabled       bool          `json:"source_sink_monitor_enabled"`
	ProducerConsumerMonitorEnabled bool          `json:"producer_consumer_monitor_enabled"`
	OffPollingInterval             core.Duration `json:"offset_polling_interval"`
}

type handler interface {
	StartProducerConsumerMonitor(brokerList []string, topic string, cgName string, consumer *consumergroup.ConsumerGroup,
		ctx context.Context)
	IngestSrcSkMetric(prefixName string, msg *sarama.ConsumerMessage)
}

func (monitor *OffMonitor) StartProducerConsumerMonitor(brokerList []string, topic string, cgName string,
	consumer *consumergroup.ConsumerGroup, ctx context.Context) {
	//if polling interval is invalid then set it to default value - 5 seconds
	if monitor.OffPollingInterval.Duration <= 0 {
		monitor.OffPollingInterval.Duration = 5 * time.Second
	}

	if monitor.ProducerConsumerMonitorEnabled {
		go monitor.MonitorProducerConsumerOffset(brokerList, topic, cgName, consumer, ctx)
	}
}

func (monitor *OffMonitor) IngestSrcSkMetric(prefixName string, msg *sarama.ConsumerMessage) {
	if monitor.SourceSinkMonitorEnabled {
		ingestMetric(prefixName+"."+msg.Topic+"."+strconv.Itoa(int(msg.Partition)), msg.Offset)
	}
}

//Ingest producer and consumer offset after a certain interval
func (monitor *OffMonitor) MonitorProducerConsumerOffset(brokerList []string, topic string, connectionName string,
	consumer *consumergroup.ConsumerGroup, ctx context.Context) {

	if client, err := sarama.NewClient(brokerList, nil); err == nil {
		for {
			select {
			case <-time.After(monitor.OffPollingInterval.Duration):
				if partitions, err := client.Partitions(topic); err == nil {
					for partition := range partitions {
						suffixName := connectionName + "." + topic + "." + strconv.Itoa(partition)
						pOff := int64(-1)
						cOff := int64(-1)

						//producerOff fetched from client
						if producerOff, errInCollection := client.GetOffset(topic, int32(partition), sarama.OffsetNewest); errInCollection == nil && producerOff > 0 {
							pOff = producerOff
							ingestMetric("producer_offset"+"."+suffixName, producerOff-1)
						}

						//consumerOff feched from consumer
						if consumerOff, errInCollection := consumer.GetConsumerOffset(topic, int32(partition)); errInCollection == nil && consumerOff > 0 {
							cOff = consumerOff
							ingestMetric("consumer_offset"+"."+suffixName, consumerOff-1)
						}

						if pOff >= 0 && cOff >= 0 && (pOff-cOff >= 0) {
							ingestMetric("lag_producer_consumer"+"."+suffixName, pOff-cOff)
						}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

func ingestMetric(name string, value int64) {
	metrics.Ingest(metrics.Metric{
		Type:  metrics.Offset,
		Name:  name,
		Value: value,
	})
}
