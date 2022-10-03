package kafka

import (
	"context"
	"github.com/go-dmux/metrics"
	"github.com/go-dmux/utils"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-dmux/kafka/consumer-group"
	"github.com/go-dmux/kafka/kazoo-go"
)

//KafkaSourceHook to track messages coming out of the source in order
type KafkaSourceHook interface {
	//Pre called before passing the message to DMux
	Pre(k KafkaMsg)
}
type KafkaMsgFactory interface {
	//Create call to wrap consumer message inside KafkaMsg
	Create(msg *sarama.ConsumerMessage) KafkaMsg
}
type KafkaMsg interface {
	MarkDone()
	GetRawMsg() *sarama.ConsumerMessage
	IsProcessed() bool
}

//KafkaSource is Source implementation which reads from Kafka. This implementation
//uses sarama lib and wvanbergen implementation of HA Kafka Consumer using
//zookeeper
type KafkaSource struct {
	conf     KafkaConf
	consumer *consumergroup.ConsumerGroup
	hook     KafkaSourceHook
	factory  KafkaMsgFactory
}

//KafkaConf holds configuration options for KafkaSource
type KafkaConf struct {
	ConsumerGroupName string     `json:"name"`
	ZkPath            string     `json:"zk_path"`
	Topic             string     `json:"topic"`
	ForceRestart      bool       `json:"force_restart"`
	ReadNewest        bool       `json:"read_newest"`
	KafkaVersion      int        `json:"kafka_version_major"`
	SASLEnabled       bool       `json:"sasl_enabled"`
	SASLUsername      string     `json:"username"`
	SASLPasswordKey   string     `json:"passwordKey"`
	LagMonitor        LagMonitor `json:"lag_monitor"`
}

type LagMonitor struct {
	enabled         bool           `json:"enabled"`
	pollingInterval utils.Duration `json:"polling_interval"`
}

//GetKafkaSource method is used to get instance of KafkaSource.
func GetKafkaSource(conf KafkaConf, factory KafkaMsgFactory) *KafkaSource {
	return &KafkaSource{
		conf:    conf,
		factory: factory,
	}
}

//RegisterHook used to registerHook with KafkSource
func (k *KafkaSource) RegisterHook(hook KafkaSourceHook) {
	k.hook = hook
}

// //MarkDone is a behaviour added to KafkaMessage to update when it has been
// //processed by the Sink
// func (k *KafkaMessage) MarkDone() {
// 	k.Processed = true
// }

//Generate is Source method implementation, which connect to Kafka and pushes
//KafkaMessage into the channel
func (k *KafkaSource) Generate(out chan<- interface{}) {

	kconf := k.conf
	//config
	config := consumergroup.NewConfig()

	if kconf.KafkaVersion > 1 {
		config.Version = sarama.V2_0_1_0
	}
	config.Offsets.ResetOffsets = kconf.ForceRestart
	if kconf.ForceRestart && kconf.ReadNewest {
		config.Offsets.Initial = sarama.OffsetNewest
	}

	if kconf.SASLEnabled {
		//sarama config plain by default
		config.Net.SASL.User = kconf.SASLUsername
		config.Net.SASL.Password = os.Getenv(kconf.SASLPasswordKey)
		config.Net.SASL.Enable = true
	}

	config.Offsets.ProcessingTimeout = 10 * time.Second

	//parse zookeeper
	zookeeperNodes, chroot := kazoo.ParseConnectionString(kconf.ZkPath)
	config.Zookeeper.Chroot = chroot

	//get topics
	kafkaTopics := []string{kconf.Topic}

	var brokerList []string
	// create consumer
	consumer, err := consumergroup.JoinConsumerGroup(kconf.ConsumerGroupName, kafkaTopics, zookeeperNodes, config, &brokerList)
	if err != nil {
		panic(err)
	}

	k.consumer = consumer

	if k.conf.LagMonitor.enabled {
		//context for gracefully shutting down the offset reader goroutine
		ctx, cancelFunc := context.WithCancel(context.Background())
		defer cancelFunc()
		//if polling interval is invalid then set it to default value - 5 seconds
		if k.conf.LagMonitor.pollingInterval.Duration <= 0 {
			k.conf.LagMonitor.pollingInterval.Duration = 5 * time.Second
		}
		go readProducerConsumerOffset(brokerList, kconf.Topic, k.conf.ConsumerGroupName, consumer, ctx, k.conf.LagMonitor.pollingInterval.Duration)
	}

	for message := range k.consumer.Messages() {
		//TODO handle Create failurex
		kafkaMsg := k.factory.Create(message)

		if k.hook != nil {
			//TODO handle PreHook failure
			k.hook.Pre(kafkaMsg)
		}

		if kconf.LagMonitor.enabled {
			//ingest sourceOffset
			metricName := "source_offset" + "." + k.conf.ConsumerGroupName + "." + k.conf.Topic + "." + strconv.Itoa(int(kafkaMsg.GetRawMsg().Partition))

			metrics.Ingest(metrics.Metric{
				Type:  metrics.Offset,
				Name:  metricName,
				Value: kafkaMsg.GetRawMsg().Offset,
			})
		}

		out <- kafkaMsg
	}

}

//Ingest producer and consumer offset after a certain interval
func readProducerConsumerOffset(brokerList []string, topic string, connectionName string, consumer *consumergroup.ConsumerGroup, ctx context.Context, pollingInterval time.Duration) {

	if client, err := sarama.NewClient(brokerList, nil); err == nil {
		for {
			select {
			case <-time.After(pollingInterval):
				if partitions, err := client.Partitions(topic); err == nil {
					for partition := range partitions {
						metricName := connectionName + "." + topic + "." + strconv.Itoa(partition)
						pOff := int64(-1)
						cOff := int64(-1)

						//producerOff fetched from client
						if producerOff, err1 := client.GetOffset(topic, int32(partition), sarama.OffsetNewest); err1 == nil && producerOff > 0 {
							pOff = producerOff
							metrics.Ingest(metrics.Metric{
								Type:  metrics.Offset,
								Name:  "producer_offset" + "." + metricName,
								Value: producerOff - 1,
							})
						}

						//consumerOff feched from consumer
						if consumerOff, err1 := consumer.GetConsumerOffset(topic, int32(partition)); err1 == nil && consumerOff > 0 {
							cOff = consumerOff
							metrics.Ingest(metrics.Metric{
								Type:  metrics.Offset,
								Name:  "consumer_offset" + "." + metricName,
								Value: consumerOff - 1,
							})
						}

						if pOff >= 0 && cOff >= 0 && (pOff-cOff >= 0) {
							metrics.Ingest(metrics.Metric{
								Type:  metrics.Offset,
								Name:  "lag_producer_consumer" + "." + metricName,
								Value: pOff - cOff,
							})
						}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

//Stop method implements Source interface stop method, to Stop the KafkaConsumer
func (k *KafkaSource) Stop() {
	err := k.consumer.Close()
	if err != nil {
		panic(err)
	}
}

//CommitOffsets enables cliento explicity commit the Offset that is processed.
func (k *KafkaSource) CommitOffsets(data KafkaMsg) error {
	return k.consumer.CommitUpto(data.GetRawMsg(), k.conf.ConsumerGroupName)

}
