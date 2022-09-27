package kafka

import (
	"context"
	"github.com/go-dmux/metrics"
	"github.com/prometheus/client_golang/prometheus"
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
	conf            KafkaConf
	consumer        *consumergroup.ConsumerGroup
	hook            KafkaSourceHook
	factory         KafkaMsgFactory
	pollingInterval time.Duration
	connectionName  string
}

//KafkaConf holds configuration options for KafkaSource
type KafkaConf struct {
	ConsumerGroupName string `json:"name"`
	ZkPath            string `json:"zk_path"`
	Topic             string `json:"topic"`
	ForceRestart      bool   `json:"force_restart"`
	ReadNewest        bool   `json:"read_newest"`
	KafkaVersion      int    `json:"kafka_version_major"`
	SASLEnabled       bool   `json:"sasl_enabled"`
	SASLUsername      string `json:"username"`
	SASLPasswordKey   string `json:"passwordKey"`
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
	consumer, err := consumergroup.JoinConsumerGroup(kconf.ConsumerGroupName, kafkaTopics, zookeeperNodes, config, k.connectionName, &brokerList)
	if err != nil {
		panic(err)
	}

	k.consumer = consumer

	//context for gracefully shutting down the offset reader goroutine
	ctx, cancelFunc := context.WithCancel(context.Background())
	go readOffset(brokerList, kconf.Topic, k.connectionName, consumer, ctx, k.pollingInterval)

	for message := range k.consumer.Messages() {
		//TODO handle Create failurex
		kafkaMsg := k.factory.Create(message)

		if k.hook != nil {
			//TODO handle PreHook failure
			k.hook.Pre(kafkaMsg)
		}
		out <- kafkaMsg
	}

	cancelFunc()
}

//Ingest producer and consumer offset after a certain interval
func readOffset(brokerList []string, topic string, connectionName string, consumer *consumergroup.ConsumerGroup, ctx context.Context, pollingInterval time.Duration) {
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
							metrics.Reg.Ingest(metrics.Metric{
								MetricType:  prometheus.GaugeValue,
								MetricName:  "producer_offset" + "." + metricName,
								MetricValue: producerOff - 1,
							})
						}

						//consumerOff feched from consumer
						if consumerOff, err1 := consumer.GetConsumerOffset(topic, int32(partition)); err1 == nil && consumerOff > 0 {
							cOff = consumerOff
							metrics.Reg.Ingest(metrics.Metric{
								MetricType:  prometheus.GaugeValue,
								MetricName:  "consumer_offset" + "." + metricName,
								MetricValue: consumerOff - 1,
							})
						}

						if pOff >= 0 && cOff >= 0 && (pOff-cOff >= 0) {
							metrics.Reg.Ingest(metrics.Metric{
								MetricType:  prometheus.GaugeValue,
								MetricName:  "lag" + "." + metricName,
								MetricValue: pOff - cOff,
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
	return k.consumer.CommitUpto(data.GetRawMsg())
}

func (k *KafkaSource) SetPollinginterval(interval time.Duration) {
	if interval <= 0 {
		interval = 5
	}
	k.pollingInterval = interval
}

func (k *KafkaSource) SetConnectionName(name string) {
	k.connectionName = name
}
