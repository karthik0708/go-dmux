package kafka

import (
	"github.com/go-dmux/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"log"
	"math"
	"strconv"
	"time"
)

//OffsetTracker is interface which defines methods to track Messages which
//have been queued for processing
type OffsetTracker interface {
	TrackMe(kmsg KafkaMsg)
}

//KafkaOffsetTracker is implementation of OffsetTracker to track offsets for
//KafkaSource, KafkaMessage
type KafkaOffsetTracker struct {
	ch             chan KafkaMsg
	source         *KafkaSource
	size           int
	connectionName string
}

//TrackMe method ensures messages to track are enqued for tracking
func (k *KafkaOffsetTracker) TrackMe(kmsg KafkaMsg) {
	if len(k.ch) == k.size {
		log.Printf("warning: pending_acks threshold %d reached, please increase pending_acks size", k.size)
	}

	msg := kmsg.GetRawMsg()
	metricName := "source_offset" + "." + k.connectionName + "." + msg.Topic + "." + strconv.Itoa(int(msg.Partition))

	metrics.Reg.Ingest(metrics.Metric{
		prometheus.GaugeValue,
		metricName,
		msg.Offset,
	})

	k.ch <- kmsg
}

//GetKafkaOffsetTracker is Global function to get instance of KafkaOffsetTracker
func GetKafkaOffsetTracker(size int, source *KafkaSource, connectionName string) OffsetTracker {
	k := &KafkaOffsetTracker{
		ch:             make(chan KafkaMsg, size),
		source:         source,
		size:           size,
		connectionName: connectionName,
	}
	go k.run()
	return k
}

func (k *KafkaOffsetTracker) run() {
	for kmsg := range k.ch {
		for !kmsg.IsProcessed() {
			//log.Printf("waiting for url %s to process, queue_len %d", kmsg.GetURLPath(), len(k.ch))
			time.Sleep(100 * time.Microsecond)
		}
		msg := kmsg.GetRawMsg()

		//take highest offset at sink
		finalSkOff := msg.Offset
		if skOff, ok := k.source.SinkOffsets.Load(msg.Topic + "." + strconv.Itoa(int(msg.Partition))); ok {
			finalSkOff = int64(math.Max(float64(finalSkOff), float64(skOff.(int64))))
		}

		k.source.SinkOffsets.Store(msg.Topic+"."+strconv.Itoa(int(msg.Partition)), finalSkOff)

		metricName := "sink_offset" + "." + k.connectionName + "." + msg.Topic + "." + strconv.Itoa(int(msg.Partition))
		metrics.Reg.Ingest(metrics.Metric{
			prometheus.GaugeValue,
			metricName,
			finalSkOff,
		})

	}
}
