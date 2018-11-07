package rediqueue

import (
	"testing"

	"git.xesv5.com/golib/gotools/utils/pprofutil"
	"git.xesv5.com/golib/redisdao"
)

func TestProducer(t *testing.T) {
	go pprofutil.Pprof()
	config := NewConfig()
	config.Producer.PartitionSize = 7
	client := redisdao.GetClient("cache")
	producer := NewProducer(client, config)
	for {
		msg := &ProducerMessage{Topic: "xes_redis_", Key: "aaabbb", Value: []byte("11111222223333344444")}
		producer.Input() <- msg

	}
}
