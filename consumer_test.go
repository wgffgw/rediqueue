package rediqueue

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis"
)

var messagePool = sync.Pool{
	New: func() interface{} {
		return &ConsumerMessage{}
	},
}

func TestConsumer(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:        ser,
		Password:    option.Password, // no password set
		DB:          option.DB,       // use default DB
		PoolSize:    option.PoolSize,
		IdleTimeout: option.IdleTimeout,
	})
	config := NewConfig()
	config.Consumer.NewValueFunc = func() *ConsumerMessage {
		buf := bufferPool.Get().(*bytes.Buffer)
		msg := messagePool.Get().(*ConsumerMessage)
		msg.Value = buf
		return msg
	}
	consumer := NewConsumer(client, config)
	partitions, _ := consumer.Partitions("rediqueue")
	for _, partition := range partitions {
		go func(partition int64) {
			p, _ := consumer.ConsumerPartition("rediqueue", partition)
			for {
				msg := <-p.Messages()
				msg.Value.Reset()
				bufferPool.Put(msg.Value)
				msg.Key = msg.Key[:0]
				msg.Topic = ""
				msg.Partition = -1
				messagePool.Put(msg)
			}
		}(partition)
	}
	time.Sleep(1000 * time.Second)
}
