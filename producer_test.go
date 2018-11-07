package rediqueue

import (
	"testing"

	"github.com/go-redis/redis"
)

func TestProducer(t *testing.T) {
	config := NewConfig()
	config.Producer.PartitionSize = 7
	client := redis.NewClient(&redis.Options{
		Addr:        ser,
		Password:    option.Password, // no password set
		DB:          option.DB,       // use default DB
		PoolSize:    option.PoolSize,
		IdleTimeout: option.IdleTimeout,
	})
	producer := NewProducer(client, config)
	for {
		msg := &ProducerMessage{Topic: "xes_redis_", Key: "aaabbb", Value: []byte("11111222223333344444")}
		producer.Input() <- msg

	}
}
