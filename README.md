# rediqueue
## 针对redis实现多队列的生产和消费,弹性内存管理,实现简单,存储可靠
### Producer
```
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
```

### Consumer
```
var messagePool = sync.Pool{
        New: func() interface{} {
                return &ConsumerMessage{}
        },
}
```

```
func TestConsumer(t *testing.T) {
        config := NewConfig()
        config.Consumer.NewValueFunc = func() *ConsumerMessage {
                buf := bufferPool.Get().(*bytes.Buffer)
                msg := messagePool.Get().(*ConsumerMessage)
                msg.Value = buf
                return msg
        }
        client := redis.NewClient(&redis.Options{
                Addr:        ser,
                Password:    option.Password, // no password set
                DB:          option.DB,       // use default DB
                PoolSize:    option.PoolSize,
                IdleTimeout: option.IdleTimeout,
        })
        consumer := NewConsumer(client, config)
        partitions, _ := consumer.Partitions("rediqueue")
        for _, partition := range partitions {
                go func(partition int64) {
                        p, _ := consumer.ConsumerPartition("redisqueue", partition)
                        for {
                                msg := <-p.Messages()
                                fmt.Println(msg.Value.String())
                                msg.Value.Reset()
                                bufferPool.Put(msg.Value)
                                msg.Key = msg.Key[:0]
                                msg.Topic = ""
                                msg.Partition = -1
                                messagePool.Put(msg)
                        }
                }(partition)
        }
```
