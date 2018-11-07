# rediqueue
## 针对redis实现多队列的生产和消费
### Producer
```
func TestProducer(t *testing.T) {
        config := NewConfig()
        config.Producer.PartitionSize = 7
        client := redisdao.GetClient("cache")
        producer := NewProducer(client, config)
        for {
                msg := &ProducerMessage{Topic: "rediqueue", Key: "aaabbb", Value: []byte("11111222223333344444")}
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
        client := redisdao.GetClient("cache")
        consumer := NewConsumer(client, config)
        partitions, _ := consumer.Partitions("rediqueue")
        fmt.Println(partitions)
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
