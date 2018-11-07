# rediqueue
## 针对redis实现多队列的生产和消费
### Consumer
(```)
var messagePool = sync.Pool{
        New: func() interface{} {
                return &ConsumerMessage{}
        },
}
(```)

(```)
func TestConsumer(t *testing.T) {
        //go pprofutil.Pprof()
        config := NewConfig()
        config.Consumer.NewValueFunc = func() *ConsumerMessage {
                buf := bufferPool.Get().(*bytes.Buffer)
                msg := messagePool.Get().(*ConsumerMessage)
                msg.Value = buf
                return msg
        }
        client := redisdao.GetClient("cache")
        consumer := NewConsumer(client, config)
        partitions, _ := consumer.Partitions("xes_redis_")
        fmt.Println(partitions)
        for _, partition := range partitions {
                go func(partition int64) {
                        p, _ := consumer.ConsumerPartition("xes_redis_", partition)
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
(```)
