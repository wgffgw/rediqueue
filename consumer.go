package rediqueue

import (
	"bytes"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type ConsumerMessage struct {
	Key       []byte
	Value     *bytes.Buffer
	Topic     string
	Partition int64
	Timestamp time.Time
}

func (c *Consumer) newConsumerMessage() *ConsumerMessage {
	if c.conf.Consumer.NewValueFunc != nil {
		return c.conf.Consumer.NewValueFunc()
	}
	return &ConsumerMessage{Value: bytes.NewBuffer(make([]byte, 0))}
}

type ConsumerError struct {
	Topic     string
	Partition int64
	Err       error
}

type Consumer struct {
	conf     *Config
	client   *redis.Client
	children map[string]map[int64]*partitionConsumer
	lock     sync.Mutex
	inFlight sync.WaitGroup
}

func NewConsumer(client *redis.Client, config *Config) *Consumer {
	c := &Consumer{
		conf:     config,
		client:   client,
		children: make(map[string]map[int64]*partitionConsumer),
	}
	return c
}

func (c *Consumer) Close() {
	for _, v := range c.children {
		for _, pc := range v {
			c.inFlight.Add(1)
			pc.close()
		}
	}
	c.inFlight.Wait()
}

func (c *Consumer) Partitions(topic string) ([]int64, error) {
	num, err := c.client.HGet(c.conf.TopicMapName, topic).Result()
	if err != nil {
		return nil, err
	}
	pnum, _ := strconv.Atoi(num)
	partitions := make([]int64, 0, pnum)
	for i := 0; i < pnum; i++ {
		partitions = append(partitions, int64(i))
	}
	return partitions, nil
}

func (c *Consumer) ConsumerPartition(topic string, partition int64) (*partitionConsumer, error) {
	child := &partitionConsumer{
		consumer:  c,
		conf:      c.conf,
		topic:     topic,
		partition: partition,
		messages:  make(chan *ConsumerMessage, 1),
		errors:    make(chan *ConsumerError, c.conf.ChannelBufferSize),
	}
	if err := c.addChild(child); err != nil {
		return &partitionConsumer{}, err
	}
	go withRecover(child.dispatcher)

	return child, nil
}

func (c *Consumer) addChild(child *partitionConsumer) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	topicChildren := c.children[child.topic]
	if topicChildren == nil {
		topicChildren = make(map[int64]*partitionConsumer)
		c.children[child.topic] = topicChildren
	}

	if topicChildren[child.partition] != nil {
		return errors.New("That topic/partition is already being consumed")
	}

	topicChildren[child.partition] = child
	return nil
}

func (c *Consumer) removeChild(child *partitionConsumer) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.children[child.topic], child.partition)
}

type partitionConsumer struct {
	consumer  *Consumer
	conf      *Config
	topic     string
	partition int64

	messages chan *ConsumerMessage
	errors   chan *ConsumerError
	shutdown bool
}

func (child *partitionConsumer) dispatcher() {
	for {
		if child.shutdown {
			break
		}
		msg, err := child.consumer.client.BRPop(30*time.Second, child.topic+strconv.Itoa(int(child.partition))).Result()
		if isNetworkError(err) || isNetworkTimeout(err) || len(msg) < 2 {
			continue
		}

		if err != nil {
			if child.conf.Consumer.Return.Errors && err != redis.Nil {
				child.errors <- &ConsumerError{Topic: child.topic, Partition: child.partition, Err: err}
			}
			continue
		}

		strs := strings.SplitN(msg[1], "_", 2)
		if len(strs) < 2 {
			continue
		}
		cmsg := child.consumer.newConsumerMessage()
		cmsg.Value.WriteString(strs[1])
		cmsg.Key = []byte(strs[0])
		cmsg.Topic = child.topic
		cmsg.Partition = child.partition
		child.messages <- cmsg
	}
	if child.shutdown {
		child.consumer.inFlight.Done()
	}

	child.consumer.removeChild(child)
}

func (child *partitionConsumer) close() {
	child.shutdown = true
}

func (child *partitionConsumer) Messages() <-chan *ConsumerMessage {
	return child.messages
}

func (child *partitionConsumer) Errors() <-chan *ConsumerError {
	return child.errors
}
