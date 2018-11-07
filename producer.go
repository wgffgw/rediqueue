package rediqueue

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

type Producer struct {
	conf                      *Config
	client                    *redis.Client
	errors                    chan *ProducerError
	input, successes, retries chan *ProducerMessage
	inFlight                  sync.WaitGroup
	children                  map[string]map[int64]*partitionProducer
	lock                      sync.Mutex
}

func NewProducer(client *redis.Client, config *Config) *Producer {
	p := &Producer{
		conf:      config,
		client:    client,
		errors:    make(chan *ProducerError),
		input:     make(chan *ProducerMessage),
		successes: make(chan *ProducerMessage),
		retries:   make(chan *ProducerMessage),
		children:  make(map[string]map[int64]*partitionProducer),
	}
	go withRecover(p.dispatcher)
	return p
}

type ProducerMessage struct {
	Topic     string
	Key       string
	Value     []byte
	Metadata  interface{}
	Partition int64
	Timestamp time.Time
}

func (p *Producer) getValue() *bytes.Buffer {
	return bufferPool.Get().(*bytes.Buffer)
}

func (p *Producer) releaseValue(buf *bytes.Buffer) {
	buf.Reset()
	bufferPool.Put(buf)
}

func (p *Producer) Input() chan<- *ProducerMessage {
	return p.input
}

func (p *Producer) Close() error {
	go withRecover(p.down)
	var errors ProducerErrors

	if p.conf.Producer.Return.Errors {
		for event := range p.errors {
			errors = append(errors, event)
		}
	}
	if len(errors) > 0 {
		return errors
	}
	return nil
}

func (p *Producer) down() {
	close(p.input)
	for _, v := range p.children {
		for _, pp := range v {
			p.inFlight.Add(1)
			pp.shutdown = true
		}
	}
	p.inFlight.Wait()
	close(p.errors)
	close(p.successes)
}

type ProducerError struct {
	Msg *ProducerMessage
	Err error
}

func (pe ProducerError) Error() string {
	return fmt.Sprintf("Failed to produce message to topic %s: %s", pe.Msg.Topic, pe.Err)
}

type ProducerErrors []*ProducerError

func (pe ProducerErrors) Error() string {
	return fmt.Sprintf("Failed to delivery %d messages.", len(pe))
}

func (p *Producer) dispatcher() {
	handlers := make(map[string]chan<- *ProducerMessage)
	for msg := range p.input {
		if msg == nil {
			continue
		}
		handler := handlers[msg.Topic]
		if handler == nil {
			handler = p.newTopicProducer(msg.Topic)
			handlers[msg.Topic] = handler
		}
		handler <- msg
	}
	for _, handler := range handlers {
		close(handler)
	}
}

type topicProducer struct {
	parent      *Producer
	topic       string
	input       chan *ProducerMessage
	handlers    map[int64]chan<- *ProducerMessage
	partitioner Partitioner
}

func (p *Producer) newTopicProducer(topic string) chan<- *ProducerMessage {
	input := make(chan *ProducerMessage, p.conf.ChannelBufferSize)
	tp := &topicProducer{
		parent:      p,
		topic:       topic,
		input:       input,
		handlers:    make(map[int64]chan<- *ProducerMessage),
		partitioner: p.conf.Producer.Partitioner(topic),
	}
	go withRecover(tp.dispatcher)
	return input
}

func (tp *topicProducer) dispatcher() {
	if err := tp.saveMeta(tp.topic); err != nil {
		if tp.parent.conf.Producer.Return.Errors {
			tp.parent.errors <- &ProducerError{Msg: &ProducerMessage{}, Err: err}
		}
		return
	}
	for msg := range tp.input {
		if err := tp.partitionMessage(msg); err != nil {
			if tp.parent.conf.Producer.Return.Errors {
				tp.parent.errors <- &ProducerError{Msg: msg, Err: err}
			}
			continue
		}
		handler := tp.handlers[msg.Partition]
		if handler == nil {
			handler = tp.newPartitionProducer(msg.Topic, msg.Partition)
			tp.handlers[msg.Partition] = handler
		}
		handler <- msg
	}
	for _, handler := range tp.handlers {
		close(handler)
	}
}

func (tp *topicProducer) saveMeta(topic string) error {
	_, er := tp.parent.client.HSet(tp.parent.conf.TopicMapName, topic, tp.parent.conf.Producer.PartitionSize).Result()
	if er != nil {
		return er
	}
	return nil
}
func (tp *topicProducer) partitionMessage(msg *ProducerMessage) error {
	choice, err := tp.partitioner.Partition(msg, tp.parent.conf.Producer.PartitionSize)
	if err != nil {
		return err
	}
	msg.Partition = choice
	return nil
}

type partitionProducer struct {
	parent    *topicProducer
	topic     string
	partition int64
	input     chan *ProducerMessage
	shutdown  bool
}

func (tp *topicProducer) newPartitionProducer(topic string, partition int64) chan *ProducerMessage {
	input := make(chan *ProducerMessage, tp.parent.conf.ChannelBufferSize)
	pp := &partitionProducer{
		parent:    tp,
		topic:     topic,
		partition: partition,
		input:     input,
	}
	tp.parent.addChild(pp)
	go withRecover(pp.dispatcher)
	return input
}

func (p *Producer) addChild(pp *partitionProducer) {
	p.lock.Lock()
	defer p.lock.Unlock()

	topicChildren := p.children[pp.topic]
	if topicChildren == nil {
		topicChildren = make(map[int64]*partitionProducer)
		p.children[pp.topic] = topicChildren
	}

	if topicChildren[pp.partition] != nil {
		return
	}

	topicChildren[pp.partition] = pp
}

func (p *Producer) removeChild(pp *partitionProducer) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.children[pp.topic], pp.partition)
}

func (pp *partitionProducer) dispatcher() {
	for msg := range pp.input {
		value := pp.parent.parent.getValue()
		value.WriteString(msg.Key)
		value.WriteByte('_')
		value.Write(msg.Value)
		//redis
		count := 0
	Loop:
		_, err := pp.parent.parent.client.LPush(msg.Topic+strconv.Itoa(int(msg.Partition)), value.Bytes()).Result()
		if isNetworkError(err) {
			if count > 2 {
				break
			}
			count++
			goto Loop
		}
		if err != nil {
			if pp.parent.parent.conf.Producer.Return.Errors {
				pp.parent.parent.errors <- &ProducerError{Msg: msg, Err: err}
			}
			continue
		} else {
			if pp.parent.parent.conf.Producer.Return.Successes {
				pp.parent.parent.successes <- msg
			}
		}
		pp.parent.parent.releaseValue(value)
	}
	if pp.shutdown {
		pp.parent.parent.inFlight.Done()
	}
	pp.parent.parent.removeChild(pp)
}
