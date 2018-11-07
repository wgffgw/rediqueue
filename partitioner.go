package rediqueue

type Partitioner interface {
	Partition(message *ProducerMessage, numPartitions int64) (int64, error)
}

type PartitionerConstructor func(topic string) Partitioner

type roundRobinPartitioner struct {
	partition int64
}

func NewRoundRobinPartitioner(topic string) Partitioner {
	return &roundRobinPartitioner{}
}

func (p *roundRobinPartitioner) Partition(message *ProducerMessage, numPartitions int64) (int64, error) {
	if p.partition >= numPartitions {
		p.partition = 0
	}
	ret := p.partition
	p.partition++
	return ret, nil
}
