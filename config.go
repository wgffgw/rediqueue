package rediqueue

import (
	"time"
)

type Config struct {
	// Producer is the namespace for configuration related to producing messages,
	// used by the Producer.
	Producer struct {
		// The level of acknowledgement reliability needed from the broker (defaults
		// to WaitForLocal). Equivalent to the `request.required.acks` setting of the
		// JVM producer.
		Timeout time.Duration
		// Generates partitioners for choosing the partition to send messages to
		// (defaults to hashing the message key). Similar to the `partitioner.class`
		// setting for the JVM producer.
		Partitioner PartitionerConstructor

		//Partition number
		PartitionSize int64

		// Return specifies what channels will be populated. If they are set to true,
		// you must read from the respective channels to prevent deadlock. If,
		// however, this config is used to create a `SyncProducer`, both must be set
		// to true and you shall not read from the channels since the producer does
		// this internally.
		Return struct {
			// If enabled, successfully delivered messages will be returned on the
			// Successes channel (default disabled).
			Successes bool

			// If enabled, messages that failed to deliver will be returned on the
			// Errors channel, including error (default enabled).
			Errors bool
		}
	}

	Consumer struct {
		//Message struct definition
		NewValueFunc func() *ConsumerMessage

		// Return specifies what channels will be populated. If they are set to true,
		// you must read from them to prevent deadlock.
		Return struct {
			// If enabled, any errors that occurred while consuming are returned on
			// the Errors channel (default disabled).
			Errors bool
		}
	}

	//TopicInfoMap saved in redis
	TopicMapName string
	// The number of events to buffer in internal and external channels. This
	// permits the producer and consumer to continue processing some messages
	// in the background while user code is working, greatly improving throughput.
	// Defaults to 256.
	ChannelBufferSize int
}

// NewConfig returns a new configuration instance with sane defaults.
func NewConfig() *Config {
	c := &Config{}
	c.TopicMapName = "rediqueueTopicInfos"
	c.Producer.Timeout = 10 * time.Second
	c.Producer.Partitioner = NewRoundRobinPartitioner
	c.Producer.Retry.Max = 3
	c.Producer.PartitionSize = 7
	c.Producer.Retry.Backoff = 100 * time.Millisecond
	c.Producer.Return.Errors = true

	c.Consumer.Retry.Backoff = 2 * time.Second
	c.Consumer.Return.Errors = false

	c.ChannelBufferSize = 256

	return c
}
