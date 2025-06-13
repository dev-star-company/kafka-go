package publish

import "github.com/segmentio/kafka-go"

type Publisher struct {
	Kafka *kafka.Conn
}
