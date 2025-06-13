package subscription

import "github.com/segmentio/kafka-go"

type Subscriptioner struct {
	Kafka *kafka.Conn
}
