package connection

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Connectioner struct {
	connections map[string]*kafka.Conn
}

func New() *Connectioner {
	return &Connectioner{
		connections: make(map[string]*kafka.Conn),
	}
}

// Use topics from topics package
func (c *Connectioner) ConnectToTopic(topic string, url string) (*kafka.Conn, error) {
	if c.connections[topic] != nil {
		return c.connections[topic], nil
	}

	conn, err := kafka.DialLeader(context.Background(), "tcp", url, topic, 0)
	if err != nil {
		return nil, err
	}

	c.connections[topic] = conn
	return conn, nil
}
