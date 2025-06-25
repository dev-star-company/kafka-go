package connection

import (
	"context"

	"github.com/dev-star-company/kafka-go/actions"
	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

type Message[T SyncEmailStruct | SyncPhoneStruct | SyncUserStruct] struct {
	Action    actions.Action `json:"action"`    // "create", "update", or "delete"
	Payload   T              `json:"object"`    // any of the SyncSomethingStruct types
	Publisher string         `json:"publisher"` // the name of the publisher
}

type Connectioner struct {
	connections     map[topics.Topic]*kafka.Conn
	consumerGroupID string
	brokerUrl       string
}

type SubResponse[T SyncEmailStruct | SyncPhoneStruct | SyncUserStruct] struct {
	Message  Message[T]   `json:"message"`   // The message received from the topic
	CommitFn func() error `json:"commit_fn"` // The commit function to call after processing the message
}

// ConsumerGroupId is the ID of the consumer group that will be used for subscribing to topics.
// It is used to ensure that multiple consumers can read from the same topic without duplicating messages.
// It is important to set this ID when creating a new Connectioner instance.
// Should be set to a unique value for each consumer group.
func New(brokerUrl string, consumerGroupID string) *Connectioner {
	return &Connectioner{
		connections:     make(map[topics.Topic]*kafka.Conn),
		consumerGroupID: consumerGroupID,
		brokerUrl:       brokerUrl,
	}
}

// Use topics from topics package
func (c *Connectioner) ConnectToTopic(topic topics.Topic) (*kafka.Conn, error) {
	if c.connections[topic] != nil {
		return c.connections[topic], nil
	}

	conn, err := c.Connect(topic)
	if err != nil {
		return nil, err
	}

	c.connections[topic] = conn
	return conn, nil
}

func (c *Connectioner) Connect(topic topics.Topic) (*kafka.Conn, error) {
	conn, err := kafka.DialLeader(context.Background(), "tcp", c.brokerUrl, string(topic), 0)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
