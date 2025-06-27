package connection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

func (c *Connectioner) SubscribeToPasswords(ctx context.Context) (<-chan SubResponse[SyncPasswordStruct], error) {
	ch := make(chan SubResponse[SyncPasswordStruct])
	conn, err := c.ConnectToTopic(topics.SyncPasswords)
	if err != nil {
		return nil, err
	}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	ctrl, err := conn.Controller()
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s:%d", ctrl.Host, ctrl.Port)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{url},
		GroupID:  c.consumerGroupID,
		Topic:    string(topics.SyncPasswords),
		MaxBytes: 1e6, // 1MB
	})

	go func() {
		defer close(ch)
		defer r.Close()

		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				// Exit on context cancellation or reader close
				return
			}

			var password Message[SyncPasswordStruct]
			if err := json.Unmarshal(msg.Value, &password); err != nil {
				continue // skip invalid messages
			}
			if password.Publisher == c.consumerGroupID {
				continue // skip messages from the same publisher
			}
			select {
			case ch <- SubResponse[SyncPasswordStruct]{Message: password, CommitFn: func() error { return r.CommitMessages(ctx, msg) }}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
