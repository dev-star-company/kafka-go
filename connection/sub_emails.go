package connection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

func (c *Connectioner) SubscribeToEmails(ctx context.Context) (<-chan SubResponse[SyncEmailStruct], error) {
	ch := make(chan SubResponse[SyncEmailStruct])
	conn, err := c.ConnectToTopic(topics.SyncEmails)
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
		Topic:    string(topics.SyncEmails),
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

			var email Message[SyncEmailStruct]
			if err := json.Unmarshal(msg.Value, &email); err != nil {
				continue // skip invalid messages
			}
			if email.Publisher == c.consumerGroupID {
				continue // skip messages from the same publisher
			}
			select {
			case ch <- SubResponse[SyncEmailStruct]{Message: email, CommitFn: func() error { return r.CommitMessages(ctx, msg) }}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
