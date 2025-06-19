package connection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

func (c Connectioner) SubscribeToUsers(ctx context.Context) (<-chan Message[SyncUserStruct], error) {
	ch := make(chan Message[SyncUserStruct])
	conn, ok := c.connections[topics.SyncUsers]
	if !ok {
		return nil, fmt.Errorf("connection not found for topic %s", topics.SyncUsers)
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
		Topic:    string(topics.SyncUsers),
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
			var user Message[SyncUserStruct]
			if err := json.Unmarshal(msg.Value, &user); err != nil {
				continue // skip invalid messages
			}
			select {
			case ch <- user:
				r.CommitMessages(ctx, msg) // Commit the message after successful processing
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
