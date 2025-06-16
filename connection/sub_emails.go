package connection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

func (c Connectioner) SubscribeToEmails(ctx context.Context) (<-chan Message[SyncEmailStruct], error) {
	ch := make(chan Message[SyncEmailStruct])
	conn, ok := c.connections[topics.SyncEmails]
	if !ok {
		return nil, fmt.Errorf("connection not found for topic %s", topics.SyncEmails)
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
		Topic:    topics.SyncEmails,
		MaxBytes: 10e6, // 10MB
	})

	go func() {
		defer close(ch)
		defer r.Close()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := r.ReadMessage(ctx)
				if err != nil {
					// Optionally log error here
					return
				}

				var email Message[SyncEmailStruct]
				if err := json.Unmarshal(msg.Value, &email); err != nil {
					// Optionally log error here
					continue
				}

				select {
				case ch <- email:
					r.CommitMessages(ctx, msg) // Commit the message after successful processing
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}
