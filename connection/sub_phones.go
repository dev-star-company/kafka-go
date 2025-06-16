package connection

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

func (p Connectioner) SubscribeToPhones(ctx context.Context) (<-chan Message[SyncPhoneStruct], error) {
	ch := make(chan Message[SyncPhoneStruct])
	conn, ok := p.connections[topics.SyncPhones]
	if !ok {
		return nil, errors.New("connection not found for topic SyncPhones")
	}

	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	ctrl, err := conn.Controller()
	if err != nil {
		return nil, err
	}

	url := ctrl.Host + ":" + strconv.Itoa(ctrl.Port)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{url},
		GroupID:  p.consumerGroupID,
		Topic:    topics.SyncPhones,
		MaxBytes: 10e6, // 10MB
	})

	// Handle graceful shutdown via context
	go func() {
		defer close(ch)
		defer r.Close()
		for {
			msg, err := r.ReadMessage(ctx)
			if err != nil {
				// Exit on context cancellation or reader close
				return
			}
			var phone Message[SyncPhoneStruct]
			if err := json.Unmarshal(msg.Value, &phone); err != nil {
				continue // skip invalid messages
			}
			select {
			case ch <- phone:
				r.CommitMessages(ctx, msg) // Commit the message after successful processing
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
