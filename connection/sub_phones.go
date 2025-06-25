package connection

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

func (p Connectioner) SubscribeToPhones(ctx context.Context) (<-chan SubResponse[SyncPhoneStruct], error) {
	ch := make(chan SubResponse[SyncPhoneStruct])
	conn, err := p.ConnectToTopic(topics.SyncPhones)
	if err != nil {
		return nil, err
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
		Topic:    string(topics.SyncPhones),
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
			var phone Message[SyncPhoneStruct]
			if err := json.Unmarshal(msg.Value, &phone); err != nil {
				continue // skip invalid messages
			}
			if phone.Publisher == p.consumerGroupID {
				continue // skip messages from the same publisher
			}
			select {
			case ch <- SubResponse[SyncPhoneStruct]{Message: phone, CommitFn: func() error { return r.CommitMessages(ctx, msg) }}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}
