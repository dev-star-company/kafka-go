package connection

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dev-star-company/custom-validate/validate"
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
		Topic:    string(topics.SyncEmails),
		MaxBytes: 1e6, // 1MB
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

				// Validation logic similar to pub_users.go
				var fields map[string]string
				switch email.Action {
				case "create":
					fields = map[string]string{
						"Id":        "required,numeric,min=1",
						"UserId":    "required,numeric,min=1",
						"Email":     "required,min=3",
						"CreatedAt": "required,datetime",
						"UpdatedAt": "required,datetime",
						"CreatedBy": "required,numeric,min=1",
						"UpdatedBy": "required,numeric,min=1",
					}
				case "update":
					fields = map[string]string{
						"Id":        "required,numeric,min=1",
						"UserId":    "optional,numeric,min=1",
						"Email":     "optional,min=3",
						"UpdatedAt": "required,datetime",
						"UpdatedBy": "required,numeric,min=1",
						"DeletedAt": "optional,datetime",
						"DeletedBy": "optional,numeric,min=1",
					}
				case "delete":
					fields = map[string]string{
						"Id":         "required,numeric,min=1",
						"DetectedAt": "required,datetime",
						"DetectedBy": "required,numeric,min=1",
					}
				default:
					continue // skip invalid action
				}

				if err := validate.Validate(fields, email.Payload); err != nil {
					continue // skip invalid messages
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
