package connection

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/dev-star-company/custom-validate/validate"
	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

type SyncPhoneStruct struct {
	ID        int        `json:"id"`
	Phone     *string    `json:"phone"`
	CreatedAt *time.Time `json:"created_at"`
	UpdatedAt *time.Time `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
	CreatedBy *int       `json:"created_by"`
	UpdatedBy *int       `json:"updated_by"`
	DeletedBy *int       `json:"deleted_by,omitempty"`
	Main      *bool      `json:"main"`
}

func (p Connectioner) PublishToSyncPhones(message Message[SyncPhoneStruct]) error {
	conn, ok := p.connections[topics.SyncPhones]
	if !ok {
		return errors.New("connection not found for topic SyncPhones")
	}

	if message.Action != "create" && message.Action != "update" && message.Action != "delete" {
		return errors.New("invalid action: must be 'create', 'update', or 'delete'")
	}

	switch message.Action {
	case "create":
		fields := map[string]string{
			"Id":        "required,numeric,min=1",
			"Phone":     "required,min=3",
			"CreatedAt": "required,datetime",
			"UpdatedAt": "required,datetime",
			"CreatedBy": "required,numeric,min=1",
			"UpdatedBy": "required,numeric,min=1",
		}
		if err := validate.Validate(fields, message.Payload); err != nil {
			return err
		}
	case "update":
		fields := map[string]string{
			"Id":        "required,numeric,min=1",
			"Phone":     "optional,min=3",
			"UpdatedAt": "required,datetime",
			"UpdatedBy": "required,numeric,min=1",
			"DeletedAt": "optional,datetime",
			"DeletedBy": "optional,numeric,min=1",
		}
		if err := validate.Validate(fields, message.Payload); err != nil {
			return err
		}
	case "delete":
		fields := map[string]string{
			"Id":         "required,numeric,min=1",
			"DetectedAt": "required,datetime",
			"DetectedBy": "required,numeric,min=1",
		}
		if err := validate.Validate(fields, message.Payload); err != nil {
			return err
		}
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	phoneBytes, err := json.Marshal(message)
	if err != nil {
		log.Fatal("failed to marshal phone:", err)
	}
	_, err = conn.WriteMessages(
		kafka.Message{Value: phoneBytes},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	return nil
}
