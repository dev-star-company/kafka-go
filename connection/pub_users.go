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

type SyncUserStruct struct {
	ID        int64   `json:"id"`
	Name      *string `json:"name"`
	Surname   *string `json:"surname"`
	CreatedAt *string `json:"created_at"`
	UpdatedAt *string `json:"updated_at"`
	CreatedBy *int64  `json:"created_by"`
	UpdatedBy *int64  `json:"updated_by"`
	DeletedAt *string `json:"deleted_at,omitempty"`
	DeletedBy *int64  `json:"deleted_by,omitempty"`
}

func (p Connectioner) PublishToSyncUsers(message Message[SyncUserStruct]) error {
	conn, ok := p.connections[topics.SyncUsers]
	if !ok {
		return errors.New("connection not found for topic SyncUsers")
	}

	if message.Action != "create" && message.Action != "update" && message.Action != "delete" {
		return errors.New("invalid action: must be 'create', 'update', or 'delete'")
	}

	switch message.Action {
	case "create":
		fields := map[string]string{
			"Id":        "required,numeric,min=1",
			"Name":      "required,min=3",
			"Surname":   "required,min=3",
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
			"Name":      "optional,min=3",
			"Surname":   "optional,min=3",
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
	userBytes, err := json.Marshal(message)
	if err != nil {
		log.Fatal("failed to marshal user:", err)
	}
	_, err = conn.WriteMessages(
		kafka.Message{Value: userBytes},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	return nil
}
