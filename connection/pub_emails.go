package connection

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/dev-star-company/custom-validate/validate"
	"github.com/dev-star-company/kafka-go/topics"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type SyncEmailStruct struct {
	Uuid      uuid.UUID `json:"uuid"`
	UserUuid  uuid.UUID `json:"user_uuid"`
	Email     *string   `json:"email"`
	CreatedAt *string   `json:"created_at"`
	UpdatedAt *string   `json:"updated_at"`
	DeletedAt *string   `json:"deleted_at,omitempty"`
	CreatedBy *int      `json:"created_by"`
	UpdatedBy *int      `json:"updated_by"`
	DeletedBy *int      `json:"deleted_by,omitempty"`
	Main      *bool     `json:"main"`
}

func (p Connectioner) PublishToSyncEmails(message Message[SyncEmailStruct]) error {
	conn, ok := p.connections[topics.SyncEmails]
	if !ok || conn == nil {
		return errors.New("failed to connect to topic SyncEmails")
	}

	if message.Action != "create" && message.Action != "update" && message.Action != "delete" {
		return errors.New("invalid action: must be 'create', 'update' or 'delete'")
	}

	switch message.Action {
	case "create":
		fields := map[string]string{
			"Uuid":      "required,uuid",
			"UserUuid":  "required,uuid",
			"Main":      "optional,boolean",
			"Email":     "required,min=3",
			"CreatedAt": "required,datetime", // "datetime" is commonly used for time.Time validation
			"UpdatedAt": "required,datetime",
			"CreatedBy": "required,numeric,min=1",
			"UpdatedBy": "required,numeric,min=1",
		}
		if err := validate.Validate(fields, message.Payload); err != nil {
			return err
		}
	case "update":
		fields := map[string]string{
			"Uuid":      "required,uuid",
			"UserUuid":  "optional,uuid",
			"Email":     "optional,min=3",
			"UpdatedAt": "required,datetime",
			"UpdatedBy": "required,numeric,min=1",
			"DeletedAt": "optional,datetime",
			"DeletedBy": "optional,numeric,min=1",
			"Main":      "optional,boolean",
		}
		if err := validate.Validate(fields, message.Payload); err != nil {
			return err
		}
	case "delete":
		fields := map[string]string{
			"Uuid":       "required,uuid",
			"DetectedAt": "required,datetime",
			"DetectedBy": "required,numeric,min=1",
		}
		if err := validate.Validate(fields, message.Payload); err != nil {
			return err
		}
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	emailBytes, err := json.Marshal(message)
	if err != nil {
		log.Fatal("failed to marshal email:", err)
	}
	_, err = conn.WriteMessages(
		kafka.Message{Value: emailBytes},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	return nil
}
