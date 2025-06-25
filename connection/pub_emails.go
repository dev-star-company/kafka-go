package connection

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type SyncEmailStruct struct {
	Uuid      uuid.UUID  `json:"uuid"`
	UserUuid  uuid.UUID  `json:"user_uuid"`
	Email     *string    `json:"email"`
	CreatedAt *time.Time `json:"created_at"`
	UpdatedAt *time.Time `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
	CreatedBy *int       `json:"created_by"`
	UpdatedBy *int       `json:"updated_by"`
	DeletedBy *int       `json:"deleted_by,omitempty"`
	Main      *bool      `json:"main"`
}

func (p *Connectioner) PublishToSyncEmails(message Message[SyncEmailStruct]) error {
	conn, err := p.Connect(topics.SyncEmails)
	if err != nil {
		return err
	}

	if message.Action != "create" && message.Action != "update" && message.Action != "delete" {
		return errors.New("invalid action: must be 'create', 'update' or 'delete'")
	}

	switch message.Action {
	case "create":
		if err := ValidateEmailCreate(message.Payload); err != nil {
			return err
		}
	case "update":
		if err := ValidateEmailUpdate(message.Payload); err != nil {
			return err
		}
	case "delete":
		if err := ValidateEmailDelete(message.Payload); err != nil {
			return err
		}
	}

	emailBytes, err := json.Marshal(message)

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if err != nil {
		return err
	}
	_, err = conn.WriteMessages(
		kafka.Message{Value: emailBytes},
	)
	if err != nil {
		return err
	}

	if err := conn.Close(); err != nil {
		return err
	}
	return nil
}
