package connection

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type SyncPhoneStruct struct {
	Uuid      uuid.UUID  `json:"uuid"`
	UserUuid  uuid.UUID  `json:"user_uuid"`
	Phone     *string    `json:"phone"`
	CreatedAt *time.Time `json:"created_at"`
	UpdatedAt *time.Time `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
	CreatedBy *int       `json:"created_by"`
	UpdatedBy *int       `json:"updated_by"`
	DeletedBy *int       `json:"deleted_by,omitempty"`
	Main      *bool      `json:"main"`
}

func (p *Connectioner) PublishToSyncPhones(message Message[SyncPhoneStruct]) error {
	conn, err := p.Connect(topics.SyncPhones)
	if err != nil {
		return err
	}

	if message.Action != "create" && message.Action != "update" && message.Action != "delete" {
		return errors.New("invalid action: must be 'create', 'update' or 'delete'")
	}

	switch message.Action {
	case "create":
		if err := ValidatePhoneCreate(message.Payload); err != nil {
			return err
		}
	case "update":
		if err := ValidatePhoneUpdate(message.Payload); err != nil {
			return err
		}
	case "delete":
		if err := ValidatePhoneDelete(message.Payload); err != nil {
			return err
		}
	}

	phoneBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: phoneBytes},
	)
	if err != nil {
		return err
	}

	if err := conn.Close(); err != nil {
		return err
	}
	return nil
}
