package connection

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type SyncUserStruct struct {
	Uuid      uuid.UUID         `json:"uuid"`
	Name      *string           `json:"name"`
	Surname   *string           `json:"surname"`
	CreatedAt *time.Time        `json:"created_at"`
	UpdatedAt *time.Time        `json:"updated_at"`
	DeletedAt *time.Time        `json:"deleted_at,omitempty"`
	CreatedBy *int              `json:"created_by"`
	UpdatedBy *int              `json:"updated_by"`
	DeletedBy *int              `json:"deleted_by,omitempty"`
}

func (p *Connectioner) PublishToSyncUsers(message Message[SyncUserStruct]) error {
	conn, err := p.Connect(topics.SyncUsers)
	if err != nil {
		return err
	}

	if message.Action != "create" && message.Action != "update" && message.Action != "delete" {
		return errors.New("invalid action: must be 'create', 'update' or 'delete'")
	}

	switch message.Action {
	case "create":
		if err := ValidateUserCreate(message.Payload); err != nil {
			return err
		}
	case "update":
		if err := ValidateUserUpdate(message.Payload); err != nil {
			return err
		}
	case "delete":
		if err := ValidateUserDelete(message.Payload); err != nil {
			return err
		}
	}

	userBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: userBytes},
	)
	if err != nil {
		return err
	}

	if err := conn.Close(); err != nil {
		return err
	}
	return nil
}
