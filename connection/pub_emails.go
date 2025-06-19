package connection

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

type SyncEmailStruct struct {
	ID        uint32  `json:"id"`
	Email     *string `json:"email"`
	CreatedAt *string `json:"created_at"`
	UpdatedAt *string `json:"updated_at"`
	DeletedAt *string `json:"deleted_at,omitempty"`
	CreatedBy *uint32 `json:"created_by"`
	UpdatedBy *uint32 `json:"updated_by"`
	DeletedBy *uint32 `json:"deleted_by,omitempty"`
	Main      *bool   `json:"main"`
}

func (p Connectioner) PublishToSyncEmails(message Message[SyncEmailStruct]) error {
	conn, ok := p.connections[topics.SyncEmails]
	if !ok || conn == nil {
		return errors.New("failed to connect to topic SyncEmails")
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
