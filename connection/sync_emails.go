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
	Email     string  `json:"email"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
	DeletedAt *string `json:"deleted_at,omitempty"`
	CreatedBy uint32  `json:"created_by"`
	UpdatedBy uint32  `json:"updated_by"`
	DeletedBy *uint32 `json:"deleted_by,omitempty"`
	Main      bool    `json:"main"`
}

func (p Connectioner) PublishToSyncEmails(emails []SyncEmailStruct) error {
	conn, ok := p.connections[topics.SyncEmails]
	if !ok {
		return errors.New("connection not found for topic SyncEmails")
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	for _, email := range emails {
		emailBytes, err := json.Marshal(email)
		if err != nil {
			log.Fatal("failed to marshal email:", err)
		}
		_, err = conn.WriteMessages(
			kafka.Message{Value: emailBytes},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}

	return nil
}
