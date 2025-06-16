package connection

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

type SyncUserStruct struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Surname   string `json:"surname"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
	CreatedBy int64  `json:"created_by"`
	UpdatedBy int64  `json:"updated_by"`
}

func (p Connectioner) PublishToSyncUsers(users []SyncUserStruct) error {
	conn, ok := p.connections[topics.SyncUsers]
	if !ok {
		return errors.New("connection not found for topic SyncUsers")
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	for _, user := range users {
		userBytes, err := json.Marshal(user)
		if err != nil {
			log.Fatal("failed to marshal user:", err)
		}
		_, err = conn.WriteMessages(
			kafka.Message{Value: userBytes},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}

	return nil
}
