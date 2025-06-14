package connection

import (
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
	conn, err := p.connections[topics.SyncUsers]
	if err != nil {
		return err
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: []byte("one!")},
		kafka.Message{Value: []byte("two!")},
		kafka.Message{Value: []byte("three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	return nil
}
