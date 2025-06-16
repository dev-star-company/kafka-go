package connection

import (
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/dev-star-company/kafka-go/topics"
	"github.com/segmentio/kafka-go"
)

type SyncPhoneStruct struct {
	ID        uint32  `json:"id"`
	Phone     string  `json:"phone"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
	DeletedAt *string `json:"deleted_at,omitempty"`
	CreatedBy uint32  `json:"created_by"`
	UpdatedBy uint32  `json:"updated_by"`
	DeletedBy *uint32 `json:"deleted_by,omitempty"`
	Main      bool    `json:"main"`
}

func (p Connectioner) PublishToSyncPhones(phones []SyncPhoneStruct) error {
	conn, ok := p.connections[topics.SyncPhones]
	if !ok {
		return errors.New("connection not found for topic SyncPhones")
	}

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	for _, phone := range phones {
		phoneBytes, err := json.Marshal(phone)
		if err != nil {
			log.Fatal("failed to marshal phone:", err)
		}
		_, err = conn.WriteMessages(
			kafka.Message{Value: phoneBytes},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
	}

	return nil
}
