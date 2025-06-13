package publish

type SyncUserStruct struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Surname   string `json:"surname"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
	CreatedBy int64  `json:"created_by"`
	UpdatedBy int64  `json:"updated_by"`
}

func (p Publisher) PublishToSyncUsers(users []SyncUserStruct) error {
	// This function would contain the logic to publish the users to a Kafka topic.
	// For example, using a Kafka producer to send the users data to the "sync.users" topic.
	// The actual implementation would depend on the Kafka library being used.
	return nil
}
