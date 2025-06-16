package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dev-star-company/kafka-go/connection"
	"github.com/dev-star-company/kafka-go/topics"
)

func main() {
	// This is the main entry point for the application.
	// The actual implementation will depend on how you want to use the connection package.
	// You can create a new Connectioner instance and use it to connect to topics and publish messages.
	// For example:
	//
	// conn := connection.New("my-consumer-group")
	// err := conn.ConnectToTopic("my-topic", "localhost:9092")
	// if err != nil {
	//     log.Fatal(err)
	// }
	//
	// // Publish messages, etc.

	for i := range 3 {
		go func() {
			c := connection.New("teste" + fmt.Sprint(i))
			_, err := c.ConnectToTopic(topics.SyncUsers, "localhost:9092")
			if err != nil {
				panic(err)
			}
			incomingUsers, err := c.SubscribeToUsers(context.Background())
			if err != nil {
				panic(err)
			}

			for user := range incomingUsers {
				fmt.Printf("On consumer %d\n", i)
				fmt.Println(user)
			}
		}()
	}

	c := connection.New("teste5")
	_, err := c.ConnectToTopic(topics.SyncUsers, "localhost:9092")
	if err != nil {
		panic(err)
	}

	for i := range 50 {
		err = c.PublishToSyncUsers(connection.Message[connection.SyncUserStruct]{
			Payload: connection.SyncUserStruct{
				ID:        int64(i),
				Name:      "John",
				Surname:   "Doe",
				CreatedAt: "2023-01-01T00:00:00Z",
				UpdatedAt: "2023-01-01T00:00:00Z",
				CreatedBy: 1,
				UpdatedBy: 1,
			},
			Action: "create",
		})
		if err != nil {
			panic(err)
		}
		// Add a 1 second delay between publishes
		time.Sleep(1 * time.Second)
	}

	fmt.Println("Finished publishing messages")
}
