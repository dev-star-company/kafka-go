package connection

import (
	"github.com/segmentio/kafka-go"
)

func New(network string, address string) (*kafka.Conn, error) {
	// to connect to the kafka leader via an existing non-leader connection rather than using DialLeader
	conn, err := kafka.Dial(network, address)
	if err != nil {
		return nil, err
	}

	return conn, nil
	// defer conn.Close()
	// controller, err := conn.Controller()
	// if err != nil {
	// 	panic(err.Error())
	// }
	// var connLeader *kafka.Conn
	// connLeader, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	// if err != nil {
	// 	panic(err.Error())
	// }
	// defer connLeader.Close()
}
