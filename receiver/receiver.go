package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "example.com/api/api"

	"github.com/golang/protobuf/proto"
	"github.com/interconnectedcloud/go-amqp"
)

func connectToBroker(address string, username string, password string) (*amqp.Client, error) {
	client, err := amqp.Dial(address,
		amqp.ConnSASLPlain(username, password),
	)
	if err != nil {
		log.Printf("Failed to connect to %s: %s\n", address, err)
		return nil, err
	}
	return client, nil
}

func main() {
	masterBroker := "amqp://10.37.129.2:61616"
	slaveBroker := "amqp://10.37.129.3:61616"
	username := "admin"
	password := "admin"
	queueName := "test/java"

	var currentClient *amqp.Client
	var err error

	for {
		if currentClient == nil {
			currentClient, err = connectToBroker(masterBroker, username, password)
			if err != nil {
				// Connect to the slave broker
				currentClient, err = connectToBroker(slaveBroker, username, password)
			}
		}

		if err != nil {
			// Retry
			time.Sleep(time.Second)
			continue
		}

		session, err := currentClient.NewSession()
		if err != nil {
			log.Fatal("Creating AMQP session:", err)
		}

		receiver, err := session.NewReceiver(
			amqp.LinkSourceAddress(queueName),
		)
		if err != nil {
			log.Fatal("Creating receiver link:", err)
		}

		ctx := context.Background()

		for {
			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Println("Receiving message:", err)
				time.Sleep(time.Second)
				continue
			}

			data := msg.GetData()

			// Print the size of the data
			fmt.Printf("Received data size: %d bytes\n", len(data))

			// Start the timer
			startTime := time.Now()

			// Unmarshal protobuf message
			protoObj := &pb.Foo{}
			err = proto.Unmarshal(data, protoObj)
			if err != nil {
				log.Println("Unmarshaling protobuf message:", err)
				msg.Accept()
				continue
			}
			elapsedTime := time.Since(startTime)
			log.Printf("Unmarshaling took %.3f ms\n", float64(elapsedTime.Milliseconds()))
			// Process the received protobuf message
			//log.Printf("Received message: %+v\n", protoObj)

			// Acknowledge the message
			msg.Accept()
		}
	}
}
