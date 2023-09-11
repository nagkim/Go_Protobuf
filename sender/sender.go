package main

import (
	"context"
	"log"
	"math/rand"
	"time"

	pb "example.com/api/api"

	"github.com/interconnectedcloud/go-amqp"
	"google.golang.org/protobuf/proto"
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

func generateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())

	// Set of characters to choose from
	chars := "abcdefghijklmnopqrstuvwxyz"

	// Generate the random string
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}

	return string(result)
}

func main() {
	masterBroker := "amqp://10.37.129.2:61616"
	slaveBroker := "amqp://10.37.129.3:61616"
	username := "admin"
	password := "admin"
	queueName := "test/java"
	var currentClient *amqp.Client
	var err error

	sizeOfArray := 1000

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

		// Open a session
		adminSession, err := currentClient.NewSession()
		if err != nil {
			log.Fatal("Creating sender AMQP session:", err)
		}

		// Manage signals
		ctx := context.Background()

		// Create a sender
		sender, err := adminSession.NewSender(
			amqp.LinkTargetAddress(queueName),
		)
		if err != nil {
			log.Fatal("Creating sender link:", err)
		}

		// Send messages
		for {
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)

			// Create a custom protobuf message
			protoObj := &pb.Foo{
				StringArray: make([]string, sizeOfArray),
				IntArray:    make([]int32, sizeOfArray),
				FloatArray:  make([]float32, sizeOfArray),
			}

			// Fill the int  with values from 1 to 10000
			for i := 0; i < sizeOfArray; i++ {
				protoObj.IntArray[i] = int32(i + 1)
			}

			// Fill the float  with values from 1 to 10000
			for i := 0; i < sizeOfArray; i++ {
				protoObj.FloatArray[i] = float32(i + 1)
			}

			// Fill the string  with values from 1 to 10000
			rand.Seed(time.Now().UnixNano())
			for i := 0; i < sizeOfArray; i++ {
				// Generate a random string of length 10
				randomString := generateRandomString(10)
				protoObj.StringArray[i] = randomString

			}

			// Start the timer
			startTime := time.Now()

			// Marshal the protobuf message
			protoData, err := proto.Marshal(protoObj)
			if err != nil {
				log.Println("Marshaling protobuf message:", err)
				currentClient.Close()
				currentClient = nil
				break
			}

			// Create a new message with protobuf data
			encodedData := amqp.NewMessage(protoData)

			// Send the message
			err = sender.Send(ctx, encodedData)
			if err != nil {
				log.Println("Sending message:", err)
				currentClient.Close()
				currentClient = nil
				break
			}
			elapsedTime := time.Since(startTime)
			log.Printf("marshaling took %.3f ms\n", float64(elapsedTime.Milliseconds()))

			log.Println("Message sent successfully")

			cancel()

			time.Sleep(6 * time.Second)
		}
	}

}
