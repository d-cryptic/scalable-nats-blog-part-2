package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nats_conn_string := "nats://jetstream:jetstream_password@localhost:4222, nats://jetstream:jetstream_password@localhost:4223, nats://jetstream:jetstream_password@localhost:4224, nats://jetstream:jetstream_password@localhost:4225"

	// Connect to nats cluster
	nc, err := nats.Connect(nats_conn_string)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Drain()
	fmt.Println("connected")

	js, err := nc.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	// Create a stream for main messages
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.*"},
		Replicas: 3,
	})

	if err != nil {
		log.Printf("Stream already exists or error: %v", err)
	}

	// Create DLQ stream for failed messages
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "DLQ",
		Subjects: []string{"dlq.orders"},
		Replicas: 1,
	})
	if err != nil {
		log.Printf("DLQ Stream already exists or error: %v", err)
	}

	// Advisory stream (important!)
	js.AddStream(&nats.StreamConfig{
		Name:      "ADVISORY",
		Subjects:  []string{"$JS.EVENT.ADVISORY.>"},
		Storage:   nats.FileStorage,
		Retention: nats.InterestPolicy,
		Replicas:  1,
	})

	// Publish messages
	for i := 1; i <= 10; i++ {
		msg := fmt.Sprintf("order_%d", i)
		_, err := js.Publish("orders.new", []byte(msg))
		if err != nil {
			log.Printf("Publish error: %v", err)
		} else {
			fmt.Printf("Published: %s\n", msg)
		}
	}

	// Create a consumer with max delivery attempts
	_, err = js.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:       "order-processor",
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
		MaxDeliver:    3,
	})
	if err != nil {
		log.Printf("Consumer already exists or error: %v", err)
	}

	// Subscribe to consumer and simulate failure for DLQ
	_, err = js.PullSubscribe("orders.*", "order-processor")
	if err != nil {
		log.Fatal(err)
	}

	// Simulate consumer handling with forced failure
	sub, err := js.PullSubscribe("orders.*", "order-processor")
	if err != nil {
		log.Fatal(err)
	}

	for {
		msgs, err := sub.Fetch(5, nats.MaxWait(2*time.Second))
		if err != nil {
			log.Println("No more messages or fetch timeout")
			break
		}

		for _, msg := range msgs {
			fmt.Printf("Received: %s\n", string(msg.Data))
			//Simulate processing failure
			if string(msg.Data) == "order_5" {
				fmt.Println("Simulated processing failure, not acking message:", string(msg.Data))
				continue
			}
			msg.Ack()
			fmt.Println("Acked:", string(msg.Data))
		}
	}

	//DLQ handling: Subscribe to Advisories and move message to DLQ Stream
	_, err = js.Subscribe("$JS.EVENT.ADVISORY.CONSUMER.*.*.*", func(m *nats.Msg) {
		metadata, err := m.Metadata()
		if err != nil {
			log.Println("Metadata error:", err)
			return
		}
		stream := metadata.Stream
		seq := metadata.Sequence.Stream

		// Fetch original message
		orgMsg, err := js.GetMsg(stream, seq)
		if err != nil {
			log.Println("Failed to get original message", err)
			return
		}

		// Publish to DLQ stream
		_, err = js.Publish("dlq.orders", orgMsg.Data)
		if err != nil {
			log.Println("Failed to publish to DLQ:", err)
		} else {
			fmt.Println("Message moved to DLQ:", string(orgMsg.Data))
		}
	})

	if err != nil {
		fmt.Println("here")
		log.Fatal(err)
	}

	// Keep application running
	select {}
}
