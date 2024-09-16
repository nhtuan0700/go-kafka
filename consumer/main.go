package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	topic := "coffee_order"
	msgCount := 0

	// 1. Create new consumer and start it
	consumerWorker, err := ConnectToConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}

	consumer, err := consumerWorker.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started!")

	// 2. Handle os signals - used to stop the process
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// 3. Create a Goroutine to run the consumer / worker
	doneCh := make(chan bool)
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				time.Sleep(3 * time.Second)
				msgCount++
				fmt.Printf("Received order Count: %d | Topic (%s) | Message (%s) \n", msgCount, msg.Topic, string(msg.Value))
				order := (msg.Value)
				fmt.Printf("Brewing coffee for order: %s\n", order)
			case <-sigchan:
				fmt.Println("Interrupt detect")
				doneCh <- true
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "message")

	// 4. Close the consumer on exit
	if err := consumerWorker.Close(); err != nil {
		panic(err)
	}
}

func ConnectToConsumer(brokers []string) (sarama.Consumer, error) {
	conf := sarama.NewConfig()
	conf.Consumer.Return.Errors = true

	return sarama.NewConsumer(brokers, conf)
}
