package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/IBM/sarama"
)

type Order struct {
	CustomerName string `json:"customer_name"`
	CoffeeType   string `json:"coffee_type"`
}

func main() {
	http.HandleFunc("/order", placeOrder)

	log.Println("Server is listening on 3000")
	http.ListenAndServe(":3000", nil)
}

func ConnectProducer(brokers []string) (sarama.SyncProducer, error) {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.RequiredAcks = sarama.WaitForAll
	conf.Producer.Retry.Max = 5

	return sarama.NewSyncProducer(brokers, conf)
}

func PushOrderToQueue(topic string, message []byte) error {
	brokers := []string{"localhost:9092"}
	// Create connection
	producer, err := ConnectProducer(brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	log.Printf("Order is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

// placeOrder handler
func placeOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// 1. Parse request body into order
	order := new(Order)
	if err := json.NewDecoder(r.Body).Decode(order); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// 2. Convert body into bytes
	orderInBytes, err := json.Marshal(order)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 3. Send the bytes to kafka
	err = PushOrderToQueue("coffee_order", orderInBytes)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	// 4. Send back to the user
	response := map[string]any{
		"success": true,
		"msg":     "Order for " + order.CustomerName + " placed successfully!",
	}

	w.Header().Set("Content-type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
