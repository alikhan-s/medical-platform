package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/alikhan-s/notification-service/internal/subscriber"
	"github.com/joho/godotenv"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func main() {
	natsURL := envOr("NATS_URL", "nats://localhost:4222")

	sub, err := subscriber.New(natsURL)
	if err != nil {
		log.Fatalf("FATAL [notification-service]: %v", err)
	}

	if err := sub.Subscribe(); err != nil {
		log.Fatalf("FATAL [notification-service]: subscribe: %v", err)
	}

	log.Println("INFO [notification-service]: running — waiting for events (Ctrl+C to stop)")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("INFO [notification-service]: shutting down gracefully…")
	sub.Drain()
	log.Println("INFO [notification-service]: exited cleanly")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
