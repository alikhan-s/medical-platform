package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alikhan-s/notification-service/internal/jobqueue"
	"github.com/alikhan-s/notification-service/internal/subscriber"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func main() {
	natsURL := envOr("NATS_URL", "nats://localhost:4222")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	gatewayURL := envOr("GATEWAY_URL", "http://mock-gateway:8080/notify")

	// Redis
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Printf("WARN [notification-service]: redis ping failed at %s: %v (continuing — idempotency disabled)", redisAddr, err)
	} else {
		log.Printf("INFO [notification-service]: connected to redis at %s", redisAddr)
	}
	cancelPing()
	defer rdb.Close()

	// Worker pool
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := jobqueue.New(rdb, gatewayURL)
	pool.Start(ctx)

	// Subscriber
	sub, err := subscriber.New(natsURL, pool)
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
	pool.Stop()
	log.Println("INFO [notification-service]: exited cleanly")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
