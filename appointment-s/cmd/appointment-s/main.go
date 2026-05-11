package main

import (
	"context"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alikhan-s/appointment-s/internal/client"
	"github.com/alikhan-s/appointment-s/internal/event"
	"github.com/alikhan-s/appointment-s/internal/repository"
	transport "github.com/alikhan-s/appointment-s/internal/transport/grpc"
	"github.com/alikhan-s/appointment-s/internal/usecase"
	pb "github.com/alikhan-s/appointment-s/proto"

	migrate "github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Print("No .env file found")
	}
}

func main() {
	dbURL := mustEnv("DATABASE_URL")
	natsURL := envOr("NATS_URL", nats.DefaultURL)
	grpcAddr := envOr("GRPC_ADDR", ":8082")
	doctorServiceAddr := envOr("DOCTOR_SERVICE_ADDR", "localhost:8081")

	// PostgreSQL
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("FATAL: connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.Ping(ctx); err != nil {
		cancel()
		log.Fatalf("FATAL: ping PostgreSQL: %v", err)
	}
	cancel()

	// Migrations
	runMigrations(toPgx5URL(dbURL))

	// NATS
	var publisher event.EventPublisher
	nc, err := nats.Connect(natsURL)
	if err != nil {
		log.Printf("WARN: NATS unavailable (%v) — events will be dropped", err)
		publisher = event.NewNoopPublisher()
	} else {
		log.Printf("INFO: connected to NATS at %s", natsURL)
		publisher = event.NewNATSPublisher(nc)
		defer nc.Drain()
	}

	// Doctor Service gRPC client
	conn, err := grpc.NewClient(doctorServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("FATAL: dial Doctor Service at %s: %v", doctorServiceAddr, err)
	}
	defer conn.Close()

	// Dependency wiring
	repo := repository.NewPostgresAppointmentRepo(pool)
	docClient := client.NewDoctorGRPCClient(conn)
	innerUC := usecase.NewAppointmentUseCase(repo, docClient)
	uc := event.NewAppointmentEventUseCase(innerUC, publisher)
	handler := transport.NewAppointmentHandler(uc)

	// gRPC server
	grpcServer := grpc.NewServer()
	pb.RegisterAppointmentServiceServer(grpcServer, handler)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("FATAL: listen %s: %v", grpcAddr, err)
	}

	go func() {
		log.Printf("INFO: Appointment Service gRPC listening on %s", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("FATAL: gRPC serve: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("INFO: shutting down Appointment Service…")
	grpcServer.GracefulStop()
	log.Println("INFO: Appointment Service stopped")
}

func runMigrations(dbURL string) {
	m, err := migrate.New("file://migrations", dbURL)
	if err != nil {
		log.Fatalf("FATAL: init migrations: %v", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatalf("FATAL: apply migrations: %v", err)
	}
	log.Println("INFO: migrations applied successfully")
}

func toPgx5URL(dbURL string) string {
	for _, pfx := range []string{"postgres://", "postgresql://"} {
		if strings.HasPrefix(dbURL, pfx) {
			return "pgx5://" + strings.TrimPrefix(dbURL, pfx)
		}
	}
	return dbURL
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("FATAL: required environment variable %q is not set", key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
