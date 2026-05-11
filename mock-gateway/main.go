package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	defaultPort   = 8080
	failureChance = 0.20
)

type requestLog struct {
	Time           string          `json:"time"`
	Method         string          `json:"method"`
	Path           string          `json:"path"`
	RemoteAddr     string          `json:"remote_addr"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	StatusCode     int             `json:"status_code"`
	Response       string          `json:"response,omitempty"`
	Body           json.RawMessage `json:"body,omitempty"`
}

type seenStore struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

func newSeenStore() *seenStore {
	return &seenStore{keys: make(map[string]struct{})}
}

// recordOrSeen returns true if the key was already present, false if it was newly recorded.
func (s *seenStore) recordOrSeen(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[key]; ok {
		return true
	}
	s.keys[key] = struct{}{}
	return false
}

func main() {
	port := portFromEnv()
	store := newSeenStore()
	enc := json.NewEncoder(os.Stdout)
	var encMu sync.Mutex
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var rngMu sync.Mutex

	logLine := func(entry requestLog) {
		entry.Time = time.Now().UTC().Format(time.RFC3339Nano)
		encMu.Lock()
		_ = enc.Encode(entry)
		encMu.Unlock()
	}

	http.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			logLine(requestLog{
				Method: r.Method, Path: r.URL.Path, RemoteAddr: r.RemoteAddr,
				StatusCode: http.StatusMethodNotAllowed, Response: "method_not_allowed",
			})
			return
		}

		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		var parsed struct {
			IdempotencyKey string `json:"idempotency_key"`
		}
		_ = json.Unmarshal(body, &parsed)
		key := parsed.IdempotencyKey
		if key == "" {
			key = r.Header.Get("X-Idempotency-Key")
		}

		rngMu.Lock()
		fail := rng.Float64() < failureChance
		rngMu.Unlock()
		if fail {
			http.Error(w, "service unavailable", http.StatusServiceUnavailable)
			logLine(requestLog{
				Method: r.Method, Path: r.URL.Path, RemoteAddr: r.RemoteAddr,
				IdempotencyKey: key, StatusCode: http.StatusServiceUnavailable,
				Response: "injected_failure", Body: rawIfValid(body),
			})
			return
		}

		status := "accepted"
		if key != "" && store.recordOrSeen(key) {
			status = "duplicate"
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status})

		logLine(requestLog{
			Method: r.Method, Path: r.URL.Path, RemoteAddr: r.RemoteAddr,
			IdempotencyKey: key, StatusCode: http.StatusOK,
			Response: status, Body: rawIfValid(body),
		})
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("INFO [mock-gateway]: listening on %s (failure_chance=%.2f)", addr, failureChance)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("FATAL [mock-gateway]: %v", err)
	}
}

func portFromEnv() int {
	v := os.Getenv("GATEWAY_PORT")
	if v == "" {
		return defaultPort
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 || n > 65535 {
		log.Printf("WARN [mock-gateway]: invalid GATEWAY_PORT=%q, using default %d", v, defaultPort)
		return defaultPort
	}
	return n
}

func rawIfValid(b []byte) json.RawMessage {
	if json.Valid(b) {
		return json.RawMessage(b)
	}
	return nil
}
