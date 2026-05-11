package jobqueue

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultPoolSize  = 3
	defaultQueueSize = 256
	idempotencyTTL   = 24 * time.Hour
	httpTimeout      = 10 * time.Second
)

var retryBackoffs = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
}

type Job struct {
	IdempotencyKey string          `json:"idempotency_key"`
	AppointmentID  string          `json:"appointment_id"`
	OccurredAt     string          `json:"occurred_at"`
	Payload        json.RawMessage `json:"payload"`
}

type WorkerPool struct {
	size       int
	jobs       chan Job
	redis      *redis.Client
	httpClient *http.Client
	gatewayURL string
	stdout     *json.Encoder
	stderr     *json.Encoder
	logMu      sync.Mutex
	wg         sync.WaitGroup
}

func New(redisClient *redis.Client, gatewayURL string) *WorkerPool {
	size := poolSizeFromEnv()
	return &WorkerPool{
		size:       size,
		jobs:       make(chan Job, defaultQueueSize),
		redis:      redisClient,
		httpClient: &http.Client{Timeout: httpTimeout},
		gatewayURL: gatewayURL,
		stdout:     json.NewEncoder(os.Stdout),
		stderr:     json.NewEncoder(os.Stderr),
	}
}

func poolSizeFromEnv() int {
	v := os.Getenv("WORKER_POOL_SIZE")
	if v == "" {
		return defaultPoolSize
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		log.Printf("WARN [jobqueue]: invalid WORKER_POOL_SIZE=%q, using default %d", v, defaultPoolSize)
		return defaultPoolSize
	}
	return n
}

func (p *WorkerPool) Start(ctx context.Context) {
	log.Printf("INFO [jobqueue]: starting worker pool size=%d gateway=%s", p.size, p.gatewayURL)
	for i := 0; i < p.size; i++ {
		p.wg.Add(1)
		workerID := i + 1
		go p.workerLoop(ctx, workerID)
	}
}

func (p *WorkerPool) Stop() {
	close(p.jobs)
	p.wg.Wait()
	log.Println("INFO [jobqueue]: worker pool stopped")
}

func IdempotencyKey(eventType, id, occurredAt string) string {
	sum := sha256.Sum256([]byte(eventType + id + occurredAt))
	return hex.EncodeToString(sum[:])
}

func (p *WorkerPool) Enqueue(ctx context.Context, job Job) {
	if p.redis != nil {
		val, err := p.redis.Get(ctx, idempKey(job.IdempotencyKey)).Result()
		if err == nil && val == "done" {
			p.logStdout("dropped_duplicate", job, 0, "idempotency key already marked done", nil)
			return
		}
		if err != nil && !errors.Is(err, redis.Nil) {
			log.Printf("WARN [jobqueue]: idempotency check failed for %s: %v (proceeding to enqueue)", job.IdempotencyKey, err)
		}
	}

	select {
	case p.jobs <- job:
		p.logStdout("enqueued", job, 0, "", nil)
	default:
		p.logStderr("dead_letter", job, 0, "queue full — dropping job", nil)
	}
}

func (p *WorkerPool) workerLoop(ctx context.Context, workerID int) {
	defer p.wg.Done()
	for job := range p.jobs {
		p.process(ctx, workerID, job)
	}
}

func (p *WorkerPool) process(ctx context.Context, workerID int, job Job) {
	p.logStdout("processing", job, workerID, "", nil)

	var lastErr error
	for attempt := 0; attempt <= len(retryBackoffs); attempt++ {
		err := p.callGateway(ctx, job)
		if err == nil {
			p.markDone(ctx, job)
			p.logStdout("success", job, workerID, "", nil)
			return
		}
		lastErr = err

		if !isRetryable(err) {
			break
		}
		if attempt == len(retryBackoffs) {
			break
		}

		backoff := retryBackoffs[attempt]
		p.logStdout("retry", job, workerID, err.Error(), map[string]any{
			"attempt":    attempt + 1,
			"max":        len(retryBackoffs),
			"backoff_ms": backoff.Milliseconds(),
		})

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
	}

	errMsg := ""
	if lastErr != nil {
		errMsg = lastErr.Error()
	}
	p.logStderr("dead_letter", job, workerID, errMsg, map[string]any{
		"attempts": len(retryBackoffs) + 1,
	})
}

type retryableError struct{ err error }

func (e *retryableError) Error() string { return e.err.Error() }
func (e *retryableError) Unwrap() error { return e.err }

func isRetryable(err error) bool {
	var re *retryableError
	return errors.As(err, &re)
}

func (p *WorkerPool) callGateway(ctx context.Context, job Job) error {
	body, _ := json.Marshal(job)
	reqCtx, cancel := context.WithTimeout(ctx, httpTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, p.gatewayURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Key", job.IdempotencyKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return &retryableError{err: fmt.Errorf("network error: %w", err)}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	switch {
	case resp.StatusCode == http.StatusOK:
		return nil
	case resp.StatusCode == http.StatusServiceUnavailable:
		return &retryableError{err: fmt.Errorf("gateway returned 503")}
	default:
		return fmt.Errorf("gateway returned %d", resp.StatusCode)
	}
}

func (p *WorkerPool) markDone(ctx context.Context, job Job) {
	if p.redis == nil {
		return
	}
	if err := p.redis.Set(ctx, idempKey(job.IdempotencyKey), "done", idempotencyTTL).Err(); err != nil {
		log.Printf("WARN [jobqueue]: mark idempotency key %s done: %v", job.IdempotencyKey, err)
	}
}

func idempKey(k string) string {
	return "notification:idempotency:" + k
}

type stateLog struct {
	Time           string         `json:"time"`
	State          string         `json:"state"`
	WorkerID       int            `json:"worker_id,omitempty"`
	IdempotencyKey string         `json:"idempotency_key"`
	AppointmentID  string         `json:"appointment_id,omitempty"`
	OccurredAt     string         `json:"occurred_at,omitempty"`
	Message        string         `json:"message,omitempty"`
	Extra          map[string]any `json:"extra,omitempty"`
}

func (p *WorkerPool) logStdout(state string, job Job, workerID int, msg string, extra map[string]any) {
	p.writeLog(p.stdout, state, job, workerID, msg, extra)
}

func (p *WorkerPool) logStderr(state string, job Job, workerID int, msg string, extra map[string]any) {
	p.writeLog(p.stderr, state, job, workerID, msg, extra)
}

func (p *WorkerPool) writeLog(enc *json.Encoder, state string, job Job, workerID int, msg string, extra map[string]any) {
	p.logMu.Lock()
	defer p.logMu.Unlock()
	_ = enc.Encode(stateLog{
		Time:           time.Now().UTC().Format(time.RFC3339Nano),
		State:          state,
		WorkerID:       workerID,
		IdempotencyKey: job.IdempotencyKey,
		AppointmentID:  job.AppointmentID,
		OccurredAt:     job.OccurredAt,
		Message:        msg,
		Extra:          extra,
	})
}
