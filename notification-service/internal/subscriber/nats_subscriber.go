package subscriber

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

var subjects = []string{
	"doctors.created",
	"appointments.created",
	"appointments.status_updated",
}

type logLine struct {
	Time    string          `json:"time"`
	Subject string          `json:"subject"`
	Event   json.RawMessage `json:"event"`
}

type NATSSubscriber struct {
	conn *nats.Conn
	subs []*nats.Subscription
}

func New(url string) (*NATSSubscriber, error) {
	delays := []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second}

	var lastErr error
	for attempt, delay := range delays {
		if delay > 0 {
			log.Printf("INFO [subscriber]: retrying NATS in %v (attempt %d/%d)…",
				delay, attempt+1, len(delays))
			time.Sleep(delay)
		}

		nc, err := nats.Connect(url,
			nats.Name("notification-service"),
			nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
				if err != nil {
					log.Printf("WARN [subscriber]: NATS disconnected: %v", err)
				}
			}),
			nats.ReconnectHandler(func(nc *nats.Conn) {
				log.Printf("INFO [subscriber]: NATS reconnected to %s", nc.ConnectedUrl())
			}),
		)
		if err == nil {
			log.Printf("INFO [subscriber]: connected to NATS at %s", nc.ConnectedUrl())
			return &NATSSubscriber{conn: nc}, nil
		}

		lastErr = err
		log.Printf("WARN [subscriber]: NATS connect attempt %d failed: %v", attempt+1, err)
	}

	return nil, fmt.Errorf("could not connect to NATS after %d attempts: %w",
		len(delays), lastErr)
}

func (s *NATSSubscriber) Subscribe() error {
	for _, subj := range subjects {
		subj := subj
		sub, err := s.conn.Subscribe(subj, func(msg *nats.Msg) {
			s.handle(msg)
		})
		if err != nil {
			return fmt.Errorf("subscribe to %q: %w", subj, err)
		}
		s.subs = append(s.subs, sub)
		log.Printf("INFO [subscriber]: subscribed to %q", subj)
	}
	return nil
}

func (s *NATSSubscriber) handle(msg *nats.Msg) {
	if !json.Valid(msg.Data) {
		log.Printf("ERROR [subscriber]: received invalid JSON on subject %q — payload: %s",
			msg.Subject, msg.Data)
		return
	}

	line := logLine{
		Time:    time.Now().UTC().Format(time.RFC3339),
		Subject: msg.Subject,
		Event:   json.RawMessage(msg.Data),
	}

	out, err := json.Marshal(line)
	if err != nil {
		log.Printf("ERROR [subscriber]: marshal log line for subject %q: %v", msg.Subject, err)
		return
	}

	fmt.Fprintln(os.Stdout, string(out))
}

func (s *NATSSubscriber) Drain() {
	log.Println("INFO [subscriber]: draining in-flight messages…")
	if err := s.conn.Drain(); err != nil {
		log.Printf("WARN [subscriber]: drain error: %v", err)
	}
	log.Println("INFO [subscriber]: NATS connection closed")
}
