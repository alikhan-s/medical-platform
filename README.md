# AP2 Assignment 3 — Message Queue & Database Migrations

Medical Scheduling Platform extended with PostgreSQL persistence and NATS event-driven messaging.

---

## 1. Project Overview — What Changed and Why

Assignment 2 used MongoDB and in-memory maps with pure gRPC communication between services.  
Assignment 3 makes three targeted changes to the **infrastructure layer only**. All domain models, use-case logic, and gRPC contracts are unchanged.

| Area | Before (A2) | After (A3) |
|------|-------------|------------|
| Storage | MongoDB (in-memory maps in some variants) | PostgreSQL — one isolated DB per service |
| Schema management | None / auto | Versioned SQL migration files via `golang-migrate` |
| Inter-service async | None | NATS Core Pub/Sub — domain events after every successful write |
| Notification | None | Standalone `notification-service` subscribing to all three event subjects |

**Why PostgreSQL?** ACID transactions, standard SQL, strong typing, and the most common choice in production Go services.  
**Why NATS Core?** See Section 2.

---

## 2. Broker Choice — NATS Core

**Chosen broker: NATS Core** (`github.com/nats-io/nats.go`)

### Reason

The notifications in this system are **stateless and fire-and-forget**: the Notification Service logs an event line and discards it. There is no requirement for durability, replay, or guaranteed delivery to multiple independent consumers. NATS Core has:

- Zero broker-side configuration — `nats-server` starts with a single Docker command and no setup.
- Sub-millisecond publish latency.
- A minimal, idiomatic Go client.
- Automatic fan-out to all active subscribers on a subject.

NATS JetStream or RabbitMQ would be chosen if durable delivery, replay-on-reconnect, or consumer-group semantics were required (see Section 8).

---

## 3. Environment Variables

### Doctor Service (`doctor-s/`)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | **yes** | — | PostgreSQL DSN, e.g. `postgres://postgres:postgres@localhost:5432/doctors_db?sslmode=disable` |
| `NATS_URL` | no | `nats://localhost:4222` | NATS connection URL |
| `GRPC_ADDR` | no | `:8081` | Address the gRPC server binds to |

### Appointment Service (`appointment-s/`)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | **yes** | — | PostgreSQL DSN, e.g. `postgres://postgres:postgres@localhost:5432/appointments_db?sslmode=disable` |
| `NATS_URL` | no | `nats://localhost:4222` | NATS connection URL |
| `GRPC_ADDR` | no | `:8082` | Address the gRPC server binds to |
| `DOCTOR_SERVICE_ADDR` | no | `localhost:8081` | Address of the Doctor Service gRPC endpoint |

### Notification Service (`notification-service/`)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `NATS_URL` | no | `nats://localhost:4222` | NATS connection URL |

---

## 4. Infrastructure Setup

### Start PostgreSQL (two isolated databases)

```bash
# Doctors database
docker run -d \
  --name pg-doctors \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=doctors_db \
  -p 5432:5432 \
  postgres:16-alpine

# Appointments database (different port to avoid conflict)
docker run -d \
  --name pg-appointments \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=appointments_db \
  -p 5433:5432 \
  postgres:16-alpine
```

### Start NATS

```bash
docker run -d \
  --name nats \
  -p 4222:4222 \
  -p 8222:8222 \
  nats:latest
```

Verify NATS is reachable: `curl http://localhost:8222/varz`

### Fetch Go dependencies

Run in each service directory after the first checkout:

```bash
cd doctor-s        && go mod tidy
cd ../appointment-s && go mod tidy
cd ../notification-service && go mod tidy
```

---

## 5. Migration Instructions

### Automatic (normal operation)

Migrations run **automatically on every service startup**, before the gRPC server accepts requests. No manual step is needed.

```
[startup] INFO: migrations applied successfully
```

If there is nothing new to apply, `golang-migrate` returns `ErrNoChange` which the service ignores gracefully.

### Manual — apply / roll back with the CLI

Install the CLI once:

```bash
go install -tags 'pgx5' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
```

```bash
# Apply (from doctor-s/)
migrate -source file://migrations \
        -database "pgx5://postgres:postgres@localhost:5432/doctors_db?sslmode=disable" \
        up

# Roll back one step
migrate -source file://migrations \
        -database "pgx5://postgres:postgres@localhost:5432/doctors_db?sslmode=disable" \
        down 1

# Same for appointments (port 5433)
migrate -source file://migrations \
        -database "pgx5://postgres:postgres@localhost:5433/appointments_db?sslmode=disable" \
        up
```

> **Note:** The `DATABASE_URL` env var uses the `postgres://` scheme. The services convert it internally to `pgx5://` before passing it to `golang-migrate`.

---

## 6. Service Startup Order

Start services in the order below. Each service must be started from its own directory because the migration source path `file://migrations` is resolved relative to the working directory.

**Terminal 1 — Doctor Service**

```bash
cd doctor-s
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/doctors_db?sslmode=disable"
export NATS_URL="nats://localhost:4222"
go run ./cmd/doctor-s/
```

**Terminal 2 — Appointment Service** (start after Doctor Service is up)

```bash
cd appointment-s
export DATABASE_URL="postgres://postgres:postgres@localhost:5433/appointments_db?sslmode=disable"
export NATS_URL="nats://localhost:4222"
export DOCTOR_SERVICE_ADDR="localhost:8081"
go run ./cmd/appointment-s/
```

**Terminal 3 — Notification Service** (can start at any time)

```bash
cd notification-service
export NATS_URL="nats://localhost:4222"
go run ./cmd/notification-service/
```

**Why this order?**  
The Appointment Service dials the Doctor Service gRPC endpoint at startup. If the Doctor Service is not yet running the dial will fail. The Notification Service has no startup dependencies beyond NATS.

---

## 7. Event Contract

All events are serialised as JSON and published **after** a successful database commit.

### `doctors.created`

Published by: Doctor Service  
Trigger: `CreateDoctor` RPC succeeds

```json
{
  "event_type":      "doctors.created",
  "occurred_at":     "2026-05-01T10:23:44Z",
  "id":              "3f7a1c2e-...",
  "full_name":       "Dr. Aisha Seitkali",
  "specialization":  "Cardiology",
  "email":           "a.seitkali@clinic.kz"
}
```

### `appointments.created`

Published by: Appointment Service  
Trigger: `CreateAppointment` RPC succeeds

```json
{
  "event_type":  "appointments.created",
  "occurred_at": "2026-05-01T10:24:01Z",
  "id":          "9b2e4d7a-...",
  "title":       "Initial cardiac consultation",
  "doctor_id":   "3f7a1c2e-...",
  "status":      "new"
}
```

### `appointments.status_updated`

Published by: Appointment Service  
Trigger: `UpdateAppointmentStatus` RPC succeeds

```json
{
  "event_type":  "appointments.status_updated",
  "occurred_at": "2026-05-01T10:25:10Z",
  "id":          "9b2e4d7a-...",
  "old_status":  "new",
  "new_status":  "in_progress"
}
```

### Notification Service stdout format

Each received event produces exactly one JSON line on stdout:

```json
{"time":"2026-05-01T10:23:44Z","subject":"doctors.created","event":{"event_type":"doctors.created","occurred_at":"2026-05-01T10:23:44Z","id":"3f7a1c2e-...","full_name":"Dr. Aisha Seitkali","specialization":"Cardiology","email":"a.seitkali@clinic.kz"}}
```

### grpcurl test commands and expected log output

```bash
# 1. Create a doctor
grpcurl -plaintext -d '{
  "full_name": "Dr. Aisha Seitkali",
  "specialization": "Cardiology",
  "email": "a.seitkali@clinic.kz"
}' localhost:8081 doctor.DoctorService/CreateDoctor
```

**Notification Service prints:**
```json
{"time":"<RFC3339>","subject":"doctors.created","event":{"event_type":"doctors.created","occurred_at":"<RFC3339>","id":"<uuid>","full_name":"Dr. Aisha Seitkali","specialization":"Cardiology","email":"a.seitkali@clinic.kz"}}
```

```bash
# 2. Create an appointment (replace <doctor_id> with id from step 1)
grpcurl -plaintext -d '{
  "title": "Initial cardiac consultation",
  "description": "First visit",
  "doctor_id": "<doctor_id>"
}' localhost:8082 appointment.AppointmentService/CreateAppointment
```

**Notification Service prints:**
```json
{"time":"<RFC3339>","subject":"appointments.created","event":{"event_type":"appointments.created","occurred_at":"<RFC3339>","id":"<uuid>","title":"Initial cardiac consultation","doctor_id":"<doctor_id>","status":"new"}}
```

```bash
# 3. Update appointment status (replace <appt_id> with id from step 2)
grpcurl -plaintext -d '{
  "id": "<appt_id>",
  "status": "in_progress"
}' localhost:8082 appointment.AppointmentService/UpdateAppointmentStatus
```

**Notification Service prints:**
```json
{"time":"<RFC3339>","subject":"appointments.status_updated","event":{"event_type":"appointments.status_updated","occurred_at":"<RFC3339>","id":"<appt_id>","old_status":"new","new_status":"in_progress"}}
```

```bash
# Additional queries (no events published — read-only)
grpcurl -plaintext -d '{"id": "<doctor_id>"}' localhost:8081 doctor.DoctorService/GetDoctor
grpcurl -plaintext localhost:8081 doctor.DoctorService/ListDoctors
grpcurl -plaintext -d '{"id": "<appt_id>"}' localhost:8082 appointment.AppointmentService/GetAppointment
grpcurl -plaintext localhost:8082 appointment.AppointmentService/ListAppointments
```

---

## 8. Consistency Trade-offs

### The gap between DB commit and NATS publish

Publishing happens **after** the database transaction commits, not inside it:

```
1. BEGIN TX
2. INSERT INTO doctors ... → COMMIT   ← durable in PostgreSQL
3. nats.Publish(...)                  ← may fail or process may crash here
```

If the process crashes at step 3 (e.g., OOM kill, power loss), the row is saved in PostgreSQL but the event is **silently lost**. Subscribers will never receive it. This is the fundamental trade-off of the "at-most-once" delivery model.

**Consequences in this system:**
- The Notification Service may miss log lines for events that were committed.
- No data corruption occurs — the database is always the source of truth.
- The inconsistency is only observable at the messaging layer.

### How to fix it: the Outbox Pattern

The Outbox Pattern eliminates the gap by making the event write **part of the same transaction** as the entity write:

```sql
BEGIN;
  INSERT INTO doctors (...) VALUES (...);
  INSERT INTO outbox (subject, payload, created_at) VALUES ('doctors.created', '...', now());
COMMIT;
```

A separate **relay process** reads unpublished rows from `outbox`, publishes them to NATS, and marks them as sent. The relay can retry on failure, providing **at-least-once** delivery semantics.

### How to fix it: NATS JetStream

NATS JetStream adds persistent streams and consumer tracking on top of NATS Core:

- Publishers receive an acknowledgement that the message was stored durably in the stream.
- Subscribers receive exactly-once or at-least-once delivery with replay on reconnect.
- A process crash during publish can be detected via timeout; the publisher can retry.

Switching from NATS Core to JetStream requires:
1. Creating a named stream (`nats.JetStream()`, `js.AddStream(...)`)
2. Publishing with `js.Publish(subject, data)` instead of `nc.Publish`
3. Subscribing with durable consumers: `js.Subscribe(subject, handler, nats.Durable("notification-svc"))`

---

## 9. Broker Comparison — NATS Core vs RabbitMQ

| Dimension | NATS Core | RabbitMQ |
|-----------|-----------|----------|
| **Delivery guarantee** | At-most-once (fire-and-forget). Messages are dropped if no subscriber is connected at publish time. | At-least-once. Messages survive in durable queues even if all consumers are offline. |
| **Persistence** | None in Core. Messages exist only in memory while in-flight. | Queue-level disk persistence. Messages survive broker restarts if queues are declared durable. |
| **Setup complexity** | Single binary, zero config. `docker run nats` is all you need. | Requires AMQP exchange, queue, and binding declarations. Additional management UI at `:15672`. |
| **Fan-out model** | Pub/Sub on subjects — all active subscribers on a subject receive every message automatically. | Requires a `fanout` exchange; each consumer binds its own exclusive queue to the exchange. |
| **Protocol** | NATS protocol (binary, custom). | AMQP 0-9-1 (standardised). Interoperable with other AMQP clients. |
| **Go client** | `github.com/nats-io/nats.go` | `github.com/rabbitmq/amqp091-go` |

**When to choose NATS Core:** Stateless notifications, low-latency telemetry, internal microservice fan-out where message loss is acceptable.

**When to choose RabbitMQ:** Financial transactions, order processing, audit logs — any domain where a lost event causes business harm and guaranteed delivery is a hard requirement.

**When to upgrade NATS Core → JetStream:** Same simplicity as NATS, but you need durable streams, consumer groups, or replay. JetStream is the recommended upgrade path within the NATS ecosystem.

---

## 10. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client (grpcurl)                         │
└──────────────────┬──────────────────────┬───────────────────────┘
                   │ gRPC :8081           │ gRPC :8082
                   ▼                      ▼
      ┌────────────────────┐   ┌────────────────────────┐
      │   Doctor Service   │   │  Appointment Service   │
      │                    │◄──│  (gRPC client)         │
      │  ┌──────────────┐  │   │  ┌──────────────────┐  │
      │  │  UseCase +   │  │   │  │  UseCase +       │  │
      │  │  Event Wrap  │  │   │  │  Event Wrap      │  │
      │  └──────┬───────┘  │   │  └────────┬─────────┘  │
      │         │          │   │           │            │
      │  ┌──────▼───────┐  │   │  ┌────────▼─────────┐  │
      │  │  PG Repo     │  │   │  │  PG Repo         │  │
      └──┴──────┬───────┴──┘   └──┴─────────┬────────┴──┘
                │                           │
                ▼                           ▼
     ┌──────────────────┐       ┌───────────────────────┐
     │  PostgreSQL      │       │  PostgreSQL           │
     │  doctors_db:5432 │       │  appointments_db:5433 │
     └──────────────────┘       └───────────────────────┘
                │                           │
                │  NATS publish             │  NATS publish
                │  "doctors.created"        │  "appointments.created"
                │                           │  "appointments.status_updated"
                └──────────────┬────────────┘
                               ▼
                    ┌─────────────────────┐
                    │   NATS :4222        │
                    └──────────┬──────────┘
                               │ subscribe (all 3 subjects)
                               ▼
                    ┌─────────────────────┐
                    │ Notification Service│
                    │  → JSON log stdout  │
                    └─────────────────────┘
```

---

## 11. Clean Architecture Notes

The Dependency Inversion Principle is preserved throughout:

- **gRPC handlers** depend only on the `usecase.DoctorUseCase` / `usecase.AppointmentUseCase` interfaces — unchanged from Assignment 2.
- **Use cases** depend on `repository.DoctorRepository` / `repository.AppointmentRepository` interfaces — unchanged from Assignment 2.
- **Event publishing** is added via a **decorator** (`event.NewDoctorEventUseCase`) that wraps the existing use case and implements the same interface. No existing code was modified.
- The `event.EventPublisher` interface lives in the `event` (infrastructure) package. The use-case layer has zero knowledge of NATS.
- No infrastructure type (`*pgxpool.Pool`, `*nats.Conn`) leaks past `main.go`.
