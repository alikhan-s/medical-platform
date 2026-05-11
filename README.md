# AP2 Assignment 4 — Redis Caching, Rate Limiting & Async Job Pipeline

Medical Scheduling Platform extended with a centralized Redis layer for read-through caching and sliding-window rate limiting, a goroutine-based background job queue inside the Notification Service, and a standalone Mock Gateway used to exercise the retry/dead-letter machinery.

---

## 1. Project Overview — What Changed Since Assignment 3

Assignment 3 introduced PostgreSQL persistence and NATS event-driven messaging. Assignment 4 keeps every domain model, use-case interface, and gRPC contract identical, but layers **four new infrastructure capabilities** around the existing services:

| Area | Before (A3) | After (A4) |
|------|-------------|------------|
| Read latency | Every gRPC read hit PostgreSQL | Cache-aside via Redis (60 s default TTL) on Doctor/Appointment reads |
| Write semantics | DB write, then fire NATS event | DB write → cache invalidate/refresh → fire NATS event (decorator order matters) |
| Abuse protection | None — every gRPC call processed | Sliding-window rate limiter (100 req/min/IP by default), enforced as a `UnaryServerInterceptor` |
| Notification fan-out | Inline log of the NATS event | Worker pool (default 3 goroutines) consumes `appointments.status_updated` events where `new_status == "done"`, calls an HTTP gateway, retries on transient failure, and dead-letters after exhausting attempts |
| External integration | None | New `mock-gateway` service simulating an unreliable downstream system (20 % injected 503s, in-memory idempotency tracking) |

Three new infrastructure containers are now part of the stack: `redis` (caching + rate limit + idempotency store), `mock-gateway` (HTTP test target), and one additional reason for `notification-service` to depend on Redis.

---

## 2. Architecture Diagram

```
                     ┌─────────────────────────┐
                     │        gRPC clients     │
                     │       (grpcurl, etc.)   │
                     └────────────┬────────────┘
                                  │
            ┌─────────────────────┴─────────────────────┐
            │                                           │
       gRPC :8081                                   gRPC :8082
            │                                           │
   ┌────────▼────────┐                       ┌──────────▼──────────┐
   │   doctor-s      │   internal gRPC       │    appointment-s    │
   │  ─────────────  │  ◄────CheckDoctor──── │  ─────────────────  │
   │ ratelimit (mw)  │                       │  ratelimit (mw)     │
   │ event wrapper   │                       │  event wrapper      │
   │ cache wrapper   │                       │  cache wrapper      │
   │ core usecase    │                       │  core usecase       │
   └──┬───────┬─────┬┘                       └──┬───────┬─────┬────┘
      │       │     │                           │       │     │
      │       │     │ pub: doctors.created      │       │     │ pub: appointments.{created,status_updated}
      ▼       ▼     ▼                           ▼       ▼     ▼
 ┌────────┐ ┌─────┐ ┌──────┐                ┌─────────┐ ┌─────┐ ┌──────┐
 │pg-     │ │redis│ │ nats │ <- shared ->   │pg-      │ │redis│ │ nats │
 │doctors │ │:6379│ │:4222 │                │appointm.│ │:6379│ │:4222 │
 └────────┘ └─────┘ └──┬───┘                └─────────┘ └─────┘ └──┬───┘
                       │                                           │
                       │  subscribe: doctors.created,              │
                       │             appointments.created,         │
                       │             appointments.status_updated   │
                       └──────────────────────┬────────────────────┘
                                              ▼
                              ┌──────────────────────────────┐
                              │   notification-service       │
                              │  ────────────────────────    │
                              │  NATS subscriber             │
                              │  ↓ (status_updated=done)     │
                              │  jobqueue.Enqueue            │
                              │  ↓ (Redis idempotency check) │
                              │  worker pool (3 goroutines)  │
                              │  ↓ POST /notify              │
                              └────────────┬─────────────────┘
                                           │
                                           ▼
                                   ┌───────────────┐
                                   │ mock-gateway  │
                                   │   :8080       │
                                   │  (20 % 503,   │
                                   │   in-mem idem)│
                                   └───────────────┘
```

The Redis box in the diagram is logically the same physical instance — every service writes to its own key namespace (`doctor:*`, `appointment:*`, `<service>:ratelimit:*`, `notification:idempotency:*`).

---

## 3. Cache Strategy

The cache layer is implemented as a **decorator** sitting between the core use case and the event-publishing decorator (`repo → core UC → cache wrapper → event wrapper`). This ordering is intentional: cache writes only happen after the DB returns success, and NATS events only fire after both the DB **and** the cache reflect the new state.

| Endpoint | Strategy | Reason |
|----------|----------|--------|
| `Doctor.GetDoctor` | **Cache-Aside** | Doctor records are read far more often than they change; a TTL-bounded cache absorbs bursts cheaply. |
| `Doctor.ListDoctors` | **Cache-Aside** keyed at `doctor:list:all` | The list is expensive to compute but rarely changes; one well-known key makes invalidation trivial. |
| `Doctor.CreateDoctor` | **Write-Through (list invalidation)** | A new doctor must never be missing from `ListDoctors` results. We do not warm a per-ID key because we don't yet know if anyone will read this doctor. |
| `Appointment.GetAppointment` | **Cache-Aside** | Same as Doctor: hot reads, occasional writes. |
| `Appointment.ListAppointments` | **Cache-Aside** keyed at `appointment:list:all` | Same as Doctor list. |
| `Appointment.CreateAppointment` | **Write-Around (list invalidation only)** | Appointments are most often read by ID, not bulk-listed immediately after creation. Skipping the per-ID cache write avoids polluting Redis with entries that may never be read before TTL expiry. |
| `Appointment.UpdateAppointmentStatus` | **Write-Through (per-ID refresh + list invalidation)** | Status is the most volatile field and the most likely to be read immediately (e.g., to confirm the transition). We refetch and `SET` the fresh entity so the next read is a hit; the list key is invalidated because order/filtering may change. |

All cache misses fall through to the DB transparently. Cache errors (Redis down, JSON decode failure, etc.) are logged at WARN and treated as misses — the request is **always** served from the source of truth.

### Key format

Defined by `cache.RedisRepository.key`:

```
<service>:<entity>:<id>
```

Examples:
- `doctor:doctor:8d3e...` — a single doctor record
- `doctor:list:all` — the cached `ListDoctors` payload
- `appointment:appointment:7c14...`
- `appointment:list:all`

---

## 4. Cache Invalidation & Consistency Window

Invalidation is driven entirely from the cache decorator. The sequence for any mutating call:

1. The core use case writes to PostgreSQL inside its own transaction.
2. **Only on success** the decorator runs the appropriate cache op (`Delete` for invalidation, `Set` for write-through refresh).
3. **Only after** the cache op the event decorator publishes to NATS.

Failure modes and their consistency implications:

- **Cache `Delete` fails** (Redis transient error): the DB is up to date but stale data may continue to be served until either the TTL elapses (default 60 s) or the key is overwritten by a subsequent read-through. Logged at WARN, never propagated to the client.
- **Cache `Set` fails after a write-through update**: the next read will see a cache miss, fall through to the DB, and re-populate. Worst-case latency penalty for one request.
- **Redis entirely unavailable**: every read becomes a cache miss; every write's invalidation is a no-op. The service stays fully functional; latency degrades to baseline DB latency.

**Worst-case staleness window** for endpoints that only invalidate (not refresh): `min(TTL, time-until-next-write-touching-this-key)`. With the default 60 s TTL, **any inconsistency self-heals within 60 seconds** without any operator intervention.

---

## 5. Rate-Limiting Algorithm — Sliding Window via Redis ZSET

Implemented in `doctor-s/internal/middleware/ratelimit.go` and `appointment-s/internal/middleware/ratelimit.go` as a `grpc.UnaryServerInterceptor`. Each interceptor instance owns its own Redis client; the key namespace `<service>:ratelimit:<ip>` keeps the doctor and appointment limits independent.

### Algorithm

For each incoming request from IP `X`:

1. `now := time.Now().UnixNano()`; `windowStart := now - 60s`.
2. In a single Redis pipeline:
   - `ZREMRANGEBYSCORE <key> 0 <windowStart>` — drop timestamps that fell out of the window.
   - `ZCARD <key>` — count what remains.
3. If count ≥ `RATE_LIMIT_RPM` → return `status.Errorf(codes.ResourceExhausted, …)` immediately (no `ZADD`, so we don't extend our own window).
4. Otherwise `ZADD <key> <now> <now>` and refresh `EXPIRE <key> 2m` so idle keys self-clean.

### Why sliding window, not fixed window

A fixed-window limiter (e.g., "100 requests between `12:00:00` and `12:00:59`") allows a client to burst **2 × limit** at the boundary by sending 100 requests at `12:00:59.9` and another 100 at `12:01:00.1`. The sliding window protects against this by counting against the trailing 60 s **at the moment of every request**, which is what users intuitively expect from "100 RPM".

Storing each request as a ZSET member with the timestamp as both score and member gives O(log N) pruning and O(1) counting in Redis with no application-side state — the whole limiter is stateless across notification-service replicas and across restarts.

### Failure mode

If the Redis pipeline returns an error, the limiter **fails open** (logs WARN, allows the request). A degraded Redis must not take a service offline; for stricter SLAs this could be switched to fail-closed by changing one return value.

---

## 6. Job Queue Design

Located in `notification-service/internal/jobqueue/worker_pool.go`. The pool is composed of:

- **`jobs chan Job`** — a single buffered channel (capacity 256) acting as the queue.
- **N worker goroutines** — `N = WORKER_POOL_SIZE` (default 3). Each runs `for job := range jobs { … }`.
- **A `sync.WaitGroup`** for graceful shutdown.
- **A `sync.Mutex` around the JSON encoders** so stdout/stderr lines never interleave under concurrency.

### Lifecycle

```
main.go
 ├── pool := jobqueue.New(redisClient, gatewayURL)
 ├── pool.Start(ctx)            // spawns N workers
 ├── sub := subscriber.New(natsURL, pool)
 ├── sub.Subscribe()             // NATS handler calls pool.Enqueue(...)
 ├── <-sigterm
 ├── sub.Drain()                 // stop accepting new events
 └── pool.Stop()                 // close jobs chan, wg.Wait()
```

### Backpressure

When the channel is full, `Enqueue` uses a non-blocking `select`:

```go
select {
case p.jobs <- job:
    log("enqueued")
default:
    log_stderr("dead_letter", reason="queue full — dropping job")
}
```

The rationale: blocking the NATS callback would back up the entire subscriber and eventually kill the connection. Dropping with a dead-letter log is loud and observable, and the lost event can be replayed manually from the NATS log line that the subscriber already emitted *before* enqueueing. Bumping `WORKER_POOL_SIZE` or the buffer constant is the right response if drops become frequent.

---

## 7. Idempotency

The Notification Service must not invoke the downstream gateway twice for the same business event, even if NATS redelivers, even if the service restarts mid-job, and even if a duplicate event slips through publishing.

### Key derivation

For every `appointments.status_updated` event with `new_status == "done"`:

```go
key := sha256(event_type + id + occurred_at)
```

(see `jobqueue.IdempotencyKey`).

- `event_type` discriminates against future event types reusing the same `id`/`occurred_at`.
- `id` is the appointment ID.
- `occurred_at` is the producer-generated RFC3339 timestamp from the publishing service.

Because all three inputs are produced **upstream** of the notification service, the same business event always produces the same SHA-256, regardless of how many times it reaches us.

### Enforcement points

Two layers, both backed by Redis:

1. **Pre-enqueue check** in `pool.Enqueue`. Reads `notification:idempotency:<sha>`; if the value is `"done"`, the job is dropped immediately with a `dropped_duplicate` JSON log line. No goroutine wakeup, no HTTP call.
2. **Mock Gateway** maintains its own in-memory `seenStore` keyed by `idempotency_key` from the request body (and `X-Idempotency-Key` header as fallback). First sighting → `{"status":"accepted"}`. Repeat → `{"status":"duplicate"}`. Both are HTTP 200 — duplicates are not failures, they are *confirmations of safety*.

After a successful 200 from the gateway, the worker writes `SET notification:idempotency:<sha> "done" EX 86400` so future replays are dropped at layer 1 for the next 24 hours.

---

## 8. Dead-Letter Strategy

A job goes through up to **4 HTTP attempts** total: initial + 3 retries on transient failure.

| Outcome | Action |
|---------|--------|
| HTTP 200 | Mark idempotency key `done` in Redis (TTL 24 h), log `success` to stdout, worker moves on. |
| HTTP 503 **or** network/timeout error | Wrap in retryable error, sleep 1 s → 2 s → 4 s, then retry. Each pre-sleep emits a `retry` JSON line on stdout with the attempt number and backoff. |
| Any other non-2xx (e.g., 400, 404, 500) | Treated as a terminal failure (the gateway said "I understood you and refuse"). Skip remaining retries, dead-letter immediately. |
| All retries exhausted | Emit a single JSON line on **stderr** with state `dead_letter`. |

### Inspecting the DLQ

The "dead-letter queue" is intentionally a *log stream*, not a Redis list — Docker/Kubernetes pipelines already capture stderr separately from stdout, and structured JSON is grep-friendly:

```bash
# Local development
docker logs notification-service 2>/tmp/dlq.log
jq -c 'select(.state=="dead_letter")' /tmp/dlq.log

# Just the dead letters, live
docker logs -f notification-service 2>&1 1>/dev/null | jq -c 'select(.state=="dead_letter")'
```

Each dead-letter line includes `idempotency_key`, `appointment_id`, `occurred_at`, the last error message, and `extra.attempts`, which is everything needed to either manually replay through the gateway or escalate to ops.

---

## 9. Environment Variables

### Shared across all services

| Variable | Default | Used by | Purpose |
|----------|---------|---------|---------|
| `REDIS_ADDR` | `localhost:6379` | doctor-s, appointment-s, notification-service | Redis host:port |

### Doctor Service & Appointment Service

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | *(required)* | PostgreSQL DSN |
| `NATS_URL` | `nats://localhost:4222` | NATS broker |
| `GRPC_ADDR` | `:8081` / `:8082` | Listen address |
| `DOCTOR_SERVICE_ADDR` | `localhost:8081` | *(appointment-s only)* — Doctor gRPC client target |
| `CACHE_TTL_SECONDS` | `60` | TTL applied to every cache `SET` |
| `RATE_LIMIT_RPM` | `100` | Max requests/min/IP at the gRPC interceptor |

### Notification Service

| Variable | Default | Purpose |
|----------|---------|---------|
| `NATS_URL` | `nats://localhost:4222` | NATS broker |
| `REDIS_ADDR` | `localhost:6379` | Idempotency store |
| `GATEWAY_URL` | `http://mock-gateway:8080/notify` | Downstream notification endpoint |
| `WORKER_POOL_SIZE` | `3` | Number of worker goroutines |

### Mock Gateway

| Variable | Default | Purpose |
|----------|---------|---------|
| `GATEWAY_PORT` | `8080` | HTTP listen port (internal to the docker network) |

---

## 10. Infrastructure & Setup

### Quick start

```bash
docker compose up --build
```

This brings up the entire stack with healthchecks and the correct startup order. The host-side ports exposed are:

| Service | Host port | Container port |
|---------|-----------|----------------|
| pg-doctors | 5433 | 5432 |
| pg-appointments | 5434 | 5432 |
| NATS | 4222 (clients), 8222 (monitor) | same |
| Redis | 6379 | 6379 |
| doctor-s | 8081 | 8081 |
| appointment-s | 8082 | 8082 |
| mock-gateway | 8090 | 8080 |

### Iterating on a single service

```bash
docker compose up -d redis nats pg-doctors pg-appointments mock-gateway
docker compose up --build doctor-s appointment-s notification-service
```

### Tearing down (keep volumes)

```bash
docker compose down
```

### Tearing down (wipe DB volumes too)

```bash
docker compose down -v
```

---

## 11. Service Startup Order

`docker compose` enforces this via `depends_on` + healthchecks, but the logical order is:

1. **Infrastructure layer** — start in parallel; nothing else can boot without them:
   - `redis` (used by all 3 Go services)
   - `pg-doctors`, `pg-appointments` (healthcheck = `pg_isready`)
   - `nats` (broker)
2. **`mock-gateway`** — pure stateless HTTP, no dependencies. Starts as soon as the network is up. We bring it up before the notification-service so the first burst of jobs has somewhere to land.
3. **`doctor-s`** — needs `pg-doctors`, `nats`, `redis`. Runs migrations on boot.
4. **`appointment-s`** — needs `pg-appointments`, `nats`, `redis`, and the **doctor-s** gRPC endpoint (its use case calls `CheckDoctorExists` synchronously during `CreateAppointment`).
5. **`notification-service`** — needs `nats`, `redis`, `mock-gateway`. Last to start because everything it touches must already be reachable.

Healthchecks for Redis and PostgreSQL are wired so dependent services don't race the boot.

---

## 12. Testing with `grpcurl`

All gRPC services expose reflection, so no `.proto` paths are needed.

### Successful path — create a doctor, see a cache miss, then a hit

```bash
# 1. Create
grpcurl -plaintext -d '{
  "full_name":"Dr. House",
  "specialization":"Diagnostics",
  "email":"house@hospital.com"
}' localhost:8081 doctor.DoctorService/CreateDoctor

# 2. First Get → cache miss → DB → SET
grpcurl -plaintext -d '{"id":"<id-from-step-1>"}' localhost:8081 doctor.DoctorService/GetDoctor

# 3. Second Get within 60s → cache hit (observe latency drop)
grpcurl -plaintext -d '{"id":"<id-from-step-1>"}' localhost:8081 doctor.DoctorService/GetDoctor
```

### Triggering the full job-queue path

```bash
# Create an appointment
grpcurl -plaintext -d '{
  "title":"Annual checkup",
  "doctor_id":"<doctor-id>"
}' localhost:8082 appointment.AppointmentService/CreateAppointment

# Move it to done — this fires appointments.status_updated which the
# notification-service picks up and pushes to the mock-gateway.
grpcurl -plaintext -d '{
  "id":"<appointment-id>",
  "status":"done"
}' localhost:8082 appointment.AppointmentService/UpdateAppointmentStatus
```

### Exercising the rate limiter

```bash
# Fire 150 requests as fast as possible; expect codes.ResourceExhausted after 100.
for i in $(seq 1 150); do
  grpcurl -plaintext -d '{}' localhost:8081 doctor.DoctorService/ListDoctors 2>&1 |
    grep -E "(ResourceExhausted|error)" || echo "ok $i"
done
```

### Expected notification-service stdout (success)

```json
{"time":"2026-05-11T19:42:01.001Z","state":"enqueued","idempotency_key":"a3f1...","appointment_id":"7c14...","occurred_at":"2026-05-11T19:42:00Z"}
{"time":"2026-05-11T19:42:01.012Z","state":"processing","worker_id":2,"idempotency_key":"a3f1...","appointment_id":"7c14..."}
{"time":"2026-05-11T19:42:01.078Z","state":"success","worker_id":2,"idempotency_key":"a3f1...","appointment_id":"7c14..."}
```

### Expected notification-service stdout (retry then success)

```json
{"time":"...","state":"enqueued","idempotency_key":"b2e0...","appointment_id":"...","occurred_at":"..."}
{"time":"...","state":"processing","worker_id":1,"idempotency_key":"b2e0..."}
{"time":"...","state":"retry","worker_id":1,"idempotency_key":"b2e0...","message":"gateway returned 503","extra":{"attempt":1,"max":3,"backoff_ms":1000}}
{"time":"...","state":"retry","worker_id":1,"idempotency_key":"b2e0...","message":"gateway returned 503","extra":{"attempt":2,"max":3,"backoff_ms":2000}}
{"time":"...","state":"success","worker_id":1,"idempotency_key":"b2e0..."}
```

### Expected notification-service stderr (dead letter)

```json
{"time":"...","state":"dead_letter","worker_id":3,"idempotency_key":"c0d1...","appointment_id":"...","occurred_at":"...","message":"gateway returned 503","extra":{"attempts":4}}
```

### Expected mock-gateway stdout (parallel view)

```json
{"time":"...","method":"POST","path":"/notify","remote_addr":"172.18.0.7:48211","idempotency_key":"a3f1...","status_code":200,"response":"accepted","body":{...}}
{"time":"...","method":"POST","path":"/notify","remote_addr":"...","idempotency_key":"b2e0...","status_code":503,"response":"injected_failure","body":{...}}
{"time":"...","method":"POST","path":"/notify","remote_addr":"...","idempotency_key":"b2e0...","status_code":200,"response":"accepted","body":{...}}
```

Note that the *second* successful call for `b2e0...` returns `accepted`, not `duplicate`, because the gateway's first response was 503 and it never recorded the key. This is exactly what you want for retry semantics — only successful invocations should be marked as seen.

---

## 13. Consistency & Trade-offs

### Cache consistency

The system is **eventually consistent** with a tight upper bound:

- The cache decorator runs inside the gRPC request path, so write-through refreshes and invalidations are synchronous from the client's perspective. A client who issues `UpdateStatus` and immediately re-reads will see the new value on the next call.
- Other readers may see stale data for **at most one TTL** (60 s by default) — and usually much less, because either the next mutation will invalidate or the next miss will refresh.
- **When Redis is down**, the system degrades gracefully: every read is a miss → falls through to PostgreSQL; every invalidation is a no-op (logged WARN). Latency rises, but no request fails because of cache infrastructure.
- We deliberately do **not** participate in a two-phase commit between DB and cache. The DB is the source of truth; the cache is a hint with a TTL. This is the right trade-off for read-heavy CRUD with weak freshness requirements.

### Rate limiting: centralized Redis vs per-instance

We chose a **centralized Redis-backed limiter** over an in-process counter for three reasons:

1. **Horizontal scaling**: as soon as `doctor-s` runs as more than one replica, a per-process limiter lets a client burst `N × limit`. Redis enforces a single budget regardless of replica count.
2. **State survives restart**: a process-local sliding window resets on every redeploy, giving a free spike on every rollout. The Redis ZSET persists.
3. **Operational visibility**: one `ZCARD doctor:ratelimit:<ip>` answers "is this client being throttled?".

The cost: every gRPC call adds one Redis round-trip (~0.5 ms on a local network). For our throughput targets this is negligible compared to PostgreSQL latency on a cache miss. The interceptor fails open on Redis errors, so the limiter cannot take production down by itself.

### Job queue durability

The in-memory channel is **non-durable**. A `SIGKILL` of the notification-service loses any jobs still in the buffer. We accept this because:

- The originating event is already in NATS, and a future iteration can switch to NATS JetStream for durable consumer semantics with zero code changes to the worker logic.
- The dead-letter path is loud enough (structured stderr) that lost events are observable.
- Idempotency keys make manual replay safe.

If durability becomes a requirement, the migration path is: replace the `chan Job` with a JetStream pull-consumer, keep the worker pool, keep the idempotency check.

---

## 14. Layout

```
medical-platform-redis/
├── docker-compose.yml
├── doctor-s/
│   ├── cmd/doctor-s/main.go               # wiring: repo → uc → cache → events
│   ├── internal/
│   │   ├── cache/                         # CacheRepository iface + Redis impl + UC decorator
│   │   ├── middleware/ratelimit.go        # gRPC sliding-window interceptor
│   │   ├── event/                         # NATS publisher + UC decorator
│   │   ├── repository/                    # PostgreSQL impl
│   │   ├── usecase/                       # core domain logic
│   │   └── transport/grpc/                # handler
│   └── migrations/
├── appointment-s/                         # mirror of doctor-s with appointment domain
├── notification-service/
│   ├── cmd/notification-service/main.go   # wiring: redis → pool → subscriber
│   └── internal/
│       ├── jobqueue/worker_pool.go        # channel + workers + retry/DLQ
│       └── subscriber/nats_subscriber.go  # filters status_updated=done → pool.Enqueue
└── mock-gateway/
    └── main.go                            # stdlib HTTP service, 20% 503, in-mem idem
```
