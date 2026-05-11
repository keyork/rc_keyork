# rc_keyork

A reliable, asynchronous HTTP notification delivery gateway written in Go.

Business systems submit fully-formed HTTP requests to this service. rc_keyork persists them, delivers them to external vendor APIs, retries on failure with exponential back-off, circuit-breaks per target domain, and optionally calls back the submitter with the final result — all without the caller needing to stay online.

> **Design report** (problem understanding, architecture, engineering trade-offs, AI usage): [`docs/report.md`](docs/report.md)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Design](#core-design)
  - [At-Least-Once Delivery](#at-least-once-delivery)
  - [Retry Strategy](#retry-strategy)
  - [Circuit Breaker](#circuit-breaker)
  - [Callback Notification](#callback-notification)
  - [Zombie Task Recovery](#zombie-task-recovery)
- [Queue Topology](#queue-topology)
- [Data Model](#data-model)
- [API Reference](#api-reference)
- [Running the Service](#running-the-service)
- [Moving to Production](#moving-to-production)
- [Project Structure](#project-structure)
- [Trade-offs & Design Decisions](#trade-offs--design-decisions)
- [Future Roadmap](#future-roadmap)

---

## Overview

| Concern | Solution |
|---------|----------|
| Decoupling | Caller gets `202 Accepted` immediately; delivery happens asynchronously |
| Reliability | Persistent queue (RabbitMQ) + manual ack; message survives worker crash |
| Retries | Exponential back-off via TTL + Dead Letter Exchange; 8 levels, up to ~14.6 hours |
| Overload protection | Per-domain circuit breaker in process memory (`sync.Map`) |
| Auditability | Every notification persisted to PostgreSQL with full status history |
| Scalability | Stateless; add worker instances freely — RabbitMQ distributes load automatically |

---

## Architecture

```
 Business System A ──┐
 Business System B ──┼──► ┌─────────────────────────┐
 Business System C ──┘    │        API Layer         │
                          │  validate → publish → 202 │
                          └────────────┬────────────-─┘
                                       │ publish (publisher confirm)
                          ┌────────────▼─────────────┐
                          │         RabbitMQ          │
                          │    (persistent queues)    │
                          └────────────┬─────────────┘
                                       │ consume (manual ack)
                          ┌────────────▼─────────────┐
                          │       Worker Pool         │
                          │  goroutine pool (n=100)   │
                          │  circuit breaker check    │
                          └──────────┬───────────────-┘
                                     │ HTTP call (30s timeout)
                          ┌──────────▼──────────────┐
                          │   External Vendor API    │
                          └──────────┬──────────────┘
                                     │
              ┌──────────────────────┼──────────────────────┐
              │                      │                       │
   ┌──────────▼──────────┐  ┌───────▼────────┐  ┌─────────▼──────────┐
   │     PostgreSQL       │  │  Callback URL  │  │  Dead-Letter Queue │
   │  (status + audit)   │  │  (async POST)  │  │  (retries exhausted)│
   └─────────────────────┘  └────────────────┘  └────────────────────┘
```

### Request Lifecycle

```
Caller                 API                 MQ              Worker            DB
  │                     │                   │                 │               │
  │──POST /notifications►│                   │                 │               │
  │                     │──validate──────────►                 │               │
  │                     │◄──ok───────────────                  │               │
  │                     │──store.Create()──────────────────────────────────────►│
  │                     │──mq.Publish()────►│                 │               │
  │◄──202 Accepted───────│                   │                 │               │
  │                     │                   │──consume────────►│               │
  │                     │                   │                 │──GET notif.───►│
  │                     │                   │                 │──HTTP call──►(vendor)
  │                     │                   │                 │──UPDATE status►│
  │                     │                   │                 │──ack───────────►
  │                     │                   │                 │──callback POST►(caller)
```

---

## Core Design

### At-Least-Once Delivery

RabbitMQ is used in **manual ack** mode. The worker only acknowledges a message after:

1. The HTTP call to the vendor succeeds (2xx), **and**
2. The notification status is updated in PostgreSQL.

If the worker crashes mid-flight, RabbitMQ redelivers the message. External vendors may therefore receive duplicate notifications. Callers are expected to implement idempotency on their end.

### Retry Strategy

HTTP response codes determine retry behavior:

| Status | Action |
|--------|--------|
| 2xx | Success — ack message |
| 429 / 5xx / timeout / connection error | Retry with exponential back-off |
| 4xx (except 429) | Fail immediately — retrying a bad request is pointless |

Back-off schedule (via RabbitMQ TTL queues):

| Retry # | Delay | Cumulative |
|---------|-------|-----------|
| 1 | 30 s | 30 s |
| 2 | 1 min | 1.5 min |
| 3 | 5 min | 6.5 min |
| 4 | 30 min | 36.5 min |
| 5 | 2 h | 2 h 36 min |
| 6 | 4 h | 6 h 36 min |
| 7 | 8 h | 14 h 36 min |
| 8 | — | → dead-letter |

### Circuit Breaker

One breaker per target domain, stored in process memory (`sync.Map`). No shared state across instances — each instance judges independently.

```
  Closed ──── failure rate > 80% (over ≥10 requests in 5 min) ────► Open
    ▲                                                                  │
    │                                                            60 s cooldown
    │                                                                  │
    └──── probe succeeds ◄──── Half-Open ◄────────────────────────────┘
```

While **Open**: messages for that domain are nacked and re-queued with the shortest delay (30 s). **No retry counter is consumed** — the circuit open period is not the vendor's fault.

While **Half-Open**: one probe request is allowed every 60 s. Success closes the breaker; failure reopens it.

**Why not Redis?** Circuit-breaking happens on every HTTP call and must be sub-millisecond. A Redis round-trip adds 1–5 ms per call under load, which is unacceptable at high concurrency. Per-instance state means breakers may differ slightly across instances, but that is an acceptable trade-off — one instance's network blip won't globally trip the breaker.

### Callback Notification

When a notification reaches a terminal state (`success` or `failed`) and the caller supplied a `callback_url`, the worker fires an async POST:

```json
{
  "notification_id": "ntf_550e8400-...",
  "status": "success",
  "target_url": "https://ads.example.com/conversion",
  "attempted_at": "2026-05-11T10:30:00Z",
  "retry_count": 2,
  "error": null
}
```

Callback retries: 3 attempts with delays of 0 s → 1 s → 5 s → 30 s. After that, the callback is abandoned. Callers can always poll `GET /api/v1/notifications/{id}` as a fallback.

### Zombie Task Recovery

A background goroutine runs every 5 minutes and scans for notifications stuck in `processing` for more than 10 minutes. These are reset to `pending` and re-enqueued, protecting against worker crashes that happen after the DB write but before the ack.

---

## Queue Topology

```
notification.exchange  (direct)
    │
    ├── notification.main          ← normal delivery; DLX → retry exchange on nack
    │
    ├── notification.retry.30s     ← TTL = 30 s  → back to main on expiry
    ├── notification.retry.1m      ← TTL = 1 min
    ├── notification.retry.5m      ← TTL = 5 min
    ├── notification.retry.30m     ← TTL = 30 min
    ├── notification.retry.2h      ← TTL = 2 h
    ├── notification.retry.4h      ← TTL = 4 h
    ├── notification.retry.8h      ← TTL = 8 h
    │
    └── notification.dead-letter   ← retries exhausted; manual intervention
```

All queues are **quorum queues** (RabbitMQ 3.8+): messages are replicated to at least 2 nodes before the publish is confirmed.

---

## Data Model

```sql
notifications (partitioned by created_at, monthly)
┌─────────────────┬──────────────┬──────────────────────────────────────┐
│ Column          │ Type         │ Notes                                │
├─────────────────┼──────────────┼──────────────────────────────────────┤
│ id              │ UUID         │ primary key                          │
│ target_url      │ TEXT         │ validated HTTPS, no RFC-1918         │
│ method          │ VARCHAR(10)  │ POST / PUT / PATCH only              │
│ headers         │ JSONB        │ max 8 KB                             │
│ body            │ TEXT         │ max 1 MB                             │
│ status          │ VARCHAR(20)  │ pending → processing → success/failed│
│ retry_count     │ INT          │ current attempt number               │
│ max_retries     │ INT          │ default 8                            │
│ next_retry_at   │ TIMESTAMPTZ  │ set on each retry                    │
│ callback_url    │ TEXT         │ optional caller webhook              │
│ callback_status │ VARCHAR(20)  │ pending / delivered / failed         │
│ last_http_status│ INT          │ last vendor response code            │
│ last_error      │ TEXT         │ last error message                   │
│ source_system   │ VARCHAR(100) │ caller identifier                    │
│ target_domain   │ VARCHAR(255) │ extracted from target_url            │
│ created_at      │ TIMESTAMPTZ  │ partition key                        │
│ updated_at      │ TIMESTAMPTZ  │ updated on every state change        │
│ completed_at    │ TIMESTAMPTZ  │ set when terminal state reached      │
└─────────────────┴──────────────┴──────────────────────────────────────┘
```

Key indexes: `(status, next_retry_at)`, `(target_domain, created_at)`, `(source_system, created_at)`.

---

## API Reference

### Error response format

All error responses share a single envelope:

```json
{ "error": "<human-readable message>" }
```

### Submit a Notification

```
POST /api/v1/notifications
Content-Type: application/json
```

```json
{
  "target_url":    "https://ads.example.com/conversion",
  "method":        "POST",
  "headers":       { "Authorization": "Bearer xxx" },
  "body":          "{\"event\":\"registration\"}",
  "callback_url":  "https://internal.example.com/hook",
  "source_system": "user-service"
}
```

**Validation rules:**

- `target_url`: must be `https://`, no localhost, no RFC-1918 addresses (SSRF protection)
- `method`: `POST`, `PUT`, or `PATCH` only (defaults to `POST` if omitted)
- `headers`: ≤ 8 KB total
- `body`: ≤ 1 MB

| Status | Body | When |
|--------|------|------|
| 202 Accepted | `{"notification_id": "ntf_...", "status": "accepted"}` | Persisted and enqueued |
| 400 Bad Request | `{"error": "invalid JSON: ..."}` | Malformed request body |
| 400 Bad Request | `{"error": "target_url must use https scheme"}` | Non-HTTPS URL |
| 400 Bad Request | `{"error": "target_url resolves to a private/loopback address"}` | SSRF-blocked URL |
| 400 Bad Request | `{"error": "method must be POST, PUT, or PATCH"}` | Unsupported HTTP method |
| 400 Bad Request | `{"error": "headers exceed 8192 bytes"}` | Oversized headers |
| 400 Bad Request | `{"error": "body exceeds 1048576 bytes"}` | Oversized body |
| 500 Internal Server Error | `{"error": "failed to persist notification"}` | Database write failure |
| 503 Service Unavailable | `{"error": "message queue unavailable"}` | MQ publish failure (record is safe in DB; zombie recovery will re-enqueue) |

---

### Get a Notification

```
GET /api/v1/notifications/{id}
```

| Status | Body | When |
|--------|------|------|
| 200 OK | Full notification object (see data model) | Found |
| 404 Not Found | `{"error": "notification ntf_... not found"}` | Unknown ID |

---

### List Notifications

```
GET /api/v1/notifications?status=failed&domain=ads.example.com&from=2026-05-01T00:00:00Z&to=2026-05-11T00:00:00Z&page=1&size=50
```

All query parameters are optional. `from` / `to` must be RFC 3339; invalid values are silently ignored.

| Status | Body | When |
|--------|------|------|
| 200 OK | `{"items": [...], "count": N}` | Always (empty list, not 404, when no results) |
| 500 Internal Server Error | `{"error": "query failed"}` | Database error |

---

### Manual Retry

```
POST /api/v1/notifications/{id}/retry
```

Resets `retry_count` to 0 and re-enqueues. Only allowed when `status = failed`.

| Status | Body | When |
|--------|------|------|
| 202 Accepted | `{"notification_id": "ntf_...", "status": "requeued"}` | Successfully requeued |
| 404 Not Found | `{"error": "notification ntf_... not found"}` | Unknown ID |
| 409 Conflict | `{"error": "only failed notifications can be retried"}` | Status is not `failed` |
| 500 Internal Server Error | `{"error": "failed to update notification"}` | Database write failure |
| 503 Service Unavailable | `{"error": "message queue unavailable"}` | MQ publish failure |

---

### Health Check

```
GET /health
→ 200 { "status": "ok" }
```

---

## Running the Service

### Mock mode (no external infra required)

All dependencies are replaced with in-memory implementations. This is the default for local development and testing.

```bash
# start server (API + Worker, mock DB + MQ) — INFO level, text logs
make run

# same but DEBUG level — traces every delivery attempt
make run-debug

# same but JSON-formatted logs — useful for piping to jq
make run-json

# or explicitly:
MOCK=true ROLE=all HTTP_ADDR=:8080 go run ./cmd/server
```

### Test scripts

With the server running on `:8080`:

```bash
# submit a notification
./scripts/test/submit.sh

# query by ID
./scripts/test/query.sh ntf_1234567890

# list failed notifications
./scripts/test/list.sh http://localhost:8080 failed

# manually retry a failed notification
./scripts/test/retry.sh ntf_1234567890
```

### Run tests

```bash
make test                          # all packages
make test-verbose                  # with -v
make test-pkg PKG=./internal/api   # single package
```

### Build binary

```bash
make build
# outputs bin/server
```

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MOCK` | `false` | `true` = in-memory fakes for DB and MQ |
| `ROLE` | `all` | `api`, `worker`, or `all` |
| `HTTP_ADDR` | `:8080` | HTTP listen address |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `LOG_FORMAT` | `text` | `text` (human-readable) or `json` (log aggregators) |
| `DB_DSN` | _(empty)_ | PostgreSQL DSN — required when `MOCK=false` |
| `MQ_URL` | _(empty)_ | RabbitMQ URL — required when `MOCK=false` |
| `NOTIFICATION_MAX_RETRIES` | `8` | Delivery attempts before dead-letter |
| `NOTIFICATION_PAGE_SIZE` | `50` | Default List page size |
| `WORKER_CONCURRENCY` | `100` | Goroutine pool size |
| `WORKER_HTTP_TIMEOUT` | `30s` | Per-call timeout for outbound HTTP |
| `WORKER_ZOMBIE_INTERVAL` | `5m` | How often zombie recovery sweeps |
| `WORKER_ZOMBIE_THRESHOLD` | `10` | Minutes stuck in "processing" before requeue |
| `SHUTDOWN_GRACE_PERIOD` | `30s` | Max wait for in-flight requests on SIGTERM |
| `CB_WINDOW` | `5m` | Circuit breaker sliding window |
| `CB_MIN_REQUESTS` | `10` | Minimum calls before tripping |
| `CB_FAILURE_RATIO` | `0.8` | Failure rate threshold (80%) |
| `CB_OPEN_DUR` | `60s` | Circuit open duration / probe interval |

### Deployment roles

The binary is a single artifact. Use the `ROLE` variable to control behavior:

```bash
# API only (for public-facing instances)
ROLE=api ./bin/server

# Worker only (for internal delivery instances, scalable independently)
ROLE=worker ./bin/server

# Both (single-instance / development)
ROLE=all ./bin/server
```

---

## Moving to Production

### What the mock layer is

`MOCK=true` replaces real infrastructure with two in-process implementations that satisfy the same interfaces:

| Interface | Mock | Real adapter location |
|-----------|------|-----------------------|
| `db.Store` | `internal/db/mock/store.go` — thread-safe in-memory map | `internal/db/postgres/` |
| `mq.Publisher` + `mq.Consumer` | `internal/mq/mock/mq.go` — channel-backed queue | `internal/mq/rabbitmq/` |

Every other package — API handlers, worker pool, circuit breaker — only ever sees these interfaces. Swapping to real infrastructure touches exactly **one place**: the wiring block in `cmd/server/main.go`.

### Code changes

Two adapter packages need to be implemented. Nothing else in the codebase changes.

**1. PostgreSQL — implement `db.Store`**

```
internal/db/postgres/store.go
```

```go
package postgres

type Store struct{ db *sql.DB }

func New(dsn string) (*Store, error) { ... }

// Implement all five methods of db.Store:
func (s *Store) Create(ctx context.Context, n *model.Notification) error { ... }
func (s *Store) Get(ctx context.Context, id string) (*model.Notification, error) { ... }
func (s *Store) Update(ctx context.Context, n *model.Notification) error { ... }
func (s *Store) List(ctx context.Context, f model.ListFilter) ([]*model.Notification, error) { ... }
func (s *Store) StuckProcessing(ctx context.Context, olderThan time.Duration) ([]*model.Notification, error) { ... }
```

**2. RabbitMQ — implement `mq.Publisher` and `mq.Consumer`**

```
internal/mq/rabbitmq/
```

Key implementation points:

- Declare quorum queues and the Dead Letter Exchange on startup (topology matches [Queue Topology](#queue-topology))
- `Publish` uses publisher confirms — do not return until the broker has confirmed the message
- `PublishRetry(level int)` routes to the appropriate TTL retry queue (`notification.retry.30s`, etc.)
- `Consume` runs in manual ack mode; call `Ack` on delivery success, `Nack(requeue=false)` to route through the DLX on failure

**3. Wire real adapters in `cmd/server/main.go`**

Replace the `os.Exit(1)` stub in the `else` branch:

```go
} else {
    pgStore, err := postgres.New(cfg.DB.DSN)
    if err != nil {
        slog.Error("failed to connect to PostgreSQL", "error", err)
        os.Exit(1)
    }
    store = pgStore

    rmq, err := rabbitmq.New(cfg.MQ.URL)
    if err != nil {
        slog.Error("failed to connect to RabbitMQ", "error", err)
        os.Exit(1)
    }
    pub, con = rmq, rmq
}
```

No other files need changes.

### Configuration changes

Set `MOCK=false` (or omit it) and supply connection credentials via environment variables:

```bash
MOCK=false \
DB_DSN="postgres://rc:password@pg-primary:5432/rc_keyork?sslmode=require" \
MQ_URL="amqp://rc:password@rmq-node1:5672/" \
ROLE=all \
LOG_LEVEL=info \
./bin/server
```

`DB_DSN` and `MQ_URL` have empty defaults intentionally — the service exits with an error if they are missing in non-mock mode.

### External infrastructure

#### PostgreSQL

| Concern | Recommendation |
|---------|----------------|
| HA / failover | 1 primary + ≥1 replica; use **Patroni** (self-hosted) or a managed service (RDS, Cloud SQL, AlloyDB) for automatic failover |
| Replication | **Synchronous** replication for zero-loss failover on primary crash; async replicas are acceptable for read-only audit queries |
| Partitioning | Monthly range partitions on `created_at` — create each partition before the month starts, or automate with **pg_partman** |
| Connection pooling | **PgBouncer** (transaction mode) in front of the primary reduces connection churn from the 100-goroutine worker pool |
| Reads vs. writes | All writes (`Create`, `Update`) and zombie recovery (`StuckProcessing`) must target the **primary**. `List` / `Get` queries for audit purposes can be routed to a replica by parameterising the DSN |

**Minimum production setup:** 1 primary + 1 synchronous standby + PgBouncer. Failover via Patroni + etcd (or cloud-managed automatic failover). Single-node PostgreSQL has no HA — avoid for production.

#### RabbitMQ

| Concern | Recommendation |
|---------|----------------|
| HA / quorum | Minimum **3 nodes** — quorum queues require a majority (≥2/3) to confirm a write; 2-node clusters cannot form quorum after any single node loss |
| Load balancing | HAProxy or a cloud load balancer in front of AMQP port 5672; clients reconnect automatically on node loss |
| Persistence | Quorum queues are durable by default — no extra configuration needed |
| Cluster formation | Use `rabbitmq_peer_discovery_k8s` (Kubernetes) or `rabbitmq_peer_discovery_etcd` for automatic cluster membership |
| Cloud-managed | **CloudAMQP** (RabbitMQ SaaS), **Amazon MQ for RabbitMQ**, or **Azure Service Bus** (AMQP 1.0 compatible) are all viable options |

**Minimum production setup:** 3-node cluster with quorum queues (the queue topology is already designed for this). A single-node RabbitMQ has no HA — do not use in production.

---

## Project Structure

```
rc_keyork/
├── cmd/server/main.go              # entrypoint: role flag, wiring, graceful shutdown
├── internal/
│   ├── config/config.go            # env-var configuration; logs WARN on invalid values
│   ├── logger/logger.go            # initialises log/slog (level + format) — called once from main
│   ├── model/notification.go       # domain types: Notification, SubmitRequest, DeliveryMessage
│   ├── db/
│   │   ├── store.go                # Store interface (Create/Get/Update/List/StuckProcessing)
│   │   ├── mock/store.go           # in-memory implementation (used in MOCK=true + tests)
│   │   └── postgres/               # real PostgreSQL adapter (placeholder)
│   ├── mq/
│   │   ├── mq.go                   # Publisher + Consumer interfaces
│   │   ├── mock/mq.go              # channel-backed implementation + test helpers
│   │   └── rabbitmq/               # real RabbitMQ adapter (placeholder)
│   ├── api/
│   │   ├── validator.go            # SSRF protection, method/header/body validation
│   │   ├── handler.go              # HTTP handlers (Submit, Get, List, Retry)
│   │   └── server.go               # ServeMux wiring
│   ├── worker/
│   │   └── worker.go               # Pool, delivery logic, callback, ZombieRecovery
│   └── circuitbreaker/
│       └── breaker.go              # per-domain breaker, sliding window, half-open probe
├── scripts/test/                   # shell scripts for manual API testing
├── docs/
│   ├── runbook.md                  # startup, expected logs, and error-scenario guide
│   └── technical_proposal.md      # original design document (Chinese)
└── Makefile
```

---

## Trade-offs & Design Decisions

### RabbitMQ over Kafka

This service is a **task dispatch** system (one consumer processes each message), not an event stream (multiple consumer groups each reading the same data). RabbitMQ's manual ack + Dead Letter Exchange natively supports retry chains and dead-lettering. Equivalent semantics in Kafka require building a custom retry-topic chain. RabbitMQ also lets you scale workers freely without being bound by partition count.

### In-process circuit breaker over Redis

The breaker is consulted on every HTTP call — a Redis round-trip at that point would add measurable latency under high concurrency. Each worker instance maintains independent state via `sync.Map`. The trade-off is that instances may be in different breaker states briefly, but a single instance's network blip cannot trip a global breaker and take down delivery for all workers.

### No vendor template management

The service accepts a complete, pre-assembled HTTP request (`url` + `method` + `headers` + `body`). Callers own the vendor-specific formatting. This keeps rc_keyork vendor-agnostic and means vendor API changes require zero changes to this service.

### No exactly-once delivery

Exactly-once requires external systems to cooperate on idempotency, which is outside this service's control. At-least-once with manual ack is the right abstraction boundary.

### No per-vendor rate limiting (v1)

Adding QPS limits per vendor increases configuration surface area and operational complexity. The circuit breaker already provides passive back-pressure when a vendor is struggling. Explicit rate limiting is planned for Phase 2 (see roadmap), triggered when 429 responses become frequent.

---

## Future Roadmap

| Phase | Trigger | Addition |
|-------|---------|----------|
| 1 — Hot/cold data split | Table > 50M rows, audit query P99 > 2 s | Archive records > 90 days to S3 (Parquet) + Athena |
| 2 — Per-vendor rate limiting | Frequent 429s or contractual QPS cap | Token-bucket per domain; optional Redis for global limit across instances |
| 3 — Multi-region | Geographic expansion or latency SLA | Independent RabbitMQ clusters per region; single PG primary with async read replicas |
| 4 — Notification priority | Differentiated delivery SLA | Split main queue into high/normal/low; weighted worker pool |
