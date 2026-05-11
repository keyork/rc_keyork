# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
make run          # start with INFO text logs (no external infra needed), listens on :8080
make run-debug    # same but LOG_LEVEL=debug — traces every delivery attempt
make run-json     # same but LOG_FORMAT=json — useful for piping to jq
make test         # run all tests
make test-verbose # run all tests with -v
make test-pkg PKG=./internal/api   # single package
make build        # compile to bin/server
make lint         # go vet ./...
make tidy         # go mod tidy
```

**Shell test scripts** (server must be running):
```bash
./scripts/test/submit.sh              # POST a notification, prints notification_id
./scripts/test/query.sh <id>          # GET single notification
./scripts/test/list.sh "" failed      # list by status filter
./scripts/test/retry.sh <id>          # POST /retry on a failed notification
```

**Key environment variables:**

| Var | Default | Purpose |
|-----|---------|---------|
| `MOCK` | `false` | `true` → in-memory DB+MQ, no external infra |
| `ROLE` | `all` | `api`, `worker`, or `all` |
| `HTTP_ADDR` | `:8080` | listen address |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `LOG_FORMAT` | `text` | `text` (human) or `json` (log aggregators) |
| `NOTIFICATION_MAX_RETRIES` | `8` | delivery attempts before dead-letter |
| `NOTIFICATION_PAGE_SIZE` | `50` | default List page size |
| `WORKER_CONCURRENCY` | `100` | goroutine pool size |
| `WORKER_HTTP_TIMEOUT` | `30s` | per-call timeout for outbound HTTP |
| `WORKER_ZOMBIE_INTERVAL` | `5m` | how often zombie recovery sweeps |
| `WORKER_ZOMBIE_THRESHOLD` | `10` | minutes stuck in "processing" before requeue |
| `SHUTDOWN_GRACE_PERIOD` | `30s` | max wait for in-flight calls on SIGTERM |
| `CB_WINDOW` | `5m` | circuit-breaker sliding window |
| `CB_MIN_REQUESTS` | `10` | min calls before breaker can trip |
| `CB_FAILURE_RATIO` | `0.8` | failure rate threshold (80%) |
| `CB_OPEN_DUR` | `60s` | breaker open duration / probe interval |

Full operational guide (startup, expected logs, error scenarios): `docs/runbook.md`.

---

## Project Overview

`rc_keyork` is an **async HTTP notification delivery gateway** written in Go. Business systems submit fully-formed HTTP requests; this service reliably delivers them to external vendor APIs with retry, circuit-breaking, callback, and audit capabilities.

- Full technical design (Chinese): `docs/technical_proposal.md`
- Operations runbook with log samples: `docs/runbook.md`

## Tech Stack

| Component | Choice | Notes |
|-----------|--------|-------|
| Language | Go 1.25 | `log/slog` for structured leveled logging |
| Message queue | RabbitMQ | quorum queues, TTL + DLX for retry chains |
| Database | PostgreSQL | range-partitioned by `created_at` (monthly) |
| Mock mode | in-memory fakes | `MOCK=true` — no external infra needed |

## Package Structure

```
cmd/server/main.go              # entrypoint: wires deps, starts subsystems, graceful shutdown
internal/
  config/config.go              # env-var config; logs WARN on invalid values
  logger/logger.go              # initialises log/slog (level + format) — call once from main
  model/notification.go         # domain types: Notification, SubmitRequest, DeliveryMessage
  db/
    store.go                    # Store interface (Create/Get/Update/List/StuckProcessing)
    mock/store.go               # thread-safe in-memory impl; List returns sorted, non-nil slice
    postgres/                   # real adapter placeholder
  mq/
    mq.go                       # Publisher + Consumer interfaces
    mock/mq.go                  # channel-backed impl; PublishRetry records but doesn't re-enqueue
    rabbitmq/                   # real adapter placeholder
  api/
    validator.go                # SSRF protection, method/header/body validation
    handler.go                  # HTTP handlers (Submit, Get, List, Retry, Health)
    server.go                   # ServeMux route wiring
  worker/
    worker.go                   # Pool (delivery), ZombieRecovery, callback, retry routing
  circuitbreaker/
    breaker.go                  # per-domain breaker, sliding window, half-open probe; logs state changes
```

## Architecture

```
Business system → POST /api/v1/notifications
                        │
                   API layer (validate + enqueue)
                        │ publisher-confirm
                   RabbitMQ main queue
                        │ manual ack
                   Worker pool (goroutine pool, default 100)
                   circuit-breaker check (per domain, in-process)
                        │ HTTP call (30s timeout)
                   External vendor API
                        │
              ┌─────────┼──────────┐
         PostgreSQL   callback   dead-letter queue
         (status+audit)  URL     (retries exhausted)
```

**Single binary**, role controlled by `ROLE` env var.

## Key Design Decisions

**Retry** — exponential backoff via RabbitMQ TTL queues:
- 8 levels: 30s → 1m → 5m → 30m → 2h → 4h → 8h → dead-letter
- 2xx → success; 429/5xx/network error → retry; 4xx (non-429) → fail immediately

**Circuit breaker** — per target domain, in-process `sync.Map`. Sliding 5-min window, trips at >80% failure rate over ≥10 requests. Half-open probe every 60s. Instances don't share state — avoids network latency and cross-instance false trips. Logs `WARN` on trip, `INFO` on recovery.

**At-least-once delivery** — RabbitMQ manual ack; worker acks only after HTTP success + DB update. Callers must be idempotent.

**SSRF protection** — `target_url` must be valid HTTPS; localhost and RFC-1918 ranges blocked at API layer.

**Zombie recovery** — background goroutine sweeps every 5 min for records stuck in `processing` >10 min, resets to `pending`, re-enqueues. Protects against worker crash between DB write and MQ ack.

**ID generation** — `crypto/rand` (128-bit hex), not time-based, to avoid collisions under concurrent requests.

**Logging** — `log/slog`, structured key=value (text) or JSON. All log lines carry `component` field (`api`, `worker`, `recovery`, `circuitbreaker`). Level guide: `DEBUG` = per-delivery traces; `INFO` = normal ops; `WARN` = recoverable problems; `ERROR` = data-loss risk.

## Common Pitfalls

- **`pathSegment(path, n)`** — paths are split after `strings.Trim(path, "/")`, so `/api/v1/notifications/{id}` has the ID at index **3**, not 4. The Retry handler shares the same index.
- **Mock MQ `PublishRetry`** — does NOT re-enqueue immediately (by design, for deterministic tests). Call `queue.Requeue()` explicitly in tests that need retry re-delivery.
- **`NewZombieRecovery`** takes a full `worker.Config`; call `SetInterval` only in tests to override the sweep cadence.
- **`NewHandler`** requires `HandlerConfig{MaxRetries, DefaultPageSize}`; zero values are replaced with safe defaults (8 and 50).
- **Real adapters** (`internal/db/postgres`, `internal/mq/rabbitmq`) are placeholder directories — `MOCK=false` calls `os.Exit(1)`.

## Future Phases

1. **Hot/cold data split** — archive records >90 days to S3 (Parquet) + Athena (trigger: table >50M rows)
2. **Per-vendor rate limiting** — token bucket per domain; Redis central limiter for dynamic scaling
3. **Multi-region** — independent RabbitMQ clusters per region; single PG primary with async read replicas
4. **Notification priority** — split main queue into high/normal/low with weighted worker pool
