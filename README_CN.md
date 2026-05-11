# rc_keyork

一个基于 Go 实现的可靠异步 HTTP 通知投递网关。

业务系统向本服务提交完整的 HTTP 请求描述，rc_keyork 负责持久化、投递到外部供应商 API、失败时指数退避重试、按目标域名熔断保护，并在投递完成后可选地回调通知提交方——全程无需调用方保持在线。

> **设计说明**（问题理解、架构设计、工程取舍、AI 使用说明）：[`docs/report.md`](docs/report.md)

---

## 目录

- [系统概述](#系统概述)
- [整体架构](#整体架构)
- [核心设计](#核心设计)
  - [至少一次投递](#至少一次投递)
  - [重试策略](#重试策略)
  - [熔断机制](#熔断机制)
  - [结果回调](#结果回调)
  - [僵死任务回收](#僵死任务回收)
- [RabbitMQ 队列拓扑](#rabbitmq-队列拓扑)
- [数据模型](#数据模型)
- [API 接口](#api-接口)
- [运行方式](#运行方式)
- [接入生产环境](#接入生产环境)
- [项目结构](#项目结构)
- [设计取舍](#设计取舍)
- [未来演进](#未来演进)

---

## 系统概述

| 问题 | 解决方案 |
|------|---------|
| 解耦 | 调用方立即收到 `202 Accepted`，投递在后台异步进行 |
| 可靠性 | RabbitMQ 持久化队列 + manual ack，Worker 崩溃不丢消息 |
| 重试 | 指数退避，通过 TTL + Dead Letter Exchange 实现；8 个层级，最长约 14.6 小时 |
| 过载保护 | 进程内按域名维护熔断器（`sync.Map`），不依赖外部存储 |
| 可审计 | 每条通知持久化到 PostgreSQL，记录完整状态流转 |
| 可扩展 | 无状态服务，直接水平扩展 Worker；RabbitMQ 自动分摊消息 |

---

## 整体架构

```
 业务系统 A ──┐
 业务系统 B ──┼──► ┌──────────────────────────┐
 业务系统 C ──┘    │         API 层            │
                   │  校验 → 入队 → 202 返回   │
                   └───────────┬──────────────┘
                               │ publish（publisher confirm 模式）
                   ┌───────────▼──────────────┐
                   │        RabbitMQ           │
                   │      （持久化队列）         │
                   └───────────┬──────────────┘
                               │ consume（manual ack 模式）
                   ┌───────────▼──────────────┐
                   │        Worker 池          │
                   │  goroutine pool（n=100）  │
                   │     熔断检查              │
                   └──────────┬───────────────┘
                              │ HTTP 调用（30s 超时）
                   ┌──────────▼──────────────┐
                   │     外部供应商 API        │
                   └──────────┬──────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                     │
┌────────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
│   PostgreSQL    │  │   回调 URL       │  │   死信队列       │
│  （状态 + 审计）│  │  （异步 POST）   │  │ （重试耗尽后）   │
└─────────────────┘  └─────────────────┘  └─────────────────┘
```

### 请求生命周期

```
调用方              API 层              消息队列          Worker            数据库
  │                   │                    │                │                │
  │──POST /notif.─────►│                    │                │                │
  │                   │──校验──────────────►                 │                │
  │                   │◄──通过──────────────                 │                │
  │                   │──写入 DB──────────────────────────────────────────────►│
  │                   │──发布消息──────────►│                │                │
  │◄──202 Accepted────│                    │                │                │
  │                   │                    │──消费消息───────►│                │
  │                   │                    │                │──读取通知────────►│
  │                   │                    │                │──HTTP 调用──────►（供应商）
  │                   │                    │                │──更新状态────────►│
  │                   │                    │                │──ack────────────►
  │                   │                    │                │──回调 POST──────►（调用方）
```

---

## 核心设计

### 至少一次投递

RabbitMQ 使用 **manual ack** 模式。Worker 仅在以下两个条件同时满足后才 ack 消息：

1. 对供应商的 HTTP 调用成功（2xx）
2. PostgreSQL 中通知状态已更新完成

Worker 崩溃时，RabbitMQ 会重新投递消息，外部供应商可能收到重复通知。这是系统边界内的合理选择——精确一次投递需要外部系统配合实现幂等，超出本服务的控制范围。

### 重试策略

HTTP 响应码决定重试行为：

| 状态码 | 处理方式 |
|--------|---------|
| 2xx | 成功，ack 消息 |
| 429 / 5xx / 超时 / 连接失败 | 进入重试流程（指数退避） |
| 4xx（非 429） | 直接标记失败，不重试（请求本身有问题，重试无意义） |

退避时间表（通过 RabbitMQ TTL 队列实现）：

| 重试次数 | 延迟 | 累计时间 |
|---------|------|---------|
| 第 1 次 | 30 秒 | 30 秒 |
| 第 2 次 | 1 分钟 | 1.5 分钟 |
| 第 3 次 | 5 分钟 | 6.5 分钟 |
| 第 4 次 | 30 分钟 | 36.5 分钟 |
| 第 5 次 | 2 小时 | 2 小时 36 分 |
| 第 6 次 | 4 小时 | 6 小时 36 分 |
| 第 7 次 | 8 小时 | 14 小时 36 分 |
| 第 8 次 | — | 进入死信队列 |

### 熔断机制

按目标域名维护熔断器，存储在进程内存（`sync.Map`）。多实例之间不共享熔断状态，各自独立判断。

```
  Closed（正常）──── 失败率 > 80%（5 分钟内 ≥10 次请求）────► Open（熔断）
       ▲                                                            │
       │                                                       60 秒冷却期
       │                                                            │
       └──── 探测成功 ◄──── Half-Open（半开）◄─────────────────────┘
```

**Open 状态时**：该域名的消息被 nack 后以最短延迟（30 秒）重新入队，**不消耗重试次数**——域名熔断不应被计为供应商的投递失败。

**Half-Open 状态时**：每 60 秒放行一个探测请求。探测成功则关闭熔断，失败则重新进入 Open 状态。

**为何不用 Redis？** 熔断判断发生在每次 HTTP 调用之前，对延迟极为敏感。Redis 网络查询在高并发下会增加 1–5 ms 的额外开销，不可接受。进程内 `sync.Map` 的代价是多实例熔断状态可能短暂不一致，但这在实际运行中完全可接受——单个实例的网络抖动不会误触发全局熔断。

### 结果回调

调用方提交通知时可以携带 `callback_url`。当通知到达终态（`success` 或 `failed`）时，Worker 向该 URL 异步发送 POST：

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

回调最多重试 3 次，延迟依次为 0 s → 1 s → 5 s → 30 s。超出后放弃，调用方可通过 `GET /api/v1/notifications/{id}` 主动查询兜底。回调不使用与主通知相同的重试机制，避免引入递归复杂度。

### 僵死任务回收

后台定时任务每 5 分钟扫描一次，查找 `status = processing` 且 `updated_at < now() - 10 分钟` 的通知，将其重置为 `pending` 并重新入队。这是对 Worker 在写入 DB 之后、ack 之前崩溃场景的兜底保护。

---

## RabbitMQ 队列拓扑

```
notification.exchange（direct）
    │
    ├── notification.main          ← 正常投递；nack 时死信到 retry exchange
    │
    ├── notification.retry.30s     ← TTL=30s，到期后回 main
    ├── notification.retry.1m      ← TTL=1m
    ├── notification.retry.5m      ← TTL=5m
    ├── notification.retry.30m     ← TTL=30m
    ├── notification.retry.2h      ← TTL=2h
    ├── notification.retry.4h      ← TTL=4h
    ├── notification.retry.8h      ← TTL=8h
    │
    └── notification.dead-letter   ← 重试耗尽，等待人工介入
```

所有队列使用 **quorum queue**（RabbitMQ 3.8+）：消息写入至少 2 个节点后才确认 publish。

---

## 数据模型

```sql
notifications（按 created_at 月度分区）
┌──────────────────┬──────────────┬──────────────────────────────────────┐
│ 字段             │ 类型         │ 说明                                 │
├──────────────────┼──────────────┼──────────────────────────────────────┤
│ id               │ UUID         │ 主键                                 │
│ target_url       │ TEXT         │ 必须为 HTTPS，禁止内网地址（防 SSRF）│
│ method           │ VARCHAR(10)  │ 仅允许 POST / PUT / PATCH            │
│ headers          │ JSONB        │ 总大小 ≤ 8KB                         │
│ body             │ TEXT         │ 大小 ≤ 1MB                           │
│ status           │ VARCHAR(20)  │ pending → processing → success/failed│
│ retry_count      │ INT          │ 当前已重试次数                        │
│ max_retries      │ INT          │ 默认 8                               │
│ next_retry_at    │ TIMESTAMPTZ  │ 每次重试时设置                        │
│ callback_url     │ TEXT         │ 可选，调用方回调地址                  │
│ callback_status  │ VARCHAR(20)  │ pending / delivered / failed         │
│ last_http_status │ INT          │ 最近一次供应商响应码                  │
│ last_error       │ TEXT         │ 最近一次错误信息                      │
│ source_system    │ VARCHAR(100) │ 来源系统标识                          │
│ target_domain    │ VARCHAR(255) │ 从 target_url 提取，用于熔断和索引    │
│ created_at       │ TIMESTAMPTZ  │ 分区键                               │
│ updated_at       │ TIMESTAMPTZ  │ 每次状态变更时更新                    │
│ completed_at     │ TIMESTAMPTZ  │ 到达终态时设置                        │
└──────────────────┴──────────────┴──────────────────────────────────────┘
```

核心索引：`(status, next_retry_at)`、`(target_domain, created_at)`、`(source_system, created_at)`。

---

## API 接口

### 错误响应格式

所有错误响应统一使用以下结构：

```json
{ "error": "<可读的错误描述>" }
```

### 提交通知

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

**校验规则：**

- `target_url`：必须为 `https://`，禁止 localhost、RFC-1918 内网地址（防 SSRF 攻击）
- `method`：仅允许 `POST`、`PUT`、`PATCH`（不填默认 `POST`）
- `headers`：总大小 ≤ 8KB
- `body`：大小 ≤ 1MB

| 状态码 | 响应 Body | 触发条件 |
|--------|-----------|---------|
| 202 Accepted | `{"notification_id": "ntf_...", "status": "accepted"}` | 持久化并入队成功 |
| 400 Bad Request | `{"error": "invalid JSON: ..."}` | 请求体 JSON 解析失败 |
| 400 Bad Request | `{"error": "target_url must use https scheme"}` | 非 HTTPS 地址 |
| 400 Bad Request | `{"error": "target_url resolves to a private/loopback address"}` | SSRF 拦截（内网/回环地址） |
| 400 Bad Request | `{"error": "method must be POST, PUT, or PATCH"}` | 不支持的 HTTP 方法 |
| 400 Bad Request | `{"error": "headers exceed 8192 bytes"}` | headers 超过 8KB |
| 400 Bad Request | `{"error": "body exceeds 1048576 bytes"}` | body 超过 1MB |
| 500 Internal Server Error | `{"error": "failed to persist notification"}` | 数据库写入失败 |
| 503 Service Unavailable | `{"error": "message queue unavailable"}` | MQ 发布失败（记录已落库，僵死回收会补投） |

---

### 查询单条通知

```
GET /api/v1/notifications/{id}
```

| 状态码 | 响应 Body | 触发条件 |
|--------|-----------|---------|
| 200 OK | 完整通知对象（字段见数据模型） | 查询成功 |
| 404 Not Found | `{"error": "notification ntf_... not found"}` | ID 不存在 |

---

### 条件查询

```
GET /api/v1/notifications?status=failed&domain=ads.example.com&from=2026-05-01T00:00:00Z&to=2026-05-11T00:00:00Z&page=1&size=50
```

所有查询参数均为可选。`from` / `to` 须为 RFC 3339 格式，格式非法时静默忽略。

| 状态码 | 响应 Body | 触发条件 |
|--------|-----------|---------|
| 200 OK | `{"items": [...], "count": N}` | 始终返回（无结果时为空列表，不返回 404） |
| 500 Internal Server Error | `{"error": "query failed"}` | 数据库查询失败 |

---

### 手动重发

```
POST /api/v1/notifications/{id}/retry
```

将 `retry_count` 重置为 0 后重新入队。仅允许对 `status = failed` 的通知操作。

| 状态码 | 响应 Body | 触发条件 |
|--------|-----------|---------|
| 202 Accepted | `{"notification_id": "ntf_...", "status": "requeued"}` | 重新入队成功 |
| 404 Not Found | `{"error": "notification ntf_... not found"}` | ID 不存在 |
| 409 Conflict | `{"error": "only failed notifications can be retried"}` | 状态不是 `failed` |
| 500 Internal Server Error | `{"error": "failed to update notification"}` | 数据库写入失败 |
| 503 Service Unavailable | `{"error": "message queue unavailable"}` | MQ 发布失败 |

---

### 健康检查

```
GET /health
→ 200 { "status": "ok" }
```

---

## 运行方式

### Mock 模式（无需任何外部基础设施）

所有外部依赖（RabbitMQ、PostgreSQL）均替换为进程内内存实现。本地开发和测试时的默认模式。

```bash
# 启动服务（API + Worker，使用 mock DB 和 MQ），INFO 级别文本日志
make run

# 同上，但启用 DEBUG 级别——可追踪每次投递细节
make run-debug

# 同上，但输出 JSON 格式日志——便于 jq 解析
make run-json

# 或手动指定：
MOCK=true ROLE=all HTTP_ADDR=:8080 go run ./cmd/server
```

### 测试脚本

服务启动后（默认监听 `:8080`）：

```bash
# 提交一条通知
./scripts/test/submit.sh

# 按 ID 查询
./scripts/test/query.sh ntf_1234567890

# 按状态列出
./scripts/test/list.sh http://localhost:8080 failed

# 手动重发失败通知
./scripts/test/retry.sh ntf_1234567890
```

### 运行测试

```bash
make test                            # 全量测试
make test-verbose                    # -v 详细输出
make test-pkg PKG=./internal/api     # 单包测试
```

### 编译二进制

```bash
make build
# 输出到 bin/server
```

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MOCK` | `false` | `true` = 使用进程内内存实现替代真实 DB 和 MQ |
| `ROLE` | `all` | `api`、`worker` 或 `all` |
| `HTTP_ADDR` | `:8080` | HTTP 监听地址 |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `LOG_FORMAT` | `text` | `text`（人类可读）或 `json`（日志聚合系统） |
| `DB_DSN` | _（空）_ | PostgreSQL 连接串——`MOCK=false` 时必须设置 |
| `MQ_URL` | _（空）_ | RabbitMQ 连接地址——`MOCK=false` 时必须设置 |
| `NOTIFICATION_MAX_RETRIES` | `8` | 进入死信前的最大投递次数 |
| `NOTIFICATION_PAGE_SIZE` | `50` | List 接口默认分页大小 |
| `WORKER_CONCURRENCY` | `100` | goroutine pool 大小 |
| `WORKER_HTTP_TIMEOUT` | `30s` | 单次出站 HTTP 调用超时 |
| `WORKER_ZOMBIE_INTERVAL` | `5m` | 僵死任务回收扫描间隔 |
| `WORKER_ZOMBIE_THRESHOLD` | `10` | 卡在 processing 超过多少分钟视为僵死 |
| `SHUTDOWN_GRACE_PERIOD` | `30s` | SIGTERM 后等待在途请求完成的最长时间 |
| `CB_WINDOW` | `5m` | 熔断滑动窗口时长 |
| `CB_MIN_REQUESTS` | `10` | 触发熔断的最小请求数 |
| `CB_FAILURE_RATIO` | `0.8` | 触发熔断的失败率阈值（80%） |
| `CB_OPEN_DUR` | `60s` | 熔断持续时间 / 半开探测间隔 |

### 部署角色分离

二进制文件是单一产物，通过 `ROLE` 变量控制行为：

```bash
# 仅 API（面向公网的实例）
ROLE=api ./bin/server

# 仅 Worker（内网投递实例，可独立扩缩容）
ROLE=worker ./bin/server

# API + Worker 合并（单机 / 开发环境）
ROLE=all ./bin/server
```

---

## 接入生产环境

### Mock 层的作用

`MOCK=true` 将真实基础设施替换为两个进程内实现，它们实现完全相同的接口：

| 接口 | Mock 实现 | 真实适配器位置 |
|------|-----------|---------------|
| `db.Store` | `internal/db/mock/store.go` — 线程安全内存 Map | `internal/db/postgres/` |
| `mq.Publisher` + `mq.Consumer` | `internal/mq/mock/mq.go` — channel 实现的队列 | `internal/mq/rabbitmq/` |

其他所有代码——API 处理器、Worker 池、熔断器——只依赖这两个接口，从不感知具体实现。切换到真实基础设施只需修改**一处**：`cmd/server/main.go` 中的依赖注入块。

### 代码层面

需要实现两个适配器包，其他代码无需改动。

**1. PostgreSQL — 实现 `db.Store`**

```
internal/db/postgres/store.go
```

```go
package postgres

type Store struct{ db *sql.DB }

func New(dsn string) (*Store, error) { ... }

// 实现 db.Store 的全部五个方法：
func (s *Store) Create(ctx context.Context, n *model.Notification) error { ... }
func (s *Store) Get(ctx context.Context, id string) (*model.Notification, error) { ... }
func (s *Store) Update(ctx context.Context, n *model.Notification) error { ... }
func (s *Store) List(ctx context.Context, f model.ListFilter) ([]*model.Notification, error) { ... }
func (s *Store) StuckProcessing(ctx context.Context, olderThan time.Duration) ([]*model.Notification, error) { ... }
```

**2. RabbitMQ — 实现 `mq.Publisher` 和 `mq.Consumer`**

```
internal/mq/rabbitmq/
```

关键实现要点：

- 启动时声明 quorum queue 和 Dead Letter Exchange（拓扑结构见[队列拓扑](#rabbitmq-队列拓扑)）
- `Publish` 使用 publisher confirm 模式——broker 确认后才返回
- `PublishRetry(level int)` 根据重试层级路由到对应 TTL 队列（`notification.retry.30s` 等）
- `Consume` 使用 manual ack 模式：投递成功调用 `Ack`，失败调用 `Nack(requeue=false)` 通过 DLX 路由

**3. 修改 `cmd/server/main.go` 的依赖注入块**

将 `else` 分支中的 `os.Exit(1)` 占位替换为真实适配器初始化：

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

其他文件无需任何改动。

### 配置层面

将 `MOCK` 设为 `false`（或直接去掉），并通过环境变量提供连接信息：

```bash
MOCK=false \
DB_DSN="postgres://rc:password@pg-primary:5432/rc_keyork?sslmode=require" \
MQ_URL="amqp://rc:password@rmq-node1:5672/" \
ROLE=all \
LOG_LEVEL=info \
./bin/server
```

`DB_DSN` 和 `MQ_URL` 默认为空字符串是有意为之——非 Mock 模式下若未设置，服务启动时会报错退出。

### 外部设施层面

#### PostgreSQL

| 关注点 | 建议 |
|--------|------|
| 高可用 / 故障转移 | 1 主 + ≥1 从；使用 **Patroni**（自建）或云托管服务（RDS、Cloud SQL、AlloyDB）做自动故障切换 |
| 复制模式 | **同步复制**保障主库宕机时零数据丢失；审计查询用的只读副本可用异步复制 |
| 分区维护 | 按 `created_at` 月度 Range 分区——每月提前创建下一个分区，或用 **pg_partman** 自动化 |
| 连接池 | 主库前部署 **PgBouncer**（transaction 模式），避免 100 个 goroutine 直连导致连接数爆炸 |
| 读写分离 | 写操作（`Create`、`Update`）及僵死任务扫描（`StuckProcessing`）必须指向**主库**；`List` / `Get` 等审计查询可通过参数化 DSN 路由到只读副本 |

**最小生产配置：** 1 主 + 1 同步从 + PgBouncer，故障切换通过 Patroni + etcd 或云平台托管自动完成。单节点 PostgreSQL 无高可用，生产环境不可用。

#### RabbitMQ

| 关注点 | 建议 |
|--------|------|
| 高可用 / Quorum | 最少 **3 节点**——quorum queue 的 Write 确认需要多数（≥2/3）节点响应；2 节点集群任意一个宕机后无法形成 quorum |
| 负载均衡 | 在 AMQP 5672 端口前置 HAProxy 或云负载均衡；客户端节点故障时自动重连 |
| 持久化 | quorum queue 默认持久化，无需额外配置 |
| 集群自动组建 | Kubernetes 下使用 `rabbitmq_peer_discovery_k8s`，自建环境使用 `rabbitmq_peer_discovery_etcd` |
| 云托管选项 | **CloudAMQP**（RabbitMQ SaaS）、**Amazon MQ for RabbitMQ**、**Azure Service Bus**（兼容 AMQP 1.0）均可 |

**最小生产配置：** 3 节点集群 + quorum queue（队列拓扑已按此设计）。单节点 RabbitMQ 无高可用，生产环境不可用。

---

## 项目结构

```
rc_keyork/
├── cmd/server/main.go              # 入口：角色参数、依赖注入、优雅退出
├── internal/
│   ├── config/config.go            # 环境变量配置；非法值记录 WARN 日志
│   ├── logger/logger.go            # 初始化 log/slog（级别 + 格式）——仅在 main 调用一次
│   ├── model/notification.go       # 领域类型：Notification、SubmitRequest、DeliveryMessage
│   ├── db/
│   │   ├── store.go                # Store 接口（Create/Get/Update/List/StuckProcessing）
│   │   ├── mock/store.go           # 内存实现（MOCK=true 及测试使用）
│   │   └── postgres/               # PostgreSQL 真实适配器（预留）
│   ├── mq/
│   │   ├── mq.go                   # Publisher + Consumer 接口
│   │   ├── mock/mq.go              # channel 实现 + 测试辅助方法
│   │   └── rabbitmq/               # RabbitMQ 真实适配器（预留）
│   ├── api/
│   │   ├── validator.go            # SSRF 防护、method/headers/body 校验
│   │   ├── handler.go              # HTTP 处理器（Submit、Get、List、Retry）
│   │   └── server.go               # ServeMux 路由注册
│   ├── worker/
│   │   └── worker.go               # Pool（投递）、回调、ZombieRecovery
│   └── circuitbreaker/
│       └── breaker.go              # 按域名熔断，滑动窗口，半开探测
├── scripts/test/                   # 手动测试 shell 脚本
├── docs/
│   ├── runbook.md                  # 启动流程、预期日志及异常场景处理指南
│   └── technical_proposal.md      # 原始技术设计文档
└── Makefile
```

---

## 设计取舍

### RabbitMQ 而非 Kafka

本系统是**任务分发**模式（一条消息由一个 Consumer 处理），而非事件流模式（多消费组各自消费同一份数据）。RabbitMQ 原生支持 manual ack + Dead Letter Exchange，天然适配重试链路和死信处理。用 Kafka 实现等价语义需要自建 retry topic 链，复杂度显著更高。此外 RabbitMQ 的 Consumer 数量与 partition 数量无关，Worker 扩缩容更自由。

### 进程内熔断而非 Redis

熔断判断在每次 HTTP 调用前同步执行，对延迟极为敏感。Redis 查询在高并发下会增加 1–5 ms 的额外开销，不可接受。各实例独立维护熔断状态的代价是：不同实例可能短暂处于不同状态。这在实践中完全可接受——单个实例的网络抖动不会通过全局熔断器影响其他实例的投递。

### 不做供应商 API 模板管理

服务接受完整的 HTTP 请求描述（`url` + `method` + `headers` + `body`），由调用方自行组装供应商格式。这让 rc_keyork 对供应商完全无感知，供应商 API 变更无需改动本服务。

### 不做精确一次投递

精确一次需要外部系统配合实现幂等校验，超出本服务的控制边界。manual ack + 至少一次是合理的抽象边界。

### v1 不做供应商级别限流

显式 QPS 限制会增加配置管理复杂度。熔断机制已经为过载的供应商提供了被动背压。限流将在 Phase 2 根据 429 响应频率触发时按需引入。

---

## 未来演进

| 阶段 | 触发条件 | 新增内容 |
|------|---------|---------|
| 一：冷热分离 | 表总量 > 5000 万行，审计查询 P99 > 2s | 90 天前数据归档至 S3（Parquet 格式），Athena 按需查询 |
| 二：供应商限流 | 429 响应频繁或合同限制 QPS | 按域名令牌桶；实例数动态变化时可引入 Redis 做集中式限流 |
| 三：多区域部署 | 业务地域扩张或有区域延迟 SLA 要求 | 各区域独立 RabbitMQ 集群；PG 单主 + 异步只读副本 |
| 四：通知优先级 | 不同业务场景对投递时效有差异化要求 | 主队列拆分为 high/normal/low；Worker 按权重消费 |
