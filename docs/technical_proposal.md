# 外部通知投递服务 — 技术设计文档

## 1. 背景与目标

企业内部多个业务系统在关键事件发生时，需要调用外部供应商提供的 HTTP(S) API 进行通知。不同供应商的请求地址、Header、Body 格式各不相同，业务系统不关心外部 API 的返回值，只需确保通知被可靠送达。

本服务的定位是：**统一的异步通知投递网关**。业务系统提交完整的 HTTP 请求描述，由本服务负责可靠投递、失败重试、结果回调和审计记录。

## 2. 系统边界

### 2.1 本系统解决的问题

- 接收业务系统提交的通知请求，持久化后立即返回，解耦业务系统与外部 API 的可用性
- 可靠投递：重试、指数退避、死信处理
- 目标域名级别的熔断保护，防止单个供应商故障拖垮整个系统
- 投递结果回调通知业务方
- 通知记录的持久化存储与审计查询
- 高可用部署，无单点故障

### 2.2 本系统不解决的问题

| 不解决的问题 | 理由 |
|---|---|
| 供应商 API 的注册与模板管理 | 每个供应商格式不同且变化频繁，由业务方自行组装请求更灵活，避免本系统耦合供应商业务逻辑 |
| 精确一次投递（exactly-once） | 需要外部系统配合做幂等校验，超出本系统控制范围。本系统保证至少一次（at-least-once） |
| 外部 API 返回值的解析和业务处理 | 业务方明确不关心返回值内容，本系统只关注 HTTP 状态码判断成功/失败 |
| 业务方提交请求的鉴权与权限管理 | 属于 API Gateway 层的职责，本系统假设上游已完成身份认证 |

## 3. 整体架构

```
                         ┌─────────────┐
                         │  业务系统 A  │
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
  业务系统 B ──────────►  │   API 层    │  ◄────── 业务系统 C
                         │  (校验+入队) │
                         └──────┬──────┘
                                │ publish
                         ┌──────▼──────┐
                         │  RabbitMQ   │
                         │  (持久化队列)│
                         └──────┬──────┘
                                │ consume
                         ┌──────▼──────┐
                         │  Worker 池  │──── HTTP ────► 外部供应商 API
                         │  (并发投递)  │
                         └──────┬──────┘
                                │
                    ┌───────────┼───────────┐
                    │           │           │
             ┌──────▼──────┐ ┌─▼─────┐ ┌──▼──────────┐
             │ PostgreSQL  │ │ 回调   │ │ 死信队列     │
             │ (状态+审计) │ │ 业务方 │ │ (最终失败)   │
             └─────────────┘ └───────┘ └─────────────┘
```

### 3.1 请求流程

1. 业务系统通过 HTTP POST 提交通知请求（完整的 URL、method、headers、body），可选传入 `callback_url`
2. API 层校验请求合法性，通过后将消息写入 RabbitMQ 持久化队列，立即返回 `202 Accepted` + `notification_id`
3. Worker 从 RabbitMQ 消费消息，在 PostgreSQL 中创建通知记录（status=processing），执行 HTTP 调用
4. 调用成功（2xx）：更新状态为 `success`，ack 消息
5. 调用失败：根据失败类型决定重试或标记失败，利用 RabbitMQ 的延迟重投机制实现指数退避
6. 最终失败（重试耗尽）：消息进入死信队列，状态标记为 `failed`，触发告警
7. 如果业务方提供了 `callback_url`，在终态（success / failed）时异步回调通知结果
8. 所有通知记录持久化到 PostgreSQL 归档表，支持审计查询

## 4. 核心设计

### 4.1 投递语义：至少一次（At-Least-Once）

RabbitMQ 使用 manual ack 模式。Worker 只有在确认 HTTP 调用成功且 DB 状态更新完成后才 ack 消息。如果 Worker 在处理过程中崩溃，消息会被 RabbitMQ 重新投递给其他 Worker。

这意味着外部系统可能收到重复通知。由于精确一次需要外部系统配合实现幂等（超出本系统控制范围），我们选择不在本系统层面解决去重问题。

### 4.2 重试策略

采用指数退避，利用 RabbitMQ 的 TTL + DLX 机制实现延迟重投：

| 重试次数 | 延迟 | 累计时间 |
|---------|------|---------|
| 第 1 次 | 30 秒 | 30 秒 |
| 第 2 次 | 1 分钟 | 1.5 分钟 |
| 第 3 次 | 5 分钟 | 6.5 分钟 |
| 第 4 次 | 30 分钟 | 36.5 分钟 |
| 第 5 次 | 2 小时 | 2 小时 36 分钟 |
| 第 6 次 | 4 小时 | 6 小时 36 分钟 |
| 第 7 次 | 8 小时 | 14 小时 36 分钟 |
| 第 8 次（最终） | — | 标记 failed |

HTTP 状态码处理规则：

- **2xx** → 成功，ack 消息
- **429 / 5xx / 超时 / 连接失败** → 进入重试流程
- **4xx（非 429）** → 不重试，直接标记 `failed`（请求本身有问题，重试无意义）

### 4.3 熔断机制

按目标域名维护熔断状态，防止单个供应商故障消耗 Worker 资源。

**状态机：**

```
Closed（正常） ──── 失败率超阈值 ────► Open（熔断）
     ▲                                    │
     │                              60 秒冷却期
     │                                    │
     └──── 探测成功 ◄──── Half-Open（探测）◄┘
```

**触发条件：** 滑动窗口（5 分钟），同一域名请求 10 次以上且失败率超过 80%。

**熔断期间的行为：** 该域名的消息被跳过（nack 后延迟重投），不消耗重试次数。每 60 秒放行一个探测请求，成功则关闭熔断。

**存储方式：** 进程内存（`sync.Map`）。多实例各自维护独立的熔断状态，不共享。理由：熔断是实时性判断，走网络查询延迟太高；各实例独立判断更健壮，单个实例的网络异常不会误触发全局熔断。

### 4.4 回调通知

业务方在提交通知时可携带 `callback_url`。当通知到达终态（success 或 failed），Worker 向该 URL 发送 POST 请求：

```json
{
  "notification_id": "ntf_550e8400-e29b-41d4-a716-446655440000",
  "status": "success",
  "target_url": "https://ads.example.com/callback",
  "attempted_at": "2026-05-11T10:30:00Z",
  "retry_count": 2,
  "error": null
}
```

回调本身最多重试 3 次（1s / 5s / 30s），失败后放弃。业务方可通过查询 API 主动拉取状态作为兜底。不对回调使用与主通知相同的重试机制，避免递归复杂度。

### 4.5 审计与查询

所有通知记录持久化到 PostgreSQL，按月分区（基于 `created_at`）。

提供以下查询接口：

- `GET /api/v1/notifications/{id}` — 查询单条通知状态与详情
- `GET /api/v1/notifications?status=failed&domain=ads.example.com&from=...&to=...` — 按条件查询
- `POST /api/v1/notifications/{id}/retry` — 手动触发重发（仅限 failed 状态）

审计数据保留策略：热数据（近 3 个月）保留在主表分区，冷数据可归档或清理（第一版手动操作，未来可自动化）。

## 5. 数据模型

### 5.1 notifications 表

```sql
CREATE TABLE notifications (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- 请求内容
    target_url      TEXT NOT NULL,
    method          VARCHAR(10) NOT NULL DEFAULT 'POST',
    headers         JSONB NOT NULL DEFAULT '{}',
    body            TEXT,
    -- 状态
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',
    retry_count     INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 8,
    next_retry_at   TIMESTAMPTZ,
    -- 回调
    callback_url    TEXT,
    callback_status VARCHAR(20),
    -- 结果
    last_http_status INT,
    last_error       TEXT,
    -- 元数据
    source_system   VARCHAR(100),
    target_domain   VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
) PARTITION BY RANGE (created_at);

-- 按月创建分区（示例）
CREATE TABLE notifications_2026_05 PARTITION OF notifications
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

-- 核心索引
CREATE INDEX idx_notifications_status ON notifications (status, next_retry_at);
CREATE INDEX idx_notifications_domain ON notifications (target_domain, created_at);
CREATE INDEX idx_notifications_source ON notifications (source_system, created_at);
```

`status` 取值流转：`pending` → `processing` → `success` / `failed`

## 6. API 设计

### 6.1 提交通知

```
POST /api/v1/notifications
Content-Type: application/json

{
  "target_url": "https://ads.example.com/conversion",
  "method": "POST",
  "headers": {
    "Authorization": "Bearer xxx",
    "Content-Type": "application/json"
  },
  "body": "{\"event\":\"registration\",\"user_id\":\"12345\"}",
  "callback_url": "https://internal.example.com/notify-result",
  "source_system": "user-service"
}
```

响应：

```
HTTP/1.1 202 Accepted

{
  "notification_id": "ntf_550e8400-e29b-41d4-a716-446655440000",
  "status": "accepted"
}
```

### 6.2 入口校验规则

- `target_url` 必须是合法的 HTTPS URL，禁止 HTTP、localhost、内网地址（防 SSRF）
- `method` 仅允许 POST / PUT / PATCH
- `headers` 大小不超过 8KB
- `body` 大小不超过 1MB
- 校验失败返回 `400 Bad Request`，不入队

### 6.3 查询通知

```
GET /api/v1/notifications/{id}
```

```
GET /api/v1/notifications?status=failed&domain=ads.example.com&from=2026-05-01&to=2026-05-11&page=1&size=50
```

### 6.4 手动重发

```
POST /api/v1/notifications/{id}/retry
```

仅限 `status = failed` 的通知，重置 `retry_count` 后重新入队。

## 7. RabbitMQ 队列设计

```
Exchange: notification.exchange (direct)
    │
    ├── Queue: notification.main         ← 正常投递
    │       死信 → notification.retry.exchange
    │
    ├── Queue: notification.retry.30s    ← TTL=30s，过期后回 main
    ├── Queue: notification.retry.1m     ← TTL=1m
    ├── Queue: notification.retry.5m     ← TTL=5m
    ├── Queue: notification.retry.30m    ← TTL=30m
    ├── Queue: notification.retry.2h     ← TTL=2h
    ├── Queue: notification.retry.4h     ← TTL=4h
    ├── Queue: notification.retry.8h     ← TTL=8h
    │
    └── Queue: notification.dead-letter  ← 最终失败（重试耗尽）
```

所有队列使用 quorum queue（RabbitMQ 3.8+），保证消息在集群内多副本持久化。

重试机制利用 RabbitMQ 原生的 TTL + Dead Letter Exchange：消息 nack 后根据当前重试次数路由到对应的延迟队列，TTL 到期后自动回到 main 队列重新消费。

## 8. 高可用设计

### 8.1 服务层

- 无状态服务，多实例部署（最少 2 个），前面挂负载均衡
- API 角色和 Worker 角色在同一个二进制中，通过启动参数控制；可混合部署，也可分开部署独立扩缩
- 优雅关闭：收到 SIGTERM 后停止从 RabbitMQ 拉取新消息，等待当前正在执行的 HTTP 调用完成（最多等 30 秒），再退出

### 8.2 RabbitMQ

- 3 节点集群，使用 quorum queue，消息至少写入 2 个节点才确认
- 生产者使用 publisher confirm 模式，确保消息成功写入后才返回 202
- 如果 RabbitMQ 不可用，API 层返回 `503 Service Unavailable`，业务方需自行重试

### 8.3 PostgreSQL

- 主从复制 + 自动 failover（Patroni 或云托管方案）
- 读查询（审计查询）可走从库，写操作走主库
- DB 短暂不可用时，Worker 暂停消费（nack 消息不 commit），等 DB 恢复后自动继续

### 8.4 僵死任务回收

定时任务（每 5 分钟）扫描 `status = 'processing' AND updated_at < now() - interval '10 minutes'` 的记录，重置为 `pending` 并重新入队。防止 Worker 拿到任务后崩溃导致任务永远卡住。

## 9. 并发与性能

### 9.1 Worker 并发模型

- 每个 Worker 实例运行固定大小的 goroutine pool（默认 100，可配置）
- RabbitMQ prefetch count 与 pool 大小一致，避免拉取过多消息堆积在内存
- 每个 HTTP 调用设 30 秒超时（connect 5s + read 25s）

### 9.2 背压控制

熔断机制天然提供背压能力：当某个域名响应慢或不可用时，该域名的消息被延迟重投，Worker 资源释放给其他健康域名使用。

### 9.3 扩容策略

- 水平扩展：增加 Worker 实例，RabbitMQ consumer 自动分摊消息
- 垂直扩展：调大单实例的 goroutine pool size
- 监控指标驱动：根据队列积压深度和 Worker 利用率决定是否扩容

## 10. 监控与告警

### 10.1 核心指标

| 指标 | 采集方式 |
|------|---------|
| 队列积压深度 | RabbitMQ Prometheus exporter |
| 投递成功率（按域名） | 应用内 Prometheus metrics |
| 平均投递延迟 | 应用内（created_at 到 completed_at 的差值） |
| 重试率 | 应用内（retry_count > 0 的比例） |
| 死信队列深度 | RabbitMQ Prometheus exporter |
| Worker 活跃数 / 空闲数 | 应用内 metrics |
| 熔断状态变更 | 应用日志 + metrics |

### 10.2 告警规则

| 告警 | 条件 |
|------|------|
| 队列积压 | 超过 10000 条持续 5 分钟 |
| 域名熔断 | 任意域名触发熔断 |
| 死信堆积 | 死信队列有新消息 |
| 成功率下降 | 5 分钟窗口内成功率低于 95% |
| Worker 饱和 | 全部 Worker 繁忙持续 2 分钟 |

## 11. 技术栈总览

| 组件 | 选型 | 理由 |
|------|------|------|
| 语言 | Go | 并发原生支持好，单二进制部署简单，HTTP 调用性能优秀 |
| 消息队列 | RabbitMQ | 任务队列模式天然匹配，原生 ack + DLX 支持重试与死信，运维成本低于 Kafka |
| 数据库 | PostgreSQL | 分区表支持审计数据管理，成熟稳定 |
| 监控 | Prometheus + Grafana | Go metrics 支持好，业界标准方案 |

## 12. 设计取舍说明

### 12.1 RabbitMQ 而非 Kafka

本系统是任务分发模式（一条消息由一个 consumer 处理），不是事件流模式（多消费者组各自消费同一份数据）。RabbitMQ 的 manual ack + Dead Letter Exchange 天然匹配重试和死信需求；Kafka 实现同等语义需要自建 retry topic 链路，复杂度更高。此外 RabbitMQ 的 consumer 模型允许灵活扩缩 worker 数量，不受 partition 数量约束。

### 12.2 进程内存做熔断而非 Redis

熔断是每次 HTTP 调用前的实时判断，走网络查询延迟不可接受。多实例各自维护独立状态，不共享——单个实例的网络抖动不会误触发全局熔断。代价是各实例熔断判断可能不完全一致，但在实际运行中这种不一致是可接受的。

### 12.3 选择不做的

- **供应商 API 模板管理：** 增加系统耦合度，每次供应商变更都要改本系统配置。让业务方自行组装请求更灵活
- **精确一次投递：** 需要外部系统配合做幂等，超出本系统控制范围
- **按供应商限 QPS：** 增加配置管理复杂度，如果未来有明确需求再加

### 12.4 未来演进方向

#### 阶段一：审计数据冷热分离（预计日通知量突破 50 万时启动）

**触发信号：** PostgreSQL notifications 表总量超过 5000 万行，审计查询 P99 延迟超过 2 秒。

**方案：**

```
PostgreSQL（热数据，近 3 个月）
        │
        │ 定时归档任务（每日凌晨）
        ▼
   S3（Parquet 格式，按 year/month/day 分目录）
        │
        ▼
   Athena（按需查询冷数据）
```

具体实施：

1. 新增一个归档 Worker（Go CronJob 或独立定时任务），每天凌晨扫描 `created_at < now() - interval '90 days'` 的记录
2. 按天导出为 Parquet 文件（使用 Go 的 parquet-go 库），上传到 S3，路径格式 `s3://notification-archive/{year}/{month}/{day}/`
3. 导出成功后删除 PG 中对应分区的数据（`DROP` 过期的月分区表）
4. 在 AWS Athena 上建外部表指向 S3 路径，运维和审计人员通过 Athena SQL 查询历史数据
5. 查询 API 层增加逻辑：先查 PG，如果时间范围超出热数据窗口，返回提示引导用户去 Athena 查询（或后期做透明代理）

**分区管理自动化：** 配合归档任务，新增分区自动创建脚本（提前创建未来 3 个月的分区），避免手动维护。

#### 阶段二：供应商级别限流（预计接入供应商超过 20 个、或任一供应商明确要求 QPS 上限时启动）

**触发信号：** 收到供应商的 429 响应显著增多，或供应商合同中明确约定调用频率上限。

**方案：**

```
Worker 拿到消息
    │
    ▼
查询域名限流器（令牌桶）
    │
    ├── 有令牌 → 执行 HTTP 调用
    │
    └── 无令牌 → nack 消息，延迟重投（短延迟 1-5s）
```

具体实施：

1. 新增限流配置表 `rate_limit_config`：

```sql
CREATE TABLE rate_limit_config (
    domain          VARCHAR(255) PRIMARY KEY,
    max_qps         INT NOT NULL,           -- 每秒最大请求数
    burst_size      INT NOT NULL,           -- 突发容量
    enabled         BOOLEAN DEFAULT true,
    updated_at      TIMESTAMPTZ DEFAULT now()
);
```

1. 使用 Go 标准库 `golang.org/x/time/rate` 实现令牌桶，每个域名一个 `rate.Limiter` 实例，存在进程内存中（`sync.Map`）
2. Worker 消费消息时，先检查目标域名是否配置了限流。如果配置了且当前无可用令牌，nack 消息并设置短延迟（1-5 秒）重投
3. 配置变更通过 Admin API 更新数据库，Worker 定时（每 30 秒）从 DB 刷新配置到内存
4. 多实例场景下，各实例独立限流。实际总 QPS = 单实例 QPS × 实例数，所以配置值需要按 `max_qps / 实例数` 设置，或引入 Redis 做集中式限流（见下文）

**进阶（实例数不固定时）：** 如果实例数动态变化（如 K8s HPA 自动扩缩），本地令牌桶无法精确控制全局 QPS。此时引入 Redis，使用 Redis + Lua 脚本实现集中式滑动窗口限流：

```lua
-- Redis Lua: 滑动窗口限流
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count < limit then
    redis.call('ZADD', key, now, now .. '-' .. math.random(1000000))
    redis.call('EXPIRE', key, window)
    return 1  -- 允许
end
return 0  -- 拒绝
```

#### 阶段三：多区域部署（预计业务扩展到多地域、或对投递延迟有区域性要求时启动）

**触发信号：** 业务系统或目标供应商分布在多个地理区域，跨区域网络延迟影响投递时效。

**方案：**

```
           Region A (us-east)                    Region B (eu-west)
    ┌─────────────────────────┐          ┌─────────────────────────┐
    │  API + Worker           │          │  API + Worker           │
    │  RabbitMQ Cluster (3节点)│          │  RabbitMQ Cluster (3节点)│
    │  PostgreSQL Primary     │◄── 异步 ──►│  PostgreSQL Read Replica│
    └─────────────────────────┘  复制    └─────────────────────────┘
```

具体实施：

1. **服务层：** 每个区域独立部署 API + Worker，前面挂全局负载均衡（如 AWS Global Accelerator 或 Cloudflare LB），按请求来源就近路由
2. **RabbitMQ：** 各区域独立集群，不做跨区域复制。消息在本区域生产、本区域消费。理由是 RabbitMQ 的跨区域 shovel/federation 延迟高且复杂，通知消息没有跨区域消费的需求
3. **PostgreSQL：** 主库在 Region A，Region B 部署异步只读副本。Region B 的审计查询走本地副本；写入仍然回主库（跨区域写延迟可接受，因为 DB 写入不在投递关键路径上——消息先写 RabbitMQ 再异步落 DB）
4. **路由规则：** 如果某个供应商的 API 端点在特定区域，可以通过目标域名 DNS 解析结果判断最优出口区域。第一步简化处理：业务方在提交通知时指定 `region` 字段，全局 LB 按此路由

**未采用 Active-Active 双写的原因：** 双写 PostgreSQL 需要处理冲突解决（同一个 notification_id 可能在两个区域被写入），引入的复杂度远超收益。通知服务的写入模式是"一次创建 + 有限次更新"，单主写入延迟在异步投递场景中完全可接受。

#### 阶段四：通知优先级（预计业务方对投递时效有差异化需求时启动）

**触发信号：** 不同业务场景对投递时效要求不同（如支付通知需要秒级，营销通知可以分钟级），当前 FIFO 队列无法满足差异化 SLA。

**方案：**

```
API 层
  │
  ├── priority=high   → notification.main.high   (prefetch 更高)
  ├── priority=normal  → notification.main.normal
  └── priority=low     → notification.main.low
```

具体实施：

1. 提交通知 API 增加 `priority` 字段（high / normal / low，默认 normal）
2. RabbitMQ 不使用原生 priority queue（性能问题已知），而是拆分为 3 个独立队列
3. Worker 按权重消费：high 队列的 prefetch count 设为 normal 的 2 倍，low 的 4 倍。Worker 内部维护多个 consumer，高优先级 consumer 数量更多
4. 监控维度增加 priority 标签，告警规则按优先级分别设置（high 队列积压 > 100 即告警，low 队列积压 > 10000 才告警）
5. 重试队列同样按优先级拆分，high 的重试间隔可以缩短（如 10s → 30s → 2m → 10m → 1h，更激进的退避策略）

**数据模型变更：** notifications 表增加 `priority VARCHAR(10) DEFAULT 'normal'`，审计查询支持按优先级过滤。

#### 演进节奏总结

| 阶段 | 触发条件 | 新增组件 | 预估工作量 |
|------|---------|---------|-----------|
| 一：冷热分离 | 数据量 > 5000 万行 | S3 + Athena + 归档 CronJob | 1-2 周 |
| 二：供应商限流 | 供应商 429 增多或合同要求 | 限流配置表 + 令牌桶（可选 Redis） | 1 周（本地）/ 2 周（Redis 集中式） |
| 三：多区域部署 | 多地域业务扩展 | 区域级 RabbitMQ 集群 + PG 副本 + 全局 LB | 3-4 周 |
| 四：通知优先级 | 差异化 SLA 需求 | 优先级队列拆分 + 权重消费 | 1-2 周 |

各阶段相互独立，可根据实际业务需求选择性实施，不要求按顺序推进。
