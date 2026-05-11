# rc_keyork 运行手册 · Runbook

> 本文档覆盖：如何启动服务、每个接口的请求示例、预期的 API 响应、以及对应的服务端日志输出。所有示例均在 `MOCK=true` 模式下运行，无需外部基础设施。

---

## 一、启动服务

### 1.1 最简启动（Mock 模式）

```bash
make run
# 等价于：
MOCK=true ROLE=all HTTP_ADDR=:8080 go run ./cmd/server
```

**预期日志输出（INFO 级别，text 格式）：**

```
time=2026-05-11T10:00:00Z level=INFO msg="starting rc_keyork" role=all mock=true addr=:8080 log_level=info log_format=text
time=2026-05-11T10:00:00Z level=INFO msg="using in-memory mocks for DB and MQ" component=main
time=2026-05-11T10:00:00Z level=INFO msg="zombie recovery started" component=recovery interval=5m0s threshold_min=10
time=2026-05-11T10:00:00Z level=INFO msg="worker pool starting" component=worker concurrency=100 http_timeout=30s
time=2026-05-11T10:00:00Z level=INFO msg="API server listening" component=main addr=:8080
```

### 1.2 切换日志级别与格式

```bash
# 开启 DEBUG 级别（可见每条投递细节）
LOG_LEVEL=debug MOCK=true go run ./cmd/server

# JSON 格式（适合日志采集系统）
LOG_FORMAT=json MOCK=true go run ./cmd/server
```

**JSON 格式样例：**

```json
{"time":"2026-05-11T10:00:00Z","level":"INFO","msg":"starting rc_keyork","role":"all","mock":true,"addr":":8080","log_level":"info","log_format":"json"}
{"time":"2026-05-11T10:00:00Z","level":"INFO","msg":"API server listening","component":"main","addr":":8080"}
```

### 1.3 仅启动 API 或 Worker

```bash
ROLE=api  MOCK=true go run ./cmd/server   # 仅接受提交请求，不投递
ROLE=worker MOCK=true go run ./cmd/server # 仅消费队列，不暴露 HTTP
```

---

## 二、健康检查

```bash
curl -s http://localhost:8080/health
```

**响应 `200 OK`：**

```json
{"status": "ok"}
```

**无日志输出**（健康检查不记录日志，避免刷屏）。

---

## 三、提交通知

### 3.1 正常提交

```bash
curl -s -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{
    "target_url":    "https://httpbin.org/post",
    "method":        "POST",
    "headers":       {"Authorization": "Bearer test-token", "Content-Type": "application/json"},
    "body":          "{\"event\":\"purchase\",\"user_id\":\"u-001\"}",
    "callback_url":  "https://httpbin.org/post",
    "source_system": "order-service"
  }'
```

**响应 `202 Accepted`：**

```json
{
  "notification_id": "ntf_a3f8c2d1e4b56789...",
  "status": "accepted"
}
```

**服务端日志（INFO）：**

```
time=... level=INFO  msg="notification accepted" component=api notification_id=ntf_a3f8c2d1... target_domain=httpbin.org source_system=order-service
time=... level=INFO  msg="delivering notification" component=worker notification_id=ntf_a3f8c2d1... target_url=https://httpbin.org/post method=POST retry_count=0
time=... level=INFO  msg="notification delivered successfully" component=worker notification_id=ntf_a3f8c2d1... http_status=200 retry_count=0 domain=httpbin.org
```

**加上 DEBUG 级别后，额外可见：**

```
time=... level=DEBUG msg="submit request received" component=api source_system=order-service target_url=https://httpbin.org/post method=POST
time=... level=DEBUG msg="delivering notification" component=worker notification_id=ntf_a3f8c2d1... target_url=https://httpbin.org/post method=POST retry_count=0
```

### 3.2 提交时省略 method（自动默认为 POST）

```bash
curl -s -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{"target_url": "https://httpbin.org/post", "source_system": "test"}'
```

**响应 `202 Accepted`：**（同 3.1，method 字段自动填充为 POST）

---

## 四、校验拒绝场景

### 4.1 非 HTTPS 地址

```bash
curl -s -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{"target_url": "http://ads.example.com/cb", "method": "POST"}'
```

**响应 `400 Bad Request`：**

```json
{"error": "target_url must use HTTPS"}
```

**无服务端日志**（校验失败属于正常客户端行为，不需要记录）。

### 4.2 不允许的 HTTP method

```bash
curl -s -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{"target_url": "https://ads.example.com/cb", "method": "DELETE"}'
```

**响应 `400 Bad Request`：**

```json
{"error": "method must be POST, PUT or PATCH"}
```

### 4.3 内网地址（SSRF 防护）

```bash
curl -s -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d '{"target_url": "https://192.168.1.100/api", "method": "POST"}'
```

**响应 `400 Bad Request`：**

```json
{"error": "target_url resolves to a private/internal address"}
```

### 4.4 Body 超过 1MB

```bash
# 生成 1.1MB 的 body
python3 -c "print('x'*1150000)" | \
curl -s -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d "{\"target_url\":\"https://httpbin.org/post\",\"method\":\"POST\",\"body\":\"$(cat)\"}"
```

**响应 `400 Bad Request`：**

```json
{"error": "body exceeds 1MB limit"}
```

### 4.5 无效 JSON

```bash
curl -s -X POST http://localhost:8080/api/v1/notifications \
  -H "Content-Type: application/json" \
  -d 'not-json'
```

**响应 `400 Bad Request`：**

```json
{"error": "invalid JSON: invalid character 'o' in literal null (expecting 'u')"}
```

---

## 五、查询通知状态

### 5.1 查询单条（正常）

```bash
# 将 ntf_xxx 替换为提交时返回的 notification_id
curl -s http://localhost:8080/api/v1/notifications/ntf_a3f8c2d1e4b56789
```

**响应 `200 OK`（投递成功后）：**

```json
{
  "id": "ntf_a3f8c2d1e4b56789",
  "target_url": "https://httpbin.org/post",
  "method": "POST",
  "status": "success",
  "retry_count": 0,
  "max_retries": 8,
  "last_http_status": 200,
  "target_domain": "httpbin.org",
  "source_system": "order-service",
  "created_at": "2026-05-11T10:00:00Z",
  "updated_at": "2026-05-11T10:00:01Z",
  "completed_at": "2026-05-11T10:00:01Z"
}
```

**服务端日志（DEBUG 级别）：**

```
time=... level=DEBUG msg="get notification" component=api notification_id=ntf_a3f8c2d1...
```

### 5.2 查询不存在的 ID

```bash
curl -s http://localhost:8080/api/v1/notifications/ntf_does_not_exist
```

**响应 `404 Not Found`：**

```json
{"error": "notification ntf_does_not_exist not found"}
```

---

## 六、列表查询

### 6.1 查询全部（分页）

```bash
curl -s "http://localhost:8080/api/v1/notifications?page=1&size=10"
```

**响应 `200 OK`：**

```json
{
  "items": [
    { "id": "ntf_...", "status": "success", ... },
    { "id": "ntf_...", "status": "pending", ... }
  ],
  "count": 2
}
```

### 6.2 按状态过滤

```bash
curl -s "http://localhost:8080/api/v1/notifications?status=failed"
```

### 6.3 按域名过滤

```bash
curl -s "http://localhost:8080/api/v1/notifications?domain=httpbin.org"
```

### 6.4 按时间范围过滤

```bash
curl -s "http://localhost:8080/api/v1/notifications?from=2026-05-11T00:00:00Z&to=2026-05-11T23:59:59Z"
```

**服务端日志（DEBUG）：**

```
time=... level=DEBUG msg="list notifications" component=api status=failed domain= page=1 size=50
```

---

## 七、投递失败与重试

### 7.1 模拟 5xx 失败（使用返回 500 的测试目标）

在测试中，worker 遇到 5xx 时会自动安排重试。日志如下：

```
time=... level=INFO  msg="scheduling retry" component=worker notification_id=ntf_... retry_level=1 delay=30s last_http_status=500
time=... level=INFO  msg="scheduling retry" component=worker notification_id=ntf_... retry_level=2 delay=1m0s last_http_status=500
...
time=... level=WARN  msg="retries exhausted, moving to dead-letter" component=worker notification_id=ntf_... retry_count=8 max_retries=8 last_http_status=500
time=... level=WARN  msg="notification delivery failed permanently" component=worker notification_id=ntf_... retry_count=8 last_http_status=500
```

### 7.2 非重试错误（4xx）

遇到 400/401/403/404 等 4xx 响应，直接标记失败，不重试：

```
time=... level=WARN  msg="notification delivery failed permanently" component=worker notification_id=ntf_... retry_count=0 last_http_status=400
```

---

## 八、手动重发

只有 `status=failed` 的通知可以手动重发。

```bash
curl -s -X POST http://localhost:8080/api/v1/notifications/ntf_a3f8c2d1.../retry
```

**响应 `202 Accepted`：**

```json
{
  "notification_id": "ntf_a3f8c2d1...",
  "status": "requeued"
}
```

**服务端日志：**

```
time=... level=INFO msg="notification requeued via manual retry" component=api notification_id=ntf_a3f8c2d1...
time=... level=INFO msg="delivering notification" component=worker notification_id=ntf_a3f8c2d1... retry_count=0
```

### 对非 failed 状态手动重发（应被拒绝）

```bash
curl -s -X POST http://localhost:8080/api/v1/notifications/ntf_pending.../retry
```

**响应 `409 Conflict`：**

```json
{"error": "only failed notifications can be retried"}
```

**服务端日志（WARN）：**

```
time=... level=WARN msg="retry rejected: wrong status" component=api notification_id=ntf_pending... current_status=pending
```

---

## 九、熔断器行为

当同一域名在 5 分钟内失败率超过 80%（且至少有 10 次请求）时触发熔断：

**触发时（WARN）：**

```
time=... level=WARN msg="circuit opened" component=circuitbreaker failure_ratio=0.9 failures=9 total=10 open_until=2026-05-11T10:01:00Z
```

**熔断期间收到该域名的通知（INFO）：**

```
time=... level=INFO msg="circuit open — requeueing without consuming retry slot" component=worker notification_id=ntf_... domain=bad-vendor.com
```

**探测请求放行（DEBUG）：**

```
time=... level=DEBUG msg="circuit half-open: probe allowed" component=circuitbreaker
```

**探测成功，熔断关闭（INFO）：**

```
time=... level=INFO msg="circuit closed: probe succeeded" component=circuitbreaker
```

**探测失败，重新打开（WARN）：**

```
time=... level=WARN msg="circuit re-opened: probe failed" component=circuitbreaker
```

---

## 十、僵死任务回收

当 Worker 在写入 `processing` 后崩溃，下次扫描（默认 5 分钟）会恢复它：

**正常情况（DEBUG，无僵尸）：**

```
time=... level=DEBUG msg="zombie sweep started" component=recovery threshold_min=10
time=... level=DEBUG msg="zombie sweep complete — no zombies found" component=recovery
```

**发现僵尸（INFO）：**

```
time=... level=INFO msg="zombie sweep found stuck notifications" component=recovery count=2
time=... level=INFO msg="zombie requeued" component=recovery notification_id=ntf_abc... retry_count=1
time=... level=INFO msg="zombie requeued" component=recovery notification_id=ntf_def... retry_count=3
```

---

## 十一、回调通知

投递到达终态后，若提交时有 `callback_url`，Worker 异步回调：

**回调成功（INFO）：**

```
time=... level=INFO msg="callback delivered" component=worker notification_id=ntf_... attempt=1 http_status=200
```

**回调失败，重试（WARN）：**

```
time=... level=WARN msg="callback attempt failed" component=worker notification_id=ntf_... attempt=1 http_status=503
time=... level=WARN msg="callback attempt failed" component=worker notification_id=ntf_... attempt=2 error="connection refused"
time=... level=WARN msg="all callback attempts exhausted" component=worker notification_id=ntf_... callback_url=https://...
```

**DEBUG 可见每次尝试：**

```
time=... level=DEBUG msg="sending callback" component=worker notification_id=ntf_... callback_url=https://... attempt=1
```

---

## 十二、优雅退出

发送 SIGTERM（或按 Ctrl+C）触发优雅退出，等待正在执行的 HTTP 调用完成后退出：

```
time=... level=INFO msg="shutdown signal received" component=main signal=interrupt
time=... level=INFO msg="initiating graceful shutdown" component=main grace_period=30s
time=... level=INFO msg="worker pool stopped" component=worker
time=... level=INFO msg="zombie recovery stopped" component=recovery
time=... level=INFO msg="shutdown complete" component=main
```

---

## 十三、环境变量一览

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MOCK` | `false` | `true` = 使用内存实现 |
| `ROLE` | `all` | `api` / `worker` / `all` |
| `HTTP_ADDR` | `:8080` | 监听地址 |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `LOG_FORMAT` | `text` | `text`（本地开发）/ `json`（生产采集）|
| `NOTIFICATION_MAX_RETRIES` | `8` | 最大重试次数 |
| `NOTIFICATION_PAGE_SIZE` | `50` | List 接口默认分页大小 |
| `WORKER_CONCURRENCY` | `100` | Goroutine pool 大小 |
| `WORKER_HTTP_TIMEOUT` | `30s` | 单次 HTTP 调用超时 |
| `WORKER_ZOMBIE_INTERVAL` | `5m` | 僵死任务扫描间隔 |
| `WORKER_ZOMBIE_THRESHOLD` | `10` | 判定僵死的分钟数 |
| `SHUTDOWN_GRACE_PERIOD` | `30s` | 优雅退出最长等待时间 |
| `CB_WINDOW` | `5m` | 熔断滑动窗口时长 |
| `CB_MIN_REQUESTS` | `10` | 触发熔断的最少请求数 |
| `CB_FAILURE_RATIO` | `0.8` | 触发熔断的失败率阈值 |
| `CB_OPEN_DUR` | `60s` | 熔断持续时间 / 探测间隔 |

---

## 十四、日志级别选择指南

| 场景 | 推荐级别 |
|------|---------|
| 本地开发，逐条追踪投递 | `DEBUG` |
| 生产正常运行 | `INFO` |
| 生产降低日志量（仅关注异常） | `WARN` |
| 排查 DB / MQ 连接问题 | `ERROR` |

```bash
# 生产推荐配置
LOG_LEVEL=info LOG_FORMAT=json MOCK=false ROLE=all ./bin/server
```

---

## 十五、脚本快捷方式

```bash
./scripts/test/submit.sh              # 提交一条通知
./scripts/test/query.sh <id>          # 查询指定通知
./scripts/test/list.sh "" failed      # 列出所有失败通知
./scripts/test/retry.sh <id>          # 手动重发
```
