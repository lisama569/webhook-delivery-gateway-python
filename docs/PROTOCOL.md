# 投递协议、幂等、重试与版本兼容说明

本服务是一个**面向第三方合作方的事件通知接入与投递网关**，全部能力可在本地
运行与验证（内存存储 + 可编排 Mock 传输层），不依赖外部服务、真实凭据或付费资源。

---

## 1. 总体模型

```
合作方系统 ──(签名的入站事件)──▶ [接入层]
                                   │  ① 租户认证/验签
                                   │  ② 事件级幂等（并发安全）
                                   │  ③ 租户独立配额
                                   │  ④ 契约版本/字段兼容校验
                                   ▼
                              事件存储（按租户隔离）
                                   │  按订阅扇出（fan-out）
                                   ▼
                        每订阅一条 FIFO 队列（顺序保证）
                                   │  HMAC 签名投递 + 指数退避重试
                                   ▼
                     接收方回调 ── 成功 / 重试 / 死信（可查询）
```

- **入站**：合作方把业务事件签名后 POST 到 `/v1/events/inbound`。
- **出站**：网关按订阅把事件签名后 POST 到合作方回调地址；失败重试，
  超过上限进入死信链路（DLQ），绝不静默丢失。

---

## 2. 签名与认证协议

### 2.1 请求头

| 头 | 含义 |
|---|---|
| `X-Tenant-Id` | 租户标识 |
| `X-Key-Id` | 使用的凭据 ID（支持同一租户多把凭据） |
| `X-Timestamp` | Unix 秒级时间戳（防重放，允许 ±300s 偏移） |
| `X-Signature` | 十六进制小写 HMAC-SHA256 |

### 2.2 签名算法

```
canonical = X-Timestamp + "." + 原始请求体字节
signature = HMAC_SHA256(secret, canonical) 的 hex 小写
```

签名覆盖的是**原始字节**，因此验签必须在解析 body 之前完成。

### 2.3 判定结论（机器可读，互不混淆）

| 判定依据 basis | HTTP | FailReason | 含义 |
|---|---|---|---|
| `OK` | 202/200 | `NONE` | 验签通过 |
| `KEY_NOT_FOUND` | 401 | `INVALID_SIGNATURE` | key_id 在该租户下不存在 |
| `CREDENTIAL_NOT_YET_VALID` | 401 | `INVALID_SIGNATURE` | 凭据 `not_before` 未到 |
| `SIGNATURE_MISMATCH` | 401 | `INVALID_SIGNATURE` | HMAC 不匹配（含报文被篡改） |
| `STALE_TIMESTAMP` / `BAD_TIMESTAMP` | 401 | `STALE_TIMESTAMP` | 时间戳超出 ±300s 或非法 |
| `EXPIRED_CREDENTIAL` | 401 | `EXPIRED_CREDENTIAL` | 凭据 `not_after` 已过，**明确拒绝** |

### 2.4 凭据轮换

- 每把凭据含 `not_before` / `not_after` 时间窗；轮换时让新旧凭据的时间窗
  **重叠**，重叠期内新旧凭据都可验签；
- `deprecated=true` 且仍在有效期内 → **允许**（弃用宽限），仅在日志中标注；
- 一旦 `now > not_after`，即使未标记 deprecated 也一律拒绝（`EXPIRED_CREDENTIAL`）；
- 认证按 `X-Tenant-Id` 在本租户凭据集合内匹配，**跨租户凭据不可互用**。

出站投递同理：网关注入 `X-Key-Id/X-Timestamp/X-Signature`（另有
`X-Delivery-Id: <event_id>:<attempt>`），接收方可在凭据轮换重叠期平滑切换。

---

## 3. 幂等与租户隔离

### 3.1 幂等键与并发保证

- 幂等键为 **`(tenant_id, event_id)`**；
- 受理时先获取**事件级互斥锁**，在锁内执行「是否已存在 → 唯一插入」，
  因此同一事件的重复请求或并发到达**只被处理一次**；
- 仅认证、配额、契约全部通过后才落库并占用幂等键（失败请求不占用键，
  客户端修正后可安全重试）。

### 3.2 稳定且可区分的结论

| 场景 | HTTP | 响应 |
|---|---|---|
| 首次受理 | **202** | `accepted=true, duplicate=false` |
| 重复请求（含并发落败者） | **200** | `accepted=true, duplicate=true`，`detail` 回带首次结论 `original status=...` |

重复响应不重新执行任何投递，只返回首次的稳定结论。

### 3.3 租户隔离

- 事件、凭据、订阅、配额窗口均以 `tenant_id` 为边界物理隔离；
- **配额**：每租户独立的滑动 60 秒窗口（种子数据：tenant-a 60/min，
  tenant-b 5/min），超限返回 **429 `QUOTA_EXCEEDED`**；一个租户耗尽额度
  不影响任何其他租户；重复请求不重复占用额度。

---

## 4. 契约（Schema）版本兼容

契约按 **`事件类型 + 主版本`** 注册（语义化版本 `M.m.p`）。

### 4.1 兼容规则（确定性）

1. 事件主版本在注册表中不存在 → 拒绝受理：**422 `UNKNOWN_EVENT_VERSION`**；
2. 订阅声明版本与事件契约**主版本不一致** → 该订阅不扇出，记录
   `CONTRACT_INCOMPATIBLE`（主版本代表破坏性变更）；
3. 同主版本内**前向兼容**：事件次版本高于注册契约时，契约未声明的字段记为
   `unknown_fields` —— **不报错、不丢弃**，payload 原样投递，字段名落库、
   在受理响应与事件查询接口中可查；
4. 注册契约中 `required=true` 的字段必须存在且类型匹配
   （`string/int/float/bool`），否则 **422 `CONTRACT_INCOMPATIBLE`**，
   `detail` 明确指出字段名与期望类型。

> 设计取向：未知版本/未知字段属于「需要确定结论」的输入，既不能导致服务
> 异常，也不能静默吞掉——未知主版本显式拒绝，同主版本未知字段显式保留并登记。

### 4.2 示例

注册契约 `order.created` 主版本 1（注册版本 `1.2.0`，必填 `order_id:string`）：

| 入站事件版本 | payload | 结论 |
|---|---|---|
| `9.0.0` | 任意 | 422 `UNKNOWN_EVENT_VERSION` |
| `1.0.0`/`1.3.0` | `{"order_id":"o1","promo":"x"}` | 受理；`unknown_fields=["promo"]`，payload 保留 `promo` |
| `1.0.0` | `{"amount":1}`（缺 order_id） | 422 `CONTRACT_INCOMPATIBLE: missing required field 'order_id'` |
| `2.0.0` | 合法 | 受理成功，但 v2 订阅与 v1 订阅互不扇出（主版本不匹配） |

---

## 5. 出站投递：重试、退避与顺序

### 5.1 响应分类（可解释）

| 接收方响应 | 分类 | 处理 |
|---|---|---|
| 2xx | `SUCCESS` | 投递成功，任务终结 |
| 408 / 425 / 429 / 5xx | `RETRYABLE` | 退避后重试 |
| 其余 4xx | `PERMANENT` | **不重试**，立即死信（`HTTP_4XX`） |
| 连接错误 / 超时 | `RETRYABLE` | 退避后重试（`NETWORK_ERROR`） |

### 5.2 退避策略（可解释、可复现）

- 第 `n` 次尝试失败后的等待：

  ```
  backoff(n) = min(MAX_BACKOFF, BASE * 2^(n-1))
  ```

  默认 `BASE=0.05s`（本地验证用小基数）、`MAX_BACKOFF=1s`，
  **无随机抖动**，因此退避序列与测试结论完全确定；
- 每次尝试的状态码、分类依据（basis）与当次 backoff 都记录在
  `attempts[]` 中，可在投递查询接口逐条审计。

### 5.3 重试上限与死信（结果不静默丢失）

- 达到订阅 `max_attempts`（默认 4）仍失败 → 状态 `DEAD_LETTER`；
- `fail_reason` 区分终局原因：
  - 可重试故障耗尽 → `ATTEMPTS_EXHAUSTED`，并在 `fail_detail` 保留
    **最后一跳**的底层依据 `last_basis=NETWORK_ERROR / HTTP_5XX`；
  - 永久 4xx → `HTTP_4XX`（仅尝试 1 次）；
- 死信可查询：`GET /admin/deliveries/dead-letter`（可按租户过滤），
  返回完整尝试历史与失败细节。

### 5.4 顺序保证

- 每个订阅一条 FIFO 队列，入队即分配单调递增 `seq`；
- **队首未终结（成功/死信）前，后续事件一律不得发出**；前序事件处于退避
  等待时会阻塞队头（head-of-line blocking），从机制上保证同一订阅严格有序；
- 不同订阅之间相互独立、可并发。

---

## 6. HTTP 接口一览

| 方法 & 路径 | 说明 |
|---|---|
| `POST /v1/events/inbound` | 入站事件受理（签名头见 §2） |
| `PUT /admin/tenants/{id}` | 创建/更新租户（含每分钟额度） |
| `PUT /admin/tenants/{id}/credentials` | 登记凭据（轮换时间窗） |
| `GET /admin/tenants/{id}/credentials` | 查看凭据及有效期 |
| `PUT /admin/schemas` | 注册事件契约（类型+版本+字段） |
| `PUT /admin/tenants/{id}/subscriptions` | 创建订阅（回调/版本/重试上限） |
| `GET /admin/deliveries` | 投递总览（可按 tenant、status 过滤） |
| `GET /admin/deliveries/dead-letter` | **死信链路查询** |
| `GET /admin/tenants/{id}/events/{eid}` | 查询事件（状态/契约版本/未知字段） |
| `POST /admin/pump` | 手动推进一次到期投递 |
| `POST /admin/drain` | 等待全部重试完成（演示用） |
| `POST /admin/mock/script` | 编排回调响应序列（本地复现故障） |
| `POST /admin/reset` | 重置到种子状态 |

事件状态机：`ACCEPTED → SCHEDULED → DELIVERING → RETRYING → … → DELIVERED`
或 `… → DEAD_LETTER`；重复入站事件以 `DUPLICATE` 结论返回（不改变状态）。
