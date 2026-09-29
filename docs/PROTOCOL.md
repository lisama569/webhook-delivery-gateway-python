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

#### 2.4.1 出站凭据有效期（与入站同一套窗口）

投递在**发起请求之前**先校验订阅配置的出站凭据，规则与入站验签完全一致：

| 出站凭据情况 | 处理 | FailReason |
|---|---|---|
| 在 `not_before ~ not_after` 窗口内 | 正常签名发出 | `NONE` |
| 凭据不存在（key 已被移除） | **不发请求**，立即死信 | `CREDENTIAL_MISSING` |
| `now < not_before`（尚未生效） | **不发请求**，立即死信 | `CREDENTIAL_NOT_YET_VALID` |
| `now > not_after`（已过期） | **不发请求**，立即死信 | `CREDENTIAL_EXPIRED` |

这是**网关侧**结论，与接收方返回的失败明确区分：凭据问题时 `http_status`
为空、`fail_detail` 带 `(no request sent)`、实际请求一次都不会发出；而
`NETWORK_ERROR/HTTP_5XX/HTTP_4XX/ATTEMPTS_EXHAUSTED` 都是请求真正到达
（或尝试到达）接收方后得到的对方侧结论。轮换出站凭据后，对该事件发起
**重放**即可用新凭据重新投递（见 §7）。

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

### 5.5 运行态自动运转（无需手工推动）

- 服务通过 FastAPI lifespan 启动一个**后台投递 worker**
  （节拍 `WORKER_POLL_SECONDS=0.02s`），周期性推进所有到期任务；
- 事件受理入队后，首次投递、退避到期重试、重试耗尽进入死信——整条链路
  **自动往前走，不需要任何手工 pump**；退避节奏仍由 `next_attempt_at` 的
  真实到期决定，确定性退避公式不变；
- worker 对单次 tick 的异常做隔离记录（`delivery_worker_tick_failed`），
  不会因个别故障退出；关停时随 lifespan 优雅取消；
- 本地演示/测试仍可使用确定性的 `run_pending(now=...)` 与 `run_until_drained`
  手动驱动，二者共用同一套投递逻辑，结论一致。

---

## 5a. 订阅暂停 / 恢复（运维开关）

粒度是**单个订阅**，只作用于"是否真的把请求发出去"，不影响入站受理。

| 规则 | 说明 |
|---|---|
| 暂停期间入站照常 | 新事件仍完成认证/幂等/配额/契约，返回 **202**，照常分配 `seq` 进入该订阅 FIFO 排队，只是**不发出** |
| 顺序保留 | 暂停不丢弃、不乱序；积压事件的相对顺序就是 `seq` 顺序 |
| 恢复后续发 | 恢复后从队首开始按原顺序投递；队头若处于退避，仍遵守队头阻塞 |
| 订阅级隔离 | 暂停订阅 A 不拖累订阅 B 的投递，也不影响任何订阅的入站受理 |
| 幂等可重复 | 对同一订阅重复暂停/恢复，结论稳定（状态恒为 `PAUSED`/`ACTIVE`） |

接口与结论：

- `POST .../subscriptions/{id}/pause` → `{"state":"PAUSED"}`；
- `POST .../subscriptions/{id}/resume` → `{"state":"ACTIVE"}`；
- `GET .../subscriptions/{id}/state` → 当前 `state` 与待投递积压数 `pending`；
- 订阅不存在或不属于该租户 → **404**。

---

## 5b. 失败事件重放（Replay）

死信链路中的事件可以重放：支持**单条**与**按订阅批量**。

### 5b.1 通用规则

1. **只有 `DEAD_LETTER` 可重放**；正在投递（`SCHEDULED/DELIVERING/RETRYING`）
   的不重复认领；已 `DELIVERED` 的**绝不重发**；记录不存在返回 `NOT_FOUND`。
2. **保序**：重放把事件重新排到该订阅 FIFO 中（其 `seq` 不变，仍在所有
   更早事件之后），绝不插到尚未终结的前序事件之前；重放后若队头进入退避，
   后序事件（含期间新入队的）一样被队头阻塞。
3. **尝试计数重置、历史归档保留**：重放后新一轮尝试从 `attempt=1` 重新计数；
   原死信结论写入 `archived_fail_reason/archived_fail_detail/archived_at`，
   `replay_count` 加一，审计时新旧两轮互不覆盖。
4. **幂等且并发安全**：单条重放在订阅锁内"判定+认领"，因此对同一事件并发或
   连续触发 N 次，**只有一次真正生效**，其余得到稳定可区分的结论。

### 5b.2 单条重放结论（`outcome`，HTTP 状态可区分）

| outcome | HTTP | 含义 |
|---|---|---|
| `REPLAYED` | **202** | 本次请求生效，事件已重新排队 |
| `ALREADY_REPLAYING` | **200** | 先前请求已重放，事件正在投递中（本次不重复） |
| `ALREADY_DELIVERED` | **409** | 已成功投递，拒绝重发 |
| `NOT_DEAD_LETTER` | **409** | 当前不在失败链路且未成功（一般不会出现，保留） |
| `NOT_FOUND` | **404** | 投递记录不存在 |

### 5b.3 批量重放

- `POST .../subscriptions/{id}/replay-dead-letters`：重放该订阅下**所有仍在
  失败链路**的事件；
- 严格按 `seq` 升序**逐条**在订阅锁内处理：天然保序，且逐条认领/排队形成
  **反压**，积压再多也是一步步处理，不会一次性压垮服务；
- 返回逐条结果与汇总计数 `replayed / already_replaying / already_delivered /
  not_dead_letter`；
- **并发/连续触发的幂等**：同一批死信在代际冷却窗
  （`BATCH_REPLAY_COOLDOWN_SECONDS=5s`）内被重复批量重放时，只有一批真正
  重新排队，另一批对同一组事件逐条返回 `ALREADY_REPLAYING`，两批 `total`
  一致、结论一一对应；冷却窗之后若事件再次落入死信，视为**新一轮**可再重放。

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
| `POST /admin/tenants/{id}/subscriptions/{sid}/pause` | **暂停**该订阅投递（幂等） |
| `POST /admin/tenants/{id}/subscriptions/{sid}/resume` | **恢复**该订阅投递（幂等） |
| `GET /admin/tenants/{id}/subscriptions/{sid}/state` | 查询订阅开关状态与积压数 |
| `POST /admin/tenants/{id}/subscriptions/{sid}/events/{eid}/replay` | **单条失败重放** |
| `POST /admin/tenants/{id}/subscriptions/{sid}/replay-dead-letters` | **批量重放**该订阅全部死信 |
| `GET /admin/deliveries` | 投递总览（可按 tenant、status 过滤） |
| `GET /admin/deliveries/dead-letter` | **死信链路查询** |
| `GET /admin/tenants/{id}/events/{eid}` | 查询事件（状态/契约版本/未知字段） |
| `POST /admin/pump` | 手动推进一次到期投递（演示/测试用） |
| `POST /admin/drain` | 等待全部重试完成（演示用） |
| `POST /admin/mock/script` | 编排回调响应序列（本地复现故障） |
| `POST /admin/reset` | 重置到种子状态 |

事件状态机：`ACCEPTED → SCHEDULED → DELIVERING → RETRYING → … → DELIVERED`
或 `… → DEAD_LETTER`；重复入站事件以 `DUPLICATE` 结论返回（不改变状态）。
`DEAD_LETTER` 经重放后回到 `SCHEDULED` 重新走投递状态机，原死信结论归档到
`archived_*` 字段、`replay_count` 加一。

---

## 7. 失败原因（FailReason）归属一览

| 侧 | FailReason | 是否发出请求 | 可否重试/重放 |
|---|---|---|---|
| 入站拒绝 | `UNKNOWN_TENANT / INVALID_SIGNATURE / EXPIRED_CREDENTIAL / STALE_TIMESTAMP / QUOTA_EXCEEDED / UNKNOWN_EVENT_VERSION / CONTRACT_INCOMPATIBLE` | — | 修正后可重新提交（不占幂等键） |
| **网关侧（出站凭据）** | `CREDENTIAL_MISSING / CREDENTIAL_NOT_YET_VALID / CREDENTIAL_EXPIRED` | **否** | 轮换凭据后**重放** |
| 接收方侧 | `NETWORK_ERROR` | 是（连接失败） | 自动退避重试 |
| 接收方侧 | `HTTP_5XX`（含 408/425/429/5xx） | 是 | 自动退避重试 |
| 接收方侧 | `HTTP_4XX`（其余 4xx） | 是 | 不重试，立即死信，可重放 |
| 终局 | `ATTEMPTS_EXHAUSTED` | 是 | 重试耗尽死信，可重放 |

---

## 8. 兼容范围

- **契约/版本**：兼容策略不变——同主版本前向兼容、未知字段保留登记、未知
  主版本 422 拒绝（见 §4）；本轮不改变入站报文与签名协议。
- **HTTP API**：所有既有路径与响应字段保持不变；本轮为**纯增量**——新增
  暂停/恢复/状态、单条/批量重放共 5 个管理端点；投递记录在原有字段上新增
  `replayed / replay_count / archived_fail_reason / archived_fail_detail /
  archived_at`（旧消费方忽略未知字段即可）。
- **失败原因枚举**：新增 `CREDENTIAL_MISSING / CREDENTIAL_NOT_YET_VALID /
  CREDENTIAL_EXPIRED` 三个出站凭据原因，既有 `NETWORK_ERROR/HTTP_4XX/
  HTTP_5XX/ATTEMPTS_EXHAUSTED` 语义不变。
- **运行形态**：默认随服务启动后台自动投递；`/admin/pump`、`/admin/drain`
  保留，便于离线确定性演示与测试。
- 存储仍为**进程内内存实现**：暂停状态、队列、重放代际标记不跨进程持久化，
  定位是本地验证与单机演示。
